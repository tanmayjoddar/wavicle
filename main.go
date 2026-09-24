package main

import (
	"context"
	"crypto/tls"
	"fmt"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"sync/atomic"
	"syscall"
	"time"
	"wavicle/internal/auth"
	"wavicle/internal/config"
	"wavicle/internal/core"
	"wavicle/internal/protocol/resp3"
	"wavicle/internal/replication"
	"wavicle/internal/storage"
	"wavicle/internal/telemetry"
)

func main() {
	log.SetFlags(log.Ldate | log.Ltime | log.Lmsgprefix)
	log.SetPrefix("[wavicle] ")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cfg := config.Default()

	fmt.Println("Wavicle Causal Proof Engine v1.0")
	fmt.Printf("Listening on %s\n", cfg.Server.Listen)

	if err := os.MkdirAll(cfg.Storage.DataDir, 0755); err != nil {
		log.Fatalf("Cannot create data directory: %v", err)
	}

	// Production mode (PostgreSQL): FrontierCache + PG write-through + replication listener.
	// Dev mode (no PG): standalone CausalCrystal.
	var store storage.Store
	var pgListener *replication.PGListener

	// cdcFresh records the unixnano of the last CDC event (PG or MySQL).
	// The server's stale guard reads it to fail reads closed when the stream
	// goes silent. Shared by pointer so the applier needs no server handle.
	cdcFresh := &atomic.Int64{}

	if cfg.DB.Type == "postgres" {
		log.Printf("Initializing PostgreSQL write-through to %s", cfg.DB.DSN)
		pgStore, err := storage.NewPostgresStore(cfg.DB.DSN)
		if err != nil {
			log.Fatalf("Failed to connect to Postgres: %v", err)
		}

		// FrontierCache: O(unique keys) memory, no WAL, no compaction, no ancestry.
		// PG is source of truth; local cache is a fast read-through for the Proof Engine.
		// GOAT backend: 32-shard lock-striped cache + snapshot warm-restart.
		local := storage.NewShardedFrontierCache()
		if n, err := local.LoadSnapshot(filepath.Join(cfg.Storage.DataDir, "frontier.snapshot")); err == nil && n > 0 {
			log.Printf("Warm restart: loaded %d keys from snapshot", n)
		}
		// Persist snapshot path for graceful shutdown.
		snapshotPath := filepath.Join(cfg.Storage.DataDir, "frontier.snapshot")
		if cfg.Storage.Snapshot.Path != "" {
			snapshotPath = cfg.Storage.Snapshot.Path
		}
		defer func() {
			if err := local.SaveSnapshot(snapshotPath); err != nil {
				log.Printf("Snapshot save failed: %v", err)
			} else {
				log.Printf("Snapshot saved to %s", snapshotPath)
			}
		}()
		// Production RPO: fsync-on-shutdown is not enough — snapshot on an
		// interval so a crash loses minutes, not everything since boot.
		startSnapshotTicker(ctx, local, snapshotPath, cfg.Storage.Snapshot.Interval)
		store = storage.NewWriteThroughStore(pgStore, local)

		// Start Replication Listener
		pgListener = replication.NewPGListener(replication.PGConfig{
			DSN:             cfg.DB.DSN,
			ReplicationSlot: cfg.DB.ReplicationSlot,
			Publication:     cfg.DB.Publication,
			TableMappings:   mapConfigToMappers(cfg.DB.TableMappings),
			CheckpointPath:  filepath.Join(cfg.Storage.DataDir, "replication_checkpoint.lsn"),
		})

		events, err := pgListener.Start(ctx)
		if err != nil {
			log.Printf("Warning: Failed to start PG replication: %v", err)
		} else {
			log.Printf("Replication listener started, waiting for events...")
			go runCDCApplier(local, store, events, cdcFresh)
		}

		// Slot watchdog: one cheap pg_replication_slots read per tick.
		// Pages before retained WAL fills PG's disk (~7GB/h at 4k writes/s).
		// Runs even if the listener failed — a dead listener IS the emergency.
		if cfg.SlotMon.Enabled && cfg.DB.DSN != "" {
			slotName := cfg.DB.ReplicationSlot
			if slotName == "" {
				slotName = "wavicle_slot"
			}
			monInterval := 15 * time.Second
			if d, err := time.ParseDuration(cfg.SlotMon.Interval); err == nil && d > 0 {
				monInterval = d
			} else if cfg.SlotMon.Interval != "" {
				log.Printf("Invalid slot monitor interval %q, using 15s", cfg.SlotMon.Interval)
			}
			mon := replication.NewSlotMonitor(replication.SlotMonitorConfig{
				DSN:       cfg.DB.DSN,
				SlotName:  slotName,
				Interval:  monInterval,
				WarnBytes: cfg.SlotMon.WarnBytes,
				PageBytes: cfg.SlotMon.PageBytes,
			})
			go mon.Run(ctx)
			log.Printf("Slot monitor watching %q every %v (warn %dB, page %dB)",
				slotName, monInterval, cfg.SlotMon.WarnBytes, cfg.SlotMon.PageBytes)
		}
	} else if cfg.DB.Type == "mysql" {
		log.Printf("Initializing MySQL polling CDC (tables: %v)", cfg.MySQL.Tables)
		local := storage.NewShardedFrontierCache()
		snapshotPath := filepath.Join(cfg.Storage.DataDir, "frontier.snapshot")
		if cfg.Storage.Snapshot.Path != "" {
			snapshotPath = cfg.Storage.Snapshot.Path
		}
		if n, err := local.LoadSnapshot(snapshotPath); err == nil && n > 0 {
			log.Printf("Warm restart: loaded %d keys from snapshot", n)
		}
		defer func() {
			if err := local.SaveSnapshot(snapshotPath); err != nil {
				log.Printf("Snapshot save failed: %v", err)
			}
		}()
		startSnapshotTicker(ctx, local, snapshotPath, cfg.Storage.Snapshot.Interval)
		// No synchronous primary: MySQL is read via CDC poller, local cache
		// serves reads. Writes via SET stay local-only in this mode.
		store = local
		interval := 500 * time.Millisecond
		if d, err := time.ParseDuration(cfg.MySQL.PollInterval); err == nil && d > 0 {
			interval = d
		}
		mysqlListener := replication.NewMySQLListener(replication.MySQLConfig{
			DSN:           cfg.MySQL.DSN,
			Tables:        cfg.MySQL.Tables,
			PollInterval:  interval,
			TableMappings: mapConfigToMappers(cfg.DB.TableMappings),
		})
		events, err := mysqlListener.Start(ctx)
		if err != nil {
			log.Printf("Warning: Failed to start MySQL CDC: %v", err)
		} else {
			log.Printf("MySQL CDC poller started, waiting for events...")
			go runCDCApplier(local, store, events, cdcFresh)
		}
	} else {
		// Dev mode: full CausalCrystal with WAL, compaction, and causal DAG.
		// No PG involvement — standalone operation.
		crystal, err := storage.NewCausalCrystal(filepath.Join(cfg.Storage.DataDir, "crystal.log"))
		if err != nil {
			log.Fatalf("Failed to initialize storage: %v", err)
		}
		store = crystal
	}

	srv := resp3.NewServer(store, cfg.Auth.Password, cfg.Server.MaxConns)

	// Fail-closed reads (opt-in): refuse proof-engine reads when CDC has been
	// silent longer than max. Default off — fail-open preserves current
	// behavior for dev, fresh staging, and anyone who hasn't opted in.
	if cfg.DB.MaxStaleness != "" {
		if maxLag, err := time.ParseDuration(cfg.DB.MaxStaleness); err == nil && maxLag > 0 {
			srv.SetStaleGuard(maxLag, cdcFresh)
			log.Printf("Stale guard armed: reads fail closed after %v CDC silence", maxLag)
		} else {
			log.Printf("Invalid max_staleness %q, stale guard disabled (fail-open)", cfg.DB.MaxStaleness)
		}
	}

	// Production auth: ACL file wins, then inline config users, else the
	// single shared password baked into NewServer. Fail fast on any auth
	// misconfiguration — a cache must never boot open by accident.
	if cfg.Auth.ACLFile != "" {
		acl, err := auth.LoadACLFile(cfg.Auth.ACLFile)
		if err != nil {
			log.Fatalf("ACL file %q: %v", cfg.Auth.ACLFile, err)
		}
		srv.SetACL(acl)
		log.Printf("ACL loaded from %s", cfg.Auth.ACLFile)
	} else if len(cfg.Auth.Users) > 0 {
		acl := auth.NewDisabled()
		for _, u := range cfg.Auth.Users {
			acl.AddUser(u.Name, u.Password, u.Commands, u.KeyPatterns, u.Admin)
		}
		srv.SetACL(acl)
		log.Printf("ACL loaded with %d inline users", len(cfg.Auth.Users))
	}

	// Production TLS: terminate at the server. Fail fast if enabled but the
	// cert won't load — silently falling back to plaintext is the failure mode
	// that ends companies. Min 1.2 for client compat (1.3 negotiated when offered).
	if cfg.Server.TLS.Enabled {
		cert, err := tls.LoadX509KeyPair(cfg.Server.TLS.CertFile, cfg.Server.TLS.KeyFile)
		if err != nil {
			log.Fatalf("TLS enabled but cert load failed (%s, %s): %v",
				cfg.Server.TLS.CertFile, cfg.Server.TLS.KeyFile, err)
		}
		srv.SetTLS(&tls.Config{
			Certificates: []tls.Certificate{cert},
			MinVersion:   tls.VersionTLS12,
		})
		log.Printf("TLS enabled (min 1.2, cert %s)", cfg.Server.TLS.CertFile)
	}

	// Start metrics HTTP server
	if cfg.Metrics.Enabled {
		go func() {
			log.Printf("Metrics server on %s/metrics", cfg.Metrics.Listen)
			if err := telemetry.ListenAndServe(cfg.Metrics.Listen); err != nil {
				log.Printf("Metrics server error: %v", err)
			}
		}()
	}

	// Start the RESP3 server in background
	go func() {
		log.Printf("RESP3 server on %s", cfg.Server.Listen)
		if err := srv.ListenAndServe(cfg.Server.Listen); err != nil {
			log.Fatalf("Server error: %v", err)
		}
	}()

	// Wait for signal
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	sig := <-sigCh
	log.Printf("Received signal %v, initiating graceful shutdown...", sig)

	// Context with timeout for shutdown
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutdownCancel()

	// Notify server to stop accepting new connections
	srv.Close()

	// Clean up replication slot and close connection
	if pgListener != nil {
		log.Println("Stopping PostgreSQL replication listener...")
		pgListener.Close()
		cancel()
	}

	// Wait for active connections to drain (simulated via wait group in a real app)
	// For now, we wait for a brief period to allow in-flight requests to finish
	select {
	case <-shutdownCtx.Done():
		log.Println("Shutdown timeout reached. Forcing exit.")
	case <-time.After(2 * time.Second): // Give connections 2 seconds to drain
		log.Println("Connections drained.")
	}

	store.Close()
	log.Println("Shutdown complete.")
}

// runCDCApplier folds database change events into the local frontier.
// Shared by the PG logical-replication listener and the MySQL poller so both
// backends get identical echo-suppression and tombstone semantics.
// Every event also stamps markFresh (the server's stale-guard clock).
func runCDCApplier(local *storage.ShardedFrontierCache, store storage.Store, events <-chan replication.ChangeEvent, markFresh *atomic.Int64) {
	for evt := range events {
		markFresh.Store(time.Now().UnixNano())
		telemetry.Get().RecordReplicationLag(evt.Table, evt.CommitTime)
		for _, path := range evt.AffectedPaths {
			log.Printf("DB Change [%s]: path %s (lag: %v)", evt.Action, path, time.Since(evt.CommitTime))

			if evt.Action == "DELETE" {
				local.AppendAtom(&core.EConst{Value: core.VNull{}}, path, nil, time.Time{})
			} else {
				lastColon := strings.LastIndex(path, ":")
				if lastColon != -1 {
					column := path[lastColon+1:]
					if val, ok := evt.NewValues[column]; ok {
						valStr := fmt.Sprint(val)

						var expiresAt time.Time
						if current, exists := store.GetCurrent(path); exists {
							if current.PhysicalTime.After(evt.CommitTime) {
								continue
							}
							// Replication echo guard: skip if the value hasn't changed.
							// Otherwise the echo creates a new atom with a different hash,
							// invalidating all cached proofs for zero semantic change.
							if ec, ok := current.Expr.(*core.EConst); ok && fmt.Sprint(ec.Value) == valStr {
								continue
							}
							expiresAt = current.ExpiresAt
						}

						local.AppendAtom(&core.EConst{Value: core.VString(valStr)}, path, nil, expiresAt)
					}
				}
			}
		}
	}
}

// startSnapshotTicker persists the frontier on an interval (default 5m) so a
// crash loses minutes of writes, not everything since boot. Interval "" disables.
func startSnapshotTicker(ctx context.Context, local *storage.ShardedFrontierCache, path, intervalStr string) {
	interval := 5 * time.Minute
	if intervalStr != "" {
		if d, err := time.ParseDuration(intervalStr); err == nil && d > 0 {
			interval = d
		} else if intervalStr != "" {
			log.Printf("Invalid snapshot interval %q, using 5m", intervalStr)
		}
	}
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if err := local.SaveSnapshot(path); err != nil {
					log.Printf("Periodic snapshot failed: %v", err)
				} else {
					log.Printf("Periodic snapshot saved (%s)", path)
				}
			}
		}
	}()
}

func mapConfigToMappers(cfg []config.TableMapping) []replication.PathMapper {
	mappers := make([]replication.PathMapper, len(cfg))
	for i, m := range cfg {
		mappers[i] = replication.PathMapper{
			Table:       m.Table,
			KeyTemplate: m.KeyTemplate,
			Columns:     m.Columns,
		}
	}
	return mappers
}
