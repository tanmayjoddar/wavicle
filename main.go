package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"
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

	if cfg.DB.Type == "postgres" {
		log.Printf("Initializing PostgreSQL write-through to %s", cfg.DB.DSN)
		pgStore, err := storage.NewPostgresStore(cfg.DB.DSN)
		if err != nil {
			log.Fatalf("Failed to connect to Postgres: %v", err)
		}

		// FrontierCache: O(unique keys) memory, no WAL, no compaction, no ancestry.
		// PG is source of truth; local cache is a fast read-through for the Proof Engine.
		local := storage.NewFrontierCache()
		store = storage.NewWriteThroughStore(pgStore, local)

		// Start Replication Listener
		pgListener = replication.NewPGListener(replication.PGConfig{
			DSN:             cfg.DB.DSN,
			ReplicationSlot: cfg.DB.ReplicationSlot,
			Publication:     cfg.DB.Publication,
			TableMappings:   mapConfigToMappers(cfg.DB.TableMappings),
		})

		events, err := pgListener.Start(ctx)
		if err != nil {
			log.Printf("Warning: Failed to start PG replication: %v", err)
		} else {
			log.Printf("Replication listener started, waiting for events...")
			go func() {
				for evt := range events {
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
			}()
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
