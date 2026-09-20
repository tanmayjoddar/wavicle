package integration_test

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	_ "github.com/lib/pq"

	"wavicle/internal/core"
	"wavicle/internal/replication"
	"wavicle/internal/storage"
)

func TestReplication_CheckpointAndResume(t *testing.T) {
	db, err := sql.Open("postgres", testDSN)
	if err != nil {
		t.Skipf("Skipping test: cannot connect to Postgres: %v", err)
	}
	defer db.Close()
	if err := db.Ping(); err != nil {
		t.Skipf("Skipping test: Postgres not reachable: %v", err)
	}

	slotName := fmt.Sprintf("wavicle_resilience_%d", time.Now().UnixNano()%10000000)
	defer func() {
		deadline := time.Now().Add(2 * time.Second)
		for time.Now().Before(deadline) {
			if _, err := db.Exec("SELECT pg_drop_replication_slot($1)", slotName); err == nil {
				break
			}
			time.Sleep(25 * time.Millisecond)
		}
	}()

	tempDir := t.TempDir()
	checkpointFile := filepath.Join(tempDir, "checkpoint.lsn")

	// Prepare users table
	_, err = db.Exec(`
		CREATE TABLE IF NOT EXISTS users (
			id TEXT PRIMARY KEY,
			name TEXT,
			email TEXT,
			created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
		);
		ALTER TABLE users REPLICA IDENTITY FULL;
		DELETE FROM users WHERE id IN ('123', 'resilience_1');
		INSERT INTO users (id, name, email) VALUES ('123', 'Initial_Val', 'init@example.com');
	`)
	if err != nil {
		t.Fatalf("Failed to initialize test table: %v", err)
	}

	// -------------------------------------------------------------
	// PHASE 1: Start listener 1, process initial mutation, and stop
	// -------------------------------------------------------------
	ctx1, cancel1 := context.WithCancel(context.Background())
	listener1 := replication.NewPGListener(replication.PGConfig{
		DSN:             testDSN,
		ReplicationSlot: slotName,
		Publication:     testPubName,
		CheckpointPath:  checkpointFile,
	})

	events1, err := listener1.Start(ctx1)
	if err != nil {
		cancel1()
		t.Fatalf("Listener 1 start failed: %v", err)
	}
	if !listener1.WaitReady(3 * time.Second) {
		cancel1()
		t.Fatal("Timeout waiting for Listener 1 to enter running state")
	}

	cache1 := storage.NewFrontierCache()
	defer cache1.Close()

	var wg1 sync.WaitGroup
	wg1.Add(1)
	phase1Seen := make(chan struct{})

	go func() {
		defer wg1.Done()
		for evt := range events1 {
			for _, path := range evt.AffectedPaths {
				if lastColon := strings.LastIndex(path, ":"); lastColon != -1 {
					col := path[lastColon+1:]
					if val, ok := evt.NewValues[col]; ok {
						valStr := fmt.Sprint(val)
						cache1.AppendAtom(&core.EConst{Value: core.VString(valStr)}, path, nil, time.Time{})
						if path == "users:123:name" && valStr == "Phase1_Alive" {
							select {
							case <-phase1Seen:
							default:
								close(phase1Seen)
							}
						}
					}
				}
			}
		}
	}()

	// Execute phase 1 mutation
	_, err = db.Exec("UPDATE users SET name = 'Phase1_Alive' WHERE id = '123'")
	if err != nil {
		t.Fatalf("Phase 1 update failed: %v", err)
	}

	select {
	case <-phase1Seen:
		t.Log("✓ Listener 1 processed Phase 1 mutation: users:123:name -> Phase1_Alive")
	case <-time.After(3 * time.Second):
		t.Fatal("Timeout waiting for Listener 1 to process Phase 1 mutation")
	}

	// Cleanly stop Listener 1 (simulating process termination)
	cancel1()
	listener1.Close()
	wg1.Wait()

	// Verify checkpoint file was written to disk
	checkpointData, err := os.ReadFile(checkpointFile)
	if err != nil {
		t.Fatalf("Expected checkpoint file to exist on disk: %v", err)
	}
	lsnStr := strings.TrimSpace(string(checkpointData))
	if lsnStr == "" || lsnStr == "0/0" {
		t.Fatalf("Invalid LSN in checkpoint file: %q", lsnStr)
	}
	t.Logf("✓ Checkpoint persisted to disk: %s", lsnStr)

	// -------------------------------------------------------------
	// PHASE 2: Mutate Postgres while Wavicle / Listener is offline
	// -------------------------------------------------------------
	t.Log("Executing mutations in Postgres while Wavicle listener is DEAD...")
	_, err = db.Exec("UPDATE users SET name = 'Phase2_OfflineUpdate' WHERE id = '123'")
	if err != nil {
		t.Fatalf("Offline update failed: %v", err)
	}
	_, err = db.Exec("INSERT INTO users (id, name, email) VALUES ('resilience_1', 'Phase2_OfflineNewUser', 'offline@wavicle.io')")
	if err != nil {
		t.Fatalf("Offline insert failed: %v", err)
	}

	// -------------------------------------------------------------
	// PHASE 3: Restart Listener 2 with existing checkpoint file
	// -------------------------------------------------------------
	t.Log("Restarting Listener 2, resuming from checkpoint...")
	ctx2, cancel2 := context.WithCancel(context.Background())
	defer cancel2()

	listener2 := replication.NewPGListener(replication.PGConfig{
		DSN:             testDSN,
		ReplicationSlot: slotName,
		Publication:     testPubName,
		CheckpointPath:  checkpointFile,
	})
	defer listener2.Close()

	events2, err := listener2.Start(ctx2)
	if err != nil {
		t.Fatalf("Listener 2 start failed: %v", err)
	}
	if !listener2.WaitReady(3 * time.Second) {
		t.Fatal("Timeout waiting for Listener 2 to enter running state")
	}

	cache2 := storage.NewFrontierCache()
	defer cache2.Close()

	phase2UpdateSeen := make(chan struct{})
	phase2InsertSeen := make(chan struct{})

	go func() {
		for evt := range events2 {
			for _, path := range evt.AffectedPaths {
				if lastColon := strings.LastIndex(path, ":"); lastColon != -1 {
					col := path[lastColon+1:]
					if val, ok := evt.NewValues[col]; ok {
						valStr := fmt.Sprint(val)
						cache2.AppendAtom(&core.EConst{Value: core.VString(valStr)}, path, nil, time.Time{})
						if path == "users:123:name" && valStr == "Phase2_OfflineUpdate" {
							select {
							case <-phase2UpdateSeen:
							default:
								close(phase2UpdateSeen)
							}
						}
						if path == "users:resilience_1:name" && valStr == "Phase2_OfflineNewUser" {
							select {
							case <-phase2InsertSeen:
							default:
								close(phase2InsertSeen)
							}
						}
					}
				}
			}
		}
	}()

	// Verify both offline mutations were caught up from WAL backlog
	select {
	case <-phase2UpdateSeen:
		t.Log("✓ Listener 2 caught up offline UPDATE from LSN checkpoint: Phase2_OfflineUpdate")
	case <-time.After(3 * time.Second):
		t.Fatal("Timeout waiting for Listener 2 to replay offline UPDATE")
	}

	select {
	case <-phase2InsertSeen:
		t.Log("✓ Listener 2 caught up offline INSERT from LSN checkpoint: Phase2_OfflineNewUser")
	case <-time.After(3 * time.Second):
		t.Fatal("Timeout waiting for Listener 2 to replay offline INSERT")
	}

	// Verify values in cache2 match reality
	atom1, ok1 := cache2.GetCurrent("users:123:name")
	if !ok1 || atom1.Expr.(*core.EConst).Value != core.VString("Phase2_OfflineUpdate") {
		t.Fatalf("Cache 2 has incorrect value for users:123:name: %+v", atom1)
	}

	atom2, ok2 := cache2.GetCurrent("users:resilience_1:name")
	if !ok2 || atom2.Expr.(*core.EConst).Value != core.VString("Phase2_OfflineNewUser") {
		t.Fatalf("Cache 2 has incorrect value for users:resilience_1:name: %+v", atom2)
	}

	t.Log("✓ Resilience verified: Mid-stream termination & restart resumed cleanly from LSN checkpoint with zero data loss or stale reads")
}
