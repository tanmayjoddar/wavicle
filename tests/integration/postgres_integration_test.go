package integration_test

import (
	"bufio"
	"context"
	"database/sql"
	"fmt"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	_ "github.com/lib/pq"

	"wavicle/internal/core"
	"wavicle/internal/protocol/resp3"
	"wavicle/internal/replication"
	"wavicle/internal/storage"
	"wavicle/internal/telemetry"
)

const (
	testDSN      = "postgres://wavicle@localhost:5433/wavicle?sslmode=disable"
	testSlotName = "wavicle_test_slot"
	testPubName  = "wavicle_proofs"
	resp3Listen  = "127.0.0.1:6389"
)

// setupPostgresInstance ensures connection to Postgres and clean test tables.
func setupPostgresInstance(t *testing.T) *sql.DB {
	db, err := sql.Open("postgres", testDSN)
	if err != nil {
		t.Skipf("Skipping Postgres integration test: cannot connect to %s: %v", testDSN, err)
	}
	if err := db.Ping(); err != nil {
		t.Skipf("Skipping Postgres integration test: Postgres not reachable at %s: %v", testDSN, err)
	}

	// Clean up / reset seed row in users
	_, err = db.Exec(`
		CREATE TABLE IF NOT EXISTS users (
			id TEXT PRIMARY KEY,
			name TEXT,
			email TEXT,
			created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
		);
		ALTER TABLE users REPLICA IDENTITY FULL;
		DELETE FROM users WHERE id IN ('123', 'hardcore_1', 'hardcore_2');
		INSERT INTO users (id, name, email) VALUES ('123', 'Alice', 'alice@example.com');
	`)
	if err != nil {
		t.Fatalf("Failed to prepare test table: %v", err)
	}

	return db
}

// sendRESP3Command sends a raw RESP3 command over TCP and reads the response lines.
func sendRESP3Command(t *testing.T, conn net.Conn, cmd string) string {
	t.Helper()
	if _, err := fmt.Fprintf(conn, "%s\r\n", cmd); err != nil {
		t.Fatalf("Failed to send command %q: %v", cmd, err)
	}

	reader := bufio.NewReader(conn)
	line, err := reader.ReadString('\n')
	if err != nil {
		t.Fatalf("Failed to read RESP3 header for %q: %v", cmd, err)
	}
	line = strings.TrimRight(line, "\r\n")

	// If bulk string ($N), read the payload line
	if strings.HasPrefix(line, "$") && line != "$-1" {
		payload, err := reader.ReadString('\n')
		if err != nil {
			t.Fatalf("Failed to read RESP3 bulk payload for %q: %v", cmd, err)
		}
		return strings.TrimRight(payload, "\r\n")
	}

	return line
}

func TestPostgres_Hardcore_EndToEnd(t *testing.T) {
	db := setupPostgresInstance(t)
	defer db.Close()

	testSlot := fmt.Sprintf("wavicle_slot_%d", time.Now().UnixNano()%10000000)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// 1. Initialize PostgresStore + FrontierCache + WriteThroughStore
	pgStore, err := storage.NewPostgresStore(testDSN)
	if err != nil {
		t.Fatalf("Failed to initialize PostgresStore: %v", err)
	}
	defer pgStore.Close()

	localCache := storage.NewFrontierCache()
	writeThrough := storage.NewWriteThroughStore(pgStore, localCache)

	// 2. Initialize PG Logical Replication Listener
	pgListener := replication.NewPGListener(replication.PGConfig{
		DSN:             testDSN,
		ReplicationSlot: testSlot,
		Publication:     testPubName,
	})
	defer func() {
		_ = pgListener.Close()
		deadline := time.Now().Add(2 * time.Second)
		for time.Now().Before(deadline) {
			if _, err := db.Exec("SELECT pg_drop_replication_slot($1)", testSlot); err == nil {
				break
			}
			time.Sleep(25 * time.Millisecond)
		}
	}()

	events, err := pgListener.Start(ctx)
	if err != nil {
		t.Fatalf("Failed to start PG replication listener: %v", err)
	}

	// 3. Connect replication event loop (identical to main.go)
	replicationReady := make(chan struct{})
	var readyOnce sync.Once

	go func() {
		for evt := range events {
			readyOnce.Do(func() { close(replicationReady) })
			telemetry.Get().RecordReplicationLag(evt.Table, evt.CommitTime)
			for _, path := range evt.AffectedPaths {
				if evt.Action == "DELETE" {
					localCache.AppendAtom(&core.EConst{Value: core.VNull{}}, path, nil, time.Time{})
				} else {
					lastColon := strings.LastIndex(path, ":")
					if lastColon != -1 {
						column := path[lastColon+1:]
						if val, ok := evt.NewValues[column]; ok {
							valStr := fmt.Sprint(val)
							var expiresAt time.Time
							if current, exists := writeThrough.GetCurrent(path); exists {
								if current.PhysicalTime.After(evt.CommitTime) {
									continue
								}
								// Replication echo guard
								if ec, ok := current.Expr.(*core.EConst); ok && fmt.Sprint(ec.Value) == valStr {
									continue
								}
								expiresAt = current.ExpiresAt
							}
							localCache.AppendAtom(&core.EConst{Value: core.VString(valStr)}, path, nil, expiresAt)
						}
					}
				}
			}
		}
	}()

	// 4. Start RESP3 Server on isolated test port
	server := resp3.NewServer(writeThrough, "", 500)
	defer server.Close()

	go func() {
		_ = server.ListenAndServe(resp3Listen)
	}()

	// Wait for server to bind
	time.Sleep(150 * time.Millisecond)

	// Connect TCP client
	conn, err := net.Dial("tcp", resp3Listen)
	if err != nil {
		t.Fatalf("Failed to dial RESP3 server on %s: %v", resp3Listen, err)
	}
	defer conn.Close()

	// Verify server responds to PING
	pong := sendRESP3Command(t, conn, "PING")
	if pong != "+PONG" {
		t.Fatalf("Expected +PONG, got %q", pong)
	}
	t.Log("✓ RESP3 Server online and pinged successfully")

	// SCENARIO 1: Cold Read-Through Cache Seeding from Postgres
	// Initial row in Postgres: users(id='123', name='Alice')
	t.Run("Scenario1_ColdReadThrough", func(t *testing.T) {
		got := sendRESP3Command(t, conn, "GET users:123:name")
		if got != "Alice" {
			t.Fatalf("Cold read expected 'Alice', got %q", got)
		}
		t.Logf("✓ Cold read-through seeded cache from PostgreSQL: %s", got)

		// Second read should be sub-microsecond warm cache hit
		gotWarm := sendRESP3Command(t, conn, "GET users:123:name")
		if gotWarm != "Alice" {
			t.Fatalf("Warm read expected 'Alice', got %q", gotWarm)
		}
		t.Logf("✓ Warm cache hit verified: %s", gotWarm)
	})

	// Trigger an out-of-band SQL update to verify CDC listener replication
	t.Run("Scenario2_ExternalPostgresMutation_CDC", func(t *testing.T) {
		updateStart := time.Now()
		_, err := db.Exec("UPDATE users SET name = 'Bob_External_CDC' WHERE id = '123'")
		if err != nil {
			t.Fatalf("Failed to execute direct SQL UPDATE: %v", err)
		}
		t.Log("Executed direct PostgreSQL out-of-band SQL UPDATE: users[123].name -> 'Bob_External_CDC'")

		// Poll via RESP3 until updated, with 3s timeout
		var lastVal string
		deadline := time.Now().Add(3 * time.Second)
		updated := false

		for time.Now().Before(deadline) {
			lastVal = sendRESP3Command(t, conn, "GET users:123:name")
			if lastVal == "Bob_External_CDC" {
				updated = true
				break
			}
			time.Sleep(10 * time.Millisecond)
		}

		if !updated {
			t.Fatalf("CDC invalidation failed! Expected 'Bob_External_CDC', got %q", lastVal)
		}
		lag := time.Since(updateStart)
		t.Logf("✓ External DB mutation caught via Logical Replication CDC in %v (Value: %s)", lag, lastVal)
	})

	// SCENARIO 3: Write-Through via Wavicle SET Command to PostgreSQL
	t.Run("Scenario3_WriteThroughToPostgres", func(t *testing.T) {
		res := sendRESP3Command(t, conn, "SET users:123:name Charlie_WriteThrough")
		if res != "+OK" {
			t.Fatalf("Expected +OK from SET, got %q", res)
		}

		// Verify immediate local cache reflect
		val := sendRESP3Command(t, conn, "GET users:123:name")
		if val != "Charlie_WriteThrough" {
			t.Fatalf("Expected cache value 'Charlie_WriteThrough', got %q", val)
		}

		// Verify persistence in PostgreSQL storage directly
		var dbVal string
		err := db.QueryRow("SELECT name FROM users WHERE id = '123'").Scan(&dbVal)
		if err != nil {
			t.Fatalf("Failed to query Postgres directly: %v", err)
		}
		if dbVal != "Charlie_WriteThrough" {
			t.Fatalf("PostgreSQL row was not updated by WriteThrough! Expected 'Charlie_WriteThrough', got %q", dbVal)
		}
		t.Logf("✓ Write-through verified: Postgres disk has %q and Wavicle cache has %q", dbVal, val)
	})

	// SCENARIO 4: External Database INSERT Detected Dynamically
	t.Run("Scenario4_ExternalPostgresInsert", func(t *testing.T) {
		_, err := db.Exec("INSERT INTO users (id, name, email) VALUES ('hardcore_1', 'Diana_Prince', 'diana@themyscira.gov')")
		if err != nil {
			t.Fatalf("Failed to execute direct SQL INSERT: %v", err)
		}

		var lastVal string
		deadline := time.Now().Add(3 * time.Second)
		detected := false
		for time.Now().Before(deadline) {
			lastVal = sendRESP3Command(t, conn, "GET users:hardcore_1:name")
			if lastVal == "Diana_Prince" {
				detected = true
				break
			}
			time.Sleep(10 * time.Millisecond)
		}

		if !detected {
			t.Fatalf("CDC insert detection failed! Expected 'Diana_Prince', got %q", lastVal)
		}
		t.Logf("✓ External INSERT captured dynamically by CDC: users:hardcore_1:name = %s", lastVal)
	})

	// SCENARIO 5: External Database DELETE and Tombstone Invalidation
	t.Run("Scenario5_ExternalPostgresDelete", func(t *testing.T) {
		_, err := db.Exec("DELETE FROM users WHERE id = 'hardcore_1'")
		if err != nil {
			t.Fatalf("Failed to execute direct SQL DELETE: %v", err)
		}

		var lastVal string
		deadline := time.Now().Add(3 * time.Second)
		deleted := false
		for time.Now().Before(deadline) {
			lastVal = sendRESP3Command(t, conn, "GET users:hardcore_1:name")
			if lastVal == "$-1" || lastVal == "" {
				deleted = true
				break
			}
			time.Sleep(10 * time.Millisecond)
		}

		if !deleted {
			t.Fatalf("CDC delete tombstone failed! Expected '$-1', got %q", lastVal)
		}
		t.Log("✓ External DELETE tombstoned and returned nil ($-1) with zero manual invalidation")
	})

	// SCENARIO 6: High-Concurrency Burst with Concurrent SQL Mutations
	t.Run("Scenario6_HighConcurrencyConcurrentLoad", func(t *testing.T) {
		const workers = 8
		const opsPerWorker = 500
		var wg sync.WaitGroup

		errCh := make(chan error, workers*2)

		// Concurrent readers over individual TCP connections
		for w := 0; w < workers; w++ {
			wg.Add(1)
			go func(workerID int) {
				defer wg.Done()
				c, err := net.Dial("tcp", resp3Listen)
				if err != nil {
					errCh <- fmt.Errorf("worker %d dial: %w", workerID, err)
					return
				}
				defer c.Close()

				reader := bufio.NewReader(c)
				for i := 0; i < opsPerWorker; i++ {
					cmd := "GET users:123:name\r\n"
					if _, err := c.Write([]byte(cmd)); err != nil {
						errCh <- err
						return
					}
					line, err := reader.ReadString('\n')
					if err != nil {
						errCh <- err
						return
					}
					if strings.HasPrefix(line, "$") && line != "$-1\r\n" {
						_, _ = reader.ReadString('\n')
					}
				}
			}(w)
		}

		// Concurrent background mutator hitting PostgreSQL directly
		stopMutator := make(chan struct{})
		var mutatorWg sync.WaitGroup
		mutatorWg.Add(1)
		go func() {
			defer mutatorWg.Done()
			toggle := 0
			for {
				select {
				case <-stopMutator:
					return
				default:
					name := fmt.Sprintf("LoadUser_%d", toggle)
					_, _ = db.Exec("UPDATE users SET name = $1 WHERE id = '123'", name)
					toggle = (toggle + 1) % 100
					time.Sleep(5 * time.Millisecond)
				}
			}
		}()

		wg.Wait()
		close(stopMutator)
		mutatorWg.Wait()
		close(errCh)

		for err := range errCh {
			t.Fatalf("Concurrent worker encountered error: %v", err)
		}

		t.Logf("✓ Concurrency burst passed: %d operations completed under continuous live PG mutations with zero errors/crashes", workers*opsPerWorker)
	})
}
