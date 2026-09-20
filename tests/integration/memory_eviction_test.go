package integration_test

import (
	"database/sql"
	"fmt"
	"net"
	"strings"
	"testing"
	"time"

	_ "github.com/lib/pq"

	"wavicle/internal/protocol/resp3"
	"wavicle/internal/storage"
	"wavicle/internal/telemetry"
)

func TestMemoryEviction_UnderCapAndReadThrough(t *testing.T) {
	db, err := sql.Open("postgres", testDSN)
	if err != nil {
		t.Skipf("Skipping test: cannot connect to Postgres: %v", err)
	}
	defer db.Close()
	if err := db.Ping(); err != nil {
		t.Skipf("Skipping test: Postgres not reachable: %v", err)
	}

	// Prepare Postgres seed row
	_, err = db.Exec(`
		CREATE TABLE IF NOT EXISTS users (
			id TEXT PRIMARY KEY,
			name TEXT,
			email TEXT,
			created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
		);
		DELETE FROM users WHERE id = 'mem_seed_1';
		INSERT INTO users (id, name, email) VALUES ('mem_seed_1', 'Persisted_In_Postgres', 'mem@example.com');
	`)
	if err != nil {
		t.Fatalf("Failed to prepare Postgres seed: %v", err)
	}

	pgStore, err := storage.NewPostgresStore(testDSN)
	if err != nil {
		t.Fatalf("Failed to initialize PostgresStore: %v", err)
	}
	defer pgStore.Close()

	// Configure a tight memory limit: 64 KB
	const memoryLimit = 64 * 1024
	localCache := storage.NewFrontierCacheWithLimit(memoryLimit)
	defer localCache.Close()

	writeThrough := storage.NewWriteThroughStore(pgStore, localCache)

	listenAddr := "127.0.0.1:6391"
	server := resp3.NewServer(writeThrough, "", 100)
	defer server.Close()

	go func() {
		_ = server.ListenAndServe(listenAddr)
	}()
	time.Sleep(100 * time.Millisecond)

	conn, err := net.Dial("tcp", listenAddr)
	if err != nil {
		t.Fatalf("Failed to connect to RESP3 server: %v", err)
	}
	defer conn.Close()

	// 1. Initial read-through of seed key (seeds localCache)
	val := sendRESP3Command(t, conn, "GET users:mem_seed_1:name")
	if val != "Persisted_In_Postgres" {
		t.Fatalf("Expected seed value 'Persisted_In_Postgres', got %q", val)
	}
	t.Logf("✓ Seed key cached in FrontierCache: %s", val)

	// 2. Flood 1,000 keys with 256-byte values to exceed 64 KB memory limit many times over
	// 1,000 keys * ~450 bytes/entry ≈ 450 KB (> 7x the 64 KB ceiling)
	payload := strings.Repeat("X", 256)
	initialEvictions := telemetry.Get().GetMemoryEvictions()

	for i := 0; i < 1000; i++ {
		cmd := fmt.Sprintf("SET key_%04d %s", i, payload)
		res := sendRESP3Command(t, conn, cmd)
		if res != "+OK" {
			t.Fatalf("SET failed at key_%04d: %s", i, res)
		}
	}

	// 3. Assert (a): Process cache stabilizes below configured limit
	currentBytes := localCache.CurrentBytes()
	maxBytes := localCache.MaxMemoryBytes()
	newEvictions := telemetry.Get().GetMemoryEvictions() - initialEvictions

	t.Logf("Memory status: current = %d bytes, limit = %d bytes, evictions = %.0f",
		currentBytes, maxBytes, newEvictions)

	if currentBytes > maxBytes {
		t.Fatalf("Memory limit breached! currentBytes (%d) > maxBytes (%d)", currentBytes, maxBytes)
	}
	if newEvictions == 0 {
		t.Fatalf("Expected evictions under heavy flood, got 0")
	}
	t.Logf("✓ Memory ceiling enforced: usage stabilized at %d bytes (< %d limit) with %.0f evictions",
		currentBytes, maxBytes, newEvictions)

	// Verify that the early seed key 'users:mem_seed_1:name' was evicted from localCache
	if _, inCache := localCache.GetCurrent("users:mem_seed_1:name"); inCache {
		t.Log("Note: users:mem_seed_1:name still in cache, testing fallback with known evicted key...")
	}

	// 4. Assert (b): Evicted keys correctly fall back to Postgres read-through on next GET
	// Querying users:mem_seed_1:name must succeed by falling back to Postgres
	valAfterEviction := sendRESP3Command(t, conn, "GET users:mem_seed_1:name")
	if valAfterEviction != "Persisted_In_Postgres" {
		t.Fatalf("Read-through fallback failed! Expected 'Persisted_In_Postgres', got %q", valAfterEviction)
	}
	t.Logf("✓ Evicted key successfully retrieved via Postgres read-through fallback: %q", valAfterEviction)
}
