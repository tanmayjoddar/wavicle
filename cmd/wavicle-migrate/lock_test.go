package main

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestPIDLock_AcquireRefuseTakeover(t *testing.T) {
	dir := t.TempDir()
	lock := filepath.Join(dir, "run.lock")

	rel1, _, err := acquireLock(lock)
	if err != nil {
		t.Fatalf("first acquire: %v", err)
	}
	defer rel1()

	// Same live owner, fresh heartbeat → hard refusal.
	if _, _, err := acquireLock(lock); err == nil {
		t.Fatal("second acquire must refuse a live owner")
	}

	// Dead PID + stale heartbeat → loud takeover.
	rel1()
	stale := pidLock{PID: 999999999, StartedAt: "2020-01-01T00:00:00Z", Heartbeat: "2020-01-01T00:00:00Z"}
	if err := writeLock(lock, stale); err != nil {
		t.Fatal(err)
	}
	rel2, beat, err := acquireLock(lock)
	if err != nil {
		t.Fatalf("stale lock must be taken over: %v", err)
	}
	defer rel2()
	beat()
	b, _ := os.ReadFile(lock)
	t.Logf("lock now: %s", string(b))

	// Release removes the file; absence never blocks.
	rel2()
	if _, err := os.Stat(lock); !os.IsNotExist(err) {
		t.Fatal("release must remove the lock file")
	}
	if _, _, err := acquireLock(lock); err != nil {
		t.Fatalf("absent lock must acquire: %v", err)
	}

	_ = time.Second // keep time import if asserts evolve
}
