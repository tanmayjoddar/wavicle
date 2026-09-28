// Chaos drills that run without cloud, PG, or root: the automatable subset.
// Manual procedures (kill -9, PG restart, partition, disk-full) live in
// docc/CHAOS_AND_PG_REALITY.md with exact commands and expected outcomes.
package chaos_test

import (
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"
	"wavicle/internal/core"
	"wavicle/internal/protocol/resp3"
	"wavicle/internal/storage"
)

// Drill 1 — CDC stall: slot-proven blind, reads fail closed; slot healthy
// again, serving recovers. (Slot health is the oracle precisely so that a
// healthy-but-quiet stream keeps serving — see TestStaleGuard_PGSlotHealth.)
func TestChaos_CDCStallFailsClosedThenRecovers(t *testing.T) {
	crystal, err := storage.NewCausalCrystal(filepath.Join(t.TempDir(), "chaos.log"))
	if err != nil {
		t.Fatal(err)
	}
	defer crystal.Close()
	srv := resp3.NewServer(crystal, "", 100)
	defer srv.Close()

	slot := &atomic.Int64{}
	srv.SetStaleGuardPG(slot, 10<<30)

	if _, err := srv.HandleCommand([]string{"SET", "k", "v"}); err != nil {
		t.Fatal(err)
	}
	// Healthy (lag 0), no events ever — idle stream serves.
	slot.Store(0)
	if _, err := srv.HandleCommand([]string{"GET", "k"}); err != nil {
		t.Fatalf("healthy idle must serve: %v", err)
	}
	// Stall: slot goes blind.
	slot.Store(-2)
	if _, err := srv.HandleCommand([]string{"GET", "k"}); err == nil || !strings.Contains(err.Error(), "STALE") {
		t.Fatalf("blind CDC must fail closed, got err=%v", err)
	}
	// Resume: slot healthy again.
	slot.Store(0)
	if _, err := srv.HandleCommand([]string{"GET", "k"}); err != nil {
		t.Fatalf("resumed stream must serve: %v", err)
	}
}

// Drill 2 — snapshot I/O failure must surface as an error, never a crash,
// and must never leave a half-written snapshot behind.
func TestChaos_SnapshotFailureIsClean(t *testing.T) {
	fc := storage.NewShardedFrontierCacheWithLimit(64 << 20)
	defer fc.Close()
	_, _ = fc.AppendAtom(core.NewEConst(core.VString("v")), "k", nil, time.Time{})

	dir := t.TempDir()
	if err := fc.SaveSnapshot(dir); err == nil {
		t.Fatal("snapshot to a directory must error")
	}
	if _, err := fc.LoadSnapshot(filepath.Join(dir, "missing.snapshot")); err == nil {
		t.Fatal("load of missing snapshot must error")
	}
	good := filepath.Join(dir, "good.snapshot")
	if err := fc.SaveSnapshot(good); err != nil {
		t.Fatalf("good save must work: %v", err)
	}
	// Failed save must not clobber the last good snapshot.
	if err := fc.SaveSnapshot(dir); err == nil {
		t.Fatal("expected error")
	}
	fc2 := storage.NewShardedFrontierCacheWithLimit(64 << 20)
	defer fc2.Close()
	n, err := fc2.LoadSnapshot(good)
	if err != nil || n != 1 {
		t.Fatalf("good snapshot must survive failed saves: n=%d err=%v", n, err)
	}
}

// Drill 3 — unwritable snapshot destination (missing parent dir stands in for
// disk-full / read-only mounts; both surface as os.Create errors, portable
// across OSes). Save must error; the live cache must keep serving.
func TestChaos_UnwritableSnapshotDir(t *testing.T) {
	fc := storage.NewShardedFrontierCacheWithLimit(64 << 20)
	defer fc.Close()
	_, _ = fc.AppendAtom(core.NewEConst(core.VString("v")), "k", nil, time.Time{})

	bad := filepath.Join(t.TempDir(), "no-such-dir", "s.snapshot")
	if err := fc.SaveSnapshot(bad); err == nil {
		t.Fatal("unwritable destination must fail the save")
	}
	if _, ok := fc.GetCurrent("k"); !ok {
		t.Fatal("cache must keep serving after snapshot failure")
	}
}
