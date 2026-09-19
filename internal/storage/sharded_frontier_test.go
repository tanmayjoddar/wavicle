package storage

import (
	"fmt"
	"path/filepath"
	"sync"
	"testing"
	"time"
	"wavicle/internal/core"
)

func TestShardedFrontier_BasicRW(t *testing.T) {
	fc := NewShardedFrontierCacheWithLimit(64 << 20)
	defer fc.Close()
	if _, err := fc.AppendAtom(core.NewEConst(core.VString("Alice")), "users:1:name", nil, time.Time{}); err != nil {
		t.Fatal(err)
	}
	a, ok := fc.GetCurrent("users:1:name")
	if !ok {
		t.Fatal("missing")
	}
	if fmt.Sprint(a.Expr.(*core.EConst).Value) != "Alice" {
		t.Fatalf("got %v", a.Expr)
	}
	if !fc.VerifyVersionVector(map[string]core.Hash{"users:1:name": a.Hash}) {
		t.Fatal("VV should match")
	}
}

func TestShardedFrontier_PoisonWrite(t *testing.T) {
	fc := NewShardedFrontierCacheWithLimit(64 << 20)
	defer fc.Close()
	for i := 0; i < 50; i++ {
		p := fmt.Sprintf("u:1:f%d", i)
		if _, err := fc.AppendAtom(core.NewEConst(core.VString(fmt.Sprintf("v%d", i))), p, nil, time.Time{}); err != nil {
			t.Fatal(err)
		}
	}
	vv := map[string]core.Hash{}
	for _, p := range fc.FrontierPaths() {
		h, _ := fc.GetCurrentHash(p)
		vv[p] = h
	}
	if !fc.VerifyVersionVector(vv) {
		t.Fatal("warm VV must match")
	}
	if _, err := fc.AppendAtom(core.NewEConst(core.VString("POISON")), "u:1:f5", nil, time.Time{}); err != nil {
		t.Fatal(err)
	}
	if fc.VerifyVersionVector(vv) {
		t.Fatal("stale VV must NOT match after poison")
	}
	changed := fc.FindChangedPaths(vv, make([]string, 0, 8))
	if len(changed) != 1 || changed[0] != "u:1:f5" {
		t.Fatalf("changed=%v", changed)
	}
	a, _ := fc.GetCurrent("u:1:f5")
	if fmt.Sprint(a.Expr.(*core.EConst).Value) != "POISON" {
		t.Fatalf("stale read: %v", a.Expr)
	}
}

func TestShardedFrontier_SnapshotRoundtrip(t *testing.T) {
	fc := NewShardedFrontierCacheWithLimit(64 << 20)
	defer fc.Close()
	_, _ = fc.AppendAtom(core.NewEConst(core.VString("a")), "k1", nil, time.Time{})
	_, _ = fc.AppendAtom(core.NewEConst(core.VInt(42)), "k2", nil, time.Time{})
	snap := filepath.Join(t.TempDir(), "frontier.snapshot")
	if err := fc.SaveSnapshot(snap); err != nil {
		t.Fatal(err)
	}
	fc2 := NewShardedFrontierCacheWithLimit(64 << 20)
	defer fc2.Close()
	n, err := fc2.LoadSnapshot(snap)
	if err != nil || n != 2 {
		t.Fatalf("load n=%d err=%v", n, err)
	}
	if a, ok := fc2.GetCurrent("k1"); !ok || fmt.Sprint(a.Expr.(*core.EConst).Value) != "a" {
		t.Fatalf("k1=%v ok=%v", a, ok)
	}
}

func TestShardedFrontier_SameValueDedup(t *testing.T) {
	fc := NewShardedFrontierCacheWithLimit(64 << 20)
	defer fc.Close()
	h1, _ := fc.AppendAtom(core.NewEConst(core.VString("same")), "k", nil, time.Time{})
	h2, _ := fc.AppendAtom(core.NewEConst(core.VString("same")), "k", nil, time.Time{})
	if h1 != h2 {
		t.Fatalf("identical SET must not mint a new hash (proofs would invalidate): %v vs %v", h1, h2)
	}
	h3, _ := fc.AppendAtom(core.NewEConst(core.VString("different")), "k", nil, time.Time{})
	if h3 == h1 {
		t.Fatal("changed value must mint a new hash")
	}
	// Same value with a new deadline is a REAL change (EXPIRE path).
	h4, _ := fc.AppendAtom(core.NewEConst(core.VString("different")), "k", nil, time.Now().Add(time.Minute))
	if h4 == h3 {
		t.Fatal("same value with new ExpiresAt must mint a new hash")
	}
}

func TestShardedFrontier_ConcurrentSameKey(t *testing.T) {
	fc := NewShardedFrontierCacheWithLimit(64 << 20)
	defer fc.Close()
	var wg sync.WaitGroup
	for w := 0; w < 16; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < 200; i++ {
				// Mixed identical + distinct writes on ONE key: exercises the
				// clock-order guard and dedup under contention.
				_, _ = fc.AppendAtom(core.NewEConst(core.VString("pinned")), "hot", nil, time.Time{})
				_, _ = fc.AppendAtom(core.NewEConst(core.VString(fmt.Sprint(w, "-", i))), "hot", nil, time.Time{})
				_, _ = fc.GetCurrent("hot")
			}
		}(w)
	}
	wg.Wait()
	if _, ok := fc.GetCurrent("hot"); !ok {
		t.Fatal("hot key lost under contention")
	}
}

func TestShardedFrontier_Concurrent(t *testing.T) {
	fc := NewShardedFrontierCacheWithLimit(64 << 20)
	defer fc.Close()
	var wg sync.WaitGroup
	for w := 0; w < 16; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < 500; i++ {
				p := fmt.Sprintf("k:%d:%d", w, i%50)
				_, _ = fc.AppendAtom(core.NewEConst(core.VString(fmt.Sprint(i))), p, nil, time.Time{})
				_, _ = fc.GetCurrent(p)
			}
		}(w)
	}
	wg.Wait()
	if len(fc.FrontierPaths()) == 0 {
		t.Fatal("no keys")
	}
}

func TestShardedFrontier_SnapshotComplexTypes(t *testing.T) {
	fc := NewShardedFrontierCacheWithLimit(64 << 20)
	defer fc.Close()
	_, _ = fc.AppendAtom(core.NewEConst(core.VArray{core.VString("a"), core.VString("b")}), "lst", nil, time.Time{})
	set := core.VRecord{"x": core.VInt(1), "y": core.VInt(1)}
	_, _ = fc.AppendAtom(core.NewEConst(set), "st", nil, time.Time{})
	z := core.VRecord{"a": core.VFloat(1), "b": core.VFloat(2)}
	_, _ = fc.AppendAtom(core.NewEConst(z), "zz", nil, time.Time{})
	snap := filepath.Join(t.TempDir(), "c.snapshot")
	if err := fc.SaveSnapshot(snap); err != nil {
		t.Fatal(err)
	}
	fc2 := NewShardedFrontierCacheWithLimit(64 << 20)
	defer fc2.Close()
	n, err := fc2.LoadSnapshot(snap)
	if err != nil || n != 3 {
		t.Fatalf("load n=%d err=%v (want 3, complex types must survive)", n, err)
	}
	if a, ok := fc2.GetCurrent("lst"); !ok || len(a.Expr.(*core.EConst).Value.(core.VArray)) != 2 {
		t.Fatalf("list lost: %v", a)
	}
	if a, ok := fc2.GetCurrent("zz"); !ok || len(a.Expr.(*core.EConst).Value.(core.VRecord)) != 2 {
		t.Fatalf("zset lost: %v", a)
	}
}
