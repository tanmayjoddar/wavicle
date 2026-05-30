package engine

import (
	"fmt"
	"path/filepath"
	"testing"
	"time"
	"wavicle/internal/core"
	"wavicle/internal/storage"
)

func TestIncrementalCorrectness_PoisonWrite(t *testing.T) {
	// This test proves that incremental reduction NEVER returns stale values.
	crystal, err := storage.NewCausalCrystal(filepath.Join(t.TempDir(), "test_poison.log"))
	if err != nil {
		t.Fatal(err)
	}
	defer crystal.Close()

	var atoms []core.Hash
	for i := 0; i < 50; i++ {
		path := fmt.Sprintf("user:123:field_%d", i)
		h, err := crystal.AppendAtom(
			core.NewEConst(core.VString(fmt.Sprintf("value_%d", i))),
			path, nil, time.Time{},
		)
		if err != nil {
			t.Fatal(err)
		}
		atoms = append(atoms, h)
	}

	// Create a compose atom so we can use ComposeProof
	path := "user:123"
	crystal.AppendAtom(core.NewECompose(atoms), path, atoms, time.Time{})

	// Step 1: Cold compose — this builds the ProofNode tree and caches values
	proof, err := ComposeProof(crystal, path, core.ModeDeductive)
	if err != nil {
		t.Fatal(err)
	}

	// Step 2: Poison write — change field_5 from "value_5" to "POISONED"
	poisonPath := "user:123:field_5"
	_, err = crystal.AppendAtom(
		core.NewEConst(core.VString("POISONED")),
		poisonPath, nil, time.Time{},
	)
	if err != nil {
		t.Fatal(err)
	}

	// Step 3: Incremental read — must return "POISONED" not "value_5"
	result, err := ReduceIncremental(proof, crystal)
	if err != nil {
		t.Fatal(err)
	}

	// Verify the result contains the poisoned value
	rec, ok := result.(core.VRecord)
	if !ok {
		t.Fatalf("expected VRecord, got %T", result)
	}

	checkPath := "user:123:field_5"
	if v, ok := rec[checkPath]; !ok {
		t.Fatalf("%s missing from result", checkPath)
	} else if v != core.VString("POISONED") {
		t.Fatalf("STALE READ: %s = %v, expected POISONED", checkPath, v)
	}

	// Step 4: Verify stats show 49/2 cache split (2 misses because we update the leaf node's Expr)
	if proof.ReduceStats.CacheHits != 49 {
		t.Fatalf("expected 49 cache hits, got %d (cache_misses=%d, changed=%d)",
			proof.ReduceStats.CacheHits, proof.ReduceStats.CacheMisses, proof.ReduceStats.ChangedPaths)
	}
}

func TestReadAfterWrite_ReturnsLatestValue(t *testing.T) {
	crystal, err := storage.NewCausalCrystal(filepath.Join(t.TempDir(), "test_rw.log"))
	if err != nil {
		t.Fatal(err)
	}
	defer crystal.Close()

	path := "user:123:name"
	_, err = crystal.AppendAtom(
		core.NewEConst(core.VString("Alice")),
		path, nil, time.Time{},
	)
	if err != nil {
		t.Fatal(err)
	}

	// Compose proof — this simulates real GET path
	proof, err := ComposeProof(crystal, path, core.ModeDeductive)
	if err != nil {
		t.Fatal(err)
	}

	verifyVal := func(v core.Value) string {
		vs, ok := v.(core.VString)
		if !ok {
			return fmt.Sprintf("not string: %T(%v)", v, v)
		}
		return string(vs)
	}

	if got := verifyVal(proof.Value); got != "Alice" {
		t.Fatalf("expected Alice, got %s", got)
	}

	// Poison write: overwrite with "Bob"
	_, err = crystal.AppendAtom(
		core.NewEConst(core.VString("Bob")),
		path, nil, time.Time{},
	)
	if err != nil {
		t.Fatal(err)
	}

	// Incremental read — must return "Bob", NOT "Alice"
	val, err := ReduceIncremental(proof, crystal)
	if err != nil {
		t.Fatal(err)
	}
	if got := verifyVal(val); got != "Bob" {
		t.Fatalf("STALE READ: expected Bob, got %s", got)
	}
}

func TestConsecutiveWrites_NoStaleReads(t *testing.T) {
	crystal, err := storage.NewCausalCrystal(filepath.Join(t.TempDir(), "test_cons_write.log"))
	if err != nil {
		t.Fatal(err)
	}
	defer crystal.Close()

	values := []string{"v0", "v1", "v2", "v3", "v4", "v5"}
	path := "key:write_test"

	for i, val := range values {
		_, err := crystal.AppendAtom(
			core.NewEConst(core.VString(val)),
			path, nil, time.Time{},
		)
		if err != nil {
			t.Fatal(err)
		}

		proof, err := ComposeProof(crystal, path, core.ModeDeductive)
		if err != nil {
			t.Fatal(err)
		}

		result, err := ReduceIncremental(proof, crystal)
		if err != nil {
			t.Fatal(err)
		}
		got := string(result.(core.VString))
		if got != val {
			t.Fatalf("write %d: expected %s, got %s", i, val, got)
		}
	}
}


