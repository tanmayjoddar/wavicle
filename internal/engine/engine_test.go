package engine

import (
	"fmt"
	"testing"
	"wavicle/internal/core"
	"wavicle/internal/storage"
)

func TestIncrementalCorrectness_PoisonWrite(t *testing.T) {
	// This test proves that incremental reduction NEVER returns stale values.
	// Scenario:
	//   1. Compose a 50-field proof (simulates caching)
	//   2. Change 1 field (poison write)
	//   3. Incremental read — must return NEW value for changed field

	crystal, err := storage.NewCausalCrystal("test_poison.log")
	if err != nil {
		t.Fatal(err)
	}

	var atoms []core.Hash
	entries := make(map[string]core.Hash)
	for i := 0; i < 50; i++ {
		path := fmt.Sprintf("user:123:field_%d", i)
		h, err := crystal.AppendAtom(
			&core.EConst{Value: core.VString(fmt.Sprintf("value_%d", i))},
			path, nil,
		)
		if err != nil {
			t.Fatal(err)
		}
		atoms = append(atoms, h)
		entries[path] = h
	}

	expr := &core.ECompose{Atoms: atoms}

	// Step 1: Cold compose — populate node cache via ReduceProofTree
	warmValue, warmCache, err := ReduceProofTree(expr, crystal)
	if err != nil {
		t.Fatal(err)
	}

	proof := &MaterializedProof{
		ProofTree:     expr,
		VersionVector: &VersionVector{Entries: entries},
		NodeCache:     warmCache,
		Value:         warmValue,
		PathToExpr:    make(map[string]core.Hash),
	}
	for i, hash := range atoms {
		proof.PathToExpr[fmt.Sprintf("user:123:field_%d", i)] = hash
	}

	// Step 2: Poison write — change field_5 from "value_5" to "POISONED"
	poisonPath := "user:123:field_5"
	_, err = crystal.AppendAtom(
		&core.EConst{Value: core.VString("POISONED")},
		poisonPath, nil,
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

	for i := 0; i < 50; i++ {
		if i == 5 {
			continue
		}
		key := fmt.Sprintf("user:123:field_%d", i)
		expected := core.VString(fmt.Sprintf("value_%d", i))
		if v, ok := rec[key]; !ok {
			t.Fatalf("%s missing from result", key)
		} else if v != expected {
			t.Fatalf("%s = %v, expected %v", key, v, expected)
		}
	}

	// Step 4: Verify stats show 49/1 cache split
	if proof.ReduceStats.CacheHits != 49 {
		t.Fatalf("expected 49 cache hits, got %d (cache_misses=%d, changed=%d)",
			proof.ReduceStats.CacheHits, proof.ReduceStats.CacheMisses, proof.ReduceStats.ChangedPaths)
	}
}

func TestReadAfterWrite_ReturnsLatestValue(t *testing.T) {
	crystal, err := storage.NewCausalCrystal("test_rw.log")
	if err != nil {
		t.Fatal(err)
	}

	_, err = crystal.AppendAtom(
		&core.EConst{Value: core.VString("Alice")},
		"user:123:name", nil,
	)
	if err != nil {
		t.Fatal(err)
	}

	// Compose proof — this simulates real GET path
	proof, err := ComposeProof(crystal, "user:123:name", core.ModeDeductive)
	if err != nil {
		t.Fatal(err)
	}

	verifyVal := func(v core.Value) string {
		rec, ok := v.(core.VRecord)
		if !ok {
			return fmt.Sprintf("not record: %T(%v)", v, v)
		}
		if s, ok := rec["user:123:name"]; ok {
			if vs, ok := s.(core.VString); ok {
				return string(vs)
			}
			return fmt.Sprintf("not string: %T(%v)", s, s)
		}
		return "user:123:name key missing"
	}

	if got := verifyVal(proof.Value); got != "Alice" {
		t.Fatalf("expected Alice, got %s", got)
	}

	// Poison write: overwrite with "Bob"
	_, err = crystal.AppendAtom(
		&core.EConst{Value: core.VString("Bob")},
		"user:123:name", nil,
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
	crystal, err := storage.NewCausalCrystal("test_cons_write.log")
	if err != nil {
		t.Fatal(err)
	}

	values := []string{"v0", "v1", "v2", "v3", "v4", "v5"}

	for i, val := range values {
		h, err := crystal.AppendAtom(
			&core.EConst{Value: core.VString(val)},
			"key:write_test", nil,
		)
		if err != nil {
			t.Fatal(err)
		}

		entries := map[string]core.Hash{"key:write_test": h}
		expr := &core.ECompose{Atoms: []core.Hash{h}}

		proof, err := ComposeProof(crystal, "key:write_test", core.ModeDeductive)
		if err != nil {
			t.Fatal(err)
		}
		proof.ProofTree = expr
		proof.VersionVector = &VersionVector{Entries: entries}

		result, err := ReduceIncremental(proof, crystal)
		if err != nil {
			t.Fatal(err)
		}
		rec, ok := result.(core.VRecord)
		if !ok {
			t.Fatalf("write %d: expected VRecord, got %T", i, result)
		}
		got := string(rec["key:write_test"].(core.VString))
		if got != val {
			t.Fatalf("write %d: expected %s, got %s", i, val, got)
		}
	}
}


