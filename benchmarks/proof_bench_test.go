package benchmarks

import (
	"fmt"
	"path/filepath"
	"testing"
	"time"
	"wavicle/internal/core"
	"wavicle/internal/engine"
	"wavicle/internal/storage"
)

func setup50FieldCrystal(b *testing.B) (*storage.CausalCrystal, *core.ECompose, map[string]core.Hash, []core.Hash) {
	crystal, err := storage.NewCausalCrystal(filepath.Join(b.TempDir(), "bench_crystal.log"))
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { crystal.Close() })
	var atoms []core.Hash
	entries := make(map[string]core.Hash)
	for i := 0; i < 50; i++ {
		path := fmt.Sprintf("user:123:field_%d", i)
		h, err := crystal.AppendAtom(
			core.NewEConst(core.VString(fmt.Sprintf("value_%d", i))),
			path, nil, time.Time{},
		)
		if err != nil {
			b.Fatal(err)
		}
		atoms = append(atoms, h)
		entries[path] = h
	}
	expr := core.NewECompose(atoms)
	return crystal, expr, entries, atoms
}

func BenchmarkProofReduction_Incremental_Revolutionary(b *testing.B) {
	crystal, expr, _, atoms := setup50FieldCrystal(b)
	
	// Create a primary path pointing to the compose expr
	path := "compose_50"
	crystal.AppendAtom(expr, path, atoms, time.Time{})

	// Build the proof using the new architecture
	proof, err := engine.ComposeProof(crystal, path, core.ModeDeductive)
	if err != nil {
		b.Fatal(err)
	}

	// Stale the first field
	crystal.AppendAtom(
		core.NewEConst(core.VString("new_value")),
		"user:123:field_0", nil, time.Time{},
	)

	// Save the stale state so we can reset it every iteration
	staleSnapshot := make(map[string]core.Hash)
	for k, v := range proof.VersionVector.Entries {
		staleSnapshot[k] = v
	}
	staleKey := "user:123:field_0"
	staleHash := staleSnapshot[staleKey]

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		proof.VersionVector.Entries[staleKey] = staleHash
		_, _ = engine.ReduceIncremental(proof, crystal)
	}

	b.Logf("cache_hits=%d cache_misses=%d changed_paths=%d",
		proof.ReduceStats.CacheHits,
		proof.ReduceStats.CacheMisses,
		proof.ReduceStats.ChangedPaths,
	)
}

func BenchmarkProofReduction_WarmReuse_FastPath1(b *testing.B) {
	crystal, expr, _, atoms := setup50FieldCrystal(b)
	path := "compose_50"
	crystal.AppendAtom(expr, path, atoms, time.Time{})
	
	proof, _ := engine.ComposeProof(crystal, path, core.ModeDeductive)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = engine.ReduceIncremental(proof, crystal)
	}
}

func BenchmarkProofReduction_FastPath2_MerkleMatch(b *testing.B) {
	crystal, err := storage.NewCausalCrystal(filepath.Join(b.TempDir(), "bench_merkle.log"))
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { crystal.Close() })

	var atoms []core.Hash
	var paths []string
	for i := 0; i < 10; i++ {
		path := fmt.Sprintf("path_%d", i)
		h, _ := crystal.AppendAtom(core.NewEConst(core.VString("val")), path, nil, time.Time{})
		atoms = append(atoms, h)
		paths = append(paths, path)
	}

	primaryPath := "compose_10"
	crystal.AppendAtom(core.NewECompose(atoms), primaryPath, atoms, time.Time{})
	
	proof, _ := engine.ComposeProof(crystal, primaryPath, core.ModeDeductive)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = engine.ReduceIncremental(proof, crystal)
	}
}

func BenchmarkProofReduction_Cold(b *testing.B) {
	crystal, expr, _, atoms := setup50FieldCrystal(b)
	
	// Create a primary path pointing to the compose expr
	path := "compose_50"
	crystal.AppendAtom(expr, path, atoms, time.Time{})

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = engine.ComposeProof(crystal, path, core.ModeDeductive)
	}
}

func BenchmarkIncremental_Breakdown(b *testing.B) {
	crystal, expr, _, atoms := setup50FieldCrystal(b)
	path := "compose_50"
	crystal.AppendAtom(expr, path, atoms, time.Time{})
	proof, _ := engine.ComposeProof(crystal, path, core.ModeDeductive)

	b.Run("MarkDirty_Upward", func(b *testing.B) {
		crystal.AppendAtom(core.NewEConst(core.VString("new_value")), "user:123:field_0", nil, time.Time{})
		var changedBuf [64]string
		changed := crystal.FindChangedPaths(proof.VersionVector.Entries, changedBuf[:0])
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			proof.RootNode.ResetDirty()
			for _, p := range changed {
				if node, ok := proof.PathToNode[p]; ok {
					node.PropagateDirtyUp()
				}
			}
		}
	})

	b.Run("FindChangedPaths", func(b *testing.B) {
		crystal.AppendAtom(core.NewEConst(core.VString("new_value")), "user:123:field_0", nil, time.Time{})
		var changedBuf [64]string
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = crystal.FindChangedPaths(proof.VersionVector.Entries, changedBuf[:0])
		}
	})

	b.Run("ReduceDirty_Only", func(b *testing.B) {
		staleKey := "user:123:field_0"
		staleHash := proof.VersionVector.Entries[staleKey]
		crystal.AppendAtom(core.NewEConst(core.VString("new_value")), staleKey, nil, time.Time{})
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			proof.VersionVector.Entries[staleKey] = staleHash
			_, _ = engine.ReduceIncremental(proof, crystal)
		}
	})
}

func setup50FieldFrontierCache(b *testing.B) (*storage.FrontierCache, *core.ECompose, map[string]core.Hash, []core.Hash) {
	fc := storage.NewFrontierCache()
	b.Cleanup(func() { fc.Close() })
	var atoms []core.Hash
	entries := make(map[string]core.Hash)
	for i := 0; i < 50; i++ {
		path := fmt.Sprintf("user:123:field_%d", i)
		h, err := fc.AppendAtom(
			core.NewEConst(core.VString(fmt.Sprintf("value_%d", i))),
			path, nil, time.Time{},
		)
		if err != nil {
			b.Fatal(err)
		}
		atoms = append(atoms, h)
		entries[path] = h
	}
	expr := core.NewECompose(atoms)
	return fc, expr, entries, atoms
}

func BenchmarkFrontierCache_Incremental(b *testing.B) {
	store, expr, _, atoms := setup50FieldFrontierCache(b)
	path := "compose_50"
	store.AppendAtom(expr, path, atoms, time.Time{})

	proof, err := engine.ComposeProof(store, path, core.ModeDeductive)
	if err != nil {
		b.Fatal(err)
	}

	staleKey := "user:123:field_0"
	staleHash := proof.VersionVector.Entries[staleKey]

	// Stale field_0
	store.AppendAtom(core.NewEConst(core.VString("new_value")), staleKey, nil, time.Time{})

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		proof.VersionVector.Entries[staleKey] = staleHash
		_, _ = engine.ReduceIncremental(proof, store)
	}
}

func BenchmarkFrontierCache_WarmReuse_FastPath1(b *testing.B) {
	store, expr, _, atoms := setup50FieldFrontierCache(b)
	path := "compose_50"
	store.AppendAtom(expr, path, atoms, time.Time{})

	proof, _ := engine.ComposeProof(store, path, core.ModeDeductive)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = engine.ReduceIncremental(proof, store)
	}
}

func BenchmarkFrontierCache_Cold(b *testing.B) {
	store, expr, _, atoms := setup50FieldFrontierCache(b)
	path := "compose_50"
	store.AppendAtom(expr, path, atoms, time.Time{})

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = engine.ComposeProof(store, path, core.ModeDeductive)
	}
}

func BenchmarkFrontierCache_SingleKeyHotRead(b *testing.B) {
	store := storage.NewFrontierCache()
	b.Cleanup(func() { store.Close() })
	key := "users:1:name"
	h, _ := store.AppendAtom(core.NewEConst(core.VString("Alice")), key, nil, time.Time{})
	atom, _ := store.GetCurrent(key)

	proof := &engine.MaterializedProof{
		ProofTree:     atom.Expr,
		Value:         atom.Expr.(*core.EConst).Value,
		VersionVector: &engine.VersionVector{Entries: map[string]core.Hash{key: h}},
		NodeCache:     make(map[core.Hash]core.Value),
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = engine.ReduceIncremental(proof, store)
	}
}

