package benchmarks

import (
	"fmt"
	"path/filepath"
	"testing"
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
			&core.EConst{Value: core.VString(fmt.Sprintf("value_%d", i))},
			path, nil,
		)
		if err != nil {
			b.Fatal(err)
		}
		atoms = append(atoms, h)
		entries[path] = h
	}
	expr := &core.ECompose{Atoms: atoms}
	return crystal, expr, entries, atoms
}

func BenchmarkProofReduction_Incremental_Warm(b *testing.B) {
	crystal, expr, entries, atoms := setup50FieldCrystal(b)

	proof := &engine.MaterializedProof{
		ProofTree:     expr,
		VersionVector: &engine.VersionVector{Entries: entries},
		NodeCache:     make(map[core.Hash]core.Value),
		PathToExpr:    make(map[string]core.Hash),
	}
	for i, atomHash := range atoms {
		proof.PathToExpr[fmt.Sprintf("user:123:field_%d", i)] = atomHash
	}

	warmVal, warmCache, err := engine.ReduceProofTree(expr, crystal)
	if err != nil {
		b.Fatal(err)
	}
	proof.NodeCache = warmCache
	proof.Value = warmVal

	if len(proof.NodeCache) < 50 {
		b.Fatalf("node cache has %d entries, expected >= 50", len(proof.NodeCache))
	}

	crystal.AppendAtom(
		&core.EConst{Value: core.VString("new_value")},
		"user:123:field_0", nil,
	)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = engine.ReduceIncremental(proof, crystal)
	}

	b.Logf("cache_hits=%d cache_misses=%d changed_paths=%d",
		proof.ReduceStats.CacheHits,
		proof.ReduceStats.CacheMisses,
		proof.ReduceStats.ChangedPaths,
	)
}

func BenchmarkProofReduction_WarmReuse_FastPath1(b *testing.B) {
	crystal, expr, entries, atoms := setup50FieldCrystal(b)

	snapshot := make(map[string]core.Hash)
	for k, v := range entries {
		snapshot[k] = v
	}

	proof := &engine.MaterializedProof{
		ProofTree:     expr,
		VersionVector: &engine.VersionVector{Entries: snapshot},
		NodeCache:     make(map[core.Hash]core.Value),
		PathToExpr:    make(map[string]core.Hash),
	}
	for i, atomHash := range atoms {
		proof.PathToExpr[fmt.Sprintf("user:123:field_%d", i)] = atomHash
	}

	warmVal, warmCache, _ := engine.ReduceProofTree(expr, crystal)
	proof.NodeCache = warmCache
	proof.Value = warmVal

	engine.ReduceIncremental(proof, crystal)

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
	entries := make(map[string]core.Hash)
	for i := 0; i < 10; i++ {
		path := fmt.Sprintf("path_%d", i)
		h, _ := crystal.AppendAtom(&core.EConst{Value: core.VString("val")}, path, nil)
		atoms = append(atoms, h)
		paths = append(paths, path)
		entries[path] = h
	}

	expr := &core.ECompose{Atoms: atoms}
	proof := &engine.MaterializedProof{
		ProofTree:     expr,
		VersionVector: &engine.VersionVector{Entries: entries},
		NodeCache:     make(map[core.Hash]core.Value),
	}

	engine.ReduceIncremental(proof, crystal)
	proof.MerkleRoot = crystal.MerkleRootForPaths(paths)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = engine.ReduceIncremental(proof, crystal)
	}
}

func BenchmarkProofReduction_Cold(b *testing.B) {
	crystal, expr, _, atoms := setup50FieldCrystal(b)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		nodeCache := make(map[core.Hash]core.Value)
		proof := &engine.MaterializedProof{
			ProofTree:     expr,
			VersionVector: &engine.VersionVector{Entries: map[string]core.Hash{"invalid": {}}},
			NodeCache:     nodeCache,
			PathToExpr:    map[string]core.Hash{},
		}
		for j, atomHash := range atoms {
			proof.PathToExpr[fmt.Sprintf("user:123:field_%d", j)] = atomHash
		}
		_, _ = engine.ReduceIncremental(proof, crystal)
	}
}
