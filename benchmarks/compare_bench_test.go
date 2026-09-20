package benchmarks

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"
	"wavicle/internal/core"
	"wavicle/internal/engine"
	"wavicle/internal/storage"
)

func BenchmarkEngine(b *testing.B) {
	backend := os.Getenv("WAVICLE_BENCH_BACKEND")
	if backend == "" {
		backend = "frontier"
	}

	var store storage.Store
	if backend == "crystal" {
		c, err := storage.NewCausalCrystal(filepath.Join(b.TempDir(), "compare_crystal.log"))
		if err != nil {
			b.Fatal(err)
		}
		b.Cleanup(func() { c.Close() })
		store = c
	} else {
		fc := storage.NewFrontierCache()
		b.Cleanup(func() { fc.Close() })
		store = fc
	}

	// Setup 50 fields
	var atoms []core.Hash
	for i := 0; i < 50; i++ {
		path := fmt.Sprintf("user:123:field_%d", i)
		h, err := store.AppendAtom(
			core.NewEConst(core.VString(fmt.Sprintf("value_%d", i))),
			path, nil, time.Time{},
		)
		if err != nil {
			b.Fatal(err)
		}
		atoms = append(atoms, h)
	}
	composePath := "compose_50"
	store.AppendAtom(core.NewECompose(atoms), composePath, atoms, time.Time{})

	proof, err := engine.ComposeProof(store, composePath, core.ModeDeductive)
	if err != nil {
		b.Fatal(err)
	}

	staleKey := "user:123:field_0"
	staleHash := proof.VersionVector.Entries[staleKey]

	// Single key atom setup
	singleKey := "users:1:name"
	singleH, _ := store.AppendAtom(core.NewEConst(core.VString("Alice")), singleKey, nil, time.Time{})
	singleProof, _ := engine.ComposeProof(store, singleKey, core.ModeDeductive)
	_ = singleH

	b.Run("ColdCompose_50Fields", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, _ = engine.ComposeProof(store, composePath, core.ModeDeductive)
		}
	})

	b.Run("Incremental_1of50Changed", func(b *testing.B) {
		// mutate field_0 in store
		store.AppendAtom(core.NewEConst(core.VString("new_val")), staleKey, nil, time.Time{})
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			proof.VersionVector.Entries[staleKey] = staleHash
			_, _ = engine.ReduceIncremental(proof, store)
		}
	})

	b.Run("WarmReuse_FastPath1", func(b *testing.B) {
		// Sync proof to store so VV matches
		_, _ = engine.ReduceIncremental(proof, store)
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, _ = engine.ReduceIncremental(proof, store)
		}
	})

	b.Run("SingleKey_HotRead", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, _ = engine.ReduceIncremental(singleProof, store)
		}
	})
}
