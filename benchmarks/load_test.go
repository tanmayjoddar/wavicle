package benchmarks

import (
	"fmt"
	"path/filepath"
	"testing"
	"wavicle/internal/core"
	"wavicle/internal/engine"
	"wavicle/internal/storage"
)

func BenchmarkConcurrency_1000Clients(b *testing.B) {
	crystal, err := storage.NewCausalCrystal(filepath.Join(b.TempDir(), "bench_load.log"))
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { crystal.Close() })

	for i := 0; i < 100; i++ {
		crystal.AppendAtom(&core.EConst{Value: core.VString("val")}, fmt.Sprintf("key_%d", i), nil)
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			key := fmt.Sprintf("key_%d", i%100)
			if i%10 == 0 {
				crystal.AppendAtom(&core.EConst{Value: core.VString("new_val")}, key, nil)
			} else {
				atom, _ := crystal.GetCurrent(key)
				if atom != nil {
					proof := &engine.MaterializedProof{
						ProofTree:     atom.Expr,
						VersionVector: &engine.VersionVector{Entries: map[string]core.Hash{key: atom.Hash}},
						NodeCache:     make(map[core.Hash]core.Value),
						Value:         atom.Expr.(*core.EConst).Value,
					}
					engine.ReduceIncremental(proof, crystal)
				}
			}
			i++
		}
	})
}

func BenchmarkWarmRead_Wavicle_vs_RedisSim(b *testing.B) {
	crystal, err := storage.NewCausalCrystal(filepath.Join(b.TempDir(), "bench_warm.log"))
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { crystal.Close() })
	key := "warm_key"
	h, _ := crystal.AppendAtom(&core.EConst{Value: core.VString("warm_val")}, key, nil)
	atom, _ := crystal.GetCurrent(key)

	proof := &engine.MaterializedProof{
		ProofTree:     atom.Expr,
		Value:         atom.Expr.(*core.EConst).Value,
		VersionVector: &engine.VersionVector{Entries: map[string]core.Hash{key: h}},
		NodeCache:     make(map[core.Hash]core.Value),
	}

	b.Run("Wavicle_WarmRead", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_, _ = engine.ReduceIncremental(proof, crystal)
		}
	})

	redisSim := make(map[string]string)
	redisSim[key] = "warm_val"

	b.Run("RedisSim_EngineOnly", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_ = redisSim[key]
		}
	})
}
