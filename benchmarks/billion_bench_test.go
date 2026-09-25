package benchmarks

import (
	"fmt"
	"testing"
	"time"
	"wavicle/internal/core"
	"wavicle/internal/storage"
)

// Billion-scale capacity probe: measures per-key write cost, parallel GET
// throughput and bytes/key on 100k keys, so 1B can be extrapolated with
// measured (not guessed) unit costs.
func BenchmarkBillion_Write100k(b *testing.B) {
	fc := storage.NewShardedFrontierCacheWithLimit(4 << 30)
	defer fc.Close()
	b.ResetTimer()
	for i := 0; i < 100000; i++ {
		p := fmt.Sprintf("user:%d:name", i)
		_, _ = fc.AppendAtom(core.NewEConst(core.VString("AliceSmith0123456789")), p, nil, time.Time{})
	}
	b.StopTimer()
	b.Logf("bytes=%d bytes_per_key=%.1f", fc.CurrentBytes(), float64(fc.CurrentBytes())/100000)
}

func BenchmarkBillion_ParallelGet100k(b *testing.B) {
	fc := storage.NewShardedFrontierCacheWithLimit(4 << 30)
	defer fc.Close()
	for i := 0; i < 100000; i++ {
		p := fmt.Sprintf("user:%d:name", i)
		_, _ = fc.AppendAtom(core.NewEConst(core.VString("v")), p, nil, time.Time{})
	}
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			_, _ = fc.GetCurrent(fmt.Sprintf("user:%d:name", i%100000))
			i++
		}
	})
}
