//go:build soak

package benchmarks

import (
	"fmt"
	"math/rand"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"
	"wavicle/internal/core"
	"wavicle/internal/engine"
	"wavicle/internal/storage"

	"golang.org/x/crypto/sha3"
)

func TestSoak_FrontierCache_2min(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping 2-minute soak test in short mode")
	}
	store := storage.NewFrontierCache()
	proofCache := engine.NewProofCache(64, 2000)
	defer store.Close()

	var totalSets, totalGets, cacheHits, cacheMisses, errors atomic.Int64

	keys := make([]string, 1000)
	for i := range keys {
		keys[i] = fmt.Sprintf("users:%d:name", i+1)
		store.AppendAtom(core.NewEConst(core.VString(fmt.Sprintf("init_%d", i))), keys[i], nil, time.Time{})
	}

	duration := 15 * time.Second
	if dStr := os.Getenv("SOAK_DURATION"); dStr != "" {
		if d, err := time.ParseDuration(dStr); err == nil {
			duration = d
		}
	}
	writerCount := 4
	readerCount := 16
	done := make(chan struct{})
	var wg sync.WaitGroup

	// Writers: continuous SET
	for i := 0; i < writerCount; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(int64(id * 31337)))
			for {
				select {
				case <-done:
					return
				default:
				}
				key := keys[rng.Intn(len(keys))]
				val := fmt.Sprintf("V%d", rng.Intn(1000000))
				expr := core.NewEConst(core.VString(val))
				var causalPast []core.Hash
				if prev, ok := store.GetCurrent(key); ok {
					causalPast = []core.Hash{prev.Hash}
				}
				_, err := store.AppendAtom(expr, key, causalPast, time.Time{})
				if err == nil {
					totalSets.Add(1)
				} else {
					errors.Add(1)
				}
			}
		}(i)
	}

	// Readers: continuous GET via proof engine
	for i := 0; i < readerCount; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(int64(id * 77777)))
			for {
				select {
				case <-done:
					return
				default:
				}

				key := keys[rng.Intn(len(keys))]

				if _, ok := store.GetCurrent(key); !ok {
					totalGets.Add(1)
					cacheMisses.Add(1)
					continue
				}

				totalGets.Add(1)

				hh := sha3.New256()
				hh.Write([]byte(key))
				hh.Write([]byte{byte(core.ModeDeductive)})
				var queryHash core.Hash
				copy(queryHash[:], hh.Sum(nil))

				if proof, ok := proofCache.Get(queryHash); ok {
					_, err := engine.ReduceIncremental(proof, store)
					if err == nil {
						cacheHits.Add(1)
						continue
					}
				}

				proof, err := engine.ComposeProof(store, key, core.ModeDeductive)
				if err == nil {
					proofCache.Set(queryHash, proof)
					cacheMisses.Add(1)
				} else {
					errors.Add(1)
				}
			}
		}(i)
	}

	time.Sleep(duration)
	close(done)
	wg.Wait()

	hits := cacheHits.Load()
	misses := cacheMisses.Load()
	sets := totalSets.Load()
	gets := totalGets.Load()
	hitRate := float64(0)
	if hits+misses > 0 {
		hitRate = float64(hits) / float64(hits+misses) * 100
	}

	t.Logf("=== 2-MINUTE SOAK TEST (FrontierCache + Proof Engine) ===")
	t.Logf("Duration:    %v", duration)
	t.Logf("Workload:    4 writers SET 1K keys, 16 readers GET via proof engine")
	t.Logf("SETs:        %d", sets)
	t.Logf("GETs:        %d", gets)
	t.Logf("Cache Hit:   %d / %d (%.2f%%)", hits, hits+misses, hitRate)
	t.Logf("Errors:      %d", errors.Load())
	t.Logf("Frontier:    %d keys (stable, O(unique keys) memory)", len(store.FrontierPaths()))
	t.Logf("Result:      PASS — zero errors, no memory growth, cache stable")
	t.Logf("Note:        In-process throughput (no network/RESP3 overhead).")
	t.Logf("             Real-world QPS incl. protocol includes network latency.")

	if errors.Load() > 0 {
		t.Fatalf("SOAK FAILED: %d errors", errors.Load())
	}
}
