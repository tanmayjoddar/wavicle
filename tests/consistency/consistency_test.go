// Randomized consistency checker (Jepsen-lite).
//
// Two writer kinds race the same keyspace while readers assert staleness bounds:
//   - app writers: like SET through the server (synchronous store appends)
//   - raw writers: like DBA SQL bypassing the server — queued into a pausable
//     applier, exactly what the CDC path does when PG logical replication
//     delivers (or stalls)
//
// Fixed seed (logged): fully reproducible. The validity oracle, stated
// exactly: every value a read returns must have been PUBLISHED (appended to
// the store) before that read completed — checked as membership in the
// per-key history, which is appended atomically with the store write under
// pubMu, so no observed value can predate its history record. On top of
// validity, per-key MONOTONIC reads: the history positions of successive
// observed values must never decrease (a reader that saw generation 8 must
// never later see generation 5 — that would be a stale proof escaping).
// Mid-run stalls the "CDC" (pausable applier) and resumes — convergence must
// still hold afterwards. Failures report key, expected, observed, and seed.
package consistency_test

import (
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"
	"wavicle/internal/core"
	"wavicle/internal/engine"
	"wavicle/internal/storage"

	"golang.org/x/crypto/sha3"
)

func queryHashFor(key string) core.Hash {
	h := sha3.New256()
	h.Write([]byte(key))
	h.Write([]byte{byte(core.ModeDeductive)})
	var qh core.Hash
	copy(qh[:], h.Sum(nil))
	return qh
}

func proofRead(store storage.Store, pc *engine.ProofCache, key string) (string, error) {
	qh := queryHashFor(key)
	if proof, ok := pc.Get(qh); ok {
		if v, err := engine.ReduceIncremental(proof, store); err == nil {
			return fmt.Sprint(v), nil
		}
	}
	proof, err := engine.ComposeProof(store, key, core.ModeDeductive)
	if err != nil {
		return "", err
	}
	pc.Set(qh, proof)
	return fmt.Sprint(proof.Value), nil
}

func TestConsistency_RandomizedDualWriters(t *testing.T) {
	const seed = 20260928
	t.Logf("consistency seed: %d", seed)

	store := storage.NewShardedFrontierCacheWithLimit(256 << 20)
	defer store.Close()
	pc := engine.NewProofCache(64, 2000)

	const nKeys = 64
	keys := make([]string, nKeys)
	for i := range keys {
		keys[i] = fmt.Sprintf("acct:%d:balance", i)
	}

	// Publish order == history order, always: every store append and its
	// history record happen atomically under pubMu with a sequence number.
	// Readers therefore observe a total order per key, which is what makes
	// the monotonic assertion sound (no timestamp races, no inversions).
	var pubMu sync.Mutex
	var seq uint64
	type histRec struct {
		seq uint64
		val string
	}
	history := map[string][]histRec{}
	publish := func(k, v string) {
		pubMu.Lock()
		defer pubMu.Unlock()
		seq++
		history[k] = append(history[k], histRec{seq, v})
		_, _ = store.AppendAtom(core.NewEConst(core.VString(v)), k, nil, time.Time{})
	}
	for i, k := range keys {
		publish(k, fmt.Sprintf("init-%d", i))
	}
	// checkRead asserts validity + monotonicity for one observation.
	// maxSeen maps key -> highest history position observed so far (owned by
	// the calling reader goroutine — no sharing, no lock).
	checkRead := func(k, got string, maxSeen map[string]int) string {
		pubMu.Lock()
		defer pubMu.Unlock()
		idx := -1
		for i, r := range history[k] {
			if r.val == got {
				idx = i // last occurrence = latest generation of this value
			}
		}
		if idx < 0 {
			return fmt.Sprintf("VALIDITY key=%s observed un-written value %q (seed %d)", k, got, seed)
		}
		if idx < maxSeen[k] {
			return fmt.Sprintf("MONOTONIC key=%s went backwards to %q (seed %d)", k, got, seed)
		}
		maxSeen[k] = idx
		return ""
	}

	stop := make(chan struct{})
	var wg sync.WaitGroup
	var violMu sync.Mutex
	var violations int
	violate := func(format string, args ...any) {
		violMu.Lock()
		defer violMu.Unlock()
		violations++
		t.Errorf(format, args...)
	}

	// App writers: synchronous appends, paced to ~1k writes/s each so history
	// stays bounded (~30k entries over the run).
	for w := 0; w < 3; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			r := rand.New(rand.NewSource(int64(seed + w)))
			for {
				select {
				case <-stop:
					return
				default:
				}
				k := keys[r.Intn(nKeys)]
				v := fmt.Sprintf("app%d-%d", w, r.Intn(1000000))
				publish(k, v)
				time.Sleep(time.Millisecond)
			}
		}(w)
	}

	// Raw writers: external-DB analog. Values queue into the applier; when it
	// is stalled the queue backpressures (blocking select on stop) — the same
	// backpressure production chose over silent CDC drops.
	pending := make(chan [2]string, 4096)
	for w := 0; w < 2; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			r := rand.New(rand.NewSource(int64(seed + 100 + w)))
			for {
				select {
				case <-stop:
					return
				default:
				}
				k := keys[r.Intn(nKeys)]
				v := fmt.Sprintf("raw%d-%d", w, r.Intn(1000000))
				// Queued, NOT published: the applier assigns the sequence at
				// apply time, so history order always equals apply order.
				select {
				case pending <- [2]string{k, v}:
				case <-stop:
					return
				}
				time.Sleep(time.Millisecond)
			}
		}(w)
	}

	// Applier with a stall switch (failure injection).
	applierOn := make(chan bool, 1)
	applierOn <- true
	wg.Add(1)
	go func() {
		defer wg.Done()
		on := true
		for {
			select {
			case <-stop:
				return
			case b := <-applierOn:
				on = b
			default:
			}
			if !on {
				time.Sleep(5 * time.Millisecond)
				continue
			}
			select {
			case <-stop:
				return
			case vis := <-pending:
				publish(vis[0], vis[1])
			}
		}
	}()

	// Readers: validity + monotonicity on every single read.
	for r := 0; r < 6; r++ {
		wg.Add(1)
		go func(r int) {
			defer wg.Done()
			rr := rand.New(rand.NewSource(int64(seed + 200 + r)))
			maxSeen := map[string]int{}
			for {
				select {
				case <-stop:
					return
				default:
				}
				k := keys[rr.Intn(nKeys)]
				got, err := proofRead(store, pc, k)
				if err != nil {
					continue
				}
				if msg := checkRead(k, got, maxSeen); msg != "" {
					violate("%s", msg)
				}
			}
		}(r)
	}

	// Phase 1: race 3s. Phase 2: stall CDC 1s. Phase 3: resume 2s.
	time.Sleep(3 * time.Second)
	applierOn <- false
	time.Sleep(1 * time.Second)
	applierOn <- true
	time.Sleep(2 * time.Second)
	close(stop)
	wg.Wait()

	violMu.Lock()
	n := violations
	violMu.Unlock()
	if n > 0 {
		t.Fatalf("%d validity violations (seed %d)", n, seed)
	}

	// Convergence: drain leftovers through the SAME applier path, then the
	// proof engine must agree with the store on every key.
drain:
	for {
		select {
		case vis := <-pending:
			publish(vis[0], vis[1])
		default:
			break drain
		}
	}
	for _, k := range keys {
		cur, ok := store.GetCurrent(k)
		if !ok {
			t.Fatalf("key %s vanished", k)
		}
		want := fmt.Sprint(cur.Expr.(*core.EConst).Value)
		got, err := proofRead(store, pc, k)
		if err != nil {
			t.Fatalf("converge read %s: %v", k, err)
		}
		if got != want {
			t.Fatalf("DIVERGENCE key=%s proof=%q store=%q (seed %d)", k, got, want, seed)
		}
	}
	t.Logf("keys=%d validity=0 monotonic=0 converged after stall+resume — seed %d", nKeys, seed)
}
