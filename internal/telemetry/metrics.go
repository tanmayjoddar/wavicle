package telemetry

import (
	"fmt"
	"net/http"
	"sync"
	"sync/atomic"
)

// Simple counter-based metrics. Prometheus integration can replace this.
type Metrics struct {
	mu sync.RWMutex

	// Request counters
	Gets     atomic.Int64
	Sets     atomic.Int64
	Dels     atomic.Int64
	MGets    atomic.Int64
	MSets    atomic.Int64
	HSets    atomic.Int64
	HGets    atomic.Int64
	HGetAlls atomic.Int64
	Expires  atomic.Int64
	TTLs     atomic.Int64
	Exists   atomic.Int64
	DbSizes  atomic.Int64
	Pings    atomic.Int64
	Errors   atomic.Int64

	// Cache metrics
	CacheHits   atomic.Int64
	CacheMisses atomic.Int64

	// Performance
	ProofReductions atomic.Int64
	IncrementalHits atomic.Int64
}

var global = &Metrics{}

func Get() *Metrics { return global }

func (m *Metrics) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/plain; version=0.0.4")
	m.mu.RLock()
	defer m.mu.RUnlock()

	fmt.Fprintf(w, "wavicle_requests_total{command=\"GET\"} %d\n", m.Gets.Load())
	fmt.Fprintf(w, "wavicle_requests_total{command=\"SET\"} %d\n", m.Sets.Load())
	fmt.Fprintf(w, "wavicle_requests_total{command=\"DEL\"} %d\n", m.Dels.Load())
	fmt.Fprintf(w, "wavicle_requests_total{command=\"MGET\"} %d\n", m.MGets.Load())
	fmt.Fprintf(w, "wavicle_requests_total{command=\"MSET\"} %d\n", m.MSets.Load())
	fmt.Fprintf(w, "wavicle_requests_total{command=\"HSET\"} %d\n", m.HSets.Load())
	fmt.Fprintf(w, "wavicle_requests_total{command=\"HGET\"} %d\n", m.HGets.Load())
	fmt.Fprintf(w, "wavicle_requests_total{command=\"HGETALL\"} %d\n", m.HGetAlls.Load())
	fmt.Fprintf(w, "wavicle_requests_total{command=\"EXPIRE\"} %d\n", m.Expires.Load())
	fmt.Fprintf(w, "wavicle_requests_total{command=\"TTL\"} %d\n", m.TTLs.Load())
	fmt.Fprintf(w, "wavicle_requests_total{command=\"EXISTS\"} %d\n", m.Exists.Load())
	fmt.Fprintf(w, "wavicle_requests_total{command=\"DBSIZE\"} %d\n", m.DbSizes.Load())
	fmt.Fprintf(w, "wavicle_requests_total{command=\"PING\"} %d\n", m.Pings.Load())
	fmt.Fprintf(w, "wavicle_requests_total{command=\"ERROR\"} %d\n", m.Errors.Load())
	fmt.Fprintf(w, "wavicle_cache_hits_total %d\n", m.CacheHits.Load())
	fmt.Fprintf(w, "wavicle_cache_misses_total %d\n", m.CacheMisses.Load())
	fmt.Fprintf(w, "wavicle_proof_reductions_total %d\n", m.ProofReductions.Load())
	fmt.Fprintf(w, "wavicle_incremental_hits_total %d\n", m.IncrementalHits.Load())
}

func ListenAndServe(addr string) error {
	mux := http.NewServeMux()
	mux.Handle("/metrics", Get())
	return http.ListenAndServe(addr, mux)
}
