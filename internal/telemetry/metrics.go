package telemetry

import (
	"encoding/json"
	"net/http"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	dto "github.com/prometheus/client_model/go"
)

// Metrics holds the official Prometheus metrics for Wavicle.
type Metrics struct {
	// Request counters
	RequestsTotal *prometheus.CounterVec
	ErrorsTotal   prometheus.Counter

	// Performance
	CacheHitsTotal       prometheus.Counter
	CacheMissesTotal      prometheus.Counter
	ProofReductionsTotal prometheus.Counter
	IncrementalHitsTotal prometheus.Counter
	RequestDuration      *prometheus.HistogramVec
	ActiveConnections    prometheus.Gauge
	DBReconnectsTotal    prometheus.Counter
	AtomCount            prometheus.Gauge // Renamed from FrontierCacheEntries for consistency
	TTLEvictionsTotal    prometheus.Counter

	// Memory Management
	MemoryUsedBytes      prometheus.Gauge
	MemoryLimitBytes     prometheus.Gauge
	MemoryEvictionsTotal prometheus.Counter

	// Replication
	ReplicationLagMs     *prometheus.GaugeVec
	ReplicationSlotBytes prometheus.Gauge

	// Registry
	Registry *prometheus.Registry
}

var global *Metrics

func init() {
	m := &Metrics{
		Registry: prometheus.NewRegistry(),
	}

	m.RequestsTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "wavicle_requests_total",
		Help: "Total number of RESP3 requests by command.",
	}, []string{"command"})

	m.ErrorsTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "wavicle_errors_total",
		Help: "Total number of internal errors.",
	})

	m.CacheHitsTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "wavicle_cache_hits_total",
		Help: "Total number of proof cache hits.",
	})

	m.CacheMissesTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "wavicle_cache_misses_total",
		Help: "Total number of proof cache misses.",
	})

	m.ProofReductionsTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "wavicle_proof_reductions_total",
		Help: "Total number of full proof reductions.",
	})

	m.IncrementalHitsTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "wavicle_incremental_hits_total",
		Help: "Total number of successful incremental reductions.",
	})

	m.RequestDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "wavicle_request_duration_seconds",
		Help:    "Latency of RESP3 commands in seconds.",
		Buckets: prometheus.DefBuckets,
	}, []string{"command"})

	m.ActiveConnections = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "wavicle_active_connections",
		Help: "Current number of active client connections.",
	})

	m.DBReconnectsTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "wavicle_db_reconnects_total",
		Help: "Total number of PostgreSQL replication reconnections.",
	})

	m.AtomCount = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "wavicle_atom_count",
		Help: "Current number of live atoms in the frontier cache (one per unique key).",
	})

	m.TTLEvictionsTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "wavicle_ttl_evictions_total",
		Help: "Total number of items automatically evicted due to TTL expiry.",
	})

	m.ReplicationLagMs = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "wavicle_replication_lag_ms",
		Help: "Current replication lag from external DB in milliseconds.",
	}, []string{"table"})

	m.MemoryUsedBytes = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "wavicle_memory_bytes",
		Help: "Estimated memory usage of the in-memory cache in bytes.",
	})

	m.MemoryLimitBytes = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "wavicle_memory_limit_bytes",
		Help: "Configured maximum memory ceiling for the cache in bytes.",
	})

	m.MemoryEvictionsTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "wavicle_memory_evictions_total",
		Help: "Total number of items evicted due to memory pressure.",
	})

	m.ReplicationSlotBytes = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "wavicle_replication_slot_bytes",
		Help: "Disk bytes consumed by PostgreSQL replication slot.",
	})

	// Register all metrics
	m.Registry.MustRegister(
		m.RequestsTotal,
		m.ErrorsTotal,
		m.CacheHitsTotal,
		m.CacheMissesTotal,
		m.ProofReductionsTotal,
		m.IncrementalHitsTotal,
		m.RequestDuration,
		m.ActiveConnections,
		m.DBReconnectsTotal,
		m.AtomCount,
		m.TTLEvictionsTotal,
		m.MemoryUsedBytes,
		m.MemoryLimitBytes,
		m.MemoryEvictionsTotal,
		m.ReplicationLagMs,
		m.ReplicationSlotBytes,
	)

	global = m
}

func Get() *Metrics { return global }

// Helper methods to match the old API while using Prometheus internally

func (m *Metrics) RecordRequest(cmd string) {
	m.RequestsTotal.WithLabelValues(cmd).Inc()
}

func (m *Metrics) RecordDuration(cmd string, duration time.Duration) {
	m.RequestDuration.WithLabelValues(cmd).Observe(duration.Seconds())
}

func (m *Metrics) IncActiveConnections() {
	m.ActiveConnections.Inc()
}

func (m *Metrics) DecActiveConnections() {
	m.ActiveConnections.Dec()
}

func (m *Metrics) RecordDBReconnect() {
	m.DBReconnectsTotal.Inc()
}

func (m *Metrics) RecordTTLEviction() {
	m.TTLEvictionsTotal.Inc()
}

func (m *Metrics) RecordMemoryUsage(used, limit int64) {
	m.MemoryUsedBytes.Set(float64(used))
	m.MemoryLimitBytes.Set(float64(limit))
}

func (m *Metrics) RecordMemoryEviction() {
	m.MemoryEvictionsTotal.Inc()
}

func (m *Metrics) RecordSlotBytes(bytes int64) {
	m.ReplicationSlotBytes.Set(float64(bytes))
}

func (m *Metrics) RecordError() {
	m.ErrorsTotal.Inc()
}

func (m *Metrics) RecordCacheHit() {
	m.CacheHitsTotal.Inc()
}

func (m *Metrics) RecordCacheMiss() {
	m.CacheMissesTotal.Inc()
}

func (m *Metrics) RecordProofReduction() {
	m.ProofReductionsTotal.Inc()
}

func (m *Metrics) RecordIncrementalHit() {
	m.IncrementalHitsTotal.Inc()
}

func (m *Metrics) RecordAtomCount(count int64) {
	m.AtomCount.Set(float64(count))
}

// maxLagTables bounds the `table` label cardinality of ReplicationLagMs.
// A fresh label per table is an unbounded series explosion if table names are
// attacker- or tenant-controlled; overflow collapses into "_other".
// Production note: the aggregate max across tables is what pages — per-table
// is for diagnosis, so losing granularity past the cap is the right trade.
const maxLagTables = 64

var (
	lagTablesMu sync.Mutex
	lagTables   = map[string]bool{}
)

func (m *Metrics) RecordReplicationLag(table string, commitTime time.Time) {
	lag := time.Since(commitTime).Milliseconds()
	label := table
	lagTablesMu.Lock()
	if !lagTables[table] {
		if len(lagTables) >= maxLagTables {
			label = "_other"
		} else {
			lagTables[table] = true
		}
	}
	lagTablesMu.Unlock()
	m.ReplicationLagMs.WithLabelValues(label).Set(float64(lag))
}

func (m *Metrics) GetMemoryEvictions() float64 {
	var metric dto.Metric
	if err := m.MemoryEvictionsTotal.Write(&metric); err == nil && metric.Counter != nil {
		return metric.Counter.GetValue()
	}
	return 0
}

func (m *Metrics) GetTTLEvictions() float64 {
	var metric dto.Metric
	if err := m.TTLEvictionsTotal.Write(&metric); err == nil && metric.Counter != nil {
		return metric.Counter.GetValue()
	}
	return 0
}

func ListenAndServe(addr string) error {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.HandlerFor(Get().Registry, promhttp.HandlerOpts{}))
	mux.HandleFunc("/healthz", healthHandler)
	mux.HandleFunc("/readyz", readyHandler)
	return http.ListenAndServe(addr, mux)
}

// ---- readiness ----

// readyCheck is one named dependency probe for /readyz.
type readyCheck struct {
	name string
	fn   func() error
}

var (
	readyMu     sync.Mutex
	readyChecks []readyCheck
)

// RegisterReadyCheck adds a /readyz dependency probe. Probes run on every
// scrape — keep them cheap (a Ping, a single-row query). Called from main.
func RegisterReadyCheck(name string, fn func() error) {
	readyMu.Lock()
	defer readyMu.Unlock()
	readyChecks = append(readyChecks, readyCheck{name, fn})
}

// resetReadyChecks exists for tests (the registry is process-global).
func resetReadyChecks() {
	readyMu.Lock()
	defer readyMu.Unlock()
	readyChecks = nil
}

// healthHandler is liveness: 200 iff the process is up and serving HTTP.
// No dependency checks — kube uses it to decide restarts, never traffic.
func healthHandler(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(`{"status":"ok"}`))
}

// readyHandler is readiness: 200 only if every registered probe passes,
// else 503 with a JSON body NAMING the failed checks. kube removes the pod
// from service on 503; the stale guard is what keeps it from serving lies.
func readyHandler(w http.ResponseWriter, _ *http.Request) {
	readyMu.Lock()
	checks := append([]readyCheck(nil), readyChecks...)
	readyMu.Unlock()
	var failed []string
	for _, c := range checks {
		if err := c.fn(); err != nil {
			failed = append(failed, c.name+": "+err.Error())
		}
	}
	w.Header().Set("Content-Type", "application/json")
	if len(failed) > 0 {
		w.WriteHeader(http.StatusServiceUnavailable)
		b, _ := json.Marshal(map[string]any{"status": "degraded", "failed": failed})
		_, _ = w.Write(b)
		return
	}
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(`{"status":"ok"}`))
}
