package telemetry

import (
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
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

	// Replication
	ReplicationLagMs *prometheus.GaugeVec

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
		m.ReplicationLagMs,
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

func (m *Metrics) RecordReplicationLag(table string, commitTime time.Time) {
	lag := time.Since(commitTime).Milliseconds()
	m.ReplicationLagMs.WithLabelValues(table).Set(float64(lag))
}

func ListenAndServe(addr string) error {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.HandlerFor(Get().Registry, promhttp.HandlerOpts{}))
	return http.ListenAndServe(addr, mux)
}
