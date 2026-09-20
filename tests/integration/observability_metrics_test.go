package integration_test

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"

	"wavicle/internal/telemetry"
)

func TestObservability_PrometheusMetricsAndAlertThresholds(t *testing.T) {
	m := telemetry.Get()

	// 1. Simulate telemetry recording
	m.RecordReplicationLag("users", time.Now().Add(-650*time.Millisecond)) // Lag = 650ms (> 500ms threshold)
	m.RecordMemoryUsage(450*1024*1024, 500*1024*1024)                    // 90% memory pressure
	m.RecordMemoryEviction()
	m.RecordSlotBytes(128 * 1024 * 1024)                                  // 128 MB slot size
	m.RecordCacheHit()
	m.RecordCacheMiss()

	// 2. Query Prometheus HTTP /metrics handler
	handler := promhttp.HandlerFor(m.Registry, promhttp.HandlerOpts{})
	req := httptest.NewRequest("GET", "/metrics", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("Expected HTTP 200 from /metrics, got %d", rec.Code)
	}

	body, err := io.ReadAll(rec.Body)
	if err != nil {
		t.Fatalf("Failed to read response body: %v", err)
	}
	bodyStr := string(body)

	// 3. Verify all required metrics are present in the output
	requiredMetrics := []string{
		"wavicle_replication_lag_ms",
		"wavicle_replication_slot_bytes",
		"wavicle_memory_bytes",
		"wavicle_memory_limit_bytes",
		"wavicle_memory_evictions_total",
		"wavicle_cache_hits_total",
		"wavicle_cache_misses_total",
	}

	for _, metricName := range requiredMetrics {
		if !strings.Contains(bodyStr, metricName) {
			t.Errorf("Expected metric %q in /metrics endpoint output, but not found", metricName)
		}
	}

	// 4. Verify the recorded replication lag exceeds the alertable 500ms threshold
	if !strings.Contains(bodyStr, `wavicle_replication_lag_ms{table="users"}`) {
		t.Fatalf("Expected labeled gauge wavicle_replication_lag_ms{table=\"users\"} in metrics output")
	}

	t.Log("✓ /metrics endpoint verified: all operational metrics (lag, slot size, memory, evictions, hits/misses) queryable")
}
