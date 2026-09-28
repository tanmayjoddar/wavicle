package telemetry

import (
	"encoding/json"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestHealthz_AlwaysOK(t *testing.T) {
	resetReadyChecks()
	req := httptest.NewRequest("GET", "/healthz", nil)
	rec := httptest.NewRecorder()
	healthHandler(rec, req)
	if rec.Code != 200 {
		t.Fatalf("healthz must be 200, got %d", rec.Code)
	}
}

func TestReadyz_NamesFailures(t *testing.T) {
	resetReadyChecks()
	defer resetReadyChecks()
	RegisterReadyCheck("postgres", func() error { return nil })
	RegisterReadyCheck("slot", func() error { return errTest("lag 99GB") })

	req := httptest.NewRequest("GET", "/readyz", nil)
	rec := httptest.NewRecorder()
	readyHandler(rec, req)
	if rec.Code != 503 {
		t.Fatalf("readyz must be 503 with a failing probe, got %d", rec.Code)
	}
	var body map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if body["status"] != "degraded" {
		t.Fatalf("status must be degraded: %v", body)
	}
	failed, _ := body["failed"].([]any)
	if len(failed) != 1 || !strings.Contains(failed[0].(string), "slot") {
		t.Fatalf("must NAME the failed check: %v", body)
	}

	// All green → 200.
	resetReadyChecks()
	RegisterReadyCheck("postgres", func() error { return nil })
	rec2 := httptest.NewRecorder()
	readyHandler(rec2, req)
	if rec2.Code != 200 {
		t.Fatalf("readyz must be 200 when green, got %d", rec2.Code)
	}
}

type errTest string

func (e errTest) Error() string { return string(e) }
