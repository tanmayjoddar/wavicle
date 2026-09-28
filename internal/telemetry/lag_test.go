package telemetry

import (
	"fmt"
	"testing"
	"time"
)

func TestReplicationLag_LabelCardinalityBounded(t *testing.T) {
	// 200 distinct tables must not create 200 series.
	for i := 0; i < 200; i++ {
		Get().RecordReplicationLag(fmt.Sprintf("tenant_table_%d", i), time.Now())
	}
	fams, err := Get().Registry.Gather()
	if err != nil {
		t.Fatal(err)
	}
	labels := map[string]bool{}
	for _, f := range fams {
		if f.GetName() != "wavicle_replication_lag_ms" {
			continue
		}
		for _, m := range f.GetMetric() {
			for _, lp := range m.GetLabel() {
				if lp.GetName() == "table" {
					labels[lp.GetValue()] = true
				}
			}
		}
	}
	if len(labels) > maxLagTables+1 { // 64 tables + "_other"
		t.Fatalf("label cardinality unbounded: %d series", len(labels))
	}
	if !labels["_other"] {
		t.Fatal("overflow must collapse into _other")
	}
}
