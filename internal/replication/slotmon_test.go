package replication

import (
	"errors"
	"testing"
)

func TestClassifySlot(t *testing.T) {
	warn, page := int64(1<<30), int64(10<<30)
	cases := []struct {
		name  string
		info  *SlotInfo
		err   error
		level SlotLevel
	}{
		{"healthy", &SlotInfo{SlotName: "s", Active: true, LagBytes: 1024}, nil, SlotOK},
		{"warn", &SlotInfo{SlotName: "s", Active: true, LagBytes: warn}, nil, SlotWarn},
		{"over warn", &SlotInfo{SlotName: "s", Active: true, LagBytes: warn + 1}, nil, SlotWarn},
		{"page", &SlotInfo{SlotName: "s", Active: true, LagBytes: page}, nil, SlotPage},
		{"inactive zero lag pages", &SlotInfo{SlotName: "s", Active: false, LagBytes: 0}, nil, SlotPage},
		{"missing slot pages", nil, nil, SlotPage},
		{"query error pages", nil, errors.New("conn refused"), SlotPage},
	}
	for _, c := range cases {
		if lvl, _ := ClassifySlot(c.info, c.err, warn, page); lvl != c.level {
			t.Fatalf("%s: got %v want %v", c.name, lvl, c.level)
		}
	}
}

func TestSlotMonitor_InvalidConfigReturns(t *testing.T) {
	m := NewSlotMonitor(SlotMonitorConfig{})
	done := make(chan struct{})
	go func() { m.Run(t.Context()); close(done) }()
	<-done // must return immediately, never block on empty DSN
}
