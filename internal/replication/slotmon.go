package replication

import (
	"context"
	"fmt"
	"log"
	"time"

	"wavicle/internal/telemetry"
)

// SlotLevel is the health of the PG logical replication slot.
type SlotLevel int

const (
	SlotOK SlotLevel = iota
	SlotWarn
	SlotPage
)

func (l SlotLevel) String() string {
	switch l {
	case SlotOK:
		return "OK"
	case SlotWarn:
		return "WARN"
	case SlotPage:
		return "PAGE"
	default:
		return "UNKNOWN"
	}
}

// ClassifySlot maps a slot query result to a health level. Pure function —
// unit-tested without a database.
//   - query error or missing slot → PAGE (CDC is halted or blind)
//   - inactive slot → PAGE (PG retains WAL, disk grows — see GOAT_PROOF §2.5)
//   - lag >= pageBytes → PAGE, lag >= warnBytes → WARN, else OK
func ClassifySlot(info *SlotInfo, queryErr error, warnBytes, pageBytes int64) (SlotLevel, string) {
	if queryErr != nil {
		return SlotPage, fmt.Sprintf("slot query failed: %v", queryErr)
	}
	if info == nil {
		return SlotPage, "replication slot not found (dropped?) — CDC halted"
	}
	if !info.Active {
		return SlotPage, fmt.Sprintf("slot %q inactive — WAL accumulating at ~7GB/h, disk will fill", info.SlotName)
	}
	switch {
	case info.LagBytes >= pageBytes:
		return SlotPage, fmt.Sprintf("slot lag %d bytes >= page threshold %d", info.LagBytes, pageBytes)
	case info.LagBytes >= warnBytes:
		return SlotWarn, fmt.Sprintf("slot lag %d bytes >= warn threshold %d", info.LagBytes, warnBytes)
	default:
		return SlotOK, fmt.Sprintf("slot lag %d bytes", info.LagBytes)
	}
}

// SlotMonitorConfig configures the watchdog. One cheap
// `pg_replication_slots` read per interval — negligible at any load.
type SlotMonitorConfig struct {
	DSN       string
	SlotName  string
	Interval  time.Duration
	WarnBytes int64
	PageBytes int64
}

// SlotMonitor polls the replication slot and escalates before PG fills its disk.
// It never drops the slot itself: dropping loses CDC backlog (freshness
// regression), so that stays a human decision — see the runbook.
type SlotMonitor struct {
	cfg      SlotMonitorConfig
	lastPage time.Time
}

func NewSlotMonitor(cfg SlotMonitorConfig) *SlotMonitor {
	if cfg.Interval <= 0 {
		cfg.Interval = 15 * time.Second
	}
	return &SlotMonitor{cfg: cfg}
}

// CheckOnce queries, classifies, records the metric, and logs on escalation.
// De-escalation (PAGE→OK) is logged once via lastPage reset.
func (m *SlotMonitor) CheckOnce(ctx context.Context) (SlotLevel, *SlotInfo) {
	info, err := QuerySlotInfo(ctx, m.cfg.DSN, m.cfg.SlotName)
	level, msg := ClassifySlot(info, err, m.cfg.WarnBytes, m.cfg.PageBytes)
	if info != nil {
		telemetry.Get().RecordSlotBytes(info.LagBytes)
	}
	switch level {
	case SlotPage:
		// Rate-limit paging to once per interval storm: log every time (each
		// tick is 15s+) but mark lastPage so recovery is visible.
		if time.Since(m.lastPage) > m.cfg.Interval {
			log.Printf("[slotmon] PAGE %s — runbook: check consumer, consider pg_drop_replication_slot", msg)
		}
		m.lastPage = time.Now()
	case SlotWarn:
		log.Printf("[slotmon] WARN %s", msg)
		m.lastPage = time.Time{}
	default:
		if !m.lastPage.IsZero() {
			log.Printf("[slotmon] recovered: %s", msg)
		}
		m.lastPage = time.Time{}
	}
	return level, info
}

// Run polls until ctx is cancelled. Returns immediately on invalid config so a
// misconfigured monitor can never take the server down with it.
func (m *SlotMonitor) Run(ctx context.Context) {
	if m.cfg.DSN == "" || m.cfg.SlotName == "" {
		log.Printf("[slotmon] disabled: DSN or slot name empty")
		return
	}
	ticker := time.NewTicker(m.cfg.Interval)
	defer ticker.Stop()
	// Immediate first check: a dead slot on boot should page in seconds, not intervals.
	m.CheckOnce(ctx)
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			m.CheckOnce(ctx)
		}
	}
}
