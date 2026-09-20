package integration_test

import (
	"database/sql"
	"strings"
	"testing"

	_ "github.com/lib/pq"
)

func TestReplicationSlot_Hygiene(t *testing.T) {
	db, err := sql.Open("postgres", testDSN)
	if err != nil {
		t.Skipf("Skipping test: cannot connect to Postgres: %v", err)
	}
	defer db.Close()
	if err := db.Ping(); err != nil {
		t.Skipf("Skipping test: Postgres not reachable: %v", err)
	}

	// Query all replication slots
	rows, err := db.Query("SELECT slot_name, active, plugin FROM pg_replication_slots")
	if err != nil {
		t.Fatalf("Failed to query pg_replication_slots: %v", err)
	}
	defer rows.Close()

	type slotRecord struct {
		name   string
		active bool
		plugin string
	}

	var allSlots []slotRecord
	var orphanedSlots []string

	for rows.Next() {
		var s slotRecord
		if err := rows.Scan(&s.name, &s.active, &s.plugin); err != nil {
			t.Fatalf("Failed to scan slot row: %v", err)
		}
		allSlots = append(allSlots, s)

		// Test slots must not be left orphaned
		if (strings.HasPrefix(s.name, "wavicle_slot_") ||
			strings.HasPrefix(s.name, "wavicle_resilience_") ||
			strings.HasPrefix(s.name, "wavicle_test_")) && !s.active {
			orphanedSlots = append(orphanedSlots, s.name)
		}
	}

	t.Logf("Audit found %d total slots in pg_replication_slots", len(allSlots))
	for _, s := range allSlots {
		t.Logf("  Slot: %-30s | Active: %-5v | Plugin: %s", s.name, s.active, s.plugin)
	}

	// If any orphaned test slots exist from aborted previous runs, clean them up and fail the check
	if len(orphanedSlots) > 0 {
		for _, name := range orphanedSlots {
			_, _ = db.Exec("SELECT pg_drop_replication_slot($1)", name)
			t.Logf("Cleaned up orphaned slot: %s", name)
		}
		t.Fatalf("Replication slot hygiene violation! Found %d orphaned test slots: %v",
			len(orphanedSlots), orphanedSlots)
	}

	t.Log("✓ Replication slot hygiene verified: No orphaned test slots found; all test slots were cleanly destroyed")
}
