package replication

import (
	"testing"

	"github.com/jackc/pglogrepl"
)

// PG reality, unit level: the pgoutput decoder must degrade gracefully on the
// shapes production actually sends — TOASTed (unchanged) columns, NULLs,
// replica-identity-short rows, and over-long tuples after ALTER TABLE ADD COLUMN
// (more tuple data than cached relation metadata).
func TestDecodeTuple_TOASTNullAndRagged(t *testing.T) {
	meta := []*pglogrepl.RelationMessageColumn{{Name: "id"}, {Name: "name"}, {Name: "bio"}}

	// TOAST 'u' (unchanged toasted value) is skipped — key must survive.
	cols := []*pglogrepl.TupleDataColumn{
		{DataType: 't', Data: []byte("42")},
		{DataType: 't', Data: []byte("Bob")},
		{DataType: 'u'},
	}
	m := decodeTupleColumns(meta, cols)
	if m["id"] != "42" || m["name"] != "Bob" {
		t.Fatalf("decoded=%v", m)
	}
	if _, present := m["bio"]; present {
		t.Fatal("TOAST-skipped column must be absent, not fabricated")
	}
	// Paths must still route on the surviving id.
	if paths := autoPathMapping("users", m); len(paths) == 0 {
		t.Fatal("row with id must still map to cache paths")
	}

	// NULL decodes to nil (DELETE/tombstone path handles it downstream).
	m2 := decodeTupleColumns(meta[:1], []*pglogrepl.TupleDataColumn{{DataType: 'n'}})
	if v, ok := m2["id"]; !ok || v != nil {
		t.Fatalf("NULL must decode to nil presence, got %v", m2)
	}

	// Ragged: more tuple columns than metadata (stale relation cache after
	// ALTER TABLE ADD COLUMN) must truncate, never index-panic.
	many := []*pglogrepl.TupleDataColumn{
		{DataType: 't', Data: []byte("1")},
		{DataType: 't', Data: []byte("x")},
		{DataType: 't', Data: []byte("extra-without-meta")},
		{DataType: 't', Data: []byte("more")},
	}
	m3 := decodeTupleColumns(meta[:2], many)
	if len(m3) != 2 {
		t.Fatalf("ragged tuple must truncate to metadata width, got %v", m3)
	}

	// REPLICA IDENTITY NOTHING + no id anywhere → no paths (nothing to route).
	// Must return nil, never a fabricated key.
	if paths := autoPathMapping("events", map[string]any{"payload": "{}"}); paths != nil {
		t.Fatalf("id-less row must map to nothing, got %v", paths)
	}
}
