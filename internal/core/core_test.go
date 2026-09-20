package core

import (
	"encoding/json"
	"testing"
	"time"
)

func TestEEmbed_Serialize_NoPanic(t *testing.T) {
	var vec [256]float32
	vec[0] = 1.0
	vec[255] = -0.5
	embed := NewEEmbed(1, vec)

	bytes := embed.Serialize()
	if len(bytes) != 1+2+1024 {
		t.Fatalf("expected serialized length %d, got %d", 1+2+1024, len(bytes))
	}
	if bytes[0] != 2 {
		t.Fatalf("expected tag 2, got %d", bytes[0])
	}
}

func TestCausalAtom_JSON_Roundtrip_ECompose(t *testing.T) {
	h1 := HashValue(VString("atom1"))
	h2 := HashValue(VString("atom2"))
	compose := NewECompose([]Hash{h1, h2})

	atom := &CausalAtom{
		Hash:         HashValue(VString("root")),
		Expr:         compose,
		Path:         "user:123",
		LogicalClock: 42,
		PhysicalTime: time.Now(),
	}

	data, err := json.Marshal(atom)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	var recovered CausalAtom
	if err := json.Unmarshal(data, &recovered); err != nil {
		t.Fatalf("unmarshal failed for ECompose: %v", err)
	}

	ec, ok := recovered.Expr.(*ECompose)
	if !ok {
		t.Fatalf("expected *ECompose, got %T", recovered.Expr)
	}
	if len(ec.Atoms) != 2 || ec.Atoms[0] != h1 || ec.Atoms[1] != h2 {
		t.Fatalf("recovered atoms mismatch: %v", ec.Atoms)
	}
}
