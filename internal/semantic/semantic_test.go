package semantic

import (
	"testing"
	"wavicle/internal/core"
)

func TestSuperpose_Empty(t *testing.T) {
	// Should not panic on empty vector slice
	res := Superpose(nil)
	if res != (core.Vector{}) {
		t.Errorf("expected zero vector, got %v", res)
	}
}
