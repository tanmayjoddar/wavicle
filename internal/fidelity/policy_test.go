package fidelity

import (
	"testing"
	"wavicle/internal/core"
)

func TestPolicy_MatchRule_ColonDelimited(t *testing.T) {
	policy := DefaultPolicy

	rule := policy.MatchRule("user:123:name")
	if rule.Mode != core.ModeInductive {
		t.Errorf("expected ModeInductive for user:123:name, got %v", rule.Mode)
	}

	rulePayment := policy.MatchRule("payment:txn_999:amount")
	if rulePayment.Mode != core.ModeDeductive {
		t.Errorf("expected ModeDeductive for payment:txn_999:amount, got %v", rulePayment.Mode)
	}
}
