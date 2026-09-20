package fidelity

import (
	"fmt"
	"strings"
	"wavicle/internal/core"
)

type FidelityRule struct {
	Pattern           string
	Mode              core.ObservationMode
	MaxLatencyMs      int
	RequireMerkleProof bool
	MinConfidence     float32
}

type FidelityPolicy struct {
	Rules []FidelityRule
}

var DefaultPolicy = FidelityPolicy{
	Rules: []FidelityRule{
		{Pattern: "/payment/**", Mode: core.ModeDeductive, MaxLatencyMs: 5, RequireMerkleProof: true, MinConfidence: 1.0},
		{Pattern: "/auth/**", Mode: core.ModeDeductive, MaxLatencyMs: 2, MinConfidence: 1.0},
		{Pattern: "/user/**", Mode: core.ModeInductive, MaxLatencyMs: 10, MinConfidence: 0.95},
		{Pattern: "/feed/**", Mode: core.ModeIntuitive, MaxLatencyMs: 1, MinConfidence: 0.70},
		{Pattern: "/analytics/**", Mode: core.ModeAbductive, MaxLatencyMs: 50, MinConfidence: 0.80},
		{Pattern: "**", Mode: core.ModeInductive, MaxLatencyMs: 10, MinConfidence: 0.0},
	},
}

type ValidatedQuery struct {
	Path     string
	Mode     core.ObservationMode
	Deadline int64
}

type PolicyError struct {
	Path      string
	Requested core.ObservationMode
	Required  core.ObservationMode
}

func (e PolicyError) Error() string {
	return fmt.Sprintf("policy: path %s requested mode %d but requires %d", e.Path, e.Requested, e.Required)
}

func matchGlob(pattern, path string) bool {
	normalize := func(s string) []string {
		s = strings.ReplaceAll(s, ":", "/")
		trimmed := strings.Trim(s, "/")
		if trimmed == "" {
			return nil
		}
		return strings.Split(trimmed, "/")
	}
	parts := normalize(pattern)
	pathParts := normalize(path)
	return matchParts(parts, pathParts)
}

func matchParts(pattern, path []string) bool {
	pi, pj := 0, 0
	for pi < len(pattern) && pj < len(path) {
		if pattern[pi] == "**" {
			if pi == len(pattern)-1 {
				return true
			}
			for k := pj; k < len(path); k++ {
				if matchParts(pattern[pi+1:], path[k:]) {
					return true
				}
			}
			return false
		}
		if pattern[pi] != "*" && pattern[pi] != path[pj] {
			return false
		}
		pi++
		pj++
	}
	return pi == len(pattern) && pj == len(path)
}

func (p *FidelityPolicy) MatchRule(path string) *FidelityRule {
	for i := range p.Rules {
		if matchGlob(p.Rules[i].Pattern, path) {
			return &p.Rules[i]
		}
	}
	return &p.Rules[len(p.Rules)-1]
}

func (p *FidelityPolicy) ModeFor(path string) core.ObservationMode {
	return p.MatchRule(path).Mode
}

func EnforcePolicy(path string, mode core.ObservationMode, policy *FidelityPolicy) (*ValidatedQuery, error) {
	rule := policy.MatchRule(path)
	if mode > rule.Mode {
		return nil, PolicyError{
			Path:      path,
			Requested: mode,
			Required:  rule.Mode,
		}
	}
	return &ValidatedQuery{
		Path:     path,
		Mode:     mode,
		Deadline: 0,
	}, nil
}
