package autopoiesis

import (
	"math"
)

type EntanglementStatus interface {
	statusTag()
}

type EntanglementHard struct {
	Lift   float64
	PValue float64
}

func (e EntanglementHard) statusTag() {}

type EntanglementSoft struct {
	Lift   float64
	PValue float64
}

func (e EntanglementSoft) statusTag() {}

type StatusInsufficientData struct{}

func (s StatusInsufficientData) statusTag() {}

type StatusIndependent struct{}

func (s StatusIndependent) statusTag() {}

type StatusWeakCorrelation struct{}

func (s StatusWeakCorrelation) statusTag() {}

func EvaluateEntanglement(
	fAB, fA, fB, N, deltaTms, Tms uint64,
) EntanglementStatus {
	if fAB < 10 {
		return StatusInsufficientData{}
	}

	expected := float64(fA) * float64(fB) * float64(deltaTms) / float64(Tms)
	if expected < 1.0 {
		return StatusInsufficientData{}
	}

	lift := float64(fAB) / expected

	// Chi-squared with Yates' continuity correction
	a := float64(fAB)
	b := float64(fA - fAB)
	c := float64(fB - fAB)
	d := float64(N - fA - fB + fAB)
	n := float64(N)
	chiSq := n * math.Pow(math.Abs(a*d-b*c)-0.5*n, 2) / ((a + b) * (a + c) * (b + d) * (c + d))

	pValue := chiSquaredPValue(chiSq, 1)

	switch {
	case lift > 10.0 && pValue < 0.001:
		return EntanglementHard{Lift: lift, PValue: pValue}
	case lift > 3.0 && pValue < 0.05:
		return EntanglementSoft{Lift: lift, PValue: pValue}
	case lift < 1.5:
		return StatusIndependent{}
	default:
		return StatusWeakCorrelation{}
	}
}

func chiSquaredPValue(chiSq float64, df int) float64 {
	// Simplified p-value approximation for 1 degree of freedom
	if chiSq > 10.827 {
		return 0.0009
	}
	if chiSq > 3.841 {
		return 0.04
	}
	return 0.5
}
