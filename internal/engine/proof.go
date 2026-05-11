package engine

import (
	"sync"
	"wavicle/internal/core"
)

// VersionVector tracks which atom versions were used to compute a proof.
type VersionVector struct {
	// path -> atom_hash at the time proof was computed
	Entries map[string]core.Hash

	// MerkleRoot of all entries (not implemented yet)
	MerkleRoot core.Hash
}

func (vv *VersionVector) PathList() []string {
	paths := make([]string, 0, len(vv.Entries))
	for p := range vv.Entries {
		paths = append(paths, p)
	}
	return paths
}

// MaterializedProof is what lives in the Proof Cache.
type MaterializedProof struct {
	// Query identity
	QueryHash core.Hash
	QueryPath string
	Mode      core.ObservationMode

	// THE ANSWER
	Value     core.Value
	ValueHash core.Hash

	// STALENESS TRACKING
	VersionVector *VersionVector
	MerkleRoot    core.Hash // Merkle root of all dependency atoms

	// INCREMENTAL REDUCTION STATE
	NodeCache  map[core.Hash]core.Value // expr_hash -> reduced_value
	PathToExpr map[string]core.Hash     // path -> expr_hash

	// Proof tree: the program that generated the value
	ProofTree core.CombinatorExpr

	// Diagnostics
	ReduceStats ReduceStats

	// Metadata
	CreatedAt      int64
	LastVerifiedAt int64
	AccessCount    uint64
	Confidence     float32

	mu sync.RWMutex
}
