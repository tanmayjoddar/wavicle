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

// ProofNode wraps a CombinatorExpr to cache its value and track its dirty state.
type ProofNode struct {
	Expr        core.CombinatorExpr
	Children    []*ProofNode
	Parent      *ProofNode // Support upward propagation
	CachedValue core.Value
	Dirty       bool

	// Metadata to map crystal updates back to this node
	SourceHash core.Hash
	SourcePath string
}

// PropagateDirtyUp marks this node as dirty and recursively marks its ancestors.
func (n *ProofNode) PropagateDirtyUp() {
	if n.Dirty {
		return // Already propagating from this branch
	}
	n.Dirty = true
	if n.Parent != nil {
		n.Parent.PropagateDirtyUp()
	}
}

// MarkDirty recursively marks a node and its parents as dirty if its source hash/path matches the changed set.
// This is O(n) and should only be used as a fallback.
func (n *ProofNode) MarkDirty(changedPaths map[string]bool) {
	if n.SourcePath != "" && changedPaths[n.SourcePath] {
		n.Dirty = true
		return
	}

	for _, c := range n.Children {
		c.MarkDirty(changedPaths)
		if c.Dirty {
			n.Dirty = true
		}
	}
}

// ResetDirty clears the dirty flags for the next reduction cycle.
func (n *ProofNode) ResetDirty() {
	n.Dirty = false
	for _, c := range n.Children {
		c.ResetDirty()
	}
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

	// The proofs of causality
	VersionVector *VersionVector
	MerkleRoot    core.Hash

	// Legacy cache (to be deprecated by RootNode)
	NodeCache  map[core.Hash]core.Value
	PathToExpr map[string]core.Hash

	// Index for O(k log d) dirty propagation
	PathToNode map[string]*ProofNode

	// Proof tree: the program that generated the value
	ProofTree core.CombinatorExpr

	// RootNode: The new zero-hash lazy evaluation tree
	RootNode *ProofNode

	// Diagnostics
	ReduceStats ReduceStats

	// Metadata
	CreatedAt      int64
	LastVerifiedAt int64
	AccessCount    uint64
	Confidence     float32

	mu sync.RWMutex
}

