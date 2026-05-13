package engine

import (
	"fmt"
	"time"
	"wavicle/internal/core"
	"wavicle/internal/storage"
)

func ComposeProof(crystal *storage.CausalCrystal, path string, mode core.ObservationMode) (*MaterializedProof, error) {
	relevantAtoms, err := gatherCausalClosure(crystal, path)
	if err != nil {
		return nil, fmt.Errorf("causal closure: %w", err)
	}

	proofTree := buildProofTree(crystal, relevantAtoms, path)

	value, nodeCache, err := ReduceProofTree(proofTree, crystal)
	if err != nil {
		return nil, fmt.Errorf("reduce proof tree: %w", err)
	}

	vv := captureVersionVector(crystal, relevantAtoms)
	merkleRoot := ComputeVersionMerkleRoot(vv, crystal)

	pathToExpr := make(map[string]core.Hash)
	for _, a := range relevantAtoms {
		pathToExpr[a.Path] = a.Expr.ExprHash()
	}

	now := time.Now().UnixNano()
	return &MaterializedProof{
		QueryPath:      path,
		Mode:           mode,
		Value:          value,
		ValueHash:      core.HashValue(value),
		VersionVector:  vv,
		MerkleRoot:     merkleRoot,
		NodeCache:      nodeCache,
		PathToExpr:     pathToExpr,
		ProofTree:      proofTree,
		CreatedAt:      now,
		LastVerifiedAt: now,
		AccessCount:    1,
		Confidence:     1.0,
	}, nil
}

func gatherCausalClosure(crystal *storage.CausalCrystal, path string) ([]*core.CausalAtom, error) {
	seen := make(map[core.Hash]bool)
	var collect func(h core.Hash)
	collect = func(h core.Hash) {
		if seen[h] {
			return
		}
		seen[h] = true
		parents, ok := crystal.GetParents(h)
		if !ok {
			return
		}
		for _, p := range parents {
			collect(p)
		}
	}

	atom, ok := crystal.GetCurrent(path)
	if !ok {
		return nil, fmt.Errorf("path not found: %s", path)
	}
	seen[atom.Hash] = true
	for _, p := range atom.CausalPast {
		collect(p)
	}

	var atoms []*core.CausalAtom
	for h := range seen {
		if a, ok := crystal.GetAtomByHash(h); ok {
			atoms = append(atoms, a)
		}
	}
	return atoms, nil
}

func buildProofTree(crystal *storage.CausalCrystal, atoms []*core.CausalAtom, primaryPath string) core.CombinatorExpr {
	hashes := make([]core.Hash, 0, len(atoms))
	seen := make(map[core.Hash]bool)
	for _, a := range atoms {
		if !seen[a.Hash] {
			seen[a.Hash] = true
			hashes = append(hashes, a.Hash)
		}
	}
	return &core.ECompose{Atoms: hashes}
}

func ReduceProofTree(expr core.CombinatorExpr, crystal *storage.CausalCrystal) (core.Value, map[core.Hash]core.Value, error) {
	cache := make(map[core.Hash]core.Value)
	value, err := reduceTree(expr, crystal, cache, nil)
	if err != nil {
		return nil, nil, err
	}
	return value, cache, nil
}

func captureVersionVector(crystal *storage.CausalCrystal, atoms []*core.CausalAtom) *VersionVector {
	entries := make(map[string]core.Hash)
	for _, a := range atoms {
		if a.Path != "" {
			entries[a.Path] = a.Hash
		}
	}
	return &VersionVector{Entries: entries}
}
