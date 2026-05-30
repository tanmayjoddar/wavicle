package engine

import (
	"fmt"
	"wavicle/internal/core"
	"wavicle/internal/storage"
)

func ComposeProof(store storage.Store, path string, mode core.ObservationMode) (*MaterializedProof, error) {
	relevantAtoms, err := gatherCausalClosure(store, path)
	if err != nil {
		return nil, fmt.Errorf("causal closure: %w", err)
	}

	pathToNode := make(map[string]*ProofNode)
	rootNode := buildProofNode(store, path, nil, pathToNode)

	// Keep the legacy ProofTree for backward compatibility
	proofTree := buildProofTree(store, relevantAtoms, path)

	proof := &MaterializedProof{
		QueryPath:     path,
		Mode:          mode,
		ProofTree:     proofTree,
		RootNode:      rootNode,
		PathToNode:    pathToNode,
		VersionVector: captureVersionVector(store, relevantAtoms),
	}

	if crystal, ok := store.(*storage.CausalCrystal); ok {
		proof.MerkleRoot = ComputeVersionMerkleRoot(proof.VersionVector, crystal)
		// Perform a cold reduction on the RootNode to populate its CachedValue fields
		val, _ := reduceDirty(proof.RootNode, crystal, proof)
		proof.Value = val
		proof.ValueHash = core.HashValue(val)
	}

	return proof, nil
}

func buildProofNode(store storage.Store, path string, parent *ProofNode, pathIndex map[string]*ProofNode) *ProofNode {
	atom, ok := store.GetCurrent(path)
	if !ok {
		return nil
	}

	node := &ProofNode{
		Expr:       atom.Expr,
		SourceHash: atom.Hash,
		SourcePath: path,
		Parent:     parent,
	}
	pathIndex[path] = node

	// Recursive construction based on expression type
	switch e := atom.Expr.(type) {
	case *core.ECompose:
		node.Children = make([]*ProofNode, len(e.Atoms))
		for i, h := range e.Atoms {
			// Find the path for this atom hash
			if childAtom, ok := store.GetAtom(h); ok {
				node.Children[i] = buildProofNode(store, childAtom.Path, node, pathIndex)
			}
		}
	}

	return node
}

func gatherCausalClosure(crystal storage.Store, path string) ([]*core.CausalAtom, error) {
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
		if a, ok := crystal.GetAtom(h); ok {
			atoms = append(atoms, a)
		}
	}
	return atoms, nil
}

func buildProofTree(crystal storage.Store, atoms []*core.CausalAtom, primaryPath string) core.CombinatorExpr {
	hashes := make([]core.Hash, 0, len(atoms))
	seen := make(map[core.Hash]bool)
	for _, a := range atoms {
		if !seen[a.Hash] {
			seen[a.Hash] = true
			hashes = append(hashes, a.Hash)
		}
	}
	return core.NewECompose(hashes)
}

func ReduceProofTree(expr core.CombinatorExpr, crystal storage.Store) (core.Value, map[core.Hash]core.Value, error) {
	cache := make(map[core.Hash]core.Value)
	value, err := reduceTree(expr, crystal, cache, nil)
	if err != nil {
		return nil, nil, err
	}
	return value, cache, nil
}

func captureVersionVector(crystal storage.Store, atoms []*core.CausalAtom) *VersionVector {
	entries := make(map[string]core.Hash)
	for _, a := range atoms {
		if a.Path != "" {
			entries[a.Path] = a.Hash
		}
	}
	return &VersionVector{Entries: entries}
}
