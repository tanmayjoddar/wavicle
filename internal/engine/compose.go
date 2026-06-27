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
		NodeCache:     make(map[core.Hash]core.Value),
		VersionVector: captureVersionVector(store, relevantAtoms),
	}

	// Extract the local store (works with CausalCrystal, FrontierCache, or WriteThroughStore)
	var local storage.Store
	switch s := store.(type) {
	case *storage.WriteThroughStore:
		local = s.Local
	default:
		local = s
	}

	// Include all proof tree paths in the version vector so that field-level
	// changes (e.g., poison writes to a compose child) are detected by
	// ReduceIncremental's FAST PATH 1 version-vector check.
	for _, node := range pathToNode {
		if node.SourcePath != "" {
			if _, exists := proof.VersionVector.Entries[node.SourcePath]; !exists {
				if h, ok := local.GetCurrentHash(node.SourcePath); ok {
					proof.VersionVector.Entries[node.SourcePath] = h
				}
			}
		}
	}

	proof.MerkleRoot = ComputeVersionMerkleRoot(proof.VersionVector, local)
	val, _ := reduceDirty(proof.RootNode, local, proof)
	proof.Value = val
	proof.ValueHash = core.HashValue(val)

	// Compute Merkle root so FastPath2 is armed for the first read
	if proof.RootNode != nil {
		proof.RootNode.ComputeMerkleRoot()
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
		if len(e.Atoms) == 0 {
			break
		}
		children := make([]*ProofNode, 0, len(e.Atoms))
		for _, h := range e.Atoms {
			if childAtom, ok := store.GetAtom(h); ok {
				if child := buildProofNode(store, childAtom.Path, node, pathIndex); child != nil {
					children = append(children, child)
				}
			}
		}
		node.Children = children
	}

	return node
}

func gatherCausalClosure(crystal storage.Store, path string) ([]*core.CausalAtom, error) {
	atom, ok := crystal.GetCurrent(path)
	if !ok {
		return nil, fmt.Errorf("path not found: %s", path)
	}
	// Only return the current frontier atom.
	// Historical ancestors are not needed for proof reduction —
	// reduceTree re-resolves all atoms against the live frontier at read time.
	// Loading the full causal chain causes O(write history) memory growth
	// which is unbounded under sustained load.
	return []*core.CausalAtom{atom}, nil
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
			// Always use the current frontier hash, not the historical atom hash.
			// This ensures the version vector reflects live state.
			if current, ok := crystal.GetCurrent(a.Path); ok {
				entries[a.Path] = current.Hash
			} else {
				entries[a.Path] = a.Hash
			}
		}
	}
	return &VersionVector{Entries: entries}
}
