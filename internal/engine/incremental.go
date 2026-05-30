package engine

import (
	"fmt"
	"time"
	"wavicle/internal/core"
	"wavicle/internal/storage"
)

// crystalQuery is the subset of CausalCrystal used by ComputeProofMerkleRoot.
type crystalQuery interface {
	GetCurrentHash(path string) (core.Hash, bool)
}

type ReduceStats struct {
	ChangedPaths  int
	CacheHits     int
	CacheMisses   int
	DurationNanos int64
}

// ReduceIncremental brings a cached proof up-to-date with the current crystal frontier.
// It uses three levels of optimization to reach theoretical speed bounds:
// 1. FAST PATH 1 (O(m)): Version vector exact match. Return cached value immediately.
// 2. FAST PATH 2 (O(1)): Merkle root match. Return cached value immediately.
// 3. INCREMENTAL PATH (O(k log d)): k fields changed. Recompute only changed subtrees using zero-hash propagation.
func ReduceIncremental(
	proof *MaterializedProof,
	store storage.Store,
) (core.Value, error) {
	proof.mu.Lock()
	defer proof.mu.Unlock()

	proof.ReduceStats = ReduceStats{}
	start := time.Now()

	// In Phase 1, we assume the local storage is a CausalCrystal for these optimizations
	crystal, isCrystal := store.(*storage.CausalCrystal)
	if !isCrystal {
		// Fallback for non-crystal stores: full re-reduce
		newValue, err := reduceTree(proof.ProofTree, store, proof.NodeCache, proof)
		if err != nil {
			return nil, err
		}
		proof.Value = newValue
		return newValue, nil
	}

	// FAST PATH 1: Version vector exact match — O(m)
	if crystal.VerifyVersionVector(proof.VersionVector.Entries) {
		proof.AccessCount++
		proof.LastVerifiedAt = time.Now().UnixNano()
		return proof.Value, nil
	}

	// FAST PATH 2: Root node still valid — O(1) tree-level shortcut
	if proof.RootNode != nil && proof.RootNode.MerkleValid && !proof.RootNode.Dirty {
		proof.AccessCount++
		proof.LastVerifiedAt = time.Now().UnixNano()
		return proof.Value, nil
	}

	// INCREMENTAL PATH — find which paths changed, zero-alloc
	var changedBuf [64]string
	changedPathsList := crystal.FindChangedPaths(proof.VersionVector.Entries, changedBuf[:0])
	proof.ReduceStats.ChangedPaths = len(changedPathsList)

	var newValue core.Value
	var err error

	if proof.RootNode != nil {
		// FAST DIRTY PROPAGATION: O(k log d)
		proof.RootNode.ResetDirty()
		for _, path := range changedPathsList {
			if node, ok := proof.PathToNode[path]; ok {
				node.PropagateDirtyUp()
			}
		}

		newValue, err = reduceDirty(proof.RootNode, crystal, proof)
	} else {
		// Legacy slow path (fallback)
		newValue, err = reduceTree(proof.ProofTree, crystal, proof.NodeCache, proof)
	}

	if err != nil {
		return nil, fmt.Errorf("incremental reduction: %w", err)
	}

	proof.Value = newValue
	
	// Update version vector for next fast-path read
	for _, path := range changedPathsList {
		if h, ok := crystal.GetCurrentHash(path); ok {
			proof.VersionVector.Entries[path] = h
		}
	}
	// ProofNode tree is now fully reduced — all dirty flags are cleared.
	// FastPath2 will check RootNode.MerkleValid on the next read.
	
	proof.LastVerifiedAt = time.Now().UnixNano()
	proof.AccessCount++
	proof.ReduceStats.DurationNanos = time.Since(start).Nanoseconds()

	return proof.Value, nil
}

func reduceDirty(node *ProofNode, crystal *storage.CausalCrystal, proof *MaterializedProof) (core.Value, error) {
	// O(1) cache hit — zero hash, direct pointer
	if !node.Dirty && node.CachedValue != nil {
		proof.ReduceStats.CacheHits++
		return node.CachedValue, nil
	}

	proof.ReduceStats.CacheMisses++

	// If this node represents a leaf (atom), update its expression from the crystal
	if node.SourcePath != "" && node.Dirty {
		if currentAtom, ok := crystal.GetCurrent(node.SourcePath); ok {
			node.Expr = currentAtom.Expr
			node.SourceHash = currentAtom.Hash
		}
	}

	// Fast path: direct EConst — avoid full type switch
	if e, ok := node.Expr.(*core.EConst); ok {
		node.CachedValue = e.Value
		node.Dirty = false
		node.MerkleValid = false
		return e.Value, nil
	}

	var result core.Value
	var err error

	switch e := node.Expr.(type) {
	case *core.EFieldAccess:
		if len(node.Children) > 0 {
			sourceVal, err := reduceDirty(node.Children[0], crystal, proof)
			if err != nil {
				return nil, err
			}
			result, err = extractField(e.Field, sourceVal)
		} else {
			return nil, fmt.Errorf("EFieldAccess missing children in ProofNode")
		}

	case *core.ECompose:
		children := node.Children
		n := len(children)
		record := make(core.VRecord, n)
		// Unroll common small sizes to reduce loop overhead
		switch n {
		case 0:
		case 1:
			subVal, err := reduceDirty(children[0], crystal, proof)
			if err != nil {
				return nil, err
			}
			record[children[0].SourcePath] = subVal
		case 2:
			subVal0, err := reduceDirty(children[0], crystal, proof)
			if err != nil {
				return nil, err
			}
			subVal1, err := reduceDirty(children[1], crystal, proof)
			if err != nil {
				return nil, err
			}
			record[children[0].SourcePath] = subVal0
			record[children[1].SourcePath] = subVal1
		case 3:
			subVal0, err := reduceDirty(children[0], crystal, proof)
			if err != nil {
				return nil, err
			}
			subVal1, err := reduceDirty(children[1], crystal, proof)
			if err != nil {
				return nil, err
			}
			subVal2, err := reduceDirty(children[2], crystal, proof)
			if err != nil {
				return nil, err
			}
			record[children[0].SourcePath] = subVal0
			record[children[1].SourcePath] = subVal1
			record[children[2].SourcePath] = subVal2
		default:
			for _, childNode := range children {
				subVal, err := reduceDirty(childNode, crystal, proof)
				if err != nil {
					return nil, err
				}
				record[childNode.SourcePath] = subVal
			}
		}
		result = record

	case *core.EApply:
		if len(node.Children) >= 2 {
			funcVal, err := reduceDirty(node.Children[0], crystal, proof)
			if err != nil {
				return nil, err
			}
			argVal, err := reduceDirty(node.Children[1], crystal, proof)
			if err != nil {
				return nil, err
			}
			result, err = applyCombinator(funcVal, argVal)
		} else {
			return nil, fmt.Errorf("EApply missing children in ProofNode")
		}

	default:
		return nil, fmt.Errorf("unsupported or incomplete expr type for incremental: %T", node.Expr)
	}

	if err != nil {
		return nil, err
	}

	node.CachedValue = result
	node.Dirty = false
	node.MerkleValid = false
	return result, nil
}

func reduceTree(
	expr core.CombinatorExpr,
	crystal storage.Store,
	nodeCache map[core.Hash]core.Value,
	proof *MaterializedProof,
) (core.Value, error) {
	exprHash := expr.ExprHash()

	if cached, ok := nodeCache[exprHash]; ok {
		if proof != nil {
			proof.ReduceStats.CacheHits++
		}
		return cached, nil
	}

	if proof != nil {
		proof.ReduceStats.CacheMisses++
	}

	var result core.Value
	var err error

	switch e := expr.(type) {
	case *core.EConst:
		result = e.Value

	case *core.EFieldAccess:
		sourceVal, err := reduceTree(e.Source, crystal, nodeCache, proof)
		if err != nil {
			return nil, err
		}
		result, err = extractField(e.Field, sourceVal)

	case *core.ECompose:
		record := make(core.VRecord, len(e.Atoms))
		for _, origHash := range e.Atoms {
			// Find current atom for this path (frontier resolution)
			// This is the core novelty: resolving atoms against the current frontier
			// ensures causal consistency without manual cache invalidation.
			var atomToReduce *core.CausalAtom
			
			// Optimization: if we have the path, use it
			// Note: this assumes atoms in ECompose are atoms with paths.
			if atom, ok := crystal.GetAtom(origHash); ok && atom.Path != "" {
				if current, ok := crystal.GetCurrent(atom.Path); ok {
					atomToReduce = current
				} else {
					atomToReduce = atom
				}
			} else {
				atomToReduce, _ = crystal.GetAtom(origHash)
			}

			if atomToReduce == nil {
				return nil, fmt.Errorf("atom not found: %x", origHash)
			}

			subVal, err := reduceTree(atomToReduce.Expr, crystal, nodeCache, proof)
			if err != nil {
				return nil, err
			}
			record[atomToReduce.Path] = subVal
		}
		result = record

	case *core.EApply:
		funcVal, err := reduceTree(e.Func, crystal, nodeCache, proof)
		if err != nil {
			return nil, err
		}
		argVal, err := reduceTree(e.Arg, crystal, nodeCache, proof)
		if err != nil {
			return nil, err
		}
		result, err = applyCombinator(funcVal, argVal)

	default:
		return nil, fmt.Errorf("unknown expr type: %T", expr)
	}

	if err != nil {
		return nil, err
	}

	nodeCache[exprHash] = result
	return result, nil
}

func extractField(field string, v core.Value) (core.Value, error) {
	switch rec := v.(type) {
	case core.VRecord:
		if val, ok := rec[field]; ok {
			return val, nil
		}
		return core.VNull{}, nil
	default:
		return core.VNull{}, nil
	}
}

func applyCombinator(f, a core.Value) (core.Value, error) {
	// Standard SKI Combinator Calculus
	// I x = x
	// K x y = x
	// S x y z = x z (y z)

	switch v := f.(type) {
	case core.VString:
		s := string(v)
		switch s {
		case "I":
			// I a = a
			return a, nil
		case "K":
			// First argument of K: K a. Returns a closure/record representing (K a).
			return core.VRecord{"fn": core.VString("K_partial"), "arg1": a}, nil
		case "S":
			// First argument of S: S a. Returns a closure/record representing (S a).
			return core.VRecord{"fn": core.VString("S_partial1"), "arg1": a}, nil
		default:
			return core.VRecord{"fn": f, "arg": a}, nil
		}

	case core.VRecord:
		fn, ok := v["fn"].(core.VString)
		if !ok {
			return core.VRecord{"fn": f, "arg": a}, nil
		}

		switch string(fn) {
		case "K_partial":
			// (K a) b = a
			return v["arg1"], nil

		case "S_partial1":
			// (S a) b. Returns a closure/record representing (S a b).
			return core.VRecord{"fn": core.VString("S_partial2"), "arg1": v["arg1"], "arg2": a}, nil

		case "S_partial2":
			// ((S a) b) c = (a c) (b c)
			a_val := v["arg1"]
			b_val := v["arg2"]
			c_val := a

			// (a c)
			ac, err := applyCombinator(a_val, c_val)
			if err != nil {
				return nil, err
			}
			// (b c)
			bc, err := applyCombinator(b_val, c_val)
			if err != nil {
				return nil, err
			}
			// (a c) (b c)
			return applyCombinator(ac, bc)

		default:
			return core.VRecord{"fn": f, "arg": a}, nil
		}

	default:
		return core.VRecord{"fn": f, "arg": a}, nil
	}
}
