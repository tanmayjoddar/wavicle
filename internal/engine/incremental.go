package engine

import (
	"fmt"
	"time"
	"wavicle/internal/core"
	"wavicle/internal/storage"
)

type ReduceStats struct {
	ChangedPaths  int
	CacheHits     int
	CacheMisses   int
	DurationNanos int64
}

func ReduceIncremental(
	proof *MaterializedProof,
	crystal *storage.CausalCrystal,
) (core.Value, error) {
	proof.mu.Lock()
	defer proof.mu.Unlock()

	proof.ReduceStats = ReduceStats{}
	start := time.Now()

	// FAST PATH 1: Version vector exact match — O(m)
	if crystal.VerifyVersionVector(proof.VersionVector.Entries) {
		proof.AccessCount++
		proof.LastVerifiedAt = time.Now().UnixNano()
		return proof.Value, nil
	}

	// FAST PATH 2: Merkle root match — O(1)
	currentRoot := crystal.MerkleRootForPaths(proof.VersionVector.PathList())
	if currentRoot != [32]byte{} && currentRoot == proof.MerkleRoot {
		proof.AccessCount++
		proof.LastVerifiedAt = time.Now().UnixNano()
		return proof.Value, nil
	}

	// INCREMENTAL PATH
	// Find which paths changed (typically 1 of 50)
	changedPaths := crystal.FindChangedPaths(proof.VersionVector.Entries)
	proof.ReduceStats.ChangedPaths = len(changedPaths)

	// Re-reduce the proof tree.
	// Frontier resolution inside reduceTree ensures current atoms are used.
	// Node cache hits for unchanged subtrees (49/50).
	newValue, err := reduceTree(proof.ProofTree, crystal, proof.NodeCache, proof)
	if err != nil {
		return nil, fmt.Errorf("incremental reduction: %w", err)
	}

	_ = changedPaths

	proof.Value = newValue
	proof.ValueHash = core.HashValue(newValue)
	proof.VersionVector.Entries = crystal.CaptureVersionVector(proof.VersionVector.PathList())
	proof.MerkleRoot = crystal.MerkleRootForPaths(proof.VersionVector.PathList())
	proof.LastVerifiedAt = time.Now().UnixNano()
	proof.AccessCount++
	proof.ReduceStats.DurationNanos = time.Since(start).Nanoseconds()

	return newValue, nil
}

func reduceTree(
	expr core.CombinatorExpr,
	crystal *storage.CausalCrystal,
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
			origAtom, ok := crystal.GetAtomByHash(origHash)
			if !ok {
				return nil, fmt.Errorf("atom not found: %x", origHash)
			}

			atomToReduce := origAtom
			if currentAtom, ok := crystal.GetCurrent(origAtom.Path); ok && currentAtom.Hash != origHash {
				atomToReduce = currentAtom
			}

			subVal, err := reduceTree(atomToReduce.Expr, crystal, nodeCache, proof)
			if err != nil {
				return nil, err
			}
			record[origAtom.Path] = subVal
		}
		result = record
		return record, nil

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

	case *core.EEmbed:
		_ = e.ModelVersion
		result = core.VString(fmt.Sprintf("vector:%v", e.Vector[:4]))

	case *core.EResonate:
		result = core.VString(fmt.Sprintf("resonance:%d", len(e.Queries)))

	default:
		return nil, fmt.Errorf("unknown expr type: %T", expr)
	}

	if err != nil {
		return nil, err
	}

	nodeCache[exprHash] = result
	return result, nil
}

func extractField(field string, val core.Value) (core.Value, error) {
	if rec, ok := val.(core.VRecord); ok {
		if v, ok := rec[field]; ok {
			return v, nil
		}
	}
	return nil, fmt.Errorf("field not found: %s", field)
}

func mergeInto(target core.VRecord, val core.Value) {
	if rec, ok := val.(core.VRecord); ok {
		for k, v := range rec {
			target[k] = v
		}
		return
	}
	key := fmt.Sprintf("field_%d", len(target))
	target[key] = val
}

func applyCombinator(f, a core.Value) (core.Value, error) {
	return a, nil
}
