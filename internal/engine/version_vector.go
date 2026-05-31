package engine

import (
	"sort"
	"wavicle/internal/core"
	"wavicle/internal/storage"
)

func ComputeVersionMerkleRoot(vv *VersionVector, crystal storage.Store) core.Hash {
	paths := make([]string, 0, len(vv.Entries))
	for p := range vv.Entries {
		paths = append(paths, p)
	}
	sort.Strings(paths)

	var hashes []core.Hash
	for _, p := range paths {
		h := vv.Entries[p]
		if h.IsZero() {
			if cur, ok := crystal.GetCurrent(p); ok {
				h = cur.Hash
			}
		}
		hashes = append(hashes, h)
	}

	if len(hashes) == 0 {
		return core.Hash{}
	}

	return computeMerkle(hashes)
}

func computeMerkle(hashes []core.Hash) core.Hash {
	if len(hashes) == 0 {
		return core.Hash{}
	}
	if len(hashes) == 1 {
		return hashes[0]
	}

	var next []core.Hash
	for i := 0; i < len(hashes); i += 2 {
		if i+1 < len(hashes) {
			h := core.HashBytes(append(hashes[i][:], hashes[i+1][:]...))
			next = append(next, h)
		} else {
			next = append(next, hashes[i])
		}
	}
	return computeMerkle(next)
}
