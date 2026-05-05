package storage

import (
	"crypto/sha3"
	"wavicle/internal/core"
)

type MerkleTree struct {
	leaves []core.Hash
}

func NewMerkleTree() *MerkleTree {
	return &MerkleTree{}
}

func (m *MerkleTree) Insert(hash core.Hash) {
	m.leaves = append(m.leaves, hash)
}

func (m *MerkleTree) Root() core.Hash {
	if len(m.leaves) == 0 {
		return core.Hash{}
	}
	return m.computeRoot(m.leaves)
}

func (m *MerkleTree) computeRoot(hashes []core.Hash) core.Hash {
	if len(hashes) == 1 {
		return hashes[0]
	}

	var nextLevel []core.Hash
	for i := 0; i < len(hashes); i += 2 {
		if i+1 < len(hashes) {
			h := sha3.New256()
			h.Write(hashes[i][:])
			h.Write(hashes[i+1][:])
			var res core.Hash
			copy(res[:], h.Sum(nil))
			nextLevel = append(nextLevel, res)
		} else {
			nextLevel = append(nextLevel, hashes[i])
		}
	}
	return m.computeRoot(nextLevel)
}
