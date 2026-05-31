package storage

import (
	"sync"
	"wavicle/internal/core"
)

// ParentIndex stores the parent hashes for each atom hash.
type ParentIndex struct {
	mu      sync.RWMutex
	parents map[core.Hash][]core.Hash
}

func NewParentIndex() *ParentIndex {
	return &ParentIndex{
		parents: make(map[core.Hash][]core.Hash),
	}
}

func (p *ParentIndex) Get(hash core.Hash) ([]core.Hash, bool) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	parents, ok := p.parents[hash]
	return parents, ok
}

func (p *ParentIndex) Set(hash core.Hash, parents []core.Hash) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.parents[hash] = parents
}

func (p *ParentIndex) Delete(hash core.Hash) {
	p.mu.Lock()
	defer p.mu.Unlock()
	delete(p.parents, hash)
}
