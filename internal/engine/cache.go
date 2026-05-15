package engine

import (
	"container/list"
	"sync"
	"wavicle/internal/core"
)

type ProofCache struct {
	shards []*cacheShard
}

type cacheShard struct {
	mu       sync.RWMutex
	entries  map[core.Hash]*list.Element
	lru      *list.List
	capacity int
}

type cacheEntry struct {
	key   core.Hash
	proof *MaterializedProof
}

func NewProofCache(shards int, capacityPerShard int) *ProofCache {
	sc := make([]*cacheShard, shards)
	for i := 0; i < shards; i++ {
		sc[i] = &cacheShard{
			entries:  make(map[core.Hash]*list.Element),
			lru:      list.New(),
			capacity: capacityPerShard,
		}
	}
	return &ProofCache{shards: sc}
}

func (pc *ProofCache) Get(key core.Hash) (*MaterializedProof, bool) {
	s := pc.shards[int(key[0])%len(pc.shards)]
	s.mu.Lock()
	defer s.mu.Unlock()

	if elem, ok := s.entries[key]; ok {
		s.lru.MoveToFront(elem)
		return elem.Value.(*cacheEntry).proof, true
	}
	return nil, false
}

func (pc *ProofCache) Set(key core.Hash, proof *MaterializedProof) {
	s := pc.shards[int(key[0])%len(pc.shards)]
	s.mu.Lock()
	defer s.mu.Unlock()

	if elem, ok := s.entries[key]; ok {
		s.lru.MoveToFront(elem)
		elem.Value.(*cacheEntry).proof = proof
		return
	}

	if s.lru.Len() >= s.capacity {
		back := s.lru.Back()
		if back != nil {
			delete(s.entries, back.Value.(*cacheEntry).key)
			s.lru.Remove(back)
		}
	}

	elem := s.lru.PushFront(&cacheEntry{key: key, proof: proof})
	s.entries[key] = elem
}

func (pc *ProofCache) Delete(key core.Hash) {
	s := pc.shards[int(key[0])%len(pc.shards)]
	s.mu.Lock()
	defer s.mu.Unlock()

	if elem, ok := s.entries[key]; ok {
		delete(s.entries, key)
		s.lru.Remove(elem)
	}
}
