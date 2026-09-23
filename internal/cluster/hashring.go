package cluster

import (
	"sort"
	"sync"

	"github.com/cespare/xxhash/v2"
)

// HashRing is a minimal consistent-hash ring with virtual nodes.
// Used to route keys to Wavicle nodes without a coordinator.
// This is the clustering stub the roadmap calls for: stateless routing
// first, Raft/Merkle-sync membership second.
type HashRing struct {
	mu       sync.RWMutex
	replicas int
	keys     []uint64
	nodes    map[uint64]string
}

func NewHashRing(replicas int) *HashRing {
	if replicas <= 0 {
		replicas = 128
	}
	return &HashRing{replicas: replicas, nodes: map[uint64]string{}}
}

func (h *HashRing) Add(node string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	for i := 0; i < h.replicas; i++ {
		// FIX: old key gen mixed rune() casts; use explicit unique vnode key.
		k := xxhash.Sum64String(node + "#vn#" + itoa(i))
		h.keys = append(h.keys, k)
		h.nodes[k] = node
	}
	sort.Slice(h.keys, func(i, j int) bool { return h.keys[i] < h.keys[j] })
}

func (h *HashRing) Remove(node string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	keep := h.keys[:0]
	for _, k := range h.keys {
		if h.nodes[k] != node {
			keep = append(keep, k)
		} else {
			delete(h.nodes, k)
		}
	}
	h.keys = keep
}

func (h *HashRing) Get(key string) string {
	h.mu.RLock()
	defer h.mu.RUnlock()
	if len(h.keys) == 0 {
		return ""
	}
	k := xxhash.Sum64String(key)
	i := sort.Search(len(h.keys), func(i int) bool { return h.keys[i] >= k })
	if i == len(h.keys) {
		i = 0
	}
	return h.nodes[h.keys[i]]
}

func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	var b [20]byte
	p := len(b)
	for i > 0 {
		p--
		b[p] = byte('0' + i%10)
		i /= 10
	}
	return string(b[p:])
}
