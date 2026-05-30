package storage

import (
	"crypto/rand"
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"time"
	"wavicle/internal/core"
	"wavicle/internal/semantic"
	"wavicle/internal/telemetry"
)

// CausalCrystal is Wavicle's storage engine.
//
// LOCK ORDER GLOBAL: proof.mu -> crystal.mu -> frontier.mu / parentIndex.mu
// crystal.mu is the top-level lock protecting Merkle tree access.
// Sub-structures (FrontierIndex, ParentIndex) have their own locks.
// NEVER acquire proof.mu while holding crystal.mu.
type CausalCrystal struct {
	// Persistent storage
	wal *WAL

	// Hot indexes (RAM)
	frontier    *FrontierIndex
	parentIndex *ParentIndex
	depthIndex  sync.Map // atom_hash -> causal_depth
	atomCache   sync.Map // atom_hash -> *core.CausalAtom

	// Merkle tree for cryptographic verification
	merkle     *MerkleTree
	merkleRoot atomic.Value // core.Hash

	// Logical clock for total ordering
	clock atomic.Uint64

	mu sync.RWMutex
}

func NewCausalCrystal(walPath string) (*CausalCrystal, error) {
	wal, err := NewWAL(walPath)
	if err != nil {
		return nil, err
	}

	c := &CausalCrystal{
		wal:         wal,
		frontier:    NewFrontierIndex(),
		parentIndex: NewParentIndex(),
		merkle:      NewMerkleTree(),
	}

	if err := c.Recover(); err != nil {
		return nil, fmt.Errorf("recovery: %w", err)
	}

	go c.sweepExpired()

	return c, nil
}

func (c *CausalCrystal) sweepExpired() {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for range ticker.C {
		now := time.Now()
		for _, path := range c.frontier.Paths() {
			if atom, ok := c.GetCurrent(path); ok && !atom.ExpiresAt.IsZero() && atom.ExpiresAt.Before(now) {
				// Append tombstone
				c.AppendAtom(&core.EConst{Value: core.VNull{}}, path, []core.Hash{atom.Hash}, time.Time{})
			}
		}
	}
}

func (c *CausalCrystal) Recover() error {
	atoms, err := c.wal.ReadAll()
	if err != nil {
		return err
	}

	for _, atom := range atoms {
		c.frontier.Set(atom.Path, atom.Hash)
		c.parentIndex.Set(atom.Hash, atom.CausalPast)
		c.depthIndex.Store(atom.Hash, atom.CausalDepth)
		c.atomCache.Store(atom.Hash, atom)
		c.merkle.Insert(atom.Hash)
		c.merkleRoot.Store(c.merkle.Root())
		if atom.LogicalClock > c.clock.Load() {
			c.clock.Store(atom.LogicalClock)
		}
	}

	return nil
}

func (c *CausalCrystal) AppendAtom(expr core.CombinatorExpr, path string, causalPast []core.Hash, expiresAt time.Time) (core.Hash, error) {
	// Step 1: Determine causal depth
	depth := uint64(0)
	for _, parentHash := range causalPast {
		if d, ok := c.depthIndex.Load(parentHash); ok {
			if d.(uint64)+1 > depth {
				depth = d.(uint64) + 1
			}
		}
	}

	// Step 2: Build atom
	atom := &core.CausalAtom{
		Expr:         expr,
		CausalPast:   causalPast,
		CausalDepth:  depth,
		Vector:       embedExpression(expr),
		Domain:       inferDomain(path),
		LogicalClock: c.clock.Add(1),
		PhysicalTime: time.Now(),
		Nonce:        randomNonce(),
		Path:         path, // Populate Path for recovery
		ExpiresAt:    expiresAt,
	}
	atom.Hash = atom.ComputeHash()

	// Step 3: fdatasync to WAL
	if err := c.wal.Append(atom); err != nil {
		return core.Hash{}, fmt.Errorf("wal append: %w", err)
	}

	walSize := c.wal.Size()
	telemetry.Get().RecordWALSize(walSize)

	// Trigger background compaction if log is too large (> 50MB)
	if walSize > 50*1024*1024 {
		go c.Compact()
	}

	// Step 4: Update hot indexes
	// LOCK ORDER: crystal.mu -> frontier.mu
	c.mu.Lock()
	c.frontier.Set(path, atom.Hash)
	c.parentIndex.Set(atom.Hash, causalPast)
	c.depthIndex.Store(atom.Hash, depth)
	c.atomCache.Store(atom.Hash, atom)

	// Step 5: Update Merkle tree
	c.merkle.Insert(atom.Hash)
	c.merkleRoot.Store(c.merkle.Root())
	c.mu.Unlock()

	return atom.Hash, nil
}

func (c *CausalCrystal) GetCurrent(path string) (*core.CausalAtom, bool) {
	hash, ok := c.frontier.Get(path)
	if !ok {
		return nil, false
	}
	return c.GetAtom(hash)
}

func (c *CausalCrystal) GetCurrentHash(path string) (core.Hash, bool) {
	return c.frontier.Get(path)
}

func (c *CausalCrystal) GetAtom(hash core.Hash) (*core.CausalAtom, bool) {
	if atom, ok := c.atomCache.Load(hash); ok {
		return atom.(*core.CausalAtom), true
	}
	return nil, false
}

func (c *CausalCrystal) VerifyVersionVector(entries map[string]core.Hash) bool {
	for path, expectedHash := range entries {
		currentHash, ok := c.GetCurrentHash(path)
		if !ok || currentHash != expectedHash {
			return false
		}
	}
	return true
}

func (c *CausalCrystal) FindChangedPaths(entries map[string]core.Hash) []string {
	var changed []string
	for path, expectedHash := range entries {
		currentHash, ok := c.GetCurrentHash(path)
		if !ok || currentHash != expectedHash {
			changed = append(changed, path)
		}
	}
	return changed
}

func (c *CausalCrystal) MerkleRootForPaths(paths []string) core.Hash {
	if len(paths) == 0 {
		return core.Hash{}
	}

	c.mu.RLock()
	defer c.mu.RUnlock()

	var hashes []core.Hash
	for _, p := range paths {
		if h, ok := c.frontier.Get(p); ok {
			hashes = append(hashes, h)
		} else {
			// If a path doesn't exist, we use a zero hash as a placeholder
			hashes = append(hashes, core.Hash{})
		}
	}

	// Sort hashes for stable Merkle root regardless of path order
	sort.Slice(hashes, func(i, j int) bool {
		for k := 0; k < 32; k++ {
			if hashes[i][k] != hashes[j][k] {
				return hashes[i][k] < hashes[j][k]
			}
		}
		return false
	})

	return c.merkle.computeRoot(hashes)
}

func (c *CausalCrystal) CaptureVersionVector(paths []string) map[string]core.Hash {
	res := make(map[string]core.Hash)
	for _, p := range paths {
		if h, ok := c.frontier.Get(p); ok {
			res[p] = h
		}
	}
	return res
}

func (c *CausalCrystal) GetParents(hash core.Hash) ([]core.Hash, bool) {
	return c.parentIndex.Get(hash)
}

func (c *CausalCrystal) FrontierPaths() []string {
	return c.frontier.Paths()
}

func (c *CausalCrystal) Close() error {
	return c.wal.Close()
}

// Compact rewrites the WAL, keeping only atoms reachable from the current Frontier.
func (c *CausalCrystal) Compact() {
	c.mu.Lock()
	defer c.mu.Unlock()

	paths := c.frontier.Paths()
	var alive []*core.CausalAtom

	// Gather the closure of all atoms currently in the frontier
	seen := make(map[core.Hash]bool)
	var collect func(h core.Hash)
	collect = func(h core.Hash) {
		if seen[h] {
			return
		}
		seen[h] = true
		if atom, ok := c.GetAtom(h); ok {
			alive = append(alive, atom)
			for _, p := range atom.CausalPast {
				collect(p)
			}
		}
	}

	for _, p := range paths {
		if h, ok := c.frontier.Get(p); ok {
			collect(h)
		}
	}

	// Sort by logical clock for stable recovery
	sort.Slice(alive, func(i, j int) bool {
		return alive[i].LogicalClock < alive[j].LogicalClock
	})

	if err := c.wal.Rewrite(alive); err != nil {
		fmt.Printf("WAL compaction failed: %v\n", err)
	} else {
		telemetry.Get().RecordCompaction()
	}
}

// Helpers

func embedExpression(expr core.CombinatorExpr) core.Vector {
	return semantic.EmbedString(string(expr.Serialize()))
}

func inferDomain(path string) core.Domain {
	// Domain inference from path prefix
	if path == "" {
		return core.DomainSystem
	}
	domain := path[0]
	switch {
	case domain == 'u' || domain == 'U':
		return core.DomainUser
	case domain == 'o' || domain == 'O':
		return core.DomainOrder
	case domain == 'p' || domain == 'P':
		return core.DomainProduct
	case domain == 's' || domain == 'S':
		if len(path) > 3 && (path[:3] == "ses" || path[:3] == "SES" || path[:3] == "Ses") {
			return core.DomainSession
		}
		return core.DomainSystem
	case domain == 'a' || domain == 'A':
		return core.DomainAnalytics
	case domain == 'c' || domain == 'C':
		return core.DomainConfig
	default:
		return core.DomainSystem
	}
}

func randomNonce() [16]byte {
	var n [16]byte
	rand.Read(n[:])
	return n
}
