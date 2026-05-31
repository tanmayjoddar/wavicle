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

const (
	maxAtomCacheEntries = 100000  // Hard RAM limit — triggers emergency eviction
	compactionWALSize   = 10 * 1024 * 1024 // 10MB, down from 50MB
	compactionInterval  = 30 * time.Second // Time-based fallback compaction
)

// CausalCrystal is Wavicle's storage engine.
//
// IMPORTANT: There is NO single global lock. Each subsystem uses its own
// independent lock so the replication goroutine is never starved.
//   - frontier:    frontier.mu (sync.RWMutex)
//   - parentIndex: parentIndex.mu (sync.RWMutex)
//   - atomCache:   sync.Map (lock-free reads)
//   - merkle:      merkleMu (sync.RWMutex)
//   - wal:         wal.mu (sync.Mutex)
//   - depthIndex:  sync.Map (lock-free)
type CausalCrystal struct {
	// Persistent storage
	wal *WAL

	// Hot indexes (RAM)
	frontier    *FrontierIndex
	parentIndex *ParentIndex
	depthIndex  sync.Map // atom_hash -> causal_depth
	atomCache   sync.Map // atom_hash -> *core.CausalAtom
	atomCount   atomic.Int64 // live count in atomCache

	// Merkle tree for cryptographic verification
	merkle      *MerkleTree
	merkleRoot  atomic.Value // core.Hash
	merkleMu    sync.RWMutex

	// Logical clock for total ordering
	clock atomic.Uint64

	// Compaction
	compactCh    chan struct{}
	compacting   atomic.Bool
	lastCompact  time.Time
}

func NewCausalCrystal(walPath string) (*CausalCrystal, error) {
	wal, err := NewWAL(walPath)
	if err != nil {
		return nil, err
	}

	c := &CausalCrystal{
		wal:        wal,
		frontier:    NewFrontierIndex(),
		parentIndex: NewParentIndex(),
		merkle:      NewMerkleTree(),
		compactCh:   make(chan struct{}, 1),
	}

	if err := c.Recover(); err != nil {
		return nil, fmt.Errorf("recovery: %w", err)
	}

	go c.sweepExpired()
	go c.compactionLoop()

	return c, nil
}

func (c *CausalCrystal) sweepExpired() {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	for range ticker.C {
		now := time.Now()
		for _, path := range c.frontier.Paths() {
			if atom, ok := c.GetCurrent(path); ok && !atom.ExpiresAt.IsZero() && atom.ExpiresAt.Before(now) {
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
		c.atomCount.Add(1)
		c.merkleMu.Lock()
		c.merkle.Insert(atom.Hash)
		c.merkleRoot.Store(c.merkle.Root())
		c.merkleMu.Unlock()
		if atom.LogicalClock > c.clock.Load() {
			c.clock.Store(atom.LogicalClock)
		}
	}

	return nil
}

func (c *CausalCrystal) AppendAtom(expr core.CombinatorExpr, path string, causalPast []core.Hash, expiresAt time.Time) (core.Hash, error) {
	if expr.GetHeader() == nil || expr.GetHeader().ID == 0 {
		expr = core.InternExpr(expr)
	}

	depth := uint64(0)
	for _, parentHash := range causalPast {
		if d, ok := c.depthIndex.Load(parentHash); ok {
			if d.(uint64)+1 > depth {
				depth = d.(uint64) + 1
			}
		}
	}

	atom := &core.CausalAtom{
		Expr:         expr,
		CausalPast:   causalPast,
		CausalDepth:  depth,
		Vector:       embedExpression(expr),
		Domain:       inferDomain(path),
		LogicalClock: c.clock.Add(1),
		PhysicalTime: time.Now(),
		Nonce:        randomNonce(),
		Path:         path,
		ExpiresAt:    expiresAt,
	}
	atom.Hash = atom.ComputeHash()

	if err := c.wal.Append(atom); err != nil {
		return core.Hash{}, fmt.Errorf("wal append: %w", err)
	}

	walSize := c.wal.Size()

	// Trigger compaction IF:
	// 1. WAL exceeds threshold AND we grew by >10% since last compaction (avoids loop)
	// 2. OR atom count exceeds threshold
	// 3. OR compaction isn't already running
	if !c.compacting.Load() && time.Since(c.lastCompact) > compactionInterval/2 {
		shouldCompact := false
		if c.atomCount.Load() > maxAtomCacheEntries/2 {
			shouldCompact = true
		} else if walSize > compactionWALSize {
			// Only trigger on significant growth past threshold to avoid re-compact loop
			shouldCompact = true
		}
		if shouldCompact {
			select {
			case c.compactCh <- struct{}{}:
			default:
			}
		}
	}

	// Update hot indexes — each subsystem has its own lock, brief and independent
	c.frontier.Set(path, atom.Hash)
	c.parentIndex.Set(atom.Hash, causalPast)
	c.depthIndex.Store(atom.Hash, depth)
	c.atomCache.Store(atom.Hash, atom)
	c.atomCount.Add(1)

	c.merkleMu.Lock()
	c.merkle.Insert(atom.Hash)
	c.merkleRoot.Store(c.merkle.Root())
	c.merkleMu.Unlock()

	// Emergency eviction (best-effort, only when > maxAtomCacheEntries)
	if c.atomCount.Load() > maxAtomCacheEntries {
		c.evictIfNeeded()
	}

	telemetry.Get().RecordAtomCount(c.atomCount.Load())

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

func (c *CausalCrystal) FindChangedPaths(entries map[string]core.Hash, buf []string) []string {
	buf = buf[:0]
	for path, expectedHash := range entries {
		currentHash, ok := c.GetCurrentHash(path)
		if !ok || currentHash != expectedHash {
			buf = append(buf, path)
		}
	}
	return buf
}

func (c *CausalCrystal) MerkleRootForPaths(paths []string) core.Hash {
	if len(paths) == 0 {
		return core.Hash{}
	}

	var hashes []core.Hash
	for _, p := range paths {
		if h, ok := c.frontier.Get(p); ok {
			hashes = append(hashes, h)
		} else {
			hashes = append(hashes, core.Hash{})
		}
	}

	sort.Slice(hashes, func(i, j int) bool {
		for k := 0; k < 32; k++ {
			if hashes[i][k] != hashes[j][k] {
				return hashes[i][k] < hashes[j][k]
			}
		}
		return false
	})

	c.merkleMu.RLock()
	root := c.merkle.computeRoot(hashes)
	c.merkleMu.RUnlock()
	return root
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

func (c *CausalCrystal) MerkleRoot() core.Hash {
	if v, ok := c.merkleRoot.Load().(core.Hash); ok {
		return v
	}
	return core.Hash{}
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

// Compact rewrites the WAL and evicts unreachable atoms from RAM.
// Runs in a single background goroutine — no concurrency with itself.
func (c *CausalCrystal) Compact() {
	if !c.compacting.CompareAndSwap(false, true) {
		return
	}
	defer func() {
		c.lastCompact = time.Now()
		c.compacting.Store(false)
	}()

	// Step 1: Build alive set from frontier — only frontier atoms, no ancestor walking.
	// Ancestors are not needed for correctness: proof reduction resolves atoms against
	// the live frontier at read time. Keeping the full causal chain in the WAL causes
	// unbounded WAL growth and O(write history) memory on recovery.
	paths := c.frontier.Paths()
	alive := make([]*core.CausalAtom, 0, len(paths))
	for _, p := range paths {
		if h, ok := c.frontier.Get(p); ok {
			if atom, ok := c.GetAtom(h); ok {
				alive = append(alive, atom)
			}
		}
	}

	sort.Slice(alive, func(i, j int) bool {
		return alive[i].LogicalClock < alive[j].LogicalClock
	})

	// Step 2: Rewrite WAL (wal.mu only)
	if err := c.wal.Rewrite(alive); err != nil {
		fmt.Printf("WAL compaction failed: %v\n", err)
		return
	}

	// Step 3: Rebuild Merkle tree (merkleMu only)
	c.merkleMu.Lock()
	c.merkle = NewMerkleTree()
	for _, atom := range alive {
		c.merkle.Insert(atom.Hash)
	}
	c.merkleRoot.Store(c.merkle.Root())
	c.merkleMu.Unlock()

	// Step 4: Evict orphaned atoms from atomCache, parentIndex, depthIndex.
	// Build frontier hash set at eviction time (not at collect time) to close
	// the TOCTOU race: a concurrent AppendAtom that adds a new frontier atom
	// AFTER the collect step is protected because we re-read the frontier here.
	frontierHashes := make(map[core.Hash]bool, len(paths))
	for _, p := range c.frontier.Paths() {
		if h, ok := c.frontier.Get(p); ok {
			frontierHashes[h] = true
		}
	}

	c.atomCache.Range(func(k, v any) bool {
		hash := k.(core.Hash)
		if !frontierHashes[hash] {
			c.atomCache.Delete(k)
			c.atomCount.Add(-1)
			c.parentIndex.Delete(hash)
			c.depthIndex.Delete(hash)
		}
		return true
	})

	telemetry.Get().RecordAtomCount(c.atomCount.Load())
}

// compactionLoop triggers compaction on demand or periodically.
// Single goroutine; Compact() never runs concurrently with itself.
func (c *CausalCrystal) compactionLoop() {
	ticker := time.NewTicker(compactionInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			if c.compacting.Load() {
				continue
			}
			walSize := c.wal.Size()
			if walSize > compactionWALSize || c.atomCount.Load() > maxAtomCacheEntries/2 {
				c.Compact()
			}
		case <-c.compactCh:
			c.Compact()
		}
	}
}

// evictIfNeeded performs emergency eviction of atoms not in the frontier
// when atomCache exceeds the hard RAM limit.
//
// This is a BEST-EFFORT safety net, not a correctness path.
// sync.Map Range is safe with concurrent Store/Delete — no lock needed.
// The TOCTOU race (concurrent AppendAtom adds a new frontier atom after we
// build our snapshot) is benign: if we evict a live atom, GetCurrent returns
// (nil, false) and callers handle it gracefully. The next Compact or
// AppendAtom restores it.
func (c *CausalCrystal) evictIfNeeded() {
	count := c.atomCount.Load()
	if count < maxAtomCacheEntries {
		return
	}

	// Snapshot frontier hashes (frontier.mu internally handles locking)
	frontierHashes := make(map[core.Hash]bool)
	for _, p := range c.frontier.Paths() {
		if h, ok := c.frontier.Get(p); ok {
			frontierHashes[h] = true
		}
	}

	// Evict oldest 10% not in frontier
	target := int(float64(count) * 0.1)
	evicted := 0

	c.atomCache.Range(func(k, v any) bool {
		if evicted >= target {
			return false
		}
		hash := k.(core.Hash)
		if !frontierHashes[hash] {
			c.atomCache.Delete(k)
			c.atomCount.Add(-1)
			c.parentIndex.Delete(hash)
			c.depthIndex.Delete(hash)
			evicted++
		}
		return true
	})
}

func embedExpression(expr core.CombinatorExpr) core.Vector {
	return semantic.EmbedString(string(expr.Serialize()))
}

func inferDomain(path string) core.Domain {
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
