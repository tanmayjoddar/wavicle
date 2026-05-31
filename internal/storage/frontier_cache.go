// internal/storage/frontier_cache.go
package storage

import (
	"sync"
	"sync/atomic"
	"time"
	"wavicle/internal/core"
	"wavicle/internal/telemetry"
)
// FrontierCache is the production-mode local cache.
// Replaces CausalCrystal when a primary database is configured.
// Memory is O(unique keys) not O(write history).
// No WAL. No compaction. No ancestry. PostgreSQL is the source of truth.
// FrontierCache is the production-mode local cache.
type FrontierCache struct {
	mu         sync.RWMutex
	entries    map[string]*core.CausalAtom
	hashToPath map[core.Hash]string // For O(1) GetAtom
	clock      atomic.Uint64
}

func NewFrontierCache() *FrontierCache {
	fc := &FrontierCache{
		entries:    make(map[string]*core.CausalAtom),
		hashToPath: make(map[core.Hash]string),
	}
	go fc.sweepExpired()
	return fc
}

func (f *FrontierCache) sweepExpired() {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for range ticker.C {
		now := time.Now()
		var evictedCount int
		f.mu.Lock()
		for path, atom := range f.entries {
			if !atom.ExpiresAt.IsZero() && atom.ExpiresAt.Before(now) {
				delete(f.entries, path)
				delete(f.hashToPath, atom.Hash)
				evictedCount++
			}
		}
		telemetry.Get().RecordAtomCount(int64(len(f.entries)))
		f.mu.Unlock()

		if evictedCount > 0 {
			telemetry.Get().TTLEvictionsTotal.Add(float64(evictedCount))
		}
	}
}

func (f *FrontierCache) AppendAtom(expr core.CombinatorExpr, path string, parents []core.Hash, expiresAt time.Time) (core.Hash, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	// Remove old entry if it exists, to clean up hashToPath
	if oldAtom, ok := f.entries[path]; ok {
		delete(f.hashToPath, oldAtom.Hash)
	}

	atom := &core.CausalAtom{
		Expr:         expr,
		Path:         path,
		LogicalClock: f.clock.Add(1),
		PhysicalTime: time.Now(),
		ExpiresAt:    expiresAt,
	}
	atom.Hash = atom.ComputeHash()

	f.entries[path] = atom
	f.hashToPath[atom.Hash] = path
	
	telemetry.Get().RecordAtomCount(int64(len(f.entries)))
	
	return atom.Hash, nil
}

func (f *FrontierCache) GetCurrent(path string) (*core.CausalAtom, bool) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	atom, ok := f.entries[path]
	return atom, ok
}

func (f *FrontierCache) GetAtom(hash core.Hash) (*core.CausalAtom, bool) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	path, ok := f.hashToPath[hash]
	if !ok {
		return nil, false
	}
	atom, ok := f.entries[path]
	return atom, ok
}

func (f *FrontierCache) GetParents(hash core.Hash) ([]core.Hash, bool) {
    return nil, false // No ancestry in production mode
}

func (f *FrontierCache) FrontierPaths() []string {
    f.mu.RLock()
    defer f.mu.RUnlock()
    paths := make([]string, 0, len(f.entries))
    for p := range f.entries {
        paths = append(paths, p)
    }
    return paths
}

func (f *FrontierCache) VerifyVersionVector(entries map[string]core.Hash) bool {
    f.mu.RLock()
    defer f.mu.RUnlock()
    for path, expectedHash := range entries {
        atom, ok := f.entries[path]
        if !ok || atom.Hash != expectedHash {
            return false
        }
    }
    return true
}

func (f *FrontierCache) GetCurrentHash(path string) (core.Hash, bool) {
    f.mu.RLock()
    defer f.mu.RUnlock()
    atom, ok := f.entries[path]
    if !ok {
        return core.Hash{}, false
    }
    return atom.Hash, true
}

func (f *FrontierCache) FindChangedPaths(entries map[string]core.Hash, buf []string) []string {
	f.mu.RLock()
	defer f.mu.RUnlock()
	buf = buf[:0]
	for path, expectedHash := range entries {
		atom, ok := f.entries[path]
		if !ok || atom.Hash != expectedHash {
			buf = append(buf, path)
		}
	}
	return buf
}

func (f *FrontierCache) Close() error {
    f.mu.Lock()
    defer f.mu.Unlock()
    f.entries = nil
    return nil
}
