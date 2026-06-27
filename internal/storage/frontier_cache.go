package storage

import (
    "sync"
    "sync/atomic"
    "time"
    "wavicle/internal/core"
	"wavicle/internal/telemetry"
)

// FrontierCache is the production-mode local cache.
// Memory is O(unique keys) not O(write history).
// No WAL. No compaction. No ancestry. PostgreSQL is the source of truth.
type FrontierCache struct {
    mu         sync.RWMutex
    entries    map[string]*core.CausalAtom
    hashToPath map[core.Hash]string
    clock      atomic.Uint64
    done       chan struct{}
}

func NewFrontierCache() *FrontierCache {
    fc := &FrontierCache{
        entries:    make(map[string]*core.CausalAtom),
        hashToPath: make(map[core.Hash]string),
        done:       make(chan struct{}),
    }
    go fc.sweepExpired()
    return fc
}

func (f *FrontierCache) sweepExpired() {
    ticker := time.NewTicker(60 * time.Second)
    defer ticker.Stop()
    for {
        select {
        case <-f.done:
            return
        case <-ticker.C:
            now := time.Now()
            var expired []struct {
                path string
                hash core.Hash
            }

            f.mu.RLock()
            for path, atom := range f.entries {
                if !atom.ExpiresAt.IsZero() && atom.ExpiresAt.Before(now) {
                    expired = append(expired, struct {
                        path string
                        hash core.Hash
                    }{path, atom.Hash})
                }
            }
            f.mu.RUnlock()

            if len(expired) == 0 {
                continue
            }

            f.mu.Lock()
            for _, e := range expired {
                // Re-check: a concurrent AppendAtom may have updated this path
                // since we released RLock. Only delete if the current atom is
                // still the same expired one.
                if current, stillHere := f.entries[e.path]; stillHere && current.Hash == e.hash {
                    delete(f.entries, e.path)
                    delete(f.hashToPath, e.hash)
                }
            }
            telemetry.Get().RecordAtomCount(int64(len(f.entries)))
            f.mu.Unlock()

            telemetry.Get().TTLEvictionsTotal.Add(float64(len(expired)))
        }
    }
}

func (f *FrontierCache) AppendAtom(expr core.CombinatorExpr, path string, parents []core.Hash, expiresAt time.Time) (core.Hash, error) {
    f.mu.Lock()
    defer f.mu.Unlock()

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
    atom, ok := f.entries[path]
    if !ok {
        f.mu.RUnlock()
        return nil, false
    }
    expired := !atom.ExpiresAt.IsZero() && atom.ExpiresAt.Before(time.Now())
    if expired {
        f.mu.RUnlock()
        return nil, false
    }
    result := *atom
    f.mu.RUnlock()
    return &result, true
}

func (f *FrontierCache) GetAtom(hash core.Hash) (*core.CausalAtom, bool) {
    f.mu.RLock()
    defer f.mu.RUnlock()
    path, ok := f.hashToPath[hash]
    if !ok {
        return nil, false
    }
    atom, ok := f.entries[path]
    if !ok {
        return nil, false
    }
    if !atom.ExpiresAt.IsZero() && atom.ExpiresAt.Before(time.Now()) {
        return nil, false
    }
    result := *atom
    return &result, true
}

func (f *FrontierCache) GetParents(hash core.Hash) ([]core.Hash, bool) {
    return nil, false
}

func (f *FrontierCache) FrontierPaths() []string {
    f.mu.RLock()
    defer f.mu.RUnlock()
    now := time.Now()
    paths := make([]string, 0, len(f.entries))
    for p, atom := range f.entries {
        if atom.ExpiresAt.IsZero() || atom.ExpiresAt.After(now) {
            paths = append(paths, p)
        }
    }
    return paths
}

func (f *FrontierCache) VerifyVersionVector(entries map[string]core.Hash) bool {
    f.mu.RLock()
    defer f.mu.RUnlock()
    now := time.Now()
    for path, expectedHash := range entries {
        atom, ok := f.entries[path]
        if !ok || atom.Hash != expectedHash {
            return false
        }
        if !atom.ExpiresAt.IsZero() && atom.ExpiresAt.Before(now) {
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
    if !atom.ExpiresAt.IsZero() && atom.ExpiresAt.Before(time.Now()) {
        return core.Hash{}, false
    }
    return atom.Hash, true
}

func (f *FrontierCache) FindChangedPaths(entries map[string]core.Hash, buf []string) []string {
	f.mu.RLock()
	defer f.mu.RUnlock()
	now := time.Now()
	buf = buf[:0]
	for path, expectedHash := range entries {
		atom, ok := f.entries[path]
		if !ok || atom.Hash != expectedHash {
			buf = append(buf, path)
			continue
		}
		if !atom.ExpiresAt.IsZero() && atom.ExpiresAt.Before(now) {
			buf = append(buf, path)
		}
	}
	return buf
}

func (f *FrontierCache) Close() error {
    close(f.done)
    f.mu.Lock()
    f.entries = nil
    f.hashToPath = nil
    f.mu.Unlock()
    return nil
}
