package storage

import (
	"container/list"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"wavicle/internal/core"
	"wavicle/internal/telemetry"
)

// FrontierCache is the production-mode local cache.
// Memory is O(unique keys) not O(write history).
// No WAL. No compaction. No ancestry. PostgreSQL is the source of truth.
// Enforces a configurable memory ceiling with LRU eviction when memory exceeds 90%.
type FrontierCache struct {
	mu             sync.RWMutex
	entries        map[string]*core.CausalAtom
	hashToPath     map[core.Hash]string
	clock          atomic.Uint64
	done           chan struct{}
	maxMemoryBytes int64
	currentBytes   int64
	lruList        *list.List
	lruNodes       map[string]*list.Element
}

func NewFrontierCacheWithLimit(maxBytes int64) *FrontierCache {
	if maxBytes <= 0 {
		maxBytes = 4 * 1024 * 1024 * 1024 // 4GB default
	}
	fc := &FrontierCache{
		entries:        make(map[string]*core.CausalAtom),
		hashToPath:     make(map[core.Hash]string),
		done:           make(chan struct{}),
		maxMemoryBytes: maxBytes,
		lruList:        list.New(),
		lruNodes:       make(map[string]*list.Element),
	}
	telemetry.Get().RecordMemoryUsage(0, maxBytes)
	go fc.sweepExpired()
	return fc
}

func NewFrontierCache() *FrontierCache {
	limit := int64(4 * 1024 * 1024 * 1024)
	if v := os.Getenv("WAVICLE_MAX_MEMORY"); v != "" {
		if parsed, err := parseMemoryString(v); err == nil && parsed > 0 {
			limit = parsed
		}
	}
	return NewFrontierCacheWithLimit(limit)
}

func parseMemoryString(s string) (int64, error) {
	s = strings.TrimSpace(strings.ToUpper(s))
	if s == "" {
		return 0, nil
	}
	multiplier := int64(1)
	valStr := s
	if strings.HasSuffix(s, "TB") || strings.HasSuffix(s, "T") {
		multiplier = 1024 * 1024 * 1024 * 1024
		valStr = strings.TrimRight(s, "TB")
	} else if strings.HasSuffix(s, "GB") || strings.HasSuffix(s, "G") {
		multiplier = 1024 * 1024 * 1024
		valStr = strings.TrimRight(s, "GB")
	} else if strings.HasSuffix(s, "MB") || strings.HasSuffix(s, "M") {
		multiplier = 1024 * 1024
		valStr = strings.TrimRight(s, "MB")
	} else if strings.HasSuffix(s, "KB") || strings.HasSuffix(s, "K") {
		multiplier = 1024
		valStr = strings.TrimRight(s, "KB")
	} else if strings.HasSuffix(s, "B") {
		valStr = strings.TrimRight(s, "B")
	}
	valStr = strings.TrimSpace(valStr)
	val, err := strconv.ParseInt(valStr, 10, 64)
	if err != nil {
		return 0, err
	}
	return val * multiplier, nil
}

func estimateAtomBytes(path string, atom *core.CausalAtom) int64 {
	if atom == nil {
		return 0
	}
	// Struct overhead:
	// CausalAtom struct + map bucket overhead + list element: ~220 bytes
	size := int64(220 + len(path))
	if atom.Expr != nil {
		if ec, ok := atom.Expr.(*core.EConst); ok {
			switch v := ec.Value.(type) {
			case core.VString:
				size += int64(16 + len(string(v)))
			case core.VBytes:
				size += int64(24 + len([]byte(v)))
			default:
				size += 16
			}
		} else {
			size += 48
		}
	}
	return size
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
					f.currentBytes -= estimateAtomBytes(e.path, current)
					if elem, exists := f.lruNodes[e.path]; exists {
						f.lruList.Remove(elem)
						delete(f.lruNodes, e.path)
					}
					delete(f.entries, e.path)
					delete(f.hashToPath, e.hash)
				}
			}
			telemetry.Get().RecordAtomCount(int64(len(f.entries)))
			telemetry.Get().RecordMemoryUsage(f.currentBytes, f.maxMemoryBytes)
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
		f.currentBytes -= estimateAtomBytes(path, oldAtom)
		if elem, exists := f.lruNodes[path]; exists {
			f.lruList.Remove(elem)
			delete(f.lruNodes, path)
		}
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
	atomBytes := estimateAtomBytes(path, atom)
	f.currentBytes += atomBytes

	elem := f.lruList.PushFront(path)
	f.lruNodes[path] = elem

	// Memory ceiling enforcement: when usage reaches >= 90% of max,
	// evict LRU entries until down to <= 80% to avoid per-write eviction ping-pong.
	if f.maxMemoryBytes > 0 && f.currentBytes >= int64(float64(f.maxMemoryBytes)*0.90) {
		target := int64(float64(f.maxMemoryBytes) * 0.80)
		for f.currentBytes > target && f.lruList.Len() > 0 {
			oldest := f.lruList.Back()
			if oldest == nil {
				break
			}
			evictPath := oldest.Value.(string)
			f.lruList.Remove(oldest)
			delete(f.lruNodes, evictPath)
			if evAtom, ok := f.entries[evictPath]; ok {
				delete(f.hashToPath, evAtom.Hash)
				delete(f.entries, evictPath)
				f.currentBytes -= estimateAtomBytes(evictPath, evAtom)
				telemetry.Get().RecordMemoryEviction()
			}
		}
	}

	telemetry.Get().RecordAtomCount(int64(len(f.entries)))
	telemetry.Get().RecordMemoryUsage(f.currentBytes, f.maxMemoryBytes)

	return atom.Hash, nil
}

func (f *FrontierCache) GetCurrent(path string) (*core.CausalAtom, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()

	atom, ok := f.entries[path]
	if !ok {
		return nil, false
	}
	expired := !atom.ExpiresAt.IsZero() && atom.ExpiresAt.Before(time.Now())
	if expired {
		return nil, false
	}
	if elem, exists := f.lruNodes[path]; exists {
		f.lruList.MoveToFront(elem)
	}
	result := *atom
	return &result, true
}

func (f *FrontierCache) CurrentBytes() int64 {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.currentBytes
}

func (f *FrontierCache) MaxMemoryBytes() int64 {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.maxMemoryBytes
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
    f.lruNodes = nil
    f.lruList = nil
    f.currentBytes = 0
    f.mu.Unlock()
    return nil
}
