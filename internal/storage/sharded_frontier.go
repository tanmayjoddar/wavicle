package storage

import (
	"bufio"
	"container/list"
	"encoding/json"
	"os"
	"sync"
	"sync/atomic"
	"time"
	"wavicle/internal/core"
	"wavicle/internal/telemetry"

	"github.com/cespare/xxhash/v2"
)

const shardedFrontierShards = 32

// frontierShard is one independent partition of the sharded cache.
// Per-shard RWMutex means GETs on different keys never contend.
type frontierShard struct {
	mu         sync.RWMutex
	entries    map[string]*core.CausalAtom
	hashToPath map[core.Hash]string
	lruList    *list.List
	lruNodes   map[string]*list.Element
	bytes      int64
}

// ShardedFrontierCache is the GOAT production backend:
// 32 independent shards (xxhash key distribution), O(unique keys) memory,
// LRU eviction with global 90%->80% ceiling, TTL sweep, snapshot save/load.
type ShardedFrontierCache struct {
	shards         [shardedFrontierShards]*frontierShard
	clock          atomic.Uint64
	done           chan struct{}
	maxMemoryBytes int64
	currentBytes   atomic.Int64
}

func NewShardedFrontierCacheWithLimit(maxBytes int64) *ShardedFrontierCache {
	if maxBytes <= 0 {
		maxBytes = 4 * 1024 * 1024 * 1024
	}
	fc := &ShardedFrontierCache{
		done:           make(chan struct{}),
		maxMemoryBytes: maxBytes,
	}
	for i := range fc.shards {
		fc.shards[i] = &frontierShard{
			entries:    make(map[string]*core.CausalAtom),
			hashToPath: make(map[core.Hash]string),
			lruList:    list.New(),
			lruNodes:   make(map[string]*list.Element),
		}
	}
	telemetry.Get().RecordMemoryUsage(0, maxBytes)
	go fc.sweepExpired()
	return fc
}

func NewShardedFrontierCache() *ShardedFrontierCache {
	// FIX (goroutine leak): old code called NewFrontierCache() just to read the
	// env limit, orphaning a FrontierCache + its sweepExpired goroutine per call.
	limit := int64(4 * 1024 * 1024 * 1024)
	if v := os.Getenv("WAVICLE_MAX_MEMORY"); v != "" {
		if parsed, err := parseMemoryString(v); err == nil && parsed > 0 {
			limit = parsed
		}
	}
	return NewShardedFrontierCacheWithLimit(limit)
}

func shardFor(path string) uint32 {
	return uint32(xxhash.Sum64String(path) % shardedFrontierShards)
}

func (f *ShardedFrontierCache) shard(path string) *frontierShard {
	return f.shards[shardFor(path)]
}

func (f *ShardedFrontierCache) AppendAtom(expr core.CombinatorExpr, path string, parents []core.Hash, expiresAt time.Time) (core.Hash, error) {
	// Production hot path: mint the atom (SHA3) BEFORE taking the shard lock
	// so contended writers queue only for the map assignment (~15ns), not the
	// hash (~hundreds of ns). Clock is atomic, so issue order is well-defined
	// even though lock acquisition order is not — see guards below.
	atom := &core.CausalAtom{
		Expr:         expr,
		Path:         path,
		LogicalClock: f.clock.Add(1),
		PhysicalTime: time.Now(),
		ExpiresAt:    expiresAt,
	}
	atom.Hash = atom.ComputeHash()

	s := f.shard(path)
	s.mu.Lock()

	if oldAtom, ok := s.entries[path]; ok {
		// Guard 1 — clock order beats lock order: hashing outside the lock
		// means an older issue can acquire the lock second. If the resident
		// atom carries a higher clock, it is newer — discard, keep resident.
		if oldAtom.LogicalClock > atom.LogicalClock {
			h := oldAtom.Hash
			s.mu.Unlock()
			return h, nil
		}
		// Guard 2 — same-value dedup: identical SETs must not mint new hashes
		// and mass-invalidate dependent proofs. EXPIRE (same value, new
		// deadline) is NOT deduped — the deadline is part of freshness.
		if oldAtom.Hash != atom.Hash && sameAtomContent(oldAtom, atom) {
			if elem, exists := s.lruNodes[path]; exists {
				s.lruList.MoveToFront(elem)
			}
			h := oldAtom.Hash
			s.mu.Unlock()
			return h, nil
		}
		delete(s.hashToPath, oldAtom.Hash)
		delta := -estimateAtomBytes(path, oldAtom)
		s.bytes += delta
		f.currentBytes.Add(delta)
		if elem, exists := s.lruNodes[path]; exists {
			s.lruList.Remove(elem)
			delete(s.lruNodes, path)
		}
	}

	s.entries[path] = atom
	s.hashToPath[atom.Hash] = path
	ab := estimateAtomBytes(path, atom)
	s.bytes += ab
	f.currentBytes.Add(ab)

	elem := s.lruList.PushFront(path)
	s.lruNodes[path] = elem
	s.mu.Unlock()

	// Global ceiling enforcement: evict LRU from this shard first, then spill.
	if f.maxMemoryBytes > 0 && f.currentBytes.Load() >= int64(float64(f.maxMemoryBytes)*0.90) {
		f.evictToTarget(int64(float64(f.maxMemoryBytes) * 0.80))
	}

	telemetry.Get().RecordMemoryUsage(f.currentBytes.Load(), f.maxMemoryBytes)
	return atom.Hash, nil
}

// sameAtomContent reports whether two atoms carry identical values with
// identical deadlines. EConst payloads compare by serialized bytes; anything
// else always counts as changed (safe direction).
func sameAtomContent(a, b *core.CausalAtom) bool {
	if a == nil || b == nil || a.Expr == nil || b.Expr == nil {
		return false
	}
	if a.ExpiresAt.UnixNano() != b.ExpiresAt.UnixNano() {
		return false
	}
	ae, aok := a.Expr.(*core.EConst)
	be, bok := b.Expr.(*core.EConst)
	if !aok || !bok || ae.Value == nil || be.Value == nil {
		return false
	}
	ab, bb := ae.Value.Serialize(), be.Value.Serialize()
	if len(ab) != len(bb) {
		return false
	}
	for i := range ab {
		if ab[i] != bb[i] {
			return false
		}
	}
	return true
}

func (f *ShardedFrontierCache) evictToTarget(target int64) {
	// Pass 1: evict from most-loaded shards first (single scan, bounded).
	for f.currentBytes.Load() > target {
		// Find shard with most bytes.
		var victim *frontierShard
		var maxB int64 = -1
		for i := range f.shards {
			f.shards[i].mu.RLock()
			b := f.shards[i].bytes
			f.shards[i].mu.RUnlock()
			if b > maxB {
				maxB = b
				victim = f.shards[i]
			}
		}
		if victim == nil || maxB <= 0 {
			return
		}
		victim.mu.Lock()
		oldest := victim.lruList.Back()
		if oldest == nil {
			victim.mu.Unlock()
			return
		}
		evictPath := oldest.Value.(string)
		victim.lruList.Remove(oldest)
		delete(victim.lruNodes, evictPath)
		if evAtom, ok := victim.entries[evictPath]; ok {
			delete(victim.hashToPath, evAtom.Hash)
			delete(victim.entries, evictPath)
			delta := -estimateAtomBytes(evictPath, evAtom)
			victim.bytes += delta
			f.currentBytes.Add(delta)
			telemetry.Get().RecordMemoryEviction()
		}
		victim.mu.Unlock()
		// Safety: avoid infinite loop when target unreachable.
		if victim.lruList.Len() == 0 {
			empty := true
			for i := range f.shards {
				f.shards[i].mu.RLock()
				if f.shards[i].lruList.Len() > 0 {
					empty = false
				}
				f.shards[i].mu.RUnlock()
				if !empty {
					break
				}
			}
			if empty {
				return
			}
		}
	}
	telemetry.Get().RecordMemoryUsage(f.currentBytes.Load(), f.maxMemoryBytes)
}

func (f *ShardedFrontierCache) GetCurrent(path string) (*core.CausalAtom, bool) {
	s := f.shard(path)
	s.mu.Lock()
	defer s.mu.Unlock()
	atom, ok := s.entries[path]
	if !ok {
		return nil, false
	}
	if !atom.ExpiresAt.IsZero() && atom.ExpiresAt.Before(time.Now()) {
		return nil, false
	}
	if elem, exists := s.lruNodes[path]; exists {
		s.lruList.MoveToFront(elem)
	}
	cp := *atom
	return &cp, true
}

func (f *ShardedFrontierCache) GetAtom(hash core.Hash) (*core.CausalAtom, bool) {
	// hash->path index is per-shard; scan shards (rare path, only composition).
	for i := range f.shards {
		s := f.shards[i]
		s.mu.RLock()
		path, ok := s.hashToPath[hash]
		if !ok {
			s.mu.RUnlock()
			continue
		}
		atom, ok := s.entries[path]
		if !ok {
			s.mu.RUnlock()
			continue
		}
		expired := !atom.ExpiresAt.IsZero() && atom.ExpiresAt.Before(time.Now())
		if expired {
			s.mu.RUnlock()
			return nil, false
		}
		cp := *atom
		s.mu.RUnlock()
		return &cp, true
	}
	return nil, false
}

func (f *ShardedFrontierCache) GetParents(hash core.Hash) ([]core.Hash, bool) {
	return nil, false
}

func (f *ShardedFrontierCache) FrontierPaths() []string {
	now := time.Now()
	var out []string
	for i := range f.shards {
		s := f.shards[i]
		s.mu.RLock()
		for p, atom := range s.entries {
			if atom.ExpiresAt.IsZero() || atom.ExpiresAt.After(now) {
				out = append(out, p)
			}
		}
		s.mu.RUnlock()
	}
	return out
}

func (f *ShardedFrontierCache) VerifyVersionVector(entries map[string]core.Hash) bool {
	now := time.Now()
	for path, expected := range entries {
		s := f.shard(path)
		s.mu.RLock()
		atom, ok := s.entries[path]
		if !ok || atom.Hash != expected || (!atom.ExpiresAt.IsZero() && atom.ExpiresAt.Before(now)) {
			s.mu.RUnlock()
			return false
		}
		s.mu.RUnlock()
	}
	return true
}

func (f *ShardedFrontierCache) GetCurrentHash(path string) (core.Hash, bool) {
	s := f.shard(path)
	s.mu.RLock()
	defer s.mu.RUnlock()
	atom, ok := s.entries[path]
	if !ok {
		return core.Hash{}, false
	}
	if !atom.ExpiresAt.IsZero() && atom.ExpiresAt.Before(time.Now()) {
		return core.Hash{}, false
	}
	return atom.Hash, true
}

func (f *ShardedFrontierCache) FindChangedPaths(entries map[string]core.Hash, buf []string) []string {
	now := time.Now()
	buf = buf[:0]
	for path, expected := range entries {
		s := f.shard(path)
		s.mu.RLock()
		atom, ok := s.entries[path]
		if !ok || atom.Hash != expected || (!atom.ExpiresAt.IsZero() && atom.ExpiresAt.Before(now)) {
			buf = append(buf, path)
		}
		s.mu.RUnlock()
	}
	return buf
}

func (f *ShardedFrontierCache) CurrentBytes() int64 { return f.currentBytes.Load() }
func (f *ShardedFrontierCache) MaxMemoryBytes() int64 { return f.maxMemoryBytes }
func (f *ShardedFrontierCache) ShardCount() int      { return shardedFrontierShards }

func (f *ShardedFrontierCache) sweepExpired() {
	ticker := time.NewTicker(60 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-f.done:
			return
		case <-ticker.C:
			now := time.Now()
			var total int
			for i := range f.shards {
				s := f.shards[i]
				s.mu.Lock()
				for path, atom := range s.entries {
					if !atom.ExpiresAt.IsZero() && atom.ExpiresAt.Before(now) {
						delete(s.hashToPath, atom.Hash)
						delete(s.entries, path)
						if elem, exists := s.lruNodes[path]; exists {
							s.lruList.Remove(elem)
							delete(s.lruNodes, path)
						}
						delta := -estimateAtomBytes(path, atom)
						s.bytes += delta
						f.currentBytes.Add(delta)
						total++
					}
				}
				s.mu.Unlock()
			}
			if total > 0 {
				telemetry.Get().TTLEvictionsTotal.Add(float64(total))
				telemetry.Get().RecordMemoryUsage(f.currentBytes.Load(), f.maxMemoryBytes)
			}
		}
	}
}

// snapshotRecord is the on-disk warm-restart format (one JSON per line).
// Kind is string|int|float|bool|list|set|zset. Data holds the JSON payload:
// scalars store the raw value, list stores [{t,v}], set stores [members],
// zset stores {member:score}.
type snapshotRecord struct {
	Path      string    `json:"path"`
	Value     string    `json:"value"`
	ValueType string    `json:"value_type"`
	Data      string    `json:"data,omitempty"`
	ExpiresAt time.Time `json:"expires_at"`
	Clock     uint64    `json:"clock"`
}

type snapElem struct {
	T string `json:"t"`
	V string `json:"v"`
}

func encodeScalar(v core.Value) (val, typ string, ok bool) {
	switch t := v.(type) {
	case core.VString:
		return string(t), "string", true
	case core.VInt:
		return jsonNumber(int64(t)), "int", true
	case core.VFloat:
		return jsonNumber(float64(t)), "float", true
	case core.VBool:
		if bool(t) {
			return "1", "bool", true
		}
		return "0", "bool", true
	case core.VBytes:
		return jsonNumber([]byte(t)), "bytes", true
	}
	return "", "", false
}

func decodeScalar(typ, val string) (core.Value, bool) {
	switch typ {
	case "string":
		return core.VString(val), true
	case "int":
		var iv int64
		if err := json.Unmarshal([]byte(val), &iv); err != nil {
			return nil, false
		}
		return core.VInt(iv), true
	case "float":
		var fv float64
		if err := json.Unmarshal([]byte(val), &fv); err != nil {
			return nil, false
		}
		return core.VFloat(fv), true
	case "bool":
		return core.VBool(val == "1"), true
	case "bytes":
		var b []byte
		if err := json.Unmarshal([]byte(val), &b); err != nil {
			return nil, false
		}
		return core.VBytes(b), true
	}
	return nil, false
}

// SaveSnapshot persists latest atoms for warm restart, including lists,
// sets and zsets (stored as EConst VArray/VRecord). Tombstones skipped.
func (f *ShardedFrontierCache) SaveSnapshot(filePath string) error {
	tmp := filePath + ".tmp"
	fh, err := os.Create(tmp)
	if err != nil {
		return err
	}
	w := bufio.NewWriter(fh)
	enc := json.NewEncoder(w)
	for _, path := range f.FrontierPaths() {
		atom, ok := f.GetCurrent(path)
		if !ok || atom.Expr == nil {
			continue
		}
		ec, ok := atom.Expr.(*core.EConst)
		if !ok {
			continue
		}
		if _, isNull := ec.Value.(core.VNull); isNull {
			continue
		}
		rec := snapshotRecord{Path: path, ExpiresAt: atom.ExpiresAt, Clock: atom.LogicalClock}
		switch v := ec.Value.(type) {
		case core.VString, core.VInt, core.VFloat, core.VBool, core.VBytes:
			val, typ, _ := encodeScalar(v)
			rec.Value, rec.ValueType = val, typ
		case core.VArray:
			elems := make([]snapElem, 0, len(v))
			okAll := true
			for _, e := range v {
				if rec2, isRec := e.(core.VRecord); isRec {
					// Stream entry: preserve as single-elem record marker.
					b, _ := json.Marshal(rec2)
					elems = append(elems, snapElem{T: "record", V: string(b)})
					continue
				}
				sv, st, ok := encodeScalar(e)
				if !ok {
					okAll = false
					break
				}
				elems = append(elems, snapElem{T: st, V: sv})
			}
			if !okAll {
				continue
			}
			b, _ := json.Marshal(elems)
			rec.ValueType, rec.Data = "list", string(b)
		case core.VRecord:
			// Distinguish zset (all VFloat) vs set (all VInt(1)) vs generic.
			isZ, isS := true, true
			for _, sv := range v {
				if _, ok := sv.(core.VFloat); !ok {
					isZ = false
				}
				if iv, ok := sv.(core.VInt); !ok || iv != 1 {
					isS = false
				}
			}
			switch {
			case isZ && len(v) > 0:
				m := map[string]float64{}
				for k, sv := range v {
					m[k] = float64(sv.(core.VFloat))
				}
				b, _ := json.Marshal(m)
				rec.ValueType, rec.Data = "zset", string(b)
			case isS:
				members := make([]string, 0, len(v))
				for k := range v {
					members = append(members, k)
				}
				b, _ := json.Marshal(members)
				rec.ValueType, rec.Data = "set", string(b)
			default:
				continue
			}
		default:
			continue
		}
		if err := enc.Encode(rec); err != nil {
			fh.Close()
			return err
		}
	}
	if err := w.Flush(); err != nil {
		fh.Close()
		return err
	}
	if err := fh.Close(); err != nil {
		return err
	}
	return os.Rename(tmp, filePath)
}

func jsonNumber(v any) string {
	b, _ := json.Marshal(v)
	return string(b)
}

// LoadSnapshot restores a snapshot written by SaveSnapshot.
func (f *ShardedFrontierCache) LoadSnapshot(filePath string) (int, error) {
	fh, err := os.Open(filePath)
	if err != nil {
		return 0, err
	}
	defer fh.Close()
	n := 0
	sc := bufio.NewScanner(fh)
	sc.Buffer(make([]byte, 1024*1024), 1024*1024)
	for sc.Scan() {
		var rec snapshotRecord
		if err := json.Unmarshal(sc.Bytes(), &rec); err != nil {
			continue
		}
		if !rec.ExpiresAt.IsZero() && rec.ExpiresAt.Before(time.Now()) {
			continue
		}
		var val core.Value
		switch rec.ValueType {
		case "string", "int", "float", "bool", "bytes":
			v, ok := decodeScalar(rec.ValueType, rec.Value)
			if !ok {
				continue
			}
			val = v
		case "list":
			var elems []snapElem
			if err := json.Unmarshal([]byte(rec.Data), &elems); err != nil {
				continue
			}
			arr := make(core.VArray, 0, len(elems))
			okAll := true
			for _, e := range elems {
				if e.T == "record" {
					var m map[string]string
					if err := json.Unmarshal([]byte(e.V), &m); err != nil {
						okAll = false
						break
					}
					r := core.VRecord{}
					for k, vs := range m {
						r[k] = core.VString(vs)
					}
					arr = append(arr, r)
					continue
				}
				sv, ok := decodeScalar(e.T, e.V)
				if !ok {
					okAll = false
					break
				}
				arr = append(arr, sv)
			}
			if !okAll {
				continue
			}
			val = arr
		case "set":
			var members []string
			if err := json.Unmarshal([]byte(rec.Data), &members); err != nil {
				continue
			}
			r := core.VRecord{}
			for _, m := range members {
				r[m] = core.VInt(1)
			}
			val = r
		case "zset":
			var m map[string]float64
			if err := json.Unmarshal([]byte(rec.Data), &m); err != nil {
				continue
			}
			r := core.VRecord{}
			for k, s := range m {
				r[k] = core.VFloat(s)
			}
			val = r
		default:
			continue
		}
		if _, err := f.AppendAtom(core.NewEConst(val), rec.Path, nil, rec.ExpiresAt); err == nil {
			n++
		}
	}
	return n, sc.Err()
}

func (f *ShardedFrontierCache) Close() error {
	select {
	case <-f.done:
	default:
		close(f.done)
	}
	return nil
}
