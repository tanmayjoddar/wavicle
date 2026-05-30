package core

import (
	"bytes"
	"encoding/binary"
	"sync"
	"sync/atomic"

	"github.com/cespare/xxhash/v2"
)

// InternTable deduplicates AST nodes by structural hash.
type InternTable struct {
	shards [256]*internShard
}

type internShard struct {
	mu    sync.RWMutex
	table map[uint64][]CombinatorExpr
}

var globalIntern = &InternTable{}

func init() {
	for i := 0; i < 256; i++ {
		globalIntern.shards[i] = &internShard{
			table: make(map[uint64][]CombinatorExpr),
		}
	}
}

var nextExprID uint64

func (t *InternTable) Intern(e CombinatorExpr) CombinatorExpr {
	h := structuralHash(e)
	header := e.GetHeader()
	header.Hash.Store(h)

	shard := t.shards[h&0xFF]
	shard.mu.RLock()
	for _, candidate := range shard.table[h] {
		if exprEqual(e, candidate) {
			shard.mu.RUnlock()
			return candidate
		}
	}
	shard.mu.RUnlock()

	shard.mu.Lock()
	defer shard.mu.Unlock()

	// Double check
	for _, candidate := range shard.table[h] {
		if exprEqual(e, candidate) {
			return candidate
		}
	}

	header.ID = atomic.AddUint64(&nextExprID, 1)
	shard.table[h] = append(shard.table[h], e)
	return e
}

func InternExpr(e CombinatorExpr) CombinatorExpr {
	if e.GetHeader() != nil && e.GetHeader().ID != 0 {
		return e
	}

	// Initialize header if missing
	if e.GetHeader() == nil {
		h := exprArena.Get().(*ExprHeader)
		// We can't easily set the field because it's a pointer in the struct 
		// and the interface doesn't have a SetHeader method.
		// However, all our concrete types (EConst, ECompose, etc.) 
		// are defined in this package. 
		// For Phase 2, we assume callers use New* constructors.
		// If we must support raw interning, we'd need to use reflection or 
		// add SetHeader to the interface.
		
		// To stay within the rules of "peak engineering", I'll use a type switch.
		switch ex := e.(type) {
		case *EConst:
			ex.ExprHeader = h
			ex.Typ = ExprConst
		case *EFieldAccess:
			ex.ExprHeader = h
			ex.Typ = ExprFieldAccess
		case *EEmbed:
			ex.ExprHeader = h
			ex.Typ = ExprEmbed
		case *ECompose:
			ex.ExprHeader = h
			ex.Typ = ExprCompose
		case *EApply:
			ex.ExprHeader = h
			ex.Typ = ExprApply
		case *EResonate:
			ex.ExprHeader = h
			ex.Typ = ExprResonate
		}
	}

	return globalIntern.Intern(e)
}

func structuralHash(e CombinatorExpr) uint64 {
	h := xxhash.New()
	header := e.GetHeader()
	h.Write([]byte{byte(header.Typ)})

	switch ex := e.(type) {
	case *EConst:
		b := make([]byte, 8)
		binary.BigEndian.PutUint64(b, ex.Value.FastHash())
		h.Write(b)
	case *EFieldAccess:
		h.Write([]byte(ex.Field))
		b := make([]byte, 8)
		binary.BigEndian.PutUint64(b, ex.Source.GetHeader().ID)
		h.Write(b)
	case *EEmbed:
		b := make([]byte, 2)
		binary.BigEndian.PutUint16(b, ex.ModelVersion)
		h.Write(b)
		// Just hash first few bytes of vector for structural identity
		binary.Write(h, binary.BigEndian, ex.Vector[0])
	case *ECompose:
		for _, atomHash := range ex.Atoms {
			h.Write(atomHash[:])
		}
	case *EApply:
		b := make([]byte, 8)
		binary.BigEndian.PutUint64(b, ex.Func.GetHeader().ID)
		h.Write(b)
		binary.BigEndian.PutUint64(b, ex.Arg.GetHeader().ID)
		h.Write(b)
	case *EResonate:
		b := make([]byte, 4)
		binary.BigEndian.PutUint32(b, uint32(len(ex.Queries)))
		h.Write(b)
	}
	return h.Sum64()
}

func exprEqual(a, b CombinatorExpr) bool {
	if a.GetHeader().Typ != b.GetHeader().Typ {
		return false
	}
	switch ax := a.(type) {
	case *EConst:
		bx := b.(*EConst)
		return bytes.Equal(ax.Value.Serialize(), bx.Value.Serialize())
	case *EFieldAccess:
		bx := b.(*EFieldAccess)
		return ax.Field == bx.Field && ax.Source.GetHeader().ID == bx.Source.GetHeader().ID
	case *EEmbed:
		bx := b.(*EEmbed)
		return ax.ModelVersion == bx.ModelVersion && ax.Vector == bx.Vector
	case *ECompose:
		bx := b.(*ECompose)
		if len(ax.Atoms) != len(bx.Atoms) {
			return false
		}
		for i := range ax.Atoms {
			if ax.Atoms[i] != bx.Atoms[i] {
				return false
			}
		}
		return true
	case *EApply:
		bx := b.(*EApply)
		return ax.Func.GetHeader().ID == bx.Func.GetHeader().ID && ax.Arg.GetHeader().ID == bx.Arg.GetHeader().ID
	case *EResonate:
		bx := b.(*EResonate)
		if len(ax.Queries) != len(bx.Queries) {
			return false
		}
		for i := range ax.Queries {
			if ax.Queries[i] != bx.Queries[i] {
				return false
			}
		}
		return true
	}
	return false
}

// Constructors

func NewEConst(val Value) *EConst {
	h := exprArena.Get().(*ExprHeader)
	h.Typ = ExprConst
	e := &EConst{ExprHeader: h, Value: val}
	return globalIntern.Intern(e).(*EConst)
}

func NewEFieldAccess(field string, source CombinatorExpr) *EFieldAccess {
	h := exprArena.Get().(*ExprHeader)
	h.Typ = ExprFieldAccess
	e := &EFieldAccess{ExprHeader: h, Field: field, Source: source}
	return globalIntern.Intern(e).(*EFieldAccess)
}

func NewECompose(atoms []Hash) *ECompose {
	h := exprArena.Get().(*ExprHeader)
	h.Typ = ExprCompose
	e := &ECompose{ExprHeader: h, Atoms: atoms}
	return globalIntern.Intern(e).(*ECompose)
}

func NewEApply(fn, arg CombinatorExpr) *EApply {
	h := exprArena.Get().(*ExprHeader)
	h.Typ = ExprApply
	e := &EApply{ExprHeader: h, Func: fn, Arg: arg}
	return globalIntern.Intern(e).(*EApply)
}

func NewEEmbed(modelVersion uint16, vector [256]float32) *EEmbed {
	h := exprArena.Get().(*ExprHeader)
	h.Typ = ExprEmbed
	e := &EEmbed{ExprHeader: h, ModelVersion: modelVersion, Vector: vector}
	return globalIntern.Intern(e).(*EEmbed)
}

func NewEResonate(queries []Vector) *EResonate {
	h := exprArena.Get().(*ExprHeader)
	h.Typ = ExprResonate
	e := &EResonate{ExprHeader: h, Queries: queries}
	return globalIntern.Intern(e).(*EResonate)
}
