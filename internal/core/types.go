package core

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"math"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cespare/xxhash/v2"
	"golang.org/x/crypto/sha3"
)

// Hash is a 32-byte SHA3-256 content address.
type Hash [32]byte

func (h Hash) String() string {
	return fmt.Sprintf("%x", h[:])
}

func (h Hash) IsZero() bool {
	return h == Hash{}
}

// Domain categorizes atoms for sharding and policy routing
type Domain uint8

const (
	DomainUser Domain = iota
	DomainOrder
	DomainProduct
	DomainSession
	DomainConfig
	DomainAnalytics
	DomainSystem
)

// ObservationMode controls the precision/speed tradeoff per query
type ObservationMode uint8

const (
	ModeDeductive ObservationMode = iota
	ModeInductive
	ModeAbductive
	ModeIntuitive
)

func (m ObservationMode) Strictness() int {
	return int(m)
}

// Vector is a 256-dimensional semantic embedding
type Vector [256]float32

// Value is the universal sum type for all atom values
type Value interface {
	valueTag()
	Serialize() []byte
	Type() string
	FastHash() uint64
}

type VNull struct{}

func (v VNull) valueTag()          {}
func (v VNull) Type() string       { return "null" }
func (v VNull) Serialize() []byte  { return []byte{0} }
func (v VNull) FastHash() uint64   { return 0 }

type VBool bool

func (v VBool) valueTag()          {}
func (v VBool) Type() string       { return "bool" }
func (v VBool) Serialize() []byte {
	if v {
		return []byte{1}
	}
	return []byte{2}
}
func (v VBool) FastHash() uint64 {
	if v {
		return 1
	}
	return 2
}

type VInt int64

func (v VInt) valueTag()          {}
func (v VInt) Type() string       { return "int" }
func (v VInt) Serialize() []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, uint64(v))
	return b
}
func (v VInt) FastHash() uint64 { return uint64(v) }

type VFloat float64

func (v VFloat) valueTag()          {}
func (v VFloat) Type() string       { return "float" }
func (v VFloat) Serialize() []byte {
	return []byte(fmt.Sprintf("%f", v))
}
func (v VFloat) FastHash() uint64 { return math.Float64bits(float64(v)) }

type VString string

func (v VString) valueTag()          {}
func (v VString) Type() string       { return "string" }
func (v VString) Serialize() []byte  { return []byte(v) }
func (v VString) FastHash() uint64   { return xxhash.Sum64String(string(v)) }

type VBytes []byte

func (v VBytes) valueTag()          {}
func (v VBytes) Type() string       { return "bytes" }
func (v VBytes) Serialize() []byte  { return v }
func (v VBytes) FastHash() uint64   { return xxhash.Sum64(v) }

type VArray []Value

func (v VArray) valueTag()      {}
func (v VArray) Type() string   { return "array" }
func (v VArray) Serialize() []byte {
	var b []byte
	for _, val := range v {
		b = append(b, val.Serialize()...)
	}
	return b
}
func (v VArray) FastHash() uint64 {
	h := xxhash.New()
	for _, val := range v {
		binary.Write(h, binary.BigEndian, val.FastHash())
	}
	return h.Sum64()
}

type VRecord map[string]Value

func (v VRecord) valueTag()      {}
func (v VRecord) Type() string   { return "record" }
func (v VRecord) Serialize() []byte {
	keys := make([]string, 0, len(v))
	for k := range v {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var b []byte
	for _, k := range keys {
		b = append(b, []byte(k)...)
		b = append(b, v[k].Serialize()...)
	}
	return b
}
func (v VRecord) FastHash() uint64 {
	h := xxhash.New()
	keys := make([]string, 0, len(v))
	for k := range v {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		h.WriteString(k)
		binary.Write(h, binary.BigEndian, v[k].FastHash())
	}
	return h.Sum64()
}

// CombinatorExpr is the stratified typed algebra.
type CombinatorExpr interface {
	exprTag()
	Level() int
	ExprHash() Hash      // Tier 3: Cryptographic identity (slow)
	FastHash() uint64    // Tier 1/2: Structural identity (fast)
	Serialize() []byte
	GetHeader() *ExprHeader
}

// ExprType identifies the node type for fast structural hashing
type ExprType uint8

const (
	ExprConst ExprType = iota
	ExprFieldAccess
	ExprEmbed
	ExprCompose
	ExprApply
	ExprResonate
)

// ExprHeader: 24 bytes total, immutable after interning.
type ExprHeader struct {
	Typ   ExprType      // 1 byte
	_     [7]byte       // padding
	ID    uint64        // monotonic ID from global intern table
	Hash  atomic.Uint64 // cached xxHash (0 = unset)
	Depth uint16        // tree depth for complexity tracking
	_     [6]byte       // padding
}

// exprArena reduces GC pressure for short-lived nodes before interning
var exprArena = sync.Pool{
	New: func() any {
		return &ExprHeader{}
	},
}

type EConst struct {
	*ExprHeader
	Value Value
}

func (e *EConst) exprTag()    {}
func (e *EConst) Level() int { return 0 }
func (e *EConst) Serialize() []byte {
	return append([]byte{0}, e.Value.Serialize()...)
}
func (e *EConst) GetHeader() *ExprHeader { return e.ExprHeader }

func (e *EConst) ExprHash() Hash {
	return sha3.Sum256(e.Serialize())
}

func (e *EConst) FastHash() uint64 {
	return e.Hash.Load()
}

type EFieldAccess struct {
	*ExprHeader
	Field  string
	Source CombinatorExpr
}

func (e *EFieldAccess) exprTag()    {}
func (e *EFieldAccess) Level() int { return 1 }
func (e *EFieldAccess) Serialize() []byte {
	return append(append([]byte{1}, []byte(e.Field)...), e.Source.Serialize()...)
}
func (e *EFieldAccess) GetHeader() *ExprHeader { return e.ExprHeader }

func (e *EFieldAccess) ExprHash() Hash {
	return sha3.Sum256(e.Serialize())
}

func (e *EFieldAccess) FastHash() uint64 {
	return e.Hash.Load()
}

type EEmbed struct {
	*ExprHeader
	ModelVersion uint16
	Vector       Vector
}

func (e *EEmbed) exprTag()    {}
func (e *EEmbed) Level() int { return 1 }
func (e *EEmbed) Serialize() []byte {
	// Level(1) + ModelVersion(2 bytes) + Vector(1024 bytes)
	b := make([]byte, 0, 1+2+1024)
	b = append(b, 2)
	binary.BigEndian.PutUint16(b[1:3], e.ModelVersion)
	for _, v := range e.Vector {
		bits := math.Float32bits(v)
		b = binary.BigEndian.AppendUint32(b, bits)
	}
	return b
}
func (e *EEmbed) GetHeader() *ExprHeader { return e.ExprHeader }

func (e *EEmbed) ExprHash() Hash {
	return sha3.Sum256(e.Serialize())
}

func (e *EEmbed) FastHash() uint64 {
	return e.Hash.Load()
}

type ECompose struct {
	*ExprHeader
	Atoms []Hash
}

func (e *ECompose) exprTag()    {}
func (e *ECompose) Level() int { return 2 }
func (e *ECompose) Serialize() []byte {
	b := []byte{3}
	for _, h := range e.Atoms {
		b = append(b, h[:]...)
	}
	return b
}
func (e *ECompose) GetHeader() *ExprHeader { return e.ExprHeader }

func (e *ECompose) ExprHash() Hash {
	return sha3.Sum256(e.Serialize())
}

func (e *ECompose) FastHash() uint64 {
	return e.Hash.Load()
}

type EApply struct {
	*ExprHeader
	Func CombinatorExpr
	Arg  CombinatorExpr
}

func (e *EApply) exprTag()    {}
func (e *EApply) Level() int { return 2 }
func (e *EApply) Serialize() []byte {
	return append(append([]byte{4}, e.Func.Serialize()...), e.Arg.Serialize()...)
}
func (e *EApply) GetHeader() *ExprHeader { return e.ExprHeader }

func (e *EApply) ExprHash() Hash {
	return sha3.Sum256(e.Serialize())
}

func (e *EApply) FastHash() uint64 {
	return e.Hash.Load()
}

type EResonate struct {
	*ExprHeader
	Queries []Vector
}

func (e *EResonate) exprTag()    {}
func (e *EResonate) Level() int { return 3 }
func (e *EResonate) Serialize() []byte {
	// Level(3) tag + query vector count(4 bytes) + vectors
	b := []byte{5, 0, 0, 0, 0} // tag + 4 bytes for count
	binary.BigEndian.PutUint32(b[1:5], uint32(len(e.Queries)))
	for _, q := range e.Queries {
		for _, v := range q {
			bits := math.Float32bits(v)
			b = binary.BigEndian.AppendUint32(b, bits)
		}
	}
	return b
}
func (e *EResonate) GetHeader() *ExprHeader { return e.ExprHeader }

func (e *EResonate) ExprHash() Hash {
	return sha3.Sum256(e.Serialize())
}

func (e *EResonate) FastHash() uint64 {
	return e.Hash.Load()
}

// CausalAtom is the immutable, content-addressed unit of truth.
type CausalAtom struct {
	Hash         Hash
	Expr         CombinatorExpr
	CausalPast   []Hash
	CausalDepth  uint64
	Vector       Vector
	Domain       Domain
	LogicalClock uint64
	PhysicalTime time.Time
	Nonce        [16]byte
	BranchID     [16]byte
	Path         string    // Added for recovery
	ExpiresAt    time.Time // When the atom expires
}

func (a *CausalAtom) ComputeHash() Hash {
	h := sha3.New256()
	h.Write(a.Expr.Serialize())
	binary.Write(h, binary.BigEndian, a.LogicalClock)
	binary.Write(h, binary.BigEndian, uint64(a.PhysicalTime.UnixNano()))
	binary.Write(h, binary.BigEndian, uint64(a.ExpiresAt.UnixNano()))
	h.Write(a.Nonce[:])
	h.Write(a.BranchID[:])
	binary.Write(h, binary.BigEndian, uint32(len(a.CausalPast)))
	for _, p := range a.CausalPast {
		h.Write(p[:])
	}
	var hash Hash
	copy(hash[:], h.Sum(nil))
	return hash
}

func HashValue(v Value) Hash {
	return sha3.Sum256(v.Serialize())
}

func HashBytes(data []byte) Hash {
	return sha3.Sum256(data)
}

// UnmarshalJSON implements custom unmarshalling to handle the Expr interface.
func (a *CausalAtom) UnmarshalJSON(data []byte) error {
	type Alias CausalAtom
	aux := &struct {
		Expr json.RawMessage `json:"Expr"`
		*Alias
	}{
		Alias: (*Alias)(a),
	}
	if err := json.Unmarshal(data, &aux); err != nil {
		return err
	}

	// Try to determine the concrete type of Expr
	// For now, only EConst is fully supported in SET commands
	var eConst EConst
	if err := json.Unmarshal(aux.Expr, &eConst); err == nil && eConst.Value != nil {
		a.Expr = NewEConst(eConst.Value)
		return nil
	}

	// Add more types as needed for recovery
	return fmt.Errorf("unknown or unsupported Expr type in JSON")
}

// EConst needs special handling for Value interface too
func (e *EConst) UnmarshalJSON(data []byte) error {
	var aux struct {
		Value interface{} `json:"Value"`
	}
	if err := json.Unmarshal(data, &aux); err != nil {
		return err
	}
	// For simplicity in the prototype, treat all values as VString
	if s, ok := aux.Value.(string); ok {
		e.Value = VString(s)
	} else {
		// Fallback for non-string values if any
		e.Value = VNull{}
	}
	return nil
}
