package storage

import (
	"sync"
	"time"
	"wavicle/internal/core"
)

// Store is the interface for storage engines (Crystal, Postgres, etc.)
type Store interface {
	// AppendAtom adds a new atom to the storage.
	AppendAtom(expr core.CombinatorExpr, path string, parents []core.Hash, expiresAt time.Time) (core.Hash, error)

	// GetCurrent retrieves the latest atom for a given path.
	GetCurrent(path string) (*core.CausalAtom, bool)

	// GetParents returns the parent hashes for a given atom hash.
	GetParents(hash core.Hash) ([]core.Hash, bool)

	// GetAtom retrieves a specific atom by its hash.
	GetAtom(hash core.Hash) (*core.CausalAtom, bool)

	// FrontierPaths returns all active paths in the system.
	FrontierPaths() []string

	// Close releases storage resources.
	Close() error
}

// WriteThroughStore wraps a primary database and the local Causal Crystal.
// Writes go to the DB first, then are reflected back via the ChangeListener.
type WriteThroughStore struct {
	Primary Store
	Local   *CausalCrystal
	ttlMap  sync.Map
}

func NewWriteThroughStore(primary Store, local *CausalCrystal) *WriteThroughStore {
	return &WriteThroughStore{
		Primary: primary,
		Local:   local,
	}
}

func (s *WriteThroughStore) AppendAtom(expr core.CombinatorExpr, path string, parents []core.Hash, expiresAt time.Time) (core.Hash, error) {
	// Step 1: Write to primary DB
	_, err := s.Primary.AppendAtom(expr, path, parents, expiresAt)
	if err != nil {
		return core.Hash{}, err
	}

	if !expiresAt.IsZero() {
		s.ttlMap.Store(path, expiresAt)
	} else {
		s.ttlMap.Delete(path)
	}

	// Step 2: Synchronously update local Crystal
	return s.Local.AppendAtom(expr, path, parents, expiresAt)
}

func (s *WriteThroughStore) GetCurrent(path string) (*core.CausalAtom, bool) {
	// Try local Crystal first (83ns path)
	if atom, ok := s.Local.GetCurrent(path); ok {
		return atom, true
	}

	// Fallback to Primary DB (seed the cache)
	if atom, ok := s.Primary.GetCurrent(path); ok {
		// Seed the local Crystal so future reads are fast
		hash, err := s.Local.AppendAtom(atom.Expr, atom.Path, nil, atom.ExpiresAt)
		if err == nil {
			return s.Local.GetAtom(hash)
		}
		return atom, true
	}

	return nil, false
}

func (s *WriteThroughStore) GetParents(hash core.Hash) ([]core.Hash, bool) {
	return s.Local.GetParents(hash)
}

func (s *WriteThroughStore) GetAtom(hash core.Hash) (*core.CausalAtom, bool) {
	return s.Local.GetAtom(hash)
}

func (s *WriteThroughStore) FrontierPaths() []string {
	return s.Local.FrontierPaths()
}

func (s *WriteThroughStore) Close() error {
	s.Primary.Close()
	return s.Local.Close()
}
