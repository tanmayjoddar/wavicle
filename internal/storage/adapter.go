package storage

import (
	"fmt"
	"strings"
	"sync"
	"time"
	"wavicle/internal/core"
)

// Store is the interface for storage engines (Crystal, FrontierCache, Postgres, etc.)
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

	// GetCurrentHash returns the hash of the current atom for a path.
	GetCurrentHash(path string) (core.Hash, bool)

	// FindChangedPaths returns paths whose hashes differ from the given version vector.
	FindChangedPaths(entries map[string]core.Hash, buf []string) []string

	// VerifyVersionVector returns true if all path hashes match the current frontier.
	VerifyVersionVector(entries map[string]core.Hash) bool
}

// WriteThroughStore wraps a primary database and a local cache.
// Writes go to the DB first, then to the local cache. The local cache is
// kept up-to-date via the ChangeListener for external DB changes.
// In production mode, the local cache is a FrontierCache (O(unique keys) memory).
// In dev mode, the local cache is a CausalCrystal (full DAG, WAL, compaction).
type WriteThroughStore struct {
	Primary Store
	Local   Store
	ttlMap  sync.Map
}

func NewWriteThroughStore(primary Store, local Store) *WriteThroughStore {
	return &WriteThroughStore{
		Primary: primary,
		Local:   local,
	}
}

func (s *WriteThroughStore) AppendAtom(expr core.CombinatorExpr, path string, parents []core.Hash, expiresAt time.Time) (core.Hash, error) {
	// Step 1: Write to primary DB
	_, err := s.Primary.AppendAtom(expr, path, parents, expiresAt)
	if err != nil {
		// Fallback for arbitrary keys or missing tables
		// This allows Wavicle to function as a general-purpose cache 
		// for keys that don't fit the Postgres table:id:column schema.
		if strings.Contains(err.Error(), "invalid path format") || 
		   strings.Contains(err.Error(), "invalid table name") || 
		   strings.Contains(err.Error(), "invalid column name") || 
		   strings.Contains(err.Error(), "does not exist") ||
		   strings.Contains(err.Error(), "pq: relation") {
			// Skip the primary error, proceed to local Crystal append
		} else {
			return core.Hash{}, err
		}
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
	// Try local Crystal first (fast path)
	if cached, ok := s.Local.GetCurrent(path); ok {
		// Only verify against PG if atom is older than 5 seconds.
		// This prevents PG hammering under load while still catching
		// external writes within a reasonable window. Replication handles
		// the real-time path; this is a backup for edge cases.
		if len(cached.CausalPast) == 0 &&
			time.Since(cached.PhysicalTime) > 30*time.Second {
			if pgAtom, pgOk := s.Primary.GetCurrent(path); pgOk {
				cv, cok := extractAtomValue(cached)
				pv, pok := extractAtomValue(pgAtom)
				if !cok || !pok || cv != pv {
					hash, err := s.Local.AppendAtom(pgAtom.Expr, pgAtom.Path, nil, pgAtom.ExpiresAt)
					if err == nil {
						if updated, ok := s.Local.GetAtom(hash); ok {
							return updated, true
						}
					}
				}
			}
		}
		return cached, true
	}

	// Fallback to Primary DB (seed the cache)
	if atom, ok := s.Primary.GetCurrent(path); ok {
		hash, err := s.Local.AppendAtom(atom.Expr, atom.Path, nil, atom.ExpiresAt)
		if err == nil {
			return s.Local.GetAtom(hash)
		}
		return atom, true
	}

	return nil, false
}

func extractAtomValue(atom *core.CausalAtom) (string, bool) {
	if atom == nil || atom.Expr == nil {
		return "", false
	}
	if e, ok := atom.Expr.(*core.EConst); ok {
		return fmt.Sprint(e.Value), true
	}
	return "", false
}

func (s *WriteThroughStore) GetParents(hash core.Hash) ([]core.Hash, bool) {
	return s.Local.GetParents(hash)
}

func (s *WriteThroughStore) GetAtom(hash core.Hash) (*core.CausalAtom, bool) {
	return s.Local.GetAtom(hash)
}

func (s *WriteThroughStore) GetCurrentHash(path string) (core.Hash, bool) {
	return s.Local.GetCurrentHash(path)
}

func (s *WriteThroughStore) FindChangedPaths(entries map[string]core.Hash, buf []string) []string {
	return s.Local.FindChangedPaths(entries, buf)
}

func (s *WriteThroughStore) VerifyVersionVector(entries map[string]core.Hash) bool {
	return s.Local.VerifyVersionVector(entries)
}

func (s *WriteThroughStore) FrontierPaths() []string {
	return s.Local.FrontierPaths()
}

func (s *WriteThroughStore) Close() error {
	s.Primary.Close()
	return s.Local.Close()
}
