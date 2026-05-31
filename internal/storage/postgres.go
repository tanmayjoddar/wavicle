package storage

import (
	"database/sql"
	"fmt"
	"strings"
	"time"
	"unicode"
	"wavicle/internal/core"

	_ "github.com/lib/pq"
)

// PostgresStore implements the Store interface for a PostgreSQL backend.
type PostgresStore struct {
	db *sql.DB
}

func NewPostgresStore(dsn string) (*PostgresStore, error) {
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		return nil, err
	}
	if err := db.Ping(); err != nil {
		return nil, err
	}
	return &PostgresStore{db: db}, nil
}

func isValidIdentifier(s string) bool {
	if len(s) == 0 || len(s) > 63 { // PostgreSQL max identifier length
		return false
	}
	for i, r := range s {
		if i == 0 && !unicode.IsLetter(r) && r != '_' {
			return false
		}
		if !unicode.IsLetter(r) && !unicode.IsDigit(r) && r != '_' {
			return false
		}
	}
	return true
}

func (s *PostgresStore) AppendAtom(expr core.CombinatorExpr, path string, parents []core.Hash, expiresAt time.Time) (core.Hash, error) {
	// Phase 1: SET key value -> UPDATE table
	parts := strings.Split(path, ":")
	if len(parts) < 3 {
		return core.Hash{}, fmt.Errorf("invalid path format for Postgres: %s (expected table:id:column)", path)
	}

	table := parts[0]
	id := parts[1] // ID can be any string, passed as parameterized argument
	column := parts[2]

	// Security Audit: Prevent SQL Injection
	// Path segments 'table' and 'column' are injected directly into the SQL string.
	// They must be strictly validated as standard SQL identifiers.
	if !isValidIdentifier(table) {
		return core.Hash{}, fmt.Errorf("invalid table name in path: %s", table)
	}
	if !isValidIdentifier(column) {
		return core.Hash{}, fmt.Errorf("invalid column name in path: %s", column)
	}

	var val any
	if ec, ok := expr.(*core.EConst); ok {
		if _, isNull := ec.Value.(core.VNull); isNull {
			val = nil // Translate to SQL NULL
		} else {
			val = ec.Value
		}
	} else {
		return core.Hash{}, fmt.Errorf("only EConst expressions are supported for Postgres writes in Phase 1")
	}

	// Use UPSERT if possible, or just UPDATE for Phase 1
	// table and column are validated above to contain only safe characters.
	query := fmt.Sprintf("INSERT INTO %s (id, %s) VALUES ($1, $2) ON CONFLICT (id) DO UPDATE SET %s = $2", table, column, column)
	_, err := s.db.Exec(query, id, val)
	if err != nil {
		return core.Hash{}, fmt.Errorf("postgres upsert: %w", err)
	}

	return core.Hash{}, nil
}

func (s *PostgresStore) GetCurrent(path string) (*core.CausalAtom, bool) {
	// Fallback SELECT to seed the local Crystal on first miss
	parts := strings.Split(path, ":")
	if len(parts) < 3 {
		return nil, false
	}

	table := parts[0]
	id := parts[1]
	column := parts[2]

	if !isValidIdentifier(table) || !isValidIdentifier(column) {
		return nil, false
	}

	query := fmt.Sprintf("SELECT %s FROM %s WHERE id = $1", column, table)
	var val string
	err := s.db.QueryRow(query, id).Scan(&val)
	if err != nil {
		return nil, false
	}

	// Return a temporary atom. In main.go, the server should detect this 
	// and seed the Crystal. Or we could seed it here if we had access to the Crystal.
	// Since Store is abstract, we return an atom that the caller can use.
	return &core.CausalAtom{
		Path: path,
		Expr: &core.EConst{Value: core.VString(val)},
	}, true
}

func (s *PostgresStore) GetParents(hash core.Hash) ([]core.Hash, bool) {
	return nil, false
}

func (s *PostgresStore) GetAtom(hash core.Hash) (*core.CausalAtom, bool) {
	return nil, false
}

func (s *PostgresStore) GetCurrentHash(path string) (core.Hash, bool) {
	atom, ok := s.GetCurrent(path)
	if !ok {
		return core.Hash{}, false
	}
	return atom.Hash, true
}

func (s *PostgresStore) FindChangedPaths(entries map[string]core.Hash, buf []string) []string {
	return buf[:0]
}

func (s *PostgresStore) VerifyVersionVector(entries map[string]core.Hash) bool {
	return false
}

func (s *PostgresStore) FrontierPaths() []string {
	return nil
}

func (s *PostgresStore) Close() error {
	return s.db.Close()
}
