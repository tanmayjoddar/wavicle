package engine

import (
	"fmt"
	"strings"
	"wavicle/internal/core"
	"wavicle/internal/storage"
)

// SQLToProofTree parses a simple SELECT query and builds a Wavicle proof tree.
// Supported: SELECT col1, col2 FROM table WHERE id = 123
func SQLToProofTree(query string, store storage.Store) (core.CombinatorExpr, string, error) {
	q := strings.ToUpper(query)
	if !strings.HasPrefix(q, "SELECT") {
		return nil, "", fmt.Errorf("unsupported SQL query: only SELECT is supported")
	}

	// Extremely naive parser for Phase 1
	// Expected: SELECT name, email FROM users WHERE id = 123
	fromIdx := strings.Index(q, "FROM")
	whereIdx := strings.Index(q, "WHERE")

	if fromIdx == -1 || whereIdx == -1 {
		return nil, "", fmt.Errorf("malformed SQL: missing FROM or WHERE")
	}

	colsPart := query[7:fromIdx]
	tablePart := strings.TrimSpace(query[fromIdx+4 : whereIdx])
	wherePart := query[whereIdx+5:]

	cols := strings.Split(colsPart, ",")
	for i := range cols {
		cols[i] = strings.TrimSpace(cols[i])
	}

	// Parse WHERE id = 123
	eqIdx := strings.Index(wherePart, "=")
	if eqIdx == -1 {
		return nil, "", fmt.Errorf("malformed SQL: only id = val is supported")
	}
	id := strings.TrimSpace(wherePart[eqIdx+1:])
	id = strings.Trim(id, "'\"")

	// The "primary path" for the proof cache is the query itself
	primaryPath := fmt.Sprintf("sql:%s:%s", tablePart, id)

	var atomHashes []core.Hash
	for _, col := range cols {
		path := fmt.Sprintf("%s:%s:%s", tablePart, id, col)
		
		atom, ok := store.GetCurrent(path)
		if ok {
			atomHashes = append(atomHashes, atom.Hash)
		} else {
			return nil, "", fmt.Errorf("dependency not found: %s", path)
		}
	}

	if len(atomHashes) == 0 {
		return nil, "", fmt.Errorf("no data found for query dependencies")
	}

	expr := core.NewECompose(atomHashes)
	return expr, primaryPath, nil
}
