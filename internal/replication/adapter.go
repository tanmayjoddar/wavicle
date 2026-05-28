package replication

import (
	"context"
	"fmt"
)

// ChangeEvent represents a data change detected by a database listener.
type ChangeEvent struct {
	Table        string         // e.g. "users"
	Action       string         // INSERT, UPDATE, DELETE
	OldValues    map[string]any // previous row values (UPDATE, DELETE)
	NewValues    map[string]any // new row values (INSERT, UPDATE)
	AffectedPaths []string      // cache paths that depend on this change
}

// ChangeListener is implemented by each database adapter.
type ChangeListener interface {
	// Start begins consuming change events from the database.
	// Events are sent to the returned channel until the context is cancelled.
	Start(ctx context.Context) (<-chan ChangeEvent, error)

	// Close shuts down the listener and releases resources.
	Close() error
}

// ChangeEventFunc converts a ChangeEvent into cache paths to invalidate.
// Implementations map table+row changes to affected proof cache entries.
type ChangeEventFunc func(ChangeEvent) []string

// PathMapper maps a database table and row to cache key paths.
type PathMapper struct {
	Table       string
	KeyTemplate string // e.g. "users:${id}"
	Columns     []string
}

// MapRowToPaths converts a database row change to cache paths using the template.
func (pm *PathMapper) MapRowToPaths(action string, values map[string]any) []string {
	if len(values) == 0 {
		return nil
	}

	// Build the key prefix from the template
	key := pm.expandTemplate(values)
	if key == "" {
		return nil
	}

	// Generate paths for each column
	paths := make([]string, 0, len(pm.Columns))
	for _, col := range pm.Columns {
		paths = append(paths, key+":"+col)
	}

	return paths
}

func (pm *PathMapper) expandTemplate(values map[string]any) string {
	var key string
	for k, v := range values {
		switch k {
		case "id", "ID", "Id":
			key = pm.Table + ":" + fmt.Sprint(v)
		}
	}
	return key
}
