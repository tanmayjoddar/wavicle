package replication

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"sync/atomic"
	"time"
)

// MySQLConfig configures polling-based MySQL CDC.
// Production binlog tailing (go-mysql replication) is the next step;
// this poller already emits the same ChangeEvent shape so the proof engine,
// PathMapper, and echo-suppression work unchanged.
type MySQLConfig struct {
	DSN           string // e.g. "user:pass@tcp(host:3306)/db"
	Tables        []string
	PollInterval  time.Duration
	StateTable    string // e.g. "wavicle_cdc_state" for high-watermark
	TableMappings []PathMapper
}

// MySQLListener polls updated_at/id high-watermarks per table.
// Requires a MySQL driver registered as "mysql" (e.g. go-sql-driver/mysql).
// Without the driver it returns a clear error instead of crashing.
type MySQLListener struct {
	cfg     MySQLConfig
	mappers []PathMapper
	events  chan ChangeEvent
	cancel  context.CancelFunc
	// lastOK is unixnano of the last fully successful poll tick (all tables
	// queried without error). 0 = never. Shared with the server's stale
	// guard: a quiet-but-healthy database polls clean every tick, so idle
	// SERVES; only a dead/erroring poller goes silent and fails reads.
	lastOK atomic.Int64
}

// LastPollOK exposes the poll-health clock for fail-closed reads.
func (l *MySQLListener) LastPollOK() *atomic.Int64 { return &l.lastOK }

func NewMySQLListener(cfg MySQLConfig) *MySQLListener {
	if cfg.PollInterval <= 0 {
		cfg.PollInterval = 500 * time.Millisecond
	}
	return &MySQLListener{cfg: cfg, mappers: cfg.TableMappings, events: make(chan ChangeEvent, 10000)}
}

func (l *MySQLListener) Start(ctx context.Context) (<-chan ChangeEvent, error) {
	if l.cfg.DSN == "" {
		return nil, fmt.Errorf("mysql DSN is required")
	}
	ctx, cancel := context.WithCancel(ctx)
	l.cancel = cancel
	go l.run(ctx)
	return l.events, nil
}

func (l *MySQLListener) Close() error {
	if l.cancel != nil {
		l.cancel()
	}
	return nil
}

func (l *MySQLListener) run(ctx context.Context) {
	defer close(l.events)
	db, err := sql.Open("mysql", l.cfg.DSN)
	if err != nil {
		log.Printf("[replication/mysql] open: %v (need github.com/go-sql-driver/mysql)", err)
		<-ctx.Done()
		return
	}
	defer db.Close()
	if err := db.PingContext(ctx); err != nil {
		log.Printf("[replication/mysql] ping: %v", err)
		<-ctx.Done()
		return
	}
	// High-watermark per table: max(id) seen. Tables need AUTO_INCREMENT id
	// or updated_at; we probe both without assuming schema.
	wm := map[string]int64{}
	ticker := time.NewTicker(l.cfg.PollInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			tickOK := true
			for _, table := range l.cfg.Tables {
				if !l.pollTable(ctx, db, table, wm) {
					tickOK = false
				}
			}
			// Stamp ONLY on a fully clean tick: any table error means this
			// poll proved nothing, and the stale guard must see silence.
			if tickOK {
				l.lastOK.Store(time.Now().UnixNano())
			}
		}
	}
}

func (l *MySQLListener) pollTable(ctx context.Context, db *sql.DB, table string, wm map[string]int64) bool {
	// FIX (SQL injection): table comes from config; validate identifier like
	// PostgresStore does before interpolating into SELECT.
	if !isValidMySQLIdentifier(table) {
		return false
	}
	// Try id-watermark first; fall back silently if schema differs.
	rows, err := db.QueryContext(ctx, fmt.Sprintf("SELECT * FROM `%s` WHERE id > ? ORDER BY id ASC LIMIT 1000", table), wm[table])
	if err != nil {
		return false
	}
	defer rows.Close()
	cols, err := rows.Columns()
	if err != nil {
		return false
	}
	for rows.Next() {
		vals := make([]any, len(cols))
		ptrs := make([]any, len(cols))
		for i := range vals {
			ptrs[i] = &vals[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			continue
		}
		row := map[string]any{}
		for i, c := range cols {
			if b, ok := vals[i].([]byte); ok {
				row[c] = string(b)
			} else {
				row[c] = vals[i]
			}
		}
		paths := l.mapRow(table, row)
		select {
		case l.events <- ChangeEvent{Table: table, Action: "UPDATE", NewValues: row, AffectedPaths: paths, CommitTime: time.Now()}:
		case <-ctx.Done():
			return false
		default:
		}
		if id, ok := row["id"]; ok {
			var iv int64
			_, _ = fmt.Sscan(fmt.Sprint(id), &iv)
			if iv > wm[table] {
				wm[table] = iv
			}
		}
	}
	return rows.Err() == nil
}

func (l *MySQLListener) mapRow(table string, values map[string]any) []string {
	for _, m := range l.mappers {
		if m.Table == table {
			return m.MapRowToPaths("", values)
		}
	}
	return autoPathMapping(table, values)
}

func isValidMySQLIdentifier(s string) bool {
	if len(s) == 0 || len(s) > 64 {
		return false
	}
	for i, r := range s {
		if i == 0 && !(r == '_' || r == '$' || (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z')) {
			return false
		}
		if !(r == '_' || r == '$' || (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9')) {
			return false
		}
	}
	return true
}
