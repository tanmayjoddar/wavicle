package replication

import (
	"context"
	"encoding/binary"
	"fmt"
	"log"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
)

const (
	standbyTimeout  = 10 * time.Second
	keepalivePeriod = 30 * time.Second
	maxBackoff      = 30 * time.Second
)

// PGConfig holds connection parameters for the PostgreSQL adapter.
type PGConfig struct {
	DSN             string
	ReplicationSlot string
	Publication     string
	TableMappings   []PathMapper
}

// PGListener consumes PostgreSQL logical replication events.
type PGListener struct {
	config    PGConfig
	mappers   []PathMapper
	events    chan ChangeEvent
	cancel    context.CancelFunc
	conn      *pgconn.PgConn
	relations map[uint32]*pglogrepl.RelationMessage
	mu        sync.Mutex
	running   atomic.Bool
}

func NewPGListener(cfg PGConfig) *PGListener {
	return &PGListener{
		config:    cfg,
		mappers:   cfg.TableMappings,
		relations: make(map[uint32]*pglogrepl.RelationMessage),
		events:    make(chan ChangeEvent, 10000),
	}
}

func (l *PGListener) Start(ctx context.Context) (<-chan ChangeEvent, error) {
	if l.config.DSN == "" {
		return nil, fmt.Errorf("postgres DSN is required")
	}
	ctx, cancel := context.WithCancel(ctx)
	l.cancel = cancel
	go l.run(ctx)
	return l.events, nil
}

func (l *PGListener) Close() error {
	if l.cancel != nil {
		l.cancel()
	}
	l.mu.Lock()
	if l.conn != nil {
		l.conn.Close(context.Background())
		l.conn = nil
	}
	l.mu.Unlock()
	return nil
}

func (l *PGListener) run(ctx context.Context) {
	defer close(l.events)

	backoff := time.Second
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		if err := l.connectAndConsume(ctx); err != nil {
			l.running.Store(false)
			log.Printf("[replication] PostgreSQL connection error: %v. Retrying in %v...", err, backoff)
			select {
			case <-ctx.Done():
				return
			case <-time.After(backoff):
				if backoff < maxBackoff {
					backoff *= 2
				}
			}
			continue
		}
		backoff = time.Second
	}
}

func (l *PGListener) connectAndConsume(ctx context.Context) error {
	// Replication connections require replication=database in the DSN
	replDSN := l.config.DSN
	if !strings.Contains(replDSN, "replication=") {
		if strings.Contains(replDSN, "?") {
			replDSN += "&replication=database"
		} else {
			replDSN += "?replication=database"
		}
	}

	conn, err := pgconn.Connect(ctx, replDSN)
	if err != nil {
		return fmt.Errorf("connect: %w", err)
	}
	l.mu.Lock()
	l.conn = conn
	l.mu.Unlock()

	slotName := l.config.ReplicationSlot
	if slotName == "" {
		slotName = "wavicle_slot"
	}
	pubName := l.config.Publication
	if pubName == "" {
		pubName = "wavicle_proofs"
	}

	// Create slot — idempotent (will error if exists, which is OK)
	_, _ = pglogrepl.CreateReplicationSlot(ctx, conn, slotName, "pgoutput",
		pglogrepl.CreateReplicationSlotOptions{Temporary: false})

	// Start replication from LSN 0 (earliest available)
	err = pglogrepl.StartReplication(ctx, conn, slotName, 0,
		pglogrepl.StartReplicationOptions{
			PluginArgs: []string{
				"proto_version '1'",
				fmt.Sprintf("publication_names '%s'", pubName),
			},
		})
	if err != nil {
		return fmt.Errorf("start replication: %w", err)
	}

	l.running.Store(true)
	lastKeepalive := time.Now()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		if time.Since(lastKeepalive) > keepalivePeriod {
			pglogrepl.SendStandbyStatusUpdate(ctx, conn,
				pglogrepl.StandbyStatusUpdate{WALWritePosition: 0})
			lastKeepalive = time.Now()
		}

		readCtx, cancel := context.WithTimeout(ctx, standbyTimeout)
		msg, err := conn.ReceiveMessage(readCtx)
		cancel()
		if err != nil {
			if pgconn.Timeout(err) {
				pglogrepl.SendStandbyStatusUpdate(ctx, conn,
					pglogrepl.StandbyStatusUpdate{WALWritePosition: 0})
				lastKeepalive = time.Now()
				continue
			}
			return fmt.Errorf("receive: %w", err)
		}

		switch m := msg.(type) {
		case *pgproto3.CopyData:
			if err := l.handleCopyData(ctx, conn, m.Data); err != nil {
				return err
			}
		case *pgproto3.ErrorResponse:
			// Try again — the error may be transient
			return fmt.Errorf("PG error: %s", m.Message)
		case *pgproto3.NoticeResponse:
			// Ignore notices
		}
	}
}

// handleCopyData processes a CopyData message from the replication stream.
// The first byte identifies the message subtype:
//
//	'w' = XLogData (contains decoded WAL entries)
//	'k' = Primary keepalive message
func (l *PGListener) handleCopyData(ctx context.Context, conn *pgconn.PgConn, data []byte) error {
	if len(data) == 0 {
		return nil
	}

	switch data[0] {
	case 'w': // XLogData — contains WAL changes
		return l.processXLogData(ctx, conn, data)

	case 'k': // Primary keepalive
		pkm, err := pglogrepl.ParsePrimaryKeepaliveMessage(data[1:])
		if err != nil {
			return fmt.Errorf("parse keepalive: %w", err)
		}
		if pkm.ReplyRequested {
			pglogrepl.SendStandbyStatusUpdate(ctx, conn,
				pglogrepl.StandbyStatusUpdate{WALWritePosition: pkm.ServerWALEnd})
		}
		return nil

	default:
		return nil
	}
}

// processXLogData decodes a WAL message and emits ChangeEvents.
func (l *PGListener) processXLogData(ctx context.Context, conn *pgconn.PgConn, data []byte) error {
	xld, err := pglogrepl.ParseXLogData(data[1:])
	if err != nil {
		return fmt.Errorf("parse xlog: %w", err)
	}

	logicalMsg, err := pglogrepl.Parse(xld.WALData)
	if err != nil {
		return nil // Unknown or unsupported message — skip
	}

	switch msg := logicalMsg.(type) {
	case *pglogrepl.RelationMessage:
		l.relations[msg.RelationID] = msg

	case *pglogrepl.InsertMessage:
		evt := l.buildInsertEvent(msg, xld.ServerTime)
		if evt != nil {
			select {
			case l.events <- *evt:
			case <-ctx.Done():
				return ctx.Err()
			}
		}

	case *pglogrepl.UpdateMessage:
		evt := l.buildUpdateEvent(msg, xld.ServerTime)
		if evt != nil {
			select {
			case l.events <- *evt:
			case <-ctx.Done():
				return ctx.Err()
			}
		}

	case *pglogrepl.DeleteMessage:
		evt := l.buildDeleteEvent(msg, xld.ServerTime)
		if evt != nil {
			select {
			case l.events <- *evt:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
	}

	return nil
}

// buildInsertEvent converts an InsertMessage to a ChangeEvent.
func (l *PGListener) buildInsertEvent(msg *pglogrepl.InsertMessage, commitTime time.Time) *ChangeEvent {
	rel, ok := l.relations[msg.RelationID]
	if !ok {
		return nil
	}
	cols := decodeTupleColumns(rel.Columns, msg.Tuple.Columns)
	paths := l.mapRow(rel.Namespace+"."+rel.RelationName, cols)
	return &ChangeEvent{
		Table:         rel.RelationName,
		Action:        "INSERT",
		NewValues:     cols,
		AffectedPaths: paths,
		CommitTime:    commitTime,
	}
}

// buildUpdateEvent converts an UpdateMessage to a ChangeEvent.
func (l *PGListener) buildUpdateEvent(msg *pglogrepl.UpdateMessage, commitTime time.Time) *ChangeEvent {
	rel, ok := l.relations[msg.RelationID]
	if !ok {
		return nil
	}
	var oldCols, newCols map[string]any
	if msg.OldTuple != nil {
		oldCols = decodeTupleColumns(rel.Columns, msg.OldTuple.Columns)
	}
	if msg.NewTuple != nil {
		newCols = decodeTupleColumns(rel.Columns, msg.NewTuple.Columns)
	}
	vals := newCols
	if vals == nil && oldCols != nil {
		vals = oldCols
	}
	if vals == nil {
		vals = oldCols
	}
	paths := l.mapRow(rel.Namespace+"."+rel.RelationName, vals)
	return &ChangeEvent{
		Table:         rel.RelationName,
		Action:        "UPDATE",
		OldValues:     oldCols,
		NewValues:     newCols,
		AffectedPaths: paths,
		CommitTime:    commitTime,
	}
}

// buildDeleteEvent converts a DeleteMessage to a ChangeEvent.
func (l *PGListener) buildDeleteEvent(msg *pglogrepl.DeleteMessage, commitTime time.Time) *ChangeEvent {
	rel, ok := l.relations[msg.RelationID]
	if !ok {
		return nil
	}
	var oldCols map[string]any
	if msg.OldTuple != nil {
		oldCols = decodeTupleColumns(rel.Columns, msg.OldTuple.Columns)
	}
	paths := l.mapRow(rel.Namespace+"."+rel.RelationName, oldCols)
	return &ChangeEvent{
		Table:         rel.RelationName,
		Action:        "DELETE",
		OldValues:     oldCols,
		AffectedPaths: paths,
		CommitTime:    commitTime,
	}
}

// mapRow converts a database row to cache paths using configured mappers.
func (l *PGListener) mapRow(table string, values map[string]any) []string {
	if len(values) == 0 {
		return nil
	}
	// Strip schema prefix: "public.users" → "users"
	shortTable := table
	if idx := strings.LastIndex(table, "."); idx != -1 {
		shortTable = table[idx+1:]
	}
	for _, m := range l.mappers {
		if m.Table == shortTable {
			return m.MapRowToPaths("", values)
		}
	}
	return autoPathMapping(shortTable, values)
}

// autoPathMapping generates cache paths when no explicit mapping exists.
func autoPathMapping(table string, values map[string]any) []string {
	var id string
	for k, v := range values {
		if k == "id" || k == "ID" || k == "Id" {
			id = fmt.Sprint(v)
			break
		}
	}
	if id == "" {
		return nil
	}
	prefix := table + ":" + id
	paths := make([]string, 0, len(values))
	for k := range values {
		if k != "id" && k != "ID" && k != "Id" {
			paths = append(paths, prefix+":"+k)
		}
	}
	return paths
}

// decodeTupleColumns converts pgoutput tuple columns to a map.
func decodeTupleColumns(meta []*pglogrepl.RelationMessageColumn, cols []*pglogrepl.TupleDataColumn) map[string]any {
	if len(cols) > len(meta) {
		cols = cols[:len(meta)]
	}
	result := make(map[string]any, len(cols))
	for i, col := range cols {
		if i >= len(meta) {
			break
		}
		name := meta[i].Name
		switch col.DataType {
		case 'n': // NULL
			result[name] = nil
		case 'u': // TOAST
			_ = col // Unchanged toast value — skip
		case 't': // Text
			result[name] = string(col.Data)
		case 'b': // Binary
			if col.Length <= 8 {
				result[name] = decodeInt(col.Data)
			} else {
				result[name] = string(col.Data)
			}
		default:
			result[name] = string(col.Data)
		}
	}
	return result
}

// decodeInt attempts to decode an integer from binary data.
func decodeInt(data []byte) int64 {
	switch len(data) {
	case 2:
		return int64(binary.BigEndian.Uint16(data))
	case 4:
		return int64(binary.BigEndian.Uint32(data))
	case 8:
		return int64(binary.BigEndian.Uint64(data))
	default:
		return 0
	}
}

// VerifyPostgresSetup returns the SQL script for database setup.
func VerifyPostgresSetup() string {
	return `-- Required PostgreSQL setup for Wavicle:
--
-- 1. Set in postgresql.conf (restart required):
--    wal_level = logical
--    max_replication_slots = 5
--    max_wal_senders = 5
--
-- 2. Run as superuser:
CREATE PUBLICATION wavicle_proofs FOR ALL TABLES;
SELECT pg_create_logical_replication_slot('wavicle_slot', 'pgoutput');
--
-- 3. Verify:
SELECT slot_name, active FROM pg_replication_slots WHERE slot_name = 'wavicle_slot';
SELECT pub_name FROM pg_publication WHERE pub_name = 'wavicle_proofs';
`
}
