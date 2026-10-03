// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Package sqlite provides an OLTP snapshot projection for Structured Data Streams (SDS)
// backed by pure-Go SQLite (modernc.org/sqlite).
package sqlite

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	sdsv1 "github.com/gke-labs/in-cluster-storage/pkg/api/sds/v1"
	"github.com/gke-labs/in-cluster-storage/pkg/sds"
	"github.com/gke-labs/in-cluster-storage/pkg/sds/record"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/descriptorpb"
	_ "modernc.org/sqlite" // Pure-Go SQLite driver registration
)

var (
	// ErrTypeNotRegistered is returned when a change is encountered for an unregistered type ID.
	ErrTypeNotRegistered = errors.New("type not registered")
	// ErrPendingTransaction is returned when attempting a snapshot while transactions are pending.
	ErrPendingTransaction = errors.New("cannot snapshot stream with pending transactions")
)

// TableName maps a fully-qualified protobuf message name (e.g. "shop.Order")
// to a valid SQLite table name (e.g. "shop_Order") by stripping any leading dot
// and replacing all dots with underscores.
func TableName(fullName string) string {
	trimmed := strings.TrimPrefix(fullName, ".")
	return strings.ReplaceAll(trimmed, ".", "_")
}

// Option configures a SQLite projection DB.
type Option func(*options)

type options struct {
	streamID    string
	lockingMode string
	journalMode string
	synchronous string
	busyTimeout int
	decoderOpts []record.DecoderOption
}

// WithStreamID sets the stream ID for the projection.
func WithStreamID(streamID string) Option {
	return func(o *options) {
		o.streamID = streamID
	}
}

// WithLockingMode sets the SQLite PRAGMA locking_mode (e.g. "NORMAL", "EXCLUSIVE").
func WithLockingMode(mode string) Option {
	return func(o *options) {
		o.lockingMode = mode
	}
}

// WithJournalMode sets the SQLite PRAGMA journal_mode (e.g. "OFF", "WAL", "DELETE").
func WithJournalMode(mode string) Option {
	return func(o *options) {
		o.journalMode = mode
	}
}

// WithSynchronous sets the SQLite PRAGMA synchronous (e.g. "OFF", "NORMAL", "FULL").
func WithSynchronous(syncMode string) Option {
	return func(o *options) {
		o.synchronous = syncMode
	}
}

// WithBusyTimeout sets the SQLite busy_timeout in milliseconds.
func WithBusyTimeout(ms int) Option {
	return func(o *options) {
		o.busyTimeout = ms
	}
}

// WithDecoderOptions passes record decoder options to the underlying ChangeReader.
func WithDecoderOptions(opts ...record.DecoderOption) Option {
	return func(o *options) {
		o.decoderOpts = append(o.decoderOpts, opts...)
	}
}

// withoutCancel returns a context that is not cancelled when ctx is cancelled.
// In modernc.org/sqlite, every query or exec call on a cancellable context spawns
// an interruptOnDone background goroutine that listens on ctx.Done(). Since these
// operations take single-digit microseconds on our local, exclusive-locked SQLite
// database, stripping cancellation on the internal hot path avoids substantial
// goroutine creation and channel synchronization overhead.
func withoutCancel(ctx context.Context) context.Context {
	return context.WithoutCancel(ctx)
}

type tableStmts struct {
	get           *sql.Stmt
	upsert        *sql.Stmt
	del           *sql.Stmt
	scanPrefix    *sql.Stmt
	scanPrefixLim *sql.Stmt
	scanGe        *sql.Stmt
	scanGeLim     *sql.Stmt
	scanAll       *sql.Stmt
	scanAllLim    *sql.Stmt
	count         *sql.Stmt
}

func (ts *tableStmts) Close() error {
	var firstErr error
	for _, s := range []*sql.Stmt{
		ts.get, ts.upsert, ts.del,
		ts.scanPrefix, ts.scanPrefixLim,
		ts.scanGe, ts.scanGeLim,
		ts.scanAll, ts.scanAllLim,
		ts.count,
	} {
		if s != nil {
			if err := s.Close(); err != nil && firstErr == nil {
				firstErr = err
			}
		}
	}
	return firstErr
}

// DB represents a SQLite projection database for a structured data stream.
type DB struct {
	mu           sync.RWMutex
	sqlDB        *sql.DB
	path         string
	streamID     string
	position     uint64
	changeReader *sds.ChangeReader
	knownTables  map[string]bool // tableName -> table created
	stmts        map[string]*tableStmts
}

// Open opens an existing SQLite projection database or creates a new one if it does not exist.
func Open(ctx context.Context, path string, opts ...Option) (*DB, error) {
	var opt options
	for _, o := range opts {
		o(&opt)
	}

	dsn := path
	var pragmas []string
	if opt.lockingMode != "" {
		pragmas = append(pragmas, fmt.Sprintf("_pragma=locking_mode(%s)", opt.lockingMode))
	}
	if opt.journalMode != "" {
		pragmas = append(pragmas, fmt.Sprintf("_pragma=journal_mode(%s)", opt.journalMode))
	}
	if opt.synchronous != "" {
		pragmas = append(pragmas, fmt.Sprintf("_pragma=synchronous(%s)", opt.synchronous))
	}
	if opt.busyTimeout > 0 {
		pragmas = append(pragmas, fmt.Sprintf("_pragma=busy_timeout(%d)", opt.busyTimeout))
	} else if len(pragmas) > 0 {
		pragmas = append(pragmas, "_pragma=busy_timeout(5000)")
	}

	if len(pragmas) > 0 {
		sep := "?"
		if strings.Contains(dsn, "?") {
			sep = "&"
		}
		dsn += sep + strings.Join(pragmas, "&")
	}

	sqlDB, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to open sqlite database at %q: %w", path, err)
	}
	sqlDB.SetMaxOpenConns(1)

	db := &DB{
		sqlDB:        sqlDB,
		path:         path,
		streamID:     opt.streamID,
		changeReader: sds.NewChangeReader(opt.decoderOpts...),
		knownTables:  make(map[string]bool),
		stmts:        make(map[string]*tableStmts),
	}

	if err := db.initBookkeeping(ctx); err != nil {
		_ = sqlDB.Close()
		return nil, err
	}

	if err := db.loadState(ctx); err != nil {
		_ = sqlDB.Close()
		return nil, err
	}

	return db, nil
}

// SQLDB returns the underlying *sql.DB.
func (d *DB) SQLDB() *sql.DB {
	return d.sqlDB
}

// Path returns the file path of the SQLite database.
func (d *DB) Path() string {
	return d.path
}

// StreamID returns the stream ID.
func (d *DB) StreamID() string {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return d.streamID
}

// Position returns the stream sequence position of the last applied record.
func (d *DB) Position() uint64 {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return d.position
}

// Registry returns the in-band type registry.
func (d *DB) Registry() *record.Registry {
	return d.changeReader.Registry()
}

// ChangeReader returns the underlying sds.ChangeReader.
func (d *DB) ChangeReader() *sds.ChangeReader {
	return d.changeReader
}

// Close closes the SQLite database connection.
func (d *DB) Close() error {
	d.mu.Lock()
	defer d.mu.Unlock()
	for _, ts := range d.stmts {
		_ = ts.Close()
	}
	d.stmts = make(map[string]*tableStmts)
	if d.sqlDB != nil {
		return d.sqlDB.Close()
	}
	return nil
}

func (d *DB) ensureTableStmtsLocked(ctx context.Context, tableName string) (*tableStmts, error) {
	if ts, ok := d.stmts[tableName]; ok && ts != nil {
		return ts, nil
	}

	cctx := withoutCancel(ctx)
	ts := &tableStmts{}
	var err error
	defer func() {
		if err != nil {
			_ = ts.Close()
		}
	}()

	prepare := func(query string) (*sql.Stmt, error) {
		return d.sqlDB.PrepareContext(cctx, fmt.Sprintf(query, tableName))
	}

	if ts.get, err = prepare("SELECT valuedata FROM %q WHERE keydata = ?;"); err != nil {
		return nil, err
	}
	if ts.upsert, err = prepare("INSERT OR REPLACE INTO %q (keydata, valuedata) VALUES (?, ?);"); err != nil {
		return nil, err
	}
	if ts.del, err = prepare("DELETE FROM %q WHERE keydata = ?;"); err != nil {
		return nil, err
	}
	if ts.scanPrefix, err = prepare("SELECT keydata, valuedata FROM %q WHERE keydata >= ? AND keydata < ? ORDER BY keydata ASC;"); err != nil {
		return nil, err
	}
	if ts.scanPrefixLim, err = prepare("SELECT keydata, valuedata FROM %q WHERE keydata >= ? AND keydata < ? ORDER BY keydata ASC LIMIT ?;"); err != nil {
		return nil, err
	}
	if ts.scanGe, err = prepare("SELECT keydata, valuedata FROM %q WHERE keydata >= ? ORDER BY keydata ASC;"); err != nil {
		return nil, err
	}
	if ts.scanGeLim, err = prepare("SELECT keydata, valuedata FROM %q WHERE keydata >= ? ORDER BY keydata ASC LIMIT ?;"); err != nil {
		return nil, err
	}
	if ts.scanAll, err = prepare("SELECT keydata, valuedata FROM %q ORDER BY keydata ASC;"); err != nil {
		return nil, err
	}
	if ts.scanAllLim, err = prepare("SELECT keydata, valuedata FROM %q ORDER BY keydata ASC LIMIT ?;"); err != nil {
		return nil, err
	}
	if ts.count, err = prepare("SELECT COUNT(*) FROM %q;"); err != nil {
		return nil, err
	}

	d.stmts[tableName] = ts
	return ts, nil
}

func (d *DB) initBookkeeping(ctx context.Context) error {
	createTypesTable := `
CREATE TABLE IF NOT EXISTS _stream_types (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    fingerprint BLOB NOT NULL,
    descriptors BLOB,
    key_fields TEXT NOT NULL
);`
	if _, err := d.sqlDB.ExecContext(ctx, createTypesTable); err != nil {
		return fmt.Errorf("failed to create _stream_types table: %w", err)
	}

	createPosTable := `
CREATE TABLE IF NOT EXISTS _stream_position (
    stream_id TEXT PRIMARY KEY,
    position INTEGER NOT NULL,
    snapshot_time INTEGER NOT NULL
);`
	if _, err := d.sqlDB.ExecContext(ctx, createPosTable); err != nil {
		return fmt.Errorf("failed to create _stream_position table: %w", err)
	}

	return nil
}

func (d *DB) loadTypes(ctx context.Context) ([]*sdsv1.TypeDefinition, error) {
	rows, err := d.sqlDB.QueryContext(ctx, "SELECT id, name, fingerprint, descriptors, key_fields FROM _stream_types ORDER BY id ASC")
	if err != nil {
		return nil, fmt.Errorf("failed to query _stream_types: %w", err)
	}
	defer rows.Close()

	var defs []*sdsv1.TypeDefinition
	for rows.Next() {
		var id uint32
		var name string
		var fingerprint []byte
		var descBytes []byte
		var keyFieldsJSON string

		if err := rows.Scan(&id, &name, &fingerprint, &descBytes, &keyFieldsJSON); err != nil {
			return nil, fmt.Errorf("failed to scan _stream_types row: %w", err)
		}

		var keyFields []int32
		if err := json.Unmarshal([]byte(keyFieldsJSON), &keyFields); err != nil {
			return nil, fmt.Errorf("failed to parse key_fields for type ID %d: %w", id, err)
		}

		var fds *descriptorpb.FileDescriptorSet
		if len(descBytes) > 0 {
			fds = &descriptorpb.FileDescriptorSet{}
			if err := proto.Unmarshal(descBytes, fds); err != nil {
				return nil, fmt.Errorf("failed to unmarshal descriptors for type ID %d: %w", id, err)
			}
		}

		defs = append(defs, &sdsv1.TypeDefinition{
			Id:          id,
			Name:        name,
			Fingerprint: fingerprint,
			Descriptors: fds,
			KeyFields:   keyFields,
		})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error reading _stream_types: %w", err)
	}
	return defs, nil
}

func (d *DB) loadPosition(ctx context.Context) (string, uint64, bool, error) {
	rows, err := d.sqlDB.QueryContext(ctx, "SELECT stream_id, position FROM _stream_position LIMIT 1")
	if err != nil {
		return "", 0, false, fmt.Errorf("failed to query _stream_position: %w", err)
	}
	defer rows.Close()

	if rows.Next() {
		var sID string
		var pos uint64
		if err := rows.Scan(&sID, &pos); err != nil {
			return "", 0, false, fmt.Errorf("failed to scan _stream_position: %w", err)
		}
		return sID, pos, true, nil
	}
	return "", 0, false, nil
}

func (d *DB) loadState(ctx context.Context) error {
	defs, err := d.loadTypes(ctx)
	if err != nil {
		return err
	}
	for _, def := range defs {
		if err := d.changeReader.Registry().Register(def); err != nil {
			return fmt.Errorf("failed to register restored type %d (%s): %w", def.GetId(), def.GetName(), err)
		}
		tableName := TableName(def.GetName())
		d.knownTables[tableName] = true
		if _, err := d.ensureTableStmtsLocked(ctx, tableName); err != nil {
			return fmt.Errorf("failed to prepare statements for %s: %w", tableName, err)
		}
	}

	sID, pos, found, err := d.loadPosition(ctx)
	if err != nil {
		return err
	}
	if found {
		if d.streamID == "" {
			d.streamID = sID
		}
		d.position = pos
	}
	return nil
}

// Feed consumes a stream payload at sequence number seq, maintaining in-band schemas,
// grouping transactions, applying committed row changes, and updating _stream_position.
func (d *DB) Feed(ctx context.Context, seq uint64, payload []byte) ([]sds.Change, error) {
	d.mu.Lock()
	defer d.mu.Unlock()

	changes, err := d.changeReader.Feed(seq, payload)
	if err != nil {
		return nil, err
	}

	// Sync any newly registered or evolved types into SQLite tables and _stream_types
	regExport := d.changeReader.Registry().Export()
	for _, def := range regExport.GetTypes() {
		if err := d.ensureTableSchema(ctx, nil, def); err != nil {
			return nil, err
		}
	}

	if len(changes) == 0 {
		// No committed changes to apply for this record.
		return nil, nil
	}

	// Apply all committed changes and advance position in a single transaction
	tx, err := d.sqlDB.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer func() {
		_ = tx.Rollback()
	}()

	for _, ch := range changes {
		if err := d.applyChangeInTx(ctx, tx, ch); err != nil {
			return nil, err
		}
	}

	// Advance _stream_position
	if err := d.updatePositionInTx(ctx, tx, seq); err != nil {
		return nil, err
	}

	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("failed to commit transaction: %w", err)
	}

	d.position = seq
	return changes, nil
}

// Apply applies a single committed row change directly to the SQLite projection.
func (d *DB) Apply(ctx context.Context, change sds.Change) error {
	return d.ApplyBatch(ctx, []sds.Change{change})
}

// ApplyBatch applies a slice of committed row changes in a single SQLite transaction.
func (d *DB) ApplyBatch(ctx context.Context, changes []sds.Change) error {
	if len(changes) == 0 {
		return nil
	}

	d.mu.Lock()
	defer d.mu.Unlock()

	cctx := context.WithoutCancel(ctx)

	// Ensure schemas and prepared statements exist BEFORE opening transaction on the single connection
	for _, ch := range changes {
		def, _, ok := d.changeReader.Registry().LookupByID(ch.TypeID)
		if !ok && ch.TypeName != "" {
			def, _, ok = d.changeReader.Registry().LookupByName(ch.TypeName)
		}
		if ok {
			if err := d.ensureTableSchema(cctx, nil, def); err != nil {
				return err
			}
		}
	}

	tx, err := d.sqlDB.BeginTx(cctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer func() {
		_ = tx.Rollback()
	}()

	var maxSeq uint64 = d.position
	for _, ch := range changes {
		if err := d.applyChangeInTx(cctx, tx, ch); err != nil {
			return err
		}
		if ch.Seq > maxSeq {
			maxSeq = ch.Seq
		}
	}

	if maxSeq > d.position {
		if err := d.updatePositionInTx(cctx, tx, maxSeq); err != nil {
			return err
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	if maxSeq > d.position {
		d.position = maxSeq
	}
	return nil
}

func (d *DB) applyChangeInTx(ctx context.Context, tx *sql.Tx, ch sds.Change) error {
	cctx := context.WithoutCancel(ctx)
	def, _, ok := d.changeReader.Registry().LookupByID(ch.TypeID)
	if !ok && ch.TypeName != "" {
		def, _, ok = d.changeReader.Registry().LookupByName(ch.TypeName)
	}
	if !ok {
		return fmt.Errorf("%w: type ID %d (%s)", ErrTypeNotRegistered, ch.TypeID, ch.TypeName)
	}

	if err := d.ensureTableSchema(cctx, tx, def); err != nil {
		return err
	}

	tableName := TableName(def.GetName())
	ts := d.stmts[tableName]

	switch ch.Op {
	case sds.OpCreate, sds.OpUpdate:
		if ts != nil && ts.upsert != nil {
			if _, err := tx.StmtContext(cctx, ts.upsert).ExecContext(cctx, ch.RawKey, ch.RawVal); err != nil {
				return fmt.Errorf("failed to upsert row in %q: %w", tableName, err)
			}
		} else {
			query := fmt.Sprintf("INSERT OR REPLACE INTO %q (keydata, valuedata) VALUES (?, ?);", tableName)
			if _, err := tx.ExecContext(cctx, query, ch.RawKey, ch.RawVal); err != nil {
				return fmt.Errorf("failed to upsert row in %q: %w", tableName, err)
			}
		}

	case sds.OpDelete:
		if ts != nil && ts.del != nil {
			if _, err := tx.StmtContext(cctx, ts.del).ExecContext(cctx, ch.RawKey); err != nil {
				return fmt.Errorf("failed to delete row in %q: %w", tableName, err)
			}
		} else {
			query := fmt.Sprintf("DELETE FROM %q WHERE keydata = ?;", tableName)
			if _, err := tx.ExecContext(cctx, query, ch.RawKey); err != nil {
				return fmt.Errorf("failed to delete row in %q: %w", tableName, err)
			}
		}

	default:
		return fmt.Errorf("unsupported change op: %v", ch.Op)
	}

	return nil
}

func (d *DB) ensureTableSchema(ctx context.Context, tx *sql.Tx, def *sdsv1.TypeDefinition) error {
	cctx := context.WithoutCancel(ctx)
	tableName := TableName(def.GetName())

	var execer interface {
		ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	}
	if tx != nil {
		execer = tx
	} else {
		execer = d.sqlDB
	}

	// 1. Sync _stream_types table
	descBytes, err := proto.Marshal(def.GetDescriptors())
	if err != nil {
		return fmt.Errorf("failed to marshal descriptors for %d: %w", def.GetId(), err)
	}
	kfBytes, err := json.Marshal(def.GetKeyFields())
	if err != nil {
		return fmt.Errorf("failed to marshal key_fields for %d: %w", def.GetId(), err)
	}

	upsertTypeSQL := `INSERT OR REPLACE INTO _stream_types (id, name, fingerprint, descriptors, key_fields) VALUES (?, ?, ?, ?, ?);`
	if _, err := execer.ExecContext(cctx, upsertTypeSQL, def.GetId(), def.GetName(), def.GetFingerprint(), descBytes, string(kfBytes)); err != nil {
		return fmt.Errorf("failed to upsert _stream_types for %d: %w", def.GetId(), err)
	}

	// 2. Ensure table exists with (keydata BLOB PRIMARY KEY, valuedata BLOB)
	if !d.knownTables[tableName] {
		createSQL := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %q (
    keydata BLOB PRIMARY KEY,
    valuedata BLOB
);`, tableName)
		if _, err := execer.ExecContext(cctx, createSQL); err != nil {
			return fmt.Errorf("failed to create table %q: %w", tableName, err)
		}
		d.knownTables[tableName] = true
	}

	if tx == nil {
		if _, err := d.ensureTableStmtsLocked(cctx, tableName); err != nil {
			return err
		}
	}

	return nil
}

func (d *DB) updatePositionInTx(ctx context.Context, tx *sql.Tx, seq uint64) error {
	cctx := context.WithoutCancel(ctx)
	nowMicros := time.Now().UnixMicro()
	sqlStr := `INSERT OR REPLACE INTO _stream_position (stream_id, position, snapshot_time) VALUES (?, ?, ?);`
	if _, err := tx.ExecContext(cctx, sqlStr, d.streamID, seq, nowMicros); err != nil {
		return fmt.Errorf("failed to update _stream_position to %d: %w", seq, err)
	}
	return nil
}

func (d *DB) getTable(ctx context.Context, typeName string) (*sdsv1.TypeDefinition, *tableStmts, error) {
	def, _, ok := d.changeReader.Registry().LookupByName(typeName)
	if !ok {
		return nil, nil, fmt.Errorf("%w: type %q", ErrTypeNotRegistered, typeName)
	}

	tableName := TableName(typeName)

	d.mu.RLock()
	ts := d.stmts[tableName]
	d.mu.RUnlock()

	if ts == nil {
		cctx := withoutCancel(ctx)
		d.mu.Lock()
		var err error
		ts, err = d.ensureTableStmtsLocked(cctx, tableName)
		d.mu.Unlock()
		if err != nil {
			return nil, nil, fmt.Errorf("failed to prepare stmts for %q: %w", tableName, err)
		}
	}
	return def, ts, nil
}

// Get retrieves a merged proto row from a table by primary key.
func (d *DB) Get(ctx context.Context, typeName string, key sds.Key) (proto.Message, bool, error) {
	def, ts, err := d.getTable(ctx, typeName)
	if err != nil {
		return nil, false, err
	}

	cctx := withoutCancel(ctx)
	var valuedata []byte
	err = ts.get.QueryRowContext(cctx, key.Bytes()).Scan(&valuedata)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, fmt.Errorf("failed to query row from %q: %w", typeName, err)
	}

	msgType, err := d.changeReader.Registry().ResolveMessageType(def.GetId())
	if err != nil {
		return nil, false, fmt.Errorf("failed to resolve message type for %q: %w", typeName, err)
	}

	target := msgType.New().Interface()
	if err := sds.MergeKeyAndNonKey(target, key.Bytes(), valuedata); err != nil {
		return nil, false, fmt.Errorf("failed to merge proto key and value: %w", err)
	}

	return target, true, nil
}

// Count returns the number of rows in the specified table.
func (d *DB) Count(ctx context.Context, typeName string) (int, error) {
	_, ts, err := d.getTable(ctx, typeName)
	if err != nil {
		return 0, err
	}

	cctx := withoutCancel(ctx)
	var count int
	if err := ts.count.QueryRowContext(cctx).Scan(&count); err != nil {
		return 0, fmt.Errorf("failed to count rows in %q: %w", typeName, err)
	}
	return count, nil
}

// Tables returns the sorted list of registered table names present in the projection.
func (d *DB) Tables() []string {
	d.mu.RLock()
	defer d.mu.RUnlock()

	regExport := d.changeReader.Registry().Export()
	var tables []string
	for _, def := range regExport.GetTypes() {
		if def.GetId() >= 16 {
			tables = append(tables, def.GetName())
		}
	}
	sort.Strings(tables)
	return tables
}

// SyncRegistry imports types from reg into the DB's registry and ensures their schemas exist in SQLite.
func (d *DB) SyncRegistry(ctx context.Context, reg *record.Registry) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	if reg == nil {
		return nil
	}
	exported := reg.Export()
	if err := d.changeReader.Registry().Import(exported); err != nil {
		return err
	}
	cctx := withoutCancel(ctx)
	for _, def := range exported.GetTypes() {
		if err := d.ensureTableSchema(cctx, nil, def); err != nil {
			return err
		}
	}
	return nil
}

// RegisterType registers a Go proto.Message type with optional primary key field numbers
// and creates its table schema in SQLite.
func (d *DB) RegisterType(ctx context.Context, msg proto.Message, keyFields ...int32) (*sdsv1.TypeDefinition, error) {
	d.mu.Lock()
	defer d.mu.Unlock()

	def, err := d.changeReader.Registry().RegisterMessage(msg, keyFields...)
	if err != nil {
		return nil, err
	}
	cctx := withoutCancel(ctx)
	if err := d.ensureTableSchema(cctx, nil, def); err != nil {
		return nil, err
	}
	return def, nil
}

func prefixLimit(prefix []byte) []byte {
	limit := make([]byte, len(prefix))
	copy(limit, prefix)
	for i := len(limit) - 1; i >= 0; i-- {
		if limit[i] < 0xff {
			limit[i]++
			return limit[:i+1]
		}
	}
	return nil
}

// ScanLimit retrieves up to limit merged proto rows from a table matching a canonical key prefix,
// ordered by keydata ASC. If limit <= 0, all matching rows are returned.
// If keyPrefix is empty, all rows in the table (up to limit) are returned.
func (d *DB) ScanLimit(ctx context.Context, typeName string, keyPrefix []byte, limit int) ([]proto.Message, error) {
	def, ts, err := d.getTable(ctx, typeName)
	if err != nil {
		return nil, err
	}

	msgType, err := d.changeReader.Registry().ResolveMessageType(def.GetId())
	if err != nil {
		return nil, fmt.Errorf("failed to resolve message type for %q: %w", typeName, err)
	}

	cctx := withoutCancel(ctx)
	var rows *sql.Rows
	if len(keyPrefix) == 0 {
		if limit > 0 {
			rows, err = ts.scanAllLim.QueryContext(cctx, limit)
		} else {
			rows, err = ts.scanAll.QueryContext(cctx)
		}
	} else {
		limitBytes := prefixLimit(keyPrefix)
		if limitBytes != nil {
			if limit > 0 {
				rows, err = ts.scanPrefixLim.QueryContext(cctx, keyPrefix, limitBytes, limit)
			} else {
				rows, err = ts.scanPrefix.QueryContext(cctx, keyPrefix, limitBytes)
			}
		} else {
			if limit > 0 {
				rows, err = ts.scanGeLim.QueryContext(cctx, keyPrefix, limit)
			} else {
				rows, err = ts.scanGe.QueryContext(cctx, keyPrefix)
			}
		}
	}
	if err != nil {
		return nil, fmt.Errorf("failed to scan rows from %q: %w", typeName, err)
	}
	defer rows.Close()

	var result []proto.Message
	for rows.Next() {
		var keydata, valuedata []byte
		if err := rows.Scan(&keydata, &valuedata); err != nil {
			return nil, fmt.Errorf("failed to scan row: %w", err)
		}

		if len(keyPrefix) > 0 && !bytes.HasPrefix(keydata, keyPrefix) {
			continue
		}

		target := msgType.New().Interface()
		if err := sds.MergeKeyAndNonKey(target, keydata, valuedata); err != nil {
			return nil, fmt.Errorf("failed to merge proto key and value: %w", err)
		}
		result = append(result, target)
		if limit > 0 && len(result) >= limit {
			break
		}
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	return result, nil
}

// Scan retrieves merged proto rows from a table matching a canonical key prefix,
// ordered by keydata ASC. If keyPrefix is empty, it returns all rows in the table.
func (d *DB) Scan(ctx context.Context, typeName string, keyPrefix []byte) ([]proto.Message, error) {
	return d.ScanLimit(ctx, typeName, keyPrefix, 0)
}

// Rows returns all merged proto rows from the specified table.
func (d *DB) Rows(ctx context.Context, typeName string) ([]proto.Message, error) {
	return d.ScanLimit(ctx, typeName, nil, 0)
}
