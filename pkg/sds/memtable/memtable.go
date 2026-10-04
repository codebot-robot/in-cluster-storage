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

package memtable

import (
	"bytes"
	"context"
	"fmt"
	"iter"
	"sort"
	"sync"

	"github.com/gke-labs/in-cluster-storage/pkg/objectstore"
	"github.com/gke-labs/in-cluster-storage/pkg/sds"
	"github.com/gke-labs/in-cluster-storage/pkg/sds/record"
	"google.golang.org/protobuf/proto"
)

var (
	_ sds.LocalIndex   = (*MemStore)(nil)
	_ sds.Snapshotter  = (*MemStore)(nil)
	_ sds.IndexFactory = (*Factory)(nil)
)

// Factory implements sds.IndexFactory for MemStore memory indexes.
type Factory struct{}

func init() {
	sds.RegisterIndexFactory(&Factory{})
}

// Format returns the format identifier "memory".
func (f *Factory) Format() string {
	return "memory"
}

// OpenLocal returns (nil, false, nil) since MemStore is in-memory only.
func (f *Factory) OpenLocal(ctx context.Context, streamID string, dir string) (sds.LocalIndex, bool, error) {
	return nil, false, nil
}

// FindLatestSnapshot finds the latest MemStore snapshot in object storage.
func (f *Factory) FindLatestSnapshot(ctx context.Context, backend objectstore.Backend, streamID string, maxPos uint64) (string, uint64, error) {
	return FindLatestSnapshot(ctx, backend, streamID, maxPos)
}

// RestoreSnapshot restores a MemStore snapshot from object storage.
func (f *Factory) RestoreSnapshot(ctx context.Context, backend objectstore.Backend, streamID string, key string, dir string) (sds.LocalIndex, uint64, error) {
	store, pos, err := RestoreSnapshotKey(ctx, backend, streamID, key, dir)
	if err != nil {
		return nil, 0, err
	}
	return store, pos, nil
}

// NewEmpty creates a new empty MemStore.
func (f *Factory) NewEmpty(ctx context.Context, streamID string, dir string) (sds.LocalIndex, error) {
	return New(WithStreamID(streamID)), nil
}

// MemTable represents an in-memory table storing rows keyed by primary key,
// protected by its own read-write lock.
type MemTable struct {
	name string
	mu   sync.RWMutex
	rows map[sds.Key]proto.Message
}

// NewTable creates a new empty MemTable with the given name.
func NewTable(name string) *MemTable {
	return &MemTable{
		name: name,
		rows: make(map[sds.Key]proto.Message),
	}
}

// Name returns the name of the table.
func (t *MemTable) Name() string {
	return t.name
}

// Get retrieves a row message by primary key.
func (t *MemTable) Get(key sds.Key) (proto.Message, bool) {
	t.mu.RLock()
	defer t.mu.RUnlock()

	msg, ok := t.rows[key]
	if !ok {
		return nil, false
	}
	return proto.Clone(msg), true
}

// Put inserts or updates a row message by primary key.
func (t *MemTable) Put(key sds.Key, msg proto.Message) {
	t.mu.Lock()
	defer t.mu.Unlock()

	t.rows[key] = proto.Clone(msg)
}

// Delete removes a row by primary key.
func (t *MemTable) Delete(key sds.Key) {
	t.mu.Lock()
	defer t.mu.Unlock()

	delete(t.rows, key)
}

// Rows returns a slice of all row messages in the table, sorted in canonical key-byte order.
func (t *MemTable) Rows() []proto.Message {
	t.mu.RLock()
	defer t.mu.RUnlock()

	keys := make([]sds.Key, 0, len(t.rows))
	for k := range t.rows {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool {
		return bytes.Compare(keys[i].Bytes(), keys[j].Bytes()) < 0
	})

	rows := make([]proto.Message, 0, len(keys))
	for _, k := range keys {
		rows = append(rows, proto.Clone(t.rows[k]))
	}
	return rows
}

// Count returns the number of rows in the table.
func (t *MemTable) Count() int {
	t.mu.RLock()
	defer t.mu.RUnlock()

	return len(t.rows)
}

// Clear removes all rows from the table.
func (t *MemTable) Clear() {
	t.mu.Lock()
	defer t.mu.Unlock()

	t.rows = make(map[sds.Key]proto.Message)
}

// MemStore is an in-memory LocalIndex projection that applies committed
// stream changes into memory tables keyed by table name and primary key.
type MemStore struct {
	mu           sync.RWMutex
	streamID     string
	registry     *record.Registry
	tables       map[string]*MemTable
	lastPosition uint64
}

// Option configures a MemStore.
type Option func(*MemStore)

// WithStreamID sets the stream ID for the MemStore.
func WithStreamID(streamID string) Option {
	return func(m *MemStore) {
		m.streamID = streamID
	}
}

// New creates a new empty MemStore.
func New(opts ...Option) *MemStore {
	m := &MemStore{
		registry: record.NewRegistry(),
		tables:   make(map[string]*MemTable),
	}
	for _, opt := range opts {
		opt(m)
	}
	return m
}

// StreamID returns the stream ID.
func (m *MemStore) StreamID() string {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.streamID
}

// SetStreamID sets the stream ID.
func (m *MemStore) SetStreamID(streamID string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.streamID = streamID
}

// Position returns the stream sequence position of the last applied change.
func (m *MemStore) Position() uint64 {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.lastPosition
}

// SetPosition sets the current stream sequence position.
func (m *MemStore) SetPosition(pos uint64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.lastPosition = pos
}

// Registry returns the in-band type registry.
func (m *MemStore) Registry() *record.Registry {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.registry
}

// SyncRegistry registers type definitions in force with the MemStore.
func (m *MemStore) SyncRegistry(ctx context.Context, reg *record.Registry) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if reg == nil {
		return nil
	}
	if m.registry == nil {
		m.registry = record.NewRegistry()
	}
	return m.registry.Import(reg.Export())
}

// Table returns the MemTable with the given name, or nil if not found.
func (m *MemStore) Table(name string) *MemTable {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.tables[name]
}

// GetOrCreateTable returns the MemTable with the given name, creating it if necessary.
func (m *MemStore) GetOrCreateTable(name string) *MemTable {
	m.mu.Lock()
	defer m.mu.Unlock()

	table, ok := m.tables[name]
	if !ok {
		table = NewTable(name)
		m.tables[name] = table
	}
	return table
}

// Apply applies a single committed row change to the projection.
func (m *MemStore) Apply(change sds.Change) error {
	return m.ApplyBatch(context.Background(), []sds.Change{change})
}

// ApplyBatch atomically applies a slice of committed row changes in order.
func (m *MemStore) ApplyBatch(ctx context.Context, changes []sds.Change) error {
	if len(changes) == 0 {
		return nil
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	for _, change := range changes {
		tableName := change.TypeName
		if tableName == "" && m.registry != nil {
			if def, _, ok := m.registry.LookupByID(change.TypeID); ok {
				tableName = def.GetName()
			}
		}
		if tableName == "" {
			return fmt.Errorf("empty table name in change")
		}

		table, ok := m.tables[tableName]
		if !ok {
			table = NewTable(tableName)
			m.tables[tableName] = table
		}

		key := change.Key
		if key.IsZero() && len(change.RawKey) > 0 {
			key = sds.NewKeyFromBytes(change.RawKey)
		}

		switch change.Op {
		case sds.OpCreate, sds.OpUpdate:
			row := change.Row
			if row == nil && m.registry != nil && (len(change.RawKey) > 0 || len(change.RawVal) > 0) {
				def, _, ok := m.registry.LookupByName(tableName)
				if !ok {
					def, _, ok = m.registry.LookupByID(change.TypeID)
				}
				if ok {
					msgType, err := m.registry.ResolveMessageType(def.GetId())
					if err == nil {
						target := msgType.New().Interface()
						if err := sds.MergeKeyAndNonKey(target, change.RawKey, change.RawVal); err == nil {
							row = target
						}
					}
				}
			}
			if row == nil {
				return fmt.Errorf("missing row message for %v", change.Op)
			}
			table.Put(key, row)
		case sds.OpDelete:
			table.Delete(key)
		default:
			return fmt.Errorf("unsupported op %v", change.Op)
		}

		if change.Seq > m.lastPosition {
			m.lastPosition = change.Seq
		}
	}
	return nil
}

// Get retrieves a row message by table name and primary key.
func (m *MemStore) Get(ctx context.Context, tableName string, key sds.Key) (proto.Message, bool, error) {
	table := m.Table(tableName)
	if table == nil {
		return nil, false, nil
	}
	msg, ok := table.Get(key)
	return msg, ok, nil
}

// Scan yields merged proto rows matching keyPrefix in canonical key-byte order.
func (m *MemStore) Scan(ctx context.Context, typeName string, keyPrefix []byte) iter.Seq2[proto.Message, error] {
	return func(yield func(proto.Message, error) bool) {
		table := m.Table(typeName)
		if table == nil {
			return
		}

		table.mu.RLock()
		type keyEntry struct {
			key sds.Key
			msg proto.Message
		}
		var entries []keyEntry
		for k, msg := range table.rows {
			if len(keyPrefix) == 0 || bytes.HasPrefix(k.Bytes(), keyPrefix) {
				entries = append(entries, keyEntry{key: k, msg: proto.Clone(msg)})
			}
		}
		table.mu.RUnlock()

		sort.Slice(entries, func(i, j int) bool {
			return bytes.Compare(entries[i].key.Bytes(), entries[j].key.Bytes()) < 0
		})

		for _, e := range entries {
			if !yield(e.msg, nil) {
				return
			}
		}
	}
}

// ScanSlice returns a slice of all merged proto rows matching keyPrefix in canonical key-byte order.
func (m *MemStore) ScanSlice(ctx context.Context, typeName string, keyPrefix []byte) ([]proto.Message, error) {
	var result []proto.Message
	for msg, err := range m.Scan(ctx, typeName, keyPrefix) {
		if err != nil {
			return nil, err
		}
		result = append(result, msg)
	}
	return result, nil
}

// Rows returns a slice of all row messages in the specified table in canonical key-byte order.
func (m *MemStore) Rows(tableName string) []proto.Message {
	res, _ := m.ScanSlice(context.Background(), tableName, nil)
	return res
}

// Count returns the number of rows in the specified table.
func (m *MemStore) Count(tableName string) int {
	table := m.Table(tableName)
	if table == nil {
		return 0
	}
	return table.Count()
}

// Tables returns a sorted slice of all table names currently present.
func (m *MemStore) Tables() []string {
	m.mu.RLock()
	defer m.mu.RUnlock()

	tables := make([]string, 0, len(m.tables))
	for name := range m.tables {
		tables = append(tables, name)
	}
	sort.Strings(tables)
	return tables
}

// Clear removes all tables and rows from the projection.
func (m *MemStore) Clear() {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.tables = make(map[string]*MemTable)
	m.lastPosition = 0
}

// PublishSnapshot creates an atomic snapshot of the MemStore and uploads it to object storage.
func (m *MemStore) PublishSnapshot(ctx context.Context, backend objectstore.Backend) (string, uint64, error) {
	return PublishSnapshot(ctx, m, backend, "")
}

// Close closes the MemStore (no-op for memory index).
func (m *MemStore) Close() error {
	return nil
}
