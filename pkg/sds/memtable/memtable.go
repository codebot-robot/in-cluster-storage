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
	"fmt"
	"sort"
	"sync"

	"github.com/gke-labs/in-cluster-storage/pkg/sds"
	"google.golang.org/protobuf/proto"
)

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

// Rows returns a slice of all row messages in the table.
func (t *MemTable) Rows() []proto.Message {
	t.mu.RLock()
	defer t.mu.RUnlock()

	rows := make([]proto.Message, 0, len(t.rows))
	for _, msg := range t.rows {
		rows = append(rows, proto.Clone(msg))
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

// MemStore is a reference in-memory projection that applies committed
// stream changes into memory tables keyed by table name and primary key.
type MemStore struct {
	mu           sync.RWMutex
	tables       map[string]*MemTable
	lastPosition uint64
}

// New creates a new empty MemStore.
func New() *MemStore {
	return &MemStore{
		tables: make(map[string]*MemTable),
	}
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
	tableName := change.TypeName
	if tableName == "" {
		return fmt.Errorf("empty table name in change")
	}

	table := m.GetOrCreateTable(tableName)

	switch change.Op {
	case sds.OpCreate, sds.OpUpdate:
		if change.Row == nil {
			return fmt.Errorf("missing row message for %v", change.Op)
		}
		table.Put(change.Key, change.Row)
	case sds.OpDelete:
		table.Delete(change.Key)
	default:
		return fmt.Errorf("unsupported op %v", change.Op)
	}

	m.mu.Lock()
	if change.Seq > m.lastPosition {
		m.lastPosition = change.Seq
	}
	m.mu.Unlock()
	return nil
}

// ApplyBatch applies a slice of committed changes in order.
func (m *MemStore) ApplyBatch(changes []sds.Change) error {
	for _, ch := range changes {
		if err := m.Apply(ch); err != nil {
			return err
		}
	}
	return nil
}

// Get retrieves a row message by table name and primary key.
func (m *MemStore) Get(tableName string, key sds.Key) (proto.Message, bool) {
	table := m.Table(tableName)
	if table == nil {
		return nil, false
	}
	return table.Get(key)
}

// Rows returns a slice of all row messages in the specified table.
func (m *MemStore) Rows(tableName string) []proto.Message {
	table := m.Table(tableName)
	if table == nil {
		return nil
	}
	return table.Rows()
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
