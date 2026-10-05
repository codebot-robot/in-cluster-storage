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

package sqlite_test

import (
	"bytes"
	"context"
	"fmt"
	"math/rand"
	"path/filepath"
	"sync"
	"testing"

	"github.com/gke-labs/in-cluster-storage/pkg/objectstore/inmemorystorage"
	"github.com/gke-labs/in-cluster-storage/pkg/sds"
	"github.com/gke-labs/in-cluster-storage/pkg/sds/memtable"
	"github.com/gke-labs/in-cluster-storage/pkg/sds/projection/sqlite"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/dynamicpb"
)

type memoryAppender struct {
	mu       sync.Mutex
	payloads [][]byte
	seq      uint64
}

func (m *memoryAppender) Append(_ context.Context, payload []byte) (uint64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.seq++
	cp := make([]byte, len(payload))
	copy(cp, payload)
	m.payloads = append(m.payloads, cp)
	return m.seq, nil
}

func (m *memoryAppender) Payloads() [][]byte {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([][]byte, len(m.payloads))
	copy(out, m.payloads)
	return out
}

func field(name string, number int32, typeKind descriptorpb.FieldDescriptorProto_Type, label descriptorpb.FieldDescriptorProto_Label) *descriptorpb.FieldDescriptorProto {
	return &descriptorpb.FieldDescriptorProto{
		Name:   proto.String(name),
		Number: proto.Int32(number),
		Type:   typeKind.Enum(),
		Label:  label.Enum(),
	}
}

func buildMD(t *testing.T, name string, fields []*descriptorpb.FieldDescriptorProto) protoreflect.MessageDescriptor {
	t.Helper()
	msgProto := &descriptorpb.DescriptorProto{
		Name:  proto.String(name),
		Field: fields,
	}

	mainFile := &descriptorpb.FileDescriptorProto{
		Name:        proto.String(name + ".proto"),
		Package:     proto.String("testpkg"),
		MessageType: []*descriptorpb.DescriptorProto{msgProto},
		Syntax:      proto.String("proto3"),
	}

	fds := &descriptorpb.FileDescriptorSet{
		File: []*descriptorpb.FileDescriptorProto{mainFile},
	}

	files, err := protodesc.NewFiles(fds)
	if err != nil {
		t.Fatalf("protodesc.NewFiles failed: %v", err)
	}

	d, err := files.FindDescriptorByName(protoreflect.FullName("testpkg." + name))
	if err != nil {
		t.Fatalf("FindDescriptorByName failed: %v", err)
	}
	return d.(protoreflect.MessageDescriptor)
}

func readAllSQLiteRows(ctx context.Context, t *testing.T, db *sqlite.DB, sqlTableName, fullTableName string) map[string]proto.Message {
	t.Helper()
	querySQL := fmt.Sprintf("SELECT keydata, valuedata FROM %q;", sqlTableName)
	rows, err := db.SQLDB().QueryContext(ctx, querySQL)
	if err != nil {
		t.Fatalf("failed to query rows from %q: %v", sqlTableName, err)
	}
	defer rows.Close()

	sqliteRows := make(map[string]proto.Message)
	for rows.Next() {
		var keydata, valuedata []byte
		if err := rows.Scan(&keydata, &valuedata); err != nil {
			t.Fatalf("failed to scan row for %q: %v", sqlTableName, err)
		}

		def, _, ok := db.Registry().LookupByName(fullTableName)
		if !ok {
			t.Fatalf("type not found for table %q in registry", fullTableName)
		}

		msgType, err := db.Registry().ResolveMessageType(def.GetId())
		if err != nil {
			t.Fatalf("failed to resolve message type for %q: %v", fullTableName, err)
		}

		msg := msgType.New().Interface()
		if err := sds.MergeKeyAndNonKey(msg, keydata, valuedata); err != nil {
			t.Fatalf("failed to merge proto key and value for %q: %v", fullTableName, err)
		}

		key, err := sds.ExtractKey(msg, def.GetKeyFields())
		if err != nil {
			t.Fatalf("failed to extract key from sqlite row: %v", err)
		}

		sqliteRows[key.String()] = msg
	}
	return sqliteRows
}

func verifySQLiteMatchesMemStore(t *testing.T, ctx context.Context, db *sqlite.DB, store *memtable.MemStore) {
	t.Helper()

	if db.Position() != store.Position() {
		t.Errorf("position mismatch: sqlite %d != memstore %d", db.Position(), store.Position())
	}

	tables := store.Tables()
	for _, fullTableName := range tables {
		memTable := store.Table(fullTableName)
		sqlTableName := sqlite.TableName(fullTableName)

		// Check row count in SQLite
		var count int
		countSQL := fmt.Sprintf("SELECT COUNT(*) FROM %q;", sqlTableName)
		if err := db.SQLDB().QueryRowContext(ctx, countSQL).Scan(&count); err != nil {
			t.Fatalf("failed to query count for %q: %v", sqlTableName, err)
		}

		if count != memTable.Count() {
			t.Errorf("table %q row count mismatch: sqlite %d != memstore %d", fullTableName, count, memTable.Count())
		}

		// Read all (keydata, valuedata) rows from SQLite
		sqliteRows := readAllSQLiteRows(ctx, t, db, sqlTableName, fullTableName)

		// Compare with memtable rows
		for _, memRow := range memTable.Rows() {
			def, _, _ := db.Registry().LookupByName(fullTableName)
			key, err := sds.ExtractKey(memRow, def.GetKeyFields())
			if err != nil {
				t.Fatalf("failed to extract key from memtable row: %v", err)
			}

			sqlRow, ok := sqliteRows[key.String()]
			if !ok {
				t.Errorf("row with key %s in memtable missing from sqlite table %q", key.String(), fullTableName)
				continue
			}

			memBytes, err := proto.MarshalOptions{Deterministic: true}.Marshal(memRow)
			if err != nil {
				t.Fatalf("failed to marshal memtable row: %v", err)
			}
			sqlBytes, err := proto.MarshalOptions{Deterministic: true}.Marshal(sqlRow)
			if err != nil {
				t.Fatalf("failed to marshal sqlite row: %v", err)
			}

			if !bytes.Equal(memBytes, sqlBytes) {
				t.Errorf("row mismatch for key %s in table %q:\n  memstore: %v\n  sqlite:   %v", key.String(), fullTableName, memRow, sqlRow)
			}

			// Also verify db.Get convenience method
			getMsg, getOk, getErr := db.Get(ctx, fullTableName, key)
			if getErr != nil {
				t.Errorf("db.Get error for %s: %v", key.String(), getErr)
			} else if !getOk {
				t.Errorf("db.Get returned false for %s", key.String())
			} else {
				getBytes, _ := proto.MarshalOptions{Deterministic: true}.Marshal(getMsg)
				if !bytes.Equal(memBytes, getBytes) {
					t.Errorf("db.Get mismatch for key %s", key.String())
				}
			}
		}
	}
}

func TestReplayAndCompareWithMemTable(t *testing.T) {
	ctx := t.Context()
	appender := &memoryAppender{}
	writer := sds.NewWriter(appender)

	userMD := buildMD(t, "User", []*descriptorpb.FieldDescriptorProto{
		field("id", 1, descriptorpb.FieldDescriptorProto_TYPE_INT64, descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL),
		field("name", 2, descriptorpb.FieldDescriptorProto_TYPE_STRING, descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL),
		field("email", 3, descriptorpb.FieldDescriptorProto_TYPE_STRING, descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL),
	})
	writer.RegisterDescriptor(userMD, 1)

	orderMD := buildMD(t, "Order", []*descriptorpb.FieldDescriptorProto{
		field("id", 1, descriptorpb.FieldDescriptorProto_TYPE_INT64, descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL),
		field("user_id", 2, descriptorpb.FieldDescriptorProto_TYPE_INT64, descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL),
		field("amount", 3, descriptorpb.FieldDescriptorProto_TYPE_DOUBLE, descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL),
	})
	writer.RegisterDescriptor(orderMD, 1)

	// Step 1: Autocommit Inserts
	u1 := dynamicpb.NewMessage(userMD)
	u1.Set(userMD.Fields().ByName("id"), protoreflect.ValueOfInt64(1))
	u1.Set(userMD.Fields().ByName("name"), protoreflect.ValueOfString("Alice"))
	u1.Set(userMD.Fields().ByName("email"), protoreflect.ValueOfString("alice@example.com"))
	writer.Create(ctx, u1)

	u2 := dynamicpb.NewMessage(userMD)
	u2.Set(userMD.Fields().ByName("id"), protoreflect.ValueOfInt64(2))
	u2.Set(userMD.Fields().ByName("name"), protoreflect.ValueOfString("Bob"))
	u2.Set(userMD.Fields().ByName("email"), protoreflect.ValueOfString("bob@example.com"))
	writer.Create(ctx, u2)

	// Step 2: Multi-record Transaction
	tx := writer.Begin()
	o1 := dynamicpb.NewMessage(orderMD)
	o1.Set(orderMD.Fields().ByName("id"), protoreflect.ValueOfInt64(101))
	o1.Set(orderMD.Fields().ByName("user_id"), protoreflect.ValueOfInt64(1))
	o1.Set(orderMD.Fields().ByName("amount"), protoreflect.ValueOfFloat64(99.50))
	tx.Create(ctx, o1)

	u1Up := dynamicpb.NewMessage(userMD)
	u1Up.Set(userMD.Fields().ByName("id"), protoreflect.ValueOfInt64(1))
	u1Up.Set(userMD.Fields().ByName("name"), protoreflect.ValueOfString("Alice Wonderland"))
	u1Up.Set(userMD.Fields().ByName("email"), protoreflect.ValueOfString("alice.w@example.com"))
	tx.Update(ctx, u1, u1Up)
	tx.Commit(ctx)

	// Step 3: Autocommit Delete
	writer.Delete(ctx, u2)

	// Feed all payloads to both MemStore and SQLite
	payloads := appender.Payloads()

	memStore := memtable.New()
	memReader := sds.NewChangeReader()

	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.sqlite")
	sqliteDB, err := sqlite.Open(ctx, dbPath, sqlite.WithStreamID("test-stream-1"))
	if err != nil {
		t.Fatalf("sqlite.Open failed: %v", err)
	}
	defer sqliteDB.Close()

	for i, payload := range payloads {
		seq := uint64(i + 1)
		changes, err := memReader.Feed(seq, payload)
		if err != nil {
			t.Fatalf("memReader.Feed failed: %v", err)
		}
		if err := memStore.ApplyBatch(ctx, changes); err != nil {
			t.Fatalf("memStore.ApplyBatch failed: %v", err)
		}

		if _, err := sqliteDB.Feed(ctx, seq, payload); err != nil {
			t.Fatalf("sqliteDB.Feed failed: %v", err)
		}
	}

	// Verify SQLite exactly matches MemStore
	verifySQLiteMatchesMemStore(t, ctx, sqliteDB, memStore)
}

func TestSnapshotRestoreAndTailFollow(t *testing.T) {
	ctx := t.Context()
	appender := &memoryAppender{}
	writer := sds.NewWriter(appender)
	backend := inmemorystorage.New()
	streamID := "stream-snap-test"

	itemMD := buildMD(t, "Item", []*descriptorpb.FieldDescriptorProto{
		field("id", 1, descriptorpb.FieldDescriptorProto_TYPE_INT64, descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL),
		field("val", 2, descriptorpb.FieldDescriptorProto_TYPE_STRING, descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL),
	})
	writer.RegisterDescriptor(itemMD, 1)

	// Record 1..10
	for i := 1; i <= 10; i++ {
		item := dynamicpb.NewMessage(itemMD)
		item.Set(itemMD.Fields().ByName("id"), protoreflect.ValueOfInt64(int64(i)))
		item.Set(itemMD.Fields().ByName("val"), protoreflect.ValueOfString(fmt.Sprintf("item-%d", i)))
		writer.Create(ctx, item)
	}

	payloadsPart1 := appender.Payloads()
	snapKey, snapPos, err := sqlite.BuildSnapshot(ctx, streamID, payloadsPart1, backend, t.TempDir())
	if err != nil {
		t.Fatalf("BuildSnapshot failed: %v", err)
	}
	if snapPos != uint64(len(payloadsPart1)) {
		t.Errorf("snapPos = %d, want %d", snapPos, len(payloadsPart1))
	}

	// Record 11..20 (live tail)
	for i := 11; i <= 20; i++ {
		item := dynamicpb.NewMessage(itemMD)
		item.Set(itemMD.Fields().ByName("id"), protoreflect.ValueOfInt64(int64(i)))
		item.Set(itemMD.Fields().ByName("val"), protoreflect.ValueOfString(fmt.Sprintf("item-%d", i)))
		writer.Create(ctx, item)
	}

	allPayloads := appender.Payloads()

	// Compute reference state in MemStore
	memStore := memtable.New()
	memReader := sds.NewChangeReader()
	for i, p := range allPayloads {
		changes, _ := memReader.Feed(uint64(i+1), p)
		memStore.ApplyBatch(ctx, changes)
	}

	// Restore SQLite projection from snapshot
	restoredPath := filepath.Join(t.TempDir(), "restored.sqlite")
	restoredDB, restoredPos, err := sqlite.RestoreSnapshot(ctx, backend, streamID, snapPos, restoredPath)
	if err != nil {
		t.Fatalf("RestoreSnapshot failed: %v", err)
	}
	defer restoredDB.Close()

	if restoredPos != snapPos {
		t.Errorf("restoredPos = %d, want %d", restoredPos, snapPos)
	}

	// Apply live tail (records from snapPos to end)
	for i := int(snapPos); i < len(allPayloads); i++ {
		seq := uint64(i + 1)
		if _, err := restoredDB.Feed(ctx, seq, allPayloads[i]); err != nil {
			t.Fatalf("restoredDB.Feed tail at seq %d failed: %v", seq, err)
		}
	}

	// Restored + replayed state must equal full reference
	verifySQLiteMatchesMemStore(t, ctx, restoredDB, memStore)

	// Test publishing snapshot from active DB
	pubKey, pubPos, err := sqlite.PublishSnapshot(ctx, restoredDB, backend, t.TempDir())
	if err != nil {
		t.Fatalf("PublishSnapshot failed: %v", err)
	}
	if pubPos != uint64(len(allPayloads)) {
		t.Errorf("pubPos = %d, want %d", pubPos, len(allPayloads))
	}
	if pubKey != sqlite.SnapshotKey(streamID, pubPos) {
		t.Errorf("pubKey = %q, want %q", pubKey, sqlite.SnapshotKey(streamID, pubPos))
	}

	_ = snapKey
}

func TestCompatibleSchemaEvolution(t *testing.T) {
	ctx := t.Context()
	appender := &memoryAppender{}
	writer := sds.NewWriter(appender)

	// Schema V1: Order (id tag 1, customer tag 2)
	fieldsV1 := []*descriptorpb.FieldDescriptorProto{
		field("id", 1, descriptorpb.FieldDescriptorProto_TYPE_INT64, descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL),
		field("customer", 2, descriptorpb.FieldDescriptorProto_TYPE_STRING, descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL),
	}
	mdV1 := buildMD(t, "Order", fieldsV1)
	writer.RegisterDescriptor(mdV1, 1)

	// Insert row under V1
	o1 := dynamicpb.NewMessage(mdV1)
	o1.Set(mdV1.Fields().ByName("id"), protoreflect.ValueOfInt64(101))
	o1.Set(mdV1.Fields().ByName("customer"), protoreflect.ValueOfString("Alice"))
	writer.Create(ctx, o1)

	// Schema V2: Order evolved with total tag 3, discount tag 4
	fieldsV2 := []*descriptorpb.FieldDescriptorProto{
		field("id", 1, descriptorpb.FieldDescriptorProto_TYPE_INT64, descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL),
		field("customer", 2, descriptorpb.FieldDescriptorProto_TYPE_STRING, descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL),
		field("total", 3, descriptorpb.FieldDescriptorProto_TYPE_DOUBLE, descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL),
		field("discount", 4, descriptorpb.FieldDescriptorProto_TYPE_DOUBLE, descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL),
	}
	mdV2 := buildMD(t, "Order", fieldsV2)
	writer.RegisterDescriptor(mdV2, 1)

	// Insert row under V2
	o2 := dynamicpb.NewMessage(mdV2)
	o2.Set(mdV2.Fields().ByName("id"), protoreflect.ValueOfInt64(102))
	o2.Set(mdV2.Fields().ByName("customer"), protoreflect.ValueOfString("Bob"))
	o2.Set(mdV2.Fields().ByName("total"), protoreflect.ValueOfFloat64(150.00))
	o2.Set(mdV2.Fields().ByName("discount"), protoreflect.ValueOfFloat64(10.00))
	writer.Create(ctx, o2)

	// Update row 101 under V2
	o1Up := dynamicpb.NewMessage(mdV2)
	o1Up.Set(mdV2.Fields().ByName("id"), protoreflect.ValueOfInt64(101))
	o1Up.Set(mdV2.Fields().ByName("customer"), protoreflect.ValueOfString("Alice"))
	o1Up.Set(mdV2.Fields().ByName("total"), protoreflect.ValueOfFloat64(75.00))
	o1Up.Set(mdV2.Fields().ByName("discount"), protoreflect.ValueOfFloat64(5.00))
	writer.Update(ctx, o1, o1Up)

	// Feed all payloads to SQLite
	dbPath := filepath.Join(t.TempDir(), "evolved.sqlite")
	db, err := sqlite.Open(ctx, dbPath, sqlite.WithStreamID("stream-evolve"))
	if err != nil {
		t.Fatalf("sqlite.Open failed: %v", err)
	}
	defer db.Close()

	for i, p := range appender.Payloads() {
		seq := uint64(i + 1)
		if _, err := db.Feed(ctx, seq, p); err != nil {
			t.Fatalf("Feed at seq %d failed: %v", seq, err)
		}
	}

	// Verify row count in SQLite table
	count, err := db.Count(ctx, "testpkg.Order")
	if err != nil {
		t.Fatalf("Count failed: %v", err)
	}
	if count != 2 {
		t.Errorf("expected 2 orders, got %d", count)
	}

	// Verify evolved row contents using Get
	k101, _ := sds.ExtractKey(o1Up, []int32{1})
	msg101, ok, err := db.Get(ctx, "testpkg.Order", k101)
	if err != nil || !ok {
		t.Fatalf("Get k101 failed: ok=%v, err=%v", ok, err)
	}
	dyn101 := msg101.(*dynamicpb.Message)
	if dyn101.Get(dyn101.Descriptor().Fields().ByName("total")).Float() != 75.00 {
		t.Errorf("order 101 total = %v, want 75.00", dyn101.Get(dyn101.Descriptor().Fields().ByName("total")).Float())
	}
	if dyn101.Get(dyn101.Descriptor().Fields().ByName("discount")).Float() != 5.00 {
		t.Errorf("order 101 discount = %v, want 5.00", dyn101.Get(dyn101.Descriptor().Fields().ByName("discount")).Float())
	}

	k102, _ := sds.ExtractKey(o2, []int32{1})
	msg102, ok, err := db.Get(ctx, "testpkg.Order", k102)
	if err != nil || !ok {
		t.Fatalf("Get k102 failed: ok=%v, err=%v", ok, err)
	}
	dyn102 := msg102.(*dynamicpb.Message)
	if dyn102.Get(dyn102.Descriptor().Fields().ByName("total")).Float() != 150.00 {
		t.Errorf("order 102 total = %v, want 150.00", dyn102.Get(dyn102.Descriptor().Fields().ByName("total")).Float())
	}
}

func TestIdempotentReapplySuffix(t *testing.T) {
	ctx := t.Context()
	appender := &memoryAppender{}
	writer := sds.NewWriter(appender)

	entityMD := buildMD(t, "Entity", []*descriptorpb.FieldDescriptorProto{
		field("id", 1, descriptorpb.FieldDescriptorProto_TYPE_INT64, descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL),
		field("data", 2, descriptorpb.FieldDescriptorProto_TYPE_STRING, descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL),
	})
	writer.RegisterDescriptor(entityMD, 1)

	// Write 5 entities
	entities := make([]*dynamicpb.Message, 6)
	for i := 1; i <= 5; i++ {
		e := dynamicpb.NewMessage(entityMD)
		e.Set(entityMD.Fields().ByName("id"), protoreflect.ValueOfInt64(int64(i)))
		e.Set(entityMD.Fields().ByName("data"), protoreflect.ValueOfString(fmt.Sprintf("initial-%d", i)))
		writer.Create(ctx, e)
		entities[i] = e
	}

	// Update entity 3 and delete entity 4
	e3Up := dynamicpb.NewMessage(entityMD)
	e3Up.Set(entityMD.Fields().ByName("id"), protoreflect.ValueOfInt64(3))
	e3Up.Set(entityMD.Fields().ByName("data"), protoreflect.ValueOfString("updated-3"))
	writer.Update(ctx, entities[3], e3Up)

	writer.Delete(ctx, entities[4])

	payloads := appender.Payloads()

	// Initial apply of all records
	dbPath := filepath.Join(t.TempDir(), "idempotent.sqlite")
	db, err := sqlite.Open(ctx, dbPath, sqlite.WithStreamID("stream-idem"))
	if err != nil {
		t.Fatalf("sqlite.Open failed: %v", err)
	}
	defer db.Close()

	for i, p := range payloads {
		seq := uint64(i + 1)
		if _, err := db.Feed(ctx, seq, p); err != nil {
			t.Fatalf("Feed failed: %v", err)
		}
	}

	// Reference memstore
	memStore := memtable.New()
	memReader := sds.NewChangeReader()
	for i, p := range payloads {
		changes, _ := memReader.Feed(uint64(i+1), p)
		memStore.ApplyBatch(ctx, changes)
	}

	verifySQLiteMatchesMemStore(t, ctx, db, memStore)

	// Re-apply the entire suffix (from record 3 onward) using a fresh reader
	suffixReader := sds.NewChangeReader()
	for i := 0; i < len(payloads); i++ {
		seq := uint64(i + 1)
		changes, err := suffixReader.Feed(seq, payloads[i])
		if err != nil {
			t.Fatalf("suffixReader.Feed failed: %v", err)
		}
		if i >= 3 {
			// Re-apply changes to SQLite
			if err := db.ApplyBatch(ctx, changes); err != nil {
				t.Fatalf("ApplyBatch re-apply failed: %v", err)
			}
		}
	}

	// State must be completely unchanged and match memStore
	verifySQLiteMatchesMemStore(t, ctx, db, memStore)
}

func TestRandomLogReplayConformance(t *testing.T) {
	ctx := t.Context()
	appender := &memoryAppender{}
	writer := sds.NewWriter(appender)

	accountMD := buildMD(t, "Account", []*descriptorpb.FieldDescriptorProto{
		field("account_id", 1, descriptorpb.FieldDescriptorProto_TYPE_INT64, descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL),
		field("holder", 2, descriptorpb.FieldDescriptorProto_TYPE_STRING, descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL),
		field("balance", 3, descriptorpb.FieldDescriptorProto_TYPE_DOUBLE, descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL),
	})
	writer.RegisterDescriptor(accountMD, 1)

	r := rand.New(rand.NewSource(42))
	accounts := make(map[int64]bool)
	accountMsgs := make(map[int64]*dynamicpb.Message)

	// Generate 100 random operations (autocommit & transactions)
	for opIdx := 0; opIdx < 100; opIdx++ {
		isTx := r.Float64() < 0.3
		var tx *sds.Tx
		if isTx {
			tx = writer.Begin()
		}

		numOpsInGroup := 1
		if isTx {
			numOpsInGroup = r.Intn(4) + 1
		}

		for g := 0; g < numOpsInGroup; g++ {
			id := int64(r.Intn(20) + 1)
			exists := accounts[id]

			if !exists {
				// Insert
				acc := dynamicpb.NewMessage(accountMD)
				acc.Set(accountMD.Fields().ByName("account_id"), protoreflect.ValueOfInt64(id))
				acc.Set(accountMD.Fields().ByName("holder"), protoreflect.ValueOfString(fmt.Sprintf("User-%d", id)))
				acc.Set(accountMD.Fields().ByName("balance"), protoreflect.ValueOfFloat64(float64(r.Intn(1000))))
				if isTx {
					tx.Create(ctx, acc)
				} else {
					writer.Create(ctx, acc)
				}
				accounts[id] = true
				accountMsgs[id] = acc
			} else {
				if r.Float64() < 0.7 {
					// Update
					oldAcc := accountMsgs[id]
					acc := dynamicpb.NewMessage(accountMD)
					acc.Set(accountMD.Fields().ByName("account_id"), protoreflect.ValueOfInt64(id))
					acc.Set(accountMD.Fields().ByName("holder"), protoreflect.ValueOfString(fmt.Sprintf("User-%d-up", id)))
					acc.Set(accountMD.Fields().ByName("balance"), protoreflect.ValueOfFloat64(float64(r.Intn(1000))))
					if isTx {
						tx.Update(ctx, oldAcc, acc)
					} else {
						writer.Update(ctx, oldAcc, acc)
					}
					accountMsgs[id] = acc
				} else {
					// Delete
					oldAcc := accountMsgs[id]
					if isTx {
						tx.Delete(ctx, oldAcc)
					} else {
						writer.Delete(ctx, oldAcc)
					}
					delete(accounts, id)
					delete(accountMsgs, id)
				}
			}
		}

		if isTx {
			tx.Commit(ctx)
		}
	}

	payloads := appender.Payloads()

	// 1. Full replay comparison
	memStore := memtable.New()
	memReader := sds.NewChangeReader()

	dbPath := filepath.Join(t.TempDir(), "random.sqlite")
	db, err := sqlite.Open(ctx, dbPath, sqlite.WithStreamID("stream-random"))
	if err != nil {
		t.Fatalf("sqlite.Open failed: %v", err)
	}
	defer db.Close()

	for i, p := range payloads {
		seq := uint64(i + 1)
		changes, _ := memReader.Feed(seq, p)
		memStore.ApplyBatch(ctx, changes)
		db.Feed(ctx, seq, p)
	}

	verifySQLiteMatchesMemStore(t, ctx, db, memStore)

	// 2. Snapshot at random safe position + replay test
	safePos := memReader.SafeSnapshotPosition()
	if safePos > 10 {
		snapPosTarget := uint64(r.Intn(int(safePos-5)) + 5)
		backend := inmemorystorage.New()
		streamID := "stream-random-snap"

		_, actualSnapPos, err := sqlite.BuildSnapshot(ctx, streamID, payloads[:snapPosTarget], backend, t.TempDir())
		if err != nil {
			t.Fatalf("BuildSnapshot failed: %v", err)
		}

		// Restore and tail replay
		restoredPath := filepath.Join(t.TempDir(), "random_restored.sqlite")
		restoredDB, _, err := sqlite.RestoreSnapshot(ctx, backend, streamID, actualSnapPos, restoredPath)
		if err != nil {
			t.Fatalf("RestoreSnapshot failed: %v", err)
		}
		defer restoredDB.Close()

		for i := int(actualSnapPos); i < len(payloads); i++ {
			seq := uint64(i + 1)
			if _, err := restoredDB.Feed(ctx, seq, payloads[i]); err != nil {
				t.Fatalf("Feed to restored DB failed at seq %d: %v", seq, err)
			}
		}

		verifySQLiteMatchesMemStore(t, ctx, restoredDB, memStore)
	}
}

func TestKeyPrefixScan(t *testing.T) {
	ctx := t.Context()
	appender := &memoryAppender{}
	writer := sds.NewWriter(appender)

	dirEntryMD := buildMD(t, "DirEntry", []*descriptorpb.FieldDescriptorProto{
		field("parent_id", 1, descriptorpb.FieldDescriptorProto_TYPE_INT64, descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL),
		field("name", 2, descriptorpb.FieldDescriptorProto_TYPE_STRING, descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL),
		field("ino", 3, descriptorpb.FieldDescriptorProto_TYPE_INT64, descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL),
		field("is_dir", 4, descriptorpb.FieldDescriptorProto_TYPE_BOOL, descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL),
	})
	writer.RegisterDescriptor(dirEntryMD, 1, 2)

	// Insert parent 1 entries
	p1Names := []string{"alpha", "beta", "gamma"}
	for i, name := range p1Names {
		de := dynamicpb.NewMessage(dirEntryMD)
		de.Set(dirEntryMD.Fields().ByName("parent_id"), protoreflect.ValueOfInt64(1))
		de.Set(dirEntryMD.Fields().ByName("name"), protoreflect.ValueOfString(name))
		de.Set(dirEntryMD.Fields().ByName("ino"), protoreflect.ValueOfInt64(int64(10+i)))
		de.Set(dirEntryMD.Fields().ByName("is_dir"), protoreflect.ValueOfBool(false))
		writer.Create(ctx, de)
	}

	// Insert parent 2 entries
	p2Names := []string{"alpha", "delta"}
	for i, name := range p2Names {
		de := dynamicpb.NewMessage(dirEntryMD)
		de.Set(dirEntryMD.Fields().ByName("parent_id"), protoreflect.ValueOfInt64(2))
		de.Set(dirEntryMD.Fields().ByName("name"), protoreflect.ValueOfString(name))
		de.Set(dirEntryMD.Fields().ByName("ino"), protoreflect.ValueOfInt64(int64(20+i)))
		de.Set(dirEntryMD.Fields().ByName("is_dir"), protoreflect.ValueOfBool(true))
		writer.Create(ctx, de)
	}

	// Insert parent 100 entry
	de100 := dynamicpb.NewMessage(dirEntryMD)
	de100.Set(dirEntryMD.Fields().ByName("parent_id"), protoreflect.ValueOfInt64(100))
	de100.Set(dirEntryMD.Fields().ByName("name"), protoreflect.ValueOfString("zeta"))
	de100.Set(dirEntryMD.Fields().ByName("ino"), protoreflect.ValueOfInt64(1000))
	de100.Set(dirEntryMD.Fields().ByName("is_dir"), protoreflect.ValueOfBool(false))
	writer.Create(ctx, de100)

	dbPath := filepath.Join(t.TempDir(), "scan_test.sqlite")
	db, err := sqlite.Open(ctx, dbPath, sqlite.WithStreamID("stream-scan"))
	if err != nil {
		t.Fatalf("sqlite.Open failed: %v", err)
	}
	defer db.Close()

	for i, p := range appender.Payloads() {
		if _, err := db.Feed(ctx, uint64(i+1), p); err != nil {
			t.Fatalf("Feed failed: %v", err)
		}
	}

	// 1. Scan for parent_id = 1
	p1PrefixMsg := dynamicpb.NewMessage(dirEntryMD)
	p1PrefixMsg.Set(dirEntryMD.Fields().ByName("parent_id"), protoreflect.ValueOfInt64(1))
	p1Prefix, err := sds.EncodeKeyPrefix(p1PrefixMsg, 1)
	if err != nil {
		t.Fatalf("EncodeKeyPrefix for parent 1 failed: %v", err)
	}

	p1Rows, err := db.ScanSlice(ctx, "testpkg.DirEntry", p1Prefix)
	if err != nil {
		t.Fatalf("db.ScanSlice parent 1 failed: %v", err)
	}
	if len(p1Rows) != 3 {
		t.Fatalf("expected 3 rows for parent 1, got %d", len(p1Rows))
	}
	gotP1Names := make(map[string]bool)
	for _, row := range p1Rows {
		dyn := row.(*dynamicpb.Message)
		name := dyn.Get(dyn.Descriptor().Fields().ByName("name")).String()
		gotP1Names[name] = true
	}
	for _, name := range p1Names {
		if !gotP1Names[name] {
			t.Errorf("missing expected name %s in parent 1", name)
		}
	}

	// 2. Scan for parent_id = 2
	p2PrefixMsg := dynamicpb.NewMessage(dirEntryMD)
	p2PrefixMsg.Set(dirEntryMD.Fields().ByName("parent_id"), protoreflect.ValueOfInt64(2))
	p2Prefix, err := sds.EncodeKeyPrefix(p2PrefixMsg, 1)
	if err != nil {
		t.Fatalf("EncodeKeyPrefix for parent 2 failed: %v", err)
	}

	p2Rows, err := db.ScanSlice(ctx, "testpkg.DirEntry", p2Prefix)
	if err != nil {
		t.Fatalf("db.ScanSlice parent 2 failed: %v", err)
	}
	if len(p2Rows) != 2 {
		t.Fatalf("expected 2 rows for parent 2, got %d", len(p2Rows))
	}

	// 3. Scan all (empty prefix)
	allRows, err := db.ScanSlice(ctx, "testpkg.DirEntry", nil)
	if err != nil {
		t.Fatalf("db.ScanSlice all failed: %v", err)
	}
	if len(allRows) != 6 {
		t.Fatalf("expected 6 total rows, got %d", len(allRows))
	}
}

func TestTxChangesAndApplyBatch(t *testing.T) {
	ctx := t.Context()
	appender := &memoryAppender{}
	writer := sds.NewWriter(appender)

	userMD := buildMD(t, "User", []*descriptorpb.FieldDescriptorProto{
		field("id", 1, descriptorpb.FieldDescriptorProto_TYPE_INT64, descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL),
		field("name", 2, descriptorpb.FieldDescriptorProto_TYPE_STRING, descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL),
	})
	writer.RegisterDescriptor(userMD, 1)

	dbPath := filepath.Join(t.TempDir(), "writethrough.sqlite")
	db, err := sqlite.Open(ctx, dbPath, sqlite.WithStreamID("stream-wt"))
	if err != nil {
		t.Fatalf("sqlite.Open failed: %v", err)
	}
	defer db.Close()

	if err := db.SyncRegistry(ctx, writer.Registry()); err != nil {
		t.Fatalf("SyncRegistry failed: %v", err)
	}

	tx := writer.Begin()
	u1 := dynamicpb.NewMessage(userMD)
	u1.Set(userMD.Fields().ByName("id"), protoreflect.ValueOfInt64(10))
	u1.Set(userMD.Fields().ByName("name"), protoreflect.ValueOfString("User 10"))
	tx.Create(ctx, u1)

	u2 := dynamicpb.NewMessage(userMD)
	u2.Set(userMD.Fields().ByName("id"), protoreflect.ValueOfInt64(20))
	u2.Set(userMD.Fields().ByName("name"), protoreflect.ValueOfString("User 20"))
	tx.Create(ctx, u2)

	commitSeq, err := tx.Commit(ctx)
	if err != nil {
		t.Fatalf("tx.Commit failed: %v", err)
	}

	changes := tx.Changes()
	if len(changes) != 2 {
		t.Fatalf("expected 2 changes, got %d", len(changes))
	}
	if changes[0].Seq != commitSeq || changes[1].Seq != commitSeq {
		t.Fatalf("changes seq mismatch: %d, %d vs %d", changes[0].Seq, changes[1].Seq, commitSeq)
	}

	// Apply write-through
	if err := db.ApplyBatch(ctx, changes); err != nil {
		t.Fatalf("db.ApplyBatch failed: %v", err)
	}

	if db.Position() != commitSeq {
		t.Errorf("db.Position = %d, want %d", db.Position(), commitSeq)
	}

	count, err := db.Count(ctx, "testpkg.User")
	if err != nil {
		t.Fatalf("db.Count failed: %v", err)
	}
	if count != 2 {
		t.Errorf("expected 2 users in db, got %d", count)
	}
}
