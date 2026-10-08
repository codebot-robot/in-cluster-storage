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
	"context"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/gke-labs/in-cluster-storage/pkg/sds"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/dynamicpb"
)

type memoryAppender struct {
	payloads [][]byte
	seq      uint64
}

func (m *memoryAppender) Append(_ context.Context, payload []byte) (uint64, error) {
	m.seq++
	cp := make([]byte, len(payload))
	copy(cp, payload)
	m.payloads = append(m.payloads, cp)
	return m.seq, nil
}

func field(name string, number int32, typeKind descriptorpb.FieldDescriptorProto_Type, label descriptorpb.FieldDescriptorProto_Label) *descriptorpb.FieldDescriptorProto {
	return &descriptorpb.FieldDescriptorProto{
		Name:   proto.String(name),
		Number: proto.Int32(number),
		Type:   typeKind.Enum(),
		Label:  label.Enum(),
	}
}

func buildDynamicMD(t *testing.T, name string, fields []*descriptorpb.FieldDescriptorProto) protoreflect.MessageDescriptor {
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

func TestMemTableDirectOperations(t *testing.T) {
	table := NewTable("testpkg.User")
	if table.Name() != "testpkg.User" {
		t.Errorf("table.Name() = %q, want 'testpkg.User'", table.Name())
	}

	md := buildDynamicMD(t, "User", []*descriptorpb.FieldDescriptorProto{
		field("id", 1, descriptorpb.FieldDescriptorProto_TYPE_INT64, descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL),
		field("name", 2, descriptorpb.FieldDescriptorProto_TYPE_STRING, descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL),
	})

	u1 := dynamicpb.NewMessage(md)
	u1.Set(md.Fields().ByName("id"), protoreflect.ValueOfInt64(1))
	u1.Set(md.Fields().ByName("name"), protoreflect.ValueOfString("Alice"))

	k1, err := sds.ExtractKey(u1, []int32{1})
	if err != nil {
		t.Fatalf("ExtractKey error: %v", err)
	}

	// Put and Get
	table.Put(k1, u1)
	if table.Count() != 1 {
		t.Errorf("Count() = %d, want 1", table.Count())
	}

	msg, ok := table.Get(k1)
	if !ok {
		t.Fatalf("Get(k1) failed")
	}
	dyn := msg.(*dynamicpb.Message)
	if dyn.Get(dyn.Descriptor().Fields().ByName("name")).String() != "Alice" {
		t.Errorf("name = %q, want 'Alice'", dyn.Get(dyn.Descriptor().Fields().ByName("name")).String())
	}

	// Rows
	rows := table.Rows()
	if len(rows) != 1 {
		t.Errorf("len(Rows()) = %d, want 1", len(rows))
	}

	// Delete
	table.Delete(k1)
	if table.Count() != 0 {
		t.Errorf("Count() after delete = %d, want 0", table.Count())
	}
	if _, ok := table.Get(k1); ok {
		t.Errorf("Get(k1) after delete succeeded, want false")
	}

	// Put and Clear
	table.Put(k1, u1)
	table.Clear()
	if table.Count() != 0 {
		t.Errorf("Count() after clear = %d, want 0", table.Count())
	}
}

func TestMemTableScriptedLogReplay(t *testing.T) {
	ctx := t.Context()
	appender := &memoryAppender{}
	writer := sds.NewWriter(appender)

	// Schema 1: Customer (id tag 1, name tag 2)
	custMD := buildDynamicMD(t, "Customer", []*descriptorpb.FieldDescriptorProto{
		field("id", 1, descriptorpb.FieldDescriptorProto_TYPE_INT64, descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL),
		field("name", 2, descriptorpb.FieldDescriptorProto_TYPE_STRING, descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL),
	})
	writer.RegisterDescriptor(custMD, 1)

	// Schema 2: Order (id tag 1, customer_id tag 2, total tag 3)
	orderMD := buildDynamicMD(t, "Order", []*descriptorpb.FieldDescriptorProto{
		field("id", 1, descriptorpb.FieldDescriptorProto_TYPE_INT64, descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL),
		field("customer_id", 2, descriptorpb.FieldDescriptorProto_TYPE_INT64, descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL),
		field("total", 3, descriptorpb.FieldDescriptorProto_TYPE_DOUBLE, descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL),
	})
	writer.RegisterDescriptor(orderMD, 1)

	// --- Step 1: Autocommit Inserts ---
	// Customer 1: Alice
	c1 := dynamicpb.NewMessage(custMD)
	c1.Set(custMD.Fields().ByName("id"), protoreflect.ValueOfInt64(1))
	c1.Set(custMD.Fields().ByName("name"), protoreflect.ValueOfString("Alice"))
	writer.Insert(ctx, c1)

	// Customer 2: Bob
	c2 := dynamicpb.NewMessage(custMD)
	c2.Set(custMD.Fields().ByName("id"), protoreflect.ValueOfInt64(2))
	c2.Set(custMD.Fields().ByName("name"), protoreflect.ValueOfString("Bob"))
	writer.Insert(ctx, c2)

	// Customer 3: Charlie
	c3 := dynamicpb.NewMessage(custMD)
	c3.Set(custMD.Fields().ByName("id"), protoreflect.ValueOfInt64(3))
	c3.Set(custMD.Fields().ByName("name"), protoreflect.ValueOfString("Charlie"))
	writer.Insert(ctx, c3)

	// Order 101 for Customer 1 ($50.00)
	o101 := dynamicpb.NewMessage(orderMD)
	o101.Set(orderMD.Fields().ByName("id"), protoreflect.ValueOfInt64(101))
	o101.Set(orderMD.Fields().ByName("customer_id"), protoreflect.ValueOfInt64(1))
	o101.Set(orderMD.Fields().ByName("total"), protoreflect.ValueOfFloat64(50.00))
	writer.Insert(ctx, o101)

	// Order 102 for Customer 2 ($75.50)
	o102 := dynamicpb.NewMessage(orderMD)
	o102.Set(orderMD.Fields().ByName("id"), protoreflect.ValueOfInt64(102))
	o102.Set(orderMD.Fields().ByName("customer_id"), protoreflect.ValueOfInt64(2))
	o102.Set(orderMD.Fields().ByName("total"), protoreflect.ValueOfFloat64(75.50))
	writer.Insert(ctx, o102)

	// --- Step 2: Multi-row Transaction ---
	tx := writer.Begin()

	// Update Order 101 total to $65.00
	o101Up := dynamicpb.NewMessage(orderMD)
	o101Up.Set(orderMD.Fields().ByName("id"), protoreflect.ValueOfInt64(101))
	o101Up.Set(orderMD.Fields().ByName("customer_id"), protoreflect.ValueOfInt64(1))
	o101Up.Set(orderMD.Fields().ByName("total"), protoreflect.ValueOfFloat64(65.00))
	tx.Update(ctx, o101Up)

	// Insert Order 103 for Customer 1 ($30.00)
	o103 := dynamicpb.NewMessage(orderMD)
	o103.Set(orderMD.Fields().ByName("id"), protoreflect.ValueOfInt64(103))
	o103.Set(orderMD.Fields().ByName("customer_id"), protoreflect.ValueOfInt64(1))
	o103.Set(orderMD.Fields().ByName("total"), protoreflect.ValueOfFloat64(30.00))
	tx.Insert(ctx, o103)

	// Commit Tx
	tx.Commit(ctx)

	// --- Step 3: Autocommit Delete ---
	// Delete Customer 3
	c3Del := dynamicpb.NewMessage(custMD)
	c3Del.Set(custMD.Fields().ByName("id"), protoreflect.ValueOfInt64(3))
	writer.Delete(ctx, c3Del)

	// --- Step 4: Uncommitted Transaction at Tail ---
	txUncommitted := writer.Begin()
	o104 := dynamicpb.NewMessage(orderMD)
	o104.Set(orderMD.Fields().ByName("id"), protoreflect.ValueOfInt64(104))
	o104.Set(orderMD.Fields().ByName("customer_id"), protoreflect.ValueOfInt64(2))
	o104.Set(orderMD.Fields().ByName("total"), protoreflect.ValueOfFloat64(999.00))
	txUncommitted.Insert(ctx, o104)

	// --- Stream Playback into ChangeReader and MemStore ---
	reader := sds.NewChangeReader()
	store := New()

	for i, payload := range appender.payloads {
		seq := uint64(i + 1)
		changes, err := reader.Feed(seq, payload)
		if err != nil {
			t.Fatalf("Feed seq %d error: %v", seq, err)
		}
		if err := store.ApplyBatch(ctx, changes); err != nil {
			t.Fatalf("ApplyBatch error: %v", err)
		}
	}

	// Verify uncommitted tail was not applied
	if reader.HasPending() {
		reader.DiscardPending()
	}

	// Verify Tables present
	tables := store.Tables()
	if len(tables) != 2 {
		t.Fatalf("expected 2 tables, got %v", tables)
	}
	if tables[0] != "testpkg.Customer" || tables[1] != "testpkg.Order" {
		t.Errorf("tables = %v", tables)
	}

	// Verify Table direct lookup
	custTable := store.Table("testpkg.Customer")
	if custTable == nil {
		t.Fatalf("store.Table('testpkg.Customer') returned nil")
	}
	if custTable.Count() != 2 {
		t.Errorf("custTable.Count() = %d, want 2", custTable.Count())
	}

	// Verify Customer table contents
	if store.Count("testpkg.Customer") != 2 {
		t.Errorf("Customer count = %d, want 2", store.Count("testpkg.Customer"))
	}

	k1, _ := sds.ExtractKey(c1, []int32{1})
	k2, _ := sds.ExtractKey(c2, []int32{1})
	k3, _ := sds.ExtractKey(c3, []int32{1})

	if msg1, ok, err := store.Get(ctx, "testpkg.Customer", k1); err != nil || !ok {
		t.Errorf("Customer 1 not found: %v", err)
	} else {
		dyn := msg1.(*dynamicpb.Message)
		if dyn.Get(dyn.Descriptor().Fields().ByName("name")).String() != "Alice" {
			t.Errorf("Customer 1 name mismatch")
		}
	}

	if msg2, ok, err := store.Get(ctx, "testpkg.Customer", k2); err != nil || !ok {
		t.Errorf("Customer 2 not found: %v", err)
	} else {
		dyn := msg2.(*dynamicpb.Message)
		if dyn.Get(dyn.Descriptor().Fields().ByName("name")).String() != "Bob" {
			t.Errorf("Customer 2 name mismatch")
		}
	}

	// Customer 3 should be deleted
	if _, ok, _ := store.Get(ctx, "testpkg.Customer", k3); ok {
		t.Errorf("Customer 3 should have been deleted")
	}

	// Verify Order table contents
	if store.Count("testpkg.Order") != 3 {
		t.Errorf("Order count = %d, want 3", store.Count("testpkg.Order"))
	}

	k101, _ := sds.ExtractKey(o101, []int32{1})
	k102, _ := sds.ExtractKey(o102, []int32{1})
	k103, _ := sds.ExtractKey(o103, []int32{1})
	k104, _ := sds.ExtractKey(o104, []int32{1})

	if msg101, ok, err := store.Get(ctx, "testpkg.Order", k101); err != nil || !ok {
		t.Errorf("Order 101 not found: %v", err)
	} else {
		dyn := msg101.(*dynamicpb.Message)
		// Should have updated total $65.00
		if dyn.Get(dyn.Descriptor().Fields().ByName("total")).Float() != 65.00 {
			t.Errorf("Order 101 total = %v, want 65.00", dyn.Get(dyn.Descriptor().Fields().ByName("total")).Float())
		}
	}

	if msg102, ok, err := store.Get(ctx, "testpkg.Order", k102); err != nil || !ok {
		t.Errorf("Order 102 not found: %v", err)
	} else {
		dyn := msg102.(*dynamicpb.Message)
		if dyn.Get(dyn.Descriptor().Fields().ByName("total")).Float() != 75.50 {
			t.Errorf("Order 102 total = %v, want 75.50", dyn.Get(dyn.Descriptor().Fields().ByName("total")).Float())
		}
	}

	if msg103, ok, err := store.Get(ctx, "testpkg.Order", k103); err != nil || !ok {
		t.Errorf("Order 103 not found: %v", err)
	} else {
		dyn := msg103.(*dynamicpb.Message)
		if dyn.Get(dyn.Descriptor().Fields().ByName("total")).Float() != 30.00 {
			t.Errorf("Order 103 total = %v, want 30.00", dyn.Get(dyn.Descriptor().Fields().ByName("total")).Float())
		}
	}

	// Order 104 was uncommitted -> must not exist in table
	if _, ok, _ := store.Get(ctx, "testpkg.Order", k104); ok {
		t.Errorf("uncommitted Order 104 should not exist in table")
	}

	// Position should match safe snapshot position
	if store.Position() != reader.SafeSnapshotPosition() {
		t.Errorf("store.Position() = %d, SafeSnapshotPosition() = %d", store.Position(), reader.SafeSnapshotPosition())
	}
}

func TestMemTableNoCloning(t *testing.T) {
	ctx := t.Context()
	md := buildDynamicMD(t, "Product", []*descriptorpb.FieldDescriptorProto{
		field("id", 1, descriptorpb.FieldDescriptorProto_TYPE_INT64, descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL),
		field("name", 2, descriptorpb.FieldDescriptorProto_TYPE_STRING, descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL),
	})

	store := New(WithStreamID("test-memtable-no-clone"))
	defer store.Close()

	p1 := dynamicpb.NewMessage(md)
	p1.Set(md.Fields().ByName("id"), protoreflect.ValueOfInt64(10))
	p1.Set(md.Fields().ByName("name"), protoreflect.ValueOfString("Gizmo"))
	k1, _ := sds.ExtractKey(p1, []int32{1})

	table := store.Table("testpkg.Product")
	if table == nil {
		table = NewTable("testpkg.Product")
		store.tables["testpkg.Product"] = table
	}

	table.Put(k1, p1)

	gotMsg, ok := table.Get(k1)
	if !ok || gotMsg != p1 {
		t.Fatalf("MemTable.Get returned different pointer: %p != %p", gotMsg, p1)
	}

	rows := table.Rows()
	if len(rows) != 1 || rows[0] != p1 {
		t.Fatalf("MemTable.Rows returned different pointer: %p != %p", rows[0], p1)
	}

	for scanned, sErr := range store.Scan(ctx, "testpkg.Product", nil) {
		if sErr != nil {
			t.Fatalf("Scan error: %v", sErr)
		}
		if scanned != p1 {
			t.Fatalf("MemStore.Scan returned different pointer: %p != %p", scanned, p1)
		}
	}
}

func TestMemStoreConcurrentApplyBatchAndScan(t *testing.T) {
	ctx := t.Context()
	md := buildDynamicMD(t, "Item", []*descriptorpb.FieldDescriptorProto{
		field("id", 1, descriptorpb.FieldDescriptorProto_TYPE_INT64, descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL),
		field("version", 2, descriptorpb.FieldDescriptorProto_TYPE_INT64, descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL),
	})

	store := New(WithStreamID("test-concurrent-apply-scan"))
	defer store.Close()

	if _, err := store.Registry().RegisterMessage(dynamicpb.NewMessage(md), 1); err != nil {
		t.Fatalf("RegisterMessage failed: %v", err)
	}

	numKeys := int64(20)
	var done atomic.Bool
	var wg sync.WaitGroup

	// Reader goroutine: continuously scans and verifies that all items have the SAME version
	wg.Add(1)
	go func() {
		defer wg.Done()
		for !done.Load() {
			var versions []int64
			for msg, err := range store.Scan(ctx, "testpkg.Item", nil) {
				if err != nil {
					t.Errorf("Scan error: %v", err)
					return
				}
				dyn := msg.(*dynamicpb.Message)
				v := dyn.Get(dyn.Descriptor().Fields().ByName("version")).Int()
				versions = append(versions, v)
			}
			if len(versions) > 0 {
				if int64(len(versions)) != numKeys {
					t.Errorf("Expected %d keys, got %d", numKeys, len(versions))
					return
				}
				firstV := versions[0]
				for _, v := range versions {
					if v != firstV {
						t.Errorf("Saw mixed versions in batch: %v vs %v", v, firstV)
						return
					}
				}
			}
		}
	}()

	// Writer goroutine: updates all numKeys in a single atomic batch with incrementing version
	for ver := int64(1); ver <= 50; ver++ {
		var changes []sds.Change
		for i := int64(0); i < numKeys; i++ {
			msg := dynamicpb.NewMessage(md)
			msg.Set(md.Fields().ByName("id"), protoreflect.ValueOfInt64(i))
			msg.Set(md.Fields().ByName("version"), protoreflect.ValueOfInt64(ver))
			k, _ := sds.ExtractKey(msg, []int32{1})
			changes = append(changes, sds.Change{
				Seq:      uint64(ver),
				TypeName: "testpkg.Item",
				TypeID:   1,
				Op:       sds.OpUpdate,
				Key:      k,
				Row:      msg,
			})
		}
		if err := store.ApplyBatch(ctx, changes); err != nil {
			t.Fatalf("ApplyBatch error: %v", err)
		}
	}

	done.Store(true)
	wg.Wait()
}
