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

package sds

import (
	"testing"

	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/dynamicpb"
)

func TestTxKeyEnforcement(t *testing.T) {
	ctx := t.Context()
	appender := &memoryAppender{}
	writer := NewWriter(appender)

	fields := []*descriptorpb.FieldDescriptorProto{
		field("id", 1, descriptorpb.FieldDescriptorProto_TYPE_INT64, descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL),
		field("val", 2, descriptorpb.FieldDescriptorProto_TYPE_STRING, descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL),
	}
	md := buildDynamicMessageDescriptor(t, "Item", fields)
	if _, err := writer.RegisterDescriptor(md, 1); err != nil {
		t.Fatalf("RegisterDescriptor failed: %v", err)
	}

	makeItem := func(id int64, val string) *dynamicpb.Message {
		m := dynamicpb.NewMessage(md)
		m.Set(md.Fields().ByName("id"), protoreflect.ValueOfInt64(id))
		m.Set(md.Fields().ByName("val"), protoreflect.ValueOfString(val))
		return m
	}

	// 1. Create then Create of same key in same Tx -> error
	{
		tx := writer.Begin()
		if _, err := tx.Create(ctx, makeItem(1, "v1")); err != nil {
			t.Fatalf("first Create failed: %v", err)
		}
		if _, err := tx.Create(ctx, makeItem(1, "v2")); err == nil {
			t.Fatalf("expected error on duplicate Create in same Tx, got nil")
		}
	}

	// 2. Create then Update of same key in same Tx -> allowed
	{
		tx := writer.Begin()
		i1 := makeItem(2, "v1")
		if _, err := tx.Create(ctx, i1); err != nil {
			t.Fatalf("Create failed: %v", err)
		}
		i2 := makeItem(2, "v2")
		if _, err := tx.Update(ctx, i1, i2); err != nil {
			t.Fatalf("Update after Create failed: %v", err)
		}
		// But another Create of key 2 in same Tx is still rejected
		if _, err := tx.Create(ctx, makeItem(2, "v3")); err == nil {
			t.Fatalf("expected error on Create after Update in same Tx, got nil")
		}
	}

	// 3. Delete then Create of same key in same Tx -> allowed
	{
		tx := writer.Begin()
		i1 := makeItem(3, "v1")
		if _, err := tx.Delete(ctx, i1); err != nil {
			t.Fatalf("Delete failed: %v", err)
		}
		i2 := makeItem(3, "v2")
		if _, err := tx.Create(ctx, i2); err != nil {
			t.Fatalf("Create after Delete in same Tx should succeed, got: %v", err)
		}
	}

	// 4. Delete then Update of same key in same Tx -> error
	{
		tx := writer.Begin()
		i1 := makeItem(4, "v1")
		if _, err := tx.Delete(ctx, i1); err != nil {
			t.Fatalf("Delete failed: %v", err)
		}
		i2 := makeItem(4, "v2")
		if _, err := tx.Update(ctx, i1, i2); err == nil {
			t.Fatalf("expected error on Update after Delete in same Tx, got nil")
		}
	}

	// 5. Delete then Delete of same key in same Tx -> error
	{
		tx := writer.Begin()
		i1 := makeItem(5, "v1")
		if _, err := tx.Delete(ctx, i1); err != nil {
			t.Fatalf("first Delete failed: %v", err)
		}
		if _, err := tx.Delete(ctx, i1); err == nil {
			t.Fatalf("expected error on double Delete in same Tx, got nil")
		}
	}
}

func TestTxUpdateValidation(t *testing.T) {
	ctx := t.Context()
	appender := &memoryAppender{}
	writer := NewWriter(appender)

	md1 := buildDynamicMessageDescriptor(t, "TypeA", []*descriptorpb.FieldDescriptorProto{
		field("id", 1, descriptorpb.FieldDescriptorProto_TYPE_INT64, descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL),
		field("val", 2, descriptorpb.FieldDescriptorProto_TYPE_STRING, descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL),
	})
	md2 := buildDynamicMessageDescriptor(t, "TypeB", []*descriptorpb.FieldDescriptorProto{
		field("id", 1, descriptorpb.FieldDescriptorProto_TYPE_INT64, descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL),
		field("val", 2, descriptorpb.FieldDescriptorProto_TYPE_STRING, descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL),
	})
	writer.RegisterDescriptor(md1, 1)
	writer.RegisterDescriptor(md2, 1)

	// 1. Nil before or after
	tx := writer.Begin()
	a1 := dynamicpb.NewMessage(md1)
	a1.Set(md1.Fields().ByName("id"), protoreflect.ValueOfInt64(1))
	if _, err := tx.Update(ctx, nil, a1); err == nil {
		t.Fatalf("expected error on nil before, got nil")
	}
	if _, err := tx.Update(ctx, a1, nil); err == nil {
		t.Fatalf("expected error on nil after, got nil")
	}
	if _, err := tx.Delete(ctx, nil); err == nil {
		t.Fatalf("expected error on nil Delete, got nil")
	}

	// 2. Different types
	b1 := dynamicpb.NewMessage(md2)
	b1.Set(md2.Fields().ByName("id"), protoreflect.ValueOfInt64(1))
	if _, err := tx.Update(ctx, a1, b1); err == nil {
		t.Fatalf("expected error on mismatched message types, got nil")
	}

	// 3. Mismatched primary keys
	a2 := dynamicpb.NewMessage(md1)
	a2.Set(md1.Fields().ByName("id"), protoreflect.ValueOfInt64(2))
	if _, err := tx.Update(ctx, a1, a2); err == nil {
		t.Fatalf("expected error on mismatched primary keys in Update, got nil")
	}
}

func TestBeforeImagesOptInAndReconstitution(t *testing.T) {
	ctx := t.Context()
	appender := &memoryAppender{}
	writer := NewWriter(appender)

	// Table with before-images enabled
	mdWithBefore := buildDynamicMessageDescriptor(t, "WithBefore", []*descriptorpb.FieldDescriptorProto{
		field("id", 1, descriptorpb.FieldDescriptorProto_TYPE_INT64, descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL),
		field("data", 2, descriptorpb.FieldDescriptorProto_TYPE_STRING, descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL),
	})
	if _, err := writer.RegisterDescriptorWithOptions(mdWithBefore, WithKeyFields(1), WithLogBeforeImages(true)); err != nil {
		t.Fatalf("RegisterDescriptorWithOptions failed: %v", err)
	}

	// Table with before-images disabled (default)
	mdWithoutBefore := buildDynamicMessageDescriptor(t, "WithoutBefore", []*descriptorpb.FieldDescriptorProto{
		field("id", 1, descriptorpb.FieldDescriptorProto_TYPE_INT64, descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL),
		field("data", 2, descriptorpb.FieldDescriptorProto_TYPE_STRING, descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL),
	})
	if _, err := writer.RegisterDescriptor(mdWithoutBefore, 1); err != nil {
		t.Fatalf("RegisterDescriptor failed: %v", err)
	}

	// Write ops for WithBefore
	wb1 := dynamicpb.NewMessage(mdWithBefore)
	wb1.Set(mdWithBefore.Fields().ByName("id"), protoreflect.ValueOfInt64(10))
	wb1.Set(mdWithBefore.Fields().ByName("data"), protoreflect.ValueOfString("initial"))
	writer.Create(ctx, wb1)

	wb1Up := dynamicpb.NewMessage(mdWithBefore)
	wb1Up.Set(mdWithBefore.Fields().ByName("id"), protoreflect.ValueOfInt64(10))
	wb1Up.Set(mdWithBefore.Fields().ByName("data"), protoreflect.ValueOfString("updated"))
	writer.Update(ctx, wb1, wb1Up)

	writer.Delete(ctx, wb1Up)

	// Write ops for WithoutBefore
	wob1 := dynamicpb.NewMessage(mdWithoutBefore)
	wob1.Set(mdWithoutBefore.Fields().ByName("id"), protoreflect.ValueOfInt64(20))
	wob1.Set(mdWithoutBefore.Fields().ByName("data"), protoreflect.ValueOfString("initial"))
	writer.Create(ctx, wob1)

	wob1Up := dynamicpb.NewMessage(mdWithoutBefore)
	wob1Up.Set(mdWithoutBefore.Fields().ByName("id"), protoreflect.ValueOfInt64(20))
	wob1Up.Set(mdWithoutBefore.Fields().ByName("data"), protoreflect.ValueOfString("updated"))
	writer.Update(ctx, wob1, wob1Up)

	writer.Delete(ctx, wob1Up)

	// Decode all with ChangeReader
	reader := NewChangeReader()
	var changes []Change
	for i, p := range appender.Payloads() {
		ch, err := reader.Feed(uint64(i+1), p)
		if err != nil {
			t.Fatalf("Feed failed: %v", err)
		}
		changes = append(changes, ch...)
	}

	if len(changes) != 6 {
		t.Fatalf("expected 6 changes, got %d", len(changes))
	}

	// 1. WithBefore Create
	if changes[0].Before != nil {
		t.Errorf("Create should have nil Before, got %v", changes[0].Before)
	}
	if changes[0].Row == nil {
		t.Errorf("Create should have non-nil Row")
	}

	// 2. WithBefore Update
	if changes[1].Before == nil {
		t.Fatalf("Update on table with before-images should have non-nil Before")
	}
	beforeDyn := changes[1].Before.(*dynamicpb.Message)
	if beforeDyn.Get(beforeDyn.Descriptor().Fields().ByName("data")).String() != "initial" {
		t.Errorf("Before.data = %q, want %q", beforeDyn.Get(beforeDyn.Descriptor().Fields().ByName("data")).String(), "initial")
	}
	rowDyn := changes[1].Row.(*dynamicpb.Message)
	if rowDyn.Get(rowDyn.Descriptor().Fields().ByName("data")).String() != "updated" {
		t.Errorf("Row.data = %q, want %q", rowDyn.Get(rowDyn.Descriptor().Fields().ByName("data")).String(), "updated")
	}

	// 3. WithBefore Delete
	if changes[2].Before == nil {
		t.Fatalf("Delete on table with before-images should have non-nil Before")
	}
	if changes[2].Row != nil {
		t.Errorf("Delete should have nil Row, got %v", changes[2].Row)
	}
	delBeforeDyn := changes[2].Before.(*dynamicpb.Message)
	if delBeforeDyn.Get(delBeforeDyn.Descriptor().Fields().ByName("data")).String() != "updated" {
		t.Errorf("Delete Before.data = %q, want %q", delBeforeDyn.Get(delBeforeDyn.Descriptor().Fields().ByName("data")).String(), "updated")
	}

	// 4. WithoutBefore Create
	if changes[3].Before != nil || changes[3].Row == nil {
		t.Errorf("WithoutBefore Create mismatch: Before=%v, Row=%v", changes[3].Before, changes[3].Row)
	}

	// 5. WithoutBefore Update
	if changes[4].Before != nil {
		t.Errorf("WithoutBefore Update should have nil Before, got %v", changes[4].Before)
	}

	// 6. WithoutBefore Delete
	if changes[5].Before != nil || changes[5].Row != nil {
		t.Errorf("WithoutBefore Delete should have nil Before and nil Row, got Before=%v, Row=%v", changes[5].Before, changes[5].Row)
	}
}
