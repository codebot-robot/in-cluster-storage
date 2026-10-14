/*
Copyright 2026 Google LLC

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package memtable

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"

	"github.com/gke-labs/in-cluster-storage/pkg/objectfs/blob"
	"github.com/gke-labs/in-cluster-storage/pkg/objectstore/inmemorystorage"
	"github.com/gke-labs/in-cluster-storage/pkg/sds"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/dynamicpb"
)

func TestMemStore_ApplyBatch_MergeError(t *testing.T) {
	ctx := t.Context()
	md := buildDynamicMD(t, "Item", []*descriptorpb.FieldDescriptorProto{
		field("id", 1, descriptorpb.FieldDescriptorProto_TYPE_INT64, descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL),
		field("version", 2, descriptorpb.FieldDescriptorProto_TYPE_INT64, descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL),
	})

	store := New(WithStreamID("test-apply-merge-err"))
	defer store.Close()

	if _, err := store.Registry().RegisterMessage(dynamicpb.NewMessage(md), 1); err != nil {
		t.Fatalf("RegisterMessage failed: %v", err)
	}

	// Corrupt raw key/val bytes that fail proto unmarshal in MergeKeyAndNonKey
	ch := sds.Change{
		TypeName: "testpkg.Item",
		Op:       sds.OpCreate,
		RawKey:   []byte{0xff, 0xff, 0xff, 0xff, 0xff},
		RawVal:   []byte{0xff, 0xff, 0xff, 0xff, 0xff},
		Seq:      1,
	}

	err := store.ApplyBatch(ctx, []sds.Change{ch})
	if err == nil {
		t.Fatal("expected ApplyBatch to fail on corrupt wire bytes, got nil")
	}
	if !strings.Contains(err.Error(), "failed to merge row fields") {
		t.Fatalf("expected error containing 'failed to merge row fields', got: %v", err)
	}
}

func TestMemStore_Rows(t *testing.T) {
	md := buildDynamicMD(t, "Item", []*descriptorpb.FieldDescriptorProto{
		field("id", 1, descriptorpb.FieldDescriptorProto_TYPE_INT64, descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL),
	})

	store := New(WithStreamID("test-rows"))
	defer store.Close()

	if _, err := store.Registry().RegisterMessage(dynamicpb.NewMessage(md), 1); err != nil {
		t.Fatalf("RegisterMessage failed: %v", err)
	}

	msg := dynamicpb.NewMessage(md)
	msg.Set(md.Fields().ByName("id"), protoreflect.ValueOfInt64(42))

	tbl := store.GetOrCreateTable("testpkg.Item")
	tbl.Put(sds.NewKeyFromBytes([]byte("key-42")), msg)

	rows, err := store.Rows("testpkg.Item")
	if err != nil {
		t.Fatalf("Rows failed: %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(rows))
	}

	// Non-existent table should return empty rows with nil error
	emptyRows, err := store.Rows("nonexistent")
	if err != nil {
		t.Fatalf("Rows for nonexistent table failed: %v", err)
	}
	if len(emptyRows) != 0 {
		t.Fatalf("expected 0 rows, got %d", len(emptyRows))
	}
}

func TestRestoreSnapshotKey_CorruptRow(t *testing.T) {
	ctx := t.Context()
	backend := inmemorystorage.New()
	streamID := "test-snap-corrupt"

	md := buildDynamicMD(t, "Item", []*descriptorpb.FieldDescriptorProto{
		field("id", 1, descriptorpb.FieldDescriptorProto_TYPE_INT64, descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL),
	})

	store := New(WithStreamID(streamID))
	if _, err := store.Registry().RegisterMessage(dynamicpb.NewMessage(md), 1); err != nil {
		t.Fatalf("RegisterMessage failed: %v", err)
	}

	msg := dynamicpb.NewMessage(md)
	msg.Set(md.Fields().ByName("id"), protoreflect.ValueOfInt64(100))
	tbl := store.GetOrCreateTable("testpkg.Item")
	tbl.Put(sds.NewKeyFromBytes([]byte("key-100")), msg)
	_ = store.SetPosition(10)

	snapKey, _, err := PublishSnapshot(ctx, store, backend, "")
	if err != nil {
		t.Fatalf("PublishSnapshot failed: %v", err)
	}

	// Read snapshot JSON, corrupt the row value, and write back
	var buf bytes.Buffer
	if err := backend.GetObject(ctx, "", snapKey, 0, 0, &buf); err != nil {
		t.Fatalf("GetObject failed: %v", err)
	}

	var data snapshotData
	if err := json.Unmarshal(buf.Bytes(), &data); err != nil {
		t.Fatalf("Unmarshal snapshot failed: %v", err)
	}

	// Inject corrupt proto bytes into the row
	data.Tables["testpkg.Item"][0].Val = []byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff}
	corruptEncoded, err := json.Marshal(data)
	if err != nil {
		t.Fatalf("Marshal corrupt snapshot failed: %v", err)
	}

	if _, err := backend.PutObject(ctx, "", snapKey, blob.NewByteStreamFromBytes(corruptEncoded)); err != nil {
		t.Fatalf("PutObject failed: %v", err)
	}

	_, _, err = RestoreSnapshotKey(ctx, backend, streamID, snapKey, "")
	if err == nil {
		t.Fatal("expected RestoreSnapshotKey to fail on corrupt row payload, got nil")
	}
	if !strings.Contains(err.Error(), "failed to unmarshal row in table") {
		t.Fatalf("expected error containing 'failed to unmarshal row in table', got: %v", err)
	}
}

func TestRestoreSnapshotKey_MissingTableInRegistry(t *testing.T) {
	ctx := t.Context()
	backend := inmemorystorage.New()
	streamID := "test-snap-missing-table"

	data := snapshotData{
		StreamID: streamID,
		Position: 1,
		Tables: map[string][]rowRecord{
			"unregistered.Table": {
				{Key: []byte("k"), Val: []byte("v")},
			},
		},
	}
	encoded, err := json.Marshal(data)
	if err != nil {
		t.Fatalf("Marshal snapshot failed: %v", err)
	}
	key := SnapshotKey(streamID, 1)
	if _, err := backend.PutObject(ctx, "", key, blob.NewByteStreamFromBytes(encoded)); err != nil {
		t.Fatalf("PutObject failed: %v", err)
	}

	_, _, err = RestoreSnapshotKey(ctx, backend, streamID, key, "")
	if err == nil {
		t.Fatal("expected RestoreSnapshotKey to fail on table missing from registry, got nil")
	}
	if !strings.Contains(err.Error(), "not found in registry") {
		t.Fatalf("expected error containing 'not found in registry', got: %v", err)
	}
}
