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

package view_test

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/gke-labs/in-cluster-storage/pkg/sds"
	"github.com/gke-labs/in-cluster-storage/pkg/sds/memtable"
	"github.com/gke-labs/in-cluster-storage/pkg/sds/record"
	"github.com/gke-labs/in-cluster-storage/pkg/sds/view"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/dynamicpb"
)

type faultyCloseIndex struct {
	sds.LocalIndex
	closeErr error
}

func (f *faultyCloseIndex) Close() error {
	_ = f.LocalIndex.Close()
	return f.closeErr
}

func TestView_SyncRegistry_ImportError(t *testing.T) {
	ctx := t.Context()
	store := memtable.New(memtable.WithStreamID("test-sync-reg-err"))
	defer store.Close()

	v := view.New(store)
	defer v.Close()

	// Initial registry with type ID 1
	reg1 := record.NewRegistry()
	userMD := dynamicDescriptor(t, "RegUser", []*descriptorpb.FieldDescriptorProto{
		protoField("id", 1, descriptorpb.FieldDescriptorProto_TYPE_INT64, descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL),
	})
	if _, err := reg1.RegisterMessage(dynamicpb.NewMessage(userMD), 1); err != nil {
		t.Fatalf("RegisterMessage in reg1 failed: %v", err)
	}
	if err := v.SyncRegistry(ctx, reg1); err != nil {
		t.Fatalf("initial SyncRegistry failed: %v", err)
	}

	// Conflicting registry with same type ID 1 but incompatible type name
	reg2 := record.NewRegistry()
	productMD := dynamicDescriptor(t, "RegProduct", []*descriptorpb.FieldDescriptorProto{
		protoField("sku", 1, descriptorpb.FieldDescriptorProto_TYPE_STRING, descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL),
	})
	if _, err := reg2.RegisterMessage(dynamicpb.NewMessage(productMD), 1); err != nil {
		t.Fatalf("RegisterMessage in reg2 failed: %v", err)
	}

	err := v.SyncRegistry(ctx, reg2)
	if err == nil {
		t.Fatal("expected SyncRegistry to fail on incompatible registry import, got nil")
	}
	if !strings.Contains(err.Error(), "failed to import registry") {
		t.Fatalf("expected error containing 'failed to import registry', got: %v", err)
	}
}

func TestView_Scan_ExtractKeyError(t *testing.T) {
	ctx := t.Context()
	store := memtable.New(memtable.WithStreamID("test-scan-extract-err"))
	defer store.Close()

	userMD := dynamicDescriptor(t, "ScanUser", []*descriptorpb.FieldDescriptorProto{
		protoField("id", 1, descriptorpb.FieldDescriptorProto_TYPE_INT64, descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL),
		protoField("name", 2, descriptorpb.FieldDescriptorProto_TYPE_STRING, descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL),
	})

	reg := record.NewRegistry()
	if _, err := reg.RegisterMessage(dynamicpb.NewMessage(userMD), 1); err != nil {
		t.Fatalf("RegisterMessage failed: %v", err)
	}

	v := view.New(store)
	defer v.Close()

	if err := v.SyncRegistry(ctx, reg); err != nil {
		t.Fatalf("SyncRegistry failed: %v", err)
	}

	// Put message into underlying store where primary key field 1 is missing
	msgWithoutKey := dynamicpb.NewMessage(userMD)
	msgWithoutKey.Set(userMD.Fields().ByName("name"), protoreflect.ValueOfString("no-key"))

	tbl := store.GetOrCreateTable("viewtest.ScanUser")
	tbl.Put(sds.NewKeyFromBytes([]byte("dummy-key")), msgWithoutKey)

	var scanErrors []error
	for _, err := range v.Scan(ctx, "viewtest.ScanUser", nil) {
		if err != nil {
			scanErrors = append(scanErrors, err)
		}
	}

	if len(scanErrors) == 0 {
		t.Fatal("expected Scan to yield error when ExtractKey fails, got 0 errors")
	}
	if !strings.Contains(scanErrors[0].Error(), "failed to extract key") {
		t.Fatalf("expected error containing 'failed to extract key', got: %v", scanErrors[0])
	}
}

func TestView_Rebuild_OldIndexCloseLogged(t *testing.T) {
	ctx := t.Context()
	baseStore := memtable.New(memtable.WithStreamID("test-rebuild-close-log"))
	faulty := &faultyCloseIndex{
		LocalIndex: baseStore,
		closeErr:   errors.New("injected close error"),
	}

	v := view.New(faulty, view.WithRebuildFunc(func(rctx context.Context) (sds.LocalIndex, uint64, error) {
		newStore := memtable.New(memtable.WithStreamID("test-rebuild-close-log"))
		return newStore, 10, nil
	}))
	defer v.Close()

	if err := v.Rebuild(ctx); err != nil {
		t.Fatalf("Rebuild failed: %v", err)
	}
}
