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

package record

import (
	"bytes"
	"errors"
	"testing"

	sdsv1 "github.com/gke-labs/in-cluster-storage/pkg/api/sds/v1"
	"google.golang.org/protobuf/types/descriptorpb"
)

func TestRegistryBasicAndIdempotency(t *testing.T) {
	reg := NewRegistry()

	fieldsV1 := []*descriptorpb.FieldDescriptorProto{
		field("id", 1, descriptorpb.FieldDescriptorProto_TYPE_INT64, descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL),
		field("customer", 2, descriptorpb.FieldDescriptorProto_TYPE_STRING, descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL),
	}
	defV1 := makeTestTypeDef("Order", fieldsV1, nil, nil, []int32{1}, nil)
	defV1.Id = 16

	// Register V1
	if err := reg.Register(defV1); err != nil {
		t.Fatalf("Register(defV1) error: %v", err)
	}

	// Lookup by ID
	lookupDef, md, ok := reg.LookupByID(16)
	if !ok || lookupDef == nil || md == nil {
		t.Fatalf("LookupByID(16) failed")
	}
	if lookupDef.GetName() != "testpkg.Order" {
		t.Errorf("lookupDef.Name = %q, want 'testpkg.Order'", lookupDef.GetName())
	}

	// Lookup by Name
	lookupDef2, md2, ok := reg.LookupByName("testpkg.Order")
	if !ok || lookupDef2 == nil || md2 == nil {
		t.Fatalf("LookupByName('testpkg.Order') failed")
	}

	// Idempotent re-registration of exact same definition
	if err := reg.Register(defV1); err != nil {
		t.Fatalf("idempotent Register(defV1) failed: %v", err)
	}

	// Compatible evolution V2
	fieldsV2 := []*descriptorpb.FieldDescriptorProto{
		field("id", 1, descriptorpb.FieldDescriptorProto_TYPE_INT64, descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL),
		field("customer", 2, descriptorpb.FieldDescriptorProto_TYPE_STRING, descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL),
		field("total", 3, descriptorpb.FieldDescriptorProto_TYPE_DOUBLE, descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL),
	}
	defV2 := makeTestTypeDef("Order", fieldsV2, nil, nil, []int32{1}, nil)
	defV2.Id = 16

	if err := reg.Register(defV2); err != nil {
		t.Fatalf("compatible evolution Register(defV2) error: %v", err)
	}

	// Verify registry updated to V2
	updatedDef, _, ok := reg.LookupByID(16)
	if !ok {
		t.Fatalf("LookupByID(16) after V2 failed")
	}
	if !bytes.Equal(updatedDef.GetFingerprint(), defV2.GetFingerprint()) {
		t.Errorf("updatedDef fingerprint did not update to V2")
	}

	// Incompatible change (removed field without reserving)
	fieldsV3Bad := []*descriptorpb.FieldDescriptorProto{
		field("id", 1, descriptorpb.FieldDescriptorProto_TYPE_INT64, descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL),
	}
	defV3Bad := makeTestTypeDef("Order", fieldsV3Bad, nil, nil, []int32{1}, nil)
	defV3Bad.Id = 16

	if err := reg.Register(defV3Bad); err == nil {
		t.Fatalf("expected error on incompatible evolution, got nil")
	}
}

func TestRegistryExportImport(t *testing.T) {
	reg := NewRegistry()

	def1 := makeTestTypeDef("Order", []*descriptorpb.FieldDescriptorProto{
		field("id", 1, descriptorpb.FieldDescriptorProto_TYPE_INT64, descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL),
	}, nil, nil, []int32{1}, nil)
	def1.Id = 16

	def2 := makeTestTypeDef("Item", []*descriptorpb.FieldDescriptorProto{
		field("sku", 1, descriptorpb.FieldDescriptorProto_TYPE_STRING, descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL),
	}, nil, nil, []int32{1}, nil)
	def2.Id = 17

	if err := reg.Register(def1); err != nil {
		t.Fatalf("Register def1 error: %v", err)
	}
	if err := reg.Register(def2); err != nil {
		t.Fatalf("Register def2 error: %v", err)
	}

	exported := reg.Export()
	if len(exported.GetTypes()) != 2 {
		t.Fatalf("exported %d types, want 2", len(exported.GetTypes()))
	}

	// Import into new registry
	reg2 := NewRegistry()
	if err := reg2.Import(exported); err != nil {
		t.Fatalf("Import error: %v", err)
	}

	if _, _, ok := reg2.LookupByID(16); !ok {
		t.Errorf("reg2 missing ID 16")
	}
	if _, _, ok := reg2.LookupByID(17); !ok {
		t.Errorf("reg2 missing ID 17")
	}
}

func TestRegistryValidationErrors(t *testing.T) {
	reg := NewRegistry()

	// Type ID < 16
	defLow := &sdsv1.TypeDefinition{
		Id:          10,
		Name:        "test.Msg",
		Fingerprint: []byte("01234567890123456789012345678901"),
	}
	if err := reg.Register(defLow); !errors.Is(err, ErrTypeIDTooLow) {
		t.Errorf("Register ID 10 got error %v, want ErrTypeIDTooLow", err)
	}

	// Missing descriptors on new registration
	defNoDesc := &sdsv1.TypeDefinition{
		Id:          16,
		Name:        "test.Msg",
		Fingerprint: []byte("01234567890123456789012345678901"),
	}
	if err := reg.Register(defNoDesc); !errors.Is(err, ErrMissingDescriptors) {
		t.Errorf("Register without descriptors got error %v, want ErrMissingDescriptors", err)
	}

	// Fingerprint mismatch
	defMismatchedFP := makeTestTypeDef("Order", []*descriptorpb.FieldDescriptorProto{
		field("id", 1, descriptorpb.FieldDescriptorProto_TYPE_INT64, descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL),
	}, nil, nil, []int32{1}, nil)
	defMismatchedFP.Id = 16
	defMismatchedFP.Fingerprint = []byte("corrupted_fingerprint_0123456789")

	if err := reg.Register(defMismatchedFP); err == nil {
		t.Errorf("expected error on fingerprint mismatch, got nil")
	}
}

func TestRegistryGoTypeResolution(t *testing.T) {
	reg := NewRegistry()

	// Register compiled Go type sdsv1.TxCommit
	commitMD := (&sdsv1.TxCommit{}).ProtoReflect().Descriptor()
	def, err := reg.RegisterDescriptor(commitMD, 1)
	if err != nil {
		t.Fatalf("RegisterDescriptor(TxCommit) error: %v", err)
	}

	msgType, err := reg.ResolveMessageType(def.GetId())
	if err != nil {
		t.Fatalf("ResolveMessageType error: %v", err)
	}

	// Verify that instantiated message is the concrete Go type
	instance := msgType.New().Interface()
	if _, ok := instance.(*sdsv1.TxCommit); !ok {
		t.Errorf("instance type = %T, want *sdsv1.TxCommit", instance)
	}
}
