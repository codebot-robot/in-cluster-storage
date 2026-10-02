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
	"testing"

	sdsv1 "github.com/gke-labs/in-cluster-storage/pkg/api/sds/v1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/descriptorpb"
)

func TestFingerprintDeterminism(t *testing.T) {
	fileA := &descriptorpb.FileDescriptorProto{
		Name:    proto.String("a.proto"),
		Package: proto.String("pkg.a"),
	}
	fileB := &descriptorpb.FileDescriptorProto{
		Name:    proto.String("b.proto"),
		Package: proto.String("pkg.b"),
	}

	set1 := &descriptorpb.FileDescriptorSet{
		File: []*descriptorpb.FileDescriptorProto{fileA, fileB},
	}
	set2 := &descriptorpb.FileDescriptorSet{
		File: []*descriptorpb.FileDescriptorProto{fileB, fileA},
	}

	fp1, err := ComputeFingerprint(set1)
	if err != nil {
		t.Fatalf("ComputeFingerprint(set1) error: %v", err)
	}
	fp2, err := ComputeFingerprint(set2)
	if err != nil {
		t.Fatalf("ComputeFingerprint(set2) error: %v", err)
	}

	if len(fp1) != 32 {
		t.Errorf("fingerprint length = %d, want 32", len(fp1))
	}
	if !bytes.Equal(fp1, fp2) {
		t.Errorf("fingerprint not deterministic with different file order: %x vs %x", fp1, fp2)
	}
}

func TestBuildTypeDefinition(t *testing.T) {
	msg := (&sdsv1.TxCommit{}).ProtoReflect().Descriptor()
	def, err := BuildTypeDefinition(16, msg, []int32{1})
	if err != nil {
		t.Fatalf("BuildTypeDefinition error: %v", err)
	}

	if def.GetId() != 16 {
		t.Errorf("def.Id = %d, want 16", def.GetId())
	}
	if def.GetName() != "sds.v1.TxCommit" {
		t.Errorf("def.Name = %q, want 'sds.v1.TxCommit'", def.GetName())
	}
	if len(def.GetFingerprint()) != 32 {
		t.Errorf("len(def.Fingerprint) = %d, want 32", len(def.GetFingerprint()))
	}
	if len(def.GetKeyFields()) != 1 || def.GetKeyFields()[0] != 1 {
		t.Errorf("def.KeyFields = %v, want [1]", def.GetKeyFields())
	}
	if def.GetDescriptors() == nil || len(def.GetDescriptors().GetFile()) == 0 {
		t.Errorf("def.Descriptors is empty")
	}
}
