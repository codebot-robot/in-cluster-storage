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
	"crypto/sha256"
	"fmt"
	"sort"

	sdsv1 "github.com/gke-labs/in-cluster-storage/pkg/api/sds/v1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"
)

// Canonical Fingerprint Algorithm:
//
// The fingerprint is a cryptographic 32-byte SHA-256 digest over the canonical
// binary serialization of a google.protobuf.FileDescriptorSet representing all
// transitive dependencies required by a registered protobuf message type.
//
// Canonicalization and serialization procedure:
// 1. Traverse the message's ParentFile and all its transitive Imports (dependencies).
// 2. Convert each unique protoreflect.FileDescriptor into a descriptorpb.FileDescriptorProto
//    using protodesc.ToFileDescriptorProto.
// 3. Assemble all FileDescriptorProto objects into a FileDescriptorSet.
// 4. Sort the FileDescriptorProto slice lexicographically in ascending order by file path
//    (using FileDescriptorProto.GetName()).
// 5. Serialize the FileDescriptorSet into protobuf wire format using deterministic serialization
//    (proto.MarshalOptions{Deterministic: true}). This guarantees deterministic tag ordering
//    and map entry sorting.
// 6. Compute the SHA-256 digest of these canonical serialized wire bytes.
//
// This procedure is fully reproducible across implementations.

// CollectFileDescriptors collects all unique transitive FileDescriptorProto dependencies
// for a root FileDescriptor, sorted lexicographically by file name.
func CollectFileDescriptors(rootFD protoreflect.FileDescriptor) []*descriptorpb.FileDescriptorProto {
	if rootFD == nil {
		return nil
	}
	visited := make(map[string]bool)
	var files []*descriptorpb.FileDescriptorProto

	var walk func(fd protoreflect.FileDescriptor)
	walk = func(fd protoreflect.FileDescriptor) {
		if fd == nil {
			return
		}
		path := fd.Path()
		if visited[path] {
			return
		}
		visited[path] = true

		imports := fd.Imports()
		for i := 0; i < imports.Len(); i++ {
			walk(imports.Get(i).FileDescriptor)
		}
		files = append(files, protodesc.ToFileDescriptorProto(fd))
	}

	walk(rootFD)

	sort.Slice(files, func(i, j int) bool {
		return files[i].GetName() < files[j].GetName()
	})

	return files
}

// ExtractFileDescriptorSet extracts the complete transitive FileDescriptorSet for a message descriptor.
func ExtractFileDescriptorSet(md protoreflect.MessageDescriptor) (*descriptorpb.FileDescriptorSet, error) {
	if md == nil {
		return nil, fmt.Errorf("nil message descriptor")
	}
	files := CollectFileDescriptors(md.ParentFile())
	return &descriptorpb.FileDescriptorSet{File: files}, nil
}

// CanonicalMarshalFileDescriptorSet canonically orders and serializes a FileDescriptorSet.
func CanonicalMarshalFileDescriptorSet(fds *descriptorpb.FileDescriptorSet) ([]byte, error) {
	if fds == nil {
		return nil, fmt.Errorf("nil FileDescriptorSet")
	}
	// Make a shallow copy of the FileDescriptorSet and sort files by name.
	files := make([]*descriptorpb.FileDescriptorProto, len(fds.GetFile()))
	copy(files, fds.GetFile())
	sort.Slice(files, func(i, j int) bool {
		return files[i].GetName() < files[j].GetName()
	})

	sortedSet := &descriptorpb.FileDescriptorSet{
		File: files,
	}

	opts := proto.MarshalOptions{
		Deterministic: true,
	}
	return opts.Marshal(sortedSet)
}

// ComputeFingerprint calculates the 32-byte SHA-256 fingerprint for a FileDescriptorSet.
func ComputeFingerprint(fds *descriptorpb.FileDescriptorSet) ([]byte, error) {
	canonicalBytes, err := CanonicalMarshalFileDescriptorSet(fds)
	if err != nil {
		return nil, err
	}
	h := sha256.Sum256(canonicalBytes)
	return h[:], nil
}

// ComputeMessageFingerprint extracts the FileDescriptorSet for a MessageDescriptor and computes its fingerprint.
func ComputeMessageFingerprint(md protoreflect.MessageDescriptor) ([]byte, *descriptorpb.FileDescriptorSet, error) {
	fds, err := ExtractFileDescriptorSet(md)
	if err != nil {
		return nil, nil, err
	}
	fp, err := ComputeFingerprint(fds)
	if err != nil {
		return nil, nil, err
	}
	return fp, fds, nil
}

// BuildTypeDefinition constructs a TypeDefinition proto for a message descriptor.
func BuildTypeDefinition(typeID uint32, md protoreflect.MessageDescriptor, keyFields []int32) (*sdsv1.TypeDefinition, error) {
	if md == nil {
		return nil, fmt.Errorf("nil message descriptor")
	}
	fp, fds, err := ComputeMessageFingerprint(md)
	if err != nil {
		return nil, err
	}
	kf := make([]int32, len(keyFields))
	copy(kf, keyFields)

	return &sdsv1.TypeDefinition{
		Id:          typeID,
		Name:        string(md.FullName()),
		Fingerprint: fp,
		Descriptors: fds,
		KeyFields:   kf,
	}, nil
}
