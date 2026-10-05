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

package controller

import (
	"syscall"
	"testing"

	pb "github.com/gke-labs/in-cluster-storage/pkg/api/objectfs/v1alpha1"
	"google.golang.org/protobuf/proto"
)

func TestUpdateVolumeStats_TypedDispatch(t *testing.T) {
	st := &pb.VolumeStats{Name: "volume"}

	// 1. Create Inode (file, size 500, ino 100)
	fileIn := &pb.Inode{
		Ino:   proto.Uint64(100),
		Mode:  0644 | syscall.S_IFREG,
		Size:  500,
		IsDir: false,
	}
	if err := UpdateVolumeStats(st, nil, fileIn); err != nil {
		t.Fatalf("UpdateVolumeStats failed: %v", err)
	}
	if st.GetMaxIno() != 100 || st.GetInodesFile() != 1 || st.GetLogicalBytes() != 500 {
		t.Errorf("stats after file create: %+v", st)
	}

	// 2. Update Inode size (500 -> 1200)
	updatedFile := &pb.Inode{
		Ino:   proto.Uint64(100),
		Mode:  0644 | syscall.S_IFREG,
		Size:  1200,
		IsDir: false,
	}
	if err := UpdateVolumeStats(st, fileIn, updatedFile); err != nil {
		t.Fatalf("UpdateVolumeStats failed: %v", err)
	}
	if st.GetLogicalBytes() != 1200 {
		t.Errorf("logical bytes after size update: got %d, want 1200", st.GetLogicalBytes())
	}

	// 3. Create Directory (ino 101)
	dirIn := &pb.Inode{
		Ino:   proto.Uint64(101),
		Mode:  0755 | syscall.S_IFDIR,
		IsDir: true,
	}
	if err := UpdateVolumeStats(st, nil, dirIn); err != nil {
		t.Fatalf("UpdateVolumeStats failed: %v", err)
	}
	if st.GetMaxIno() != 101 || st.GetInodesDir() != 1 {
		t.Errorf("stats after dir create: %+v", st)
	}

	// 4. Create FileChunk
	chunk := &pb.FileChunk{Ino: proto.Uint64(100), Index: proto.Uint32(0)}
	if err := UpdateVolumeStats(st, nil, chunk); err != nil {
		t.Fatalf("UpdateVolumeStats chunk failed: %v", err)
	}
	if st.GetFileChunks() != 1 {
		t.Errorf("file chunks: got %d, want 1", st.GetFileChunks())
	}

	// 5. Delete FileChunk
	if err := UpdateVolumeStats(st, chunk, nil); err != nil {
		t.Fatalf("UpdateVolumeStats delete chunk failed: %v", err)
	}
	if st.GetFileChunks() != 0 {
		t.Errorf("file chunks after delete: got %d, want 0", st.GetFileChunks())
	}

	// 6. Delete File
	if err := UpdateVolumeStats(st, updatedFile, nil); err != nil {
		t.Fatalf("UpdateVolumeStats delete file failed: %v", err)
	}
	if st.GetInodesFile() != 0 || st.GetLogicalBytes() != 0 {
		t.Errorf("stats after file delete: %+v", st)
	}

	// 7. Unexpected row type returns error
	if err := UpdateVolumeStats(st, nil, &pb.GetVolumeStatsRequest{}); err == nil {
		t.Errorf("expected error for unexpected row type, got nil")
	}
}
