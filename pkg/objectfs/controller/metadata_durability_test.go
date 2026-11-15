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

package controller

import (
	"fmt"
	"testing"
	"time"

	pb "github.com/gke-labs/in-cluster-storage/pkg/api/objectfs/v1alpha1"
	"github.com/gke-labs/in-cluster-storage/pkg/wal/buffer/buffertest"
	walclient "github.com/gke-labs/in-cluster-storage/pkg/wal/client"
	"google.golang.org/protobuf/proto"
)

// newMetadataDurabilityTestServer creates a test server and volume backed by recordingStream.
// Registers server cleanup on t. Takes no locks.
func newMetadataDurabilityTestServer(t *testing.T, stream *recordingStream, mode pb.MetadataDurability) (*Server, *Volume) {
	t.Helper()
	var serverOptions []ServerOption
	serverOptions = append(serverOptions,
		WithServerStreamFactory(func(string) (walclient.Stream, error) {
			return stream, nil
		}),
		WithServerDurability(walclient.Witness),
	)
	if mode != pb.MetadataDurability_METADATA_DURABILITY_UNSPECIFIED {
		serverOptions = append(serverOptions, WithServerMetadataDurability(mode))
	}
	server := NewTestServer(t, NewMemoryBackend(), serverOptions...)
	t.Cleanup(func() {
		if err := server.Close(); err != nil {
			t.Errorf("server.Close failed: %v", err)
		}
	})
	volume, err := server.getOrCreateVolume("test-vol")
	if err != nil {
		t.Fatalf("getOrCreateVolume failed: %v", err)
	}
	return server, volume
}

// TestMetadataDurability_BarrierMode verifies that under barrier durability mode,
// metadata operations return without waiting on the WAL, and a subsequent fsync
// barrier collects durability by waiting on the newest commit sequence.
func TestMetadataDurability_BarrierMode(t *testing.T) {
	ctx := t.Context()
	stream := &recordingStream{}
	server, volume := newMetadataDurabilityTestServer(t, stream, pb.MetadataDurability_BARRIER)

	if volume.MetadataDurability() != pb.MetadataDurability_BARRIER {
		t.Fatalf("volume metadata durability = %v, want %v", volume.MetadataDurability(), pb.MetadataDurability_BARRIER)
	}

	// Reset counters after setup so assertions are not already true.
	stream.waitCount.Store(0)
	stream.lastWaitSequence.Store(0)

	createResponse, err := server.CreateFile(ctx, &pb.CreateFileRequest{
		VolumeId:    "test-vol",
		ParentInode: 1,
		Name:        "file1.txt",
		Mode:        0644,
	})
	if err != nil || createResponse.GetError() != 0 {
		t.Fatalf("CreateFile failed: %v (code %d)", err, createResponse.GetError())
	}
	if waits := stream.waitCount.Load(); waits != 0 {
		t.Fatalf("expected 0 durability waits for CreateFile in barrier mode, got %d", waits)
	}

	setAttrResponse, err := server.SetAttr(ctx, &pb.SetAttrRequest{
		VolumeId: "test-vol",
		Inode:    createResponse.GetAttr().GetInode().GetIno(),
		Mode:     proto.Uint32(0600),
	})
	if err != nil || setAttrResponse.GetError() != 0 {
		t.Fatalf("SetAttr failed: %v (code %d)", err, setAttrResponse.GetError())
	}
	if waits := stream.waitCount.Load(); waits != 0 {
		t.Fatalf("expected 0 durability waits after SetAttr in barrier mode, got %d", waits)
	}

	fsyncResponse, err := server.Fsync(ctx, &pb.FsyncRequest{
		VolumeId: "test-vol",
		Inode:    createResponse.GetAttr().GetInode().GetIno(),
		Blocking: true,
	})
	if err != nil || !fsyncResponse.GetSuccess() {
		t.Fatalf("Fsync failed: %v (resp %v)", err, fsyncResponse)
	}
	if waits := stream.waitCount.Load(); waits != 1 {
		t.Fatalf("expected exactly 1 durability wait from Fsync barrier, got %d", waits)
	}
	if lastWait := stream.lastWaitSequence.Load(); lastWait < volume.Position() {
		t.Fatalf("expected lastWaitSequence >= volume position %d, got %d", volume.Position(), lastWait)
	}
}

// TestMetadataDurability_OperationMode verifies that under per-operation durability mode,
// each metadata operation waits once on the WAL.
func TestMetadataDurability_OperationMode(t *testing.T) {
	ctx := t.Context()
	stream := &recordingStream{}
	server, volume := newMetadataDurabilityTestServer(t, stream, pb.MetadataDurability_OPERATION)

	if volume.MetadataDurability() != pb.MetadataDurability_OPERATION {
		t.Fatalf("volume metadata durability = %v, want %v", volume.MetadataDurability(), pb.MetadataDurability_OPERATION)
	}

	// Reset counters after setup so assertions are not already true.
	stream.waitCount.Store(0)
	stream.lastWaitSequence.Store(0)

	createResponse, err := server.CreateFile(ctx, &pb.CreateFileRequest{
		VolumeId:    "test-vol",
		ParentInode: 1,
		Name:        "file_op.txt",
		Mode:        0644,
	})
	if err != nil || createResponse.GetError() != 0 {
		t.Fatalf("CreateFile failed: %v (code %d)", err, createResponse.GetError())
	}
	if waits := stream.waitCount.Load(); waits != 1 {
		t.Fatalf("expected 1 durability wait for CreateFile in operation mode, got %d", waits)
	}

	stream.waitCount.Store(0)
	setAttrResponse, err := server.SetAttr(ctx, &pb.SetAttrRequest{
		VolumeId: "test-vol",
		Inode:    createResponse.GetAttr().GetInode().GetIno(),
		Mode:     proto.Uint32(0600),
	})
	if err != nil || setAttrResponse.GetError() != 0 {
		t.Fatalf("SetAttr failed: %v (code %d)", err, setAttrResponse.GetError())
	}
	if waits := stream.waitCount.Load(); waits != 1 {
		t.Fatalf("expected 1 durability wait for SetAttr in operation mode, got %d", waits)
	}
}

// TestMetadataDurability_DefaultIsOperationMode verifies that the default metadata durability
// mode is per-operation, preserving legacy behaviour without explicit configuration.
func TestMetadataDurability_DefaultIsOperationMode(t *testing.T) {
	ctx := t.Context()
	stream := &recordingStream{}
	server, volume := newMetadataDurabilityTestServer(t, stream, pb.MetadataDurability_METADATA_DURABILITY_UNSPECIFIED)

	if volume.MetadataDurability() != pb.MetadataDurability_OPERATION {
		t.Fatalf("default volume metadata durability = %v, want %v", volume.MetadataDurability(), pb.MetadataDurability_OPERATION)
	}

	stream.waitCount.Store(0)
	stream.lastWaitSequence.Store(0)

	createResponse, err := server.CreateFile(ctx, &pb.CreateFileRequest{
		VolumeId:    "test-vol",
		ParentInode: 1,
		Name:        "file_default.txt",
		Mode:        0644,
	})
	if err != nil || createResponse.GetError() != 0 {
		t.Fatalf("CreateFile failed: %v (code %d)", err, createResponse.GetError())
	}
	if waits := stream.waitCount.Load(); waits != 1 {
		t.Fatalf("expected 1 durability wait for CreateFile in default mode, got %d", waits)
	}
}

// runBenchmarkCreate1000Files measures the elapsed time to create 1000 files on a single volume
// under the specified metadata durability mode against an in-process buffer. Takes no locks.
func runBenchmarkCreate1000Files(b *testing.B, mode pb.MetadataDurability) {
	ctx := b.Context()
	backend := NewMemoryBackend()
	bufferServer, bufferCleanup := buffertest.Start(ctx, b, buffertest.WithBackend(backend))
	b.Cleanup(bufferCleanup)

	walDirectory := b.TempDir()
	server := NewTestServer(b, backend,
		WithServerWAL(walDirectory, bufferServer.Target, walclient.Witness),
		WithServerWALDialOptions(bufferServer.DialOpts...),
		WithServerMetadataDurability(mode),
	)
	b.Cleanup(func() {
		if err := server.Close(); err != nil {
			b.Errorf("server.Close failed: %v", err)
		}
	})

	volumeID := fmt.Sprintf("bench-1000-files-%v", mode)
	b.ResetTimer()

	start := time.Now()
	for i := 0; i < 1000; i++ {
		response, err := server.CreateFile(ctx, &pb.CreateFileRequest{
			VolumeId:    volumeID,
			ParentInode: 1,
			Name:        fmt.Sprintf("file_%d.txt", i),
			Mode:        0644,
		})
		if err != nil || response.GetError() != 0 {
			b.Fatalf("CreateFile %d failed: err=%v, resp=%v", i, err, response)
		}
	}
	elapsed := time.Since(start)
	b.Logf("[%v] Created 1000 files in %v (%v/op)", mode, elapsed, elapsed/1000)
}

// BenchmarkCreate1000Files_Barrier measures 1000 file creates in barrier metadata durability mode. Takes no locks.
func BenchmarkCreate1000Files_Barrier(b *testing.B) {
	runBenchmarkCreate1000Files(b, pb.MetadataDurability_BARRIER)
}

// BenchmarkCreate1000Files_Operation measures 1000 file creates in per-operation metadata durability mode. Takes no locks.
func BenchmarkCreate1000Files_Operation(b *testing.B) {
	runBenchmarkCreate1000Files(b, pb.MetadataDurability_OPERATION)
}
