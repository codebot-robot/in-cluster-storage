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

package fuse

import (
	"bytes"
	"context"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	pb "github.com/gke-labs/in-cluster-storage/pkg/api/objectfs/v1alpha1"
	"github.com/hanwen/go-fuse/v2/fuse"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// fakeStageControllerClient mocks controller staging, packing, and epochs for client tests.
type fakeStageControllerClient struct {
	pb.ObjectFSControllerClient

	mu               sync.Mutex
	stagingEpoch     uint64
	writeCalls       int
	fsyncCalls       int
	releaseCalls     int
	blockingSyncs    int
	nonblockingSyncs int
	failEpoch        bool
	injectedFsyncErr int32

	stagedBytes int64
}

func newFakeStageController(baseClient pb.ObjectFSControllerClient) *fakeStageControllerClient {
	return &fakeStageControllerClient{
		ObjectFSControllerClient: baseClient,
		stagingEpoch:             100,
	}
}

func (f *fakeStageControllerClient) WriteFile(ctx context.Context, in *pb.WriteFileRequest, opts ...grpc.CallOption) (*pb.WriteFileResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.writeCalls++
	f.stagedBytes += int64(len(in.GetData()))
	return &pb.WriteFileResponse{
		BytesWritten: int64(len(in.GetData())),
		NewSize:      in.GetOffset() + int64(len(in.GetData())),
		ModTime:      timestamppb.Now(),
		StagingEpoch: f.stagingEpoch,
	}, nil
}

func (f *fakeStageControllerClient) Fsync(ctx context.Context, in *pb.FsyncRequest, opts ...grpc.CallOption) (*pb.FsyncResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.fsyncCalls++
	if in.GetBlocking() {
		f.blockingSyncs++
	} else {
		f.nonblockingSyncs++
	}

	if f.failEpoch || (in.GetStagingEpoch() != 0 && in.GetStagingEpoch() != f.stagingEpoch) {
		return &pb.FsyncResponse{
			Success:      false,
			Error:        int32(syscall.ESTALE),
			StagingEpoch: f.stagingEpoch,
		}, nil
	}

	if f.injectedFsyncErr != 0 {
		errCode := f.injectedFsyncErr
		f.injectedFsyncErr = 0
		return &pb.FsyncResponse{
			Success:      false,
			Error:        errCode,
			StagingEpoch: f.stagingEpoch,
		}, nil
	}

	f.stagedBytes = 0
	return &pb.FsyncResponse{
		Success:      true,
		StagingEpoch: f.stagingEpoch,
	}, nil
}

func (f *fakeStageControllerClient) Release(ctx context.Context, in *pb.ReleaseRequest, opts ...grpc.CallOption) (*pb.ReleaseResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.releaseCalls++
	return &pb.ReleaseResponse{}, nil
}

// TestFUSEReleaseNonBlockingSyncOnClose tests:
// Close of a written file (Release) schedules non-blocking sync without blocking caller.
func TestFUSEReleaseNonBlockingSyncOnClose(t *testing.T) {
	baseClient, cleanup := createTestClient(t)
	defer cleanup()
	client := newFakeStageController(baseClient)
	cache := NewNodeCache(8 * 1024 * 1024)
	fs := NewObjectFS(client, "vol-release-sync", pb.WriteMode_WRITE_THROUGH_FSYNC, cache)
	defer fs.Close()

	var createOut fuse.CreateOut
	if status := fs.Create(nil, &fuse.CreateIn{InHeader: fuse.InHeader{NodeId: 1}, Mode: 0644}, "test.txt", &createOut); status != fuse.OK {
		t.Fatalf("Create failed: %v", status)
	}
	ino := createOut.EntryOut.NodeId

	// Write data to file
	data := []byte("content written to file before close")
	written, status := fs.Write(nil, &fuse.WriteIn{InHeader: fuse.InHeader{NodeId: ino}, Offset: 0}, data)
	if status != fuse.OK || int(written) != len(data) {
		t.Fatalf("Write failed: status=%v", status)
	}

	// Release should return immediately without blocking!
	start := time.Now()
	fs.Release(nil, &fuse.ReleaseIn{InHeader: fuse.InHeader{NodeId: ino}, Fh: 1})
	duration := time.Since(start)
	if duration > 50*time.Millisecond {
		t.Fatalf("Release took %v; expected non-blocking immediate return", duration)
	}

	// Wait for background syncer to process the non-blocking sync
	deadline := time.Now().Add(1 * time.Second)
	for time.Now().Before(deadline) {
		client.mu.Lock()
		nonblocking := client.nonblockingSyncs
		client.mu.Unlock()
		if nonblocking > 0 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	client.mu.Lock()
	if client.nonblockingSyncs == 0 {
		t.Fatalf("Expected background syncer to issue non-blocking Fsync after Release")
	}
	client.mu.Unlock()

	// Cache should be clean after the covering sync acked
	if _, isDirty := cache.GetDirty(ino); isDirty {
		t.Fatalf("Expected cache to be clean after background sync acked")
	}
}

// TestFUSEClientEpochMismatchRecovery tests:
// Controller crash before sync returns ESTALE / epoch mismatch;
// client re-pushes still-dirty chunks and successfully recovers.
func TestFUSEClientEpochMismatchRecovery(t *testing.T) {
	baseClient, cleanup := createTestClient(t)
	defer cleanup()
	client := newFakeStageController(baseClient)
	cache := NewNodeCache(8 * 1024 * 1024)
	fs := NewObjectFS(client, "vol-epoch-recovery", pb.WriteMode_WRITE_THROUGH_FSYNC, cache)
	defer fs.Close()

	var createOut fuse.CreateOut
	if status := fs.Create(nil, &fuse.CreateIn{InHeader: fuse.InHeader{NodeId: 1}, Mode: 0644}, "recover.txt", &createOut); status != fuse.OK {
		t.Fatalf("Create failed: %v", status)
	}
	ino := createOut.EntryOut.NodeId

	// Write data
	data := []byte("data staged before crash")
	fs.Write(nil, &fuse.WriteIn{InHeader: fuse.InHeader{NodeId: ino}, Offset: 0}, data)

	// Stage writes via syncFileToService (which records epoch 100)
	ctx := t.Context()
	if err := fs.syncFileToService(ctx, ino); err != nil {
		t.Fatalf("syncFileToService failed: %v", err)
	}

	// Simulate controller crash and restart with a new epoch 200
	client.mu.Lock()
	client.stagingEpoch = 200
	client.mu.Unlock()

	// Calling Fsync: client sends epoch 100, controller rejects with ESTALE + epoch 200,
	// client automatically detects epoch mismatch, re-pushes dirty data, and retries Fsync!
	if status := fs.Fsync(nil, &fuse.FsyncIn{InHeader: fuse.InHeader{NodeId: ino}}); status != fuse.OK {
		t.Fatalf("Fsync with epoch recovery failed: status=%v", status)
	}

	// Verify that client re-pushed (at least 2 WriteFile calls total)
	client.mu.Lock()
	writeCalls := client.writeCalls
	client.mu.Unlock()
	if writeCalls < 2 {
		t.Fatalf("Expected client to re-push WriteFile after epoch mismatch, got %d calls", writeCalls)
	}

	// Verify cache is now clean after successful retry
	if _, isDirty := cache.GetDirty(ino); isDirty {
		t.Fatalf("Expected cache to be clean after successful epoch recovery")
	}
}

// TestFUSECacheBudgetOverflowThrottling tests:
// Client cache budget overflow: dirty bytes reaching maxBytes causes write to wait for sync.
func TestFUSECacheBudgetOverflowThrottling(t *testing.T) {
	baseClient, cleanup := createTestClient(t)
	defer cleanup()
	client := newFakeStageController(baseClient)
	const maxCacheBytes = 128 * 1024 // 128 KiB cache budget
	cache := NewNodeCache(maxCacheBytes)
	fs := NewObjectFS(client, "vol-cache-budget", pb.WriteMode_WRITE_THROUGH_FSYNC, cache)
	defer fs.Close()

	var createOut fuse.CreateOut
	if status := fs.Create(nil, &fuse.CreateIn{InHeader: fuse.InHeader{NodeId: 1}, Mode: 0644}, "overflow.txt", &createOut); status != fuse.OK {
		t.Fatalf("Create failed: %v", status)
	}
	ino := createOut.EntryOut.NodeId

	// Write 128 KiB: fills cache budget with dirty bytes
	chunk64k := bytes.Repeat([]byte("A"), 64*1024)
	fs.Write(nil, &fuse.WriteIn{InHeader: fuse.InHeader{NodeId: ino}, Offset: 0}, chunk64k)
	fs.Write(nil, &fuse.WriteIn{InHeader: fuse.InHeader{NodeId: ino}, Offset: 64 * 1024}, chunk64k)

	client.mu.Lock()
	syncsBefore := client.blockingSyncs
	client.mu.Unlock()

	// Next write would overflow the dirty budget:
	// It MUST trigger a synchronous sync to clean dirty chunks and unblock!
	var writeCompleted atomic.Bool
	go func() {
		fs.Write(nil, &fuse.WriteIn{InHeader: fuse.InHeader{NodeId: ino}, Offset: 128 * 1024}, []byte("overflow-data"))
		writeCompleted.Store(true)
	}()

	// Wait for write to complete
	deadline := time.Now().Add(1 * time.Second)
	for time.Now().Before(deadline) {
		if writeCompleted.Load() {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	if !writeCompleted.Load() {
		t.Fatalf("Write should have unblocked after triggering cache flush")
	}

	client.mu.Lock()
	syncsAfter := client.blockingSyncs
	client.mu.Unlock()

	if syncsAfter <= syncsBefore {
		t.Fatalf("Expected cache budget overflow to trigger blocking sync (before=%d, after=%d)", syncsBefore, syncsAfter)
	}
}
