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
	"bytes"
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	pb "github.com/gke-labs/in-cluster-storage/pkg/api/objectfs/v1alpha1"
	"github.com/gke-labs/in-cluster-storage/pkg/objectfs/blob"
	"google.golang.org/protobuf/proto"
)

// recordingBackend wraps ObjectStorageBackend and tracks PutObject calls and stored object keys.
type recordingBackend struct {
	ObjectStorageBackend
	mu       sync.Mutex
	putCount int
	keys     []string
	failPuts bool
}

func newRecordingBackend(backend ObjectStorageBackend) *recordingBackend {
	return &recordingBackend{
		ObjectStorageBackend: backend,
	}
}

func (r *recordingBackend) PutObject(ctx context.Context, volumeID, key string, data blob.ByteStream) (string, error) {
	r.mu.Lock()
	if r.failPuts {
		r.mu.Unlock()
		return "", fmt.Errorf("injected backend PutObject error")
	}
	r.putCount++
	r.keys = append(r.keys, key)
	r.mu.Unlock()
	return r.ObjectStorageBackend.PutObject(ctx, volumeID, key, data)
}

func (r *recordingBackend) Puts() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.putCount
}

func (r *recordingBackend) StoredKeys() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	res := make([]string, len(r.keys))
	copy(res, r.keys)
	return res
}

// TestStagedWritesPackOnFsync tests:
// N writes across several inodes, one fsync: one pack object and all rows in one transaction logged after it.
func TestStagedWritesPackOnFsync(t *testing.T) {
	ctx := t.Context()
	rawBackend := NewMemoryBackend()
	recBackend := newRecordingBackend(rawBackend)
	server := NewServer(recBackend)
	defer func() { _ = server.Close() }()

	volumeID := "vol-pack-test"

	// Create 3 files
	const numFiles = 3
	fileInos := make([]uint64, numFiles)
	for i := 0; i < numFiles; i++ {
		resp, err := server.CreateFile(ctx, &pb.CreateFileRequest{
			VolumeId:    volumeID,
			ParentInode: 1,
			Name:        fmt.Sprintf("file_%d.bin", i),
			Mode:        0644,
		})
		if err != nil || resp.GetError() != 0 {
			t.Fatalf("CreateFile %d failed: %v", i, err)
		}
		fileInos[i] = resp.GetAttr().GetInode().GetIno()
	}

	initialPuts := recBackend.Puts()

	// Write data to each file across multiple chunks (e.g. 128 KiB each = 2 chunks each)
	chunkSize := 64 * 1024
	fileData := make([][]byte, numFiles)
	for i := 0; i < numFiles; i++ {
		fileData[i] = bytes.Repeat([]byte{byte(i + 1)}, 2*chunkSize)
		wResp, err := server.WriteFile(ctx, &pb.WriteFileRequest{
			VolumeId: volumeID,
			Inode:    fileInos[i],
			Offset:   0,
			Data:     fileData[i],
		})
		if err != nil || wResp.GetError() != 0 {
			t.Fatalf("WriteFile %d failed: %v", i, err)
		}
	}

	// Verify no PUTs happened to object storage during WriteFile (all staged in memory)
	if recBackend.Puts() != initialPuts {
		t.Fatalf("Expected 0 PutObject calls during WriteFile, got %d", recBackend.Puts()-initialPuts)
	}

	// Now execute one Fsync covering the volume
	fsyncResp, err := server.Fsync(ctx, &pb.FsyncRequest{
		VolumeId: volumeID,
		Inode:    0,
		Blocking: proto.Bool(true),
	})
	if err != nil || !fsyncResp.GetSuccess() {
		t.Fatalf("Fsync failed: %v, resp=%v", err, fsyncResp)
	}

	// Exactly 1 pack object should be created for all chunks across all 3 files
	newPuts := recBackend.Puts() - initialPuts
	if newPuts != 1 {
		t.Fatalf("Expected exactly 1 pack PutObject call on fsync, got %d", newPuts)
	}

	// Verify all 3 files can now be read back from controller
	for i := 0; i < numFiles; i++ {
		rResp, err := server.ReadFile(ctx, &pb.ReadFileRequest{
			VolumeId: volumeID,
			Inode:    fileInos[i],
			Offset:   0,
			Size:     int64(len(fileData[i])),
		})
		if err != nil || rResp.GetError() != 0 {
			t.Fatalf("ReadFile %d failed: %v", i, err)
		}
		if !bytes.Equal(rResp.GetData(), fileData[i]) {
			t.Fatalf("Content mismatch for file %d", i)
		}
	}
}

// TestSecondClientReadsBetweenWriteAndFsync tests:
// Second client reads between write and fsync: pre-write content and size; after fsync: new content.
func TestSecondClientReadsBetweenWriteAndFsync(t *testing.T) {
	ctx := t.Context()
	backend := NewMemoryBackend()
	server := NewServer(backend)
	defer func() { _ = server.Close() }()

	volumeID := "vol-visibility"

	initContent := []byte("hello initial version")
	createResp, err := server.CreateFile(ctx, &pb.CreateFileRequest{
		VolumeId:       volumeID,
		ParentInode:    1,
		Name:           "doc.txt",
		Mode:           0644,
		InitialContent: initContent,
	})
	if err != nil || createResp.GetError() != 0 {
		t.Fatalf("CreateFile failed: %v", err)
	}
	ino := createResp.GetAttr().GetInode().GetIno()

	// Client 1 stages a write with new content
	newContent := []byte("overwritten with fresh content!")
	wResp, err := server.WriteFile(ctx, &pb.WriteFileRequest{
		VolumeId: volumeID,
		Inode:    ino,
		Offset:   0,
		Data:     newContent,
	})
	if err != nil || wResp.GetError() != 0 {
		t.Fatalf("WriteFile failed: %v", err)
	}

	// Client 2 (reading through server ReadFile without fsync):
	// Must see pre-write content and size!
	readPre, err := server.ReadFile(ctx, &pb.ReadFileRequest{
		VolumeId: volumeID,
		Inode:    ino,
		Offset:   0,
		Size:     1024,
	})
	if err != nil || readPre.GetError() != 0 {
		t.Fatalf("ReadFile pre-sync failed: %v", err)
	}
	if !bytes.Equal(readPre.GetData(), initContent) {
		t.Fatalf("Pre-sync read got %q, want pre-write content %q", string(readPre.GetData()), string(initContent))
	}
	if readPre.GetTotalSize() != int64(len(initContent)) {
		t.Fatalf("Pre-sync total size got %d, want %d", readPre.GetTotalSize(), len(initContent))
	}

	// Client 1 calls Fsync
	fsyncResp, err := server.Fsync(ctx, &pb.FsyncRequest{
		VolumeId: volumeID,
		Inode:    ino,
		Blocking: proto.Bool(true),
	})
	if err != nil || !fsyncResp.GetSuccess() {
		t.Fatalf("Fsync failed: %v", err)
	}

	// Client 2 reads again: must now see new content and size!
	readPost, err := server.ReadFile(ctx, &pb.ReadFileRequest{
		VolumeId: volumeID,
		Inode:    ino,
		Offset:   0,
		Size:     1024,
	})
	if err != nil || readPost.GetError() != 0 {
		t.Fatalf("ReadFile post-sync failed: %v", err)
	}
	if !bytes.Equal(readPost.GetData(), newContent) {
		t.Fatalf("Post-sync read got %q, want new content %q", string(readPost.GetData()), string(newContent))
	}
	if readPost.GetTotalSize() != int64(len(newContent)) {
		t.Fatalf("Post-sync total size got %d, want %d", readPost.GetTotalSize(), len(newContent))
	}
}

// TestCrashBetweenWritesAndFsync_CleanRestart tests:
// Crash between writes and fsync, restart on the same backend:
// files read as of the last sync, no EIO, no dangling rows; then client re-push + fsync + read back.
func TestCrashBetweenWritesAndFsync_CleanRestart(t *testing.T) {
	ctx := t.Context()
	backend := NewMemoryBackend()
	volumeID := "vol-crash-restart"

	v1Content := []byte("version-1-durable-data")
	var ino uint64
	var epoch1 uint64

	// Session 1: write version 1, fsync, then write version 2 (staged), then crash!
	{
		server1 := NewServer(backend)

		cResp, err := server1.CreateFile(ctx, &pb.CreateFileRequest{
			VolumeId:       volumeID,
			ParentInode:    1,
			Name:           "testfile.bin",
			Mode:           0644,
			InitialContent: v1Content,
		})
		if err != nil || cResp.GetError() != 0 {
			t.Fatalf("CreateFile failed: %v", err)
		}
		ino = cResp.GetAttr().GetInode().GetIno()

		// Fsync version 1 so it is durable
		fResp, err := server1.Fsync(ctx, &pb.FsyncRequest{
			VolumeId: volumeID,
			Inode:    ino,
			Blocking: proto.Bool(true),
		})
		if err != nil || !fResp.GetSuccess() {
			t.Fatalf("Fsync v1 failed: %v", err)
		}
		if err := server1.FlushAll(ctx); err != nil {
			t.Fatalf("FlushAll v1 failed: %v", err)
		}

		// Stage version 2 writes
		v2Data := []byte("version-2-staged-not-synced")
		wResp, err := server1.WriteFile(ctx, &pb.WriteFileRequest{
			VolumeId: volumeID,
			Inode:    ino,
			Offset:   0,
			Data:     v2Data,
		})
		if err != nil || wResp.GetError() != 0 {
			t.Fatalf("WriteFile v2 failed: %v", err)
		}
		epoch1 = wResp.GetStagingEpoch()

		// Crash: Close server without syncing version 2!
		_ = server1.Close()
	}

	// Session 2: Restart server pointing to same backend
	{
		server2 := NewServer(backend)
		defer func() { _ = server2.Close() }()

		// Verify files read as of the last sync: no EIO, no dangling rows, returns version 1
		rResp, err := server2.ReadFile(ctx, &pb.ReadFileRequest{
			VolumeId: volumeID,
			Inode:    ino,
			Offset:   0,
			Size:     1024,
		})
		if err != nil || rResp.GetError() != 0 {
			t.Fatalf("ReadFile after crash failed with error: %v, code=%d", err, rResp.GetError())
		}
		if !bytes.Equal(rResp.GetData(), v1Content) {
			t.Fatalf("Expected version 1 content after crash restart, got %q", string(rResp.GetData()))
		}

		// Client sends Fsync with old staging epoch:
		// Controller must reject with recognizable epoch mismatch / ESTALE!
		staleFsync, err := server2.Fsync(ctx, &pb.FsyncRequest{
			VolumeId:     volumeID,
			Inode:        ino,
			Blocking:     proto.Bool(true),
			StagingEpoch: epoch1,
		})
		if err != nil {
			t.Fatalf("unexpected gRPC error on stale fsync: %v", err)
		}
		if staleFsync.GetError() != int32(syscall.ESTALE) {
			t.Fatalf("expected ESTALE error (%d), got %d", int32(syscall.ESTALE), staleFsync.GetError())
		}

		// Client now re-pushes still-dirty version 2 data with the new epoch and fsyncs
		v2Data := []byte("version-2-staged-not-synced")
		wResp2, err := server2.WriteFile(ctx, &pb.WriteFileRequest{
			VolumeId: volumeID,
			Inode:    ino,
			Offset:   0,
			Data:     v2Data,
		})
		if err != nil || wResp2.GetError() != 0 {
			t.Fatalf("WriteFile re-push failed: %v", err)
		}
		epoch2 := wResp2.GetStagingEpoch()
		if epoch2 == epoch1 {
			t.Fatalf("Expected new epoch after restart, got same %d", epoch2)
		}

		// Fsync version 2 succeeds
		fResp2, err := server2.Fsync(ctx, &pb.FsyncRequest{
			VolumeId:     volumeID,
			Inode:        ino,
			Blocking:     proto.Bool(true),
			StagingEpoch: epoch2,
		})
		if err != nil || !fResp2.GetSuccess() {
			t.Fatalf("Fsync v2 retry failed: %v", err)
		}

		// Read back version 2 data
		rResp2, err := server2.ReadFile(ctx, &pb.ReadFileRequest{
			VolumeId: volumeID,
			Inode:    ino,
			Offset:   0,
			Size:     1024,
		})
		if err != nil || rResp2.GetError() != 0 {
			t.Fatalf("ReadFile v2 failed: %v", err)
		}
		if !bytes.Equal(rResp2.GetData(), v2Data) {
			t.Fatalf("Expected version 2 data after re-push and fsync, got %q", string(rResp2.GetData()))
		}
	}
}

// TestCloseAndCoalesceWindow tests:
// 1. Close without fsync becomes visible to a second client within the coalesce window.
// 2. A burst of N small-file non-blocking syncs in the window yields one pack.
// 3. A blocking fsync mid-window returns only after its data is durable and includes the window's other writes.
// 4. A failed non-blocking sync is retried and surfaces on the next blocking fsync.
func TestCloseAndCoalesceWindow(t *testing.T) {
	ctx := t.Context()
	rawBackend := NewMemoryBackend()
	recBackend := newRecordingBackend(rawBackend)
	coalesceWindow := 100 * time.Millisecond

	server := NewServer(recBackend, WithServerSyncCoalesceWindow(coalesceWindow))
	defer func() { _ = server.Close() }()

	volumeID := "vol-coalesce"

	// 1. A burst of N small-file closes in the window yields one pack
	const burstCount = 10
	burstInos := make([]uint64, burstCount)
	for i := 0; i < burstCount; i++ {
		cResp, err := server.CreateFile(ctx, &pb.CreateFileRequest{
			VolumeId:    volumeID,
			ParentInode: 1,
			Name:        fmt.Sprintf("small_%d.txt", i),
			Mode:        0644,
		})
		if err != nil || cResp.GetError() != 0 {
			t.Fatalf("CreateFile %d failed: %v", i, err)
		}
		burstInos[i] = cResp.GetAttr().GetInode().GetIno()
	}

	initialPuts := recBackend.Puts()

	// Write and send non-blocking sync (simulating close with --sync-on-close) for all 10 files
	burstPayload := []byte("small file payload in window")
	for i := 0; i < burstCount; i++ {
		_, _ = server.WriteFile(ctx, &pb.WriteFileRequest{
			VolumeId: volumeID,
			Inode:    burstInos[i],
			Offset:   0,
			Data:     burstPayload,
		})
	}

	var burstWg sync.WaitGroup
	for i := 0; i < burstCount; i++ {
		burstWg.Add(1)
		go func(ino uint64) {
			defer burstWg.Done()
			resp, err := server.Fsync(ctx, &pb.FsyncRequest{
				VolumeId: volumeID,
				Inode:    ino,
				Blocking: proto.Bool(false),
			})
			if err != nil || !resp.GetSuccess() {
				t.Errorf("Non-blocking Fsync failed: %v", err)
			}
		}(burstInos[i])
	}
	burstWg.Wait()

	// Exactly 1 pack PUT should have been executed for all 10 small files in the window!
	if recBackend.Puts()-initialPuts != 1 {
		t.Fatalf("Expected 1 coalesced pack PUT for burst of 10 files, got %d", recBackend.Puts()-initialPuts)
	}

	// Verify all 10 files are visible to a second client
	for i := 0; i < burstCount; i++ {
		rResp, err := server.ReadFile(ctx, &pb.ReadFileRequest{
			VolumeId: volumeID,
			Inode:    burstInos[i],
			Offset:   0,
			Size:     1024,
		})
		if err != nil || rResp.GetError() != 0 {
			t.Fatalf("ReadFile %d failed: %v", i, err)
		}
		if !bytes.Equal(rResp.GetData(), burstPayload) {
			t.Fatalf("Content mismatch for file %d", i)
		}
	}

	// 2. A blocking fsync mid-window returns only after its data is durable and includes the window's other writes
	cResp1, _ := server.CreateFile(ctx, &pb.CreateFileRequest{VolumeId: volumeID, ParentInode: 1, Name: "mid1.bin", Mode: 0644})
	cResp2, _ := server.CreateFile(ctx, &pb.CreateFileRequest{VolumeId: volumeID, ParentInode: 1, Name: "mid2.bin", Mode: 0644})
	mid1Ino := cResp1.GetAttr().GetInode().GetIno()
	mid2Ino := cResp2.GetAttr().GetInode().GetIno()

	// Write file 1 and issue non-blocking sync (starts window)
	_, _ = server.WriteFile(ctx, &pb.WriteFileRequest{VolumeId: volumeID, Inode: mid1Ino, Offset: 0, Data: []byte("mid1-data")})
	_, _ = server.Fsync(ctx, &pb.FsyncRequest{VolumeId: volumeID, Inode: mid1Ino, Blocking: proto.Bool(false)})

	// Mid-window: write file 2 and issue blocking fsync!
	_, _ = server.WriteFile(ctx, &pb.WriteFileRequest{VolumeId: volumeID, Inode: mid2Ino, Offset: 0, Data: []byte("mid2-data")})
	putsBeforeBlocking := recBackend.Puts()
	blockingResp, err := server.Fsync(ctx, &pb.FsyncRequest{VolumeId: volumeID, Inode: mid2Ino, Blocking: proto.Bool(true)})
	if err != nil || !blockingResp.GetSuccess() {
		t.Fatalf("Blocking fsync failed: %v", err)
	}

	// Blocking fsync flushed immediately, producing exactly 1 new pack
	if recBackend.Puts()-putsBeforeBlocking != 1 {
		t.Fatalf("Expected 1 immediate pack PUT on blocking fsync mid-window, got %d", recBackend.Puts()-putsBeforeBlocking)
	}

	// Both mid1 and mid2 must be immediately visible and durable
	r1, _ := server.ReadFile(ctx, &pb.ReadFileRequest{VolumeId: volumeID, Inode: mid1Ino, Offset: 0, Size: 1024})
	r2, _ := server.ReadFile(ctx, &pb.ReadFileRequest{VolumeId: volumeID, Inode: mid2Ino, Offset: 0, Size: 1024})
	if string(r1.GetData()) != "mid1-data" || string(r2.GetData()) != "mid2-data" {
		t.Fatalf("Both writes should be durable and visible: got r1=%q, r2=%q", string(r1.GetData()), string(r2.GetData()))
	}

	// 3. A failed non-blocking sync is retried and surfaces on the next blocking fsync
	cRespFail, _ := server.CreateFile(ctx, &pb.CreateFileRequest{VolumeId: volumeID, ParentInode: 1, Name: "fail.bin", Mode: 0644})
	failIno := cRespFail.GetAttr().GetInode().GetIno()

	_, _ = server.WriteFile(ctx, &pb.WriteFileRequest{VolumeId: volumeID, Inode: failIno, Offset: 0, Data: []byte("fail-data")})

	// Inject backend failure
	recBackend.mu.Lock()
	recBackend.failPuts = true
	recBackend.mu.Unlock()

	// Trigger non-blocking sync
	_, _ = server.Fsync(ctx, &pb.FsyncRequest{VolumeId: volumeID, Inode: failIno, Blocking: proto.Bool(false)})

	// Wait for coalesce window to attempt sync and fail
	time.Sleep(coalesceWindow + 50*time.Millisecond)

	// Restore backend health
	recBackend.mu.Lock()
	recBackend.failPuts = false
	recBackend.mu.Unlock()

	// Next blocking fsync on the affected inode must surface the error!
	fsyncErrResp, err := server.Fsync(ctx, &pb.FsyncRequest{VolumeId: volumeID, Inode: failIno, Blocking: proto.Bool(true)})
	if err != nil {
		t.Fatalf("unexpected gRPC error: %v", err)
	}
	if fsyncErrResp.GetError() == 0 || fsyncErrResp.GetSuccess() {
		t.Fatalf("expected blocking fsync to surface retained non-blocking sync failure, got success")
	}

	// Retry blocking fsync: must now succeed with healthy backend!
	fsyncSuccessResp, err := server.Fsync(ctx, &pb.FsyncRequest{VolumeId: volumeID, Inode: failIno, Blocking: proto.Bool(true)})
	if err != nil || !fsyncSuccessResp.GetSuccess() {
		t.Fatalf("retry fsync failed: %v", err)
	}
	readFailDoc, _ := server.ReadFile(ctx, &pb.ReadFileRequest{VolumeId: volumeID, Inode: failIno, Offset: 0, Size: 1024})
	if string(readFailDoc.GetData()) != "fail-data" {
		t.Fatalf("Expected fail-data after successful retry, got %q", string(readFailDoc.GetData()))
	}
}

// TestWriteThenTruncateDown_And_TruncateThenWrite tests:
// Write-then-truncate-down and truncate-then-write must both compose correctly.
func TestWriteThenTruncateDown_And_TruncateThenWrite(t *testing.T) {
	ctx := t.Context()
	server := NewServer(nil)
	defer func() { _ = server.Close() }()

	volumeID := "vol-trunc-compose"

	// 1. Write-then-truncate-down
	cResp1, _ := server.CreateFile(ctx, &pb.CreateFileRequest{VolumeId: volumeID, ParentInode: 1, Name: "down.bin", Mode: 0644})
	ino1 := cResp1.GetAttr().GetInode().GetIno()

	// Write 128 KiB
	data128k := bytes.Repeat([]byte("X"), 128*1024)
	wResp1, err := server.WriteFile(ctx, &pb.WriteFileRequest{VolumeId: volumeID, Inode: ino1, Offset: 0, Data: data128k})
	if err != nil || wResp1.GetError() != 0 {
		t.Fatalf("WriteFile down failed: %v", err)
	}

	// Truncate down to 32 KiB
	tResp1, err := server.TruncateFile(ctx, &pb.TruncateFileRequest{VolumeId: volumeID, Inode: ino1, Size: 32 * 1024})
	if err != nil || tResp1.GetError() != 0 {
		t.Fatalf("TruncateFile down failed: %v", err)
	}
	if tResp1.GetAttr().GetInode().GetSize() != 32*1024 {
		t.Fatalf("expected truncated size 32768, got %d", tResp1.GetAttr().GetInode().GetSize())
	}

	// Sync
	fResp1, err := server.Fsync(ctx, &pb.FsyncRequest{VolumeId: volumeID, Inode: ino1, Blocking: proto.Bool(true)})
	if err != nil || !fResp1.GetSuccess() {
		t.Fatalf("Fsync down failed: %v", err)
	}

	// Read back: exactly 32 KiB of 'X'
	r1, err := server.ReadFile(ctx, &pb.ReadFileRequest{VolumeId: volumeID, Inode: ino1, Offset: 0, Size: 128 * 1024})
	if err != nil || r1.GetError() != 0 {
		t.Fatalf("ReadFile down failed: %v", err)
	}
	if r1.GetTotalSize() != 32*1024 || len(r1.GetData()) != 32*1024 || !bytes.Equal(r1.GetData(), data128k[:32*1024]) {
		t.Fatalf("ReadFile down data mismatch: total=%d, len=%d", r1.GetTotalSize(), len(r1.GetData()))
	}

	// 2. Truncate-then-write
	cResp2, _ := server.CreateFile(ctx, &pb.CreateFileRequest{VolumeId: volumeID, ParentInode: 1, Name: "up.bin", Mode: 0644})
	ino2 := cResp2.GetAttr().GetInode().GetIno()

	// Truncate to 100 KiB (sparse)
	tResp2, err := server.TruncateFile(ctx, &pb.TruncateFileRequest{VolumeId: volumeID, Inode: ino2, Size: 100 * 1024})
	if err != nil || tResp2.GetError() != 0 {
		t.Fatalf("TruncateFile up failed: %v", err)
	}

	// Write 20 KiB at offset 10 KiB
	data20k := bytes.Repeat([]byte("Y"), 20*1024)
	wResp2, err := server.WriteFile(ctx, &pb.WriteFileRequest{VolumeId: volumeID, Inode: ino2, Offset: 10 * 1024, Data: data20k})
	if err != nil || wResp2.GetError() != 0 {
		t.Fatalf("WriteFile up failed: %v", err)
	}

	// Sync
	fResp2, err := server.Fsync(ctx, &pb.FsyncRequest{VolumeId: volumeID, Inode: ino2, Blocking: proto.Bool(true)})
	if err != nil || !fResp2.GetSuccess() {
		t.Fatalf("Fsync up failed: %v", err)
	}

	// Read back: total size 100 KiB, bytes [10K..30K) are 'Y', others zeros
	r2, err := server.ReadFile(ctx, &pb.ReadFileRequest{VolumeId: volumeID, Inode: ino2, Offset: 0, Size: 100 * 1024})
	if err != nil || r2.GetError() != 0 {
		t.Fatalf("ReadFile up failed: %v", err)
	}
	if r2.GetTotalSize() != 100*1024 || len(r2.GetData()) != 100*1024 {
		t.Fatalf("ReadFile up total mismatch: %d", r2.GetTotalSize())
	}
	expectedUp := make([]byte, 100*1024)
	copy(expectedUp[10*1024:], data20k)
	if !bytes.Equal(r2.GetData(), expectedUp) {
		t.Fatalf("ReadFile up data mismatch")
	}
}

// TestStagingCapBackpressure tests:
// Controller staging cap: past it, back-pressure WriteFile; do not sync silently.
func TestStagingCapBackpressure(t *testing.T) {
	ctx := t.Context()
	server := NewServer(nil, WithServerStagingCap(128*1024)) // 128 KiB cap
	defer func() { _ = server.Close() }()

	volumeID := "vol-backpressure"

	cResp, _ := server.CreateFile(ctx, &pb.CreateFileRequest{VolumeId: volumeID, ParentInode: 1, Name: "capped.bin", Mode: 0644})
	ino := cResp.GetAttr().GetInode().GetIno()

	// Write 128 KiB: fills staging cap
	data128k := bytes.Repeat([]byte("B"), 128*1024)
	wResp1, err := server.WriteFile(ctx, &pb.WriteFileRequest{VolumeId: volumeID, Inode: ino, Offset: 0, Data: data128k})
	if err != nil || wResp1.GetError() != 0 {
		t.Fatalf("Write 1 failed: %v", err)
	}

	// Next write should block because staging cap is full.
	// We test with a short timeout context to verify backpressure blocks!
	shortCtx, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
	defer cancel()

	_, errBlocked := server.WriteFile(shortCtx, &pb.WriteFileRequest{
		VolumeId: volumeID,
		Inode:    ino,
		Offset:   128 * 1024,
		Data:     []byte("more"),
	})
	if errBlocked == nil || !errors.Is(errBlocked, context.DeadlineExceeded) && !strings.Contains(errBlocked.Error(), "context deadline exceeded") {
		t.Fatalf("Expected write past cap to block and time out, got err=%v", errBlocked)
	}

	// Verify that the server did NOT sync silently
	vol := server.GetVolume(volumeID)
	attr, _ := vol.GetAttr(ctx, ino)
	if attr.GetInode().GetSize() != 0 {
		t.Fatalf("Server should not sync silently past cap! row size=%d, want 0", attr.GetInode().GetSize())
	}

	// Fsync drains staging bytes and frees space
	fResp, err := server.Fsync(ctx, &pb.FsyncRequest{VolumeId: volumeID, Inode: ino, Blocking: proto.Bool(true)})
	if err != nil || !fResp.GetSuccess() {
		t.Fatalf("Fsync failed: %v", err)
	}

	// Now write can proceed immediately!
	wResp2, err := server.WriteFile(ctx, &pb.WriteFileRequest{
		VolumeId: volumeID,
		Inode:    ino,
		Offset:   128 * 1024,
		Data:     []byte("more"),
	})
	if err != nil || wResp2.GetError() != 0 {
		t.Fatalf("Write after fsync failed: %v", err)
	}
}
