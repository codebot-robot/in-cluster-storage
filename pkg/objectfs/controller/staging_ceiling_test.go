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
	"context"
	"sync"
	"syscall"
	"testing"
	"time"

	pb "github.com/gke-labs/in-cluster-storage/pkg/api/objectfs/v1alpha1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// setupStagedCeilingServer sets up a Server with Volume 1 staging writes at ceilingBytes,
// and arms a storage upload gate to park syncs.
// Requires no locks held.
func setupStagedCeilingServer(t *testing.T, ceilingBytes int64) (*Server, string, chan struct{}, func()) {
	t.Helper()
	ctx := t.Context()
	backend := newRecordingBackend(NewMemoryBackend())
	server := NewTestServer(t, backend, WithServerMaxLocalStagingBytes(ceilingBytes))

	volume1ID := "test-ceiling-vol1"
	createResponse1, err := testCreateFile(ctx, server, volume1ID, "/file1.bin", 0644, nil, 0, 0)
	if err != nil || createResponse1.GetError() != 0 {
		t.Fatalf("CreateFile volume 1 failed: %v", err)
	}
	inode1 := createResponse1.GetAttr().GetInode().GetIno()

	writeResponse1, err := server.WriteFile(ctx, &pb.WriteFileRequest{
		VolumeId:  volume1ID,
		Inode:     inode1,
		Offset:    0,
		Data:      make([]byte, ceilingBytes),
		WriteMode: pb.WriteMode_LAZY,
	})
	if err != nil || writeResponse1.GetError() != 0 {
		t.Fatalf("WriteFile volume 1 failed: %v", err)
	}

	gateEntered := make(chan struct{}, 1)
	gateRelease := make(chan struct{})
	backend.setGate(gateEntered, gateRelease)
	unblock := sync.OnceFunc(func() { close(gateRelease) })

	return server, volume1ID, gateEntered, unblock
}

// startAsyncWrite starts an asynchronous WriteFile call on volumeID and inodeID.
// Requires no locks held.
func startAsyncWrite(ctx context.Context, server *Server, volumeID string, inodeID uint64, data []byte) (<-chan *pb.WriteFileResponse, <-chan error) {
	responseChannel := make(chan *pb.WriteFileResponse, 1)
	errChannel := make(chan error, 1)
	go func() {
		response, writeErr := server.WriteFile(ctx, &pb.WriteFileRequest{
			VolumeId:  volumeID,
			Inode:     inodeID,
			Offset:    0,
			Data:      data,
			WriteMode: pb.WriteMode_LAZY,
		})
		responseChannel <- response
		errChannel <- writeErr
	}()
	return responseChannel, errChannel
}

// TestStagingCeiling_BlocksThenProceeds verifies that when the controller-wide staging ceiling
// is reached, a new write blocks, triggers a sync on the volume holding staged bytes, and proceeds
// once the sync completes and frees space.
// Requires no locks held.
func TestStagingCeiling_BlocksThenProceeds(t *testing.T) {
	ctx := t.Context()
	ceilingBytes := int64(64 * 1024)
	server, volume1ID, gateEntered, unblock := setupStagedCeilingServer(t, ceilingBytes)
	defer func() {
		if closeError := server.Close(); closeError != nil {
			t.Errorf("server.Close failed: %v", closeError)
		}
	}()
	defer unblock()

	if staged := server.Stats().LocalStagingBytes; staged != ceilingBytes {
		t.Fatalf("expected LocalStagingBytes = %d, got %d", ceilingBytes, staged)
	}

	volume2ID := "test-ceiling-vol2"
	createResponse2, err := testCreateFile(ctx, server, volume2ID, "/file2.bin", 0644, nil, 0, 0)
	if err != nil || createResponse2.GetError() != 0 {
		t.Fatalf("CreateFile volume 2 failed: %v", err)
	}
	inode2 := createResponse2.GetAttr().GetInode().GetIno()

	writeResponseChannel, writeErrChannel := startAsyncWrite(ctx, server, volume2ID, inode2, make([]byte, 10*1024))

	select {
	case <-gateEntered:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for backpressure to trigger sync upload on volume 1")
	}

	// Assert the write had not returned when upload was parked.
	select {
	case earlyResponse := <-writeResponseChannel:
		t.Fatalf("write returned early while upload was parked: %+v", earlyResponse)
	default:
	}

	unblock()

	select {
	case writeErr := <-writeErrChannel:
		if writeErr != nil {
			t.Fatalf("WriteFile volume 2 returned RPC error: %v", writeErr)
		}
		writeResponse := <-writeResponseChannel
		if writeResponse.GetError() != 0 {
			t.Fatalf("WriteFile volume 2 returned unexpected syscall error code: %d", writeResponse.GetError())
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for volume 2 write to unblock and proceed")
	}

	if staged := server.Stats().LocalStagingBytes; staged != 10*1024 {
		t.Fatalf("expected LocalStagingBytes = %d after sync and unblock, got %d", 10*1024, staged)
	}

	volume1, getError := server.getOrCreateVolume(volume1ID)
	if getError != nil {
		t.Fatalf("getOrCreateVolume failed: %v", getError)
	}
	volume1.stagingMu.Lock()
	volume1.waitForSyncIdleLocked()
	volume1.stagingMu.Unlock()
}

// TestStagingCeiling_TimeoutReturnsENOSPC verifies that when uploads are parked and cannot free space,
// a write exceeding the ceiling blocks until its deadline margin passes and returns ENOSPC.
// Requires no locks held.
func TestStagingCeiling_TimeoutReturnsENOSPC(t *testing.T) {
	ctx := t.Context()
	ceilingBytes := int64(64 * 1024)
	server, volume1ID, gateEntered, unblock := setupStagedCeilingServer(t, ceilingBytes)
	defer func() {
		if closeError := server.Close(); closeError != nil {
			t.Errorf("server.Close failed: %v", closeError)
		}
	}()
	defer unblock()

	volume2ID := "test-timeout-vol2"
	createResponse2, err := testCreateFile(ctx, server, volume2ID, "/file2.bin", 0644, nil, 0, 0)
	if err != nil || createResponse2.GetError() != 0 {
		t.Fatalf("CreateFile volume 2 failed: %v", err)
	}
	inode2 := createResponse2.GetAttr().GetInode().GetIno()

	timeout := 800 * time.Millisecond
	expectedWaitDuration := timeout - stagingWaitDeadlineMargin
	startTime := time.Now()

	writeContext, writeCancel := context.WithTimeout(ctx, timeout)
	defer writeCancel()

	writeResponseChannel, writeErrChannel := startAsyncWrite(writeContext, server, volume2ID, inode2, make([]byte, 10*1024))

	select {
	case <-gateEntered:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for parked sync upload gate")
	}

	// Assert the write had not returned when upload was parked.
	select {
	case earlyResponse := <-writeResponseChannel:
		t.Fatalf("write returned early while upload was parked: %+v", earlyResponse)
	default:
	}

	select {
	case writeErr := <-writeErrChannel:
		elapsed := time.Since(startTime)
		if writeErr != nil {
			t.Fatalf("expected syscall error response, got RPC error: %v", writeErr)
		}
		writeResponse := <-writeResponseChannel
		if writeResponse.GetError() != int32(syscall.ENOSPC) {
			t.Fatalf("expected error ENOSPC (%d), got %d", syscall.ENOSPC, writeResponse.GetError())
		}
		if elapsed < expectedWaitDuration {
			t.Fatalf("write returned too early: elapsed %v, expected at least %v", elapsed, expectedWaitDuration)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for write to return ENOSPC after deadline expired")
	}

	unblock()
	volume1, getError := server.getOrCreateVolume(volume1ID)
	if getError != nil {
		t.Fatalf("getOrCreateVolume failed: %v", getError)
	}
	volume1.stagingMu.Lock()
	volume1.waitForSyncIdleLocked()
	volume1.stagingMu.Unlock()
}

// TestStagingCeiling_NeverFitsRefusesImmediately verifies that a write exceeding the ceiling
// by itself is refused immediately with ENOSPC without waiting.
// Requires no locks held.
func TestStagingCeiling_NeverFitsRefusesImmediately(t *testing.T) {
	ctx := t.Context()
	backend := newRecordingBackend(NewMemoryBackend())
	ceilingBytes := int64(64 * 1024)
	server := NewTestServer(t, backend, WithServerMaxLocalStagingBytes(ceilingBytes))
	defer func() {
		if closeError := server.Close(); closeError != nil {
			t.Errorf("server.Close failed: %v", closeError)
		}
	}()

	volumeID := "test-never-fits-vol"
	createResponse, err := testCreateFile(ctx, server, volumeID, "/large.bin", 0644, nil, 0, 0)
	if err != nil || createResponse.GetError() != 0 {
		t.Fatalf("CreateFile failed: %v", err)
	}
	inodeID := createResponse.GetAttr().GetInode().GetIno()

	startTime := time.Now()
	// Write 128 KiB (2 chunks = 128 KiB > 64 KiB ceiling) on an empty controller.
	writeResponse, err := server.WriteFile(ctx, &pb.WriteFileRequest{
		VolumeId:  volumeID,
		Inode:     inodeID,
		Offset:    0,
		Data:      make([]byte, 128*1024),
		WriteMode: pb.WriteMode_LAZY,
	})
	elapsed := time.Since(startTime)
	if err != nil {
		t.Fatalf("WriteFile failed: %v", err)
	}
	if writeResponse.GetError() != int32(syscall.ENOSPC) {
		t.Fatalf("expected error ENOSPC (%d), got %d", syscall.ENOSPC, writeResponse.GetError())
	}
	if elapsed > 200*time.Millisecond {
		t.Fatalf("expected immediate refusal, took %v", elapsed)
	}
}

// TestStagingCeiling_CancellationReturnsContextError verifies that when a waiting write's context
// is cancelled, it returns the context's error without logging.
// Requires no locks held.
func TestStagingCeiling_CancellationReturnsContextError(t *testing.T) {
	ctx := t.Context()
	ceilingBytes := int64(64 * 1024)
	server, volume1ID, gateEntered, unblock := setupStagedCeilingServer(t, ceilingBytes)
	defer func() {
		if closeError := server.Close(); closeError != nil {
			t.Errorf("server.Close failed: %v", closeError)
		}
	}()
	defer unblock()

	volume2ID := "test-cancel-vol2"
	createResponse2, err := testCreateFile(ctx, server, volume2ID, "/file2.bin", 0644, nil, 0, 0)
	if err != nil || createResponse2.GetError() != 0 {
		t.Fatalf("CreateFile volume 2 failed: %v", err)
	}
	inode2 := createResponse2.GetAttr().GetInode().GetIno()

	cancelContext, cancelFunc := context.WithCancel(ctx)

	_, writeErrChannel := startAsyncWrite(cancelContext, server, volume2ID, inode2, make([]byte, 10*1024))

	select {
	case <-gateEntered:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for parked sync upload gate")
	}

	cancelFunc()

	select {
	case writeErr := <-writeErrChannel:
		if writeErr == nil || status.Code(writeErr) != codes.Canceled {
			t.Fatalf("expected Canceled RPC error, got: %v", writeErr)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for cancelled write to return")
	}

	unblock()
	volume1, getError := server.getOrCreateVolume(volume1ID)
	if getError != nil {
		t.Fatalf("getOrCreateVolume failed: %v", getError)
	}
	volume1.stagingMu.Lock()
	volume1.waitForSyncIdleLocked()
	volume1.stagingMu.Unlock()
}

// TestStagingCeiling_Stats verifies that Server.Stats reports the configured ceiling alongside current total.
// Requires no locks held.
func TestStagingCeiling_Stats(t *testing.T) {
	ctx := t.Context()
	backend := newRecordingBackend(NewMemoryBackend())
	ceilingBytes := int64(16 * 1024 * 1024 * 1024)
	server := NewTestServer(t, backend, WithServerMaxLocalStagingBytes(ceilingBytes))
	defer func() {
		if closeError := server.Close(); closeError != nil {
			t.Errorf("server.Close failed: %v", closeError)
		}
	}()

	createResponse, err := testCreateFile(ctx, server, "test-stats-vol", "/file.bin", 0644, nil, 0, 0)
	if err != nil || createResponse.GetError() != 0 {
		t.Fatalf("CreateFile failed: %v", err)
	}

	stats := server.Stats()
	if stats.MaxLocalStagingBytes != ceilingBytes {
		t.Fatalf("expected MaxLocalStagingBytes = %d, got %d", ceilingBytes, stats.MaxLocalStagingBytes)
	}
	if stats.LocalStagingBytes != 0 {
		t.Fatalf("expected LocalStagingBytes = 0, got %d", stats.LocalStagingBytes)
	}
}

// setSyncTriggerHook configures a callback invoked when triggerSyncIfStaged initiates a coalesce sync (testing only).
// Requires no locks held; acquires stagingMu.
func (v *Volume) setSyncTriggerHook(hook func()) {
	v.stagingMu.Lock()
	defer v.stagingMu.Unlock()
	v.syncTriggerHook = hook
}

// TestStagingCeiling_SyncsLargestStagedVolumesFirst verifies that when the staging ceiling is reached,
// coalesce syncs are triggered in descending order of each volume's unuploaded staged bytes.
// Driven by signals with no sleeps, and fails if the order is reversed or left to map iteration.
// Requires no locks held.
func TestStagingCeiling_SyncsLargestStagedVolumesFirst(t *testing.T) {
	ctx := t.Context()
	backend := NewMemoryBackend()
	ceilingBytes := int64(384 * 1024)
	server := NewTestServer(t, backend, WithServerMaxLocalStagingBytes(ceilingBytes))
	defer func() {
		if closeError := server.Close(); closeError != nil {
			t.Errorf("server.Close failed: %v", closeError)
		}
	}()

	// Set up three volumes with different staged byte counts.
	// Names are chosen so alphabetical order differs from staged byte order.
	volumeLowID := "test-vol-a-low"
	volumeHighID := "test-vol-b-high"
	volumeMidID := "test-vol-c-mid"

	orderSignal := make(chan string)

	setupVolume := func(volumeID string, dataBytes int64) {
		t.Helper()
		createResponse, err := testCreateFile(ctx, server, volumeID, "/file.bin", 0644, nil, 0, 0)
		if err != nil || createResponse.GetError() != 0 {
			t.Fatalf("CreateFile %s failed: %v", volumeID, err)
		}
		inode := createResponse.GetAttr().GetInode().GetIno()
		writeResponse, err := server.WriteFile(ctx, &pb.WriteFileRequest{
			VolumeId:  volumeID,
			Inode:     inode,
			Offset:    0,
			Data:      make([]byte, dataBytes),
			WriteMode: pb.WriteMode_LAZY,
		})
		if err != nil || writeResponse.GetError() != 0 {
			t.Fatalf("WriteFile %s failed: %v", volumeID, err)
		}

		volume, getErr := server.getOrCreateVolume(volumeID)
		if getErr != nil {
			t.Fatalf("getOrCreateVolume %s failed: %v", volumeID, getErr)
		}
		volume.setSyncTriggerHook(func() {
			orderSignal <- volumeID
		})
	}

	setupVolume(volumeLowID, 64*1024)
	setupVolume(volumeHighID, 192*1024)
	setupVolume(volumeMidID, 128*1024)

	// Volume for the waiting write that will hit the ceiling.
	waiterVolumeID := "test-vol-d-waiter"
	createResponse, err := testCreateFile(ctx, server, waiterVolumeID, "/waiter.bin", 0644, nil, 0, 0)
	if err != nil || createResponse.GetError() != 0 {
		t.Fatalf("CreateFile waiter failed: %v", err)
	}
	waiterInode := createResponse.GetAttr().GetInode().GetIno()

	writeResponseChannel, writeErrChannel := startAsyncWrite(ctx, server, waiterVolumeID, waiterInode, make([]byte, 10*1024))

	expectedOrder := []string{volumeHighID, volumeMidID, volumeLowID}
	for i, expectedVolume := range expectedOrder {
		select {
		case actualVolume := <-orderSignal:
			if actualVolume != expectedVolume {
				t.Fatalf("sync %d started on %s, want %s (expected order: %v)", i, actualVolume, expectedVolume, expectedOrder)
			}
		case <-time.After(5 * time.Second):
			t.Fatalf("timed out waiting for sync %d on volume %s to start", i, expectedVolume)
		}
	}

	select {
	case writeErr := <-writeErrChannel:
		if writeErr != nil {
			t.Fatalf("WriteFile waiter returned RPC error: %v", writeErr)
		}
		writeResponse := <-writeResponseChannel
		if writeResponse.GetError() != 0 {
			t.Fatalf("WriteFile waiter returned unexpected syscall error code: %d", writeResponse.GetError())
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for waiter write to complete")
	}
}
