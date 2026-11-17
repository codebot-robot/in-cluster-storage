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
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	pb "github.com/gke-labs/in-cluster-storage/pkg/api/objectfs/v1alpha1"
	"github.com/gke-labs/in-cluster-storage/pkg/objectfs/blob"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// createTestInode creates a file and returns its inode number.
// Requires no locks held.
func createTestInode(ctx context.Context, t *testing.T, server *Server, volumeID, filePath string) uint64 {
	t.Helper()
	createResponse, err := testCreateFile(ctx, server, volumeID, filePath, 0644, nil, 0, 0)
	if err != nil || createResponse.GetError() != 0 {
		t.Fatalf("CreateFile %s %s failed: %v", volumeID, filePath, err)
	}
	return createResponse.GetAttr().GetInode().GetIno()
}

// awaitWriteResult waits for an asynchronous WriteFile RPC to complete and returns its response.
// Requires no locks held.
func awaitWriteResult(t *testing.T, responseChannel <-chan *pb.WriteFileResponse, errorChannel <-chan error) *pb.WriteFileResponse {
	t.Helper()
	select {
	case err := <-errorChannel:
		if err != nil {
			t.Fatalf("WriteFile returned RPC error: %v", err)
		}
		response := <-responseChannel
		if response.GetError() != 0 {
			t.Fatalf("WriteFile returned unexpected syscall error code: %d", response.GetError())
		}
		return response
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for WriteFile result")
		return nil
	}
}

// awaitGateEntered waits for a background sync upload to enter the storage gate.
// Requires no locks held.
func awaitGateEntered(t *testing.T, gateEntered <-chan struct{}) {
	t.Helper()
	select {
	case <-gateEntered:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for storage upload gate")
	}
}

// syncSignalHook returns a hook that optionally increments a counter and signals a channel.
func syncSignalHook(counter *atomic.Int32, signalChannel chan struct{}) func() {
	return func() {
		if counter != nil {
			counter.Add(1)
		}
		if signalChannel != nil {
			select {
			case signalChannel <- struct{}{}:
			default:
			}
		}
	}
}

// setupStagedCeilingServer sets up a Server with Volume 1 staging writes at ceilingBytes,
// and arms a storage upload gate to park syncs.
// Requires no locks held.
func setupStagedCeilingServer(t *testing.T, ceilingBytes int64) (*Server, string, chan struct{}, func()) {
	t.Helper()
	ctx := t.Context()
	backend := newRecordingBackend(NewMemoryBackend())
	server := NewTestServer(t, backend, WithServerMaxLocalStagingBytes(ceilingBytes))

	volume1ID := "test-ceiling-vol1"
	inode1 := createTestInode(ctx, t, server, volume1ID, "/file1.bin")

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
	errorChannel := make(chan error, 1)
	go func() {
		response, writeErr := server.WriteFile(ctx, &pb.WriteFileRequest{
			VolumeId:  volumeID,
			Inode:     inodeID,
			Offset:    0,
			Data:      data,
			WriteMode: pb.WriteMode_LAZY,
		})
		responseChannel <- response
		errorChannel <- writeErr
	}()
	return responseChannel, errorChannel
}

// setupTestVolume creates a file, registers an optional sync hook, and writes dataBytes into it.
// Requires no locks held.
func setupTestVolume(ctx context.Context, t *testing.T, server *Server, volumeID string, dataBytes int64, hook func()) *Volume {
	t.Helper()
	inode := createTestInode(ctx, t, server, volumeID, "/file.bin")
	volume, getError := server.getOrCreateVolume(volumeID)
	if getError != nil {
		t.Fatalf("getOrCreateVolume %s failed: %v", volumeID, getError)
	}
	if hook != nil {
		volume.setSyncTriggerHook(hook)
	}
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
	return volume
}

// drainPassCount returns the number of evaluation passes completed by the drain loop (testing only).
func (s *Server) drainPassCount() int64 {
	return s.drain.passCount.Load()
}

// waitForDrainPass waits until the drain loop has completed at least targetPass passes.
// Requires no locks held.
func waitForDrainPass(t *testing.T, server *Server, targetPass int64) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for server.drainPassCount() < targetPass {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for drain pass %d (current: %d)", targetPass, server.drainPassCount())
		}
		time.Sleep(time.Millisecond)
	}
}

// wakeAndAwaitPass wakes the drain loop and waits until the subsequent evaluation pass completes.
// Requires no locks held.
func wakeAndAwaitPass(t *testing.T, server *Server) {
	t.Helper()
	passBefore := server.drainPassCount()
	server.drain.wakeDrain()
	waitForDrainPass(t, server, passBefore+1)
}

// waitStagedAtOrBelow waits until server disk limits staged bytes drops to or below targetBytes.
// Requires no locks held.
func waitStagedAtOrBelow(t *testing.T, server *Server, targetBytes int64) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		if server.Stats().LocalStagingBytes <= targetBytes {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for LocalStagingBytes <= %d; current = %d", targetBytes, server.Stats().LocalStagingBytes)
		}
		time.Sleep(time.Millisecond)
	}
}

// TestStagingCeiling_BlocksThenProceeds verifies that when the controller-wide staging ceiling
// is reached, a new write blocks, triggers a sync on the volume holding staged bytes, and proceeds
// once the sync completes and frees space.
// Requires no locks held.
func TestStagingCeiling_BlocksThenProceeds(t *testing.T) {
	ctx := t.Context()
	ceilingBytes := int64(64 * 1024)
	server, volume1ID, gateEntered, unblock := setupStagedCeilingServer(t, ceilingBytes)
	defer unblock()

	if staged := server.Stats().LocalStagingBytes; staged != ceilingBytes {
		t.Fatalf("expected LocalStagingBytes = %d, got %d", ceilingBytes, staged)
	}

	volume2ID := "test-ceiling-vol2"
	inode2 := createTestInode(ctx, t, server, volume2ID, "/file2.bin")

	writeResponseChannel, errorChannel := startAsyncWrite(ctx, server, volume2ID, inode2, make([]byte, 10*1024))
	awaitGateEntered(t, gateEntered)

	// Assert the write had not returned when upload was parked.
	select {
	case earlyResponse := <-writeResponseChannel:
		t.Fatalf("write returned early while upload was parked: %+v", earlyResponse)
	default:
	}

	unblock()
	awaitWriteResult(t, writeResponseChannel, errorChannel)

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
	defer unblock()

	volume2ID := "test-timeout-vol2"
	inode2 := createTestInode(ctx, t, server, volume2ID, "/file2.bin")

	timeout := 800 * time.Millisecond
	expectedWaitDuration := timeout - stagingWaitDeadlineMargin
	startTime := time.Now()

	writeContext, writeCancel := context.WithTimeout(ctx, timeout)
	defer writeCancel()

	writeResponseChannel, errorChannel := startAsyncWrite(writeContext, server, volume2ID, inode2, make([]byte, 10*1024))
	awaitGateEntered(t, gateEntered)

	// Assert the write had not returned when upload was parked.
	select {
	case earlyResponse := <-writeResponseChannel:
		t.Fatalf("write returned early while upload was parked: %+v", earlyResponse)
	default:
	}

	select {
	case writeErr := <-errorChannel:
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

	volumeID := "test-never-fits-vol"
	inodeID := createTestInode(ctx, t, server, volumeID, "/large.bin")

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
	defer unblock()

	volume2ID := "test-cancel-vol2"
	inode2 := createTestInode(ctx, t, server, volume2ID, "/file2.bin")

	cancelContext, cancelFunc := context.WithCancel(ctx)

	_, errorChannel := startAsyncWrite(cancelContext, server, volume2ID, inode2, make([]byte, 10*1024))
	awaitGateEntered(t, gateEntered)

	cancelFunc()

	select {
	case writeErr := <-errorChannel:
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

	_ = createTestInode(ctx, t, server, "test-stats-vol", "/file.bin")

	stats := server.Stats()
	if stats.MaxLocalStagingBytes != ceilingBytes {
		t.Fatalf("expected MaxLocalStagingBytes = %d, got %d", ceilingBytes, stats.MaxLocalStagingBytes)
	}
	if stats.LocalStagingBytes != 0 {
		t.Fatalf("expected LocalStagingBytes = 0, got %d", stats.LocalStagingBytes)
	}
}

// setSyncTriggerHook configures a callback invoked when a coalesce sync is initiated (testing only).
// Requires no locks held; acquires stagingMu.
func (v *Volume) setSyncTriggerHook(hook func()) {
	v.stagingMu.Lock()
	defer v.stagingMu.Unlock()
	v.syncTriggerHook = hook
}

// TestStagingCeiling_SyncsLargestStagedVolumesFirst verifies that when the staging ceiling is reached,
// coalesce syncs are triggered in descending order of each volume's unuploaded staged bytes.
// Candidate volume names are chosen so alphabetical order differs from staged byte order.
// Requires no locks held.
func TestStagingCeiling_SyncsLargestStagedVolumesFirst(t *testing.T) {
	ctx := t.Context()
	backend := NewMemoryBackend()
	ceilingBytes := int64(384 * 1024)
	server := NewTestServer(t, backend, WithServerMaxLocalStagingBytes(ceilingBytes))

	volumeLowID := "test-vol-a-low"
	volumeHighID := "test-vol-b-high"
	volumeMidID := "test-vol-c-mid"

	orderSignal := make(chan string)

	setupVolume := func(volumeID string, dataBytes int64) {
		t.Helper()
		setupTestVolume(ctx, t, server, volumeID, dataBytes, func() {
			orderSignal <- volumeID
		})
	}

	setupVolume(volumeLowID, 64*1024)
	setupVolume(volumeHighID, 192*1024)
	setupVolume(volumeMidID, 128*1024)

	// Volume for the waiting write that will hit the ceiling.
	waiterVolumeID := "test-vol-d-waiter"
	waiterInode := createTestInode(ctx, t, server, waiterVolumeID, "/waiter.bin")

	writeResponseChannel, errorChannel := startAsyncWrite(ctx, server, waiterVolumeID, waiterInode, make([]byte, 10*1024))

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

	awaitWriteResult(t, writeResponseChannel, errorChannel)
}

// TestStagingCeiling_TriggerSyncIfStaged_ZeroLengthWriteSyncs verifies that a zero-length write
// on an empty file (which stages a zero-length chunk with stagedBytes == 0) is eligible for syncing
// and triggers a sync when triggerSyncIfStaged is called.
// Also verifies that when the drain declines to start a volume (stagedBytes <= 0), it leaves its
// close timer and closeInodes untouched.
// Requires no locks held.
func TestStagingCeiling_TriggerSyncIfStaged_ZeroLengthWriteSyncs(t *testing.T) {
	ctx := t.Context()
	server := NewTestServer(t, NewMemoryBackend())
	volumeID := "test-zero-len-vol"
	inode := createTestInode(ctx, t, server, volumeID, "/empty.bin")
	volume, err := server.getOrCreateVolume(volumeID)
	if err != nil {
		t.Fatalf("getOrCreateVolume failed: %v", err)
	}

	writeResponse, err := server.WriteFile(ctx, &pb.WriteFileRequest{
		VolumeId:  volumeID,
		Inode:     inode,
		Offset:    0,
		Data:      []byte{},
		WriteMode: pb.WriteMode_LAZY,
	})
	if err != nil || writeResponse.GetError() != 0 {
		t.Fatalf("WriteFile failed: %v", err)
	}

	// Arm closeTimer and populate closeInodes to verify triggerDrainSync does not touch them when declined.
	volume.stagingMu.Lock()
	volume.closeInodes = map[uint64]struct{}{inode: {}}
	volume.closeTimer = time.AfterFunc(10*time.Minute, volume.triggerCloseSync)
	volume.stagingMu.Unlock()

	// Drain declines to start because stagedBytes is 0.
	if drainedBytes := volume.triggerDrainSync(); drainedBytes != 0 {
		t.Fatalf("triggerDrainSync returned %d, want 0", drainedBytes)
	}

	// Assert that declining to start left closeTimer and closeInodes intact.
	volume.stagingMu.Lock()
	hasCloseTimer := volume.closeTimer != nil
	closeInodesCount := len(volume.closeInodes)
	volume.stagingMu.Unlock()
	if !hasCloseTimer || closeInodesCount != 1 {
		t.Fatalf("declined drain sync modified timers/closeInodes: hasTimer=%v, closeInodes=%d", hasCloseTimer, closeInodesCount)
	}

	syncTriggered := make(chan struct{}, 1)
	volume.setSyncTriggerHook(func() {
		select {
		case syncTriggered <- struct{}{}:
		default:
		}
	})

	volume.triggerSyncIfStaged()

	select {
	case <-syncTriggered:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for triggerSyncIfStaged on zero-length chunk")
	}

	volume.stagingMu.Lock()
	volume.waitForSyncIdleLocked()
	volume.stagingMu.Unlock()
}

// TestStagingDrain_VolumeGrowingAboveHighWaterTriggersDrain verifies that when total staged bytes
// are already above the high-water mark with no eligible candidates, an add that grows a volume
// to or above the 1 MiB minimum signals the drain loop, which drains it without manual wake.
// Requires no locks held.
func TestStagingDrain_VolumeGrowingAboveHighWaterTriggersDrain(t *testing.T) {
	ctx := t.Context()
	backend := NewMemoryBackend()
	ceilingBytes := int64(8 * 1024 * 1024)
	server := NewTestServer(t, backend, WithServerMaxLocalStagingBytes(ceilingBytes), WithServerSyncCoalesceWindow(10*time.Minute))

	// Ceiling: 8 MiB (8192 KiB). High-water (75%): 6144 KiB. Low-water (50%): 4096 KiB.
	// Step 1: 7 base volumes of 900 KiB = 6300 KiB (~76.9% of ceiling, > 6144 KiB).
	// None are eligible for background drain (< 1 MiB minimum).
	for i := 0; i < 7; i++ {
		setupTestVolume(ctx, t, server, fmt.Sprintf("test-grow-vol-%d", i), 900*1024, nil)
	}

	// Drain loop woke on crossing 75% and found no eligible candidates.
	waitForDrainPass(t, server, 1)

	// Step 2: Grow volume 0 by 600 KiB to 1500 KiB (>= 1 MiB minimum).
	// Total becomes 6300 + 600 = 6900 KiB (~84.2% of ceiling).
	// The add of 600 KiB leaves total >= 75% and signals the loop.
	inode0 := createTestInode(ctx, t, server, "test-grow-vol-0", "/file2.bin")
	writeResponse, err := server.WriteFile(ctx, &pb.WriteFileRequest{
		VolumeId:  "test-grow-vol-0",
		Inode:     inode0,
		Offset:    0,
		Data:      make([]byte, 600*1024),
		WriteMode: pb.WriteMode_LAZY,
	})
	if err != nil || writeResponse.GetError() != 0 {
		t.Fatalf("WriteFile grow failed: %v", err)
	}

	// The drain loop must automatically trigger volume 0, dropping staged bytes to 6900 - 1500 = 5400 KiB.
	waitStagedAtOrBelow(t, server, 5400*1024)

	growVolume, getError := server.getOrCreateVolume("test-grow-vol-0")
	if getError != nil {
		t.Fatalf("getOrCreateVolume failed: %v", getError)
	}
	if staged := growVolume.StagedBytes(); staged != 0 {
		t.Fatalf("expected test-grow-vol-0 staged bytes = 0, got %d", staged)
	}
}

// TestStagingDrain_HighAndLowWaterMarks verifies that crossing the high-water mark starts background draining
// with no writer waiting, drains largest volumes first down to the 50% low-water mark, and skips volumes under
// the 1 MiB minimum.
// Also verifies holding total staged bytes between 51% and 75% triggers no drain starts (catching a 51% mutant),
// and that draining stops at 50% rather than 60% (catching a 60% low-water mutant).
// Requires no locks held.
func TestStagingDrain_HighAndLowWaterMarks(t *testing.T) {
	ctx := t.Context()
	backend := NewMemoryBackend()
	ceilingBytes := int64(10 * 1024 * 1024)
	server := NewTestServer(t, backend, WithServerMaxLocalStagingBytes(ceilingBytes), WithServerSyncCoalesceWindow(10*time.Minute))

	// Ceiling: 10 MiB (10240 KiB).
	// High-water (75%): 7680 KiB.
	// Low-water (50%): 5120 KiB.
	// Minimum: 1 MiB (1024 KiB).
	//
	// Step 1: Hold staged total between 51% (5222 KiB) and 75% (7680 KiB) with a volume
	// holding >= 1 MiB (2000 KiB) to catch any mutant with a 51% high-water mark.
	for i := 0; i < 8; i++ {
		setupTestVolume(ctx, t, server, fmt.Sprintf("test-drain-vol-base-%d", i), 500*1024, nil)
	}
	var countVolume1, countVolume2, countVolume3 atomic.Int32
	setupTestVolume(ctx, t, server, "test-drain-vol-1", 2000*1024, syncSignalHook(&countVolume1, nil))

	// Total held = 8 * 500 KiB + 2000 KiB = 6000 KiB (58.6% of ceiling).
	wakeAndAwaitPass(t, server)
	if count := countVolume1.Load(); count != 0 {
		t.Fatalf("sync started while staged total was below high-water mark (75%%): count = %d", count)
	}

	// Step 2: Stage Candidate 2 (1500 KiB >= 1 MiB) and Candidate 3 (1500 KiB >= 1 MiB).
	// Total becomes 6000 + 1500 + 1500 = 9000 KiB > 7680 KiB (75% high-water mark).
	setupTestVolume(ctx, t, server, "test-drain-vol-2", 1500*1024, syncSignalHook(&countVolume2, nil))
	setupTestVolume(ctx, t, server, "test-drain-vol-3", 1500*1024, syncSignalHook(&countVolume3, nil))

	// Draining starts on Candidate 1 (2000 KiB): 9000 - 2000 = 7000 KiB.
	// Then Candidate 2 (1500 KiB): 7000 - 1500 = 5500 KiB.
	// 5500 KiB is strictly between 50% (5120 KiB) and 60% (6144 KiB).
	// A 60% low-water mutant would stop here without starting Candidate 3.
	// The 50% low-water mark requires starting Candidate 3, dropping total to 4000 KiB <= 5120 KiB.
	waitStagedAtOrBelow(t, server, 5120*1024)

	if count := countVolume1.Load(); count != 1 {
		t.Fatalf("Volume 1 started %d times, want 1", count)
	}
	if count := countVolume2.Load(); count != 1 {
		t.Fatalf("Volume 2 started %d times, want 1", count)
	}
	if count := countVolume3.Load(); count != 1 {
		t.Fatalf("Volume 3 started %d times, want 1 (proves drain stops at 50%% rather than 60%%)", count)
	}
}

// TestStagingDrain_InFlightBytesCoverGap verifies that when in-flight bytes from an active drain
// sync cover the low-water gap, subsequent candidate volumes are not started in the same pass.
// Requires no locks held.
func TestStagingDrain_InFlightBytesCoverGap(t *testing.T) {
	ctx := t.Context()
	backend := newRecordingBackend(NewMemoryBackend())
	ceilingBytes := int64(8 * 1024 * 1024)
	server := NewTestServer(t, backend, WithServerMaxLocalStagingBytes(ceilingBytes), WithServerSyncCoalesceWindow(10*time.Minute))

	gateEntered := make(chan struct{}, 1)
	gateRelease := make(chan struct{})
	backend.setGate(gateEntered, gateRelease)

	var countCandidate1, countCandidate2 atomic.Int32
	setupTestVolume(ctx, t, server, "test-gap-vol-base", 2000*1024, nil)
	setupTestVolume(ctx, t, server, "test-gap-vol-2", 1500*1024, syncSignalHook(&countCandidate2, nil))

	// Write Candidate 1 (3000 KiB): total = 2000 + 1500 + 3000 = 6500 KiB > 6144 KiB (75% high-water mark).
	// Candidate 1 is triggered and enters the upload gate.
	setupTestVolume(ctx, t, server, "test-gap-vol-1", 3000*1024, syncSignalHook(&countCandidate1, nil))
	awaitGateEntered(t, gateEntered)

	// While Candidate 1 is in gate (3000 KiB in flight):
	// Projected staged = 6500 - 3000 = 3500 KiB <= 4096 KiB (low-water mark).
	// Candidate 2 must NOT be started.
	wakeAndAwaitPass(t, server)
	if count := countCandidate2.Load(); count != 0 {
		t.Fatalf("Candidate 2 was started %d times, want 0 (Candidate 1 in-flight sync covers gap)", count)
	}

	close(gateRelease)
	waitStagedAtOrBelow(t, server, 4096*1024)

	if count := countCandidate2.Load(); count != 0 {
		t.Fatalf("Candidate 2 was started %d times, want 0", count)
	}
	if count := countCandidate1.Load(); count != 1 {
		t.Fatalf("Candidate 1 started %d times, want 1", count)
	}
}

// firstPutFailsBackend optionally gates the first PutObject and injects a failure.
type firstPutFailsBackend struct {
	*recordingBackend
	gateEntered chan struct{}
	gateRelease chan struct{}
	failedOnce  sync.Once
}

func (b *firstPutFailsBackend) PutObject(ctx context.Context, volumeID, key string, stream blob.ByteStream) (string, error) {
	var shouldFail bool
	b.failedOnce.Do(func() {
		shouldFail = true
	})
	if shouldFail {
		if b.gateEntered != nil {
			select {
			case b.gateEntered <- struct{}{}:
			default:
			}
		}
		if b.gateRelease != nil {
			select {
			case <-b.gateRelease:
			case <-ctx.Done():
				return "", ctx.Err()
			}
		}
		return "", errors.New("injected backend upload error")
	}
	return b.recordingBackend.PutObject(ctx, volumeID, key, stream)
}

// TestStagingDrain_FailedSyncAdvancesToNextCandidate verifies that when a drain sync fails,
// the failed volume is not retried during the same episode, draining advances to the next candidate,
// and after the episode ends, the draining flag and skip set are cleared so a failed volume is retried in a new episode.
// Also verifies that an error occurring outside draining does not persist into subsequent episodes.
// Uses distinct candidate sizes (3500 KiB, 2600 KiB, 512 KiB).
// Requires no locks held.
func TestStagingDrain_FailedSyncAdvancesToNextCandidate(t *testing.T) {
	ctx := t.Context()
	volumeHighID := "test-fail-vol-high"
	volumeMidID := "test-fail-vol-mid"

	backend := &firstPutFailsBackend{
		recordingBackend: newRecordingBackend(NewMemoryBackend()),
	}
	ceilingBytes := int64(8 * 1024 * 1024)
	server := NewTestServer(t, backend, WithServerMaxLocalStagingBytes(ceilingBytes), WithServerSyncCoalesceWindow(10*time.Minute))

	var highStartCount, midStartCount atomic.Int32
	midStarted := make(chan struct{}, 1)

	// Episode 1:
	// Ceiling: 8 MiB (8192 KiB). High-water (75%): 6144 KiB. Low-water (50%): 4096 KiB.
	// Base: 512 KiB
	// Volume Mid: 2600 KiB (succeeds)
	// Volume High: 3500 KiB (fails first upload attempt)
	// Total: 512 + 2600 + 3500 = 6612 KiB > 6144 KiB (high-water). Distinct sizes.
	setupTestVolume(ctx, t, server, "test-fail-vol-base", 512*1024, nil)
	setupTestVolume(ctx, t, server, volumeMidID, 2600*1024, syncSignalHook(&midStartCount, midStarted))
	volumeHigh := setupTestVolume(ctx, t, server, volumeHighID, 3500*1024, syncSignalHook(&highStartCount, nil))

	// Volume High fails backend upload. Drain must advance to Volume Mid.
	select {
	case <-midStarted:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for Volume Mid drain sync after Volume High failure")
	}

	// Volume Mid completes and frees 2600 KiB.
	// Staged bytes drop to 6612 - 2600 = 4012 KiB <= 4096 KiB (low-water mark).
	// Episode 1 ends, clearing draining and skippedThisEpisode.
	waitStagedAtOrBelow(t, server, 4096*1024)

	if starts := highStartCount.Load(); starts != 1 {
		t.Fatalf("Volume High started %d times in Episode 1, want exactly 1 (must not restart in same episode)", starts)
	}
	if starts := midStartCount.Load(); starts != 1 {
		t.Fatalf("Volume Mid started %d times in Episode 1, want exactly 1", starts)
	}
	if stagedHigh := volumeHigh.StagedBytes(); stagedHigh != 3500*1024 {
		t.Fatalf("Volume High staged bytes = %d, want %d (failed data remains staged)", stagedHigh, 3500*1024)
	}

	// Episode 2 verification:
	// Step A: Stage 1500 KiB on a new volume to raise staged bytes between low-water (4096 KiB)
	// and high-water (6144 KiB). Total: 4012 + 1500 = 5512 KiB.
	// Assert no drain sync starts (verifies draining flag was cleared at end of Episode 1).
	setupTestVolume(ctx, t, server, "test-fail-vol-between", 1500*1024, nil)
	wakeAndAwaitPass(t, server)

	if starts := highStartCount.Load(); starts != 1 {
		t.Fatalf("Volume High started %d times between watermarks after episode ended, want 1", starts)
	}

	// Step B: Stage another 1500 KiB to cross high-water mark: 5512 + 1500 = 7012 KiB > 6144 KiB.
	// Episode 2 begins. Because skippedThisEpisode was cleared when Episode 1 ended,
	// Volume High is eligible again and must be retried.
	setupTestVolume(ctx, t, server, "test-fail-vol-trigger", 1500*1024, nil)

	// In Episode 2, backend upload succeeds (firstPutFailsBackend only fails once).
	waitStagedAtOrBelow(t, server, 4096*1024)

	if starts := highStartCount.Load(); starts != 2 {
		t.Fatalf("Volume High started %d times across two episodes, want 2 (must retry in new episode)", starts)
	}
	if stagedHigh := volumeHigh.StagedBytes(); stagedHigh != 0 {
		t.Fatalf("Volume High staged bytes = %d after Episode 2, want 0", stagedHigh)
	}
}

// TestStagingDrain_ExternalSyncWakeContinuesDrain verifies that when an external sync finishes
// during a drain episode, finishRealSync wakes the drain loop to continue draining remaining candidates.
// Requires no locks held.
func TestStagingDrain_ExternalSyncWakeContinuesDrain(t *testing.T) {
	ctx := t.Context()
	gateEntered := make(chan struct{}, 1)
	gateRelease := make(chan struct{})
	backend := &firstPutFailsBackend{
		recordingBackend: newRecordingBackend(NewMemoryBackend()),
		gateEntered:      gateEntered,
		gateRelease:      gateRelease,
	}
	ceilingBytes := int64(12 * 1024 * 1024)
	server := NewTestServer(t, backend, WithServerMaxLocalStagingBytes(ceilingBytes), WithServerSyncCoalesceWindow(10*time.Minute))

	volume1ID := "test-ext-vol-1"
	volume2ID := "test-ext-vol-2"

	var volume2StartCount atomic.Int32
	volume2Started := make(chan struct{}, 1)

	// Ceiling: 12 MiB (12288 KiB). High-water (75%): 9216 KiB. Low-water (50%): 6144 KiB.
	// Step 1: Stage Volume 1 (3500 KiB) below 75% high-water mark (9216 KiB).
	volume1 := setupTestVolume(ctx, t, server, volume1ID, 3500*1024, nil)

	volume1SyncErr := make(chan error, 1)
	// Start Volume 1 external sync, parking it in the upload gate.
	go func() {
		volume1SyncErr <- volume1.runCoalesceSync()
	}()
	awaitGateEntered(t, gateEntered)

	// Step 2: While Volume 1 is in gate (3500 KiB in-flight):
	// Stage Volume 2 (3600 KiB) and Base (2500 KiB).
	// Total staged = 3500 + 3600 + 2500 = 9600 KiB > 9216 KiB (high-water).
	// Projected staged = 9600 - 3500 = 6100 KiB <= 6144 KiB (low-water mark).
	// Because in-flight sync covers the gap to low-water, Volume 2 must NOT be started.
	setupTestVolume(ctx, t, server, volume2ID, 3600*1024, syncSignalHook(&volume2StartCount, volume2Started))
	setupTestVolume(ctx, t, server, "test-ext-vol-base", 2500*1024, nil)

	wakeAndAwaitPass(t, server)
	if count := volume2StartCount.Load(); count != 0 {
		t.Fatal("Volume 2 started early while Volume 1 in-flight sync covered the low-water gap")
	}

	// Step 3: Release upload gate so Volume 1 external sync finishes with failure (firstPutFailsBackend).
	// Because Volume 1 failed, 0 bytes are freed and total staged remains 9600 KiB > 6144 KiB (low-water mark).
	// finishRealSync must wake the drain loop, which then selects Volume 2 (3600 KiB > 3500 KiB)
	// and triggers its drain sync with no further write.
	close(gateRelease)

	select {
	case err := <-volume1SyncErr:
		if err == nil || !strings.Contains(err.Error(), "injected backend upload error") {
			t.Fatalf("expected Volume 1 sync to fail with injected upload error, got: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for Volume 1 sync to complete with error")
	}

	select {
	case <-volume2Started:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for finishRealSync wake to trigger Volume 2 drain sync")
	}

	waitStagedAtOrBelow(t, server, 6144*1024)

	if count := volume2StartCount.Load(); count != 1 {
		t.Fatalf("Volume 2 started %d times, want 1", count)
	}
}

// TestStagingDrain_FailedSyncAfterEpisodeEndsDoesNotStaleSkip verifies that when a drain-started
// sync on the largest volume is gated, the episode ends, the total sits between the marks, the gated sync
// fails, and total then rises above the high-water mark, the failed volume is retried in the new episode.
// Requires no locks held.
func TestStagingDrain_FailedSyncAfterEpisodeEndsDoesNotStaleSkip(t *testing.T) {
	ctx := t.Context()
	volumeHighID := "test-stale-vol-high"
	volumeMidID := "test-stale-vol-mid"

	gateEntered := make(chan struct{}, 1)
	gateRelease := make(chan struct{})
	backend := &firstPutFailsBackend{
		recordingBackend: newRecordingBackend(NewMemoryBackend()),
		gateEntered:      gateEntered,
		gateRelease:      gateRelease,
	}

	ceilingBytes := int64(8 * 1024 * 1024)
	server := NewTestServer(t, backend, WithServerMaxLocalStagingBytes(ceilingBytes), WithServerSyncCoalesceWindow(10*time.Minute))

	// Ceiling: 8 MiB (8192 KiB). High-water (75%): 6144 KiB. Low-water (50%): 4096 KiB.
	// Step 1: Drain-started sync on the largest volume is gated:
	// Base: 512 KiB.
	// Volume Mid: 2600 KiB.
	// Volume High: 3500 KiB (gated, largest).
	// Total = 512 + 2600 + 3500 = 6612 KiB > 6144 KiB (high-water mark).
	var highStartCount atomic.Int32
	setupTestVolume(ctx, t, server, "test-stale-vol-base", 512*1024, nil)
	volumeMid := setupTestVolume(ctx, t, server, volumeMidID, 2600*1024, nil)
	volumeHigh := setupTestVolume(ctx, t, server, volumeHighID, 3500*1024, syncSignalHook(&highStartCount, nil))

	// Volume High is largest, starts drain sync, and enters the upload gate:
	awaitGateEntered(t, gateEntered)

	// Step 2: An external sync on Volume Mid runs and completes, ending Episode 1:
	// Volume Mid frees 2600 KiB: total drops to 6612 - 2600 = 4012 KiB <= 4096 KiB (low-water mark).
	// Episode 1 ends (draining becomes false).
	if syncErr := volumeMid.runCoalesceSync(); syncErr != nil {
		t.Fatalf("Volume Mid sync failed: %v", syncErr)
	}
	waitStagedAtOrBelow(t, server, 4096*1024)

	// Step 3: Total sits between the marks (4012 KiB), and the gated sync fails:
	// Release gate so Volume High fails with an error outside draining.
	close(gateRelease)
	wakeAndAwaitPass(t, server)

	// Step 4: Total rises above the high-water mark:
	// Stage 3000 KiB on a trigger volume: total becomes 4012 + 3000 = 7012 KiB > 6144 KiB.
	// Episode 2 begins. Volume High must be retried.
	setupTestVolume(ctx, t, server, "test-stale-vol-trigger", 3000*1024, nil)

	waitStagedAtOrBelow(t, server, 4096*1024)

	if starts := highStartCount.Load(); starts != 2 {
		t.Fatalf("Volume High started %d times across two episodes, want 2 (must not be stale-skipped)", starts)
	}
	if stagedHigh := volumeHigh.StagedBytes(); stagedHigh != 0 {
		t.Fatalf("Volume High staged bytes = %d after retry, want 0", stagedHigh)
	}
}

// TestStagingDrain_LoopStopsOnCloseAndShutdown verifies that Server.Close and Server.Shutdown
// stop the drainLoop goroutine and wait for it to exit, and that Start after Close returns an error.
// Requires no locks held.
func TestStagingDrain_LoopStopsOnCloseAndShutdown(t *testing.T) {
	t.Run("Close", func(t *testing.T) {
		backend := NewMemoryBackend()
		server := NewServer(backend, WithServerMaxLocalStagingBytes(1024*1024))
		ctx := t.Context()
		if err := server.Start(ctx); err != nil {
			t.Fatalf("server.Start failed: %v", err)
		}

		// Verify loop is running by waiting for a pass.
		wakeAndAwaitPass(t, server)

		if err := server.Close(); err != nil {
			t.Fatalf("server.Close failed: %v", err)
		}

		select {
		case <-server.drain.loopDone:
		case <-time.After(5 * time.Second):
			t.Fatal("timed out waiting for drain loop goroutine to exit on Close")
		}

		// Start after Close must return an error.
		if err := server.Start(ctx); err == nil {
			t.Fatal("expected server.Start after Close to return an error, got nil")
		}
	})

	t.Run("Shutdown", func(t *testing.T) {
		backend := NewMemoryBackend()
		server := NewServer(backend, WithServerMaxLocalStagingBytes(1024*1024))
		ctx := t.Context()
		if err := server.Start(ctx); err != nil {
			t.Fatalf("server.Start failed: %v", err)
		}

		// Verify loop is running by waiting for a pass.
		wakeAndAwaitPass(t, server)

		if err := server.Shutdown(ctx); err != nil {
			t.Fatalf("server.Shutdown failed: %v", err)
		}

		select {
		case <-server.drain.loopDone:
		case <-time.After(5 * time.Second):
			t.Fatal("timed out waiting for drain loop goroutine to exit on Shutdown")
		}
	})
}
