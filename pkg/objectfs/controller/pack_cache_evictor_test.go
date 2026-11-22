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
	"bytes"
	"encoding/binary"
	"sync"
	"testing"
	"time"
)

// setLocalPackCacheTargetBytesForTest sets packCacheTargetBytes under diskLimits.mu without exposing a production setter.
func setLocalPackCacheTargetBytesForTest(server *Server, targetBytes int64) {
	server.diskLimits.mu.Lock()
	server.diskLimits.packCacheTargetBytes = targetBytes
	server.diskLimits.mu.Unlock()
}

// TestPackCacheEviction_TwoVolumes_LeastRecentlyReadFirst verifies that across multiple volumes,
// the globally least recently read evictable pack is evicted first.
func TestPackCacheEviction_TwoVolumes_LeastRecentlyReadFirst(t *testing.T) {
	clock := &testClock{now: time.Date(2026, 11, 21, 12, 0, 0, 0, time.UTC)}
	server := NewTestServer(t, newRecordingBackend(NewMemoryBackend()), WithServerClock(clock))

	volume1, err := server.getOrCreateVolume("test-evict-two-vol-1")
	if err != nil {
		t.Fatalf("getOrCreateVolume volume 1 failed: %v", err)
	}
	volume2, err := server.getOrCreateVolume("test-evict-two-vol-2")
	if err != nil {
		t.Fatalf("getOrCreateVolume volume 2 failed: %v", err)
	}

	inodeID1 := createAndUploadPack(t, server, volume1, "/file1.bin", 0, 1)

	clock.Advance(10 * time.Minute)
	_ = createAndUploadPack(t, server, volume2, "/file2.bin", 1000000, 1)

	clock.Advance(10 * time.Minute)
	// Read file on volume 1 so pack 1 on volume 1 has a more recent read timestamp than pack 1 on volume 2.
	if _, _, _, err := volume1.ReadFile(t.Context(), inodeID1, 0, 16); err != nil {
		t.Fatalf("ReadFile volume 1 failed: %v", err)
	}

	volume1.stagingMu.Lock()
	size1 := volume1.packs[1].pack.ByteSize()
	volume1.stagingMu.Unlock()

	volume2.stagingMu.Lock()
	size2 := volume2.packs[1].pack.ByteSize()
	volume2.stagingMu.Unlock()

	totalBefore := server.Stats().LocalPackCacheBytes
	if totalBefore != size1+size2 {
		t.Fatalf("expected initial pack cache bytes %d, got %d", size1+size2, totalBefore)
	}

	// Set target to size1: volume 2 pack 1 (least recently read) must be evicted first, leaving volume 1 pack 1.
	setLocalPackCacheTargetBytesForTest(server, size1)

	server.runPackCacheEvictionPass(t.Context())

	volume1.stagingMu.Lock()
	_, pack1Vol1Exists := volume1.packs[1]
	volume1.stagingMu.Unlock()

	volume2.stagingMu.Lock()
	_, pack1Vol2Exists := volume2.packs[1]
	volume2.stagingMu.Unlock()

	if !pack1Vol1Exists {
		t.Fatal("expected volume 1 pack 1 (more recently read) to be retained")
	}
	if pack1Vol2Exists {
		t.Fatal("expected volume 2 pack 1 (least recently read across volumes) to be evicted")
	}
	if current := server.Stats().LocalPackCacheBytes; current != size1 {
		t.Fatalf("expected pack cache bytes after eviction = %d, got %d", size1, current)
	}
}

// TestPackCacheEviction_LowerPackIDBreaksTies verifies that when two evictable packs have identical
// lastRead timestamps, the pack with the lower pack ID is evicted first.
func TestPackCacheEviction_LowerPackIDBreaksTies(t *testing.T) {
	clock := &testClock{now: time.Date(2026, 11, 21, 12, 0, 0, 0, time.UTC)}
	server := NewTestServer(t, newRecordingBackend(NewMemoryBackend()), WithServerClock(clock))

	volume, err := server.getOrCreateVolume("test-evict-pack-id-tie")
	if err != nil {
		t.Fatalf("getOrCreateVolume failed: %v", err)
	}

	// Upload two packs at the same clock time with no reads. Both have identical upload/lastRead timestamps.
	_ = createAndUploadPack(t, server, volume, "/file1.bin", 0, 1)
	_ = createAndUploadPack(t, server, volume, "/file2.bin", 1000000, 2)

	volume.stagingMu.Lock()
	size2 := volume.packs[2].pack.ByteSize()
	volume.stagingMu.Unlock()

	// Set target to size2: one pack must be evicted. Pack 1 has lower ID (1 < 2), so pack 1 must be evicted first.
	setLocalPackCacheTargetBytesForTest(server, size2)

	server.runPackCacheEvictionPass(t.Context())

	volume.stagingMu.Lock()
	_, pack1Exists := volume.packs[1]
	_, pack2Exists := volume.packs[2]
	volume.stagingMu.Unlock()

	if pack1Exists {
		t.Fatal("expected pack 1 (lower pack ID) to be evicted first")
	}
	if !pack2Exists {
		t.Fatal("expected pack 2 to be retained")
	}
	if current := server.Stats().LocalPackCacheBytes; current != size2 {
		t.Fatalf("expected pack cache bytes = %d, got %d", size2, current)
	}
}

// TestPackCacheEviction_StopsAtTargetAndLeavesRest verifies that eviction stops as soon as the total
// is at or below the target, leaving remaining evictable packs untouched.
func TestPackCacheEviction_StopsAtTargetAndLeavesRest(t *testing.T) {
	clock := &testClock{now: time.Date(2026, 11, 21, 12, 0, 0, 0, time.UTC)}
	server := NewTestServer(t, newRecordingBackend(NewMemoryBackend()), WithServerClock(clock))

	volume, err := server.getOrCreateVolume("test-evict-stop-at-target")
	if err != nil {
		t.Fatalf("getOrCreateVolume failed: %v", err)
	}

	_ = createAndUploadPack(t, server, volume, "/file1.bin", 0, 1)
	clock.Advance(10 * time.Minute)
	_ = createAndUploadPack(t, server, volume, "/file2.bin", 1000000, 2)
	clock.Advance(10 * time.Minute)
	_ = createAndUploadPack(t, server, volume, "/file3.bin", 2000000, 3)

	volume.stagingMu.Lock()
	size2 := volume.packs[2].pack.ByteSize()
	size3 := volume.packs[3].pack.ByteSize()
	volume.stagingMu.Unlock()

	// Setting target to size2 + size3 requires evicting only pack 1 (least recently read).
	setLocalPackCacheTargetBytesForTest(server, size2+size3)

	server.runPackCacheEvictionPass(t.Context())

	volume.stagingMu.Lock()
	_, pack1Exists := volume.packs[1]
	_, pack2Exists := volume.packs[2]
	_, pack3Exists := volume.packs[3]
	volume.stagingMu.Unlock()

	if pack1Exists {
		t.Fatal("expected pack 1 to be evicted")
	}
	if !pack2Exists {
		t.Fatal("expected pack 2 to be retained")
	}
	if !pack3Exists {
		t.Fatal("expected pack 3 to be retained")
	}
	if current := server.Stats().LocalPackCacheBytes; current != size2+size3 {
		t.Fatalf("expected pack cache bytes = %d, got %d", size2+size3, current)
	}
}

// TestPackCacheEviction_LiveReferencesAndNotUploadedNeverEvicted verifies that packs with live references
// or packs that are not yet uploaded are never evicted even when cache is above target.
func TestPackCacheEviction_LiveReferencesAndNotUploadedNeverEvicted(t *testing.T) {
	clock := &testClock{now: time.Date(2026, 11, 21, 12, 0, 0, 0, time.UTC)}
	server := NewTestServer(t, newRecordingBackend(NewMemoryBackend()), WithServerClock(clock))

	volume, err := server.getOrCreateVolume("test-evict-live-ref")
	if err != nil {
		t.Fatalf("getOrCreateVolume failed: %v", err)
	}

	_ = createAndUploadPack(t, server, volume, "/file1.bin", 0, 1)

	volume.stagingMu.Lock()
	volume.packs[1].liveReferences = 1
	packSize := volume.packs[1].pack.ByteSize()
	volume.stagingMu.Unlock()

	// Target 1 byte: evictor wants to evict, but pack 1 has a live reference.
	setLocalPackCacheTargetBytesForTest(server, 1)

	server.runPackCacheEvictionPass(t.Context())

	volume.stagingMu.Lock()
	_, pack1Exists := volume.packs[1]
	volume.stagingMu.Unlock()

	if !pack1Exists {
		t.Fatal("expected pack with live references to never be evicted")
	}
	if current := server.Stats().LocalPackCacheBytes; current != packSize {
		t.Fatalf("expected pack cache bytes = %d, got %d", packSize, current)
	}

	// Release live reference and verify that unreferenced uploaded pack 1 can be evicted,
	// while a second pack in packUploading state is never evicted.
	volume.stagingMu.Lock()
	volume.packs[1].liveReferences = 0
	volume.stagingMu.Unlock()

	ctx := t.Context()
	createResponse, err := testCreateFile(ctx, server, volume.volumeID, "/file2.bin", 0644, nil, 0, 0)
	if err != nil || createResponse.GetError() != 0 {
		t.Fatalf("CreateFile failed: %v", err)
	}
	inodeID2 := createResponse.GetAttr().GetInode().GetIno()
	fillAndSealPackWithSeed(t, volume, ctx, inodeID2, 1000000)

	volume.stagingMu.Lock()
	volume.packs[2].state = packUploading
	volume.stagingMu.Unlock()

	server.runPackCacheEvictionPass(t.Context())

	volume.stagingMu.Lock()
	_, pack1AfterEvict := volume.packs[1]
	_, pack2AfterEvict := volume.packs[2]
	volume.stagingMu.Unlock()

	if pack1AfterEvict {
		t.Fatal("expected uploaded unreferenced pack 1 to be evicted")
	}
	if !pack2AfterEvict {
		t.Fatal("expected pack 2 in packUploading to never be evicted")
	}
}

// TestPackCacheEviction_TargetZeroEvictsNothing verifies that a target of 0 disables background eviction.
func TestPackCacheEviction_TargetZeroEvictsNothing(t *testing.T) {
	clock := &testClock{now: time.Date(2026, 11, 21, 12, 0, 0, 0, time.UTC)}
	server := NewTestServer(t, newRecordingBackend(NewMemoryBackend()), WithServerClock(clock))

	volume, err := server.getOrCreateVolume("test-evict-target-zero")
	if err != nil {
		t.Fatalf("getOrCreateVolume failed: %v", err)
	}

	_ = createAndUploadPack(t, server, volume, "/file1.bin", 0, 1)

	volume.stagingMu.Lock()
	packSize := volume.packs[1].pack.ByteSize()
	volume.stagingMu.Unlock()

	setLocalPackCacheTargetBytesForTest(server, 0)

	server.runPackCacheEvictionPass(t.Context())

	volume.stagingMu.Lock()
	_, pack1Exists := volume.packs[1]
	volume.stagingMu.Unlock()

	if !pack1Exists {
		t.Fatal("expected pack 1 to be retained when target is 0")
	}
	if current := server.Stats().LocalPackCacheBytes; current != packSize {
		t.Fatalf("expected pack cache bytes = %d, got %d", packSize, current)
	}
}

// TestPackCacheEviction_CloseAndShutdownDoNotDeadlockWithPendingWake verifies that Close and Shutdown
// return without deadlock even while another goroutine continuously keeps an evictor wake signal pending.
func TestPackCacheEviction_CloseAndShutdownDoNotDeadlockWithPendingWake(t *testing.T) {
	t.Run("Close", func(t *testing.T) {
		server := NewTestServer(t, NewMemoryBackend())
		volume, err := server.getOrCreateVolume("test-deadlock-close-vol")
		if err != nil {
			t.Fatalf("getOrCreateVolume failed: %v", err)
		}
		_ = createAndUploadPack(t, server, volume, "/file1.bin", 0, 1)
		_ = createAndUploadPack(t, server, volume, "/file2.bin", 1000000, 2)
		volume.stagingMu.Lock()
		volume.packs[2].liveReferences = 1
		volume.stagingMu.Unlock()
		setLocalPackCacheTargetBytesForTest(server, 1)

		closeDone := make(chan error, 1)
		stopWake := make(chan struct{})
		var wakeWaitGroup sync.WaitGroup
		wakeWaitGroup.Add(1)
		go func() {
			defer wakeWaitGroup.Done()
			for {
				select {
				case <-stopWake:
					return
				default:
					server.evictor.wakeEvictor()
				}
			}
		}()

		go func() {
			closeDone <- server.Close()
		}()

		select {
		case err := <-closeDone:
			if err != nil {
				t.Fatalf("server.Close failed: %v", err)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("server.Close deadlocked while evictor wake pending")
		}

		close(stopWake)
		wakeWaitGroup.Wait()
	})

	t.Run("Shutdown", func(t *testing.T) {
		server := NewTestServer(t, NewMemoryBackend())
		volume, err := server.getOrCreateVolume("test-deadlock-shutdown-vol")
		if err != nil {
			t.Fatalf("getOrCreateVolume failed: %v", err)
		}
		_ = createAndUploadPack(t, server, volume, "/file1.bin", 0, 1)
		_ = createAndUploadPack(t, server, volume, "/file2.bin", 1000000, 2)
		volume.stagingMu.Lock()
		volume.packs[2].liveReferences = 1
		volume.stagingMu.Unlock()
		setLocalPackCacheTargetBytesForTest(server, 1)

		stopWake := make(chan struct{})
		var wakeWaitGroup sync.WaitGroup
		wakeWaitGroup.Add(1)
		go func() {
			defer wakeWaitGroup.Done()
			for {
				select {
				case <-stopWake:
					return
				default:
					server.evictor.wakeEvictor()
				}
			}
		}()

		shutdownDone := make(chan error, 1)
		go func() {
			shutdownDone <- server.Shutdown(t.Context())
		}()

		select {
		case err := <-shutdownDone:
			if err != nil {
				t.Fatalf("server.Shutdown failed: %v", err)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("server.Shutdown deadlocked while evictor wake pending")
		}

		close(stopWake)
		wakeWaitGroup.Wait()
	})
}

// TestPackCacheEviction_RealWriteFsync_EndsAtOrUnderTarget verifies that in the normal path
// with a target set, a real write and fsync wakes the evictor when the pack's live references
// drop to zero, bringing the total pack cache bytes at or under the target.
func TestPackCacheEviction_RealWriteFsync_EndsAtOrUnderTarget(t *testing.T) {
	server := NewTestServer(t, newRecordingBackend(NewMemoryBackend()), WithServerLocalPackCacheTargetBytes(1))

	volume, err := server.getOrCreateVolume("test-evict-real-write-vol")
	if err != nil {
		t.Fatalf("getOrCreateVolume failed: %v", err)
	}

	ctx := t.Context()
	createResponse, err := testCreateFile(ctx, server, volume.volumeID, "/real.bin", 0644, nil, 0, 0)
	if err != nil || createResponse.GetError() != 0 {
		t.Fatalf("CreateFile failed: %v", err)
	}
	inodeID := createResponse.GetAttr().GetInode().GetIno()

	fillAndSealPack(t, volume, ctx, inodeID)
	if err := volume.Fsync(ctx, inodeID); err != nil {
		t.Fatalf("Fsync failed: %v", err)
	}

	deadline := time.Now().Add(5 * time.Second)
	for {
		if server.Stats().LocalPackCacheBytes <= 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for pack cache bytes <= 1, got %d", server.Stats().LocalPackCacheBytes)
		}
		time.Sleep(time.Millisecond)
	}

	if current := server.Stats().LocalPackCacheBytes; current > 1 {
		t.Fatalf("expected pack cache bytes <= 1, got %d", current)
	}
}

// evictorPassCount returns the number of evaluation passes completed by the evictor loop (testing only).
func (s *Server) evictorPassCount() int64 {
	return s.evictor.passesCompleted.Value()
}

// waitForEvictorPass waits until the evictor loop has completed at least targetPass passes.
// Requires no locks held.
func waitForEvictorPass(t *testing.T, server *Server, targetPass int64) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for server.evictorPassCount() < targetPass {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for evictor pass %d (current: %d)", targetPass, server.evictorPassCount())
		}
		time.Sleep(time.Millisecond)
	}
}

// waitForLocalPackCacheTarget waits until the local pack cache bytes fall at or below targetBytes.
// Requires no locks held.
func waitForLocalPackCacheTarget(t *testing.T, server *Server, targetBytes int64) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for server.Stats().LocalPackCacheBytes > targetBytes {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for pack cache bytes <= %d, current: %d", targetBytes, server.Stats().LocalPackCacheBytes)
		}
		time.Sleep(time.Millisecond)
	}
}

// TestPackCacheEviction_TwoVolumes_RealWriteFsync verifies that in the normal path
// with a small target set via WithServerLocalPackCacheTargetBytes, files written and fsynced
// on two volumes until the total has exceeded the target trigger background eviction,
// bringing the total at or under target, and that evicted data remains readable from object storage.
func TestPackCacheEviction_TwoVolumes_RealWriteFsync(t *testing.T) {
	// Each pack with DefaultPackSlots is ~82 KiB. Setting a target of 180 KiB (~2.2 packs' worth).
	targetBytes := int64(180 * 1024)
	server := NewTestServer(t, NewMemoryBackend(), WithServerLocalPackCacheTargetBytes(targetBytes))

	volume1, err := server.getOrCreateVolume("test-evict-real-two-vol-1")
	if err != nil {
		t.Fatalf("getOrCreateVolume volume 1 failed: %v", err)
	}
	volume2, err := server.getOrCreateVolume("test-evict-real-two-vol-2")
	if err != nil {
		t.Fatalf("getOrCreateVolume volume 2 failed: %v", err)
	}

	ctx := t.Context()

	// Write and fsync 4 packs (2 on volume 1, 2 on volume 2).
	// Total unevicted uploaded size will be ~328 KiB, exceeding the 180 KiB target.
	createResponse1, err := testCreateFile(ctx, server, volume1.volumeID, "/file1.bin", 0644, nil, 0, 0)
	if err != nil || createResponse1.GetError() != 0 {
		t.Fatalf("CreateFile volume 1 file 1 failed: %v", err)
	}
	inodeID1 := createResponse1.GetAttr().GetInode().GetIno()
	fillAndSealPackWithSeed(t, volume1, ctx, inodeID1, 0)
	if err := volume1.Fsync(ctx, inodeID1); err != nil {
		t.Fatalf("Fsync volume 1 file 1 failed: %v", err)
	}

	createResponse2, err := testCreateFile(ctx, server, volume2.volumeID, "/file2.bin", 0644, nil, 0, 0)
	if err != nil || createResponse2.GetError() != 0 {
		t.Fatalf("CreateFile volume 2 file 2 failed: %v", err)
	}
	inodeID2 := createResponse2.GetAttr().GetInode().GetIno()
	fillAndSealPackWithSeed(t, volume2, ctx, inodeID2, 1000000)
	if err := volume2.Fsync(ctx, inodeID2); err != nil {
		t.Fatalf("Fsync volume 2 file 2 failed: %v", err)
	}

	createResponse3, err := testCreateFile(ctx, server, volume1.volumeID, "/file3.bin", 0644, nil, 0, 0)
	if err != nil || createResponse3.GetError() != 0 {
		t.Fatalf("CreateFile volume 1 file 3 failed: %v", err)
	}
	inodeID3 := createResponse3.GetAttr().GetInode().GetIno()
	fillAndSealPackWithSeed(t, volume1, ctx, inodeID3, 2000000)
	if err := volume1.Fsync(ctx, inodeID3); err != nil {
		t.Fatalf("Fsync volume 1 file 3 failed: %v", err)
	}

	createResponse4, err := testCreateFile(ctx, server, volume2.volumeID, "/file4.bin", 0644, nil, 0, 0)
	if err != nil || createResponse4.GetError() != 0 {
		t.Fatalf("CreateFile volume 2 file 4 failed: %v", err)
	}
	inodeID4 := createResponse4.GetAttr().GetInode().GetIno()
	fillAndSealPackWithSeed(t, volume2, ctx, inodeID4, 3000000)
	if err := volume2.Fsync(ctx, inodeID4); err != nil {
		t.Fatalf("Fsync volume 2 file 4 failed: %v", err)
	}

	// Wait on the eviction counter and ensure total pack cache bytes end at or under target.
	waitForEvictorPass(t, server, 1)
	waitForLocalPackCacheTarget(t, server, targetBytes)

	if current := server.Stats().LocalPackCacheBytes; current > targetBytes {
		t.Fatalf("expected pack cache bytes <= %d, got %d", targetBytes, current)
	}

	// Verify that at least one pack was evicted from local disk.
	var remainingPacks int
	volume1.stagingMu.Lock()
	remainingPacks += len(volume1.packs)
	volume1.stagingMu.Unlock()
	volume2.stagingMu.Lock()
	remainingPacks += len(volume2.packs)
	volume2.stagingMu.Unlock()

	if remainingPacks >= 4 {
		t.Fatalf("expected fewer than 4 packs remaining after eviction, got %d", remainingPacks)
	}

	// Verify that data from all files (including evicted packs fetched from object storage) is readable and intact.
	testFiles := []struct {
		volume  *Volume
		inodeID uint64
		seed    uint64
	}{
		{volume: volume1, inodeID: inodeID1, seed: 0},
		{volume: volume2, inodeID: inodeID2, seed: 1000000},
		{volume: volume1, inodeID: inodeID3, seed: 2000000},
		{volume: volume2, inodeID: inodeID4, seed: 3000000},
	}

	for _, testFile := range testFiles {
		data, _, _, err := testFile.volume.ReadFile(ctx, testFile.inodeID, 0, 16)
		if err != nil {
			t.Fatalf("ReadFile volume %s inode %d failed: %v", testFile.volume.volumeID, testFile.inodeID, err)
		}
		expected := make([]byte, 16)
		binary.BigEndian.PutUint64(expected[0:8], testFile.seed)
		binary.BigEndian.PutUint64(expected[8:16], testFile.seed+1)
		if !bytes.Equal(data, expected) {
			t.Fatalf("ReadFile volume %s inode %d data mismatch: got %v, want %v", testFile.volume.volumeID, testFile.inodeID, data, expected)
		}
	}
}

// TestPackCacheEviction_RecheckBeforeDelete_PackGainsLiveReferenceOrDeleted verifies that if a pack
// is evictable at scan time but gains a live reference or is deleted before its turn,
// it is not evicted and the total pack cache bytes are not changed for it.
func TestPackCacheEviction_RecheckBeforeDelete_PackGainsLiveReferenceOrDeleted(t *testing.T) {
	t.Run("GainsLiveReference", func(t *testing.T) {
		clock := &testClock{now: time.Date(2026, 11, 21, 12, 0, 0, 0, time.UTC)}
		server := NewTestServer(t, newRecordingBackend(NewMemoryBackend()), WithServerClock(clock))

		volume, err := server.getOrCreateVolume("test-evict-recheck-ref")
		if err != nil {
			t.Fatalf("getOrCreateVolume failed: %v", err)
		}

		_ = createAndUploadPack(t, server, volume, "/file.bin", 0, 1)

		volume.stagingMu.Lock()
		packSize := volume.packs[1].pack.ByteSize()
		volume.stagingMu.Unlock()

		setLocalPackCacheTargetBytesForTest(server, 1)

		candidate := evictablePackCandidate{
			volume: volume,
			packID: 1,
		}

		// Give the pack a live reference before its turn to be deleted.
		volume.stagingMu.Lock()
		volume.packs[1].liveReferences = 1
		volume.stagingMu.Unlock()

		evicted, _ := server.evictCandidatePack(candidate)
		if evicted {
			t.Fatal("expected pack that gained live reference before deletion not to be evicted")
		}

		volume.stagingMu.Lock()
		packInfo, exists := volume.packs[1]
		volume.stagingMu.Unlock()

		if !exists {
			t.Fatal("expected pack that gained live reference before deletion to be retained")
		}
		if packInfo.liveReferences != 1 {
			t.Fatalf("expected pack live references = 1, got %d", packInfo.liveReferences)
		}
		if current := server.Stats().LocalPackCacheBytes; current != packSize {
			t.Fatalf("expected pack cache bytes unchanged = %d, got %d", packSize, current)
		}
	})

	t.Run("AlreadyDeleted", func(t *testing.T) {
		clock := &testClock{now: time.Date(2026, 11, 21, 12, 0, 0, 0, time.UTC)}
		server := NewTestServer(t, newRecordingBackend(NewMemoryBackend()), WithServerClock(clock))

		volume, err := server.getOrCreateVolume("test-evict-recheck-del")
		if err != nil {
			t.Fatalf("getOrCreateVolume failed: %v", err)
		}

		_ = createAndUploadPack(t, server, volume, "/file.bin", 0, 1)

		volume.stagingMu.Lock()
		packSize := volume.packs[1].pack.ByteSize()
		volume.stagingMu.Unlock()

		setLocalPackCacheTargetBytesForTest(server, 1)

		candidate := evictablePackCandidate{
			volume: volume,
			packID: 1,
		}

		// Simulate the pack being deleted by volume-level eviction before evictCandidatePack is called.
		volume.stagingMu.Lock()
		volume.deletePackLocked(1)
		volume.stagingMu.Unlock()

		evicted, _ := server.evictCandidatePack(candidate)
		if evicted {
			t.Fatal("expected already-deleted pack not to be reported as evicted")
		}

		// Total should reflect the deletion performed by deletePackLocked, and should NOT be subtracted twice.
		if current := server.Stats().LocalPackCacheBytes; current != 0 {
			t.Fatalf("expected pack cache bytes = 0, got %d (packSize was %d)", current, packSize)
		}
	})
}
