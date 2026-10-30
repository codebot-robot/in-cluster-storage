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
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
)

func TestStagingManager_StageAndReadChunk(t *testing.T) {
	dir := t.TempDir()
	volumeID := "test-vol-staging-append"
	epoch := int64(123456789)

	sm, err := NewStagingManager(dir, volumeID, epoch)
	if err != nil {
		t.Fatalf("NewStagingManager failed: %v", err)
	}
	defer func() { _ = sm.Close() }()

	data1 := []byte("hello staging manager chunk 1")
	ref1, p1, pSeq1, l1, lSeq1, err := sm.StageChunk(100, 0, 1, data1)
	if err != nil {
		t.Fatalf("StageChunk 1 failed: %v", err)
	}
	if err := p1.SyncToSeq(pSeq1); err != nil {
		t.Fatalf("SyncToSeq pack 1 failed: %v", err)
	}
	if err := l1.SyncToSeq(lSeq1); err != nil {
		t.Fatalf("SyncToSeq log 1 failed: %v", err)
	}

	data2 := bytes.Repeat([]byte("B"), 65536)
	ref2, p2, pSeq2, l2, lSeq2, err := sm.StageChunk(100, 1, 2, data2)
	if err != nil {
		t.Fatalf("StageChunk 2 failed: %v", err)
	}
	if err := p2.SyncToSeq(pSeq2); err != nil {
		t.Fatalf("SyncToSeq pack 2 failed: %v", err)
	}
	if err := l2.SyncToSeq(lSeq2); err != nil {
		t.Fatalf("SyncToSeq log 2 failed: %v", err)
	}

	read1, err := sm.ReadChunk(ref1)
	if err != nil || !bytes.Equal(read1, data1) {
		t.Fatalf("ReadChunk 1 failed: %v, data=%q", err, string(read1))
	}

	read2, err := sm.ReadChunk(ref2)
	if err != nil || !bytes.Equal(read2, data2) {
		t.Fatalf("ReadChunk 2 failed: %v", err)
	}
}

func TestStagingManager_GroupCommit(t *testing.T) {
	dir := t.TempDir()
	volumeID := "test-vol-staging-group-commit"
	epoch := int64(987654321)

	sm, err := NewStagingManager(dir, volumeID, epoch)
	if err != nil {
		t.Fatalf("NewStagingManager failed: %v", err)
	}
	defer func() { _ = sm.Close() }()

	const numWriters = 8
	const writesPerWriter = 8
	var wg sync.WaitGroup
	refs := make([][]StagedChunkRef, numWriters)

	for w := 0; w < numWriters; w++ {
		wg.Add(1)
		wID := w
		go func() {
			defer wg.Done()
			wRefs := make([]StagedChunkRef, writesPerWriter)
			for i := 0; i < writesPerWriter; i++ {
				data := []byte(fmt.Sprintf("writer-%d-chunk-%d", wID, i))
				ref, p, pSeq, l, lSeq, err := sm.StageChunk(uint64(1000+wID), uint32(i), uint64(i+1), data)
				if err != nil {
					t.Errorf("writer %d stage %d failed: %v", wID, i, err)
					return
				}
				if err := p.SyncToSeq(pSeq); err != nil {
					t.Errorf("writer %d pack sync %d failed: %v", wID, i, err)
					return
				}
				if err := l.SyncToSeq(lSeq); err != nil {
					t.Errorf("writer %d log sync %d failed: %v", wID, i, err)
					return
				}
				wRefs[i] = ref
			}
			refs[wID] = wRefs
		}()
	}
	wg.Wait()

	for w := 0; w < numWriters; w++ {
		for i := 0; i < writesPerWriter; i++ {
			expectedData := []byte(fmt.Sprintf("writer-%d-chunk-%d", w, i))
			data, err := sm.ReadChunk(refs[w][i])
			if err != nil || !bytes.Equal(data, expectedData) {
				t.Fatalf("ReadChunk writer %d chunk %d mismatch", w, i)
			}
		}
	}
}

func TestStagingManager_RotateAndSeal(t *testing.T) {
	dir := t.TempDir()
	volumeID := "test-vol-staging-rotate"
	epoch := int64(111222333)

	sm, err := NewStagingManager(dir, volumeID, epoch)
	if err != nil {
		t.Fatalf("NewStagingManager failed: %v", err)
	}
	defer func() { _ = sm.Close() }()

	data1 := []byte("chunk-in-pack-1")
	ref1, p1, pSeq1, l1, lSeq1, err := sm.StageChunk(200, 0, 1, data1)
	if err != nil {
		t.Fatalf("StageChunk 1 failed: %v", err)
	}

	// Rotate seals active pack and active log unconditionally
	if err := sm.Rotate(); err != nil {
		t.Fatalf("Rotate failed: %v", err)
	}

	if err := p1.SyncToSeq(pSeq1); err != nil {
		t.Fatalf("SyncToSeq on sealed p1 failed: %v", err)
	}
	if err := l1.SyncToSeq(lSeq1); err != nil {
		t.Fatalf("SyncToSeq on sealed l1 failed: %v", err)
	}

	data2 := []byte("chunk-in-pack-2")
	ref2, p2, pSeq2, l2, lSeq2, err := sm.StageChunk(200, 1, 2, data2)
	if err != nil {
		t.Fatalf("StageChunk 2 failed: %v", err)
	}
	_ = p2.SyncToSeq(pSeq2)
	_ = l2.SyncToSeq(lSeq2)

	if ref1.PackID == ref2.PackID {
		t.Errorf("expected different pack IDs after rotate: p1=%d, p2=%d", ref1.PackID, ref2.PackID)
	}

	sealed := sm.SealedPacksToUpload()
	if len(sealed) != 1 || sealed[0].ID() != ref1.PackID {
		t.Fatalf("expected 1 sealed pack with ID %d, got %v", ref1.PackID, sealed)
	}
}

func TestStagingManager_TornTailRecovery(t *testing.T) {
	ctx := t.Context()
	dir := t.TempDir()
	volumeID := "test-vol-staging-torn-tail"
	epoch := int64(444555666)

	sm, err := NewStagingManager(dir, volumeID, epoch)
	if err != nil {
		t.Fatalf("NewStagingManager failed: %v", err)
	}

	data1 := []byte("valid-chunk-one")
	ref1, p1, ps1, l1, ls1, err := sm.StageChunk(300, 0, 1, data1)
	if err != nil {
		t.Fatalf("StageChunk 1 failed: %v", err)
	}
	_ = p1.SyncToSeq(ps1)
	_ = l1.SyncToSeq(ls1)

	logPath := l1.Path()
	_ = sm.Close()

	// Append torn bytes at end of newest intent log
	f, err := os.OpenFile(logPath, os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		t.Fatalf("OpenFile failed: %v", err)
	}
	if _, err := f.Write([]byte("torn-incomplete-intent-bytes")); err != nil {
		t.Fatalf("Write torn tail failed: %v", err)
	}
	_ = f.Close()

	// Reopen StagingManager and recover
	reopened, err := NewStagingManager(dir, volumeID, epoch)
	if err != nil {
		t.Fatalf("NewStagingManager failed: %v", err)
	}
	defer func() { _ = reopened.Close() }()

	index, err := reopened.ScanAndRecover(ctx)
	if err != nil {
		t.Fatalf("ScanAndRecover failed on torn tail: %v", err)
	}

	if len(index[300]) != 1 {
		t.Fatalf("expected 1 recovered chunk for inode 300, got %d", len(index[300]))
	}

	recoveredRef := index[300][0]
	if recoveredRef.PackID != ref1.PackID || recoveredRef.Length != ref1.Length {
		t.Fatalf("recovered ref mismatch: %+v vs %+v", recoveredRef, ref1)
	}

	recoveredData, err := reopened.ReadChunk(recoveredRef)
	if err != nil || !bytes.Equal(recoveredData, data1) {
		t.Fatalf("recovered data mismatch: %v, got %q", err, string(recoveredData))
	}
}

func TestStagingManager_OlderCorruptionFailsLoudly(t *testing.T) {
	ctx := t.Context()
	dir := t.TempDir()
	volumeID := "test-vol-staging-corrupt-older"
	epoch := int64(777888999)

	sm, err := NewStagingManager(dir, volumeID, epoch)
	if err != nil {
		t.Fatalf("NewStagingManager failed: %v", err)
	}

	data1 := []byte("chunk-in-older-pack")
	_, p1, ps1, l1, ls1, err := sm.StageChunk(400, 0, 1, data1)
	if err != nil {
		t.Fatalf("StageChunk failed: %v", err)
	}
	_ = p1.SyncToSeq(ps1)
	_ = l1.SyncToSeq(ls1)
	oldLogPath := l1.Path()

	// Rotate to create a newer pack and log
	if err := sm.Rotate(); err != nil {
		t.Fatalf("Rotate failed: %v", err)
	}

	data2 := []byte("chunk-in-newer-pack")
	_, p2, ps2, l2, ls2, err := sm.StageChunk(400, 1, 2, data2)
	if err != nil {
		t.Fatalf("StageChunk 2 failed: %v", err)
	}
	_ = p2.SyncToSeq(ps2)
	_ = l2.SyncToSeq(ls2)
	_ = sm.Close()

	// Corrupt a byte in older intent log record
	logBytes, err := os.ReadFile(oldLogPath)
	if err != nil {
		t.Fatalf("ReadFile failed: %v", err)
	}
	logBytes[IntentLogHeaderLength+10] ^= 0xFF
	if err := os.WriteFile(oldLogPath, logBytes, 0644); err != nil {
		t.Fatalf("WriteFile failed: %v", err)
	}

	// Reopen: older log corruption must fail loudly naming the file
	reopened, err := NewStagingManager(dir, volumeID, epoch)
	if err != nil {
		t.Fatalf("NewStagingManager failed: %v", err)
	}
	defer func() { _ = reopened.Close() }()

	_, err = reopened.ScanAndRecover(ctx)
	if err == nil {
		t.Fatalf("expected ScanAndRecover on corrupt older log to fail")
	}
	if !strings.Contains(err.Error(), filepath.Base(oldLogPath)) {
		t.Errorf("expected error to name log %q, got: %v", filepath.Base(oldLogPath), err)
	}
}

func TestStagingManager_EpochReadErrorNotSwallowed(t *testing.T) {
	dir := t.TempDir()
	volumeID := "test-vol-epoch-err"

	epochPath := filepath.Join(dir, "epoch")
	if err := os.WriteFile(epochPath, []byte("invalid-epoch-data"), 0644); err != nil {
		t.Fatalf("WriteFile failed: %v", err)
	}

	_, err := NewStagingManager(dir, volumeID, 0)
	if err == nil {
		t.Fatalf("expected NewStagingManager to fail on invalid epoch file")
	}
	if !strings.Contains(err.Error(), "invalid staging epoch") {
		t.Errorf("expected error to mention invalid staging epoch, got: %v", err)
	}
}
