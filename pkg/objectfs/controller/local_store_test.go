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
)

func TestLocalStoreInodeRecordEncodeDecode(t *testing.T) {
	now := time.Now().Truncate(time.Nanosecond)
	original := &InodeRecord{
		InodeID: 42,
		Mode:    0644,
		Size:    1024,
		ModTime: now,
		IsDir:   false,
		Sha256:  "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
		ETag:    "\"etag-12345\"",
	}

	payload, err := EncodeInodeRecord(original)
	if err != nil {
		t.Fatalf("EncodeInodeRecord failed: %v", err)
	}

	decoded, err := DecodeInodeRecord(payload)
	if err != nil {
		t.Fatalf("DecodeInodeRecord failed: %v", err)
	}

	if decoded.InodeID != original.InodeID {
		t.Errorf("InodeID mismatch: got %d, want %d", decoded.InodeID, original.InodeID)
	}
	if decoded.Mode != original.Mode {
		t.Errorf("Mode mismatch: got %d, want %d", decoded.Mode, original.Mode)
	}
	if decoded.Size != original.Size {
		t.Errorf("Size mismatch: got %d, want %d", decoded.Size, original.Size)
	}
	if !decoded.ModTime.Equal(original.ModTime) {
		t.Errorf("ModTime mismatch: got %v, want %v", decoded.ModTime, original.ModTime)
	}
	if decoded.IsDir != original.IsDir {
		t.Errorf("IsDir mismatch: got %v, want %v", decoded.IsDir, original.IsDir)
	}
	if decoded.Sha256 != original.Sha256 {
		t.Errorf("Sha256 mismatch: got %s, want %s", decoded.Sha256, original.Sha256)
	}
	if decoded.ETag != original.ETag {
		t.Errorf("ETag mismatch: got %s, want %s", decoded.ETag, original.ETag)
	}
}

func TestLocalStoreDirDeltaRecordEncodeDecode(t *testing.T) {
	original := &DirDeltaRecord{
		InodeID:    100,
		PrevOffset: PackOffset(2, 512),
		Deleted:    []string{"fileA.txt", "fileB.txt"},
		Added: []DirEntry{
			{Name: "fileC.txt", InodeID: 101, IsDir: false, Mode: 0644},
			{Name: "subfolder", InodeID: 102, IsDir: true, Mode: 0755},
		},
	}

	payload, err := EncodeDirDeltaRecord(original)
	if err != nil {
		t.Fatalf("EncodeDirDeltaRecord failed: %v", err)
	}

	decoded, err := DecodeDirDeltaRecord(payload)
	if err != nil {
		t.Fatalf("DecodeDirDeltaRecord failed: %v", err)
	}

	if decoded.InodeID != original.InodeID {
		t.Errorf("InodeID mismatch: got %d, want %d", decoded.InodeID, original.InodeID)
	}
	if decoded.PrevOffset != original.PrevOffset {
		t.Errorf("PrevOffset mismatch: got %d, want %d", decoded.PrevOffset, original.PrevOffset)
	}
	if len(decoded.Deleted) != len(original.Deleted) {
		t.Fatalf("Deleted count mismatch: got %d, want %d", len(decoded.Deleted), len(original.Deleted))
	}
	for i, del := range original.Deleted {
		if decoded.Deleted[i] != del {
			t.Errorf("Deleted[%d] mismatch: got %s, want %s", i, decoded.Deleted[i], del)
		}
	}
	if len(decoded.Added) != len(original.Added) {
		t.Fatalf("Added count mismatch: got %d, want %d", len(decoded.Added), len(original.Added))
	}
	for i, add := range original.Added {
		if decoded.Added[i] != add {
			t.Errorf("Added[%d] mismatch: got %+v, want %+v", i, decoded.Added[i], add)
		}
	}
}

func TestLocalStorageWriteAndReadRecords(t *testing.T) {
	dir := t.TempDir()
	ls, err := NewLocalStorage(dir)
	if err != nil {
		t.Fatalf("NewLocalStorage failed: %v", err)
	}
	defer ls.Close()

	rec1 := &InodeRecord{
		InodeID: 10,
		Mode:    0644,
		Size:    500,
		ModTime: time.Now(),
		Sha256:  "sha-1",
	}
	p1, _ := EncodeInodeRecord(rec1)
	off1, err := ls.WriteRecord(RecordTypeInode, p1)
	if err != nil {
		t.Fatalf("WriteRecord 1 failed: %v", err)
	}

	rec2 := &DirDeltaRecord{
		InodeID:    1,
		PrevOffset: NoOffset,
		Added:      []DirEntry{{Name: "test.txt", InodeID: 10, IsDir: false, Mode: 0644}},
	}
	p2, _ := EncodeDirDeltaRecord(rec2)
	off2, err := ls.WriteRecord(RecordTypeDirDelta, p2)
	if err != nil {
		t.Fatalf("WriteRecord 2 failed: %v", err)
	}

	// Read rec1 back
	t1, readP1, err := ls.ReadRecord(off1)
	if err != nil {
		t.Fatalf("ReadRecord 1 failed: %v", err)
	}
	if t1 != RecordTypeInode {
		t.Errorf("Expected RecordTypeInode, got %d", t1)
	}
	dec1, err := DecodeInodeRecord(readP1)
	if err != nil || dec1.InodeID != 10 || dec1.Sha256 != "sha-1" {
		t.Errorf("Decoded rec1 mismatch: %+v (err: %v)", dec1, err)
	}

	// Read rec2 back
	t2, readP2, err := ls.ReadRecord(off2)
	if err != nil {
		t.Fatalf("ReadRecord 2 failed: %v", err)
	}
	if t2 != RecordTypeDirDelta {
		t.Errorf("Expected RecordTypeDirDelta, got %d", t2)
	}
	dec2, err := DecodeDirDeltaRecord(readP2)
	if err != nil || dec2.InodeID != 1 || len(dec2.Added) != 1 || dec2.Added[0].Name != "test.txt" {
		t.Errorf("Decoded rec2 mismatch: %+v (err: %v)", dec2, err)
	}

	// Truncate and verify offsets reset
	if err := ls.Truncate(); err != nil {
		t.Fatalf("Truncate failed: %v", err)
	}
	if ls.ActiveFileSize() != HeaderLen {
		t.Errorf("Expected active file size %d after truncate, got %d", HeaderLen, ls.ActiveFileSize())
	}
}

func TestLRUCacheOperations(t *testing.T) {
	evicted := make(map[string]int)
	lru := NewLRUCache[string, int](3, func(k string, v int) {
		evicted[k] = v
	})

	lru.Put("a", 1)
	lru.Put("b", 2)
	lru.Put("c", 3)

	if lru.Len() != 3 {
		t.Fatalf("Expected len 3, got %d", lru.Len())
	}

	// Access "a" to make it MRU (order now: a, c, b)
	val, ok := lru.Get("a")
	if !ok || val != 1 {
		t.Fatalf("Expected to get 'a'=1, got %d, %v", val, ok)
	}

	// Add "d", should evict LRU "b"
	lru.Put("d", 4)
	if lru.Len() != 3 {
		t.Fatalf("Expected len 3, got %d", lru.Len())
	}
	if evicted["b"] != 2 {
		t.Fatalf("Expected 'b' to be evicted, got: %v", evicted)
	}

	// "b" should not be in cache
	if _, ok := lru.Get("b"); ok {
		t.Fatalf("Expected 'b' to be missing from cache")
	}

	// EvictAll
	lru.EvictAll()
	if lru.Len() != 0 {
		t.Fatalf("Expected len 0 after EvictAll, got %d", lru.Len())
	}
	if len(evicted) != 4 {
		t.Fatalf("Expected 4 total evictions, got %d: %v", len(evicted), evicted)
	}
}

func TestTieredMetadataLRUEvictionAndReload(t *testing.T) {
	ctx := t.Context()
	backend := NewMemoryBackend()
	localDir := t.TempDir()

	// Create volume with very small RAM capacity (max 3 inodes, max 2 dirs)
	vol := NewVolume("tiered-test-vol", backend, NewEventBroadcaster(),
		WithMaxRAMEntries(3, 2),
		WithLocalStorageDir(localDir),
	)
	defer vol.Close()

	// Create 10 files across 3 subdirectories
	for d := 1; d <= 3; d++ {
		dirPath := fmt.Sprintf("/dir%d", d)
		_, err := vol.Mkdir(ctx, dirPath, 0755)
		if err != nil {
			t.Fatalf("Mkdir %s failed: %v", dirPath, err)
		}

		for f := 1; f <= 4; f++ {
			filePath := fmt.Sprintf("%s/file%d.txt", dirPath, f)
			content := []byte(fmt.Sprintf("content of dir%d file%d", d, f))
			_, err := vol.CreateFile(ctx, filePath, 0644, content)
			if err != nil {
				t.Fatalf("CreateFile %s failed: %v", filePath, err)
			}
		}
	}

	// Verify local storage has recorded evicted records
	if len(vol.dirtyInodes) == 0 && len(vol.dirtyDirs) == 0 {
		t.Fatalf("Expected dirty entries in tiered storage maps")
	}

	// Read all 10 files back to test transparent LRU cache miss -> local file reload
	for d := 1; d <= 3; d++ {
		dirPath := fmt.Sprintf("/dir%d", d)
		entries, err := vol.ReadDir(ctx, dirPath)
		if err != nil {
			t.Fatalf("ReadDir %s failed: %v", dirPath, err)
		}
		if len(entries) != 4 {
			t.Fatalf("Expected 4 entries in %s, got %d", dirPath, len(entries))
		}

		for f := 1; f <= 4; f++ {
			filePath := fmt.Sprintf("%s/file%d.txt", dirPath, f)
			attr, err := vol.GetAttr(ctx, filePath)
			if err != nil {
				t.Fatalf("GetAttr %s failed: %v", filePath, err)
			}
			expectedContent := fmt.Sprintf("content of dir%d file%d", d, f)
			if attr.Size != int64(len(expectedContent)) {
				t.Fatalf("Size mismatch for %s: got %d, want %d", filePath, attr.Size, len(expectedContent))
			}

			data, total, _, err := vol.ReadFile(ctx, filePath, 0, 100)
			if err != nil {
				t.Fatalf("ReadFile %s failed: %v", filePath, err)
			}
			if total != int64(len(expectedContent)) || string(data) != expectedContent {
				t.Fatalf("Content mismatch for %s: got %q, want %q", filePath, string(data), expectedContent)
			}
		}
	}

	// Perform mutations on evicted files (write and rename)
	_, _, _, err := vol.WriteFile(ctx, "/dir1/file1.txt", 0, []byte("updated content!"), pb.WriteMode_LAZY_WRITE)
	if err != nil {
		t.Fatalf("WriteFile failed: %v", err)
	}

	_, err = vol.Rename(ctx, "/dir2/file2.txt", "/dir2/file2_renamed.txt")
	if err != nil {
		t.Fatalf("Rename failed: %v", err)
	}

	// Create snapshot from tiered metadata
	snapName, err := vol.CreateSnapshot(ctx)
	if err != nil {
		t.Fatalf("CreateSnapshot failed: %v", err)
	}
	if snapName == "" {
		t.Fatalf("Expected non-empty snapshot name")
	}

	// Verify local eviction storage is reset after snapshot
	if len(vol.dirtyInodes) != 0 || len(vol.dirtyDirs) != 0 {
		t.Fatalf("Expected dirty maps to be empty after snapshot, got %d inodes, %d dirs", len(vol.dirtyInodes), len(vol.dirtyDirs))
	}

	// Verify recovery on a fresh Volume pointing to same backend
	recoveredVol := NewVolume("tiered-test-vol", backend, NewEventBroadcaster(),
		WithMaxRAMEntries(3, 2),
	)
	defer recoveredVol.Close()

	if err := recoveredVol.LoadFromBackend(ctx); err != nil {
		t.Fatalf("LoadFromBackend failed: %v", err)
	}

	// Verify updated file in recovered volume
	upData, _, _, err := recoveredVol.ReadFile(ctx, "/dir1/file1.txt", 0, 100)
	if err != nil || string(upData) != "updated content!" {
		t.Fatalf("Expected 'updated content!' in recovered volume, got %q (err=%v)", string(upData), err)
	}

	// Verify renamed file in recovered volume
	renamedAttr, err := recoveredVol.GetAttr(ctx, "/dir2/file2_renamed.txt")
	if err != nil || renamedAttr.Name != "file2_renamed.txt" {
		t.Fatalf("Expected renamed file in recovered volume: %v", err)
	}
}

func TestLargeDirectoryDeltaEviction(t *testing.T) {
	ctx := t.Context()
	backend := NewMemoryBackend()
	localDir := t.TempDir()

	// Cache with only 1 directory in RAM
	vol := NewVolume("large-dir-test", backend, NewEventBroadcaster(),
		WithMaxRAMEntries(10, 1),
		WithLocalStorageDir(localDir),
	)
	defer vol.Close()

	// 1. Create directory /bigdir with 50 files
	_, err := vol.Mkdir(ctx, "/bigdir", 0755)
	if err != nil {
		t.Fatalf("Mkdir /bigdir failed: %v", err)
	}

	for i := 0; i < 50; i++ {
		filePath := fmt.Sprintf("/bigdir/file_%03d.txt", i)
		_, err := vol.CreateFile(ctx, filePath, 0644, []byte(fmt.Sprintf("data-%d", i)))
		if err != nil {
			t.Fatalf("CreateFile %s failed: %v", filePath, err)
		}
	}

	// Force eviction of /bigdir by accessing root and another directory
	_, _ = vol.Mkdir(ctx, "/otherdir", 0755)
	_, _ = vol.GetAttr(ctx, "/")

	// Verify /bigdir is recorded in dirtyDirs
	if len(vol.dirtyDirs) == 0 {
		t.Fatalf("Expected dirty directory recorded in tiered storage")
	}

	// 2. Perform delta operations: delete 1 file and add 1 new file
	if err := vol.Unlink(ctx, "/bigdir/file_005.txt"); err != nil {
		t.Fatalf("Unlink file_005 failed: %v", err)
	}
	_, err = vol.CreateFile(ctx, "/bigdir/new_file.txt", 0644, []byte("new file content"))
	if err != nil {
		t.Fatalf("CreateFile new_file.txt failed: %v", err)
	}

	// Force eviction of /bigdir again
	_, _ = vol.Mkdir(ctx, "/third_dir", 0755)

	// 3. ReadDir /bigdir: should replay deltas correctly
	entries, err := vol.ReadDir(ctx, "/bigdir")
	if err != nil {
		t.Fatalf("ReadDir /bigdir failed: %v", err)
	}
	if len(entries) != 50 { // 50 initial - 1 deleted + 1 added = 50
		t.Fatalf("Expected 50 entries in /bigdir after deltas, got %d", len(entries))
	}

	foundNew := false
	foundDeleted := false
	for _, e := range entries {
		if e.Name == "new_file.txt" {
			foundNew = true
		}
		if e.Name == "file_005.txt" {
			foundDeleted = true
		}
	}
	if !foundNew {
		t.Errorf("Expected new_file.txt in /bigdir")
	}
	if foundDeleted {
		t.Errorf("Expected file_005.txt to be removed from /bigdir")
	}
}

func TestAutoSnapshotTriggerOnThreshold(t *testing.T) {
	ctx := t.Context()
	backend := NewMemoryBackend()
	localDir := t.TempDir()

	// Threshold: snapshot triggers when dirty records >= 5
	vol := NewVolume("snap-trigger-test", backend, NewEventBroadcaster(),
		WithMaxRAMEntries(2, 2),
		WithLocalStorageDir(localDir),
		WithSnapshotThreshold(5, 0),
	)
	defer vol.Close()

	// Create 10 files which evicts and reaches threshold of 5 dirty records
	for i := 0; i < 10; i++ {
		filePath := fmt.Sprintf("/auto_file_%d.txt", i)
		_, err := vol.CreateFile(ctx, filePath, 0644, []byte(fmt.Sprintf("data-%d", i)))
		if err != nil {
			t.Fatalf("CreateFile %s failed: %v", filePath, err)
		}
	}

	// Verify snapshots were automatically generated and saved to backend
	snapshots, err := vol.ListSnapshots(ctx)
	if err != nil {
		t.Fatalf("ListSnapshots failed: %v", err)
	}
	if len(snapshots) == 0 {
		t.Fatalf("Expected auto-triggered snapshot in backend, got none")
	}
}
