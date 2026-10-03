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
	"sync"
	"testing"
	"time"

	pb "github.com/gke-labs/in-cluster-storage/pkg/api/objectfs/v1alpha1"
	"github.com/gke-labs/in-cluster-storage/pkg/objectstore/inmemorystorage"
	walclient "github.com/gke-labs/in-cluster-storage/pkg/wal/client"
	"github.com/google/uuid"
)

func TestSQLiteMetadataStoreOperations(t *testing.T) {
	ctx := t.Context()
	localDir := t.TempDir()
	backend := inmemorystorage.New()
	broadcaster := NewEventBroadcaster()
	volID := "vol-sqlite-ops"

	vol := NewVolume(volID, backend, broadcaster,
		WithMetadataStore("sqlite"),
		WithLocalStorageDir(localDir),
	)
	defer vol.Close()

	if err := vol.LoadFromBackend(ctx); err != nil {
		t.Fatalf("LoadFromBackend failed: %v", err)
	}

	// 1. Root attr
	rootAttr, err := vol.GetAttr(ctx, 1)
	if err != nil {
		t.Fatalf("GetAttr(1) failed: %v", err)
	}
	if !rootAttr.IsDir {
		t.Errorf("expected root to be dir")
	}

	// 2. Mkdir
	dirAttr, err := vol.Mkdir(ctx, 1, "mydir", 0755, 1000, 1000)
	if err != nil {
		t.Fatalf("Mkdir failed: %v", err)
	}
	if dirAttr.Inode == 0 || dirAttr.Name != "mydir" || !dirAttr.IsDir {
		t.Fatalf("unexpected dirAttr: %+v", dirAttr)
	}

	// 3. CreateFile (inline)
	data1 := []byte("hello sqlite world")
	fileAttr, err := vol.CreateFile(ctx, dirAttr.Inode, "hello.txt", 0644, data1, 1000, 1000)
	if err != nil {
		t.Fatalf("CreateFile failed: %v", err)
	}
	if fileAttr.Size != int64(len(data1)) {
		t.Errorf("file size mismatch: got %d, want %d", fileAttr.Size, len(data1))
	}

	// 4. Lookup
	lookupAttr, err := vol.Lookup(ctx, dirAttr.Inode, "hello.txt")
	if err != nil {
		t.Fatalf("Lookup failed: %v", err)
	}
	if lookupAttr.Inode != fileAttr.Inode {
		t.Errorf("lookup inode mismatch: %d vs %d", lookupAttr.Inode, fileAttr.Inode)
	}

	// 5. ReadFile
	readData, totalSize, _, err := vol.ReadFile(ctx, fileAttr.Inode, 0, 100)
	if err != nil {
		t.Fatalf("ReadFile failed: %v", err)
	}
	if totalSize != int64(len(data1)) || !bytes.Equal(readData, data1) {
		t.Errorf("ReadFile mismatch: got %q (total=%d)", string(readData), totalSize)
	}

	// 6. Large chunked write
	largeData := make([]byte, 128*1024)
	for i := range largeData {
		largeData[i] = byte(i % 251)
	}
	chunkAttr, err := vol.CreateFile(ctx, dirAttr.Inode, "large.bin", 0644, nil, 1000, 1000)
	if err != nil {
		t.Fatalf("CreateFile large.bin failed: %v", err)
	}
	nWritten, newSize, _, err := vol.WriteFile(ctx, chunkAttr.Inode, 0, largeData, pb.WriteMode_WRITE_MODE_UNSPECIFIED)
	if err != nil {
		t.Fatalf("WriteFile failed: %v", err)
	}
	if nWritten != int64(len(largeData)) || newSize != int64(len(largeData)) {
		t.Errorf("WriteFile result mismatch: nWritten=%d, newSize=%d", nWritten, newSize)
	}

	readChunk, _, _, err := vol.ReadFile(ctx, chunkAttr.Inode, 0, int64(len(largeData)))
	if err != nil {
		t.Fatalf("ReadFile large.bin failed: %v", err)
	}
	if !bytes.Equal(readChunk, largeData) {
		t.Fatalf("readChunk content mismatch")
	}

	// 7. ReadDir
	entries, err := vol.ReadDir(ctx, dirAttr.Inode)
	if err != nil {
		t.Fatalf("ReadDir failed: %v", err)
	}
	if len(entries) != 2 {
		t.Fatalf("expected 2 entries in mydir, got %d", len(entries))
	}

	// 8. Rename
	renamedAttr, err := vol.Rename(ctx, dirAttr.Inode, "hello.txt", dirAttr.Inode, "greeting.txt")
	if err != nil {
		t.Fatalf("Rename failed: %v", err)
	}
	if renamedAttr.Name != "greeting.txt" {
		t.Errorf("renamed name mismatch: %s", renamedAttr.Name)
	}

	// 9. Unlink
	if err := vol.Unlink(ctx, dirAttr.Inode, "greeting.txt"); err != nil {
		t.Fatalf("Unlink failed: %v", err)
	}
	if _, err := vol.Lookup(ctx, dirAttr.Inode, "greeting.txt"); err == nil {
		t.Errorf("expected ENOENT after unlink")
	}

	// 10. Truncate
	if _, err := vol.TruncateFile(ctx, chunkAttr.Inode, 10); err != nil {
		t.Fatalf("TruncateFile failed: %v", err)
	}
	tAttr, _ := vol.GetAttr(ctx, chunkAttr.Inode)
	if tAttr.Size != 10 {
		t.Errorf("expected truncated size 10, got %d", tAttr.Size)
	}

	// 11. Clean up large.bin and Rmdir
	if err := vol.Unlink(ctx, dirAttr.Inode, "large.bin"); err != nil {
		t.Fatalf("Unlink large.bin failed: %v", err)
	}
	if err := vol.Rmdir(ctx, 1, "mydir"); err != nil {
		t.Fatalf("Rmdir failed: %v", err)
	}
}

func TestSQLiteMetadataStoreRecoveryFromLocalDB(t *testing.T) {
	ctx := t.Context()
	localDir := t.TempDir()
	backend := inmemorystorage.New()
	broadcaster := NewEventBroadcaster()
	volID := "vol-local-rec"

	// 1. First run: create files
	vol1 := NewVolume(volID, backend, broadcaster,
		WithMetadataStore("sqlite"),
		WithLocalStorageDir(localDir),
	)
	if err := vol1.LoadFromBackend(ctx); err != nil {
		t.Fatalf("LoadFromBackend failed: %v", err)
	}

	dirAttr, err := vol1.Mkdir(ctx, 1, "data", 0755, 0, 0)
	if err != nil {
		t.Fatalf("Mkdir failed: %v", err)
	}

	content := []byte("persisted in sqlite")
	fileAttr, err := vol1.CreateFile(ctx, dirAttr.Inode, "test.txt", 0644, content, 0, 0)
	if err != nil {
		t.Fatalf("CreateFile failed: %v", err)
	}

	// Close volume
	if err := vol1.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	// 2. Second run: reopen with same localDir
	vol2 := NewVolume(volID, backend, broadcaster,
		WithMetadataStore("sqlite"),
		WithLocalStorageDir(localDir),
	)
	defer vol2.Close()

	if err := vol2.LoadFromBackend(ctx); err != nil {
		t.Fatalf("LoadFromBackend run 2 failed: %v", err)
	}

	lookupAttr, err := vol2.Lookup(ctx, dirAttr.Inode, "test.txt")
	if err != nil {
		t.Fatalf("Lookup after local restart failed: %v", err)
	}
	if lookupAttr.Inode != fileAttr.Inode {
		t.Errorf("inode mismatch: %d vs %d", lookupAttr.Inode, fileAttr.Inode)
	}

	data, _, _, err := vol2.ReadFile(ctx, lookupAttr.Inode, 0, 100)
	if err != nil {
		t.Fatalf("ReadFile failed: %v", err)
	}
	if !bytes.Equal(data, content) {
		t.Errorf("content mismatch: got %q, want %q", string(data), string(content))
	}
}

func TestSQLiteMetadataStoreRecoveryFromPublishedSnapshot(t *testing.T) {
	ctx := t.Context()
	localDir1 := t.TempDir()
	localDir2 := t.TempDir()
	backend := inmemorystorage.New()
	broadcaster := NewEventBroadcaster()
	volID := "vol-snap-rec"
	streamID := uuid.New()

	// 1. First run: create files and publish snapshot
	vol1 := NewVolume(volID, backend, broadcaster,
		WithMetadataStore("sqlite"),
		WithLocalStorageDir(localDir1),
		WithStreamID(streamID),
	)
	if err := vol1.LoadFromBackend(ctx); err != nil {
		t.Fatalf("LoadFromBackend failed: %v", err)
	}

	dirAttr, err := vol1.Mkdir(ctx, 1, "snapdir", 0755, 0, 0)
	if err != nil {
		t.Fatalf("Mkdir failed: %v", err)
	}
	content := []byte("snapshot restored content")
	fileAttr, err := vol1.CreateFile(ctx, dirAttr.Inode, "file.txt", 0644, content, 0, 0)
	if err != nil {
		t.Fatalf("CreateFile failed: %v", err)
	}

	// Flush to backend publishes SQLite snapshot and EROFS snapshot
	if err := vol1.FlushToBackend(ctx); err != nil {
		t.Fatalf("FlushToBackend failed: %v", err)
	}
	_ = vol1.Close()

	// 2. Second run: restore into a clean new local directory from published snapshot
	vol2 := NewVolume(volID, backend, broadcaster,
		WithMetadataStore("sqlite"),
		WithLocalStorageDir(localDir2),
		WithStreamID(streamID),
	)
	defer vol2.Close()

	if err := vol2.LoadFromBackend(ctx); err != nil {
		t.Fatalf("LoadFromBackend from snapshot failed: %v", err)
	}

	lookupAttr, err := vol2.Lookup(ctx, dirAttr.Inode, "file.txt")
	if err != nil {
		t.Fatalf("Lookup after snapshot restore failed: %v", err)
	}
	if lookupAttr.Inode != fileAttr.Inode {
		t.Errorf("inode mismatch: %d vs %d", lookupAttr.Inode, fileAttr.Inode)
	}

	data, _, _, err := vol2.ReadFile(ctx, lookupAttr.Inode, 0, 100)
	if err != nil {
		t.Fatalf("ReadFile failed: %v", err)
	}
	if !bytes.Equal(data, content) {
		t.Errorf("content mismatch: got %q, want %q", string(data), string(content))
	}
}

func TestSQLiteMetadataStoreRecoveryFromErofsSnapshot(t *testing.T) {
	ctx := t.Context()
	localDir1 := t.TempDir()
	localDir2 := t.TempDir()
	backend := inmemorystorage.New()
	broadcaster := NewEventBroadcaster()
	volID := "vol-erofs-import"
	streamID := uuid.New()

	// 1. First run: Legacy mode creates EROFS snapshot
	vol1 := NewVolume(volID, backend, broadcaster,
		WithMetadataStore("legacy"),
		WithLocalStorageDir(localDir1),
		WithStreamID(streamID),
	)
	if err := vol1.LoadFromBackend(ctx); err != nil {
		t.Fatalf("LoadFromBackend failed: %v", err)
	}

	dirAttr, err := vol1.Mkdir(ctx, 1, "erofsdir", 0755, 0, 0)
	if err != nil {
		t.Fatalf("Mkdir failed: %v", err)
	}
	content := []byte("erofs imported content")
	_, err = vol1.CreateFile(ctx, dirAttr.Inode, "doc.txt", 0644, content, 0, 0)
	if err != nil {
		t.Fatalf("CreateFile failed: %v", err)
	}

	if err := vol1.FlushToBackend(ctx); err != nil {
		t.Fatalf("FlushToBackend failed: %v", err)
	}
	_ = vol1.Close()

	// Delete any sqlite snapshots from backend to ensure EROFS snapshot import is tested
	snapObjs, _ := backend.ListObjects(ctx, "", "snapshots/")
	for _, objKey := range snapObjs {
		_ = backend.DeleteObject(ctx, "", objKey)
	}

	// 2. Second run: Open in SQLite mode without local db -> imports EROFS snapshot
	vol2 := NewVolume(volID, backend, broadcaster,
		WithMetadataStore("sqlite"),
		WithLocalStorageDir(localDir2),
		WithStreamID(streamID),
	)
	defer vol2.Close()

	if err := vol2.LoadFromBackend(ctx); err != nil {
		t.Fatalf("LoadFromBackend SQLite importing EROFS failed: %v", err)
	}

	dirLookup, err := vol2.Lookup(ctx, 1, "erofsdir")
	if err != nil {
		t.Fatalf("Lookup erofsdir after EROFS import failed: %v", err)
	}

	lookupAttr, err := vol2.Lookup(ctx, dirLookup.Inode, "doc.txt")
	if err != nil {
		t.Fatalf("Lookup doc.txt after EROFS import failed: %v", err)
	}
	if lookupAttr.Size != int64(len(content)) {
		t.Errorf("size mismatch: %d vs %d", lookupAttr.Size, len(content))
	}

	data, _, _, err := vol2.ReadFile(ctx, lookupAttr.Inode, 0, 100)
	if err != nil {
		t.Fatalf("ReadFile failed: %v", err)
	}
	if !bytes.Equal(data, content) {
		t.Errorf("content mismatch: got %q, want %q", string(data), string(content))
	}
}

func TestSQLiteMetadataStoreCrashRecoveryWithStream(t *testing.T) {
	ctx := t.Context()
	walDir := t.TempDir()
	localDir := t.TempDir()
	backend := inmemorystorage.New()
	broadcaster := NewEventBroadcaster()
	volID := "vol-crash-rec"
	streamID := StreamIDForVolume(volID)

	stream1, err := walclient.Open(ctx, walDir, streamID, "")
	if err != nil {
		t.Fatalf("walclient.Open failed: %v", err)
	}

	vol1 := NewVolume(volID, backend, broadcaster,
		WithMetadataStore("sqlite"),
		WithLocalStorageDir(localDir),
		WithStream(stream1),
	)
	if err := vol1.LoadFromBackend(ctx); err != nil {
		t.Fatalf("LoadFromBackend failed: %v", err)
	}

	// Perform 10 operations
	for i := 0; i < 10; i++ {
		name := fmt.Sprintf("dir_%d", i)
		_, err := vol1.Mkdir(ctx, 1, name, 0755, 0, 0)
		if err != nil {
			t.Fatalf("Mkdir %s failed: %v", name, err)
		}
	}

	// Simulate crash: close stream and delete SQLite DB to force stream replay from 0
	_ = stream1.Close()
	_ = vol1.Close()
	_ = os.Remove(filepath.Join(localDir, "metadata.sqlite"))

	// Reopen with new stream instance
	stream2, err := walclient.Open(ctx, walDir, streamID, "")
	if err != nil {
		t.Fatalf("walclient.Open run 2 failed: %v", err)
	}

	vol2 := NewVolume(volID, backend, broadcaster,
		WithMetadataStore("sqlite"),
		WithLocalStorageDir(localDir),
		WithStream(stream2),
	)
	defer vol2.Close()

	if err := vol2.LoadFromBackend(ctx); err != nil {
		t.Fatalf("LoadFromBackend replay failed: %v", err)
	}

	// Verify all 10 directories are present
	entries, err := vol2.ReadDir(ctx, 1)
	if err != nil {
		t.Fatalf("ReadDir failed: %v", err)
	}
	if len(entries) != 10 {
		t.Fatalf("expected 10 replayed entries, got %d", len(entries))
	}
}

func TestSQLiteReadCacheHitRateAndNegativeCaching(t *testing.T) {
	ctx := t.Context()
	localDir := t.TempDir()
	backend := inmemorystorage.New()
	volID := "vol-sqlite-cache-test"

	vol := NewVolume(volID, backend, NewEventBroadcaster(),
		WithMetadataStore("sqlite"),
		WithLocalStorageDir(localDir),
	)
	defer vol.Close()

	if err := vol.LoadFromBackend(ctx); err != nil {
		t.Fatalf("LoadFromBackend failed: %v", err)
	}

	vol.SQLiteCacheResetStats()

	// 1. Point lookup of non-existent file -> miss + negative cache entry created
	_, err := vol.Lookup(ctx, 1, "does_not_exist.txt")
	if err == nil {
		t.Fatalf("expected error looking up non-existent file")
	}
	stats := vol.SQLiteCacheStats()
	if stats.Misses != 1 || stats.Hits != 0 {
		t.Fatalf("expected 1 miss and 0 hits on first lookup, got misses=%d hits=%d", stats.Misses, stats.Hits)
	}
	if stats.Entries == 0 {
		t.Fatalf("expected negative cache entry stored, got entries=%d", stats.Entries)
	}

	// 2. Second lookup of the same non-existent file -> negative cache hit (no SQLite query!)
	_, err = vol.Lookup(ctx, 1, "does_not_exist.txt")
	if err == nil {
		t.Fatalf("expected error looking up non-existent file")
	}
	stats = vol.SQLiteCacheStats()
	if stats.Hits != 1 || stats.Misses != 1 {
		t.Fatalf("expected 1 hit and 1 miss after second lookup, got hits=%d misses=%d", stats.Hits, stats.Misses)
	}
	if stats.HitRate != 0.5 {
		t.Fatalf("expected hitRate 0.5, got %f", stats.HitRate)
	}

	// 3. Create the file -> replaces negative cache entry with positive entry in same transaction
	attr, err := vol.CreateFile(ctx, 1, "does_not_exist.txt", 0644, []byte("now I exist"), 0, 0)
	if err != nil {
		t.Fatalf("CreateFile failed: %v", err)
	}

	// 4. Lookup now succeeds directly from cache (hit!)
	lookupAttr, err := vol.Lookup(ctx, 1, "does_not_exist.txt")
	if err != nil {
		t.Fatalf("Lookup failed: %v", err)
	}
	if lookupAttr.Inode != attr.Inode || lookupAttr.Size != int64(len("now I exist")) {
		t.Fatalf("unexpected lookup attr: %+v", lookupAttr)
	}
	stats = vol.SQLiteCacheStats()
	// Lookup checks DirEntry (hit) and toEntryAttr checks Inode (hit)
	if stats.Hits < 2 {
		t.Fatalf("expected hits >= 2, got %d", stats.Hits)
	}

	// 5. Stat the inode -> cache hit!
	vol.SQLiteCacheResetStats()
	statAttr, err := vol.GetAttr(ctx, attr.Inode)
	if err != nil {
		t.Fatalf("GetAttr failed: %v", err)
	}
	if statAttr.Inode != attr.Inode {
		t.Fatalf("mismatched stat inode: %d vs %d", statAttr.Inode, attr.Inode)
	}
	stats = vol.SQLiteCacheStats()
	if stats.Hits != 1 || stats.Misses != 0 {
		t.Fatalf("expected 1 hit 0 misses on GetAttr, got hits=%d misses=%d", stats.Hits, stats.Misses)
	}

	// 6. Delete file -> updates cache entry to negative
	err = vol.Unlink(ctx, 1, "does_not_exist.txt")
	if err != nil {
		t.Fatalf("Unlink failed: %v", err)
	}

	// 7. Lookup after deletion -> immediate negative cache hit (no SQLite query!)
	vol.SQLiteCacheResetStats()
	_, err = vol.Lookup(ctx, 1, "does_not_exist.txt")
	if err == nil {
		t.Fatalf("expected error looking up unlinked file")
	}
	stats = vol.SQLiteCacheStats()
	if stats.Hits != 1 || stats.Misses != 0 {
		t.Fatalf("expected 1 hit (negative cache hit) and 0 misses, got hits=%d misses=%d", stats.Hits, stats.Misses)
	}
}

func TestSQLiteReadCacheCoherence(t *testing.T) {
	ctx := t.Context()
	localDir := t.TempDir()
	backend := inmemorystorage.New()
	volID := "vol-sqlite-coherence"

	vol := NewVolume(volID, backend, NewEventBroadcaster(),
		WithMetadataStore("sqlite"),
		WithLocalStorageDir(localDir),
	)
	defer vol.Close()

	if err := vol.LoadFromBackend(ctx); err != nil {
		t.Fatalf("LoadFromBackend failed: %v", err)
	}

	// Create file
	fAttr, err := vol.CreateFile(ctx, 1, "initial.txt", 0644, []byte("hello"), 0, 0)
	if err != nil {
		t.Fatalf("CreateFile failed: %v", err)
	}

	// Rename file
	_, err = vol.Rename(ctx, 1, "initial.txt", 1, "renamed.txt")
	if err != nil {
		t.Fatalf("Rename failed: %v", err)
	}

	// Old name should be negative cache hit
	vol.SQLiteCacheResetStats()
	_, err = vol.Lookup(ctx, 1, "initial.txt")
	if err == nil {
		t.Fatalf("expected error for old name after rename")
	}
	stats := vol.SQLiteCacheStats()
	if stats.Hits != 1 || stats.Misses != 0 {
		t.Fatalf("expected 1 negative hit for old name, got hits=%d misses=%d", stats.Hits, stats.Misses)
	}

	// New name should be positive cache hit
	vol.SQLiteCacheResetStats()
	rAttr, err := vol.Lookup(ctx, 1, "renamed.txt")
	if err != nil {
		t.Fatalf("Lookup for renamed file failed: %v", err)
	}
	if rAttr.Inode != fAttr.Inode {
		t.Fatalf("mismatched inode: %d vs %d", rAttr.Inode, fAttr.Inode)
	}

	// Truncate file updates cached inode size
	_, err = vol.TruncateFile(ctx, fAttr.Inode, 100)
	if err != nil {
		t.Fatalf("TruncateFile failed: %v", err)
	}
	tAttr, err := vol.GetAttr(ctx, fAttr.Inode)
	if err != nil {
		t.Fatalf("GetAttr failed: %v", err)
	}
	if tAttr.Size != 100 {
		t.Fatalf("expected size 100 after truncate, got %d", tAttr.Size)
	}

	// WriteFile updates cached inode size and mtime
	_, newSize, _, err := vol.WriteFile(ctx, fAttr.Inode, 100, []byte(" appended"), 0)
	if err != nil {
		t.Fatalf("WriteFile failed: %v", err)
	}
	if newSize != 109 {
		t.Fatalf("expected size 109 after write, got %d", newSize)
	}
	wAttr, err := vol.GetAttr(ctx, fAttr.Inode)
	if err != nil {
		t.Fatalf("GetAttr failed: %v", err)
	}
	if wAttr.Size != 109 {
		t.Fatalf("expected size 109 in cached inode after write, got %d", wAttr.Size)
	}
}

func TestSQLiteReadCacheMemoryLimits(t *testing.T) {
	ctx := t.Context()
	localDir := t.TempDir()
	backend := inmemorystorage.New()
	volID := "vol-sqlite-limits"

	// Bounded to 50 entries and 10 KB
	vol := NewVolume(volID, backend, NewEventBroadcaster(),
		WithMetadataStore("sqlite"),
		WithLocalStorageDir(localDir),
		WithMetadataCacheLimits(50, 10*1024),
	)
	defer vol.Close()

	if err := vol.LoadFromBackend(ctx); err != nil {
		t.Fatalf("LoadFromBackend failed: %v", err)
	}

	var inodes []uint64
	for i := 0; i < 200; i++ {
		attr, err := vol.CreateFile(ctx, 1, fmt.Sprintf("file_%d.txt", i), 0644, []byte("content"), 0, 0)
		if err != nil {
			t.Fatalf("CreateFile failed: %v", err)
		}
		inodes = append(inodes, attr.Inode)
	}

	stats := vol.SQLiteCacheStats()
	if stats.Entries > 50 {
		t.Fatalf("expected <= 50 cache entries, got %d", stats.Entries)
	}
	if stats.Bytes > 10*1024 {
		t.Fatalf("expected <= %d bytes, got %d bytes", stats.MaxBytes, stats.Bytes)
	}

	// Lookup an early evicted file -> cache miss, reloads cleanly from SQLite
	vol.SQLiteCacheResetStats()
	attr, err := vol.Lookup(ctx, 1, "file_0.txt")
	if err != nil {
		t.Fatalf("Lookup for evicted file_0.txt failed: %v", err)
	}
	if attr.Inode != inodes[0] {
		t.Fatalf("mismatched inode for reloaded file_0.txt: %d vs %d", attr.Inode, inodes[0])
	}
	stats = vol.SQLiteCacheStats()
	if stats.Misses == 0 {
		t.Fatalf("expected at least 1 cache miss for evicted file_0.txt")
	}
}

func TestSQLiteReadCacheConcurrentStress(t *testing.T) {
	ctx := t.Context()
	localDir := t.TempDir()
	backend := inmemorystorage.New()
	volID := "vol-sqlite-concurrent-stress"

	vol := NewVolume(volID, backend, NewEventBroadcaster(),
		WithMetadataStore("sqlite"),
		WithLocalStorageDir(localDir),
	)
	defer vol.Close()

	if err := vol.LoadFromBackend(ctx); err != nil {
		t.Fatalf("LoadFromBackend failed: %v", err)
	}

	// Pre-create initial files and directories
	var initInodes []uint64
	for i := 0; i < 30; i++ {
		attr, err := vol.CreateFile(ctx, 1, fmt.Sprintf("init_%d.txt", i), 0644, []byte("init"), 0, 0)
		if err != nil {
			t.Fatalf("CreateFile failed: %v", err)
		}
		initInodes = append(initInodes, attr.Inode)
	}

	done := make(chan struct{})
	var wg sync.WaitGroup

	// Reader goroutines doing concurrent GetAttr and Lookup
	for r := 0; r < 8; r++ {
		wg.Add(1)
		go func(readerID int) {
			defer wg.Done()
			for {
				select {
				case <-done:
					return
				default:
					idx := readerID % len(initInodes)
					_, _ = vol.GetAttr(ctx, initInodes[idx])
					_, _ = vol.Lookup(ctx, 1, fmt.Sprintf("init_%d.txt", idx))
					// Also lookup non-existent files to stress negative caching concurrently
					_, _ = vol.Lookup(ctx, 1, fmt.Sprintf("missing_%d.txt", readerID))
				}
			}
		}(r)
	}

	// Writer goroutines creating, renaming, and unlinking
	for w := 0; w < 3; w++ {
		wg.Add(1)
		go func(writerID int) {
			defer wg.Done()
			for i := 0; i < 100; i++ {
				name := fmt.Sprintf("w_%d_%d.txt", writerID, i)
				attr, err := vol.CreateFile(ctx, 1, name, 0644, []byte("data"), 0, 0)
				if err == nil {
					renamed := fmt.Sprintf("w_%d_%d_renamed.txt", writerID, i)
					_, _ = vol.Rename(ctx, 1, name, 1, renamed)
					_, _ = vol.GetAttr(ctx, attr.Inode)
					_ = vol.Unlink(ctx, 1, renamed)
				}
			}
		}(w)
	}

	// Let readers and writers run
	time.Sleep(300 * time.Millisecond)
	close(done)
	wg.Wait()

	stats := vol.SQLiteCacheStats()
	t.Logf("Concurrent stress test completed: Hits=%d Misses=%d HitRate=%.2f%% Entries=%d Bytes=%d KB",
		stats.Hits, stats.Misses, stats.HitRate*100, stats.Entries, stats.Bytes/1024)
}
