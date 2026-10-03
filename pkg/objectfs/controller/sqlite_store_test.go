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
	"testing"

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
