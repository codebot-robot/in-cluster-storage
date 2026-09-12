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
	"encoding/json"
	"testing"
	"time"

	pb "github.com/gke-labs/in-cluster-storage/pkg/api/objectfs/v1alpha1"
)

func TestControllerServiceOperations(t *testing.T) {
	ctx := t.Context()
	backend := NewMemoryBackend()
	server := NewServer(backend)
	volumeID := "test-vol-1"

	// 1. Root attribute
	rootAttr, err := server.GetAttr(ctx, &pb.GetAttrRequest{
		VolumeId: volumeID,
		Path:     "/",
	})
	if err != nil {
		t.Fatalf("Failed to get root attr: %v", err)
	}
	if !rootAttr.Attr.IsDir {
		t.Fatalf("Expected root to be directory")
	}

	// 2. Mkdir
	mkdirResp, err := server.Mkdir(ctx, &pb.MkdirRequest{
		VolumeId: volumeID,
		Path:     "/subdir",
		Mode:     0755,
	})
	if err != nil {
		t.Fatalf("Failed to mkdir /subdir: %v", err)
	}
	if !mkdirResp.Attr.IsDir || mkdirResp.Attr.Name != "subdir" {
		t.Fatalf("Unexpected mkdir attr: %v", mkdirResp.Attr)
	}

	// 3. Create file
	createResp, err := server.CreateFile(ctx, &pb.CreateFileRequest{
		VolumeId:       volumeID,
		Path:           "/subdir/hello.txt",
		Mode:           0644,
		InitialContent: []byte("initial content"),
	})
	if err != nil {
		t.Fatalf("Failed to create file: %v", err)
	}
	if createResp.Attr.Size != int64(len("initial content")) {
		t.Fatalf("Unexpected file size: %d", createResp.Attr.Size)
	}

	// 4. Lookup
	lookupResp, err := server.Lookup(ctx, &pb.LookupRequest{
		VolumeId:   volumeID,
		ParentPath: "/subdir",
		Name:       "hello.txt",
	})
	if err != nil {
		t.Fatalf("Failed to lookup: %v", err)
	}
	if lookupResp.Attr.Path != "/subdir/hello.txt" {
		t.Fatalf("Unexpected path in lookup: %s", lookupResp.Attr.Path)
	}

	// 5. Read file
	readResp, err := server.ReadFile(ctx, &pb.ReadFileRequest{
		VolumeId: volumeID,
		Path:     "/subdir/hello.txt",
		Offset:   0,
		Size:     1024,
	})
	if err != nil {
		t.Fatalf("Failed to read file: %v", err)
	}
	if string(readResp.Data) != "initial content" {
		t.Fatalf("Unexpected data: %s", string(readResp.Data))
	}

	// 6. Write file
	writeResp, err := server.WriteFile(ctx, &pb.WriteFileRequest{
		VolumeId:  volumeID,
		Path:      "/subdir/hello.txt",
		Offset:    int64(len("initial ")),
		Data:      []byte("objectfs!"),
		WriteMode: pb.WriteMode_WRITE_THROUGH_FSYNC,
	})
	if err != nil {
		t.Fatalf("Failed to write file: %v", err)
	}
	if writeResp.NewSize != int64(len("initial objectfs!")) {
		t.Fatalf("Unexpected new size: %d", writeResp.NewSize)
	}

	// Read back modified
	readResp2, err := server.ReadFile(ctx, &pb.ReadFileRequest{
		VolumeId: volumeID,
		Path:     "/subdir/hello.txt",
		Offset:   0,
		Size:     1024,
	})
	if err != nil {
		t.Fatalf("Failed to read modified file: %v", err)
	}
	if string(readResp2.Data) != "initial objectfs!" {
		t.Fatalf("Unexpected content after write: %s", string(readResp2.Data))
	}

	// 7. ReadDir
	readdirResp, err := server.ReadDir(ctx, &pb.ReadDirRequest{
		VolumeId: volumeID,
		Path:     "/subdir",
	})
	if err != nil {
		t.Fatalf("Failed to readdir: %v", err)
	}
	if len(readdirResp.Entries) != 1 || readdirResp.Entries[0].Name != "hello.txt" {
		t.Fatalf("Unexpected readdir entries: %v", readdirResp.Entries)
	}

	// 8. Rename
	renameResp, err := server.Rename(ctx, &pb.RenameRequest{
		VolumeId: volumeID,
		OldPath:  "/subdir/hello.txt",
		NewPath:  "/subdir/renamed.txt",
	})
	if err != nil {
		t.Fatalf("Failed to rename: %v", err)
	}
	if renameResp.Attr.Name != "renamed.txt" {
		t.Fatalf("Unexpected rename attr: %v", renameResp.Attr)
	}

	// Verify old path not found
	_, err = server.GetAttr(ctx, &pb.GetAttrRequest{
		VolumeId: volumeID,
		Path:     "/subdir/hello.txt",
	})
	if err == nil {
		t.Fatalf("Expected old path to not exist after rename")
	}

	// 9. Truncate
	truncResp, err := server.TruncateFile(ctx, &pb.TruncateFileRequest{
		VolumeId: volumeID,
		Path:     "/subdir/renamed.txt",
		Size:     7,
	})
	if err != nil {
		t.Fatalf("Failed to truncate: %v", err)
	}
	if truncResp.Attr.Size != 7 {
		t.Fatalf("Expected size 7 after truncate, got %d", truncResp.Attr.Size)
	}

	// 10. Unlink & Rmdir
	_, err = server.Unlink(ctx, &pb.UnlinkRequest{
		VolumeId: volumeID,
		Path:     "/subdir/renamed.txt",
	})
	if err != nil {
		t.Fatalf("Failed to unlink: %v", err)
	}

	_, err = server.Rmdir(ctx, &pb.RmdirRequest{
		VolumeId: volumeID,
		Path:     "/subdir",
	})
	if err != nil {
		t.Fatalf("Failed to rmdir: %v", err)
	}

	// Verify subdir gone
	_, err = server.GetAttr(ctx, &pb.GetAttrRequest{
		VolumeId: volumeID,
		Path:     "/subdir",
	})
	if err == nil {
		t.Fatalf("Expected subdir to not exist after rmdir")
	}
}

func TestBackendPeriodicAndIncrementalFlush(t *testing.T) {
	ctx := t.Context()
	backend := NewMemoryBackend()
	server := NewServer(backend)
	volumeID := "test-flush-vol"

	// Create files
	_, err := server.CreateFile(ctx, &pb.CreateFileRequest{
		VolumeId:       volumeID,
		Path:           "/file1.txt",
		Mode:           0644,
		InitialContent: []byte("file 1 initial data"),
	})
	if err != nil {
		t.Fatalf("Failed to create file1: %v", err)
	}

	_, err = server.CreateFile(ctx, &pb.CreateFileRequest{
		VolumeId:       volumeID,
		Path:           "/file2.txt",
		Mode:           0644,
		InitialContent: []byte("file 2 initial data"),
	})
	if err != nil {
		t.Fatalf("Failed to create file2: %v", err)
	}

	// Before flush, backend should not have the raw objects
	if _, err := backend.GetObject(ctx, volumeID, "file1.txt", 0, 0); err == nil {
		t.Fatalf("Expected backend to not have file1 before flush")
	}

	// Flush to backend
	if err := server.FlushAll(ctx); err != nil {
		t.Fatalf("FlushAll failed: %v", err)
	}

	// Verify raw objects and metadata file exist in backend
	f1Data, err := backend.GetObject(ctx, volumeID, "file1.txt", 0, 100)
	if err != nil || string(f1Data) != "file 1 initial data" {
		t.Fatalf("Expected file1 in backend with initial data, got: %q, err: %v", string(f1Data), err)
	}

	metaBytes, err := backend.GetObject(ctx, volumeID, MetadataFileName, 0, 0)
	if err != nil || len(metaBytes) == 0 {
		t.Fatalf("Expected metadata file in backend, got err: %v", err)
	}

	var meta VolumeMetadata
	if err := json.Unmarshal(metaBytes, &meta); err != nil {
		t.Fatalf("Failed to unmarshal metadata: %v", err)
	}
	if len(meta.Entries) != 3 { // root /, /file1.txt, /file2.txt
		t.Fatalf("Expected 3 entries in metadata, got %d", len(meta.Entries))
	}
	if meta.Entries["/file1.txt"].Size != int64(len("file 1 initial data")) {
		t.Fatalf("Unexpected file1 metadata size: %d", meta.Entries["/file1.txt"].Size)
	}

	// Incremental write: modify only file2
	_, err = server.WriteFile(ctx, &pb.WriteFileRequest{
		VolumeId: volumeID,
		Path:     "/file2.txt",
		Offset:   0,
		Data:     []byte("file 2 updated content!"),
	})
	if err != nil {
		t.Fatalf("Failed to update file2: %v", err)
	}

	// Unlink file1
	_, err = server.Unlink(ctx, &pb.UnlinkRequest{
		VolumeId: volumeID,
		Path:     "/file1.txt",
	})
	if err != nil {
		t.Fatalf("Failed to unlink file1: %v", err)
	}

	// Flush again
	if err := server.FlushAll(ctx); err != nil {
		t.Fatalf("Second FlushAll failed: %v", err)
	}

	// Verify file2 updated and file1 deleted in backend
	f2Data, err := backend.GetObject(ctx, volumeID, "file2.txt", 0, 100)
	if err != nil || string(f2Data) != "file 2 updated content!" {
		t.Fatalf("Expected updated file2 in backend, got: %q, err: %v", string(f2Data), err)
	}

	if _, err := backend.GetObject(ctx, volumeID, "file1.txt", 0, 100); err == nil {
		t.Fatalf("Expected file1 to be deleted from backend after unlink & flush")
	}

	// Test Recovery / LoadFromBackend
	// Create a new server pointing to the same backend
	newServer := NewServer(backend)
	readResp, err := newServer.ReadFile(ctx, &pb.ReadFileRequest{
		VolumeId: volumeID,
		Path:     "/file2.txt",
		Offset:   0,
		Size:     100,
	})
	if err != nil {
		t.Fatalf("Failed to read file2 from recovered server: %v", err)
	}
	if string(readResp.GetData()) != "file 2 updated content!" {
		t.Fatalf("Expected recovered server to read 'file 2 updated content!', got: %q", string(readResp.GetData()))
	}
}

func TestPeriodicFlusherLifecycle(t *testing.T) {
	ctx := t.Context()
	backend := NewMemoryBackend()
	server := NewServer(backend)
	volumeID := "test-periodic-vol"

	server.StartPeriodicFlush(ctx, 10*time.Millisecond)

	_, err := server.CreateFile(ctx, &pb.CreateFileRequest{
		VolumeId:       volumeID,
		Path:           "/auto-flushed.txt",
		Mode:           0644,
		InitialContent: []byte("auto flushed data"),
	})
	if err != nil {
		t.Fatalf("Failed to create file: %v", err)
	}

	// Wait for periodic flusher to run
	time.Sleep(50 * time.Millisecond)

	server.StopPeriodicFlush()

	// Verify backend received the file
	data, err := backend.GetObject(ctx, volumeID, "auto-flushed.txt", 0, 100)
	if err != nil || string(data) != "auto flushed data" {
		t.Fatalf("Expected periodic flusher to sync auto-flushed.txt to backend, got %q (err=%v)", string(data), err)
	}
}

func TestControllerPushNotifications(t *testing.T) {
	ctx := t.Context()
	server := NewServer(nil)
	volumeID := "test-watch-vol"

	ch := server.broadcaster.Subscribe(volumeID)
	defer server.broadcaster.Unsubscribe(volumeID, ch)

	// Trigger create
	_, err := server.CreateFile(ctx, &pb.CreateFileRequest{
		VolumeId:       volumeID,
		Path:           "/event-test.txt",
		Mode:           0644,
		InitialContent: []byte("event data"),
	})
	if err != nil {
		t.Fatalf("Failed to create file: %v", err)
	}

	select {
	case ev := <-ch:
		if ev.EventType != pb.WatchEventType_EVENT_CREATED || ev.Path != "/event-test.txt" {
			t.Fatalf("Unexpected event received: %v", ev)
		}
	case <-time.After(1 * time.Second):
		t.Fatalf("Timed out waiting for push notification")
	}

	// Trigger modify
	_, err = server.WriteFile(ctx, &pb.WriteFileRequest{
		VolumeId: volumeID,
		Path:     "/event-test.txt",
		Offset:   0,
		Data:     []byte("more data"),
	})
	if err != nil {
		t.Fatalf("Failed to write file: %v", err)
	}

	select {
	case ev := <-ch:
		if ev.EventType != pb.WatchEventType_EVENT_MODIFIED || ev.Path != "/event-test.txt" {
			t.Fatalf("Unexpected modify event: %v", ev)
		}
	case <-time.After(1 * time.Second):
		t.Fatalf("Timed out waiting for modify event")
	}
}

func TestEventBroadcasterSlowSubscriber(t *testing.T) {
	eb := NewEventBroadcaster()
	volumeID := "test-slow-sub"

	ch := eb.Subscribe(volumeID)
	defer eb.Unsubscribe(volumeID, ch)

	// Fill the buffer (128 items)
	for i := 0; i < 128; i++ {
		eb.Broadcast(volumeID, &pb.WatchVolumeResponse{
			EventType: pb.WatchEventType_EVENT_MODIFIED,
			Path:      "/file.txt",
		})
	}

	// Next broadcast should detect full channel and unsubscribe/close it asynchronously
	eb.Broadcast(volumeID, &pb.WatchVolumeResponse{
		EventType: pb.WatchEventType_EVENT_MODIFIED,
		Path:      "/overflow.txt",
	})

	// Drain items from ch until closed
	closed := false
	timeout := time.After(2 * time.Second)
	for !closed {
		select {
		case _, ok := <-ch:
			if !ok {
				closed = true
			}
		case <-timeout:
			t.Fatalf("Timed out waiting for full subscriber channel to be closed")
		}
	}
}
