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
