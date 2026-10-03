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
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"net"
	"path"
	"path/filepath"
	"slices"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/container-storage-interface/spec/lib/go/csi"
	pb "github.com/gke-labs/in-cluster-storage/pkg/api/objectfs/v1alpha1"
	sdsv1 "github.com/gke-labs/in-cluster-storage/pkg/api/sds/v1"
	walpb "github.com/gke-labs/in-cluster-storage/pkg/api/wal/v1alpha1"
	"github.com/gke-labs/in-cluster-storage/pkg/erofs"
	"github.com/gke-labs/in-cluster-storage/pkg/sds"
	"github.com/gke-labs/in-cluster-storage/pkg/sds/record"
	"github.com/gke-labs/in-cluster-storage/pkg/wal"
	walbuffer "github.com/gke-labs/in-cluster-storage/pkg/wal/buffer"
	walclient "github.com/gke-labs/in-cluster-storage/pkg/wal/client"
	"google.golang.org/grpc"
)

func resolvePath(ctx context.Context, server *Server, volumeID, p string) (uint64, error) {
	vol, err := server.getOrCreateVolume(volumeID)
	if err != nil {
		return 0, err
	}
	return vol.ResolvePath(ctx, p)
}

func testGetAttr(ctx context.Context, server *Server, volumeID, p string) (*pb.GetAttrResponse, error) {
	ino, err := resolvePath(ctx, server, volumeID, p)
	if err != nil {
		return &pb.GetAttrResponse{Error: volErrToSyscall(err)}, nil
	}
	return server.GetAttr(ctx, &pb.GetAttrRequest{VolumeId: volumeID, Inode: ino})
}

func testReadFile(ctx context.Context, server *Server, volumeID, p string, offset, size int64) (*pb.ReadFileResponse, error) {
	ino, err := resolvePath(ctx, server, volumeID, p)
	if err != nil {
		return &pb.ReadFileResponse{Error: volErrToSyscall(err)}, nil
	}
	return server.ReadFile(ctx, &pb.ReadFileRequest{VolumeId: volumeID, Inode: ino, Offset: offset, Size: size})
}

func testWriteFile(ctx context.Context, server *Server, volumeID, p string, offset int64, data []byte, mode pb.WriteMode) (*pb.WriteFileResponse, error) {
	ino, err := resolvePath(ctx, server, volumeID, p)
	if err != nil {
		return &pb.WriteFileResponse{Error: volErrToSyscall(err)}, nil
	}
	return server.WriteFile(ctx, &pb.WriteFileRequest{VolumeId: volumeID, Inode: ino, Offset: offset, Data: data, WriteMode: mode})
}

func testTruncateFile(ctx context.Context, server *Server, volumeID, p string, size int64) (*pb.TruncateFileResponse, error) {
	ino, err := resolvePath(ctx, server, volumeID, p)
	if err != nil {
		return &pb.TruncateFileResponse{Error: volErrToSyscall(err)}, nil
	}
	return server.TruncateFile(ctx, &pb.TruncateFileRequest{VolumeId: volumeID, Inode: ino, Size: size})
}

func testReadDir(ctx context.Context, server *Server, volumeID, p string) (*pb.ReadDirResponse, error) {
	ino, err := resolvePath(ctx, server, volumeID, p)
	if err != nil {
		return &pb.ReadDirResponse{Error: volErrToSyscall(err)}, nil
	}
	return server.ReadDir(ctx, &pb.ReadDirRequest{VolumeId: volumeID, Inode: ino})
}

func testMkdir(ctx context.Context, server *Server, volumeID, p string, mode uint32, uid, gid uint32) (*pb.MkdirResponse, error) {
	p = cleanPath(p)
	parentPath := path.Dir(p)
	name := path.Base(p)
	parentIno, err := resolvePath(ctx, server, volumeID, parentPath)
	if err != nil {
		return &pb.MkdirResponse{Error: volErrToSyscall(err)}, nil
	}
	return server.Mkdir(ctx, &pb.MkdirRequest{VolumeId: volumeID, ParentInode: parentIno, Name: name, Mode: mode, Uid: uid, Gid: gid})
}

func testCreateFile(ctx context.Context, server *Server, volumeID, p string, mode uint32, initialContent []byte, uid, gid uint32) (*pb.CreateFileResponse, error) {
	p = cleanPath(p)
	parentPath := path.Dir(p)
	name := path.Base(p)
	parentIno, err := resolvePath(ctx, server, volumeID, parentPath)
	if err != nil {
		return &pb.CreateFileResponse{Error: volErrToSyscall(err)}, nil
	}
	return server.CreateFile(ctx, &pb.CreateFileRequest{VolumeId: volumeID, ParentInode: parentIno, Name: name, Mode: mode, InitialContent: initialContent, Uid: uid, Gid: gid})
}

func testUnlink(ctx context.Context, server *Server, volumeID, p string) (*pb.UnlinkResponse, error) {
	p = cleanPath(p)
	parentPath := path.Dir(p)
	name := path.Base(p)
	parentIno, err := resolvePath(ctx, server, volumeID, parentPath)
	if err != nil {
		return &pb.UnlinkResponse{Success: false, Error: volErrToSyscall(err)}, nil
	}
	return server.Unlink(ctx, &pb.UnlinkRequest{VolumeId: volumeID, ParentInode: parentIno, Name: name})
}

func testRmdir(ctx context.Context, server *Server, volumeID, p string) (*pb.RmdirResponse, error) {
	p = cleanPath(p)
	parentPath := path.Dir(p)
	name := path.Base(p)
	parentIno, err := resolvePath(ctx, server, volumeID, parentPath)
	if err != nil {
		return &pb.RmdirResponse{Success: false, Error: volErrToSyscall(err)}, nil
	}
	return server.Rmdir(ctx, &pb.RmdirRequest{VolumeId: volumeID, ParentInode: parentIno, Name: name})
}

func testRename(ctx context.Context, server *Server, volumeID, oldP, newP string) (*pb.RenameResponse, error) {
	oldP = cleanPath(oldP)
	newP = cleanPath(newP)
	oldParentIno, err := resolvePath(ctx, server, volumeID, path.Dir(oldP))
	if err != nil {
		return &pb.RenameResponse{Error: volErrToSyscall(err)}, nil
	}
	newParentIno, err := resolvePath(ctx, server, volumeID, path.Dir(newP))
	if err != nil {
		return &pb.RenameResponse{Error: volErrToSyscall(err)}, nil
	}
	return server.Rename(ctx, &pb.RenameRequest{
		VolumeId:       volumeID,
		OldParentInode: oldParentIno,
		OldName:        path.Base(oldP),
		NewParentInode: newParentIno,
		NewName:        path.Base(newP),
	})
}

func volGetAttr(ctx context.Context, vol *Volume, p string) (*pb.EntryAttr, error) {
	ino, err := vol.ResolvePath(ctx, p)
	if err != nil {
		return nil, err
	}
	return vol.GetAttr(ctx, ino)
}

func volReadFile(ctx context.Context, vol *Volume, p string, offset, length int64) ([]byte, int64, string, error) {
	ino, err := vol.ResolvePath(ctx, p)
	if err != nil {
		return nil, 0, "", err
	}
	return vol.ReadFile(ctx, ino, offset, length)
}

func volWriteFile(ctx context.Context, vol *Volume, p string, offset int64, data []byte, mode pb.WriteMode) (int64, int64, time.Time, error) {
	ino, err := vol.ResolvePath(ctx, p)
	if err != nil {
		return 0, 0, time.Time{}, err
	}
	return vol.WriteFile(ctx, ino, offset, data, mode)
}

func volTruncateFile(ctx context.Context, vol *Volume, p string, size int64) (*pb.EntryAttr, error) {
	ino, err := vol.ResolvePath(ctx, p)
	if err != nil {
		return nil, err
	}
	return vol.TruncateFile(ctx, ino, size)
}

func volCreateFile(ctx context.Context, vol *Volume, p string, mode uint32, initialContent []byte, uid, gid uint32) (*pb.EntryAttr, error) {
	p = cleanPath(p)
	parentIno, err := vol.ResolvePath(ctx, path.Dir(p))
	if err != nil {
		return nil, err
	}
	return vol.CreateFile(ctx, parentIno, path.Base(p), mode, initialContent, uid, gid)
}

func TestControllerServiceOperations(t *testing.T) {
	ctx := t.Context()
	backend := NewMemoryBackend()
	server := NewServer(backend)
	volumeID := "test-vol-1"

	// 1. Root attribute
	rootAttr, err := server.GetAttr(ctx, &pb.GetAttrRequest{
		VolumeId: volumeID,
		Inode:    1,
	})
	if err != nil {
		t.Fatalf("Failed to get root attr: %v", err)
	}
	if !rootAttr.Attr.IsDir {
		t.Fatalf("Expected root to be directory")
	}

	// 2. Mkdir
	mkdirResp, err := server.Mkdir(ctx, &pb.MkdirRequest{
		VolumeId:    volumeID,
		ParentInode: 1,
		Name:        "subdir",
		Mode:        0755,
		Uid:         1001,
		Gid:         1002,
	})
	if err != nil {
		t.Fatalf("Failed to mkdir /subdir: %v", err)
	}
	if !mkdirResp.Attr.IsDir || mkdirResp.Attr.Name != "subdir" {
		t.Fatalf("Unexpected mkdir attr: %v", mkdirResp.Attr)
	}
	if mkdirResp.Attr.Uid != 1001 || mkdirResp.Attr.Gid != 1002 {
		t.Fatalf("Unexpected mkdir owner: uid=%d, gid=%d", mkdirResp.Attr.Uid, mkdirResp.Attr.Gid)
	}
	subdirIno := mkdirResp.Attr.Inode

	// 3. Create file
	createResp, err := server.CreateFile(ctx, &pb.CreateFileRequest{
		VolumeId:       volumeID,
		ParentInode:    subdirIno,
		Name:           "hello.txt",
		Mode:           0644,
		InitialContent: []byte("initial content"),
		Uid:            5001,
		Gid:            5002,
	})
	if err != nil {
		t.Fatalf("Failed to create file: %v", err)
	}
	if createResp.Attr.Size != int64(len("initial content")) {
		t.Fatalf("Unexpected file size: %d", createResp.Attr.Size)
	}
	if createResp.Attr.Uid != 5001 || createResp.Attr.Gid != 5002 {
		t.Fatalf("Unexpected file owner: uid=%d, gid=%d", createResp.Attr.Uid, createResp.Attr.Gid)
	}
	fileIno := createResp.Attr.Inode

	// 4. Lookup
	lookupResp, err := server.Lookup(ctx, &pb.LookupRequest{
		VolumeId:    volumeID,
		ParentInode: subdirIno,
		Name:        "hello.txt",
	})
	if err != nil {
		t.Fatalf("Failed to lookup: %v", err)
	}
	if lookupResp.Attr.Name != "hello.txt" || lookupResp.Attr.Inode != fileIno {
		t.Fatalf("Unexpected attr in lookup: %v", lookupResp.Attr)
	}
	if lookupResp.Attr.Uid != 5001 || lookupResp.Attr.Gid != 5002 {
		t.Fatalf("Unexpected owner in lookup: uid=%d, gid=%d", lookupResp.Attr.Uid, lookupResp.Attr.Gid)
	}

	// 5. Read file
	readResp, err := server.ReadFile(ctx, &pb.ReadFileRequest{
		VolumeId: volumeID,
		Inode:    fileIno,
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
		Inode:     fileIno,
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
		Inode:    fileIno,
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
		Inode:    subdirIno,
	})
	if err != nil {
		t.Fatalf("Failed to readdir: %v", err)
	}
	if len(readdirResp.Entries) != 1 || readdirResp.Entries[0].Name != "hello.txt" {
		t.Fatalf("Unexpected readdir entries: %v", readdirResp.Entries)
	}

	// 8. Rename
	renameResp, err := server.Rename(ctx, &pb.RenameRequest{
		VolumeId:       volumeID,
		OldParentInode: subdirIno,
		OldName:        "hello.txt",
		NewParentInode: subdirIno,
		NewName:        "renamed.txt",
	})
	if err != nil {
		t.Fatalf("Failed to rename: %v", err)
	}
	if renameResp.Attr.Name != "renamed.txt" {
		t.Fatalf("Unexpected rename attr: %v", renameResp.Attr)
	}

	// Verify old name not found in lookup
	oldResp, err := server.Lookup(ctx, &pb.LookupRequest{
		VolumeId:    volumeID,
		ParentInode: subdirIno,
		Name:        "hello.txt",
	})
	if err != nil {
		t.Fatalf("Lookup failed: %v", err)
	}
	if oldResp.GetError() != int32(syscall.ENOENT) {
		t.Fatalf("Expected old name to not exist after rename, got error %d", oldResp.GetError())
	}

	// 9. Truncate
	truncResp, err := server.TruncateFile(ctx, &pb.TruncateFileRequest{
		VolumeId: volumeID,
		Inode:    fileIno,
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
		VolumeId:    volumeID,
		ParentInode: subdirIno,
		Name:        "renamed.txt",
	})
	if err != nil {
		t.Fatalf("Failed to unlink: %v", err)
	}

	_, err = server.Rmdir(ctx, &pb.RmdirRequest{
		VolumeId:    volumeID,
		ParentInode: 1,
		Name:        "subdir",
	})
	if err != nil {
		t.Fatalf("Failed to rmdir: %v", err)
	}

	// Verify subdir gone
	goneResp, err := server.Lookup(ctx, &pb.LookupRequest{
		VolumeId:    volumeID,
		ParentInode: 1,
		Name:        "subdir",
	})
	if err != nil {
		t.Fatalf("Lookup failed: %v", err)
	}
	if goneResp.GetError() != int32(syscall.ENOENT) {
		t.Fatalf("Expected subdir to not exist after rmdir, got error %d", goneResp.GetError())
	}
}

func TestServerRmdirAndUnlinkErrorCodes(t *testing.T) {
	ctx := t.Context()
	backend := NewMemoryBackend()
	server := NewServer(backend)
	volumeID := "test-error-codes-vol"

	// Create /parent/child.txt
	mkdirResp, err := testMkdir(ctx, server, volumeID, "/parent", 0755, 0, 0)
	if err != nil || mkdirResp.GetError() != 0 {
		t.Fatalf("Mkdir failed: err=%v, resp=%v", err, mkdirResp)
	}

	createResp, err := testCreateFile(ctx, server, volumeID, "/parent/child.txt", 0644, nil, 0, 0)
	if err != nil || createResp.GetError() != 0 {
		t.Fatalf("CreateFile failed: err=%v, resp=%v", err, createResp)
	}

	// 1. Rmdir non-empty directory -> returns non-error gRPC response with error = ENOTEMPTY
	rmdirResp, err := testRmdir(ctx, server, volumeID, "/parent")
	if err != nil {
		t.Fatalf("Expected non-error gRPC response, got err: %v", err)
	}
	if rmdirResp.GetError() != int32(syscall.ENOTEMPTY) {
		t.Fatalf("Expected error code ENOTEMPTY (%d), got %d", syscall.ENOTEMPTY, rmdirResp.GetError())
	}
	if rmdirResp.GetSuccess() {
		t.Fatalf("Expected success to be false for non-empty rmdir")
	}

	// 2. Unlink directory -> returns non-error gRPC response with error = EISDIR
	unlinkResp, err := testUnlink(ctx, server, volumeID, "/parent")
	if err != nil {
		t.Fatalf("Expected non-error gRPC response, got err: %v", err)
	}
	if unlinkResp.GetError() != int32(syscall.EISDIR) {
		t.Fatalf("Expected error code EISDIR (%d), got %d", syscall.EISDIR, unlinkResp.GetError())
	}
	if unlinkResp.GetSuccess() {
		t.Fatalf("Expected success to be false for unlinking directory")
	}

	// 3. Rmdir regular file -> returns non-error gRPC response with error = ENOTDIR
	rmdirFileResp, err := testRmdir(ctx, server, volumeID, "/parent/child.txt")
	if err != nil {
		t.Fatalf("Expected non-error gRPC response, got err: %v", err)
	}
	if rmdirFileResp.GetError() != int32(syscall.ENOTDIR) {
		t.Fatalf("Expected error code ENOTDIR (%d), got %d", syscall.ENOTDIR, rmdirFileResp.GetError())
	}

	// 4. Rmdir nonexistent -> returns non-error gRPC response with error = ENOENT
	rmdirNoneResp, err := testRmdir(ctx, server, volumeID, "/nonexistent")
	if err != nil {
		t.Fatalf("Expected non-error gRPC response, got err: %v", err)
	}
	if rmdirNoneResp.GetError() != int32(syscall.ENOENT) {
		t.Fatalf("Expected error code ENOENT (%d), got %d", syscall.ENOENT, rmdirNoneResp.GetError())
	}

	// 5. Mkdir already exists -> returns non-error gRPC response with error = EEXIST
	mkdirExistResp, err := testMkdir(ctx, server, volumeID, "/parent", 0755, 0, 0)
	if err != nil {
		t.Fatalf("Expected non-error gRPC response, got err: %v", err)
	}
	if mkdirExistResp.GetError() != int32(syscall.EEXIST) {
		t.Fatalf("Expected error code EEXIST (%d), got %d", syscall.EEXIST, mkdirExistResp.GetError())
	}
}

func TestBackendPeriodicAndIncrementalFlush(t *testing.T) {
	ctx := t.Context()
	backend := NewMemoryBackend()
	server := NewServer(backend)
	volumeID := "test-flush-vol"

	// Create files
	_, err := testCreateFile(ctx, server, volumeID, "/file1.txt", 0644, []byte("file 1 initial data"), 0, 0)
	if err != nil {
		t.Fatalf("Failed to create file1: %v", err)
	}

	_, err = testCreateFile(ctx, server, volumeID, "/file2.txt", 0644, []byte("file 2 initial data"), 0, 0)
	if err != nil {
		t.Fatalf("Failed to create file2: %v", err)
	}

	// Before flush, backend should not have the raw objects
	var dummyBuf bytes.Buffer
	if err := backend.GetObject(ctx, volumeID, "file1.txt", 0, 0, &dummyBuf); err == nil {
		t.Fatalf("Expected backend to not have file1 before flush")
	}

	// Flush to backend
	if err := server.FlushAll(ctx); err != nil {
		t.Fatalf("FlushAll failed: %v", err)
	}

	// Verify raw objects and metadata file exist in backend
	var f1Buf bytes.Buffer
	err = backend.GetObject(ctx, volumeID, "file1.txt", 0, 100, &f1Buf)
	f1Data := f1Buf.Bytes()
	if err != nil || string(f1Data) != "file 1 initial data" {
		t.Fatalf("Expected file1 in backend with initial data, got: %q, err: %v", string(f1Data), err)
	}

	var metaBuf bytes.Buffer
	err = backend.GetObject(ctx, volumeID, MetadataFileName, 0, 0, &metaBuf)
	metaBytes := metaBuf.Bytes()
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
	_, err = testWriteFile(ctx, server, volumeID, "/file2.txt", 0, []byte("file 2 updated content!"), pb.WriteMode_WRITE_MODE_UNSPECIFIED)
	if err != nil {
		t.Fatalf("Failed to update file2: %v", err)
	}

	// Unlink file1
	_, err = testUnlink(ctx, server, volumeID, "/file1.txt")
	if err != nil {
		t.Fatalf("Failed to unlink file1: %v", err)
	}

	// Flush again
	if err := server.FlushAll(ctx); err != nil {
		t.Fatalf("Second FlushAll failed: %v", err)
	}

	// Verify file2 updated and file1 deleted in backend
	var f2Buf bytes.Buffer
	err = backend.GetObject(ctx, volumeID, "file2.txt", 0, 100, &f2Buf)
	f2Data := f2Buf.Bytes()
	if err != nil || string(f2Data) != "file 2 updated content!" {
		t.Fatalf("Expected updated file2 in backend, got: %q, err: %v", string(f2Data), err)
	}

	var f1DeletedBuf bytes.Buffer
	if err := backend.GetObject(ctx, volumeID, "file1.txt", 0, 100, &f1DeletedBuf); err == nil {
		t.Fatalf("Expected file1 to be deleted from backend after unlink & flush")
	}

	// Test Recovery / LoadFromBackend
	// Create a new server pointing to the same backend
	newServer := NewServer(backend)
	readResp, err := testReadFile(ctx, newServer, volumeID, "/file2.txt", 0, 100)
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

	_, err := testCreateFile(ctx, server, volumeID, "/auto-flushed.txt", 0644, []byte("auto flushed data"), 0, 0)
	if err != nil {
		t.Fatalf("Failed to create file: %v", err)
	}

	// Wait for periodic flusher to run
	time.Sleep(50 * time.Millisecond)

	server.StopPeriodicFlush()

	// Verify backend received the file
	var autoBuf bytes.Buffer
	err = backend.GetObject(ctx, volumeID, "auto-flushed.txt", 0, 100, &autoBuf)
	data := autoBuf.Bytes()
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
	_, err := testCreateFile(ctx, server, volumeID, "/event-test.txt", 0644, []byte("event data"), 0, 0)
	if err != nil {
		t.Fatalf("Failed to create file: %v", err)
	}

	select {
	case ev := <-ch:
		if ev.EventType != pb.WatchEventType_EVENT_CREATED || ev.GetInode() == 0 {
			t.Fatalf("Unexpected event received: %v", ev)
		}
	case <-time.After(1 * time.Second):
		t.Fatalf("Timed out waiting for push notification")
	}

	// Trigger modify
	_, err = testWriteFile(ctx, server, volumeID, "/event-test.txt", 0, []byte("more data"), pb.WriteMode_WRITE_MODE_UNSPECIFIED)
	if err != nil {
		t.Fatalf("Failed to write file: %v", err)
	}

	select {
	case ev := <-ch:
		if ev.EventType != pb.WatchEventType_EVENT_MODIFIED || ev.GetInode() == 0 {
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
			Inode:     uint64(i + 1),
			Name:      "file.txt",
		})
	}

	// Next broadcast should detect full channel and unsubscribe/close it asynchronously
	eb.Broadcast(volumeID, &pb.WatchVolumeResponse{
		EventType: pb.WatchEventType_EVENT_MODIFIED,
		Inode:     999,
		Name:      "overflow.txt",
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

func TestErofsSnapshotCreationAndRecovery(t *testing.T) {
	ctx := t.Context()
	backend := NewMemoryBackend()
	server := NewServer(backend)
	volumeID := "test-erofs-snap-vol"

	// Create a nested directory hierarchy and files
	_, err := testMkdir(ctx, server, volumeID, "/data", 0755, 0, 0)
	if err != nil {
		t.Fatalf("Mkdir /data failed: %v", err)
	}

	_, err = testCreateFile(ctx, server, volumeID, "/data/file1.txt", 0644, []byte("file 1 content for snapshot"), 0, 0)
	if err != nil {
		t.Fatalf("CreateFile failed: %v", err)
	}

	_, err = testCreateFile(ctx, server, volumeID, "/root-file.txt", 0644, []byte("root file content"), 0, 0)
	if err != nil {
		t.Fatalf("CreateFile failed: %v", err)
	}

	// Create snapshot
	snapResp, err := server.CreateSnapshot(ctx, &pb.CreateSnapshotRequest{
		VolumeId: volumeID,
	})
	if err != nil {
		t.Fatalf("CreateSnapshot failed: %v", err)
	}
	snapName := snapResp.GetSnapshotName()
	if snapName == "" || !strings.HasSuffix(snapName, ".erofs") {
		t.Fatalf("Expected .erofs snapshot name, got: %s", snapName)
	}

	// Verify backend object layout:
	// 1. volumes/<volumeID>/meta/<snapName> exists
	snapKey := "volumes/" + volumeID + "/meta/" + snapName
	var snapBuf bytes.Buffer
	err = backend.GetObject(ctx, "", snapKey, 0, 0, &snapBuf)
	snapBytes := snapBuf.Bytes()
	if err != nil || len(snapBytes) == 0 {
		t.Fatalf("Expected EROFS snapshot in backend at %s: %v", snapKey, err)
	}

	// 2. Blobs exist in blobs/ (packfiles or standalone)
	blobObjects, err := backend.ListObjects(ctx, "", "blobs/")
	if err != nil || len(blobObjects) == 0 {
		t.Fatalf("Expected blobs in backend under blobs/, got: %v (err=%v)", blobObjects, err)
	}

	// 3. List snapshots returns the snapshot
	listResp, err := server.ListSnapshots(ctx, &pb.ListSnapshotsRequest{
		VolumeId: volumeID,
	})
	if err != nil {
		t.Fatalf("ListSnapshots failed: %v", err)
	}
	if len(listResp.GetSnapshots()) != 1 || listResp.GetSnapshots()[0].GetName() != snapName {
		t.Fatalf("Expected snapshots [%s], got %v", snapName, listResp.GetSnapshots())
	}

	// 4. Test Recovery on a new Server instance using only the backend
	newServer := NewServer(backend)

	// Verify directory structure on recovered server
	dirResp, err := testReadDir(ctx, newServer, volumeID, "/data")
	if err != nil {
		t.Fatalf("Recovered server ReadDir /data failed: %v", err)
	}
	if len(dirResp.Entries) != 1 || dirResp.Entries[0].Name != "file1.txt" {
		t.Fatalf("Unexpected entries in recovered /data: %v", dirResp.Entries)
	}

	// Read content from recovered server (verifying lazy blob download)
	readResp, err := testReadFile(ctx, newServer, volumeID, "/data/file1.txt", 0, 1024)
	if err != nil {
		t.Fatalf("Recovered server ReadFile failed: %v", err)
	}
	if string(readResp.Data) != "file 1 content for snapshot" {
		t.Fatalf("Recovered data mismatch: got %q", string(readResp.Data))
	}

	readRootResp, err := testReadFile(ctx, newServer, volumeID, "/root-file.txt", 0, 1024)
	if err != nil {
		t.Fatalf("Recovered server ReadFile root-file failed: %v", err)
	}
	if string(readRootResp.Data) != "root file content" {
		t.Fatalf("Recovered root data mismatch: got %q", string(readRootResp.Data))
	}
}

func TestErofsSnapshotRollback(t *testing.T) {
	ctx := t.Context()
	backend := NewMemoryBackend()
	server := NewServer(backend)
	volumeID := "test-rollback-vol"

	// State 1: create v1
	_, err := testCreateFile(ctx, server, volumeID, "/doc.txt", 0644, []byte("version 1 data"), 0, 0)
	if err != nil {
		t.Fatalf("CreateFile failed: %v", err)
	}

	snapResp1, err := server.CreateSnapshot(ctx, &pb.CreateSnapshotRequest{
		VolumeId: volumeID,
	})
	if err != nil {
		t.Fatalf("CreateSnapshot 1 failed: %v", err)
	}
	snap1 := snapResp1.GetSnapshotName()

	time.Sleep(10 * time.Millisecond)

	// State 2: modify doc.txt and add doc2.txt
	_, err = testWriteFile(ctx, server, volumeID, "/doc.txt", 0, []byte("version 2 data overwritten"), pb.WriteMode_WRITE_MODE_UNSPECIFIED)
	if err != nil {
		t.Fatalf("WriteFile failed: %v", err)
	}

	_, err = testCreateFile(ctx, server, volumeID, "/doc2.txt", 0644, []byte("version 2 second document"), 0, 0)
	if err != nil {
		t.Fatalf("CreateFile doc2 failed: %v", err)
	}

	snapResp2, err := server.CreateSnapshot(ctx, &pb.CreateSnapshotRequest{
		VolumeId: volumeID,
	})
	if err != nil {
		t.Fatalf("CreateSnapshot 2 failed: %v", err)
	}
	snap2 := snapResp2.GetSnapshotName()

	// Verify 2 snapshots listed
	listResp, err := server.ListSnapshots(ctx, &pb.ListSnapshotsRequest{
		VolumeId: volumeID,
	})
	if err != nil {
		t.Fatalf("ListSnapshots failed: %v", err)
	}
	if len(listResp.GetSnapshots()) < 2 {
		t.Fatalf("Expected at least 2 snapshots, got %d", len(listResp.GetSnapshots()))
	}

	// Roll back to snap1
	if err := server.RestoreSnapshot(ctx, volumeID, snap1); err != nil {
		t.Fatalf("RestoreSnapshot to %s failed: %v", snap1, err)
	}

	// Read /doc.txt -> should be version 1 data
	readResp, err := testReadFile(ctx, server, volumeID, "/doc.txt", 0, 1024)
	if err != nil {
		t.Fatalf("ReadFile after rollback failed: %v", err)
	}
	if string(readResp.Data) != "version 1 data" {
		t.Fatalf("Expected 'version 1 data' after rollback, got %q", string(readResp.Data))
	}

	// /doc2.txt should not exist
	doc2Resp, err := testGetAttr(ctx, server, volumeID, "/doc2.txt")
	if err != nil {
		t.Fatalf("GetAttr failed: %v", err)
	}
	if doc2Resp.GetError() != int32(syscall.ENOENT) {
		t.Fatalf("Expected /doc2.txt to not exist after rollback to snap1, got error %d", doc2Resp.GetError())
	}

	// Now roll forward to snap2
	if err := server.RestoreSnapshot(ctx, volumeID, snap2); err != nil {
		t.Fatalf("RestoreSnapshot to %s failed: %v", snap2, err)
	}

	readResp2, err := testReadFile(ctx, server, volumeID, "/doc.txt", 0, 1024)
	if err != nil {
		t.Fatalf("ReadFile after restore to snap2 failed: %v", err)
	}
	if string(readResp2.Data) != "version 2 data overwritten" {
		t.Fatalf("Expected 'version 2 data overwritten' after restore to snap2, got %q", string(readResp2.Data))
	}
}

func TestCSIControllerOperations(t *testing.T) {
	ctx := t.Context()
	backend := NewMemoryBackend()
	server := NewServer(backend)
	csiController := NewCSIController(server)

	// 1. GetPluginInfo
	pluginInfo, err := csiController.GetPluginInfo(ctx, &csi.GetPluginInfoRequest{})
	if err != nil {
		t.Fatalf("GetPluginInfo failed: %v", err)
	}
	if pluginInfo.GetName() != "objectfs.labs.gke.io" {
		t.Fatalf("Expected plugin name objectfs.labs.gke.io, got %s", pluginInfo.GetName())
	}

	// 2. GetPluginCapabilities
	pluginCaps, err := csiController.GetPluginCapabilities(ctx, &csi.GetPluginCapabilitiesRequest{})
	if err != nil {
		t.Fatalf("GetPluginCapabilities failed: %v", err)
	}
	if len(pluginCaps.GetCapabilities()) == 0 {
		t.Fatalf("Expected plugin capabilities, got none")
	}

	// 3. Probe
	if _, err := csiController.Probe(ctx, &csi.ProbeRequest{}); err != nil {
		t.Fatalf("Probe failed: %v", err)
	}

	// 4. ControllerGetCapabilities
	ctrlCaps, err := csiController.ControllerGetCapabilities(ctx, &csi.ControllerGetCapabilitiesRequest{})
	if err != nil {
		t.Fatalf("ControllerGetCapabilities failed: %v", err)
	}
	hasCreateDelete := false
	for _, cap := range ctrlCaps.GetCapabilities() {
		if cap.GetRpc().GetType() == csi.ControllerServiceCapability_RPC_CREATE_DELETE_VOLUME {
			hasCreateDelete = true
		}
	}
	if !hasCreateDelete {
		t.Fatalf("Expected CREATE_DELETE_VOLUME capability")
	}

	// 5. CreateVolume
	createVolResp, err := csiController.CreateVolume(ctx, &csi.CreateVolumeRequest{
		Name: "test-pvc-volume-1",
		CapacityRange: &csi.CapacityRange{
			RequiredBytes: 5 * 1024 * 1024 * 1024,
		},
		VolumeCapabilities: []*csi.VolumeCapability{
			{
				AccessMode: &csi.VolumeCapability_AccessMode{
					Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER,
				},
			},
		},
		Parameters: map[string]string{
			"writeMode": "lazy",
		},
	})
	if err != nil {
		t.Fatalf("CreateVolume failed: %v", err)
	}
	if createVolResp.GetVolume().GetVolumeId() != "test-pvc-volume-1" {
		t.Fatalf("Unexpected volume ID: %s", createVolResp.GetVolume().GetVolumeId())
	}
	if createVolResp.GetVolume().GetCapacityBytes() != 5*1024*1024*1024 {
		t.Fatalf("Unexpected capacity bytes: %d", createVolResp.GetVolume().GetCapacityBytes())
	}
	if createVolResp.GetVolume().GetVolumeContext()["writeMode"] != "lazy" {
		t.Fatalf("Unexpected volume context writeMode: %s", createVolResp.GetVolume().GetVolumeContext()["writeMode"])
	}

	// 6. ValidateVolumeCapabilities
	valResp, err := csiController.ValidateVolumeCapabilities(ctx, &csi.ValidateVolumeCapabilitiesRequest{
		VolumeId: "test-pvc-volume-1",
		VolumeCapabilities: []*csi.VolumeCapability{
			{
				AccessMode: &csi.VolumeCapability_AccessMode{
					Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER,
				},
			},
		},
	})
	if err != nil {
		t.Fatalf("ValidateVolumeCapabilities failed: %v", err)
	}
	if valResp.GetConfirmed() == nil {
		t.Fatalf("Expected confirmed capabilities")
	}

	// 7. DeleteVolume
	if _, err := csiController.DeleteVolume(ctx, &csi.DeleteVolumeRequest{VolumeId: "test-pvc-volume-1"}); err != nil {
		t.Fatalf("DeleteVolume failed: %v", err)
	}
}

type testGetBlobServer struct {
	pb.ObjectFSController_GetBlobServer
	ctx    context.Context
	chunks [][]byte
}

func (t *testGetBlobServer) Context() context.Context {
	return t.ctx
}

func (t *testGetBlobServer) Send(resp *pb.GetBlobResponse) error {
	t.chunks = append(t.chunks, resp.GetData())
	return nil
}

func TestControllerListBlobsAndGetBlob(t *testing.T) {
	ctx := t.Context()
	backend := NewMemoryBackend()
	server := NewServer(backend)
	volumeID := "test-blob-vol"

	content1 := []byte("blob data content number 1")
	content2 := []byte("blob data content number 2 - slightly longer test blob")

	_, err := testCreateFile(ctx, server, volumeID, "/file1.txt", 0644, content1, 0, 0)
	if err != nil {
		t.Fatalf("CreateFile file1 failed: %v", err)
	}

	_, err = testCreateFile(ctx, server, volumeID, "/file2.txt", 0644, content2, 0, 0)
	if err != nil {
		t.Fatalf("CreateFile file2 failed: %v", err)
	}

	// Flush volume to write blobs to blob store
	if err := server.FlushAll(ctx); err != nil {
		t.Fatalf("FlushAll failed: %v", err)
	}

	// 1. ListBlobs full
	listResp, err := server.ListBlobs(ctx, &pb.ListBlobsRequest{})
	if err != nil {
		t.Fatalf("ListBlobs failed: %v", err)
	}
	if len(listResp.GetSha256()) != 2 {
		t.Fatalf("Expected 2 blobs, got %d: %v", len(listResp.GetSha256()), listResp.GetSha256())
	}
	if !listResp.GetEndOfData() {
		t.Fatalf("Expected EndOfData to be true for full list")
	}

	// 2. ListBlobs pagination
	p1, err := server.ListBlobs(ctx, &pb.ListBlobsRequest{Limit: 1})
	if err != nil {
		t.Fatalf("ListBlobs page 1 failed: %v", err)
	}
	if len(p1.GetSha256()) != 1 || p1.GetEndOfData() {
		t.Fatalf("Expected 1 sha and EndOfData=false for page 1, got %v (EndOfData=%v)", p1.GetSha256(), p1.GetEndOfData())
	}

	p2, err := server.ListBlobs(ctx, &pb.ListBlobsRequest{FromSha: p1.GetSha256()[0], Limit: 1})
	if err != nil {
		t.Fatalf("ListBlobs page 2 failed: %v", err)
	}
	if len(p2.GetSha256()) != 1 || !p2.GetEndOfData() {
		t.Fatalf("Expected 1 sha and EndOfData=true for page 2, got %v (EndOfData=%v)", p2.GetSha256(), p2.GetEndOfData())
	}

	// 3. ListBlobs prefix
	prefix := listResp.GetSha256()[0][:6]
	prefResp, err := server.ListBlobs(ctx, &pb.ListBlobsRequest{ShaPrefix: prefix})
	if err != nil {
		t.Fatalf("ListBlobs prefix failed: %v", err)
	}
	for _, sha := range prefResp.GetSha256() {
		if !strings.HasPrefix(sha, prefix) {
			t.Fatalf("Expected sha %s to start with %s", sha, prefix)
		}
	}

	// 4. GetBlob for both blobs
	for _, sha := range listResp.GetSha256() {
		stream := &testGetBlobServer{ctx: ctx}
		if err := server.GetBlob(&pb.GetBlobRequest{Sha256: sha}, stream); err != nil {
			t.Fatalf("GetBlob failed for sha %s: %v", sha, err)
		}
		var fullData []byte
		for _, chunk := range stream.chunks {
			fullData = append(fullData, chunk...)
		}
		if string(fullData) != string(content1) && string(fullData) != string(content2) {
			t.Fatalf("Unexpected blob content: %q", string(fullData))
		}
	}

	// 5. GetBlob with offset and limit
	sha0 := listResp.GetSha256()[0]
	streamPartial := &testGetBlobServer{ctx: ctx}
	if err := server.GetBlob(&pb.GetBlobRequest{
		Sha256: sha0,
		Offset: 5,
		Limit:  4,
	}, streamPartial); err != nil {
		t.Fatalf("GetBlob with offset/limit failed: %v", err)
	}
	var partialData []byte
	for _, chunk := range streamPartial.chunks {
		partialData = append(partialData, chunk...)
	}
	if len(partialData) != 4 {
		t.Fatalf("Expected 4 bytes, got %d (%q)", len(partialData), string(partialData))
	}

	// 6. GetBlob for non-existent blob
	streamNotFound := &testGetBlobServer{ctx: ctx}
	if err := server.GetBlob(&pb.GetBlobRequest{Sha256: "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"}, streamNotFound); err == nil {
		t.Fatalf("Expected error for non-existent blob, got nil")
	}
}

func TestListVolumes(t *testing.T) {
	ctx := t.Context()
	backend := NewMemoryBackend()
	server := NewServer(backend)

	// 1. Empty server returns empty list
	resp, err := server.ListVolumes(ctx, &pb.ListVolumesRequest{})
	if err != nil {
		t.Fatalf("ListVolumes failed: %v", err)
	}
	if len(resp.GetVolumes()) != 0 || !resp.GetEndOfData() {
		t.Fatalf("Expected 0 volumes, got %v", resp.GetVolumes())
	}

	// 2. Create files in 3 volumes
	volNames := []string{"vol-c", "vol-a", "vol-b"}
	for _, v := range volNames {
		_, err := testCreateFile(ctx, server, v, "/hello.txt", 0644, []byte("content for "+v), 0, 0)
		if err != nil {
			t.Fatalf("CreateFile for %s failed: %v", v, err)
		}
	}

	// 3. List all volumes (should be sorted alphabetically: vol-a, vol-b, vol-c)
	resp, err = server.ListVolumes(ctx, &pb.ListVolumesRequest{})
	if err != nil {
		t.Fatalf("ListVolumes failed: %v", err)
	}
	if len(resp.GetVolumes()) != 3 {
		t.Fatalf("Expected 3 volumes, got %d", len(resp.GetVolumes()))
	}
	expected := []string{"vol-a", "vol-b", "vol-c"}
	for i, exp := range expected {
		if resp.GetVolumes()[i].GetVolumeId() != exp {
			t.Fatalf("Index %d: expected %s, got %s", i, exp, resp.GetVolumes()[i].GetVolumeId())
		}
	}

	// 4. List with pagination (limit 2)
	resp, err = server.ListVolumes(ctx, &pb.ListVolumesRequest{Limit: 2})
	if err != nil {
		t.Fatalf("ListVolumes with limit failed: %v", err)
	}
	if len(resp.GetVolumes()) != 2 || resp.GetEndOfData() {
		t.Fatalf("Expected 2 volumes and EndOfData=false, got %d volumes, EndOfData=%v", len(resp.GetVolumes()), resp.GetEndOfData())
	}

	// 5. List with from_volume_id
	resp, err = server.ListVolumes(ctx, &pb.ListVolumesRequest{FromVolumeId: "vol-a"})
	if err != nil {
		t.Fatalf("ListVolumes with FromVolumeId failed: %v", err)
	}
	if len(resp.GetVolumes()) != 2 || resp.GetVolumes()[0].GetVolumeId() != "vol-b" || resp.GetVolumes()[1].GetVolumeId() != "vol-c" {
		t.Fatalf("Unexpected volumes after vol-a: %v", resp.GetVolumes())
	}

	// 6. Flush vol-a and check backend discovery with a fresh Server instance
	if err := server.FlushAll(ctx); err != nil {
		t.Fatalf("FlushAll failed: %v", err)
	}
	recoveredServer := NewServer(backend)
	recResp, err := recoveredServer.ListVolumes(ctx, &pb.ListVolumesRequest{})
	if err != nil {
		t.Fatalf("recovered server ListVolumes failed: %v", err)
	}
	if len(recResp.GetVolumes()) != 3 {
		t.Fatalf("Expected 3 volumes discovered in backend, got %d: %v", len(recResp.GetVolumes()), recResp.GetVolumes())
	}
}

func TestSnapshotsServicePagination(t *testing.T) {
	ctx := t.Context()
	backend := NewMemoryBackend()
	server := NewServer(backend)

	// 1. Validation errors
	if _, err := server.CreateSnapshot(ctx, &pb.CreateSnapshotRequest{}); err == nil {
		t.Fatalf("Expected error for empty volume_id on CreateSnapshot")
	}
	if _, err := server.ListSnapshots(ctx, &pb.ListSnapshotsRequest{}); err == nil {
		t.Fatalf("Expected error for empty volume_id on ListSnapshots")
	}

	volumeID := "test-snap-pag"

	// 2. Create snapshot 1
	_, err := testCreateFile(ctx, server, volumeID, "/file1.txt", 0644, []byte("snap 1 content"), 0, 0)
	if err != nil {
		t.Fatalf("CreateFile 1 failed: %v", err)
	}

	snapResp1, err := server.CreateSnapshot(ctx, &pb.CreateSnapshotRequest{VolumeId: volumeID})
	if err != nil {
		t.Fatalf("CreateSnapshot 1 failed: %v", err)
	}
	snap1 := snapResp1.GetSnapshotName()
	if snapResp1.GetSnapshot().GetCreatedAt() == nil {
		t.Fatalf("Expected non-nil CreatedAt in SnapshotInfo")
	}

	time.Sleep(15 * time.Millisecond)

	// Create snapshot 2
	_, err = testCreateFile(ctx, server, volumeID, "/file2.txt", 0644, []byte("snap 2 content"), 0, 0)
	if err != nil {
		t.Fatalf("CreateFile 2 failed: %v", err)
	}

	snapResp2, err := server.CreateSnapshot(ctx, &pb.CreateSnapshotRequest{VolumeId: volumeID})
	if err != nil {
		t.Fatalf("CreateSnapshot 2 failed: %v", err)
	}
	snap2 := snapResp2.GetSnapshotName()

	time.Sleep(15 * time.Millisecond)

	// Create snapshot 3
	_, err = testCreateFile(ctx, server, volumeID, "/file3.txt", 0644, []byte("snap 3 content"), 0, 0)
	if err != nil {
		t.Fatalf("CreateFile 3 failed: %v", err)
	}

	snapResp3, err := server.CreateSnapshot(ctx, &pb.CreateSnapshotRequest{VolumeId: volumeID})
	if err != nil {
		t.Fatalf("CreateSnapshot 3 failed: %v", err)
	}
	snap3 := snapResp3.GetSnapshotName()

	// 3. List all 3
	listResp, err := server.ListSnapshots(ctx, &pb.ListSnapshotsRequest{VolumeId: volumeID})
	if err != nil {
		t.Fatalf("ListSnapshots failed: %v", err)
	}
	if len(listResp.GetSnapshots()) != 3 || !listResp.GetEndOfData() {
		t.Fatalf("Expected 3 snapshots, got %d (endOfData=%v)", len(listResp.GetSnapshots()), listResp.GetEndOfData())
	}
	if listResp.GetSnapshots()[0].GetName() != snap1 ||
		listResp.GetSnapshots()[1].GetName() != snap2 ||
		listResp.GetSnapshots()[2].GetName() != snap3 {
		t.Fatalf("Snapshots not in chronological order: %v", listResp.GetSnapshots())
	}

	// 4. List with Limit: 2
	limResp, err := server.ListSnapshots(ctx, &pb.ListSnapshotsRequest{VolumeId: volumeID, Limit: 2})
	if err != nil {
		t.Fatalf("ListSnapshots with limit failed: %v", err)
	}
	if len(limResp.GetSnapshots()) != 2 || limResp.GetEndOfData() {
		t.Fatalf("Expected 2 snapshots with EndOfData=false, got %d (endOfData=%v)", len(limResp.GetSnapshots()), limResp.GetEndOfData())
	}

	// 5. List with FromSnapshot
	cursorResp, err := server.ListSnapshots(ctx, &pb.ListSnapshotsRequest{
		VolumeId:     volumeID,
		FromSnapshot: snap1,
	})
	if err != nil {
		t.Fatalf("ListSnapshots with FromSnapshot failed: %v", err)
	}
	if len(cursorResp.GetSnapshots()) != 2 || cursorResp.GetSnapshots()[0].GetName() != snap2 || cursorResp.GetSnapshots()[1].GetName() != snap3 {
		t.Fatalf("Unexpected snapshots after snap1: %v", cursorResp.GetSnapshots())
	}

	// 6. List with FromTime (using snap2 timestamp)
	t2 := snapResp2.GetSnapshot().GetCreatedAt()
	fromTimeResp, err := server.ListSnapshots(ctx, &pb.ListSnapshotsRequest{
		VolumeId: volumeID,
		FromTime: t2,
	})
	if err != nil {
		t.Fatalf("ListSnapshots with FromTime failed: %v", err)
	}
	if len(fromTimeResp.GetSnapshots()) < 2 {
		t.Fatalf("Expected at least 2 snapshots from snap2 onwards, got %d", len(fromTimeResp.GetSnapshots()))
	}

	// 7. List with ToTime (using snap2 timestamp)
	toTimeResp, err := server.ListSnapshots(ctx, &pb.ListSnapshotsRequest{
		VolumeId: volumeID,
		ToTime:   t2,
	})
	if err != nil {
		t.Fatalf("ListSnapshots with ToTime failed: %v", err)
	}
	if len(toTimeResp.GetSnapshots()) < 2 {
		t.Fatalf("Expected at least 2 snapshots up to snap2, got %d", len(toTimeResp.GetSnapshots()))
	}
}

func startTestWalBufferServer(t *testing.T, dir string) (*walbuffer.Server, string, func()) {
	ctx := t.Context()
	srv, err := walbuffer.NewServer(ctx, walbuffer.ServerConfig{
		Backend:       NewMemoryBackend(),
		DataDir:       dir,
		FlushInterval: 10 * time.Second,
		BatchMaxDelay: 2 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("failed to create walbuffer server: %v", err)
	}

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to listen for walbuffer: %v", err)
	}

	grpcServer := grpc.NewServer()
	walpb.RegisterWalBufferServer(grpcServer, srv)

	go func() {
		_ = grpcServer.Serve(listener)
	}()

	cleanup := func() {
		grpcServer.Stop()
		_ = srv.Close()
		_ = listener.Close()
	}

	return srv, listener.Addr().String(), cleanup
}

func TestStreamsChangeLogLogging(t *testing.T) {
	ctx := t.Context()
	walDir := t.TempDir()
	backend := NewMemoryBackend()
	server := NewServer(backend, WithServerWAL(walDir, "", walclient.Local))
	defer func() { _ = server.Close() }()

	volumeID := "wal-vol-test"

	// 1. Mkdir should append a mutation record
	mkdirResp, err := testMkdir(ctx, server, volumeID, "/testdir", 0755, 0, 0)
	if err != nil {
		t.Fatalf("Mkdir failed: %v", err)
	}
	if mkdirResp.Attr.Name != "testdir" {
		t.Fatalf("Unexpected mkdir name: %s", mkdirResp.Attr.Name)
	}

	vol := server.GetVolume(volumeID)
	if vol == nil || vol.Stream() == nil {
		t.Fatalf("Expected volume to have an active WAL stream")
	}

	localSeq, _, _ := vol.Stream().Watermarks()
	if localSeq < 1 {
		t.Fatalf("Expected localSeq >= 1 after Mkdir, got %d", localSeq)
	}

	// 2. CreateFile should log to stream
	createResp, err := testCreateFile(ctx, server, volumeID, "/testdir/data.txt", 0644, []byte("hello streams"), 0, 0)
	if err != nil {
		t.Fatalf("CreateFile failed: %v", err)
	}
	if createResp.Attr.Size != int64(len("hello streams")) {
		t.Fatalf("Unexpected size: %d", createResp.Attr.Size)
	}

	localSeq2, _, _ := vol.Stream().Watermarks()
	if localSeq2 <= localSeq {
		t.Fatalf("Expected localSeq to advance after CreateFile: %d -> %d", localSeq, localSeq2)
	}

	// 3. WriteFile should log to stream
	writeResp, err := testWriteFile(ctx, server, volumeID, "/testdir/data.txt", 5, []byte(" world!"), pb.WriteMode_WRITE_MODE_UNSPECIFIED)
	if err != nil {
		t.Fatalf("WriteFile failed: %v", err)
	}
	if writeResp.BytesWritten != int64(len(" world!")) {
		t.Fatalf("Unexpected bytes written: %d", writeResp.BytesWritten)
	}

	// 4. TruncateFile should log to stream
	truncResp, err := testTruncateFile(ctx, server, volumeID, "/testdir/data.txt", 5)
	if err != nil {
		t.Fatalf("TruncateFile failed: %v", err)
	}
	if truncResp.Attr.Size != 5 {
		t.Fatalf("Unexpected truncated size: %d", truncResp.Attr.Size)
	}

	// 5. Rename should log to stream
	renameResp, err := testRename(ctx, server, volumeID, "/testdir/data.txt", "/testdir/renamed.txt")
	if err != nil {
		t.Fatalf("Rename failed: %v", err)
	}
	if renameResp.Attr.Name != "renamed.txt" {
		t.Fatalf("Unexpected rename name: %s", renameResp.Attr.Name)
	}

	// 6. Unlink should log to stream
	_, err = testUnlink(ctx, server, volumeID, "/testdir/renamed.txt")
	if err != nil {
		t.Fatalf("Unlink failed: %v", err)
	}

	// 7. Rmdir should log to stream
	_, err = testRmdir(ctx, server, volumeID, "/testdir")
	if err != nil {
		t.Fatalf("Rmdir failed: %v", err)
	}

	// Verify sequential records were logged
	finalSeq, _, _ := vol.Stream().Watermarks()
	if finalSeq < 7 {
		t.Fatalf("Expected at least 7 mutation records logged, got %d", finalSeq)
	}
}

func TestStreamsCrashRecoveryReplay(t *testing.T) {
	ctx := t.Context()
	walDir := t.TempDir()
	backend := NewMemoryBackend()
	volumeID := "recovery-vol"

	// Step 1: Initialize server 1 and perform initial changes
	server1 := NewServer(backend, WithServerWAL(walDir, "", walclient.Local))
	_, err := testMkdir(ctx, server1, volumeID, "/base", 0755, 0, 0)
	if err != nil {
		t.Fatalf("server1 Mkdir failed: %v", err)
	}
	_, err = testCreateFile(ctx, server1, volumeID, "/base/initial.txt", 0644, []byte("initial snapshot content"), 0, 0)
	if err != nil {
		t.Fatalf("server1 CreateFile failed: %v", err)
	}

	// Step 2: Flush EROFS snapshot to backend
	vol1 := server1.GetVolume(volumeID)
	snapName, err := vol1.CreateSnapshot(ctx)
	if err != nil {
		t.Fatalf("CreateSnapshot failed: %v", err)
	}
	if snapName == "" {
		t.Fatalf("Empty snapshot name returned")
	}

	// Step 3: Perform mutations AFTER snapshot (these are in the WAL change-log, not in snapshot)
	_, err = testCreateFile(ctx, server1, volumeID, "/base/post_snapshot.txt", 0644, []byte("post snapshot content"), 0, 0)
	if err != nil {
		t.Fatalf("server1 CreateFile post snapshot failed: %v", err)
	}

	_, err = testRename(ctx, server1, volumeID, "/base/initial.txt", "/base/renamed_initial.txt")
	if err != nil {
		t.Fatalf("server1 Rename failed: %v", err)
	}

	// Simulate crash: close server1 without flushing snapshot to backend
	_ = server1.Close()

	// Step 4: Start new server instance with same backend and walDir
	server2 := NewServer(backend, WithServerWAL(walDir, "", walclient.Local))
	defer func() { _ = server2.Close() }()

	vol2 := server2.GetVolume(volumeID)
	if vol2 == nil {
		t.Fatalf("Failed to get recovered volume")
	}

	// Verify that state reflects both the snapshot AND replayed WAL mutations:
	// - /base should exist
	baseAttr, err := testGetAttr(ctx, server2, volumeID, "/base")
	if err != nil || !baseAttr.Attr.IsDir {
		t.Fatalf("Recovered base directory missing or not dir: %v", err)
	}

	// - /base/initial.txt should have been renamed to /base/renamed_initial.txt
	initResp, err := testGetAttr(ctx, server2, volumeID, "/base/initial.txt")
	if err != nil {
		t.Fatalf("GetAttr failed: %v", err)
	}
	if initResp.GetError() != int32(syscall.ENOENT) {
		t.Fatalf("Expected /base/initial.txt to not exist after rename replay, got error %d", initResp.GetError())
	}

	renamedAttr, err := testGetAttr(ctx, server2, volumeID, "/base/renamed_initial.txt")
	if err != nil {
		t.Fatalf("Expected /base/renamed_initial.txt to exist: %v", err)
	}
	if renamedAttr.Attr.Size != int64(len("initial snapshot content")) {
		t.Fatalf("Unexpected size on renamed file: %d", renamedAttr.Attr.Size)
	}

	// - /base/post_snapshot.txt should exist and have correct size and content
	readResp, err := testReadFile(ctx, server2, volumeID, "/base/post_snapshot.txt", 0, 1024)
	if err != nil {
		t.Fatalf("ReadFile on replayed post-snapshot file failed: %v", err)
	}
	if string(readResp.Data) != "post snapshot content" {
		t.Fatalf("Unexpected data in replayed file: %q", string(readResp.Data))
	}
}

func TestStreamsDurabilityModes(t *testing.T) {
	bufDir := t.TempDir()
	_, target, cleanup := startTestWalBufferServer(t, bufDir)
	defer cleanup()

	clientDir := t.TempDir()
	backend := NewMemoryBackend()
	volumeID := "durability-vol"

	// Create server with Witness durability
	server := NewServer(backend, WithServerWAL(clientDir, target, walclient.Witness))
	defer func() { _ = server.Close() }()

	ctx := t.Context()

	// 1. Mkdir with default durability (Witness)
	_, err := testMkdir(ctx, server, volumeID, "/witness_dir", 0755, 0, 0)
	if err != nil {
		t.Fatalf("Mkdir with Witness durability failed: %v", err)
	}

	vol := server.GetVolume(volumeID)
	local, witness, _ := vol.Stream().Watermarks()
	if local < 1 || witness < 1 {
		t.Fatalf("Expected local >= 1 and witness >= 1, got local=%d, witness=%d", local, witness)
	}

	// 2. WriteFile with WRITE_THROUGH_FSYNC (Permanent)
	_, err = testCreateFile(ctx, server, volumeID, "/witness_dir/file.bin", 0644, []byte("data"), 0, 0)
	if err != nil {
		t.Fatalf("CreateFile failed: %v", err)
	}

	_, err = testWriteFile(ctx, server, volumeID, "/witness_dir/file.bin", 4, []byte("more"), pb.WriteMode_WRITE_THROUGH_FSYNC)
	if err != nil {
		t.Fatalf("WriteFile with WRITE_THROUGH_FSYNC failed: %v", err)
	}

	_, _, permanent := vol.Stream().Watermarks()
	if permanent < 1 {
		t.Fatalf("Expected permanent watermark >= 1 after WRITE_THROUGH_FSYNC, got %d", permanent)
	}

	// 3. Fsync RPC flushes stream
	ino, _ := resolvePath(ctx, server, volumeID, "/witness_dir/file.bin")
	fsyncResp, err := server.Fsync(ctx, &pb.FsyncRequest{
		VolumeId: volumeID,
		Inode:    ino,
	})
	if err != nil || !fsyncResp.GetSuccess() {
		t.Fatalf("Fsync failed: %v", err)
	}
}

func TestApplyRecordDirect(t *testing.T) {
	ctx := t.Context()
	backend := NewMemoryBackend()
	vol := NewVolume("apply-test", backend, NewEventBroadcaster())

	// Apply Mkdir
	err := vol.ApplyRecordLocked(&MutationRecord{
		Type:        MutationMkdir,
		VolumeId:    "apply-test",
		ParentInode: 1,
		Name:        "c",
		Mode:        0755,
		Inode:       10,
	})
	if err != nil {
		t.Fatalf("Apply Mkdir failed: %v", err)
	}

	attr, err := vol.GetAttr(ctx, 10)
	if err != nil || !attr.IsDir {
		t.Fatalf("Expected directory inode 10: %v", err)
	}

	// Apply CreateFile
	err = vol.ApplyRecordLocked(&MutationRecord{
		Type:        MutationCreateFile,
		VolumeId:    "apply-test",
		ParentInode: 10,
		Name:        "foo.txt",
		Mode:        0644,
		Size:        4,
		Inode:       11,
		Data:        []byte("test"),
	})
	if err != nil {
		t.Fatalf("Apply CreateFile failed: %v", err)
	}

	data, total, _, err := vol.ReadFile(ctx, 11, 0, 100)
	if err != nil || total != 4 || string(data) != "test" {
		t.Fatalf("Unexpected file content: %s (err: %v)", string(data), err)
	}

	// Apply TruncateFile
	err = vol.ApplyRecordLocked(&MutationRecord{
		Type:     MutationTruncateFile,
		VolumeId: "apply-test",
		Inode:    11,
		Size:     2,
	})
	if err != nil {
		t.Fatalf("Apply TruncateFile failed: %v", err)
	}
	data, total, _, err = vol.ReadFile(ctx, 11, 0, 100)
	if err != nil || total != 2 || string(data) != "te" {
		t.Fatalf("Unexpected truncated content: %s (err: %v)", string(data), err)
	}

	// Apply Rename
	err = vol.ApplyRecordLocked(&MutationRecord{
		Type:           MutationRename,
		VolumeId:       "apply-test",
		OldParentInode: 10,
		OldName:        "foo.txt",
		ParentInode:    10,
		Name:           "bar.txt",
	})
	if err != nil {
		t.Fatalf("Apply Rename failed: %v", err)
	}
	_, err = vol.Lookup(ctx, 10, "foo.txt")
	if err == nil {
		t.Fatalf("Expected foo.txt to be removed after rename")
	}
	barAttr, err := vol.Lookup(ctx, 10, "bar.txt")
	if err != nil || barAttr.Name != "bar.txt" {
		t.Fatalf("Expected bar.txt to exist: %v", err)
	}

	// Apply Unlink
	err = vol.ApplyRecordLocked(&MutationRecord{
		Type:        MutationUnlink,
		VolumeId:    "apply-test",
		ParentInode: 10,
		Name:        "bar.txt",
	})
	if err != nil {
		t.Fatalf("Apply Unlink failed: %v", err)
	}
	_, err = vol.Lookup(ctx, 10, "bar.txt")
	if err == nil {
		t.Fatalf("Expected bar.txt to be unlinked")
	}

	// Apply Rmdir
	err = vol.ApplyRecordLocked(&MutationRecord{
		Type:        MutationRmdir,
		VolumeId:    "apply-test",
		ParentInode: 1,
		Name:        "c",
	})
	if err != nil {
		t.Fatalf("Apply Rmdir failed: %v", err)
	}
	_, err = vol.Lookup(ctx, 1, "c")
	if err == nil {
		t.Fatalf("Expected c to be deleted")
	}
}

func TestTargetlessWALDurabilityFastFail(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()

	walDir := t.TempDir()
	backend := NewMemoryBackend()
	server := NewServer(backend, WithServerWAL(walDir, "", walclient.Local))
	defer func() { _ = server.Close() }()

	volumeID := "test-vol-targetless-wal"

	// Create file should succeed under default Local durability
	createResp, err := testCreateFile(ctx, server, volumeID, "/test.txt", 0644, []byte("initial"), 0, 0)
	if err != nil || createResp.GetError() != 0 {
		t.Fatalf("CreateFile failed: err=%v, resp.Error=%d", err, createResp.GetError())
	}

	// Direct Volume.WriteFile with WRITE_THROUGH_FSYNC (Permanent) should fail promptly with typed error
	vol := server.GetVolume(volumeID)
	_, _, _, err = volWriteFile(ctx, vol, "/test.txt", 0, []byte("data-permanent"), pb.WriteMode_WRITE_THROUGH_FSYNC)
	if err == nil {
		t.Fatalf("expected vol.WriteFile(WRITE_THROUGH_FSYNC) to fail on target-less WAL, got nil")
	}
	if !strings.Contains(err.Error(), "durability level permanent requested but WAL has no remote target") {
		t.Fatalf("unexpected error message: %v", err)
	}

	// Server.WriteFile with WRITE_THROUGH_FSYNC (Permanent) should return error in response
	writeResp, err := testWriteFile(ctx, server, volumeID, "/test.txt", 0, []byte("data-permanent"), pb.WriteMode_WRITE_THROUGH_FSYNC)
	if err != nil {
		t.Fatalf("unexpected gRPC error: %v", err)
	}
	if writeResp.GetError() == 0 {
		t.Fatalf("expected WriteFile(WRITE_THROUGH_FSYNC) to return non-zero error on target-less WAL")
	}

	// Direct Volume.WriteFile with EAGER_REPLICATION (Witness) should fail promptly with typed error
	_, _, _, err = volWriteFile(ctx, vol, "/test.txt", 0, []byte("data-witness"), pb.WriteMode_EAGER_REPLICATION)
	if err == nil {
		t.Fatalf("expected vol.WriteFile(EAGER_REPLICATION) to fail on target-less WAL, got nil")
	}
	if !strings.Contains(err.Error(), "durability level witness requested but WAL has no remote target") {
		t.Fatalf("unexpected error message: %v", err)
	}

	// Server.WriteFile with EAGER_REPLICATION (Witness) should return error in response
	writeResp, err = testWriteFile(ctx, server, volumeID, "/test.txt", 0, []byte("data-witness"), pb.WriteMode_EAGER_REPLICATION)
	if err != nil {
		t.Fatalf("unexpected gRPC error: %v", err)
	}
	if writeResp.GetError() == 0 {
		t.Fatalf("expected WriteFile(EAGER_REPLICATION) to return non-zero error on target-less WAL")
	}

	// WriteFile with LAZY_WRITE (Local) should succeed
	writeResp, err = testWriteFile(ctx, server, volumeID, "/test.txt", 0, []byte("data-local"), pb.WriteMode_LAZY_WRITE)
	if err != nil || writeResp.GetError() != 0 {
		t.Fatalf("WriteFile(LAZY_WRITE) failed: err=%v, resp.Error=%d", err, writeResp.GetError())
	}
	if writeResp.BytesWritten != int64(len("data-local")) {
		t.Fatalf("expected %d bytes written, got %d", len("data-local"), writeResp.BytesWritten)
	}

	// Direct Volume.Fsync should fail fast on target-less WAL because it flushes to permanent storage
	testIno, _ := vol.ResolvePath(ctx, "/test.txt")
	err = vol.Fsync(ctx, testIno)
	if err == nil {
		t.Fatalf("expected vol.Fsync to fail on target-less WAL with unflushed records, got nil")
	}
	if !strings.Contains(err.Error(), "durability level permanent requested but WAL has no remote target") {
		t.Fatalf("unexpected error message: %v", err)
	}

	// Server.Fsync should return error in response
	fsyncResp, err := server.Fsync(ctx, &pb.FsyncRequest{
		VolumeId: volumeID,
		Inode:    testIno,
	})
	if err != nil {
		t.Fatalf("unexpected gRPC error: %v", err)
	}
	if fsyncResp.GetError() == 0 || fsyncResp.GetSuccess() {
		t.Fatalf("expected Fsync to return non-zero error on target-less WAL with unflushed records")
	}
}

type fakeBlockingStream struct {
	appendCalled chan struct{}
	waitCalled   chan struct{}
	waitGate     chan struct{}
	localSeq     atomic.Uint64
}

func (s *fakeBlockingStream) Append(ctx context.Context, payload []byte) (uint64, error) {
	seq := s.localSeq.Add(1)
	select {
	case s.appendCalled <- struct{}{}:
	default:
	}
	return seq, nil
}

func (s *fakeBlockingStream) Wait(ctx context.Context, seq uint64, level walclient.Level, requestFlush bool) error {
	select {
	case s.waitCalled <- struct{}{}:
	default:
	}
	select {
	case <-s.waitGate:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (s *fakeBlockingStream) Flush(ctx context.Context) error {
	return nil
}

func (s *fakeBlockingStream) Watermarks() (local, witness, permanent uint64) {
	return s.localSeq.Load(), 0, 0
}

func (s *fakeBlockingStream) RecoveredRecords() []*wal.ClientRecord {
	return nil
}

func (s *fakeBlockingStream) ReplicationLevel() walclient.Level {
	return walclient.Permanent
}

func (s *fakeBlockingStream) Close() error {
	return nil
}

func TestStreamsDurabilityConcurrency(t *testing.T) {
	ctx := t.Context()
	fakeStream := &fakeBlockingStream{
		appendCalled: make(chan struct{}, 10),
		waitCalled:   make(chan struct{}, 10),
		waitGate:     make(chan struct{}),
	}

	backend := NewMemoryBackend()
	server := NewServer(backend,
		WithServerStreamFactory(func(volumeID string) (walclient.Stream, error) {
			return fakeStream, nil
		}),
		WithServerDurability(walclient.Witness),
	)
	defer func() { _ = server.Close() }()

	volumeID := "test-concurrency-vol"

	// 1. Concurrently start CreateFile which blocks on fakeStream.Wait
	type createResult struct {
		resp *pb.CreateFileResponse
		err  error
	}
	createCh := make(chan createResult, 1)

	go func() {
		resp, err := testCreateFile(ctx, server, volumeID, "/blocking_file.txt", 0644, []byte("initial-data"), 0, 0)
		createCh <- createResult{resp: resp, err: err}
	}()

	// Wait until CreateFile enters fakeStream.Wait (outside v.mu)
	select {
	case <-fakeStream.waitCalled:
	case <-time.After(5 * time.Second):
		t.Fatalf("Timed out waiting for CreateFile to enter stream.Wait")
	}

	// While CreateFile is still blocked in Wait, ensure GetAttr, Lookup, and ReadFile complete promptly
	readDone := make(chan struct{})
	go func() {
		defer close(readDone)

		// Root GetAttr
		rootAttr, err := testGetAttr(ctx, server, volumeID, "/")
		if err != nil || !rootAttr.Attr.IsDir {
			t.Errorf("GetAttr root failed while write is waiting for durability: %v", err)
			return
		}

		// Lookup the new file (in-memory state is already updated)
		lookupResp, err := server.Lookup(ctx, &pb.LookupRequest{
			VolumeId:    volumeID,
			ParentInode: 1,
			Name:        "blocking_file.txt",
		})
		if err != nil || lookupResp.Attr.Name != "blocking_file.txt" {
			t.Errorf("Lookup new file failed while write is waiting for durability: %v", err)
			return
		}

		// GetAttr on the new file
		fileAttr, err := testGetAttr(ctx, server, volumeID, "/blocking_file.txt")
		if err != nil || fileAttr.Attr.Size != int64(len("initial-data")) {
			t.Errorf("GetAttr new file failed while write is waiting for durability: %v", err)
			return
		}

		// ReadFile on the new file
		readResp, err := testReadFile(ctx, server, volumeID, "/blocking_file.txt", 0, 1024)
		if err != nil || string(readResp.Data) != "initial-data" {
			t.Errorf("ReadFile failed while write is waiting for durability: %v", err)
			return
		}
	}()

	select {
	case <-readDone:
	case <-time.After(2 * time.Second):
		t.Fatalf("Concurrent reads stalled while writer waited for WAL durability")
	}

	// Unblock the stream and ensure CreateFile completes successfully
	close(fakeStream.waitGate)
	select {
	case res := <-createCh:
		if res.err != nil {
			t.Fatalf("CreateFile failed: %v", res.err)
		}
		if res.resp.Attr.Name != "blocking_file.txt" {
			t.Fatalf("Unexpected CreateFile attr: %v", res.resp.Attr)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("Timed out waiting for CreateFile to return after unblocking stream")
	}

	// 2. Test wait failure semantics: context cancellation during Wait returns error to caller,
	// but in-memory mutation remains visible.
	fakeStream2 := &fakeBlockingStream{
		appendCalled: make(chan struct{}, 10),
		waitCalled:   make(chan struct{}, 10),
		waitGate:     make(chan struct{}),
	}
	server2 := NewServer(backend,
		WithServerStreamFactory(func(volumeID string) (walclient.Stream, error) {
			return fakeStream2, nil
		}),
		WithServerDurability(walclient.Witness),
	)
	defer func() { _ = server2.Close() }()

	vol2 := "test-wait-fail-vol"
	writeCtx, cancelWrite := context.WithCancel(ctx)

	type createFailResult struct {
		resp *pb.CreateFileResponse
		err  error
	}
	writeCh := make(chan createFailResult, 1)
	go func() {
		resp, err := testCreateFile(writeCtx, server2, vol2, "/fail_durability.txt", 0644, []byte("persisted-in-mem"), 0, 0)
		writeCh <- createFailResult{resp: resp, err: err}
	}()

	select {
	case <-fakeStream2.waitCalled:
	case <-time.After(5 * time.Second):
		t.Fatalf("Timed out waiting for CreateFile on server2 to enter stream.Wait")
	}

	// Cancel context during wait
	cancelWrite()

	select {
	case res := <-writeCh:
		if res.err == nil && (res.resp == nil || res.resp.GetError() == 0) {
			t.Fatalf("Expected CreateFile to fail on canceled context, got success")
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("Timed out waiting for CreateFile to fail on canceled context")
	}

	// Verify in-memory state is still visible (not rolled back)
	attr, err := testGetAttr(ctx, server2, vol2, "/fail_durability.txt")
	if err != nil || attr.GetError() != 0 || attr.Attr.Inode == 0 {
		t.Fatalf("Expected in-memory state to remain intact after durability wait failure: %v (attr: %v)", err, attr)
	}
}

func TestStreamsDurabilityConcurrencyAllMutations(t *testing.T) {
	ctx := t.Context()
	fakeStream := &fakeBlockingStream{
		appendCalled: make(chan struct{}, 10),
		waitCalled:   make(chan struct{}, 10),
		waitGate:     make(chan struct{}),
	}

	backend := NewMemoryBackend()
	server := NewServer(backend,
		WithServerStreamFactory(func(volumeID string) (walclient.Stream, error) {
			return fakeStream, nil
		}),
		WithServerDurability(walclient.Witness),
	)
	defer func() { _ = server.Close() }()

	volumeID := "test-all-mutations-vol"

	// Helper to run a mutation while checking that concurrent reads succeed
	runMutationTest := func(name string, mutate func(), checkReads func()) {
		// New waitGate for this mutation
		fakeStream.waitGate = make(chan struct{})

		done := make(chan struct{})
		go func() {
			defer close(done)
			mutate()
		}()

		select {
		case <-fakeStream.waitCalled:
		case <-time.After(5 * time.Second):
			t.Fatalf("[%s] Timed out waiting for mutation to enter stream.Wait", name)
		}

		readDone := make(chan struct{})
		go func() {
			defer close(readDone)
			checkReads()
		}()

		select {
		case <-readDone:
		case <-time.After(2 * time.Second):
			t.Fatalf("[%s] Concurrent reads stalled while waiting for durability", name)
		}

		close(fakeStream.waitGate)
		select {
		case <-done:
		case <-time.After(2 * time.Second):
			t.Fatalf("[%s] Mutation did not complete after unblocking wait", name)
		}
	}

	// 1. CreateFile
	runMutationTest("CreateFile", func() {
		_, err := testCreateFile(ctx, server, volumeID, "/test_file.txt", 0644, []byte("initial"), 0, 0)
		if err != nil {
			t.Errorf("CreateFile failed: %v", err)
		}
	}, func() {
		attr, err := testGetAttr(ctx, server, volumeID, "/test_file.txt")
		if err != nil || attr.Attr.Size != 7 {
			t.Errorf("GetAttr during CreateFile failed: %v", err)
		}
	})

	// 2. WriteFile
	runMutationTest("WriteFile", func() {
		_, err := testWriteFile(ctx, server, volumeID, "/test_file.txt", 7, []byte("-appended"), pb.WriteMode_WRITE_THROUGH_FSYNC)
		if err != nil {
			t.Errorf("WriteFile failed: %v", err)
		}
	}, func() {
		resp, err := testReadFile(ctx, server, volumeID, "/test_file.txt", 0, 1024)
		if err != nil || string(resp.Data) != "initial-appended" {
			t.Errorf("ReadFile during WriteFile durability wait failed: %v, data=%q", err, string(resp.Data))
		}
	})

	// 3. Mkdir
	runMutationTest("Mkdir", func() {
		_, err := testMkdir(ctx, server, volumeID, "/newdir", 0755, 0, 0)
		if err != nil {
			t.Errorf("Mkdir failed: %v", err)
		}
	}, func() {
		dirAttr, err := testGetAttr(ctx, server, volumeID, "/newdir")
		if err != nil || !dirAttr.Attr.IsDir {
			t.Errorf("GetAttr during Mkdir durability wait failed: %v", err)
		}
	})

	// 4. Rename
	runMutationTest("Rename", func() {
		_, err := testRename(ctx, server, volumeID, "/test_file.txt", "/renamed_file.txt")
		if err != nil {
			t.Errorf("Rename failed: %v", err)
		}
	}, func() {
		renamedAttr, err := testGetAttr(ctx, server, volumeID, "/renamed_file.txt")
		if err != nil || renamedAttr.GetError() != 0 || renamedAttr.Attr == nil || renamedAttr.Attr.Inode == 0 {
			t.Errorf("GetAttr during Rename durability wait failed: %v", err)
		}
	})

	// 5. TruncateFile
	runMutationTest("TruncateFile", func() {
		_, err := testTruncateFile(ctx, server, volumeID, "/renamed_file.txt", 7)
		if err != nil {
			t.Errorf("TruncateFile failed: %v", err)
		}
	}, func() {
		truncRead, err := testReadFile(ctx, server, volumeID, "/renamed_file.txt", 0, 1024)
		if err != nil || string(truncRead.Data) != "initial" {
			t.Errorf("ReadFile during TruncateFile durability wait failed: %v, data=%q", err, string(truncRead.Data))
		}
	})

	// 6. Unlink
	runMutationTest("Unlink", func() {
		_, err := testUnlink(ctx, server, volumeID, "/renamed_file.txt")
		if err != nil {
			t.Errorf("Unlink failed: %v", err)
		}
	}, func() {
		resp, err := testGetAttr(ctx, server, volumeID, "/renamed_file.txt")
		if err == nil && resp.GetError() == 0 {
			t.Errorf("Expected file to be unlinked in memory during Unlink durability wait")
		}
	})

	// 7. Rmdir
	runMutationTest("Rmdir", func() {
		_, err := testRmdir(ctx, server, volumeID, "/newdir")
		if err != nil {
			t.Errorf("Rmdir failed: %v", err)
		}
	}, func() {
		resp, err := testGetAttr(ctx, server, volumeID, "/newdir")
		if err == nil && resp.GetError() == 0 {
			t.Errorf("Expected directory to be removed in memory during Rmdir durability wait")
		}
	})
}

func TestVolumeFixedBoundaryChunking(t *testing.T) {
	ctx := t.Context()
	backend := NewMemoryBackend()
	chunkSize := uint32(16 * 1024) // 16 KiB chunk size for testing
	vol := NewVolume("chunk-test-vol", backend, NewEventBroadcaster(), WithChunkSize(chunkSize))

	// 1. Small file <= 1 chunk (e.g. 100 bytes)
	smallData := []byte("small file content unchunked")
	smallAttr, err := volCreateFile(ctx, vol, "/small.txt", 0644, smallData, 0, 0)
	if err != nil {
		t.Fatalf("CreateFile /small.txt failed: %v", err)
	}
	smallSha := fmt.Sprintf("%x", sha256.Sum256(smallData))
	if smallAttr.ManifestSha256 != "" {
		t.Fatalf("expected empty ManifestSha256 for small file, got %s", smallAttr.ManifestSha256)
	}
	if smallAttr.ContentSha256 != smallSha {
		t.Fatalf("expected ContentSha256 %s, got %s", smallSha, smallAttr.ContentSha256)
	}

	// 2. Large file > 1 chunk (e.g. 40 KiB = 2.5 chunks)
	largeData := make([]byte, 40*1024)
	for i := range largeData {
		largeData[i] = byte(i % 251)
	}
	largeSha := fmt.Sprintf("%x", sha256.Sum256(largeData))

	largeAttr, err := volCreateFile(ctx, vol, "/large.bin", 0644, largeData, 0, 0)
	if err != nil {
		t.Fatalf("CreateFile /large.bin failed: %v", err)
	}
	if largeAttr.ManifestSha256 == "" {
		t.Fatalf("expected non-empty ManifestSha256 for chunked file")
	}
	if largeAttr.ContentSha256 != largeSha {
		t.Fatalf("expected ContentSha256 %s, got %s", largeSha, largeAttr.ContentSha256)
	}

	// 3. Read partial ranges spanning chunk boundaries
	// Read 20 KiB starting at offset 10 KiB (spans chunk 0 and chunk 1)
	partData, total, _, err := volReadFile(ctx, vol, "/large.bin", 10*1024, 20*1024)
	if err != nil {
		t.Fatalf("ReadFile partial spanning chunks failed: %v", err)
	}
	if total != int64(len(largeData)) {
		t.Fatalf("expected total %d, got %d", len(largeData), total)
	}
	if !bytes.Equal(partData, largeData[10*1024:30*1024]) {
		t.Fatalf("partial data mismatch spanning chunks")
	}

	// 4. Random write touching chunk 1 (offset 20 KiB, length 4 KiB)
	patch := []byte("random patch in chunk 1")
	copy(largeData[20*1024:], patch)
	_, newSize, _, err := volWriteFile(ctx, vol, "/large.bin", 20*1024, patch, pb.WriteMode_LAZY_WRITE)
	if err != nil {
		t.Fatalf("WriteFile random write failed: %v", err)
	}
	if newSize != int64(len(largeData)) {
		t.Fatalf("expected size %d, got %d", len(largeData), newSize)
	}

	updatedAttr, err := volGetAttr(ctx, vol, "/large.bin")
	if err != nil {
		t.Fatalf("GetAttr after random write failed: %v", err)
	}
	// ContentSha256 should be cleared (marked unknown) after random write
	if updatedAttr.ContentSha256 != "" {
		t.Fatalf("expected ContentSha256 to be unknown (\"\") after random write, got %s", updatedAttr.ContentSha256)
	}

	// Read back modified range
	readBack, _, _, err := volReadFile(ctx, vol, "/large.bin", 20*1024, int64(len(patch)))
	if err != nil || !bytes.Equal(readBack, patch) {
		t.Fatalf("read back patch mismatch: %q vs %q", string(readBack), string(patch))
	}

	// 5. Test snapshot compilation: lazy ContentSha256 recomputation and EROFS xattrs
	if err := vol.FlushToBackend(ctx); err != nil {
		t.Fatalf("FlushToBackend failed: %v", err)
	}

	newLargeSha := fmt.Sprintf("%x", sha256.Sum256(largeData))
	postSnapAttr, err := volGetAttr(ctx, vol, "/large.bin")
	if err != nil {
		t.Fatalf("GetAttr post-snapshot failed: %v", err)
	}
	if postSnapAttr.ContentSha256 != newLargeSha {
		t.Fatalf("expected recomputed ContentSha256 %s post-snapshot, got %s", newLargeSha, postSnapAttr.ContentSha256)
	}

	// 6. Test migration: writing to an unchunked file that grows > chunkSize turns into chunked
	growData := make([]byte, 20*1024)
	for i := range growData {
		growData[i] = 'G'
	}
	_, _, _, err = volWriteFile(ctx, vol, "/small.txt", 100, growData, pb.WriteMode_LAZY_WRITE)
	if err != nil {
		t.Fatalf("WriteFile growing small file failed: %v", err)
	}

	growAttr, err := volGetAttr(ctx, vol, "/small.txt")
	if err != nil {
		t.Fatalf("GetAttr for grown file failed: %v", err)
	}
	if growAttr.ManifestSha256 == "" {
		t.Fatalf("expected grown file to have ManifestSha256 populated")
	}

	// 7. Test truncation across chunks
	truncAttr, err := volTruncateFile(ctx, vol, "/large.bin", 18*1024)
	if err != nil {
		t.Fatalf("TruncateFile failed: %v", err)
	}
	if truncAttr.Size != 18*1024 {
		t.Fatalf("expected size %d after truncation, got %d", 18*1024, truncAttr.Size)
	}
	truncData, total, _, err := volReadFile(ctx, vol, "/large.bin", 0, 20*1024)
	if err != nil || total != 18*1024 || len(truncData) != 18*1024 {
		t.Fatalf("ReadFile after truncation failed: total=%d, len=%d, err=%v", total, len(truncData), err)
	}
}

func TestStableInodeNumbersAcrossSnapshots(t *testing.T) {
	ctx := t.Context()
	backend := NewMemoryBackend()
	server := NewServer(backend)
	volumeID := "stable-ino-vol"

	// 1. Create a directory /dir1 and file /file1.txt and /dir1/file2.txt
	_, err := testMkdir(ctx, server, volumeID, "/dir1", 0755, 0, 0)
	if err != nil {
		t.Fatalf("Failed to mkdir /dir1: %v", err)
	}

	create1, err := testCreateFile(ctx, server, volumeID, "/file1.txt", 0644, []byte("content of file 1"), 0, 0)
	if err != nil {
		t.Fatalf("Failed to create /file1.txt: %v", err)
	}

	create2, err := testCreateFile(ctx, server, volumeID, "/dir1/file2.txt", 0644, []byte("content of file 2"), 0, 0)
	if err != nil {
		t.Fatalf("Failed to create /dir1/file2.txt: %v", err)
	}

	dir1Attr, err := testGetAttr(ctx, server, volumeID, "/dir1")
	if err != nil {
		t.Fatalf("Failed to get /dir1 attr: %v", err)
	}

	file1InoInitial := create1.Attr.Inode
	dir1InoInitial := dir1Attr.Attr.Inode
	file2InoInitial := create2.Attr.Inode

	if file1InoInitial == 0 || dir1InoInitial == 0 || file2InoInitial == 0 {
		t.Fatalf("Expected non-zero inode IDs, got file1=%d, dir1=%d, file2=%d", file1InoInitial, dir1InoInitial, file2InoInitial)
	}

	// 2. Take first snapshot
	snap1Resp, err := server.CreateSnapshot(ctx, &pb.CreateSnapshotRequest{VolumeId: volumeID})
	if err != nil {
		t.Fatalf("Failed to create first snapshot: %v", err)
	}
	snap1Name := snap1Resp.SnapshotName

	// Verify attributes after snapshot 1
	file1AttrAfterSnap1, err := testGetAttr(ctx, server, volumeID, "/file1.txt")
	if err != nil {
		t.Fatalf("Failed to get /file1.txt after snap 1: %v", err)
	}
	if file1AttrAfterSnap1.Attr.Inode != file1InoInitial {
		t.Fatalf("Inode changed after snap 1: expected %d, got %d", file1InoInitial, file1AttrAfterSnap1.Attr.Inode)
	}

	dir1AttrAfterSnap1, err := testGetAttr(ctx, server, volumeID, "/dir1")
	if err != nil {
		t.Fatalf("Failed to get /dir1 after snap 1: %v", err)
	}
	if dir1AttrAfterSnap1.Attr.Inode != dir1InoInitial {
		t.Fatalf("Dir inode changed after snap 1: expected %d, got %d", dir1InoInitial, dir1AttrAfterSnap1.Attr.Inode)
	}

	file2AttrAfterSnap1, err := testGetAttr(ctx, server, volumeID, "/dir1/file2.txt")
	if err != nil {
		t.Fatalf("Failed to get /dir1/file2.txt after snap 1: %v", err)
	}
	if file2AttrAfterSnap1.Attr.Inode != file2InoInitial {
		t.Fatalf("File2 inode changed after snap 1: expected %d, got %d", file2InoInitial, file2AttrAfterSnap1.Attr.Inode)
	}

	// Read and verify EROFS snapshot 1 image directly
	var img1Buf bytes.Buffer
	snap1Key := path.Join("volumes", volumeID, "meta", snap1Name)
	if err := backend.GetObject(ctx, "", snap1Key, 0, 0, &img1Buf); err != nil {
		t.Fatalf("Failed to fetch snap 1 bytes: %v", err)
	}
	reader1, err := erofs.NewReader(bytes.NewReader(img1Buf.Bytes()))
	if err != nil {
		t.Fatalf("Failed to parse snap 1 image: %v", err)
	}

	// Verify root dirents in snapshot 1
	rootDirents1, err := reader1.ListDirectory(reader1.GetRootNID())
	if err != nil {
		t.Fatalf("Failed to list root in snap 1: %v", err)
	}
	for _, de := range rootDirents1 {
		if de.Name == "file1.txt" && de.NID != file1InoInitial {
			t.Fatalf("Snap 1 file1.txt NID mismatch: expected %d, got %d", file1InoInitial, de.NID)
		}
		if de.Name == "dir1" && de.NID != dir1InoInitial {
			t.Fatalf("Snap 1 dir1 NID mismatch: expected %d, got %d", dir1InoInitial, de.NID)
		}
	}

	// 3. Create a new file before taking second snapshot
	create3, err := testCreateFile(ctx, server, volumeID, "/file3.txt", 0644, []byte("content of file 3"), 0, 0)
	if err != nil {
		t.Fatalf("Failed to create /file3.txt: %v", err)
	}
	file3InoInitial := create3.Attr.Inode

	// 4. Take second snapshot
	snap2Resp, err := server.CreateSnapshot(ctx, &pb.CreateSnapshotRequest{VolumeId: volumeID})
	if err != nil {
		t.Fatalf("Failed to create second snapshot: %v", err)
	}
	snap2Name := snap2Resp.SnapshotName

	// Verify all attributes after snapshot 2: previous files MUST have identical Inode IDs
	file1AttrAfterSnap2, err := testGetAttr(ctx, server, volumeID, "/file1.txt")
	if err != nil {
		t.Fatalf("Failed to get /file1.txt after snap 2: %v", err)
	}
	if file1AttrAfterSnap2.Attr.Inode != file1InoInitial {
		t.Fatalf("Inode changed after snap 2: expected %d, got %d", file1InoInitial, file1AttrAfterSnap2.Attr.Inode)
	}

	dir1AttrAfterSnap2, err := testGetAttr(ctx, server, volumeID, "/dir1")
	if err != nil {
		t.Fatalf("Failed to get /dir1 after snap 2: %v", err)
	}
	if dir1AttrAfterSnap2.Attr.Inode != dir1InoInitial {
		t.Fatalf("Dir inode changed after snap 2: expected %d, got %d", dir1InoInitial, dir1AttrAfterSnap2.Attr.Inode)
	}

	file2AttrAfterSnap2, err := testGetAttr(ctx, server, volumeID, "/dir1/file2.txt")
	if err != nil {
		t.Fatalf("Failed to get /dir1/file2.txt after snap 2: %v", err)
	}
	if file2AttrAfterSnap2.Attr.Inode != file2InoInitial {
		t.Fatalf("File2 inode changed after snap 2: expected %d, got %d", file2InoInitial, file2AttrAfterSnap2.Attr.Inode)
	}

	file3AttrAfterSnap2, err := testGetAttr(ctx, server, volumeID, "/file3.txt")
	if err != nil {
		t.Fatalf("Failed to get /file3.txt after snap 2: %v", err)
	}
	if file3AttrAfterSnap2.Attr.Inode != file3InoInitial {
		t.Fatalf("File3 inode changed after snap 2: expected %d, got %d", file3InoInitial, file3AttrAfterSnap2.Attr.Inode)
	}

	// Read and verify EROFS snapshot 2 image directly
	var img2Buf bytes.Buffer
	snap2Key := path.Join("volumes", volumeID, "meta", snap2Name)
	if err := backend.GetObject(ctx, "", snap2Key, 0, 0, &img2Buf); err != nil {
		t.Fatalf("Failed to fetch snap 2 bytes: %v", err)
	}
	reader2, err := erofs.NewReader(bytes.NewReader(img2Buf.Bytes()))
	if err != nil {
		t.Fatalf("Failed to parse snap 2 image: %v", err)
	}

	// Verify root dirents in snapshot 2
	rootDirents2, err := reader2.ListDirectory(reader2.GetRootNID())
	if err != nil {
		t.Fatalf("Failed to list root in snap 2: %v", err)
	}
	for _, de := range rootDirents2 {
		if de.Name == "file1.txt" && de.NID != file1InoInitial {
			t.Fatalf("Snap 2 file1.txt NID mismatch: expected %d, got %d", file1InoInitial, de.NID)
		}
		if de.Name == "dir1" && de.NID != dir1InoInitial {
			t.Fatalf("Snap 2 dir1 NID mismatch: expected %d, got %d", dir1InoInitial, de.NID)
		}
		if de.Name == "file3.txt" && de.NID != file3InoInitial {
			t.Fatalf("Snap 2 file3.txt NID mismatch: expected %d, got %d", file3InoInitial, de.NID)
		}
	}
}

func TestSDSStepReplayScratch(t *testing.T) {
	ctx := t.Context()
	walDir := t.TempDir()
	backend := NewMemoryBackend()
	volumeID := "sds-replay-vol"

	server := NewServer(backend, WithServerWAL(walDir, "", walclient.Local))

	// 1. Create hierarchy of directories and files
	_, err := testMkdir(ctx, server, volumeID, "/docs", 0755, 0, 0)
	if err != nil {
		t.Fatalf("Mkdir /docs failed: %v", err)
	}
	_, err = testMkdir(ctx, server, volumeID, "/docs/sub", 0755, 0, 0)
	if err != nil {
		t.Fatalf("Mkdir /docs/sub failed: %v", err)
	}
	_, err = testCreateFile(ctx, server, volumeID, "/hello.txt", 0644, []byte("initial hello"), 0, 0)
	if err != nil {
		t.Fatalf("CreateFile /hello.txt failed: %v", err)
	}
	_, err = testCreateFile(ctx, server, volumeID, "/docs/doc1.txt", 0644, []byte("doc1 content"), 0, 0)
	if err != nil {
		t.Fatalf("CreateFile /docs/doc1.txt failed: %v", err)
	}
	_, err = testCreateFile(ctx, server, volumeID, "/docs/sub/doc2.txt", 0644, []byte("doc2 content"), 0, 0)
	if err != nil {
		t.Fatalf("CreateFile /docs/sub/doc2.txt failed: %v", err)
	}

	// 2. Write, Truncate, Rename, Unlink
	_, err = testWriteFile(ctx, server, volumeID, "/hello.txt", 8, []byte("world!"), pb.WriteMode_WRITE_MODE_UNSPECIFIED)
	if err != nil {
		t.Fatalf("WriteFile failed: %v", err)
	}
	_, err = testTruncateFile(ctx, server, volumeID, "/docs/doc1.txt", 4)
	if err != nil {
		t.Fatalf("TruncateFile failed: %v", err)
	}
	_, err = testRename(ctx, server, volumeID, "/docs/sub/doc2.txt", "/docs/doc2_renamed.txt")
	if err != nil {
		t.Fatalf("Rename failed: %v", err)
	}
	_, err = testUnlink(ctx, server, volumeID, "/docs/doc1.txt")
	if err != nil {
		t.Fatalf("Unlink failed: %v", err)
	}
	_, err = testRmdir(ctx, server, volumeID, "/docs/sub")
	if err != nil {
		t.Fatalf("Rmdir failed: %v", err)
	}

	liveVol := server.GetVolume(volumeID)
	if liveVol == nil || liveVol.Stream() == nil {
		t.Fatalf("Expected live volume with active stream")
	}

	// Read all states from live volume
	liveHelloAttr, err := volGetAttr(ctx, liveVol, "/hello.txt")
	if err != nil {
		t.Fatalf("live GetAttr /hello.txt failed: %v", err)
	}
	liveHelloData, _, _, err := volReadFile(ctx, liveVol, "/hello.txt", 0, 100)
	if err != nil {
		t.Fatalf("live ReadFile /hello.txt failed: %v", err)
	}

	liveRenamedAttr, err := volGetAttr(ctx, liveVol, "/docs/doc2_renamed.txt")
	if err != nil {
		t.Fatalf("live GetAttr /docs/doc2_renamed.txt failed: %v", err)
	}
	liveRenamedData, _, _, err := volReadFile(ctx, liveVol, "/docs/doc2_renamed.txt", 0, 100)
	if err != nil {
		t.Fatalf("live ReadFile /docs/doc2_renamed.txt failed: %v", err)
	}

	// 3. Create fresh volume and replay SDS stream from scratch
	streamID := StreamIDForVolume(volumeID)
	replayedVol := NewVolume(volumeID, backend, NewEventBroadcaster())
	legStream, err := walclient.Open(ctx, walDir, streamID, "")
	if err != nil {
		t.Fatalf("Failed to open WAL stream for replay: %v", err)
	}
	defer func() { _ = legStream.Close() }()

	recovered := legStream.RecoveredRecords()
	sr := sds.NewStreamReader("", streamID, sds.WithRecoveredRecords(recovered))
	changes, err := sr.FeedRecovered(0)
	if err != nil {
		t.Fatalf("FeedRecovered failed: %v", err)
	}

	for _, c := range changes {
		if err := replayedVol.ApplySDSChangeLocked(ctx, c); err != nil {
			t.Fatalf("ApplySDSChangeLocked failed: %v", err)
		}
	}

	// 4. Verify replayed volume matches live volume
	repHelloAttr, err := volGetAttr(ctx, replayedVol, "/hello.txt")
	if err != nil {
		t.Fatalf("replayed GetAttr /hello.txt failed: %v", err)
	}
	if repHelloAttr.Inode != liveHelloAttr.Inode || repHelloAttr.Size != liveHelloAttr.Size {
		t.Fatalf("Replayed /hello.txt attr mismatch: %+v vs %+v", repHelloAttr, liveHelloAttr)
	}
	repHelloData, _, _, err := volReadFile(ctx, replayedVol, "/hello.txt", 0, 100)
	if err != nil || string(repHelloData) != string(liveHelloData) {
		t.Fatalf("Replayed /hello.txt data mismatch: %q vs %q", string(repHelloData), string(liveHelloData))
	}

	repRenamedAttr, err := volGetAttr(ctx, replayedVol, "/docs/doc2_renamed.txt")
	if err != nil {
		t.Fatalf("replayed GetAttr /docs/doc2_renamed.txt failed: %v", err)
	}
	if repRenamedAttr.Inode != liveRenamedAttr.Inode || repRenamedAttr.Size != liveRenamedAttr.Size {
		t.Fatalf("Replayed doc2_renamed attr mismatch: %+v vs %+v", repRenamedAttr, liveRenamedAttr)
	}
	repRenamedData, _, _, err := volReadFile(ctx, replayedVol, "/docs/doc2_renamed.txt", 0, 100)
	if err != nil || string(repRenamedData) != string(liveRenamedData) {
		t.Fatalf("Replayed doc2_renamed data mismatch: %q vs %q", string(repRenamedData), string(liveRenamedData))
	}

	// Verify unlinked file and rmdir'd dir do not exist
	_, err = volGetAttr(ctx, replayedVol, "/docs/doc1.txt")
	if err == nil {
		t.Fatalf("Expected /docs/doc1.txt to not exist in replayed volume")
	}
	_, err = volGetAttr(ctx, replayedVol, "/docs/sub")
	if err == nil {
		t.Fatalf("Expected /docs/sub to not exist in replayed volume")
	}

	_ = server.Close()
}

func TestSDSCatOnObjectFSStream(t *testing.T) {
	ctx := t.Context()
	walDir := t.TempDir()
	backend := NewMemoryBackend()
	volumeID := "sds-cat-vol"

	server := NewServer(backend, WithServerWAL(walDir, "", walclient.Local))

	// Perform changes that register Inode, DirEntry, Content
	_, err := testMkdir(ctx, server, volumeID, "/cats", 0755, 0, 0)
	if err != nil {
		t.Fatalf("Mkdir failed: %v", err)
	}
	_, err = testCreateFile(ctx, server, volumeID, "/cats/fluffy.txt", 0644, []byte("meow meow"), 0, 0)
	if err != nil {
		t.Fatalf("CreateFile failed: %v", err)
	}

	vol := server.GetVolume(volumeID)
	if vol == nil || vol.Stream() == nil {
		t.Fatalf("Expected active volume stream")
	}

	// Find the segment file
	files, err := filepath.Glob(filepath.Join(walDir, fmt.Sprintf("stream-%s-*.wal", vol.StreamID())))
	if err != nil || len(files) == 0 {
		t.Fatalf("No WAL segment file found in %s: %v", walDir, err)
	}

	// Read segment file with Decoder
	clientRecs, _, err := wal.ScanClientSegmentFile(files[0])
	if err != nil {
		t.Fatalf("Failed to scan segment file: %v", err)
	}

	reg := record.NewRegistry()
	dec := record.NewDecoder(record.WithDecoderRegistry(reg))

	var seenTypeDefs []string
	var seenOps int
	var seenCommits int

	for _, clientRec := range clientRecs {
		item, err := dec.Decode(clientRec.Payload)
		if err != nil {
			t.Fatalf("Decode error: %v", err)
		}
		if item.TypeID == record.TypeIDTypeDefinition {
			if def, ok := item.Message.(*sdsv1.TypeDefinition); ok {
				seenTypeDefs = append(seenTypeDefs, def.GetName())
			}
		}
		if item.TypeID == record.TypeIDOpRecord {
			seenOps++
		}
		if item.TypeID == record.TypeIDTxCommit {
			seenCommits++
		}
	}

	// Verify that TypeDefinitions for Inode, DirEntry, Content were announced in-band
	hasInodeDef := slices.Contains(seenTypeDefs, "objectfs.v1alpha1.Inode")
	hasDirDef := slices.Contains(seenTypeDefs, "objectfs.v1alpha1.DirEntry")
	hasContentDef := slices.Contains(seenTypeDefs, "objectfs.v1alpha1.Content")

	if !hasInodeDef || !hasDirDef || !hasContentDef {
		t.Fatalf("Expected Inode, DirEntry, and Content TypeDefinitions announced in stream, got %v", seenTypeDefs)
	}

	if seenOps < 3 {
		t.Fatalf("Expected at least 3 OpRecords in stream, got %d", seenOps)
	}
	if seenCommits < 2 {
		t.Fatalf("Expected at least 2 TxCommits in stream, got %d", seenCommits)
	}

	_ = server.Close()
}

func TestTruncateUpwardAndSparseRead(t *testing.T) {
	ctx := t.Context()
	backend := NewMemoryBackend()
	server := NewServer(backend)
	volumeID := "test-vol-truncate"

	// Create a file with small initial content
	_, err := testCreateFile(ctx, server, volumeID, "/sparse.bin", 0644, []byte("hello world"), 0, 0)
	if err != nil {
		t.Fatalf("Failed to create file: %v", err)
	}

	// Truncate up to 256KB across multiple chunks
	targetSize := int64(256 * 1024)
	truncResp, err := testTruncateFile(ctx, server, volumeID, "/sparse.bin", targetSize)
	if err != nil {
		t.Fatalf("Failed to truncate upward: %v", err)
	}
	if truncResp.GetError() != 0 {
		t.Fatalf("TruncateFile returned error: %d", truncResp.GetError())
	}

	// Read back whole file and verify size and contents
	readResp, err := testReadFile(ctx, server, volumeID, "/sparse.bin", 0, 0)
	if err != nil {
		t.Fatalf("Failed to read file: %v", err)
	}
	if readResp.GetTotalSize() != targetSize {
		t.Fatalf("Expected total size %d, got %d", targetSize, readResp.GetTotalSize())
	}
	if int64(len(readResp.GetData())) != targetSize {
		t.Fatalf("Expected data length %d, got %d", targetSize, len(readResp.GetData()))
	}
	if string(readResp.GetData()[:11]) != "hello world" {
		t.Fatalf("Expected prefix 'hello world', got %q", string(readResp.GetData()[:11]))
	}
	// Verify that the extended portion is all zeroes
	for i, b := range readResp.GetData()[11:] {
		if b != 0 {
			t.Fatalf("Expected zero byte at offset %d, got %d", 11+i, b)
		}
	}
}
