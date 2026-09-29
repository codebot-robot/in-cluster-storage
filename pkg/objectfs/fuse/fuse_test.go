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

package fuse

import (
	"context"
	"net"
	"syscall"
	"testing"

	pb "github.com/gke-labs/in-cluster-storage/pkg/api/objectfs/v1alpha1"
	"github.com/gke-labs/in-cluster-storage/pkg/objectfs/controller"
	"github.com/hanwen/go-fuse/v2/fuse"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

func createTestClient(t *testing.T) (pb.ObjectFSControllerClient, func()) {
	lis := bufconn.Listen(1024 * 1024)
	grpcServer := grpc.NewServer()
	server := controller.NewServer(nil)
	pb.RegisterObjectFSControllerServer(grpcServer, server)

	go func() {
		_ = grpcServer.Serve(lis)
	}()

	conn, err := grpc.NewClient("passthrough://bufnet",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return lis.Dial()
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("Failed to dial bufnet: %v", err)
	}

	client := pb.NewObjectFSControllerClient(conn)
	cleanup := func() {
		_ = conn.Close()
		grpcServer.Stop()
		_ = lis.Close()
	}
	return client, cleanup
}

func TestRawFileSystemOperations(t *testing.T) {
	client, cleanup := createTestClient(t)
	defer cleanup()

	cache := NewNodeCache(1024 * 1024)
	rawFS := NewObjectFS(client, "vol-1", pb.WriteMode_WRITE_THROUGH_FSYNC, cache)

	// 1. Getattr on root
	var attrOut fuse.AttrOut
	if status := rawFS.GetAttr(nil, &fuse.GetAttrIn{InHeader: fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID}}, &attrOut); status != fuse.OK {
		t.Fatalf("Getattr on root failed: %v", status)
	}
	if attrOut.Attr.Mode&syscall.S_IFDIR == 0 {
		t.Fatalf("Expected root to have S_IFDIR mode, got: %o", attrOut.Attr.Mode)
	}
	if attrOut.Attr.Nlink != 2 {
		t.Fatalf("Expected root Nlink to be 2, got: %d", attrOut.Attr.Nlink)
	}
	if attrOut.Attr.Owner.Uid != 0 || attrOut.Attr.Owner.Gid != 0 {
		t.Fatalf("Expected root Owner to be 0/0, got: %d/%d", attrOut.Attr.Owner.Uid, attrOut.Attr.Owner.Gid)
	}

	const testUID = 1001
	const testGID = 1002

	// 2. Mkdir "docs"
	var docsEntryOut fuse.EntryOut
	if status := rawFS.Mkdir(nil, &fuse.MkdirIn{
		InHeader: fuse.InHeader{
			NodeId: fuse.FUSE_ROOT_ID,
			Caller: fuse.Caller{Owner: fuse.Owner{Uid: testUID, Gid: testGID}},
		},
		Mode: 0755,
	}, "docs", &docsEntryOut); status != fuse.OK {
		t.Fatalf("Mkdir docs failed: %v", status)
	}
	if docsEntryOut.NodeId == 0 {
		t.Fatalf("Expected valid Inode id in EntryOut")
	}
	if docsEntryOut.Attr.Nlink != 2 {
		t.Fatalf("Expected dir Nlink to be 2, got: %d", docsEntryOut.Attr.Nlink)
	}
	if docsEntryOut.Attr.Owner.Uid != testUID || docsEntryOut.Attr.Owner.Gid != testGID {
		t.Fatalf("Expected dir Owner to match caller UID/GID, got: %d/%d", docsEntryOut.Attr.Owner.Uid, docsEntryOut.Attr.Owner.Gid)
	}

	// 3. Create file "docs/readme.txt"
	var fileCreateOut fuse.CreateOut
	if status := rawFS.Create(nil, &fuse.CreateIn{
		InHeader: fuse.InHeader{
			NodeId: docsEntryOut.NodeId,
			Caller: fuse.Caller{Owner: fuse.Owner{Uid: testUID, Gid: testGID}},
		},
		Mode: 0644,
	}, "readme.txt", &fileCreateOut); status != fuse.OK {
		t.Fatalf("Create file failed: %v", status)
	}
	fileID := fileCreateOut.EntryOut.NodeId
	if fileID == 0 {
		t.Fatalf("Expected valid file Inode id")
	}
	if fileCreateOut.EntryOut.Attr.Nlink != 1 {
		t.Fatalf("Expected file Nlink to be 1, got: %d", fileCreateOut.EntryOut.Attr.Nlink)
	}
	if fileCreateOut.EntryOut.Attr.Owner.Uid != testUID || fileCreateOut.EntryOut.Attr.Owner.Gid != testGID {
		t.Fatalf("Expected file Owner to match caller UID/GID, got: %d/%d", fileCreateOut.EntryOut.Attr.Owner.Uid, fileCreateOut.EntryOut.Attr.Owner.Gid)
	}

	// 4. Write data to file
	testData := []byte("Hello ObjectFS Raw FUSE!")
	written, status := rawFS.Write(nil, &fuse.WriteIn{InHeader: fuse.InHeader{NodeId: fileID}}, testData)
	if status != fuse.OK {
		t.Fatalf("Write failed: %v", status)
	}
	if int(written) != len(testData) {
		t.Fatalf("Expected %d bytes written, got %d", len(testData), written)
	}

	// 5. Read data back
	readBuf := make([]byte, 64)
	readRes, status := rawFS.Read(nil, &fuse.ReadIn{InHeader: fuse.InHeader{NodeId: fileID}, Size: 64}, readBuf)
	if status != fuse.OK {
		t.Fatalf("Read failed: %v", status)
	}
	resBytes, readStatus := readRes.Bytes(readBuf)
	if readStatus != fuse.OK {
		t.Fatalf("Read result status not OK: %v", readStatus)
	}
	if string(resBytes) != string(testData) {
		t.Fatalf("Read data mismatch: got %q, want %q", string(resBytes), string(testData))
	}

	// 6. ReadDir on "docs"
	dirEntries := fuse.NewDirEntryList(make([]byte, 4096), 0)
	if status := rawFS.ReadDir(nil, &fuse.ReadIn{InHeader: fuse.InHeader{NodeId: docsEntryOut.NodeId}}, dirEntries); status != fuse.OK {
		t.Fatalf("ReadDir failed: %v", status)
	}

	// 7. Flush / Fsync
	if status := rawFS.Flush(nil, &fuse.FlushIn{InHeader: fuse.InHeader{NodeId: fileID}}); status != fuse.OK {
		t.Fatalf("Flush failed: %v", status)
	}
	if status := rawFS.Fsync(nil, &fuse.FsyncIn{InHeader: fuse.InHeader{NodeId: fileID}}); status != fuse.OK {
		t.Fatalf("Fsync failed: %v", status)
	}

	// 8. Lookup "readme.txt" in "docs"
	var lookupOut fuse.EntryOut
	if status := rawFS.Lookup(nil, &fuse.InHeader{NodeId: docsEntryOut.NodeId}, "readme.txt", &lookupOut); status != fuse.OK {
		t.Fatalf("Lookup failed: %v", status)
	}
	if lookupOut.NodeId != fileID {
		t.Fatalf("Lookup returned node %d, want %d", lookupOut.NodeId, fileID)
	}
	if lookupOut.Attr.Nlink != 1 {
		t.Fatalf("Expected file Nlink to be 1 in lookup, got: %d", lookupOut.Attr.Nlink)
	}
	if lookupOut.Attr.Owner.Uid != testUID || lookupOut.Attr.Owner.Gid != testGID {
		t.Fatalf("Expected file Owner to match test UID/GID in lookup, got: %d/%d", lookupOut.Attr.Owner.Uid, lookupOut.Attr.Owner.Gid)
	}

	// 8a. Rmdir "docs" while non-empty should fail with ENOTEMPTY
	if st := rawFS.Rmdir(nil, &fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID}, "docs"); st != fuse.Status(syscall.ENOTEMPTY) {
		t.Fatalf("Rmdir on non-empty dir: expected ENOTEMPTY, got %v", st)
	}

	// 8b. Unlink "docs" (a directory) should fail with EISDIR
	if st := rawFS.Unlink(nil, &fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID}, "docs"); st != fuse.Status(syscall.EISDIR) {
		t.Fatalf("Unlink on directory: expected EISDIR, got %v", st)
	}

	// 8c. Rmdir "readme.txt" (a file) should fail with ENOTDIR
	if st := rawFS.Rmdir(nil, &fuse.InHeader{NodeId: docsEntryOut.NodeId}, "readme.txt"); st != fuse.Status(syscall.ENOTDIR) {
		t.Fatalf("Rmdir on file: expected ENOTDIR, got %v", st)
	}

	// 9. Unlink "readme.txt"
	if status := rawFS.Unlink(nil, &fuse.InHeader{NodeId: docsEntryOut.NodeId}, "readme.txt"); status != fuse.OK {
		t.Fatalf("Unlink failed: %v", status)
	}

	// 10. Rmdir "docs"
	if status := rawFS.Rmdir(nil, &fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID}, "docs"); status != fuse.OK {
		t.Fatalf("Rmdir failed: %v", status)
	}
}

func TestFUSERmdirAndUnlinkErrors(t *testing.T) {
	client, cleanup := createTestClient(t)
	defer cleanup()

	cache := NewNodeCache(1024 * 1024)
	rawFS := NewObjectFS(client, "vol-errors", pb.WriteMode_WRITE_THROUGH_FSYNC, cache)

	// Create directory /dir and child /dir/file
	var dirOut fuse.EntryOut
	if st := rawFS.Mkdir(nil, &fuse.MkdirIn{InHeader: fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID}, Mode: 0755}, "dir", &dirOut); st != fuse.OK {
		t.Fatalf("Mkdir failed: %v", st)
	}

	var fileOut fuse.CreateOut
	if st := rawFS.Create(nil, &fuse.CreateIn{InHeader: fuse.InHeader{NodeId: dirOut.NodeId}, Mode: 0644}, "file", &fileOut); st != fuse.OK {
		t.Fatalf("Create failed: %v", st)
	}

	// 1. Rmdir non-empty directory returns ENOTEMPTY
	if st := rawFS.Rmdir(nil, &fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID}, "dir"); st != fuse.Status(syscall.ENOTEMPTY) {
		t.Fatalf("Expected ENOTEMPTY for non-empty dir, got %v", st)
	}

	// 2. Unlink directory returns EISDIR
	if st := rawFS.Unlink(nil, &fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID}, "dir"); st != fuse.Status(syscall.EISDIR) {
		t.Fatalf("Expected EISDIR for unlinking directory, got %v", st)
	}

	// 3. Rmdir regular file returns ENOTDIR
	if st := rawFS.Rmdir(nil, &fuse.InHeader{NodeId: dirOut.NodeId}, "file"); st != fuse.Status(syscall.ENOTDIR) {
		t.Fatalf("Expected ENOTDIR for rmdir on regular file, got %v", st)
	}

	// 4. Rmdir nonexistent returns ENOENT
	if st := rawFS.Rmdir(nil, &fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID}, "nonexistent"); st != fuse.ENOENT {
		t.Fatalf("Expected ENOENT for nonexistent dir, got %v", st)
	}

	// 5. Unlink child file
	if st := rawFS.Unlink(nil, &fuse.InHeader{NodeId: dirOut.NodeId}, "file"); st != fuse.OK {
		t.Fatalf("Unlink file failed: %v", st)
	}

	// 6. Rmdir now-empty directory succeeds
	if st := rawFS.Rmdir(nil, &fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID}, "dir"); st != fuse.OK {
		t.Fatalf("Rmdir on empty dir failed: %v", st)
	}
}

func TestGrpcErrorToStatus(t *testing.T) {
	tests := []struct {
		name     string
		err      error
		expected fuse.Status
	}{
		{"nil error", nil, fuse.OK},
		{"not found", status.Error(codes.NotFound, "not found"), fuse.ENOENT},
		{"already exists", status.Error(codes.AlreadyExists, "already exists"), fuse.Status(syscall.EEXIST)},
		{"invalid argument", status.Error(codes.InvalidArgument, "invalid argument"), fuse.EINVAL},
		{"failed precondition", status.Error(codes.FailedPrecondition, "failed precondition"), fuse.EINVAL},
		{"permission denied", status.Error(codes.PermissionDenied, "permission denied"), fuse.EACCES},
		{"unauthenticated", status.Error(codes.Unauthenticated, "unauthenticated"), fuse.EACCES},
		{"unimplemented", status.Error(codes.Unimplemented, "unimplemented"), fuse.ENOSYS},
		{"deadline exceeded", status.Error(codes.DeadlineExceeded, "deadline"), fuse.Status(syscall.ETIMEDOUT)},
		{"canceled", status.Error(codes.Canceled, "canceled"), fuse.Status(syscall.EINTR)},
		{"resource exhausted", status.Error(codes.ResourceExhausted, "out of space"), fuse.Status(syscall.ENOSPC)},
		{"aborted", status.Error(codes.Aborted, "aborted"), fuse.Status(syscall.EBUSY)},
		{"unavailable", status.Error(codes.Unavailable, "unavailable"), fuse.Status(syscall.EBUSY)},
		{"internal generic", status.Error(codes.Internal, "internal disk corruption"), fuse.Status(syscall.EIO)},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := grpcErrorToStatus(tc.err)
			if got != tc.expected {
				t.Errorf("grpcErrorToStatus(%v) = %v, want %v", tc.err, got, tc.expected)
			}
		})
	}
}

func TestLocalWriteBufferingAndSync(t *testing.T) {
	client, cleanup := createTestClient(t)
	defer cleanup()

	cache := NewNodeCache(1024 * 1024)
	rawFS := NewObjectFS(client, "vol-buffering", pb.WriteMode_WRITE_THROUGH_FSYNC, cache)

	// Create file
	var createOut fuse.CreateOut
	if status := rawFS.Create(nil, &fuse.CreateIn{InHeader: fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID}, Mode: 0644}, "buffered.txt", &createOut); status != fuse.OK {
		t.Fatalf("Create failed: %v", status)
	}
	fileID := createOut.EntryOut.NodeId

	// Write locally
	localData := []byte("buffered content not yet on service")
	written, status := rawFS.Write(nil, &fuse.WriteIn{InHeader: fuse.InHeader{NodeId: fileID}}, localData)
	if status != fuse.OK || int(written) != len(localData) {
		t.Fatalf("Write failed: status=%v, written=%d", status, written)
	}

	// Verify local cache has it and marks it dirty
	entry, isDirty := cache.GetDirty("/buffered.txt")
	if !isDirty || string(entry.Data) != string(localData) {
		t.Fatalf("Expected dirty cache entry with local data")
	}

	// Direct controller ReadFile before sync should NOT have the written data yet (it has 0 bytes initial)
	ctx := t.Context()
	ctrlResp, err := client.ReadFile(ctx, &pb.ReadFileRequest{
		VolumeId: "vol-buffering",
		Path:     "/buffered.txt",
		Offset:   0,
		Size:     1024,
	})
	if err != nil {
		t.Fatalf("Controller read failed: %v", err)
	}
	if len(ctrlResp.GetData()) != 0 {
		t.Fatalf("Expected controller to have 0 bytes before sync, got: %q", string(ctrlResp.GetData()))
	}

	// Now call Flush / Fsync on the FUSE layer
	if status := rawFS.Flush(nil, &fuse.FlushIn{InHeader: fuse.InHeader{NodeId: fileID}}); status != fuse.OK {
		t.Fatalf("Flush failed: %v", status)
	}

	// Controller should now have the synced data
	ctrlResp2, err := client.ReadFile(ctx, &pb.ReadFileRequest{
		VolumeId: "vol-buffering",
		Path:     "/buffered.txt",
		Offset:   0,
		Size:     1024,
	})
	if err != nil {
		t.Fatalf("Controller read after flush failed: %v", err)
	}
	if string(ctrlResp2.GetData()) != string(localData) {
		t.Fatalf("Expected controller to have %q, got %q", string(localData), string(ctrlResp2.GetData()))
	}

	// Cache entry should no longer be dirty
	if _, isDirty := cache.GetDirty("/buffered.txt"); isDirty {
		t.Fatalf("Expected cache entry to be marked clean after flush")
	}
}

func TestFUSEAttributesOwnerAndNlink(t *testing.T) {
	client, cleanup := createTestClient(t)
	defer cleanup()

	const callerUID = 553677
	const callerGID = 1000

	cache := NewNodeCache(1024 * 1024)
	rawFS := NewObjectFS(client, "vol-attrs", pb.WriteMode_WRITE_THROUGH_FSYNC, cache)

	// 1. Root directory GetAttr (root has default 0/0 UID/GID in EROFS)
	var rootAttrOut fuse.AttrOut
	if status := rawFS.GetAttr(nil, &fuse.GetAttrIn{InHeader: fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID}}, &rootAttrOut); status != fuse.OK {
		t.Fatalf("GetAttr root failed: %v", status)
	}
	if rootAttrOut.Attr.Nlink != 2 {
		t.Fatalf("Root directory Nlink = %d, want 2", rootAttrOut.Attr.Nlink)
	}

	// 2. Mkdir with caller UID/GID
	var dirOut fuse.EntryOut
	mkdirIn := &fuse.MkdirIn{
		InHeader: fuse.InHeader{
			NodeId: fuse.FUSE_ROOT_ID,
			Caller: fuse.Caller{Owner: fuse.Owner{Uid: callerUID, Gid: callerGID}},
		},
		Mode: 0755,
	}
	if status := rawFS.Mkdir(nil, mkdirIn, "sub", &dirOut); status != fuse.OK {
		t.Fatalf("Mkdir failed: %v", status)
	}
	if dirOut.Attr.Nlink != 2 {
		t.Fatalf("Mkdir Nlink = %d, want 2", dirOut.Attr.Nlink)
	}
	if dirOut.Attr.Owner.Uid != callerUID || dirOut.Attr.Owner.Gid != callerGID {
		t.Fatalf("Mkdir Owner = %d/%d, want %d/%d", dirOut.Attr.Owner.Uid, dirOut.Attr.Owner.Gid, callerUID, callerGID)
	}

	// 3. Create regular file with caller UID/GID
	var createOut fuse.CreateOut
	createIn := &fuse.CreateIn{
		InHeader: fuse.InHeader{
			NodeId: fuse.FUSE_ROOT_ID,
			Caller: fuse.Caller{Owner: fuse.Owner{Uid: callerUID, Gid: callerGID}},
		},
		Mode: 0644,
	}
	if status := rawFS.Create(nil, createIn, "hello.txt", &createOut); status != fuse.OK {
		t.Fatalf("Create failed: %v", status)
	}
	if createOut.EntryOut.Attr.Nlink != 1 {
		t.Fatalf("Create Nlink = %d, want 1", createOut.EntryOut.Attr.Nlink)
	}
	if createOut.EntryOut.Attr.Owner.Uid != callerUID || createOut.EntryOut.Attr.Owner.Gid != callerGID {
		t.Fatalf("Create Owner = %d/%d, want %d/%d", createOut.EntryOut.Attr.Owner.Uid, createOut.EntryOut.Attr.Owner.Gid, callerUID, callerGID)
	}
	fileID := createOut.EntryOut.NodeId

	// 4. Mknod file with caller UID/GID
	var mknodOut fuse.EntryOut
	mknodIn := &fuse.MknodIn{
		InHeader: fuse.InHeader{
			NodeId: fuse.FUSE_ROOT_ID,
			Caller: fuse.Caller{Owner: fuse.Owner{Uid: callerUID, Gid: callerGID}},
		},
		Mode: 0644,
	}
	if status := rawFS.Mknod(nil, mknodIn, "mknod.txt", &mknodOut); status != fuse.OK {
		t.Fatalf("Mknod failed: %v", status)
	}
	if mknodOut.Attr.Nlink != 1 {
		t.Fatalf("Mknod Nlink = %d, want 1", mknodOut.Attr.Nlink)
	}
	if mknodOut.Attr.Owner.Uid != callerUID || mknodOut.Attr.Owner.Gid != callerGID {
		t.Fatalf("Mknod Owner = %d/%d, want %d/%d", mknodOut.Attr.Owner.Uid, mknodOut.Attr.Owner.Gid, callerUID, callerGID)
	}

	// 5. GetAttr on file
	var fileAttrOut fuse.AttrOut
	if status := rawFS.GetAttr(nil, &fuse.GetAttrIn{InHeader: fuse.InHeader{NodeId: fileID}}, &fileAttrOut); status != fuse.OK {
		t.Fatalf("GetAttr file failed: %v", status)
	}
	if fileAttrOut.Attr.Nlink != 1 {
		t.Fatalf("GetAttr file Nlink = %d, want 1", fileAttrOut.Attr.Nlink)
	}
	if fileAttrOut.Attr.Owner.Uid != callerUID || fileAttrOut.Attr.Owner.Gid != callerGID {
		t.Fatalf("GetAttr file Owner = %d/%d, want %d/%d", fileAttrOut.Attr.Owner.Uid, fileAttrOut.Attr.Owner.Gid, callerUID, callerGID)
	}

	// 6. SetAttr (truncate)
	var setAttrTruncOut fuse.AttrOut
	if status := rawFS.SetAttr(nil, &fuse.SetAttrIn{SetAttrInCommon: fuse.SetAttrInCommon{InHeader: fuse.InHeader{NodeId: fileID}, Valid: fuse.FATTR_SIZE, Size: 10}}, &setAttrTruncOut); status != fuse.OK {
		t.Fatalf("SetAttr truncate failed: %v", status)
	}
	if setAttrTruncOut.Attr.Nlink != 1 {
		t.Fatalf("SetAttr truncate Nlink = %d, want 1", setAttrTruncOut.Attr.Nlink)
	}
	if setAttrTruncOut.Attr.Owner.Uid != callerUID || setAttrTruncOut.Attr.Owner.Gid != callerGID {
		t.Fatalf("SetAttr truncate Owner = %d/%d, want %d/%d", setAttrTruncOut.Attr.Owner.Uid, setAttrTruncOut.Attr.Owner.Gid, callerUID, callerGID)
	}

	// 7. SetAttr (non-truncate / touch / mode)
	var setAttrOut fuse.AttrOut
	if status := rawFS.SetAttr(nil, &fuse.SetAttrIn{SetAttrInCommon: fuse.SetAttrInCommon{InHeader: fuse.InHeader{NodeId: fileID}, Valid: fuse.FATTR_MODE, Mode: 0600}}, &setAttrOut); status != fuse.OK {
		t.Fatalf("SetAttr failed: %v", status)
	}
	if setAttrOut.Attr.Nlink != 1 {
		t.Fatalf("SetAttr Nlink = %d, want 1", setAttrOut.Attr.Nlink)
	}
	if setAttrOut.Attr.Owner.Uid != callerUID || setAttrOut.Attr.Owner.Gid != callerGID {
		t.Fatalf("SetAttr Owner = %d/%d, want %d/%d", setAttrOut.Attr.Owner.Uid, setAttrOut.Attr.Owner.Gid, callerUID, callerGID)
	}

	// 8. Lookup directory and file
	var lookupDirOut fuse.EntryOut
	if status := rawFS.Lookup(nil, &fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID}, "sub", &lookupDirOut); status != fuse.OK {
		t.Fatalf("Lookup sub failed: %v", status)
	}
	if lookupDirOut.Attr.Nlink != 2 {
		t.Fatalf("Lookup dir Nlink = %d, want 2", lookupDirOut.Attr.Nlink)
	}
	if lookupDirOut.Attr.Owner.Uid != callerUID || lookupDirOut.Attr.Owner.Gid != callerGID {
		t.Fatalf("Lookup dir Owner = %d/%d, want %d/%d", lookupDirOut.Attr.Owner.Uid, lookupDirOut.Attr.Owner.Gid, callerUID, callerGID)
	}

	var lookupFileOut fuse.EntryOut
	if status := rawFS.Lookup(nil, &fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID}, "hello.txt", &lookupFileOut); status != fuse.OK {
		t.Fatalf("Lookup hello.txt failed: %v", status)
	}
	if lookupFileOut.Attr.Nlink != 1 {
		t.Fatalf("Lookup file Nlink = %d, want 1", lookupFileOut.Attr.Nlink)
	}
	if lookupFileOut.Attr.Owner.Uid != callerUID || lookupFileOut.Attr.Owner.Gid != callerGID {
		t.Fatalf("Lookup file Owner = %d/%d, want %d/%d", lookupFileOut.Attr.Owner.Uid, lookupFileOut.Attr.Owner.Gid, callerUID, callerGID)
	}

	// 9. ReadDirPlus on root
	dirList := fuse.NewDirEntryList(make([]byte, 4096), 0)
	if status := rawFS.ReadDirPlus(nil, &fuse.ReadIn{InHeader: fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID}}, dirList); status != fuse.OK {
		t.Fatalf("ReadDirPlus failed: %v", status)
	}
}
