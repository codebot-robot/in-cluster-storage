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
	"google.golang.org/grpc/credentials/insecure"
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
	if status := rawFS.GetAttr(nil, &fuse.GetAttrIn{NodeId: fuse.FUSE_ROOT_ID}, &attrOut); status != fuse.OK {
		t.Fatalf("Getattr on root failed: %v", status)
	}
	if attrOut.Attr.Mode&syscall.S_IFDIR == 0 {
		t.Fatalf("Expected root to have S_IFDIR mode, got: %o", attrOut.Attr.Mode)
	}

	// 2. Mkdir "docs"
	var docsEntryOut fuse.EntryOut
	if status := rawFS.Mkdir(nil, &fuse.MkdirIn{InHeader: fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID}, Mode: 0755}, "docs", &docsEntryOut); status != fuse.OK {
		t.Fatalf("Mkdir docs failed: %v", status)
	}
	if docsEntryOut.NodeId == 0 {
		t.Fatalf("Expected valid Inode id in EntryOut")
	}

	// 3. Create file "docs/readme.txt"
	var fileCreateOut fuse.CreateOut
	if status := rawFS.Create(nil, &fuse.CreateIn{InHeader: fuse.InHeader{NodeId: docsEntryOut.NodeId}, Mode: 0644}, "readme.txt", &fileCreateOut); status != fuse.OK {
		t.Fatalf("Create file failed: %v", status)
	}
	fileID := fileCreateOut.EntryOut.NodeId
	if fileID == 0 {
		t.Fatalf("Expected valid file Inode id")
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

	// 9. Unlink "readme.txt"
	if status := rawFS.Unlink(nil, &fuse.InHeader{NodeId: docsEntryOut.NodeId}, "readme.txt"); status != fuse.OK {
		t.Fatalf("Unlink failed: %v", status)
	}

	// 10. Rmdir "docs"
	if status := rawFS.Rmdir(nil, &fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID}, "docs"); status != fuse.OK {
		t.Fatalf("Rmdir failed: %v", status)
	}
}
