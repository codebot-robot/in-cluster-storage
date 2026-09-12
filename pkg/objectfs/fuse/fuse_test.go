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
	"github.com/hanwen/go-fuse/v2/fs"
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

func TestFSNodeOperations(t *testing.T) {
	client, cleanup := createTestClient(t)
	defer cleanup()

	ctx := t.Context()
	cache := NewNodeCache(1024 * 1024)
	root := NewRootNode(client, "vol-1", pb.WriteMode_WRITE_THROUGH_FSYNC, cache)
	_ = fs.NewNodeFS(root, &fs.Options{})

	// 1. Getattr on root
	var attrOut fuse.AttrOut
	if errno := root.Getattr(ctx, nil, &attrOut); errno != 0 {
		t.Fatalf("Getattr on root failed: %v", errno)
	}
	if attrOut.Mode&syscall.S_IFDIR == 0 {
		t.Fatalf("Expected root to have S_IFDIR mode, got: %o", attrOut.Mode)
	}

	// 2. Mkdir "docs"
	var entryOut fuse.EntryOut
	docsNode, errno := root.Mkdir(ctx, "docs", 0755, &entryOut)
	if errno != 0 {
		t.Fatalf("Mkdir docs failed: %v", errno)
	}
	if entryOut.NodeId == 0 {
		t.Fatalf("Expected valid Inode id in EntryOut")
	}

	// 3. Create file "docs/readme.txt"
	docsEmbedder, ok := docsNode.Operations().(*FSNode)
	if !ok {
		t.Fatalf("Expected docsNode operations to be *FSNode")
	}

	var fileEntryOut fuse.EntryOut
	fileNode, _, _, errno := docsEmbedder.Create(ctx, "readme.txt", 0, 0644, &fileEntryOut)
	if errno != 0 {
		t.Fatalf("Create file failed: %v", errno)
	}

	fileEmbedder, ok := fileNode.Operations().(*FSNode)
	if !ok {
		t.Fatalf("Expected fileNode operations to be *FSNode")
	}

	// 4. Write data to file
	testData := []byte("Hello ObjectFS FUSE!")
	written, errno := fileEmbedder.Write(ctx, testData, 0)
	if errno != 0 {
		t.Fatalf("Write failed: %v", errno)
	}
	if int(written) != len(testData) {
		t.Fatalf("Expected %d bytes written, got %d", len(testData), written)
	}

	// 5. Read data back
	readBuf := make([]byte, 64)
	readRes, errno := fileEmbedder.Read(ctx, readBuf, 0)
	if errno != 0 {
		t.Fatalf("Read failed: %v", errno)
	}
	resBytes, status := readRes.Bytes(readBuf)
	if status != fuse.OK {
		t.Fatalf("Read status not OK: %v", status)
	}
	if string(resBytes) != string(testData) {
		t.Fatalf("Read data mismatch: got %q, want %q", string(resBytes), string(testData))
	}

	// 6. Readdir on "docs"
	dirStream, errno := docsEmbedder.Readdir(ctx)
	if errno != 0 {
		t.Fatalf("Readdir failed: %v", errno)
	}
	var entries []string
	for dirStream.HasNext() {
		entry, statusErrno := dirStream.Next()
		if statusErrno != fs.OK {
			t.Fatalf("Next entry failed: %v", statusErrno)
		}
		entries = append(entries, entry.Name)
	}
	if len(entries) != 1 || entries[0] != "readme.txt" {
		t.Fatalf("Readdir unexpected entries: %v", entries)
	}

	// 7. Flush / Fsync
	if errno := fileEmbedder.Flush(ctx); errno != 0 {
		t.Fatalf("Flush failed: %v", errno)
	}
	if errno := fileEmbedder.Fsync(ctx, 0); errno != 0 {
		t.Fatalf("Fsync failed: %v", errno)
	}

	// 8. Unlink "readme.txt"
	if errno := docsEmbedder.Unlink(ctx, "readme.txt"); errno != 0 {
		t.Fatalf("Unlink failed: %v", errno)
	}

	// 9. Rmdir "docs"
	if errno := root.Rmdir(ctx, "docs"); errno != 0 {
		t.Fatalf("Rmdir failed: %v", errno)
	}
}
