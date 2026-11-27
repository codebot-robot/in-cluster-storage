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
	"context"
	"errors"
	"syscall"
	"testing"

	pb "github.com/gke-labs/in-cluster-storage/pkg/api/objectfs/v1alpha1"
	"github.com/gke-labs/in-cluster-storage/pkg/grpcutil"
	"github.com/gke-labs/in-cluster-storage/pkg/wal/buffer/buffertest"
	walclient "github.com/gke-labs/in-cluster-storage/pkg/wal/client"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

func withRequestID(ctx context.Context, requestID []byte) context.Context {
	return metadata.AppendToOutgoingContext(ctx, grpcutil.RequestIDMetadataKey, string(requestID))
}

func verifyCreateRepeat(t *testing.T, ctx context.Context, label string, keyA, keyB []byte, call func(context.Context) (*pb.EntryAttr, int32, error)) *pb.EntryAttr {
	t.Helper()
	first, errno, err := call(withRequestID(ctx, keyA))
	if err != nil || errno != 0 {
		t.Fatalf("%s initial: err=%v errno=%d", label, err, errno)
	}
	repeat, errno, err := call(withRequestID(ctx, keyA))
	if err != nil || errno != 0 || repeat.GetInode().GetIno() != first.GetInode().GetIno() || repeat.GetInode().GetMode() != first.GetInode().GetMode() {
		t.Fatalf("%s repeat: err=%v errno=%d got=(%d,%o) want=(%d,%o)", label, err, errno, repeat.GetInode().GetIno(), repeat.GetInode().GetMode(), first.GetInode().GetIno(), first.GetInode().GetMode())
	}
	for _, callCtx := range []context.Context{withRequestID(ctx, keyB), ctx} {
		if _, errno, err := call(callCtx); err != nil || errno != int32(syscall.EEXIST) {
			t.Fatalf("%s non-matching key: err=%v errno=%d, want EEXIST", label, err, errno)
		}
	}
	return repeat
}

func TestDirEntryCreatedByRequest(t *testing.T) {
	keyA, keyB := []byte("key-a"), []byte("key-b")
	entry := &pb.DirEntry{Mode: syscall.S_IFDIR | 0755, RequestId: keyA}
	noKey := &pb.DirEntry{Mode: syscall.S_IFDIR | 0755}
	if !dirEntryCreatedByRequest(entry, keyA, syscall.S_IFDIR) {
		t.Error("matching entry and key should match")
	}
	if dirEntryCreatedByRequest(entry, keyB, syscall.S_IFDIR) || dirEntryCreatedByRequest(entry, nil, syscall.S_IFDIR) ||
		dirEntryCreatedByRequest(noKey, nil, syscall.S_IFDIR) || dirEntryCreatedByRequest(noKey, keyA, syscall.S_IFDIR) ||
		dirEntryCreatedByRequest(entry, keyA, syscall.S_IFLNK) || dirEntryCreatedByRequest(nil, keyA, syscall.S_IFDIR) {
		t.Error("mismatched key, empty key, wrong kind, or nil entry must not match")
	}
}

func TestCreateIdempotency_MkdirAcrossSnapshotReload(t *testing.T) {
	ctx := t.Context()
	backend := NewMemoryBackend()
	bufferServer, cleanup := buffertest.Start(ctx, t, buffertest.WithBackend(backend))
	t.Cleanup(cleanup)

	const volumeID = "test-mkdir-idempotency"
	keyA, keyB := []byte("mkdir-key-a"), []byte("mkdir-key-b")
	newController := func() (*Server, pb.ObjectFSControllerClient) {
		server := NewTestServer(t, backend, WithServerWAL(t.TempDir(), bufferServer.Target, walclient.Witness),
			WithServerWALClient(bufferServer.Client), WithServerWALDialOptions(bufferServer.DialOpts...), WithServerMetadataDir(t.TempDir()))
		return server, newTestClient(t, server, grpc.UnaryInterceptor(grpcutil.RequestIDUnaryServerInterceptor()))
	}

	server1, client1 := newController()
	request := &pb.MkdirRequest{VolumeId: volumeID, ParentInode: 1, Name: "alpha", Mode: 0750}
	first, err := client1.Mkdir(withRequestID(ctx, keyA), request)
	if err != nil || first.GetError() != 0 {
		t.Fatalf("initial Mkdir: err=%v errno=%d", err, first.GetError())
	}
	parentBefore, err := mustGetVolume(t, server1, volumeID).GetAttr(ctx, 1)
	if err != nil {
		t.Fatalf("GetAttr parent before snapshot: %v", err)
	}
	if _, err := client1.CreateSnapshot(ctx, &pb.CreateSnapshotRequest{VolumeId: volumeID}); err != nil {
		t.Fatalf("CreateSnapshot: %v", err)
	}
	if err := server1.Close(); err != nil {
		t.Fatalf("server1.Close: %v", err)
	}

	server2, client2 := newController()
	repeat, err := client2.Mkdir(withRequestID(ctx, keyA), request)
	if err != nil || repeat.GetError() != 0 || repeat.GetAttr().GetInode().GetIno() != first.GetAttr().GetInode().GetIno() || repeat.GetAttr().GetInode().GetMode() != first.GetAttr().GetInode().GetMode() {
		t.Fatalf("repeat Mkdir: err=%v errno=%d got=(%d,%o) want=(%d,%o)", err, repeat.GetError(), repeat.GetAttr().GetInode().GetIno(), repeat.GetAttr().GetInode().GetMode(), first.GetAttr().GetInode().GetIno(), first.GetAttr().GetInode().GetMode())
	}
	volume2 := mustGetVolume(t, server2, volumeID)
	if volume2.replaySnapshotLocation == "" {
		t.Fatal("expected second controller to load from snapshot")
	}
	parentAfter, err := volume2.GetAttr(ctx, 1)
	if err != nil {
		t.Fatalf("GetAttr parent after repeat: %v", err)
	}
	if !parentAfter.GetInode().GetMtime().AsTime().Equal(parentBefore.GetInode().GetMtime().AsTime()) || parentAfter.GetInode().GetNlink() != parentBefore.GetInode().GetNlink() {
		t.Fatalf("repeat Mkdir mutated parent: before=(%v,%d) after=(%v,%d)", parentBefore.GetInode().GetMtime().AsTime(), parentBefore.GetInode().GetNlink(), parentAfter.GetInode().GetMtime().AsTime(), parentAfter.GetInode().GetNlink())
	}
	for _, callCtx := range []context.Context{withRequestID(ctx, keyB), ctx} {
		if response, err := client2.Mkdir(callCtx, request); err != nil || response.GetError() != int32(syscall.EEXIST) {
			t.Fatalf("Mkdir non-matching key: err=%v errno=%d, want EEXIST", err, response.GetError())
		}
	}
	if response, err := client2.Symlink(withRequestID(ctx, keyA), &pb.SymlinkRequest{VolumeId: volumeID, ParentInode: 1, Name: "alpha", Target: "/elsewhere"}); err != nil || response.GetError() != int32(syscall.EEXIST) {
		t.Fatalf("Symlink on dir: err=%v errno=%d, want EEXIST", err, response.GetError())
	}
}

func TestCreateIdempotency_SymlinkLinkSpecialFile(t *testing.T) {
	ctx := t.Context()
	client := newTestClient(t, NewTestServer(t, NewMemoryBackend()), grpc.UnaryInterceptor(grpcutil.RequestIDUnaryServerInterceptor()))
	const volumeID = "test-create-idempotency"
	keyA, keyB := []byte("create-key-a"), []byte("create-key-b")

	verifyCreateRepeat(t, ctx, "Symlink", keyA, keyB, func(callCtx context.Context) (*pb.EntryAttr, int32, error) {
		response, err := client.Symlink(callCtx, &pb.SymlinkRequest{VolumeId: volumeID, ParentInode: 1, Name: "sym", Target: "/target"})
		return response.GetAttr(), response.GetError(), err
	})

	var fileInodes [2]uint64
	for i, name := range []string{"base.txt", "other.txt"} {
		created, err := client.CreateFile(ctx, &pb.CreateFileRequest{VolumeId: volumeID, ParentInode: 1, Name: name, Mode: 0644})
		if err != nil || created.GetError() != 0 {
			t.Fatalf("CreateFile %s: err=%v errno=%d", name, err, created.GetError())
		}
		fileInodes[i] = created.GetAttr().GetInode().GetIno()
	}
	repeatLink := verifyCreateRepeat(t, ctx, "Link", keyA, keyB, func(callCtx context.Context) (*pb.EntryAttr, int32, error) {
		response, err := client.Link(callCtx, &pb.LinkRequest{VolumeId: volumeID, OldInode: fileInodes[0], NewParentInode: 1, NewName: "hard"})
		return response.GetAttr(), response.GetError(), err
	})
	if repeatLink.GetInode().GetNlink() != 2 {
		t.Fatalf("repeat Link nlink: got %d, want 2", repeatLink.GetInode().GetNlink())
	}
	if response, err := client.Link(withRequestID(ctx, keyA), &pb.LinkRequest{VolumeId: volumeID, OldInode: fileInodes[1], NewParentInode: 1, NewName: "hard"}); err != nil || response.GetError() != int32(syscall.EEXIST) {
		t.Fatalf("Link different inode: err=%v errno=%d, want EEXIST", err, response.GetError())
	}

	verifyCreateRepeat(t, ctx, "SpecialFile", keyA, keyB, func(callCtx context.Context) (*pb.EntryAttr, int32, error) {
		response, err := client.CreateFile(callCtx, &pb.CreateFileRequest{VolumeId: volumeID, ParentInode: 1, Name: "pipe", Mode: syscall.S_IFIFO | 0640})
		return response.GetAttr(), response.GetError(), err
	})
	if response, err := client.CreateFile(withRequestID(ctx, keyA), &pb.CreateFileRequest{VolumeId: volumeID, ParentInode: 1, Name: "pipe", Mode: syscall.S_IFSOCK | 0640}); err != nil || response.GetError() != int32(syscall.EEXIST) {
		t.Fatalf("SpecialFile wrong type: err=%v errno=%d, want EEXIST", err, response.GetError())
	}
}

func TestCreateIdempotency_RepeatWaitsForDurability(t *testing.T) {
	ctx := t.Context()
	stream := &recordingStream{}
	server, volume := newMetadataDurabilityTestServer(t, stream, pb.MetadataDurability_OPERATION)
	client := newTestClient(t, server, grpc.UnaryInterceptor(grpcutil.RequestIDUnaryServerInterceptor()))

	key := []byte("durability-key-1")
	request := &pb.MkdirRequest{VolumeId: volume.volumeID, ParentInode: 1, Name: "dir", Mode: 0755}
	stream.setWaitError(errors.New("wait interrupted"))
	if response, err := client.Mkdir(withRequestID(ctx, key), request); err == nil && response.GetError() == 0 {
		t.Fatal("expected initial Mkdir to fail when wait fails")
	}
	if response, err := client.Mkdir(withRequestID(ctx, key), request); err == nil && response.GetError() == 0 {
		t.Fatal("expected repeat Mkdir to fail while durability wait still fails")
	}
	appends, commitSeq := stream.appendCount.Load(), volume.Position()
	stream.setWaitError(nil)
	stream.waitCount.Store(0)
	stream.lastWaitSequence.Store(0)
	if repeat, err := client.Mkdir(withRequestID(ctx, key), request); err != nil || repeat.GetError() != 0 {
		t.Fatalf("repeat Mkdir: err=%v errno=%d", err, repeat.GetError())
	}
	if got := stream.waitCount.Load(); stream.appendCount.Load() != appends || got != 1 || stream.lastWaitSequence.Load() < commitSeq {
		t.Fatalf("repeat Mkdir durability wait: appends=%d(want %d) count=%d(want 1) seq=%d(want >=%d)",
			stream.appendCount.Load(), appends, got, stream.lastWaitSequence.Load(), commitSeq)
	}
}
