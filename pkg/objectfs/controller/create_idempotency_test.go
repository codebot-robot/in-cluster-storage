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
	"errors"
	"syscall"
	"testing"

	pb "github.com/gke-labs/in-cluster-storage/pkg/api/objectfs/v1alpha1"
	"github.com/gke-labs/in-cluster-storage/pkg/wal/buffer/buffertest"
	walclient "github.com/gke-labs/in-cluster-storage/pkg/wal/client"
)

func verifyCreateRepeat(t *testing.T, label string, keyA, keyB []byte, call func(requestID []byte) (*pb.EntryAttr, int32, error)) *pb.EntryAttr {
	t.Helper()
	first, errno, err := call(keyA)
	if err != nil || errno != 0 {
		t.Fatalf("%s initial: err=%v errno=%d", label, err, errno)
	}
	repeat, errno, err := call(keyA)
	if err != nil || errno != 0 || repeat.GetInode().GetIno() != first.GetInode().GetIno() || repeat.GetInode().GetMode() != first.GetInode().GetMode() {
		t.Fatalf("%s repeat: err=%v errno=%d got=(%d,%o) want=(%d,%o)", label, err, errno, repeat.GetInode().GetIno(), repeat.GetInode().GetMode(), first.GetInode().GetIno(), first.GetInode().GetMode())
	}
	for _, requestID := range [][]byte{keyB, nil} {
		if _, errno, err := call(requestID); err != nil || errno != int32(syscall.EEXIST) {
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
	newController := func() *Server {
		return NewTestServer(t, backend, WithServerWAL(t.TempDir(), bufferServer.Target, walclient.Witness),
			WithServerWALClient(bufferServer.Client), WithServerWALDialOptions(bufferServer.DialOpts...), WithServerMetadataDir(t.TempDir()))
	}

	server1 := newController()
	first, err := server1.Mkdir(ctx, &pb.MkdirRequest{VolumeId: volumeID, ParentInode: 1, Name: "alpha", Mode: 0750, RequestId: keyA})
	if err != nil || first.GetError() != 0 {
		t.Fatalf("initial Mkdir: err=%v errno=%d", err, first.GetError())
	}
	parentBefore, err := mustGetVolume(t, server1, volumeID).GetAttr(ctx, 1)
	if err != nil {
		t.Fatalf("GetAttr parent before snapshot: %v", err)
	}
	if _, err := server1.CreateSnapshot(ctx, &pb.CreateSnapshotRequest{VolumeId: volumeID}); err != nil {
		t.Fatalf("CreateSnapshot: %v", err)
	}
	if err := server1.Close(); err != nil {
		t.Fatalf("server1.Close: %v", err)
	}

	server2 := newController()
	repeat, err := server2.Mkdir(ctx, &pb.MkdirRequest{VolumeId: volumeID, ParentInode: 1, Name: "alpha", Mode: 0750, RequestId: keyA})
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
	for _, requestID := range [][]byte{keyB, nil} {
		if response, err := server2.Mkdir(ctx, &pb.MkdirRequest{VolumeId: volumeID, ParentInode: 1, Name: "alpha", Mode: 0750, RequestId: requestID}); err != nil || response.GetError() != int32(syscall.EEXIST) {
			t.Fatalf("Mkdir non-matching key: err=%v errno=%d, want EEXIST", err, response.GetError())
		}
	}
	if response, err := server2.Symlink(ctx, &pb.SymlinkRequest{VolumeId: volumeID, ParentInode: 1, Name: "alpha", Target: "/elsewhere", RequestId: keyA}); err != nil || response.GetError() != int32(syscall.EEXIST) {
		t.Fatalf("Symlink on dir: err=%v errno=%d, want EEXIST", err, response.GetError())
	}
}

func TestCreateIdempotency_SymlinkAndLink(t *testing.T) {
	ctx := t.Context()
	server := NewTestServer(t, NewMemoryBackend())
	const volumeID = "test-create-idempotency"
	keyA, keyB := []byte("create-key-a"), []byte("create-key-b")

	verifyCreateRepeat(t, "Symlink", keyA, keyB, func(requestID []byte) (*pb.EntryAttr, int32, error) {
		response, err := server.Symlink(ctx, &pb.SymlinkRequest{VolumeId: volumeID, ParentInode: 1, Name: "sym", Target: "/target", RequestId: requestID})
		return response.GetAttr(), response.GetError(), err
	})

	var fileInodes [2]uint64
	for i, name := range []string{"base.txt", "other.txt"} {
		created, err := server.CreateFile(ctx, &pb.CreateFileRequest{VolumeId: volumeID, ParentInode: 1, Name: name, Mode: 0644})
		if err != nil || created.GetError() != 0 {
			t.Fatalf("CreateFile %s: err=%v errno=%d", name, err, created.GetError())
		}
		fileInodes[i] = created.GetAttr().GetInode().GetIno()
	}
	repeatLink := verifyCreateRepeat(t, "Link", keyA, keyB, func(requestID []byte) (*pb.EntryAttr, int32, error) {
		response, err := server.Link(ctx, &pb.LinkRequest{VolumeId: volumeID, OldInode: fileInodes[0], NewParentInode: 1, NewName: "hard", RequestId: requestID})
		return response.GetAttr(), response.GetError(), err
	})
	if repeatLink.GetInode().GetNlink() != 2 {
		t.Fatalf("repeat Link nlink: got %d, want 2", repeatLink.GetInode().GetNlink())
	}
	if response, err := server.Link(ctx, &pb.LinkRequest{VolumeId: volumeID, OldInode: fileInodes[1], NewParentInode: 1, NewName: "hard", RequestId: keyA}); err != nil || response.GetError() != int32(syscall.EEXIST) {
		t.Fatalf("Link different inode: err=%v errno=%d, want EEXIST", err, response.GetError())
	}
}

func TestCreateIdempotency_RepeatWaitsForDurability(t *testing.T) {
	ctx := t.Context()
	stream := &recordingStream{}
	server, volume := newMetadataDurabilityTestServer(t, stream, pb.MetadataDurability_OPERATION)

	request := &pb.MkdirRequest{VolumeId: volume.volumeID, ParentInode: 1, Name: "dir", Mode: 0755, RequestId: []byte("durability-key-1")}
	stream.setWaitError(errors.New("wait interrupted"))
	if response, err := server.Mkdir(ctx, request); err == nil && response.GetError() == 0 {
		t.Fatal("expected initial Mkdir to fail when wait fails")
	}
	if response, err := server.Mkdir(ctx, request); err == nil && response.GetError() == 0 {
		t.Fatal("expected repeat Mkdir to fail while durability wait still fails")
	}
	appends, commitSequence := stream.appendCount.Load(), volume.Position()
	stream.setWaitError(nil)
	stream.waitCount.Store(0)
	stream.lastWaitSequence.Store(0)
	if repeat, err := server.Mkdir(ctx, request); err != nil || repeat.GetError() != 0 {
		t.Fatalf("repeat Mkdir: err=%v errno=%d", err, repeat.GetError())
	}
	if got := stream.waitCount.Load(); stream.appendCount.Load() != appends || got != 1 || stream.lastWaitSequence.Load() < commitSequence {
		t.Fatalf("repeat Mkdir durability wait: appends=%d(want %d) count=%d(want 1) sequence=%d(want >=%d)",
			stream.appendCount.Load(), appends, got, stream.lastWaitSequence.Load(), commitSequence)
	}
}
