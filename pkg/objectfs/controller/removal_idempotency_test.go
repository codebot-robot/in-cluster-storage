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
	"time"

	pb "github.com/gke-labs/in-cluster-storage/pkg/api/objectfs/v1alpha1"
	"github.com/gke-labs/in-cluster-storage/pkg/wal/buffer/buffertest"
	walclient "github.com/gke-labs/in-cluster-storage/pkg/wal/client"
)

func TestParentRemovedByRequest(t *testing.T) {
	keyA, keyB := []byte("key-a"), []byte("key-b")
	parent := &pb.Inode{LastRemovalRequestId: keyA}
	noKey := &pb.Inode{}
	if !parentRemovedByRequest(parent, keyA) {
		t.Error("matching parent inode and key should match")
	}
	if parentRemovedByRequest(parent, keyB) || parentRemovedByRequest(parent, nil) ||
		parentRemovedByRequest(noKey, nil) || parentRemovedByRequest(noKey, keyA) ||
		parentRemovedByRequest(nil, keyA) {
		t.Error("mismatched key, empty key, or nil parent inode must not match")
	}
}

func TestRemovalIdempotency_UnlinkAndRmdir(t *testing.T) {
	ctx := t.Context()
	backend := NewMemoryBackend()
	bufferServer, cleanup := buffertest.Start(ctx, t, buffertest.WithBackend(backend))
	t.Cleanup(cleanup)

	const volumeID = "test-removal-idempotency"
	keyA, keyB, keyC := []byte("remove-key-a"), []byte("remove-key-b"), []byte("remove-key-c")
	newController := func() *Server {
		return NewTestServer(t, backend, WithServerWAL(t.TempDir(), bufferServer.Target, walclient.Witness),
			WithServerWALClient(bufferServer.Client), WithServerWALDialOptions(bufferServer.DialOpts...), WithServerMetadataDir(t.TempDir()))
	}

	server1 := newController()
	for _, name := range []string{"a", "b", "c"} {
		if created, err := server1.CreateFile(ctx, &pb.CreateFileRequest{VolumeId: volumeID, ParentInode: 1, Name: name, Mode: 0644}); err != nil || created.GetError() != 0 {
			t.Fatalf("CreateFile %s: err=%v errno=%d", name, err, created.GetError())
		}
	}
	if created, err := server1.Mkdir(ctx, &pb.MkdirRequest{VolumeId: volumeID, ParentInode: 1, Name: "sub", Mode: 0755}); err != nil || created.GetError() != 0 {
		t.Fatalf("Mkdir sub: err=%v errno=%d", err, created.GetError())
	}

	unlinkA := &pb.UnlinkRequest{VolumeId: volumeID, ParentInode: 1, Name: "a", RequestId: keyA}
	first, err := server1.Unlink(ctx, unlinkA)
	if err != nil || first.GetError() != 0 || !first.GetSuccess() {
		t.Fatalf("initial Unlink a: err=%v errno=%d success=%v", err, first.GetError(), first.GetSuccess())
	}
	parentTimes := func(volume *Volume) (time.Time, time.Time) {
		t.Helper()
		attr, err := volume.GetAttr(ctx, 1)
		if err != nil {
			t.Fatalf("GetAttr parent: %v", err)
		}
		return attr.GetInode().GetMtime().AsTime(), attr.GetInode().GetCtime().AsTime()
	}
	volume1 := mustGetVolume(t, server1, volumeID)
	mtimeBefore, ctimeBefore := parentTimes(volume1)
	positionBefore := volume1.Position()

	assertUnlinkRepeatUnchanged := func(label string, server *Server, wantPosition uint64) *Volume {
		t.Helper()
		repeat, err := server.Unlink(ctx, unlinkA)
		if err != nil || repeat.GetError() != 0 || !repeat.GetSuccess() || repeat.GetStreamPosition() != wantPosition {
			t.Fatalf("%s repeat Unlink a: err=%v errno=%d success=%v position=%d want=%d",
				label, err, repeat.GetError(), repeat.GetSuccess(), repeat.GetStreamPosition(), wantPosition)
		}
		volume := mustGetVolume(t, server, volumeID)
		if volume.Position() != wantPosition {
			t.Fatalf("%s repeat Unlink a wrote stream record: position=%d want=%d", label, volume.Position(), wantPosition)
		}
		if mtimeAfter, ctimeAfter := parentTimes(volume); !mtimeAfter.Equal(mtimeBefore) || !ctimeAfter.Equal(ctimeBefore) {
			t.Fatalf("%s repeat Unlink a mutated parent times: before=(%v,%v) after=(%v,%v)",
				label, mtimeBefore, ctimeBefore, mtimeAfter, ctimeAfter)
		}
		return volume
	}

	assertUnlinkRepeatUnchanged("same controller", server1, positionBefore)
	if _, err := server1.CreateSnapshot(ctx, &pb.CreateSnapshotRequest{VolumeId: volumeID}); err != nil {
		t.Fatalf("CreateSnapshot: %v", err)
	}
	if err := server1.Close(); err != nil {
		t.Fatalf("server1.Close: %v", err)
	}

	server2 := newController()
	if response, err := server2.GetAttr(ctx, &pb.GetAttrRequest{VolumeId: volumeID, Inode: 1}); err != nil || response.GetError() != 0 {
		t.Fatalf("server2 GetAttr: err=%v errno=%d", err, response.GetError())
	}
	positionAfterReload := mustGetVolume(t, server2, volumeID).Position()
	volume2 := assertUnlinkRepeatUnchanged("after snapshot reload", server2, positionAfterReload)
	if volume2.replaySnapshotLocation == "" {
		t.Fatal("expected second controller to load from snapshot")
	}
	for _, requestID := range [][]byte{keyB, nil} {
		if response, err := server2.Unlink(ctx, &pb.UnlinkRequest{VolumeId: volumeID, ParentInode: 1, Name: "a", RequestId: requestID}); err != nil || response.GetError() != int32(syscall.ENOENT) {
			t.Fatalf("Unlink a non-matching key: err=%v errno=%d, want ENOENT", err, response.GetError())
		}
	}

	// One-slot limit: a second keyed removal in the same directory overwrites the remembered key.
	unlinkB := &pb.UnlinkRequest{VolumeId: volumeID, ParentInode: 1, Name: "b", RequestId: keyB}
	if response, err := server2.Unlink(ctx, unlinkB); err != nil || response.GetError() != 0 || !response.GetSuccess() {
		t.Fatalf("Unlink b: err=%v errno=%d success=%v", err, response.GetError(), response.GetSuccess())
	}
	if response, err := server2.Unlink(ctx, unlinkA); err != nil || response.GetError() != int32(syscall.ENOENT) {
		t.Fatalf("Unlink a after Unlink b: err=%v errno=%d, want ENOENT", err, response.GetError())
	}
	// A removal without a key does not overwrite last_removal_request_id.
	if response, err := server2.Unlink(ctx, &pb.UnlinkRequest{VolumeId: volumeID, ParentInode: 1, Name: "c"}); err != nil || response.GetError() != 0 {
		t.Fatalf("Unlink c without key: err=%v errno=%d", err, response.GetError())
	}
	if response, err := server2.Unlink(ctx, unlinkB); err != nil || response.GetError() != 0 || !response.GetSuccess() {
		t.Fatalf("repeat Unlink b after keyless Unlink c: err=%v errno=%d success=%v", err, response.GetError(), response.GetSuccess())
	}

	rmdirSub := &pb.RmdirRequest{VolumeId: volumeID, ParentInode: 1, Name: "sub", RequestId: keyC}
	if response, err := server2.Rmdir(ctx, rmdirSub); err != nil || response.GetError() != 0 || !response.GetSuccess() {
		t.Fatalf("initial Rmdir sub: err=%v errno=%d success=%v", err, response.GetError(), response.GetSuccess())
	}
	rmdirPosition := volume2.Position()
	if repeat, err := server2.Rmdir(ctx, rmdirSub); err != nil || repeat.GetError() != 0 || !repeat.GetSuccess() || repeat.GetStreamPosition() != rmdirPosition || volume2.Position() != rmdirPosition {
		t.Fatalf("repeat Rmdir sub: err=%v errno=%d success=%v position=%d volume=%d want=%d",
			err, repeat.GetError(), repeat.GetSuccess(), repeat.GetStreamPosition(), volume2.Position(), rmdirPosition)
	}
}

func TestRemovalIdempotency_RepeatWaitsForDurability(t *testing.T) {
	ctx := t.Context()
	stream := &recordingStream{}
	server, volume := newMetadataDurabilityTestServer(t, stream, pb.MetadataDurability_OPERATION)

	if created, err := server.CreateFile(ctx, &pb.CreateFileRequest{VolumeId: volume.volumeID, ParentInode: 1, Name: "victim.txt", Mode: 0644}); err != nil || created.GetError() != 0 {
		t.Fatalf("CreateFile: err=%v errno=%d", err, created.GetError())
	}
	if created, err := server.Mkdir(ctx, &pb.MkdirRequest{VolumeId: volume.volumeID, ParentInode: 1, Name: "victim-dir", Mode: 0755}); err != nil || created.GetError() != 0 {
		t.Fatalf("Mkdir: err=%v errno=%d", err, created.GetError())
	}

	verifyWait := func(label string, call func() (bool, int32, uint64, error)) {
		t.Helper()
		stream.setWaitError(errors.New("wait interrupted"))
		if success, errno, _, err := call(); err == nil && errno == 0 && success {
			t.Fatalf("expected initial %s to fail when wait fails", label)
		}
		if success, errno, _, err := call(); err == nil && errno == 0 && success {
			t.Fatalf("expected repeat %s to fail while durability wait still fails", label)
		}
		appends, commitSequence := stream.appendCount.Load(), volume.Position()
		stream.setWaitError(nil)
		stream.waitCount.Store(0)
		stream.lastWaitSequence.Store(0)
		success, errno, position, err := call()
		if err != nil || errno != 0 || !success || position != commitSequence {
			t.Fatalf("repeat %s: err=%v errno=%d success=%v position=%d want=%d", label, err, errno, success, position, commitSequence)
		}
		if got := stream.waitCount.Load(); stream.appendCount.Load() != appends || got != 1 || stream.lastWaitSequence.Load() < commitSequence {
			t.Fatalf("repeat %s durability wait: appends=%d(want %d) count=%d(want 1) sequence=%d(want >=%d)",
				label, stream.appendCount.Load(), appends, got, stream.lastWaitSequence.Load(), commitSequence)
		}
	}

	unlinkRequest := &pb.UnlinkRequest{VolumeId: volume.volumeID, ParentInode: 1, Name: "victim.txt", RequestId: []byte("unlink-durability-key")}
	verifyWait("Unlink", func() (bool, int32, uint64, error) {
		response, err := server.Unlink(ctx, unlinkRequest)
		return response.GetSuccess(), response.GetError(), response.GetStreamPosition(), err
	})
	rmdirRequest := &pb.RmdirRequest{VolumeId: volume.volumeID, ParentInode: 1, Name: "victim-dir", RequestId: []byte("rmdir-durability-key")}
	verifyWait("Rmdir", func() (bool, int32, uint64, error) {
		response, err := server.Rmdir(ctx, rmdirRequest)
		return response.GetSuccess(), response.GetError(), response.GetStreamPosition(), err
	})
}
