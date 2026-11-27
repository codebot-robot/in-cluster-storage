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
	"sync"
	"testing"
	"time"

	pb "github.com/gke-labs/in-cluster-storage/pkg/api/objectfs/v1alpha1"
)

func TestVolume_StagedChunksDroppedOnOrphanReleaseAndRenameOverwrite(t *testing.T) {
	ctx := t.Context()
	server := NewTestServer(t, NewMemoryBackend())
	volume, err := server.getOrCreateVolume("orphan-vol")
	if err != nil {
		t.Fatalf("getOrCreateVolume failed: %v", err)
	}

	// Open file -> unlink while open -> stage write -> release last handle -> orphan deleted.
	createResponse, err := testCreateFile(ctx, server, "orphan-vol", "/orphan.bin", 0644, nil, 0, 0)
	if err != nil || createResponse.GetError() != 0 {
		t.Fatalf("CreateFile failed: err=%v code=%d", err, createResponse.GetError())
	}
	orphanInodeID := createResponse.GetAttr().GetInode().GetIno()
	if _, err := volume.Unlink(ctx, 1, "orphan.bin"); err != nil {
		t.Fatalf("Unlink failed: %v", err)
	}
	if _, _, _, _, err := volume.WriteFileWithEpoch(ctx, orphanInodeID, 0, []byte("staged-orphan"), pb.WriteMode_FSYNC); err != nil {
		t.Fatalf("WriteFileWithEpoch failed: %v", err)
	}
	if err := volume.Release(ctx, orphanInodeID, createResponse.GetFh()); err != nil {
		t.Fatalf("Release failed: %v", err)
	}
	if staged := volume.StagedBytes(); staged != 0 {
		t.Fatalf("expected 0 staged bytes after orphan release, got %d", staged)
	}

	// Rename overwriting a closed file with unsynced staged writes drops target staged chunks.
	targetResponse, err := testCreateFile(ctx, server, "orphan-vol", "/target.bin", 0644, nil, 0, 0)
	if err != nil || targetResponse.GetError() != 0 {
		t.Fatalf("CreateFile target failed: err=%v code=%d", err, targetResponse.GetError())
	}
	targetInodeID := targetResponse.GetAttr().GetInode().GetIno()
	if _, _, _, _, err := volume.WriteFileWithEpoch(ctx, targetInodeID, 0, []byte("old-target"), pb.WriteMode_FSYNC); err != nil {
		t.Fatalf("WriteFileWithEpoch target failed: %v", err)
	}
	if err := volume.Release(ctx, targetInodeID, targetResponse.GetFh()); err != nil {
		t.Fatalf("Release target failed: %v", err)
	}
	if _, err := testCreateFile(ctx, server, "orphan-vol", "/src.bin", 0644, nil, 0, 0); err != nil {
		t.Fatalf("CreateFile src failed: %v", err)
	}
	if _, err := volume.Rename(ctx, 1, "src.bin", 1, "target.bin"); err != nil {
		t.Fatalf("Rename failed: %v", err)
	}
	if staged := volume.StagedBytes(); staged != 0 {
		t.Fatalf("expected 0 staged bytes after rename overwrite, got %d", staged)
	}

	if err := volume.Stop(ctx); err != nil {
		t.Fatalf("volume.Stop failed after orphan release and rename overwrite: %v", err)
	}
}

func TestVolume_UnlinkPreservesStagedWritesWhileOpenOrHardLinked(t *testing.T) {
	ctx := t.Context()
	backend := NewMemoryBackend()
	server := NewTestServer(t, backend)
	volume, err := server.getOrCreateVolume("unlink-preserve-vol")
	if err != nil {
		t.Fatalf("getOrCreateVolume failed: %v", err)
	}

	// 1. Open file unlinked before close must keep staged writes readable until release.
	openResponse, err := testCreateFile(ctx, server, "unlink-preserve-vol", "/open.bin", 0644, nil, 0, 0)
	if err != nil || openResponse.GetError() != 0 {
		t.Fatalf("CreateFile open.bin failed: err=%v code=%d", err, openResponse.GetError())
	}
	openInodeID := openResponse.GetAttr().GetInode().GetIno()
	if _, _, _, _, err := volume.WriteFileWithEpoch(ctx, openInodeID, 0, []byte("open-data"), pb.WriteMode_FSYNC); err != nil {
		t.Fatalf("WriteFileWithEpoch open.bin failed: %v", err)
	}
	if _, err := volume.Unlink(ctx, 1, "open.bin"); err != nil {
		t.Fatalf("Unlink open.bin failed: %v", err)
	}
	if staged := volume.StagedBytes(); staged == 0 {
		t.Fatal("expected staged writes preserved while open handle exists")
	}
	if err := volume.Fsync(ctx, openInodeID); err != nil {
		t.Fatalf("Fsync open.bin failed: %v", err)
	}
	data, _, _, err := volume.ReadFile(ctx, openInodeID, 0, 9)
	if err != nil || !bytes.Equal(data, []byte("open-data")) {
		t.Fatalf("ReadFile on open unlinked file: data=%q err=%v, want %q", string(data), err, "open-data")
	}
	if err := volume.Release(ctx, openInodeID, openResponse.GetFh()); err != nil {
		t.Fatalf("Release open.bin failed: %v", err)
	}

	// 2. Hard-linked file unlinked via one path must keep staged writes for the remaining link.
	linkResponse, err := testCreateFile(ctx, server, "unlink-preserve-vol", "/link1.bin", 0644, nil, 0, 0)
	if err != nil || linkResponse.GetError() != 0 {
		t.Fatalf("CreateFile link1.bin failed: err=%v code=%d", err, linkResponse.GetError())
	}
	linkInodeID := linkResponse.GetAttr().GetInode().GetIno()
	if err := volume.Release(ctx, linkInodeID, linkResponse.GetFh()); err != nil {
		t.Fatalf("Release link1.bin failed: %v", err)
	}
	if _, err := volume.Link(ctx, linkInodeID, 1, "link2.bin"); err != nil {
		t.Fatalf("Link link2.bin failed: %v", err)
	}
	if _, _, _, _, err := volume.WriteFileWithEpoch(ctx, linkInodeID, 0, []byte("linked-data"), pb.WriteMode_FSYNC); err != nil {
		t.Fatalf("WriteFileWithEpoch link1.bin failed: %v", err)
	}
	if _, err := volume.Unlink(ctx, 1, "link1.bin"); err != nil {
		t.Fatalf("Unlink link1.bin failed: %v", err)
	}
	if staged := volume.StagedBytes(); staged == 0 {
		t.Fatal("expected staged writes preserved while second hard link exists")
	}
	if err := volume.Stop(ctx); err != nil {
		t.Fatalf("volume.Stop failed: %v", err)
	}

	recovered := NewTestServer(t, backend)
	readResponse, err := testReadFile(ctx, recovered, "unlink-preserve-vol", "/link2.bin", 0, 11)
	if err != nil || readResponse.GetError() != 0 || !bytes.Equal(readResponse.GetData(), []byte("linked-data")) {
		t.Fatalf("recovered link2.bin: err=%v code=%d data=%q, want %q", err, readResponse.GetError(), string(readResponse.GetData()), "linked-data")
	}
}

func TestVolume_SyncSkipsInodeDeletedDuringUpload(t *testing.T) {
	ctx := t.Context()
	backend := newRecordingBackend(NewMemoryBackend())
	gateEntered, gateRelease := make(chan struct{}, 4), make(chan struct{})
	backend.setGate(gateEntered, gateRelease)
	releaseGate := sync.OnceFunc(func() { close(gateRelease) })
	t.Cleanup(releaseGate)

	server := NewTestServer(t, backend)
	volume, err := server.getOrCreateVolume("concurrent-delete-vol")
	if err != nil {
		t.Fatalf("getOrCreateVolume failed: %v", err)
	}

	keepResponse, err := testCreateFile(ctx, server, "concurrent-delete-vol", "/keep.bin", 0644, nil, 0, 0)
	if err != nil || keepResponse.GetError() != 0 {
		t.Fatalf("CreateFile keep.bin failed: err=%v code=%d", err, keepResponse.GetError())
	}
	keepInodeID := keepResponse.GetAttr().GetInode().GetIno()
	if _, _, _, _, err := volume.WriteFileWithEpoch(ctx, keepInodeID, 0, []byte("keep-data"), pb.WriteMode_FSYNC); err != nil {
		t.Fatalf("WriteFileWithEpoch keep.bin failed: %v", err)
	}

	goneResponse, err := testCreateFile(ctx, server, "concurrent-delete-vol", "/gone.bin", 0644, nil, 0, 0)
	if err != nil || goneResponse.GetError() != 0 {
		t.Fatalf("CreateFile gone.bin failed: err=%v code=%d", err, goneResponse.GetError())
	}
	goneInodeID := goneResponse.GetAttr().GetInode().GetIno()
	if _, _, _, _, err := volume.WriteFileWithEpoch(ctx, goneInodeID, 0, []byte("gone-data"), pb.WriteMode_FSYNC); err != nil {
		t.Fatalf("WriteFileWithEpoch gone.bin failed: %v", err)
	}
	if err := volume.Release(ctx, goneInodeID, goneResponse.GetFh()); err != nil {
		t.Fatalf("Release gone.bin failed: %v", err)
	}

	syncDone := make(chan error, 1)
	go func() { syncDone <- volume.Fsync(ctx, keepInodeID) }()

	select {
	case <-gateEntered:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for pack upload gate")
	}

	if _, err := volume.Unlink(ctx, 1, "gone.bin"); err != nil {
		t.Fatalf("Unlink gone.bin during upload failed: %v", err)
	}
	releaseGate()

	if err := waitSyncDone(t, syncDone); err != nil {
		t.Fatalf("Fsync failed when another snapshotted inode was unlinked mid-upload: %v", err)
	}
	if staged := volume.StagedBytes(); staged != 0 {
		t.Fatalf("expected 0 staged bytes after sync, got %d", staged)
	}
	if err := volume.Stop(ctx); err != nil {
		t.Fatalf("volume.Stop failed: %v", err)
	}

	recovered := NewTestServer(t, backend)
	readResponse, err := testReadFile(ctx, recovered, "concurrent-delete-vol", "/keep.bin", 0, 9)
	if err != nil || readResponse.GetError() != 0 || !bytes.Equal(readResponse.GetData(), []byte("keep-data")) {
		t.Fatalf("recovered keep.bin: err=%v code=%d data=%q, want %q", err, readResponse.GetError(), string(readResponse.GetData()), "keep-data")
	}
}
