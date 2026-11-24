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
	"strings"
	"sync"
	"testing"
	"time"

	pb "github.com/gke-labs/in-cluster-storage/pkg/api/objectfs/v1alpha1"
	"github.com/gke-labs/in-cluster-storage/pkg/objectfs/blob"
	walclient "github.com/gke-labs/in-cluster-storage/pkg/wal/client"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// gatedPackUploadBackend wraps an ObjectStorageBackend to hold pack uploads until released.
type gatedPackUploadBackend struct {
	ObjectStorageBackend
	entered chan struct{}
	release chan struct{}
}

func (b *gatedPackUploadBackend) PutObject(ctx context.Context, volumeID, key string, stream blob.ByteStream) (string, error) {
	isPack := strings.Contains(key, "blobs/") || strings.HasSuffix(key, ".pack")
	etag, err := b.ObjectStorageBackend.PutObject(ctx, volumeID, key, stream)
	if isPack {
		select {
		case b.entered <- struct{}{}:
		default:
		}
		<-b.release
	}
	return etag, err
}

func TestVolumeClose_InFlightSyncStops(t *testing.T) {
	ctx := t.Context()
	backend := &gatedPackUploadBackend{
		ObjectStorageBackend: NewMemoryBackend(),
		entered:              make(chan struct{}, 1),
		release:              make(chan struct{}),
	}
	var releaseOnce sync.Once
	releaseUpload := func() {
		releaseOnce.Do(func() {
			close(backend.release)
		})
	}
	t.Cleanup(releaseUpload)

	stream := &recordingStream{}
	server := NewTestServer(t, backend,
		WithServerMetadataIndex("memory"),
		WithServerStreamFactory(func(string) (walclient.Stream, error) {
			return stream, nil
		}),
	)

	volumeID := "test-close-inflight-sync-stops"
	createResponse, err := testCreateFile(ctx, server, volumeID, "/file.bin", 0644, nil, 0, 0)
	if err != nil || createResponse.GetError() != 0 {
		t.Fatalf("CreateFile failed: err=%v code=%d", err, createResponse.GetError())
	}
	inode := createResponse.GetAttr().GetInode().GetIno()

	// Stage a single small chunk (within inline threshold).
	writeResponse, err := server.WriteFile(ctx, &pb.WriteFileRequest{
		VolumeId:  volumeID,
		Inode:     inode,
		Offset:    0,
		Data:      make([]byte, 128),
		WriteMode: pb.WriteMode_LAZY,
	})
	if err != nil || writeResponse.GetError() != 0 {
		t.Fatalf("WriteFile failed: err=%v code=%d", err, writeResponse.GetError())
	}

	type fsyncResult struct {
		response *pb.FsyncResponse
		err      error
	}
	syncDone := make(chan fsyncResult, 1)
	go func() {
		response, fsyncError := server.Fsync(ctx, &pb.FsyncRequest{
			VolumeId: volumeID,
			Inode:    inode,
			Blocking: true,
		})
		syncDone <- fsyncResult{response: response, err: fsyncError}
	}()

	select {
	case <-backend.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for pack upload to enter gate")
	}

	volume, err := server.getOrCreateVolume(volumeID)
	if err != nil {
		t.Fatalf("getOrCreateVolume failed: %v", err)
	}
	if err := volume.Close(); err != nil {
		t.Fatalf("volume.Close failed: %v", err)
	}

	appendsAtClose := stream.appendCount.Load()
	releaseUpload()

	var result fsyncResult
	select {
	case result = <-syncDone:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for Fsync to finish")
	}

	if status.Code(result.err) != codes.Unavailable {
		volume.stagingMu.Lock()
		lastSyncError := volume.lastSyncError[inode]
		volume.stagingMu.Unlock()
		t.Fatalf("expected Fsync status Unavailable after Close, got err=%v response=%v (lastSyncError=%v)",
			result.err, result.response, lastSyncError)
	}
	if appendsAfter := stream.appendCount.Load(); appendsAfter != appendsAtClose {
		t.Fatalf("expected no stream appends after Close (before=%d, after=%d)", appendsAtClose, appendsAfter)
	}
}
