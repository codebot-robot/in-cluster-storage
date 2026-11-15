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
	walclient "github.com/gke-labs/in-cluster-storage/pkg/wal/client"
)

// TestVolumeRelease_GetAttrCompletesWhileDurabilityWaitBlocked verifies that
// Volume.Release does not hold the volume lock across its WAL durability wait,
// allowing concurrent operations like GetAttr to complete while Release is blocked.
func TestVolumeRelease_GetAttrCompletesWhileDurabilityWaitBlocked(t *testing.T) {
	ctx := t.Context()
	stream := &stopTestStream{}
	server := NewTestServer(t, NewMemoryBackend(),
		WithServerStreamFactory(func(volumeID string) (walclient.Stream, error) {
			return stream, nil
		}),
		WithServerDurability(walclient.Witness),
	)

	createResp, err := server.CreateFile(ctx, &pb.CreateFileRequest{
		VolumeId:    "test-release-durability-vol",
		ParentInode: 1,
		Name:        "orphan.txt",
		Mode:        0644,
	})
	if err != nil || createResp.GetError() != 0 {
		t.Fatalf("CreateFile failed: %v (code %d)", err, createResp.GetError())
	}
	ino := createResp.GetAttr().GetInode().GetIno()
	fh := createResp.GetFh()

	volume, err := server.getOrCreateVolume("test-release-durability-vol")
	if err != nil {
		t.Fatalf("getOrCreateVolume failed: %v", err)
	}

	if _, err := volume.Unlink(ctx, 1, "orphan.txt"); err != nil {
		t.Fatalf("Unlink failed: %v", err)
	}

	stream.waitEntered = make(chan struct{}, 1)
	stream.waitGate = make(chan struct{})

	releaseChannel := runAsync(func() error {
		return volume.Release(ctx, ino, fh)
	})

	select {
	case <-stream.waitEntered:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for Volume.Release to enter durability wait")
	}

	// While Release is blocked in durability wait, GetAttr on the same volume must complete.
	getattrChannel := runAsync(func() error {
		_, err := volume.GetAttr(ctx, 1)
		return err
	})
	if err := awaitResult(t, getattrChannel, "volume.GetAttr while Release durability wait is blocked"); err != nil {
		t.Fatalf("GetAttr failed: %v", err)
	}

	select {
	case earlyError := <-releaseChannel:
		t.Fatalf("Volume.Release returned before wait gate was released: %v", earlyError)
	default:
	}

	close(stream.waitGate)
	if err := awaitResult(t, releaseChannel, "Volume.Release after gate release"); err != nil {
		t.Fatalf("Volume.Release failed: %v", err)
	}
}
