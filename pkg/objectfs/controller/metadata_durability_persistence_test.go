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
	"path/filepath"
	"testing"

	pb "github.com/gke-labs/in-cluster-storage/pkg/api/objectfs/v1alpha1"
	"github.com/gke-labs/in-cluster-storage/pkg/sds"
	"github.com/gke-labs/in-cluster-storage/pkg/wal/buffer/buffertest"
	walclient "github.com/gke-labs/in-cluster-storage/pkg/wal/client"
	"google.golang.org/protobuf/proto"
)

// durabilityTestEnvironment manages shared backend, metadata, and buffer state across controller restarts.
type durabilityTestEnvironment struct {
	backend           *MemoryBackend
	metadataDirectory string
	walDirectory      string
	bufferTest        *buffertest.BufferTest
}

// newDurabilityTestEnvironment initializes a shared test environment for metadata durability tests.
func newDurabilityTestEnvironment(t *testing.T, ctx context.Context) *durabilityTestEnvironment {
	backend := NewMemoryBackend()
	bufferTest, _ := buffertest.Start(ctx, t, buffertest.WithBackend(backend))
	return &durabilityTestEnvironment{
		backend:           backend,
		metadataDirectory: filepath.Join(t.TempDir(), "metadata"),
		walDirectory:      t.TempDir(),
		bufferTest:        bufferTest,
	}
}

// newServer starts a Server within the test environment with the given default durability.
func (e *durabilityTestEnvironment) newServer(t *testing.T, defaultDurability pb.MetadataDurability) *Server {
	return NewTestServer(t, e.backend,
		WithServerMetadataDir(e.metadataDirectory),
		WithServerMetadataIndex("sqlite"),
		WithServerWAL(e.walDirectory, e.bufferTest.Target, walclient.Witness),
		WithServerWALDialOptions(e.bufferTest.DialOpts...),
		WithServerMetadataDurability(defaultDurability),
	)
}

// assertVolumeDurabilityInList asserts that ListVolumes contains the volume with the expected durability mode.
func assertVolumeDurabilityInList(ctx context.Context, t *testing.T, server *Server, volumeID string, expectedMode pb.MetadataDurability) {
	t.Helper()
	listResponse, err := server.ListVolumes(ctx, &pb.ListVolumesRequest{})
	if err != nil {
		t.Fatalf("ListVolumes failed: %v", err)
	}
	for _, volumeInfo := range listResponse.GetVolumes() {
		if volumeInfo.GetVolumeId() == volumeID {
			if volumeInfo.GetMetadataDurability() != expectedMode {
				t.Fatalf("expected volume %s to have mode %v in ListVolumes, got %v", volumeID, expectedMode, volumeInfo.GetMetadataDurability())
			}
			return
		}
	}
	t.Fatalf("volume %s not found in ListVolumes", volumeID)
}

// assertStoredVolumeFields verifies that VolumeInfo has the expected consistency and durability modes,
// and confirms that pre-existing fields (deletion marker and retention policy) survived read-modify-write.
// Requires no volume locks held; queries volume.metadataView directly under its internal synchronization.
func assertStoredVolumeFields(ctx context.Context, t *testing.T, volume *Volume, volumeInfoKey sds.Key, expectedConsistency pb.ConsistencyMode, expectedDurability pb.MetadataDurability) {
	t.Helper()
	storedMessage, ok, err := volume.metadataView.Get(ctx, VolumeInfoTypeName, volumeInfoKey)
	if err != nil || !ok || storedMessage == nil {
		t.Fatalf("failed to retrieve VolumeInfo: ok=%v, err=%v", ok, err)
	}
	storedVolumeInfo, ok := storedMessage.(*pb.VolumeInfo)
	if !ok {
		t.Fatalf("stored message is not *pb.VolumeInfo: %T", storedMessage)
	}
	if storedVolumeInfo.GetConsistencyMode() != expectedConsistency {
		t.Fatalf("expected ConsistencyMode %v, got %v", expectedConsistency, storedVolumeInfo.GetConsistencyMode())
	}
	if storedVolumeInfo.GetMetadataDurability() != expectedDurability {
		t.Fatalf("expected MetadataDurability %v, got %v", expectedDurability, storedVolumeInfo.GetMetadataDurability())
	}
	if !storedVolumeInfo.GetDeleted() {
		t.Fatalf("expected Deleted=true to survive read-modify-write, got false")
	}
	if storedVolumeInfo.GetRetention() != 42 {
		t.Fatalf("expected Retention=42 to survive read-modify-write, got %d", storedVolumeInfo.GetRetention())
	}
}

// TestMetadataDurability_SurvivesControllerRestart verifies that a volume's persisted
// metadata durability mode survives a controller restart and overrides the server default,
// while a volume without a stored mode falls back to the server default.
func TestMetadataDurability_SurvivesControllerRestart(t *testing.T) {
	ctx := t.Context()
	environment := newDurabilityTestEnvironment(t, ctx)

	// First server has server default BARRIER.
	firstServer := environment.newServer(t, pb.MetadataDurability_BARRIER)

	volumeWithStoredMode := "volume-persisted-mode"
	storedVolume, err := firstServer.getOrCreateVolume(volumeWithStoredMode)
	if err != nil {
		t.Fatalf("getOrCreateVolume failed for %s: %v", volumeWithStoredMode, err)
	}
	if err := storedVolume.PersistMetadataDurability(ctx, pb.MetadataDurability_OPERATION); err != nil {
		t.Fatalf("PersistMetadataDurability failed: %v", err)
	}

	volumeWithDefaultMode := "volume-default-mode"
	if _, err := firstServer.getOrCreateVolume(volumeWithDefaultMode); err != nil {
		t.Fatalf("getOrCreateVolume failed for %s: %v", volumeWithDefaultMode, err)
	}

	// Verify before restart: stored value beats default, nothing stored uses default.
	assertVolumeDurabilityInList(ctx, t, firstServer, volumeWithStoredMode, pb.MetadataDurability_OPERATION)
	assertVolumeDurabilityInList(ctx, t, firstServer, volumeWithDefaultMode, pb.MetadataDurability_BARRIER)

	if err := firstServer.Close(); err != nil {
		t.Fatalf("firstServer.Close failed: %v", err)
	}

	// Restart controller with server default set to BARRIER.
	secondServer := environment.newServer(t, pb.MetadataDurability_BARRIER)
	defer func() {
		if err := secondServer.Close(); err != nil {
			t.Errorf("secondServer.Close failed: %v", err)
		}
	}()

	restartedStoredVolume, err := secondServer.getOrCreateVolume(volumeWithStoredMode)
	if err != nil {
		t.Fatalf("getOrCreateVolume failed for %s: %v", volumeWithStoredMode, err)
	}
	if restartedStoredVolume.MetadataDurability() != pb.MetadataDurability_OPERATION {
		t.Fatalf("persisted durability mode did not survive restart: got %v, want OPERATION", restartedStoredVolume.MetadataDurability())
	}

	restartedDefaultVolume, err := secondServer.getOrCreateVolume(volumeWithDefaultMode)
	if err != nil {
		t.Fatalf("getOrCreateVolume failed for %s: %v", volumeWithDefaultMode, err)
	}
	if restartedDefaultVolume.MetadataDurability() != pb.MetadataDurability_BARRIER {
		t.Fatalf("default volume durability expected BARRIER, got %v", restartedDefaultVolume.MetadataDurability())
	}

	// Verify after restart: stored mode survived (OPERATION beats BARRIER default), default volume used controller default (BARRIER).
	assertVolumeDurabilityInList(ctx, t, secondServer, volumeWithStoredMode, pb.MetadataDurability_OPERATION)
	assertVolumeDurabilityInList(ctx, t, secondServer, volumeWithDefaultMode, pb.MetadataDurability_BARRIER)
}

// TestMetadataDurability_PreExistingFieldsSurviveReadModifyWrite verifies that writing
// VolumeInfo via PersistMetadataDurability and PersistConsistencyMode preserves all pre-existing
// fields in the stored row such as deletion markers and retention policies.
func TestMetadataDurability_PreExistingFieldsSurviveReadModifyWrite(t *testing.T) {
	ctx := t.Context()
	environment := newDurabilityTestEnvironment(t, ctx)
	server := environment.newServer(t, pb.MetadataDurability_OPERATION)
	defer func() {
		if err := server.Close(); err != nil {
			t.Errorf("server.Close failed: %v", err)
		}
	}()

	volumeID := "volume-pre-existing-fields"
	volume, err := server.getOrCreateVolume(volumeID)
	if err != nil {
		t.Fatalf("getOrCreateVolume failed: %v", err)
	}

	// Seed VolumeInfo with pre-existing fields: Deleted=true and Retention=42.
	if _, err := volume.persistVolumeInfo(ctx, func(volumeInfo *pb.VolumeInfo) {
		volumeInfo.Deleted = true
		volumeInfo.Retention = proto.Int32(42)
	}); err != nil {
		t.Fatalf("persistVolumeInfo seed failed: %v", err)
	}

	volumeInfoKey, err := pkVolumeInfo.Extract(&pb.VolumeInfo{VolumeId: volumeID})
	if err != nil {
		t.Fatalf("Extract VolumeInfo key failed: %v", err)
	}

	// 1. Update metadata durability and verify pre-existing fields survive.
	if err := volume.PersistMetadataDurability(ctx, pb.MetadataDurability_BARRIER); err != nil {
		t.Fatalf("PersistMetadataDurability failed: %v", err)
	}
	assertStoredVolumeFields(ctx, t, volume, volumeInfoKey, pb.ConsistencyMode_CONSISTENCY_MODE_UNSPECIFIED, pb.MetadataDurability_BARRIER)

	// 2. Update consistency mode and verify durability mode, deletion marker, and retention all survive.
	if err := volume.PersistConsistencyMode(ctx, pb.ConsistencyMode_CLOSE_TO_OPEN); err != nil {
		t.Fatalf("PersistConsistencyMode failed: %v", err)
	}
	assertStoredVolumeFields(ctx, t, volume, volumeInfoKey, pb.ConsistencyMode_CLOSE_TO_OPEN, pb.MetadataDurability_BARRIER)
}
