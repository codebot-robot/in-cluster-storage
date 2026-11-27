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
	"testing"

	pb "github.com/gke-labs/in-cluster-storage/pkg/api/objectfs/v1alpha1"
	walclient "github.com/gke-labs/in-cluster-storage/pkg/wal/client"
)

func newServerWithWALDurability(t *testing.T, environment *durabilityTestEnvironment, durability walclient.Level) *Server {
	t.Helper()
	return NewTestServer(t, environment.backend,
		WithServerMetadataDir(environment.metadataDirectory),
		WithServerMetadataIndex("sqlite"),
		WithServerWAL(environment.walDirectory, environment.bufferTest.Target, durability),
		WithServerWALDialOptions(environment.bufferTest.DialOpts...),
	)
}

func assertVolumeWALDurability(ctx context.Context, t *testing.T, server *Server, volumeID string, expectedLevel pb.WalDurability) {
	t.Helper()
	list, err := server.ListVolumes(ctx, &pb.ListVolumesRequest{})
	if err != nil {
		t.Fatalf("ListVolumes: %v", err)
	}
	for _, info := range list.GetVolumes() {
		if info.GetVolumeId() == volumeID {
			if info.GetWalDurability() != expectedLevel {
				t.Fatalf("expected volume %s wal durability %v in ListVolumes, got %v", volumeID, expectedLevel, info.GetWalDurability())
			}
			return
		}
	}
	t.Fatalf("volume %s not found in ListVolumes", volumeID)
}

func assertVolumeDurabilitySettings(t *testing.T, volume *Volume, expectedLevel, expectedStored pb.WalDurability) {
	t.Helper()
	if volume.WalDurability() != expectedLevel || volume.StoredWalDurability() != expectedStored {
		t.Fatalf("volume %s: got WalDurability=%v, Stored=%v; want %v, %v",
			volume.volumeID, volume.WalDurability(), volume.StoredWalDurability(),
			expectedLevel, expectedStored)
	}
}

// TestWALDurability_WaitsUseEffectiveLevel verifies that persisting a WAL durability level
// updates the effective level used by durability waits, including subsequent blocking fsyncs.
func TestWALDurability_WaitsUseEffectiveLevel(t *testing.T) {
	ctx := t.Context()
	stream := &recordingStream{}
	server := NewTestServer(t, NewMemoryBackend(),
		WithServerStreamFactory(func(string) (walclient.Stream, error) { return stream, nil }),
		WithServerDurability(walclient.Local),
	)
	defer func() {
		if err := server.Close(); err != nil {
			t.Errorf("server.Close: %v", err)
		}
	}()

	volume, err := server.getOrCreateVolume("vol-effective-level")
	if err != nil {
		t.Fatalf("getOrCreateVolume: %v", err)
	}

	var lastObservedCount int64
	assertWaitAdvanced := func(expected walclient.Level) {
		t.Helper()
		count := stream.waitCount.Load()
		if count <= lastObservedCount {
			t.Fatalf("expected wait count > %d, got %d", lastObservedCount, count)
		}
		lastObservedCount = count
		levelValue := stream.lastWaitLevel.Load()
		if levelValue == nil {
			t.Fatalf("no wait level recorded")
		}
		if recordedLevel, ok := levelValue.(walclient.Level); !ok || recordedLevel != expected {
			t.Fatalf("expected wait level %v, got %v", expected, levelValue)
		}
	}

	if err := volume.waitForSequence(ctx, 1); err != nil {
		t.Fatalf("waitForSequence: %v", err)
	}
	assertWaitAdvanced(walclient.Local)

	if err := volume.PersistWalDurability(ctx, pb.WalDurability_WITNESS); err != nil {
		t.Fatalf("PersistWalDurability: %v", err)
	}
	assertWaitAdvanced(walclient.Witness)
	if volume.StoredWalDurability() != pb.WalDurability_WITNESS {
		t.Fatalf("expected StoredWalDurability WITNESS, got %v", volume.StoredWalDurability())
	}
	if err := volume.waitForSequence(ctx, 2); err != nil {
		t.Fatalf("waitForSequence: %v", err)
	}
	assertWaitAdvanced(walclient.Witness)

	fileAttr, err := volume.CreateFile(ctx, &pb.CreateFileRequest{ParentInode: 1, Name: "staged-file", Mode: 0644})
	if err != nil {
		t.Fatalf("CreateFile: %v", err)
	}
	inodeID := fileAttr.GetInode().GetIno()
	if _, _, _, _, err := volume.WriteFileWithEpoch(ctx, inodeID, 0, []byte("data"), pb.WriteMode_LAZY); err != nil {
		t.Fatalf("WriteFileWithEpoch: %v", err)
	}
	if err := volume.Fsync(ctx, inodeID); err != nil {
		t.Fatalf("Fsync: %v", err)
	}
	assertWaitAdvanced(walclient.Witness)

	if err := volume.PersistWalDurability(ctx, pb.WalDurability_PERMANENT); err != nil {
		t.Fatalf("PersistWalDurability: %v", err)
	}
	assertWaitAdvanced(walclient.Permanent)
	if volume.StoredWalDurability() != pb.WalDurability_PERMANENT {
		t.Fatalf("expected StoredWalDurability PERMANENT, got %v", volume.StoredWalDurability())
	}
	if err := volume.waitForSequence(ctx, 3); err != nil {
		t.Fatalf("waitForSequence: %v", err)
	}
	assertWaitAdvanced(walclient.Permanent)
}

// TestWALDurability_PersistRejections verifies that persisting UNSPECIFIED or an unknown value returns an error and changes nothing.
func TestWALDurability_PersistRejections(t *testing.T) {
	ctx := t.Context()
	server := NewTestServer(t, NewMemoryBackend(), WithServerDurability(walclient.Local))
	defer func() {
		if err := server.Close(); err != nil {
			t.Errorf("server.Close: %v", err)
		}
	}()
	volume, err := server.getOrCreateVolume("vol-reject")
	if err != nil {
		t.Fatalf("getOrCreateVolume: %v", err)
	}
	for _, invalid := range []pb.WalDurability{pb.WalDurability_WAL_DURABILITY_UNSPECIFIED, pb.WalDurability(999)} {
		if err := volume.PersistWalDurability(ctx, invalid); err == nil {
			t.Fatalf("expected error for invalid durability %v, got nil", invalid)
		}
		assertVolumeDurabilitySettings(t, volume, pb.WalDurability_LOCAL, pb.WalDurability_WAL_DURABILITY_UNSPECIFIED)
	}
}

// TestWALDurability_SurvivesRestartAndControllerDefault verifies that stored WAL durability
// survives restart and overrides the controller default, while an unstored volume adopts the controller default.
func TestWALDurability_SurvivesRestartAndControllerDefault(t *testing.T) {
	ctx := t.Context()
	environment := newDurabilityTestEnvironment(t, ctx)

	server1 := newServerWithWALDurability(t, environment, walclient.Local)
	storedVolume, err := server1.getOrCreateVolume("vol-stored")
	if err != nil {
		t.Fatalf("getOrCreateVolume: %v", err)
	}
	if err := storedVolume.PersistWalDurability(ctx, pb.WalDurability_WITNESS); err != nil {
		t.Fatalf("PersistWalDurability: %v", err)
	}
	if _, err := server1.getOrCreateVolume("vol-unstored"); err != nil {
		t.Fatalf("getOrCreateVolume: %v", err)
	}
	assertVolumeWALDurability(ctx, t, server1, "vol-stored", pb.WalDurability_WITNESS)
	assertVolumeWALDurability(ctx, t, server1, "vol-unstored", pb.WalDurability_LOCAL)
	if err := server1.Close(); err != nil {
		t.Fatalf("server1.Close: %v", err)
	}

	server2 := newServerWithWALDurability(t, environment, walclient.Permanent)
	defer func() {
		if err := server2.Close(); err != nil {
			t.Errorf("server2.Close: %v", err)
		}
	}()
	restartedStored, err := server2.getOrCreateVolume("vol-stored")
	if err != nil {
		t.Fatalf("getOrCreateVolume restart: %v", err)
	}
	assertVolumeDurabilitySettings(t, restartedStored, pb.WalDurability_WITNESS, pb.WalDurability_WITNESS)

	restartedDefault, err := server2.getOrCreateVolume("vol-unstored")
	if err != nil {
		t.Fatalf("getOrCreateVolume restart: %v", err)
	}
	assertVolumeDurabilitySettings(t, restartedDefault, pb.WalDurability_PERMANENT, pb.WalDurability_WAL_DURABILITY_UNSPECIFIED)
	assertVolumeWALDurability(ctx, t, server2, "vol-stored", pb.WalDurability_WITNESS)
	assertVolumeWALDurability(ctx, t, server2, "vol-unstored", pb.WalDurability_PERMANENT)
}

// TestWALDurability_LoadVolumeInfoPath verifies that loadVolumeInfoLocked restores the stored WAL durability level.
func TestWALDurability_LoadVolumeInfoPath(t *testing.T) {
	ctx := t.Context()
	environment := newDurabilityTestEnvironment(t, ctx)
	server := newServerWithWALDurability(t, environment, walclient.Local)
	defer func() {
		if err := server.Close(); err != nil {
			t.Errorf("server.Close: %v", err)
		}
	}()
	volume, err := server.getOrCreateVolume("vol-load-info")
	if err != nil {
		t.Fatalf("getOrCreateVolume: %v", err)
	}
	if err := volume.PersistWalDurability(ctx, pb.WalDurability_WITNESS); err != nil {
		t.Fatalf("PersistWalDurability: %v", err)
	}

	volume.mu.Lock()
	volume.configured.walDurability = pb.WalDurability_WAL_DURABILITY_UNSPECIFIED
	volume.recomputeEffectiveLocked()
	if err := volume.loadVolumeInfoLocked(ctx); err != nil {
		volume.mu.Unlock()
		t.Fatalf("loadVolumeInfoLocked: %v", err)
	}
	volume.mu.Unlock()
	assertVolumeDurabilitySettings(t, volume, pb.WalDurability_WITNESS, pb.WalDurability_WITNESS)
}

// TestWALDurability_UnknownStoredValueFailsLoad verifies that a VolumeInfo row with an unknown
// wal_durability value fails the volume's load with an error naming the value, and does not
// fall back to the controller default.
func TestWALDurability_UnknownStoredValueFailsLoad(t *testing.T) {
	ctx := t.Context()
	environment := newDurabilityTestEnvironment(t, ctx)
	server1 := newServerWithWALDurability(t, environment, walclient.Local)
	volume, err := server1.getOrCreateVolume("vol-unknown-durability")
	if err != nil {
		t.Fatalf("getOrCreateVolume: %v", err)
	}

	if _, err := volume.persistVolumeInfo(ctx, func(volumeInfo *pb.VolumeInfo) {
		volumeInfo.WalDurability = pb.WalDurability(999)
	}); err != nil {
		t.Fatalf("persistVolumeInfo: %v", err)
	}

	// 1. Direct loadVolumeInfoLocked fails with an error naming the value.
	volume.mu.Lock()
	loadErr := volume.loadVolumeInfoLocked(ctx)
	volume.mu.Unlock()
	if loadErr == nil {
		t.Fatalf("expected loadVolumeInfoLocked to fail for unknown WAL durability, got nil")
	}
	if !strings.Contains(loadErr.Error(), "999") {
		t.Fatalf("expected error to name unknown value 999, got: %v", loadErr)
	}

	if err := server1.Close(); err != nil {
		t.Fatalf("server1.Close: %v", err)
	}

	// 2. Volume load on restart fails and does not fall back to controller default.
	server2 := newServerWithWALDurability(t, environment, walclient.Permanent)
	defer func() {
		if err := server2.Close(); err != nil {
			t.Errorf("server2.Close: %v", err)
		}
	}()

	restartedVolume, err := server2.getOrCreateVolume("vol-unknown-durability")
	if err == nil {
		t.Fatalf("expected getOrCreateVolume to fail on restart for unknown WAL durability, got volume with effective=%v", restartedVolume.WalDurability())
	}
	if !strings.Contains(err.Error(), "999") {
		t.Fatalf("expected restart error to name unknown value 999, got: %v", err)
	}
}
