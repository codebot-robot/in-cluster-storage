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
	"testing"

	"github.com/container-storage-interface/spec/lib/go/csi"
	pb "github.com/gke-labs/in-cluster-storage/pkg/api/objectfs/v1alpha1"
	"github.com/gke-labs/in-cluster-storage/pkg/objectfs/waldurability"
	walclient "github.com/gke-labs/in-cluster-storage/pkg/wal/client"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

func newServerWithWALDurability(t *testing.T, env *durabilityTestEnvironment, durability walclient.Level) *Server {
	return NewTestServer(t, env.backend,
		WithServerMetadataDir(env.metadataDirectory),
		WithServerMetadataIndex("sqlite"),
		WithServerWAL(env.walDirectory, env.bufferTest.Target, durability),
		WithServerWALDialOptions(env.bufferTest.DialOpts...),
	)
}

func assertVolumeWALDurabilityInList(ctx context.Context, t *testing.T, server *Server, volumeID string, expectedLevel pb.WalDurability) {
	t.Helper()
	listResponse, err := server.ListVolumes(ctx, &pb.ListVolumesRequest{})
	if err != nil {
		t.Fatalf("ListVolumes failed: %v", err)
	}
	for _, volumeInfo := range listResponse.GetVolumes() {
		if volumeInfo.GetVolumeId() == volumeID {
			if volumeInfo.GetWalDurability() != expectedLevel {
				t.Fatalf("expected volume %s to have wal durability %v in ListVolumes, got %v", volumeID, expectedLevel, volumeInfo.GetWalDurability())
			}
			return
		}
	}
	t.Fatalf("volume %s not found in ListVolumes", volumeID)
}

// TestWALDurability_CSIParameter verifies that CSI volume creation correctly
// sets the WAL durability level, rejects unknown values, supports idempotent re-create,
// and rejects conflicting levels with AlreadyExists.
func TestWALDurability_CSIParameter(t *testing.T) {
	ctx := t.Context()
	env := newDurabilityTestEnvironment(t, ctx)
	server := newServerWithWALDurability(t, env, walclient.Witness)
	defer func() {
		if err := server.Close(); err != nil {
			t.Errorf("server.Close failed: %v", err)
		}
	}()
	csiController := NewCSIController(server)

	type scenario struct {
		paramValue    string
		expectedLevel pb.WalDurability
		expectedWal   walclient.Level
		expectedStore pb.WalDurability
		expectErr     bool
	}

	scenarios := map[string]scenario{
		"vol-wal-local": {
			paramValue:    "local",
			expectedLevel: pb.WalDurability_LOCAL,
			expectedWal:   walclient.Local,
			expectedStore: pb.WalDurability_LOCAL,
		},
		"vol-wal-witness": {
			paramValue:    "witness",
			expectedLevel: pb.WalDurability_WITNESS,
			expectedWal:   walclient.Witness,
			expectedStore: pb.WalDurability_WITNESS,
		},
		"vol-wal-permanent": {
			paramValue:    "permanent",
			expectedLevel: pb.WalDurability_PERMANENT,
			expectedWal:   walclient.Permanent,
			expectedStore: pb.WalDurability_PERMANENT,
		},
		"vol-wal-default": {
			paramValue:    "",
			expectedLevel: pb.WalDurability_WITNESS,
			expectedWal:   walclient.Witness,
			expectedStore: pb.WalDurability_WAL_DURABILITY_UNSPECIFIED,
		},
		"vol-wal-invalid": {
			paramValue: "bogus",
			expectErr:  true,
		},
	}

	for volumeName, sc := range scenarios {
		params := map[string]string{}
		if sc.paramValue != "" {
			params[waldurability.ParameterName] = sc.paramValue
		}
		_, err := csiController.CreateVolume(ctx, &csi.CreateVolumeRequest{
			Name:               volumeName,
			VolumeCapabilities: mountCapability(),
			Parameters:         params,
		})
		if sc.expectErr {
			if status.Code(err) != codes.InvalidArgument {
				t.Fatalf("expected InvalidArgument for %q, got %v", sc.paramValue, status.Code(err))
			}
			server.mu.RLock()
			_, exists := server.volumes[volumeName]
			server.mu.RUnlock()
			if exists {
				t.Fatalf("volume %s was unexpectedly created on invalid parameter", volumeName)
			}
			continue
		}
		if err != nil {
			t.Fatalf("CreateVolume failed for %s (%s): %v", volumeName, sc.paramValue, err)
		}
		vol, err := server.getOrCreateVolume(volumeName)
		if err != nil {
			t.Fatalf("getOrCreateVolume failed for %s: %v", volumeName, err)
		}
		if vol.WalDurability() != sc.expectedLevel {
			t.Fatalf("expected WalDurability %v, got %v", sc.expectedLevel, vol.WalDurability())
		}
		if vol.Durability() != sc.expectedWal {
			t.Fatalf("expected Durability %v, got %v", sc.expectedWal, vol.Durability())
		}
		if vol.StoredWalDurability() != sc.expectedStore {
			t.Fatalf("expected StoredWalDurability %v, got %v", sc.expectedStore, vol.StoredWalDurability())
		}
	}

	// Idempotent re-create with same value succeeds.
	if _, err := csiController.CreateVolume(ctx, &csi.CreateVolumeRequest{
		Name:               "vol-wal-local",
		VolumeCapabilities: mountCapability(),
		Parameters:         map[string]string{waldurability.ParameterName: "local"},
	}); err != nil {
		t.Fatalf("re-create with same parameter failed: %v", err)
	}

	// Idempotent re-create with omitted parameter succeeds without changing stored value.
	if _, err := csiController.CreateVolume(ctx, &csi.CreateVolumeRequest{
		Name:               "vol-wal-local",
		VolumeCapabilities: mountCapability(),
	}); err != nil {
		t.Fatalf("re-create with omitted parameter failed: %v", err)
	}
	volLocal, _ := server.getOrCreateVolume("vol-wal-local")
	if volLocal.StoredWalDurability() != pb.WalDurability_LOCAL {
		t.Fatalf("expected StoredWalDurability to remain LOCAL, got %v", volLocal.StoredWalDurability())
	}

	// Re-create with conflicting value fails with AlreadyExists.
	_, err := csiController.CreateVolume(ctx, &csi.CreateVolumeRequest{
		Name:               "vol-wal-local",
		VolumeCapabilities: mountCapability(),
		Parameters:         map[string]string{waldurability.ParameterName: "permanent"},
	})
	if status.Code(err) != codes.AlreadyExists {
		t.Fatalf("expected AlreadyExists on conflicting wal-durability, got %v", err)
	}
	if volLocal.WalDurability() != pb.WalDurability_LOCAL {
		t.Fatalf("expected WalDurability to remain LOCAL after rejected re-create, got %v", volLocal.WalDurability())
	}
}

// TestWALDurability_SurvivesRestartAndControllerDefault verifies that stored WAL durability
// survives restart and overrides controller default, while an unstored volume adopts the controller default.
func TestWALDurability_SurvivesRestartAndControllerDefault(t *testing.T) {
	ctx := t.Context()
	env := newDurabilityTestEnvironment(t, ctx)

	// Server 1 starts with controller default Local.
	server1 := newServerWithWALDurability(t, env, walclient.Local)
	storedVolName := "vol-stored-witness"
	volStored, err := server1.getOrCreateVolume(storedVolName)
	if err != nil {
		t.Fatalf("getOrCreateVolume failed: %v", err)
	}
	if err := volStored.PersistWalDurability(ctx, pb.WalDurability_WITNESS); err != nil {
		t.Fatalf("PersistWalDurability failed: %v", err)
	}

	defaultVolName := "vol-default-unstored"
	if _, err := server1.getOrCreateVolume(defaultVolName); err != nil {
		t.Fatalf("getOrCreateVolume failed: %v", err)
	}

	assertVolumeWALDurabilityInList(ctx, t, server1, storedVolName, pb.WalDurability_WITNESS)
	assertVolumeWALDurabilityInList(ctx, t, server1, defaultVolName, pb.WalDurability_LOCAL)

	if err := server1.Close(); err != nil {
		t.Fatalf("server1.Close failed: %v", err)
	}

	// Server 2 restarts with controller default Permanent.
	server2 := newServerWithWALDurability(t, env, walclient.Permanent)
	defer func() {
		if err := server2.Close(); err != nil {
			t.Errorf("server2.Close failed: %v", err)
		}
	}()

	restartedStored, err := server2.getOrCreateVolume(storedVolName)
	if err != nil {
		t.Fatalf("getOrCreateVolume failed on restart: %v", err)
	}
	if restartedStored.WalDurability() != pb.WalDurability_WITNESS {
		t.Fatalf("expected stored volume to retain WITNESS across restart, got %v", restartedStored.WalDurability())
	}
	if restartedStored.Durability() != walclient.Witness {
		t.Fatalf("expected stored volume runtime durability to be Witness, got %v", restartedStored.Durability())
	}

	restartedDefault, err := server2.getOrCreateVolume(defaultVolName)
	if err != nil {
		t.Fatalf("getOrCreateVolume failed on restart: %v", err)
	}
	if restartedDefault.WalDurability() != pb.WalDurability_PERMANENT {
		t.Fatalf("expected unstored volume to adopt new controller default PERMANENT, got %v", restartedDefault.WalDurability())
	}
	if restartedDefault.Durability() != walclient.Permanent {
		t.Fatalf("expected unstored volume runtime durability to be Permanent, got %v", restartedDefault.Durability())
	}

	assertVolumeWALDurabilityInList(ctx, t, server2, storedVolName, pb.WalDurability_WITNESS)
	assertVolumeWALDurabilityInList(ctx, t, server2, defaultVolName, pb.WalDurability_PERMANENT)
}

// TestWALDurability_PreExistingFieldsSurviveReadModifyWrite verifies that VolumeInfo
// read-modify-write preserves existing fields (deleted marker, retention) when persisting WAL durability.
func TestWALDurability_PreExistingFieldsSurviveReadModifyWrite(t *testing.T) {
	ctx := t.Context()
	env := newDurabilityTestEnvironment(t, ctx)
	server := newServerWithWALDurability(t, env, walclient.Local)
	defer func() {
		if err := server.Close(); err != nil {
			t.Errorf("server.Close failed: %v", err)
		}
	}()

	volName := "vol-rmw"
	vol, err := server.getOrCreateVolume(volName)
	if err != nil {
		t.Fatalf("getOrCreateVolume failed: %v", err)
	}

	if _, err := vol.persistVolumeInfo(ctx, func(info *pb.VolumeInfo) {
		info.Deleted = true
		info.Retention = proto.Int32(42)
	}); err != nil {
		t.Fatalf("persistVolumeInfo seed failed: %v", err)
	}

	if err := vol.PersistWalDurability(ctx, pb.WalDurability_WITNESS); err != nil {
		t.Fatalf("PersistWalDurability failed: %v", err)
	}

	infoKey, err := pkVolumeInfo.Extract(&pb.VolumeInfo{VolumeId: volName})
	if err != nil {
		t.Fatalf("Extract failed: %v", err)
	}
	msg, ok, err := vol.metadataView.Get(ctx, VolumeInfoTypeName, infoKey)
	if err != nil || !ok || msg == nil {
		t.Fatalf("failed to retrieve VolumeInfo: ok=%v, err=%v", ok, err)
	}
	info, ok := msg.(*pb.VolumeInfo)
	if !ok {
		t.Fatalf("stored message is not *pb.VolumeInfo: %T", msg)
	}
	if info.GetWalDurability() != pb.WalDurability_WITNESS {
		t.Fatalf("expected WalDurability WITNESS, got %v", info.GetWalDurability())
	}
	if !info.GetDeleted() {
		t.Fatalf("expected Deleted=true to survive read-modify-write")
	}
	if info.GetRetention() != 42 {
		t.Fatalf("expected Retention=42 to survive read-modify-write, got %d", info.GetRetention())
	}
}
