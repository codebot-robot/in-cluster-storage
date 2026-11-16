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

	"github.com/container-storage-interface/spec/lib/go/csi"
	pb "github.com/gke-labs/in-cluster-storage/pkg/api/objectfs/v1alpha1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// mountCapability returns standard mount volume capabilities for CSI CreateVolume requests.
// Takes no locks.
func mountCapability() []*csi.VolumeCapability {
	return []*csi.VolumeCapability{{
		AccessType: &csi.VolumeCapability_Mount{Mount: &csi.VolumeCapability_MountVolume{}},
		AccessMode: &csi.VolumeCapability_AccessMode{Mode: csi.VolumeCapability_AccessMode_MULTI_NODE_MULTI_WRITER},
	}}
}

// TestMetadataDurability_CSIParameter verifies that CSI volume creation correctly
// sets the metadata durability mode for valid parameters, rejects unknown parameter values
// without creating a volume, succeeds as a no-op when re-created with the same mode or no mode,
// and rejects conflicting modes with AlreadyExists while leaving the volume unchanged.
func TestMetadataDurability_CSIParameter(t *testing.T) {
	ctx := t.Context()
	environment := newDurabilityTestEnvironment(t, ctx)
	// Server default is set to BARRIER (which differs from NewVolume's hardcoded default OPERATION).
	server := environment.newServer(t, pb.MetadataDurability_BARRIER)
	defer func() {
		if err := server.Close(); err != nil {
			t.Errorf("server.Close failed: %v", err)
		}
	}()
	csiController := NewCSIController(server)

	type durabilityScenario struct {
		parameterValue     string
		expectedMode       pb.MetadataDurability
		expectedStoredMode pb.MetadataDurability
		expectError        bool
	}

	scenarios := map[string]durabilityScenario{
		"volume-param-operation": {
			parameterValue:     "operation",
			expectedMode:       pb.MetadataDurability_OPERATION,
			expectedStoredMode: pb.MetadataDurability_OPERATION,
		},
		"volume-param-barrier": {
			parameterValue:     "barrier",
			expectedMode:       pb.MetadataDurability_BARRIER,
			expectedStoredMode: pb.MetadataDurability_BARRIER,
		},
		"volume-param-none": {
			parameterValue:     "",
			expectedMode:       pb.MetadataDurability_BARRIER,
			expectedStoredMode: pb.MetadataDurability_METADATA_DURABILITY_UNSPECIFIED,
		},
		"volume-param-invalid": {
			parameterValue: "bogus",
			expectError:    true,
		},
	}

	for volumeName, scenario := range scenarios {
		parameters := map[string]string{}
		if scenario.parameterValue != "" {
			parameters["metadata-durability"] = scenario.parameterValue
		}
		_, err := csiController.CreateVolume(ctx, &csi.CreateVolumeRequest{
			Name:               volumeName,
			VolumeCapabilities: mountCapability(),
			Parameters:         parameters,
		})
		if scenario.expectError {
			if status.Code(err) != codes.InvalidArgument {
				t.Fatalf("expected InvalidArgument for parameter %q, got %v", scenario.parameterValue, status.Code(err))
			}
			// Verify bad parameter did not create a volume in the server.
			server.mu.RLock()
			_, exists := server.volumes[volumeName]
			server.mu.RUnlock()
			if exists {
				t.Fatalf("volume %s was unexpectedly created on invalid parameter", volumeName)
			}
			continue
		}
		if err != nil {
			t.Fatalf("CreateVolume failed for %s (%s): %v", volumeName, scenario.parameterValue, err)
		}
		volume, err := server.getOrCreateVolume(volumeName)
		if err != nil {
			t.Fatalf("getOrCreateVolume failed for %s: %v", volumeName, err)
		}
		if volume.MetadataDurability() != scenario.expectedMode {
			t.Fatalf("expected mode %v for parameter %q, got %v", scenario.expectedMode, scenario.parameterValue, volume.MetadataDurability())
		}
		if volume.StoredMetadataDurability() != scenario.expectedStoredMode {
			t.Fatalf("expected stored mode %v for parameter %q, got %v", scenario.expectedStoredMode, scenario.parameterValue, volume.StoredMetadataDurability())
		}
	}

	// Re-create with the same value succeeds as a no-op.
	if _, err := csiController.CreateVolume(ctx, &csi.CreateVolumeRequest{
		Name:               "volume-param-operation",
		VolumeCapabilities: mountCapability(),
		Parameters:         map[string]string{"metadata-durability": "operation"},
	}); err != nil {
		t.Fatalf("idempotent CreateVolume with same mode failed: %v", err)
	}

	// Re-create with no value succeeds and changes nothing.
	if _, err := csiController.CreateVolume(ctx, &csi.CreateVolumeRequest{
		Name:               "volume-param-operation",
		VolumeCapabilities: mountCapability(),
	}); err != nil {
		t.Fatalf("idempotent CreateVolume with no mode parameter failed: %v", err)
	}
	existingOperationVolume, err := server.getOrCreateVolume("volume-param-operation")
	if err != nil {
		t.Fatalf("getOrCreateVolume failed: %v", err)
	}
	if existingOperationVolume.MetadataDurability() != pb.MetadataDurability_OPERATION {
		t.Fatalf("expected durability mode to remain OPERATION after re-create with no value, got %v", existingOperationVolume.MetadataDurability())
	}
	if existingOperationVolume.StoredMetadataDurability() != pb.MetadataDurability_OPERATION {
		t.Fatalf("expected stored mode to remain OPERATION after re-create with no value, got %v", existingOperationVolume.StoredMetadataDurability())
	}

	// Re-create with a different value returns AlreadyExists and leaves the mode unchanged.
	_, err = csiController.CreateVolume(ctx, &csi.CreateVolumeRequest{
		Name:               "volume-param-operation",
		VolumeCapabilities: mountCapability(),
		Parameters:         map[string]string{"metadata-durability": "barrier"},
	})
	if err == nil {
		t.Fatal("expected CreateVolume with conflicting metadata-durability to fail, got nil")
	}
	if status.Code(err) != codes.AlreadyExists {
		t.Fatalf("expected AlreadyExists, got %v (%v)", status.Code(err), err)
	}
	if existingOperationVolume.MetadataDurability() != pb.MetadataDurability_OPERATION {
		t.Fatalf("expected durability mode to remain OPERATION, got %v", existingOperationVolume.MetadataDurability())
	}
	if existingOperationVolume.StoredMetadataDurability() != pb.MetadataDurability_OPERATION {
		t.Fatalf("expected stored mode to remain OPERATION, got %v", existingOperationVolume.StoredMetadataDurability())
	}

	// Re-create with conflicting value on barrier volume also returns AlreadyExists and leaves the mode unchanged.
	_, err = csiController.CreateVolume(ctx, &csi.CreateVolumeRequest{
		Name:               "volume-param-barrier",
		VolumeCapabilities: mountCapability(),
		Parameters:         map[string]string{"metadata-durability": "operation"},
	})
	if err == nil {
		t.Fatal("expected CreateVolume with conflicting metadata-durability to fail on barrier volume, got nil")
	}
	if status.Code(err) != codes.AlreadyExists {
		t.Fatalf("expected AlreadyExists, got %v (%v)", status.Code(err), err)
	}
	existingBarrierVolume, err := server.getOrCreateVolume("volume-param-barrier")
	if err != nil {
		t.Fatalf("getOrCreateVolume failed: %v", err)
	}
	if existingBarrierVolume.MetadataDurability() != pb.MetadataDurability_BARRIER {
		t.Fatalf("expected durability mode to remain BARRIER, got %v", existingBarrierVolume.MetadataDurability())
	}
	if existingBarrierVolume.StoredMetadataDurability() != pb.MetadataDurability_BARRIER {
		t.Fatalf("expected stored mode to remain BARRIER, got %v", existingBarrierVolume.StoredMetadataDurability())
	}

	// Re-create with conflicting durability and a different consistency value returns AlreadyExists
	// and leaves both consistency mode and durability mode unchanged.
	conflictVolumeName := "volume-param-conflict-consistency"
	if _, err := csiController.CreateVolume(ctx, &csi.CreateVolumeRequest{
		Name:               conflictVolumeName,
		VolumeCapabilities: mountCapability(),
		Parameters: map[string]string{
			"metadata-durability": "operation",
			"consistency":         "standard",
		},
	}); err != nil {
		t.Fatalf("CreateVolume failed for %s: %v", conflictVolumeName, err)
	}
	conflictVolume, err := server.getOrCreateVolume(conflictVolumeName)
	if err != nil {
		t.Fatalf("getOrCreateVolume failed for %s: %v", conflictVolumeName, err)
	}
	if conflictVolume.ConsistencyMode() != pb.ConsistencyMode_STANDARD {
		t.Fatalf("expected initial consistency mode STANDARD, got %v", conflictVolume.ConsistencyMode())
	}
	if conflictVolume.MetadataDurability() != pb.MetadataDurability_OPERATION {
		t.Fatalf("expected initial durability mode OPERATION, got %v", conflictVolume.MetadataDurability())
	}

	_, err = csiController.CreateVolume(ctx, &csi.CreateVolumeRequest{
		Name:               conflictVolumeName,
		VolumeCapabilities: mountCapability(),
		Parameters: map[string]string{
			"metadata-durability": "barrier",
			"consistency":         "close-to-open",
		},
	})
	if err == nil {
		t.Fatal("expected CreateVolume with conflicting metadata-durability and new consistency to fail, got nil")
	}
	if status.Code(err) != codes.AlreadyExists {
		t.Fatalf("expected AlreadyExists, got %v (%v)", status.Code(err), err)
	}
	if conflictVolume.ConsistencyMode() != pb.ConsistencyMode_STANDARD {
		t.Fatalf("expected consistency mode to remain STANDARD after refused re-create, got %v", conflictVolume.ConsistencyMode())
	}
	if conflictVolume.MetadataDurability() != pb.MetadataDurability_OPERATION {
		t.Fatalf("expected durability mode to remain OPERATION after refused re-create, got %v", conflictVolume.MetadataDurability())
	}
	if conflictVolume.StoredMetadataDurability() != pb.MetadataDurability_OPERATION {
		t.Fatalf("expected stored durability mode to remain OPERATION after refused re-create, got %v", conflictVolume.StoredMetadataDurability())
	}
}
