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
	"fmt"
	"os"
	"path/filepath"
	"testing"

	sdsv1 "github.com/gke-labs/in-cluster-storage/pkg/api/sds/v1"
	"github.com/gke-labs/in-cluster-storage/pkg/sds/record"
	"github.com/gke-labs/in-cluster-storage/pkg/wal"
	walclient "github.com/gke-labs/in-cluster-storage/pkg/wal/client"
	"google.golang.org/protobuf/proto"
)

func TestController_UnversionedUpgrade(t *testing.T) {
	ctx := t.Context()
	walDir := t.TempDir()
	streamID := StreamIDForVolume("vol-legacy")

	payload, err := record.EncodeFrame(record.TypeIDPadding, []byte("legacy-padding"))
	if err != nil {
		t.Fatalf("EncodeFrame failed: %v", err)
	}

	// Create legacy unversioned client segment file
	clientRec := &wal.ClientRecord{
		StreamID:  streamID,
		StreamSeq: 1,
		Payload:   payload,
	}
	clientRec.CRC32C = clientRec.ComputeCRC32C()
	segPath := filepath.Join(walDir, fmt.Sprintf("stream-%s-%019d.wal", streamID.String(), 1))
	if err := os.WriteFile(segPath, clientRec.Encode(), 0644); err != nil {
		t.Fatalf("failed to write unversioned segment: %v", err)
	}

	// 1. Without flag: Server.walclient.Open fails
	_, err = walclient.Open(ctx, walDir, streamID, "")
	if err == nil {
		t.Fatal("expected error without acceptUnversioned, got nil")
	}
	if !errors.Is(err, wal.ErrUnversionedSegment) {
		t.Fatalf("expected ErrUnversionedSegment, got: %v", err)
	}

	// 2. With flag: Server.walclient.Open succeeds and volume loads
	srv := NewServer(NewMemoryBackend(),
		WithServerWAL(walDir, "", walclient.Local),
		WithServerAcceptUnversioned(true),
	)
	defer srv.Close()

	vol, err := srv.getOrCreateVolume("vol-legacy")
	if err != nil {
		t.Fatalf("getOrCreateVolume failed with acceptUnversioned: %v", err)
	}

	if vol.VolumeID() != "vol-legacy" {
		t.Errorf("vol.VolumeID = %q, want vol-legacy", vol.VolumeID())
	}
}

func TestVolumeMetadataPinnedTypeIDs(t *testing.T) {
	vol := MustNewVolume(t, "test-pinned-ids", nil, NewEventBroadcaster())
	defer func() {
		if err := vol.Close(); err != nil {
			t.Errorf("vol.Close failed: %v", err)
		}
	}()

	reg := vol.metadataStream.Registry()
	expectedPinned := []struct {
		name string
		id   uint32
	}{
		{name: "objectfs.v1alpha1.Inode", id: 16},
		{name: "objectfs.v1alpha1.DirEntry", id: 17},
		{name: "objectfs.v1alpha1.FileChunk", id: 18},
		{name: "objectfs.v1alpha1.Content", id: 19},
		{name: "objectfs.v1alpha1.VolumeStats", id: 20},
	}

	for _, tc := range expectedPinned {
		def, _, ok := reg.LookupByName(tc.name)
		if !ok {
			t.Fatalf("required table %q not registered in metadata stream registry", tc.name)
		}
		if def.GetId() != tc.id {
			t.Errorf("table %q has ID %d, want pinned ID %d (reordering breaks existing volumes)",
				tc.name, def.GetId(), tc.id)
		}

		defByID, _, okByID := reg.LookupByID(tc.id)
		if !okByID {
			t.Errorf("pinned ID %d not found in registry", tc.id)
		} else if defByID.GetName() != tc.name {
			t.Errorf("pinned ID %d maps to name %q, want %q", tc.id, defByID.GetName(), tc.name)
		}
	}

	// Verify all registered types: pinned types must have their exact IDs,
	// and any future additions must be appended after VolumeStats (ID >= 21).
	for _, def := range reg.Export().GetTypes() {
		if def.GetId() <= 20 {
			var found bool
			for _, tc := range expectedPinned {
				if tc.id == def.GetId() && tc.name == def.GetName() {
					found = true
					break
				}
			}
			if !found {
				t.Errorf("unexpected type ID %d (%q) in pinned range [16..20]", def.GetId(), def.GetName())
			}
		} else if def.GetId() < 21 {
			t.Errorf("new table %q must be appended with ID >= 21, got %d", def.GetName(), def.GetId())
		}
	}

	// Verify that a volume index holding a conflicting / reordered registration order
	// fails to sync with the writer's registry.
	conflictingReg := record.NewRegistry()
	for _, tc := range expectedPinned {
		if tc.id < 19 {
			def, _, ok := reg.LookupByID(tc.id)
			if !ok {
				t.Fatalf("missing baseline ID %d", tc.id)
			}
			if err := conflictingReg.Register(def); err != nil {
				t.Fatalf("failed registering baseline type %d: %v", tc.id, err)
			}
		}
	}

	// Simulate an older volume index where VolumeStats was registered at ID 19
	// (e.g. before Content was introduced or due to a reordering).
	statsDef, _, ok := reg.LookupByName("objectfs.v1alpha1.VolumeStats")
	if !ok {
		t.Fatalf("VolumeStats not found")
	}
	conflictingStats := proto.Clone(statsDef).(*sdsv1.TypeDefinition)
	conflictingStats.Id = 19
	if err := conflictingReg.Register(conflictingStats); err != nil {
		t.Fatalf("failed registering conflicting type: %v", err)
	}

	// Syncing (Import) the metadata stream's registry into the conflicting index must fail.
	err := conflictingReg.Import(reg.Export())
	if !errors.Is(err, record.ErrTypeConflict) {
		t.Fatalf("expected ErrTypeConflict when importing into index with reordered types, got: %v", err)
	}
}
