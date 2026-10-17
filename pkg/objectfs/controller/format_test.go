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

	"github.com/gke-labs/in-cluster-storage/pkg/sds/record"
	"github.com/gke-labs/in-cluster-storage/pkg/wal"
	walclient "github.com/gke-labs/in-cluster-storage/pkg/wal/client"
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
