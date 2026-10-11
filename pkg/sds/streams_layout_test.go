// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package sds_test

import (
	"fmt"
	"strings"
	"testing"

	"github.com/gke-labs/in-cluster-storage/pkg/objectfs/blob"
	"github.com/gke-labs/in-cluster-storage/pkg/objectstore/inmemorystorage"
	"github.com/gke-labs/in-cluster-storage/pkg/sds/memtable"
	"github.com/gke-labs/in-cluster-storage/pkg/sds/projection/sqlite"
	"github.com/gke-labs/in-cluster-storage/pkg/wal"
	"github.com/google/uuid"
)

// TestStreamsWalLayoutTolerance verifies that when the aggregated WAL log lives under
// streams/wal/segments/, enumerating streams/ does not break ParseStreamPath or the
// memtable and sqlite snapshot listers, and they properly skip streams/wal/ entries.
func TestStreamsWalLayoutTolerance(t *testing.T) {
	ctx := t.Context()
	backend := inmemorystorage.New()

	streamID := uuid.New()
	streamFile := fmt.Sprintf("streams/%s/from00000000000000000001-to00000000000000000010.wal", streamID)
	memSnap := fmt.Sprintf("streams/%s/snapshots/memory/00000000000000000010.snap", streamID)
	sqliteSnap := fmt.Sprintf("streams/%s/snapshots/sqlite/00000000000000000010.sqlite", streamID)

	aggSeg1 := "streams/wal/segments/000000000001-000000000010.wal"
	aggSeg2 := "streams/wal/segments/000000000011-000000000020.wal"
	walManifest := "streams/wal/manifest.json"

	dummyData := []byte("dummy")
	for _, k := range []string{aggSeg1, aggSeg2, walManifest, streamFile, memSnap, sqliteSnap} {
		if _, err := backend.PutObject(ctx, "", k, blob.NewByteStreamFromBytes(dummyData)); err != nil {
			t.Fatalf("PutObject %s failed: %v", k, err)
		}
	}

	// 1. List objects under streams/
	keys, err := backend.ListObjects(ctx, "", "streams/")
	if err != nil {
		t.Fatalf("ListObjects streams/ failed: %v", err)
	}
	if len(keys) != 6 {
		t.Fatalf("expected 6 objects under streams/, got %d: %v", len(keys), keys)
	}

	// 2. ParseStreamPath must skip streams/wal/ entries and succeed for valid stream files
	var parsedStreams []string
	for _, k := range keys {
		from, to, sealed, parseErr := wal.ParseStreamPath(k)
		if strings.HasPrefix(k, "streams/wal/") {
			if parseErr == nil {
				t.Errorf("ParseStreamPath expected error for %s under reserved streams/wal/, got success (from=%d, to=%d, sealed=%v)", k, from, to, sealed)
			}
			continue
		}
		if k == streamFile {
			if parseErr != nil {
				t.Errorf("ParseStreamPath failed for valid stream file %s: %v", k, parseErr)
			} else if from != 1 || to != 10 || !sealed {
				t.Errorf("ParseStreamPath result mismatch for %s: from=%d, to=%d, sealed=%v", k, from, to, sealed)
			} else {
				parsedStreams = append(parsedStreams, k)
			}
		}
	}
	if len(parsedStreams) != 1 {
		t.Errorf("expected 1 valid stream file parsed, got %d", len(parsedStreams))
	}

	// 3. Memtable snapshot lister: ParseSnapshotKey and FindLatestSnapshot
	var parsedMemSnaps []string
	for _, k := range keys {
		sID, pos, parseErr := memtable.ParseSnapshotKey(k)
		if strings.HasPrefix(k, "streams/wal/") {
			if parseErr == nil {
				t.Errorf("memtable.ParseSnapshotKey expected error for %s, got sID=%s, pos=%d", k, sID, pos)
			}
			continue
		}
		if k == memSnap {
			if parseErr != nil {
				t.Errorf("memtable.ParseSnapshotKey failed for %s: %v", k, parseErr)
			} else if sID != streamID.String() || pos != 10 {
				t.Errorf("memtable.ParseSnapshotKey mismatch: sID=%s, pos=%d", sID, pos)
			} else {
				parsedMemSnaps = append(parsedMemSnaps, k)
			}
		}
	}
	if len(parsedMemSnaps) != 1 {
		t.Errorf("expected 1 valid mem snapshot parsed, got %d", len(parsedMemSnaps))
	}

	memKey, memPos, err := memtable.FindLatestSnapshot(ctx, backend, streamID.String(), 0)
	if err != nil {
		t.Fatalf("memtable.FindLatestSnapshot failed: %v", err)
	}
	if memKey != memSnap || memPos != 10 {
		t.Errorf("memtable.FindLatestSnapshot result mismatch: got (%s, %d), want (%s, 10)", memKey, memPos, memSnap)
	}

	// Calling FindLatestSnapshot on reserved "wal" must fail
	if _, _, err := memtable.FindLatestSnapshot(ctx, backend, "wal", 0); err == nil {
		t.Errorf("memtable.FindLatestSnapshot expected error for reserved stream ID 'wal', got nil")
	}

	// 4. SQLite snapshot lister: ParseSnapshotKey and FindLatestSnapshot
	var parsedSQLiteSnaps []string
	for _, k := range keys {
		sID, pos, parseErr := sqlite.ParseSnapshotKey(k)
		if strings.HasPrefix(k, "streams/wal/") {
			if parseErr == nil {
				t.Errorf("sqlite.ParseSnapshotKey expected error for %s, got sID=%s, pos=%d", k, sID, pos)
			}
			continue
		}
		if k == sqliteSnap {
			if parseErr != nil {
				t.Errorf("sqlite.ParseSnapshotKey failed for %s: %v", k, parseErr)
			} else if sID != streamID.String() || pos != 10 {
				t.Errorf("sqlite.ParseSnapshotKey mismatch: sID=%s, pos=%d", sID, pos)
			} else {
				parsedSQLiteSnaps = append(parsedSQLiteSnaps, k)
			}
		}
	}
	if len(parsedSQLiteSnaps) != 1 {
		t.Errorf("expected 1 valid sqlite snapshot parsed, got %d", len(parsedSQLiteSnaps))
	}

	sqliteKey, sqlitePos, err := sqlite.FindLatestSnapshot(ctx, backend, streamID.String(), 0)
	if err != nil {
		t.Fatalf("sqlite.FindLatestSnapshot failed: %v", err)
	}
	if sqliteKey != sqliteSnap || sqlitePos != 10 {
		t.Errorf("sqlite.FindLatestSnapshot result mismatch: got (%s, %d), want (%s, 10)", sqliteKey, sqlitePos, sqliteSnap)
	}

	// Calling FindLatestSnapshot on reserved "wal" must fail
	if _, _, err := sqlite.FindLatestSnapshot(ctx, backend, "wal", 0); err == nil {
		t.Errorf("sqlite.FindLatestSnapshot expected error for reserved stream ID 'wal', got nil")
	}

	// 5. Stream enumeration skipping wal
	discoveredStreams := make(map[string]bool)
	for _, k := range keys {
		trimmed := strings.TrimPrefix(k, "streams/")
		parts := strings.Split(trimmed, "/")
		if len(parts) > 0 {
			label := parts[0]
			if label == "wal" {
				continue // reserved prefix skipped
			}
			if _, parseErr := uuid.Parse(label); parseErr == nil {
				discoveredStreams[label] = true
			}
		}
	}
	if len(discoveredStreams) != 1 || !discoveredStreams[streamID.String()] {
		t.Errorf("stream discovery mismatch: got %+v, want [%s]", discoveredStreams, streamID.String())
	}
}
