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

package memtable

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"

	sdsv1 "github.com/gke-labs/in-cluster-storage/pkg/api/sds/v1"
	"github.com/gke-labs/in-cluster-storage/pkg/objectfs/blob"
	"github.com/gke-labs/in-cluster-storage/pkg/objectstore"
	"github.com/gke-labs/in-cluster-storage/pkg/sds"
	"google.golang.org/protobuf/proto"
)

var (
	// ErrSnapshotNotFound is returned when no snapshot is found matching the search criteria.
	ErrSnapshotNotFound = errors.New("snapshot not found")
	// ErrInvalidSnapshotKey is returned when a snapshot object key does not match the expected naming pattern.
	ErrInvalidSnapshotKey = errors.New("invalid snapshot key")
)

// SnapshotKey returns the canonical object storage key for a MemStore snapshot at the given position:
// "streams/<streamID>/snapshots/memory/<position, 20 digits>.snap".
func SnapshotKey(streamID string, position uint64) string {
	return fmt.Sprintf("streams/%s/snapshots/memory/%020d.snap", streamID, position)
}

// ParseSnapshotKey parses a canonical snapshot key into its stream ID and position.
func ParseSnapshotKey(key string) (streamID string, position uint64, err error) {
	// Expected format: streams/<stream-uuid>/snapshots/memory/<20-digits>.snap
	parts := strings.Split(key, "/")
	if len(parts) != 5 || parts[0] != "streams" || parts[2] != "snapshots" || parts[3] != "memory" {
		return "", 0, fmt.Errorf("%w: %q", ErrInvalidSnapshotKey, key)
	}

	streamID = parts[1]
	filePart := parts[4]
	if !strings.HasSuffix(filePart, ".snap") {
		return "", 0, fmt.Errorf("%w: missing .snap suffix in %q", ErrInvalidSnapshotKey, key)
	}

	posStr := strings.TrimSuffix(filePart, ".snap")
	if len(posStr) != 20 {
		return "", 0, fmt.Errorf("%w: expected 20-digit position, got %q in %q", ErrInvalidSnapshotKey, posStr, key)
	}

	pos, err := strconv.ParseUint(posStr, 10, 64)
	if err != nil {
		return "", 0, fmt.Errorf("%w: invalid position in %q: %w", ErrInvalidSnapshotKey, key, err)
	}

	return streamID, pos, nil
}

// FindLatestSnapshot finds the latest MemStore snapshot in object storage at or before maxPosition.
func FindLatestSnapshot(ctx context.Context, backend objectstore.Backend, streamID string, maxPosition uint64) (key string, position uint64, err error) {
	if backend == nil {
		return "", 0, errors.New("nil backend")
	}

	prefix := fmt.Sprintf("streams/%s/snapshots/memory/", streamID)
	keys, err := backend.ListObjects(ctx, "", prefix)
	if err != nil {
		return "", 0, fmt.Errorf("failed to list memory snapshots for stream %s: %w", streamID, err)
	}

	type match struct {
		key string
		pos uint64
	}
	var matches []match

	for _, k := range keys {
		sID, pos, parseErr := ParseSnapshotKey(k)
		if parseErr != nil || sID != streamID {
			continue
		}
		if maxPosition == 0 || pos <= maxPosition {
			matches = append(matches, match{key: k, pos: pos})
		}
	}

	if len(matches) == 0 {
		return "", 0, ErrSnapshotNotFound
	}

	sort.Slice(matches, func(i, j int) bool {
		return matches[i].pos < matches[j].pos
	})

	latest := matches[len(matches)-1]
	return latest.key, latest.pos, nil
}

// snapshotData represents the serialized format of a MemStore snapshot.
// The MemStore snapshot format encodes all tables and rows as JSON.
// This is intended for testing and small/ephemeral stream volumes, not for large production volumes.
type snapshotData struct {
	StreamID string                 `json:"stream_id"`
	Position uint64                 `json:"position"`
	Registry []byte                 `json:"registry"`
	Tables   map[string][]rowRecord `json:"tables"`
}

type rowRecord struct {
	Key []byte `json:"key"`
	Val []byte `json:"val"`
}

// PublishSnapshot creates an atomic snapshot of an active MemStore at its current safe position
// and uploads it to object storage.
// Note: This format serializes all rows into JSON and is intended for tests and small/ephemeral volumes.
func PublishSnapshot(ctx context.Context, store *MemStore, backend objectstore.Backend, tempDir string) (string, uint64, error) {
	if store == nil {
		return "", 0, errors.New("nil store")
	}
	if backend == nil {
		return "", 0, errors.New("nil backend")
	}

	store.mu.RLock()
	streamID := store.streamID
	pos := store.lastPosition

	var regBytes []byte
	if store.registry != nil {
		exported := store.registry.Export()
		var err error
		regBytes, err = proto.Marshal(exported)
		if err != nil {
			store.mu.RUnlock()
			return "", 0, fmt.Errorf("failed to marshal registry: %w", err)
		}
	}

	tablesData := make(map[string][]rowRecord)
	for tblName, tbl := range store.tables {
		tbl.mu.RLock()
		var rows []rowRecord
		for k, msg := range tbl.rows {
			valBytes, err := proto.Marshal(msg)
			if err != nil {
				tbl.mu.RUnlock()
				store.mu.RUnlock()
				return "", 0, fmt.Errorf("failed to marshal row in table %q: %w", tblName, err)
			}
			rows = append(rows, rowRecord{Key: k.Bytes(), Val: valBytes})
		}
		tbl.mu.RUnlock()
		tablesData[tblName] = rows
	}
	store.mu.RUnlock()

	data := snapshotData{
		StreamID: streamID,
		Position: pos,
		Registry: regBytes,
		Tables:   tablesData,
	}

	encoded, err := json.Marshal(data)
	if err != nil {
		return "", 0, fmt.Errorf("failed to encode snapshot data: %w", err)
	}

	key := SnapshotKey(streamID, pos)
	stream := blob.NewByteStreamFromBytes(encoded)

	if _, err := backend.PutObject(ctx, "", key, stream); err != nil {
		return "", 0, fmt.Errorf("failed to upload memory snapshot %q: %w", key, err)
	}

	return key, pos, nil
}

// RestoreSnapshot downloads the latest MemStore snapshot at or before maxPosition and returns a new populated MemStore.
func RestoreSnapshot(ctx context.Context, backend objectstore.Backend, streamID string, maxPosition uint64, targetPath string, opts ...Option) (*MemStore, uint64, error) {
	key, _, err := FindLatestSnapshot(ctx, backend, streamID, maxPosition)
	if err != nil {
		return nil, 0, err
	}
	return RestoreSnapshotKey(ctx, backend, streamID, key, targetPath, opts...)
}

// RestoreSnapshotKey downloads a MemStore snapshot by object key and returns a new populated MemStore.
func RestoreSnapshotKey(ctx context.Context, backend objectstore.Backend, streamID string, key string, targetPath string, opts ...Option) (*MemStore, uint64, error) {
	var buf bytes.Buffer
	if err := backend.GetObject(ctx, "", key, 0, 0, &buf); err != nil {
		return nil, 0, fmt.Errorf("failed to download snapshot %q: %w", key, err)
	}

	var data snapshotData
	if err := json.Unmarshal(buf.Bytes(), &data); err != nil {
		return nil, 0, fmt.Errorf("failed to decode snapshot %q: %w", key, err)
	}

	allOpts := append([]Option{WithStreamID(streamID)}, opts...)
	store := New(allOpts...)
	store.SetPosition(data.Position)

	if len(data.Registry) > 0 {
		var regProto sdsv1.Registry
		if err := proto.Unmarshal(data.Registry, &regProto); err != nil {
			return nil, 0, fmt.Errorf("failed to unmarshal snapshot registry: %w", err)
		}
		if err := store.registry.Import(&regProto); err != nil {
			return nil, 0, fmt.Errorf("failed to import snapshot registry: %w", err)
		}
	}

	for tblName, rows := range data.Tables {
		tbl := store.GetOrCreateTable(tblName)
		def, _, ok := store.registry.LookupByName(tblName)
		for _, r := range rows {
			key := sds.NewKeyFromBytes(r.Key)
			if ok {
				msgType, err := store.registry.ResolveMessageType(def.GetId())
				if err == nil {
					target := msgType.New().Interface()
					if err := proto.Unmarshal(r.Val, target); err == nil {
						tbl.Put(key, target)
						continue
					}
				}
			}
		}
	}

	return store, data.Position, nil
}
