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

package sds

import (
	"context"
	"fmt"
	"iter"
	"sync"

	"github.com/gke-labs/in-cluster-storage/pkg/objectstore"
	"github.com/gke-labs/in-cluster-storage/pkg/sds/record"
	"google.golang.org/protobuf/proto"
)

// LocalIndex defines the contract for a derived, queryable local index of a structured data stream.
// Implementations maintain a materialization of the stream state at a known stream sequence position.
type LocalIndex interface {
	// Position returns the stream sequence position of the last applied change.
	Position() uint64

	// SyncRegistry registers type definitions in force with the local index.
	SyncRegistry(ctx context.Context, reg *record.Registry) error

	// ApplyBatch atomically applies a slice of committed row changes, advancing Position in the same commit.
	ApplyBatch(ctx context.Context, changes []Change) error

	// Get retrieves a merged proto row by table type name and primary key.
	// Returns (msg, true, nil) if found, or (nil, false, nil) if not found.
	Get(ctx context.Context, typeName string, key Key) (proto.Message, bool, error)

	// Scan yields merged proto rows matching keyPrefix in canonical key-byte order.
	// If keyPrefix is empty, all rows in the table are yielded.
	Scan(ctx context.Context, typeName string, keyPrefix []byte) iter.Seq2[proto.Message, error]

	// Close closes any underlying resources associated with the local index.
	Close() error
}

// Snapshotter is an interface implemented by LocalIndex types that support publishing
// consistent snapshots to object storage.
type Snapshotter interface {
	// PublishSnapshot creates an atomic snapshot of the local index at its current position
	// and uploads it to object storage, returning the object key and position.
	PublishSnapshot(ctx context.Context, backend objectstore.Backend) (key string, position uint64, err error)
}

// IndexFactory is the lifecycle and snapshot management interface for a LocalIndex type.
type IndexFactory interface {
	// Format returns the format identifier (e.g. "sqlite", "memory").
	Format() string

	// OpenLocal opens an existing local index in dir if present, returning (nil, false, nil) if none exists.
	OpenLocal(ctx context.Context, streamID string, dir string) (LocalIndex, bool, error)

	// FindLatestSnapshot finds the latest snapshot in object storage at or before maxPos.
	FindLatestSnapshot(ctx context.Context, backend objectstore.Backend, streamID string, maxPos uint64) (key string, pos uint64, err error)

	// RestoreSnapshot downloads and restores a snapshot into dir, returning the populated LocalIndex and its position.
	RestoreSnapshot(ctx context.Context, backend objectstore.Backend, streamID string, key string, dir string) (LocalIndex, uint64, error)

	// NewEmpty creates a new empty local index in dir.
	NewEmpty(ctx context.Context, streamID string, dir string) (LocalIndex, error)
}

var (
	factoriesMu sync.RWMutex
	factories   = make(map[string]IndexFactory)
)

// RegisterIndexFactory registers an IndexFactory under its format name.
func RegisterIndexFactory(f IndexFactory) {
	factoriesMu.Lock()
	defer factoriesMu.Unlock()
	factories[f.Format()] = f
}

// GetIndexFactory returns the registered IndexFactory for format (e.g. "sqlite", "memory").
func GetIndexFactory(format string) (IndexFactory, error) {
	factoriesMu.RLock()
	defer factoriesMu.RUnlock()
	f, ok := factories[format]
	if !ok {
		return nil, fmt.Errorf("unknown metadata index format %q (registered: %v)", format, registeredFormatsLocked())
	}
	return f, nil
}

func registeredFormatsLocked() []string {
	var names []string
	for name := range factories {
		names = append(names, name)
	}
	return names
}
