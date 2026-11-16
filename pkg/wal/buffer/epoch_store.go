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

package buffer

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"

	"github.com/gke-labs/in-cluster-storage/pkg/tempfile"
	"github.com/google/uuid"
	"k8s.io/klog/v2"
)

// EpochStore persists and retrieves accepted stream epochs on local disk.
type EpochStore struct {
	mu      sync.RWMutex
	dir     string
	noFsync bool
	epochs  map[uuid.UUID]uint64
}

// NewEpochStore initializes an EpochStore backed by the epochs subdirectory of dataDirectory.
// It loads all persisted stream epochs from disk into memory.
func NewEpochStore(dataDirectory string, noFsync bool) (*EpochStore, error) {
	epochsDirectory := filepath.Join(dataDirectory, "epochs")
	if err := os.MkdirAll(epochsDirectory, 0755); err != nil {
		return nil, fmt.Errorf("failed to create epochs directory %s: %w", epochsDirectory, err)
	}

	entries, err := os.ReadDir(epochsDirectory)
	if err != nil {
		return nil, fmt.Errorf("failed to read epochs directory %s: %w", epochsDirectory, err)
	}

	loadedEpochs := make(map[uuid.UUID]uint64)
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		streamID, err := uuid.Parse(entry.Name())
		if err != nil {
			klog.Warningf("EpochStore: ignoring non-UUID file in %s: %s (%v)", epochsDirectory, entry.Name(), err)
			continue
		}
		filePath := filepath.Join(epochsDirectory, entry.Name())
		content, err := os.ReadFile(filePath)
		if err != nil {
			return nil, fmt.Errorf("failed to read epoch file %s: %w", filePath, err)
		}
		epoch, err := strconv.ParseUint(strings.TrimSpace(string(content)), 10, 64)
		if err != nil {
			klog.Warningf("EpochStore: ignoring unparseable epoch file %s: %v", filePath, err)
			continue
		}
		loadedEpochs[streamID] = epoch
	}

	return &EpochStore{
		dir:     epochsDirectory,
		noFsync: noFsync,
		epochs:  loadedEpochs,
	}, nil
}

// GetEpoch returns the highest accepted epoch for streamID, or 0 if none has been recorded.
func (store *EpochStore) GetEpoch(streamID uuid.UUID) uint64 {
	store.mu.RLock()
	defer store.mu.RUnlock()
	return store.epochs[streamID]
}

// SaveEpoch atomically persists epoch for streamID to disk and updates in-memory state.
func (store *EpochStore) SaveEpoch(streamID uuid.UUID, epoch uint64) error {
	store.mu.Lock()
	defer store.mu.Unlock()

	current := store.epochs[streamID]
	if epoch <= current && current > 0 {
		return nil
	}

	tempFile, err := tempfile.Create(store.dir, ".tmp-epoch-*")
	if err != nil {
		return fmt.Errorf("failed to create temp epoch file: %w", err)
	}
	defer tempFile.CloseAndLog(context.Background())

	content := []byte(strconv.FormatUint(epoch, 10) + "\n")
	if _, err := tempFile.Write(content); err != nil {
		return fmt.Errorf("failed to write temp epoch file: %w", err)
	}
	if !store.noFsync {
		if err := tempFile.Sync(); err != nil {
			return fmt.Errorf("failed to sync temp epoch file: %w", err)
		}
	}

	targetPath := filepath.Join(store.dir, streamID.String())
	if err := tempFile.RenameTo(targetPath); err != nil {
		return fmt.Errorf("failed to rename temp epoch file to %s: %w", targetPath, err)
	}

	store.epochs[streamID] = epoch
	return nil
}
