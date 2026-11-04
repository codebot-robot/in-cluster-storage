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

package inmemorystorage

import (
	"context"
	"crypto/sha256"
	"fmt"
	"io"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/gke-labs/in-cluster-storage/pkg/objectfs/blob"
)

type memoryObject struct {
	data    []byte
	modTime time.Time
}

// Backend is an in-memory implementation of ObjectStorageBackend.
type Backend struct {
	mu      sync.RWMutex
	objects map[string]memoryObject // key: volumeID + "/" + key
}

// New creates a new in-memory object storage backend.
func New() *Backend {
	return &Backend{
		objects: make(map[string]memoryObject),
	}
}

// NewMemoryBackend creates a new in-memory object storage backend (alias for backwards compatibility).
func NewMemoryBackend() *Backend {
	return New()
}

func (backend *Backend) storageKey(volumeID, key string) string {
	if volumeID == "" || strings.HasPrefix(key, "volumes/") || strings.HasPrefix(key, "blobs/") {
		return key
	}
	return fmt.Sprintf("%s/%s", volumeID, key)
}

func (backend *Backend) PutObject(ctx context.Context, volumeID, key string, stream blob.ByteStream) (string, error) {
	backend.mu.Lock()
	defer backend.mu.Unlock()

	storageKey := backend.storageKey(volumeID, key)
	if err := stream.Rewind(); err != nil {
		return "", err
	}
	buf, err := io.ReadAll(stream)
	if err != nil {
		return "", err
	}
	backend.objects[storageKey] = memoryObject{
		data:    buf,
		modTime: time.Now().UTC(),
	}

	h := sha256.Sum256(buf)
	return fmt.Sprintf("%x", h), nil
}

func (backend *Backend) GetObject(ctx context.Context, volumeID, key string, offset, length int64, w io.Writer) error {
	backend.mu.RLock()
	defer backend.mu.RUnlock()

	storageKey := backend.storageKey(volumeID, key)
	obj, ok := backend.objects[storageKey]
	if !ok {
		return fmt.Errorf("object %s not found in volume %s", key, volumeID)
	}

	total := int64(len(obj.data))
	if offset >= total {
		return nil
	}
	end := offset + length
	if length <= 0 || end > total {
		end = total
	}

	_, err := w.Write(obj.data[offset:end])
	return err
}

func (backend *Backend) DeleteObject(ctx context.Context, volumeID, key string) error {
	backend.mu.Lock()
	defer backend.mu.Unlock()

	storageKey := backend.storageKey(volumeID, key)
	delete(backend.objects, storageKey)
	return nil
}

func (backend *Backend) GetRedirectURL(ctx context.Context, volumeID, key string) (string, error) {
	// For memory backend or when direct redirect is not used, returns empty string.
	return "", nil
}

func (backend *Backend) ListObjects(ctx context.Context, volumeID, prefix string) ([]blob.ObjectEntry, error) {
	backend.mu.RLock()
	defer backend.mu.RUnlock()

	fullPrefix := backend.storageKey(volumeID, prefix)
	var matches []blob.ObjectEntry
	for storageKey, obj := range backend.objects {
		if strings.HasPrefix(storageKey, fullPrefix) {
			matches = append(matches, blob.ObjectEntry{
				Key:          storageKey,
				Size:         int64(len(obj.data)),
				LastModified: obj.modTime,
			})
		}
	}
	sort.Slice(matches, func(firstIndex, secondIndex int) bool {
		return matches[firstIndex].Key < matches[secondIndex].Key
	})
	return matches, nil
}
