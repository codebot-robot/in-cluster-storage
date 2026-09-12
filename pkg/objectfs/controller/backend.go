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
	"crypto/sha256"
	"fmt"
	"sync"
)

// ObjectStorageBackend is the interface for persisting objects to backing storage (e.g., S3/GCS or Memory).
type ObjectStorageBackend interface {
	PutObject(ctx context.Context, volumeID, key string, data []byte) (etag string, err error)
	GetObject(ctx context.Context, volumeID, key string, offset, length int64) (data []byte, err error)
	DeleteObject(ctx context.Context, volumeID, key string) error
	GetRedirectURL(ctx context.Context, volumeID, key string) (string, error)
}

// MemoryBackend is an in-memory implementation of ObjectStorageBackend.
type MemoryBackend struct {
	mu      sync.RWMutex
	objects map[string][]byte // key: volumeID + "/" + key
}

// NewMemoryBackend creates a new in-memory object storage backend.
func NewMemoryBackend() *MemoryBackend {
	return &MemoryBackend{
		objects: make(map[string][]byte),
	}
}

func (m *MemoryBackend) storageKey(volumeID, key string) string {
	return fmt.Sprintf("%s/%s", volumeID, key)
}

func (m *MemoryBackend) PutObject(ctx context.Context, volumeID, key string, data []byte) (string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	k := m.storageKey(volumeID, key)
	buf := make([]byte, len(data))
	copy(buf, data)
	m.objects[k] = buf

	h := sha256.Sum256(buf)
	return fmt.Sprintf("%x", h), nil
}

func (m *MemoryBackend) GetObject(ctx context.Context, volumeID, key string, offset, length int64) ([]byte, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	k := m.storageKey(volumeID, key)
	data, ok := m.objects[k]
	if !ok {
		return nil, fmt.Errorf("object %s not found in volume %s", key, volumeID)
	}

	total := int64(len(data))
	if offset >= total {
		return []byte{}, nil
	}
	end := offset + length
	if length <= 0 || end > total {
		end = total
	}

	res := make([]byte, end-offset)
	copy(res, data[offset:end])
	return res, nil
}

func (m *MemoryBackend) DeleteObject(ctx context.Context, volumeID, key string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	k := m.storageKey(volumeID, key)
	delete(m.objects, k)
	return nil
}

func (m *MemoryBackend) GetRedirectURL(ctx context.Context, volumeID, key string) (string, error) {
	// For memory backend or when direct redirect is not used, returns empty string.
	return "", nil
}
