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

package blob

import (
	"bytes"
	"context"
	"crypto/sha256"
	"fmt"
	"io"
	"path"
	"strings"
	"sync"
	"time"
)

// ObjectStorageBackend is the interface required for blob persistence.
type ObjectStorageBackend interface {
	PutObject(ctx context.Context, volumeID, key string, data []byte) (etag string, err error)
	GetObject(ctx context.Context, volumeID, key string, offset, length int64) (data []byte, err error)
	DeleteObject(ctx context.Context, volumeID, key string) error
	GetRedirectURL(ctx context.Context, volumeID, key string) (string, error)
	ListObjects(ctx context.Context, volumeID, prefix string) ([]string, error)
}

// BlobLocation describes where a blob can be fetched from.
type BlobLocation struct {
	IsStandalone bool
	PackID       string
	Entry        BlobEntry
}

// Store manages reading and writing blobs and packfiles.
type Store struct {
	backend        ObjectStorageBackend
	largeThreshold int64

	mu             sync.RWMutex
	packIndexes    map[string][]BlobEntry  // packID -> sorted entries
	shaToLocation  map[string]BlobLocation // sha256Hex -> location
	loadedPackList bool
}

// NewStore creates a new Blob Store with the given backend and threshold.
func NewStore(backend ObjectStorageBackend, largeThreshold int64) *Store {
	if largeThreshold <= 0 {
		largeThreshold = DefaultLargeBlobThreshold
	}
	return &Store{
		backend:        backend,
		largeThreshold: largeThreshold,
		packIndexes:    make(map[string][]BlobEntry),
		shaToLocation:  make(map[string]BlobLocation),
	}
}

func (s *Store) blobKey(sha256Hex string) string {
	return path.Join("blobs", sha256Hex)
}

func (s *Store) packKey(packID string) string {
	return path.Join("blobs", packID+".pack")
}

// PutBlobs writes a batch of blobs: large blobs are written standalone and smaller blobs are grouped into a packfile.
func (s *Store) PutBlobs(ctx context.Context, blobs map[string][]byte) error {
	if len(blobs) == 0 {
		return nil
	}

	var smallInputs []RawBlobInput
	for _, data := range blobs {
		if int64(len(data)) > s.largeThreshold {
			h := sha256.Sum256(data)
			shaHex := fmt.Sprintf("%x", h)

			rc, _, _, err := EncodeSingleBlob(bytes.NewReader(data), true)
			if err != nil {
				return fmt.Errorf("failed to encode standalone blob %s: %w", shaHex, err)
			}
			encodedBytes, err := io.ReadAll(rc)
			_ = rc.Close()
			if err != nil {
				return fmt.Errorf("failed to read standalone blob %s: %w", shaHex, err)
			}

			key := s.blobKey(shaHex)
			if _, err := s.backend.PutObject(ctx, "", key, encodedBytes); err != nil {
				return fmt.Errorf("failed to write standalone blob %s: %w", key, err)
			}

			s.mu.Lock()
			s.shaToLocation[shaHex] = BlobLocation{
				IsStandalone: true,
			}
			s.mu.Unlock()
		} else {
			smallInputs = append(smallInputs, RawBlobInput{
				Reader:   bytes.NewReader(data),
				Compress: true,
			})
		}
	}

	if len(smallInputs) == 0 {
		return nil
	}

	packID := fmt.Sprintf("%x", sha256.Sum256([]byte(fmt.Sprintf("pack-%d-%d", len(smallInputs), time.Now().UnixNano()))))
	packRC, entries, _, err := EncodePack(smallInputs)
	if err != nil {
		return fmt.Errorf("failed to encode pack: %w", err)
	}
	packBytes, err := io.ReadAll(packRC)
	_ = packRC.Close()
	if err != nil {
		return fmt.Errorf("failed to read pack bytes: %w", err)
	}

	if _, err := s.backend.PutObject(ctx, "", s.packKey(packID), packBytes); err != nil {
		return fmt.Errorf("failed to write packfile %s: %w", packID, err)
	}

	s.mu.Lock()
	s.packIndexes[packID] = entries
	for _, entry := range entries {
		s.shaToLocation[entry.SHA256Hex()] = BlobLocation{
			IsStandalone: false,
			PackID:       packID,
			Entry:        entry,
		}
	}
	s.mu.Unlock()

	return nil
}

// RefreshIndexes discovers all packfiles in the backend and reads their packed headers/tables.
func (s *Store) RefreshIndexes(ctx context.Context) error {
	objects, err := s.backend.ListObjects(ctx, "", "blobs/")
	if err != nil {
		return fmt.Errorf("failed to list blobs: %w", err)
	}

	for _, obj := range objects {
		if strings.HasSuffix(obj, ".pack") {
			packID := strings.TrimPrefix(obj, "blobs/")
			packID = strings.TrimSuffix(packID, ".pack")

			s.mu.RLock()
			_, exists := s.packIndexes[packID]
			s.mu.RUnlock()

			if !exists {
				// Read pack header + tables
				// First read 16 bytes pack header to get count N
				hdrBytes, err := s.backend.GetObject(ctx, "", obj, 0, int64(PackHeaderSize))
				if err != nil || len(hdrBytes) < PackHeaderSize {
					continue
				}
				entriesCount := int(hdrBytes[8])<<24 | int(hdrBytes[9])<<16 | int(hdrBytes[10])<<8 | int(hdrBytes[11])
				totalTableSize := int64(PackHeaderSize + entriesCount*64)

				tableData, err := s.backend.GetObject(ctx, "", obj, 0, totalTableSize)
				if err != nil || int64(len(tableData)) < totalTableSize {
					continue
				}

				entries, err := DecodePackTable(bytes.NewReader(tableData))
				if err != nil {
					continue
				}

				s.mu.Lock()
				s.packIndexes[packID] = entries
				for _, entry := range entries {
					s.shaToLocation[entry.SHA256Hex()] = BlobLocation{
						IsStandalone: false,
						PackID:       packID,
						Entry:        entry,
					}
				}
				s.mu.Unlock()
			}
		}
	}

	s.mu.Lock()
	s.loadedPackList = true
	s.mu.Unlock()

	return nil
}

// GetBlobReader returns a streaming io.ReadCloser for the requested blob.
func (s *Store) GetBlobReader(ctx context.Context, sha256Hex string) (io.ReadCloser, error) {
	s.mu.RLock()
	loc, found := s.shaToLocation[sha256Hex]
	s.mu.RUnlock()

	if found {
		if loc.IsStandalone {
			data, err := s.backend.GetObject(ctx, "", s.blobKey(sha256Hex), 0, 0)
			if err != nil {
				return nil, err
			}
			rc, _, err := DecodeSingleBlob(bytes.NewReader(data))
			return rc, err
		}

		// Read range from packfile
		packKey := s.packKey(loc.PackID)
		payload, err := s.backend.GetObject(ctx, "", packKey, int64(loc.Entry.DataOffset), int64(loc.Entry.StoredLength))
		if err != nil {
			return nil, fmt.Errorf("failed to read blob slice from pack %s: %w", packKey, err)
		}
		return DecodeBlobFromPayload(bytes.NewReader(payload), loc.Entry)
	}

	// 1. Try standalone blob directly
	standaloneKey := s.blobKey(sha256Hex)
	blobBytes, err := s.backend.GetObject(ctx, "", standaloneKey, 0, 0)
	if err == nil && len(blobBytes) >= SingleBlobHeaderSize {
		rc, _, err := DecodeSingleBlob(bytes.NewReader(blobBytes))
		if err == nil {
			s.mu.Lock()
			s.shaToLocation[sha256Hex] = BlobLocation{IsStandalone: true}
			s.mu.Unlock()
			return rc, nil
		}
	}

	// 2. Try refreshing pack indexes
	_ = s.RefreshIndexes(ctx)

	s.mu.RLock()
	loc, found = s.shaToLocation[sha256Hex]
	s.mu.RUnlock()

	if found {
		packKey := s.packKey(loc.PackID)
		payload, err := s.backend.GetObject(ctx, "", packKey, int64(loc.Entry.DataOffset), int64(loc.Entry.StoredLength))
		if err != nil {
			return nil, fmt.Errorf("failed to read blob from pack %s: %w", packKey, err)
		}
		return DecodeBlobFromPayload(bytes.NewReader(payload), loc.Entry)
	}

	return nil, fmt.Errorf("blob %s not found", sha256Hex)
}

// GetBlob retrieves and verifies a blob, returning its contents.
func (s *Store) GetBlob(ctx context.Context, sha256Hex string) ([]byte, error) {
	rc, err := s.GetBlobReader(ctx, sha256Hex)
	if err != nil {
		return nil, err
	}
	defer rc.Close()
	return io.ReadAll(rc)
}

// HasBlob checks if a blob exists in the store.
func (s *Store) HasBlob(ctx context.Context, sha256Hex string) (bool, error) {
	s.mu.RLock()
	_, found := s.shaToLocation[sha256Hex]
	s.mu.RUnlock()
	if found {
		return true, nil
	}

	// Try reading
	rc, err := s.GetBlobReader(ctx, sha256Hex)
	if err == nil {
		_ = rc.Close()
		return true, nil
	}
	return false, nil
}
