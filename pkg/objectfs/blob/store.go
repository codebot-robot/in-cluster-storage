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
	"os"
	"path"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gke-labs/in-cluster-storage/pkg/errutil"
	"k8s.io/klog/v2"
)

// ObjectStorageBackend is the interface required for blob persistence.
type ObjectStorageBackend interface {
	PutObject(ctx context.Context, volumeID, key string, stream ByteStream) (etag string, err error)
	GetObject(ctx context.Context, volumeID, key string, offset, length int64, w io.Writer) error
	DeleteObject(ctx context.Context, volumeID, key string) error
	GetRedirectURL(ctx context.Context, volumeID, key string) (string, error)
	ListObjects(ctx context.Context, volumeID, prefix string) ([]string, error)
}

// BlobLocation describes where a blob can be fetched from.
type BlobLocation struct {
	IsStandalone bool
	PackKey      string
	Entry        BlobEntry
}

// Store manages reading and writing blobs and packfiles.
type Store struct {
	backend        ObjectStorageBackend
	largeThreshold int64

	mu            sync.RWMutex
	packIndexes   map[string][]BlobEntry  // packKey -> sorted entries
	shaToLocation map[string]BlobLocation // sha256Hex -> location
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

func (s *Store) packKey(packID string, count int) string {
	return path.Join("blobs", fmt.Sprintf("%s-%d.pack", packID, count))
}

// PutBlobs writes a batch of blobs: large blobs are written standalone loose blobs and smaller blobs are grouped into a packfile.
func (s *Store) PutBlobs(ctx context.Context, blobs map[string]ByteStream) error {
	if len(blobs) == 0 {
		return nil
	}

	var smallInputs []*EncodedBlob
	for _, stream := range blobs {
		prep, err := EncodeBlob(stream, true)
		if err != nil {
			return fmt.Errorf("failed to encode blob: %w", err)
		}

		if stream.Length() > s.largeThreshold {
			shaHex := prep.SHA256Hex()
			looseStream, err := EncodeLooseBlob(prep)
			if err != nil {
				errutil.CloseAndLog(prep, "encoded blob")
				return fmt.Errorf("failed to encode loose blob %s: %w", shaHex, err)
			}

			key := s.blobKey(shaHex)
			if _, err := s.backend.PutObject(ctx, "", key, looseStream); err != nil {
				errutil.CloseAndLog(looseStream, "loose blob stream")
				return fmt.Errorf("failed to write standalone loose blob %s: %w", key, err)
			}
			if err := looseStream.Close(); err != nil {
				return fmt.Errorf("failed to close loose blob stream %s: %w", key, err)
			}

			s.mu.Lock()
			s.shaToLocation[shaHex] = BlobLocation{
				IsStandalone: true,
			}
			s.mu.Unlock()
		} else {
			smallInputs = append(smallInputs, prep)
		}
	}

	if len(smallInputs) == 0 {
		return nil
	}

	packID := fmt.Sprintf("%x", sha256.Sum256([]byte(fmt.Sprintf("pack-%d-%d", len(smallInputs), time.Now().UnixNano()))))
	tf, err := os.CreateTemp("", "objectfs-pack-write-*")
	if err != nil {
		return fmt.Errorf("failed to create temp file for packfile: %w", err)
	}
	tempPath := tf.Name()

	packFile, err := WritePackfile(tf, smallInputs)
	if err != nil {
		errutil.CloseAndLog(tf, "temp packfile")
		if remErr := os.Remove(tempPath); remErr != nil && !os.IsNotExist(remErr) {
			klog.Warningf("failed to remove temp packfile %s: %v", tempPath, remErr)
		}
		return fmt.Errorf("failed to write packfile: %w", err)
	}

	st, err := tf.Stat()
	if err != nil {
		errutil.CloseAndLog(tf, "temp packfile")
		if remErr := os.Remove(tempPath); remErr != nil && !os.IsNotExist(remErr) {
			klog.Warningf("failed to remove temp packfile %s: %v", tempPath, remErr)
		}
		return fmt.Errorf("failed to stat temp packfile: %w", err)
	}
	if _, err := tf.Seek(0, io.SeekStart); err != nil {
		errutil.CloseAndLog(tf, "temp packfile")
		if remErr := os.Remove(tempPath); remErr != nil && !os.IsNotExist(remErr) {
			klog.Warningf("failed to remove temp packfile %s: %v", tempPath, remErr)
		}
		return fmt.Errorf("failed to seek temp packfile: %w", err)
	}

	packStream := NewByteStreamFromFile(tf, st.Size(), true)
	packFileKey := s.packKey(packID, len(smallInputs))
	if _, err := s.backend.PutObject(ctx, "", packFileKey, packStream); err != nil {
		errutil.CloseAndLog(packStream, "temp packfile stream")
		return fmt.Errorf("failed to write packfile %s to backend: %w", packFileKey, err)
	}
	if err := packStream.Close(); err != nil {
		return fmt.Errorf("failed to close packfile stream %s: %w", packFileKey, err)
	}

	s.mu.Lock()
	s.packIndexes[packFileKey] = packFile.Entries
	for _, entry := range packFile.Entries {
		s.shaToLocation[entry.SHA256Hex()] = BlobLocation{
			IsStandalone: false,
			PackKey:      packFileKey,
			Entry:        entry,
		}
	}
	s.mu.Unlock()

	return nil
}

// RefreshIndexes discovers all packfiles in the backend and loads their packed headers and tables.
func (s *Store) RefreshIndexes(ctx context.Context) error {
	objects, err := s.backend.ListObjects(ctx, "", "blobs/")
	if err != nil {
		klog.Warningf("Failed to list blobs during RefreshIndexes: %v", err)
		return fmt.Errorf("failed to list blobs: %w", err)
	}

	for _, obj := range objects {
		if strings.HasSuffix(obj, ".pack") {
			s.mu.RLock()
			_, exists := s.packIndexes[obj]
			s.mu.RUnlock()

			if exists {
				continue
			}

			// Smuggle count from filename format: <packID>-<count>.pack
			baseName := strings.TrimSuffix(path.Base(obj), ".pack")
			var count int
			if idx := strings.LastIndex(baseName, "-"); idx != -1 {
				if parsed, err := strconv.Atoi(baseName[idx+1:]); err == nil && parsed > 0 {
					count = parsed
				}
			}

			var tableData []byte
			if count > 0 {
				tableSize := int64(PackHeaderSize + count*32 + (count+1)*4)
				var buf bytes.Buffer
				if err := s.backend.GetObject(ctx, "", obj, 0, tableSize, &buf); err == nil && int64(buf.Len()) >= tableSize {
					tableData = buf.Bytes()
				}
			}

			if len(tableData) == 0 {
				var hdrBuf bytes.Buffer
				if err := s.backend.GetObject(ctx, "", obj, 0, int64(PackHeaderSize), &hdrBuf); err != nil || hdrBuf.Len() < PackHeaderSize {
					klog.Warningf("Failed reading header for packfile %s: %v", obj, err)
					continue
				}
				rawHdr := hdrBuf.Bytes()
				entriesCount := int(rawHdr[8])<<24 | int(rawHdr[9])<<16 | int(rawHdr[10])<<8 | int(rawHdr[11])
				totalTableSize := int64(PackHeaderSize + entriesCount*32 + (entriesCount+1)*4)

				var totalBuf bytes.Buffer
				if err := s.backend.GetObject(ctx, "", obj, 0, totalTableSize, &totalBuf); err != nil || int64(totalBuf.Len()) < totalTableSize {
					klog.Warningf("Failed reading tables for packfile %s: %v", obj, err)
					continue
				}
				tableData = totalBuf.Bytes()
			}

			packFile, err := DecodePackTable(bytes.NewReader(tableData))
			if err != nil {
				klog.Warningf("Failed to decode table for packfile %s: %v", obj, err)
				continue
			}

			s.mu.Lock()
			s.packIndexes[obj] = packFile.Entries
			for _, entry := range packFile.Entries {
				s.shaToLocation[entry.SHA256Hex()] = BlobLocation{
					IsStandalone: false,
					PackKey:      obj,
					Entry:        entry,
				}
			}
			s.mu.Unlock()
		} else {
			baseName := strings.TrimPrefix(obj, "blobs/")
			baseName = strings.TrimPrefix(baseName, "/")
			if len(baseName) == 64 && !strings.Contains(baseName, ".") {
				s.mu.Lock()
				if _, exists := s.shaToLocation[baseName]; !exists {
					s.shaToLocation[baseName] = BlobLocation{
						IsStandalone: true,
					}
				}
				s.mu.Unlock()
			}
		}
	}

	return nil
}

// ListBlobsOptions specifies filtering and pagination options for ListBlobs.
type ListBlobsOptions struct {
	FromSHA   string
	Limit     int
	SHAPrefix string
}

// ListBlobs returns the list of blob SHA256 hashes available in the store matching the given options.
func (s *Store) ListBlobs(ctx context.Context, opts ListBlobsOptions) ([]string, bool, error) {
	if err := s.RefreshIndexes(ctx); err != nil {
		return nil, false, err
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	var matching []string
	for sha := range s.shaToLocation {
		if opts.SHAPrefix != "" && !strings.HasPrefix(sha, opts.SHAPrefix) {
			continue
		}
		if opts.FromSHA != "" && sha <= opts.FromSHA {
			continue
		}
		matching = append(matching, sha)
	}
	sort.Strings(matching)

	if opts.Limit <= 0 || len(matching) <= opts.Limit {
		return matching, true, nil
	}

	return matching[:opts.Limit], false, nil
}

// GetBlob retrieves a blob as a ByteStream directly.
func (s *Store) GetBlob(ctx context.Context, sha256Hex string) (ByteStream, error) {
	s.mu.RLock()
	loc, found := s.shaToLocation[sha256Hex]
	s.mu.RUnlock()

	if found {
		if loc.IsStandalone {
			var buf bytes.Buffer
			if err := s.backend.GetObject(ctx, "", s.blobKey(sha256Hex), 0, 0, &buf); err != nil {
				return nil, err
			}
			stream, _, err := DecodeLooseBlob(&buf)
			return stream, err
		}

		// Read range from packfile
		var buf bytes.Buffer
		if err := s.backend.GetObject(ctx, "", loc.PackKey, int64(loc.Entry.DataOffset), int64(loc.Entry.StoredLength), &buf); err != nil {
			return nil, fmt.Errorf("failed to read blob slice from pack %s: %w", loc.PackKey, err)
		}
		return DecodeBlobFromPayload(&buf, loc.Entry)
	}

	// 1. Try standalone loose blob directly
	standaloneKey := s.blobKey(sha256Hex)
	var looseBuf bytes.Buffer
	if err := s.backend.GetObject(ctx, "", standaloneKey, 0, 0, &looseBuf); err == nil && looseBuf.Len() >= SingleBlobHeaderSize {
		stream, _, err := DecodeLooseBlob(&looseBuf)
		if err != nil {
			klog.Warningf("failed to decode standalone loose blob %s: %v", standaloneKey, err)
		} else {
			s.mu.Lock()
			s.shaToLocation[sha256Hex] = BlobLocation{IsStandalone: true}
			s.mu.Unlock()
			return stream, nil
		}
	}

	// 2. Try refreshing pack indexes
	// TODO: cache miss rate limiting to avoid refreshing on every unknown blob
	if err := s.RefreshIndexes(ctx); err != nil {
		klog.Warningf("RefreshIndexes failed during blob fetch %s: %v", sha256Hex, err)
	}

	s.mu.RLock()
	loc, found = s.shaToLocation[sha256Hex]
	s.mu.RUnlock()

	if found {
		var buf bytes.Buffer
		if err := s.backend.GetObject(ctx, "", loc.PackKey, int64(loc.Entry.DataOffset), int64(loc.Entry.StoredLength), &buf); err != nil {
			return nil, fmt.Errorf("failed to read blob from pack %s: %w", loc.PackKey, err)
		}
		return DecodeBlobFromPayload(&buf, loc.Entry)
	}

	return nil, fmt.Errorf("blob %s not found", sha256Hex)
}
