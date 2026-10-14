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

package filesystemstorage

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/gke-labs/in-cluster-storage/pkg/errutil"
	"github.com/gke-labs/in-cluster-storage/pkg/objectfs/blob"
	"k8s.io/klog/v2"
)

var (
	// ErrNotSupported is returned when an operation is not supported by the backend.
	ErrNotSupported = errors.New("operation not supported by backend")
)

// Backend is a local filesystem implementation of ObjectStorageBackend.
type Backend struct {
	rootDir string
}

// New creates a new filesystem-backed object storage backend rooted at rootDir.
func New(rootDir string) (*Backend, error) {
	if rootDir == "" {
		return nil, errors.New("root directory cannot be empty")
	}
	absDir, err := filepath.Abs(rootDir)
	if err != nil {
		return nil, fmt.Errorf("failed to resolve absolute path for %s: %w", rootDir, err)
	}
	if err := os.MkdirAll(absDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create root directory %s: %w", absDir, err)
	}
	return &Backend{
		rootDir: absDir,
	}, nil
}

func (b *Backend) storageKey(volumeID, key string) string {
	if volumeID == "" || strings.HasPrefix(key, "volumes/") || strings.HasPrefix(key, "blobs/") {
		return key
	}
	return fmt.Sprintf("%s/%s", volumeID, key)
}

func (b *Backend) objectPath(volumeID, key string) string {
	k := b.storageKey(volumeID, key)
	return filepath.Join(b.rootDir, filepath.FromSlash(k))
}

func (b *Backend) PutObject(ctx context.Context, volumeID, key string, stream blob.ByteStream) (string, error) {
	targetPath := b.objectPath(volumeID, key)
	dir := filepath.Dir(targetPath)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return "", fmt.Errorf("failed to create directory %s: %w", dir, err)
	}

	tf, err := os.CreateTemp(dir, ".tmp-upload-*")
	if err != nil {
		return "", fmt.Errorf("failed to create temp file in %s: %w", dir, err)
	}
	tempPath := tf.Name()
	cleanup := true
	defer func() {
		if cleanup {
			errutil.CloseAndLog(tf, "temp upload file")
			if remErr := os.Remove(tempPath); remErr != nil && !os.IsNotExist(remErr) {
				klog.Warningf("failed to remove temp upload file %s: %v", tempPath, remErr)
			}
		}
	}()

	if err := stream.Rewind(); err != nil {
		return "", fmt.Errorf("failed to rewind stream: %w", err)
	}

	hasher := sha256.New()
	mw := io.MultiWriter(tf, hasher)
	if _, err := io.Copy(mw, stream); err != nil {
		return "", fmt.Errorf("failed to write object data: %w", err)
	}

	if err := tf.Sync(); err != nil {
		return "", fmt.Errorf("failed to fsync temp file: %w", err)
	}
	if err := tf.Close(); err != nil {
		return "", fmt.Errorf("failed to close temp file: %w", err)
	}

	if err := os.Rename(tempPath, targetPath); err != nil {
		return "", fmt.Errorf("failed to rename temp file to %s: %w", targetPath, err)
	}
	cleanup = false

	etag := fmt.Sprintf("%x", hasher.Sum(nil))
	return etag, nil
}

func (b *Backend) GetObject(ctx context.Context, volumeID, key string, offset, length int64, w io.Writer) error {
	targetPath := b.objectPath(volumeID, key)
	f, err := os.Open(targetPath)
	if err != nil {
		if os.IsNotExist(err) {
			return fmt.Errorf("object %s not found in volume %s: %w", key, volumeID, err)
		}
		return err
	}
	defer errutil.CloseAndLog(f, "backend object file")

	st, err := f.Stat()
	if err != nil {
		return err
	}

	total := st.Size()
	if offset >= total {
		return nil
	}
	if offset > 0 {
		if _, err := f.Seek(offset, io.SeekStart); err != nil {
			return err
		}
	}

	end := offset + length
	if length <= 0 || end > total {
		length = total - offset
	}

	_, err = io.CopyN(w, f, length)
	return err
}

func (b *Backend) DeleteObject(ctx context.Context, volumeID, key string) error {
	targetPath := b.objectPath(volumeID, key)
	if err := os.Remove(targetPath); err != nil && !os.IsNotExist(err) {
		return err
	}
	return nil
}

func (b *Backend) GetRedirectURL(ctx context.Context, volumeID, key string) (string, error) {
	return "", ErrNotSupported
}

func (b *Backend) ListObjects(ctx context.Context, volumeID, prefix string) ([]string, error) {
	fullPrefix := b.storageKey(volumeID, prefix)
	var matches []string

	err := filepath.Walk(b.rootDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			if os.IsNotExist(err) {
				return nil
			}
			return err
		}
		if info.IsDir() {
			return nil
		}
		if strings.HasPrefix(info.Name(), ".tmp-upload-") {
			return nil
		}
		rel, err := filepath.Rel(b.rootDir, path)
		if err != nil {
			return err
		}
		relSlash := filepath.ToSlash(rel)
		if strings.HasPrefix(relSlash, fullPrefix) {
			matches = append(matches, relSlash)
		}
		return nil
	})
	if err != nil && !os.IsNotExist(err) {
		return nil, err
	}
	return matches, nil
}
