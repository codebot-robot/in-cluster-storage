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

package gcsstorage

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	"cloud.google.com/go/storage"
	"github.com/gke-labs/in-cluster-storage/pkg/errutil"
	"github.com/gke-labs/in-cluster-storage/pkg/objectfs/blob"
	"google.golang.org/api/iterator"
)

var (
	// ErrNotSupported is returned when an operation is not supported by the backend.
	ErrNotSupported = errors.New("operation not supported by backend")
)

// Config holds configuration options for initializing a GCS backend.
type Config struct {
	Bucket string
	Prefix string
}

// Backend is a Google Cloud Storage implementation of ObjectStorageBackend.
type Backend struct {
	client *storage.Client
	bucket string
	prefix string
}

// New creates a new GCS backend with the provided configuration.
func New(ctx context.Context, cfg Config) (*Backend, error) {
	if cfg.Bucket == "" {
		return nil, errors.New("gcs bucket must be specified")
	}
	client, err := storage.NewClient(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to create GCS client: %w", err)
	}
	return &Backend{
		client: client,
		bucket: cfg.Bucket,
		prefix: strings.Trim(cfg.Prefix, "/"),
	}, nil
}

func storageKey(volumeID, key string) string {
	if volumeID == "" || strings.HasPrefix(key, "volumes/") || strings.HasPrefix(key, "blobs/") {
		return key
	}
	return fmt.Sprintf("%s/%s", volumeID, key)
}

func (b *Backend) fullKey(volumeID, key string) string {
	k := storageKey(volumeID, key)
	if b.prefix == "" {
		return k
	}
	return fmt.Sprintf("%s/%s", b.prefix, k)
}

func (b *Backend) relativeKey(fullKey string) string {
	if b.prefix == "" {
		return fullKey
	}
	return strings.TrimPrefix(strings.TrimPrefix(fullKey, b.prefix), "/")
}

func (b *Backend) PutObject(ctx context.Context, volumeID, key string, stream blob.ByteStream) (string, error) {
	if err := stream.Rewind(); err != nil {
		return "", fmt.Errorf("failed to rewind stream: %w", err)
	}

	gcsKey := b.fullKey(volumeID, key)
	obj := b.client.Bucket(b.bucket).Object(gcsKey)
	w := obj.NewWriter(ctx)

	if _, err := io.Copy(w, stream); err != nil {
		errutil.CloseAndLog(w, "gcs writer")
		return "", fmt.Errorf("failed to write gcs object %s: %w", gcsKey, err)
	}
	if err := w.Close(); err != nil {
		return "", fmt.Errorf("failed to close gcs writer %s: %w", gcsKey, err)
	}

	attrs, err := obj.Attrs(ctx)
	if err != nil {
		return fmt.Sprintf("%d", w.Attrs().Generation), nil
	}
	return fmt.Sprintf("%d", attrs.Generation), nil
}

func (b *Backend) GetObject(ctx context.Context, volumeID, key string, offset, length int64, w io.Writer) error {
	gcsKey := b.fullKey(volumeID, key)
	obj := b.client.Bucket(b.bucket).Object(gcsKey)

	var r *storage.Reader
	var err error
	if offset > 0 || length > 0 {
		readLen := length
		if readLen <= 0 {
			readLen = -1
		}
		r, err = obj.NewRangeReader(ctx, offset, readLen)
	} else {
		r, err = obj.NewReader(ctx)
	}

	if err != nil {
		if errors.Is(err, storage.ErrObjectNotExist) {
			return fmt.Errorf("object %s not found in volume %s: %w", key, volumeID, err)
		}
		return fmt.Errorf("failed to get gcs object %s: %w", gcsKey, err)
	}
	defer errutil.CloseAndLog(r, "gcs reader")

	if _, err := io.Copy(w, r); err != nil {
		return fmt.Errorf("failed to stream gcs object %s: %w", gcsKey, err)
	}
	return nil
}

func (b *Backend) DeleteObject(ctx context.Context, volumeID, key string) error {
	gcsKey := b.fullKey(volumeID, key)
	err := b.client.Bucket(b.bucket).Object(gcsKey).Delete(ctx)
	if err != nil && !errors.Is(err, storage.ErrObjectNotExist) {
		return fmt.Errorf("failed to delete gcs object %s: %w", gcsKey, err)
	}
	return nil
}

func (b *Backend) GetRedirectURL(ctx context.Context, volumeID, key string) (string, error) {
	gcsKey := b.fullKey(volumeID, key)
	opts := &storage.SignedURLOptions{
		Scheme:  storage.SigningSchemeV4,
		Method:  "GET",
		Expires: time.Now().Add(15 * time.Minute),
	}
	url, err := b.client.Bucket(b.bucket).SignedURL(gcsKey, opts)
	if err != nil {
		return "", ErrNotSupported
	}
	return url, nil
}

func (b *Backend) ListObjects(ctx context.Context, volumeID, prefix string) ([]string, error) {
	fullPrefix := b.fullKey(volumeID, prefix)
	query := &storage.Query{
		Prefix: fullPrefix,
	}
	it := b.client.Bucket(b.bucket).Objects(ctx, query)
	var matches []string
	for {
		attrs, err := it.Next()
		if errors.Is(err, iterator.Done) {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("failed to list gcs objects with prefix %s: %w", fullPrefix, err)
		}
		matches = append(matches, b.relativeKey(attrs.Name))
	}
	return matches, nil
}
