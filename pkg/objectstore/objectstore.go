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

package objectstore

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/url"
	"path/filepath"
	"strings"

	"github.com/gke-labs/in-cluster-storage/pkg/objectfs/blob"
	"github.com/gke-labs/in-cluster-storage/pkg/objectstore/filesystemstorage"
	"github.com/gke-labs/in-cluster-storage/pkg/objectstore/gcsstorage"
	"github.com/gke-labs/in-cluster-storage/pkg/objectstore/inmemorystorage"
	"github.com/gke-labs/in-cluster-storage/pkg/objectstore/s3storage"
)

var (
	// ErrNotSupported is returned when an operation (such as generating a redirect URL)
	// is not supported by the underlying storage backend.
	ErrNotSupported = errors.New("operation not supported by backend")
	// ErrNotFound is returned when the requested object does not exist.
	ErrNotFound = errors.New("object not found")
)

// ObjectEntry represents an object entry returned by ListObjects.
type ObjectEntry = blob.ObjectEntry

// Backend defines the common interface for object storage persistence.
type Backend interface {
	// PutObject atomically uploads the provided byte stream to the object store at the given key
	// scoped to volumeID. It returns an ETag or generation identifier for the written object.
	PutObject(ctx context.Context, volumeID, key string, stream blob.ByteStream) (etag string, err error)

	// GetObject retrieves the object (or a byte range if offset/length > 0) and writes its contents to w.
	// If the object does not exist, an error is returned. If offset is at or beyond the end of the object,
	// zero bytes are written without error.
	GetObject(ctx context.Context, volumeID, key string, offset, length int64, w io.Writer) error

	// DeleteObject deletes the object located at the given key scoped to volumeID.
	// If the object does not exist, DeleteObject succeeds idempotently.
	DeleteObject(ctx context.Context, volumeID, key string) error

	// GetRedirectURL returns a direct, presigned or signed URL for clients to read the object
	// directly from backing storage (e.g. S3 presigned GET or GCS signed URL).
	// If direct client redirection is not supported by the backend (e.g. local filesystem)
	// or no signing credentials are configured, it returns ErrNotSupported (or empty string for memory backend).
	GetRedirectURL(ctx context.Context, volumeID, key string) (string, error)

	// ListObjects returns relative object entries matching the given prefix within the volumeID scope.
	ListObjects(ctx context.Context, volumeID, prefix string) ([]ObjectEntry, error)
}

// StorageKey resolves the full relative key within the storage root for a given volumeID and key.
func StorageKey(volumeID, key string) string {
	if volumeID == "" || strings.HasPrefix(key, "volumes/") || strings.HasPrefix(key, "blobs/") {
		return key
	}
	return fmt.Sprintf("%s/%s", volumeID, key)
}

// Open initializes and returns a Backend based on the provided URL scheme.
// Supported schemes:
//   - memory:// (or "memory")
//   - file:///path/to/dir
//   - s3://bucket/prefix?endpoint=http://minio:9000&region=us-east-1
//   - gs://bucket/prefix (or gcs://bucket/prefix)
func Open(ctx context.Context, rawURL string) (Backend, error) {
	if rawURL == "" || rawURL == "memory" || rawURL == "memory://" {
		return inmemorystorage.New(), nil
	}

	u, err := url.Parse(rawURL)
	if err != nil {
		return nil, fmt.Errorf("invalid backend URL %q: %w", rawURL, err)
	}

	switch u.Scheme {
	case "memory":
		return inmemorystorage.New(), nil

	case "file":
		path := u.Path
		if u.Host != "" {
			path = filepath.Join(u.Host, u.Path)
		}
		if path == "" {
			return nil, fmt.Errorf("file backend requires a non-empty path: %s", rawURL)
		}
		return filesystemstorage.New(path)

	case "s3":
		bucket := u.Host
		if bucket == "" {
			return nil, fmt.Errorf("s3 backend requires a bucket name: %s", rawURL)
		}
		prefix := strings.TrimPrefix(u.Path, "/")
		query := u.Query()
		endpoint := query.Get("endpoint")
		region := query.Get("region")
		usePathStyle := query.Get("use_path_style") == "true" || query.Get("s3ForcePathStyle") == "true"

		return s3storage.New(ctx, s3storage.Config{
			Bucket:       bucket,
			Prefix:       prefix,
			Endpoint:     endpoint,
			Region:       region,
			UsePathStyle: usePathStyle,
		})

	case "gs", "gcs":
		bucket := u.Host
		if bucket == "" {
			return nil, fmt.Errorf("gcs backend requires a bucket name: %s", rawURL)
		}
		prefix := strings.TrimPrefix(u.Path, "/")

		return gcsstorage.New(ctx, gcsstorage.Config{
			Bucket: bucket,
			Prefix: prefix,
		})

	default:
		return nil, fmt.Errorf("unsupported backend scheme %q in URL %s", u.Scheme, rawURL)
	}
}
