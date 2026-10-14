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
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/gke-labs/in-cluster-storage/pkg/objectfs/blob"
)

type faultyReadStream struct {
	blob.ByteStream
	readErr error
}

func (f *faultyReadStream) Read(p []byte) (int, error) {
	return 0, f.readErr
}

func TestBackend_PutObject_CleanupOnWriteError(t *testing.T) {
	ctx := t.Context()
	rootDir := t.TempDir()

	b, err := New(rootDir)
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}

	baseStream := blob.NewByteStreamFromBytes([]byte("hello world"))
	fStream := &faultyReadStream{
		ByteStream: baseStream,
		readErr:    errors.New("injected read error"),
	}

	_, err = b.PutObject(ctx, "vol-1", "test-key", fStream)
	if err == nil {
		t.Fatal("expected PutObject to fail on stream read error, got nil")
	}
	if !strings.Contains(err.Error(), "failed to write object data") {
		t.Fatalf("expected error containing 'failed to write object data', got: %v", err)
	}

	// Verify that no temporary upload files were leaked in the volume directory
	volDir := filepath.Join(rootDir, "vol-1")
	entries, err := os.ReadDir(volDir)
	if err != nil && !os.IsNotExist(err) {
		t.Fatalf("ReadDir failed: %v", err)
	}
	for _, entry := range entries {
		if strings.HasPrefix(entry.Name(), ".tmp-upload-") {
			t.Fatalf("found leaked temp file: %s", entry.Name())
		}
	}
}

func TestBackend_GetObject_SuccessAndCleanup(t *testing.T) {
	ctx := t.Context()
	rootDir := t.TempDir()

	b, err := New(rootDir)
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}

	content := []byte("object content to read")
	if _, err := b.PutObject(ctx, "vol-1", "read-key", blob.NewByteStreamFromBytes(content)); err != nil {
		t.Fatalf("PutObject failed: %v", err)
	}

	var buf strings.Builder
	if err := b.GetObject(ctx, "vol-1", "read-key", 0, 0, &buf); err != nil {
		t.Fatalf("GetObject failed: %v", err)
	}
	if buf.String() != string(content) {
		t.Fatalf("got %q, want %q", buf.String(), string(content))
	}
}
