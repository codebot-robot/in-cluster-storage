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
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
)

type faultyCloseStream struct {
	ByteStream
	closeErr error
}

func (f *faultyCloseStream) Close() error {
	_ = f.ByteStream.Close()
	return f.closeErr
}

type faultyReadStream struct {
	ByteStream
	failRead atomic.Bool
	readErr  error
}

func (f *faultyReadStream) Read(p []byte) (int, error) {
	if f.failRead.Load() {
		return 0, f.readErr
	}
	return f.ByteStream.Read(p)
}

type faultyPutBackend struct {
	ObjectStorageBackend
	failPut atomic.Bool
	putErr  error
}

func (b *faultyPutBackend) PutObject(ctx context.Context, volumeID, key string, stream ByteStream) (string, error) {
	if b.failPut.Load() {
		return "", b.putErr
	}
	return b.ObjectStorageBackend.PutObject(ctx, volumeID, key, stream)
}

func TestFileByteStream_Close_AutoDelete(t *testing.T) {
	tf, err := os.CreateTemp("", "test-file-byte-stream-*")
	if err != nil {
		t.Fatalf("CreateTemp failed: %v", err)
	}
	path := tf.Name()
	if _, err := tf.WriteString("temporary data"); err != nil {
		t.Fatalf("WriteString failed: %v", err)
	}

	stream := NewByteStreamFromFile(tf, 14, true)
	if err := stream.Close(); err != nil {
		t.Fatalf("stream.Close failed: %v", err)
	}

	// Verify file was deleted
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatalf("expected file to be deleted, got err: %v", err)
	}

	// Second close should be benign
	_ = stream.Close()
}

func TestEncodeBlob_ReadFailureCleanup(t *testing.T) {
	// Large data exceeding MemoryThreshold (256KB) so a temp file is created
	data := bytes.Repeat([]byte("test large repetitive data "), 20000)
	baseStream := NewByteStreamFromBytes(data)
	fStream := &faultyReadStream{
		ByteStream: baseStream,
		readErr:    errors.New("injected read failure"),
	}
	fStream.failRead.Store(true)

	_, err := EncodeBlob(fStream, true)
	if err == nil {
		t.Fatal("expected EncodeBlob to fail on read failure, got nil")
	}
	if !strings.Contains(err.Error(), "compression failed") {
		t.Fatalf("expected error containing 'compression failed', got: %v", err)
	}
}

func TestPutBlobs_LooseBlob_BackendFailure(t *testing.T) {
	ctx := t.Context()
	backend := &faultyPutBackend{
		ObjectStorageBackend: newTestMemoryBackend(),
		putErr:               errors.New("injected loose blob put failure"),
	}
	backend.failPut.Store(true)

	store := NewStore(backend, 50) // 50 bytes threshold
	largeData := bytes.Repeat([]byte("large blob data exceeding threshold "), 5)
	batch := map[string]ByteStream{
		"large": NewByteStreamFromBytes(largeData),
	}

	err := store.PutBlobs(ctx, batch)
	if err == nil {
		t.Fatal("expected PutBlobs to fail on backend PutObject error, got nil")
	}
	if !strings.Contains(err.Error(), "injected loose blob put failure") {
		t.Fatalf("expected error containing injected failure, got: %v", err)
	}
}

func TestPutBlobs_LooseBlob_CloseFailure(t *testing.T) {
	ctx := t.Context()
	backend := newTestMemoryBackend()
	store := NewStore(backend, 50) // 50 bytes threshold

	// Random-like non-compressible data so compression is not a win and src is used directly
	data := make([]byte, 200)
	for i := range data {
		data[i] = byte((i * 37) % 256)
	}
	baseStream := NewByteStreamFromBytes(data)
	closeErr := errors.New("injected loose stream close failure")
	fStream := &faultyCloseStream{
		ByteStream: baseStream,
		closeErr:   closeErr,
	}

	batch := map[string]ByteStream{
		"large": fStream,
	}

	err := store.PutBlobs(ctx, batch)
	if err == nil {
		t.Fatal("expected PutBlobs to fail when looseStream.Close fails, got nil")
	}
	if !errors.Is(err, closeErr) {
		t.Fatalf("expected error wrapping %v, got: %v", closeErr, err)
	}
}

func TestPutBlobs_Packfile_BackendFailure(t *testing.T) {
	ctx := t.Context()
	backend := &faultyPutBackend{
		ObjectStorageBackend: newTestMemoryBackend(),
		putErr:               errors.New("injected packfile put failure"),
	}
	backend.failPut.Store(true)

	store := NewStore(backend, 10000) // 10000 bytes threshold
	smallData := []byte("small pack blob")
	batch := map[string]ByteStream{
		"small": NewByteStreamFromBytes(smallData),
	}

	err := store.PutBlobs(ctx, batch)
	if err == nil {
		t.Fatal("expected PutBlobs to fail on backend PutObject error for packfile, got nil")
	}
	if !strings.Contains(err.Error(), "injected packfile put failure") {
		t.Fatalf("expected error containing injected failure, got: %v", err)
	}
}

func TestGetBlob_CorruptStandaloneLooseBlob_FallbackToPack(t *testing.T) {
	ctx := t.Context()
	backend := newTestMemoryBackend()
	store := NewStore(backend, 1000)

	blobData := []byte("fallback-blob-data")
	blobSHA := fmt.Sprintf("%x", sha256.Sum256(blobData))

	// Put blob into packfile first
	if err := store.PutBlobs(ctx, map[string]ByteStream{
		blobSHA: NewByteStreamFromBytes(blobData),
	}); err != nil {
		t.Fatalf("PutBlobs failed: %v", err)
	}

	// Create a new store pointing to the same backend
	freshStore := NewStore(backend, 1000)

	// Now overwrite blobs/<sha> directly in backend with garbage bytes >= SingleBlobHeaderSize (64 bytes)
	// so direct loose blob read finds it, but DecodeLooseBlob fails on corrupt header
	corruptPayload := bytes.Repeat([]byte("badheader"), 10)
	corruptStream := NewByteStreamFromBytes(corruptPayload)
	if _, err := backend.PutObject(ctx, "", store.blobKey(blobSHA), corruptStream); err != nil {
		t.Fatalf("failed to inject corrupt loose blob: %v", err)
	}

	// GetBlob should log a warning about corrupt loose blob, fall back to RefreshIndexes,
	// find the blob in the packfile, and return it successfully!
	stream, err := freshStore.GetBlob(ctx, blobSHA)
	if err != nil {
		t.Fatalf("GetBlob failed to fall back to packfile: %v", err)
	}
	defer stream.Close()

	got, err := io.ReadAll(stream)
	if err != nil {
		t.Fatalf("ReadAll failed: %v", err)
	}
	if !bytes.Equal(got, blobData) {
		t.Fatalf("got %q, expected %q", string(got), string(blobData))
	}
}

func TestRefreshIndexes_CorruptPackfileSkipped(t *testing.T) {
	ctx := t.Context()
	backend := newTestMemoryBackend()
	store := NewStore(backend, 1000)

	// Write a valid packfile with one blob
	validData := []byte("valid-blob-data")
	validSHA := fmt.Sprintf("%x", sha256.Sum256(validData))
	if err := store.PutBlobs(ctx, map[string]ByteStream{
		validSHA: NewByteStreamFromBytes(validData),
	}); err != nil {
		t.Fatalf("PutBlobs failed: %v", err)
	}

	// Inject a corrupt packfile in backend
	corruptPackKey := filepath.Join("blobs", "corrupt-pack-1.pack")
	corruptPackStream := NewByteStreamFromBytes([]byte("corrupt-garbage-not-a-pack"))
	if _, err := backend.PutObject(ctx, "", corruptPackKey, corruptPackStream); err != nil {
		t.Fatalf("PutObject corrupt pack failed: %v", err)
	}

	freshStore := NewStore(backend, 1000)
	// RefreshIndexes must not fail the whole store; it should log a warning and continue
	if err := freshStore.RefreshIndexes(ctx); err != nil {
		t.Fatalf("RefreshIndexes failed on corrupt packfile: %v", err)
	}

	// The valid blob should still be readable
	stream, err := freshStore.GetBlob(ctx, validSHA)
	if err != nil {
		t.Fatalf("GetBlob on valid blob failed: %v", err)
	}
	defer stream.Close()

	got, err := io.ReadAll(stream)
	if err != nil {
		t.Fatalf("ReadAll failed: %v", err)
	}
	if !bytes.Equal(got, validData) {
		t.Fatalf("got %q, expected %q", string(got), string(validData))
	}
}
