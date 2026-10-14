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
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	pb "github.com/gke-labs/in-cluster-storage/pkg/api/wal/v1alpha1"
	"github.com/gke-labs/in-cluster-storage/pkg/objectfs/blob"
	"github.com/gke-labs/in-cluster-storage/pkg/objectstore/inmemorystorage"
	"github.com/gke-labs/in-cluster-storage/pkg/wal"
	"github.com/google/uuid"
	"google.golang.org/grpc"
)

func TestStreamStore_Recover_NonUUIDDirectory(t *testing.T) {
	ctx := t.Context()
	dataDir := t.TempDir()
	streamsDir := filepath.Join(dataDir, "streams")
	if err := os.MkdirAll(streamsDir, 0755); err != nil {
		t.Fatalf("failed to create streams dir: %v", err)
	}

	// Create a valid UUID dir and an invalid non-UUID dir
	validID := uuid.New()
	if err := os.MkdirAll(filepath.Join(streamsDir, validID.String()), 0755); err != nil {
		t.Fatalf("failed to create valid stream dir: %v", err)
	}
	if err := os.MkdirAll(filepath.Join(streamsDir, "not-a-uuid"), 0755); err != nil {
		t.Fatalf("failed to create non-uuid dir: %v", err)
	}

	backend := inmemorystorage.New()
	store := NewStreamStore(dataDir, backend, time.Hour, DefaultStreamSealBytes)
	defer func() { _ = store.Close(ctx) }()
	if err := store.Recover(ctx, nil); err != nil {
		t.Fatalf("expected Recover to succeed while ignoring non-UUID dir, got: %v", err)
	}

	if store.getStream(validID) == nil {
		t.Fatalf("expected valid stream to be recovered")
	}
}

func TestStreamStore_Recover_ReadDirError(t *testing.T) {
	ctx := t.Context()
	dataDir := t.TempDir()
	streamsPath := filepath.Join(dataDir, "streams")
	// Create a file instead of a directory at "streams" path so ReadDir fails with ENOTDIR
	if err := os.WriteFile(streamsPath, []byte("not a dir"), 0644); err != nil {
		t.Fatalf("failed to create streams file: %v", err)
	}

	backend := inmemorystorage.New()
	store := NewStreamStore(dataDir, backend, time.Hour, DefaultStreamSealBytes)
	defer func() { _ = store.Close(ctx) }()
	err := store.Recover(ctx, nil)
	if err == nil {
		t.Fatalf("expected error from Recover when ReadDir fails, got nil")
	}
	if !strings.Contains(err.Error(), "failed to read streams directory") {
		t.Errorf("expected failed to read streams directory in error, got: %v", err)
	}
}

func TestStreamWriter_RecoverStream_ReadDirError(t *testing.T) {
	ctx := t.Context()
	dataDir := t.TempDir()
	streamID := uuid.New()
	streamPath := filepath.Join(dataDir, "streams", streamID.String())
	// Create parent dir first
	if err := os.MkdirAll(filepath.Join(dataDir, "streams"), 0755); err != nil {
		t.Fatalf("failed to create streams parent dir: %v", err)
	}
	// Create a file instead of a directory at streamPath so ReadDir fails with ENOTDIR
	if err := os.WriteFile(streamPath, []byte("not a dir"), 0644); err != nil {
		t.Fatalf("failed to create stream file: %v", err)
	}

	backend := inmemorystorage.New()
	store := NewStreamStore(dataDir, backend, time.Hour, DefaultStreamSealBytes)
	defer func() { _ = store.Close(ctx) }()
	w := store.getOrCreateStream(streamID)
	err := w.recoverStream(ctx, backend, nil)
	if err == nil {
		t.Fatalf("expected error from recoverStream when ReadDir fails, got nil")
	}
	if !strings.Contains(err.Error(), "failed to read stream directory") {
		t.Errorf("expected failed to read stream directory in error, got: %v", err)
	}
}

func TestStreamWriter_RecoverStream_ScanOpenSegmentFailure(t *testing.T) {
	ctx := t.Context()
	dataDir := t.TempDir()
	streamID := uuid.New()
	streamDir := filepath.Join(dataDir, "streams", streamID.String())
	if err := os.MkdirAll(streamDir, 0755); err != nil {
		t.Fatalf("failed to create stream dir: %v", err)
	}

	// Create a broken symlink for an open stream segment
	openName := wal.FormatStreamFileName(1, 0, false)
	brokenSymlink := filepath.Join(streamDir, openName)
	if err := os.Symlink(filepath.Join(dataDir, "nonexistent"), brokenSymlink); err != nil {
		t.Fatalf("failed to create symlink: %v", err)
	}

	backend := inmemorystorage.New()
	store := NewStreamStore(dataDir, backend, time.Hour, DefaultStreamSealBytes)
	defer func() { _ = store.Close(ctx) }()
	w := store.getOrCreateStream(streamID)

	err := w.recoverStream(ctx, backend, nil)
	if err == nil {
		t.Fatalf("expected error from recoverStream when open segment cannot be scanned, got nil")
	}
	if !strings.Contains(err.Error(), "failed to scan open segment file") {
		t.Errorf("expected 'failed to scan open segment file' in error, got: %v", err)
	}
}

func TestReadRecordsFromOffset_Nonexistent(t *testing.T) {
	nonexistent := filepath.Join(t.TempDir(), "nonexistent.wal")
	records, off, err := ReadRecordsFromOffset(nonexistent, 42)
	if err != nil {
		t.Fatalf("expected nil error for nonexistent file, got: %v", err)
	}
	if records != nil {
		t.Errorf("expected nil records, got: %v", records)
	}
	if off != 42 {
		t.Errorf("expected offset 42, got %d", off)
	}
}

type mockTailServer struct {
	grpc.ServerStream
	ctx  context.Context
	sent []*pb.TailResponse
}

func (m *mockTailServer) Send(resp *pb.TailResponse) error {
	m.sent = append(m.sent, resp)
	return nil
}

func (m *mockTailServer) Context() context.Context {
	return m.ctx
}

func TestTailPerStream_ReadSegmentFromBackendFailure(t *testing.T) {
	ctx := t.Context()
	dataDir := t.TempDir()
	streamID := uuid.New()

	backend := inmemorystorage.New()
	server, err := NewServer(ctx, ServerConfig{
		Backend: backend,
		DataDir: dataDir,
	})
	if err != nil {
		t.Fatalf("failed to create server: %v", err)
	}
	defer server.Close()

	// Put an invalid sealed segment in backend that will fail to decode
	badKey := wal.FormatStreamObjectKey(streamID, 1, 10)
	corruptData := []byte("corrupt-not-valid-log-record")
	stream := blob.NewByteStreamFromBytes(corruptData)
	defer stream.Close()
	if _, err := backend.PutObject(ctx, "", badKey, stream); err != nil {
		t.Fatalf("failed to put bad object: %v", err)
	}

	// Set up sealed meta on stream
	w := server.streamStore.getOrCreateStream(streamID)
	w.sealed = append(w.sealed, &StreamMeta{
		FromSeq: 1,
		ToSeq:   10,
		Key:     badKey,
	})

	mockStream := &mockTailServer{ctx: ctx}
	err = server.tailPerStream(ctx, streamID, &pb.TailRequest{
		FromStreamSeq: 0,
	}, mockStream)
	if err == nil {
		t.Fatalf("expected error from tailPerStream when ReadSegmentFromBackend fails, got nil")
	}
	if !strings.Contains(err.Error(), "failed to read sealed segment") {
		t.Errorf("expected failed to read sealed segment in error, got: %v", err)
	}
}
