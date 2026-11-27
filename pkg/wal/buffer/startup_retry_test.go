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
	"errors"
	"fmt"
	"io"
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	pb "github.com/gke-labs/in-cluster-storage/pkg/api/wal/v1alpha1"
	"github.com/gke-labs/in-cluster-storage/pkg/errutil"
	"github.com/gke-labs/in-cluster-storage/pkg/objectfs/blob"
	"github.com/gke-labs/in-cluster-storage/pkg/objectstore"
	"github.com/gke-labs/in-cluster-storage/pkg/objectstore/inmemorystorage"
	"github.com/gke-labs/in-cluster-storage/pkg/wal"
	"github.com/google/uuid"
)

// faultyListBackend wraps an in-memory backend to inject ListObjects and GetObject
// failures with listError during startup retry tests. When failListCalls is zero,
// every ListObjects call fails while listError is non-nil.
type faultyListBackend struct {
	objectstore.Backend
	listError     error
	failListCalls int32
	failGetCalls  int32
	listAttempts  atomic.Int32
	getAttempts   atomic.Int32
	onList        func()
}

func (b *faultyListBackend) ListObjects(ctx context.Context, volumeID, prefix string) ([]objectstore.ObjectEntry, error) {
	attempts := b.listAttempts.Add(1)
	if b.onList != nil {
		b.onList()
	}
	if b.listError != nil && (b.failListCalls == 0 || attempts <= b.failListCalls) {
		return nil, b.listError
	}
	return b.Backend.ListObjects(ctx, volumeID, prefix)
}

func (b *faultyListBackend) GetObject(ctx context.Context, volumeID, key string, offset, length int64, writer io.Writer) error {
	if b.listError != nil && b.getAttempts.Add(1) <= b.failGetCalls {
		return b.listError
	}
	return b.Backend.GetObject(ctx, volumeID, key, offset, length, writer)
}

func TestNewServerWithRetry_UnavailableThenRecovers(t *testing.T) {
	ctx := t.Context()
	backend := &faultyListBackend{
		Backend:       inmemorystorage.New(),
		listError:     fmt.Errorf("simulated 503: %w", objectstore.ErrUnavailable),
		failListCalls: 2,
		failGetCalls:  1,
	}
	segmentStream := blob.NewByteStreamFromBytes(wal.DefaultSegmentHeader().Encode())
	defer errutil.CloseAndLog(segmentStream, "seed segment")
	if _, err := backend.PutObject(ctx, "", SegmentsPrefix+"000000000001-000000000001.wal", segmentStream); err != nil {
		t.Fatalf("PutObject failed: %v", err)
	}

	baselineGoroutines := runtime.NumGoroutine()
	server, err := newServerWithRetry(ctx, ServerConfig{
		Backend:       backend,
		DataDir:       t.TempDir(),
		FlushInterval: 10 * time.Second,
		BatchMaxDelay: 2 * time.Millisecond,
	}, 5*time.Millisecond, 20*time.Millisecond)
	if err != nil {
		t.Fatalf("newServerWithRetry failed after backend recovered: %v", err)
	}
	if listCount, getCount := backend.listAttempts.Load(), backend.getAttempts.Load(); listCount != 4 || getCount != 2 {
		t.Errorf("startup attempts = (list=%d, get=%d), want (4, 2)", listCount, getCount)
	}

	appendDirectRecord(t, server, uuid.New(), 1, []byte("hello"))
	flushResponse, flushError := server.Flush(ctx, &pb.FlushRequest{})
	if closeError := server.Close(); closeError != nil {
		t.Fatalf("failed to close server: %v", closeError)
	}
	if flushError != nil || flushResponse.GetLastPosition() != 2 {
		t.Fatalf("Flush() = (%v, %v), want last position 2", flushResponse, flushError)
	}

	for deadline := time.Now().Add(time.Second); runtime.NumGoroutine() > baselineGoroutines && time.Now().Before(deadline); {
		time.Sleep(5 * time.Millisecond)
	}
	if got := runtime.NumGoroutine(); got > baselineGoroutines {
		t.Fatalf("goroutine count = %d after retries and close, want <= %d", got, baselineGoroutines)
	}
}

func TestNewServerWithRetry_NonUnavailableFailsImmediately(t *testing.T) {
	permanentError := errors.New("access denied")
	backend := &faultyListBackend{Backend: inmemorystorage.New(), listError: permanentError}
	server, err := newServerWithRetry(t.Context(), ServerConfig{Backend: backend, DataDir: t.TempDir()}, 5*time.Millisecond, 20*time.Millisecond)
	if server != nil {
		if closeError := server.Close(); closeError != nil {
			t.Errorf("failed to close unexpected server: %v", closeError)
		}
		t.Fatal("expected nil server on permanent error")
	}
	if !errors.Is(err, permanentError) {
		t.Fatalf("got error %v, want %v", err, permanentError)
	}
	if got := backend.listAttempts.Load(); got != 1 {
		t.Fatalf("list attempts = %d, want 1", got)
	}
}

func TestNewServerWithRetry_ContextCanceledStopsRetrying(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	backend := &faultyListBackend{
		Backend:   inmemorystorage.New(),
		listError: fmt.Errorf("simulated 503: %w", objectstore.ErrUnavailable),
		onList:    cancel,
	}
	errorChannel := make(chan error, 1)
	go func() {
		server, err := newServerWithRetry(ctx, ServerConfig{Backend: backend, DataDir: t.TempDir()}, time.Minute, time.Minute)
		if server != nil {
			err = errors.Join(err, errors.New("unexpected server created"), server.Close())
		}
		errorChannel <- err
	}()

	select {
	case err := <-errorChannel:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("got error %v, want context.Canceled", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for newServerWithRetry to exit after context cancellation")
	}
}
