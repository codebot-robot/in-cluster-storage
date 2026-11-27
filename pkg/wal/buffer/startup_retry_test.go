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
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	pb "github.com/gke-labs/in-cluster-storage/pkg/api/wal/v1alpha1"
	"github.com/gke-labs/in-cluster-storage/pkg/objectstore"
	"github.com/gke-labs/in-cluster-storage/pkg/objectstore/inmemorystorage"
	"github.com/google/uuid"
)

// faultyListBackend wraps an in-memory backend and fails ListObjects with listError until recovered is true.
type faultyListBackend struct {
	objectstore.Backend
	recovered      atomic.Bool
	listError      error
	listAttempts   atomic.Int32
	attemptChannel chan struct{}
}

func newFaultyListBackend(listError error) *faultyListBackend {
	return &faultyListBackend{
		Backend:        inmemorystorage.New(),
		listError:      listError,
		attemptChannel: make(chan struct{}, 1),
	}
}

func (b *faultyListBackend) ListObjects(ctx context.Context, volumeID, prefix string) ([]objectstore.ObjectEntry, error) {
	b.listAttempts.Add(1)
	select {
	case b.attemptChannel <- struct{}{}:
	default:
	}
	if !b.recovered.Load() {
		return nil, b.listError
	}
	return b.Backend.ListObjects(ctx, volumeID, prefix)
}

// retryStartResult carries the return values of newServerWithRetry from a test goroutine.
type retryStartResult struct {
	server *Server
	err    error
}

func closeServer(t *testing.T, server *Server) {
	t.Helper()
	if server != nil {
		if err := server.Close(); err != nil {
			t.Errorf("failed to close server: %v", err)
		}
	}
}

func TestNewServerWithRetry_UnavailableThenRecovers(t *testing.T) {
	ctx := t.Context()
	runtime.GC()
	baselineGoroutines := runtime.NumGoroutine()

	backend := newFaultyListBackend(fmt.Errorf("simulated 503: %w", objectstore.ErrUnavailable))
	resultChannel := make(chan retryStartResult, 1)
	go func() {
		server, err := newServerWithRetry(ctx, ServerConfig{Backend: backend, DataDir: t.TempDir()}, 5*time.Millisecond, 20*time.Millisecond)
		resultChannel <- retryStartResult{server: server, err: err}
	}()

	for backend.listAttempts.Load() < 2 {
		select {
		case <-backend.attemptChannel:
		case <-time.After(5 * time.Second):
			t.Fatalf("timed out waiting for 2 list attempts (got %d)", backend.listAttempts.Load())
		}
	}
	select {
	case result := <-resultChannel:
		closeServer(t, result.server)
		t.Fatalf("newServerWithRetry returned before backend recovered: err=%v", result.err)
	default:
	}

	backend.recovered.Store(true)
	var result retryStartResult
	select {
	case result = <-resultChannel:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for newServerWithRetry to succeed after backend recovered")
	}
	if result.err != nil {
		t.Fatalf("newServerWithRetry failed after backend recovered: %v", result.err)
	}
	defer closeServer(t, result.server)

	appendDirectRecord(t, result.server, uuid.New(), 1, []byte("hello"))
	flushResponse, err := result.server.Flush(ctx, &pb.FlushRequest{})
	if err != nil || flushResponse.GetLastPosition() != 1 {
		t.Fatalf("Flush() = (%v, %v), want last position 1", flushResponse, err)
	}

	closeServer(t, result.server)
	for deadline := time.Now().Add(2 * time.Second); runtime.NumGoroutine() > baselineGoroutines && time.Now().Before(deadline); {
		time.Sleep(10 * time.Millisecond)
	}
	if got := runtime.NumGoroutine(); got > baselineGoroutines {
		t.Fatalf("goroutine count after recovery and close = %d, want <= %d", got, baselineGoroutines)
	}
}

func TestNewServerWithRetry_NonUnavailableFailsImmediately(t *testing.T) {
	permanentError := errors.New("access denied")
	backend := newFaultyListBackend(permanentError)
	server, err := newServerWithRetry(t.Context(), ServerConfig{Backend: backend, DataDir: t.TempDir()}, 5*time.Millisecond, 20*time.Millisecond)
	if server != nil {
		closeServer(t, server)
		t.Fatalf("expected nil server on permanent error")
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

	backend := newFaultyListBackend(fmt.Errorf("simulated 503: %w", objectstore.ErrUnavailable))
	errorChannel := make(chan error, 1)
	go func() {
		server, err := newServerWithRetry(ctx, ServerConfig{Backend: backend, DataDir: t.TempDir()}, time.Minute, time.Minute)
		closeServer(t, server)
		errorChannel <- err
	}()

	select {
	case <-backend.attemptChannel:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for first list attempt")
	}
	cancel()

	select {
	case err := <-errorChannel:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("got error %v, want context.Canceled", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for newServerWithRetry to exit after context cancellation")
	}
}
