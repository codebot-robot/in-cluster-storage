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
	"sync"
	"testing"
	"time"

	pb "github.com/gke-labs/in-cluster-storage/pkg/api/wal/v1alpha1"
	"github.com/gke-labs/in-cluster-storage/pkg/objectstore"
	"github.com/gke-labs/in-cluster-storage/pkg/objectstore/inmemorystorage"
	"github.com/google/uuid"
)

type faultyListBackend struct {
	objectstore.Backend

	mu             sync.Mutex
	listError      error
	listAttempts   int
	attemptChannel chan int
}

func newFaultyListBackend(initialError error) *faultyListBackend {
	return &faultyListBackend{
		Backend:        inmemorystorage.New(),
		listError:      initialError,
		attemptChannel: make(chan int, 32),
	}
}

func (b *faultyListBackend) setListError(err error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.listError = err
}

func (b *faultyListBackend) attempts() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.listAttempts
}

func (b *faultyListBackend) ListObjects(ctx context.Context, volumeID, prefix string) ([]objectstore.ObjectEntry, error) {
	b.mu.Lock()
	b.listAttempts++
	attempts := b.listAttempts
	err := b.listError
	b.mu.Unlock()

	select {
	case b.attemptChannel <- attempts:
	default:
	}
	if err != nil {
		return nil, err
	}
	return b.Backend.ListObjects(ctx, volumeID, prefix)
}

type retryStartResult struct {
	server *Server
	err    error
}

func TestNewServerWithRetry_UnavailableThenRecovers(t *testing.T) {
	ctx := t.Context()
	backend := newFaultyListBackend(fmt.Errorf("simulated 503: %w", objectstore.ErrUnavailable))
	config := ServerConfig{
		Backend:       backend,
		DataDir:       t.TempDir(),
		FlushInterval: 10 * time.Second,
		BatchMaxDelay: 2 * time.Millisecond,
	}

	resultChannel := make(chan retryStartResult, 1)
	go func() {
		server, err := newServerWithRetry(ctx, config, 5*time.Millisecond, 20*time.Millisecond)
		resultChannel <- retryStartResult{server: server, err: err}
	}()

	for attempts := 0; attempts < 2; {
		select {
		case attempts = <-backend.attemptChannel:
		case <-time.After(5 * time.Second):
			t.Fatalf("timed out waiting for at least 2 list attempts (got %d)", attempts)
		}
	}

	select {
	case result := <-resultChannel:
		if result.server != nil {
			if closeError := result.server.Close(); closeError != nil {
				t.Errorf("failed to close unexpected server: %v", closeError)
			}
		}
		t.Fatalf("newServerWithRetry returned before backend recovered: err=%v", result.err)
	default:
	}

	backend.setListError(nil)

	var result retryStartResult
	select {
	case result = <-resultChannel:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for newServerWithRetry to succeed after backend recovered")
	}
	if result.err != nil {
		t.Fatalf("newServerWithRetry failed after backend recovered: %v", result.err)
	}
	defer func() {
		if closeError := result.server.Close(); closeError != nil {
			t.Errorf("failed to close server: %v", closeError)
		}
	}()

	appendDirectRecord(t, result.server, uuid.New(), 1, []byte("hello"))
	flushResponse, err := result.server.Flush(ctx, &pb.FlushRequest{})
	if err != nil {
		t.Fatalf("Flush failed on recovered server: %v", err)
	}
	if flushResponse.GetLastPosition() != 1 {
		t.Fatalf("Flush LastPosition = %d, want 1", flushResponse.GetLastPosition())
	}
}

func TestNewServerWithRetry_NonUnavailableFailsImmediately(t *testing.T) {
	permanentError := errors.New("access denied")
	backend := newFaultyListBackend(permanentError)
	server, err := newServerWithRetry(t.Context(), ServerConfig{
		Backend: backend,
		DataDir: t.TempDir(),
	}, 5*time.Millisecond, 20*time.Millisecond)
	if server != nil {
		if closeError := server.Close(); closeError != nil {
			t.Errorf("failed to close unexpected server: %v", closeError)
		}
		t.Fatalf("expected nil server on permanent error")
	}
	if !errors.Is(err, permanentError) {
		t.Fatalf("got error %v, want %v", err, permanentError)
	}
	if got := backend.attempts(); got != 1 {
		t.Fatalf("list attempts = %d, want 1", got)
	}
}

func TestNewServerWithRetry_ContextCanceledStopsRetrying(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	backend := newFaultyListBackend(fmt.Errorf("simulated 503: %w", objectstore.ErrUnavailable))
	resultChannel := make(chan retryStartResult, 1)
	go func() {
		server, err := newServerWithRetry(ctx, ServerConfig{
			Backend: backend,
			DataDir: t.TempDir(),
		}, 5*time.Second, 15*time.Second)
		resultChannel <- retryStartResult{server: server, err: err}
	}()

	select {
	case <-backend.attemptChannel:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for first list attempt")
	}
	cancel()

	select {
	case result := <-resultChannel:
		if result.server != nil {
			if closeError := result.server.Close(); closeError != nil {
				t.Errorf("failed to close unexpected server: %v", closeError)
			}
		}
		if !errors.Is(result.err, context.Canceled) {
			t.Fatalf("got error %v, want context.Canceled", result.err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for newServerWithRetry to exit after context cancellation")
	}
}
