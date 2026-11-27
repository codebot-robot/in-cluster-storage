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

package controller

import (
	"bytes"
	"context"
	"errors"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gke-labs/in-cluster-storage/pkg/objectfs/blob"
)

// switchableUploadBackend wraps an ObjectStorageBackend to inject pack upload
// failures or fixed delays while counting pack upload attempts.
type switchableUploadBackend struct {
	ObjectStorageBackend
	failUploads   atomic.Bool
	uploadDelay   time.Duration
	attempts      atomic.Int64
	attemptNotify chan struct{}
}

func newSwitchableUploadBackend(t *testing.T) *switchableUploadBackend {
	t.Helper()
	backend := &switchableUploadBackend{
		ObjectStorageBackend: NewMemoryBackend(),
		attemptNotify:        make(chan struct{}, 1),
	}
	t.Cleanup(func() { backend.failUploads.Store(false) })
	return backend
}

func (b *switchableUploadBackend) PutObject(ctx context.Context, volumeID, key string, stream blob.ByteStream) (string, error) {
	if strings.HasSuffix(key, ".pack") || strings.Contains(key, "blobs/") {
		b.attempts.Add(1)
		select {
		case b.attemptNotify <- struct{}{}:
		default:
		}
		if b.uploadDelay > 0 {
			timer := time.NewTimer(b.uploadDelay)
			select {
			case <-timer.C:
			case <-ctx.Done():
				timer.Stop()
				return "", ctx.Err()
			}
		}
		if b.failUploads.Load() {
			return "", errors.New("injected upload failure")
		}
	}
	return b.ObjectStorageBackend.PutObject(ctx, volumeID, key, stream)
}

func (b *switchableUploadBackend) waitForAttempts(t *testing.T, target int64) {
	t.Helper()
	deadline := time.After(5 * time.Second)
	for b.attempts.Load() < target {
		select {
		case <-b.attemptNotify:
		case <-deadline:
			t.Fatalf("timed out waiting for %d upload attempts, got %d", target, b.attempts.Load())
		}
	}
}

func assertStagedDataIntact(t *testing.T, volume *Volume) {
	t.Helper()
	volume.mu.Lock()
	closed := volume.closed
	volume.mu.Unlock()
	volume.stagingMu.Lock()
	stagedChunks, stagingDir := len(volume.stagedChunks), volume.stagingDir
	volume.stagingMu.Unlock()
	if closed || stagedChunks == 0 || volume.StagedBytes() == 0 {
		t.Fatalf("expected open volume with staged data, got closed=%v chunks=%d bytes=%d", closed, stagedChunks, volume.StagedBytes())
	}
	if _, err := os.Stat(stagingDir); err != nil {
		t.Fatalf("expected staging directory %s intact: %v", stagingDir, err)
	}
}

func assertStagedPayloadRecovered(t *testing.T, backend ObjectStorageBackend) {
	t.Helper()
	recovered := NewTestServer(t, backend)
	readResponse, err := testReadFile(t.Context(), recovered, "test-vol", "/test.bin", 0, 4)
	if err != nil || readResponse.GetError() != 0 || !bytes.Equal(readResponse.GetData(), []byte("data")) {
		t.Fatalf("recovered read failed: err=%v code=%d data=%q", err, readResponse.GetError(), string(readResponse.GetData()))
	}
}

func TestServer_Shutdown_RetriesUntilUploadSucceeds(t *testing.T) {
	backend := newSwitchableUploadBackend(t)
	backend.failUploads.Store(true)
	server, volume, _ := setupStagedVolume(t, backend)

	shutdownDone := make(chan error, 1)
	go func() {
		shutdownDone <- server.Shutdown(t.Context(), 100*time.Millisecond, 10*time.Millisecond)
	}()

	// Wait for more than three failed attempts to verify nothing ends the loop early.
	backend.waitForAttempts(t, 5)
	select {
	case err := <-shutdownDone:
		t.Fatalf("Shutdown returned early while uploads were failing: %v", err)
	default:
	}
	assertStagedDataIntact(t, volume)

	backend.failUploads.Store(false)
	if err := waitSyncDone(t, shutdownDone); err != nil {
		t.Fatalf("expected Shutdown to return nil after uploads recovered, got: %v", err)
	}
	assertStagedPayloadRecovered(t, backend)
}

func TestServer_Shutdown_SlowUploadFinishesAcrossAttemptTimeouts(t *testing.T) {
	backend := newSwitchableUploadBackend(t)
	backend.uploadDelay = 120 * time.Millisecond
	server, _, _ := setupStagedVolume(t, backend)

	if err := server.Shutdown(t.Context(), 60*time.Millisecond, 10*time.Millisecond); err != nil {
		t.Fatalf("expected Shutdown to succeed for slow upload, got: %v", err)
	}
	if attempts := backend.attempts.Load(); attempts != 1 {
		t.Fatalf("pack upload attempts = %d, want 1", attempts)
	}
	assertStagedPayloadRecovered(t, backend)
}

func TestServer_Shutdown_CancelledParentLeavesStagedDataIntact(t *testing.T) {
	backend := newSwitchableUploadBackend(t)
	backend.failUploads.Store(true)
	server, volume, _ := setupStagedVolume(t, backend)

	parentCtx, parentCancel := context.WithCancel(t.Context())
	defer parentCancel()
	shutdownDone := make(chan error, 1)
	go func() {
		shutdownDone <- server.Shutdown(parentCtx, 100*time.Millisecond, 10*time.Millisecond)
	}()

	backend.waitForAttempts(t, 2)
	parentCancel()
	if err := waitSyncDone(t, shutdownDone); !errors.Is(err, context.Canceled) {
		t.Fatalf("expected Shutdown error wrapping context.Canceled, got: %v", err)
	}
	assertStagedDataIntact(t, volume)
}

func TestVolume_Stop_RetriesWithNewFinalSyncAfterFailure(t *testing.T) {
	ctx := t.Context()
	backend := newSwitchableUploadBackend(t)
	backend.failUploads.Store(true)
	server, volume, _ := setupStagedVolume(t, backend)

	if err := volume.Stop(ctx); err == nil {
		t.Fatal("expected first Volume.Stop to fail while uploads fail")
	}
	if attempts := backend.attempts.Load(); attempts != 1 {
		t.Fatalf("pack upload attempts after first Stop = %d, want 1", attempts)
	}
	assertStagedDataIntact(t, volume)

	backend.failUploads.Store(false)
	if err := volume.Stop(ctx); err != nil {
		t.Fatalf("expected second Volume.Stop to succeed, got: %v", err)
	}
	if attempts := backend.attempts.Load(); attempts != 2 {
		t.Fatalf("pack upload attempts after second Stop = %d, want 2", attempts)
	}
	if staged := volume.StagedBytes(); staged != 0 {
		t.Fatalf("expected 0 staged bytes after second Stop, got %d", staged)
	}
	if err := server.Close(); err != nil {
		t.Fatalf("server.Close failed: %v", err)
	}
	assertStagedPayloadRecovered(t, backend)
}
