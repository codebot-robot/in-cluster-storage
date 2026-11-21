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
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/gke-labs/in-cluster-storage/pkg/objectstore/inmemorystorage"
	"github.com/gke-labs/in-cluster-storage/pkg/scoped"
	"github.com/go-logr/logr"
)

func TestDataDirectoryLock(t *testing.T) {
	dataDirectory := t.TempDir()
	backend := inmemorystorage.New()

	serverConfig := ServerConfig{
		Backend: backend,
		DataDir: dataDirectory,
	}

	firstServer, err := NewServer(t.Context(), serverConfig)
	if err != nil {
		t.Fatalf("first NewServer failed: %v", err)
	}

	// While the first server is open, a second NewServer on the same data directory must fail.
	secondServer, err := NewServer(t.Context(), serverConfig)
	if err == nil {
		if closeError := secondServer.Close(); closeError != nil {
			t.Errorf("secondServer.Close failed: %v", closeError)
		}
		t.Fatalf("second NewServer on same data directory unexpectedly succeeded")
	}

	expectedMessage := fmt.Sprintf("%s: %s", dataDirectory, ErrDataDirectoryInUse)
	if !strings.Contains(err.Error(), expectedMessage) {
		t.Fatalf("expected error containing %q, got: %v", expectedMessage, err)
	}
	if !errors.Is(err, ErrDataDirectoryInUse) {
		t.Fatalf("expected errors.Is(err, ErrDataDirectoryInUse) to be true, got: %v", err)
	}

	// Close the first server to release the lock.
	if err := firstServer.Close(); err != nil {
		t.Fatalf("firstServer.Close failed: %v", err)
	}

	// Lock file must not be deleted.
	lockPath := filepath.Join(dataDirectory, lockFileName)
	if _, err := os.Stat(lockPath); err != nil {
		t.Fatalf("expected lock file %s to be preserved on disk, got: %v", lockPath, err)
	}

	// After the first server is closed, a new NewServer on the same directory must succeed.
	thirdServer, err := NewServer(t.Context(), serverConfig)
	if err != nil {
		t.Fatalf("NewServer on same data directory after Close failed: %v", err)
	}
	if err := thirdServer.Close(); err != nil {
		t.Fatalf("thirdServer.Close failed: %v", err)
	}
}

func TestDataDirectoryLock_WaitsUntilReleased(t *testing.T) {
	dataDirectory := t.TempDir()
	backend := inmemorystorage.New()

	firstServer, err := NewServer(t.Context(), ServerConfig{Backend: backend, DataDir: dataDirectory})
	if err != nil {
		t.Fatalf("first NewServer failed: %v", err)
	}

	sink := newLoggingTestSink()
	ctx := scoped.WithLog(t.Context(), logr.New(sink))

	serverChan := make(chan *Server, 1)
	errChan := make(chan error, 1)
	go func() {
		server, err := NewServer(ctx, ServerConfig{
			Backend:            backend,
			DataDir:            dataDirectory,
			DataDirLockTimeout: 5 * time.Second,
		})
		if err != nil {
			errChan <- err
			return
		}
		serverChan <- server
	}()

	// Verify second NewServer is still waiting while the first server is open.
	select {
	case server := <-serverChan:
		_ = server.Close()
		t.Fatalf("second NewServer succeeded while first was still open")
	case err := <-errChan:
		t.Fatalf("second NewServer failed while first was still open: %v", err)
	case <-time.After(150 * time.Millisecond):
	}

	// Verify it logged once when waiting started.
	sink.state.mu.Lock()
	foundLog := false
	for _, entry := range sink.state.entries {
		if entry["message"] == "Data directory is held by another process; waiting for lock" {
			foundLog = true
			if entry["directory"] != dataDirectory || entry["wait"] != 5*time.Second {
				t.Errorf("logged entry = %v, want directory %s and wait 5s", entry, dataDirectory)
			}
			break
		}
	}
	sink.state.mu.Unlock()
	if !foundLog {
		t.Fatalf("expected log entry for waiting on data directory lock, got: %v", sink.state.entries)
	}

	// Close the first server; the second NewServer must acquire the lock and succeed.
	if err := firstServer.Close(); err != nil {
		t.Fatalf("firstServer.Close failed: %v", err)
	}

	select {
	case err := <-errChan:
		t.Fatalf("second NewServer failed after first server closed: %v", err)
	case secondServer := <-serverChan:
		if err := secondServer.Close(); err != nil {
			t.Fatalf("secondServer.Close failed: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatalf("timed out waiting for second NewServer to acquire lock")
	}
}

func TestDataDirectoryLock_WaitTimeout(t *testing.T) {
	dataDirectory := t.TempDir()
	backend := inmemorystorage.New()

	firstServer, err := NewServer(t.Context(), ServerConfig{Backend: backend, DataDir: dataDirectory})
	if err != nil {
		t.Fatalf("first NewServer failed: %v", err)
	}
	defer func() {
		if err := firstServer.Close(); err != nil {
			t.Errorf("firstServer.Close failed: %v", err)
		}
	}()

	waitTimeout := 100 * time.Millisecond
	startTime := time.Now()
	secondServer, err := NewServer(t.Context(), ServerConfig{
		Backend:            backend,
		DataDir:            dataDirectory,
		DataDirLockTimeout: waitTimeout,
	})
	elapsed := time.Since(startTime)
	if err == nil {
		_ = secondServer.Close()
		t.Fatalf("second NewServer unexpectedly succeeded while first was open")
	}

	if !errors.Is(err, ErrDataDirectoryInUse) {
		t.Fatalf("expected errors.Is(err, ErrDataDirectoryInUse) to be true, got: %v", err)
	}
	expectedMessage := fmt.Sprintf("%s: %s", dataDirectory, ErrDataDirectoryInUse)
	if !strings.Contains(err.Error(), expectedMessage) {
		t.Fatalf("expected error containing %q, got: %v", expectedMessage, err)
	}
	if elapsed < waitTimeout {
		t.Fatalf("expected NewServer to wait at least %v, waited %v", waitTimeout, elapsed)
	}
}
