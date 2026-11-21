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

	"github.com/gke-labs/in-cluster-storage/pkg/objectstore/inmemorystorage"
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
