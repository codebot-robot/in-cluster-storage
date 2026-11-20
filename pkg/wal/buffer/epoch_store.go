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
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/gke-labs/in-cluster-storage/pkg/errutil"
	"github.com/gke-labs/in-cluster-storage/pkg/tempfile"
	"github.com/google/uuid"
)

// streamEpochStore manages durable storage of the highest accepted writer epoch per stream.
type streamEpochStore struct {
	// directory is the stream directory containing the epoch file.
	directory string

	// filePath is the absolute path to the epoch file.
	filePath string

	// streamID identifies the stream owning this epoch store.
	streamID uuid.UUID

	// noFsync controls whether fsync calls to the file and parent directory are skipped.
	noFsync bool
}

// newStreamEpochStore creates a new streamEpochStore for streamID under dataDir.
func newStreamEpochStore(dataDir string, streamID uuid.UUID, noFsync bool) *streamEpochStore {
	directory := filepath.Join(dataDir, "streams", streamID.String())
	return &streamEpochStore{
		directory: directory,
		filePath:  filepath.Join(directory, "epoch"),
		streamID:  streamID,
		noFsync:   noFsync,
	}
}

// Load reads the highest accepted epoch from disk. A missing file returns 0 and nil.
// An empty, unterminated, or unparseable file returns an error naming the stream and file.
func (s *streamEpochStore) Load() (uint64, error) {
	data, err := os.ReadFile(s.filePath)
	if os.IsNotExist(err) {
		return 0, nil
	}
	if err != nil {
		return 0, fmt.Errorf("failed to read epoch file %s for stream %s: %w", s.filePath, s.streamID, err)
	}
	if len(data) == 0 || data[len(data)-1] != '\n' {
		return 0, fmt.Errorf("corrupt epoch file %s for stream %s: empty or missing trailing newline", s.filePath, s.streamID)
	}
	epoch, err := strconv.ParseUint(strings.TrimSuffix(string(data), "\n"), 10, 64)
	if err != nil {
		return 0, fmt.Errorf("corrupt epoch file %s for stream %s: %w", s.filePath, s.streamID, err)
	}
	return epoch, nil
}

// syncDirectory flushes directory metadata changes to durable storage.
func (s *streamEpochStore) syncDirectory() error {
	if s.noFsync {
		return nil
	}
	directoryFile, openErr := os.Open(s.directory)
	if openErr != nil {
		return fmt.Errorf("open stream dir %s: %w", s.directory, openErr)
	}
	defer errutil.CloseAndLog(directoryFile, s.directory)
	if syncErr := directoryFile.Sync(); syncErr != nil {
		return fmt.Errorf("sync stream dir %s: %w", s.directory, syncErr)
	}
	return nil
}

// Save durably writes epoch to disk before returning. Lower epoch is an error; 0 is a no-op;
// equal epoch syncs the directory before returning.
// Failure semantics: before rename nothing has changed on disk; if directory fsync fails after rename,
// the file may hold the new epoch, the caller must refuse the session, and restart may read either value.
func (s *streamEpochStore) Save(epoch uint64) (err error) {
	if epoch == 0 {
		return nil
	}
	stored, err := s.Load()
	if err != nil {
		return err
	}
	if epoch < stored {
		return fmt.Errorf("refusing to save epoch %d lower than stored %d for stream %s (file: %s)", epoch, stored, s.streamID, s.filePath)
	}
	if epoch == stored {
		return s.syncDirectory()
	}
	if err := os.MkdirAll(s.directory, 0755); err != nil {
		return fmt.Errorf("create stream dir %s: %w", s.directory, err)
	}
	temporaryFile, err := tempfile.Create(s.directory, "epoch.*.tmp")
	if err != nil {
		return fmt.Errorf("create temp epoch file in %s: %w", s.directory, err)
	}
	defer errutil.CloseWithErr(temporaryFile, &err)

	if _, err := fmt.Fprintf(temporaryFile, "%d\n", epoch); err != nil {
		return fmt.Errorf("write epoch to %s: %w", temporaryFile.Name(), err)
	}
	if !s.noFsync {
		if err := temporaryFile.Sync(); err != nil {
			return fmt.Errorf("sync temp epoch file %s: %w", temporaryFile.Name(), err)
		}
	}
	if err := temporaryFile.RenameTo(s.filePath); err != nil {
		return fmt.Errorf("rename temp epoch file to %s: %w", s.filePath, err)
	}
	return s.syncDirectory()
}
