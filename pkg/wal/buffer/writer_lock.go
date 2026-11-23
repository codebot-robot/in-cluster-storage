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
	"bytes"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/gke-labs/in-cluster-storage/pkg/errutil"
	"github.com/gke-labs/in-cluster-storage/pkg/tempfile"
	"github.com/google/uuid"
)

// writerLockStore manages persistence of stream writer lock holders across restarts and idle evictions.
type writerLockStore interface {
	// load reads the stream's writer lock holder from disk. If the file does not exist,
	// it returns an empty holder string and nil error. If the file is unreadable or corrupt,
	// it returns an error describing the failure.
	load(streamID uuid.UUID) (string, error)

	// save writes the clientID as a hex-encoded string followed by a newline to directory/<streamID>
	// using a temporary file, fsync, atomic rename, and directory fsync.
	save(streamID uuid.UUID, clientID string) error
}

// diskWriterLockStore persists writer lock holders as small files under <dataDir>/writer-locks/<streamID>.
type diskWriterLockStore struct {
	// directory is the directory where stream writer lock holder files are stored.
	directory string
}

// newDiskWriterLockStore creates a writerLockStore that persists writer lock holder files in directory.
func newDiskWriterLockStore(directory string) writerLockStore {
	return &diskWriterLockStore{directory: directory}
}

// Interface satisfaction checks.
var (
	_ writerLockStore = (*diskWriterLockStore)(nil)
	_                 = (*diskWriterLockStore).load
	_                 = (*diskWriterLockStore).save
)

// load reads the stream's writer lock holder from disk. If the file does not exist,
// it returns an empty holder string and nil error. If the file is unreadable or corrupt,
// it returns an error with the file path describing the failure.
func (s *diskWriterLockStore) load(streamID uuid.UUID) (string, error) {
	filePath := filepath.Join(s.directory, streamID.String())
	data, err := os.ReadFile(filePath)
	if err != nil {
		if os.IsNotExist(err) {
			return "", nil
		}
		return "", fmt.Errorf("reading writer lock holder file %s: %w", filePath, err)
	}

	if !bytes.HasSuffix(data, []byte("\n")) {
		return "", fmt.Errorf("writer lock holder file %s is corrupt: missing trailing newline", filePath)
	}
	line := data[:len(data)-1]
	if len(line) == 0 {
		return "", fmt.Errorf("writer lock holder file %s is corrupt: empty file", filePath)
	}
	decoded, err := hex.DecodeString(string(line))
	if err != nil {
		return "", fmt.Errorf("writer lock holder file %s is corrupt: %w", filePath, err)
	}
	if len(decoded) == 0 {
		return "", fmt.Errorf("writer lock holder file %s is corrupt: empty client ID", filePath)
	}
	return string(decoded), nil
}

// save writes the clientID as a hex-encoded string followed by a newline to directory/<streamID>
// using a temporary file, fsync, atomic rename, and directory fsync.
func (s *diskWriterLockStore) save(streamID uuid.UUID, clientID string) error {
	if err := os.MkdirAll(s.directory, 0755); err != nil {
		return err
	}

	tempFile, err := tempfile.Create(s.directory, ".tmp-holder-*")
	if err != nil {
		return err
	}
	defer errutil.CloseAndLog(tempFile, "temporary holder file")

	content := hex.EncodeToString([]byte(clientID)) + "\n"
	if _, err := tempFile.Write([]byte(content)); err != nil {
		return err
	}

	if err := tempFile.Sync(); err != nil {
		return err
	}

	targetPath := filepath.Join(s.directory, streamID.String())
	if err := tempFile.RenameTo(targetPath); err != nil {
		return err
	}

	if err := tempFile.Close(); err != nil {
		return err
	}

	directoryFile, err := os.Open(s.directory)
	if err != nil {
		return err
	}
	defer errutil.CloseAndLog(directoryFile, "writer locks directory")

	if err := directoryFile.Sync(); err != nil {
		return err
	}

	return nil
}

// holderFile tracks what one stream's writer lock holder file is known to contain, so the
// file is read once after the stream state is created and written only when the holder changes.
type holderFile struct {
	// mu serialises the attaching sessions of one stream and guards the fields below.
	// It is held across the file I/O. Lock order: holderFile.mu, then Server.mu, then streamState.mu.
	mu sync.Mutex
	// loaded reports whether the file has been read since the stream state was created.
	loaded bool
	// contents is the client ID the file names; empty when there is no file.
	contents string
	// saveFailed reports that the last save failed, so the file may name either the client in
	// contents or the client that save was for. The next save always writes.
	saveFailed bool
}

// loadLocked reads the stream's writer lock holder file once from store.
// If the file was already loaded, it returns the known contents and firstLoad=false without reading disk.
// Requires f.mu to be held.
func (f *holderFile) loadLocked(store writerLockStore, streamID uuid.UUID) (holder string, firstLoad bool, err error) {
	if f.loaded {
		return f.contents, false, nil
	}
	h, err := store.load(streamID)
	if err != nil {
		return "", false, err
	}
	f.loaded = true
	f.contents = h
	return h, true, nil
}

// needsSaveLocked reports whether the holder file needs to be saved for clientID.
// It returns true if the last save failed or if the file contents differ from clientID.
// Requires f.mu to be held.
func (f *holderFile) needsSaveLocked(clientID string) bool {
	return f.saveFailed || f.contents != clientID
}

// saveLocked writes clientID as the stream's writer lock holder to store.
// It sets saveFailed on error; on success clears it and sets contents.
// Requires f.mu to be held.
func (f *holderFile) saveLocked(store writerLockStore, streamID uuid.UUID, clientID string) error {
	if err := store.save(streamID, clientID); err != nil {
		f.saveFailed = true
		return err
	}
	f.saveFailed = false
	f.contents = clientID
	return nil
}

// contentsKnown reports whether the holder file contents in memory are known to match disk.
// It never blocks (it is called with Server.mu held): TryLock returns false if it cannot lock,
// otherwise returns !saveFailed.
func (f *holderFile) contentsKnown() bool {
	if !f.mu.TryLock() {
		return false
	}
	defer f.mu.Unlock()
	return !f.saveFailed
}
