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
	"sync"

	"github.com/gke-labs/in-cluster-storage/pkg/errutil"
	"golang.org/x/sys/unix"
)

const lockFileName = "lock"

// ErrDataDirectoryInUse indicates that another stream server process holds the directory lock.
var ErrDataDirectoryInUse = errors.New("another stream server process is using it")

// directoryLock holds an exclusive advisory flock on the stream server data directory.
//
// The lock prevents two stream server processes from serving from the same data
// directory simultaneously. This advisory lock is only as good as the underlying
// filesystem's flock support; locking across machines over a network filesystem
// shared by two nodes is out of scope.
type directoryLock struct {
	mu   sync.Mutex
	file *os.File
}

// acquireDirectoryLock acquires an exclusive, non-blocking advisory lock on the data directory.
// The lock file itself is never deleted.
func acquireDirectoryLock(dataDirectory string) (*directoryLock, error) {
	if err := os.MkdirAll(dataDirectory, 0o755); err != nil {
		return nil, fmt.Errorf("failed to create data directory %s: %w", dataDirectory, err)
	}

	lockPath := filepath.Join(dataDirectory, lockFileName)
	file, err := os.OpenFile(lockPath, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil, fmt.Errorf("failed to open lock file %s: %w", lockPath, err)
	}

	if err := unix.Flock(int(file.Fd()), unix.LOCK_EX|unix.LOCK_NB); err != nil {
		errutil.CloseAndLog(file, "directory lock file")
		if errors.Is(err, unix.EWOULDBLOCK) || errors.Is(err, unix.EAGAIN) {
			return nil, fmt.Errorf("data directory %s: %w", dataDirectory, ErrDataDirectoryInUse)
		}
		return nil, fmt.Errorf("failed to lock data directory %s: %w", dataDirectory, err)
	}

	return &directoryLock{file: file}, nil
}

// Close releases the directory lock by unlocking and closing the underlying file.
// The lock file itself is never deleted.
func (l *directoryLock) Close() error {
	if l == nil {
		return nil
	}
	l.mu.Lock()
	file := l.file
	l.file = nil
	l.mu.Unlock()

	if file == nil {
		return nil
	}
	unlockError := unix.Flock(int(file.Fd()), unix.LOCK_UN)
	closeError := file.Close()
	return errors.Join(unlockError, closeError)
}
