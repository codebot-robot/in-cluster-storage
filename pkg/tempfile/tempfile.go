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

package tempfile

import (
	"context"
	"errors"
	"os"
	"sync"

	"k8s.io/klog/v2"
)

// TempFile wraps an os.File and owns a temporary file's lifetime.
// If RenameTo was not called, Close removes the temporary file on disk.
type TempFile struct {
	*os.File
	path            string
	renamed, closed bool
	mu              sync.Mutex
}

// Create creates a new temporary file in directory using os.CreateTemp.
// The returned TempFile embeds *os.File and owns the temporary file's lifetime.
func Create(directory, pattern string) (*TempFile, error) {
	file, err := os.CreateTemp(directory, pattern)
	if err != nil {
		return nil, err
	}
	return &TempFile{File: file, path: file.Name()}, nil
}

// RenameTo renames the temporary file to path and transfers ownership to the caller.
// Durability of the rename is the caller's decision and is out of scope here;
// RenameTo does not perform an fsync.
func (t *TempFile) RenameTo(path string) error {
	if t == nil {
		return errors.New("tempfile: nil TempFile")
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.closed {
		return errors.New("tempfile: file is closed")
	}
	if t.renamed {
		return errors.New("tempfile: file already renamed")
	}
	if err := os.Rename(t.path, path); err != nil {
		return err
	}
	t.renamed = true
	return nil
}

// Close closes the underlying descriptor and, if RenameTo was not called,
// removes the temporary file from disk. A missing file on remove is an error.
// A second call to Close returns nil and does nothing.
func (t *TempFile) Close() error {
	if t == nil {
		return nil
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.closed {
		return nil
	}
	t.closed = true

	closeErr := t.File.Close()
	var removeErr error
	if !t.renamed {
		removeErr = os.Remove(t.path)
	}
	return errors.Join(closeErr, removeErr)
}

// CloseAndLog closes the temporary file and logs any close or remove error
// using klog.FromContext(ctx).
func (t *TempFile) CloseAndLog(ctx context.Context) {
	if t == nil {
		return
	}
	if err := t.Close(); err != nil {
		logger := klog.FromContext(ctx)
		logger.Error(err, "error closing temporary file", "path", t.path)
	}
}
