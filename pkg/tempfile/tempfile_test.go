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
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestCloseWithoutRenameRemoves(t *testing.T) {
	tempFile, err := Create(t.TempDir(), "test-*")
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if err := tempFile.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if _, err := os.Stat(tempFile.Name()); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("expected os.ErrNotExist after Close, got: %v", err)
	}
}

func TestCloseAfterRenameKeepsRenamedFile(t *testing.T) {
	directory := t.TempDir()
	tempFile, err := Create(directory, "test-*")
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	content := []byte("persisted")
	if _, err := tempFile.Write(content); err != nil {
		t.Fatalf("Write: %v", err)
	}
	destination := filepath.Join(directory, "final.txt")
	if err := tempFile.RenameTo(destination); err != nil {
		t.Fatalf("RenameTo: %v", err)
	}
	if err := tempFile.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	data, err := os.ReadFile(destination)
	if err != nil || !bytes.Equal(data, content) {
		t.Fatalf("ReadFile got %q, err %v", data, err)
	}
	if _, err := os.Stat(tempFile.Name()); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("expected original path to not exist, got: %v", err)
	}
}

func TestDoubleClose(t *testing.T) {
	tempFile, err := Create(t.TempDir(), "test-*")
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if err := tempFile.Close(); err != nil {
		t.Fatalf("first Close: %v", err)
	}
	if err := tempFile.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
}

func TestRemoveFailureSurfaces(t *testing.T) {
	tempFile, err := Create(t.TempDir(), "test-*")
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if err := os.Remove(tempFile.Name()); err != nil {
		t.Fatalf("os.Remove: %v", err)
	}
	if err := tempFile.Close(); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("expected os.ErrNotExist on Close after remove, got: %v", err)
	}
}

func TestRenameToAfterCloseFails(t *testing.T) {
	directory := t.TempDir()
	tempFile, err := Create(directory, "test-*")
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if err := tempFile.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := tempFile.RenameTo(filepath.Join(directory, "final.txt")); err == nil {
		t.Fatal("expected error on RenameTo after Close, got nil")
	}
}

func TestSecondRenameToFails(t *testing.T) {
	directory := t.TempDir()
	tempFile, err := Create(directory, "test-*")
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	firstDestination := filepath.Join(directory, "first.txt")
	if err := tempFile.RenameTo(firstDestination); err != nil {
		t.Fatalf("first RenameTo: %v", err)
	}
	secondDestination := filepath.Join(directory, "second.txt")
	if err := tempFile.RenameTo(secondDestination); err == nil {
		t.Fatal("expected error on second RenameTo, got nil")
	}
	if err := tempFile.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

func TestCloseAndLog(t *testing.T) {
	ctx := t.Context()
	var nilTempFile *TempFile
	nilTempFile.CloseAndLog(ctx)

	tempFile, err := Create(t.TempDir(), "test-*")
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	path := tempFile.Name()
	tempFile.CloseAndLog(ctx)
	if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("expected os.ErrNotExist after CloseAndLog, got: %v", err)
	}

	// Verify CloseAndLog logs errors when remove fails without panicking.
	tempFile2, err := Create(t.TempDir(), "test-*")
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if err := os.Remove(tempFile2.Name()); err != nil {
		t.Fatalf("os.Remove: %v", err)
	}
	tempFile2.CloseAndLog(ctx)
}
