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
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/google/uuid"
)

func TestStreamEpochStore(t *testing.T) {
	dataDir := t.TempDir()
	streamID := uuid.New()
	store := newStreamEpochStore(dataDir, streamID, false)

	if epoch, err := store.Load(); err != nil || epoch != 0 {
		t.Fatalf("Load on missing file got epoch=%d, err=%v", epoch, err)
	}
	if err := store.Save(42); err != nil {
		t.Fatalf("Save(42) failed: %v", err)
	}
	store2 := newStreamEpochStore(dataDir, streamID, false)
	if epoch, err := store2.Load(); err != nil || epoch != 42 {
		t.Fatalf("new store Load got epoch=%d, err=%v", epoch, err)
	}
	if err := store.Save(40); err == nil {
		t.Fatal("expected lower save to fail, got nil")
	}
	if err := store.Save(42); err != nil {
		t.Fatalf("equal save should be no-op, got: %v", err)
	}
	if err := store.Save(0); err != nil {
		t.Fatalf("epoch 0 save should be no-op, got: %v", err)
	}
}

func TestStreamEpochStore_CorruptFiles(t *testing.T) {
	for name, content := range map[string][]byte{"empty": nil, "unterminated": []byte("42"), "garbage": []byte("abc\n")} {
		t.Run(name, func(t *testing.T) {
			store := newStreamEpochStore(t.TempDir(), uuid.New(), false)
			if err := os.MkdirAll(store.dir, 0755); err != nil || os.WriteFile(store.filePath, content, 0644) != nil {
				t.Fatal("setup failed")
			}
			_, err := store.Load()
			if err == nil || !strings.Contains(err.Error(), store.filePath) || !strings.Contains(err.Error(), store.streamID.String()) {
				t.Fatalf("expected error naming file and stream, got: %v", err)
			}
		})
	}
}

func TestStreamEpochStore_FailedSaveLeavesOldValueReadable(t *testing.T) {
	dataDir := t.TempDir()
	store := newStreamEpochStore(dataDir, uuid.New(), false)
	if err := store.Save(2); err != nil {
		t.Fatalf("initial Save failed: %v", err)
	}
	blocker := filepath.Join(dataDir, "blocker")
	if err := os.WriteFile(blocker, []byte("x"), 0644); err != nil {
		t.Fatalf("WriteFile failed: %v", err)
	}
	failing := &streamEpochStore{dir: filepath.Join(blocker, "s"), filePath: store.filePath, streamID: store.streamID}
	if err := failing.Save(3); err == nil {
		t.Fatal("expected Save to fail before rename, got nil")
	}
	if epoch, err := store.Load(); err != nil || epoch != 2 {
		t.Fatalf("expected old epoch 2 readable after failed save, got epoch=%d, err=%v", epoch, err)
	}
}
