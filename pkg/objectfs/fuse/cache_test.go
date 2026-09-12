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

package fuse

import (
	"testing"
	"time"
)

func TestNodeCacheOperations(t *testing.T) {
	cache := NewNodeCache(100) // 100 bytes max

	// Put entry 1 (30 bytes)
	data1 := []byte("123456789012345678901234567890")
	cache.Put("/file1", data1, time.Now(), "sha1")

	entry1, ok := cache.Get("/file1")
	if !ok {
		t.Fatalf("Expected /file1 to be in cache")
	}
	if string(entry1.Data) != string(data1) {
		t.Fatalf("Data mismatch for /file1: %s", string(entry1.Data))
	}

	// Invalidate entry 1
	cache.Invalidate("/file1")
	if _, ok := cache.Get("/file1"); ok {
		t.Fatalf("Expected /file1 to be invalidated")
	}

	// Test eviction when capacity exceeded
	dataA := make([]byte, 60)
	dataB := make([]byte, 60)
	cache.Put("/fileA", dataA, time.Now(), "shaA")
	time.Sleep(10 * time.Millisecond)
	cache.Put("/fileB", dataB, time.Now(), "shaB")

	// /fileA should have been evicted because 60 + 60 > 100
	if _, ok := cache.Get("/fileA"); ok {
		t.Fatalf("Expected /fileA to be evicted")
	}
	if _, ok := cache.Get("/fileB"); !ok {
		t.Fatalf("Expected /fileB to remain in cache")
	}

	// Test Clear
	cache.Clear()
	if _, ok := cache.Get("/fileB"); ok {
		t.Fatalf("Expected /fileB to be cleared")
	}
}

func TestNodeCacheWriteAndDirtyTracking(t *testing.T) {
	cache := NewNodeCache(1024)

	// Write locally at offset 0
	cache.WriteAt("/file1.txt", 0, []byte("hello "), time.Now())
	// Write locally at offset 6
	cache.WriteAt("/file1.txt", 6, []byte("world!"), time.Now())

	entry, ok := cache.Get("/file1.txt")
	if !ok {
		t.Fatalf("Expected /file1.txt to be in cache")
	}
	if !entry.IsDirty {
		t.Fatalf("Expected entry to be marked dirty")
	}
	if string(entry.Data) != "hello world!" {
		t.Fatalf("Expected 'hello world!', got %q", string(entry.Data))
	}
	if entry.Size != 12 {
		t.Fatalf("Expected size 12, got %d", entry.Size)
	}

	dirtyEntries := cache.GetDirtyEntries()
	if len(dirtyEntries) != 1 || dirtyEntries[0].Path != "/file1.txt" {
		t.Fatalf("Expected 1 dirty entry for /file1.txt, got %v", dirtyEntries)
	}

	// Mark clean
	cache.MarkClean("/file1.txt")
	if _, isDirty := cache.GetDirty("/file1.txt"); isDirty {
		t.Fatalf("Expected /file1.txt to be clean after MarkClean")
	}
	if len(cache.GetDirtyEntries()) != 0 {
		t.Fatalf("Expected 0 dirty entries after MarkClean")
	}

	// Truncate
	cache.Truncate("/file1.txt", 5, time.Now())
	entryTrunc, ok := cache.Get("/file1.txt")
	if !ok || !entryTrunc.IsDirty || string(entryTrunc.Data) != "hello" {
		t.Fatalf("Expected dirty truncated entry 'hello', got %q (dirty=%v)", string(entryTrunc.Data), entryTrunc.IsDirty)
	}
}
