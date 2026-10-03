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
	cache.Put(10, data1, time.Now(), "sha1")

	entry1, ok := cache.Get(10)
	if !ok {
		t.Fatalf("Expected inode 10 to be in cache")
	}
	if string(entry1.Data) != string(data1) {
		t.Fatalf("Data mismatch for inode 10: %s", string(entry1.Data))
	}

	// Invalidate entry 1
	cache.Invalidate(10)
	if _, ok := cache.Get(10); ok {
		t.Fatalf("Expected inode 10 to be invalidated")
	}

	// Test eviction when capacity exceeded
	dataA := make([]byte, 60)
	dataB := make([]byte, 60)
	cache.Put(20, dataA, time.Now(), "shaA")
	time.Sleep(10 * time.Millisecond)
	cache.Put(30, dataB, time.Now(), "shaB")

	// Inode 20 should have been evicted because 60 + 60 > 100
	if _, ok := cache.Get(20); ok {
		t.Fatalf("Expected inode 20 to be evicted")
	}
	if _, ok := cache.Get(30); !ok {
		t.Fatalf("Expected inode 30 to remain in cache")
	}

	// Test Clear
	cache.Clear()
	if _, ok := cache.Get(30); ok {
		t.Fatalf("Expected inode 30 to be cleared")
	}
}

func TestNodeCacheWriteAndDirtyTracking(t *testing.T) {
	cache := NewNodeCache(1024)

	// Write locally at offset 0
	cache.WriteAt(100, 0, []byte("hello "), time.Now())
	// Write locally at offset 6
	cache.WriteAt(100, 6, []byte("world!"), time.Now())

	entry, ok := cache.Get(100)
	if !ok {
		t.Fatalf("Expected inode 100 to be in cache")
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
	if len(dirtyEntries) != 1 || dirtyEntries[0].Inode != 100 {
		t.Fatalf("Expected 1 dirty entry for inode 100, got %v", dirtyEntries)
	}

	// Mark clean
	cache.MarkClean(100)
	if _, isDirty := cache.GetDirty(100); isDirty {
		t.Fatalf("Expected inode 100 to be clean after MarkClean")
	}
	if len(cache.GetDirtyEntries()) != 0 {
		t.Fatalf("Expected 0 dirty entries after MarkClean")
	}

	// Truncate
	cache.Truncate(100, 5, time.Now())
	entryTrunc, ok := cache.Get(100)
	if !ok || !entryTrunc.IsDirty || string(entryTrunc.Data) != "hello" {
		t.Fatalf("Expected dirty truncated entry 'hello', got %q (dirty=%v)", string(entryTrunc.Data), entryTrunc.IsDirty)
	}
}

func TestNodeCacheChunkOperations(t *testing.T) {
	cache := NewNodeCache(1024)

	chunkSize := uint32(16)
	totalSize := int64(48)
	c0 := []byte("0123456789abcdef")
	c1 := []byte("ghijklmnopqrstuv")
	c2 := []byte("wxyz0123456789AB")

	cache.PutChunk(200, 0, chunkSize, totalSize, c0, time.Now())
	cache.PutChunk(200, 1, chunkSize, totalSize, c1, time.Now())
	cache.PutChunk(200, 2, chunkSize, totalSize, c2, time.Now())

	// Get individual chunk
	readC1, ok := cache.GetChunk(200, 1)
	if !ok || string(readC1) != string(c1) {
		t.Fatalf("GetChunk 1 failed: ok=%v, data=%q", ok, string(readC1))
	}

	// Get range spanning across chunk 0 and chunk 1 (offset 10, length 10)
	// c0[10:16] = "abcdef" (6 bytes) + c1[0:4] = "ghij" (4 bytes) -> "abcdefghij"
	rangeData, ok := cache.GetRange(200, 10, 10)
	if !ok || string(rangeData) != "abcdefghij" {
		t.Fatalf("GetRange failed: ok=%v, data=%q", ok, string(rangeData))
	}

	// Range with missing chunk should return false (cache miss)
	cache.Invalidate(200)
	cache.PutChunk(200, 0, chunkSize, totalSize, c0, time.Now())
	// Chunk 1 is missing
	_, ok = cache.GetRange(200, 10, 10)
	if ok {
		t.Fatalf("Expected cache miss for range requiring missing chunk 1")
	}
}

func TestNodeCacheSparseGetRangeAndWriteAt(t *testing.T) {
	cache := NewNodeCache(1024 * 1024)

	// Write at offset 10 with length 5 on an empty file -> creates hole of 10 zeroes
	cache.WriteAt(300, 10, []byte("world"), time.Now())
	data, ok := cache.GetRange(300, 0, 15)
	if !ok {
		t.Fatalf("GetRange failed on sparse write")
	}
	if len(data) != 15 {
		t.Fatalf("Expected length 15, got %d", len(data))
	}
	for i := 0; i < 10; i++ {
		if data[i] != 0 {
			t.Fatalf("Expected zero byte at %d, got %d", i, data[i])
		}
	}
	if string(data[10:]) != "world" {
		t.Fatalf("Expected 'world' at offset 10, got %q", string(data[10:]))
	}
}
