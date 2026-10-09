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
	dirty, gens, _, _, ok := cache.GetDirtyChunks(100)
	if !ok || len(dirty) == 0 {
		t.Fatalf("Expected dirty chunks for inode 100")
	}
	for idx, g := range gens {
		cache.MarkChunkClean(100, idx, g)
	}
	if _, isDirty := cache.GetDirty(100); isDirty {
		t.Fatalf("Expected inode 100 to be clean after MarkChunkClean")
	}
	if len(cache.GetDirtyEntries()) != 0 {
		t.Fatalf("Expected 0 dirty entries after MarkChunkClean")
	}

	// Truncate
	truncGen := cache.Truncate(100, 5, time.Now())
	entryTrunc, ok := cache.Get(100)
	if !ok || !entryTrunc.IsDirty || string(entryTrunc.Data) != "hello" {
		t.Fatalf("Expected dirty truncated entry 'hello', got %q (dirty=%v)", string(entryTrunc.Data), entryTrunc.IsDirty)
	}
	cache.MarkSizeClean(100, truncGen)
	if _, isDirty := cache.GetDirty(100); isDirty {
		t.Fatalf("Expected inode 100 to be clean after MarkSizeClean")
	}
}

func TestNodeCacheChunkOperations(t *testing.T) {
	cache := NewNodeCache(1024)

	chunkSize := uint32(16)
	totalSize := int64(48)
	c0 := []byte("0123456789abcdef")
	c1 := []byte("ghijklmnopqrstuv")
	c2 := []byte("wxyz0123456789AB")

	cache.PutChunk(200, 0, chunkSize, totalSize, c0, time.Now(), cache.GetGeneration(200))
	cache.PutChunk(200, 1, chunkSize, totalSize, c1, time.Now(), cache.GetGeneration(200))
	cache.PutChunk(200, 2, chunkSize, totalSize, c2, time.Now(), cache.GetGeneration(200))

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
	cache.PutChunk(200, 0, chunkSize, totalSize, c0, time.Now(), cache.GetGeneration(200))
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

func TestNodeCacheFlushRaceWriteGeneration(t *testing.T) {
	c := NewNodeCache(1024 * 1024)
	ino := uint64(10)
	now := time.Now()

	c.WriteAt(ino, 0, []byte("version-1"), now)
	dirty, gens, _, _, ok := c.GetDirtyChunks(ino)
	if !ok || len(dirty) != 1 {
		t.Fatalf("Expected 1 dirty chunk for version-1")
	}

	// Concurrent write arrives while flusher is uploading version-1
	c.WriteAt(ino, 0, []byte("version-2"), now)

	// Flusher finishes uploading version-1 and calls MarkChunkClean with version-1 generation
	for idx := range dirty {
		c.MarkChunkClean(ino, idx, gens[idx])
	}

	// Cached chunk 0 must still be dirty with "version-2" (not lost!)
	dirtyAfter, gensAfter, _, _, isDirty := c.GetDirtyChunks(ino)
	if !isDirty {
		t.Fatalf("Expected entry to remain dirty after concurrent write during flush")
	}
	if len(dirtyAfter) != 1 || string(dirtyAfter[0]) != "version-2" {
		t.Fatalf("Expected dirty chunk 0 to be 'version-2', got %v", dirtyAfter)
	}

	// Subsequent flush of version-2 should successfully clear dirty state
	c.MarkChunkClean(ino, 0, gensAfter[0])
	if _, _, _, _, isDirty := c.GetDirtyChunks(ino); isDirty {
		t.Fatalf("Expected entry to be clean after flushing version-2")
	}
}

func TestNodeCacheStaleReadSizeAfterTruncate(t *testing.T) {
	c := NewNodeCache(1 << 30)
	now := time.Now()
	ino := uint64(20)

	// Op 108: file at size 0x8110df
	cs := int64(DefaultChunkSize)
	c.PutChunk(ino, 0x5a, DefaultChunkSize, 0x8110df, make([]byte, cs), now, c.GetGeneration(ino))

	// Op 111: mapread issues an RPC, capturing generation
	readGen := c.GetGeneration(ino)

	// Op 112: truncate down to 0x71046a and mark clean
	truncGen := c.Truncate(ino, 0x71046a, now)
	c.MarkSizeClean(ino, truncGen)

	// Late readahead completes with older generation
	c.PutChunk(ino, 0x81, DefaultChunkSize, 0x8110df, make([]byte, 0x10df), now, readGen)

	// Op 114: in-place write well inside file
	c.WriteAt(ino, 0x1e7c27, make([]byte, 0xcdf4), now)

	entry, ok := c.GetDirty(ino)
	if !ok {
		t.Fatalf("Expected dirty entry after write")
	}
	if entry.Size != 0x71046a {
		t.Fatalf("Size error: got 0x%x, want 0x71046a (stale read resurrected old size)", entry.Size)
	}
}

func TestNodeCacheReadRacingTruncateRPC(t *testing.T) {
	c := NewNodeCache(1 << 30)
	ino := uint64(30)
	now := time.Now()
	cs := int64(DefaultChunkSize)

	// File on remote has 3 chunks (size 3*cs)
	c.PutChunk(ino, 0, DefaultChunkSize, 3*cs, make([]byte, cs), now, c.GetGeneration(ino))

	// Local truncate down to cs/2; TruncateFile RPC in flight
	truncGen := c.Truncate(ino, cs/2, now)

	// Concurrent reader issues ReadFile carrying the current generation
	readGen := c.GetGeneration(ino)

	// Controller answers with pre-truncate size (3*cs) and chunk 2
	c.PutChunk(ino, 2, DefaultChunkSize, 3*cs, make([]byte, cs), now, readGen)

	// TruncateFile RPC finishes and acknowledges size
	c.MarkSizeClean(ino, truncGen)

	// Subsequent in-place write
	c.WriteAt(ino, 0, []byte("x"), now)

	entry, ok := c.GetDirty(ino)
	if !ok {
		t.Fatalf("Expected dirty entry after write")
	}
	if entry.Size != cs/2 {
		t.Fatalf("Size error: got %d, want %d (stale remote size resurrected after truncate)", entry.Size, cs/2)
	}
	if _, ok := c.GetChunk(ino, 2); ok {
		t.Fatalf("Chunk 2 should have been dropped by PutChunk past local EOF")
	}
}

func TestNodeCacheGenerationsSurviveEviction(t *testing.T) {
	c := NewNodeCache(1 << 30)
	ino := uint64(40)
	now := time.Now()
	cs := int64(DefaultChunkSize)

	// Writes advance generation
	c.WriteAt(ino, 0, []byte("initial-data"), now)
	readGen := c.GetGeneration(ino)

	// Truncate and mark size clean
	truncGen := c.Truncate(ino, 3, now)
	c.MarkSizeClean(ino, truncGen)
	// Clear any dirty chunks from initial write so entry is fully clean
	_, gens, _, _, _ := c.GetDirtyChunks(ino)
	for idx, g := range gens {
		c.MarkChunkClean(ino, idx, g)
	}

	// Evict the clean entry
	c.InvalidateIfNotDirty(ino)
	if _, ok := c.Get(ino); ok {
		t.Fatalf("Expected entry to be evicted")
	}

	// Inode is recreated by a new write (advancing cache-wide generation)
	c.WriteAt(ino, 0, []byte("new"), now)

	// Stale PutChunk from before eviction arrives carrying readGen
	c.PutChunk(ino, 1, DefaultChunkSize, 2*cs, make([]byte, cs), now, readGen)

	entry, ok := c.GetDirty(ino)
	if !ok {
		t.Fatalf("Expected dirty entry")
	}
	if entry.Size != 3 {
		t.Fatalf("Expected entry size 3, got %d (stale completion survived eviction)", entry.Size)
	}
	if _, ok := c.GetChunk(ino, 1); ok {
		t.Fatalf("Chunk 1 should have been dropped due to stale generation")
	}
}
