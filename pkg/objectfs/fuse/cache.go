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
	"sync"
	"time"
)

const DefaultChunkSize uint32 = 64 * 1024

// Generation represents a monotonically increasing cache-wide sequence number
// used to order local file mutations and asynchronous completions.
//
// The node cache has to support concurrent reads, writes and truncates on one
// file, plus asynchronous completions (read replies, flush acknowledgements,
// truncate acknowledgements, watch invalidations) whose arrival order does not
// match the order in which the operations took effect. We therefore linearise
// on a cache-wide monotonic Generation: every local mutation takes the next value,
// every in-flight operation records the generation it was issued against, and a
// completion is applied only if nothing newer has happened to the state it touches.
// Locally-mutated state that has not yet been acknowledged by the controller is
// authoritative over anything the controller reports in the meantime.
type Generation uint64

type CachedEntry struct {
	Inode       uint64
	Path        string
	Data        []byte
	Size        int64
	ModTime     time.Time
	LastRead    time.Time
	IsDirty     bool
	Sha256      string
	ChunkSize   uint32
	Chunks      map[int][]byte
	DirtyChunks map[int]Generation
	Generation  Generation
	SizeGen     Generation
}

type NodeCache struct {
	mu       sync.RWMutex
	entries  map[uint64]*CachedEntry
	maxBytes int64
	curBytes int64
	curGen   Generation
}

func NewNodeCache(maxBytes int64) *NodeCache {
	if maxBytes <= 0 {
		maxBytes = 128 * 1024 * 1024 // 128MB default cache
	}
	return &NodeCache{
		entries:  make(map[uint64]*CachedEntry),
		maxBytes: maxBytes,
	}
}

func (c *NodeCache) Get(inode uint64) (*CachedEntry, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	entry, ok := c.entries[inode]
	if !ok {
		return nil, false
	}
	entry.LastRead = time.Now()
	copyEntry := *entry
	if len(copyEntry.Data) == 0 && copyEntry.Size > 0 && len(entry.Chunks) > 0 && copyEntry.Size <= 16*1024*1024 {
		data := make([]byte, copyEntry.Size)
		cs := int64(entry.ChunkSize)
		if cs == 0 {
			cs = int64(DefaultChunkSize)
		}
		for idx, chunk := range entry.Chunks {
			off := int64(idx) * cs
			if off < copyEntry.Size {
				cLen := int64(len(chunk))
				if off+cLen > copyEntry.Size {
					cLen = copyEntry.Size - off
				}
				copy(data[off:off+cLen], chunk[:cLen])
			}
		}
		copyEntry.Data = data
	}
	return &copyEntry, true
}

func (c *NodeCache) GetRange(inode uint64, offset, length int64) ([]byte, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	entry, ok := c.entries[inode]
	if !ok {
		return nil, false
	}
	entry.LastRead = time.Now()

	if offset >= entry.Size {
		return []byte{}, true
	}

	end := offset + length
	if length <= 0 || end > entry.Size {
		end = entry.Size
	}

	if len(entry.Chunks) == 0 && len(entry.Data) > 0 {
		if end <= int64(len(entry.Data)) {
			res := make([]byte, end-offset)
			copy(res, entry.Data[offset:end])
			return res, true
		}
		return nil, false
	}

	cs := int64(entry.ChunkSize)
	if cs == 0 {
		cs = int64(DefaultChunkSize)
	}

	startChunk := int(offset / cs)
	endChunk := int((end - 1) / cs)

	var res []byte
	for i := startChunk; i <= endChunk; i++ {
		chunk, ok := entry.Chunks[i]
		if !ok {
			return nil, false
		}
		chunkLen := cs
		if int64(i+1)*cs > entry.Size {
			chunkLen = entry.Size - int64(i)*cs
		}
		chunkStart := int64(i) * cs
		rStart := offset - chunkStart
		if rStart < 0 {
			rStart = 0
		}
		rEnd := end - chunkStart
		if rEnd > chunkLen {
			rEnd = chunkLen
		}
		if rEnd > rStart {
			for b := rStart; b < rEnd; b++ {
				if b < int64(len(chunk)) {
					res = append(res, chunk[b])
				} else {
					res = append(res, 0)
				}
			}
		}
	}
	return res, true
}

func (c *NodeCache) GetGeneration(inode uint64) Generation {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if entry, ok := c.entries[inode]; ok {
		return entry.Generation
	}
	return c.curGen
}

func (c *NodeCache) PutChunk(inode uint64, chunkIdx int, chunkSize uint32, totalSize int64, data []byte, modTime time.Time, gen Generation) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if chunkSize == 0 {
		chunkSize = DefaultChunkSize
	}

	entry, ok := c.entries[inode]
	currentGen := c.curGen
	if ok {
		currentGen = entry.Generation
	}
	if gen < currentGen {
		return
	}

	if !ok {
		entry = &CachedEntry{
			Inode:       inode,
			Size:        totalSize,
			ModTime:     modTime,
			LastRead:    time.Now(),
			ChunkSize:   chunkSize,
			Chunks:      make(map[int][]byte),
			DirtyChunks: make(map[int]Generation),
			Generation:  gen,
		}
		c.entries[inode] = entry
	}
	if entry.Chunks == nil {
		entry.Chunks = make(map[int][]byte)
	}
	if entry.DirtyChunks == nil {
		entry.DirtyChunks = make(map[int]Generation)
	}

	// Don't overwrite dirty chunk with data from remote
	if _, isDirty := entry.DirtyChunks[chunkIdx]; isDirty {
		return
	}

	isLocalDirty := ok && (entry.IsDirty || len(entry.DirtyChunks) > 0 || entry.SizeGen != 0)
	chunkStart := int64(chunkIdx) * int64(chunkSize)

	// If entry has uncommitted local modifications (e.g. locally dirty or truncated),
	// do not let a read racing before controller sync resurrect chunks past local EOF
	// or expand the entry size. For clean entries, trust remote totalSize.
	if isLocalDirty && chunkStart >= entry.Size {
		return
	}

	if oldChunk, exists := entry.Chunks[chunkIdx]; exists {
		c.curBytes -= int64(len(oldChunk))
	}

	dataLen := int64(len(data))
	if isLocalDirty && chunkStart+dataLen > entry.Size {
		dataLen = entry.Size - chunkStart
		if dataLen < 0 {
			dataLen = 0
		}
		data = data[:dataLen]
	}
	c.evictIfNeededLocked(dataLen)

	buf := make([]byte, len(data))
	copy(buf, data)
	entry.Chunks[chunkIdx] = buf
	c.curBytes += dataLen
	if !isLocalDirty && totalSize > entry.Size {
		entry.Size = totalSize
	}
	entry.ChunkSize = chunkSize
	entry.ModTime = modTime
	entry.LastRead = time.Now()
}

func (c *NodeCache) GetChunk(inode uint64, chunkIdx int) ([]byte, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	entry, ok := c.entries[inode]
	if !ok || entry.Chunks == nil {
		return nil, false
	}
	chunk, ok := entry.Chunks[chunkIdx]
	if !ok {
		return nil, false
	}
	entry.LastRead = time.Now()
	res := make([]byte, len(chunk))
	copy(res, chunk)
	return res, true
}

func (c *NodeCache) GetDirty(inode uint64) (*CachedEntry, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	entry, ok := c.entries[inode]
	if !ok || !entry.IsDirty {
		return nil, false
	}
	entry.LastRead = time.Now()
	copyEntry := *entry
	if len(copyEntry.Data) == 0 && copyEntry.Size > 0 && len(entry.Chunks) > 0 && copyEntry.Size <= 16*1024*1024 {
		data := make([]byte, copyEntry.Size)
		cs := int64(entry.ChunkSize)
		if cs == 0 {
			cs = int64(DefaultChunkSize)
		}
		for idx, chunk := range entry.Chunks {
			off := int64(idx) * cs
			if off < copyEntry.Size {
				cLen := int64(len(chunk))
				if off+cLen > copyEntry.Size {
					cLen = copyEntry.Size - off
				}
				copy(data[off:off+cLen], chunk[:cLen])
			}
		}
		copyEntry.Data = data
	}
	return &copyEntry, true
}

func (c *NodeCache) GetDirtyChunks(inode uint64) (map[int][]byte, map[int]Generation, uint32, int64, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	entry, ok := c.entries[inode]
	if !ok || !entry.IsDirty || len(entry.DirtyChunks) == 0 {
		return nil, nil, 0, 0, false
	}

	chunkSize := entry.ChunkSize
	if chunkSize == 0 {
		chunkSize = DefaultChunkSize
	}

	res := make(map[int][]byte, len(entry.DirtyChunks))
	gens := make(map[int]Generation, len(entry.DirtyChunks))
	for idx, gen := range entry.DirtyChunks {
		if chunk, ok := entry.Chunks[idx]; ok {
			cCopy := make([]byte, len(chunk))
			copy(cCopy, chunk)
			res[idx] = cCopy
			gens[idx] = gen
		}
	}
	return res, gens, chunkSize, entry.Size, true
}

func (c *NodeCache) MarkChunkClean(inode uint64, chunkIdx int, gen Generation) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if entry, ok := c.entries[inode]; ok {
		if entry.DirtyChunks != nil {
			if dirtyGen, ok := entry.DirtyChunks[chunkIdx]; ok && dirtyGen <= gen {
				delete(entry.DirtyChunks, chunkIdx)
			}
		}
		if len(entry.DirtyChunks) == 0 && entry.SizeGen == 0 {
			entry.IsDirty = false
		}
	}
}

func (c *NodeCache) GetDirtyEntries() []*CachedEntry {
	c.mu.Lock()
	defer c.mu.Unlock()

	var dirty []*CachedEntry
	for _, entry := range c.entries {
		if entry.IsDirty {
			copyEntry := *entry
			dirty = append(dirty, &copyEntry)
		}
	}
	return dirty
}

func (c *NodeCache) MarkSizeClean(inode uint64, gen Generation) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if entry, ok := c.entries[inode]; ok {
		if entry.SizeGen <= gen {
			entry.SizeGen = 0
		}
		if len(entry.DirtyChunks) == 0 && entry.SizeGen == 0 {
			entry.IsDirty = false
		}
	}
}

func entryBytes(e *CachedEntry) int64 {
	if e == nil {
		return 0
	}
	if len(e.Chunks) > 0 {
		var total int64
		for _, chunk := range e.Chunks {
			total += int64(len(chunk))
		}
		return total
	}
	return int64(len(e.Data))
}

func (c *NodeCache) Put(inode uint64, data []byte, modTime time.Time, sha256 string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if old, ok := c.entries[inode]; ok {
		// If existing entry is dirty, don't overwrite dirty uncommitted data with stale data
		if old.IsDirty || len(old.DirtyChunks) > 0 {
			return
		}
		c.curBytes -= entryBytes(old)
		delete(c.entries, inode)
	}

	dataLen := int64(len(data))
	c.evictIfNeededLocked(dataLen)

	buf := make([]byte, len(data))
	copy(buf, data)

	entry := &CachedEntry{
		Inode:       inode,
		Data:        buf,
		Size:        dataLen,
		ModTime:     modTime,
		LastRead:    time.Now(),
		Sha256:      sha256,
		IsDirty:     false,
		ChunkSize:   DefaultChunkSize,
		Chunks:      make(map[int][]byte),
		DirtyChunks: make(map[int]Generation),
		Generation:  c.curGen,
	}

	cs := int64(DefaultChunkSize)
	numChunks := int((dataLen + cs - 1) / cs)
	for i := 0; i < numChunks; i++ {
		cStart := int64(i) * cs
		cEnd := cStart + cs
		if cEnd > dataLen {
			cEnd = dataLen
		}
		cBuf := make([]byte, cEnd-cStart)
		copy(cBuf, data[cStart:cEnd])
		entry.Chunks[i] = cBuf
	}

	c.entries[inode] = entry
	c.curBytes += dataLen
}

func (c *NodeCache) GetSize(inode uint64) (int64, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	entry, ok := c.entries[inode]
	if !ok {
		return 0, false
	}
	return entry.Size, true
}

func (c *NodeCache) WriteAt(inode uint64, offset int64, data []byte, modTime time.Time) int64 {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.curGen++
	gen := c.curGen

	entry, ok := c.entries[inode]
	if !ok {
		entry = &CachedEntry{
			Inode:       inode,
			Size:        0,
			ModTime:     modTime,
			LastRead:    time.Now(),
			IsDirty:     true,
			ChunkSize:   DefaultChunkSize,
			Chunks:      make(map[int][]byte),
			DirtyChunks: make(map[int]Generation),
		}
		c.entries[inode] = entry
	}
	if entry.Chunks == nil {
		entry.Chunks = make(map[int][]byte)
	}
	if entry.DirtyChunks == nil {
		entry.DirtyChunks = make(map[int]Generation)
	}
	if entry.ChunkSize == 0 {
		entry.ChunkSize = DefaultChunkSize
	}

	entry.Generation = gen

	cs := int64(entry.ChunkSize)
	newSize := offset + int64(len(data))
	if newSize < entry.Size {
		newSize = entry.Size
	}

	startChunk := int(offset / cs)
	endChunk := int((offset + int64(len(data)) - 1) / cs)

	for i := startChunk; i <= endChunk; i++ {
		chunkStart := int64(i) * cs
		chunkEnd := chunkStart + cs
		wStart := offset - chunkStart
		if wStart < 0 {
			wStart = 0
		}
		wEnd := offset + int64(len(data)) - chunkStart
		if wEnd > cs {
			wEnd = cs
		}
		dataStart := chunkStart - offset
		if dataStart < 0 {
			dataStart = 0
		}
		dataEnd := chunkEnd - offset
		if dataEnd > int64(len(data)) {
			dataEnd = int64(len(data))
		}

		expectedChunkLen := cs
		if int64(i+1)*cs > newSize {
			expectedChunkLen = newSize - int64(i)*cs
		}

		chunkBuf := entry.Chunks[i]
		oldChunkLen := int64(len(chunkBuf))
		if int64(len(chunkBuf)) < expectedChunkLen {
			newBuf := make([]byte, expectedChunkLen)
			copy(newBuf, chunkBuf)
			chunkBuf = newBuf
			c.curBytes += (expectedChunkLen - oldChunkLen)
		}

		copy(chunkBuf[wStart:wEnd], data[dataStart:dataEnd])
		entry.Chunks[i] = chunkBuf
		entry.DirtyChunks[i] = gen
	}

	entry.Size = newSize
	entry.ModTime = modTime
	entry.LastRead = time.Now()
	entry.IsDirty = true
	entry.Data = nil

	c.evictIfNeededLocked(0)
	return entry.Size
}

func (c *NodeCache) Truncate(inode uint64, size int64, modTime time.Time) Generation {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.curGen++
	gen := c.curGen

	entry, ok := c.entries[inode]
	if !ok {
		entry = &CachedEntry{
			Inode:       inode,
			Size:        size,
			ModTime:     modTime,
			LastRead:    time.Now(),
			IsDirty:     true,
			ChunkSize:   DefaultChunkSize,
			Chunks:      make(map[int][]byte),
			DirtyChunks: make(map[int]Generation),
			Generation:  gen,
			SizeGen:     gen,
		}
		c.entries[inode] = entry
		return gen
	}

	entry.Generation = gen
	entry.SizeGen = gen

	cs := int64(entry.ChunkSize)
	if cs == 0 {
		cs = int64(DefaultChunkSize)
	}

	numChunks := int((size + cs - 1) / cs)
	for idx, chunk := range entry.Chunks {
		if idx >= numChunks {
			c.curBytes -= int64(len(chunk))
			delete(entry.Chunks, idx)
			delete(entry.DirtyChunks, idx)
		} else if idx == numChunks-1 && numChunks > 0 {
			lastChunkLen := size - int64(idx)*cs
			if int64(len(chunk)) > lastChunkLen {
				c.curBytes -= (int64(len(chunk)) - lastChunkLen)
				entry.Chunks[idx] = chunk[:lastChunkLen]
			}
		}
	}

	if len(entry.Data) > 0 {
		if size < int64(len(entry.Data)) {
			c.curBytes -= (int64(len(entry.Data)) - size)
			entry.Data = entry.Data[:size]
		}
	}

	entry.Size = size
	entry.ModTime = modTime
	entry.LastRead = time.Now()
	entry.IsDirty = true
	return gen
}

func (c *NodeCache) evictIfNeededLocked(neededBytes int64) {
	for c.curBytes+neededBytes > c.maxBytes && len(c.entries) > 0 {
		var oldestInode uint64
		var oldestTime time.Time
		foundClean := false

		// Prefer evicting non-dirty entries first
		for ino, e := range c.entries {
			if !e.IsDirty && len(e.DirtyChunks) == 0 && e.SizeGen == 0 {
				if !foundClean || e.LastRead.Before(oldestTime) {
					oldestInode = ino
					oldestTime = e.LastRead
					foundClean = true
				}
			}
		}

		if !foundClean {
			// Never evict dirty data to prevent uncommitted data loss
			break
		}

		if oldestInode != 0 {
			c.curBytes -= entryBytes(c.entries[oldestInode])
			delete(c.entries, oldestInode)
		} else {
			break
		}
	}
}

func (c *NodeCache) Invalidate(inode uint64) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if old, ok := c.entries[inode]; ok {
		c.curBytes -= entryBytes(old)
		delete(c.entries, inode)
	}
}

// InvalidateIfNotDirty evicts inode from cache if it has no uncommitted local mutations.
// Watch events from the controller do not carry client generations; we drop clean cached
// state so subsequent reads will refetch authoritative state from the controller.
func (c *NodeCache) InvalidateIfNotDirty(inode uint64) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if old, ok := c.entries[inode]; ok && !old.IsDirty && len(old.DirtyChunks) == 0 && old.SizeGen == 0 {
		c.curBytes -= entryBytes(old)
		delete(c.entries, inode)
	}
}

func (c *NodeCache) InvalidateAllClean() {
	c.mu.Lock()
	defer c.mu.Unlock()

	for ino, entry := range c.entries {
		if !entry.IsDirty && len(entry.DirtyChunks) == 0 && entry.SizeGen == 0 {
			c.curBytes -= entryBytes(entry)
			delete(c.entries, ino)
		}
	}
}

func (c *NodeCache) Clear() {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.entries = make(map[uint64]*CachedEntry)
	c.curBytes = 0
}
