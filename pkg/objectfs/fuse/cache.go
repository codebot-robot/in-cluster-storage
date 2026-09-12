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

type CachedEntry struct {
	Path     string
	Data     []byte
	Size     int64
	ModTime  time.Time
	LastRead time.Time
	IsDirty  bool
	Sha256   string
}

type NodeCache struct {
	mu       sync.RWMutex
	entries  map[string]*CachedEntry
	maxBytes int64
	curBytes int64
}

func NewNodeCache(maxBytes int64) *NodeCache {
	if maxBytes <= 0 {
		maxBytes = 128 * 1024 * 1024 // 128MB default cache
	}
	return &NodeCache{
		entries:  make(map[string]*CachedEntry),
		maxBytes: maxBytes,
	}
}

func (c *NodeCache) Get(path string) (*CachedEntry, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	entry, ok := c.entries[path]
	if !ok {
		return nil, false
	}
	entry.LastRead = time.Now()
	// Return shallow copy
	copyEntry := *entry
	return &copyEntry, true
}

func (c *NodeCache) GetDirty(path string) (*CachedEntry, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	entry, ok := c.entries[path]
	if !ok || !entry.IsDirty {
		return nil, false
	}
	entry.LastRead = time.Now()
	copyEntry := *entry
	return &copyEntry, true
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

func (c *NodeCache) MarkClean(path string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if entry, ok := c.entries[path]; ok {
		entry.IsDirty = false
	}
}

func (c *NodeCache) Put(path string, data []byte, modTime time.Time, sha256 string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if old, ok := c.entries[path]; ok {
		// If existing entry is dirty, don't overwrite dirty uncommitted data with stale data
		if old.IsDirty {
			return
		}
		c.curBytes -= int64(len(old.Data))
		delete(c.entries, path)
	}

	dataLen := int64(len(data))
	c.evictIfNeededLocked(dataLen)

	buf := make([]byte, len(data))
	copy(buf, data)

	c.entries[path] = &CachedEntry{
		Path:     path,
		Data:     buf,
		Size:     dataLen,
		ModTime:  modTime,
		LastRead: time.Now(),
		Sha256:   sha256,
		IsDirty:  false,
	}
	c.curBytes += dataLen
}

func (c *NodeCache) WriteAt(path string, offset int64, data []byte, modTime time.Time) int64 {
	c.mu.Lock()
	defer c.mu.Unlock()

	entry, ok := c.entries[path]
	if !ok {
		entry = &CachedEntry{
			Path:     path,
			Data:     make([]byte, 0),
			Size:     0,
			ModTime:  modTime,
			LastRead: time.Now(),
			IsDirty:  true,
		}
		c.entries[path] = entry
	}

	oldLen := int64(len(entry.Data))
	newNeeded := offset + int64(len(data))

	if newNeeded > oldLen {
		c.evictIfNeededLocked(newNeeded - oldLen)
		newBuf := make([]byte, newNeeded)
		copy(newBuf, entry.Data)
		entry.Data = newBuf
		c.curBytes += (newNeeded - oldLen)
	}

	copy(entry.Data[offset:], data)
	entry.Size = int64(len(entry.Data))
	entry.ModTime = modTime
	entry.LastRead = time.Now()
	entry.IsDirty = true

	return entry.Size
}

func (c *NodeCache) Truncate(path string, size int64, modTime time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()

	entry, ok := c.entries[path]
	if !ok {
		entry = &CachedEntry{
			Path:     path,
			Data:     make([]byte, size),
			Size:     size,
			ModTime:  modTime,
			LastRead: time.Now(),
			IsDirty:  true,
		}
		c.entries[path] = entry
		c.curBytes += size
		return
	}

	oldLen := int64(len(entry.Data))
	if size < oldLen {
		entry.Data = entry.Data[:size]
		c.curBytes -= (oldLen - size)
	} else if size > oldLen {
		c.evictIfNeededLocked(size - oldLen)
		newBuf := make([]byte, size)
		copy(newBuf, entry.Data)
		entry.Data = newBuf
		c.curBytes += (size - oldLen)
	}
	entry.Size = size
	entry.ModTime = modTime
	entry.LastRead = time.Now()
	entry.IsDirty = true
}

func (c *NodeCache) evictIfNeededLocked(neededBytes int64) {
	for c.curBytes+neededBytes > c.maxBytes && len(c.entries) > 0 {
		var oldestPath string
		var oldestTime time.Time
		foundClean := false

		// Prefer evicting non-dirty entries first
		for p, e := range c.entries {
			if !e.IsDirty {
				if !foundClean || e.LastRead.Before(oldestTime) {
					oldestPath = p
					oldestTime = e.LastRead
					foundClean = true
				}
			}
		}

		if !foundClean {
			// If all entries are dirty, we do not evict dirty data to prevent uncommitted data loss
			break
		}

		if oldestPath != "" {
			c.curBytes -= int64(len(c.entries[oldestPath].Data))
			delete(c.entries, oldestPath)
		} else {
			break
		}
	}
}

func (c *NodeCache) Invalidate(path string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if old, ok := c.entries[path]; ok {
		c.curBytes -= int64(len(old.Data))
		delete(c.entries, path)
	}
}

func (c *NodeCache) InvalidateIfNotDirty(path string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if old, ok := c.entries[path]; ok && !old.IsDirty {
		c.curBytes -= int64(len(old.Data))
		delete(c.entries, path)
	}
}

func (c *NodeCache) Clear() {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.entries = make(map[string]*CachedEntry)
	c.curBytes = 0
}
