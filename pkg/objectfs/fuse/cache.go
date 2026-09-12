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

func (c *NodeCache) Put(path string, data []byte, modTime time.Time, sha256 string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if old, ok := c.entries[path]; ok {
		c.curBytes -= int64(len(old.Data))
		delete(c.entries, path)
	}

	// Evict older entries if over maxBytes
	dataLen := int64(len(data))
	for c.curBytes+dataLen > c.maxBytes && len(c.entries) > 0 {
		var oldestPath string
		var oldestTime time.Time
		first := true
		for p, e := range c.entries {
			if first || e.LastRead.Before(oldestTime) {
				oldestPath = p
				oldestTime = e.LastRead
				first = false
			}
		}
		if oldestPath != "" {
			c.curBytes -= int64(len(c.entries[oldestPath].Data))
			delete(c.entries, oldestPath)
		} else {
			break
		}
	}

	buf := make([]byte, len(data))
	copy(buf, data)

	c.entries[path] = &CachedEntry{
		Path:     path,
		Data:     buf,
		Size:     dataLen,
		ModTime:  modTime,
		LastRead: time.Now(),
		Sha256:   sha256,
	}
	c.curBytes += dataLen
}

func (c *NodeCache) Invalidate(path string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if old, ok := c.entries[path]; ok {
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
