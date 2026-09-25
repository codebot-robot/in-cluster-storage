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

package controller

import (
	"time"

	"github.com/gke-labs/in-cluster-storage/pkg/objectfs/blob"
)

// readMonotonicClock returns the current monotonic time in nanoseconds.
// In Go, time.Now() contains a monotonic clock reading that is fast (VDSO syscall-free on modern Linux).
// Extracted into a helper function to allow swapping clock implementations or benchmarking alternative timers.
func readMonotonicClock() int64 {
	return time.Now().UnixNano()
}

type cacheElement[K comparable, V any] struct {
	key      K
	value    V
	lastUsed int64 // Monotonic nanoseconds
}

// LRUCache implements an in-memory least-recently-used cache tracking access times.
type LRUCache[K comparable, V any] struct {
	capacity int
	items    map[K]*cacheElement[K, V]
	onEvict  func(key K, value V)
}

// NewLRUCache creates a new LRUCache with the specified capacity and eviction callback.
func NewLRUCache[K comparable, V any](capacity int, onEvict func(key K, value V)) *LRUCache[K, V] {
	if capacity <= 0 {
		capacity = 1024
	}
	return &LRUCache[K, V]{
		capacity: capacity,
		items:    make(map[K]*cacheElement[K, V]),
		onEvict:  onEvict,
	}
}

// Get returns the value associated with key, updating its lastUsed timestamp.
func (c *LRUCache[K, V]) Get(key K) (V, bool) {
	elem, ok := c.items[key]
	if !ok {
		var zero V
		return zero, false
	}
	elem.lastUsed = readMonotonicClock()
	return elem.value, true
}

// Peek returns the value associated with key without modifying access time.
func (c *LRUCache[K, V]) Peek(key K) (V, bool) {
	elem, ok := c.items[key]
	if !ok {
		var zero V
		return zero, false
	}
	return elem.value, true
}

// Put inserts or updates key-value pair, evicting the least recently used item(s) if capacity is exceeded.
func (c *LRUCache[K, V]) Put(key K, value V) {
	now := readMonotonicClock()
	if elem, ok := c.items[key]; ok {
		elem.value = value
		elem.lastUsed = now
		return
	}

	elem := &cacheElement[K, V]{
		key:      key,
		value:    value,
		lastUsed: now,
	}
	c.items[key] = elem

	if len(c.items) > c.capacity {
		c.evictOldest()
	}
}

// Remove deletes key from the cache without triggering eviction callback.
func (c *LRUCache[K, V]) Remove(key K) {
	delete(c.items, key)
}

// Clear clears the cache without triggering eviction callback.
func (c *LRUCache[K, V]) Clear() {
	c.items = make(map[K]*cacheElement[K, V])
}

// Len returns the number of items currently in the cache.
func (c *LRUCache[K, V]) Len() int {
	return len(c.items)
}

// Capacity returns the maximum capacity of the cache.
func (c *LRUCache[K, V]) Capacity() int {
	return c.capacity
}

// ForEach iterates over all items in the cache.
func (c *LRUCache[K, V]) ForEach(fn func(key K, value V)) {
	for k, elem := range c.items {
		fn(k, elem.value)
	}
}

// EvictAll evicts all elements currently in cache, invoking the onEvict callback for each.
func (c *LRUCache[K, V]) EvictAll() {
	type kv struct {
		k K
		v V
	}
	var toEvict []kv
	for k, elem := range c.items {
		toEvict = append(toEvict, kv{k: k, v: elem.value})
	}
	c.items = make(map[K]*cacheElement[K, V])
	if c.onEvict != nil {
		for _, item := range toEvict {
			c.onEvict(item.k, item.v)
		}
	}
}

func (c *LRUCache[K, V]) evictOldest() {
	if len(c.items) == 0 {
		return
	}

	// Find element with oldest timestamp
	var oldestKey K
	var oldestVal V
	var oldestTime int64 = 1<<63 - 1
	var found bool

	// Optimization: sample or scan. Since Go maps have pseudo-random iteration,
	// if map size is large we could sample, but for precise LRU scan items
	for k, elem := range c.items {
		if elem.lastUsed < oldestTime {
			oldestTime = elem.lastUsed
			oldestKey = k
			oldestVal = elem.value
			found = true
		}
	}

	if found {
		delete(c.items, oldestKey)
		if c.onEvict != nil {
			c.onEvict(oldestKey, oldestVal)
		}
	}
}

// CachedInode represents an inode metadata entry held in memory.
type CachedInode struct {
	ID          uint64
	Mode        uint32
	Size        int64
	ModTime     time.Time
	IsDir       bool
	Sha256      string
	ETag        string
	RedirectURL string
	Data        blob.ByteStream
	IsDirty     bool
}

// CachedDir represents a directory metadata entry held in memory.
type CachedDir struct {
	ID         uint64
	Entries    map[string]DirEntry
	Added      map[string]bool
	Deleted    map[string]bool
	PrevOffset LocalOffset
	IsDirty    bool
}
