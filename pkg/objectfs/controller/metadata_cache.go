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
	"google.golang.org/protobuf/proto"
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
	size     int64
	lastUsed int64 // Monotonic nanoseconds
	prev     *cacheElement[K, V]
	next     *cacheElement[K, V]
}

// LRUCache implements an in-memory least-recently-used cache tracking access times and byte sizes with O(1) operations.
type LRUCache[K comparable, V any] struct {
	capacity int
	maxBytes int64
	curBytes int64
	items    map[K]*cacheElement[K, V]
	head     *cacheElement[K, V] // MRU
	tail     *cacheElement[K, V] // LRU
	sizeFn   func(key K, value V) int64
	onEvict  func(key K, value V)
	hits     uint64
	misses   uint64
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

// NewLRUCacheWithLimits creates a new LRUCache with entry count and byte bounds, custom size function, and eviction callback.
func NewLRUCacheWithLimits[K comparable, V any](capacity int, maxBytes int64, sizeFn func(key K, value V) int64, onEvict func(key K, value V)) *LRUCache[K, V] {
	if capacity <= 0 {
		capacity = 1024
	}
	return &LRUCache[K, V]{
		capacity: capacity,
		maxBytes: maxBytes,
		items:    make(map[K]*cacheElement[K, V]),
		sizeFn:   sizeFn,
		onEvict:  onEvict,
	}
}

func (c *LRUCache[K, V]) pushFront(elem *cacheElement[K, V]) {
	elem.prev = nil
	elem.next = c.head
	if c.head != nil {
		c.head.prev = elem
	}
	c.head = elem
	if c.tail == nil {
		c.tail = elem
	}
}

func (c *LRUCache[K, V]) removeElement(elem *cacheElement[K, V]) {
	if elem.prev != nil {
		elem.prev.next = elem.next
	} else {
		c.head = elem.next
	}
	if elem.next != nil {
		elem.next.prev = elem.prev
	} else {
		c.tail = elem.prev
	}
	elem.prev = nil
	elem.next = nil
}

func (c *LRUCache[K, V]) moveToFront(elem *cacheElement[K, V]) {
	if c.head == elem {
		return
	}
	c.removeElement(elem)
	c.pushFront(elem)
}

// Get returns the value associated with key, updating its access time and LRU order.
func (c *LRUCache[K, V]) Get(key K) (V, bool) {
	elem, ok := c.items[key]
	if !ok {
		c.misses++
		var zero V
		return zero, false
	}
	c.hits++
	elem.lastUsed = readMonotonicClock()
	c.moveToFront(elem)
	return elem.value, true
}

// Peek returns the value associated with key without modifying access time or LRU order.
func (c *LRUCache[K, V]) Peek(key K) (V, bool) {
	elem, ok := c.items[key]
	if !ok {
		var zero V
		return zero, false
	}
	return elem.value, true
}

// Put inserts or updates key-value pair, evicting the least recently used item(s) if capacity or byte limit is exceeded.
func (c *LRUCache[K, V]) Put(key K, value V) {
	now := readMonotonicClock()
	var sz int64
	if c.sizeFn != nil {
		sz = c.sizeFn(key, value)
	}

	if elem, ok := c.items[key]; ok {
		c.curBytes += sz - elem.size
		elem.value = value
		elem.size = sz
		elem.lastUsed = now
		c.moveToFront(elem)
		c.evictExcess()
		return
	}

	elem := &cacheElement[K, V]{
		key:      key,
		value:    value,
		size:     sz,
		lastUsed: now,
	}
	c.items[key] = elem
	c.curBytes += sz
	c.pushFront(elem)

	c.evictExcess()
}

func (c *LRUCache[K, V]) evictExcess() {
	for c.tail != nil && ((c.capacity > 0 && len(c.items) > c.capacity) || (c.maxBytes > 0 && c.curBytes > c.maxBytes && len(c.items) > 0)) {
		oldest := c.tail
		c.removeElement(oldest)
		delete(c.items, oldest.key)
		c.curBytes -= oldest.size
		if c.onEvict != nil {
			c.onEvict(oldest.key, oldest.value)
		}
	}
}

// Remove deletes key from the cache without triggering eviction callback.
func (c *LRUCache[K, V]) Remove(key K) {
	if elem, ok := c.items[key]; ok {
		c.removeElement(elem)
		delete(c.items, key)
		c.curBytes -= elem.size
	}
}

// Clear clears the cache without triggering eviction callback.
func (c *LRUCache[K, V]) Clear() {
	c.items = make(map[K]*cacheElement[K, V])
	c.head = nil
	c.tail = nil
	c.curBytes = 0
}

// Len returns the number of items currently in the cache.
func (c *LRUCache[K, V]) Len() int {
	return len(c.items)
}

// Capacity returns the maximum capacity of the cache.
func (c *LRUCache[K, V]) Capacity() int {
	return c.capacity
}

// Bytes returns the current tracked byte size of elements in the cache.
func (c *LRUCache[K, V]) Bytes() int64 {
	return c.curBytes
}

// MaxBytes returns the maximum byte limit of the cache.
func (c *LRUCache[K, V]) MaxBytes() int64 {
	return c.maxBytes
}

// SetLimits updates the entry count and byte capacity of the cache and evicts excess elements.
func (c *LRUCache[K, V]) SetLimits(capacity int, maxBytes int64) {
	if capacity > 0 {
		c.capacity = capacity
	}
	c.maxBytes = maxBytes
	c.evictExcess()
}

// Hits returns the total number of successful cache hits.
func (c *LRUCache[K, V]) Hits() uint64 {
	return c.hits
}

// Misses returns the total number of cache misses.
func (c *LRUCache[K, V]) Misses() uint64 {
	return c.misses
}

// HitRate returns the ratio of hits to total lookups (0.0 to 1.0).
func (c *LRUCache[K, V]) HitRate() float64 {
	total := c.hits + c.misses
	if total == 0 {
		return 0.0
	}
	return float64(c.hits) / float64(total)
}

// ResetStats resets the hits and misses counters.
func (c *LRUCache[K, V]) ResetStats() {
	c.hits = 0
	c.misses = 0
}

// ForEach iterates over all items in the cache.
func (c *LRUCache[K, V]) ForEach(fn func(key K, value V)) {
	for k, elem := range c.items {
		fn(k, elem.value)
	}
}

// EvictAll evicts all elements currently in cache, invoking the onEvict callback for each.
func (c *LRUCache[K, V]) EvictAll() {
	for c.tail != nil {
		oldest := c.tail
		c.removeElement(oldest)
		delete(c.items, oldest.key)
		c.curBytes -= oldest.size
		if c.onEvict != nil {
			c.onEvict(oldest.key, oldest.value)
		}
	}
}

// SQLiteCacheKey identifies a decoded row in the SQLite metadata cache by table name and canonical key.
type SQLiteCacheKey struct {
	Table string
	Key   string
}

// SQLiteCachedRow holds a decoded protobuf message row or a negative entry marker.
type SQLiteCachedRow struct {
	Msg    proto.Message
	Exists bool
}

// SQLiteCacheSizeFn calculates the approximate memory footprint of a cached SQLite row.
func SQLiteCacheSizeFn(k SQLiteCacheKey, v *SQLiteCachedRow) int64 {
	size := int64(len(k.Table) + len(k.Key) + 96)
	if v != nil && v.Exists && v.Msg != nil {
		size += int64(proto.Size(v.Msg) + 64)
	}
	return size
}

// CachedInode represents an inode metadata entry held in memory.
type CachedInode struct {
	ID             uint64
	Mode           uint32
	Size           int64
	ModTime        time.Time
	IsDir          bool
	Sha256         string
	ManifestSha256 string
	ContentSha256  string
	ChunkSize      uint32
	Chunks         map[uint32]string
	InlineData     []byte
	StagedChunks   map[int][]byte
	DirtyChunks    map[int][]byte
	ETag           string
	RedirectURL    string
	Data           blob.ByteStream
	Uid            uint32
	Gid            uint32
	IsDirty        bool
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
