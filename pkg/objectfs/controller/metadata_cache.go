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
	"sync"

	"google.golang.org/protobuf/proto"

	pb "github.com/gke-labs/in-cluster-storage/pkg/api/objectfs/v1alpha1"
	"github.com/gke-labs/in-cluster-storage/pkg/objectfs/blob"
)

type cacheElement[K comparable, V any] struct {
	key   K
	value V
	size  int64
	prev  *cacheElement[K, V]
	next  *cacheElement[K, V]
}

// LRUCacheStats captures snapshot statistics for an LRUCache.
type LRUCacheStats struct {
	Hits     uint64
	Misses   uint64
	HitRate  float64
	Entries  int
	Capacity int
	Bytes    int64
	MaxBytes int64
}

// LRUCache implements a thread-safe in-memory least-recently-used cache bounded by entry count and byte size.
type LRUCache[K comparable, V any] struct {
	mu       sync.Mutex
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

// NewLRUCache creates a new LRUCache with the specified entry capacity and eviction callback.
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
	if capacity <= 0 && maxBytes <= 0 {
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

// Get returns the value associated with key, updating its LRU order. Thread-safe.
func (c *LRUCache[K, V]) Get(key K) (V, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	elem, ok := c.items[key]
	if !ok {
		c.misses++
		var zero V
		return zero, false
	}
	c.hits++
	c.moveToFront(elem)
	return elem.value, true
}

// Peek returns the value associated with key without modifying LRU order. Thread-safe.
func (c *LRUCache[K, V]) Peek(key K) (V, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	elem, ok := c.items[key]
	if !ok {
		var zero V
		return zero, false
	}
	return elem.value, true
}

// Put inserts or updates a key-value pair, evicting the least recently used item(s) if capacity or byte limit is exceeded. Thread-safe.
func (c *LRUCache[K, V]) Put(key K, value V) {
	var sz int64
	if c.sizeFn != nil {
		sz = c.sizeFn(key, value)
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	if elem, ok := c.items[key]; ok {
		if c.onEvict != nil {
			c.onEvict(elem.key, elem.value)
		}
		c.curBytes += sz - elem.size
		elem.value = value
		elem.size = sz
		c.moveToFront(elem)
		c.evictExcessLocked()
		return
	}

	elem := &cacheElement[K, V]{
		key:   key,
		value: value,
		size:  sz,
	}
	c.items[key] = elem
	c.curBytes += sz
	c.pushFront(elem)

	c.evictExcessLocked()
}

func (c *LRUCache[K, V]) evictExcessLocked() {
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

// Remove deletes key from the cache without triggering eviction callback. Thread-safe.
func (c *LRUCache[K, V]) Remove(key K) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if elem, ok := c.items[key]; ok {
		c.removeElement(elem)
		delete(c.items, key)
		c.curBytes -= elem.size
	}
}

// Clear clears the cache without triggering eviction callback. Thread-safe.
func (c *LRUCache[K, V]) Clear() {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.items = make(map[K]*cacheElement[K, V])
	c.head = nil
	c.tail = nil
	c.curBytes = 0
}

// Len returns the number of items currently in the cache. Thread-safe.
func (c *LRUCache[K, V]) Len() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.items)
}

// Capacity returns the maximum entry capacity of the cache. Thread-safe.
func (c *LRUCache[K, V]) Capacity() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.capacity
}

// Bytes returns the current tracked byte size of elements in the cache. Thread-safe.
func (c *LRUCache[K, V]) Bytes() int64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.curBytes
}

// MaxBytes returns the maximum byte limit of the cache. Thread-safe.
func (c *LRUCache[K, V]) MaxBytes() int64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.maxBytes
}

// SetLimits updates the entry count and byte capacity of the cache and evicts excess elements. Thread-safe.
func (c *LRUCache[K, V]) SetLimits(capacity int, maxBytes int64) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.capacity = capacity
	c.maxBytes = maxBytes
	c.evictExcessLocked()
}

// Stats returns a snapshot of cache hit/miss and capacity metrics. Thread-safe.
func (c *LRUCache[K, V]) Stats() LRUCacheStats {
	c.mu.Lock()
	defer c.mu.Unlock()

	var hitRate float64
	total := c.hits + c.misses
	if total > 0 {
		hitRate = float64(c.hits) / float64(total)
	}
	return LRUCacheStats{
		Hits:     c.hits,
		Misses:   c.misses,
		HitRate:  hitRate,
		Entries:  len(c.items),
		Capacity: c.capacity,
		Bytes:    c.curBytes,
		MaxBytes: c.maxBytes,
	}
}

// Hits returns the total number of successful cache hits. Thread-safe.
func (c *LRUCache[K, V]) Hits() uint64 {
	return c.Stats().Hits
}

// Misses returns the total number of cache misses. Thread-safe.
func (c *LRUCache[K, V]) Misses() uint64 {
	return c.Stats().Misses
}

// HitRate returns the ratio of hits to total lookups (0.0 to 1.0). Thread-safe.
func (c *LRUCache[K, V]) HitRate() float64 {
	return c.Stats().HitRate
}

// ResetStats resets the hits and misses counters. Thread-safe.
func (c *LRUCache[K, V]) ResetStats() {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.hits = 0
	c.misses = 0
}

// ForEach iterates over all items in the cache. Thread-safe.
func (c *LRUCache[K, V]) ForEach(fn func(key K, value V)) {
	c.mu.Lock()
	defer c.mu.Unlock()

	for k, elem := range c.items {
		fn(k, elem.value)
	}
}

// EvictAll evicts all elements currently in cache, invoking the onEvict callback for each. Thread-safe.
func (c *LRUCache[K, V]) EvictAll() {
	c.mu.Lock()
	defer c.mu.Unlock()

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

// CachedInode represents an inode metadata entry held in memory.
type CachedInode struct {
	Row          *pb.Inode
	Chunks       map[uint32]string
	InlineData   []byte
	StagedChunks map[int][]byte
	DirtyChunks  map[int][]byte
	RedirectURL  string
	Data         blob.ByteStream
	IsDirty      bool
}

// mutate applies fn to a cloned copy of Row and updates Row to the new clone.
// It returns the newly created row. This ensures rows are never mutated in place.
func (n *CachedInode) mutate(fn func(*pb.Inode)) *pb.Inode {
	if n == nil {
		return nil
	}
	var newRow *pb.Inode
	if n.Row != nil {
		newRow = proto.Clone(n.Row).(*pb.Inode)
	} else {
		newRow = &pb.Inode{}
	}
	if fn != nil {
		fn(newRow)
	}
	n.Row = newRow
	return newRow
}
