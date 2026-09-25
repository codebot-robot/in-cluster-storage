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

type lruElement[K comparable, V any] struct {
	key   K
	value V
	prev  *lruElement[K, V]
	next  *lruElement[K, V]
}

// LRUCache implements an in-memory least-recently-used cache with an eviction callback.
type LRUCache[K comparable, V any] struct {
	capacity int
	items    map[K]*lruElement[K, V]
	head     *lruElement[K, V]
	tail     *lruElement[K, V]
	onEvict  func(key K, value V)
}

// NewLRUCache creates a new LRUCache with the specified capacity and eviction callback.
func NewLRUCache[K comparable, V any](capacity int, onEvict func(key K, value V)) *LRUCache[K, V] {
	if capacity <= 0 {
		capacity = 1024
	}
	return &LRUCache[K, V]{
		capacity: capacity,
		items:    make(map[K]*lruElement[K, V]),
		onEvict:  onEvict,
	}
}

// Get returns the value associated with key, marking it as most recently used.
func (c *LRUCache[K, V]) Get(key K) (V, bool) {
	elem, ok := c.items[key]
	if !ok {
		var zero V
		return zero, false
	}
	c.moveToHead(elem)
	return elem.value, true
}

// Peek returns the value associated with key without modifying the LRU order.
func (c *LRUCache[K, V]) Peek(key K) (V, bool) {
	elem, ok := c.items[key]
	if !ok {
		var zero V
		return zero, false
	}
	return elem.value, true
}

// Put inserts or updates key-value pair, evicting the least recently used item if capacity is exceeded.
func (c *LRUCache[K, V]) Put(key K, value V) {
	if elem, ok := c.items[key]; ok {
		elem.value = value
		c.moveToHead(elem)
		return
	}

	elem := &lruElement[K, V]{
		key:   key,
		value: value,
	}
	c.items[key] = elem
	c.addToHead(elem)

	if len(c.items) > c.capacity {
		c.evictTail()
	}
}

// Remove deletes key from the cache without triggering eviction callback.
func (c *LRUCache[K, V]) Remove(key K) {
	if elem, ok := c.items[key]; ok {
		c.removeElement(elem)
		delete(c.items, key)
	}
}

// Clear clears the cache without triggering eviction callback.
func (c *LRUCache[K, V]) Clear() {
	c.items = make(map[K]*lruElement[K, V])
	c.head = nil
	c.tail = nil
}

// Len returns the number of items currently in the cache.
func (c *LRUCache[K, V]) Len() int {
	return len(c.items)
}

// Capacity returns the maximum capacity of the cache.
func (c *LRUCache[K, V]) Capacity() int {
	return c.capacity
}

// ForEach iterates over all items in MRU to LRU order.
func (c *LRUCache[K, V]) ForEach(fn func(key K, value V)) {
	curr := c.head
	for curr != nil {
		fn(curr.key, curr.value)
		curr = curr.next
	}
}

// EvictAll evicts all elements currently in cache, invoking the onEvict callback for each.
func (c *LRUCache[K, V]) EvictAll() {
	for c.tail != nil {
		c.evictTail()
	}
}

func (c *LRUCache[K, V]) addToHead(elem *lruElement[K, V]) {
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

func (c *LRUCache[K, V]) moveToHead(elem *lruElement[K, V]) {
	if c.head == elem {
		return
	}
	c.removeElement(elem)
	c.addToHead(elem)
}

func (c *LRUCache[K, V]) removeElement(elem *lruElement[K, V]) {
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

func (c *LRUCache[K, V]) evictTail() {
	if c.tail == nil {
		return
	}
	tail := c.tail
	c.removeElement(tail)
	delete(c.items, tail.key)
	if c.onEvict != nil {
		c.onEvict(tail.key, tail.value)
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
	Added      map[string]DirEntry
	Deleted    map[string]bool
	PrevOffset LocalOffset
	IsDirty    bool
}
