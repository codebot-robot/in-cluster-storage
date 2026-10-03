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
	"testing"
)

func TestLRUCacheOperations(t *testing.T) {
	evicted := make(map[string]int)
	lru := NewLRUCache[string, int](3, func(k string, v int) {
		evicted[k] = v
	})

	lru.Put("a", 1)
	lru.Put("b", 2)
	lru.Put("c", 3)

	if lru.Len() != 3 {
		t.Fatalf("Expected len 3, got %d", lru.Len())
	}

	// Access "a" to make it MRU (order now: a, c, b)
	val, ok := lru.Get("a")
	if !ok || val != 1 {
		t.Fatalf("Expected to get 'a'=1, got %d, %v", val, ok)
	}

	// Add "d", should evict LRU "b"
	lru.Put("d", 4)
	if lru.Len() != 3 {
		t.Fatalf("Expected len 3, got %d", lru.Len())
	}
	if evicted["b"] != 2 {
		t.Fatalf("Expected 'b' to be evicted, got: %v", evicted)
	}

	// "b" should not be in cache
	if _, ok := lru.Get("b"); ok {
		t.Fatalf("Expected 'b' to be missing from cache")
	}

	// EvictAll
	lru.EvictAll()
	if lru.Len() != 0 {
		t.Fatalf("Expected len 0 after EvictAll, got %d", lru.Len())
	}
	if len(evicted) != 4 {
		t.Fatalf("Expected 4 total evictions, got %d: %v", len(evicted), evicted)
	}
}

func TestLRUCacheByteLimitsAndStats(t *testing.T) {
	evicted := make(map[string]string)
	sizeFn := func(k string, v string) int64 {
		return int64(len(k) + len(v))
	}
	// Capacity 10 entries, but max 10 bytes limit
	lru := NewLRUCacheWithLimits[string, string](10, 10, sizeFn, func(k string, v string) {
		evicted[k] = v
	})

	// Put "k1": "1234" -> 2 + 4 = 6 bytes
	lru.Put("k1", "1234")
	if lru.Bytes() != 6 {
		t.Fatalf("expected 6 bytes, got %d", lru.Bytes())
	}

	// Put "k2": "5678" -> 2 + 4 = 6 bytes. Total would be 12 > 10, so k1 must be evicted!
	lru.Put("k2", "5678")
	if lru.Bytes() != 6 {
		t.Fatalf("expected 6 bytes after eviction, got %d", lru.Bytes())
	}
	if evicted["k1"] != "1234" {
		t.Fatalf("expected k1 evicted, got %v", evicted)
	}

	// Stats tracking
	val, ok := lru.Get("k2")
	if !ok || val != "5678" {
		t.Fatalf("expected k2 found, got %v, %v", val, ok)
	}
	_, ok = lru.Get("nonexistent")
	if ok {
		t.Fatalf("expected nonexistent not found")
	}

	stats := lru.Stats()
	if stats.Hits != 1 {
		t.Fatalf("expected 1 hit, got %d", stats.Hits)
	}
	if stats.Misses != 1 {
		t.Fatalf("expected 1 miss, got %d", stats.Misses)
	}
	if stats.HitRate != 0.5 {
		t.Fatalf("expected hit rate 0.5, got %f", stats.HitRate)
	}
	if stats.Entries != 1 {
		t.Fatalf("expected 1 entry, got %d", stats.Entries)
	}
	if stats.Bytes != 6 {
		t.Fatalf("expected 6 bytes, got %d", stats.Bytes)
	}

	lru.ResetStats()
	stats = lru.Stats()
	if stats.Hits != 0 || stats.Misses != 0 || stats.HitRate != 0.0 {
		t.Fatalf("expected reset stats, got hits=%d misses=%d hitRate=%f", stats.Hits, stats.Misses, stats.HitRate)
	}

	// Peek should not alter hit/miss stats or LRU order
	val, ok = lru.Peek("k2")
	if !ok || val != "5678" {
		t.Fatalf("expected peek k2 found, got %v", ok)
	}
	if lru.Stats().Hits != 0 {
		t.Fatalf("peek should not increment hits")
	}

	// Dynamic limit update
	lru.SetLimits(2, 20)
	lru.Put("k3", "9012") // 6 bytes -> total 12 bytes <= 20
	if lru.Len() != 2 {
		t.Fatalf("expected len 2, got %d", lru.Len())
	}
	if lru.Bytes() != 12 {
		t.Fatalf("expected 12 bytes, got %d", lru.Bytes())
	}
}

func TestLRUCacheConcurrent(t *testing.T) {
	lru := NewLRUCache[int, int](100, nil)
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for j := 0; j < 500; j++ {
				key := (workerID*500 + j) % 150
				if j%2 == 0 {
					lru.Put(key, key*10)
				} else {
					_, _ = lru.Get(key)
				}
			}
		}(i)
	}
	wg.Wait()
}
