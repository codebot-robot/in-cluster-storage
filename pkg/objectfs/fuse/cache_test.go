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
	cache.Put("/file1", data1, time.Now(), "sha1")

	entry1, ok := cache.Get("/file1")
	if !ok {
		t.Fatalf("Expected /file1 to be in cache")
	}
	if string(entry1.Data) != string(data1) {
		t.Fatalf("Data mismatch for /file1: %s", string(entry1.Data))
	}

	// Invalidate entry 1
	cache.Invalidate("/file1")
	if _, ok := cache.Get("/file1"); ok {
		t.Fatalf("Expected /file1 to be invalidated")
	}

	// Test eviction when capacity exceeded
	dataA := make([]byte, 60)
	dataB := make([]byte, 60)
	cache.Put("/fileA", dataA, time.Now(), "shaA")
	time.Sleep(10 * time.Millisecond)
	cache.Put("/fileB", dataB, time.Now(), "shaB")

	// /fileA should have been evicted because 60 + 60 > 100
	if _, ok := cache.Get("/fileA"); ok {
		t.Fatalf("Expected /fileA to be evicted")
	}
	if _, ok := cache.Get("/fileB"); !ok {
		t.Fatalf("Expected /fileB to remain in cache")
	}

	// Test Clear
	cache.Clear()
	if _, ok := cache.Get("/fileB"); ok {
		t.Fatalf("Expected /fileB to be cleared")
	}
}
