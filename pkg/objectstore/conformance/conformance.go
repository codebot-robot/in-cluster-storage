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

package conformance

import (
	"bytes"
	"sort"
	"testing"

	"github.com/gke-labs/in-cluster-storage/pkg/objectfs/blob"
	"github.com/gke-labs/in-cluster-storage/pkg/objectstore"
)

// RunBackendTests executes the full conformance suite against a Backend implementation.
func RunBackendTests(t *testing.T, b objectstore.Backend) {
	ctx := t.Context()

	t.Run("PutAndGet", func(t *testing.T) {
		key := "conformance/put-get.txt"
		content := []byte("Hello, ObjectStore Conformance Testing!")
		stream := blob.NewByteStreamFromBytes(content)

		etag, err := b.PutObject(ctx, "", key, stream)
		if err != nil {
			t.Fatalf("PutObject failed: %v", err)
		}
		if etag == "" {
			t.Errorf("Expected non-empty ETag")
		}

		var buf bytes.Buffer
		if err := b.GetObject(ctx, "", key, 0, 0, &buf); err != nil {
			t.Fatalf("GetObject failed: %v", err)
		}
		if !bytes.Equal(buf.Bytes(), content) {
			t.Fatalf("GetObject content mismatch: got %q, want %q", buf.String(), string(content))
		}
	})

	t.Run("RangedGet", func(t *testing.T) {
		key := "conformance/range.txt"
		content := []byte("0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ")
		stream := blob.NewByteStreamFromBytes(content)

		if _, err := b.PutObject(ctx, "", key, stream); err != nil {
			t.Fatalf("PutObject failed: %v", err)
		}

		// 1. Prefix range
		var buf1 bytes.Buffer
		if err := b.GetObject(ctx, "", key, 0, 10, &buf1); err != nil {
			t.Fatalf("GetObject [0:10] failed: %v", err)
		}
		if buf1.String() != "0123456789" {
			t.Fatalf("GetObject [0:10] got %q, want %q", buf1.String(), "0123456789")
		}

		// 2. Middle range
		var buf2 bytes.Buffer
		if err := b.GetObject(ctx, "", key, 10, 5, &buf2); err != nil {
			t.Fatalf("GetObject [10:15] failed: %v", err)
		}
		if buf2.String() != "ABCDE" {
			t.Fatalf("GetObject [10:15] got %q, want %q", buf2.String(), "ABCDE")
		}

		// 3. Tail range (offset to EOF)
		var buf3 bytes.Buffer
		if err := b.GetObject(ctx, "", key, 26, 0, &buf3); err != nil {
			t.Fatalf("GetObject [26:] failed: %v", err)
		}
		if buf3.String() != "QRSTUVWXYZ" {
			t.Fatalf("GetObject [26:] got %q, want %q", buf3.String(), "QRSTUVWXYZ")
		}

		// 4. Past EOF range
		var buf4 bytes.Buffer
		if err := b.GetObject(ctx, "", key, int64(len(content)+50), 10, &buf4); err != nil {
			t.Fatalf("GetObject past EOF failed: %v", err)
		}
		if buf4.Len() != 0 {
			t.Fatalf("GetObject past EOF got %d bytes, want 0", buf4.Len())
		}
	})

	t.Run("Overwrite", func(t *testing.T) {
		key := "conformance/overwrite.txt"
		data1 := []byte("initial content")
		data2 := []byte("overwritten new content")

		if _, err := b.PutObject(ctx, "", key, blob.NewByteStreamFromBytes(data1)); err != nil {
			t.Fatalf("PutObject initial failed: %v", err)
		}

		var buf1 bytes.Buffer
		if err := b.GetObject(ctx, "", key, 0, 0, &buf1); err != nil {
			t.Fatalf("GetObject initial failed: %v", err)
		}
		if !bytes.Equal(buf1.Bytes(), data1) {
			t.Fatalf("Initial content mismatch: got %q, want %q", buf1.String(), string(data1))
		}

		if _, err := b.PutObject(ctx, "", key, blob.NewByteStreamFromBytes(data2)); err != nil {
			t.Fatalf("PutObject overwrite failed: %v", err)
		}

		var buf2 bytes.Buffer
		if err := b.GetObject(ctx, "", key, 0, 0, &buf2); err != nil {
			t.Fatalf("GetObject overwrite failed: %v", err)
		}
		if !bytes.Equal(buf2.Bytes(), data2) {
			t.Fatalf("Overwritten content mismatch: got %q, want %q", buf2.String(), string(data2))
		}
	})

	t.Run("Delete", func(t *testing.T) {
		key := "conformance/to-delete.txt"
		data := []byte("delete me")

		if _, err := b.PutObject(ctx, "", key, blob.NewByteStreamFromBytes(data)); err != nil {
			t.Fatalf("PutObject failed: %v", err)
		}

		if err := b.DeleteObject(ctx, "", key); err != nil {
			t.Fatalf("DeleteObject failed: %v", err)
		}

		var buf bytes.Buffer
		if err := b.GetObject(ctx, "", key, 0, 0, &buf); err == nil {
			t.Fatalf("Expected GetObject after DeleteObject to fail, got success with %d bytes", buf.Len())
		} else {
			t.Logf("GetObject after DeleteObject returned expected error: %v", err)
		}

		// Idempotent delete on non-existent object
		if err := b.DeleteObject(ctx, "", "conformance/non-existent-key.txt"); err != nil {
			t.Fatalf("Idempotent DeleteObject failed: %v", err)
		}
	})

	t.Run("NotFound", func(t *testing.T) {
		var buf bytes.Buffer
		if err := b.GetObject(ctx, "", "conformance/definitely-not-found.dat", 0, 0, &buf); err == nil {
			t.Fatalf("Expected GetObject on non-existent object to return error, got nil")
		} else {
			t.Logf("GetObject on non-existent object returned expected error: %v", err)
		}
	})

	t.Run("ListObjectsWithPrefix", func(t *testing.T) {
		prefix := "conformance/list/"
		firstKey := prefix + "a/1.txt"
		secondKey := prefix + "a/2.txt"
		thirdKey := prefix + "b/3.txt"

		for _, itemKey := range []string{firstKey, secondKey, thirdKey} {
			if _, err := b.PutObject(ctx, "", itemKey, blob.NewByteStreamFromBytes([]byte(itemKey))); err != nil {
				t.Fatalf("PutObject %s failed: %v", itemKey, err)
			}
		}

		// List all under prefix
		listAll, err := b.ListObjects(ctx, "", prefix)
		if err != nil {
			t.Fatalf("ListObjects %s failed: %v", prefix, err)
		}
		var allKeys []string
		for _, entry := range listAll {
			allKeys = append(allKeys, entry.Key)
		}
		sort.Strings(allKeys)
		if len(allKeys) < 3 {
			t.Fatalf("ListObjects expected at least 3 items, got %v", allKeys)
		}

		// List sub-prefix
		listA, err := b.ListObjects(ctx, "", prefix+"a/")
		if err != nil {
			t.Fatalf("ListObjects subprefix failed: %v", err)
		}
		var subKeys []string
		for _, entry := range listA {
			subKeys = append(subKeys, entry.Key)
		}
		sort.Strings(subKeys)
		if len(subKeys) != 2 || subKeys[0] != firstKey || subKeys[1] != secondKey {
			t.Fatalf("ListObjects subprefix mismatch: got %v, want [%s, %s]", subKeys, firstKey, secondKey)
		}
	})

	t.Run("ListObjectsMetadata", func(t *testing.T) {
		prefix := "conformance/metadata/"
		firstKey := prefix + "first.txt"
		secondKey := prefix + "second.txt"
		firstContent := []byte("first object content")
		secondContent := []byte("second object longer content")

		if _, err := b.PutObject(ctx, "", firstKey, blob.NewByteStreamFromBytes(firstContent)); err != nil {
			t.Fatalf("PutObject %s failed: %v", firstKey, err)
		}

		if _, err := b.PutObject(ctx, "", secondKey, blob.NewByteStreamFromBytes(secondContent)); err != nil {
			t.Fatalf("PutObject %s failed: %v", secondKey, err)
		}

		entries, err := b.ListObjects(ctx, "", prefix)
		if err != nil {
			t.Fatalf("ListObjects failed: %v", err)
		}
		if len(entries) != 2 {
			t.Fatalf("expected 2 entries, got %d", len(entries))
		}

		sort.Slice(entries, func(firstIndex, secondIndex int) bool {
			return entries[firstIndex].Key < entries[secondIndex].Key
		})

		firstEntry := entries[0]
		secondEntry := entries[1]

		if firstEntry.Key != firstKey {
			t.Errorf("expected first key %s, got %s", firstKey, firstEntry.Key)
		}
		if firstEntry.Size != int64(len(firstContent)) {
			t.Errorf("expected first size %d, got %d", len(firstContent), firstEntry.Size)
		}
		if firstEntry.LastModified.IsZero() {
			t.Errorf("expected non-zero LastModified for first entry")
		}

		if secondEntry.Key != secondKey {
			t.Errorf("expected second key %s, got %s", secondKey, secondEntry.Key)
		}
		if secondEntry.Size != int64(len(secondContent)) {
			t.Errorf("expected second size %d, got %d", len(secondContent), secondEntry.Size)
		}
		if secondEntry.LastModified.IsZero() {
			t.Errorf("expected non-zero LastModified for second entry")
		}

		if firstEntry.LastModified.After(secondEntry.LastModified) {
			t.Errorf("expected LastModified ordered: first (%v) after second (%v)", firstEntry.LastModified, secondEntry.LastModified)
		}
	})

	t.Run("VolumeIDScoping", func(t *testing.T) {
		vol1 := "vol-conf-1"
		vol2 := "vol-conf-2"
		key := "file.txt"
		data1 := []byte("vol1 content")
		data2 := []byte("vol2 content")

		if _, err := b.PutObject(ctx, vol1, key, blob.NewByteStreamFromBytes(data1)); err != nil {
			t.Fatalf("PutObject vol1 failed: %v", err)
		}
		if _, err := b.PutObject(ctx, vol2, key, blob.NewByteStreamFromBytes(data2)); err != nil {
			t.Fatalf("PutObject vol2 failed: %v", err)
		}

		var buf1 bytes.Buffer
		if err := b.GetObject(ctx, vol1, key, 0, 0, &buf1); err != nil {
			t.Fatalf("GetObject vol1 failed: %v", err)
		}
		if !bytes.Equal(buf1.Bytes(), data1) {
			t.Fatalf("vol1 content mismatch: got %q, want %q", buf1.String(), string(data1))
		}

		var buf2 bytes.Buffer
		if err := b.GetObject(ctx, vol2, key, 0, 0, &buf2); err != nil {
			t.Fatalf("GetObject vol2 failed: %v", err)
		}
		if !bytes.Equal(buf2.Bytes(), data2) {
			t.Fatalf("vol2 content mismatch: got %q, want %q", buf2.String(), string(data2))
		}
	})
}
