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
		k1 := prefix + "a/1.txt"
		k2 := prefix + "a/2.txt"
		k3 := prefix + "b/3.txt"

		for _, k := range []string{k1, k2, k3} {
			if _, err := b.PutObject(ctx, "", k, blob.NewByteStreamFromBytes([]byte(k))); err != nil {
				t.Fatalf("PutObject %s failed: %v", k, err)
			}
		}

		// List all under prefix
		listAll, err := b.ListObjects(ctx, "", prefix)
		if err != nil {
			t.Fatalf("ListObjects %s failed: %v", prefix, err)
		}
		sort.Strings(listAll)
		if len(listAll) < 3 {
			t.Fatalf("ListObjects expected at least 3 items, got %v", listAll)
		}

		// List sub-prefix
		listA, err := b.ListObjects(ctx, "", prefix+"a/")
		if err != nil {
			t.Fatalf("ListObjects subprefix failed: %v", err)
		}
		sort.Strings(listA)
		if len(listA) != 2 || listA[0] != k1 || listA[1] != k2 {
			t.Fatalf("ListObjects subprefix mismatch: got %v, want [%s, %s]", listA, k1, k2)
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
