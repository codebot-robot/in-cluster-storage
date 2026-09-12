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

package blob

import (
	"bytes"
	"context"
	"crypto/sha256"
	"fmt"
	"io"
	"strings"
	"sync"
	"testing"
)

type testMemoryBackend struct {
	mu      sync.RWMutex
	objects map[string][]byte
}

func newTestMemoryBackend() *testMemoryBackend {
	return &testMemoryBackend{
		objects: make(map[string][]byte),
	}
}

func (m *testMemoryBackend) storageKey(volumeID, key string) string {
	if volumeID == "" || strings.HasPrefix(key, "volumes/") || strings.HasPrefix(key, "blobs/") {
		return key
	}
	return fmt.Sprintf("%s/%s", volumeID, key)
}

func (m *testMemoryBackend) PutObject(ctx context.Context, volumeID, key string, data []byte) (string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	k := m.storageKey(volumeID, key)
	buf := make([]byte, len(data))
	copy(buf, data)
	m.objects[k] = buf

	h := sha256.Sum256(buf)
	return fmt.Sprintf("%x", h), nil
}

func (m *testMemoryBackend) GetObject(ctx context.Context, volumeID, key string, offset, length int64) ([]byte, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	k := m.storageKey(volumeID, key)
	data, ok := m.objects[k]
	if !ok {
		return nil, fmt.Errorf("object %s not found in volume %s", key, volumeID)
	}

	total := int64(len(data))
	if offset >= total {
		return []byte{}, nil
	}
	end := offset + length
	if length <= 0 || end > total {
		end = total
	}

	res := make([]byte, end-offset)
	copy(res, data[offset:end])
	return res, nil
}

func (m *testMemoryBackend) DeleteObject(ctx context.Context, volumeID, key string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	k := m.storageKey(volumeID, key)
	delete(m.objects, k)
	return nil
}

func (m *testMemoryBackend) GetRedirectURL(ctx context.Context, volumeID, key string) (string, error) {
	return "", nil
}

func (m *testMemoryBackend) ListObjects(ctx context.Context, volumeID, prefix string) ([]string, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	fullPrefix := m.storageKey(volumeID, prefix)
	var matches []string
	for k := range m.objects {
		if strings.HasPrefix(k, fullPrefix) {
			matches = append(matches, k)
		}
	}
	return matches, nil
}

func TestSingleBlobEncodeDecode(t *testing.T) {
	data := []byte(strings.Repeat("This is repetitive data to test compression. ", 100))
	rc, finalSHA, size, err := EncodeSingleBlob(bytes.NewReader(data), true)
	if err != nil {
		t.Fatalf("failed to encode single blob: %v", err)
	}
	encoded, err := io.ReadAll(rc)
	_ = rc.Close()
	if err != nil {
		t.Fatalf("failed reading encoded blob: %v", err)
	}

	if uint64(len(encoded)) != size {
		t.Fatalf("size mismatch: got %d, expected %d", len(encoded), size)
	}

	// Verify header
	if len(encoded) < SingleBlobHeaderSize {
		t.Fatalf("encoded blob too small: %d", len(encoded))
	}

	hdr, err := ParseFileHeader(encoded[:8])
	if err != nil {
		t.Fatalf("failed to parse file header: %v", err)
	}
	if hdr.FileType != FileTypeBlob {
		t.Fatalf("expected FileTypeBlob (1), got %d", hdr.FileType)
	}

	decRC, entry, err := DecodeSingleBlob(bytes.NewReader(encoded))
	if err != nil {
		t.Fatalf("failed to decode single blob: %v", err)
	}

	decoded, err := io.ReadAll(decRC)
	_ = decRC.Close()
	if err != nil {
		t.Fatalf("failed reading decoded data: %v", err)
	}

	if entry.SHA256 != finalSHA {
		t.Fatalf("SHA mismatch: %x vs %x", entry.SHA256, finalSHA)
	}

	if !bytes.Equal(decoded, data) {
		t.Fatalf("decoded data does not match original")
	}
}

func TestLargeTempFileStreaming(t *testing.T) {
	// Generate large content > 300KB to trigger tempfile creation in PrepareBlobPayload
	largeData := []byte(strings.Repeat("0123456789abcdef", 25*1024)) // 400KB

	rc, finalSHA, totalSize, err := EncodeSingleBlob(bytes.NewReader(largeData), true)
	if err != nil {
		t.Fatalf("EncodeSingleBlob failed: %v", err)
	}
	defer rc.Close()

	if totalSize <= uint64(len(largeData)/2) {
		t.Logf("Compressed 400KB to %d bytes", totalSize)
	}

	decRC, entry, err := DecodeSingleBlob(rc)
	if err != nil {
		t.Fatalf("DecodeSingleBlob failed: %v", err)
	}
	defer decRC.Close()

	if entry.SHA256 != finalSHA {
		t.Fatalf("SHA mismatch: %x vs %x", entry.SHA256, finalSHA)
	}

	out, err := io.ReadAll(decRC)
	if err != nil {
		t.Fatalf("ReadAll failed: %v", err)
	}
	if !bytes.Equal(out, largeData) {
		t.Fatalf("Decoded data mismatch")
	}
}

func TestPackfileEncodeDecode(t *testing.T) {
	inputs := []RawBlobInput{
		{Reader: bytes.NewReader([]byte("blob 1 content")), Compress: false},
		{Reader: bytes.NewReader([]byte("blob 2 with some more text for testing")), Compress: true},
		{Reader: bytes.NewReader([]byte(strings.Repeat("blob 3 highly repetitive repetitive data! ", 50))), Compress: true},
	}

	origContents := [][]byte{
		[]byte("blob 1 content"),
		[]byte("blob 2 with some more text for testing"),
		[]byte(strings.Repeat("blob 3 highly repetitive repetitive data! ", 50)),
	}

	packRC, entries, totalSize, err := EncodePack(inputs)
	if err != nil {
		t.Fatalf("failed to encode pack: %v", err)
	}
	packData, err := io.ReadAll(packRC)
	_ = packRC.Close()
	if err != nil {
		t.Fatalf("failed to read pack data: %v", err)
	}

	if uint64(len(packData)) != totalSize {
		t.Fatalf("pack size mismatch: %d vs %d", len(packData), totalSize)
	}

	// Verify pack tables
	parsedEntries, err := DecodePackTable(bytes.NewReader(packData))
	if err != nil {
		t.Fatalf("failed to decode pack table: %v", err)
	}
	if len(parsedEntries) != len(inputs) {
		t.Fatalf("expected %d entries, got %d", len(inputs), len(parsedEntries))
	}

	// For each original input, verify lookup in table and streaming extraction from pack
	for _, orig := range origContents {
		targetSHA := sha256.Sum256(orig)
		entry, found := LookupInEntries(parsedEntries, targetSHA)
		if !found {
			t.Fatalf("SHA %x not found in pack entries", targetSHA)
		}

		payloadReader := bytes.NewReader(packData[entry.DataOffset:])
		blobRC, err := DecodeBlobFromPayload(payloadReader, entry)
		if err != nil {
			t.Fatalf("failed to decode blob payload: %v", err)
		}
		extracted, err := io.ReadAll(blobRC)
		_ = blobRC.Close()
		if err != nil {
			t.Fatalf("failed reading extracted payload: %v", err)
		}
		if !bytes.Equal(extracted, orig) {
			t.Fatalf("extracted data mismatch for %s: got %q, expected %q", entry.SHA256Hex(), string(extracted), string(orig))
		}
	}

	_ = entries
}

func TestBlobStoreOperations(t *testing.T) {
	ctx := t.Context()
	backend := newTestMemoryBackend()
	store := NewStore(backend, 500) // 500 bytes threshold for testing small vs large

	smallData1 := []byte("hello small blob 1")
	smallData2 := []byte("hello small blob 2")
	largeData := []byte(strings.Repeat("large data blob exceeding threshold ", 30)) // > 1000 bytes

	smallSHA1 := fmt.Sprintf("%x", sha256.Sum256(smallData1))
	smallSHA2 := fmt.Sprintf("%x", sha256.Sum256(smallData2))
	largeSHA := fmt.Sprintf("%x", sha256.Sum256(largeData))

	// 1. PutBlobs batch
	batch := map[string][]byte{
		"small1": smallData1,
		"small2": smallData2,
		"large":  largeData,
	}
	if err := store.PutBlobs(ctx, batch); err != nil {
		t.Fatalf("PutBlobs failed: %v", err)
	}

	// 2. HasBlob check
	hasSmall, err := store.HasBlob(ctx, smallSHA1)
	if err != nil || !hasSmall {
		t.Fatalf("expected HasBlob for small1 to be true, got %v (err=%v)", hasSmall, err)
	}
	hasLarge, err := store.HasBlob(ctx, largeSHA)
	if err != nil || !hasLarge {
		t.Fatalf("expected HasBlob for large to be true, got %v (err=%v)", hasLarge, err)
	}

	// 3. GetBlob check
	gotSmall, err := store.GetBlob(ctx, smallSHA1)
	if err != nil || !bytes.Equal(gotSmall, smallData1) {
		t.Fatalf("failed to get small blob 1: %v", err)
	}

	gotSmall2, err := store.GetBlob(ctx, smallSHA2)
	if err != nil || !bytes.Equal(gotSmall2, smallData2) {
		t.Fatalf("failed to get small blob 2: %v", err)
	}

	gotLarge, err := store.GetBlob(ctx, largeSHA)
	if err != nil || !bytes.Equal(gotLarge, largeData) {
		t.Fatalf("failed to get large blob: %v", err)
	}

	// 4. Test recovery / fresh store pointing to same backend (testing pack discovery without .idx files)
	freshStore := NewStore(backend, 500)
	recoveredSmall, err := freshStore.GetBlob(ctx, smallSHA1)
	if err != nil || !bytes.Equal(recoveredSmall, smallData1) {
		t.Fatalf("fresh store failed to recover small blob: %v", err)
	}

	recoveredLarge, err := freshStore.GetBlob(ctx, largeSHA)
	if err != nil || !bytes.Equal(recoveredLarge, largeData) {
		t.Fatalf("fresh store failed to recover large blob: %v", err)
	}
}
