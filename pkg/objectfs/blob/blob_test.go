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
	"encoding/binary"
	"fmt"
	"io"
	"math/rand"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gke-labs/in-cluster-storage/pkg/errutil"
	"github.com/gke-labs/in-cluster-storage/pkg/scoped"
	"github.com/go-logr/logr"
)

type testMemoryObject struct {
	data    []byte
	modTime time.Time
}

type testMemoryBackend struct {
	mu      sync.RWMutex
	objects map[string]testMemoryObject
}

func newTestMemoryBackend() *testMemoryBackend {
	return &testMemoryBackend{
		objects: make(map[string]testMemoryObject),
	}
}

func (backend *testMemoryBackend) storageKey(volumeID, key string) string {
	if volumeID == "" || strings.HasPrefix(key, "volumes/") || strings.HasPrefix(key, "blobs/") {
		return key
	}
	return fmt.Sprintf("%s/%s", volumeID, key)
}

func (backend *testMemoryBackend) PutObject(ctx context.Context, volumeID, key string, stream ByteStream) (string, error) {
	backend.mu.Lock()
	defer backend.mu.Unlock()

	storageKey := backend.storageKey(volumeID, key)
	if err := stream.Rewind(); err != nil {
		return "", err
	}
	buf, err := io.ReadAll(stream)
	if err != nil {
		return "", err
	}
	backend.objects[storageKey] = testMemoryObject{
		data:    buf,
		modTime: time.Now().UTC(),
	}

	h := sha256.Sum256(buf)
	return fmt.Sprintf("%x", h), nil
}

func (backend *testMemoryBackend) GetObject(ctx context.Context, volumeID, key string, offset, length int64, w io.Writer) error {
	backend.mu.RLock()
	defer backend.mu.RUnlock()

	storageKey := backend.storageKey(volumeID, key)
	obj, ok := backend.objects[storageKey]
	if !ok {
		return fmt.Errorf("object %s not found in volume %s", key, volumeID)
	}

	total := int64(len(obj.data))
	if offset >= total {
		return nil
	}
	end := offset + length
	if length <= 0 || end > total {
		end = total
	}

	_, err := w.Write(obj.data[offset:end])
	return err
}

func (backend *testMemoryBackend) DeleteObject(ctx context.Context, volumeID, key string) error {
	backend.mu.Lock()
	defer backend.mu.Unlock()

	storageKey := backend.storageKey(volumeID, key)
	delete(backend.objects, storageKey)
	return nil
}

func (backend *testMemoryBackend) GetRedirectURL(ctx context.Context, volumeID, key string) (string, error) {
	return "", nil
}

func (backend *testMemoryBackend) ListObjects(ctx context.Context, volumeID, prefix string) ([]ObjectEntry, error) {
	backend.mu.RLock()
	defer backend.mu.RUnlock()

	fullPrefix := backend.storageKey(volumeID, prefix)
	var matches []ObjectEntry
	for storageKey, obj := range backend.objects {
		if strings.HasPrefix(storageKey, fullPrefix) {
			matches = append(matches, ObjectEntry{
				Key:          storageKey,
				Size:         int64(len(obj.data)),
				LastModified: obj.modTime,
			})
		}
	}
	sort.Slice(matches, func(firstIndex, secondIndex int) bool {
		return matches[firstIndex].Key < matches[secondIndex].Key
	})
	return matches, nil
}

func TestTestMemoryBackendListObjectsMetadata(t *testing.T) {
	ctx := t.Context()
	backend := newTestMemoryBackend()

	prefix := "test-blobs/"
	firstKey := prefix + "first.dat"
	secondKey := prefix + "second.dat"
	firstContent := []byte("first memory object")
	secondContent := []byte("second memory object longer content")

	if _, err := backend.PutObject(ctx, "", firstKey, NewByteStreamFromBytes(firstContent)); err != nil {
		t.Fatalf("PutObject %s failed: %v", firstKey, err)
	}

	if _, err := backend.PutObject(ctx, "", secondKey, NewByteStreamFromBytes(secondContent)); err != nil {
		t.Fatalf("PutObject %s failed: %v", secondKey, err)
	}

	entries, err := backend.ListObjects(ctx, "", prefix)
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
}

func TestSingleBlobEncodeDecode(t *testing.T) {
	data := []byte(strings.Repeat("This is repetitive data to test compression. ", 100))
	stream := NewByteStreamFromBytes(data)
	prep, err := EncodeBlob(t.Context(), stream, true)
	if err != nil {
		t.Fatalf("EncodeBlob failed: %v", err)
	}

	encodedStream, err := EncodeLooseBlob(prep)
	if err != nil {
		t.Fatalf("EncodeLooseBlob failed: %v", err)
	}
	encoded, err := io.ReadAll(encodedStream)
	_ = encodedStream.Close()
	if err != nil {
		t.Fatalf("failed reading encoded loose blob: %v", err)
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

	decStream, entry, err := DecodeLooseBlob(t.Context(), bytes.NewReader(encoded))
	if err != nil {
		t.Fatalf("failed to decode loose blob: %v", err)
	}

	decoded, err := io.ReadAll(decStream)
	_ = decStream.Close()
	if err != nil {
		t.Fatalf("failed reading decoded data: %v", err)
	}

	if entry.SHA256 != prep.SHA256 {
		t.Fatalf("SHA mismatch: %x vs %x", entry.SHA256, prep.SHA256)
	}

	if !bytes.Equal(decoded, data) {
		t.Fatalf("decoded data does not match original")
	}
}

func TestLargeTempFileStreaming(t *testing.T) {
	// Generate large content > 300KB to trigger tempfile creation in EncodeBlob
	largeData := []byte(strings.Repeat("0123456789abcdef", 25*1024)) // 400KB
	stream := NewByteStreamFromBytes(largeData)

	prep, err := EncodeBlob(t.Context(), stream, true)
	if err != nil {
		t.Fatalf("EncodeBlob failed: %v", err)
	}

	encodedStream, err := EncodeLooseBlob(prep)
	if err != nil {
		t.Fatalf("EncodeLooseBlob failed: %v", err)
	}

	decStream, entry, err := DecodeLooseBlob(t.Context(), encodedStream)
	if err != nil {
		t.Fatalf("DecodeLooseBlob failed: %v", err)
	}

	out, err := io.ReadAll(decStream)
	_ = decStream.Close()
	if err != nil {
		t.Fatalf("ReadAll failed: %v", err)
	}
	if !bytes.Equal(out, largeData) {
		t.Fatalf("Decoded data mismatch")
	}
	if entry.SHA256 != prep.SHA256 {
		t.Fatalf("SHA mismatch: %x vs %x", entry.SHA256, prep.SHA256)
	}
}

func TestPackfileEncodeDecode(t *testing.T) {
	origContents := [][]byte{
		[]byte("blob 1 content"),
		[]byte("blob 2 with some more text for testing"),
		[]byte(strings.Repeat("blob 3 highly repetitive repetitive data! ", 50)),
	}

	var preps []*EncodedBlob
	for _, c := range origContents {
		p, err := EncodeBlob(t.Context(), NewByteStreamFromBytes(c), true)
		if err != nil {
			t.Fatalf("EncodeBlob failed: %v", err)
		}
		preps = append(preps, p)
	}

	packData, packFile, err := writePackfileForTest(t, preps)
	if err != nil {
		t.Fatalf("WritePackfile failed: %v", err)
	}

	// Verify pack tables
	parsedPack, err := DecodePackTable(bytes.NewReader(packData))
	if err != nil {
		t.Fatalf("failed to decode pack table: %v", err)
	}
	if len(parsedPack.Entries) != len(origContents) {
		t.Fatalf("expected %d entries, got %d", len(origContents), len(parsedPack.Entries))
	}

	// For each original input, verify lookup in table and streaming extraction from pack
	for _, orig := range origContents {
		targetSHA := sha256.Sum256(orig)
		entry, found := parsedPack.Lookup(targetSHA)
		if !found {
			t.Fatalf("SHA %x not found in pack entries", targetSHA)
		}

		payloadReader := bytes.NewReader(packData[entry.DataOffset:])
		blobStream, err := parsedPack.DecodeBlob(t.Context(), payloadReader, entry)
		if err != nil {
			t.Fatalf("failed to decode blob payload: %v", err)
		}
		if blobStream.Length() != int64(len(orig)) {
			t.Fatalf("length mismatch: got %d, expected %d", blobStream.Length(), len(orig))
		}
		extracted, err := io.ReadAll(blobStream)
		_ = blobStream.Close()
		if err != nil {
			t.Fatalf("failed reading extracted payload: %v", err)
		}
		if !bytes.Equal(extracted, orig) {
			t.Fatalf("extracted data mismatch for %s: got %q, expected %q", entry.SHA256Hex(), string(extracted), string(orig))
		}
	}

	_ = packFile
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
	batch := map[string]ByteStream{
		"small1": NewByteStreamFromBytes(smallData1),
		"small2": NewByteStreamFromBytes(smallData2),
		"large":  NewByteStreamFromBytes(largeData),
	}
	if err := store.PutBlobs(ctx, batch); err != nil {
		t.Fatalf("PutBlobs failed: %v", err)
	}

	// 2. GetBlob check
	streamSmall, err := store.GetBlob(ctx, smallSHA1)
	if err != nil {
		t.Fatalf("failed to get small blob 1: %v", err)
	}
	gotSmall, _ := io.ReadAll(streamSmall)
	_ = streamSmall.Close()
	if !bytes.Equal(gotSmall, smallData1) {
		t.Fatalf("got small blob 1 mismatch: %q vs %q", string(gotSmall), string(smallData1))
	}

	streamSmall2, err := store.GetBlob(ctx, smallSHA2)
	if err != nil {
		t.Fatalf("failed to get small blob 2: %v", err)
	}
	gotSmall2, _ := io.ReadAll(streamSmall2)
	_ = streamSmall2.Close()
	if !bytes.Equal(gotSmall2, smallData2) {
		t.Fatalf("got small blob 2 mismatch: %q vs %q", string(gotSmall2), string(smallData2))
	}

	streamLarge, err := store.GetBlob(ctx, largeSHA)
	if err != nil {
		t.Fatalf("failed to get large blob: %v", err)
	}
	gotLarge, _ := io.ReadAll(streamLarge)
	_ = streamLarge.Close()
	if !bytes.Equal(gotLarge, largeData) {
		t.Fatalf("got large blob mismatch: %d vs %d", len(gotLarge), len(largeData))
	}

	// 3. Test recovery / fresh store pointing to same backend
	freshStore := NewStore(backend, 500)
	streamRecoveredSmall, err := freshStore.GetBlob(ctx, smallSHA1)
	if err != nil {
		t.Fatalf("fresh store failed to recover small blob: %v", err)
	}
	recSmall, _ := io.ReadAll(streamRecoveredSmall)
	_ = streamRecoveredSmall.Close()
	if !bytes.Equal(recSmall, smallData1) {
		t.Fatalf("recovered small blob mismatch: %q vs %q", string(recSmall), string(smallData1))
	}

	streamRecoveredLarge, err := freshStore.GetBlob(ctx, largeSHA)
	if err != nil {
		t.Fatalf("fresh store failed to recover large blob: %v", err)
	}
	recLarge, _ := io.ReadAll(streamRecoveredLarge)
	_ = streamRecoveredLarge.Close()
	if !bytes.Equal(recLarge, largeData) {
		t.Fatalf("recovered large blob mismatch: %d vs %d", len(recLarge), len(largeData))
	}

	// 4. Test ListBlobs
	shas, endOfData, err := freshStore.ListBlobs(ctx, ListBlobsOptions{})
	if err != nil {
		t.Fatalf("ListBlobs failed: %v", err)
	}
	if !endOfData {
		t.Fatalf("expected endOfData to be true for full list")
	}
	if len(shas) != 3 {
		t.Fatalf("expected 3 blobs listed, got %d (%v)", len(shas), shas)
	}
	allExpected := []string{smallSHA1, smallSHA2, largeSHA}
	sort.Strings(allExpected)
	for i, s := range shas {
		if s != allExpected[i] {
			t.Fatalf("sorted list mismatch at index %d: got %s, expected %s", i, s, allExpected[i])
		}
	}

	// 5. Test ListBlobs pagination
	page1, endOfData1, err := freshStore.ListBlobs(ctx, ListBlobsOptions{Limit: 2})
	if err != nil {
		t.Fatalf("ListBlobs page 1 failed: %v", err)
	}
	if endOfData1 {
		t.Fatalf("expected endOfData to be false for page 1")
	}
	if len(page1) != 2 || page1[0] != allExpected[0] || page1[1] != allExpected[1] {
		t.Fatalf("unexpected page 1: %v", page1)
	}

	page2, endOfData2, err := freshStore.ListBlobs(ctx, ListBlobsOptions{FromSHA: page1[1], Limit: 2})
	if err != nil {
		t.Fatalf("ListBlobs page 2 failed: %v", err)
	}
	if !endOfData2 {
		t.Fatalf("expected endOfData to be true for page 2")
	}
	if len(page2) != 1 || page2[0] != allExpected[2] {
		t.Fatalf("unexpected page 2: %v", page2)
	}

	// 6. Test ListBlobs prefix
	prefix := allExpected[0][:4]
	prefixResults, _, err := freshStore.ListBlobs(ctx, ListBlobsOptions{SHAPrefix: prefix})
	if err != nil {
		t.Fatalf("ListBlobs prefix failed: %v", err)
	}
	for _, s := range prefixResults {
		if !strings.HasPrefix(s, prefix) {
			t.Fatalf("result %s does not match prefix %s", s, prefix)
		}
	}
}

func TestManifestEncodeDecodeAndChunkBlobs(t *testing.T) {
	// 1. Create a 100KB stream and chunk it into 16KB chunks
	data := make([]byte, 100*1024)
	for i := range data {
		data[i] = byte(i % 251)
	}

	src := NewByteStreamFromBytes(data)

	// Test chunkSize 0 error
	if _, _, _, err := ChunkBlobs(t.Context(), src, 0, true); err == nil {
		t.Fatalf("expected error for chunkSize 0, got nil")
	}

	chunkSize := uint32(16 * 1024)

	chunkBlobs, manifest, manifestBlob, err := ChunkBlobs(t.Context(), src, chunkSize, true)
	if err != nil {
		t.Fatalf("ChunkBlobs failed: %v", err)
	}

	expectedChunks := (len(data) + int(chunkSize) - 1) / int(chunkSize)
	if len(chunkBlobs) != expectedChunks {
		t.Fatalf("expected %d chunks, got %d", expectedChunks, len(chunkBlobs))
	}
	if manifest.ChunkSize != chunkSize {
		t.Fatalf("expected manifest chunkSize %d, got %d", chunkSize, manifest.ChunkSize)
	}
	if manifest.TotalLength != uint64(len(data)) {
		t.Fatalf("expected manifest total length %d, got %d", len(data), manifest.TotalLength)
	}
	if manifestBlob.Encoding != EncodingChunked {
		t.Fatalf("expected manifest blob encoding %d, got %d", EncodingChunked, manifestBlob.Encoding)
	}

	// 2. Decode manifest from manifestBlob stream
	if err := manifestBlob.Stream.Rewind(); err != nil {
		t.Fatalf("rewind manifest stream failed: %v", err)
	}
	decodedManifest, err := DecodeManifest(manifestBlob.Stream)
	if err != nil {
		t.Fatalf("DecodeManifest failed: %v", err)
	}
	if decodedManifest.ChunkSize != manifest.ChunkSize {
		t.Fatalf("decoded chunkSize mismatch: %d vs %d", decodedManifest.ChunkSize, manifest.ChunkSize)
	}
	if decodedManifest.TotalLength != manifest.TotalLength {
		t.Fatalf("decoded total length mismatch: %d vs %d", decodedManifest.TotalLength, manifest.TotalLength)
	}
	if len(decodedManifest.Chunks) != len(manifest.Chunks) {
		t.Fatalf("decoded chunks count mismatch: %d vs %d", len(decodedManifest.Chunks), len(manifest.Chunks))
	}
	for i := range manifest.Chunks {
		if decodedManifest.Chunks[i] != manifest.Chunks[i] {
			t.Fatalf("chunk SHA mismatch at index %d", i)
		}
	}
}

func TestStoreChunkedManifestInPacks(t *testing.T) {
	ctx := t.Context()
	backend := newTestMemoryBackend()
	store := NewStore(backend, 0)

	// Create 3 chunks and 1 manifest
	c1Data := []byte("chunk-1-content-hello")
	c2Data := []byte("chunk-2-content-world")
	c1Sha := sha256.Sum256(c1Data)
	c2Sha := sha256.Sum256(c2Data)

	manifest := &Manifest{
		ChunkSize:   16384,
		TotalLength: uint64(len(c1Data) + len(c2Data)),
		Chunks:      [][32]byte{c1Sha, c2Sha},
	}
	manifestBlob, err := EncodeManifest(manifest)
	if err != nil {
		t.Fatalf("EncodeManifest failed: %v", err)
	}

	blobsMap := map[string]ByteStream{
		fmt.Sprintf("%x", c1Sha): NewByteStreamFromBytes(c1Data),
		fmt.Sprintf("%x", c2Sha): NewByteStreamFromBytes(c2Data),
		manifestBlob.SHA256Hex(): manifestBlob.Stream,
	}

	if err := store.PutBlobs(ctx, blobsMap); err != nil {
		t.Fatalf("PutBlobs failed: %v", err)
	}

	// Read back manifest blob from store
	mStream, err := store.GetBlob(ctx, manifestBlob.SHA256Hex())
	if err != nil {
		t.Fatalf("GetBlob on manifest failed: %v", err)
	}
	defer mStream.Close()

	decM, err := DecodeManifest(mStream)
	if err != nil {
		t.Fatalf("DecodeManifest from store blob failed: %v", err)
	}
	if decM.TotalLength != manifest.TotalLength {
		t.Fatalf("manifest totalLength mismatch: %d vs %d", decM.TotalLength, manifest.TotalLength)
	}

	// Read back chunks
	c1Stream, err := store.GetBlob(ctx, fmt.Sprintf("%x", c1Sha))
	if err != nil {
		t.Fatalf("GetBlob on c1 failed: %v", err)
	}
	defer c1Stream.Close()
	c1Read, _ := io.ReadAll(c1Stream)
	if !bytes.Equal(c1Read, c1Data) {
		t.Fatalf("c1 content mismatch: %q vs %q", string(c1Read), string(c1Data))
	}
}

func generateHighEntropyData(prefix []byte, totalSize int, seed int64) []byte {
	buf := make([]byte, totalSize)
	copy(buf, prefix)
	r := rand.New(rand.NewSource(seed))
	_, _ = r.Read(buf[len(prefix):])
	return buf
}

func TestAdaptiveCompressionByContentKind(t *testing.T) {
	tests := []struct {
		name             string
		data             []byte
		expectedEncoding uint32
	}{
		{
			name:             "text-repetitive",
			data:             []byte(strings.Repeat("Package blob implements content-addressable storage for objectfs. It compresses data adaptively. ", 500)),
			expectedEncoding: EncodingCompressed,
		},
		{
			name: "audio-incompressible-random",
			// Incompressible random bytes with MP3 sync word (0xFFFB) standing in for pre-compressed audio
			data:             generateHighEntropyData([]byte{0xFF, 0xFB, 0x90, 0x64}, 64*1024, 1001),
			expectedEncoding: EncodingRaw,
		},
		{
			name: "video-incompressible-random",
			// Incompressible random bytes with MP4 ftyp box standing in for pre-compressed video
			data:             generateHighEntropyData([]byte{0x00, 0x00, 0x00, 0x18, 'f', 't', 'y', 'p', 'm', 'p', '4', '2'}, 64*1024, 2002),
			expectedEncoding: EncodingRaw,
		},
		{
			name: "images-incompressible-random",
			// Incompressible random bytes with JPEG SOI/APP0 marker standing in for pre-compressed images
			data:             generateHighEntropyData([]byte{0xFF, 0xD8, 0xFF, 0xE0, 0x00, 0x10, 'J', 'F', 'I', 'F'}, 64*1024, 3003),
			expectedEncoding: EncodingRaw,
		},
		{
			name: "archives-incompressible-random",
			// Incompressible random bytes with ZIP header standing in for pre-compressed archives
			data:             generateHighEntropyData([]byte{'P', 'K', 0x03, 0x04, 0x14, 0x00, 0x00, 0x00, 0x08, 0x00}, 64*1024, 4004),
			expectedEncoding: EncodingRaw,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			stream := NewByteStreamFromBytes(tc.data)
			start := time.Now()
			encoded, err := EncodeBlob(t.Context(), stream, true)
			encodeElapsed := time.Since(start)
			if err != nil {
				t.Fatalf("EncodeBlob failed: %v", err)
			}
			defer func() { _ = encoded.Close() }()

			if encoded.Encoding != tc.expectedEncoding {
				t.Errorf("%s: expected encoding %d, got %d (rawLen=%d, storedLen=%d, ratio=%.2f%%)",
					tc.name, tc.expectedEncoding, encoded.Encoding, len(tc.data), encoded.StoredLength,
					float64(encoded.StoredLength)*100.0/float64(len(tc.data)))
			}

			// Verify decode
			readStart := time.Now()
			decStream, err := decodePayloadToByteStream(t.Context(), encoded.Stream, encoded.Encoding, int64(encoded.FinalLength))
			if err != nil {
				t.Fatalf("%s: decode failed: %v", tc.name, err)
			}
			defer func() { _ = decStream.Close() }()

			decoded, err := io.ReadAll(decStream)
			readElapsed := time.Since(readStart)
			if err != nil {
				t.Fatalf("%s: ReadAll failed: %v", tc.name, err)
			}
			if !bytes.Equal(decoded, tc.data) {
				t.Fatalf("%s: decoded data mismatch", tc.name)
			}

			ratio := float64(encoded.StoredLength) * 100.0 / float64(len(tc.data))
			savings := 100.0 - ratio
			t.Logf("%-10s: raw=%6d B, stored=%6d B, savings=%6.2f%%, encoding=%d, encode_cpu=%v, read_cpu=%v",
				tc.name, len(tc.data), encoded.StoredLength, savings, encoded.Encoding, encodeElapsed, readElapsed)
		})
	}
}

func BenchmarkAdaptiveCompressionByContentKind(b *testing.B) {
	kinds := []struct {
		name string
		data []byte
	}{
		{
			name: "text-repetitive",
			data: []byte(strings.Repeat("Package blob implements content-addressable storage for objectfs. It compresses data adaptively. ", 500)),
		},
		{
			name: "audio-incompressible-random",
			data: generateHighEntropyData([]byte{0xFF, 0xFB, 0x90, 0x64}, 64*1024, 1001),
		},
		{
			name: "video-incompressible-random",
			data: generateHighEntropyData([]byte{0x00, 0x00, 0x00, 0x18, 'f', 't', 'y', 'p', 'm', 'p', '4', '2'}, 64*1024, 2002),
		},
		{
			name: "images-incompressible-random",
			data: generateHighEntropyData([]byte{0xFF, 0xD8, 0xFF, 0xE0, 0x00, 0x10, 'J', 'F', 'I', 'F'}, 64*1024, 3003),
		},
		{
			name: "archives-incompressible-random",
			data: generateHighEntropyData([]byte{'P', 'K', 0x03, 0x04, 0x14, 0x00, 0x00, 0x00, 0x08, 0x00}, 64*1024, 4004),
		},
	}

	for _, k := range kinds {
		b.Run(k.name+"/encode", func(b *testing.B) {
			stream := NewByteStreamFromBytes(k.data)
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				enc, err := EncodeBlob(b.Context(), stream, true)
				if err != nil {
					b.Fatalf("EncodeBlob failed: %v", err)
				}
				_ = enc.Close()
			}
		})
		b.Run(k.name+"/read", func(b *testing.B) {
			stream := NewByteStreamFromBytes(k.data)
			enc, err := EncodeBlob(b.Context(), stream, true)
			if err != nil {
				b.Fatalf("EncodeBlob failed: %v", err)
			}
			defer func() { _ = enc.Close() }()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_ = enc.Stream.Rewind()
				dec, err := decodePayloadToByteStream(b.Context(), enc.Stream, enc.Encoding, int64(enc.FinalLength))
				if err != nil {
					b.Fatalf("decode failed: %v", err)
				}
				_, _ = io.Copy(io.Discard, dec)
				_ = dec.Close()
			}
		})
	}
}

func TestPackfileV2RoundTripUnsorted(t *testing.T) {
	origContents := [][]byte{
		[]byte("gamma payload 3 - this will sort last or middle depending on sha"),
		[]byte("alpha payload 1 - testing unsorted arrival order in pack v2"),
		[]byte("beta payload 2 - highly repetitive " + strings.Repeat("repeat ", 30)),
		[]byte("delta payload 4 - short text"),
	}

	var preps []*EncodedBlob
	for _, c := range origContents {
		p, err := EncodeBlob(t.Context(), NewByteStreamFromBytes(c), true)
		if err != nil {
			t.Fatalf("EncodeBlob failed: %v", err)
		}
		preps = append(preps, p)
	}

	// Verify that preps are indeed not sorted by SHA-256
	isSorted := true
	for i := 0; i < len(preps)-1; i++ {
		if bytes.Compare(preps[i].SHA256[:], preps[i+1].SHA256[:]) > 0 {
			isSorted = false
			break
		}
	}
	if isSorted {
		preps[0], preps[1] = preps[1], preps[0]
		origContents[0], origContents[1] = origContents[1], origContents[0]
	}

	packData, packFile, err := writePackfileForTest(t, preps)
	if err != nil {
		t.Fatalf("WritePackfile failed: %v", err)
	}
	if packFile.Version != VersionV2 {
		t.Fatalf("expected VersionV2, got %d", packFile.Version)
	}
	if packFile.Slots != DefaultPackSlots {
		t.Fatalf("expected default slots %d, got %d", DefaultPackSlots, packFile.Slots)
	}
	if packFile.Flags != PackFlagSealed {
		t.Fatalf("expected PackFlagSealed flags, got %d", packFile.Flags)
	}

	// Verify raw header bytes: Magic (0..4), Version (4), FileType (5), Slots (8..12), Used (12..16), Flags (16..20)
	if binary.BigEndian.Uint32(packData[0:4]) != Magic {
		t.Fatalf("invalid magic")
	}
	if packData[4] != VersionV2 {
		t.Fatalf("expected version %d, got %d", VersionV2, packData[4])
	}
	if packData[5] != FileTypePack {
		t.Fatalf("expected file type %d, got %d", FileTypePack, packData[5])
	}
	slots := binary.BigEndian.Uint32(packData[8:12])
	used := binary.BigEndian.Uint32(packData[12:16])
	flags := binary.BigEndian.Uint32(packData[16:20])
	if slots != DefaultPackSlots || used != uint32(len(origContents)) || flags != PackFlagSealed {
		t.Fatalf("header mismatch: slots=%d, used=%d, flags=%d", slots, used, flags)
	}

	// Verify data section layout: items were written in preps arrival order
	expectedOffset := uint32(PackV2HeaderSize) + slots*PackV2SlotSize
	for i, prep := range preps {
		expectedPayloadStart := expectedOffset + uint32(BlobItemHeaderSize)
		entry, found := packFile.Lookup(prep.SHA256)
		if !found {
			t.Fatalf("prep %d (%s) not found in packFile", i, prep.SHA256Hex())
		}
		if entry.DataOffset != expectedPayloadStart {
			t.Fatalf("prep %d data offset mismatch: got %d, expected %d (arrival order check)",
				i, entry.DataOffset, expectedPayloadStart)
		}
		expectedOffset = expectedPayloadStart + entry.StoredLength
	}

	// Decode table
	parsedPack, err := DecodePackTable(bytes.NewReader(packData))
	if err != nil {
		t.Fatalf("DecodePackTable failed: %v", err)
	}
	if len(parsedPack.Entries) != len(origContents) {
		t.Fatalf("expected %d entries, got %d", len(origContents), len(parsedPack.Entries))
	}

	// Verify entries in table are sorted by SHA-256
	for i := 0; i < len(parsedPack.Entries)-1; i++ {
		if bytes.Compare(parsedPack.Entries[i].SHA256[:], parsedPack.Entries[i+1].SHA256[:]) >= 0 {
			t.Fatalf("entries not strictly sorted by SHA-256 at index %d", i)
		}
	}

	// Decode every blob
	for i, orig := range origContents {
		targetSHA := sha256.Sum256(orig)
		entry, found := parsedPack.Lookup(targetSHA)
		if !found {
			t.Fatalf("blob %d (%x) not found via Lookup", i, targetSHA)
		}
		r := bytes.NewReader(packData[entry.DataOffset : entry.DataOffset+entry.StoredLength])
		stream, err := parsedPack.DecodeBlob(t.Context(), r, entry)
		if err != nil {
			t.Fatalf("DecodeBlob failed: %v", err)
		}
		got, err := io.ReadAll(stream)
		_ = stream.Close()
		if err != nil {
			t.Fatalf("ReadAll failed: %v", err)
		}
		if !bytes.Equal(got, orig) {
			t.Fatalf("blob %d content mismatch: got %q, expected %q", i, string(got), string(orig))
		}
	}
}

func TestPackfileV2BinarySearchLessSlots(t *testing.T) {
	// N = 4, S = 16 (so 12 zeroed slots)
	origContents := [][]byte{
		[]byte("item-A-data"),
		[]byte("item-B-data"),
		[]byte("item-C-data"),
		[]byte("item-D-data"),
	}

	var preps []*EncodedBlob
	for _, c := range origContents {
		p, err := EncodeBlob(t.Context(), NewByteStreamFromBytes(c), true)
		if err != nil {
			t.Fatalf("EncodeBlob failed: %v", err)
		}
		preps = append(preps, p)
	}

	packBytes, pack, err := writePackfileWithSlotsForTest(t, preps, 16)
	if err != nil {
		t.Fatalf("WritePackfileWithSlots failed: %v", err)
	}
	if pack.Slots != 16 || len(pack.Entries) != 4 {
		t.Fatalf("expected 16 slots, 4 entries; got slots=%d, entries=%d", pack.Slots, len(pack.Entries))
	}

	parsed, err := DecodePackTable(bytes.NewReader(packBytes))
	if err != nil {
		t.Fatalf("DecodePackTable failed: %v", err)
	}
	if parsed.Slots != 16 || len(parsed.Entries) != 4 {
		t.Fatalf("parsed pack slots=%d, entries=%d", parsed.Slots, len(parsed.Entries))
	}

	// 1. All valid SHAs must be found
	for _, p := range preps {
		entry, found := parsed.Lookup(p.SHA256)
		if !found {
			t.Fatalf("expected SHA %s to be found", p.SHA256Hex())
		}
		if entry.SHA256 != p.SHA256 {
			t.Fatalf("entry SHA mismatch")
		}
	}

	// 2. Absent zero-hash must NOT be found (ensuring zeroed slots in table are ignored)
	var zeroSHA [32]byte
	if _, found := parsed.Lookup(zeroSHA); found {
		t.Fatalf("zero-SHA should not be found in active entries")
	}

	// 3. Absent hash sorting before lowest
	beforeSHA := parsed.Entries[0].SHA256
	for i := 0; i < 32; i++ {
		if beforeSHA[i] > 0 {
			beforeSHA[i]--
			break
		}
	}
	if beforeSHA != parsed.Entries[0].SHA256 {
		if _, found := parsed.Lookup(beforeSHA); found {
			t.Fatalf("hash before lowest should not be found")
		}
	}

	// 4. Absent hash sorting after highest
	afterSHA := parsed.Entries[len(parsed.Entries)-1].SHA256
	for i := 0; i < 32; i++ {
		if afterSHA[i] < 0xFF {
			afterSHA[i]++
			break
		}
	}
	if afterSHA != parsed.Entries[len(parsed.Entries)-1].SHA256 {
		if _, found := parsed.Lookup(afterSHA); found {
			t.Fatalf("hash after highest should not be found")
		}
	}
}

func TestPackfileV2FullSlots(t *testing.T) {
	// S = 10, N = 10 (N == S)
	slotCount := uint32(10)
	var preps []*EncodedBlob
	var origs [][]byte
	for i := 0; i < int(slotCount); i++ {
		data := []byte(fmt.Sprintf("full-slot-blob-%d-payload", i))
		origs = append(origs, data)
		p, err := EncodeBlob(t.Context(), NewByteStreamFromBytes(data), true)
		if err != nil {
			t.Fatalf("EncodeBlob failed: %v", err)
		}
		preps = append(preps, p)
	}

	packBytes, pack, err := writePackfileWithSlotsForTest(t, preps, slotCount)
	if err != nil {
		t.Fatalf("WritePackfileWithSlots failed: %v", err)
	}
	if pack.Slots != slotCount || len(pack.Entries) != int(slotCount) {
		t.Fatalf("expected slots=%d, entries=%d; got slots=%d, entries=%d",
			slotCount, slotCount, pack.Slots, len(pack.Entries))
	}
	if pack.Flags&PackFlagSealed == 0 {
		t.Fatalf("expected sealed bit set in flags")
	}

	parsed, err := DecodePackTable(bytes.NewReader(packBytes))
	if err != nil {
		t.Fatalf("DecodePackTable failed: %v", err)
	}
	if parsed.Slots != slotCount || len(parsed.Entries) != int(slotCount) {
		t.Fatalf("parsed pack mismatch: slots=%d, entries=%d", parsed.Slots, len(parsed.Entries))
	}

	for i, orig := range origs {
		targetSHA := sha256.Sum256(orig)
		entry, found := parsed.Lookup(targetSHA)
		if !found {
			t.Fatalf("item %d (%x) not found in full pack", i, targetSHA)
		}
		r := bytes.NewReader(packBytes[entry.DataOffset : entry.DataOffset+entry.StoredLength])
		stream, err := parsed.DecodeBlob(t.Context(), r, entry)
		if err != nil {
			t.Fatalf("DecodeBlob failed: %v", err)
		}
		got, err := io.ReadAll(stream)
		_ = stream.Close()
		if err != nil {
			t.Fatalf("ReadAll failed: %v", err)
		}
		if !bytes.Equal(got, orig) {
			t.Fatalf("item %d mismatch: got %q, expected %q", i, string(got), string(orig))
		}
	}
}

func TestPackfileV1FixtureRead(t *testing.T) {
	fixturePath := filepath.Join("testdata", "v1_fixture.pack")
	fixtureData, err := os.ReadFile(fixturePath)
	if err != nil {
		t.Fatalf("failed reading checked-in v1 fixture %s: %v", fixturePath, err)
	}

	// Verify fixture header is VersionV1
	hdr, err := ParseFileHeader(fixtureData[:8])
	if err != nil {
		t.Fatalf("failed to parse fixture header: %v", err)
	}
	if hdr.Version != VersionV1 {
		t.Fatalf("expected fixture version 1, got %d", hdr.Version)
	}
	if hdr.FileType != FileTypePack {
		t.Fatalf("expected FileTypePack, got %d", hdr.FileType)
	}

	parsed, err := DecodePackTable(bytes.NewReader(fixtureData))
	if err != nil {
		t.Fatalf("DecodePackTable failed on v1 fixture: %v", err)
	}
	if parsed.Version != VersionV1 {
		t.Fatalf("expected parsed VersionV1, got %d", parsed.Version)
	}
	if len(parsed.Entries) != 3 {
		t.Fatalf("expected 3 entries in v1 fixture, got %d", len(parsed.Entries))
	}

	expectedBlobs := [][]byte{
		[]byte("fixture-v1-blob-1-plain-text"),
		[]byte("fixture-v1-blob-2-repetitive-data-repetitive-data-repetitive-data-repetitive-data"),
		[]byte("fixture-v1-blob-3-binary-\x00\x01\x02\x03\x04\x05\x06\x07\x08\x09"),
	}

	for i, expected := range expectedBlobs {
		targetSHA := sha256.Sum256(expected)
		entry, found := parsed.Lookup(targetSHA)
		if !found {
			t.Fatalf("blob %d (%x) not found in v1 fixture", i, targetSHA)
		}
		r := bytes.NewReader(fixtureData[entry.DataOffset : entry.DataOffset+entry.StoredLength])
		stream, err := parsed.DecodeBlob(t.Context(), r, entry)
		if err != nil {
			t.Fatalf("DecodeBlob failed on fixture blob %d: %v", i, err)
		}
		got, err := io.ReadAll(stream)
		_ = stream.Close()
		if err != nil {
			t.Fatalf("ReadAll failed on fixture blob %d: %v", i, err)
		}
		if !bytes.Equal(got, expected) {
			t.Fatalf("fixture blob %d mismatch: got %q, expected %q", i, string(got), string(expected))
		}
	}

	// Also verify reading the fixture via Store.GetBlob and RefreshIndexes
	ctx := t.Context()
	backend := newTestMemoryBackend()
	packKey := "blobs/fixture-12345-3.pack"
	if _, err := backend.PutObject(ctx, "", packKey, NewByteStreamFromBytes(fixtureData)); err != nil {
		t.Fatalf("PutObject failed: %v", err)
	}

	store := NewStore(backend, 500)
	if err := store.RefreshIndexes(ctx); err != nil {
		t.Fatalf("RefreshIndexes failed on v1 fixture: %v", err)
	}

	for i, expected := range expectedBlobs {
		shaHex := fmt.Sprintf("%x", sha256.Sum256(expected))
		stream, err := store.GetBlob(ctx, shaHex)
		if err != nil {
			t.Fatalf("store.GetBlob on v1 fixture blob %d (%s) failed: %v", i, shaHex, err)
		}
		got, err := io.ReadAll(stream)
		_ = stream.Close()
		if err != nil {
			t.Fatalf("ReadAll failed: %v", err)
		}
		if !bytes.Equal(got, expected) {
			t.Fatalf("store blob %d content mismatch", i)
		}
	}
}

func TestMixedStoreV1V2(t *testing.T) {
	ctx := t.Context()
	backend := newTestMemoryBackend()

	v1Data1 := []byte("v1-blob-one-mixed-store-test")
	v1Data2 := []byte("v1-blob-two-repetitive-" + strings.Repeat("hello ", 20))
	v1SHA1 := fmt.Sprintf("%x", sha256.Sum256(v1Data1))
	v1SHA2 := fmt.Sprintf("%x", sha256.Sum256(v1Data2))

	v2Data1 := []byte("v2-blob-one-mixed-store-test")
	v2Data2 := []byte("v2-blob-two-repetitive-" + strings.Repeat("world ", 20))
	v2SHA1 := fmt.Sprintf("%x", sha256.Sum256(v2Data1))
	v2SHA2 := fmt.Sprintf("%x", sha256.Sum256(v2Data2))

	// Write v1 packfile
	prepV1_1, err := EncodeBlob(t.Context(), NewByteStreamFromBytes(v1Data1), true)
	if err != nil {
		t.Fatalf("EncodeBlob failed: %v", err)
	}
	prepV1_2, err := EncodeBlob(t.Context(), NewByteStreamFromBytes(v1Data2), true)
	if err != nil {
		t.Fatalf("EncodeBlob failed: %v", err)
	}
	var v1Buf bytes.Buffer
	if _, err := WritePackfileV1(&v1Buf, []*EncodedBlob{prepV1_1, prepV1_2}); err != nil {
		t.Fatalf("WritePackfileV1 failed: %v", err)
	}
	if _, err := backend.PutObject(ctx, "", "blobs/pack-v1-test-2.pack", NewByteStreamFromBytes(v1Buf.Bytes())); err != nil {
		t.Fatalf("PutObject v1 pack failed: %v", err)
	}

	// Write v2 packfile
	prepV2_1, err := EncodeBlob(t.Context(), NewByteStreamFromBytes(v2Data1), true)
	if err != nil {
		t.Fatalf("EncodeBlob failed: %v", err)
	}
	prepV2_2, err := EncodeBlob(t.Context(), NewByteStreamFromBytes(v2Data2), true)
	if err != nil {
		t.Fatalf("EncodeBlob failed: %v", err)
	}
	v2Data, _, err := writePackfileForTest(t, []*EncodedBlob{prepV2_1, prepV2_2})
	if err != nil {
		t.Fatalf("WritePackfile v2 failed: %v", err)
	}
	if _, err := backend.PutObject(ctx, "", "blobs/pack-v2-test-2.pack", NewByteStreamFromBytes(v2Data)); err != nil {
		t.Fatalf("PutObject v2 pack failed: %v", err)
	}

	// Fresh store discovers both v1 and v2 packfiles seamlessly
	store := NewStore(backend, 500)
	if err := store.RefreshIndexes(ctx); err != nil {
		t.Fatalf("RefreshIndexes failed: %v", err)
	}

	// Verify GetBlob works for all 4 blobs
	expectedMap := map[string][]byte{
		v1SHA1: v1Data1,
		v1SHA2: v1Data2,
		v2SHA1: v2Data1,
		v2SHA2: v2Data2,
	}

	for shaHex, expected := range expectedMap {
		stream, err := store.GetBlob(ctx, shaHex)
		if err != nil {
			t.Fatalf("GetBlob failed for %s: %v", shaHex, err)
		}
		got, err := io.ReadAll(stream)
		_ = stream.Close()
		if err != nil {
			t.Fatalf("ReadAll failed for %s: %v", shaHex, err)
		}
		if !bytes.Equal(got, expected) {
			t.Fatalf("mismatch for blob %s: got %q, expected %q", shaHex, string(got), string(expected))
		}
	}

	// Verify ListBlobs lists all 4 blobs sorted
	allSHAs, endOfData, err := store.ListBlobs(ctx, ListBlobsOptions{})
	if err != nil {
		t.Fatalf("ListBlobs failed: %v", err)
	}
	if !endOfData {
		t.Fatalf("expected endOfData = true")
	}
	if len(allSHAs) != 4 {
		t.Fatalf("expected 4 blobs listed, got %d", len(allSHAs))
	}
	expectedSHAList := []string{v1SHA1, v1SHA2, v2SHA1, v2SHA2}
	sort.Strings(expectedSHAList)
	for i, s := range allSHAs {
		if s != expectedSHAList[i] {
			t.Fatalf("sorted list mismatch at index %d: got %s, expected %s", i, s, expectedSHAList[i])
		}
	}
}

func TestPackfileV2CorruptEntry_Raw(t *testing.T) {
	ctx := t.Context()
	backend := newTestMemoryBackend()

	// 4096 bytes of incompressible raw data
	rawData := generateHighEntropyData([]byte("raw-head"), 4096, 777)
	prep, err := EncodeBlob(t.Context(), NewByteStreamFromBytes(rawData), true)
	if err != nil {
		t.Fatalf("EncodeBlob failed: %v", err)
	}
	if prep.Encoding != EncodingRaw {
		t.Fatalf("expected raw encoding for random data, got %d", prep.Encoding)
	}

	packBytes, _, err := writePackfileWithSlotsForTest(t, []*EncodedBlob{prep}, 4)
	if err != nil {
		t.Fatalf("WritePackfile failed: %v", err)
	}

	// Slot 0 starts at offset 32 (PackV2HeaderSize).
	// Slot layout: SHA256 (0..32), Start (32..36), StoredLength (36..40)
	// Mutate Start to point past EOF
	slotStartOff := PackV2HeaderSize + 32
	badOffset := uint32(len(packBytes) + 1000)
	binary.BigEndian.PutUint32(packBytes[slotStartOff:slotStartOff+4], badOffset)

	packKey := "blobs/corrupt-raw-1.pack"
	if _, err := backend.PutObject(ctx, "", packKey, NewByteStreamFromBytes(packBytes)); err != nil {
		t.Fatalf("PutObject failed: %v", err)
	}

	store := NewStore(backend, 500)
	if err := store.RefreshIndexes(ctx); err != nil {
		t.Fatalf("RefreshIndexes failed: %v", err)
	}

	shaHex := prep.SHA256Hex()
	stream, err := store.GetBlob(ctx, shaHex)
	if err == nil {
		if stream != nil {
			_ = stream.Close()
		}
		t.Fatalf("expected error for raw entry with start past EOF, got nil")
	}

	errStr := err.Error()
	// Verify error names pack, SHA-256, and offset
	if !strings.Contains(errStr, packKey) {
		t.Errorf("error %q should contain pack key %q", errStr, packKey)
	}
	if !strings.Contains(errStr, shaHex) {
		t.Errorf("error %q should contain sha256 %q", errStr, shaHex)
	}
	if !strings.Contains(errStr, fmt.Sprintf("%d", badOffset)) {
		t.Errorf("error %q should contain offset %d", errStr, badOffset)
	}

	// Also test DecodeBlob directly with an empty reader (simulating read past EOF)
	parsedPack, err := DecodePackTable(bytes.NewReader(packBytes))
	if err != nil {
		t.Fatalf("DecodePackTable failed: %v", err)
	}
	entry, found := parsedPack.Lookup(prep.SHA256)
	if !found {
		t.Fatalf("entry not found")
	}
	emptyReader := bytes.NewReader(nil)
	decStream, err := parsedPack.DecodeBlob(t.Context(), emptyReader, entry)
	if err == nil {
		if decStream != nil {
			_ = decStream.Close()
		}
		t.Fatalf("expected error from DecodeBlob on empty reader, got nil")
	}
}

func TestPackfileV2CorruptEntry_Compressed(t *testing.T) {
	ctx := t.Context()
	backend := newTestMemoryBackend()

	// Compressible repetitive data
	compData := []byte(strings.Repeat("compressed-repetitive-data-chunk-", 150))
	prep, err := EncodeBlob(t.Context(), NewByteStreamFromBytes(compData), true)
	if err != nil {
		t.Fatalf("EncodeBlob failed: %v", err)
	}
	if prep.Encoding != EncodingCompressed {
		t.Fatalf("expected compressed encoding, got %d", prep.Encoding)
	}

	packBytes, _, err := writePackfileWithSlotsForTest(t, []*EncodedBlob{prep}, 4)
	if err != nil {
		t.Fatalf("WritePackfile failed: %v", err)
	}

	slotStartOff := PackV2HeaderSize + 32
	badOffset := uint32(len(packBytes) + 5000)
	binary.BigEndian.PutUint32(packBytes[slotStartOff:slotStartOff+4], badOffset)

	packKey := "blobs/corrupt-comp-1.pack"
	if _, err := backend.PutObject(ctx, "", packKey, NewByteStreamFromBytes(packBytes)); err != nil {
		t.Fatalf("PutObject failed: %v", err)
	}

	store := NewStore(backend, 500)
	if err := store.RefreshIndexes(ctx); err != nil {
		t.Fatalf("RefreshIndexes failed: %v", err)
	}

	shaHex := prep.SHA256Hex()
	stream, err := store.GetBlob(ctx, shaHex)
	if err == nil {
		if stream != nil {
			_ = stream.Close()
		}
		t.Fatalf("expected error for compressed entry with start past EOF, got nil")
	}

	errStr := err.Error()
	if !strings.Contains(errStr, packKey) {
		t.Errorf("error %q should contain pack key %q", errStr, packKey)
	}
	if !strings.Contains(errStr, shaHex) {
		t.Errorf("error %q should contain sha256 %q", errStr, shaHex)
	}
	if !strings.Contains(errStr, fmt.Sprintf("%d", badOffset)) {
		t.Errorf("error %q should contain offset %d", errStr, badOffset)
	}
}

func TestPackfileV2CorruptEntry_ShortRangeRead(t *testing.T) {
	ctx := t.Context()
	backend := newTestMemoryBackend()

	data := []byte("short-range-read-test-payload-data")
	prep, err := EncodeBlob(t.Context(), NewByteStreamFromBytes(data), true)
	if err != nil {
		t.Fatalf("EncodeBlob failed: %v", err)
	}

	packBytes, _, err := writePackfileForTest(t, []*EncodedBlob{prep})
	if err != nil {
		t.Fatalf("WritePackfile failed: %v", err)
	}

	truncatedPack := packBytes[:len(packBytes)-5]

	packKey := "blobs/truncated-pack-1.pack"
	if _, err := backend.PutObject(ctx, "", packKey, NewByteStreamFromBytes(truncatedPack)); err != nil {
		t.Fatalf("PutObject failed: %v", err)
	}

	store := NewStore(backend, 500)
	if err := store.RefreshIndexes(ctx); err != nil {
		t.Fatalf("RefreshIndexes failed: %v", err)
	}

	shaHex := prep.SHA256Hex()
	stream, err := store.GetBlob(ctx, shaHex)
	if err == nil {
		if stream != nil {
			_ = stream.Close()
		}
		t.Fatalf("expected error on truncated range read, got nil")
	}
	errStr := err.Error()
	if !strings.Contains(errStr, packKey) || !strings.Contains(errStr, shaHex) {
		t.Errorf("error %q should contain pack key and sha256", errStr)
	}
}

func TestPackfileV2CorruptEntry_ChecksumMismatch(t *testing.T) {
	ctx := t.Context()
	backend := newTestMemoryBackend()

	data := []byte("checksum-mismatch-test-payload-data")
	prep, err := EncodeBlob(t.Context(), NewByteStreamFromBytes(data), false)
	if err != nil {
		t.Fatalf("EncodeBlob failed: %v", err)
	}

	packBytes, pack, err := writePackfileForTest(t, []*EncodedBlob{prep})
	if err != nil {
		t.Fatalf("WritePackfile failed: %v", err)
	}

	// Corrupt 1 byte in payload
	entry := pack.Entries[0]
	packBytes[entry.DataOffset] ^= 0xFF

	packKey := "blobs/bitrot-pack-1.pack"
	if _, err := backend.PutObject(ctx, "", packKey, NewByteStreamFromBytes(packBytes)); err != nil {
		t.Fatalf("PutObject failed: %v", err)
	}

	store := NewStore(backend, 500)
	if err := store.RefreshIndexes(ctx); err != nil {
		t.Fatalf("RefreshIndexes failed: %v", err)
	}

	shaHex := prep.SHA256Hex()
	stream, err := store.GetBlob(ctx, shaHex)
	if err == nil {
		if stream != nil {
			_ = stream.Close()
		}
		t.Fatalf("expected checksum mismatch error, got nil")
	}
	errStr := err.Error()
	if !strings.Contains(errStr, "checksum mismatch") {
		t.Errorf("expected checksum mismatch in error %q", errStr)
	}
}

func TestWritePackfile_ByteIdenticalToBatch(t *testing.T) {
	fixtureData, err := os.ReadFile(filepath.Join("testdata", "v2_fixture.pack"))
	if err != nil {
		t.Fatalf("failed reading checked-in v2 fixture: %v", err)
	}

	rawBlobs := [][]byte{
		[]byte("fixture-v2-blob-1-plain-text"),
		[]byte("fixture-v2-blob-2-repetitive-data-repetitive-data-repetitive-data-repetitive-data"),
		[]byte("fixture-v2-blob-3-binary-\x00\x01\x02\x03\x04\x05\x06\x07\x08\x09"),
	}
	blobs := make([]*EncodedBlob, len(rawBlobs))
	for i, raw := range rawBlobs {
		encoded, err := EncodeBlob(t.Context(), NewByteStreamFromBytes(raw), true)
		if err != nil {
			t.Fatalf("EncodeBlob %d: %v", i, err)
		}
		t.Cleanup(func() {
			if err := encoded.Close(); err != nil {
				t.Errorf("Close encoded blob: %v", err)
			}
		})
		blobs[i] = encoded
	}

	packData, _, err := writePackfileWithSlotsForTest(t, blobs, 8)
	if err != nil {
		t.Fatalf("WritePackfileWithSlots: %v", err)
	}

	if !bytes.Equal(packData, fixtureData) {
		t.Fatalf("WritePackfile output does not match golden fixture v2_fixture.pack")
	}
}

func TestIncrementalPack_LookupAndReadBlob(t *testing.T) {
	packPath := filepath.Join(t.TempDir(), "lookup-read.pack")
	pack, err := NewIncrementalPack(t.Context(), packPath, 4)
	if err != nil {
		t.Fatalf("NewIncrementalPack: %v", err)
	}
	defer func() {
		if err := pack.Close(); err != nil {
			t.Fatalf("Close failed: %v", err)
		}
	}()

	chunk1, chunk2 := []byte("payload-one"), []byte("payload-two")
	entry1, err := pack.Append(t.Context(), chunk1)
	if err != nil {
		t.Fatalf("Append 1: %v", err)
	}
	entry2, err := pack.Append(t.Context(), chunk2)
	if err != nil {
		t.Fatalf("Append 2: %v", err)
	}

	for _, testCase := range []struct {
		entry BlobEntry
		want  []byte
	}{{entry1, chunk1}, {entry2, chunk2}} {
		if got, ok := pack.Lookup(testCase.entry.SHA256); !ok || got != testCase.entry {
			t.Fatalf("Lookup open: got %v, want %v", got, testCase.entry)
		}
		stream, err := pack.ReadBlob(t.Context(), testCase.entry)
		if err != nil {
			t.Fatalf("ReadBlob open: %v", err)
		}
		data, err := io.ReadAll(stream)
		closeErr := stream.Close()
		if err != nil || closeErr != nil || !bytes.Equal(data, testCase.want) {
			t.Fatalf("ReadBlob open: got %q, want %q, readErr=%v, closeErr=%v", data, testCase.want, err, closeErr)
		}
	}
	if _, ok := pack.Lookup([32]byte{0xff}); ok {
		t.Fatalf("expected missing lookup false")
	}

	if err := pack.Seal(t.Context()); err != nil {
		t.Fatalf("Seal failed: %v", err)
	}
	if got, ok := pack.Lookup(entry2.SHA256); !ok || got != entry2 {
		t.Fatalf("Lookup sealed: got %v, want %v", got, entry2)
	}
	stream, err := pack.ReadBlob(t.Context(), entry2)
	if err != nil {
		t.Fatalf("ReadBlob sealed: %v", err)
	}
	data, err := io.ReadAll(stream)
	closeErr := stream.Close()
	if err != nil || closeErr != nil || !bytes.Equal(data, chunk2) {
		t.Fatalf("ReadBlob sealed: got %q, want %q, readErr=%v, closeErr=%v", data, chunk2, err, closeErr)
	}
}

func TestIncrementalPack_SealWriteFailureLeavesPackAppendable(t *testing.T) {
	dir := t.TempDir()
	packPath := filepath.Join(dir, "pack.pack")
	pack, err := NewIncrementalPack(t.Context(), packPath, 10)
	if err != nil {
		t.Fatalf("NewIncrementalPack failed: %v", err)
	}
	t.Cleanup(func() {
		if closeErr := pack.Close(); closeErr != nil {
			t.Errorf("pack.Close failed: %v", closeErr)
		}
	})

	chunk1 := []byte("first chunk data")
	entry1, err := pack.Append(t.Context(), chunk1)
	if err != nil {
		t.Fatalf("Append 1 failed: %v", err)
	}

	// Swap the underlying file with a read-only descriptor to cause WriteAt in Seal to fail.
	rwFile := pack.file
	roFile, err := os.Open(packPath)
	if err != nil {
		t.Fatalf("os.Open read-only failed: %v", err)
	}
	pack.file = roFile

	if err := pack.Seal(t.Context()); err == nil {
		t.Fatalf("expected Seal to fail on read-only file descriptor")
	}

	// Restore the writable file descriptor.
	if closeErr := roFile.Close(); closeErr != nil {
		t.Fatalf("roFile.Close failed: %v", closeErr)
	}
	pack.file = rwFile

	// Verify that the pack was NOT marked sealed: Append of a new chunk must succeed.
	chunk2 := []byte("second chunk data")
	entry2, err := pack.Append(t.Context(), chunk2)
	if err != nil {
		t.Fatalf("Append 2 failed after seal failure: %v", err)
	}

	// Retry Seal with the writable descriptor; should succeed.
	if err := pack.Seal(t.Context()); err != nil {
		t.Fatalf("Seal retry failed: %v", err)
	}

	// Once sealed, Append must be refused with "incremental pack is sealed".
	if _, err := pack.Append(t.Context(), []byte("third chunk")); err == nil || !strings.Contains(err.Error(), "sealed") {
		t.Fatalf("expected Append on sealed pack to fail with sealed error, got: %v", err)
	}

	// Verify entries exist and can be read back.
	for _, testCase := range []struct {
		entry BlobEntry
		want  []byte
	}{{entry1, chunk1}, {entry2, chunk2}} {
		stream, err := pack.ReadBlob(t.Context(), testCase.entry)
		if err != nil {
			t.Fatalf("ReadBlob failed: %v", err)
		}
		data, err := io.ReadAll(stream)
		closeErr := stream.Close()
		if err != nil || closeErr != nil || !bytes.Equal(data, testCase.want) {
			t.Fatalf("ReadBlob mismatch: got %q, want %q, readErr=%v, closeErr=%v", data, testCase.want, err, closeErr)
		}
	}
}

func writePackfileWithFnForTest(t *testing.T, writeFn func(*os.File) (*PackFile, error)) ([]byte, *PackFile, error) {
	t.Helper()
	file, err := os.Create(filepath.Join(t.TempDir(), "test.pack"))
	if err != nil {
		return nil, nil, err
	}
	packFile, err := writeFn(file)
	if err != nil {
		errutil.CloseAndLog(file, "test packfile")
		return nil, nil, err
	}
	if err := file.Close(); err != nil {
		return nil, nil, err
	}
	data, err := os.ReadFile(file.Name())
	if err != nil {
		return nil, nil, err
	}
	return data, packFile, nil
}

func writePackfileWithSlotsForTest(t *testing.T, inputs []*EncodedBlob, slots uint32) ([]byte, *PackFile, error) {
	t.Helper()
	return writePackfileWithFnForTest(t, func(file *os.File) (*PackFile, error) {
		return WritePackfileWithSlots(t.Context(), file, inputs, slots)
	})
}

func writePackfileForTest(t *testing.T, inputs []*EncodedBlob) ([]byte, *PackFile, error) {
	t.Helper()
	return writePackfileWithFnForTest(t, func(file *os.File) (*PackFile, error) {
		return WritePackfile(t.Context(), file, inputs)
	})
}

func TestIncrementalPack_Close(t *testing.T) {
	// 1. Calling Close on a nil *IncrementalPack returns nil without panic.
	var nilPack *IncrementalPack
	if err := nilPack.Close(); err != nil {
		t.Fatalf("nil pack Close: %v", err)
	}

	// 2. Normal close clears p.file and subsequent close returns nil.
	tempPath := filepath.Join(t.TempDir(), "test-close.pack")
	pack, err := NewIncrementalPack(t.Context(), tempPath, 4)
	if err != nil {
		t.Fatalf("NewIncrementalPack: %v", err)
	}
	if err := pack.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}
	if pack.file != nil {
		t.Fatalf("expected pack.file to be nil after successful close")
	}
	if err := pack.Close(); err != nil {
		t.Fatalf("second Close failed: %v", err)
	}

	// 3. Failed file close returns error and leaves p.file set so descriptor is not leaked.
	tempPath2 := filepath.Join(t.TempDir(), "test-close-fail.pack")
	pack2, err := NewIncrementalPack(t.Context(), tempPath2, 4)
	if err != nil {
		t.Fatalf("NewIncrementalPack: %v", err)
	}
	if err := pack2.file.Close(); err != nil {
		t.Fatalf("direct file Close: %v", err)
	}
	if err := pack2.Close(); err == nil {
		t.Fatalf("expected error closing already-closed file, got nil")
	}
	if pack2.file == nil {
		t.Fatalf("expected pack2.file to remain non-nil on failed close")
	}
}

func TestNewIncrementalPackFromFile(t *testing.T) {
	tempPath := filepath.Join(t.TempDir(), "from-file.pack")
	file, err := os.Create(tempPath)
	if err != nil {
		t.Fatalf("os.Create: %v", err)
	}

	pack, err := NewIncrementalPackFromFile(file, 4)
	if err != nil {
		errutil.CloseAndLog(file, "temp packfile")
		t.Fatalf("NewIncrementalPackFromFile: %v", err)
	}
	entry, err := pack.Append(t.Context(), []byte("hello-from-file"))
	if err != nil {
		t.Fatalf("Append: %v", err)
	}
	if err := pack.Seal(t.Context()); err != nil {
		t.Fatalf("Seal: %v", err)
	}
	if got, ok := pack.Lookup(entry.SHA256); !ok || got.SHA256 != entry.SHA256 {
		t.Fatalf("Lookup mismatch")
	}
	if err := pack.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	// Read the file back through DecodePackTable to verify absolute offset structure
	data, err := os.ReadFile(tempPath)
	if err != nil {
		t.Fatalf("os.ReadFile: %v", err)
	}
	parsed, err := DecodePackTable(bytes.NewReader(data))
	if err != nil {
		t.Fatalf("DecodePackTable: %v", err)
	}
	if parsed.Slots != 4 || len(parsed.Entries) != 1 {
		t.Fatalf("parsed pack mismatch: slots=%d, entries=%d", parsed.Slots, len(parsed.Entries))
	}
	if foundEntry, ok := parsed.Lookup(entry.SHA256); !ok || foundEntry.SHA256 != entry.SHA256 {
		t.Fatalf("parsed lookup mismatch")
	}
}

type blobLogCaptureSink struct {
	mu            sync.Mutex
	messages      []string
	keysAndValues [][]any
	entries       []map[string]any
	parent        *blobLogCaptureSink
	values        map[string]any
}

func (s *blobLogCaptureSink) Init(info logr.RuntimeInfo) {}
func (s *blobLogCaptureSink) Enabled(level int) bool     { return true }
func (s *blobLogCaptureSink) Info(level int, message string, keysAndValues ...any) {
	root := s
	for root.parent != nil {
		root = root.parent
	}
	root.mu.Lock()
	defer root.mu.Unlock()
	entry := make(map[string]any, len(s.values)+len(keysAndValues)/2)
	for key, value := range s.values {
		entry[key] = value
	}
	for index := 0; index < len(keysAndValues)-1; index += 2 {
		if key, ok := keysAndValues[index].(string); ok {
			entry[key] = keysAndValues[index+1]
		}
	}
	root.messages = append(root.messages, message)
	root.keysAndValues = append(root.keysAndValues, keysAndValues)
	root.entries = append(root.entries, entry)
}
func (s *blobLogCaptureSink) Error(err error, message string, keysAndValues ...any) {}
func (s *blobLogCaptureSink) WithName(name string) logr.LogSink                     { return s }
func (s *blobLogCaptureSink) WithValues(keysAndValues ...any) logr.LogSink {
	s.mu.Lock()
	defer s.mu.Unlock()
	newValues := make(map[string]any, len(s.values)+len(keysAndValues)/2)
	for key, value := range s.values {
		newValues[key] = value
	}
	for index := 0; index < len(keysAndValues)-1; index += 2 {
		if key, ok := keysAndValues[index].(string); ok {
			newValues[key] = keysAndValues[index+1]
		}
	}
	root := s
	for root.parent != nil {
		root = root.parent
	}
	return &blobLogCaptureSink{parent: root, values: newValues}
}

func TestWritePackfile_ScopedLog(t *testing.T) {
	sink := &blobLogCaptureSink{}
	logger := logr.New(sink)
	ctx := scoped.WithLog(t.Context(), logger)

	tempFile, err := os.CreateTemp(t.TempDir(), "test-log-pack-*")
	if err != nil {
		t.Fatalf("os.CreateTemp: %v", err)
	}
	defer errutil.CloseAndLog(tempFile, "tempFile")

	var inputs []*EncodedBlob
	for index := 0; index < int(DefaultPackSlots)+1; index++ {
		data := []byte(fmt.Sprintf("chunk-%d", index))
		encoded, err := EncodeBlob(ctx, NewByteStreamFromBytes(data), false)
		if err != nil {
			t.Fatalf("EncodeBlob: %v", err)
		}
		inputs = append(inputs, encoded)
	}

	_, err = WritePackfile(ctx, tempFile, inputs)
	if err != nil {
		t.Fatalf("WritePackfile: %v", err)
	}

	sink.mu.Lock()
	defer sink.mu.Unlock()
	if len(sink.messages) == 0 {
		t.Fatal("expected at least one log message from WritePackfile")
	}
	expectedMessage := "batch size exceeds default pack slots; reserving for pack"
	if sink.messages[0] != expectedMessage {
		t.Fatalf("expected message %q, got %q", expectedMessage, sink.messages[0])
	}
}

func TestIncrementalPack_Seal_ScopedLog(t *testing.T) {
	sink := &blobLogCaptureSink{}
	logger := logr.New(sink).WithValues("callerKey", "callerValue")
	ctx := scoped.WithLog(t.Context(), logger)

	packPath := filepath.Join(t.TempDir(), "test-seal-log.pack")
	pack, err := NewIncrementalPack(ctx, packPath, 2)
	if err != nil {
		t.Fatalf("NewIncrementalPack: %v", err)
	}
	defer func() {
		if closeErr := pack.Close(); closeErr != nil {
			t.Errorf("Close failed: %v", closeErr)
		}
	}()

	chunk1 := []byte("first-chunk")
	chunk2 := []byte("second-chunk")
	if _, err := pack.Append(ctx, chunk1); err != nil {
		t.Fatalf("Append 1: %v", err)
	}
	if _, err := pack.Append(ctx, chunk2); err != nil {
		t.Fatalf("Append 2: %v", err)
	}

	if err := pack.Seal(ctx); err != nil {
		t.Fatalf("Seal failed: %v", err)
	}

	sink.mu.Lock()
	defer sink.mu.Unlock()
	if len(sink.messages) == 0 {
		t.Fatal("expected at least one log message from Seal")
	}
	expectedMessage := "packfile sealed with all slots full"
	var found bool
	for _, entry := range sink.entries {
		if entry["callerKey"] == "callerValue" {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("expected sealed log entry to carry callerKey, got entries: %v", sink.entries)
	}
	if sink.messages[len(sink.messages)-1] != expectedMessage {
		t.Fatalf("expected last message %q, got %q", expectedMessage, sink.messages[len(sink.messages)-1])
	}
}
