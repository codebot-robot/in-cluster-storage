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
	"sort"
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

func (m *testMemoryBackend) PutObject(ctx context.Context, volumeID, key string, stream ByteStream) (string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	k := m.storageKey(volumeID, key)
	if err := stream.Rewind(); err != nil {
		return "", err
	}
	buf, err := io.ReadAll(stream)
	if err != nil {
		return "", err
	}
	m.objects[k] = buf

	h := sha256.Sum256(buf)
	return fmt.Sprintf("%x", h), nil
}

func (m *testMemoryBackend) GetObject(ctx context.Context, volumeID, key string, offset, length int64, w io.Writer) error {
	m.mu.RLock()
	defer m.mu.RUnlock()

	k := m.storageKey(volumeID, key)
	data, ok := m.objects[k]
	if !ok {
		return fmt.Errorf("object %s not found in volume %s", key, volumeID)
	}

	total := int64(len(data))
	if offset >= total {
		return nil
	}
	end := offset + length
	if length <= 0 || end > total {
		end = total
	}

	_, err := w.Write(data[offset:end])
	return err
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
	stream := NewByteStreamFromBytes(data)
	prep, err := EncodeBlob(stream, true)
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

	decStream, entry, err := DecodeLooseBlob(bytes.NewReader(encoded))
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

	prep, err := EncodeBlob(stream, true)
	if err != nil {
		t.Fatalf("EncodeBlob failed: %v", err)
	}

	encodedStream, err := EncodeLooseBlob(prep)
	if err != nil {
		t.Fatalf("EncodeLooseBlob failed: %v", err)
	}

	decStream, entry, err := DecodeLooseBlob(encodedStream)
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
		p, err := EncodeBlob(NewByteStreamFromBytes(c), true)
		if err != nil {
			t.Fatalf("EncodeBlob failed: %v", err)
		}
		preps = append(preps, p)
	}

	var packBuf bytes.Buffer
	packFile, err := WritePackfile(&packBuf, preps)
	if err != nil {
		t.Fatalf("WritePackfile failed: %v", err)
	}
	packData := packBuf.Bytes()

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
		blobStream, err := parsedPack.DecodeBlob(payloadReader, entry)
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
	if _, _, _, err := ChunkBlobs(src, 0, true); err == nil {
		t.Fatalf("expected error for chunkSize 0, got nil")
	}

	chunkSize := uint32(16 * 1024)

	chunkBlobs, manifest, manifestBlob, err := ChunkBlobs(src, chunkSize, true)
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
