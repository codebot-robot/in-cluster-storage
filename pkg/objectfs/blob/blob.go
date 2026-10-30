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
	"compress/zlib"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"sort"
	"sync"

	"github.com/gke-labs/in-cluster-storage/pkg/errutil"
	"k8s.io/klog/v2"
)

const (
	// Magic is the 4-byte header identifier "OBJB" (0x4F424A42).
	Magic uint32 = 0x4F424A42

	// Version format constants.
	VersionV1 uint8 = 1
	VersionV2 uint8 = 2

	// Version is the current file format version.
	Version uint8 = VersionV2

	// File Types
	FileTypeBlob uint8 = 1 // Standalone large blob ("Loose Blob")
	FileTypePack uint8 = 2 // Packfile containing multiple blobs

	// Encoding Types
	EncodingRaw        uint32 = 0 // Uncompressed raw binary
	EncodingCompressed uint32 = 1 // Standard zlib compression
	EncodingChunked    uint32 = 2 // Chunked encoding (list of chunk SHA256s)

	// FileHeaderSize is the fixed size of the base file header (8 bytes).
	FileHeaderSize = 8

	// ManifestHeaderSize is the fixed size of the Manifest payload header (24 bytes).
	ManifestHeaderSize = 24

	// SingleBlobHeaderSize is the total size of the loose blob header (64 bytes).
	SingleBlobHeaderSize = 64

	// PackV1HeaderSize is the fixed header of a v1 packfile (16 bytes):
	// 8 bytes base header + 4 bytes count N + 4 bytes reserved.
	PackV1HeaderSize = 16

	// PackHeaderSize is retained for backwards compatibility with v1.
	PackHeaderSize = 16

	// PackV2HeaderSize is the fixed header of a v2 packfile (32 bytes):
	// 8 bytes base header + 4 bytes slots S + 4 bytes used N + 4 bytes flags + 12 bytes reserved.
	PackV2HeaderSize = 32

	// PackV2SlotSize is the fixed size of each index slot in a v2 packfile (48 bytes):
	// 32 bytes SHA-256 + 4 bytes start + 4 bytes stored_length + 4 bytes final_length + 4 bytes encoding.
	PackV2SlotSize = 48

	// PackFlagSealed indicates the packfile is sealed (bit 0 = 1).
	PackFlagSealed uint32 = 1

	// DefaultPackSlots is the default reserved slot count for v2 packfiles (1024).
	DefaultPackSlots uint32 = 1024

	// BlobItemHeaderSize is the fixed header for each blob stored inside a packfile (16 bytes):
	// 4 bytes encoding + 4 bytes finalLength + 8 bytes reserved/padding.
	BlobItemHeaderSize = 16

	// MemoryThreshold is the maximum blob size (256KB) to buffer in memory during encoding.
	// Larger inputs use temporary files on disk to prevent unbounded memory consumption.
	MemoryThreshold int64 = 256 * 1024

	// CompressionSavingsThreshold is the minimum fraction of size savings required
	// to keep a blob compressed (at least 10% smaller than raw uncompressed data).
	CompressionSavingsThreshold float64 = 0.10

	// DefaultLargeBlobThreshold is 64MB (blobs larger than 64MB are stored as standalone loose blobs).
	DefaultLargeBlobThreshold int64 = 64 * 1024 * 1024
)

// ByteStream represents a seekable, closeable stream with known length.
type ByteStream interface {
	io.ReadSeekCloser
	Length() int64
	Rewind() error
}

type memoryByteStream struct {
	data []byte
	r    *bytes.Reader
}

// NewByteStreamFromBytes creates an in-memory ByteStream from a byte slice.
// This should only be used for small-sized data (e.g. <= 256KB) to avoid excessive memory usage.
func NewByteStreamFromBytes(data []byte) ByteStream {
	return &memoryByteStream{
		data: data,
		r:    bytes.NewReader(data),
	}
}

func (m *memoryByteStream) Read(p []byte) (int, error) {
	return m.r.Read(p)
}

func (m *memoryByteStream) Seek(offset int64, whence int) (int64, error) {
	return m.r.Seek(offset, whence)
}

func (m *memoryByteStream) Close() error {
	return nil
}

func (m *memoryByteStream) Length() int64 {
	return int64(len(m.data))
}

func (m *memoryByteStream) Rewind() error {
	_, err := m.r.Seek(0, io.SeekStart)
	return err
}

func (m *memoryByteStream) Bytes() []byte {
	return m.data
}

type fileByteStream struct {
	file       *os.File
	path       string
	length     int64
	autoDelete bool
}

// NewByteStreamFromFile creates a ByteStream backed by an os.File on disk with an explicit length.
func NewByteStreamFromFile(f *os.File, length int64, autoDelete bool) ByteStream {
	return &fileByteStream{
		file:       f,
		path:       f.Name(),
		length:     length,
		autoDelete: autoDelete,
	}
}

func (f *fileByteStream) Read(p []byte) (int, error) {
	return f.file.Read(p)
}

func (f *fileByteStream) Seek(offset int64, whence int) (int64, error) {
	return f.file.Seek(offset, whence)
}

func (f *fileByteStream) Close() error {
	err := f.file.Close()
	if f.autoDelete && f.path != "" {
		if remErr := os.Remove(f.path); remErr != nil && !os.IsNotExist(remErr) {
			klog.Warningf("failed to remove temporary file %s: %v", f.path, remErr)
		}
	}
	return err
}

func (f *fileByteStream) Length() int64 {
	return f.length
}

func (f *fileByteStream) Rewind() error {
	_, err := f.file.Seek(0, io.SeekStart)
	return err
}

type readSeekCloserByteStream struct {
	rsc    io.ReadSeekCloser
	length int64
}

// NewByteStreamFromReadSeekCloser wraps an io.ReadSeekCloser and its length as a ByteStream.
func NewByteStreamFromReadSeekCloser(rsc io.ReadSeekCloser, length int64) ByteStream {
	return &readSeekCloserByteStream{
		rsc:    rsc,
		length: length,
	}
}

func (r *readSeekCloserByteStream) Read(p []byte) (int, error) {
	return r.rsc.Read(p)
}

func (r *readSeekCloserByteStream) Seek(offset int64, whence int) (int64, error) {
	return r.rsc.Seek(offset, whence)
}

func (r *readSeekCloserByteStream) Close() error {
	return r.rsc.Close()
}

func (r *readSeekCloserByteStream) Length() int64 {
	return r.length
}

func (r *readSeekCloserByteStream) Rewind() error {
	_, err := r.rsc.Seek(0, io.SeekStart)
	return err
}

// FileHeader represents the fixed 8-byte file header.
type FileHeader struct {
	Magic    uint32
	Version  uint8
	FileType uint8
	Reserved uint16
}

// BlobEntry describes a single blob inside a packfile.
type BlobEntry struct {
	SHA256       [32]byte
	DataOffset   uint32
	StoredLength uint32
	FinalLength  uint32
	Encoding     uint32
}

// SHA256Hex returns the hex string representation of the entry's SHA256.
func (e *BlobEntry) SHA256Hex() string {
	return hex.EncodeToString(e.SHA256[:])
}

// ParseFileHeader parses and verifies the 8-byte base file header.
func ParseFileHeader(data []byte) (FileHeader, error) {
	if len(data) < FileHeaderSize {
		return FileHeader{}, errors.New("truncated file header: less than 8 bytes")
	}

	magic := binary.BigEndian.Uint32(data[0:4])
	if magic != Magic {
		return FileHeader{}, fmt.Errorf("invalid magic: got 0x%08X, expected 0x%08X", magic, Magic)
	}

	ver := data[4]
	if ver != VersionV1 && ver != VersionV2 {
		return FileHeader{}, fmt.Errorf("unsupported version %d (expected %d or %d)", ver, VersionV1, VersionV2)
	}

	ft := data[5]
	if ft != FileTypeBlob && ft != FileTypePack {
		return FileHeader{}, fmt.Errorf("unknown file type: %d", ft)
	}

	reserved := binary.BigEndian.Uint16(data[6:8])
	return FileHeader{
		Magic:    magic,
		Version:  ver,
		FileType: ft,
		Reserved: reserved,
	}, nil
}

// WriteFileHeader writes the 8-byte base file header into a buffer with the default Version.
func WriteFileHeader(fileType uint8) []byte {
	return WriteFileHeaderWithVersion(fileType, Version)
}

// WriteFileHeaderWithVersion writes the 8-byte base file header with a specific version into a buffer.
func WriteFileHeaderWithVersion(fileType uint8, version uint8) []byte {
	buf := make([]byte, FileHeaderSize)
	binary.BigEndian.PutUint32(buf[0:4], Magic)
	buf[4] = version
	buf[5] = fileType
	buf[6] = 0
	buf[7] = 0
	return buf
}

// EncodedBlob holds a processed blob payload ready for streaming as a loose blob or packing into a packfile.
type EncodedBlob struct {
	Stream       ByteStream
	StoredLength uint64
	FinalLength  uint64
	SHA256       [32]byte
	Encoding     uint32
}

// SHA256Hex returns the hex string representation of the blob's SHA256.
func (b *EncodedBlob) SHA256Hex() string {
	return hex.EncodeToString(b.SHA256[:])
}

func (b *EncodedBlob) Close() error {
	if b.Stream != nil {
		return b.Stream.Close()
	}
	return nil
}

// EncodeBlob takes a ByteStream and encodes it, computing the SHA256 and compressing by default.
// It performs a single pass over src when compressing. If compression is not a win, it rewinds src and uses raw uncompressed data.
func EncodeBlob(src ByteStream, compress bool) (*EncodedBlob, error) {
	if err := src.Rewind(); err != nil {
		return nil, fmt.Errorf("failed to rewind input stream: %w", err)
	}

	rawLen := src.Length()
	hasher := sha256.New()

	if !compress || rawLen == 0 {
		if _, err := io.Copy(hasher, src); err != nil {
			return nil, fmt.Errorf("failed to compute sha256: %w", err)
		}
		var finalSHA [32]byte
		copy(finalSHA[:], hasher.Sum(nil))
		if err := src.Rewind(); err != nil {
			return nil, err
		}
		return &EncodedBlob{
			Stream:       src,
			StoredLength: uint64(rawLen),
			FinalLength:  uint64(rawLen),
			SHA256:       finalSHA,
			Encoding:     EncodingRaw,
		}, nil
	}

	// Single pass compression + hashing
	var outputMem bytes.Buffer
	var outputTempFile *os.File
	var outputTempPath string
	var outputWriter io.Writer

	useTemp := rawLen > MemoryThreshold
	if useTemp {
		tf, err := os.CreateTemp("", "objectfs-comp-*")
		if err != nil {
			return nil, fmt.Errorf("failed to create temp file for compression: %w", err)
		}
		outputTempFile = tf
		outputTempPath = tf.Name()
		outputWriter = tf
	} else {
		outputWriter = &outputMem
	}

	zw := zlib.NewWriter(outputWriter)
	tee := io.TeeReader(src, hasher)
	if _, err := io.Copy(zw, tee); err != nil {
		errutil.CloseAndLog(zw, "zlib compressor")
		if outputTempFile != nil {
			errutil.CloseAndLog(outputTempFile, "temp compression file")
			if remErr := os.Remove(outputTempPath); remErr != nil && !os.IsNotExist(remErr) {
				klog.Warningf("failed to remove temporary compression file %s: %v", outputTempPath, remErr)
			}
		}
		return nil, fmt.Errorf("compression failed: %w", err)
	}
	if err := zw.Close(); err != nil {
		if outputTempFile != nil {
			errutil.CloseAndLog(outputTempFile, "temp compression file")
			if remErr := os.Remove(outputTempPath); remErr != nil && !os.IsNotExist(remErr) {
				klog.Warningf("failed to remove temporary compression file %s: %v", outputTempPath, remErr)
			}
		}
		return nil, fmt.Errorf("compression close failed: %w", err)
	}

	var finalSHA [32]byte
	copy(finalSHA[:], hasher.Sum(nil))

	var outputLen int64
	if useTemp {
		st, err := outputTempFile.Stat()
		if err != nil {
			errutil.CloseAndLog(outputTempFile, "temp compression file")
			if remErr := os.Remove(outputTempPath); remErr != nil && !os.IsNotExist(remErr) {
				klog.Warningf("failed to remove temporary compression file %s: %v", outputTempPath, remErr)
			}
			return nil, fmt.Errorf("failed to stat temp compression file: %w", err)
		}
		outputLen = st.Size()
	} else {
		outputLen = int64(outputMem.Len())
	}

	savings := float64(rawLen-outputLen) / float64(rawLen)
	if savings >= CompressionSavingsThreshold {
		// Compression is a win (at least 10% smaller than raw)
		if useTemp {
			if _, err := outputTempFile.Seek(0, io.SeekStart); err != nil {
				errutil.CloseAndLog(outputTempFile, "temp compression file")
				if remErr := os.Remove(outputTempPath); remErr != nil && !os.IsNotExist(remErr) {
					klog.Warningf("failed to remove temporary compression file %s: %v", outputTempPath, remErr)
				}
				return nil, fmt.Errorf("failed to seek temp compression file: %w", err)
			}
			return &EncodedBlob{
				Stream:       NewByteStreamFromFile(outputTempFile, outputLen, true),
				StoredLength: uint64(outputLen),
				FinalLength:  uint64(rawLen),
				SHA256:       finalSHA,
				Encoding:     EncodingCompressed,
			}, nil
		}
		return &EncodedBlob{
			Stream:       NewByteStreamFromBytes(outputMem.Bytes()),
			StoredLength: uint64(outputLen),
			FinalLength:  uint64(rawLen),
			SHA256:       finalSHA,
			Encoding:     EncodingCompressed,
		}, nil
	}

	// Compression was not a win, discard compressed output and use raw src
	if outputTempFile != nil {
		errutil.CloseAndLog(outputTempFile, "temp compression file")
		if remErr := os.Remove(outputTempPath); remErr != nil && !os.IsNotExist(remErr) {
			klog.Warningf("failed to remove temporary compression file %s: %v", outputTempPath, remErr)
		}
	}
	if err := src.Rewind(); err != nil {
		return nil, err
	}
	return &EncodedBlob{
		Stream:       src,
		StoredLength: uint64(rawLen),
		FinalLength:  uint64(rawLen),
		SHA256:       finalSHA,
		Encoding:     EncodingRaw,
	}, nil
}

// EncodeLooseBlob returns a ByteStream representing a standalone loose blob file (filetype=1).
func EncodeLooseBlob(blob *EncodedBlob) (ByteStream, error) {
	if err := blob.Stream.Rewind(); err != nil {
		return nil, err
	}

	hdr := make([]byte, SingleBlobHeaderSize)
	copy(hdr[0:8], WriteFileHeaderWithVersion(FileTypeBlob, VersionV1))
	binary.BigEndian.PutUint32(hdr[8:12], blob.Encoding)
	binary.BigEndian.PutUint64(hdr[12:20], blob.FinalLength)
	copy(hdr[20:52], blob.SHA256[:])

	return &compositeByteStream{
		header: hdr,
		body:   blob.Stream,
	}, nil
}

type compositeByteStream struct {
	header []byte
	body   ByteStream
	r      io.Reader
}

func (c *compositeByteStream) Read(p []byte) (int, error) {
	if c.r == nil {
		c.r = io.MultiReader(bytes.NewReader(c.header), c.body)
	}
	return c.r.Read(p)
}

func (c *compositeByteStream) Seek(offset int64, whence int) (int64, error) {
	if offset == 0 && whence == io.SeekStart {
		return 0, c.Rewind()
	}
	return 0, errors.New("arbitrary seek on composite stream not supported; use Rewind")
}

func (c *compositeByteStream) Length() int64 {
	return int64(len(c.header)) + c.body.Length()
}

func (c *compositeByteStream) Rewind() error {
	if err := c.body.Rewind(); err != nil {
		return err
	}
	c.r = io.MultiReader(bytes.NewReader(c.header), c.body)
	return nil
}

func (c *compositeByteStream) Close() error {
	return c.body.Close()
}

// computeStreamSHA256 computes the SHA-256 hash of stream, rewinding before and after.
func computeStreamSHA256(stream ByteStream) ([32]byte, error) {
	if err := stream.Rewind(); err != nil {
		return [32]byte{}, fmt.Errorf("failed to rewind stream: %w", err)
	}
	hasher := sha256.New()
	if _, err := io.Copy(hasher, stream); err != nil {
		return [32]byte{}, fmt.Errorf("failed to hash stream: %w", err)
	}
	if err := stream.Rewind(); err != nil {
		return [32]byte{}, fmt.Errorf("failed to rewind stream after hash: %w", err)
	}
	var sum [32]byte
	copy(sum[:], hasher.Sum(nil))
	return sum, nil
}

func decodePayloadToByteStream(r io.Reader, encoding uint32, finalLen int64) (ByteStream, error) {
	var src io.Reader = r
	if encoding == EncodingCompressed {
		zr, err := zlib.NewReader(r)
		if err != nil {
			return nil, fmt.Errorf("failed to initialize zlib decompressor: %w", err)
		}
		defer errutil.CloseAndLog(zr, "zlib decompressor")
		src = zr
	} else if encoding != EncodingRaw && encoding != EncodingChunked {
		return nil, fmt.Errorf("unsupported encoding: %d", encoding)
	}

	if finalLen >= 0 && finalLen <= MemoryThreshold {
		buf := make([]byte, finalLen)
		if _, err := io.ReadFull(src, buf); err != nil {
			return nil, fmt.Errorf("failed reading payload (expected %d bytes): %w", finalLen, err)
		}
		var extra [1]byte
		n, err := src.Read(extra[:])
		if n > 0 {
			return nil, fmt.Errorf("decoded payload exceeds expected length %d", finalLen)
		}
		if err != nil && err != io.EOF {
			return nil, fmt.Errorf("failed checking trailing payload data: %w", err)
		}
		return NewByteStreamFromBytes(buf), nil
	}

	tf, err := os.CreateTemp("", "objectfs-blob-data-*")
	if err != nil {
		return nil, fmt.Errorf("failed to create temp file: %w", err)
	}
	tempPath := tf.Name()

	written, err := io.Copy(tf, src)
	if err != nil {
		errutil.CloseAndLog(tf, "temp payload file")
		if remErr := os.Remove(tempPath); remErr != nil && !os.IsNotExist(remErr) {
			klog.Warningf("failed to remove temporary payload file %s: %v", tempPath, remErr)
		}
		return nil, fmt.Errorf("failed writing to temp file: %w", err)
	}
	if written != finalLen {
		errutil.CloseAndLog(tf, "temp payload file")
		if remErr := os.Remove(tempPath); remErr != nil && !os.IsNotExist(remErr) {
			klog.Warningf("failed to remove temporary payload file %s: %v", tempPath, remErr)
		}
		return nil, fmt.Errorf("decoded payload length mismatch: expected %d bytes, got %d", finalLen, written)
	}

	if _, err := tf.Seek(0, io.SeekStart); err != nil {
		errutil.CloseAndLog(tf, "temp payload file")
		if remErr := os.Remove(tempPath); remErr != nil && !os.IsNotExist(remErr) {
			klog.Warningf("failed to remove temporary payload file %s: %v", tempPath, remErr)
		}
		return nil, fmt.Errorf("failed to seek temp payload file: %w", err)
	}

	return NewByteStreamFromFile(tf, written, true), nil
}

// DecodeLooseBlob parses the standalone loose blob header from r and returns a ByteStream for the decoded data.
// It is the caller's responsibility to verify the SHA256 if needed.
func DecodeLooseBlob(r io.Reader) (ByteStream, BlobEntry, error) {
	hdrBuf := make([]byte, SingleBlobHeaderSize)
	if _, err := io.ReadFull(r, hdrBuf); err != nil {
		return nil, BlobEntry{}, fmt.Errorf("failed to read loose blob header: %w", err)
	}

	hdr, err := ParseFileHeader(hdrBuf[0:8])
	if err != nil {
		return nil, BlobEntry{}, err
	}
	if hdr.FileType != FileTypeBlob {
		return nil, BlobEntry{}, fmt.Errorf("expected filetype %d, got %d", FileTypeBlob, hdr.FileType)
	}

	encoding := binary.BigEndian.Uint32(hdrBuf[8:12])
	finalLen := binary.BigEndian.Uint64(hdrBuf[12:20])
	var expectedSHA [32]byte
	copy(expectedSHA[:], hdrBuf[20:52])

	entry := BlobEntry{
		SHA256:      expectedSHA,
		FinalLength: uint32(finalLen),
		Encoding:    encoding,
	}

	stream, err := decodePayloadToByteStream(r, encoding, int64(finalLen))
	if err != nil {
		return nil, BlobEntry{}, err
	}
	return stream, entry, nil
}

// PackFile represents a parsed packfile with its indexed entries.
type PackFile struct {
	Version uint8
	Slots   uint32
	Flags   uint32
	Entries []BlobEntry
}

// Lookup performs binary search for a SHA256 in the packfile entries.
func (p *PackFile) Lookup(target [32]byte) (BlobEntry, bool) {
	idx := sort.Search(len(p.Entries), func(i int) bool {
		return bytes.Compare(p.Entries[i].SHA256[:], target[:]) >= 0
	})
	if idx < len(p.Entries) && p.Entries[idx].SHA256 == target {
		return p.Entries[idx], true
	}
	return BlobEntry{}, false
}

var (
	packSlotsFullMu    sync.Mutex
	packSlotsFullCount uint64
)

// PackSlotsFullCount returns the total number of times a packfile was written with all slots filled.
func PackSlotsFullCount() uint64 {
	packSlotsFullMu.Lock()
	defer packSlotsFullMu.Unlock()
	return packSlotsFullCount
}

// DecodeBlob decodes a blob payload read from the packfile.
func (p *PackFile) DecodeBlob(r io.Reader, entry BlobEntry) (ByteStream, error) {
	if p.Version == VersionV1 {
		return DecodeBlobFromPayload(r, entry)
	}
	// For v2: require exactly stored_length bytes from the input
	storedBuf := make([]byte, entry.StoredLength)
	if _, err := io.ReadFull(r, storedBuf); err != nil {
		return nil, fmt.Errorf("corrupt packfile entry for sha256 %s at offset %d: expected stored length %d: %w",
			entry.SHA256Hex(), entry.DataOffset, entry.StoredLength, err)
	}
	stream, err := decodePayloadToByteStream(bytes.NewReader(storedBuf), entry.Encoding, int64(entry.FinalLength))
	if err != nil {
		return nil, fmt.Errorf("corrupt packfile entry for sha256 %s at offset %d: %w",
			entry.SHA256Hex(), entry.DataOffset, err)
	}
	if entry.SHA256 != [32]byte{} {
		actualSHA, err := computeStreamSHA256(stream)
		if err != nil {
			errutil.CloseAndLog(stream, "decoded stream")
			return nil, fmt.Errorf("corrupt packfile entry for sha256 %s at offset %d: %w",
				entry.SHA256Hex(), entry.DataOffset, err)
		}
		if actualSHA != entry.SHA256 {
			errutil.CloseAndLog(stream, "decoded stream")
			return nil, fmt.Errorf("corrupt packfile entry for sha256 %s at offset %d: checksum mismatch: got %x, expected %x",
				entry.SHA256Hex(), entry.DataOffset, actualSHA, entry.SHA256)
		}
	}
	return stream, nil
}

// WritePackfile writes a v2 packfile directly to w using the default slot count (1024).
func WritePackfile(w io.Writer, inputs []*EncodedBlob) (*PackFile, error) {
	slots := DefaultPackSlots
	if uint32(len(inputs)) > slots {
		slots = uint32(len(inputs))
		klog.V(2).Infof("batch size %d exceeds default pack slots %d; reserving S = %d for pack", len(inputs), DefaultPackSlots, slots)
	}
	return WritePackfileWithSlots(w, inputs, slots)
}

// WritePackfileWithSlots writes a v2 packfile with explicit slot reservation to w:
// [Header 32B: 8B base (version 2) + 4B slots S + 4B used N + 4B flags (bit 0=sealed) + 12B reserved]
// [Table: S * 48B slots: first N sorted by SHA256, remaining S - N zeroed]
// [Data: for each blob in inputs order: 16B BlobItemHeader + payload data]
func WritePackfileWithSlots(w io.Writer, inputs []*EncodedBlob, slots uint32) (*PackFile, error) {
	n := len(inputs)
	if n == 0 {
		return nil, errors.New("cannot create empty packfile with 0 blobs")
	}
	if slots < uint32(n) {
		return nil, fmt.Errorf("slots %d cannot be less than blob count %d", slots, n)
	}

	if uint32(n) == slots {
		packSlotsFullMu.Lock()
		packSlotsFullCount++
		count := packSlotsFullCount
		packSlotsFullMu.Unlock()
		klog.V(2).Infof("packfile sealed with all %d slots full (total full packs: %d)", slots, count)
	}

	frontSectionSize := uint32(PackV2HeaderSize) + slots*PackV2SlotSize
	currentOffset := frontSectionSize

	entries := make([]BlobEntry, n)
	for i, prep := range inputs {
		itemHdrOffset := currentOffset
		payloadStart := itemHdrOffset + uint32(BlobItemHeaderSize)
		storedLen := uint32(prep.StoredLength)
		entries[i] = BlobEntry{
			SHA256:       prep.SHA256,
			DataOffset:   payloadStart,
			StoredLength: storedLen,
			FinalLength:  uint32(prep.FinalLength),
			Encoding:     prep.Encoding,
		}
		currentOffset = payloadStart + storedLen
	}

	// First N slots must be sorted by SHA256
	sortedEntries := make([]BlobEntry, n)
	copy(sortedEntries, entries)
	sort.Slice(sortedEntries, func(i, j int) bool {
		return bytes.Compare(sortedEntries[i].SHA256[:], sortedEntries[j].SHA256[:]) < 0
	})

	// Construct header (32 bytes)
	hdrBuf := make([]byte, PackV2HeaderSize)
	copy(hdrBuf[0:8], WriteFileHeaderWithVersion(FileTypePack, VersionV2))
	binary.BigEndian.PutUint32(hdrBuf[8:12], slots)
	binary.BigEndian.PutUint32(hdrBuf[12:16], uint32(n))
	binary.BigEndian.PutUint32(hdrBuf[16:20], PackFlagSealed)
	// hdrBuf[20:32] is reserved (zeroed)

	if _, err := w.Write(hdrBuf); err != nil {
		return nil, fmt.Errorf("failed writing pack v2 header: %w", err)
	}

	// Construct table (S * 48 bytes)
	tableBuf := make([]byte, slots*PackV2SlotSize)
	for i, entry := range sortedEntries {
		off := uint32(i) * PackV2SlotSize
		copy(tableBuf[off:off+32], entry.SHA256[:])
		binary.BigEndian.PutUint32(tableBuf[off+32:off+36], entry.DataOffset)
		binary.BigEndian.PutUint32(tableBuf[off+36:off+40], entry.StoredLength)
		binary.BigEndian.PutUint32(tableBuf[off+40:off+44], entry.FinalLength)
		binary.BigEndian.PutUint32(tableBuf[off+44:off+48], entry.Encoding)
	}
	// remaining (slots - n) * 48 bytes are already zeroed

	if _, err := w.Write(tableBuf); err != nil {
		return nil, fmt.Errorf("failed writing pack v2 tables: %w", err)
	}

	// Write data items in input order
	for _, prep := range inputs {
		if err := prep.Stream.Rewind(); err != nil {
			return nil, fmt.Errorf("failed rewinding blob stream: %w", err)
		}
		itemHdr := make([]byte, BlobItemHeaderSize)
		binary.BigEndian.PutUint32(itemHdr[0:4], prep.Encoding)
		binary.BigEndian.PutUint32(itemHdr[4:8], uint32(prep.FinalLength))
		binary.BigEndian.PutUint64(itemHdr[8:16], 0) // padding/reserved

		if _, err := w.Write(itemHdr); err != nil {
			return nil, fmt.Errorf("failed writing blob item header: %w", err)
		}
		if _, err := io.Copy(w, prep.Stream); err != nil {
			return nil, fmt.Errorf("failed writing blob payload: %w", err)
		}
	}

	return &PackFile{
		Version: VersionV2,
		Slots:   slots,
		Flags:   PackFlagSealed,
		Entries: sortedEntries,
	}, nil
}

// WritePackfileV1 writes a v1 packfile directly to w (kept for compatibility and testing):
// [Header 16B: 8B base (version 1) + 4B count N + 4B reserved]
// [Table 1: N * 32B SHAs (sorted)]
// [Table 2: (N + 1) * 4B DataOffsets]
// [Data Payload Section: for each blob: 16B BlobItemHeader + payload data...]
func WritePackfileV1(w io.Writer, inputs []*EncodedBlob) (*PackFile, error) {
	n := len(inputs)
	if n == 0 {
		return nil, errors.New("cannot create empty packfile with 0 blobs")
	}

	// Sort inputs by SHA256
	sortedInputs := make([]*EncodedBlob, n)
	copy(sortedInputs, inputs)
	sort.Slice(sortedInputs, func(i, j int) bool {
		return bytes.Compare(sortedInputs[i].SHA256[:], sortedInputs[j].SHA256[:]) < 0
	})

	frontTableSize := uint32(PackV1HeaderSize + n*32 + (n+1)*4)
	currentOffset := frontTableSize

	entries := make([]BlobEntry, n)
	offsets := make([]uint32, n+1)

	for i, prep := range sortedInputs {
		offsets[i] = currentOffset
		storedLen := uint32(BlobItemHeaderSize) + uint32(prep.StoredLength)
		entries[i] = BlobEntry{
			SHA256:       prep.SHA256,
			DataOffset:   currentOffset,
			StoredLength: storedLen,
			FinalLength:  uint32(prep.FinalLength),
			Encoding:     prep.Encoding,
		}
		currentOffset += storedLen
	}
	offsets[n] = currentOffset // end of file offset

	// Construct front table buffer
	tableBuf := make([]byte, frontTableSize)
	copy(tableBuf[0:8], WriteFileHeaderWithVersion(FileTypePack, VersionV1))
	binary.BigEndian.PutUint32(tableBuf[8:12], uint32(n))
	binary.BigEndian.PutUint32(tableBuf[12:16], 0)

	shaOff := uint32(PackV1HeaderSize)
	offsetOff := shaOff + uint32(n*32)

	for i, prep := range sortedInputs {
		copy(tableBuf[shaOff+uint32(i*32):shaOff+uint32(i*32)+32], prep.SHA256[:])
		binary.BigEndian.PutUint32(tableBuf[offsetOff+uint32(i*4):offsetOff+uint32(i*4)+4], offsets[i])
	}
	binary.BigEndian.PutUint32(tableBuf[offsetOff+uint32(n*4):offsetOff+uint32(n*4)+4], offsets[n])

	if _, err := w.Write(tableBuf); err != nil {
		return nil, fmt.Errorf("failed writing pack header/tables: %w", err)
	}

	for _, prep := range sortedInputs {
		if err := prep.Stream.Rewind(); err != nil {
			return nil, fmt.Errorf("failed rewinding blob stream: %w", err)
		}
		itemHdr := make([]byte, BlobItemHeaderSize)
		binary.BigEndian.PutUint32(itemHdr[0:4], prep.Encoding)
		binary.BigEndian.PutUint32(itemHdr[4:8], uint32(prep.FinalLength))
		binary.BigEndian.PutUint64(itemHdr[8:16], 0) // padding/reserved

		if _, err := w.Write(itemHdr); err != nil {
			return nil, fmt.Errorf("failed writing blob item header: %w", err)
		}
		if _, err := io.Copy(w, prep.Stream); err != nil {
			return nil, fmt.Errorf("failed writing blob payload: %w", err)
		}
	}

	return &PackFile{
		Version: VersionV1,
		Entries: entries,
	}, nil
}

// DecodePackTable parses the front tables from a packfile stream (v1 or v2) and returns a PackFile.
func DecodePackTable(r io.Reader) (*PackFile, error) {
	baseHdr := make([]byte, FileHeaderSize)
	if _, err := io.ReadFull(r, baseHdr); err != nil {
		return nil, fmt.Errorf("failed to read pack base header: %w", err)
	}

	hdr, err := ParseFileHeader(baseHdr)
	if err != nil {
		return nil, err
	}
	if hdr.FileType != FileTypePack {
		return nil, fmt.Errorf("expected filetype %d for pack, got %d", FileTypePack, hdr.FileType)
	}

	if hdr.Version == VersionV1 {
		restHdr := make([]byte, PackV1HeaderSize-FileHeaderSize)
		if _, err := io.ReadFull(r, restHdr); err != nil {
			return nil, fmt.Errorf("failed to read pack v1 header: %w", err)
		}
		count := binary.BigEndian.Uint32(restHdr[0:4])
		n := int(count)
		if n == 0 {
			return &PackFile{Version: VersionV1, Entries: nil}, nil
		}

		tablesSize := n*32 + (n+1)*4
		tablesBuf := make([]byte, tablesSize)
		if _, err := io.ReadFull(r, tablesBuf); err != nil {
			return nil, fmt.Errorf("failed to read pack tables: %w", err)
		}

		shaOff := 0
		offsetOff := shaOff + n*32

		entries := make([]BlobEntry, n)
		offsets := make([]uint32, n+1)

		for i := 0; i <= n; i++ {
			offsets[i] = binary.BigEndian.Uint32(tablesBuf[offsetOff+i*4 : offsetOff+i*4+4])
		}

		for i := 0; i < n; i++ {
			var sha [32]byte
			copy(sha[:], tablesBuf[shaOff+i*32:shaOff+i*32+32])
			entries[i] = BlobEntry{
				SHA256:       sha,
				DataOffset:   offsets[i],
				StoredLength: offsets[i+1] - offsets[i],
			}
		}

		return &PackFile{Version: VersionV1, Entries: entries}, nil
	}

	if hdr.Version == VersionV2 {
		restHdr := make([]byte, PackV2HeaderSize-FileHeaderSize)
		if _, err := io.ReadFull(r, restHdr); err != nil {
			return nil, fmt.Errorf("failed to read pack v2 header: %w", err)
		}
		slots := binary.BigEndian.Uint32(restHdr[0:4])
		used := binary.BigEndian.Uint32(restHdr[4:8])
		flags := binary.BigEndian.Uint32(restHdr[8:12])

		if used > slots {
			return nil, fmt.Errorf("invalid pack v2 header: used %d exceeds slots %d", used, slots)
		}

		tableBuf := make([]byte, slots*PackV2SlotSize)
		if _, err := io.ReadFull(r, tableBuf); err != nil {
			return nil, fmt.Errorf("failed to read pack v2 table: %w", err)
		}

		entries := make([]BlobEntry, used)
		for i := 0; i < int(used); i++ {
			off := i * int(PackV2SlotSize)
			var sha [32]byte
			copy(sha[:], tableBuf[off:off+32])
			start := binary.BigEndian.Uint32(tableBuf[off+32 : off+36])
			storedLen := binary.BigEndian.Uint32(tableBuf[off+36 : off+40])
			finalLen := binary.BigEndian.Uint32(tableBuf[off+40 : off+44])
			encoding := binary.BigEndian.Uint32(tableBuf[off+44 : off+48])

			entries[i] = BlobEntry{
				SHA256:       sha,
				DataOffset:   start,
				StoredLength: storedLen,
				FinalLength:  finalLen,
				Encoding:     encoding,
			}
		}

		return &PackFile{
			Version: VersionV2,
			Slots:   slots,
			Flags:   flags,
			Entries: entries,
		}, nil
	}

	return nil, fmt.Errorf("unsupported pack version: %d", hdr.Version)
}

// DecodeBlobFromPayload reads the per-blob item header and returns a ByteStream for the blob payload.
// It is the caller's responsibility to verify the SHA256 if desired.
func DecodeBlobFromPayload(r io.Reader, entry BlobEntry) (ByteStream, error) {
	itemHdr := make([]byte, BlobItemHeaderSize)
	if _, err := io.ReadFull(r, itemHdr); err != nil {
		return nil, fmt.Errorf("failed to read blob item header: %w", err)
	}

	encoding := binary.BigEndian.Uint32(itemHdr[0:4])
	finalLen := int64(binary.BigEndian.Uint32(itemHdr[4:8]))

	payloadStoredLen := int64(entry.StoredLength) - BlobItemHeaderSize
	if payloadStoredLen < 0 {
		return nil, fmt.Errorf("invalid stored length %d", entry.StoredLength)
	}

	limited := io.LimitReader(r, payloadStoredLen)
	return decodePayloadToByteStream(limited, encoding, finalLen)
}

// Manifest describes a chunked file consisting of fixed-size chunk blobs.
type Manifest struct {
	ChunkSize   uint32
	TotalLength uint64
	Chunks      [][32]byte
}

// ChunkHexSHAs returns the hex string representations of the chunk SHA256 hashes.
func (m *Manifest) ChunkHexSHAs() []string {
	res := make([]string, len(m.Chunks))
	for i, c := range m.Chunks {
		res[i] = hex.EncodeToString(c[:])
	}
	return res
}

// EncodeManifest encodes a Manifest into its binary representation and returns an EncodedBlob.
func EncodeManifest(m *Manifest) (*EncodedBlob, error) {
	n := len(m.Chunks)
	buf := make([]byte, ManifestHeaderSize+n*32)
	binary.BigEndian.PutUint32(buf[0:4], m.ChunkSize)
	binary.BigEndian.PutUint32(buf[4:8], 0) // Reserved
	binary.BigEndian.PutUint64(buf[8:16], m.TotalLength)
	binary.BigEndian.PutUint32(buf[16:20], uint32(n))
	binary.BigEndian.PutUint32(buf[20:24], 0) // Reserved2

	for i, c := range m.Chunks {
		copy(buf[ManifestHeaderSize+i*32:ManifestHeaderSize+(i+1)*32], c[:])
	}

	h := sha256.Sum256(buf)
	return &EncodedBlob{
		Stream:       NewByteStreamFromBytes(buf),
		StoredLength: uint64(len(buf)),
		FinalLength:  uint64(len(buf)),
		SHA256:       h,
		Encoding:     EncodingChunked,
	}, nil
}

// DecodeManifest parses a Manifest binary representation from r.
func DecodeManifest(r io.Reader) (*Manifest, error) {
	hdr := make([]byte, ManifestHeaderSize)
	if _, err := io.ReadFull(r, hdr); err != nil {
		return nil, fmt.Errorf("failed to read manifest header: %w", err)
	}

	chunkSize := binary.BigEndian.Uint32(hdr[0:4])
	totalLength := binary.BigEndian.Uint64(hdr[8:16])
	count := int(binary.BigEndian.Uint32(hdr[16:20]))

	chunksBuf := make([]byte, count*32)
	if _, err := io.ReadFull(r, chunksBuf); err != nil {
		return nil, fmt.Errorf("failed to read chunk SHAs: %w", err)
	}

	chunks := make([][32]byte, count)
	for i := 0; i < count; i++ {
		copy(chunks[i][:], chunksBuf[i*32:(i+1)*32])
	}

	return &Manifest{
		ChunkSize:   chunkSize,
		TotalLength: totalLength,
		Chunks:      chunks,
	}, nil
}

// ChunkBlobs splits a ByteStream into fixed-size chunk blobs and produces a Manifest.
func ChunkBlobs(src ByteStream, chunkSize uint32, compress bool) ([]*EncodedBlob, *Manifest, *EncodedBlob, error) {
	if chunkSize == 0 {
		return nil, nil, nil, fmt.Errorf("chunkSize must be greater than 0")
	}

	if err := src.Rewind(); err != nil {
		return nil, nil, nil, fmt.Errorf("failed to rewind input stream: %w", err)
	}

	var chunkBlobs []*EncodedBlob
	var chunkSHAs [][32]byte
	var totalLen uint64

	buf := make([]byte, chunkSize)
	for {
		n, err := io.ReadFull(src, buf)
		if n > 0 {
			totalLen += uint64(n)
			chunkData := make([]byte, n)
			copy(chunkData, buf[:n])
			chunkBlob, encodeErr := EncodeBlob(NewByteStreamFromBytes(chunkData), compress)
			if encodeErr != nil {
				return nil, nil, nil, fmt.Errorf("failed encoding chunk: %w", encodeErr)
			}
			chunkBlobs = append(chunkBlobs, chunkBlob)
			chunkSHAs = append(chunkSHAs, chunkBlob.SHA256)
		}
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			break
		}
		if err != nil {
			return nil, nil, nil, fmt.Errorf("failed reading chunk data: %w", err)
		}
	}

	manifest := &Manifest{
		ChunkSize:   chunkSize,
		TotalLength: totalLen,
		Chunks:      chunkSHAs,
	}

	manifestBlob, err := EncodeManifest(manifest)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("failed encoding manifest: %w", err)
	}

	return chunkBlobs, manifest, manifestBlob, nil
}
