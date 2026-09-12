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
	"hash"
	"io"
	"os"
	"sort"
)

const (
	// Magic is the 4-byte header identifier "OBJB" (0x4F424A42).
	Magic uint32 = 0x4F424A42

	// Version is the current file format version.
	Version uint8 = 1

	// File Types
	FileTypeBlob uint8 = 1 // Standalone large blob
	FileTypePack uint8 = 2 // Packfile containing multiple blobs

	// Encoding Types
	EncodingRaw        uint32 = 0 // Uncompressed raw binary
	EncodingCompressed uint32 = 1 // Standard zlib compression
	EncodingChunked    uint32 = 2 // Chunked encoding (list of chunk SHA256s)

	// FileHeaderSize is the fixed size of the base file header (8 bytes).
	FileHeaderSize = 8

	// SingleBlobHeaderSize is the total size of the standalone blob header (64 bytes).
	SingleBlobHeaderSize = 64

	// PackHeaderSize is the fixed header of a packfile:
	// 8 bytes (base header) + 4 bytes (count N) + 4 bytes (reserved) = 16 bytes.
	PackHeaderSize = 16

	// MemoryThreshold is the maximum blob size (256KB) to buffer in memory during encoding.
	// Larger inputs use temporary files on disk to prevent unbounded memory consumption.
	MemoryThreshold int64 = 256 * 1024

	// DefaultLargeBlobThreshold is 64MB (blobs larger than 64MB are stored standalone).
	DefaultLargeBlobThreshold int64 = 64 * 1024 * 1024
)

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
	DataOffset   uint64
	StoredLength uint64
	FinalLength  uint64
	Encoding     uint32
	Reserved     uint32
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
	if ver != Version {
		return FileHeader{}, fmt.Errorf("unsupported version %d (expected %d)", ver, Version)
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

// WriteFileHeader writes the 8-byte base file header into a buffer.
func WriteFileHeader(fileType uint8) []byte {
	buf := make([]byte, FileHeaderSize)
	binary.BigEndian.PutUint32(buf[0:4], Magic)
	buf[4] = Version
	buf[5] = fileType
	buf[6] = 0
	buf[7] = 0
	return buf
}

// tempFileReadCloser wraps an *os.File so that Close() automatically removes the temp file.
type tempFileReadCloser struct {
	file *os.File
	path string
}

func (t *tempFileReadCloser) Read(p []byte) (int, error) {
	return t.file.Read(p)
}

func (t *tempFileReadCloser) Close() error {
	err := t.file.Close()
	_ = os.Remove(t.path)
	return err
}

// PreparedPayload holds the encoded (possibly compressed) blob payload ready for streaming.
type PreparedPayload struct {
	Reader       io.ReadCloser
	StoredLength uint64
	FinalLength  uint64
	SHA256       [32]byte
	Encoding     uint32
}

// PrepareBlobPayload reads from src, computing sha256 and compressing if requested.
// If payload size exceeds 256KB, a temporary file is used to avoid high memory buffering.
func PrepareBlobPayload(src io.Reader, compress bool) (*PreparedPayload, error) {
	hasher := sha256.New()
	tee := io.TeeReader(src, hasher)

	// First buffer up to MemoryThreshold in memory
	var memBuf bytes.Buffer
	var rawLength int64
	var tempFile *os.File
	var tempPath string
	var cleanupTemp = func() {
		if tempFile != nil {
			_ = tempFile.Close()
			_ = os.Remove(tempPath)
		}
	}

	var rawWriter io.Writer = &memBuf
	buf := make([]byte, 32*1024)

	for {
		nr, rErr := tee.Read(buf)
		if nr > 0 {
			rawLength += int64(nr)
			if tempFile == nil && rawLength > MemoryThreshold {
				// Switch to temp file
				tf, err := os.CreateTemp("", "objectfs-blob-raw-*")
				if err != nil {
					return nil, fmt.Errorf("failed to create temp file: %w", err)
				}
				tempFile = tf
				tempPath = tf.Name()
				if _, err := tf.Write(memBuf.Bytes()); err != nil {
					cleanupTemp()
					return nil, fmt.Errorf("failed to spill to temp file: %w", err)
				}
				memBuf.Reset()
				rawWriter = tf
			}
			if _, err := rawWriter.Write(buf[:nr]); err != nil {
				cleanupTemp()
				return nil, fmt.Errorf("write payload failed: %w", err)
			}
		}
		if rErr != nil {
			if rErr == io.EOF {
				break
			}
			cleanupTemp()
			return nil, fmt.Errorf("read payload error: %w", rErr)
		}
	}

	var finalSHA [32]byte
	copy(finalSHA[:], hasher.Sum(nil))

	// If compression not requested or 0 bytes
	if !compress || rawLength == 0 {
		if tempFile != nil {
			if _, err := tempFile.Seek(0, io.SeekStart); err != nil {
				cleanupTemp()
				return nil, err
			}
			return &PreparedPayload{
				Reader:       &tempFileReadCloser{file: tempFile, path: tempPath},
				StoredLength: uint64(rawLength),
				FinalLength:  uint64(rawLength),
				SHA256:       finalSHA,
				Encoding:     EncodingRaw,
			}, nil
		}
		return &PreparedPayload{
			Reader:       io.NopCloser(bytes.NewReader(memBuf.Bytes())),
			StoredLength: uint64(rawLength),
			FinalLength:  uint64(rawLength),
			SHA256:       finalSHA,
			Encoding:     EncodingRaw,
		}, nil
	}

	// Try compression
	var rawReader io.Reader
	if tempFile != nil {
		if _, err := tempFile.Seek(0, io.SeekStart); err != nil {
			cleanupTemp()
			return nil, err
		}
		rawReader = tempFile
	} else {
		rawReader = bytes.NewReader(memBuf.Bytes())
	}

	// Compress to a separate buffer / file
	var compMem bytes.Buffer
	var compWriter io.Writer = &compMem

	zw := zlib.NewWriter(compWriter)
	var compLength int64

	compBuf := make([]byte, 32*1024)
	for {
		nr, rErr := rawReader.Read(compBuf)
		if nr > 0 {
			if _, err := zw.Write(compBuf[:nr]); err != nil {
				_ = zw.Close()
				cleanupTemp()
				return nil, fmt.Errorf("compression failed: %w", err)
			}
		}
		if rErr != nil {
			if rErr == io.EOF {
				break
			}
			_ = zw.Close()
			cleanupTemp()
			return nil, rErr
		}
	}
	if err := zw.Close(); err != nil {
		cleanupTemp()
		return nil, fmt.Errorf("compression close failed: %w", err)
	}

	compLength = int64(compMem.Len())
	// Only use compressed if it actually reduced size
	if compLength < rawLength {
		// Clean up raw temp file if exists
		cleanupTemp()

		// If comp is large, spill if needed
		if compLength > MemoryThreshold {
			ctf, err := os.CreateTemp("", "objectfs-blob-comp-*")
			if err != nil {
				return nil, err
			}
			if _, err := ctf.Write(compMem.Bytes()); err != nil {
				_ = ctf.Close()
				_ = os.Remove(ctf.Name())
				return nil, err
			}
			if _, err := ctf.Seek(0, io.SeekStart); err != nil {
				_ = ctf.Close()
				_ = os.Remove(ctf.Name())
				return nil, err
			}
			return &PreparedPayload{
				Reader:       &tempFileReadCloser{file: ctf, path: ctf.Name()},
				StoredLength: uint64(compLength),
				FinalLength:  uint64(rawLength),
				SHA256:       finalSHA,
				Encoding:     EncodingCompressed,
			}, nil
		}

		return &PreparedPayload{
			Reader:       io.NopCloser(bytes.NewReader(compMem.Bytes())),
			StoredLength: uint64(compLength),
			FinalLength:  uint64(rawLength),
			SHA256:       finalSHA,
			Encoding:     EncodingCompressed,
		}, nil
	}

	// Keep raw
	if tempFile != nil {
		if _, err := tempFile.Seek(0, io.SeekStart); err != nil {
			cleanupTemp()
			return nil, err
		}
		return &PreparedPayload{
			Reader:       &tempFileReadCloser{file: tempFile, path: tempPath},
			StoredLength: uint64(rawLength),
			FinalLength:  uint64(rawLength),
			SHA256:       finalSHA,
			Encoding:     EncodingRaw,
		}, nil
	}

	return &PreparedPayload{
		Reader:       io.NopCloser(bytes.NewReader(memBuf.Bytes())),
		StoredLength: uint64(rawLength),
		FinalLength:  uint64(rawLength),
		SHA256:       finalSHA,
		Encoding:     EncodingRaw,
	}, nil
}

// EncodeSingleBlob streams a standalone blob file into an io.Reader without buffering large files in memory.
func EncodeSingleBlob(src io.Reader, compress bool) (io.ReadCloser, [32]byte, uint64, error) {
	prepared, err := PrepareBlobPayload(src, compress)
	if err != nil {
		return nil, [32]byte{}, 0, err
	}

	hdr := make([]byte, SingleBlobHeaderSize)
	copy(hdr[0:8], WriteFileHeader(FileTypeBlob))
	binary.BigEndian.PutUint32(hdr[8:12], prepared.Encoding)
	binary.BigEndian.PutUint64(hdr[12:20], prepared.FinalLength)
	copy(hdr[20:52], prepared.SHA256[:])

	totalSize := uint64(SingleBlobHeaderSize) + prepared.StoredLength
	multi := io.MultiReader(bytes.NewReader(hdr), prepared.Reader)
	return &wrappedReadCloser{r: multi, c: prepared.Reader}, prepared.SHA256, totalSize, nil
}

type wrappedReadCloser struct {
	r io.Reader
	c io.Closer
}

func (w *wrappedReadCloser) Read(p []byte) (int, error) {
	return w.r.Read(p)
}

func (w *wrappedReadCloser) Close() error {
	if w.c != nil {
		return w.c.Close()
	}
	return nil
}

// verifyingReadCloser verifies SHA256 and expected length on EOF and Close.
type verifyingReadCloser struct {
	r           io.Reader
	c           io.Closer
	hasher      hash.Hash
	expectedSHA [32]byte
	expectedLen uint64
	readLen     uint64
	verified    bool
}

func (v *verifyingReadCloser) Read(p []byte) (int, error) {
	n, err := v.r.Read(p)
	if n > 0 {
		v.hasher.Write(p[:n])
		v.readLen += uint64(n)
	}
	if err == io.EOF {
		if err := v.verify(); err != nil {
			return n, err
		}
	}
	return n, err
}

func (v *verifyingReadCloser) verify() error {
	if v.verified {
		return nil
	}
	v.verified = true
	if v.readLen != v.expectedLen {
		return fmt.Errorf("length mismatch: read %d, expected %d", v.readLen, v.expectedLen)
	}
	var actualSHA [32]byte
	copy(actualSHA[:], v.hasher.Sum(nil))
	if actualSHA != v.expectedSHA {
		return fmt.Errorf("sha256 mismatch: got %x, expected %x", actualSHA, v.expectedSHA)
	}
	return nil
}

func (v *verifyingReadCloser) Close() error {
	var firstErr error
	if !v.verified {
		firstErr = v.verify()
	}
	if v.c != nil {
		if err := v.c.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

// DecodeSingleBlob parses the header and returns a streaming io.ReadCloser that verifies data on the fly.
func DecodeSingleBlob(r io.Reader) (io.ReadCloser, BlobEntry, error) {
	hdrBuf := make([]byte, SingleBlobHeaderSize)
	if _, err := io.ReadFull(r, hdrBuf); err != nil {
		return nil, BlobEntry{}, fmt.Errorf("failed to read standalone blob header: %w", err)
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
		FinalLength: finalLen,
		Encoding:    encoding,
	}

	var payloadReader io.Reader = r
	var closer io.Closer
	if c, ok := r.(io.Closer); ok {
		closer = c
	}

	switch encoding {
	case EncodingRaw:
		return &verifyingReadCloser{
			r:           payloadReader,
			c:           closer,
			hasher:      sha256.New(),
			expectedSHA: expectedSHA,
			expectedLen: finalLen,
		}, entry, nil
	case EncodingCompressed:
		zr, err := zlib.NewReader(payloadReader)
		if err != nil {
			return nil, BlobEntry{}, fmt.Errorf("failed to initialize zlib decompressor: %w", err)
		}
		return &verifyingReadCloser{
			r:           zr,
			c:           &multiCloser{c1: zr, c2: closer},
			hasher:      sha256.New(),
			expectedSHA: expectedSHA,
			expectedLen: finalLen,
		}, entry, nil
	default:
		return nil, BlobEntry{}, fmt.Errorf("unsupported encoding: %d", encoding)
	}
}

type multiCloser struct {
	c1 io.Closer
	c2 io.Closer
}

func (m *multiCloser) Close() error {
	var err1, err2 error
	if m.c1 != nil {
		err1 = m.c1.Close()
	}
	if m.c2 != nil {
		err2 = m.c2.Close()
	}
	if err1 != nil {
		return err1
	}
	return err2
}

// RawBlobInput holds input data to be packed.
type RawBlobInput struct {
	Reader   io.Reader
	Compress bool
}

// EncodePack creates a packfile with column-oriented dense tables:
// [Header 16B: 8B base + 4B count N + 4B reserved]
// [Table 1: N * 32B SHAs (sorted)]
// [Table 2: N * 8B DataOffsets]
// [Table 3: N * 8B StoredLengths]
// [Table 4: N * 8B FinalLengths]
// [Table 5: N * 4B Encodings]
// [Table 6: N * 4B Reserved]
// [Data Payload Section...]
func EncodePack(inputs []RawBlobInput) (io.ReadCloser, []BlobEntry, uint64, error) {
	n := len(inputs)
	if n == 0 {
		return nil, nil, 0, errors.New("cannot create empty packfile")
	}

	preparedList := make([]*PreparedPayload, n)
	for i, in := range inputs {
		prep, err := PrepareBlobPayload(in.Reader, in.Compress)
		if err != nil {
			// Clean up previous
			for j := 0; j < i; j++ {
				_ = preparedList[j].Reader.Close()
			}
			return nil, nil, 0, fmt.Errorf("failed preparing blob %d: %w", i, err)
		}
		preparedList[i] = prep
	}

	// Sort prepared entries by SHA256
	sort.Slice(preparedList, func(i, j int) bool {
		return bytes.Compare(preparedList[i].SHA256[:], preparedList[j].SHA256[:]) < 0
	})

	// Calculate table layout
	// Table sizes:
	// SHAs: N * 32
	// Offsets: N * 8
	// StoredLengths: N * 8
	// FinalLengths: N * 8
	// Encodings: N * 4
	// Reserved: N * 4
	// Total tables = N * (32 + 8 + 8 + 8 + 4 + 4) = N * 64
	tablesSize := uint64(n * 64)
	dataSectionOffset := uint64(PackHeaderSize) + tablesSize

	currentDataOffset := dataSectionOffset
	entries := make([]BlobEntry, n)

	// Build headers and tables in memory
	tableBuf := make([]byte, uint64(PackHeaderSize)+tablesSize)
	copy(tableBuf[0:8], WriteFileHeader(FileTypePack))
	binary.BigEndian.PutUint32(tableBuf[8:12], uint32(n))
	binary.BigEndian.PutUint32(tableBuf[12:16], 0) // reserved

	shaOff := uint64(PackHeaderSize)
	offsetOff := shaOff + uint64(n*32)
	storedLenOff := offsetOff + uint64(n*8)
	finalLenOff := storedLenOff + uint64(n*8)
	encOff := finalLenOff + uint64(n*8)
	resOff := encOff + uint64(n*4)

	readers := make([]io.Reader, 0, 1+n)
	closers := make([]io.Closer, 0, n)
	readers = append(readers, bytes.NewReader(tableBuf))

	for i, prep := range preparedList {
		entries[i] = BlobEntry{
			SHA256:       prep.SHA256,
			DataOffset:   currentDataOffset,
			StoredLength: prep.StoredLength,
			FinalLength:  prep.FinalLength,
			Encoding:     prep.Encoding,
			Reserved:     0,
		}

		copy(tableBuf[shaOff+uint64(i*32):shaOff+uint64(i*32)+32], prep.SHA256[:])
		binary.BigEndian.PutUint64(tableBuf[offsetOff+uint64(i*8):offsetOff+uint64(i*8)+8], currentDataOffset)
		binary.BigEndian.PutUint64(tableBuf[storedLenOff+uint64(i*8):storedLenOff+uint64(i*8)+8], prep.StoredLength)
		binary.BigEndian.PutUint64(tableBuf[finalLenOff+uint64(i*8):finalLenOff+uint64(i*8)+8], prep.FinalLength)
		binary.BigEndian.PutUint32(tableBuf[encOff+uint64(i*4):encOff+uint64(i*4)+4], prep.Encoding)
		binary.BigEndian.PutUint32(tableBuf[resOff+uint64(i*4):resOff+uint64(i*4)+4], 0)

		currentDataOffset += prep.StoredLength
		readers = append(readers, prep.Reader)
		closers = append(closers, prep.Reader)
	}

	totalPackSize := currentDataOffset
	multiReader := io.MultiReader(readers...)
	return &packReadCloser{r: multiReader, closers: closers}, entries, totalPackSize, nil
}

type packReadCloser struct {
	r       io.Reader
	closers []io.Closer
}

func (p *packReadCloser) Read(b []byte) (int, error) {
	return p.r.Read(b)
}

func (p *packReadCloser) Close() error {
	var firstErr error
	for _, c := range p.closers {
		if c != nil {
			if err := c.Close(); err != nil && firstErr == nil {
				firstErr = err
			}
		}
	}
	return firstErr
}

// DecodePackTable reads and parses the pack table from an io.Reader.
func DecodePackTable(r io.Reader) ([]BlobEntry, error) {
	hdrBuf := make([]byte, PackHeaderSize)
	if _, err := io.ReadFull(r, hdrBuf); err != nil {
		return nil, fmt.Errorf("failed to read pack header: %w", err)
	}

	hdr, err := ParseFileHeader(hdrBuf[0:8])
	if err != nil {
		return nil, err
	}
	if hdr.FileType != FileTypePack {
		return nil, fmt.Errorf("expected filetype %d for pack, got %d", FileTypePack, hdr.FileType)
	}

	count := binary.BigEndian.Uint32(hdrBuf[8:12])
	n := int(count)
	tablesSize := n * 64
	tablesBuf := make([]byte, tablesSize)
	if _, err := io.ReadFull(r, tablesBuf); err != nil {
		return nil, fmt.Errorf("failed to read pack tables: %w", err)
	}

	shaOff := 0
	offsetOff := shaOff + n*32
	storedLenOff := offsetOff + n*8
	finalLenOff := storedLenOff + n*8
	encOff := finalLenOff + n*8
	resOff := encOff + n*4

	entries := make([]BlobEntry, n)
	for i := 0; i < n; i++ {
		var sha [32]byte
		copy(sha[:], tablesBuf[shaOff+i*32:shaOff+i*32+32])
		entries[i] = BlobEntry{
			SHA256:       sha,
			DataOffset:   binary.BigEndian.Uint64(tablesBuf[offsetOff+i*8 : offsetOff+i*8+8]),
			StoredLength: binary.BigEndian.Uint64(tablesBuf[storedLenOff+i*8 : storedLenOff+i*8+8]),
			FinalLength:  binary.BigEndian.Uint64(tablesBuf[finalLenOff+i*8 : finalLenOff+i*8+8]),
			Encoding:     binary.BigEndian.Uint32(tablesBuf[encOff+i*4 : encOff+i*4+4]),
			Reserved:     binary.BigEndian.Uint32(tablesBuf[resOff+i*4 : resOff+i*4+4]),
		}
	}

	return entries, nil
}

// LookupInEntries performs binary search for a SHA256 in a sorted BlobEntry list.
func LookupInEntries(entries []BlobEntry, target [32]byte) (BlobEntry, bool) {
	idx := sort.Search(len(entries), func(i int) bool {
		return bytes.Compare(entries[i].SHA256[:], target[:]) >= 0
	})
	if idx < len(entries) && entries[idx].SHA256 == target {
		return entries[idx], true
	}
	return BlobEntry{}, false
}

// DecodeBlobFromPayload returns a streaming io.ReadCloser for a blob payload.
func DecodeBlobFromPayload(r io.Reader, entry BlobEntry) (io.ReadCloser, error) {
	limited := io.LimitReader(r, int64(entry.StoredLength))
	var closer io.Closer
	if c, ok := r.(io.Closer); ok {
		closer = c
	}

	switch entry.Encoding {
	case EncodingRaw:
		return &verifyingReadCloser{
			r:           limited,
			c:           closer,
			hasher:      sha256.New(),
			expectedSHA: entry.SHA256,
			expectedLen: entry.FinalLength,
		}, nil
	case EncodingCompressed:
		zr, err := zlib.NewReader(limited)
		if err != nil {
			return nil, fmt.Errorf("failed to initialize zlib decompressor: %w", err)
		}
		return &verifyingReadCloser{
			r:           zr,
			c:           &multiCloser{c1: zr, c2: closer},
			hasher:      sha256.New(),
			expectedSHA: entry.SHA256,
			expectedLen: entry.FinalLength,
		}, nil
	default:
		return nil, fmt.Errorf("unsupported blob encoding %d", entry.Encoding)
	}
}
