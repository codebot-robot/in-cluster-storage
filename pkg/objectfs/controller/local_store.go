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
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"

	"k8s.io/klog/v2"
)

// LocalOffset represents a 32-bit packed pointer into local metadata eviction storage.
// Bits 28..31 (top 4 bits): File index (0..15).
// Bits 0..27 (lower 28 bits): Byte offset within that file (up to 256MB).
type LocalOffset uint32

const (
	NoOffset LocalOffset = 0

	MaxLocalFiles      = 16
	MaxFileSizeInBytes = 256 * 1024 * 1024 // 256 MB per file (28 bits)
	MaxRecordPayload   = 1 * 1024 * 1024   // 1 MB maximum payload size sanity check for metadata records
	LocalFileHeader    = "OBJSLOG1"
	HeaderLen          = 8
)

const (
	RecordTypeInode    byte = 1
	RecordTypeDirDelta byte = 2
)

func PackOffset(fileID int, byteOffset int64) LocalOffset {
	if byteOffset <= 0 || byteOffset > MaxFileSizeInBytes {
		return NoOffset
	}
	return (LocalOffset(fileID&0x0F) << 28) | LocalOffset(byteOffset&0x0FFFFFFF)
}

func UnpackOffset(off LocalOffset) (int, int64) {
	fileID := int((off >> 28) & 0x0F)
	byteOffset := int64(off & 0x0FFFFFFF)
	return fileID, byteOffset
}

// InodeRecord stores serialized metadata for an evicted inode.
type InodeRecord struct {
	InodeID uint64
	Mode    uint32
	Size    int64
	ModTime time.Time
	IsDir   bool
	Sha256  string
	ETag    string
}

// DirEntry represents a single directory entry.
type DirEntry struct {
	Name    string
	InodeID uint64
	IsDir   bool
	Mode    uint32
}

// DirDeltaRecord stores a delta mutation for an evicted directory.
type DirDeltaRecord struct {
	InodeID    uint64
	PrevOffset LocalOffset
	Deleted    []string
	Added      []DirEntry
}

func EncodeInodeRecord(rec *InodeRecord) ([]byte, error) {
	var buf bytes.Buffer
	var b8 [8]byte
	var b4 [4]byte
	var b2 [2]byte

	// InodeID (8)
	binary.BigEndian.PutUint64(b8[:], rec.InodeID)
	buf.Write(b8[:])

	// Mode (4)
	binary.BigEndian.PutUint32(b4[:], rec.Mode)
	buf.Write(b4[:])

	// Size (8)
	binary.BigEndian.PutUint64(b8[:], uint64(rec.Size))
	buf.Write(b8[:])

	// ModTime unix nano (8)
	binary.BigEndian.PutUint64(b8[:], uint64(rec.ModTime.UnixNano()))
	buf.Write(b8[:])

	// IsDir (1)
	if rec.IsDir {
		buf.WriteByte(1)
	} else {
		buf.WriteByte(0)
	}

	// Sha256 length (2) + bytes
	shaBytes := []byte(rec.Sha256)
	binary.BigEndian.PutUint16(b2[:], uint16(len(shaBytes)))
	buf.Write(b2[:])
	buf.Write(shaBytes)

	// ETag length (2) + bytes
	etagBytes := []byte(rec.ETag)
	binary.BigEndian.PutUint16(b2[:], uint16(len(etagBytes)))
	buf.Write(b2[:])
	buf.Write(etagBytes)

	return buf.Bytes(), nil
}

// DecodeInodeRecord decodes an InodeRecord from binary representation.
// TODO: optimize encoding/decoding by parsing slice offsets directly instead of allocating bytes.NewReader wrapper.
func DecodeInodeRecord(data []byte) (*InodeRecord, error) {
	if len(data) < 29 {
		return nil, fmt.Errorf("data too short for InodeRecord: %d bytes", len(data))
	}
	r := bytes.NewReader(data)
	var b8 [8]byte
	var b4 [4]byte
	var b2 [2]byte

	if _, err := io.ReadFull(r, b8[:]); err != nil {
		return nil, err
	}
	inodeID := binary.BigEndian.Uint64(b8[:])

	if _, err := io.ReadFull(r, b4[:]); err != nil {
		return nil, err
	}
	mode := binary.BigEndian.Uint32(b4[:])

	if _, err := io.ReadFull(r, b8[:]); err != nil {
		return nil, err
	}
	size := int64(binary.BigEndian.Uint64(b8[:]))

	if _, err := io.ReadFull(r, b8[:]); err != nil {
		return nil, err
	}
	modTimeNano := int64(binary.BigEndian.Uint64(b8[:]))
	modTime := time.Unix(0, modTimeNano)

	isDirByte, err := r.ReadByte()
	if err != nil {
		return nil, err
	}
	isDir := isDirByte != 0

	if _, err := io.ReadFull(r, b2[:]); err != nil {
		return nil, err
	}
	shaLen := int(binary.BigEndian.Uint16(b2[:]))
	shaBytes := make([]byte, shaLen)
	if _, err := io.ReadFull(r, shaBytes); err != nil {
		return nil, err
	}

	if _, err := io.ReadFull(r, b2[:]); err != nil {
		return nil, err
	}
	etagLen := int(binary.BigEndian.Uint16(b2[:]))
	etagBytes := make([]byte, etagLen)
	if _, err := io.ReadFull(r, etagBytes); err != nil {
		return nil, err
	}

	return &InodeRecord{
		InodeID: inodeID,
		Mode:    mode,
		Size:    size,
		ModTime: modTime,
		IsDir:   isDir,
		Sha256:  string(shaBytes),
		ETag:    string(etagBytes),
	}, nil
}

func EncodeDirDeltaRecord(rec *DirDeltaRecord) ([]byte, error) {
	var buf bytes.Buffer
	var b8 [8]byte
	var b4 [4]byte
	var b2 [2]byte

	// InodeID (8)
	binary.BigEndian.PutUint64(b8[:], rec.InodeID)
	buf.Write(b8[:])

	// PrevOffset (4)
	binary.BigEndian.PutUint32(b4[:], uint32(rec.PrevOffset))
	buf.Write(b4[:])

	// Deleted count (4)
	binary.BigEndian.PutUint32(b4[:], uint32(len(rec.Deleted)))
	buf.Write(b4[:])
	for _, del := range rec.Deleted {
		delBytes := []byte(del)
		binary.BigEndian.PutUint16(b2[:], uint16(len(delBytes)))
		buf.Write(b2[:])
		buf.Write(delBytes)
	}

	// Added count (4)
	binary.BigEndian.PutUint32(b4[:], uint32(len(rec.Added)))
	buf.Write(b4[:])
	for _, add := range rec.Added {
		nameBytes := []byte(add.Name)
		binary.BigEndian.PutUint16(b2[:], uint16(len(nameBytes)))
		buf.Write(b2[:])
		buf.Write(nameBytes)

		binary.BigEndian.PutUint64(b8[:], add.InodeID)
		buf.Write(b8[:])

		if add.IsDir {
			buf.WriteByte(1)
		} else {
			buf.WriteByte(0)
		}

		binary.BigEndian.PutUint32(b4[:], add.Mode)
		buf.Write(b4[:])
	}

	return buf.Bytes(), nil
}

// DecodeDirDeltaRecord decodes a DirDeltaRecord from binary representation.
// TODO: optimize encoding/decoding by parsing slice offsets directly instead of allocating bytes.NewReader wrapper.
func DecodeDirDeltaRecord(data []byte) (*DirDeltaRecord, error) {
	if len(data) < 16 {
		return nil, fmt.Errorf("data too short for DirDeltaRecord: %d bytes", len(data))
	}
	r := bytes.NewReader(data)
	var b8 [8]byte
	var b4 [4]byte
	var b2 [2]byte

	if _, err := io.ReadFull(r, b8[:]); err != nil {
		return nil, err
	}
	inodeID := binary.BigEndian.Uint64(b8[:])

	if _, err := io.ReadFull(r, b4[:]); err != nil {
		return nil, err
	}
	prevOffset := LocalOffset(binary.BigEndian.Uint32(b4[:]))

	if _, err := io.ReadFull(r, b4[:]); err != nil {
		return nil, err
	}
	deletedCount := int(binary.BigEndian.Uint32(b4[:]))
	deleted := make([]string, 0, deletedCount)
	for i := 0; i < deletedCount; i++ {
		if _, err := io.ReadFull(r, b2[:]); err != nil {
			return nil, err
		}
		nameLen := int(binary.BigEndian.Uint16(b2[:]))
		nameBytes := make([]byte, nameLen)
		if _, err := io.ReadFull(r, nameBytes); err != nil {
			return nil, err
		}
		deleted = append(deleted, string(nameBytes))
	}

	if _, err := io.ReadFull(r, b4[:]); err != nil {
		return nil, err
	}
	addedCount := int(binary.BigEndian.Uint32(b4[:]))
	added := make([]DirEntry, 0, addedCount)
	for i := 0; i < addedCount; i++ {
		if _, err := io.ReadFull(r, b2[:]); err != nil {
			return nil, err
		}
		nameLen := int(binary.BigEndian.Uint16(b2[:]))
		nameBytes := make([]byte, nameLen)
		if _, err := io.ReadFull(r, nameBytes); err != nil {
			return nil, err
		}

		if _, err := io.ReadFull(r, b8[:]); err != nil {
			return nil, err
		}
		childInodeID := binary.BigEndian.Uint64(b8[:])

		isDirByte, err := r.ReadByte()
		if err != nil {
			return nil, err
		}

		if _, err := io.ReadFull(r, b4[:]); err != nil {
			return nil, err
		}
		mode := binary.BigEndian.Uint32(b4[:])

		added = append(added, DirEntry{
			Name:    string(nameBytes),
			InodeID: childInodeID,
			IsDir:   isDirByte != 0,
			Mode:    mode,
		})
	}

	return &DirDeltaRecord{
		InodeID:    inodeID,
		PrevOffset: prevOffset,
		Deleted:    deleted,
		Added:      added,
	}, nil
}

// LocalStorage manages append-only local files for evicted inodes and directory deltas.
type LocalStorage struct {
	mu           sync.RWMutex
	dir          string
	files        [MaxLocalFiles]*os.File
	oldestFileID int
	activeFileID int
	activeOffset int64
	maxFileSize  int64
}

// LocalStorageOption configures LocalStorage instances.
type LocalStorageOption func(*LocalStorage)

// WithMaxLocalFileSize sets the maximum size in bytes before rotating to the next local buffer file.
func WithMaxLocalFileSize(size int64) LocalStorageOption {
	return func(s *LocalStorage) {
		if size <= 0 || size > MaxFileSizeInBytes {
			s.maxFileSize = MaxFileSizeInBytes
		} else {
			s.maxFileSize = size
		}
	}
}

// NewLocalStorage creates or opens a local storage instance in the specified directory.
func NewLocalStorage(dir string, opts ...LocalStorageOption) (*LocalStorage, error) {
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create local storage directory %s: %w", dir, err)
	}

	s := &LocalStorage{
		dir:          dir,
		oldestFileID: 0,
		activeFileID: 0,
		activeOffset: HeaderLen,
		maxFileSize:  MaxFileSizeInBytes,
	}

	for _, opt := range opts {
		opt(s)
	}

	if s.maxFileSize <= 0 || s.maxFileSize > MaxFileSizeInBytes {
		s.maxFileSize = MaxFileSizeInBytes
	}

	// Create initial active file with O_CREATE|O_EXCL. If it already exists, return the error.
	if _, err := s.createNewFileLocked(0); err != nil {
		return nil, fmt.Errorf("failed to create initial local storage file: %w", err)
	}

	return s, nil
}

func (s *LocalStorage) getFileLocked(fileID int) (*os.File, error) {
	if fileID < 0 || fileID >= MaxLocalFiles {
		return nil, fmt.Errorf("invalid file ID %d", fileID)
	}
	f := s.files[fileID]
	if f == nil {
		return nil, fmt.Errorf("local storage file %d is not open", fileID)
	}
	return f, nil
}

func (s *LocalStorage) createNewFileLocked(fileID int) (*os.File, error) {
	if fileID < 0 || fileID >= MaxLocalFiles {
		return nil, fmt.Errorf("invalid file ID %d", fileID)
	}
	if s.files[fileID] != nil {
		return nil, fmt.Errorf("local storage file %d is already open", fileID)
	}

	filePath := filepath.Join(s.dir, fmt.Sprintf("meta-%02d.dat", fileID))
	f, err := os.OpenFile(filePath, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0644)
	if err != nil {
		return nil, err
	}
	s.files[fileID] = f

	if _, err := f.WriteAt([]byte(LocalFileHeader), 0); err != nil {
		_ = f.Close()
		s.files[fileID] = nil
		if rErr := os.Remove(filePath); rErr != nil {
			klog.Warningf("Failed to clean up uninitialized local storage file %s: %v", filePath, rErr)
		}
		return nil, fmt.Errorf("failed to write header to %s: %w", filePath, err)
	}

	return f, nil
}

// WriteRecord serializes and appends a record of recType with payload to the active local file.
func (s *LocalStorage) WriteRecord(recType byte, payload []byte) (LocalOffset, error) {
	if len(payload) > MaxRecordPayload {
		return NoOffset, fmt.Errorf("payload size %d exceeds max allowed %d bytes", len(payload), MaxRecordPayload)
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	payloadLen := uint32(len(payload))
	recordLen := int64(1 + 4 + 4 + len(payload)) // [type (1B)][len (4B)][crc (4B)][payload]

	// If active file would exceed max size, rotate to next file with O_EXCL to prevent silent overwrites.
	if s.activeOffset+recordLen > s.maxFileSize {
		nextFileID := (s.activeFileID + 1) % MaxLocalFiles
		if s.files[nextFileID] != nil {
			return NoOffset, fmt.Errorf("local storage full: all %d buffer files in use", MaxLocalFiles)
		}
		if _, err := s.createNewFileLocked(nextFileID); err != nil {
			return NoOffset, fmt.Errorf("failed to create rotated local file %d with O_EXCL: %w", nextFileID, err)
		}
		s.activeFileID = nextFileID
		s.activeOffset = HeaderLen
	}

	f, err := s.getFileLocked(s.activeFileID)
	if err != nil {
		return NoOffset, err
	}

	crc := crc32.ChecksumIEEE(payload)
	headerBuf := make([]byte, 9)
	headerBuf[0] = recType
	binary.BigEndian.PutUint32(headerBuf[1:5], payloadLen)
	binary.BigEndian.PutUint32(headerBuf[5:9], crc)

	recordOffset := s.activeOffset
	if _, err := f.WriteAt(headerBuf, recordOffset); err != nil {
		return NoOffset, fmt.Errorf("failed to write record header: %w", err)
	}
	if len(payload) > 0 {
		if _, err := f.WriteAt(payload, recordOffset+9); err != nil {
			return NoOffset, fmt.Errorf("failed to write record payload: %w", err)
		}
	}

	s.activeOffset += recordLen
	return PackOffset(s.activeFileID, recordOffset), nil
}

// ReadRecord retrieves and verifies a record from the specified LocalOffset.
func (s *LocalStorage) ReadRecord(off LocalOffset) (byte, []byte, error) {
	if off == NoOffset {
		return 0, nil, fmt.Errorf("invalid offset %d", off)
	}
	fileID, byteOffset := UnpackOffset(off)
	if fileID < 0 || fileID >= MaxLocalFiles {
		return 0, nil, fmt.Errorf("invalid file ID %d in offset %d", fileID, off)
	}

	s.mu.RLock()
	f := s.files[fileID]
	s.mu.RUnlock()

	if f == nil {
		return 0, nil, fmt.Errorf("local storage file %d is not open", fileID)
	}

	headerBuf := make([]byte, 9)
	if _, err := f.ReadAt(headerBuf, byteOffset); err != nil {
		return 0, nil, fmt.Errorf("failed to read record header at offset %d: %w", off, err)
	}

	recType := headerBuf[0]
	payloadLen := binary.BigEndian.Uint32(headerBuf[1:5])
	expectedCrc := binary.BigEndian.Uint32(headerBuf[5:9])

	if payloadLen > MaxRecordPayload {
		return 0, nil, fmt.Errorf("corrupted record payload length %d exceeds max allowed %d bytes at offset %d", payloadLen, MaxRecordPayload, off)
	}

	payload := make([]byte, payloadLen)
	if payloadLen > 0 {
		if _, err := f.ReadAt(payload, byteOffset+9); err != nil {
			return 0, nil, fmt.Errorf("failed to read record payload at offset %d: %w", off, err)
		}
	}

	actualCrc := crc32.ChecksumIEEE(payload)
	if actualCrc != expectedCrc {
		return 0, nil, fmt.Errorf("CRC mismatch at offset %d: expected %x, got %x", off, expectedCrc, actualCrc)
	}

	return recType, payload, nil
}

// trimBeforeFile closes and deletes all local storage buffer files strictly older than cutoffFileID in circular sequence.
func (s *LocalStorage) trimBeforeFile(cutoffFileID int) error {
	if cutoffFileID < 0 || cutoffFileID >= MaxLocalFiles {
		return fmt.Errorf("invalid cutoff file ID %d", cutoffFileID)
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	for s.oldestFileID != cutoffFileID && s.oldestFileID != s.activeFileID {
		fileID := s.oldestFileID
		if s.files[fileID] != nil {
			if err := s.files[fileID].Close(); err != nil {
				klog.Warningf("Failed to close trimmed local storage file %d: %v", fileID, err)
			}
			s.files[fileID] = nil
			filePath := filepath.Join(s.dir, fmt.Sprintf("meta-%02d.dat", fileID))
			if err := os.Remove(filePath); err != nil && !os.IsNotExist(err) {
				klog.Warningf("Failed to remove trimmed local storage file %s: %v", filePath, err)
			}
		}
		s.oldestFileID = (s.oldestFileID + 1) % MaxLocalFiles
	}
	return nil
}

// TrimBefore closes and deletes all local storage buffer files strictly older than the file ID of cutoff offset.
func (s *LocalStorage) TrimBefore(cutoff LocalOffset) error {
	if cutoff == NoOffset {
		return nil
	}
	cutoffFileID, _ := UnpackOffset(cutoff)
	return s.trimBeforeFile(cutoffFileID)
}

// CurrentOffset returns the current write pointer packed LocalOffset.
func (s *LocalStorage) CurrentOffset() LocalOffset {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return PackOffset(s.activeFileID, s.activeOffset)
}

// FileCount returns the number of currently active/open buffer files.
func (s *LocalStorage) FileCount() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	count := 0
	for i := 0; i < MaxLocalFiles; i++ {
		if s.files[i] != nil {
			count++
		}
	}
	return count
}

// ActiveFileID returns the currently active file ID.
func (s *LocalStorage) ActiveFileID() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.activeFileID
}

// OldestFileID returns the oldest open file ID in the circular buffer.
func (s *LocalStorage) OldestFileID() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.oldestFileID
}

// DeleteAllAndReset closes and deletes all local storage files on disk and resets write pointers.
func (s *LocalStorage) DeleteAllAndReset() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	for i := 0; i < MaxLocalFiles; i++ {
		if s.files[i] != nil {
			_ = s.files[i].Close()
			s.files[i] = nil
		}
		filePath := filepath.Join(s.dir, fmt.Sprintf("meta-%02d.dat", i))
		_ = os.Remove(filePath)
	}
	s.oldestFileID = 0
	s.activeFileID = 0
	s.activeOffset = HeaderLen

	// Re-initialize active file 0
	if _, err := s.createNewFileLocked(0); err != nil {
		return fmt.Errorf("failed to create local storage file 0 on reset: %w", err)
	}
	return nil
}

// ActiveFileSize returns the byte size of the currently active local file.
func (s *LocalStorage) ActiveFileSize() int64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.activeOffset
}

// Close closes all open local storage file descriptors.
func (s *LocalStorage) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	var firstErr error
	for i := 0; i < MaxLocalFiles; i++ {
		if s.files[i] != nil {
			if err := s.files[i].Close(); err != nil && firstErr == nil {
				firstErr = err
			}
			s.files[i] = nil
		}
	}
	return firstErr
}
