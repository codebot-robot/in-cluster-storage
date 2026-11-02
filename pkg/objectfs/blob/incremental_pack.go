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
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"sync"

	"github.com/gke-labs/in-cluster-storage/pkg/errutil"
	"k8s.io/klog/v2"
)

var (
	// ErrPackSlotsFull is returned when all reserved slots in an IncrementalPack are occupied.
	ErrPackSlotsFull = errors.New("incremental pack slots are full")
)

// IncrementalPack represents an open version-2 packfile constructed incrementally.
// Chunks are appended to the data section in arrival order, the in-place sorted table is updated,
// and when sealed, the packfile is ready for direct upload to object storage without repacking.
type IncrementalPack struct {
	mu      sync.Mutex
	path    string
	file    *os.File
	ws      io.WriteSeeker
	slots   uint32
	offset  int64
	sealed  bool
	entries []BlobEntry
}

// NewIncrementalPack creates a new open version-2 packfile on disk at path with reserved slots.
func NewIncrementalPack(path string, slots uint32) (*IncrementalPack, error) {
	if slots == 0 {
		slots = DefaultPackSlots
	}
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return nil, fmt.Errorf("failed creating parent directory for incremental pack %s: %w", path, err)
	}
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed creating incremental pack %s: %w", path, err)
	}
	pack, err := NewIncrementalPackWriter(f, slots)
	if err != nil {
		errutil.CloseAndLog(f, "closing pack file on init error")
		return nil, err
	}
	pack.file = f
	pack.path = path
	return pack, nil
}

// NewIncrementalPackWriter creates a new incremental packfile targeting an io.WriteSeeker.
func NewIncrementalPackWriter(ws io.WriteSeeker, slots uint32) (*IncrementalPack, error) {
	if slots == 0 {
		slots = DefaultPackSlots
	}

	p := &IncrementalPack{
		ws:      ws,
		slots:   slots,
		entries: make([]BlobEntry, 0, slots),
	}

	// Write 32-byte v2 header:
	// base header(8) + slots(4) + used(4) + flags(4) + reserved(12)
	hdr := make([]byte, PackV2HeaderSize)
	copy(hdr[0:8], WriteFileHeaderWithVersion(FileTypePack, VersionV2))
	binary.BigEndian.PutUint32(hdr[8:12], slots)
	binary.BigEndian.PutUint32(hdr[12:16], 0) // used = 0
	binary.BigEndian.PutUint32(hdr[16:20], 0) // flags = 0 (open, not sealed)

	if err := p.writeAt(hdr, 0); err != nil {
		return nil, fmt.Errorf("failed writing incremental pack header: %w", err)
	}

	// Zero-fill reserved table slots
	zeroTable := make([]byte, int(slots)*PackV2SlotSize)
	if err := p.writeAt(zeroTable, int64(PackV2HeaderSize)); err != nil {
		return nil, fmt.Errorf("failed reserving incremental pack table: %w", err)
	}

	p.offset = int64(PackV2HeaderSize) + int64(slots)*int64(PackV2SlotSize)
	return p, nil
}

func (p *IncrementalPack) writeAt(b []byte, off int64) error {
	if _, err := p.ws.Seek(off, io.SeekStart); err != nil {
		return err
	}
	_, err := p.ws.Write(b)
	return err
}

// Append encodes raw chunk data with compression and appends it to the pack.
func (p *IncrementalPack) Append(data []byte) (BlobEntry, error) {
	stream := NewByteStreamFromBytes(data)
	prep, err := EncodeBlob(stream, true)
	if err != nil {
		return BlobEntry{}, fmt.Errorf("failed to encode chunk for incremental pack: %w", err)
	}
	defer errutil.CloseAndLog(prep, "encoded blob")
	return p.AppendBlob(prep)
}

// AppendBlob appends an EncodedBlob to the pack data section and updates the sorted table.
func (p *IncrementalPack) AppendBlob(prep *EncodedBlob) (BlobEntry, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.sealed {
		return BlobEntry{}, errors.New("incremental pack is sealed")
	}

	if existing, ok := p.lookupLocked(prep.SHA256); ok {
		return existing, nil
	}

	if uint32(len(p.entries)) >= p.slots {
		return BlobEntry{}, ErrPackSlotsFull
	}

	if err := prep.Stream.Rewind(); err != nil {
		return BlobEntry{}, fmt.Errorf("failed rewinding blob stream: %w", err)
	}

	itemHdrOffset := p.offset
	payloadStart := itemHdrOffset + int64(BlobItemHeaderSize)

	itemHdr := make([]byte, BlobItemHeaderSize)
	binary.BigEndian.PutUint32(itemHdr[0:4], prep.Encoding)
	binary.BigEndian.PutUint32(itemHdr[4:8], uint32(prep.FinalLength))
	binary.BigEndian.PutUint64(itemHdr[8:16], 0)

	if err := p.writeAt(itemHdr, itemHdrOffset); err != nil {
		return BlobEntry{}, fmt.Errorf("failed writing blob item header: %w", err)
	}

	if _, err := p.ws.Seek(payloadStart, io.SeekStart); err != nil {
		return BlobEntry{}, fmt.Errorf("failed seeking to payload offset: %w", err)
	}
	if _, err := io.Copy(p.ws, prep.Stream); err != nil {
		return BlobEntry{}, fmt.Errorf("failed writing blob payload: %w", err)
	}

	p.offset = payloadStart + int64(prep.StoredLength)

	newEntry := BlobEntry{
		SHA256:       prep.SHA256,
		DataOffset:   uint32(payloadStart),
		StoredLength: uint32(prep.StoredLength),
		FinalLength:  uint32(prep.FinalLength),
		Encoding:     prep.Encoding,
	}

	idx := sort.Search(len(p.entries), func(i int) bool {
		return bytes.Compare(p.entries[i].SHA256[:], newEntry.SHA256[:]) >= 0
	})
	p.entries = append(p.entries, BlobEntry{})
	copy(p.entries[idx+1:], p.entries[idx:])
	p.entries[idx] = newEntry
	n := len(p.entries)

	// Rewrite table slots from insertion index onward
	tableBuf := make([]byte, (n-idx)*PackV2SlotSize)
	for i := idx; i < n; i++ {
		slotOff := (i - idx) * PackV2SlotSize
		ent := p.entries[i]
		copy(tableBuf[slotOff:slotOff+32], ent.SHA256[:])
		binary.BigEndian.PutUint32(tableBuf[slotOff+32:slotOff+36], ent.DataOffset)
		binary.BigEndian.PutUint32(tableBuf[slotOff+36:slotOff+40], ent.StoredLength)
		binary.BigEndian.PutUint32(tableBuf[slotOff+40:slotOff+44], ent.FinalLength)
		binary.BigEndian.PutUint32(tableBuf[slotOff+44:slotOff+48], ent.Encoding)
	}

	writeOffset := int64(PackV2HeaderSize) + int64(idx)*int64(PackV2SlotSize)
	if err := p.writeAt(tableBuf, writeOffset); err != nil {
		return BlobEntry{}, fmt.Errorf("failed updating table: %w", err)
	}

	var countBuf [4]byte
	binary.BigEndian.PutUint32(countBuf[:], uint32(n))
	if err := p.writeAt(countBuf[:], 12); err != nil {
		return BlobEntry{}, fmt.Errorf("failed updating used count: %w", err)
	}

	return newEntry, nil
}

// Seal marks the pack sealed and updates the header flag.
func (p *IncrementalPack) Seal() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.sealed {
		return nil
	}

	if uint32(len(p.entries)) == p.slots {
		packSlotsFullMu.Lock()
		packSlotsFullCount++
		count := packSlotsFullCount
		packSlotsFullMu.Unlock()
		klog.V(2).Infof("packfile sealed with all %d slots full (total full packs: %d)", p.slots, count)
	}

	var flagBuf [4]byte
	binary.BigEndian.PutUint32(flagBuf[:], PackFlagSealed)
	if err := p.writeAt(flagBuf[:], 16); err != nil {
		return fmt.Errorf("failed writing sealed flag to header: %w", err)
	}

	p.sealed = true
	if _, err := p.ws.Seek(p.offset, io.SeekStart); err != nil {
		return fmt.Errorf("failed seeking to pack end: %w", err)
	}
	return nil
}

func (p *IncrementalPack) lookupLocked(target [32]byte) (BlobEntry, bool) {
	idx := sort.Search(len(p.entries), func(i int) bool {
		return bytes.Compare(p.entries[i].SHA256[:], target[:]) >= 0
	})
	if idx < len(p.entries) && p.entries[idx].SHA256 == target {
		return p.entries[idx], true
	}
	return BlobEntry{}, false
}

// Lookup finds a blob by SHA256 in the packfile.
func (p *IncrementalPack) Lookup(target [32]byte) (BlobEntry, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.lookupLocked(target)
}

// ReadBlob reads and decodes the blob payload corresponding to entry.
func (p *IncrementalPack) ReadBlob(entry BlobEntry) ([]byte, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	buf := make([]byte, entry.StoredLength)
	if p.file != nil {
		if _, err := p.file.ReadAt(buf, int64(entry.DataOffset)); err != nil {
			return nil, fmt.Errorf("failed reading blob from file: %w", err)
		}
	} else if ra, ok := p.ws.(io.ReaderAt); ok {
		if _, err := ra.ReadAt(buf, int64(entry.DataOffset)); err != nil {
			return nil, fmt.Errorf("failed reading blob from writer: %w", err)
		}
	} else {
		return nil, errors.New("cannot read blob from non-reader pack")
	}

	stream, err := decodePayloadToByteStream(bytes.NewReader(buf), entry.Encoding, int64(entry.FinalLength))
	if err != nil {
		return nil, err
	}
	defer errutil.CloseAndLog(stream, "decoded stream")
	return io.ReadAll(stream)
}

// Entries returns a copy of all current blob entries.
func (p *IncrementalPack) Entries() []BlobEntry {
	p.mu.Lock()
	defer p.mu.Unlock()
	res := make([]BlobEntry, len(p.entries))
	copy(res, p.entries)
	return res
}

// Count returns the number of blobs appended.
func (p *IncrementalPack) Count() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.entries)
}

// ByteSize returns the current total size of the packfile in bytes.
func (p *IncrementalPack) ByteSize() int64 {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.offset
}

// Path returns the on-disk file path, if file-backed.
func (p *IncrementalPack) Path() string {
	return p.path
}

// Sealed returns true if the packfile has been sealed.
func (p *IncrementalPack) Sealed() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.sealed
}

// Close closes the underlying file, if file-backed.
func (p *IncrementalPack) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.file != nil {
		err := p.file.Close()
		p.file = nil
		return err
	}
	return nil
}
