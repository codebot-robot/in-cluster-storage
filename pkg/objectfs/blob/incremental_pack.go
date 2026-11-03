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

	"k8s.io/klog/v2"
)

var ErrPackSlotsFull = errors.New("incremental pack slots are full")

type IncrementalPack struct {
	mu      sync.Mutex
	file    *os.File
	slots   uint32
	offset  int64
	sealed  bool
	entries []BlobEntry
}

func NewIncrementalPack(path string, slots uint32) (*IncrementalPack, error) {
	if slots == 0 {
		slots = DefaultPackSlots
	}
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return nil, err
	}
	f, err := os.Create(path)
	if err != nil {
		return nil, err
	}
	hdr := make([]byte, PackV2HeaderSize+int(slots)*PackV2SlotSize)
	copy(hdr, BuildFileHeaderWithVersion(FileTypePack, VersionV2))
	binary.BigEndian.PutUint32(hdr[8:], slots)
	if _, err := f.Write(hdr); err != nil {
		return nil, errors.Join(err, f.Close())
	}
	return &IncrementalPack{file: f, slots: slots, offset: int64(len(hdr))}, nil
}

func (p *IncrementalPack) verifyOpenLocked() error {
	if p.file == nil {
		return errors.New("incremental pack is closed")
	}
	return nil
}

func (p *IncrementalPack) Append(data []byte) (BlobEntry, error) {
	blob, err := EncodeBlob(NewByteStreamFromBytes(data), true)
	if err != nil {
		return BlobEntry{}, err
	}
	ent, err := p.appendBlob(blob)
	return ent, errors.Join(err, blob.Close())
}

func (p *IncrementalPack) appendBlob(blob *EncodedBlob) (BlobEntry, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if err := p.verifyOpenLocked(); err != nil {
		return BlobEntry{}, err
	}
	if p.sealed {
		return BlobEntry{}, errors.New("incremental pack is sealed")
	}
	if existing, ok := p.lookupLocked(blob.SHA256); ok {
		return existing, nil
	}
	if uint32(len(p.entries)) >= p.slots {
		return BlobEntry{}, ErrPackSlotsFull
	}
	if err := blob.Stream.Rewind(); err != nil {
		return BlobEntry{}, err
	}
	payloadStart := p.offset + int64(BlobItemHeaderSize)
	itemHdr := buildItemHeader(blob.Encoding, uint32(blob.FinalLength))
	if _, err := p.file.Seek(p.offset, io.SeekStart); err != nil {
		return BlobEntry{}, err
	}
	if _, err := io.Copy(p.file, io.MultiReader(bytes.NewReader(itemHdr), blob.Stream)); err != nil {
		return BlobEntry{}, err
	}
	p.offset = payloadStart + int64(blob.StoredLength)
	ent := BlobEntry{blob.SHA256, uint32(payloadStart), uint32(blob.StoredLength), uint32(blob.FinalLength), blob.Encoding}
	if _, err := p.file.WriteAt(buildSlot(ent), int64(PackV2HeaderSize+len(p.entries)*PackV2SlotSize)); err != nil {
		return BlobEntry{}, err
	}
	p.entries = append(p.entries, ent)
	return ent, nil
}

func (p *IncrementalPack) lookupLocked(target [32]byte) (BlobEntry, bool) {
	for _, entry := range p.entries {
		if entry.SHA256 == target {
			return entry, true
		}
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
func (p *IncrementalPack) ReadBlob(entry BlobEntry) (ByteStream, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if err := p.verifyOpenLocked(); err != nil {
		return nil, err
	}
	sectionReader := io.NewSectionReader(p.file, int64(entry.DataOffset), int64(entry.StoredLength))
	return decodePayloadToByteStream(sectionReader, entry.Encoding, int64(entry.FinalLength))
}

func (p *IncrementalPack) writeTo(w io.Writer) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if err := p.verifyOpenLocked(); err != nil {
		return err
	}
	if _, err := p.file.Seek(0, io.SeekStart); err != nil {
		return fmt.Errorf("failed seeking packfile: %w", err)
	}
	if _, err := io.Copy(w, p.file); err != nil {
		return fmt.Errorf("failed writing packfile to destination: %w", err)
	}
	return nil
}

func buildItemHeader(encoding uint32, finalLength uint32) []byte {
	hdr := make([]byte, BlobItemHeaderSize)
	binary.BigEndian.PutUint32(hdr[0:4], encoding)
	binary.BigEndian.PutUint32(hdr[4:8], finalLength)
	return hdr
}

func buildSlot(e BlobEntry) []byte {
	buf := make([]byte, PackV2SlotSize)
	copy(buf[0:32], e.SHA256[:])
	binary.BigEndian.PutUint32(buf[32:36], e.DataOffset)
	binary.BigEndian.PutUint32(buf[36:40], e.StoredLength)
	binary.BigEndian.PutUint32(buf[40:44], e.FinalLength)
	binary.BigEndian.PutUint32(buf[44:48], e.Encoding)
	return buf
}

// Seal marks the pack sealed without syncing; caller syncs if durability is needed.
func (p *IncrementalPack) Seal() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.sealed {
		return nil
	}
	if err := p.verifyOpenLocked(); err != nil {
		return err
	}
	if uint32(len(p.entries)) == p.slots {
		packSlotsFullMu.Lock()
		packSlotsFullCount++
		count := packSlotsFullCount
		packSlotsFullMu.Unlock()
		klog.V(2).Infof("packfile sealed with all %d slots full (total full packs: %d)", p.slots, count)
	}
	p.sealed = true
	sort.Slice(p.entries, func(i, j int) bool {
		return bytes.Compare(p.entries[i].SHA256[:], p.entries[j].SHA256[:]) < 0
	})
	tableBuf := make([]byte, len(p.entries)*PackV2SlotSize)
	for i, e := range p.entries {
		copy(tableBuf[i*PackV2SlotSize:], buildSlot(e))
	}
	if _, err := p.file.WriteAt(tableBuf, PackV2HeaderSize); err != nil {
		return err
	}
	hdr := make([]byte, 8)
	binary.BigEndian.PutUint32(hdr[0:4], uint32(len(p.entries)))
	binary.BigEndian.PutUint32(hdr[4:8], PackFlagSealed)
	_, err := p.file.WriteAt(hdr, 12)
	return err
}

func (p *IncrementalPack) Entries() []BlobEntry {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]BlobEntry(nil), p.entries...)
}

func (p *IncrementalPack) Count() int      { p.mu.Lock(); defer p.mu.Unlock(); return len(p.entries) }
func (p *IncrementalPack) ByteSize() int64 { p.mu.Lock(); defer p.mu.Unlock(); return p.offset }

func (p *IncrementalPack) Close() (err error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.file != nil {
		p.file, err = nil, p.file.Close()
	}
	return
}
