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
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"sort"
	"sync"
	"sync/atomic"

	"github.com/gke-labs/in-cluster-storage/pkg/errutil"
	"k8s.io/klog/v2"
)

var (
	// ErrPackSlotsFull is returned when all reserved slots in an IncrementalPack are occupied.
	ErrPackSlotsFull = errors.New("incremental pack slots are full")
)

// IncrementalPack represents an open version-2 packfile constructed incrementally on local disk.
// As chunks arrive, each is appended to the data section, the in-place sorted table is updated,
// and the file is fsynced via group commit before acknowledgement. When sealed, the file is a
// complete, standard v2 packfile ready for direct upload to object storage without repacking.
type IncrementalPack struct {
	id     uint64
	path   string
	file   *os.File
	slots  uint32
	offset int64
	sealed bool

	liveRefs int32
	uploaded bool

	entries []BlobEntry

	mu          sync.Mutex
	syncMu      sync.Mutex
	syncCond    *sync.Cond
	syncing     bool
	pendingSeq  uint64
	fsyncedSeq  uint64
	lastSyncErr error
}

// NewIncrementalPack creates a new open version-2 packfile at path reserving DefaultPackSlots (1024).
func NewIncrementalPack(path string, id uint64) (*IncrementalPack, error) {
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed creating incremental pack %s: %w", path, err)
	}

	slots := DefaultPackSlots
	frontSectionSize := int64(PackV2HeaderSize) + int64(slots)*int64(PackV2SlotSize)

	// Write 32-byte v2 header:
	// base header(8) + slots(4) + used(4) + flags(4) + reserved(12)
	var hdr [PackV2HeaderSize]byte
	copy(hdr[0:8], WriteFileHeaderWithVersion(FileTypePack, VersionV2))
	binary.BigEndian.PutUint32(hdr[8:12], slots)
	binary.BigEndian.PutUint32(hdr[12:16], 0) // used = 0
	binary.BigEndian.PutUint32(hdr[16:20], 0) // flags = 0 (open, not sealed)

	if _, err := f.WriteAt(hdr[:], 0); err != nil {
		errutil.CloseAndLog(f, "incremental pack header write error")
		return nil, fmt.Errorf("failed writing incremental pack header: %w", err)
	}

	// Zero-fill the reserved table slots
	zeroTable := make([]byte, int(slots)*PackV2SlotSize)
	if _, err := f.WriteAt(zeroTable, int64(PackV2HeaderSize)); err != nil {
		errutil.CloseAndLog(f, "incremental pack table reservation error")
		return nil, fmt.Errorf("failed reserving incremental pack table: %w", err)
	}

	if err := fdatasync(f); err != nil {
		errutil.CloseAndLog(f, "incremental pack init sync error")
		return nil, fmt.Errorf("failed syncing incremental pack header: %w", err)
	}

	pack := &IncrementalPack{
		id:      id,
		path:    path,
		file:    f,
		slots:   slots,
		offset:  frontSectionSize,
		entries: make([]BlobEntry, 0, slots),
	}
	pack.syncCond = sync.NewCond(&pack.syncMu)
	return pack, nil
}

// Append appends a chunk to the data section, rewrites the sorted table in place,
// and assigns a group commit sequence number under lock.
func (p *IncrementalPack) Append(data []byte) (BlobEntry, uint64, error) {
	// Encode blob with adaptive per-item compression (10% threshold)
	stream := NewByteStreamFromBytes(data)
	prep, err := EncodeBlob(stream, true)
	if err != nil {
		return BlobEntry{}, 0, fmt.Errorf("failed to encode chunk for incremental pack: %w", err)
	}
	defer errutil.CloseAndLog(prep, "encoded blob")

	payloadBytes, err := io.ReadAll(prep.Stream)
	if err != nil {
		return BlobEntry{}, 0, fmt.Errorf("failed reading encoded payload: %w", err)
	}

	storedLen := uint32(len(payloadBytes))
	finalLen := uint32(prep.FinalLength)
	encoding := prep.Encoding

	// Construct 16-byte BlobItemHeader:
	// encoding(4) + finalLength(4) + storedLength(4) + reserved(4)
	var itemHdr [BlobItemHeaderSize]byte
	binary.BigEndian.PutUint32(itemHdr[0:4], encoding)
	binary.BigEndian.PutUint32(itemHdr[4:8], finalLen)
	binary.BigEndian.PutUint32(itemHdr[8:12], storedLen)

	p.mu.Lock()
	if p.sealed {
		p.mu.Unlock()
		return BlobEntry{}, 0, errors.New("incremental pack is sealed")
	}
	if uint32(len(p.entries)) >= p.slots {
		p.mu.Unlock()
		return BlobEntry{}, 0, ErrPackSlotsFull
	}

	itemHdrOffset := p.offset
	payloadStart := itemHdrOffset + int64(BlobItemHeaderSize)

	// Write item header and payload
	if _, err := p.file.WriteAt(itemHdr[:], itemHdrOffset); err != nil {
		p.mu.Unlock()
		return BlobEntry{}, 0, fmt.Errorf("failed writing blob item header to %s: %w", p.path, err)
	}
	if storedLen > 0 {
		if _, err := p.file.WriteAt(payloadBytes, payloadStart); err != nil {
			p.mu.Unlock()
			return BlobEntry{}, 0, fmt.Errorf("failed writing blob payload to %s: %w", p.path, err)
		}
	}

	p.offset = payloadStart + int64(storedLen)

	newEntry := BlobEntry{
		SHA256:       prep.SHA256,
		DataOffset:   uint32(payloadStart),
		StoredLength: storedLen,
		FinalLength:  finalLen,
		Encoding:     encoding,
	}

	// Insert into sorted entries slice at binary-search insertion index
	idx := sort.Search(len(p.entries), func(i int) bool {
		return bytes.Compare(p.entries[i].SHA256[:], newEntry.SHA256[:]) >= 0
	})
	p.entries = append(p.entries, BlobEntry{})
	copy(p.entries[idx+1:], p.entries[idx:])
	p.entries[idx] = newEntry
	n := len(p.entries)

	// Rewrite table slots from insertion index onward, halving write amplification on average
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
	if _, err := p.file.WriteAt(tableBuf, writeOffset); err != nil {
		p.mu.Unlock()
		return BlobEntry{}, 0, fmt.Errorf("failed updating table in %s: %w", p.path, err)
	}

	// Update used count in header at offset 12
	var countBuf [4]byte
	binary.BigEndian.PutUint32(countBuf[:], uint32(n))
	if _, err := p.file.WriteAt(countBuf[:], 12); err != nil {
		p.mu.Unlock()
		return BlobEntry{}, 0, fmt.Errorf("failed updating used count in %s: %w", p.path, err)
	}

	p.pendingSeq++
	mySeq := p.pendingSeq
	p.mu.Unlock()

	return newEntry, mySeq, nil
}

// SyncToSeq blocks until the packfile has been fsynced up to at least targetSeq using group commit.
func (p *IncrementalPack) SyncToSeq(targetSeq uint64) error {
	p.syncMu.Lock()
	defer p.syncMu.Unlock()

	for p.fsyncedSeq < targetSeq {
		if p.syncing {
			p.syncCond.Wait()
			continue
		}
		p.syncing = true
		p.mu.Lock()
		maxPending := p.pendingSeq
		p.mu.Unlock()

		p.syncMu.Unlock()
		syncErr := fdatasync(p.file)
		p.syncMu.Lock()

		p.syncing = false
		if syncErr != nil {
			p.lastSyncErr = syncErr
			p.syncCond.Broadcast()
			return fmt.Errorf("incremental pack fdatasync failed: %w", syncErr)
		}
		p.lastSyncErr = nil
		if maxPending > p.fsyncedSeq {
			p.fsyncedSeq = maxPending
		}
		p.syncCond.Broadcast()
	}

	if p.lastSyncErr != nil {
		return fmt.Errorf("incremental pack fdatasync failed: %w", p.lastSyncErr)
	}
	return nil
}

// Seal marks the pack sealed, records PackFlagSealed in the header on disk, and unconditionally fsyncs.
func (p *IncrementalPack) Seal() error {
	p.syncMu.Lock()
	for p.syncing {
		p.syncCond.Wait()
	}
	p.syncing = true
	p.syncMu.Unlock()

	// Phase 1: ensure all pending data payloads and table updates are durable
	if err := fdatasync(p.file); err != nil {
		p.syncMu.Lock()
		p.syncing = false
		p.syncCond.Broadcast()
		p.syncMu.Unlock()
		return fmt.Errorf("failed syncing incremental pack before seal %s: %w", p.path, err)
	}

	p.mu.Lock()
	p.sealed = true

	var flagsBuf [4]byte
	binary.BigEndian.PutUint32(flagsBuf[:], PackFlagSealed)
	if _, err := p.file.WriteAt(flagsBuf[:], 16); err != nil {
		p.mu.Unlock()
		p.syncMu.Lock()
		p.syncing = false
		p.syncCond.Broadcast()
		p.syncMu.Unlock()
		return fmt.Errorf("failed writing sealed flag to %s: %w", p.path, err)
	}
	p.fsyncedSeq = p.pendingSeq
	p.mu.Unlock()

	// Phase 2: ensure the sealed flag write is durable
	if err := fdatasync(p.file); err != nil {
		p.syncMu.Lock()
		p.syncing = false
		p.syncCond.Broadcast()
		p.syncMu.Unlock()
		return fmt.Errorf("failed syncing sealed packfile %s: %w", p.path, err)
	}

	p.syncMu.Lock()
	p.syncing = false
	p.syncCond.Broadcast()
	p.syncMu.Unlock()
	return nil
}

// ReadBlob reads and decodes the blob payload at entry.DataOffset, verifying its SHA-256.
func (p *IncrementalPack) ReadBlob(entry BlobEntry) ([]byte, error) {
	p.mu.Lock()
	f := p.file
	p.mu.Unlock()

	payload := make([]byte, entry.StoredLength)
	if entry.StoredLength > 0 {
		if _, err := f.ReadAt(payload, int64(entry.DataOffset)); err != nil {
			return nil, fmt.Errorf("failed reading blob from %s at offset %d: %w", p.path, entry.DataOffset, err)
		}
	}

	stream, err := decodePayloadToByteStream(bytes.NewReader(payload), entry.Encoding, int64(entry.FinalLength))
	if err != nil {
		return nil, fmt.Errorf("failed decoding blob payload from %s: %w", p.path, err)
	}
	defer errutil.CloseAndLog(stream, "decoded blob stream")

	data, err := io.ReadAll(stream)
	if err != nil {
		return nil, fmt.Errorf("failed reading decoded blob data from %s: %w", p.path, err)
	}

	actualSHA := sha256.Sum256(data)
	if actualSHA != entry.SHA256 {
		return nil, fmt.Errorf("checksum mismatch on blob read from %s: got %x, expected %x",
			p.path, actualSHA, entry.SHA256)
	}

	return data, nil
}

// Entries returns a copy of the packfile's blob entries.
func (p *IncrementalPack) Entries() []BlobEntry {
	p.mu.Lock()
	defer p.mu.Unlock()
	cp := make([]BlobEntry, len(p.entries))
	copy(cp, p.entries)
	return cp
}

// Count returns the number of blobs currently stored in the pack.
func (p *IncrementalPack) Count() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.entries)
}

// ByteSize returns the current on-disk length of the packfile.
func (p *IncrementalPack) ByteSize() int64 {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.offset
}

// Path returns the filesystem path of the packfile.
func (p *IncrementalPack) Path() string {
	return p.path
}

// ID returns the numeric identifier of the packfile.
func (p *IncrementalPack) ID() uint64 {
	return p.id
}

// Sealed returns true if the packfile has been sealed.
func (p *IncrementalPack) Sealed() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.sealed
}

// SetLiveRefs sets the count of live staged chunk references for this pack.
func (p *IncrementalPack) SetLiveRefs(n int32) {
	atomic.StoreInt32(&p.liveRefs, n)
}

// AddLiveRefs atomically adds delta to the live reference count.
func (p *IncrementalPack) AddLiveRefs(delta int32) int32 {
	return atomic.AddInt32(&p.liveRefs, delta)
}

// LiveRefs returns the count of live staged chunk references.
func (p *IncrementalPack) LiveRefs() int32 {
	return atomic.LoadInt32(&p.liveRefs)
}

// SetUploaded marks whether the pack has been uploaded to object storage.
func (p *IncrementalPack) SetUploaded(u bool) {
	p.mu.Lock()
	p.uploaded = u
	p.mu.Unlock()
}

// Uploaded returns whether the pack has been uploaded to object storage.
func (p *IncrementalPack) Uploaded() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.uploaded
}

// Close closes the underlying packfile.
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

// RecoverIncrementalPack opens an existing incremental packfile from disk,
// repairs any torn tail on an unsealed pack (failing loudly if Truncate fails),
// and loads its entries into memory.
func RecoverIncrementalPack(path string, id uint64, isNewest bool) (*IncrementalPack, error) {
	f, err := os.OpenFile(path, os.O_RDWR, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed opening incremental pack %s: %w", path, err)
	}

	st, err := f.Stat()
	if err != nil {
		errutil.CloseAndLog(f, "stat incremental pack")
		return nil, fmt.Errorf("failed stating incremental pack %s: %w", path, err)
	}

	if st.Size() < int64(PackV2HeaderSize) {
		errutil.CloseAndLog(f, "truncated incremental pack header")
		if isNewest {
			klog.Warningf("Incremental pack %s: discarded torn header (%d bytes)", path, st.Size())
			if remErr := os.Remove(path); remErr != nil && !os.IsNotExist(remErr) {
				return nil, fmt.Errorf("failed to remove torn incremental pack %s: %w", path, remErr)
			}
			return nil, nil
		}
		return nil, fmt.Errorf("incremental pack %s: truncated header (%d bytes < %d)", path, st.Size(), PackV2HeaderSize)
	}

	var hdr [PackV2HeaderSize]byte
	if _, err := f.ReadAt(hdr[:], 0); err != nil {
		errutil.CloseAndLog(f, "read incremental pack header")
		return nil, fmt.Errorf("failed reading header from %s: %w", path, err)
	}

	fh, err := ParseFileHeader(hdr[0:8])
	if err != nil {
		errutil.CloseAndLog(f, "parse file header")
		return nil, fmt.Errorf("invalid file header in %s: %w", path, err)
	}
	if fh.FileType != FileTypePack || fh.Version != VersionV2 {
		errutil.CloseAndLog(f, "header version check")
		return nil, fmt.Errorf("incremental pack %s has invalid type %d or version %d", path, fh.FileType, fh.Version)
	}

	slots := binary.BigEndian.Uint32(hdr[8:12])
	used := binary.BigEndian.Uint32(hdr[12:16])
	flags := binary.BigEndian.Uint32(hdr[16:20])
	sealed := (flags & PackFlagSealed) != 0

	// Reject corrupt used count exceeding reserved slots
	if used > slots {
		errutil.CloseAndLog(f, "corrupt used count")
		return nil, fmt.Errorf("incremental pack %s: invalid used count %d > slots %d", path, used, slots)
	}

	frontSectionSize := int64(PackV2HeaderSize) + int64(slots)*int64(PackV2SlotSize)
	if st.Size() < frontSectionSize {
		errutil.CloseAndLog(f, "truncated table slots")
		if isNewest && !sealed {
			klog.Warningf("Incremental pack %s: discarded torn table slots (%d bytes < %d)", path, st.Size(), frontSectionSize)
			if remErr := os.Remove(path); remErr != nil && !os.IsNotExist(remErr) {
				return nil, fmt.Errorf("failed to remove torn incremental pack %s: %w", path, remErr)
			}
			return nil, nil
		}
		return nil, fmt.Errorf("incremental pack %s: truncated table slots (%d bytes < %d)", path, st.Size(), frontSectionSize)
	}

	var entries []BlobEntry
	validOffset := frontSectionSize

	// Data items are the source of truth for all local packs, sealed or unsealed.
	// Scan items sequentially from frontSectionSize, recompute each SHA-256,
	// and rebuild the table and count from the scan before upload.
	currOffset := frontSectionSize

	for currOffset < st.Size() {
		if currOffset+int64(BlobItemHeaderSize) > st.Size() {
			if isNewest && !sealed {
				discardBytes := st.Size() - validOffset
				klog.Warningf("Incremental pack %s: discarded torn item header at offset %d (%d bytes)", path, validOffset, discardBytes)
				if truncErr := f.Truncate(validOffset); truncErr != nil {
					errutil.CloseAndLog(f, "truncate torn pack")
					return nil, fmt.Errorf("failed to truncate torn tail on pack %s at offset %d: %w", path, validOffset, truncErr)
				}
				break
			}
			errutil.CloseAndLog(f, "truncated item header")
			return nil, fmt.Errorf("incremental pack %s: truncated item header at offset %d", path, currOffset)
		}

		var itemHdr [BlobItemHeaderSize]byte
		if _, err := f.ReadAt(itemHdr[:], currOffset); err != nil {
			if isNewest && !sealed {
				discardBytes := st.Size() - validOffset
				klog.Warningf("Incremental pack %s: discarded unreadable item header at offset %d (%d bytes)", path, validOffset, discardBytes)
				if truncErr := f.Truncate(validOffset); truncErr != nil {
					errutil.CloseAndLog(f, "truncate torn pack")
					return nil, fmt.Errorf("failed to truncate torn tail on pack %s at offset %d: %w", path, validOffset, truncErr)
				}
				break
			}
			errutil.CloseAndLog(f, "read item header")
			return nil, fmt.Errorf("incremental pack %s: error reading item header at offset %d: %w", path, currOffset, err)
		}

		encoding := binary.BigEndian.Uint32(itemHdr[0:4])
		finalLen := binary.BigEndian.Uint32(itemHdr[4:8])
		storedLen := binary.BigEndian.Uint32(itemHdr[8:12])

		if encoding > 1 {
			if isNewest && !sealed {
				discardBytes := st.Size() - validOffset
				klog.Warningf("Incremental pack %s: discarded invalid item encoding %d at offset %d (%d bytes)", path, encoding, validOffset, discardBytes)
				if truncErr := f.Truncate(validOffset); truncErr != nil {
					errutil.CloseAndLog(f, "truncate invalid encoding")
					return nil, fmt.Errorf("failed to truncate torn tail on pack %s at offset %d: %w", path, validOffset, truncErr)
				}
				break
			}
			errutil.CloseAndLog(f, "invalid item encoding")
			return nil, fmt.Errorf("incremental pack %s: invalid item encoding %d at offset %d", path, encoding, currOffset)
		}

		totalItemLen := int64(BlobItemHeaderSize) + int64(storedLen)
		if currOffset+totalItemLen > st.Size() {
			if isNewest && !sealed {
				discardBytes := st.Size() - validOffset
				klog.Warningf("Incremental pack %s: discarded truncated item payload at offset %d (%d bytes)", path, validOffset, discardBytes)
				if truncErr := f.Truncate(validOffset); truncErr != nil {
					errutil.CloseAndLog(f, "truncate truncated payload")
					return nil, fmt.Errorf("failed to truncate torn tail on pack %s at offset %d: %w", path, validOffset, truncErr)
				}
				break
			}
			errutil.CloseAndLog(f, "truncated item payload")
			return nil, fmt.Errorf("incremental pack %s: truncated payload at offset %d", path, currOffset)
		}

		payloadStart := currOffset + int64(BlobItemHeaderSize)
		payload := make([]byte, storedLen)
		if storedLen > 0 {
			if _, err := f.ReadAt(payload, payloadStart); err != nil {
				if isNewest && !sealed {
					discardBytes := st.Size() - validOffset
					klog.Warningf("Incremental pack %s: discarded unreadable payload at offset %d (%d bytes)", path, validOffset, discardBytes)
					if truncErr := f.Truncate(validOffset); truncErr != nil {
						errutil.CloseAndLog(f, "truncate unreadable payload")
						return nil, fmt.Errorf("failed to truncate torn tail on pack %s at offset %d: %w", path, validOffset, truncErr)
					}
					break
				}
				errutil.CloseAndLog(f, "read payload")
				return nil, fmt.Errorf("incremental pack %s: error reading payload at offset %d: %w", path, payloadStart, err)
			}
		}

		// Verify payload decoding and compute true SHA-256
		stream, decErr := decodePayloadToByteStream(bytes.NewReader(payload), encoding, int64(finalLen))
		if decErr != nil {
			if isNewest && !sealed {
				discardBytes := st.Size() - validOffset
				klog.Warningf("Incremental pack %s: discarded undecodable payload at offset %d (%d bytes)", path, validOffset, discardBytes)
				if truncErr := f.Truncate(validOffset); truncErr != nil {
					errutil.CloseAndLog(f, "truncate undecodable payload")
					return nil, fmt.Errorf("failed to truncate torn tail on pack %s at offset %d: %w", path, validOffset, truncErr)
				}
				break
			}
			errutil.CloseAndLog(f, "decode payload")
			return nil, fmt.Errorf("incremental pack %s: failed decoding payload at offset %d: %w", path, payloadStart, decErr)
		}

		data, readErr := io.ReadAll(stream)
		errutil.CloseAndLog(stream, "recovery stream")
		if readErr != nil {
			if isNewest && !sealed {
				discardBytes := st.Size() - validOffset
				klog.Warningf("Incremental pack %s: discarded unreadable stream at offset %d (%d bytes)", path, validOffset, discardBytes)
				if truncErr := f.Truncate(validOffset); truncErr != nil {
					errutil.CloseAndLog(f, "truncate unreadable stream")
					return nil, fmt.Errorf("failed to truncate torn tail on pack %s at offset %d: %w", path, validOffset, truncErr)
				}
				break
			}
			errutil.CloseAndLog(f, "read stream")
			return nil, fmt.Errorf("incremental pack %s: error reading stream at offset %d: %w", path, payloadStart, readErr)
		}

		sha := sha256.Sum256(data)
		entries = append(entries, BlobEntry{
			SHA256:       sha,
			DataOffset:   uint32(payloadStart),
			StoredLength: storedLen,
			FinalLength:  finalLen,
			Encoding:     encoding,
		})

		validOffset = currOffset + totalItemLen
		currOffset = validOffset
	}

	// Rebuild sorted table and count on disk from valid entries for all packs
	sort.Slice(entries, func(i, j int) bool {
		return bytes.Compare(entries[i].SHA256[:], entries[j].SHA256[:]) < 0
	})

	tableBuf := make([]byte, int(slots)*PackV2SlotSize)
	for i, ent := range entries {
		slotOff := i * PackV2SlotSize
		copy(tableBuf[slotOff:slotOff+32], ent.SHA256[:])
		binary.BigEndian.PutUint32(tableBuf[slotOff+32:slotOff+36], ent.DataOffset)
		binary.BigEndian.PutUint32(tableBuf[slotOff+36:slotOff+40], ent.StoredLength)
		binary.BigEndian.PutUint32(tableBuf[slotOff+40:slotOff+44], ent.FinalLength)
		binary.BigEndian.PutUint32(tableBuf[slotOff+44:slotOff+48], ent.Encoding)
	}

	if _, err := f.WriteAt(tableBuf, int64(PackV2HeaderSize)); err != nil {
		errutil.CloseAndLog(f, "write rebuilt table")
		return nil, fmt.Errorf("failed writing rebuilt table in %s: %w", path, err)
	}

	var countBuf [4]byte
	binary.BigEndian.PutUint32(countBuf[:], uint32(len(entries)))
	if _, err := f.WriteAt(countBuf[:], 12); err != nil {
		errutil.CloseAndLog(f, "write rebuilt count")
		return nil, fmt.Errorf("failed writing rebuilt count in %s: %w", path, err)
	}

	if err := fdatasync(f); err != nil {
		errutil.CloseAndLog(f, "sync rebuilt pack")
		return nil, fmt.Errorf("failed syncing rebuilt pack %s: %w", path, err)
	}

	pack := &IncrementalPack{
		id:      id,
		path:    path,
		file:    f,
		slots:   slots,
		offset:  validOffset,
		sealed:  sealed,
		entries: entries,
	}
	pack.syncCond = sync.NewCond(&pack.syncMu)
	return pack, nil
}
