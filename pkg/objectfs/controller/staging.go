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
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gke-labs/in-cluster-storage/pkg/errutil"
	"github.com/gke-labs/in-cluster-storage/pkg/objectfs/blob"
	"k8s.io/klog/v2"
)

const (
	// IntentLogMagic is the 4-byte header identifier "INTL" (0x494E544C).
	IntentLogMagic = "INTL"

	// IntentLogVersion is the current intent log format version.
	IntentLogVersion uint16 = 1

	// IntentLogHeaderLength is the fixed size of an intent log header (16 bytes).
	IntentLogHeaderLength uint16 = 16

	// IntentRecordMagic is the 4-byte record header magic "RECD" (0x52454344).
	IntentRecordMagic uint32 = 0x52454344

	// IntentRecordLength is the fixed size of an intent log record (64 bytes):
	// magic(4) + inode(8) + chunk_index(4) + generation(8) + length(4) + sha256(32) + crc32c(4)
	IntentRecordLength = 64
)

var (
	// crcCastagnoliTable uses the Castagnoli polynomial.
	crcCastagnoliTable = crc32.MakeTable(crc32.Castagnoli)
)

// StagedChunkRef references a chunk payload stored in an incremental v2 packfile.
type StagedChunkRef struct {
	PackID     uint64
	Entry      blob.BlobEntry
	Generation uint64
	Length     uint32
}

// IntentLog manages an append-only transaction intent log paired with an IncrementalPack.
type IntentLog struct {
	id     uint64
	path   string
	file   *os.File
	offset int64
	sealed bool

	mu          sync.Mutex
	syncMu      sync.Mutex
	syncCond    *sync.Cond
	syncing     bool
	pendingSeq  uint64
	fsyncedSeq  uint64
	lastSyncErr error
}

func computeIntentCRC(record []byte) uint32 {
	h := crc32.New(crcCastagnoliTable)
	h.Write(record[:IntentRecordLength-4]) // magic through sha256, excluding crc32c
	return h.Sum32()
}

// NewIntentLog creates an open intent log paired with pack ID.
func NewIntentLog(path string, id uint64, epoch int64) (*IntentLog, error) {
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed creating intent log %s: %w", path, err)
	}

	var hdr [IntentLogHeaderLength]byte
	copy(hdr[0:4], []byte(IntentLogMagic))
	binary.BigEndian.PutUint16(hdr[4:6], IntentLogVersion)
	binary.BigEndian.PutUint16(hdr[6:8], IntentLogHeaderLength)
	binary.BigEndian.PutUint64(hdr[8:16], uint64(epoch))

	if _, err := f.WriteAt(hdr[:], 0); err != nil {
		errutil.CloseAndLog(f, "intent log header write error")
		return nil, fmt.Errorf("failed writing intent log header: %w", err)
	}
	if err := fdatasync(f); err != nil {
		errutil.CloseAndLog(f, "intent log init sync error")
		return nil, fmt.Errorf("failed syncing intent log header: %w", err)
	}

	log := &IntentLog{
		id:     id,
		path:   path,
		file:   f,
		offset: int64(IntentLogHeaderLength),
	}
	log.syncCond = sync.NewCond(&log.syncMu)
	return log, nil
}

// Append writes an intent log record and assigns a group commit sequence number under lock.
func (l *IntentLog) Append(inode uint64, chunkIndex uint32, gen uint64, length uint32, sha [32]byte) (uint64, error) {
	var rec [IntentRecordLength]byte
	binary.BigEndian.PutUint32(rec[0:4], IntentRecordMagic)
	binary.BigEndian.PutUint64(rec[4:12], inode)
	binary.BigEndian.PutUint32(rec[12:16], chunkIndex)
	binary.BigEndian.PutUint64(rec[16:24], gen)
	binary.BigEndian.PutUint32(rec[24:28], length)
	copy(rec[28:60], sha[:])
	crc := computeIntentCRC(rec[:])
	binary.BigEndian.PutUint32(rec[60:64], crc)

	l.mu.Lock()
	if l.sealed {
		l.mu.Unlock()
		return 0, errors.New("intent log is sealed")
	}

	recordOffset := l.offset
	if _, err := l.file.WriteAt(rec[:], recordOffset); err != nil {
		l.mu.Unlock()
		return 0, fmt.Errorf("failed appending record to intent log %s: %w", l.path, err)
	}
	l.offset += IntentRecordLength

	l.pendingSeq++
	mySeq := l.pendingSeq
	l.mu.Unlock()

	return mySeq, nil
}

// SyncToSeq blocks until the intent log has been fsynced up to at least targetSeq using group commit.
func (l *IntentLog) SyncToSeq(targetSeq uint64) error {
	l.syncMu.Lock()
	defer l.syncMu.Unlock()

	for l.fsyncedSeq < targetSeq {
		if l.syncing {
			l.syncCond.Wait()
			continue
		}
		l.syncing = true
		l.mu.Lock()
		maxPending := l.pendingSeq
		l.mu.Unlock()

		l.syncMu.Unlock()
		syncErr := fdatasync(l.file)
		l.syncMu.Lock()

		l.syncing = false
		if syncErr != nil {
			l.lastSyncErr = syncErr
			l.syncCond.Broadcast()
			return fmt.Errorf("intent log fdatasync failed: %w", syncErr)
		}
		l.lastSyncErr = nil
		if maxPending > l.fsyncedSeq {
			l.fsyncedSeq = maxPending
		}
		l.syncCond.Broadcast()
	}

	if l.lastSyncErr != nil {
		return fmt.Errorf("intent log fdatasync failed: %w", l.lastSyncErr)
	}
	return nil
}

// Seal marks the intent log sealed and unconditionally fsyncs it.
func (l *IntentLog) Seal() error {
	l.mu.Lock()
	l.sealed = true
	l.mu.Unlock()

	l.syncMu.Lock()
	defer l.syncMu.Unlock()

	for l.syncing {
		l.syncCond.Wait()
	}

	if err := fdatasync(l.file); err != nil {
		return fmt.Errorf("failed syncing sealed intent log %s: %w", l.path, err)
	}

	l.mu.Lock()
	l.fsyncedSeq = l.pendingSeq
	l.mu.Unlock()
	l.syncCond.Broadcast()
	return nil
}

// Close closes the intent log file.
func (l *IntentLog) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.file != nil {
		err := l.file.Close()
		l.file = nil
		return err
	}
	return nil
}

// Path returns the filesystem path of the intent log.
func (l *IntentLog) Path() string {
	return l.path
}

// StagingManager coordinates the open incremental v2 packfiles and their paired intent logs.
type StagingManager struct {
	dir       string
	volumeID  string
	epoch     int64
	blobStore *blob.Store

	mu         sync.Mutex
	activePack *blob.IncrementalPack
	activeLog  *IntentLog
	packs      map[uint64]*blob.IncrementalPack
	logs       map[uint64]*IntentLog
	nextID     uint64
	slotSeals  int64
	byteSeals  int64
}

// SlotSeals returns the total count of packfile seals triggered by filling all slots.
func (sm *StagingManager) SlotSeals() int64 {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	return sm.slotSeals
}

// ByteSeals returns the total count of packfile seals triggered by the byte limit.
func (sm *StagingManager) ByteSeals() int64 {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	return sm.byteSeals
}

// IncrByteSeals increments the count of byte-triggered seals.
func (sm *StagingManager) IncrByteSeals() {
	sm.mu.Lock()
	sm.byteSeals++
	sm.mu.Unlock()
}

// SetBlobStore attaches the volume's blob.Store for reading committed/uploaded blobs.
func (sm *StagingManager) SetBlobStore(bs *blob.Store) {
	sm.mu.Lock()
	sm.blobStore = bs
	sm.mu.Unlock()
}

// NewStagingManager creates or loads a StagingManager under dir.
func NewStagingManager(dir, volumeID string, epoch int64) (*StagingManager, error) {
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("failed creating staging directory %s: %w", dir, err)
	}

	epochPath := filepath.Join(dir, "epoch")
	if data, err := os.ReadFile(epochPath); err == nil {
		val, parseErr := strconv.ParseInt(strings.TrimSpace(string(data)), 10, 64)
		if parseErr != nil {
			return nil, fmt.Errorf("invalid staging epoch file %s: %w", epochPath, parseErr)
		}
		if val > 0 {
			epoch = val
		}
	} else if os.IsNotExist(err) {
		if epoch <= 0 {
			epoch = time.Now().UnixNano()
		}
		if writeErr := os.WriteFile(epochPath, []byte(strconv.FormatInt(epoch, 10)), 0644); writeErr != nil {
			return nil, fmt.Errorf("failed writing staging epoch file: %w", writeErr)
		}
	} else {
		return nil, fmt.Errorf("failed reading staging epoch file %s: %w", epochPath, err)
	}

	sm := &StagingManager{
		dir:      dir,
		volumeID: volumeID,
		epoch:    epoch,
		packs:    make(map[uint64]*blob.IncrementalPack),
		logs:     make(map[uint64]*IntentLog),
		nextID:   1,
	}
	return sm, nil
}

// Epoch returns the persistent staging epoch.
func (sm *StagingManager) Epoch() int64 {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	return sm.epoch
}

func (sm *StagingManager) packPath(id uint64) string {
	return filepath.Join(sm.dir, fmt.Sprintf("pack-%016x.pack", id))
}

func (sm *StagingManager) logPath(id uint64) string {
	return filepath.Join(sm.dir, fmt.Sprintf("intent-%016x.log", id))
}

func (sm *StagingManager) ensureActiveLocked() error {
	if sm.activePack != nil && sm.activeLog != nil {
		if sm.activePack.Count() < int(blob.DefaultPackSlots) {
			return nil
		}
		// Active pack reached 1024 slots: seal and start a new open pack
		sm.slotSeals++
		if err := sm.rotateLocked(); err != nil {
			return fmt.Errorf("failed sealing full pack on slot cap: %w", err)
		}
	}

	id := sm.nextID
	sm.nextID++

	pack, err := blob.NewIncrementalPack(sm.packPath(id), id)
	if err != nil {
		return fmt.Errorf("failed creating incremental pack: %w", err)
	}

	log, err := NewIntentLog(sm.logPath(id), id, sm.epoch)
	if err != nil {
		errutil.CloseAndLog(pack, "closing pack on log error")
		return fmt.Errorf("failed creating intent log: %w", err)
	}

	sm.packs[id] = pack
	sm.logs[id] = log
	sm.activePack = pack
	sm.activeLog = log
	return nil
}

func (sm *StagingManager) rotateLocked() error {
	for _, pack := range sm.packs {
		if !pack.Sealed() {
			if err := pack.Seal(); err != nil {
				return fmt.Errorf("failed sealing pack %d on rotate: %w", pack.ID(), err)
			}
		}
	}
	for _, log := range sm.logs {
		log.mu.Lock()
		sealed := log.sealed
		log.mu.Unlock()
		if !sealed {
			if err := log.Seal(); err != nil {
				return fmt.Errorf("failed sealing intent log %d on rotate: %w", log.id, err)
			}
		}
	}
	sm.activePack = nil
	sm.activeLog = nil
	return nil
}

// Rotate seals the active incremental pack and active intent log.
func (sm *StagingManager) Rotate() error {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	return sm.rotateLocked()
}

// StageChunk appends a chunk to the active pack and active intent log under lock.
func (sm *StagingManager) StageChunk(inode uint64, chunkIndex uint32, gen uint64, data []byte) (StagedChunkRef, *blob.IncrementalPack, uint64, *IntentLog, uint64, error) {
	sm.mu.Lock()
	if err := sm.ensureActiveLocked(); err != nil {
		sm.mu.Unlock()
		return StagedChunkRef{}, nil, 0, nil, 0, err
	}
	pack := sm.activePack
	log := sm.activeLog
	sm.mu.Unlock()

	entry, pSeq, err := pack.Append(data)
	if err != nil {
		if errors.Is(err, blob.ErrPackSlotsFull) {
			// Slots full; rotate and retry once
			sm.mu.Lock()
			sm.slotSeals++
			if err := sm.rotateLocked(); err != nil {
				sm.mu.Unlock()
				return StagedChunkRef{}, nil, 0, nil, 0, err
			}
			sm.mu.Unlock()
			return sm.StageChunk(inode, chunkIndex, gen, data)
		}
		return StagedChunkRef{}, nil, 0, nil, 0, err
	}

	lSeq, err := log.Append(inode, chunkIndex, gen, uint32(len(data)), entry.SHA256)
	if err != nil {
		return StagedChunkRef{}, nil, 0, nil, 0, err
	}

	pack.AddLiveRefs(1)

	ref := StagedChunkRef{
		PackID:     pack.ID(),
		Entry:      entry,
		Generation: gen,
		Length:     uint32(len(data)),
	}
	return ref, pack, pSeq, log, lSeq, nil
}

// ReadChunk reads chunk bytes for the given reference from its incremental packfile,
// or falls back to the blob store if the pack was already uploaded and unlinked.
func (sm *StagingManager) ReadChunk(ref StagedChunkRef) ([]byte, error) {
	sm.mu.Lock()
	pack, ok := sm.packs[ref.PackID]
	bs := sm.blobStore
	sm.mu.Unlock()

	if ok {
		data, err := pack.ReadBlob(ref.Entry)
		if err != nil {
			return nil, fmt.Errorf("failed reading blob from pack %d: %w", ref.PackID, err)
		}
		return data, nil
	}

	// If pack was already uploaded and removed from local staging,
	// fetch from blob store via SHA-256
	if bs != nil {
		stream, err := bs.GetBlob(context.Background(), ref.Entry.SHA256Hex())
		if err != nil {
			return nil, fmt.Errorf("failed fetching uploaded blob %s: %w", ref.Entry.SHA256Hex(), err)
		}
		defer errutil.CloseAndLog(stream, "fallback blob stream")
		data, readErr := io.ReadAll(stream)
		if readErr != nil {
			return nil, fmt.Errorf("failed reading fallback blob %s: %w", ref.Entry.SHA256Hex(), readErr)
		}
		return data, nil
	}

	return nil, fmt.Errorf("staging pack %d not found", ref.PackID)
}

// ReleaseChunk decrements the live reference count for ref.PackID,
// and deletes the packfile and intent log if the pack was uploaded and liveRefs reach 0.
func (sm *StagingManager) ReleaseChunk(ref StagedChunkRef) {
	sm.mu.Lock()
	pack, ok := sm.packs[ref.PackID]
	if !ok {
		sm.mu.Unlock()
		return
	}
	newRefs := pack.AddLiveRefs(-1)
	uploaded := pack.Uploaded()

	if uploaded && newRefs <= 0 {
		id := pack.ID()
		delete(sm.packs, id)
		log := sm.logs[id]
		delete(sm.logs, id)
		sm.mu.Unlock()

		errutil.CloseAndLog(pack, "removing uploaded unreferenced pack")
		if err := os.Remove(pack.Path()); err != nil && !os.IsNotExist(err) {
			klog.Warningf("failed removing uploaded pack %s: %v", pack.Path(), err)
		}
		if log != nil {
			errutil.CloseAndLog(log, "removing uploaded unreferenced intent log")
			if err := os.Remove(log.Path()); err != nil && !os.IsNotExist(err) {
				klog.Warningf("failed removing intent log %s: %v", log.Path(), err)
			}
		}
		return
	}
	sm.mu.Unlock()
}

// SealedPacksToUpload returns all sealed incremental packs that are ready for upload.
func (sm *StagingManager) SealedPacksToUpload() []*blob.IncrementalPack {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	var sealed []*blob.IncrementalPack
	for _, p := range sm.packs {
		if p.Sealed() {
			sealed = append(sealed, p)
		}
	}
	sort.Slice(sealed, func(i, j int) bool {
		return sealed[i].ID() < sealed[j].ID()
	})
	return sealed
}

// OnPackUploadComplete marks the pack uploaded and deletes it if unreferenced.
func (sm *StagingManager) OnPackUploadComplete(pack *blob.IncrementalPack) {
	sm.mu.Lock()
	pack.SetUploaded(true)
	id := pack.ID()
	if pack.LiveRefs() <= 0 {
		delete(sm.packs, id)
		log := sm.logs[id]
		delete(sm.logs, id)
		sm.mu.Unlock()

		errutil.CloseAndLog(pack, "removing uploaded pack")
		if err := os.Remove(pack.Path()); err != nil && !os.IsNotExist(err) {
			klog.Warningf("failed removing uploaded pack %s: %v", pack.Path(), err)
		}
		if log != nil {
			errutil.CloseAndLog(log, "removing uploaded intent log")
			if err := os.Remove(log.Path()); err != nil && !os.IsNotExist(err) {
				klog.Warningf("failed removing intent log %s: %v", log.Path(), err)
			}
		}
		return
	}
	sm.mu.Unlock()
}

// OnPackUploadFailed is called when upload fails to retain the pack for retry.
func (sm *StagingManager) OnPackUploadFailed(pack *blob.IncrementalPack) {
	// Pack is retained in sm.packs for retry on the next sync
}

// StagedBytesOnDisk returns the sum of on-disk bytes across all packs and intent logs.
func (sm *StagingManager) StagedBytesOnDisk() int64 {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	var total int64
	for _, p := range sm.packs {
		total += p.ByteSize()
	}
	for _, l := range sm.logs {
		l.mu.Lock()
		total += l.offset
		l.mu.Unlock()
	}
	return total
}

// Close closes all open packs and intent logs.
func (sm *StagingManager) Close() error {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	var firstErr error
	for _, p := range sm.packs {
		if err := p.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	for _, l := range sm.logs {
		if err := l.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	sm.activePack = nil
	sm.activeLog = nil
	return firstErr
}

// ScanAndRecover scans all intent logs and packfiles on reload, repairing torn tails
// on the newest unsealed file (failing loudly if Truncate fails), and loading the index.
func (sm *StagingManager) ScanAndRecover(ctx context.Context) (map[uint64]map[int]StagedChunkRef, error) {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	entries, err := os.ReadDir(sm.dir)
	if err != nil {
		if os.IsNotExist(err) {
			return make(map[uint64]map[int]StagedChunkRef), nil
		}
		return nil, fmt.Errorf("failed reading staging directory %s: %w", sm.dir, err)
	}

	var packFiles []string
	var logFiles []string
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		if strings.HasPrefix(e.Name(), "pack-") && strings.HasSuffix(e.Name(), ".pack") {
			packFiles = append(packFiles, e.Name())
		}
		if strings.HasPrefix(e.Name(), "intent-") && strings.HasSuffix(e.Name(), ".log") {
			logFiles = append(logFiles, e.Name())
		}
	}
	sort.Strings(packFiles)
	sort.Strings(logFiles)

	var maxID uint64

	// Load packs
	for i, pName := range packFiles {
		path := filepath.Join(sm.dir, pName)
		hexStr := strings.TrimSuffix(strings.TrimPrefix(pName, "pack-"), ".pack")
		id, parseErr := strconv.ParseUint(hexStr, 16, 64)
		if parseErr != nil {
			continue
		}
		if id > maxID {
			maxID = id
		}
		isNewest := (i == len(packFiles)-1)

		pack, err := blob.RecoverIncrementalPack(path, id, isNewest)
		if err != nil {
			return nil, err
		}
		if pack != nil {
			sm.packs[id] = pack
		}
	}

	index := make(map[uint64]map[int]StagedChunkRef)

	// Load intent logs and populate index
	for i, lName := range logFiles {
		path := filepath.Join(sm.dir, lName)
		hexStr := strings.TrimSuffix(strings.TrimPrefix(lName, "intent-"), ".log")
		id, parseErr := strconv.ParseUint(hexStr, 16, 64)
		if parseErr != nil {
			continue
		}
		if id > maxID {
			maxID = id
		}
		isNewest := (i == len(logFiles)-1)

		f, openErr := os.OpenFile(path, os.O_RDWR, 0644)
		if openErr != nil {
			return nil, fmt.Errorf("failed opening intent log %s: %w", path, openErr)
		}

		st, statErr := f.Stat()
		if statErr != nil {
			errutil.CloseAndLog(f, "stat intent log")
			return nil, fmt.Errorf("failed stating intent log %s: %w", path, statErr)
		}

		if st.Size() < int64(IntentLogHeaderLength) {
			errutil.CloseAndLog(f, "truncated intent log header")
			if isNewest {
				klog.Warningf("Intent log %s: discarded torn header (%d bytes)", path, st.Size())
				if remErr := os.Remove(path); remErr != nil && !os.IsNotExist(remErr) {
					return nil, fmt.Errorf("failed removing torn intent log %s: %w", path, remErr)
				}
				continue
			}
			return nil, fmt.Errorf("intent log %s: truncated header (%d bytes < %d)", path, st.Size(), IntentLogHeaderLength)
		}

		var hdr [IntentLogHeaderLength]byte
		if _, err := f.ReadAt(hdr[:], 0); err != nil {
			errutil.CloseAndLog(f, "read intent log header")
			return nil, fmt.Errorf("failed reading header from %s: %w", path, err)
		}

		if string(hdr[0:4]) != IntentLogMagic {
			errutil.CloseAndLog(f, "invalid intent log magic")
			return nil, fmt.Errorf("intent log %s: invalid magic %q", path, string(hdr[0:4]))
		}
		epoch := int64(binary.BigEndian.Uint64(hdr[8:16]))
		if epoch > sm.epoch {
			sm.epoch = epoch
		}

		pack := sm.packs[id]
		var packEntriesBySHA map[[32]byte]blob.BlobEntry
		if pack != nil {
			packEntriesBySHA = make(map[[32]byte]blob.BlobEntry, pack.Count())
			for _, ent := range pack.Entries() {
				packEntriesBySHA[ent.SHA256] = ent
			}
		}

		validOffset := int64(IntentLogHeaderLength)
		currOffset := int64(IntentLogHeaderLength)

		for {
			if currOffset+IntentRecordLength > st.Size() {
				if currOffset < st.Size() {
					if isNewest {
						discardBytes := st.Size() - validOffset
						klog.Warningf("Intent log %s: discarded torn tail at offset %d (%d bytes)", path, validOffset, discardBytes)
						if truncErr := f.Truncate(validOffset); truncErr != nil {
							errutil.CloseAndLog(f, "truncate torn intent log")
							return nil, fmt.Errorf("failed to truncate torn tail on intent log %s at offset %d: %w", path, validOffset, truncErr)
						}
					} else {
						errutil.CloseAndLog(f, "corrupted trailing bytes in intent log")
						return nil, fmt.Errorf("intent log %s: corrupted trailing bytes at offset %d", path, currOffset)
					}
				}
				break
			}

			var rec [IntentRecordLength]byte
			if _, err := f.ReadAt(rec[:], currOffset); err != nil {
				if isNewest {
					discardBytes := st.Size() - validOffset
					klog.Warningf("Intent log %s: discarded torn tail at offset %d (%d bytes)", path, validOffset, discardBytes)
					if truncErr := f.Truncate(validOffset); truncErr != nil {
						errutil.CloseAndLog(f, "truncate torn intent log")
						return nil, fmt.Errorf("failed to truncate torn tail on intent log %s at offset %d: %w", path, validOffset, truncErr)
					}
					break
				}
				errutil.CloseAndLog(f, "read intent record")
				return nil, fmt.Errorf("intent log %s: error reading record at offset %d: %w", path, currOffset, err)
			}

			magic := binary.BigEndian.Uint32(rec[0:4])
			ino := binary.BigEndian.Uint64(rec[4:12])
			cIdx := binary.BigEndian.Uint32(rec[12:16])
			gen := binary.BigEndian.Uint64(rec[16:24])
			length := binary.BigEndian.Uint32(rec[24:28])
			var sha [32]byte
			copy(sha[:], rec[28:60])
			expectedCRC := binary.BigEndian.Uint32(rec[60:64])
			actualCRC := computeIntentCRC(rec[:])

			if magic != IntentRecordMagic || actualCRC != expectedCRC {
				if isNewest {
					discardBytes := st.Size() - validOffset
					klog.Warningf("Intent log %s: discarded torn record with bad crc at offset %d (%d bytes)", path, validOffset, discardBytes)
					if truncErr := f.Truncate(validOffset); truncErr != nil {
						errutil.CloseAndLog(f, "truncate torn intent log")
						return nil, fmt.Errorf("failed to truncate torn tail on intent log %s at offset %d: %w", path, validOffset, truncErr)
					}
					break
				}
				errutil.CloseAndLog(f, "checksum failure in intent log")
				return nil, fmt.Errorf("intent log %s: checksum mismatch at offset %d (magic=0x%08X, crc=0x%08X, want=0x%08X)",
					path, currOffset, magic, actualCRC, expectedCRC)
			}

			if packEntriesBySHA != nil {
				if ent, ok := packEntriesBySHA[sha]; ok {
					if index[ino] == nil {
						index[ino] = make(map[int]StagedChunkRef)
					}
					index[ino][int(cIdx)] = StagedChunkRef{
						PackID:     id,
						Entry:      ent,
						Generation: gen,
						Length:     length,
					}
				}
			}

			validOffset = currOffset + IntentRecordLength
			currOffset += IntentRecordLength
		}

		log := &IntentLog{
			id:     id,
			path:   path,
			file:   f,
			offset: validOffset,
			sealed: true,
		}
		log.syncCond = sync.NewCond(&log.syncMu)
		sm.logs[id] = log
	}

	sm.nextID = maxID + 1

	// Delete empty sealed packs
	for id, p := range sm.packs {
		hasLiveRefs := false
		for _, chunks := range index {
			for _, ref := range chunks {
				if ref.PackID == id {
					hasLiveRefs = true
					break
				}
			}
			if hasLiveRefs {
				break
			}
		}
		if !hasLiveRefs && p.Sealed() {
			sm.OnPackUploadComplete(p)
		}
	}

	return index, nil
}
