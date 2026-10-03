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
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	pb "github.com/gke-labs/in-cluster-storage/pkg/api/objectfs/v1alpha1"
	sdsv1 "github.com/gke-labs/in-cluster-storage/pkg/api/sds/v1"
	"github.com/gke-labs/in-cluster-storage/pkg/erofs"
	"github.com/gke-labs/in-cluster-storage/pkg/objectfs/blob"
	"github.com/gke-labs/in-cluster-storage/pkg/sds"
	"github.com/gke-labs/in-cluster-storage/pkg/sds/record"
	"github.com/gke-labs/in-cluster-storage/pkg/wal"
	walclient "github.com/gke-labs/in-cluster-storage/pkg/wal/client"
	"github.com/google/uuid"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const MetadataFileName = ".objectfs-metadata.json"

type VolumeMetadata struct {
	VolumeID    string                  `json:"volume_id"`
	Version     int                     `json:"version"`
	LastFlushed time.Time               `json:"last_flushed"`
	NextInode   uint64                  `json:"next_inode"`
	Entries     map[string]FileMetadata `json:"entries"`
}

type FileMetadata struct {
	Inode          uint64    `json:"inode"`
	Path           string    `json:"path"`
	Name           string    `json:"name"`
	IsDir          bool      `json:"is_dir"`
	Mode           uint32    `json:"mode"`
	Size           int64     `json:"size"`
	ModTime        time.Time `json:"mod_time"`
	Sha256         string    `json:"sha256,omitempty"`
	ManifestSha256 string    `json:"manifest_sha256,omitempty"`
	ContentSha256  string    `json:"content_sha256,omitempty"`
	ETag           string    `json:"etag,omitempty"`
	Uid            uint32    `json:"uid,omitempty"`
	Gid            uint32    `json:"gid,omitempty"`
}

// metadataResolver provides unified read methods for resolving inodes and directories
// across active in-memory state/caches, local eviction storage, and immutable base EROFS snapshots.
type metadataResolver struct {
	localStore     *LocalStorage
	snapshotReader *erofs.Reader
	snapshotRaw    io.ReaderAt
	inodeCache     *LRUCache[uint64, *CachedInode]
	dirCache       *LRUCache[uint64, *CachedDir]

	// dirtyInodes maps inode IDs to their latest eviction offset in LocalStorage for inodes
	// that have changed since the last EROFS snapshot. Additional unflushed modifications
	// may also reside in inodeCache.
	dirtyInodes map[uint64]LocalOffset

	// dirtyDirs maps directory inode IDs to their latest delta eviction offset in LocalStorage
	// for directories that have changed since the last EROFS snapshot. Additional unflushed
	// delta modifications may also reside in dirCache.
	dirtyDirs map[uint64]LocalOffset
}

type Volume struct {
	metadataResolver

	mu           sync.RWMutex
	volumeID     string
	rootInodeID  uint64
	nextInode    uint64
	backend      ObjectStorageBackend
	blobStore    *blob.Store
	broadcaster  *EventBroadcaster
	maxInlineLen int64
	chunkSize    uint32

	stream     walclient.Stream
	durability walclient.Level
	streamID   uuid.UUID

	metadataStream   *sds.Writer
	recoveredContent map[string][]byte

	localStorageDir string

	snapshotCutoff LocalOffset
	snapshotMu     sync.Mutex

	// Snapshot trigger limits
	maxBufferFiles   int
	maxDirtyRecords  int
	maxLocalFileSize int64

	lastFlushedMetadata    *VolumeMetadata
	deletedPathsSinceFlush []string
	dirParents             map[uint64]uint64
}

// VolumeOption configures a Volume instance.
type VolumeOption func(*Volume)

// WithStream sets the WAL stream for metadata change-logging.
func WithStream(stream walclient.Stream) VolumeOption {
	return func(v *Volume) {
		v.stream = stream
		if err := v.initMetadataStreamLocked(); err != nil {
			panic(fmt.Sprintf("failed to init metadata stream: %v", err))
		}
	}
}

// WithDurability sets the default durability level for metadata changes.
func WithDurability(level walclient.Level) VolumeOption {
	return func(v *Volume) {
		v.durability = level
	}
}

// WithStreamID sets an explicit Stream UUID for this volume.
func WithStreamID(id uuid.UUID) VolumeOption {
	return func(v *Volume) {
		v.streamID = id
	}
}

// WithChunkSize sets the fixed chunk size for large files on this volume.
func WithChunkSize(chunkSize uint32) VolumeOption {
	return func(v *Volume) {
		if chunkSize > 0 {
			v.chunkSize = chunkSize
		}
	}
}

// WithMaxRAMEntries sets the maximum number of inodes and directories kept in RAM.
func WithMaxRAMEntries(maxInodes, maxDirs int) VolumeOption {
	return func(v *Volume) {
		if maxInodes > 0 && v.inodeCache != nil {
			v.inodeCache.capacity = maxInodes
		}
		if maxDirs > 0 && v.dirCache != nil {
			v.dirCache.capacity = maxDirs
		}
	}
}

// WithLocalStorageDir sets the local directory for evicted metadata storage.
func WithLocalStorageDir(dir string) VolumeOption {
	return func(v *Volume) {
		v.localStorageDir = dir
	}
}

// WithMaxBufferFiles sets the maximum number of buffer files before auto-triggering a snapshot.
func WithMaxBufferFiles(count int) VolumeOption {
	return func(v *Volume) {
		v.maxBufferFiles = count
	}
}

// WithSnapshotThreshold sets limits before auto-triggering a snapshot.
func WithSnapshotThreshold(maxDirtyRecords int, maxFileSize int64) VolumeOption {
	return func(v *Volume) {
		v.maxDirtyRecords = maxDirtyRecords
		v.maxLocalFileSize = maxFileSize
	}
}

// StreamIDForVolume generates a deterministic UUID for an SDS structured stream for the volume.
func StreamIDForVolume(volumeID string) uuid.UUID {
	return uuid.NewSHA1(uuid.NameSpaceURL, []byte("objectfs-sds:"+volumeID))
}

func NewVolume(volumeID string, backend ObjectStorageBackend, broadcaster *EventBroadcaster, opts ...VolumeOption) *Volume {
	var blobStore *blob.Store
	if backend != nil {
		blobStore = blob.NewStore(backend, 0)
	}

	v := &Volume{
		metadataResolver: metadataResolver{
			dirtyInodes: make(map[uint64]LocalOffset),
			dirtyDirs:   make(map[uint64]LocalOffset),
		},
		volumeID:         volumeID,
		rootInodeID:      0,
		nextInode:        erofs.DefaultInodeStride,
		backend:          backend,
		blobStore:        blobStore,
		broadcaster:      broadcaster,
		maxInlineLen:     32 * 1024, // 32KB default inline threshold
		chunkSize:        32 * 1024, // 32KB default chunk size
		durability:       walclient.Local,
		streamID:         StreamIDForVolume(volumeID),
		maxBufferFiles:   4,
		maxDirtyRecords:  100000,
		maxLocalFileSize: 250 * 1024 * 1024,
		recoveredContent: make(map[string][]byte),
		dirParents:       make(map[uint64]uint64),
	}

	v.inodeCache = NewLRUCache[uint64, *CachedInode](10000, v.onEvictInode)
	v.dirCache = NewLRUCache[uint64, *CachedDir](2000, v.onEvictDir)

	for _, opt := range opts {
		opt(v)
	}

	if err := v.initMetadataStreamLocked(); err != nil {
		panic(fmt.Sprintf("failed to init metadata stream: %v", err))
	}

	if v.localStorageDir == "" {
		v.localStorageDir = path.Join(os.TempDir(), fmt.Sprintf("objectfs-local-%s-%d", volumeID, time.Now().UnixNano()))
	}
	ls, err := NewLocalStorage(v.localStorageDir, WithMaxLocalFileSize(v.maxLocalFileSize))
	if err == nil {
		v.localStore = ls
	}

	// Always initialize an initial base EROFS snapshot with empty root directory
	rootNode := erofs.NewMemoryNode("", true, 0755, nil, nil, erofs.WithIno(1), erofs.WithMtime(uint64(time.Now().Unix())))
	var initialErofsBuf bufferWriterAt
	if err := erofs.WriteImage(&initialErofsBuf, rootNode); err == nil {
		v.snapshotRaw = bytes.NewReader(initialErofsBuf.buf)
		if r, err := erofs.NewReader(v.snapshotRaw); err == nil {
			v.snapshotReader = r
			v.rootInodeID = r.GetRootNID()
			if v.rootInodeID == 0 {
				v.rootInodeID = 1
			}
			v.nextInode = erofs.DefaultInodeStride
		}
	}
	if v.rootInodeID == 0 {
		v.rootInodeID = 1
	}
	v.dirParents[v.rootInodeID] = v.rootInodeID

	return v
}

type memoryAppender struct {
	mu       sync.Mutex
	payloads [][]byte
}

func (m *memoryAppender) Append(_ context.Context, payload []byte) (uint64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.payloads = append(m.payloads, payload)
	return uint64(len(m.payloads)), nil
}

func (v *Volume) initMetadataStreamLocked() error {
	var appender record.Appender
	if v.stream != nil {
		appender = sds.NewWALAppender(v.stream, walclient.Local)
	} else {
		appender = &memoryAppender{}
	}
	w := sds.NewWriter(appender)
	if _, err := w.RegisterType(&pb.Inode{}, 1); err != nil {
		return fmt.Errorf("failed to register Inode type: %w", err)
	}
	if _, err := w.RegisterType(&pb.DirEntry{}, 1, 2); err != nil {
		return fmt.Errorf("failed to register DirEntry type: %w", err)
	}
	if _, err := w.RegisterType(&pb.Content{}, 1); err != nil {
		return fmt.Errorf("failed to register Content type: %w", err)
	}
	v.metadataStream = w
	return nil
}

func (v *Volume) ensureInodeChunksLoadedLocked(ctx context.Context, node *CachedInode) error {
	if node.ManifestSha256 == "" {
		return nil
	}
	if node.ChunkSize > 0 && len(node.Chunks) > 0 {
		return nil
	}
	if v.recoveredContent != nil {
		if mData, ok := v.recoveredContent[node.ManifestSha256]; ok {
			manifest, err := blob.DecodeManifest(bytes.NewReader(mData))
			if err == nil {
				node.Chunks = manifest.ChunkHexSHAs()
				node.ChunkSize = manifest.ChunkSize
				if node.Size == 0 {
					node.Size = int64(manifest.TotalLength)
				}
				return nil
			}
		}
	}
	if v.blobStore != nil {
		mStream, err := v.blobStore.GetBlob(ctx, node.ManifestSha256)
		if err == nil {
			defer mStream.Close()
			manifest, err := blob.DecodeManifest(mStream)
			if err == nil {
				node.Chunks = manifest.ChunkHexSHAs()
				node.ChunkSize = manifest.ChunkSize
				if node.Size == 0 {
					node.Size = int64(manifest.TotalLength)
				}
				return nil
			}
		}
	}
	return nil
}

func (v *Volume) readChunkLocked(ctx context.Context, node *CachedInode, chunkIdx int) ([]byte, error) {
	if node.DirtyChunks != nil {
		if data, ok := node.DirtyChunks[chunkIdx]; ok {
			res := make([]byte, len(data))
			copy(res, data)
			return res, nil
		}
	}

	_ = v.ensureInodeChunksLoadedLocked(ctx, node)

	if chunkIdx < len(node.Chunks) && node.Chunks[chunkIdx] == "" {
		return nil, nil
	}

	if chunkIdx < len(node.Chunks) && node.Chunks[chunkIdx] != "" {
		if v.recoveredContent != nil {
			if data, ok := v.recoveredContent[node.Chunks[chunkIdx]]; ok {
				res := make([]byte, len(data))
				copy(res, data)
				return res, nil
			}
		}
		if v.blobStore != nil {
			stream, err := v.blobStore.GetBlob(ctx, node.Chunks[chunkIdx])
			if err == nil {
				defer stream.Close()
				return io.ReadAll(stream)
			}
		}
	}

	if node.Data != nil && node.ChunkSize > 0 {
		off := int64(chunkIdx) * int64(node.ChunkSize)
		if off < node.Size {
			readLen := int64(node.ChunkSize)
			if off+readLen > node.Size {
				readLen = node.Size - off
			}
			buf := make([]byte, readLen)
			_ = node.Data.Rewind()
			if _, err := node.Data.Seek(off, io.SeekStart); err == nil {
				n, _ := io.ReadFull(node.Data, buf)
				return buf[:n], nil
			}
		}
	}

	return nil, fmt.Errorf("chunk %d not found", chunkIdx)
}

// persistInode uploads the inode's blob payload if dirty and writes the inode metadata record to local eviction storage.
func (v *Volume) persistInode(ctx context.Context, node *CachedInode) error {
	if !node.IsDirty {
		return nil
	}

	if node.ChunkSize > 0 && v.blobStore != nil {
		dirtyBlobs := make(map[string]blob.ByteStream)
		for idx, chunkBytes := range node.DirtyChunks {
			if idx < len(node.Chunks) && node.Chunks[idx] != "" {
				dirtyBlobs[node.Chunks[idx]] = blob.NewByteStreamFromBytes(chunkBytes)
			}
		}
		if node.ManifestSha256 != "" {
			manifestChunks := make([][32]byte, len(node.Chunks))
			for i, c := range node.Chunks {
				raw, _ := hex.DecodeString(c)
				if len(raw) == 32 {
					copy(manifestChunks[i][:], raw)
				}
			}
			manifest := &blob.Manifest{
				ChunkSize:   node.ChunkSize,
				TotalLength: uint64(node.Size),
				Chunks:      manifestChunks,
			}
			manifestBlob, err := blob.EncodeManifest(manifest)
			if err == nil {
				dirtyBlobs[manifestBlob.SHA256Hex()] = manifestBlob.Stream
			}
		}
		if len(dirtyBlobs) > 0 {
			if err := v.blobStore.PutBlobs(ctx, dirtyBlobs); err != nil {
				return fmt.Errorf("failed to persist chunk blobs for inode %d: %w", node.ID, err)
			}
		}
		node.DirtyChunks = nil
	} else if node.Data != nil && node.Sha256 != "" && v.blobStore != nil {
		if err := node.Data.Rewind(); err != nil {
			return fmt.Errorf("failed to rewind node data: %w", err)
		}
		if err := v.blobStore.PutBlobs(ctx, map[string]blob.ByteStream{node.Sha256: node.Data}); err != nil {
			return fmt.Errorf("failed to persist inode %d blob to blobStore: %w", node.ID, err)
		}
		_ = node.Data.Close()
		node.Data = nil
	}

	if v.localStore != nil {
		rec := &InodeRecord{
			InodeID:        node.ID,
			Mode:           node.Mode,
			Size:           node.Size,
			ModTime:        node.ModTime,
			IsDir:          node.IsDir,
			Sha256:         node.Sha256,
			ETag:           node.ETag,
			Uid:            node.Uid,
			Gid:            node.Gid,
			ManifestSha256: node.ManifestSha256,
			ContentSha256:  node.ContentSha256,
		}
		payload, err := EncodeInodeRecord(rec)
		if err != nil {
			return fmt.Errorf("failed to encode inode record %d: %w", node.ID, err)
		}
		off, err := v.localStore.WriteRecord(RecordTypeInode, payload)
		if err != nil {
			return fmt.Errorf("failed to write inode record %d to local storage: %w", node.ID, err)
		}
		v.dirtyInodes[node.ID] = off
	}

	node.IsDirty = false
	return nil
}

func (v *Volume) onEvictInode(inodeID uint64, node *CachedInode) {
	_ = v.persistInode(context.Background(), node)
}

func (v *Volume) onEvictDir(dirID uint64, dir *CachedDir) {
	if !dir.IsDirty || v.localStore == nil {
		return
	}
	if len(dir.Added) == 0 && len(dir.Deleted) == 0 && dir.PrevOffset != NoOffset {
		return
	}
	deletedList := make([]string, 0, len(dir.Deleted))
	for del := range dir.Deleted {
		deletedList = append(deletedList, del)
	}
	addedList := make([]DirEntry, 0, len(dir.Added))
	for name := range dir.Added {
		if entry, ok := dir.Entries[name]; ok {
			addedList = append(addedList, entry)
		}
	}
	rec := &DirDeltaRecord{
		InodeID:    dir.ID,
		PrevOffset: dir.PrevOffset,
		Deleted:    deletedList,
		Added:      addedList,
	}
	payload, err := EncodeDirDeltaRecord(rec)
	if err != nil {
		return
	}
	off, err := v.localStore.WriteRecord(RecordTypeDirDelta, payload)
	if err == nil {
		v.dirtyDirs[dir.ID] = off
		dir.PrevOffset = off
		dir.Added = make(map[string]bool)
		dir.Deleted = make(map[string]bool)
		dir.IsDirty = false
	}
}

// VolumeID returns the volume identifier.
func (v *Volume) VolumeID() string {
	return v.volumeID
}

// Stream returns the configured WAL stream, or nil.
func (v *Volume) Stream() walclient.Stream {
	v.mu.RLock()
	defer v.mu.RUnlock()
	return v.stream
}

// StreamID returns the stream UUID associated with this volume.
func (v *Volume) StreamID() uuid.UUID {
	return v.streamID
}

// Durability returns the default durability level configured on this volume.
func (v *Volume) Durability() walclient.Level {
	v.mu.RLock()
	defer v.mu.RUnlock()
	return v.durability
}

// Close closes the volume, local storage, and any underlying WAL streams.
func (v *Volume) Close() error {
	v.mu.Lock()
	defer v.mu.Unlock()

	var firstErr error
	if v.localStore != nil {
		if err := v.localStore.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
		_ = os.RemoveAll(v.localStorageDir)
	}
	if v.stream != nil {
		if err := v.stream.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

// logMutationLocked appends a mutation record to the WAL stream under v.mu.
// It returns a wait function that blocks until the requested durability level is reached.
// The wait function MUST be invoked outside v.mu so that WAL durability waits do not
// block concurrent volume operations.
//
// If the durability wait fails (e.g. context cancelled, stream closed), the mutation
// has already been applied to in-memory state and appended locally, so no rollback is
// attempted; the error is returned to inform the caller that durability was not achieved.
func (v *Volume) makeWaitFn(commitSeq uint64, reqLevel *walclient.Level) func(context.Context) error {
	if v.stream == nil || commitSeq == 0 {
		return nil
	}
	durability := v.durability
	if reqLevel != nil {
		durability = *reqLevel
	}

	return func(waitCtx context.Context) error {
		switch durability {
		case walclient.Permanent:
			return v.stream.Wait(waitCtx, commitSeq, walclient.Permanent, true)
		case walclient.Witness:
			return v.stream.Wait(waitCtx, commitSeq, walclient.Witness, false)
		case walclient.Local:
			// Append already fsynced locally
			return nil
		}
		return nil
	}
}

func (v *Volume) allocInode() uint64 {
	return atomic.AddUint64(&v.nextInode, erofs.DefaultInodeStride) - erofs.DefaultInodeStride
}

func cleanPath(p string) string {
	cleaned := path.Clean("/" + p)
	if !strings.HasPrefix(cleaned, "/") {
		cleaned = "/" + cleaned
	}
	return cleaned
}

func (r *metadataResolver) resolveInode(ctx context.Context, inodeID uint64, populateCache bool) (*CachedInode, error) {
	if r.inodeCache != nil {
		if populateCache {
			if node, ok := r.inodeCache.Get(inodeID); ok {
				return node, nil
			}
		} else {
			if node, ok := r.inodeCache.Peek(inodeID); ok {
				return node, nil
			}
		}
	}

	// 1. Check if evicted to local storage
	if off, isDirty := r.dirtyInodes[inodeID]; isDirty && off != NoOffset && r.localStore != nil {
		_, payload, err := r.localStore.ReadRecord(off)
		if err == nil {
			rec, err := DecodeInodeRecord(payload)
			if err == nil {
				node := &CachedInode{
					ID:             rec.InodeID,
					Mode:           rec.Mode,
					Size:           rec.Size,
					ModTime:        rec.ModTime,
					IsDir:          rec.IsDir,
					Sha256:         rec.Sha256,
					ManifestSha256: rec.ManifestSha256,
					ContentSha256:  rec.ContentSha256,
					ETag:           rec.ETag,
					Uid:            rec.Uid,
					Gid:            rec.Gid,
					IsDirty:        populateCache,
				}
				if populateCache && r.inodeCache != nil {
					r.inodeCache.Put(inodeID, node)
				}
				return node, nil
			}
		}
	}

	// 2. Fetch from base snapshot
	if r.snapshotReader != nil && r.snapshotRaw != nil {
		lookupNID := inodeID
		if lookupNID == 1 && r.snapshotReader.GetRootNID() == 0 {
			lookupNID = 0
		}
		erofsInode, err := erofs.ReadInode(r.snapshotRaw, r.snapshotReader.Superblock(), lookupNID)
		if err == nil {
			var shaStr, manifestSha, contentSha string
			xattrs, xErr := r.snapshotReader.GetXattrs(lookupNID)
			if xErr == nil && !xattrs.IsEmpty() {
				if xattrs.UserDigest != "" {
					shaStr = xattrs.UserDigest
					contentSha = xattrs.UserDigest
				} else if xattrs.UserSHA256 != "" {
					shaStr = xattrs.UserSHA256
					contentSha = xattrs.UserSHA256
				}
				if xattrs.UserManifest != "" {
					manifestSha = xattrs.UserManifest
					shaStr = manifestSha
				}
			}
			if manifestSha == "" && contentSha == "" {
				contentSha = shaStr
			}

			isDir := (erofsInode.Mode & erofs.S_IFMT) == erofs.S_IFDIR
			mode := uint32(erofsInode.Mode)
			if isDir {
				mode |= syscall.S_IFDIR
			} else {
				mode |= syscall.S_IFREG
			}

			mtime := time.Unix(int64(erofsInode.Mtime), int64(erofsInode.MtimeNsec))
			if erofsInode.Mtime == 0 {
				mtime = time.Now()
			}

			node := &CachedInode{
				ID:             inodeID,
				Mode:           mode,
				Size:           int64(erofsInode.Size),
				ModTime:        mtime,
				IsDir:          isDir,
				Sha256:         shaStr,
				ManifestSha256: manifestSha,
				ContentSha256:  contentSha,
				Uid:            erofsInode.UID,
				Gid:            erofsInode.GID,
				IsDirty:        false,
			}
			if populateCache && r.inodeCache != nil {
				r.inodeCache.Put(inodeID, node)
			}
			return node, nil
		}
	}

	return nil, fmt.Errorf("inode %d not found: %w", inodeID, syscall.ENOENT)
}

func (r *metadataResolver) resolveDir(ctx context.Context, inodeID uint64, populateCache bool) (*CachedDir, error) {
	if r.dirCache != nil {
		if populateCache {
			if dir, ok := r.dirCache.Get(inodeID); ok {
				return dir, nil
			}
		} else {
			if cached, ok := r.dirCache.Peek(inodeID); ok {
				entriesCopy := make(map[string]DirEntry, len(cached.Entries))
				for k, v := range cached.Entries {
					entriesCopy[k] = v
				}
				return &CachedDir{
					ID:         cached.ID,
					Entries:    entriesCopy,
					Added:      make(map[string]bool),
					Deleted:    make(map[string]bool),
					PrevOffset: cached.PrevOffset,
					IsDirty:    false,
				}, nil
			}
		}
	}

	entries := make(map[string]DirEntry)

	// 1. Check if dirty delta exists in local storage
	if off, isDirty := r.dirtyDirs[inodeID]; isDirty && off != NoOffset && r.localStore != nil {
		var deltas []*DirDeltaRecord
		currOff := off
		visited := make(map[LocalOffset]bool)
		for currOff != NoOffset && !visited[currOff] {
			visited[currOff] = true
			_, payload, err := r.localStore.ReadRecord(currOff)
			if err != nil {
				break
			}
			rec, err := DecodeDirDeltaRecord(payload)
			if err != nil {
				break
			}
			deltas = append(deltas, rec)
			currOff = rec.PrevOffset
		}

		// Read base from base snapshot if present
		if r.snapshotReader != nil {
			dirents, err := r.snapshotReader.ListDirectory(inodeID)
			if err == nil {
				for _, de := range dirents {
					if de.Name == "." || de.Name == ".." {
						continue
					}
					isDir := de.FileType == erofs.FTDir
					mode := uint32(0644 | syscall.S_IFREG)
					if isDir {
						mode = uint32(0755 | syscall.S_IFDIR)
					}
					entries[de.Name] = DirEntry{
						Name:    de.Name,
						InodeID: de.NID,
						IsDir:   isDir,
						Mode:    mode,
					}
				}
			}
		}

		// Replay deltas in chronological order (oldest to newest)
		for i := len(deltas) - 1; i >= 0; i-- {
			d := deltas[i]
			for _, del := range d.Deleted {
				delete(entries, del)
			}
			for _, add := range d.Added {
				entries[add.Name] = add
			}
		}

		dir := &CachedDir{
			ID:         inodeID,
			Entries:    entries,
			Added:      make(map[string]bool),
			Deleted:    make(map[string]bool),
			PrevOffset: off,
			IsDirty:    false,
		}
		if populateCache && r.dirCache != nil {
			r.dirCache.Put(inodeID, dir)
		}
		return dir, nil
	}

	// 2. Fetch clean directory from snapshot
	if r.snapshotReader != nil {
		lookupNID := inodeID
		if lookupNID == 1 && r.snapshotReader.GetRootNID() == 0 {
			lookupNID = 0
		}
		dirents, err := r.snapshotReader.ListDirectory(lookupNID)
		if err == nil {
			for _, de := range dirents {
				if de.Name == "." || de.Name == ".." {
					continue
				}
				isDir := de.FileType == erofs.FTDir
				mode := uint32(0644 | syscall.S_IFREG)
				if isDir {
					mode = uint32(0755 | syscall.S_IFDIR)
				}
				childIno := de.NID
				if childIno == 0 {
					childIno = 1
				}
				entries[de.Name] = DirEntry{
					Name:    de.Name,
					InodeID: childIno,
					IsDir:   isDir,
					Mode:    mode,
				}
			}
			dir := &CachedDir{
				ID:         inodeID,
				Entries:    entries,
				Added:      make(map[string]bool),
				Deleted:    make(map[string]bool),
				PrevOffset: NoOffset,
				IsDirty:    false,
			}
			if populateCache && r.dirCache != nil {
				r.dirCache.Put(inodeID, dir)
			}
			return dir, nil
		}
	}

	return nil, fmt.Errorf("directory inode %d not found: %w", inodeID, syscall.ENOENT)
}

func (v *Volume) getOrLoadInodeLocked(ctx context.Context, inodeID uint64) (*CachedInode, error) {
	return v.resolveInode(ctx, inodeID, true)
}

func (v *Volume) getOrLoadDirLocked(ctx context.Context, inodeID uint64) (*CachedDir, error) {
	return v.resolveDir(ctx, inodeID, true)
}

func (v *Volume) resolvePathLocked(ctx context.Context, p string) (uint64, uint64, string, error) {
	p = cleanPath(p)
	if p == "/" {
		return v.rootInodeID, 0, "", nil
	}
	parts := strings.Split(strings.Trim(p, "/"), "/")
	currInodeID := v.rootInodeID
	var parentInodeID uint64
	var baseName string
	for i, part := range parts {
		dir, err := v.getOrLoadDirLocked(ctx, currInodeID)
		if err != nil {
			return 0, 0, "", fmt.Errorf("directory for inode %d not found: %w", currInodeID, syscall.ENOENT)
		}
		entry, exists := dir.Entries[part]
		if !exists {
			return 0, 0, "", fmt.Errorf("path component %s not found: %w", part, syscall.ENOENT)
		}
		if i == len(parts)-1 {
			return entry.InodeID, currInodeID, part, nil
		}
		if !entry.IsDir {
			return 0, 0, "", fmt.Errorf("path component %s is not a directory: %w", part, syscall.ENOTDIR)
		}
		parentInodeID = currInodeID
		baseName = part
		currInodeID = entry.InodeID
	}
	return currInodeID, parentInodeID, baseName, nil
}

func (v *Volume) toEntryAttrLocked(ctx context.Context, inodeID uint64, name string) (*pb.EntryAttr, error) {
	node, err := v.getOrLoadInodeLocked(ctx, inodeID)
	if err != nil {
		return nil, err
	}
	mSha := node.ManifestSha256
	cSha := node.ContentSha256
	if node.ChunkSize == 0 && mSha == "" {
		cSha = node.Sha256
	} else if mSha == "" {
		mSha = node.Sha256
	}
	if name == "" && inodeID == v.rootInodeID {
		name = "/"
	}
	return &pb.EntryAttr{
		Inode:          node.ID,
		Name:           name,
		IsDir:          node.IsDir,
		Size:           node.Size,
		Mode:           node.Mode,
		ModTime:        timestamppb.New(node.ModTime),
		Sha256:         node.Sha256,
		ManifestSha256: mSha,
		ContentSha256:  cSha,
		RedirectUrl:    node.RedirectURL,
		Uid:            node.Uid,
		Gid:            node.Gid,
	}, nil
}

func (v *Volume) GetAttr(ctx context.Context, inodeID uint64) (*pb.EntryAttr, error) {
	v.mu.RLock()
	defer v.mu.RUnlock()

	if inodeID == 0 {
		inodeID = v.rootInodeID
	}
	return v.toEntryAttrLocked(ctx, inodeID, "")
}

func (v *Volume) Lookup(ctx context.Context, parentInodeID uint64, name string) (*pb.EntryAttr, error) {
	v.mu.RLock()
	defer v.mu.RUnlock()

	if parentInodeID == 0 {
		parentInodeID = v.rootInodeID
	}

	if name == "." {
		return v.toEntryAttrLocked(ctx, parentInodeID, ".")
	}
	if name == ".." {
		if parentInodeID == v.rootInodeID {
			return v.toEntryAttrLocked(ctx, v.rootInodeID, "..")
		}
		parentNode, err := v.getOrLoadInodeLocked(ctx, parentInodeID)
		if err != nil {
			return nil, err
		}
		if !parentNode.IsDir {
			return nil, syscall.ENOTDIR
		}
		pIno, ok := v.dirParents[parentInodeID]
		if !ok || pIno == 0 {
			pIno = v.rootInodeID
		}
		return v.toEntryAttrLocked(ctx, pIno, "..")
	}

	parentDir, err := v.getOrLoadDirLocked(ctx, parentInodeID)
	if err != nil {
		return nil, err
	}

	entry, ok := parentDir.Entries[name]
	if !ok {
		return nil, fmt.Errorf("child %s not found in inode %d: %w", name, parentInodeID, syscall.ENOENT)
	}

	return v.toEntryAttrLocked(ctx, entry.InodeID, name)
}

func (v *Volume) ReadDir(ctx context.Context, dirInodeID uint64) ([]*pb.EntryAttr, error) {
	v.mu.RLock()
	defer v.mu.RUnlock()

	if dirInodeID == 0 {
		dirInodeID = v.rootInodeID
	}

	dirNode, err := v.getOrLoadInodeLocked(ctx, dirInodeID)
	if err != nil {
		return nil, err
	}
	if !dirNode.IsDir {
		return nil, fmt.Errorf("inode %d is not a directory: %w", dirInodeID, syscall.ENOTDIR)
	}

	dir, err := v.getOrLoadDirLocked(ctx, dirInodeID)
	if err != nil {
		return nil, err
	}

	names := make([]string, 0, len(dir.Entries))
	for name := range dir.Entries {
		names = append(names, name)
	}
	sort.Strings(names)

	var entries []*pb.EntryAttr
	for _, name := range names {
		entry := dir.Entries[name]
		attr, err := v.toEntryAttrLocked(ctx, entry.InodeID, name)
		if err == nil {
			entries = append(entries, attr)
		}
	}
	return entries, nil
}

func (v *Volume) Mkdir(ctx context.Context, parentInodeID uint64, name string, mode uint32, uid, gid uint32) (*pb.EntryAttr, error) {
	attr, waitFn, err := func() (*pb.EntryAttr, func(context.Context) error, error) {
		v.mu.Lock()
		defer v.mu.Unlock()

		if parentInodeID == 0 {
			parentInodeID = v.rootInodeID
		}
		if name == "" || name == "." || name == ".." {
			return nil, nil, fmt.Errorf("invalid directory name %q: %w", name, syscall.EINVAL)
		}

		parentInode, err := v.getOrLoadInodeLocked(ctx, parentInodeID)
		if err != nil {
			return nil, nil, err
		}
		if !parentInode.IsDir {
			return nil, nil, fmt.Errorf("parent inode %d is not a directory: %w", parentInodeID, syscall.ENOTDIR)
		}

		parentDir, err := v.getOrLoadDirLocked(ctx, parentInodeID)
		if err != nil {
			return nil, nil, err
		}

		if _, exists := parentDir.Entries[name]; exists {
			return nil, nil, fmt.Errorf("directory %s already exists under inode %d: %w", name, parentInodeID, syscall.EEXIST)
		}

		if mode == 0 {
			mode = 0755
		}
		mode |= syscall.S_IFDIR

		now := time.Now()
		childInodeID := v.allocInode()

		newEntry := DirEntry{
			Name:    name,
			InodeID: childInodeID,
			IsDir:   true,
			Mode:    mode,
		}
		parentDir.Entries[name] = newEntry
		parentDir.Added[name] = true
		delete(parentDir.Deleted, name)
		parentDir.IsDirty = true
		parentInode.ModTime = now
		parentInode.IsDirty = true

		if v.dirParents == nil {
			v.dirParents = make(map[uint64]uint64)
		}
		v.dirParents[childInodeID] = parentInodeID

		childInode := &CachedInode{
			ID:      childInodeID,
			Mode:    mode,
			ModTime: now,
			IsDir:   true,
			Uid:     uid,
			Gid:     gid,
			IsDirty: true,
		}
		v.inodeCache.Put(childInodeID, childInode)

		childDir := &CachedDir{
			ID:         childInodeID,
			Entries:    make(map[string]DirEntry),
			Added:      make(map[string]bool),
			Deleted:    make(map[string]bool),
			PrevOffset: NoOffset,
			IsDirty:    true,
		}
		v.dirCache.Put(childInodeID, childDir)

		var commitSeq uint64
		tx := v.metadataStream.Begin()
		childInodeMsg := &pb.Inode{
			Ino:   proto.Uint64(childInodeID),
			Mode:  mode,
			Size:  0,
			Mtime: timestamppb.New(now),
			IsDir: true,
		}
		if _, err := tx.Insert(ctx, childInodeMsg); err != nil {
			delete(parentDir.Entries, name)
			delete(parentDir.Added, name)
			delete(v.dirParents, childInodeID)
			return nil, nil, fmt.Errorf("failed to log child inode creation: %w", err)
		}

		dirEntryMsg := &pb.DirEntry{
			ParentIno: proto.Uint64(parentInodeID),
			Name:      proto.String(name),
			Ino:       childInodeID,
			IsDir:     true,
			Mode:      mode,
		}
		if _, err := tx.Insert(ctx, dirEntryMsg); err != nil {
			delete(parentDir.Entries, name)
			delete(parentDir.Added, name)
			delete(v.dirParents, childInodeID)
			return nil, nil, fmt.Errorf("failed to log directory entry: %w", err)
		}

		parentInodeMsg := &pb.Inode{
			Ino:            proto.Uint64(parentInode.ID),
			Mode:           parentInode.Mode,
			Size:           parentInode.Size,
			Mtime:          timestamppb.New(now),
			IsDir:          true,
			Sha256:         parentInode.Sha256,
			Etag:           parentInode.ETag,
			ManifestSha256: parentInode.ManifestSha256,
			ContentSha256:  parentInode.ContentSha256,
			ChunkSize:      parentInode.ChunkSize,
		}
		if _, err := tx.Update(ctx, parentInodeMsg); err != nil {
			delete(parentDir.Entries, name)
			delete(parentDir.Added, name)
			delete(v.dirParents, childInodeID)
			return nil, nil, fmt.Errorf("failed to log parent inode update: %w", err)
		}

		commitSeq, err = tx.Commit(ctx)
		if err != nil {
			delete(parentDir.Entries, name)
			delete(parentDir.Added, name)
			delete(v.dirParents, childInodeID)
			return nil, nil, fmt.Errorf("failed to commit mkdir transaction: %w", err)
		}

		waitFn := v.makeWaitFn(commitSeq, nil)

		attr := &pb.EntryAttr{
			Inode:   childInodeID,
			Name:    name,
			IsDir:   true,
			Size:    0,
			Mode:    mode,
			ModTime: timestamppb.New(now),
			Uid:     uid,
			Gid:     gid,
		}
		v.broadcaster.Broadcast(v.volumeID, &pb.WatchVolumeResponse{
			EventType:   pb.WatchEventType_EVENT_CREATED,
			Attr:        attr,
			Inode:       childInodeID,
			ParentInode: parentInodeID,
			Name:        name,
		})

		v.checkAutoSnapshotTriggerLocked(ctx)
		return attr, waitFn, nil
	}()
	if err != nil {
		return nil, err
	}
	if waitFn != nil {
		if err := waitFn(ctx); err != nil {
			return nil, err
		}
	}
	return attr, nil
}

func (v *Volume) CreateFile(ctx context.Context, parentInodeID uint64, name string, mode uint32, initialContent []byte, uid, gid uint32) (*pb.EntryAttr, error) {
	attr, waitFn, err := func() (*pb.EntryAttr, func(context.Context) error, error) {
		v.mu.Lock()
		defer v.mu.Unlock()

		if parentInodeID == 0 {
			parentInodeID = v.rootInodeID
		}
		if name == "" || name == "." || name == ".." {
			return nil, nil, fmt.Errorf("invalid file name %q: %w", name, syscall.EINVAL)
		}

		parentInode, err := v.getOrLoadInodeLocked(ctx, parentInodeID)
		if err != nil {
			return nil, nil, err
		}
		if !parentInode.IsDir {
			return nil, nil, fmt.Errorf("parent inode %d is not a directory: %w", parentInodeID, syscall.ENOTDIR)
		}

		parentDir, err := v.getOrLoadDirLocked(ctx, parentInodeID)
		if err != nil {
			return nil, nil, err
		}

		if mode == 0 {
			mode = 0644
		}
		mode |= syscall.S_IFREG

		now := time.Now()
		var hashStr string
		if len(initialContent) > 0 {
			h := sha256.Sum256(initialContent)
			hashStr = fmt.Sprintf("%x", h)
		}

		dataCopy := make([]byte, len(initialContent))
		copy(dataCopy, initialContent)

		var stream blob.ByteStream
		if len(dataCopy) > 0 {
			stream = blob.NewByteStreamFromBytes(dataCopy)
		}

		entry, exists := parentDir.Entries[name]
		if exists {
			childInode, err := v.getOrLoadInodeLocked(ctx, entry.InodeID)
			if err != nil {
				return nil, nil, err
			}
			if childInode.IsDir {
				return nil, nil, fmt.Errorf("cannot overwrite directory with file: %w", syscall.EISDIR)
			}
			if childInode.Data != nil {
				_ = childInode.Data.Close()
				childInode.Data = nil
			}
			childInode.Mode = mode
			childInode.Size = int64(len(dataCopy))
			childInode.ModTime = now
			childInode.Uid = uid
			childInode.Gid = gid
			childInode.IsDirty = true

			if len(dataCopy) > int(v.chunkSize) && v.chunkSize > 0 {
				_, manifest, manifestBlob, _ := blob.ChunkBlobs(stream, v.chunkSize, true)
				childInode.ChunkSize = v.chunkSize
				childInode.Chunks = manifest.ChunkHexSHAs()
				childInode.ManifestSha256 = manifestBlob.SHA256Hex()
				childInode.ContentSha256 = hashStr
				childInode.Sha256 = childInode.ManifestSha256
				childInode.DirtyChunks = make(map[int][]byte)
				for i := 0; i < len(childInode.Chunks); i++ {
					start := i * int(v.chunkSize)
					end := len(dataCopy)
					if (i+1)*int(v.chunkSize) < end {
						end = (i + 1) * int(v.chunkSize)
					}
					cSlice := make([]byte, end-start)
					copy(cSlice, dataCopy[start:end])
					childInode.DirtyChunks[i] = cSlice
				}
			} else {
				childInode.Data = stream
				childInode.Sha256 = hashStr
				childInode.ContentSha256 = hashStr
				childInode.ManifestSha256 = ""
				childInode.ChunkSize = 0
				childInode.Chunks = nil
				childInode.DirtyChunks = nil
			}

			var commitSeq uint64
			tx := v.metadataStream.Begin()
			if childInode.ChunkSize > 0 && len(childInode.DirtyChunks) > 0 {
				for idx, chunkBytes := range childInode.DirtyChunks {
					if idx < len(childInode.Chunks) && childInode.Chunks[idx] != "" {
						if _, err := tx.Insert(ctx, &pb.Content{Sha256: proto.String(childInode.Chunks[idx]), Data: chunkBytes}); err != nil {
							return nil, nil, fmt.Errorf("failed to log chunk content: %w", err)
						}
					}
				}
				if childInode.ManifestSha256 != "" {
					manifestChunks := make([][32]byte, len(childInode.Chunks))
					for i, c := range childInode.Chunks {
						raw, _ := hex.DecodeString(c)
						if len(raw) == 32 {
							copy(manifestChunks[i][:], raw)
						}
					}
					manifest := &blob.Manifest{
						ChunkSize:   childInode.ChunkSize,
						TotalLength: uint64(childInode.Size),
						Chunks:      manifestChunks,
					}
					manifestBlob, err := blob.EncodeManifest(manifest)
					if err != nil {
						return nil, nil, fmt.Errorf("failed to encode manifest: %w", err)
					}
					if err := manifestBlob.Stream.Rewind(); err != nil {
						return nil, nil, fmt.Errorf("failed to rewind manifest stream: %w", err)
					}
					mBytes, err := io.ReadAll(manifestBlob.Stream)
					if err != nil {
						return nil, nil, fmt.Errorf("failed to read manifest stream: %w", err)
					}
					if _, err := tx.Insert(ctx, &pb.Content{Sha256: proto.String(manifestBlob.SHA256Hex()), Data: mBytes}); err != nil {
						return nil, nil, fmt.Errorf("failed to log manifest content: %w", err)
					}
				}
			} else if len(dataCopy) > 0 {
				if _, err := tx.Insert(ctx, &pb.Content{Sha256: proto.String(childInode.Sha256), Data: dataCopy}); err != nil {
					return nil, nil, fmt.Errorf("failed to log file content: %w", err)
				}
			}

			childInodeMsg := &pb.Inode{
				Ino:            proto.Uint64(childInode.ID),
				Mode:           childInode.Mode,
				Size:           childInode.Size,
				Mtime:          timestamppb.New(now),
				IsDir:          false,
				Sha256:         childInode.Sha256,
				Etag:           childInode.ETag,
				ManifestSha256: childInode.ManifestSha256,
				ContentSha256:  childInode.ContentSha256,
				ChunkSize:      childInode.ChunkSize,
			}
			if _, err := tx.Update(ctx, childInodeMsg); err != nil {
				return nil, nil, fmt.Errorf("failed to log child inode update: %w", err)
			}

			parentInodeMsg := &pb.Inode{
				Ino:            proto.Uint64(parentInode.ID),
				Mode:           parentInode.Mode,
				Size:           parentInode.Size,
				Mtime:          timestamppb.New(now),
				IsDir:          true,
				Sha256:         parentInode.Sha256,
				Etag:           parentInode.ETag,
				ManifestSha256: parentInode.ManifestSha256,
				ContentSha256:  parentInode.ContentSha256,
				ChunkSize:      parentInode.ChunkSize,
			}
			if _, err := tx.Update(ctx, parentInodeMsg); err != nil {
				return nil, nil, fmt.Errorf("failed to log parent inode update: %w", err)
			}

			commitSeq, err = tx.Commit(ctx)
			if err != nil {
				return nil, nil, fmt.Errorf("failed to commit overwrite file transaction: %w", err)
			}

			waitFn := v.makeWaitFn(commitSeq, nil)

			attr := &pb.EntryAttr{
				Inode:          childInode.ID,
				Name:           name,
				IsDir:          false,
				Size:           childInode.Size,
				Mode:           childInode.Mode,
				ModTime:        timestamppb.New(now),
				Sha256:         childInode.Sha256,
				ManifestSha256: childInode.ManifestSha256,
				ContentSha256:  childInode.ContentSha256,
				Uid:            uid,
				Gid:            gid,
			}
			v.broadcaster.Broadcast(v.volumeID, &pb.WatchVolumeResponse{
				EventType:   pb.WatchEventType_EVENT_MODIFIED,
				Attr:        attr,
				Inode:       childInode.ID,
				ParentInode: parentInodeID,
				Name:        name,
			})
			v.checkAutoSnapshotTriggerLocked(ctx)
			return attr, waitFn, nil
		}

		childInodeID := v.allocInode()
		newEntry := DirEntry{
			Name:    name,
			InodeID: childInodeID,
			IsDir:   false,
			Mode:    mode,
		}
		parentDir.Entries[name] = newEntry
		parentDir.Added[name] = true
		delete(parentDir.Deleted, name)
		parentDir.IsDirty = true
		parentInode.ModTime = now
		parentInode.IsDirty = true

		childInode := &CachedInode{
			ID:      childInodeID,
			Mode:    mode,
			Size:    int64(len(dataCopy)),
			ModTime: now,
			IsDir:   false,
			Uid:     uid,
			Gid:     gid,
			IsDirty: true,
		}

		if len(dataCopy) > int(v.chunkSize) && v.chunkSize > 0 {
			_, manifest, manifestBlob, _ := blob.ChunkBlobs(stream, v.chunkSize, true)
			childInode.ChunkSize = v.chunkSize
			childInode.Chunks = manifest.ChunkHexSHAs()
			childInode.ManifestSha256 = manifestBlob.SHA256Hex()
			childInode.ContentSha256 = hashStr
			childInode.Sha256 = childInode.ManifestSha256
			childInode.DirtyChunks = make(map[int][]byte)
			for i := 0; i < len(childInode.Chunks); i++ {
				start := i * int(v.chunkSize)
				end := len(dataCopy)
				if (i+1)*int(v.chunkSize) < end {
					end = (i + 1) * int(v.chunkSize)
				}
				cSlice := make([]byte, end-start)
				copy(cSlice, dataCopy[start:end])
				childInode.DirtyChunks[i] = cSlice
			}
		} else {
			childInode.Data = stream
			childInode.Sha256 = hashStr
			childInode.ContentSha256 = hashStr
			childInode.ManifestSha256 = ""
			childInode.ChunkSize = 0
		}
		v.inodeCache.Put(childInodeID, childInode)

		var commitSeq uint64
		tx := v.metadataStream.Begin()
		if childInode.ChunkSize > 0 && len(childInode.DirtyChunks) > 0 {
			for idx, chunkBytes := range childInode.DirtyChunks {
				if idx < len(childInode.Chunks) && childInode.Chunks[idx] != "" {
					if _, err := tx.Insert(ctx, &pb.Content{Sha256: proto.String(childInode.Chunks[idx]), Data: chunkBytes}); err != nil {
						delete(parentDir.Entries, name)
						delete(parentDir.Added, name)
						return nil, nil, fmt.Errorf("failed to log chunk content: %w", err)
					}
				}
			}
			if childInode.ManifestSha256 != "" {
				manifestChunks := make([][32]byte, len(childInode.Chunks))
				for i, c := range childInode.Chunks {
					raw, _ := hex.DecodeString(c)
					if len(raw) == 32 {
						copy(manifestChunks[i][:], raw)
					}
				}
				manifest := &blob.Manifest{
					ChunkSize:   childInode.ChunkSize,
					TotalLength: uint64(childInode.Size),
					Chunks:      manifestChunks,
				}
				manifestBlob, err := blob.EncodeManifest(manifest)
				if err != nil {
					delete(parentDir.Entries, name)
					delete(parentDir.Added, name)
					return nil, nil, fmt.Errorf("failed to encode manifest: %w", err)
				}
				if err := manifestBlob.Stream.Rewind(); err != nil {
					delete(parentDir.Entries, name)
					delete(parentDir.Added, name)
					return nil, nil, fmt.Errorf("failed to rewind manifest stream: %w", err)
				}
				mBytes, err := io.ReadAll(manifestBlob.Stream)
				if err != nil {
					delete(parentDir.Entries, name)
					delete(parentDir.Added, name)
					return nil, nil, fmt.Errorf("failed to read manifest stream: %w", err)
				}
				if _, err := tx.Insert(ctx, &pb.Content{Sha256: proto.String(manifestBlob.SHA256Hex()), Data: mBytes}); err != nil {
					delete(parentDir.Entries, name)
					delete(parentDir.Added, name)
					return nil, nil, fmt.Errorf("failed to log manifest content: %w", err)
				}
			}
		} else if len(dataCopy) > 0 {
			if _, err := tx.Insert(ctx, &pb.Content{Sha256: proto.String(childInode.Sha256), Data: dataCopy}); err != nil {
				delete(parentDir.Entries, name)
				delete(parentDir.Added, name)
				return nil, nil, fmt.Errorf("failed to log file content: %w", err)
			}
		}

		childInodeMsg := &pb.Inode{
			Ino:            proto.Uint64(childInodeID),
			Mode:           mode,
			Size:           int64(len(dataCopy)),
			Mtime:          timestamppb.New(now),
			IsDir:          false,
			Sha256:         childInode.Sha256,
			Etag:           childInode.ETag,
			ManifestSha256: childInode.ManifestSha256,
			ContentSha256:  childInode.ContentSha256,
			ChunkSize:      childInode.ChunkSize,
		}
		if _, err := tx.Insert(ctx, childInodeMsg); err != nil {
			delete(parentDir.Entries, name)
			delete(parentDir.Added, name)
			return nil, nil, fmt.Errorf("failed to log child inode creation: %w", err)
		}

		dirEntryMsg := &pb.DirEntry{
			ParentIno: proto.Uint64(parentInodeID),
			Name:      proto.String(name),
			Ino:       childInodeID,
			IsDir:     false,
			Mode:      mode,
		}
		if _, err := tx.Insert(ctx, dirEntryMsg); err != nil {
			delete(parentDir.Entries, name)
			delete(parentDir.Added, name)
			return nil, nil, fmt.Errorf("failed to log directory entry: %w", err)
		}

		parentInodeMsg := &pb.Inode{
			Ino:            proto.Uint64(parentInode.ID),
			Mode:           parentInode.Mode,
			Size:           parentInode.Size,
			Mtime:          timestamppb.New(now),
			IsDir:          true,
			Sha256:         parentInode.Sha256,
			Etag:           parentInode.ETag,
			ManifestSha256: parentInode.ManifestSha256,
			ContentSha256:  parentInode.ContentSha256,
			ChunkSize:      parentInode.ChunkSize,
		}
		if _, err := tx.Update(ctx, parentInodeMsg); err != nil {
			delete(parentDir.Entries, name)
			delete(parentDir.Added, name)
			return nil, nil, fmt.Errorf("failed to log parent inode update: %w", err)
		}
		commitSeq, err = tx.Commit(ctx)
		if err != nil {
			delete(parentDir.Entries, name)
			delete(parentDir.Added, name)
			return nil, nil, fmt.Errorf("failed to commit create file transaction: %w", err)
		}

		waitFn := v.makeWaitFn(commitSeq, nil)

		attr := &pb.EntryAttr{
			Inode:          childInodeID,
			Name:           name,
			IsDir:          false,
			Size:           int64(len(dataCopy)),
			Mode:           mode,
			ModTime:        timestamppb.New(now),
			Sha256:         childInode.Sha256,
			ManifestSha256: childInode.ManifestSha256,
			ContentSha256:  childInode.ContentSha256,
			Uid:            uid,
			Gid:            gid,
		}
		v.broadcaster.Broadcast(v.volumeID, &pb.WatchVolumeResponse{
			EventType:   pb.WatchEventType_EVENT_CREATED,
			Attr:        attr,
			Inode:       childInodeID,
			ParentInode: parentInodeID,
			Name:        name,
		})

		v.checkAutoSnapshotTriggerLocked(ctx)
		return attr, waitFn, nil
	}()
	if err != nil {
		return nil, err
	}
	if waitFn != nil {
		if err := waitFn(ctx); err != nil {
			return nil, err
		}
	}
	return attr, nil
}

func (v *Volume) ReadFile(ctx context.Context, inodeID uint64, offset, length int64) ([]byte, int64, string, error) {
	v.mu.RLock()
	if inodeID == 0 {
		inodeID = v.rootInodeID
	}

	node, err := v.getOrLoadInodeLocked(ctx, inodeID)
	v.mu.RUnlock()
	if err != nil {
		return nil, 0, "", err
	}

	if node.IsDir {
		return nil, 0, "", fmt.Errorf("cannot read directory as file: %w", syscall.EISDIR)
	}

	total := node.Size
	if node.RedirectURL != "" && length > v.maxInlineLen {
		return nil, total, node.RedirectURL, nil
	}

	if offset >= total {
		return []byte{}, total, "", nil
	}

	end := offset + length
	if length <= 0 || end > total {
		end = total
	}
	readLen := end - offset

	v.mu.Lock()
	defer v.mu.Unlock()

	if node.ManifestSha256 != "" || node.ChunkSize > 0 {
		_ = v.ensureInodeChunksLoadedLocked(ctx, node)
		cs := int64(node.ChunkSize)
		if cs == 0 {
			cs = int64(v.chunkSize)
			if cs == 0 {
				cs = 32 * 1024
			}
		}
		startChunk := int(offset / cs)
		endChunk := int((end - 1) / cs)

		var res bytes.Buffer
		for i := startChunk; i <= endChunk; i++ {
			chunkData, err := v.readChunkLocked(ctx, node, i)
			if err != nil {
				return nil, 0, "", fmt.Errorf("failed to read chunk %d: %w", i, err)
			}
			chunkLen := cs
			if int64(i+1)*cs > total {
				chunkLen = total - int64(i)*cs
			}
			chunkStart := int64(i) * cs
			rStart := offset - chunkStart
			if rStart < 0 {
				rStart = 0
			}
			rEnd := end - chunkStart
			if rEnd > chunkLen {
				rEnd = chunkLen
			}
			if rEnd > rStart {
				for b := rStart; b < rEnd; b++ {
					if b < int64(len(chunkData)) {
						res.WriteByte(chunkData[b])
					} else {
						res.WriteByte(0)
					}
				}
			}
		}
		return res.Bytes(), total, "", nil
	}

	// Lazy load data from blob store if not currently in memory
	if node.Data == nil && node.Size > 0 {
		if node.Sha256 != "" && v.blobStore != nil {
			stream, err := v.blobStore.GetBlob(ctx, node.Sha256)
			if err == nil {
				node.Data = stream
			}
		}
	}

	if node.Data == nil {
		return []byte{}, total, "", nil
	}

	if _, err := node.Data.Seek(offset, io.SeekStart); err != nil {
		return nil, 0, "", fmt.Errorf("failed to seek node data: %w", err)
	}

	res := make([]byte, readLen)
	n, err := io.ReadFull(node.Data, res)
	if err != nil && err != io.EOF && err != io.ErrUnexpectedEOF {
		return nil, 0, "", fmt.Errorf("failed to read node data: %w", err)
	}
	return res[:n], total, "", nil
}

func (v *Volume) WriteFile(ctx context.Context, inodeID uint64, offset int64, data []byte, writeMode pb.WriteMode) (int64, int64, time.Time, error) {
	nWritten, newSize, modTime, waitFn, err := func() (int64, int64, time.Time, func(context.Context) error, error) {
		v.mu.Lock()
		defer v.mu.Unlock()

		if inodeID == 0 {
			inodeID = v.rootInodeID
		}

		node, err := v.getOrLoadInodeLocked(ctx, inodeID)
		if err != nil {
			return 0, 0, time.Time{}, nil, err
		}

		if node.IsDir {
			return 0, 0, time.Time{}, nil, fmt.Errorf("cannot write to directory: %w", syscall.EISDIR)
		}

		effectiveChunkSize := v.chunkSize
		if effectiveChunkSize == 0 {
			effectiveChunkSize = 32 * 1024
		}
		if node.ManifestSha256 != "" && node.ChunkSize == 0 {
			_ = v.ensureInodeChunksLoadedLocked(ctx, node)
		}
		if node.ChunkSize > 0 {
			effectiveChunkSize = node.ChunkSize
		}

		neededLen := offset + int64(len(data))
		calculatedSize := neededLen
		if calculatedSize < node.Size {
			calculatedSize = node.Size
		}

		now := time.Now()
		node.ModTime = now
		node.IsDirty = true

		if calculatedSize > int64(effectiveChunkSize) || node.ManifestSha256 != "" || node.ChunkSize > 0 {
			if node.ChunkSize == 0 {
				var currentData []byte
				if node.Data != nil {
					_ = node.Data.Rewind()
					currentData, _ = io.ReadAll(node.Data)
					_ = node.Data.Close()
					node.Data = nil
				} else if node.Sha256 != "" && v.blobStore != nil {
					stream, err := v.blobStore.GetBlob(ctx, node.Sha256)
					if err == nil {
						currentData, _ = io.ReadAll(stream)
						_ = stream.Close()
					}
				}
				node.ChunkSize = effectiveChunkSize
				node.Chunks = nil
				if node.DirtyChunks == nil {
					node.DirtyChunks = make(map[int][]byte)
				}
				rem := int64(len(currentData))
				chunkIdx := 0
				for rem > 0 {
					take := int64(effectiveChunkSize)
					if rem < take {
						take = rem
					}
					cSlice := make([]byte, take)
					copy(cSlice, currentData[int64(chunkIdx)*int64(effectiveChunkSize):int64(chunkIdx)*int64(effectiveChunkSize)+take])
					node.DirtyChunks[chunkIdx] = cSlice
					cSha := fmt.Sprintf("%x", sha256.Sum256(cSlice))
					node.Chunks = append(node.Chunks, cSha)
					rem -= take
					chunkIdx++
				}
			} else {
				_ = v.ensureInodeChunksLoadedLocked(ctx, node)
				if node.DirtyChunks == nil {
					node.DirtyChunks = make(map[int][]byte)
				}
			}

			cs := int64(effectiveChunkSize)
			startChunk := int(offset / cs)
			endChunk := int((offset + int64(len(data)) - 1) / cs)

			for len(node.Chunks) <= endChunk {
				node.Chunks = append(node.Chunks, "")
			}

			for i := startChunk; i <= endChunk; i++ {
				chunkStart := int64(i) * cs
				chunkEnd := chunkStart + cs
				wStart := offset - chunkStart
				if wStart < 0 {
					wStart = 0
				}
				wEnd := offset + int64(len(data)) - chunkStart
				if wEnd > cs {
					wEnd = cs
				}
				dataStart := chunkStart - offset
				if dataStart < 0 {
					dataStart = 0
				}
				dataEnd := chunkEnd - offset
				if dataEnd > int64(len(data)) {
					dataEnd = int64(len(data))
				}

				chunkData, _ := v.readChunkLocked(ctx, node, i)
				expectedChunkLen := cs
				if int64(i+1)*cs > calculatedSize {
					expectedChunkLen = calculatedSize - int64(i)*cs
				}
				if int64(len(chunkData)) < expectedChunkLen {
					newBuf := make([]byte, expectedChunkLen)
					copy(newBuf, chunkData)
					chunkData = newBuf
				}
				copy(chunkData[wStart:wEnd], data[dataStart:dataEnd])
				chunkSha := fmt.Sprintf("%x", sha256.Sum256(chunkData))
				node.Chunks[i] = chunkSha
				node.DirtyChunks[i] = chunkData
			}

			node.Size = calculatedSize

			manifestChunks := make([][32]byte, len(node.Chunks))
			for i, c := range node.Chunks {
				raw, _ := hex.DecodeString(c)
				if len(raw) == 32 {
					copy(manifestChunks[i][:], raw)
				}
			}
			manifest := &blob.Manifest{
				ChunkSize:   effectiveChunkSize,
				TotalLength: uint64(node.Size),
				Chunks:      manifestChunks,
			}
			manifestBlob, err := blob.EncodeManifest(manifest)
			if err == nil {
				node.ManifestSha256 = manifestBlob.SHA256Hex()
				node.Sha256 = node.ManifestSha256
			}

			if offset == 0 && int64(len(data)) == node.Size {
				h := sha256.Sum256(data)
				node.ContentSha256 = fmt.Sprintf("%x", h)
			} else {
				node.ContentSha256 = ""
			}
		} else {
			var currentData []byte
			if node.Data != nil {
				_ = node.Data.Rewind()
				currentData, _ = io.ReadAll(node.Data)
				_ = node.Data.Close()
			}

			if neededLen > int64(len(currentData)) {
				newBuf := make([]byte, neededLen)
				copy(newBuf, currentData)
				currentData = newBuf
			}
			copy(currentData[offset:], data)
			node.Size = int64(len(currentData))

			h := sha256.Sum256(currentData)
			node.Sha256 = fmt.Sprintf("%x", h)
			node.ContentSha256 = node.Sha256
			node.ManifestSha256 = ""
			node.ChunkSize = 0
			node.Chunks = nil
			node.DirtyChunks = nil

			if node.Size <= blob.MemoryThreshold {
				node.Data = blob.NewByteStreamFromBytes(currentData)
			} else {
				tf, err := os.CreateTemp("", "objectfs-node-*")
				if err == nil {
					_, _ = tf.Write(currentData)
					_, _ = tf.Seek(0, io.SeekStart)
					node.Data = blob.NewByteStreamFromFile(tf, int64(len(currentData)), true)
				} else {
					node.Data = blob.NewByteStreamFromBytes(currentData)
				}
			}
		}

		var reqLevel *walclient.Level
		switch writeMode {
		case pb.WriteMode_WRITE_THROUGH_FSYNC:
			l := walclient.Permanent
			reqLevel = &l
		case pb.WriteMode_EAGER_REPLICATION:
			l := walclient.Witness
			reqLevel = &l
		case pb.WriteMode_LAZY_WRITE:
			l := walclient.Local
			reqLevel = &l
		}

		var commitSeq uint64
		tx := v.metadataStream.Begin()
		if node.ChunkSize > 0 && len(node.DirtyChunks) > 0 {
			for idx, chunkBytes := range node.DirtyChunks {
				if idx < len(node.Chunks) && node.Chunks[idx] != "" {
					if _, err := tx.Insert(ctx, &pb.Content{Sha256: proto.String(node.Chunks[idx]), Data: chunkBytes}); err != nil {
						return 0, 0, time.Time{}, nil, fmt.Errorf("failed to log chunk content: %w", err)
					}
				}
			}
			if node.ManifestSha256 != "" {
				manifestChunks := make([][32]byte, len(node.Chunks))
				for i, c := range node.Chunks {
					raw, _ := hex.DecodeString(c)
					if len(raw) == 32 {
						copy(manifestChunks[i][:], raw)
					}
				}
				manifest := &blob.Manifest{
					ChunkSize:   node.ChunkSize,
					TotalLength: uint64(node.Size),
					Chunks:      manifestChunks,
				}
				manifestBlob, err := blob.EncodeManifest(manifest)
				if err != nil {
					return 0, 0, time.Time{}, nil, fmt.Errorf("failed to encode manifest: %w", err)
				}
				if err := manifestBlob.Stream.Rewind(); err != nil {
					return 0, 0, time.Time{}, nil, fmt.Errorf("failed to rewind manifest stream: %w", err)
				}
				mBytes, err := io.ReadAll(manifestBlob.Stream)
				if err != nil {
					return 0, 0, time.Time{}, nil, fmt.Errorf("failed to read manifest stream: %w", err)
				}
				if _, err := tx.Insert(ctx, &pb.Content{Sha256: proto.String(manifestBlob.SHA256Hex()), Data: mBytes}); err != nil {
					return 0, 0, time.Time{}, nil, fmt.Errorf("failed to log manifest content: %w", err)
				}
			}
		} else if node.Data != nil && node.Sha256 != "" {
			if err := node.Data.Rewind(); err != nil {
				return 0, 0, time.Time{}, nil, fmt.Errorf("failed to rewind node data: %w", err)
			}
			fullBytes, err := io.ReadAll(node.Data)
			if err != nil {
				return 0, 0, time.Time{}, nil, fmt.Errorf("failed to read node data: %w", err)
			}
			if err := node.Data.Rewind(); err != nil {
				return 0, 0, time.Time{}, nil, fmt.Errorf("failed to rewind node data after read: %w", err)
			}
			if _, err := tx.Insert(ctx, &pb.Content{Sha256: proto.String(node.Sha256), Data: fullBytes}); err != nil {
				return 0, 0, time.Time{}, nil, fmt.Errorf("failed to log file content: %w", err)
			}
		}

		nodeInodeMsg := &pb.Inode{
			Ino:            proto.Uint64(node.ID),
			Mode:           node.Mode,
			Size:           node.Size,
			Mtime:          timestamppb.New(now),
			IsDir:          false,
			Sha256:         node.Sha256,
			Etag:           node.ETag,
			ManifestSha256: node.ManifestSha256,
			ContentSha256:  node.ContentSha256,
			ChunkSize:      node.ChunkSize,
		}
		if _, err := tx.Update(ctx, nodeInodeMsg); err != nil {
			return 0, 0, time.Time{}, nil, fmt.Errorf("failed to log node inode update: %w", err)
		}

		commitSeq, err = tx.Commit(ctx)
		if err != nil {
			return 0, 0, time.Time{}, nil, fmt.Errorf("failed to commit write file transaction: %w", err)
		}

		waitFn := v.makeWaitFn(commitSeq, reqLevel)

		attr := &pb.EntryAttr{
			Inode:          node.ID,
			IsDir:          false,
			Size:           node.Size,
			Mode:           node.Mode,
			ModTime:        timestamppb.New(now),
			Sha256:         node.Sha256,
			ManifestSha256: node.ManifestSha256,
			ContentSha256:  node.ContentSha256,
			Uid:            node.Uid,
			Gid:            node.Gid,
		}
		v.broadcaster.Broadcast(v.volumeID, &pb.WatchVolumeResponse{
			EventType: pb.WatchEventType_EVENT_MODIFIED,
			Attr:      attr,
			Inode:     node.ID,
		})

		v.checkAutoSnapshotTriggerLocked(ctx)
		return int64(len(data)), node.Size, now, waitFn, nil
	}()
	if err != nil {
		return 0, 0, time.Time{}, err
	}
	if waitFn != nil {
		if err := waitFn(ctx); err != nil {
			return 0, 0, time.Time{}, err
		}
	}
	return nWritten, newSize, modTime, nil
}

func (v *Volume) TruncateFile(ctx context.Context, inodeID uint64, size int64) (*pb.EntryAttr, error) {
	attr, waitFn, err := func() (*pb.EntryAttr, func(context.Context) error, error) {
		v.mu.Lock()
		defer v.mu.Unlock()

		if inodeID == 0 {
			inodeID = v.rootInodeID
		}

		node, err := v.getOrLoadInodeLocked(ctx, inodeID)
		if err != nil {
			return nil, nil, err
		}

		if node.IsDir {
			return nil, nil, fmt.Errorf("cannot truncate directory: %w", syscall.EISDIR)
		}

		if size < 0 {
			return nil, nil, fmt.Errorf("invalid size %d: %w", size, syscall.EINVAL)
		}

		now := time.Now()
		node.ModTime = now
		node.IsDirty = true

		if node.ManifestSha256 != "" || node.ChunkSize > 0 {
			_ = v.ensureInodeChunksLoadedLocked(ctx, node)
			if node.DirtyChunks == nil {
				node.DirtyChunks = make(map[int][]byte)
			}

			if size == 0 {
				node.Size = 0
				node.Chunks = nil
				node.DirtyChunks = nil
				node.ChunkSize = 0
				node.ManifestSha256 = ""
				node.ContentSha256 = fmt.Sprintf("%x", sha256.Sum256([]byte{}))
				node.Sha256 = node.ContentSha256
				if node.Data != nil {
					_ = node.Data.Close()
					node.Data = nil
				}
			} else {
				cs := int64(node.ChunkSize)
				numChunks := int((size + cs - 1) / cs)
				if numChunks < len(node.Chunks) {
					for k := range node.DirtyChunks {
						if k >= numChunks {
							delete(node.DirtyChunks, k)
						}
					}
					node.Chunks = node.Chunks[:numChunks]
				} else {
					for len(node.Chunks) < numChunks {
						node.Chunks = append(node.Chunks, "")
					}
				}
				if numChunks > 0 {
					lastIdx := numChunks - 1
					lastChunkLen := size - int64(lastIdx)*cs
					chunkData, _ := v.readChunkLocked(ctx, node, lastIdx)
					if int64(len(chunkData)) > lastChunkLen {
						chunkData = chunkData[:lastChunkLen]
					} else if int64(len(chunkData)) < lastChunkLen {
						newBuf := make([]byte, lastChunkLen)
						copy(newBuf, chunkData)
						chunkData = newBuf
					}
					chunkSha := fmt.Sprintf("%x", sha256.Sum256(chunkData))
					node.Chunks[lastIdx] = chunkSha
					node.DirtyChunks[lastIdx] = chunkData
				}

				node.Size = size

				manifestChunks := make([][32]byte, len(node.Chunks))
				for i, c := range node.Chunks {
					raw, _ := hex.DecodeString(c)
					if len(raw) == 32 {
						copy(manifestChunks[i][:], raw)
					}
				}
				manifest := &blob.Manifest{
					ChunkSize:   node.ChunkSize,
					TotalLength: uint64(node.Size),
					Chunks:      manifestChunks,
				}
				manifestBlob, err := blob.EncodeManifest(manifest)
				if err == nil {
					node.ManifestSha256 = manifestBlob.SHA256Hex()
					node.Sha256 = node.ManifestSha256
				}
				node.ContentSha256 = ""
			}
		} else {
			var currentData []byte
			if node.Data != nil {
				_ = node.Data.Rewind()
				currentData, _ = io.ReadAll(node.Data)
				_ = node.Data.Close()
			}

			if size < int64(len(currentData)) {
				currentData = currentData[:size]
			} else if size > int64(len(currentData)) {
				newBuf := make([]byte, size)
				copy(newBuf, currentData)
				currentData = newBuf
			}
			node.Size = size
			h := sha256.Sum256(currentData)
			node.Sha256 = fmt.Sprintf("%x", h)
			node.ContentSha256 = node.Sha256
			node.ManifestSha256 = ""
			node.ChunkSize = 0

			if node.Size <= blob.MemoryThreshold {
				node.Data = blob.NewByteStreamFromBytes(currentData)
			} else {
				tf, err := os.CreateTemp("", "objectfs-node-*")
				if err == nil {
					_, _ = tf.Write(currentData)
					_, _ = tf.Seek(0, io.SeekStart)
					node.Data = blob.NewByteStreamFromFile(tf, int64(len(currentData)), true)
				} else {
					node.Data = blob.NewByteStreamFromBytes(currentData)
				}
			}
		}

		var commitSeq uint64
		tx := v.metadataStream.Begin()
		if node.ChunkSize > 0 {
			if len(node.DirtyChunks) > 0 {
				for idx, chunkBytes := range node.DirtyChunks {
					if idx < len(node.Chunks) && node.Chunks[idx] != "" {
						if _, err := tx.Insert(ctx, &pb.Content{Sha256: proto.String(node.Chunks[idx]), Data: chunkBytes}); err != nil {
							return nil, nil, fmt.Errorf("failed to log chunk content: %w", err)
						}
					}
				}
			}
			if node.ManifestSha256 != "" {
				manifestChunks := make([][32]byte, len(node.Chunks))
				for i, c := range node.Chunks {
					raw, _ := hex.DecodeString(c)
					if len(raw) == 32 {
						copy(manifestChunks[i][:], raw)
					}
				}
				manifest := &blob.Manifest{
					ChunkSize:   node.ChunkSize,
					TotalLength: uint64(node.Size),
					Chunks:      manifestChunks,
				}
				manifestBlob, err := blob.EncodeManifest(manifest)
				if err != nil {
					return nil, nil, fmt.Errorf("failed to encode manifest: %w", err)
				}
				if err := manifestBlob.Stream.Rewind(); err != nil {
					return nil, nil, fmt.Errorf("failed to rewind manifest stream: %w", err)
				}
				mBytes, err := io.ReadAll(manifestBlob.Stream)
				if err != nil {
					return nil, nil, fmt.Errorf("failed to read manifest stream: %w", err)
				}
				if _, err := tx.Insert(ctx, &pb.Content{Sha256: proto.String(manifestBlob.SHA256Hex()), Data: mBytes}); err != nil {
					return nil, nil, fmt.Errorf("failed to log manifest content: %w", err)
				}
			}
		} else if node.Data != nil && node.Sha256 != "" {
			if err := node.Data.Rewind(); err != nil {
				return nil, nil, fmt.Errorf("failed to rewind node data: %w", err)
			}
			fullBytes, err := io.ReadAll(node.Data)
			if err != nil {
				return nil, nil, fmt.Errorf("failed to read node data: %w", err)
			}
			if err := node.Data.Rewind(); err != nil {
				return nil, nil, fmt.Errorf("failed to rewind node data after read: %w", err)
			}
			if _, err := tx.Insert(ctx, &pb.Content{Sha256: proto.String(node.Sha256), Data: fullBytes}); err != nil {
				return nil, nil, fmt.Errorf("failed to log file content: %w", err)
			}
		}

		nodeInodeMsg := &pb.Inode{
			Ino:            proto.Uint64(node.ID),
			Mode:           node.Mode,
			Size:           node.Size,
			Mtime:          timestamppb.New(now),
			IsDir:          false,
			Sha256:         node.Sha256,
			Etag:           node.ETag,
			ManifestSha256: node.ManifestSha256,
			ContentSha256:  node.ContentSha256,
			ChunkSize:      node.ChunkSize,
		}
		if _, err := tx.Update(ctx, nodeInodeMsg); err != nil {
			return nil, nil, fmt.Errorf("failed to log node inode update: %w", err)
		}

		commitSeq, err = tx.Commit(ctx)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to commit truncate file transaction: %w", err)
		}

		waitFn := v.makeWaitFn(commitSeq, nil)

		attr := &pb.EntryAttr{
			Inode:          node.ID,
			IsDir:          false,
			Size:           node.Size,
			Mode:           node.Mode,
			ModTime:        timestamppb.New(now),
			Sha256:         node.Sha256,
			ManifestSha256: node.ManifestSha256,
			ContentSha256:  node.ContentSha256,
			Uid:            node.Uid,
			Gid:            node.Gid,
		}
		v.broadcaster.Broadcast(v.volumeID, &pb.WatchVolumeResponse{
			EventType: pb.WatchEventType_EVENT_MODIFIED,
			Attr:      attr,
			Inode:     node.ID,
		})
		v.checkAutoSnapshotTriggerLocked(ctx)
		return attr, waitFn, nil
	}()
	if err != nil {
		return nil, err
	}
	if waitFn != nil {
		if err := waitFn(ctx); err != nil {
			return nil, err
		}
	}
	return attr, nil
}

func (v *Volume) Unlink(ctx context.Context, parentInodeID uint64, name string) error {
	waitFn, err := func() (func(context.Context) error, error) {
		v.mu.Lock()
		defer v.mu.Unlock()

		if parentInodeID == 0 {
			parentInodeID = v.rootInodeID
		}

		parentDir, err := v.getOrLoadDirLocked(ctx, parentInodeID)
		if err != nil {
			return nil, err
		}

		entry, ok := parentDir.Entries[name]
		if !ok {
			return nil, fmt.Errorf("file %s not found under inode %d: %w", name, parentInodeID, syscall.ENOENT)
		}

		childInode, err := v.getOrLoadInodeLocked(ctx, entry.InodeID)
		if err != nil {
			return nil, err
		}
		if childInode.IsDir {
			return nil, fmt.Errorf("cannot unlink directory %s: %w", name, syscall.EISDIR)
		}

		if childInode.Data != nil {
			_ = childInode.Data.Close()
		}

		delete(parentDir.Entries, name)
		delete(parentDir.Added, name)
		parentDir.Deleted[name] = true
		parentDir.IsDirty = true

		parentInode, _ := v.getOrLoadInodeLocked(ctx, parentInodeID)
		if parentInode != nil {
			parentInode.ModTime = time.Now()
			parentInode.IsDirty = true
		}

		var commitSeq uint64
		tx := v.metadataStream.Begin()
		if _, err := tx.Delete(ctx, &pb.DirEntry{ParentIno: proto.Uint64(parentInodeID), Name: proto.String(name)}); err != nil {
			return nil, fmt.Errorf("failed to log dir entry deletion: %w", err)
		}
		if _, err := tx.Delete(ctx, &pb.Inode{Ino: proto.Uint64(childInode.ID)}); err != nil {
			return nil, fmt.Errorf("failed to log inode deletion: %w", err)
		}
		if parentInode != nil {
			parentInodeMsg := &pb.Inode{
				Ino:            proto.Uint64(parentInode.ID),
				Mode:           parentInode.Mode,
				Size:           parentInode.Size,
				Mtime:          timestamppb.New(parentInode.ModTime),
				IsDir:          true,
				Sha256:         parentInode.Sha256,
				Etag:           parentInode.ETag,
				ManifestSha256: parentInode.ManifestSha256,
				ContentSha256:  parentInode.ContentSha256,
				ChunkSize:      parentInode.ChunkSize,
			}
			if _, err := tx.Update(ctx, parentInodeMsg); err != nil {
				return nil, fmt.Errorf("failed to log parent inode update: %w", err)
			}
		}
		commitSeq, err = tx.Commit(ctx)
		if err != nil {
			return nil, fmt.Errorf("failed to commit transaction: %w", err)
		}

		waitFn := v.makeWaitFn(commitSeq, nil)

		v.broadcaster.Broadcast(v.volumeID, &pb.WatchVolumeResponse{
			EventType:   pb.WatchEventType_EVENT_DELETED,
			Inode:       childInode.ID,
			ParentInode: parentInodeID,
			Name:        name,
		})

		v.checkAutoSnapshotTriggerLocked(ctx)
		return waitFn, nil
	}()
	if err != nil {
		return err
	}
	if waitFn != nil {
		if err := waitFn(ctx); err != nil {
			return err
		}
	}
	return nil
}

func (v *Volume) Rmdir(ctx context.Context, parentInodeID uint64, name string) error {
	waitFn, err := func() (func(context.Context) error, error) {
		v.mu.Lock()
		defer v.mu.Unlock()

		if parentInodeID == 0 {
			parentInodeID = v.rootInodeID
		}

		parentDir, err := v.getOrLoadDirLocked(ctx, parentInodeID)
		if err != nil {
			return nil, err
		}

		entry, ok := parentDir.Entries[name]
		if !ok {
			return nil, fmt.Errorf("directory %s not found under inode %d: %w", name, parentInodeID, syscall.ENOENT)
		}

		childInode, err := v.getOrLoadInodeLocked(ctx, entry.InodeID)
		if err != nil {
			return nil, err
		}
		if !childInode.IsDir {
			return nil, fmt.Errorf("cannot rmdir non-directory %s: %w", name, syscall.ENOTDIR)
		}

		childDir, err := v.getOrLoadDirLocked(ctx, entry.InodeID)
		if err != nil {
			return nil, err
		}
		if len(childDir.Entries) > 0 {
			return nil, fmt.Errorf("directory %s not empty: %w", name, syscall.ENOTEMPTY)
		}

		delete(parentDir.Entries, name)
		delete(parentDir.Added, name)
		parentDir.Deleted[name] = true
		parentDir.IsDirty = true
		delete(v.dirParents, childInode.ID)

		parentInode, _ := v.getOrLoadInodeLocked(ctx, parentInodeID)
		if parentInode != nil {
			parentInode.ModTime = time.Now()
			parentInode.IsDirty = true
		}

		var commitSeq uint64
		tx := v.metadataStream.Begin()
		if _, err := tx.Delete(ctx, &pb.DirEntry{ParentIno: proto.Uint64(parentInodeID), Name: proto.String(name)}); err != nil {
			return nil, fmt.Errorf("failed to log dir entry deletion: %w", err)
		}
		if _, err := tx.Delete(ctx, &pb.Inode{Ino: proto.Uint64(childInode.ID)}); err != nil {
			return nil, fmt.Errorf("failed to log inode deletion: %w", err)
		}
		if parentInode != nil {
			parentInodeMsg := &pb.Inode{
				Ino:            proto.Uint64(parentInode.ID),
				Mode:           parentInode.Mode,
				Size:           parentInode.Size,
				Mtime:          timestamppb.New(parentInode.ModTime),
				IsDir:          true,
				Sha256:         parentInode.Sha256,
				Etag:           parentInode.ETag,
				ManifestSha256: parentInode.ManifestSha256,
				ContentSha256:  parentInode.ContentSha256,
				ChunkSize:      parentInode.ChunkSize,
			}
			if _, err := tx.Update(ctx, parentInodeMsg); err != nil {
				return nil, fmt.Errorf("failed to log parent inode update: %w", err)
			}
		}
		commitSeq, err = tx.Commit(ctx)
		if err != nil {
			return nil, fmt.Errorf("failed to commit transaction: %w", err)
		}

		waitFn := v.makeWaitFn(commitSeq, nil)

		v.broadcaster.Broadcast(v.volumeID, &pb.WatchVolumeResponse{
			EventType:   pb.WatchEventType_EVENT_DELETED,
			Inode:       childInode.ID,
			ParentInode: parentInodeID,
			Name:        name,
		})

		v.checkAutoSnapshotTriggerLocked(ctx)
		return waitFn, nil
	}()
	if err != nil {
		return err
	}
	if waitFn != nil {
		if err := waitFn(ctx); err != nil {
			return err
		}
	}
	return nil
}

func (v *Volume) Rename(ctx context.Context, oldParentInodeID uint64, oldName string, newParentInodeID uint64, newName string) (*pb.EntryAttr, error) {
	attr, waitFn, err := func() (*pb.EntryAttr, func(context.Context) error, error) {
		v.mu.Lock()
		defer v.mu.Unlock()

		if oldParentInodeID == 0 {
			oldParentInodeID = v.rootInodeID
		}
		if newParentInodeID == 0 {
			newParentInodeID = v.rootInodeID
		}

		if oldName == "" || oldName == "." || oldName == ".." || newName == "" || newName == "." || newName == ".." {
			return nil, nil, fmt.Errorf("invalid name for rename: %w", syscall.EINVAL)
		}

		oldParentDir, err := v.getOrLoadDirLocked(ctx, oldParentInodeID)
		if err != nil {
			return nil, nil, fmt.Errorf("old parent directory not loaded: %w", err)
		}

		entry, ok := oldParentDir.Entries[oldName]
		if !ok {
			return nil, nil, fmt.Errorf("source %s not found in parent %d: %w", oldName, oldParentInodeID, syscall.ENOENT)
		}

		newParentInode, err := v.getOrLoadInodeLocked(ctx, newParentInodeID)
		if err != nil {
			return nil, nil, err
		}
		if !newParentInode.IsDir {
			return nil, nil, fmt.Errorf("target parent %d is not a directory: %w", newParentInodeID, syscall.ENOTDIR)
		}

		newParentDir, err := v.getOrLoadDirLocked(ctx, newParentInodeID)
		if err != nil {
			return nil, nil, err
		}

		childInode, err := v.getOrLoadInodeLocked(ctx, entry.InodeID)
		if err != nil {
			return nil, nil, err
		}

		if childInode.IsDir && newParentInodeID != oldParentInodeID {
			currCheck := newParentInodeID
			for currCheck != 0 && currCheck != v.rootInodeID {
				if currCheck == childInode.ID {
					return nil, nil, fmt.Errorf("cannot move directory into its own subdirectory: %w", syscall.EINVAL)
				}
				pIno, ok := v.dirParents[currCheck]
				if !ok || pIno == currCheck {
					break
				}
				currCheck = pIno
			}
			if v.dirParents == nil {
				v.dirParents = make(map[uint64]uint64)
			}
			v.dirParents[childInode.ID] = newParentInodeID
		}

		targetEntry, targetExists := newParentDir.Entries[newName]
		if targetExists {
			targetInode, _ := v.getOrLoadInodeLocked(ctx, targetEntry.InodeID)
			if targetInode != nil && targetInode.Data != nil {
				_ = targetInode.Data.Close()
			}
		}

		// Move entry
		delete(oldParentDir.Entries, oldName)
		delete(oldParentDir.Added, oldName)
		oldParentDir.Deleted[oldName] = true
		oldParentDir.IsDirty = true

		entry.Name = newName
		newParentDir.Entries[newName] = entry
		newParentDir.Added[newName] = true
		delete(newParentDir.Deleted, newName)
		newParentDir.IsDirty = true

		now := time.Now()
		childInode.ModTime = now
		childInode.IsDirty = true

		oldParentInode, _ := v.getOrLoadInodeLocked(ctx, oldParentInodeID)
		if oldParentInode != nil {
			oldParentInode.ModTime = now
			oldParentInode.IsDirty = true
		}
		newParentInode.ModTime = now
		newParentInode.IsDirty = true

		var commitSeq uint64
		tx := v.metadataStream.Begin()
		if _, err := tx.Delete(ctx, &pb.DirEntry{ParentIno: proto.Uint64(oldParentInodeID), Name: proto.String(oldName)}); err != nil {
			return nil, nil, fmt.Errorf("failed to log old dir entry deletion: %w", err)
		}
		if targetExists {
			if _, err := tx.Delete(ctx, &pb.DirEntry{ParentIno: proto.Uint64(newParentInodeID), Name: proto.String(newName)}); err != nil {
				return nil, nil, fmt.Errorf("failed to log target dir entry deletion: %w", err)
			}
			if _, err := tx.Delete(ctx, &pb.Inode{Ino: proto.Uint64(targetEntry.InodeID)}); err != nil {
				return nil, nil, fmt.Errorf("failed to log target inode deletion: %w", err)
			}
		}
		if _, err := tx.Insert(ctx, &pb.DirEntry{
			ParentIno: proto.Uint64(newParentInodeID),
			Name:      proto.String(newName),
			Ino:       entry.InodeID,
			IsDir:     entry.IsDir,
			Mode:      entry.Mode,
		}); err != nil {
			return nil, nil, fmt.Errorf("failed to log new dir entry insertion: %w", err)
		}
		if childInode != nil {
			childInodeMsg := &pb.Inode{
				Ino:            proto.Uint64(childInode.ID),
				Mode:           childInode.Mode,
				Size:           childInode.Size,
				Mtime:          timestamppb.New(now),
				IsDir:          childInode.IsDir,
				Sha256:         childInode.Sha256,
				Etag:           childInode.ETag,
				ManifestSha256: childInode.ManifestSha256,
				ContentSha256:  childInode.ContentSha256,
				ChunkSize:      childInode.ChunkSize,
			}
			if _, err := tx.Update(ctx, childInodeMsg); err != nil {
				return nil, nil, fmt.Errorf("failed to log child inode update: %w", err)
			}
		}
		if oldParentInode != nil {
			oldParentInodeMsg := &pb.Inode{
				Ino:            proto.Uint64(oldParentInode.ID),
				Mode:           oldParentInode.Mode,
				Size:           oldParentInode.Size,
				Mtime:          timestamppb.New(now),
				IsDir:          true,
				Sha256:         oldParentInode.Sha256,
				Etag:           oldParentInode.ETag,
				ManifestSha256: oldParentInode.ManifestSha256,
				ContentSha256:  oldParentInode.ContentSha256,
				ChunkSize:      oldParentInode.ChunkSize,
			}
			if _, err := tx.Update(ctx, oldParentInodeMsg); err != nil {
				return nil, nil, fmt.Errorf("failed to log old parent inode update: %w", err)
			}
		}
		if newParentInode != nil && newParentInodeID != oldParentInodeID {
			newParentInodeMsg := &pb.Inode{
				Ino:            proto.Uint64(newParentInode.ID),
				Mode:           newParentInode.Mode,
				Size:           newParentInode.Size,
				Mtime:          timestamppb.New(now),
				IsDir:          true,
				Sha256:         newParentInode.Sha256,
				Etag:           newParentInode.ETag,
				ManifestSha256: newParentInode.ManifestSha256,
				ContentSha256:  newParentInode.ContentSha256,
				ChunkSize:      newParentInode.ChunkSize,
			}
			if _, err := tx.Update(ctx, newParentInodeMsg); err != nil {
				return nil, nil, fmt.Errorf("failed to log new parent inode update: %w", err)
			}
		}
		commitSeq, err = tx.Commit(ctx)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to commit rename transaction: %w", err)
		}

		waitFn := v.makeWaitFn(commitSeq, nil)

		attr := &pb.EntryAttr{
			Inode:   entry.InodeID,
			Name:    newName,
			IsDir:   entry.IsDir,
			Size:    childInode.Size,
			Mode:    childInode.Mode,
			ModTime: timestamppb.New(now),
			Sha256:  childInode.Sha256,
			Uid:     childInode.Uid,
			Gid:     childInode.Gid,
		}
		v.broadcaster.Broadcast(v.volumeID, &pb.WatchVolumeResponse{
			EventType:      pb.WatchEventType_EVENT_RENAMED,
			Attr:           attr,
			Inode:          entry.InodeID,
			OldParentInode: oldParentInodeID,
			OldName:        oldName,
			ParentInode:    newParentInodeID,
			Name:           newName,
		})

		v.checkAutoSnapshotTriggerLocked(ctx)
		return attr, waitFn, nil
	}()
	if err != nil {
		return nil, err
	}
	if waitFn != nil {
		if err := waitFn(ctx); err != nil {
			return nil, err
		}
	}
	return attr, nil
}

func (v *Volume) Fsync(ctx context.Context, inodeID uint64) error {
	v.mu.RLock()
	if inodeID == 0 {
		inodeID = v.rootInodeID
	}
	_, err := v.getOrLoadInodeLocked(ctx, inodeID)
	v.mu.RUnlock()
	if err != nil {
		return err
	}
	if v.stream != nil {
		if err := v.stream.Flush(ctx); err != nil {
			return err
		}
	}
	return nil
}

func (v *Volume) ResolvePath(ctx context.Context, p string) (uint64, error) {
	v.mu.RLock()
	defer v.mu.RUnlock()
	inodeID, _, _, err := v.resolvePathLocked(ctx, p)
	return inodeID, err
}

type bufferWriterAt struct {
	buf []byte
}

func (b *bufferWriterAt) WriteAt(p []byte, off int64) (int, error) {
	end := off + int64(len(p))
	if end > int64(len(b.buf)) {
		newBuf := make([]byte, end)
		copy(newBuf, b.buf)
		b.buf = newBuf
	}
	copy(b.buf[off:end], p)
	return len(p), nil
}

type snapshotResolver struct {
	metadataResolver
	vol *Volume
}

func (r *snapshotResolver) getOrLoadInode(ctx context.Context, inodeID uint64) (*CachedInode, error) {
	return r.resolveInode(ctx, inodeID, false)
}

func (r *snapshotResolver) getOrLoadDir(ctx context.Context, inodeID uint64) (map[string]DirEntry, error) {
	dir, err := r.resolveDir(ctx, inodeID, false)
	if err != nil {
		return nil, err
	}
	return dir.Entries, nil
}

func (r *snapshotResolver) buildErofsTree(ctx context.Context, dirInodeID uint64, dirName, currentPath string, dirtyBlobs map[string]blob.ByteStream, currentEntries map[string]FileMetadata) (erofs.Node, error) {
	entries, err := r.getOrLoadDir(ctx, dirInodeID)
	if err != nil {
		return nil, err
	}
	dirInode, err := r.getOrLoadInode(ctx, dirInodeID)
	if err != nil {
		return nil, err
	}

	currentEntries[currentPath] = FileMetadata{
		Inode:   dirInode.ID,
		Path:    currentPath,
		Name:    dirName,
		IsDir:   true,
		Mode:    dirInode.Mode,
		Size:    dirInode.Size,
		ModTime: dirInode.ModTime,
		Uid:     dirInode.Uid,
		Gid:     dirInode.Gid,
	}

	var childNames []string
	for name := range entries {
		childNames = append(childNames, name)
	}
	sort.Strings(childNames)

	var children []erofs.Node
	for _, name := range childNames {
		entry := entries[name]
		childPath := path.Join(currentPath, name)
		if entry.IsDir {
			childDirNode, err := r.buildErofsTree(ctx, entry.InodeID, name, childPath, dirtyBlobs, currentEntries)
			if err != nil {
				return nil, err
			}
			children = append(children, childDirNode)
		} else {
			childInode, err := r.getOrLoadInode(ctx, entry.InodeID)
			if err != nil {
				return nil, err
			}

			if childInode.ContentSha256 == "" && childInode.Size > 0 {
				hasher := sha256.New()
				if childInode.ManifestSha256 != "" || childInode.ChunkSize > 0 {
					_ = r.vol.ensureInodeChunksLoadedLocked(ctx, childInode)
					for i := 0; i < len(childInode.Chunks); i++ {
						chunkBytes, err := r.vol.readChunkLocked(ctx, childInode, i)
						if err == nil {
							hasher.Write(chunkBytes)
						}
					}
				} else if childInode.Data != nil {
					_ = childInode.Data.Rewind()
					_, _ = io.Copy(hasher, childInode.Data)
				} else if childInode.Sha256 != "" && r.vol.blobStore != nil {
					stream, err := r.vol.blobStore.GetBlob(ctx, childInode.Sha256)
					if err == nil {
						_, _ = io.Copy(hasher, stream)
						_ = stream.Close()
					}
				}
				childInode.ContentSha256 = fmt.Sprintf("%x", hasher.Sum(nil))
				if inMemNode, ok := r.vol.inodeCache.Peek(childInode.ID); ok {
					inMemNode.ContentSha256 = childInode.ContentSha256
				}
			}

			meta := FileMetadata{
				Inode:          childInode.ID,
				Path:           childPath,
				Name:           name,
				IsDir:          false,
				Mode:           childInode.Mode,
				Size:           childInode.Size,
				ModTime:        childInode.ModTime,
				Sha256:         childInode.Sha256,
				ManifestSha256: childInode.ManifestSha256,
				ContentSha256:  childInode.ContentSha256,
				ETag:           childInode.ETag,
				Uid:            childInode.Uid,
				Gid:            childInode.Gid,
			}

			needsUpload := childInode.ETag == ""
			if r.vol.lastFlushedMetadata != nil {
				if lastEntry, exists := r.vol.lastFlushedMetadata.Entries[childPath]; !exists || lastEntry.Sha256 != childInode.Sha256 {
					needsUpload = true
				}
			}
			fetchSha := childInode.Sha256
			if childInode.ManifestSha256 != "" {
				fetchSha = childInode.ManifestSha256
			}
			if needsUpload && fetchSha != "" && r.vol.blobStore != nil && r.vol.backend != nil {
				key := strings.TrimPrefix(childPath, "/")
				blobReader, bErr := r.vol.blobStore.GetBlob(ctx, fetchSha)
				if bErr == nil && blobReader != nil {
					etag, err := r.vol.backend.PutObject(ctx, r.vol.volumeID, key, blobReader)
					if err == nil {
						childInode.ETag = etag
						meta.ETag = etag
					}
					_ = blobReader.Close()
				}
			}

			currentEntries[childPath] = meta

			var xattrs erofs.Xattrs
			if childInode.ContentSha256 != "" {
				xattrs.UserDigest = childInode.ContentSha256
			} else if childInode.Sha256 != "" {
				xattrs.UserDigest = childInode.Sha256
			}
			if childInode.ManifestSha256 != "" {
				xattrs.UserManifest = childInode.ManifestSha256
			}

			leafNode := erofs.NewMemoryNode(
				name,
				false,
				uint16(childInode.Mode),
				nil,
				nil,
				erofs.WithIno(childInode.ID),
				erofs.WithMetadataOnly(true),
				erofs.WithSize(uint64(childInode.Size)),
				erofs.WithMtime(uint64(childInode.ModTime.Unix())),
				erofs.WithUID(childInode.Uid),
				erofs.WithGID(childInode.Gid),
				erofs.WithXattrs(xattrs),
			)
			children = append(children, leafNode)
		}
	}

	dirNodeName := dirName
	if dirNodeName == "/" {
		dirNodeName = ""
	}
	return erofs.NewMemoryNode(
		dirNodeName,
		true,
		uint16(dirInode.Mode),
		nil,
		children,
		erofs.WithIno(dirInode.ID),
		erofs.WithMtime(uint64(dirInode.ModTime.Unix())),
		erofs.WithUID(dirInode.Uid),
		erofs.WithGID(dirInode.Gid),
	), nil
}

func (v *Volume) flushToBackendLocked(ctx context.Context) error {
	if v.backend == nil {
		return nil
	}

	// 1. Phase 1: Persist all in-memory dirty cache entries to local storage / blob store.
	v.inodeCache.ForEach(func(id uint64, node *CachedInode) {
		if node.IsDirty {
			_ = v.persistInode(ctx, node)
		}
	})
	v.dirCache.ForEach(func(id uint64, dir *CachedDir) {
		if dir.IsDirty {
			v.onEvictDir(id, dir)
		}
	})

	// 2. Phase 2: Capture point-in-time metadata pointers.
	var cutoffOffset LocalOffset
	if v.localStore != nil {
		cutoffOffset = v.localStore.CurrentOffset()
	}

	snapDirtyInodes := make(map[uint64]LocalOffset, len(v.dirtyInodes))
	for k, off := range v.dirtyInodes {
		snapDirtyInodes[k] = off
	}

	snapDirtyDirs := make(map[uint64]LocalOffset, len(v.dirtyDirs))
	for k, off := range v.dirtyDirs {
		snapDirtyDirs[k] = off
	}

	baseReader := v.snapshotReader
	baseRaw := v.snapshotRaw
	rootID := v.rootInodeID
	delPaths := make([]string, len(v.deletedPathsSinceFlush))
	copy(delPaths, v.deletedPathsSinceFlush)
	v.deletedPathsSinceFlush = nil

	// Release v.mu during snapshot compilation and cloud storage upload to avoid stopping the world
	v.mu.Unlock()

	var erofsBuf bufferWriterAt
	var currentEntries map[string]FileMetadata

	snapErr := func() error {
		for _, delPath := range delPaths {
			key := strings.TrimPrefix(delPath, "/")
			_ = v.backend.DeleteObject(ctx, v.volumeID, key)
		}

		currentEntries = make(map[string]FileMetadata)
		dirtyBlobs := make(map[string]blob.ByteStream)

		resolver := &snapshotResolver{
			vol: v,
			metadataResolver: metadataResolver{
				localStore:     v.localStore,
				snapshotReader: baseReader,
				snapshotRaw:    baseRaw,
				dirtyInodes:    snapDirtyInodes,
				dirtyDirs:      snapDirtyDirs,
			},
		}

		erofsTree, err := resolver.buildErofsTree(ctx, rootID, "/", "/", dirtyBlobs, currentEntries)
		if err != nil {
			return fmt.Errorf("failed to build hierarchy for snapshot: %w", err)
		}

		if v.lastFlushedMetadata != nil {
			for oldPath := range v.lastFlushedMetadata.Entries {
				if _, stillExists := currentEntries[oldPath]; !stillExists {
					key := strings.TrimPrefix(oldPath, "/")
					_ = v.backend.DeleteObject(ctx, v.volumeID, key)
				}
			}
		}

		if len(dirtyBlobs) > 0 && v.blobStore != nil {
			if err := v.blobStore.PutBlobs(ctx, dirtyBlobs); err != nil {
				return fmt.Errorf("failed to persist blobs: %w", err)
			}
		}

		if err := erofs.WriteImage(&erofsBuf, erofsTree); err != nil {
			return fmt.Errorf("failed to compile EROFS snapshot: %w", err)
		}

		readerAt := bytes.NewReader(erofsBuf.buf)
		if err := erofs.Fsck(readerAt); err != nil {
			return fmt.Errorf("Fsck failed on generated EROFS snapshot: %w", err)
		}

		timestamp := time.Now().UTC().Format("20060102T150405.000000Z")
		snapshotName := fmt.Sprintf("%s.erofs", timestamp)
		snapshotKey := path.Join("volumes", v.volumeID, "meta", snapshotName)
		erofsStream := blob.NewByteStreamFromBytes(erofsBuf.buf)
		if _, err := v.backend.PutObject(ctx, "", snapshotKey, erofsStream); err != nil {
			_ = erofsStream.Close()
			return fmt.Errorf("failed to save EROFS snapshot %s: %w", snapshotKey, err)
		}
		_ = erofsStream.Close()

		latestKey := path.Join("volumes", v.volumeID, "meta", "latest")
		latestStream := blob.NewByteStreamFromBytes([]byte(snapshotName))
		if _, err := v.backend.PutObject(ctx, "", latestKey, latestStream); err != nil {
			_ = latestStream.Close()
			return fmt.Errorf("failed to update latest snapshot: %w", err)
		}
		_ = latestStream.Close()

		return nil
	}()

	// Re-acquire v.mu.Lock() for Phase 4
	v.mu.Lock()

	if snapErr != nil {
		return snapErr
	}

	// 4. Phase 4: Update base snapshot reader, prune committed dirty maps, and trim circular buffer
	v.snapshotRaw = bytes.NewReader(erofsBuf.buf)
	if r, err := erofs.NewReader(v.snapshotRaw); err == nil {
		v.snapshotReader = r
		v.rootInodeID = r.GetRootNID()
	}
	v.snapshotCutoff = cutoffOffset

	for inodeID, snapOff := range snapDirtyInodes {
		if currOff, ok := v.dirtyInodes[inodeID]; ok && currOff == snapOff {
			delete(v.dirtyInodes, inodeID)
		}
	}

	for dirID, snapOff := range snapDirtyDirs {
		if currOff, ok := v.dirtyDirs[dirID]; ok && currOff == snapOff {
			delete(v.dirtyDirs, dirID)
		}
	}

	if v.localStore != nil && cutoffOffset != NoOffset {
		_ = v.localStore.TrimBefore(cutoffOffset)
	}

	newMeta := VolumeMetadata{
		VolumeID:    v.volumeID,
		Version:     2,
		LastFlushed: time.Now(),
		NextInode:   v.nextInode,
		Entries:     currentEntries,
	}

	metaBytes, err := json.MarshalIndent(newMeta, "", "  ")
	if err == nil {
		metaStream := blob.NewByteStreamFromBytes(metaBytes)
		_, _ = v.backend.PutObject(ctx, v.volumeID, MetadataFileName, metaStream)
		_ = metaStream.Close()
	}
	v.lastFlushedMetadata = &newMeta

	return nil
}

func (v *Volume) checkAutoSnapshotTriggerLocked(ctx context.Context) {
	if v.backend == nil {
		return
	}
	shouldSnapshot := false
	if v.maxDirtyRecords > 0 && (len(v.dirtyInodes)+len(v.dirtyDirs)) >= v.maxDirtyRecords {
		shouldSnapshot = true
	}
	if v.localStore != nil {
		if v.maxLocalFileSize > 0 && v.localStore.ActiveFileSize() >= v.maxLocalFileSize {
			shouldSnapshot = true
		}
		if v.maxBufferFiles > 0 && v.localStore.FileCount() >= v.maxBufferFiles {
			shouldSnapshot = true
		}
	}
	if shouldSnapshot {
		if !v.snapshotMu.TryLock() {
			return
		}
		defer v.snapshotMu.Unlock()
		_ = v.flushToBackendLocked(ctx)
	}
}

func (v *Volume) FlushToBackend(ctx context.Context) error {
	v.snapshotMu.Lock()
	defer v.snapshotMu.Unlock()

	v.mu.Lock()
	defer v.mu.Unlock()
	return v.flushToBackendLocked(ctx)
}

func (v *Volume) findLatestSnapshotNameLocked(ctx context.Context) (string, error) {
	latestKey := path.Join("volumes", v.volumeID, "meta", "latest")
	var latestBuf bytes.Buffer
	err := v.backend.GetObject(ctx, "", latestKey, 0, 0, &latestBuf)
	if err == nil && latestBuf.Len() > 0 {
		return strings.TrimSpace(latestBuf.String()), nil
	}

	metaPrefix := path.Join("volumes", v.volumeID, "meta") + "/"
	objects, err := v.backend.ListObjects(ctx, "", metaPrefix)
	if err != nil {
		return "", err
	}

	var snapshots []string
	for _, obj := range objects {
		if strings.HasSuffix(obj, ".erofs") {
			base := path.Base(obj)
			snapshots = append(snapshots, base)
		}
	}
	if len(snapshots) == 0 {
		return "", fmt.Errorf("no snapshots found for volume %s", v.volumeID)
	}
	sort.Strings(snapshots)
	return snapshots[len(snapshots)-1], nil
}

// ApplyRecordLocked applies a single mutation record to the filesystem tables.
func (v *Volume) ApplyRecordLocked(record *MutationRecord) error {
	ctx := context.Background()
	if record.Inode >= v.nextInode {
		v.nextInode = ((record.Inode / erofs.DefaultInodeStride) + 1) * erofs.DefaultInodeStride
	}

	switch record.Type {
	case MutationMkdir:
		parentInodeID := record.ParentInode
		if parentInodeID == 0 {
			parentInodeID = v.rootInodeID
		}
		baseName := record.Name
		if baseName == "" {
			return nil
		}

		parentDir, err := v.getOrLoadDirLocked(ctx, parentInodeID)
		if err != nil {
			return err
		}

		mode := record.Mode
		if mode == 0 {
			mode = 0755
		}
		mode |= syscall.S_IFDIR

		inodeID := record.Inode
		if inodeID == 0 {
			inodeID = v.allocInode()
		}

		var modTime time.Time
		if record.ModTime != nil {
			modTime = record.ModTime.AsTime()
		}
		if modTime.IsZero() {
			modTime = time.Now()
		}

		childInode := &CachedInode{
			ID:      inodeID,
			Mode:    mode,
			ModTime: modTime,
			IsDir:   true,
			Uid:     record.Uid,
			Gid:     record.Gid,
			IsDirty: false,
		}
		v.inodeCache.Put(inodeID, childInode)

		childDir := &CachedDir{
			ID:         inodeID,
			Entries:    make(map[string]DirEntry),
			Added:      make(map[string]bool),
			Deleted:    make(map[string]bool),
			PrevOffset: NoOffset,
			IsDirty:    false,
		}
		v.dirCache.Put(inodeID, childDir)

		newEntry := DirEntry{
			Name:    baseName,
			InodeID: inodeID,
			IsDir:   true,
			Mode:    mode,
		}
		parentDir.Entries[baseName] = newEntry
		if v.dirParents == nil {
			v.dirParents = make(map[uint64]uint64)
		}
		v.dirParents[inodeID] = parentInodeID
		return nil

	case MutationCreateFile:
		parentInodeID := record.ParentInode
		if parentInodeID == 0 {
			parentInodeID = v.rootInodeID
		}
		baseName := record.Name
		if baseName == "" {
			return nil
		}

		parentDir, err := v.getOrLoadDirLocked(ctx, parentInodeID)
		if err != nil {
			return err
		}

		mode := record.Mode
		if mode == 0 {
			mode = 0644
		}
		mode |= syscall.S_IFREG

		inodeID := record.Inode
		if inodeID == 0 {
			inodeID = v.allocInode()
		}

		var modTime time.Time
		if record.ModTime != nil {
			modTime = record.ModTime.AsTime()
		}
		if modTime.IsZero() {
			modTime = time.Now()
		}

		var stream blob.ByteStream
		if len(record.Data) > 0 {
			stream = blob.NewByteStreamFromBytes(record.Data)
		}

		childInode := &CachedInode{
			ID:             inodeID,
			Mode:           mode,
			Size:           record.Size,
			ModTime:        modTime,
			Data:           stream,
			Sha256:         record.Sha256,
			ManifestSha256: record.ManifestSha256,
			ContentSha256:  record.ContentSha256,
			IsDir:          false,
			Uid:            record.Uid,
			Gid:            record.Gid,
			IsDirty:        false,
		}
		v.inodeCache.Put(inodeID, childInode)

		newEntry := DirEntry{
			Name:    baseName,
			InodeID: inodeID,
			IsDir:   false,
			Mode:    mode,
		}
		parentDir.Entries[baseName] = newEntry
		return nil

	case MutationWriteFile:
		inodeID := record.Inode
		if inodeID == 0 {
			return nil
		}
		node, err := v.getOrLoadInodeLocked(ctx, inodeID)
		if err != nil {
			return err
		}

		if len(record.Data) > 0 {
			var currentData []byte
			if node.Data != nil {
				_ = node.Data.Rewind()
				currentData, _ = io.ReadAll(node.Data)
				_ = node.Data.Close()
			}
			neededLen := record.Offset + int64(len(record.Data))
			if neededLen > int64(len(currentData)) {
				newBuf := make([]byte, neededLen)
				copy(newBuf, currentData)
				currentData = newBuf
			}
			copy(currentData[record.Offset:], record.Data)
			if node.Size < int64(len(currentData)) {
				node.Size = int64(len(currentData))
			}
			node.Data = blob.NewByteStreamFromBytes(currentData)
		}
		if record.Size > 0 {
			node.Size = record.Size
		}
		if record.Sha256 != "" {
			node.Sha256 = record.Sha256
		}
		if record.ManifestSha256 != "" {
			node.ManifestSha256 = record.ManifestSha256
		}
		if record.ContentSha256 != "" {
			node.ContentSha256 = record.ContentSha256
		}
		if record.ModTime != nil {
			node.ModTime = record.ModTime.AsTime()
		}
		return nil

	case MutationTruncateFile:
		inodeID := record.Inode
		if inodeID == 0 {
			return nil
		}
		node, err := v.getOrLoadInodeLocked(ctx, inodeID)
		if err != nil {
			return err
		}
		node.Size = record.Size
		if record.Sha256 != "" {
			node.Sha256 = record.Sha256
		}
		if record.ManifestSha256 != "" {
			node.ManifestSha256 = record.ManifestSha256
		}
		if record.ContentSha256 != "" {
			node.ContentSha256 = record.ContentSha256
		}
		if record.ModTime != nil {
			node.ModTime = record.ModTime.AsTime()
		}
		if node.Data != nil {
			_ = node.Data.Rewind()
			currentData, _ := io.ReadAll(node.Data)
			_ = node.Data.Close()
			if record.Size < int64(len(currentData)) {
				currentData = currentData[:record.Size]
			} else if record.Size > int64(len(currentData)) {
				newBuf := make([]byte, record.Size)
				copy(newBuf, currentData)
				currentData = newBuf
			}
			node.Data = blob.NewByteStreamFromBytes(currentData)
		}
		return nil

	case MutationUnlink:
		parentInodeID := record.ParentInode
		if parentInodeID == 0 {
			parentInodeID = v.rootInodeID
		}
		baseName := record.Name
		parentDir, err := v.getOrLoadDirLocked(ctx, parentInodeID)
		if err != nil {
			return nil
		}
		delete(parentDir.Entries, baseName)
		return nil

	case MutationRmdir:
		parentInodeID := record.ParentInode
		if parentInodeID == 0 {
			parentInodeID = v.rootInodeID
		}
		baseName := record.Name
		parentDir, err := v.getOrLoadDirLocked(ctx, parentInodeID)
		if err != nil {
			return nil
		}
		if entry, ok := parentDir.Entries[baseName]; ok {
			delete(v.dirParents, entry.InodeID)
		}
		delete(parentDir.Entries, baseName)
		return nil

	case MutationRename:
		oldParentInodeID := record.OldParentInode
		if oldParentInodeID == 0 {
			oldParentInodeID = v.rootInodeID
		}
		oldBase := record.OldName
		newParentInodeID := record.ParentInode
		if newParentInodeID == 0 {
			newParentInodeID = v.rootInodeID
		}
		newBase := record.Name

		oldParentDir, err := v.getOrLoadDirLocked(ctx, oldParentInodeID)
		if err != nil {
			return err
		}

		entry, ok := oldParentDir.Entries[oldBase]
		if !ok {
			return fmt.Errorf("rename source not found: %s in inode %d", oldBase, oldParentInodeID)
		}

		newParentDir, err := v.getOrLoadDirLocked(ctx, newParentInodeID)
		if err != nil {
			return err
		}

		delete(oldParentDir.Entries, oldBase)
		entry.Name = newBase
		newParentDir.Entries[newBase] = entry
		if entry.IsDir {
			if v.dirParents == nil {
				v.dirParents = make(map[uint64]uint64)
			}
			v.dirParents[entry.InodeID] = newParentInodeID
		}

		if record.ModTime != nil {
			childInode, _ := v.getOrLoadInodeLocked(ctx, entry.InodeID)
			if childInode != nil {
				childInode.ModTime = record.ModTime.AsTime()
			}
		}
		return nil

	default:
		return fmt.Errorf("unknown mutation type: %s", record.Type)
	}
}

func (v *Volume) replayRecordsLocked(records []*MutationRecord) error {
	for _, rec := range records {
		if err := v.ApplyRecordLocked(rec); err != nil {
			return fmt.Errorf("failed to apply record %v: %w", rec, err)
		}
	}
	return nil
}

func (v *Volume) replayClientRecordsLocked(records []*wal.ClientRecord) error {
	for _, cr := range records {
		mut, err := DecodeMutationRecord(cr.Payload)
		if err != nil {
			return fmt.Errorf("failed to decode mutation record at seq %d: %w", cr.StreamSeq, err)
		}
		mut.StreamSeq = cr.StreamSeq
		if err := v.ApplyRecordLocked(mut); err != nil {
			return fmt.Errorf("failed to apply mutation record at seq %d: %w", cr.StreamSeq, err)
		}
	}
	return nil
}

// ApplySDSChangeLocked applies a single committed SDS change to the in-memory filesystem caches.
func (v *Volume) ApplySDSChangeLocked(ctx context.Context, change sds.Change) error {
	// TODO: Avoid string comparisons by using RegisteredType structs with type IDs.
	msg := change.Row
	if msg == nil {
		return fmt.Errorf("change at seq %d has nil Row for type %q (op=%v)", change.Seq, change.TypeName, change.Op)
	}

	switch row := msg.(type) {
	case *pb.Content:
		switch change.Op {
		case sdsv1.OpRecord_CREATE, sdsv1.OpRecord_UPDATE:
			if v.recoveredContent == nil {
				v.recoveredContent = make(map[string][]byte)
			}
			v.recoveredContent[row.GetSha256()] = row.GetData()
		case sdsv1.OpRecord_DELETE:
			if v.recoveredContent != nil {
				delete(v.recoveredContent, row.GetSha256())
			}
		}

	case *pb.Inode:
		ino := row.GetIno()
		if ino >= v.nextInode {
			v.nextInode = ((ino / erofs.DefaultInodeStride) + 1) * erofs.DefaultInodeStride
		}
		switch change.Op {
		case sdsv1.OpRecord_CREATE, sdsv1.OpRecord_UPDATE:
			var modTime time.Time
			if row.GetMtime() != nil {
				modTime = row.GetMtime().AsTime()
			}
			if modTime.IsZero() {
				modTime = time.Now()
			}

			if row.GetIsDir() {
				node := &CachedInode{
					ID:      ino,
					Mode:    row.GetMode(),
					ModTime: modTime,
					IsDir:   true,
					IsDirty: false,
				}
				v.inodeCache.Put(ino, node)
				if _, ok := v.dirCache.Peek(ino); !ok {
					v.dirCache.Put(ino, &CachedDir{
						ID:         ino,
						Entries:    make(map[string]DirEntry),
						Added:      make(map[string]bool),
						Deleted:    make(map[string]bool),
						PrevOffset: NoOffset,
						IsDirty:    false,
					})
				}
			} else {
				node := &CachedInode{
					ID:             ino,
					Mode:           row.GetMode(),
					Size:           row.GetSize(),
					ModTime:        modTime,
					Sha256:         row.GetSha256(),
					ManifestSha256: row.GetManifestSha256(),
					ContentSha256:  row.GetContentSha256(),
					ChunkSize:      row.GetChunkSize(),
					ETag:           row.GetEtag(),
					IsDir:          false,
					IsDirty:        false,
				}

				if node.ChunkSize > 0 && node.ManifestSha256 != "" {
					if mData, ok := v.recoveredContent[node.ManifestSha256]; ok {
						manifest, err := blob.DecodeManifest(bytes.NewReader(mData))
						if err == nil {
							node.Chunks = manifest.ChunkHexSHAs()
							node.ChunkSize = manifest.ChunkSize
							if node.Size == 0 {
								node.Size = int64(manifest.TotalLength)
							}
						}
					}
				} else if node.Sha256 != "" {
					if cData, ok := v.recoveredContent[node.Sha256]; ok {
						node.Data = blob.NewByteStreamFromBytes(cData)
					}
				}
				v.inodeCache.Put(ino, node)
			}

		case sdsv1.OpRecord_DELETE:
			v.inodeCache.Remove(ino)
			v.dirCache.Remove(ino)
			delete(v.dirtyInodes, ino)
			delete(v.dirtyDirs, ino)
		}

	case *pb.DirEntry:
		parentIno := row.GetParentIno()
		name := row.GetName()
		parentDir, _ := v.getOrLoadDirLocked(ctx, parentIno)
		if parentDir == nil {
			parentDir = &CachedDir{
				ID:         parentIno,
				Entries:    make(map[string]DirEntry),
				Added:      make(map[string]bool),
				Deleted:    make(map[string]bool),
				PrevOffset: NoOffset,
				IsDirty:    false,
			}
			v.dirCache.Put(parentIno, parentDir)
		}

		if v.dirParents == nil {
			v.dirParents = make(map[uint64]uint64)
		}

		switch change.Op {
		case sdsv1.OpRecord_CREATE, sdsv1.OpRecord_UPDATE:
			parentDir.Entries[name] = DirEntry{
				Name:    name,
				InodeID: row.GetIno(),
				IsDir:   row.GetIsDir(),
				Mode:    row.GetMode(),
			}
			delete(parentDir.Deleted, name)
			if row.GetIsDir() {
				v.dirParents[row.GetIno()] = parentIno
			}
		case sdsv1.OpRecord_DELETE:
			delete(parentDir.Entries, name)
			delete(v.dirParents, row.GetIno())
		}

	default:
		return fmt.Errorf("unrecognized type %T (%s) for SDS change", msg, change.TypeName)
	}
	return nil
}

// ReplaySDSChanges applies an ordered list of committed SDS changes to the volume.
func (v *Volume) ReplaySDSChanges(changes []sds.Change) error {
	v.mu.Lock()
	defer v.mu.Unlock()
	ctx := context.Background()
	for _, c := range changes {
		if err := v.ApplySDSChangeLocked(ctx, c); err != nil {
			return err
		}
	}
	return nil
}

// ReplayRecords applies an ordered list of mutation records to the volume.
func (v *Volume) ReplayRecords(records []*MutationRecord) error {
	v.mu.Lock()
	defer v.mu.Unlock()
	return v.replayRecordsLocked(records)
}

// ReplayClientRecords decodes and applies an ordered list of WAL client records to the volume.
func (v *Volume) ReplayClientRecords(records []*wal.ClientRecord) error {
	v.mu.Lock()
	defer v.mu.Unlock()
	return v.replayClientRecordsLocked(records)
}

func (v *Volume) LoadFromBackend(ctx context.Context) error {
	v.mu.Lock()
	defer v.mu.Unlock()

	if v.backend != nil {
		var meta VolumeMetadata
		var metaBuf bytes.Buffer
		if err := v.backend.GetObject(ctx, v.volumeID, MetadataFileName, 0, 0, &metaBuf); err == nil && metaBuf.Len() > 0 {
			if err := json.Unmarshal(metaBuf.Bytes(), &meta); err == nil {
				v.lastFlushedMetadata = &meta
				if meta.NextInode > 0 {
					v.nextInode = meta.NextInode
				}
			}
		}

		latestSnapshotName, err := v.findLatestSnapshotNameLocked(ctx)
		if err == nil && latestSnapshotName != "" {
			snapshotKey := path.Join("volumes", v.volumeID, "meta", latestSnapshotName)
			var imgBuf bytes.Buffer
			err := v.backend.GetObject(ctx, "", snapshotKey, 0, 0, &imgBuf)
			if err == nil && imgBuf.Len() > 0 {
				snapBytes := imgBuf.Bytes()
				readerAt := bytes.NewReader(snapBytes)
				reader, err := erofs.NewReader(readerAt)
				if err == nil {
					v.snapshotRaw = readerAt
					v.snapshotReader = reader
					v.rootInodeID = reader.GetRootNID()
					maxNID := ((uint64(len(snapBytes))/32 + erofs.DefaultInodeStride - 1) / erofs.DefaultInodeStride) * erofs.DefaultInodeStride
					if v.nextInode < maxNID {
						v.nextInode = maxNID
					}
					if v.nextInode < erofs.DefaultInodeStride {
						v.nextInode = erofs.DefaultInodeStride
					}
					v.inodeCache.Clear()
					v.dirCache.Clear()
					v.dirtyInodes = make(map[uint64]LocalOffset)
					v.dirtyDirs = make(map[uint64]LocalOffset)
				}
			}
		}
	}

	// SDS Stream replay
	if v.stream != nil {
		recovered := v.stream.RecoveredRecords()
		if len(recovered) > 0 {
			sr := sds.NewStreamReader("", v.streamID, sds.WithRecoveredRecords(recovered))
			changes, err := sr.FeedRecovered(0)
			if err != nil {
				return fmt.Errorf("failed to recover SDS stream records: %w", err)
			}
			for _, change := range changes {
				if err := v.ApplySDSChangeLocked(ctx, change); err != nil {
					return fmt.Errorf("failed to apply recovered SDS change: %w", err)
				}
			}
			sr.ChangeReader().DiscardPending()
		}
	}

	return nil
}

// CreateSnapshot creates and returns a new EROFS snapshot of the current volume state.
func (v *Volume) CreateSnapshot(ctx context.Context) (string, error) {
	if err := v.FlushToBackend(ctx); err != nil {
		return "", err
	}
	v.mu.RLock()
	defer v.mu.RUnlock()
	return v.findLatestSnapshotNameLocked(ctx)
}

// ListSnapshots returns all EROFS snapshot filenames for this volume sorted chronologically.
func (v *Volume) ListSnapshots(ctx context.Context) ([]string, error) {
	v.mu.RLock()
	defer v.mu.RUnlock()

	metaPrefix := path.Join("volumes", v.volumeID, "meta") + "/"
	objects, err := v.backend.ListObjects(ctx, "", metaPrefix)
	if err != nil {
		return nil, err
	}

	var snapshots []string
	for _, obj := range objects {
		if strings.HasSuffix(obj, ".erofs") {
			base := path.Base(obj)
			snapshots = append(snapshots, base)
		}
	}
	sort.Strings(snapshots)
	return snapshots, nil
}

// RestoreSnapshot restores the volume filesystem state to a specific EROFS snapshot.
func (v *Volume) RestoreSnapshot(ctx context.Context, snapshotName string) error {
	v.snapshotMu.Lock()
	defer v.snapshotMu.Unlock()

	v.mu.Lock()
	defer v.mu.Unlock()

	if v.backend == nil {
		return fmt.Errorf("no backend configured")
	}

	snapshotKey := path.Join("volumes", v.volumeID, "meta", snapshotName)
	var imgBuf bytes.Buffer
	if err := v.backend.GetObject(ctx, "", snapshotKey, 0, 0, &imgBuf); err != nil {
		return fmt.Errorf("failed to fetch snapshot %s: %w", snapshotKey, err)
	}

	snapBytes := imgBuf.Bytes()
	readerAt := bytes.NewReader(snapBytes)
	reader, err := erofs.NewReader(readerAt)
	if err != nil {
		return fmt.Errorf("failed to parse snapshot %s: %w", snapshotName, err)
	}

	v.snapshotRaw = readerAt
	v.snapshotReader = reader
	v.rootInodeID = reader.GetRootNID()
	maxNID := ((uint64(len(snapBytes))/32 + erofs.DefaultInodeStride - 1) / erofs.DefaultInodeStride) * erofs.DefaultInodeStride
	if v.nextInode < maxNID {
		v.nextInode = maxNID
	}
	if v.nextInode < erofs.DefaultInodeStride {
		v.nextInode = erofs.DefaultInodeStride
	}

	v.inodeCache.Clear()
	v.dirCache.Clear()
	v.dirtyInodes = make(map[uint64]LocalOffset)
	v.dirtyDirs = make(map[uint64]LocalOffset)
	v.snapshotCutoff = NoOffset
	if v.localStore != nil {
		_ = v.localStore.DeleteAllAndReset()
	}

	return nil
}
