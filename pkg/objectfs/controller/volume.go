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
	"path/filepath"
	"sort"
	"strconv"
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
	"github.com/gke-labs/in-cluster-storage/pkg/sds/projection/sqlite"
	"github.com/gke-labs/in-cluster-storage/pkg/sds/record"
	"github.com/gke-labs/in-cluster-storage/pkg/wal"
	walclient "github.com/gke-labs/in-cluster-storage/pkg/wal/client"
	"github.com/google/uuid"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type MetadataStore string

const (
	MetadataStoreLegacy MetadataStore = "legacy"
	MetadataStoreSQLite MetadataStore = "sqlite"
)

const (
	MetadataFileName = ".objectfs-metadata.json"
	// MaxNameLength is the maximum allowed byte length for a path component name
	// (matching the POSIX NAME_MAX limit of 255 bytes advertised by statfs).
	MaxNameLength = 255
)

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
	Atime          time.Time `json:"atime,omitempty"`
	Ctime          time.Time `json:"ctime,omitempty"`
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

type InodeUpload struct {
	wg  sync.WaitGroup
	err error
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

	pendingUploadsMu sync.Mutex
	pendingUploadsWg sync.WaitGroup
	inodeUploads     map[uint64]*InodeUpload

	stream     walclient.Stream
	durability walclient.Level
	streamID   uuid.UUID

	metadataStream   *sds.Writer
	lastCommitSeq    uint64
	recoveredContent map[string][]byte

	localStorageDir string
	metadataStore   MetadataStore
	sqliteDB        *sqlite.DB

	sqliteCache         *LRUCache[SQLiteCacheKey, *SQLiteCachedRow]
	sqliteCacheDisabled bool
	sqliteOverlay       map[SQLiteCacheKey]SQLiteOverlayEntry
	unappliedBytes      int64
	maxUnappliedBytes   int64
	applierBatchSize    int
	applierFaultHook    func() error
	sqliteApplier       *sqliteApplier
	sqliteAppliedPos    uint64
	backpressureCond    *sync.Cond
	flushCond           *sync.Cond
	closed              bool
	closedCh            chan struct{}

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

// WithMetadataStore sets the metadata storage engine ("legacy" or "sqlite").
func WithMetadataStore(store string) VolumeOption {
	return func(v *Volume) {
		if strings.ToLower(store) == "sqlite" {
			v.metadataStore = MetadataStoreSQLite
		} else {
			v.metadataStore = MetadataStoreLegacy
		}
	}
}

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
		if v.sqliteCache != nil && (maxInodes > 0 || maxDirs > 0) {
			entries := maxInodes + maxDirs
			if entries <= 0 {
				entries = 65536
			}
			v.sqliteCache.capacity = entries
		}
	}
}

// WithMetadataCacheLimits sets the maximum entry count and byte capacity for the SQLite metadata read cache.
func WithMetadataCacheLimits(maxEntries int, maxBytes int64) VolumeOption {
	return func(v *Volume) {
		if v.sqliteCache != nil {
			v.sqliteCache.SetLimits(maxEntries, maxBytes)
		}
	}
}

// WithSQLiteCacheDisabled enables or disables the SQLite metadata read cache.
func WithSQLiteCacheDisabled(disabled bool) VolumeOption {
	return func(v *Volume) {
		v.sqliteCacheDisabled = disabled
	}
}

// WithMaxUnappliedBytes sets the maximum byte bound for unapplied overlay rows before applying backpressure.
func WithMaxUnappliedBytes(maxBytes int64) VolumeOption {
	return func(v *Volume) {
		v.maxUnappliedBytes = maxBytes
	}
}

// WithApplierBatchSize sets the maximum batch size for the background SQLite applier.
func WithApplierBatchSize(batchSize int) VolumeOption {
	return func(v *Volume) {
		v.applierBatchSize = batchSize
	}
}

// WithApplierFaultHook sets a fault injection callback for the background SQLite applier (testing only).
func WithApplierFaultHook(hook func() error) VolumeOption {
	return func(v *Volume) {
		v.applierFaultHook = hook
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
		volumeID:          volumeID,
		rootInodeID:       0,
		nextInode:         erofs.DefaultInodeStride,
		backend:           backend,
		blobStore:         blobStore,
		broadcaster:       broadcaster,
		maxInlineLen:      4096,      // 4KB default inline threshold for tiny files
		chunkSize:         64 * 1024, // 64KB default chunk size
		durability:        walclient.Local,
		streamID:          StreamIDForVolume(volumeID),
		maxBufferFiles:    4,
		maxDirtyRecords:   100000,
		maxLocalFileSize:  250 * 1024 * 1024,
		maxUnappliedBytes: 64 * 1024 * 1024,
		applierBatchSize:  defaultApplierBatchSize,
		sqliteOverlay:     make(map[SQLiteCacheKey]SQLiteOverlayEntry),
		closedCh:          make(chan struct{}),
		recoveredContent:  make(map[string][]byte),
		dirParents:        make(map[uint64]uint64),
		inodeUploads:      make(map[uint64]*InodeUpload),
	}
	v.backpressureCond = sync.NewCond(&v.mu)
	v.flushCond = sync.NewCond(&v.mu)

	v.inodeCache = NewLRUCache[uint64, *CachedInode](10000, v.onEvictInode)
	v.dirCache = NewLRUCache[uint64, *CachedDir](2000, v.onEvictDir)
	v.sqliteCache = NewLRUCacheWithLimits[SQLiteCacheKey, *SQLiteCachedRow](
		0,
		64*1024*1024,
		SQLiteCacheSizeFn,
		nil,
	)

	for _, opt := range opts {
		opt(v)
	}

	if err := v.initMetadataStreamLocked(); err != nil {
		panic(fmt.Sprintf("failed to init metadata stream: %v", err))
	}

	if v.localStorageDir == "" {
		v.localStorageDir = path.Join(os.TempDir(), fmt.Sprintf("objectfs-local-%s-%d", volumeID, time.Now().UnixNano()))
	}

	if v.metadataStore == MetadataStoreSQLite {
		_ = os.MkdirAll(v.localStorageDir, 0755)
		dbPath := filepath.Join(v.localStorageDir, "metadata.sqlite")
		db, err := sqlite.Open(context.Background(), dbPath,
			sqlite.WithStreamID(v.streamID.String()),
			sqlite.WithLockingMode("EXCLUSIVE"),
			sqlite.WithJournalMode("WAL"),
			sqlite.WithSynchronous("NORMAL"),
		)
		if err == nil {
			v.sqliteDB = db
			if v.metadataStream != nil {
				_ = v.sqliteDB.SyncRegistry(context.Background(), v.metadataStream.Registry())
			}
			count, _ := v.sqliteDB.Count(context.Background(), "objectfs.v1alpha1.Inode")
			if count == 0 {
				rootInodeMsg := &pb.Inode{
					Ino:   proto.Uint64(1),
					Mode:  0755 | syscall.S_IFDIR,
					Mtime: timestamppb.Now(),
					IsDir: true,
				}
				keyBytes, valBytes, _ := sds.SplitKeyAndNonKey(rootInodeMsg, []int32{1})
				initChanges := []sds.Change{
					{
						Seq:      0,
						TypeID:   16,
						TypeName: "objectfs.v1alpha1.Inode",
						Op:       sds.OpCreate,
						Key:      sds.NewKeyFromBytes(keyBytes),
						RawKey:   keyBytes,
						RawVal:   valBytes,
						Row:      rootInodeMsg,
					},
				}
				_ = v.sqliteDB.ApplyBatch(context.Background(), initChanges)
				v.applyChangesToSQLiteCacheLocked(initChanges)
			}
		}
		if v.sqliteDB != nil {
			v.sqliteAppliedPos = v.sqliteDB.Position()
		}
		v.sqliteApplier = newSQLiteApplier(v, v.applierBatchSize, v.applierFaultHook)
		v.sqliteApplier.start()
		v.rootInodeID = 1
		v.nextInode = erofs.DefaultInodeStride
	} else {
		ls, err := NewLocalStorage(v.localStorageDir, WithMaxLocalFileSize(v.maxLocalFileSize))
		if err == nil {
			v.localStore = ls
		}
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

func (v *Volume) registerPendingUpload(ino uint64) func(error) {
	v.pendingUploadsMu.Lock()
	defer v.pendingUploadsMu.Unlock()

	if v.inodeUploads == nil {
		v.inodeUploads = make(map[uint64]*InodeUpload)
	}

	upload, exists := v.inodeUploads[ino]
	if !exists {
		upload = &InodeUpload{}
		v.inodeUploads[ino] = upload
	}

	v.pendingUploadsWg.Add(1)
	upload.wg.Add(1)

	return func(err error) {
		v.pendingUploadsMu.Lock()
		defer v.pendingUploadsMu.Unlock()

		if err != nil {
			upload.err = err
		}
		upload.wg.Done()
		v.pendingUploadsWg.Done()
	}
}

func (v *Volume) waitForInodeUploads(ctx context.Context, ino uint64) error {
	v.pendingUploadsMu.Lock()
	upload := v.inodeUploads[ino]
	v.pendingUploadsMu.Unlock()

	if upload != nil {
		done := make(chan struct{})
		go func() {
			upload.wg.Wait()
			close(done)
		}()
		select {
		case <-done:
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	v.pendingUploadsMu.Lock()
	var err error
	if upload := v.inodeUploads[ino]; upload != nil {
		err = upload.err
		delete(v.inodeUploads, ino)
	}
	v.pendingUploadsMu.Unlock()

	return err
}

func (v *Volume) waitForVolumeUploads(ctx context.Context) error {
	done := make(chan struct{})
	go func() {
		v.pendingUploadsWg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-ctx.Done():
		return ctx.Err()
	}

	v.pendingUploadsMu.Lock()
	var firstErr error
	for ino, upload := range v.inodeUploads {
		if upload != nil && upload.err != nil && firstErr == nil {
			firstErr = upload.err
		}
		delete(v.inodeUploads, ino)
	}
	v.pendingUploadsMu.Unlock()

	return firstErr
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
	if _, err := w.RegisterType(&pb.FileChunk{}, 1, 2); err != nil {
		return fmt.Errorf("failed to register FileChunk type: %w", err)
	}
	if _, err := w.RegisterType(&pb.Content{}, 1); err != nil {
		return fmt.Errorf("failed to register Content type: %w", err)
	}
	v.metadataStream = w
	return nil
}

func (v *Volume) ensureInodeChunksLoadedLocked(ctx context.Context, node *CachedInode) error {
	if node.ChunkSize > 0 && len(node.Chunks) > 0 {
		return nil
	}
	if v.metadataStore == MetadataStoreSQLite && v.sqliteDB != nil {
		prefixBytes, pErr := sds.EncodeKeyPrefix(&pb.FileChunk{Ino: proto.Uint64(node.ID)}, 1)
		if pErr == nil {
			chunkMsgs, sErr := v.scanSQLiteRowsLocked(ctx, "objectfs.v1alpha1.FileChunk", prefixBytes)
			if sErr == nil {
				for _, cMsg := range chunkMsgs {
					chunk := cMsg.(*pb.FileChunk)
					if chunk.GetIndex() == 0 && len(chunk.GetInlineData()) > 0 {
						node.InlineData = chunk.GetInlineData()
					} else if chunk.GetSha256() != "" {
						if node.Chunks == nil {
							node.Chunks = make(map[uint32]string)
						}
						node.Chunks[chunk.GetIndex()] = chunk.GetSha256()
					}
				}
			}
		}
		return nil
	}
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
				node.Chunks = make(map[uint32]string, len(manifest.Chunks))
				for idx, hexSha := range manifest.ChunkHexSHAs() {
					if hexSha != "" {
						node.Chunks[uint32(idx)] = hexSha
					}
				}
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
				node.Chunks = make(map[uint32]string, len(manifest.Chunks))
				for idx, hexSha := range manifest.ChunkHexSHAs() {
					if hexSha != "" {
						node.Chunks[uint32(idx)] = hexSha
					}
				}
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
	if chunkIdx == 0 && len(node.InlineData) > 0 {
		res := make([]byte, len(node.InlineData))
		copy(res, node.InlineData)
		return res, nil
	}

	if node.StagedChunks != nil {
		if data, ok := node.StagedChunks[chunkIdx]; ok {
			res := make([]byte, len(data))
			copy(res, data)
			return res, nil
		}
	}

	if node.DirtyChunks != nil {
		if data, ok := node.DirtyChunks[chunkIdx]; ok {
			res := make([]byte, len(data))
			copy(res, data)
			return res, nil
		}
	}

	_ = v.ensureInodeChunksLoadedLocked(ctx, node)

	if chunkSha, ok := node.Chunks[uint32(chunkIdx)]; ok && chunkSha != "" {
		if v.recoveredContent != nil {
			if data, ok := v.recoveredContent[chunkSha]; ok {
				res := make([]byte, len(data))
				copy(res, data)
				return res, nil
			}
		}
		if v.blobStore != nil {
			stream, err := v.blobStore.GetBlob(ctx, chunkSha)
			if err == nil {
				defer stream.Close()
				return io.ReadAll(stream)
			}
		}
	}

	if chunkIdx == 0 && (node.Sha256 != "" || node.ContentSha256 != "") && len(node.Chunks) == 0 {
		sha := node.Sha256
		if sha == "" {
			sha = node.ContentSha256
		}
		if v.recoveredContent != nil {
			if data, ok := v.recoveredContent[sha]; ok {
				res := make([]byte, len(data))
				copy(res, data)
				return res, nil
			}
		}
		if v.blobStore != nil {
			stream, err := v.blobStore.GetBlob(ctx, sha)
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

	return nil, nil
}

// persistInode uploads the inode's blob payload if dirty and writes the inode metadata record to local eviction storage.
func (v *Volume) persistInode(ctx context.Context, node *CachedInode) error {
	if !node.IsDirty {
		return nil
	}

	if v.blobStore != nil {
		dirtyBlobs := make(map[string]blob.ByteStream)
		if len(node.InlineData) > 0 {
			sha := fmt.Sprintf("%x", sha256.Sum256(node.InlineData))
			node.Sha256 = sha
			node.ContentSha256 = sha
			dirtyBlobs[sha] = blob.NewByteStreamFromBytes(node.InlineData)
		}
		for idx, chunkBytes := range node.DirtyChunks {
			if sha, ok := node.Chunks[uint32(idx)]; ok && sha != "" {
				dirtyBlobs[sha] = blob.NewByteStreamFromBytes(chunkBytes)
			}
		}
		for idx, chunkBytes := range node.StagedChunks {
			if sha, ok := node.Chunks[uint32(idx)]; ok && sha != "" {
				dirtyBlobs[sha] = blob.NewByteStreamFromBytes(chunkBytes)
			}
		}
		if node.ManifestSha256 != "" {
			cs := int64(node.ChunkSize)
			if cs == 0 {
				cs = 64 * 1024
			}
			numChunks := int((node.Size + cs - 1) / cs)
			manifestChunks := make([][32]byte, numChunks)
			for i := 0; i < numChunks; i++ {
				if c, ok := node.Chunks[uint32(i)]; ok && c != "" {
					raw, _ := hex.DecodeString(c)
					if len(raw) == 32 {
						copy(manifestChunks[i][:], raw)
					}
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
			Atime:          node.Atime,
			Ctime:          node.Ctime,
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
	if v.metadataStore == MetadataStoreSQLite {
		return
	}
	_ = v.persistInode(context.Background(), node)
}

func (v *Volume) onEvictDir(dirID uint64, dir *CachedDir) {
	if v.metadataStore == MetadataStoreSQLite {
		return
	}
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
	v.closed = true
	if v.closedCh != nil {
		select {
		case <-v.closedCh:
		default:
			close(v.closedCh)
		}
	}
	if v.backpressureCond != nil {
		v.backpressureCond.Broadcast()
	}
	if v.flushCond != nil {
		v.flushCond.Broadcast()
	}
	applier := v.sqliteApplier
	v.mu.Unlock()

	if applier != nil {
		applier.stop()
	}

	v.mu.Lock()
	defer v.mu.Unlock()

	var firstErr error
	if v.sqliteCache != nil {
		v.sqliteCache.Clear()
	}
	if v.sqliteDB != nil {
		if err := v.sqliteDB.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	if v.localStore != nil {
		if err := v.localStore.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
		if v.metadataStore != MetadataStoreSQLite {
			_ = os.RemoveAll(v.localStorageDir)
		}
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
	if commitSeq > v.lastCommitSeq {
		v.lastCommitSeq = commitSeq
	}
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

// Position returns the latest committed stream sequence number for this volume.
func (v *Volume) Position() uint64 {
	v.mu.RLock()
	defer v.mu.RUnlock()
	return v.lastCommitSeq
}

// SafeSnapshotPosition returns the highest stream sequence number that is safe to snapshot
// (no pending transaction and never beyond the Permanent / s3Seq watermark).
func (v *Volume) SafeSnapshotPosition() uint64 {
	v.mu.RLock()
	defer v.mu.RUnlock()
	return v.safeSnapshotPositionLocked()
}

func (v *Volume) safeSnapshotPositionLocked() uint64 {
	snapPos := v.lastCommitSeq
	if v.stream != nil {
		_, _, s3Seq := v.stream.Watermarks()
		if s3Seq > 0 && snapPos > s3Seq {
			snapPos = s3Seq
		} else if s3Seq > 0 && snapPos == 0 {
			snapPos = s3Seq
		}
	}
	return snapPos
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
					Atime:          rec.Atime,
					Ctime:          rec.Ctime,
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
				Atime:          mtime,
				Ctime:          mtime,
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

var (
	pkInode     = sds.NewPrimaryKey(1)
	pkDirEntry  = sds.NewPrimaryKey(1, 2)
	pkFileChunk = sds.NewPrimaryKey(1, 2)
)

// SQLiteCacheStats returns cache hit/miss and memory usage statistics.
func (v *Volume) SQLiteCacheStats() LRUCacheStats {
	v.mu.RLock()
	defer v.mu.RUnlock()
	if v.sqliteCache == nil {
		return LRUCacheStats{}
	}
	return v.sqliteCache.Stats()
}

// SQLiteCacheResetStats resets the cache hits and misses counters.
func (v *Volume) SQLiteCacheResetStats() {
	v.mu.Lock()
	defer v.mu.Unlock()
	if v.sqliteCache != nil {
		v.sqliteCache.ResetStats()
	}
}

// SetSQLiteCacheLimits updates the entry count and byte capacity of the SQLite read cache.
func (v *Volume) SetSQLiteCacheLimits(maxEntries int, maxBytes int64) {
	v.mu.Lock()
	defer v.mu.Unlock()
	if v.sqliteCache != nil {
		v.sqliteCache.SetLimits(maxEntries, maxBytes)
	}
}

// ApplyLag returns the difference between the stream sequence of the last committed transaction
// and the position applied to SQLite.
func (v *Volume) ApplyLag() uint64 {
	v.mu.RLock()
	defer v.mu.RUnlock()
	if v.lastCommitSeq > v.sqliteAppliedPos {
		return v.lastCommitSeq - v.sqliteAppliedPos
	}
	return 0
}

// ApplyFailures returns the total number of batch apply failures encountered by the background applier.
func (v *Volume) ApplyFailures() uint64 {
	if v.sqliteApplier != nil {
		return v.sqliteApplier.failures.Load()
	}
	return 0
}

// IsDegraded returns whether the SQLite metadata store is currently in a degraded state due to persistent errors.
func (v *Volume) IsDegraded() bool {
	if v.sqliteApplier != nil {
		return v.sqliteApplier.isDegraded.Load()
	}
	return false
}

// UnappliedBytes returns the current memory footprint in bytes of unapplied overlay rows.
func (v *Volume) UnappliedBytes() int64 {
	v.mu.RLock()
	defer v.mu.RUnlock()
	return v.unappliedBytes
}

// SetApplierFaultHook configures a fault injection callback for the background SQLite applier (testing only).
func (v *Volume) SetApplierFaultHook(hook func() error) {
	v.mu.Lock()
	defer v.mu.Unlock()
	v.applierFaultHook = hook
	if v.sqliteApplier != nil {
		v.sqliteApplier.faultHook = hook
	}
}

// SQLiteAppliedPosition returns the highest stream sequence position applied to SQLite.
func (v *Volume) SQLiteAppliedPosition() uint64 {
	v.mu.RLock()
	defer v.mu.RUnlock()
	return v.sqliteAppliedPos
}

// FlushOverlay blocks until all currently committed stream transactions have been applied to SQLite.
func (v *Volume) FlushOverlay(ctx context.Context) error {
	v.mu.Lock()
	defer v.mu.Unlock()
	return v.flushOverlayLocked(ctx)
}

func (v *Volume) flushOverlayLocked(ctx context.Context) error {
	if v.metadataStore != MetadataStoreSQLite || v.sqliteApplier == nil {
		return nil
	}

	targetSeq := v.lastCommitSeq
	for v.sqliteAppliedPos < targetSeq && !v.closed {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		v.sqliteApplier.wake()
		v.flushCond.Wait()
	}
	if v.closed {
		return fmt.Errorf("volume closed")
	}
	return nil
}

func (v *Volume) checkBackpressureLocked(ctx context.Context) error {
	if v.maxUnappliedBytes <= 0 || v.metadataStore != MetadataStoreSQLite {
		return nil
	}
	if v.unappliedBytes < v.maxUnappliedBytes {
		return nil
	}

	ctxDone := ctx.Done()
	if ctxDone != nil {
		stopCancel := make(chan struct{})
		defer close(stopCancel)
		go func() {
			select {
			case <-ctxDone:
				v.mu.Lock()
				if v.backpressureCond != nil {
					v.backpressureCond.Broadcast()
				}
				v.mu.Unlock()
			case <-stopCancel:
			case <-v.closedCh:
			}
		}()
	}

	for v.unappliedBytes >= v.maxUnappliedBytes && !v.closed {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if v.sqliteApplier != nil {
			v.sqliteApplier.wake()
		}
		v.backpressureCond.Wait()
	}
	if v.closed {
		return fmt.Errorf("volume closed")
	}
	return ctx.Err()
}

func (v *Volume) recordOverlayTxChangesLocked(tx *sds.Tx) {
	if tx == nil || v.metadataStore != MetadataStoreSQLite {
		return
	}
	changes := tx.Changes()
	if len(changes) == 0 {
		return
	}

	if v.sqliteOverlay == nil {
		v.sqliteOverlay = make(map[SQLiteCacheKey]SQLiteOverlayEntry)
	}

	for _, ch := range changes {
		typeName := ch.TypeName
		if typeName == "" && v.metadataStream != nil {
			if def, _, ok := v.metadataStream.Registry().LookupByID(ch.TypeID); ok {
				typeName = def.GetName()
			}
		}
		if typeName == "" && v.sqliteDB != nil {
			if def, _, ok := v.sqliteDB.Registry().LookupByID(ch.TypeID); ok {
				typeName = def.GetName()
			}
		}
		if typeName == "" {
			continue
		}

		key := ch.Key
		if key.IsZero() && len(ch.RawKey) > 0 {
			key = sds.NewKeyFromBytes(ch.RawKey)
		}
		ck := SQLiteCacheKey{Table: typeName, Key: key}

		sz := int64(len(typeName) + len(key.String()) + 96)
		if ch.Row != nil {
			sz += int64(proto.Size(ch.Row))
		}

		if old, ok := v.sqliteOverlay[ck]; ok {
			v.unappliedBytes += sz - old.Size
		} else {
			v.unappliedBytes += sz
		}

		var op sdsv1.OpRecord_Op
		switch ch.Op {
		case sds.OpCreate:
			op = sdsv1.OpRecord_CREATE
		case sds.OpUpdate:
			op = sdsv1.OpRecord_UPDATE
		case sds.OpDelete:
			op = sdsv1.OpRecord_DELETE
		}

		v.sqliteOverlay[ck] = SQLiteOverlayEntry{
			Op:   op,
			Row:  ch.Row,
			Seq:  ch.Seq,
			Size: sz,
		}
	}

	if v.sqliteApplier != nil {
		v.sqliteApplier.enqueueLocked(changes)
	}
}

func (v *Volume) scanSQLiteRowsLocked(ctx context.Context, typeName string, prefixBytes []byte) ([]proto.Message, error) {
	var sqliteMsgs []proto.Message
	if v.sqliteDB != nil {
		var err error
		sqliteMsgs, err = v.sqliteDB.Scan(ctx, typeName, prefixBytes)
		if err != nil {
			return nil, err
		}
	}

	if len(v.sqliteOverlay) == 0 {
		return sqliteMsgs, nil
	}

	type rowEntry struct {
		key sds.Key
		msg proto.Message
	}
	merged := make(map[string]rowEntry)

	for _, msg := range sqliteMsgs {
		var k sds.Key
		switch m := msg.(type) {
		case *pb.DirEntry:
			k, _ = pkDirEntry.Extract(m)
		case *pb.FileChunk:
			k, _ = pkFileChunk.Extract(m)
		case *pb.Inode:
			k, _ = pkInode.Extract(m)
		}
		if !k.IsZero() {
			merged[k.String()] = rowEntry{key: k, msg: msg}
		}
	}

	for ck, entry := range v.sqliteOverlay {
		if ck.Table != typeName {
			continue
		}
		if len(prefixBytes) > 0 && !bytes.HasPrefix(ck.Key.Bytes(), prefixBytes) {
			continue
		}
		kStr := ck.Key.String()
		if entry.Op == sdsv1.OpRecord_DELETE || entry.Row == nil {
			delete(merged, kStr)
		} else {
			merged[kStr] = rowEntry{key: ck.Key, msg: entry.Row}
		}
	}

	rows := make([]rowEntry, 0, len(merged))
	for _, r := range merged {
		rows = append(rows, r)
	}
	sort.Slice(rows, func(i, j int) bool {
		return bytes.Compare(rows[i].key.Bytes(), rows[j].key.Bytes()) < 0
	})

	result := make([]proto.Message, len(rows))
	for i, r := range rows {
		result[i] = r.msg
	}
	return result, nil
}

func (v *Volume) scanLimitSQLiteRowsLocked(ctx context.Context, typeName string, prefixBytes []byte, limit int) ([]proto.Message, error) {
	msgs, err := v.scanSQLiteRowsLocked(ctx, typeName, prefixBytes)
	if err != nil {
		return nil, err
	}
	if limit > 0 && len(msgs) > limit {
		msgs = msgs[:limit]
	}
	return msgs, nil
}

func (v *Volume) getSQLiteRowLocked(ctx context.Context, typeName string, key sds.Key) (proto.Message, bool, error) {
	// 1. Check unapplied in-memory overlay first
	if v.sqliteOverlay != nil {
		ck := SQLiteCacheKey{Table: typeName, Key: key}
		if entry, ok := v.sqliteOverlay[ck]; ok {
			if entry.Op == sdsv1.OpRecord_DELETE || entry.Row == nil {
				return nil, false, nil
			}
			return entry.Row, true, nil
		}
	}

	// 2. Check read cache
	if v.sqliteCache != nil && !v.sqliteCacheDisabled {
		ck := SQLiteCacheKey{Table: typeName, Key: key}
		if row, ok := v.sqliteCache.Get(ck); ok {
			if !row.Exists {
				return nil, false, nil
			}
			return row.Msg, true, nil
		}
	}

	if v.sqliteDB == nil {
		return nil, false, nil
	}

	// 3. Query SQLite
	msg, ok, err := v.sqliteDB.Get(ctx, typeName, key)
	if err != nil {
		return nil, false, err
	}

	if v.sqliteCache != nil && !v.sqliteCacheDisabled {
		ck := SQLiteCacheKey{Table: typeName, Key: key}
		if !ok {
			v.sqliteCache.Put(ck, &SQLiteCachedRow{Exists: false})
		} else {
			v.sqliteCache.Put(ck, &SQLiteCachedRow{Exists: true, Msg: msg})
		}
	}

	return msg, ok, nil
}

func (v *Volume) applyChangesToSQLiteCacheLocked(changes []sds.Change) {
	if v.sqliteCache == nil || v.sqliteCacheDisabled {
		return
	}
	for _, ch := range changes {
		typeName := ch.TypeName
		if typeName == "" && v.metadataStream != nil {
			if def, _, ok := v.metadataStream.Registry().LookupByID(ch.TypeID); ok {
				typeName = def.GetName()
			}
		}
		if typeName == "" && v.sqliteDB != nil {
			if def, _, ok := v.sqliteDB.Registry().LookupByID(ch.TypeID); ok {
				typeName = def.GetName()
			}
		}
		if typeName == "" {
			continue
		}
		key := ch.Key
		if key.IsZero() && len(ch.RawKey) > 0 {
			key = sds.NewKeyFromBytes(ch.RawKey)
		}
		ck := SQLiteCacheKey{Table: typeName, Key: key}
		switch ch.Op {
		case sds.OpCreate, sds.OpUpdate:
			if ch.Row != nil {
				v.sqliteCache.Put(ck, &SQLiteCachedRow{Exists: true, Msg: ch.Row})
			} else {
				v.sqliteCache.Remove(ck)
			}
		case sds.OpDelete:
			v.sqliteCache.Put(ck, &SQLiteCachedRow{Exists: false})
		}
	}
}

func (v *Volume) normalizeInodeID(id uint64) uint64 {
	if id == 0 || (v.metadataStore != MetadataStoreSQLite && id == 1) {
		if v.rootInodeID != 0 {
			return v.rootInodeID
		}
		return 1
	}
	return id
}

func (v *Volume) getOrLoadInodeLocked(ctx context.Context, inodeID uint64) (*CachedInode, error) {
	inodeID = v.normalizeInodeID(inodeID)

	if v.metadataStore == MetadataStoreSQLite && v.sqliteDB != nil {
		key, err := pkInode.Extract(&pb.Inode{Ino: proto.Uint64(inodeID)})
		if err != nil {
			return nil, err
		}
		msg, ok, err := v.getSQLiteRowLocked(ctx, "objectfs.v1alpha1.Inode", key)
		if err != nil {
			return nil, err
		}
		if !ok {
			return nil, fmt.Errorf("inode %d: %w", inodeID, syscall.ENOENT)
		}
		inode := msg.(*pb.Inode)
		chunkSize := inode.GetChunkSize()
		if chunkSize == 0 && v.chunkSize > 0 {
			chunkSize = v.chunkSize
		}
		node := &CachedInode{
			ID:             inode.GetIno(),
			Mode:           inode.GetMode(),
			Size:           inode.GetSize(),
			IsDir:          inode.GetIsDir(),
			Sha256:         inode.GetSha256(),
			ETag:           inode.GetEtag(),
			ManifestSha256: inode.GetManifestSha256(),
			ContentSha256:  inode.GetContentSha256(),
			ChunkSize:      chunkSize,
			Uid:            inode.GetUid(),
			Gid:            inode.GetGid(),
		}
		if inode.GetMtime() != nil {
			node.ModTime = inode.GetMtime().AsTime()
		}
		if inode.GetAtime() != nil {
			node.Atime = inode.GetAtime().AsTime()
		} else {
			node.Atime = node.ModTime
		}
		if inode.GetCtime() != nil {
			node.Ctime = inode.GetCtime().AsTime()
		} else {
			node.Ctime = node.ModTime
		}

		_ = v.ensureInodeChunksLoadedLocked(ctx, node)

		return node, nil
	}

	return v.resolveInode(ctx, inodeID, true)
}

func (v *Volume) getOrLoadDirLocked(ctx context.Context, inodeID uint64) (*CachedDir, error) {
	inodeID = v.normalizeInodeID(inodeID)

	if v.metadataStore == MetadataStoreSQLite && v.sqliteDB != nil {
		dirNode, err := v.getOrLoadInodeLocked(ctx, inodeID)
		if err != nil {
			return nil, err
		}
		if !dirNode.IsDir {
			return nil, fmt.Errorf("inode %d: %w", inodeID, syscall.ENOTDIR)
		}

		prefixBytes, pErr := sds.EncodeKeyPrefix(&pb.DirEntry{ParentIno: proto.Uint64(inodeID)}, 1)
		if pErr != nil {
			return nil, pErr
		}
		entryMsgs, err := v.scanSQLiteRowsLocked(ctx, "objectfs.v1alpha1.DirEntry", prefixBytes)
		if err != nil {
			return nil, err
		}

		dir := &CachedDir{
			ID:      inodeID,
			Entries: make(map[string]DirEntry, len(entryMsgs)),
			Added:   make(map[string]bool),
			Deleted: make(map[string]bool),
		}

		for _, msg := range entryMsgs {
			de := msg.(*pb.DirEntry)
			name := de.GetName()
			dir.Entries[name] = DirEntry{
				Name:    name,
				InodeID: de.GetIno(),
				IsDir:   de.GetIsDir(),
				Mode:    de.GetMode(),
			}
			if de.GetIsDir() {
				if v.dirParents == nil {
					v.dirParents = make(map[uint64]uint64)
				}
				v.dirParents[de.GetIno()] = inodeID
			}
			if v.sqliteCache != nil && !v.sqliteCacheDisabled {
				if k, kErr := pkDirEntry.Extract(&pb.DirEntry{
					ParentIno: proto.Uint64(inodeID),
					Name:      proto.String(name),
				}); kErr == nil && !k.IsZero() {
					ck := SQLiteCacheKey{Table: "objectfs.v1alpha1.DirEntry", Key: k}
					v.sqliteCache.Put(ck, &SQLiteCachedRow{Exists: true, Msg: de})
				}
			}
		}

		return dir, nil
	}

	return v.resolveDir(ctx, inodeID, true)
}

type InodeEntry struct {
	InodeID uint64
	IsDir   bool
	Mode    uint32
}

func (v *Volume) getDirEntrySQLiteLocked(ctx context.Context, parentInodeID uint64, name string) (*pb.DirEntry, bool, error) {
	if v.sqliteDB == nil {
		return nil, false, nil
	}
	key, err := pkDirEntry.Extract(&pb.DirEntry{
		ParentIno: proto.Uint64(parentInodeID),
		Name:      proto.String(name),
	})
	if err != nil {
		return nil, false, err
	}
	msg, ok, err := v.getSQLiteRowLocked(ctx, "objectfs.v1alpha1.DirEntry", key)
	if err != nil {
		return nil, false, err
	}
	if !ok {
		return nil, false, nil
	}
	return msg.(*pb.DirEntry), true, nil
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
		if len(part) > MaxNameLength {
			return 0, 0, "", fmt.Errorf("path component %q exceeds maximum length: %w", part, syscall.ENAMETOOLONG)
		}
		var entryInodeID uint64
		var isDir bool
		if v.metadataStore == MetadataStoreSQLite && v.sqliteDB != nil {
			de, ok, err := v.getDirEntrySQLiteLocked(ctx, currInodeID, part)
			if err != nil {
				return 0, 0, "", fmt.Errorf("directory for inode %d not found: %w", currInodeID, err)
			}
			if !ok {
				return 0, 0, "", fmt.Errorf("path component %s not found: %w", part, syscall.ENOENT)
			}
			entryInodeID = de.GetIno()
			isDir = de.GetIsDir()
		} else {
			dir, err := v.getOrLoadDirLocked(ctx, currInodeID)
			if err != nil {
				return 0, 0, "", fmt.Errorf("directory for inode %d not found: %w", currInodeID, syscall.ENOENT)
			}
			entry, exists := dir.Entries[part]
			if !exists {
				return 0, 0, "", fmt.Errorf("path component %s not found: %w", part, syscall.ENOENT)
			}
			entryInodeID = entry.InodeID
			isDir = entry.IsDir
		}
		if i == len(parts)-1 {
			return entryInodeID, currInodeID, part, nil
		}
		if !isDir {
			return 0, 0, "", fmt.Errorf("path component %s is not a directory: %w", part, syscall.ENOTDIR)
		}
		parentInodeID = currInodeID
		baseName = part
		currInodeID = entryInodeID
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
	atime := node.Atime
	if atime.IsZero() {
		atime = node.ModTime
	}
	ctime := node.Ctime
	if ctime.IsZero() {
		ctime = node.ModTime
	}
	ino := node.ID
	if node.ID == v.rootInodeID {
		ino = 1
	}
	return &pb.EntryAttr{
		Inode:          ino,
		Name:           name,
		IsDir:          node.IsDir,
		Size:           node.Size,
		Mode:           node.Mode,
		ModTime:        timestamppb.New(node.ModTime),
		Atime:          timestamppb.New(atime),
		Ctime:          timestamppb.New(ctime),
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

	inodeID = v.normalizeInodeID(inodeID)
	return v.toEntryAttrLocked(ctx, inodeID, "")
}

func (v *Volume) SetAttr(ctx context.Context, inodeID uint64, mode *uint32, uid *uint32, gid *uint32, atime *time.Time, atimeNow bool, mtime *time.Time, mtimeNow bool, ctime *time.Time, ctimeNow bool) (*pb.EntryAttr, error) {
	attr, waitFn, err := func() (*pb.EntryAttr, func(context.Context) error, error) {
		v.mu.Lock()
		defer v.mu.Unlock()

		if err := v.checkBackpressureLocked(ctx); err != nil {
			return nil, nil, err
		}

		inodeID = v.normalizeInodeID(inodeID)

		node, err := v.getOrLoadInodeLocked(ctx, inodeID)
		if err != nil {
			return nil, nil, err
		}

		now := time.Now()
		modified := false

		if mode != nil {
			// keep setuid, setgid and sticky (07777), and the file-type bits untouched by chmod
			node.Mode = (node.Mode & ^uint32(07777)) | (*mode & 07777)
			modified = true
		}
		if uid != nil {
			node.Uid = *uid
			modified = true
		}
		if gid != nil {
			node.Gid = *gid
			modified = true
		}
		if atimeNow {
			node.Atime = now
			modified = true
		} else if atime != nil {
			node.Atime = *atime
			modified = true
		}
		if mtimeNow {
			node.ModTime = now
			modified = true
		} else if mtime != nil {
			node.ModTime = *mtime
			modified = true
		}

		if ctimeNow {
			node.Ctime = now
			modified = true
		} else if ctime != nil {
			node.Ctime = *ctime
			modified = true
		} else if modified {
			node.Ctime = now
		}

		if !modified {
			attr, err := v.toEntryAttrLocked(ctx, inodeID, "")
			return attr, nil, err
		}

		node.IsDirty = true
		v.inodeCache.Put(inodeID, node)

		tx := v.metadataStream.Begin()
		mAtime := node.Atime
		if mAtime.IsZero() {
			mAtime = node.ModTime
		}
		mCtime := node.Ctime
		if mCtime.IsZero() {
			mCtime = node.ModTime
		}
		inodeMsg := &pb.Inode{
			Ino:            proto.Uint64(node.ID),
			Mode:           node.Mode,
			Size:           node.Size,
			Mtime:          timestamppb.New(node.ModTime),
			Atime:          timestamppb.New(mAtime),
			Ctime:          timestamppb.New(mCtime),
			IsDir:          node.IsDir,
			Sha256:         node.Sha256,
			Etag:           node.ETag,
			ManifestSha256: node.ManifestSha256,
			ContentSha256:  node.ContentSha256,
			ChunkSize:      node.ChunkSize,
			Uid:            node.Uid,
			Gid:            node.Gid,
		}
		if _, err := tx.Update(ctx, inodeMsg); err != nil {
			return nil, nil, fmt.Errorf("failed to log inode update: %w", err)
		}

		commitSeq, err := tx.Commit(ctx)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to commit setattr transaction: %w", err)
		}
		if err := v.applyTxChangesLocked(ctx, tx); err != nil {
			return nil, nil, err
		}

		waitFn := v.makeWaitFn(commitSeq, nil)

		mSha := node.ManifestSha256
		cSha := node.ContentSha256
		if node.ChunkSize == 0 && mSha == "" {
			cSha = node.Sha256
		} else if mSha == "" {
			mSha = node.Sha256
		}
		attr := &pb.EntryAttr{
			Inode:          node.ID,
			IsDir:          node.IsDir,
			Size:           node.Size,
			Mode:           node.Mode,
			ModTime:        timestamppb.New(node.ModTime),
			Atime:          timestamppb.New(mAtime),
			Ctime:          timestamppb.New(mCtime),
			Sha256:         node.Sha256,
			ManifestSha256: mSha,
			ContentSha256:  cSha,
			RedirectUrl:    node.RedirectURL,
			Uid:            node.Uid,
			Gid:            node.Gid,
		}

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
	if v.broadcaster != nil {
		v.broadcaster.Broadcast(v.volumeID, &pb.WatchVolumeResponse{
			EventType: pb.WatchEventType_EVENT_MODIFIED,
			Attr:      attr,
			Inode:     attr.Inode,
		})
	}
	return attr, nil
}

func (v *Volume) Lookup(ctx context.Context, parentInodeID uint64, name string) (*pb.EntryAttr, error) {
	if len(name) > MaxNameLength {
		return nil, fmt.Errorf("name %q exceeds maximum length: %w", name, syscall.ENAMETOOLONG)
	}

	v.mu.RLock()
	defer v.mu.RUnlock()

	parentInodeID = v.normalizeInodeID(parentInodeID)

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

	if v.metadataStore == MetadataStoreSQLite && v.sqliteDB != nil {
		de, ok, err := v.getDirEntrySQLiteLocked(ctx, parentInodeID, name)
		if err != nil {
			return nil, err
		}
		if !ok {
			return nil, fmt.Errorf("child %s not found in inode %d: %w", name, parentInodeID, syscall.ENOENT)
		}
		return v.toEntryAttrLocked(ctx, de.GetIno(), name)
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

	dirInodeID = v.normalizeInodeID(dirInodeID)

	dirNode, err := v.getOrLoadInodeLocked(ctx, dirInodeID)
	if err != nil {
		return nil, err
	}
	if !dirNode.IsDir {
		return nil, fmt.Errorf("inode %d is not a directory: %w", dirInodeID, syscall.ENOTDIR)
	}

	if v.metadataStore == MetadataStoreSQLite && v.sqliteDB != nil {
		prefixBytes, pErr := sds.EncodeKeyPrefix(&pb.DirEntry{ParentIno: proto.Uint64(dirInodeID)}, 1)
		if pErr != nil {
			return nil, pErr
		}
		entryMsgs, err := v.scanSQLiteRowsLocked(ctx, "objectfs.v1alpha1.DirEntry", prefixBytes)
		if err != nil {
			return nil, err
		}
		names := make([]string, 0, len(entryMsgs))
		entriesMap := make(map[string]*pb.DirEntry, len(entryMsgs))
		for _, msg := range entryMsgs {
			de := msg.(*pb.DirEntry)
			name := de.GetName()
			names = append(names, name)
			entriesMap[name] = de
		}
		sort.Strings(names)

		var entries []*pb.EntryAttr
		for _, name := range names {
			de := entriesMap[name]
			attr, err := v.toEntryAttrLocked(ctx, de.GetIno(), name)
			if err == nil {
				entries = append(entries, attr)
			}
		}
		return entries, nil
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

func (v *Volume) applyTxChangesLocked(ctx context.Context, tx *sds.Tx) error {
	if v.metadataStore == MetadataStoreSQLite && tx != nil {
		v.recordOverlayTxChangesLocked(tx)
	}
	return nil
}

func (v *Volume) Mkdir(ctx context.Context, parentInodeID uint64, name string, mode uint32, uid, gid uint32) (*pb.EntryAttr, error) {
	if len(name) > MaxNameLength {
		return nil, fmt.Errorf("directory name %q exceeds maximum length: %w", name, syscall.ENAMETOOLONG)
	}

	attr, waitFn, err := func() (*pb.EntryAttr, func(context.Context) error, error) {
		v.mu.Lock()
		defer v.mu.Unlock()

		if err := v.checkBackpressureLocked(ctx); err != nil {
			return nil, nil, err
		}

		parentInodeID = v.normalizeInodeID(parentInodeID)
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

		var parentDir *CachedDir
		if v.metadataStore == MetadataStoreSQLite && v.sqliteDB != nil {
			_, exists, err := v.getDirEntrySQLiteLocked(ctx, parentInodeID, name)
			if err != nil {
				return nil, nil, err
			}
			if exists {
				return nil, nil, fmt.Errorf("directory %s already exists under inode %d: %w", name, parentInodeID, syscall.EEXIST)
			}
		} else {
			var err error
			parentDir, err = v.getOrLoadDirLocked(ctx, parentInodeID)
			if err != nil {
				return nil, nil, err
			}
			if _, exists := parentDir.Entries[name]; exists {
				return nil, nil, fmt.Errorf("directory %s already exists under inode %d: %w", name, parentInodeID, syscall.EEXIST)
			}
		}

		if mode == 0 {
			mode = 0755
		}
		mode |= syscall.S_IFDIR

		if (parentInode.Mode & 02000) != 0 {
			gid = parentInode.Gid
			mode |= 02000
		}

		now := time.Now()
		childInodeID := v.allocInode()

		if parentDir != nil {
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
		}
		parentInode.ModTime = now
		parentInode.Ctime = now
		parentInode.IsDirty = true
		if v.inodeCache != nil {
			v.inodeCache.Put(parentInode.ID, parentInode)
		}

		if v.dirParents == nil {
			v.dirParents = make(map[uint64]uint64)
		}
		v.dirParents[childInodeID] = parentInodeID

		if v.metadataStore != MetadataStoreSQLite {
			childInode := &CachedInode{
				ID:      childInodeID,
				Mode:    mode,
				ModTime: now,
				Atime:   now,
				Ctime:   now,
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
		}

		var commitSeq uint64
		tx := v.metadataStream.Begin()
		childInodeMsg := &pb.Inode{
			Ino:   proto.Uint64(childInodeID),
			Mode:  mode,
			Size:  0,
			Mtime: timestamppb.New(now),
			Atime: timestamppb.New(now),
			Ctime: timestamppb.New(now),
			Uid:   uid,
			Gid:   gid,
			IsDir: true,
		}
		if _, err := tx.Insert(ctx, childInodeMsg); err != nil {
			if parentDir != nil {
				delete(parentDir.Entries, name)
				delete(parentDir.Added, name)
			}
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
			if parentDir != nil {
				delete(parentDir.Entries, name)
				delete(parentDir.Added, name)
			}
			delete(v.dirParents, childInodeID)
			return nil, nil, fmt.Errorf("failed to log directory entry: %w", err)
		}

		pAtime := parentInode.Atime
		if pAtime.IsZero() {
			pAtime = parentInode.ModTime
		}
		parentInodeMsg := &pb.Inode{
			Ino:            proto.Uint64(parentInode.ID),
			Mode:           parentInode.Mode,
			Size:           parentInode.Size,
			Mtime:          timestamppb.New(now),
			Atime:          timestamppb.New(pAtime),
			Ctime:          timestamppb.New(now),
			Uid:            parentInode.Uid,
			Gid:            parentInode.Gid,
			IsDir:          true,
			Sha256:         parentInode.Sha256,
			Etag:           parentInode.ETag,
			ManifestSha256: parentInode.ManifestSha256,
			ContentSha256:  parentInode.ContentSha256,
			ChunkSize:      parentInode.ChunkSize,
		}
		if _, err := tx.Update(ctx, parentInodeMsg); err != nil {
			if parentDir != nil {
				delete(parentDir.Entries, name)
				delete(parentDir.Added, name)
			}
			delete(v.dirParents, childInodeID)
			return nil, nil, fmt.Errorf("failed to log parent inode update: %w", err)
		}

		commitSeq, err = tx.Commit(ctx)
		if err != nil {
			if parentDir != nil {
				delete(parentDir.Entries, name)
				delete(parentDir.Added, name)
			}
			delete(v.dirParents, childInodeID)
			return nil, nil, fmt.Errorf("failed to commit mkdir transaction: %w", err)
		}
		if err := v.applyTxChangesLocked(ctx, tx); err != nil {
			if parentDir != nil {
				delete(parentDir.Entries, name)
				delete(parentDir.Added, name)
			}
			delete(v.dirParents, childInodeID)
			return nil, nil, err
		}

		waitFn := v.makeWaitFn(commitSeq, nil)

		attr := &pb.EntryAttr{
			Inode:   childInodeID,
			Name:    name,
			IsDir:   true,
			Size:    0,
			Mode:    mode,
			ModTime: timestamppb.New(now),
			Atime:   timestamppb.New(now),
			Ctime:   timestamppb.New(now),
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
	if len(name) > MaxNameLength {
		return nil, fmt.Errorf("file name %q exceeds maximum length: %w", name, syscall.ENAMETOOLONG)
	}

	attr, waitFn, err := func() (*pb.EntryAttr, func(context.Context) error, error) {
		v.mu.Lock()
		defer v.mu.Unlock()

		if err := v.checkBackpressureLocked(ctx); err != nil {
			return nil, nil, err
		}

		parentInodeID = v.normalizeInodeID(parentInodeID)
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

		var parentDir *CachedDir
		var existingChildIno uint64
		var exists bool
		if v.metadataStore == MetadataStoreSQLite && v.sqliteDB != nil {
			de, ok, err := v.getDirEntrySQLiteLocked(ctx, parentInodeID, name)
			if err != nil {
				return nil, nil, err
			}
			if ok {
				exists = true
				existingChildIno = de.GetIno()
			}
		} else {
			var err error
			parentDir, err = v.getOrLoadDirLocked(ctx, parentInodeID)
			if err != nil {
				return nil, nil, err
			}
			if entry, ok := parentDir.Entries[name]; ok {
				exists = true
				existingChildIno = entry.InodeID
			}
		}

		if mode == 0 {
			mode = 0644
		}
		mode |= syscall.S_IFREG

		if (parentInode.Mode & 02000) != 0 {
			gid = parentInode.Gid
		}

		now := time.Now()
		var hashStr string
		if len(initialContent) > 0 {
			h := sha256.Sum256(initialContent)
			hashStr = fmt.Sprintf("%x", h)
		}

		dataCopy := make([]byte, len(initialContent))
		copy(dataCopy, initialContent)

		if exists {
			childInode, err := v.getOrLoadInodeLocked(ctx, existingChildIno)
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
			childInode.Ctime = now
			childInode.Uid = uid
			childInode.Gid = gid
			childInode.IsDirty = true

			parentInode.ModTime = now
			parentInode.Ctime = now
			parentInode.IsDirty = true
			if v.inodeCache != nil {
				v.inodeCache.Put(parentInode.ID, parentInode)
			}

			effectiveChunkSize := childInode.ChunkSize
			if effectiveChunkSize == 0 {
				effectiveChunkSize = v.chunkSize
				if effectiveChunkSize == 0 {
					effectiveChunkSize = 64 * 1024
				}
				childInode.ChunkSize = effectiveChunkSize
			}

			oldChunks := childInode.Chunks
			tx := v.metadataStream.Begin()

			if len(dataCopy) <= int(v.maxInlineLen) {
				childInode.InlineData = dataCopy
				childInode.ContentSha256 = hashStr
				childInode.Chunks = nil
				childInode.StagedChunks = nil
				childInode.DirtyChunks = nil
				childInode.ManifestSha256 = ""

				if v.metadataStore == MetadataStoreSQLite && v.sqliteDB != nil {
					prefixBytes, _ := sds.EncodeKeyPrefix(&pb.FileChunk{Ino: proto.Uint64(childInode.ID)}, 1)
					chunkMsgs, _ := v.scanSQLiteRowsLocked(ctx, "objectfs.v1alpha1.FileChunk", prefixBytes)
					for _, msg := range chunkMsgs {
						c := msg.(*pb.FileChunk)
						if _, err := tx.Delete(ctx, &pb.FileChunk{Ino: proto.Uint64(childInode.ID), Index: proto.Uint32(c.GetIndex())}); err != nil {
							return nil, nil, fmt.Errorf("failed to delete old FileChunk: %w", err)
						}
					}
				} else {
					for i := range oldChunks {
						if _, err := tx.Delete(ctx, &pb.FileChunk{Ino: proto.Uint64(childInode.ID), Index: proto.Uint32(i)}); err != nil {
							return nil, nil, fmt.Errorf("failed to delete old FileChunk: %w", err)
						}
					}
				}
				if len(dataCopy) > 0 {
					if _, err := tx.Insert(ctx, &pb.FileChunk{Ino: proto.Uint64(childInode.ID), Index: proto.Uint32(0), InlineData: dataCopy}); err != nil {
						return nil, nil, fmt.Errorf("failed to log inline FileChunk: %w", err)
					}
				}
			} else {
				childInode.InlineData = nil
				childInode.ContentSha256 = hashStr
				childInode.Chunks = make(map[uint32]string)
				childInode.StagedChunks = make(map[int][]byte)
				childInode.DirtyChunks = nil

				blobsMap := make(map[string]blob.ByteStream)
				cs := int(effectiveChunkSize)
				numChunks := (len(dataCopy) + cs - 1) / cs
				for i := 0; i < numChunks; i++ {
					start := i * cs
					end := (i + 1) * cs
					if end > len(dataCopy) {
						end = len(dataCopy)
					}
					cBytes := make([]byte, end-start)
					copy(cBytes, dataCopy[start:end])
					cSha := fmt.Sprintf("%x", sha256.Sum256(cBytes))
					childInode.Chunks[uint32(i)] = cSha
					childInode.StagedChunks[i] = cBytes
					blobsMap[cSha] = blob.NewByteStreamFromBytes(cBytes)
				}

				if v.blobStore != nil && len(blobsMap) > 0 {
					if err := v.blobStore.PutBlobs(ctx, blobsMap); err != nil {
						return nil, nil, fmt.Errorf("failed to upload blobs: %w", err)
					}
				}

				if v.metadataStore == MetadataStoreSQLite && v.sqliteDB != nil {
					prefixBytes, _ := sds.EncodeKeyPrefix(&pb.FileChunk{Ino: proto.Uint64(childInode.ID)}, 1)
					chunkMsgs, _ := v.scanSQLiteRowsLocked(ctx, "objectfs.v1alpha1.FileChunk", prefixBytes)
					for _, msg := range chunkMsgs {
						c := msg.(*pb.FileChunk)
						if _, stillPresent := childInode.Chunks[c.GetIndex()]; !stillPresent {
							if _, err := tx.Delete(ctx, &pb.FileChunk{Ino: proto.Uint64(childInode.ID), Index: proto.Uint32(c.GetIndex())}); err != nil {
								return nil, nil, fmt.Errorf("failed to delete old FileChunk: %w", err)
							}
						}
					}
				} else {
					for i := range oldChunks {
						if _, stillPresent := childInode.Chunks[i]; !stillPresent {
							if _, err := tx.Delete(ctx, &pb.FileChunk{Ino: proto.Uint64(childInode.ID), Index: proto.Uint32(i)}); err != nil {
								return nil, nil, fmt.Errorf("failed to delete old FileChunk: %w", err)
							}
						}
					}
				}
				for i, sha := range childInode.Chunks {
					if _, err := tx.Insert(ctx, &pb.FileChunk{Ino: proto.Uint64(childInode.ID), Index: proto.Uint32(i), Sha256: sha}); err != nil {
						return nil, nil, fmt.Errorf("failed to log FileChunk: %w", err)
					}
				}
			}

			cAtime := childInode.Atime
			if cAtime.IsZero() {
				cAtime = now
			}
			childInodeMsg := &pb.Inode{
				Ino:           proto.Uint64(childInode.ID),
				Mode:          childInode.Mode,
				Size:          childInode.Size,
				Mtime:         timestamppb.New(now),
				Atime:         timestamppb.New(cAtime),
				Ctime:         timestamppb.New(now),
				Uid:           uid,
				Gid:           gid,
				IsDir:         false,
				ContentSha256: childInode.ContentSha256,
				ChunkSize:     childInode.ChunkSize,
			}
			if _, err := tx.Update(ctx, childInodeMsg); err != nil {
				return nil, nil, fmt.Errorf("failed to log child inode update: %w", err)
			}

			pAtime := parentInode.Atime
			if pAtime.IsZero() {
				pAtime = parentInode.ModTime
			}
			parentInodeMsg := &pb.Inode{
				Ino:            proto.Uint64(parentInode.ID),
				Mode:           parentInode.Mode,
				Size:           parentInode.Size,
				Mtime:          timestamppb.New(now),
				Atime:          timestamppb.New(pAtime),
				Ctime:          timestamppb.New(now),
				Uid:            parentInode.Uid,
				Gid:            parentInode.Gid,
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

			commitSeq, err := tx.Commit(ctx)
			if err != nil {
				return nil, nil, fmt.Errorf("failed to commit overwrite file transaction: %w", err)
			}
			if err := v.applyTxChangesLocked(ctx, tx); err != nil {
				return nil, nil, err
			}

			waitFn := v.makeWaitFn(commitSeq, nil)

			attr := &pb.EntryAttr{
				Inode:          childInode.ID,
				Name:           name,
				IsDir:          false,
				Size:           childInode.Size,
				Mode:           childInode.Mode,
				ModTime:        timestamppb.New(now),
				Atime:          timestamppb.New(cAtime),
				Ctime:          timestamppb.New(now),
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
		if parentDir != nil {
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
		}
		parentInode.ModTime = now
		parentInode.Ctime = now
		parentInode.IsDirty = true
		if v.inodeCache != nil {
			v.inodeCache.Put(parentInode.ID, parentInode)
		}

		effectiveChunkSize := v.chunkSize
		if effectiveChunkSize == 0 {
			effectiveChunkSize = 64 * 1024
		}

		childInode := &CachedInode{
			ID:        childInodeID,
			Mode:      mode,
			Size:      int64(len(dataCopy)),
			ModTime:   now,
			Atime:     now,
			Ctime:     now,
			IsDir:     false,
			Uid:       uid,
			Gid:       gid,
			ChunkSize: effectiveChunkSize,
			IsDirty:   true,
		}

		tx := v.metadataStream.Begin()

		if len(dataCopy) <= int(v.maxInlineLen) {
			childInode.InlineData = dataCopy
			childInode.ContentSha256 = hashStr

			if len(dataCopy) > 0 {
				if _, err := tx.Insert(ctx, &pb.FileChunk{Ino: proto.Uint64(childInodeID), Index: proto.Uint32(0), InlineData: dataCopy}); err != nil {
					if parentDir != nil {
						delete(parentDir.Entries, name)
						delete(parentDir.Added, name)
					}
					return nil, nil, fmt.Errorf("failed to log inline FileChunk: %w", err)
				}
			}
		} else {
			childInode.ContentSha256 = hashStr
			childInode.Chunks = make(map[uint32]string)
			childInode.StagedChunks = make(map[int][]byte)

			blobsMap := make(map[string]blob.ByteStream)
			cs := int(effectiveChunkSize)
			numChunks := (len(dataCopy) + cs - 1) / cs
			for i := 0; i < numChunks; i++ {
				start := i * cs
				end := (i + 1) * cs
				if end > len(dataCopy) {
					end = len(dataCopy)
				}
				cBytes := make([]byte, end-start)
				copy(cBytes, dataCopy[start:end])
				cSha := fmt.Sprintf("%x", sha256.Sum256(cBytes))
				childInode.Chunks[uint32(i)] = cSha
				childInode.StagedChunks[i] = cBytes
				blobsMap[cSha] = blob.NewByteStreamFromBytes(cBytes)
			}

			if v.blobStore != nil && len(blobsMap) > 0 {
				if err := v.blobStore.PutBlobs(ctx, blobsMap); err != nil {
					if parentDir != nil {
						delete(parentDir.Entries, name)
						delete(parentDir.Added, name)
					}
					return nil, nil, fmt.Errorf("failed to upload blobs: %w", err)
				}
			}

			for i, sha := range childInode.Chunks {
				if _, err := tx.Insert(ctx, &pb.FileChunk{Ino: proto.Uint64(childInodeID), Index: proto.Uint32(i), Sha256: sha}); err != nil {
					if parentDir != nil {
						delete(parentDir.Entries, name)
						delete(parentDir.Added, name)
					}
					return nil, nil, fmt.Errorf("failed to log FileChunk: %w", err)
				}
			}
		}

		if v.metadataStore != MetadataStoreSQLite {
			v.inodeCache.Put(childInodeID, childInode)
		}

		childInodeMsg := &pb.Inode{
			Ino:           proto.Uint64(childInodeID),
			Mode:          mode,
			Size:          int64(len(dataCopy)),
			Mtime:         timestamppb.New(now),
			Atime:         timestamppb.New(now),
			Ctime:         timestamppb.New(now),
			Uid:           uid,
			Gid:           gid,
			IsDir:         false,
			ContentSha256: childInode.ContentSha256,
			ChunkSize:     childInode.ChunkSize,
		}
		if _, err := tx.Insert(ctx, childInodeMsg); err != nil {
			if parentDir != nil {
				delete(parentDir.Entries, name)
				delete(parentDir.Added, name)
			}
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
			if parentDir != nil {
				delete(parentDir.Entries, name)
				delete(parentDir.Added, name)
			}
			return nil, nil, fmt.Errorf("failed to log directory entry: %w", err)
		}

		pAtime := parentInode.Atime
		if pAtime.IsZero() {
			pAtime = parentInode.ModTime
		}
		parentInodeMsg := &pb.Inode{
			Ino:            proto.Uint64(parentInode.ID),
			Mode:           parentInode.Mode,
			Size:           parentInode.Size,
			Mtime:          timestamppb.New(now),
			Atime:          timestamppb.New(pAtime),
			Ctime:          timestamppb.New(now),
			Uid:            parentInode.Uid,
			Gid:            parentInode.Gid,
			IsDir:          true,
			Sha256:         parentInode.Sha256,
			Etag:           parentInode.ETag,
			ManifestSha256: parentInode.ManifestSha256,
			ContentSha256:  parentInode.ContentSha256,
			ChunkSize:      parentInode.ChunkSize,
		}
		if _, err := tx.Update(ctx, parentInodeMsg); err != nil {
			if parentDir != nil {
				delete(parentDir.Entries, name)
				delete(parentDir.Added, name)
			}
			return nil, nil, fmt.Errorf("failed to log parent inode update: %w", err)
		}
		commitSeq, err := tx.Commit(ctx)
		if err != nil {
			if parentDir != nil {
				delete(parentDir.Entries, name)
				delete(parentDir.Added, name)
			}
			return nil, nil, fmt.Errorf("failed to commit create file transaction: %w", err)
		}
		if err := v.applyTxChangesLocked(ctx, tx); err != nil {
			if parentDir != nil {
				delete(parentDir.Entries, name)
				delete(parentDir.Added, name)
			}
			return nil, nil, err
		}

		waitFn := v.makeWaitFn(commitSeq, nil)

		attr := &pb.EntryAttr{
			Inode:          childInodeID,
			Name:           name,
			IsDir:          false,
			Size:           int64(len(dataCopy)),
			Mode:           mode,
			ModTime:        timestamppb.New(now),
			Atime:          timestamppb.New(now),
			Ctime:          timestamppb.New(now),
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
	if inodeID == 0 {
		v.mu.RLock()
		inodeID = v.rootInodeID
		v.mu.RUnlock()
	}
	_ = v.waitForInodeUploads(ctx, inodeID)

	v.mu.RLock()
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

	if offset >= total || total == 0 {
		return []byte{}, total, "", nil
	}

	end := offset + length
	if length <= 0 || end > total {
		end = total
	}
	readLen := end - offset

	v.mu.Lock()
	defer v.mu.Unlock()

	if len(node.InlineData) > 0 {
		res := make([]byte, readLen)
		if offset < int64(len(node.InlineData)) {
			copyEnd := end
			if copyEnd > int64(len(node.InlineData)) {
				copyEnd = int64(len(node.InlineData))
			}
			copy(res, node.InlineData[offset:copyEnd])
		}
		return res, total, "", nil
	}

	if node.ManifestSha256 != "" || node.ChunkSize > 0 || len(node.Chunks) > 0 || len(node.StagedChunks) > 0 {
		_ = v.ensureInodeChunksLoadedLocked(ctx, node)
		cs := int64(node.ChunkSize)
		if cs == 0 {
			cs = int64(v.chunkSize)
			if cs == 0 {
				cs = 64 * 1024
			}
		}
		startChunk := int(offset / cs)
		endChunk := int((end - 1) / cs)

		var res bytes.Buffer
		for i := startChunk; i <= endChunk; i++ {
			chunkData, _ := v.readChunkLocked(ctx, node, i)
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
		return make([]byte, readLen), total, "", nil
	}

	if _, err := node.Data.Seek(offset, io.SeekStart); err == nil {
		res := make([]byte, readLen)
		n, err := io.ReadFull(node.Data, res)
		if err != nil && err != io.EOF && err != io.ErrUnexpectedEOF {
			return nil, 0, "", fmt.Errorf("failed to read node data: %w", err)
		}
		return res[:n], total, "", nil
	}

	return make([]byte, readLen), total, "", nil
}

func (v *Volume) WriteFile(ctx context.Context, inodeID uint64, offset int64, data []byte, writeMode pb.WriteMode) (int64, int64, time.Time, error) {
	nWritten, newSize, modTime, waitFn, err := func() (int64, int64, time.Time, func(context.Context) error, error) {
		v.mu.Lock()
		defer v.mu.Unlock()

		if err := v.checkBackpressureLocked(ctx); err != nil {
			return 0, 0, time.Time{}, nil, err
		}

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
			effectiveChunkSize = 64 * 1024
		}
		if node.ChunkSize > 0 {
			effectiveChunkSize = node.ChunkSize
		} else {
			node.ChunkSize = effectiveChunkSize
		}

		neededLen := offset + int64(len(data))
		calculatedSize := neededLen
		if calculatedSize < node.Size {
			calculatedSize = node.Size
		}

		now := time.Now()
		node.ModTime = now
		node.Ctime = now
		node.IsDirty = true

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

		// Tiny file check
		if calculatedSize <= v.maxInlineLen && len(node.Chunks) == 0 {
			if node.InlineData == nil {
				if offset == 0 {
					node.InlineData = make([]byte, len(data))
					copy(node.InlineData, data)
				} else {
					chunk0, _ := v.readChunkLocked(ctx, node, 0)
					if neededLen > int64(len(chunk0)) {
						newBuf := make([]byte, neededLen)
						copy(newBuf, chunk0)
						chunk0 = newBuf
					}
					copy(chunk0[offset:], data)
					node.InlineData = chunk0
				}
			} else {
				if neededLen > int64(len(node.InlineData)) {
					newBuf := make([]byte, neededLen)
					copy(newBuf, node.InlineData)
					node.InlineData = newBuf
				}
				copy(node.InlineData[offset:], data)
			}
			node.Size = int64(len(node.InlineData))
			if offset == 0 && int64(len(data)) == node.Size {
				h := sha256.Sum256(data)
				node.ContentSha256 = fmt.Sprintf("%x", h)
			} else {
				node.ContentSha256 = ""
			}

			tx := v.metadataStream.Begin()
			if _, err := tx.Insert(ctx, &pb.FileChunk{
				Ino:        proto.Uint64(node.ID),
				Index:      proto.Uint32(0),
				InlineData: node.InlineData,
			}); err != nil {
				return 0, 0, time.Time{}, nil, fmt.Errorf("failed to log inline FileChunk: %w", err)
			}
			nAtime := node.Atime
			if nAtime.IsZero() {
				nAtime = now
			}
			nodeInodeMsg := &pb.Inode{
				Ino:           proto.Uint64(node.ID),
				Mode:          node.Mode,
				Size:          node.Size,
				Mtime:         timestamppb.New(now),
				Atime:         timestamppb.New(nAtime),
				Ctime:         timestamppb.New(now),
				Uid:           node.Uid,
				Gid:           node.Gid,
				IsDir:         false,
				ContentSha256: node.ContentSha256,
				ChunkSize:     node.ChunkSize,
			}
			if _, err := tx.Update(ctx, nodeInodeMsg); err != nil {
				return 0, 0, time.Time{}, nil, fmt.Errorf("failed to log node inode update: %w", err)
			}

			commitSeq, err := tx.Commit(ctx)
			if err != nil {
				return 0, 0, time.Time{}, nil, fmt.Errorf("failed to commit tiny write transaction: %w", err)
			}
			if err := v.applyTxChangesLocked(ctx, tx); err != nil {
				return 0, 0, time.Time{}, nil, err
			}
			waitFn := v.makeWaitFn(commitSeq, reqLevel)

			attr := &pb.EntryAttr{
				Inode:         node.ID,
				IsDir:         false,
				Size:          node.Size,
				Mode:          node.Mode,
				ModTime:       timestamppb.New(now),
				Atime:         timestamppb.New(nAtime),
				Ctime:         timestamppb.New(now),
				ContentSha256: node.ContentSha256,
				Uid:           node.Uid,
				Gid:           node.Gid,
			}
			v.broadcaster.Broadcast(v.volumeID, &pb.WatchVolumeResponse{
				EventType: pb.WatchEventType_EVENT_MODIFIED,
				Attr:      attr,
				Inode:     node.ID,
			})
			v.checkAutoSnapshotTriggerLocked(ctx)
			return int64(len(data)), node.Size, now, waitFn, nil
		}

		// Chunked file write
		if len(node.InlineData) > 0 {
			if node.StagedChunks == nil {
				node.StagedChunks = make(map[int][]byte)
			}
			node.StagedChunks[0] = node.InlineData
			cSha := fmt.Sprintf("%x", sha256.Sum256(node.InlineData))
			node.Chunks = map[uint32]string{0: cSha}
			node.InlineData = nil
		}
		if node.Chunks == nil {
			node.Chunks = make(map[uint32]string)
		}
		if node.StagedChunks == nil {
			node.StagedChunks = make(map[int][]byte)
		}
		_ = v.ensureInodeChunksLoadedLocked(ctx, node)

		cs := int64(effectiveChunkSize)
		node.ChunkSize = uint32(effectiveChunkSize)
		startChunk := int(offset / cs)
		endChunk := int((offset + int64(len(data)) - 1) / cs)

		touchedIndices := make([]int, 0, endChunk-startChunk+1)
		chunkBlobs := make(map[int][]byte)
		chunkShas := make(map[int]string)

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
			node.Chunks[uint32(i)] = chunkSha
			node.StagedChunks[i] = chunkData

			touchedIndices = append(touchedIndices, i)
			cCopy := make([]byte, len(chunkData))
			copy(cCopy, chunkData)
			chunkBlobs[i] = cCopy
			chunkShas[i] = chunkSha
		}

		node.Size = calculatedSize
		node.ContentSha256 = ""

		done := v.registerPendingUpload(node.ID)
		ino := node.ID
		mode := node.Mode

		go func() {
			var uploadErr error
			defer func() { done(uploadErr) }()

			if v.blobStore != nil && len(chunkBlobs) > 0 {
				blobsMap := make(map[string]blob.ByteStream, len(chunkBlobs))
				for idx, cBytes := range chunkBlobs {
					blobsMap[chunkShas[idx]] = blob.NewByteStreamFromBytes(cBytes)
				}
				if err := v.blobStore.PutBlobs(context.Background(), blobsMap); err != nil {
					uploadErr = err
					return
				}
			}

			v.mu.Lock()
			defer v.mu.Unlock()

			currNode, _ := v.getOrLoadInodeLocked(context.Background(), ino)
			if currNode == nil {
				return
			}
			if currNode.Size < calculatedSize {
				currNode.Size = calculatedSize
			}
			if currNode.ChunkSize == 0 {
				currNode.ChunkSize = uint32(effectiveChunkSize)
			}

			tx := v.metadataStream.Begin()
			for _, idx := range touchedIndices {
				cSize := int64(currNode.ChunkSize)
				if cSize == 0 {
					cSize = int64(effectiveChunkSize)
				}
				if int64(idx)*cSize < currNode.Size {
					if _, err := tx.Insert(context.Background(), &pb.FileChunk{
						Ino:    proto.Uint64(ino),
						Index:  proto.Uint32(uint32(idx)),
						Sha256: chunkShas[idx],
					}); err != nil {
						uploadErr = err
						return
					}
				}
			}

			curAtime := currNode.Atime
			if curAtime.IsZero() {
				curAtime = currNode.ModTime
			}
			curCtime := currNode.Ctime
			if curCtime.IsZero() {
				curCtime = currNode.ModTime
			}
			nodeInodeMsg := &pb.Inode{
				Ino:       proto.Uint64(ino),
				Mode:      mode,
				Size:      currNode.Size,
				Mtime:     timestamppb.New(currNode.ModTime),
				Atime:     timestamppb.New(curAtime),
				Ctime:     timestamppb.New(curCtime),
				Uid:       currNode.Uid,
				Gid:       currNode.Gid,
				IsDir:     false,
				ChunkSize: currNode.ChunkSize,
			}
			if _, err := tx.Update(context.Background(), nodeInodeMsg); err != nil {
				uploadErr = err
				return
			}

			if _, err := tx.Commit(context.Background()); err != nil {
				uploadErr = err
				return
			}
			_ = v.applyTxChangesLocked(context.Background(), tx)
		}()

		nAtime := node.Atime
		if nAtime.IsZero() {
			nAtime = now
		}
		attr := &pb.EntryAttr{
			Inode:   node.ID,
			IsDir:   false,
			Size:    node.Size,
			Mode:    node.Mode,
			ModTime: timestamppb.New(now),
			Atime:   timestamppb.New(nAtime),
			Ctime:   timestamppb.New(now),
			Uid:     node.Uid,
			Gid:     node.Gid,
		}
		v.broadcaster.Broadcast(v.volumeID, &pb.WatchVolumeResponse{
			EventType: pb.WatchEventType_EVENT_MODIFIED,
			Attr:      attr,
			Inode:     node.ID,
		})

		v.checkAutoSnapshotTriggerLocked(ctx)
		return int64(len(data)), node.Size, now, nil, nil
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
	v.mu.RLock()
	inodeID = v.normalizeInodeID(inodeID)
	v.mu.RUnlock()

	if err := v.waitForInodeUploads(ctx, inodeID); err != nil {
		return nil, err
	}

	attr, waitFn, err := func() (*pb.EntryAttr, func(context.Context) error, error) {
		v.mu.Lock()
		defer v.mu.Unlock()

		if err := v.checkBackpressureLocked(ctx); err != nil {
			return nil, nil, err
		}

		inodeID = v.normalizeInodeID(inodeID)

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
		node.Ctime = now
		node.IsDirty = true
		oldChunks := node.Chunks

		tx := v.metadataStream.Begin()

		if size == 0 {
			node.Size = 0
			node.Chunks = nil
			node.StagedChunks = nil
			node.DirtyChunks = nil
			node.InlineData = nil
			node.ManifestSha256 = ""
			node.ContentSha256 = fmt.Sprintf("%x", sha256.Sum256([]byte{}))
			node.Sha256 = node.ContentSha256
			if node.Data != nil {
				_ = node.Data.Close()
				node.Data = nil
			}

			for i := range oldChunks {
				if _, err := tx.Delete(ctx, &pb.FileChunk{Ino: proto.Uint64(node.ID), Index: proto.Uint32(i)}); err != nil {
					return nil, nil, fmt.Errorf("failed to delete FileChunk row: %w", err)
				}
			}
			if _, err := tx.Delete(ctx, &pb.FileChunk{Ino: proto.Uint64(node.ID), Index: proto.Uint32(0)}); err != nil {
				// delete chunk 0 inline if existed
			}
		} else if size <= v.maxInlineLen && len(oldChunks) <= 1 {
			chunk0, _ := v.readChunkLocked(ctx, node, 0)
			if int64(len(chunk0)) > size {
				chunk0 = chunk0[:size]
			} else if int64(len(chunk0)) < size {
				newBuf := make([]byte, size)
				copy(newBuf, chunk0)
				chunk0 = newBuf
			}
			node.InlineData = chunk0
			node.Size = size
			node.Chunks = nil
			node.StagedChunks = nil
			node.DirtyChunks = nil
			node.ContentSha256 = ""

			for i := range oldChunks {
				if i != 0 {
					if _, err := tx.Delete(ctx, &pb.FileChunk{Ino: proto.Uint64(node.ID), Index: proto.Uint32(i)}); err != nil {
						return nil, nil, fmt.Errorf("failed to delete FileChunk row: %w", err)
					}
				}
			}
			if _, err := tx.Insert(ctx, &pb.FileChunk{Ino: proto.Uint64(node.ID), Index: proto.Uint32(0), InlineData: node.InlineData}); err != nil {
				return nil, nil, fmt.Errorf("failed to update inline FileChunk: %w", err)
			}
		} else {
			cs := int64(node.ChunkSize)
			if cs == 0 {
				cs = int64(v.chunkSize)
				if cs == 0 {
					cs = 64 * 1024
				}
				node.ChunkSize = uint32(cs)
			}
			_ = v.ensureInodeChunksLoadedLocked(ctx, node)

			if node.Chunks == nil {
				node.Chunks = make(map[uint32]string)
			}

			newNumChunks := int((size + cs - 1) / cs)
			// Remove staged and logged chunks beyond newNumChunks
			for k := range node.StagedChunks {
				if k >= newNumChunks {
					delete(node.StagedChunks, k)
				}
			}
			for i := range node.Chunks {
				if int(i) >= newNumChunks {
					delete(node.Chunks, i)
					if _, err := tx.Delete(ctx, &pb.FileChunk{Ino: proto.Uint64(node.ID), Index: proto.Uint32(i)}); err != nil {
						return nil, nil, fmt.Errorf("failed to delete FileChunk row: %w", err)
					}
				}
			}

			if newNumChunks > 0 && size < node.Size {
				lastIdx := newNumChunks - 1
				lastChunkLen := size - int64(lastIdx)*cs
				if chunkData, ok := node.StagedChunks[lastIdx]; ok && int64(len(chunkData)) > lastChunkLen {
					chunkData = chunkData[:lastChunkLen]
					chunkSha := fmt.Sprintf("%x", sha256.Sum256(chunkData))
					node.Chunks[uint32(lastIdx)] = chunkSha
					node.StagedChunks[lastIdx] = chunkData
					if v.blobStore != nil {
						_ = v.blobStore.PutBlobs(ctx, map[string]blob.ByteStream{
							chunkSha: blob.NewByteStreamFromBytes(chunkData),
						})
					}
					if _, err := tx.Insert(ctx, &pb.FileChunk{
						Ino:    proto.Uint64(node.ID),
						Index:  proto.Uint32(uint32(lastIdx)),
						Sha256: chunkSha,
					}); err != nil {
						return nil, nil, fmt.Errorf("failed to log truncated chunk: %w", err)
					}
				} else if _, exists := node.Chunks[uint32(lastIdx)]; exists {
					chunkData, _ := v.readChunkLocked(ctx, node, lastIdx)
					if int64(len(chunkData)) > lastChunkLen {
						chunkData = chunkData[:lastChunkLen]
						chunkSha := fmt.Sprintf("%x", sha256.Sum256(chunkData))
						node.Chunks[uint32(lastIdx)] = chunkSha
						if node.StagedChunks == nil {
							node.StagedChunks = make(map[int][]byte)
						}
						node.StagedChunks[lastIdx] = chunkData
						if v.blobStore != nil {
							_ = v.blobStore.PutBlobs(ctx, map[string]blob.ByteStream{
								chunkSha: blob.NewByteStreamFromBytes(chunkData),
							})
						}
						if _, err := tx.Insert(ctx, &pb.FileChunk{
							Ino:    proto.Uint64(node.ID),
							Index:  proto.Uint32(uint32(lastIdx)),
							Sha256: chunkSha,
						}); err != nil {
							return nil, nil, fmt.Errorf("failed to log truncated chunk: %w", err)
						}
					}
				}
			}

			node.Size = size
			node.ContentSha256 = ""
		}

		nAtime := node.Atime
		if nAtime.IsZero() {
			nAtime = now
		}
		childInodeMsg := &pb.Inode{
			Ino:       proto.Uint64(node.ID),
			Mode:      node.Mode,
			Size:      node.Size,
			Mtime:     timestamppb.New(now),
			Atime:     timestamppb.New(nAtime),
			Ctime:     timestamppb.New(now),
			Uid:       node.Uid,
			Gid:       node.Gid,
			IsDir:     false,
			ChunkSize: node.ChunkSize,
		}
		if _, err := tx.Update(ctx, childInodeMsg); err != nil {
			return nil, nil, fmt.Errorf("failed to log node inode update: %w", err)
		}

		commitSeq, err := tx.Commit(ctx)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to commit truncate file transaction: %w", err)
		}
		if err := v.applyTxChangesLocked(ctx, tx); err != nil {
			return nil, nil, err
		}

		waitFn := v.makeWaitFn(commitSeq, nil)

		attr := &pb.EntryAttr{
			Inode:   node.ID,
			IsDir:   false,
			Size:    node.Size,
			Mode:    node.Mode,
			ModTime: timestamppb.New(now),
			Atime:   timestamppb.New(nAtime),
			Ctime:   timestamppb.New(now),
			Uid:     node.Uid,
			Gid:     node.Gid,
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
	if len(name) > MaxNameLength {
		return fmt.Errorf("file name %q exceeds maximum length: %w", name, syscall.ENAMETOOLONG)
	}

	v.mu.Lock()
	parentInodeID = v.normalizeInodeID(parentInodeID)

	var childInodeID uint64
	var oldChunks map[uint32]string
	if v.metadataStore == MetadataStoreSQLite && v.sqliteDB != nil {
		de, ok, err := v.getDirEntrySQLiteLocked(ctx, parentInodeID, name)
		if err != nil {
			v.mu.Unlock()
			return err
		}
		if !ok {
			v.mu.Unlock()
			return fmt.Errorf("file %s not found under inode %d: %w", name, parentInodeID, syscall.ENOENT)
		}
		childInode, err := v.getOrLoadInodeLocked(ctx, de.GetIno())
		if err != nil {
			v.mu.Unlock()
			return err
		}
		if childInode.IsDir {
			v.mu.Unlock()
			return fmt.Errorf("cannot unlink directory %s: %w", name, syscall.EISDIR)
		}
		childInodeID = childInode.ID
		oldChunks = childInode.Chunks
	} else {
		parentDir, err := v.getOrLoadDirLocked(ctx, parentInodeID)
		if err != nil {
			v.mu.Unlock()
			return err
		}
		entry, ok := parentDir.Entries[name]
		if !ok {
			v.mu.Unlock()
			return fmt.Errorf("file %s not found under inode %d: %w", name, parentInodeID, syscall.ENOENT)
		}
		childInode, err := v.getOrLoadInodeLocked(ctx, entry.InodeID)
		if err != nil {
			v.mu.Unlock()
			return err
		}
		if childInode.IsDir {
			v.mu.Unlock()
			return fmt.Errorf("cannot unlink directory %s: %w", name, syscall.EISDIR)
		}
		childInodeID = childInode.ID
		oldChunks = childInode.Chunks
	}
	v.mu.Unlock()

	if err := v.waitForInodeUploads(ctx, childInodeID); err != nil {
		return err
	}

	waitFn, err := func() (func(context.Context) error, error) {
		v.mu.Lock()
		defer v.mu.Unlock()

		if err := v.checkBackpressureLocked(ctx); err != nil {
			return nil, err
		}

		childInode, err := v.getOrLoadInodeLocked(ctx, childInodeID)
		if err != nil {
			return nil, err
		}

		if childInode.Data != nil {
			_ = childInode.Data.Close()
		}

		if v.metadataStore != MetadataStoreSQLite {
			parentDir, err := v.getOrLoadDirLocked(ctx, parentInodeID)
			if err != nil {
				return nil, err
			}
			delete(parentDir.Entries, name)
			delete(parentDir.Added, name)
			parentDir.Deleted[name] = true
			parentDir.IsDirty = true
		}

		now := time.Now()
		parentInode, _ := v.getOrLoadInodeLocked(ctx, parentInodeID)
		if parentInode != nil {
			parentInode.ModTime = now
			parentInode.Ctime = now
			parentInode.IsDirty = true
			if v.inodeCache != nil {
				v.inodeCache.Put(parentInode.ID, parentInode)
			}
		}

		var commitSeq uint64
		tx := v.metadataStream.Begin()
		if _, err := tx.Delete(ctx, &pb.DirEntry{ParentIno: proto.Uint64(parentInodeID), Name: proto.String(name)}); err != nil {
			return nil, fmt.Errorf("failed to log dir entry deletion: %w", err)
		}
		if v.metadataStore == MetadataStoreSQLite && v.sqliteDB != nil {
			prefixBytes, _ := sds.EncodeKeyPrefix(&pb.FileChunk{Ino: proto.Uint64(childInodeID)}, 1)
			chunkMsgs, _ := v.scanSQLiteRowsLocked(ctx, "objectfs.v1alpha1.FileChunk", prefixBytes)
			for _, msg := range chunkMsgs {
				c := msg.(*pb.FileChunk)
				if _, err := tx.Delete(ctx, &pb.FileChunk{Ino: proto.Uint64(childInodeID), Index: proto.Uint32(c.GetIndex())}); err != nil {
					return nil, fmt.Errorf("failed to log FileChunk deletion: %w", err)
				}
			}
		} else {
			for i := range oldChunks {
				if _, err := tx.Delete(ctx, &pb.FileChunk{Ino: proto.Uint64(childInodeID), Index: proto.Uint32(i)}); err != nil {
					return nil, fmt.Errorf("failed to log FileChunk deletion: %w", err)
				}
			}
			if _, err := tx.Delete(ctx, &pb.FileChunk{Ino: proto.Uint64(childInodeID), Index: proto.Uint32(0)}); err != nil {
				// delete inline chunk 0 if existed
			}
		}
		if _, err := tx.Delete(ctx, &pb.Inode{Ino: proto.Uint64(childInodeID)}); err != nil {
			return nil, fmt.Errorf("failed to log inode deletion: %w", err)
		}
		if parentInode != nil {
			pAtime := parentInode.Atime
			if pAtime.IsZero() {
				pAtime = parentInode.ModTime
			}
			parentInodeMsg := &pb.Inode{
				Ino:            proto.Uint64(parentInode.ID),
				Mode:           parentInode.Mode,
				Size:           parentInode.Size,
				Mtime:          timestamppb.New(now),
				Atime:          timestamppb.New(pAtime),
				Ctime:          timestamppb.New(now),
				Uid:            parentInode.Uid,
				Gid:            parentInode.Gid,
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
		if err := v.applyTxChangesLocked(ctx, tx); err != nil {
			return nil, err
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
	if len(name) > MaxNameLength {
		return fmt.Errorf("directory name %q exceeds maximum length: %w", name, syscall.ENAMETOOLONG)
	}

	waitFn, err := func() (func(context.Context) error, error) {
		v.mu.Lock()
		defer v.mu.Unlock()

		if err := v.checkBackpressureLocked(ctx); err != nil {
			return nil, err
		}

		parentInodeID = v.normalizeInodeID(parentInodeID)

		var childInodeID uint64
		if v.metadataStore == MetadataStoreSQLite && v.sqliteDB != nil {
			de, ok, err := v.getDirEntrySQLiteLocked(ctx, parentInodeID, name)
			if err != nil {
				return nil, err
			}
			if !ok {
				return nil, fmt.Errorf("directory %s not found under inode %d: %w", name, parentInodeID, syscall.ENOENT)
			}
			childInodeID = de.GetIno()
			childInode, err := v.getOrLoadInodeLocked(ctx, childInodeID)
			if err != nil {
				return nil, err
			}
			if !childInode.IsDir {
				return nil, fmt.Errorf("cannot rmdir non-directory %s: %w", name, syscall.ENOTDIR)
			}

			// Emptiness check: scan child directory with limit 1
			prefixBytes, pErr := sds.EncodeKeyPrefix(&pb.DirEntry{ParentIno: proto.Uint64(childInodeID)}, 1)
			if pErr != nil {
				return nil, pErr
			}
			subEntries, err := v.scanLimitSQLiteRowsLocked(ctx, "objectfs.v1alpha1.DirEntry", prefixBytes, 1)
			if err != nil {
				return nil, err
			}
			if len(subEntries) > 0 {
				return nil, fmt.Errorf("directory %s not empty: %w", name, syscall.ENOTEMPTY)
			}
		} else {
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
			childInodeID = childInode.ID
		}

		delete(v.dirParents, childInodeID)

		now := time.Now()
		parentInode, _ := v.getOrLoadInodeLocked(ctx, parentInodeID)
		if parentInode != nil {
			parentInode.ModTime = now
			parentInode.Ctime = now
			parentInode.IsDirty = true
			if v.inodeCache != nil {
				v.inodeCache.Put(parentInode.ID, parentInode)
			}
		}

		var commitSeq uint64
		var err error
		tx := v.metadataStream.Begin()
		if _, err = tx.Delete(ctx, &pb.DirEntry{ParentIno: proto.Uint64(parentInodeID), Name: proto.String(name)}); err != nil {
			return nil, fmt.Errorf("failed to log dir entry deletion: %w", err)
		}
		if _, err = tx.Delete(ctx, &pb.Inode{Ino: proto.Uint64(childInodeID)}); err != nil {
			return nil, fmt.Errorf("failed to log inode deletion: %w", err)
		}
		if parentInode != nil {
			pAtime := parentInode.Atime
			if pAtime.IsZero() {
				pAtime = parentInode.ModTime
			}
			parentInodeMsg := &pb.Inode{
				Ino:            proto.Uint64(parentInode.ID),
				Mode:           parentInode.Mode,
				Size:           parentInode.Size,
				Mtime:          timestamppb.New(now),
				Atime:          timestamppb.New(pAtime),
				Ctime:          timestamppb.New(now),
				Uid:            parentInode.Uid,
				Gid:            parentInode.Gid,
				IsDir:          true,
				Sha256:         parentInode.Sha256,
				Etag:           parentInode.ETag,
				ManifestSha256: parentInode.ManifestSha256,
				ContentSha256:  parentInode.ContentSha256,
				ChunkSize:      parentInode.ChunkSize,
			}
			if _, err = tx.Update(ctx, parentInodeMsg); err != nil {
				return nil, fmt.Errorf("failed to log parent inode update: %w", err)
			}
		}
		commitSeq, err = tx.Commit(ctx)
		if err != nil {
			return nil, fmt.Errorf("failed to commit transaction: %w", err)
		}
		if err = v.applyTxChangesLocked(ctx, tx); err != nil {
			return nil, err
		}

		waitFn := v.makeWaitFn(commitSeq, nil)

		v.broadcaster.Broadcast(v.volumeID, &pb.WatchVolumeResponse{
			EventType:   pb.WatchEventType_EVENT_DELETED,
			Inode:       childInodeID,
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
	if len(oldName) > MaxNameLength || len(newName) > MaxNameLength {
		return nil, fmt.Errorf("name exceeds maximum length: %w", syscall.ENAMETOOLONG)
	}

	attr, waitFn, err := func() (*pb.EntryAttr, func(context.Context) error, error) {
		v.mu.Lock()
		defer v.mu.Unlock()

		if err := v.checkBackpressureLocked(ctx); err != nil {
			return nil, nil, err
		}

		oldParentInodeID = v.normalizeInodeID(oldParentInodeID)
		newParentInodeID = v.normalizeInodeID(newParentInodeID)

		if oldName == "" || oldName == "." || oldName == ".." || newName == "" || newName == "." || newName == ".." {
			return nil, nil, fmt.Errorf("invalid name for rename: %w", syscall.EINVAL)
		}

		var entry InodeEntry
		var oldParentDir, newParentDir *CachedDir
		var targetExists bool
		var targetInodeID uint64

		if v.metadataStore == MetadataStoreSQLite && v.sqliteDB != nil {
			oldDe, ok, err := v.getDirEntrySQLiteLocked(ctx, oldParentInodeID, oldName)
			if err != nil {
				return nil, nil, err
			}
			if !ok {
				return nil, nil, fmt.Errorf("source %s not found in parent %d: %w", oldName, oldParentInodeID, syscall.ENOENT)
			}
			entry = InodeEntry{
				InodeID: oldDe.GetIno(),
				IsDir:   oldDe.GetIsDir(),
				Mode:    oldDe.GetMode(),
			}

			newParentInode, err := v.getOrLoadInodeLocked(ctx, newParentInodeID)
			if err != nil {
				return nil, nil, err
			}
			if !newParentInode.IsDir {
				return nil, nil, fmt.Errorf("target parent %d is not a directory: %w", newParentInodeID, syscall.ENOTDIR)
			}

			targetDe, ok, err := v.getDirEntrySQLiteLocked(ctx, newParentInodeID, newName)
			if err != nil {
				return nil, nil, err
			}
			if ok {
				targetExists = true
				targetInodeID = targetDe.GetIno()
			}
		} else {
			var err error
			oldParentDir, err = v.getOrLoadDirLocked(ctx, oldParentInodeID)
			if err != nil {
				return nil, nil, fmt.Errorf("old parent directory not loaded: %w", err)
			}

			e, ok := oldParentDir.Entries[oldName]
			if !ok {
				return nil, nil, fmt.Errorf("source %s not found in parent %d: %w", oldName, oldParentInodeID, syscall.ENOENT)
			}
			entry = InodeEntry{
				InodeID: e.InodeID,
				IsDir:   e.IsDir,
				Mode:    e.Mode,
			}

			newParentInode, err := v.getOrLoadInodeLocked(ctx, newParentInodeID)
			if err != nil {
				return nil, nil, err
			}
			if !newParentInode.IsDir {
				return nil, nil, fmt.Errorf("target parent %d is not a directory: %w", newParentInodeID, syscall.ENOTDIR)
			}

			newParentDir, err = v.getOrLoadDirLocked(ctx, newParentInodeID)
			if err != nil {
				return nil, nil, err
			}

			if targetEntry, ok := newParentDir.Entries[newName]; ok {
				targetExists = true
				targetInodeID = targetEntry.InodeID
			}
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

		if targetExists {
			targetInode, _ := v.getOrLoadInodeLocked(ctx, targetInodeID)
			if targetInode != nil && targetInode.Data != nil {
				_ = targetInode.Data.Close()
			}
		}

		if oldParentDir != nil && newParentDir != nil {
			delete(oldParentDir.Entries, oldName)
			delete(oldParentDir.Added, oldName)
			oldParentDir.Deleted[oldName] = true
			oldParentDir.IsDirty = true

			newParentDir.Entries[newName] = DirEntry{
				Name:    newName,
				InodeID: entry.InodeID,
				IsDir:   entry.IsDir,
				Mode:    entry.Mode,
			}
			newParentDir.Added[newName] = true
			delete(newParentDir.Deleted, newName)
			newParentDir.IsDirty = true
		}

		now := time.Now()
		childInode.Ctime = now
		childInode.IsDirty = true

		oldParentInode, _ := v.getOrLoadInodeLocked(ctx, oldParentInodeID)
		if oldParentInode != nil {
			oldParentInode.ModTime = now
			oldParentInode.Ctime = now
			oldParentInode.IsDirty = true
			if v.inodeCache != nil {
				v.inodeCache.Put(oldParentInode.ID, oldParentInode)
			}
		}
		newParentInode, _ := v.getOrLoadInodeLocked(ctx, newParentInodeID)
		if newParentInode != nil {
			newParentInode.ModTime = now
			newParentInode.Ctime = now
			newParentInode.IsDirty = true
			if v.inodeCache != nil {
				v.inodeCache.Put(newParentInode.ID, newParentInode)
			}
		}

		var commitSeq uint64
		tx := v.metadataStream.Begin()
		if _, err := tx.Delete(ctx, &pb.DirEntry{ParentIno: proto.Uint64(oldParentInodeID), Name: proto.String(oldName)}); err != nil {
			return nil, nil, fmt.Errorf("failed to log old dir entry deletion: %w", err)
		}
		if targetExists {
			if _, err := tx.Delete(ctx, &pb.DirEntry{ParentIno: proto.Uint64(newParentInodeID), Name: proto.String(newName)}); err != nil {
				return nil, nil, fmt.Errorf("failed to log target dir entry deletion: %w", err)
			}
			if _, err := tx.Delete(ctx, &pb.Inode{Ino: proto.Uint64(targetInodeID)}); err != nil {
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
			cAtime := childInode.Atime
			if cAtime.IsZero() {
				cAtime = childInode.ModTime
			}
			childInodeMsg := &pb.Inode{
				Ino:            proto.Uint64(childInode.ID),
				Mode:           childInode.Mode,
				Size:           childInode.Size,
				Mtime:          timestamppb.New(childInode.ModTime),
				Atime:          timestamppb.New(cAtime),
				Ctime:          timestamppb.New(now),
				Uid:            childInode.Uid,
				Gid:            childInode.Gid,
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
			oldAtime := oldParentInode.Atime
			if oldAtime.IsZero() {
				oldAtime = oldParentInode.ModTime
			}
			oldParentInodeMsg := &pb.Inode{
				Ino:            proto.Uint64(oldParentInode.ID),
				Mode:           oldParentInode.Mode,
				Size:           oldParentInode.Size,
				Mtime:          timestamppb.New(now),
				Atime:          timestamppb.New(oldAtime),
				Ctime:          timestamppb.New(now),
				Uid:            oldParentInode.Uid,
				Gid:            oldParentInode.Gid,
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
			newAtime := newParentInode.Atime
			if newAtime.IsZero() {
				newAtime = newParentInode.ModTime
			}
			newParentInodeMsg := &pb.Inode{
				Ino:            proto.Uint64(newParentInode.ID),
				Mode:           newParentInode.Mode,
				Size:           newParentInode.Size,
				Mtime:          timestamppb.New(now),
				Atime:          timestamppb.New(newAtime),
				Ctime:          timestamppb.New(now),
				Uid:            newParentInode.Uid,
				Gid:            newParentInode.Gid,
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
		if err := v.applyTxChangesLocked(ctx, tx); err != nil {
			return nil, nil, err
		}

		waitFn := v.makeWaitFn(commitSeq, nil)

		cAtime := childInode.Atime
		if cAtime.IsZero() {
			cAtime = childInode.ModTime
		}
		attr := &pb.EntryAttr{
			Inode:   entry.InodeID,
			Name:    newName,
			IsDir:   entry.IsDir,
			Size:    childInode.Size,
			Mode:    childInode.Mode,
			ModTime: timestamppb.New(childInode.ModTime),
			Atime:   timestamppb.New(cAtime),
			Ctime:   timestamppb.New(now),
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
	isRoot := (inodeID == v.rootInodeID)
	v.mu.RUnlock()
	if err != nil {
		return err
	}

	if isRoot {
		if err := v.waitForVolumeUploads(ctx); err != nil {
			return err
		}
	} else {
		if err := v.waitForInodeUploads(ctx, inodeID); err != nil {
			return err
		}
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
	vol        *Volume
	rootXattrs *erofs.Xattrs
}

func (r *snapshotResolver) getOrLoadInode(ctx context.Context, inodeID uint64) (*CachedInode, error) {
	if r.vol != nil && r.vol.metadataStore == MetadataStoreSQLite {
		return r.vol.getOrLoadInodeLocked(ctx, inodeID)
	}
	if r.vol != nil && r.vol.inodeCache != nil {
		if node, ok := r.vol.inodeCache.Peek(inodeID); ok {
			return node, nil
		}
	}
	return r.resolveInode(ctx, inodeID, false)
}

func (r *snapshotResolver) getOrLoadDir(ctx context.Context, inodeID uint64) (map[string]DirEntry, error) {
	if r.vol != nil && r.vol.metadataStore == MetadataStoreSQLite {
		dir, err := r.vol.getOrLoadDirLocked(ctx, inodeID)
		if err != nil {
			return nil, err
		}
		return dir.Entries, nil
	}
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
		Atime:   dirInode.Atime,
		Ctime:   dirInode.Ctime,
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

			if childInode.ChunkSize > 0 && len(childInode.Chunks) > 0 {
				_ = r.vol.ensureInodeChunksLoadedLocked(ctx, childInode)
				cs := int64(childInode.ChunkSize)
				numChunks := int((childInode.Size + cs - 1) / cs)
				manifestChunks := make([][32]byte, numChunks)
				for i := 0; i < numChunks; i++ {
					if c, ok := childInode.Chunks[uint32(i)]; ok && c != "" {
						raw, _ := hex.DecodeString(c)
						if len(raw) == 32 {
							copy(manifestChunks[i][:], raw)
						}
					}
				}
				manifest := &blob.Manifest{
					ChunkSize:   childInode.ChunkSize,
					TotalLength: uint64(childInode.Size),
					Chunks:      manifestChunks,
				}
				manifestBlob, err := blob.EncodeManifest(manifest)
				if err == nil {
					childInode.ManifestSha256 = manifestBlob.SHA256Hex()
					dirtyBlobs[manifestBlob.SHA256Hex()] = manifestBlob.Stream
				}
			}

			if childInode.ContentSha256 == "" && childInode.Size > 0 {
				hasher := sha256.New()
				if childInode.ManifestSha256 != "" || childInode.ChunkSize > 0 {
					_ = r.vol.ensureInodeChunksLoadedLocked(ctx, childInode)
					cs := int64(childInode.ChunkSize)
					if cs == 0 {
						cs = 64 * 1024
					}
					numChunks := int((childInode.Size + cs - 1) / cs)
					for i := 0; i < numChunks; i++ {
						chunkBytes, err := r.vol.readChunkLocked(ctx, childInode, i)
						if err == nil && len(chunkBytes) > 0 {
							hasher.Write(chunkBytes)
						} else {
							chunkLen := cs
							if int64(i+1)*cs > childInode.Size {
								chunkLen = childInode.Size - int64(i)*cs
							}
							if chunkLen > 0 {
								hasher.Write(make([]byte, chunkLen))
							}
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

			// Collect dirty / unpersisted blobs for flush
			if childInode.Sha256 != "" {
				if cData, ok := r.vol.recoveredContent[childInode.Sha256]; ok {
					dirtyBlobs[childInode.Sha256] = blob.NewByteStreamFromBytes(cData)
				} else if childInode.Data != nil {
					_ = childInode.Data.Rewind()
					dataBytes, _ := io.ReadAll(childInode.Data)
					_ = childInode.Data.Rewind()
					dirtyBlobs[childInode.Sha256] = blob.NewByteStreamFromBytes(dataBytes)
				}
			}
			if childInode.ManifestSha256 != "" {
				if mData, ok := r.vol.recoveredContent[childInode.ManifestSha256]; ok {
					dirtyBlobs[childInode.ManifestSha256] = blob.NewByteStreamFromBytes(mData)
				}
			}
			for _, chunkSha := range childInode.Chunks {
				if chunkData, ok := r.vol.recoveredContent[chunkSha]; ok {
					dirtyBlobs[chunkSha] = blob.NewByteStreamFromBytes(chunkData)
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
				Atime:          childInode.Atime,
				Ctime:          childInode.Ctime,
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
	var dirOpts []erofs.MemoryNodeOption
	dirOpts = append(dirOpts,
		erofs.WithIno(dirInode.ID),
		erofs.WithMtime(uint64(dirInode.ModTime.Unix())),
		erofs.WithUID(dirInode.Uid),
		erofs.WithGID(dirInode.Gid),
	)
	if r.rootXattrs != nil && currentPath == "/" {
		dirOpts = append(dirOpts, erofs.WithXattrs(*r.rootXattrs))
	}
	return erofs.NewMemoryNode(
		dirNodeName,
		true,
		uint16(dirInode.Mode),
		nil,
		children,
		dirOpts...,
	), nil
}

func (v *Volume) flushToBackendLocked(ctx context.Context) error {
	if v.backend == nil {
		return nil
	}

	if v.stream != nil && v.stream.ReplicationLevel() == walclient.Permanent {
		if err := v.stream.Flush(ctx); err != nil {
			return fmt.Errorf("failed to flush WAL stream: %w", err)
		}
	}

	snapPos := v.safeSnapshotPositionLocked()

	if v.metadataStore == MetadataStoreSQLite && v.sqliteDB != nil && v.backend != nil {
		if err := v.flushOverlayLocked(ctx); err != nil {
			return err
		}
		snapKey, actualPos, err := sqlite.PublishSnapshot(ctx, v.sqliteDB, v.backend, os.TempDir())
		if err == nil && v.metadataStream != nil {
			ptr := &sdsv1.SnapshotPointer{
				Position: actualPos,
				Format:   "sqlite",
				Location: snapKey,
			}
			_, _ = v.metadataStream.AppendSnapshotPointer(ctx, ptr)
		}
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

	var regBytes []byte
	if v.metadataStream != nil {
		regProto := v.metadataStream.Registry().Export()
		var err error
		regBytes, err = proto.Marshal(regProto)
		if err != nil {
			return fmt.Errorf("failed to marshal registry: %w", err)
		}
	}
	snapTime := time.Now().UTC()
	rootXattrs := erofs.Xattrs{
		Others: map[string]string{
			"trusted.sds.position":      strconv.FormatUint(snapPos, 10),
			"trusted.sds.registry":      string(regBytes),
			"trusted.sds.snapshot_time": snapTime.Format(time.RFC3339Nano),
		},
	}

	// Release v.mu during snapshot compilation and cloud storage upload to avoid stopping the world
	v.mu.Unlock()

	var erofsBuf bufferWriterAt
	var currentEntries map[string]FileMetadata
	snapshotName := fmt.Sprintf("%020d.erofs", snapPos)
	snapshotKey := path.Join("volumes", v.volumeID, "meta", snapshotName)

	snapErr := func() error {
		for _, delPath := range delPaths {
			key := strings.TrimPrefix(delPath, "/")
			_ = v.backend.DeleteObject(ctx, v.volumeID, key)
		}

		currentEntries = make(map[string]FileMetadata)
		dirtyBlobs := make(map[string]blob.ByteStream)

		resolver := &snapshotResolver{
			vol:        v,
			rootXattrs: &rootXattrs,
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

		erofsStream := blob.NewByteStreamFromBytes(erofsBuf.buf)
		if _, err := v.backend.PutObject(ctx, "", snapshotKey, erofsStream); err != nil {
			_ = erofsStream.Close()
			return fmt.Errorf("failed to save EROFS snapshot %s: %w", snapshotKey, err)
		}
		_ = erofsStream.Close()

		if v.metadataStream != nil {
			regFP := sha256.Sum256(regBytes)
			ptr := &sdsv1.SnapshotPointer{
				Position:            snapPos,
				Format:              "erofs",
				Location:            snapshotKey,
				RegistryFingerprint: regFP[:],
			}
			if _, err := v.metadataStream.AppendSnapshotPointer(ctx, ptr); err != nil {
				return fmt.Errorf("failed to append SnapshotPointer: %w", err)
			}
		}

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
	v.recoveredContent = make(map[string][]byte)

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
	_ = v.waitForVolumeUploads(ctx)

	v.snapshotMu.Lock()
	defer v.snapshotMu.Unlock()

	v.mu.Lock()
	defer v.mu.Unlock()
	return v.flushToBackendLocked(ctx)
}

func (v *Volume) findLatestSnapshotNameLocked(ctx context.Context) (string, error) {
	metaPrefix := path.Join("volumes", v.volumeID, "meta") + "/"
	objects, err := v.backend.ListObjects(ctx, "", metaPrefix)
	if err != nil {
		return "", err
	}

	type snapItem struct {
		name string
		pos  uint64
	}
	var snapshots []snapItem
	for _, obj := range objects {
		if strings.HasSuffix(obj, ".erofs") {
			base := path.Base(obj)
			trimmed := strings.TrimSuffix(base, ".erofs")
			pos, _ := strconv.ParseUint(trimmed, 10, 64)
			snapshots = append(snapshots, snapItem{name: base, pos: pos})
		}
	}
	if len(snapshots) == 0 {
		return "", fmt.Errorf("no snapshots found for volume %s", v.volumeID)
	}
	sort.Slice(snapshots, func(i, j int) bool {
		if snapshots[i].pos != snapshots[j].pos {
			return snapshots[i].pos < snapshots[j].pos
		}
		return snapshots[i].name < snapshots[j].name
	})
	return snapshots[len(snapshots)-1].name, nil
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

	case MutationSetAttr:
		inodeID := record.Inode
		if inodeID == 0 {
			inodeID = v.rootInodeID
		}
		node, _ := v.inodeCache.Get(inodeID)
		if node == nil {
			var err error
			node, err = v.getOrLoadInodeLocked(ctx, inodeID)
			if err != nil {
				return err
			}
		}
		if record.Mode != 0 {
			node.Mode = (node.Mode & ^uint32(07777)) | (record.Mode & 07777)
		}
		if record.Uid != 0 {
			node.Uid = record.Uid
		}
		if record.Gid != 0 {
			node.Gid = record.Gid
		}
		if record.ModTime != nil {
			node.ModTime = record.ModTime.AsTime()
		}
		if record.Atime != nil {
			node.Atime = record.Atime.AsTime()
		}
		if record.Ctime != nil {
			node.Ctime = record.Ctime.AsTime()
		}
		node.IsDirty = false
		v.inodeCache.Put(inodeID, node)
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
	case *pb.FileChunk:
		ino := row.GetIno()
		idx := row.GetIndex()
		node, _ := v.inodeCache.Get(ino)
		if node == nil {
			node = &CachedInode{
				ID:        ino,
				ChunkSize: v.chunkSize,
				IsDirty:   false,
			}
			if node.ChunkSize == 0 {
				node.ChunkSize = 64 * 1024
			}
			v.inodeCache.Put(ino, node)
		}
		switch change.Op {
		case sdsv1.OpRecord_CREATE, sdsv1.OpRecord_UPDATE:
			if len(row.GetInlineData()) > 0 {
				node.InlineData = row.GetInlineData()
				node.Chunks = nil
			} else if row.GetSha256() != "" {
				node.InlineData = nil
				if node.Chunks == nil {
					node.Chunks = make(map[uint32]string)
				}
				node.Chunks[idx] = row.GetSha256()
			}
		case sdsv1.OpRecord_DELETE:
			if node.Chunks != nil {
				delete(node.Chunks, idx)
			}
			if idx == 0 && len(node.InlineData) > 0 {
				node.InlineData = nil
			}
		}

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
			var atime, ctime time.Time
			if row.GetAtime() != nil {
				atime = row.GetAtime().AsTime()
			}
			if atime.IsZero() {
				atime = modTime
			}
			if row.GetCtime() != nil {
				ctime = row.GetCtime().AsTime()
			}
			if ctime.IsZero() {
				ctime = modTime
			}

			if row.GetIsDir() {
				node, _ := v.inodeCache.Get(ino)
				if node == nil {
					node = &CachedInode{
						ID:      ino,
						IsDir:   true,
						IsDirty: false,
					}
					v.inodeCache.Put(ino, node)
				}
				node.Mode = row.GetMode()
				node.ModTime = modTime
				node.Atime = atime
				node.Ctime = ctime
				node.Uid = row.GetUid()
				node.Gid = row.GetGid()
				node.IsDir = true
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
				node, _ := v.inodeCache.Get(ino)
				if node == nil {
					node = &CachedInode{
						ID:      ino,
						IsDir:   false,
						IsDirty: false,
					}
					v.inodeCache.Put(ino, node)
				}
				node.Mode = row.GetMode()
				node.Size = row.GetSize()
				node.ModTime = modTime
				node.Atime = atime
				node.Ctime = ctime
				node.Uid = row.GetUid()
				node.Gid = row.GetGid()
				node.Sha256 = row.GetSha256()
				node.ManifestSha256 = row.GetManifestSha256()
				node.ContentSha256 = row.GetContentSha256()
				node.ChunkSize = row.GetChunkSize()
				node.ETag = row.GetEtag()
				node.IsDir = false

				if node.ChunkSize > 0 && node.ManifestSha256 != "" {
					if mData, ok := v.recoveredContent[node.ManifestSha256]; ok {
						manifest, err := blob.DecodeManifest(bytes.NewReader(mData))
						if err == nil {
							node.Chunks = make(map[uint32]string, len(manifest.Chunks))
							for idx, hexSha := range manifest.ChunkHexSHAs() {
								if hexSha != "" {
									node.Chunks[uint32(idx)] = hexSha
								}
							}
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

func (v *Volume) updateNextInodeFromSQLiteLocked(ctx context.Context) {
	if v.sqliteDB == nil {
		return
	}
	rows, err := v.sqliteDB.Rows(ctx, "objectfs.v1alpha1.Inode")
	if err != nil {
		return
	}
	var maxIno uint64
	for _, row := range rows {
		if inode, ok := row.(*pb.Inode); ok {
			if inode.GetIno() > maxIno {
				maxIno = inode.GetIno()
			}
		}
	}
	if maxIno > 0 {
		next := ((maxIno + erofs.DefaultInodeStride) / erofs.DefaultInodeStride) * erofs.DefaultInodeStride
		if next > v.nextInode {
			v.nextInode = next
		}
	}
	if v.nextInode < erofs.DefaultInodeStride {
		v.nextInode = erofs.DefaultInodeStride
	}
	v.rootInodeID = 1
}

func (v *Volume) importErofsToSQLiteLocked(ctx context.Context, reader *erofs.Reader, snapPos uint64) error {
	if v.sqliteDB == nil || reader == nil {
		return nil
	}

	rootNID := reader.GetRootNID()
	var changes []sds.Change

	queue := []uint64{rootNID}
	visited := make(map[uint64]bool)

	for len(queue) > 0 {
		nid := queue[0]
		queue = queue[1:]
		if visited[nid] {
			continue
		}
		visited[nid] = true

		ino := nid
		if nid == rootNID || nid == 0 {
			ino = 1
		}

		erofsInode, err := erofs.ReadInode(v.snapshotRaw, reader.Superblock(), nid)
		if err != nil {
			return fmt.Errorf("ReadInode failed for nid %d: %w", nid, err)
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

		var shaStr, manifestSha, contentSha string
		var chunkSize uint32
		xattrs, _ := reader.GetXattrs(nid)
		if !xattrs.IsEmpty() {
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

		inoMsg := &pb.Inode{
			Ino:            proto.Uint64(ino),
			Mode:           mode,
			Size:           int64(erofsInode.Size),
			Mtime:          timestamppb.New(mtime),
			IsDir:          isDir,
			Sha256:         shaStr,
			ManifestSha256: manifestSha,
			ContentSha256:  contentSha,
			ChunkSize:      chunkSize,
		}

		keyBytes, valBytes, _ := sds.SplitKeyAndNonKey(inoMsg, []int32{1})
		changes = append(changes, sds.Change{
			Seq:      snapPos,
			TypeID:   16,
			TypeName: "objectfs.v1alpha1.Inode",
			Op:       sds.OpCreate,
			Key:      sds.NewKeyFromBytes(keyBytes),
			RawKey:   keyBytes,
			RawVal:   valBytes,
			Row:      inoMsg,
		})

		if isDir {
			dirents, err := reader.ListDirectory(nid)
			if err == nil {
				for _, de := range dirents {
					if de.Name == "." || de.Name == ".." {
						continue
					}
					childName := de.Name
					childNID := de.NID
					childIno := de.NID
					if childNID == rootNID || childNID == 0 {
						childIno = 1
					}
					childIsDir := de.FileType == erofs.FTDir
					childMode := uint32(0644 | syscall.S_IFREG)
					if childIsDir {
						childMode = uint32(0755 | syscall.S_IFDIR)
					}

					dirEntryMsg := &pb.DirEntry{
						ParentIno: proto.Uint64(ino),
						Name:      proto.String(childName),
						Ino:       childIno,
						IsDir:     childIsDir,
						Mode:      childMode,
					}
					deKeyBytes, deValBytes, _ := sds.SplitKeyAndNonKey(dirEntryMsg, []int32{1, 2})
					changes = append(changes, sds.Change{
						Seq:      snapPos,
						TypeID:   17,
						TypeName: "objectfs.v1alpha1.DirEntry",
						Op:       sds.OpCreate,
						Key:      sds.NewKeyFromBytes(deKeyBytes),
						RawKey:   deKeyBytes,
						RawVal:   deValBytes,
						Row:      dirEntryMsg,
					})
					if v.dirParents == nil {
						v.dirParents = make(map[uint64]uint64)
					}
					v.dirParents[childIno] = ino
					queue = append(queue, childNID)
				}
			}
		} else {
			if erofsInode.Size <= 4096 && erofsInode.Size > 0 {
				if r, err := reader.ReadFileContent(nid); err == nil {
					data, err := io.ReadAll(r)
					if err == nil && len(data) > 0 {
						chunkMsg := &pb.FileChunk{
							Ino:        proto.Uint64(ino),
							Index:      proto.Uint32(0),
							InlineData: data,
						}
						cKeyBytes, cValBytes, _ := sds.SplitKeyAndNonKey(chunkMsg, []int32{1, 2})
						changes = append(changes, sds.Change{
							Seq:      snapPos,
							TypeID:   18,
							TypeName: "objectfs.v1alpha1.FileChunk",
							Op:       sds.OpCreate,
							Key:      sds.NewKeyFromBytes(cKeyBytes),
							RawKey:   cKeyBytes,
							RawVal:   cValBytes,
							Row:      chunkMsg,
						})
					}
				}
			}
		}
	}

	if len(changes) > 0 {
		if err := v.sqliteDB.ApplyBatch(ctx, changes); err != nil {
			return fmt.Errorf("failed to import EROFS snapshot into SQLite: %w", err)
		}
	}
	v.lastCommitSeq = snapPos
	return nil
}

func (v *Volume) loadFromBackendSQLiteLocked(ctx context.Context) error {
	dbPath := filepath.Join(v.localStorageDir, "metadata.sqlite")

	var snapPos uint64
	var initialized bool

	// 1. Check if local SQLite file exists and has rows
	if v.sqliteDB != nil {
		pos := v.sqliteDB.Position()
		count, _ := v.sqliteDB.Count(ctx, "objectfs.v1alpha1.Inode")
		if count > 0 && pos > 0 {
			snapPos = pos
			initialized = true
		}
	} else if _, err := os.Stat(dbPath); err == nil {
		db, err := sqlite.Open(ctx, dbPath,
			sqlite.WithStreamID(v.streamID.String()),
			sqlite.WithLockingMode("EXCLUSIVE"),
			sqlite.WithJournalMode("WAL"),
			sqlite.WithSynchronous("NORMAL"),
		)
		if err == nil {
			v.sqliteDB = db
			if v.metadataStream != nil {
				_ = v.sqliteDB.SyncRegistry(ctx, v.metadataStream.Registry())
			}
			pos := db.Position()
			count, _ := db.Count(ctx, "objectfs.v1alpha1.Inode")
			if count > 0 && pos > 0 {
				snapPos = pos
				initialized = true
			}
		}
	}

	// 2. Otherwise restore latest published SQLite snapshot
	if !initialized && v.backend != nil {
		if snapKey, pos, err := sqlite.FindLatestSnapshot(ctx, v.backend, v.streamID.String(), 0); err == nil && snapKey != "" {
			if v.sqliteDB != nil {
				_ = v.sqliteDB.Close()
				v.sqliteDB = nil
			}
			_ = os.Remove(dbPath)
			db, pos, err := sqlite.RestoreSnapshot(ctx, v.backend, v.streamID.String(), pos, dbPath,
				sqlite.WithLockingMode("EXCLUSIVE"),
				sqlite.WithJournalMode("WAL"),
				sqlite.WithSynchronous("NORMAL"),
			)
			if err == nil {
				v.sqliteDB = db
				if v.metadataStream != nil {
					_ = v.sqliteDB.SyncRegistry(ctx, v.metadataStream.Registry())
				}
				snapPos = pos
				initialized = true
			}
		}
	}

	// 3. Otherwise, for volumes that only have EROFS snapshots (or initial startup)
	if !initialized {
		if v.sqliteDB == nil {
			_ = os.MkdirAll(v.localStorageDir, 0755)
			db, err := sqlite.Open(ctx, dbPath,
				sqlite.WithStreamID(v.streamID.String()),
				sqlite.WithLockingMode("EXCLUSIVE"),
				sqlite.WithJournalMode("WAL"),
				sqlite.WithSynchronous("NORMAL"),
			)
			if err != nil {
				return fmt.Errorf("failed to open SQLite database: %w", err)
			}
			v.sqliteDB = db
		}
		if v.metadataStream != nil {
			_ = v.sqliteDB.SyncRegistry(ctx, v.metadataStream.Registry())
		}

		if v.backend != nil {
			latestSnapshotName, err := v.findLatestSnapshotNameLocked(ctx)
			if err == nil && latestSnapshotName != "" {
				snapshotKey := path.Join("volumes", v.volumeID, "meta", latestSnapshotName)
				var imgBuf bytes.Buffer
				if err := v.backend.GetObject(ctx, "", snapshotKey, 0, 0, &imgBuf); err == nil && imgBuf.Len() > 0 {
					snapBytes := imgBuf.Bytes()
					readerAt := bytes.NewReader(snapBytes)
					if reader, err := erofs.NewReader(readerAt); err == nil {
						v.snapshotRaw = readerAt
						v.snapshotReader = reader
						pos := uint64(0)
						if rootXattrs, xErr := reader.GetXattrs(reader.GetRootNID()); xErr == nil {
							if posStr, ok := rootXattrs.Others["trusted.sds.position"]; ok && posStr != "" {
								if p, pErr := strconv.ParseUint(posStr, 10, 64); pErr == nil {
									pos = p
								}
							}
						}
						if pos == 0 {
							trimmed := strings.TrimSuffix(latestSnapshotName, ".erofs")
							if p, pErr := strconv.ParseUint(trimmed, 10, 64); pErr == nil {
								pos = p
							}
						}
						snapPos = pos
						if err := v.importErofsToSQLiteLocked(ctx, reader, snapPos); err != nil {
							return fmt.Errorf("importErofsToSQLiteLocked failed: %w", err)
						}
						initialized = true
					}
				}
			}
		}

		if !initialized {
			count, _ := v.sqliteDB.Count(ctx, "objectfs.v1alpha1.Inode")
			if count == 0 {
				rootInodeMsg := &pb.Inode{
					Ino:   proto.Uint64(1),
					Mode:  0755 | syscall.S_IFDIR,
					Mtime: timestamppb.Now(),
					IsDir: true,
				}
				keyBytes, valBytes, _ := sds.SplitKeyAndNonKey(rootInodeMsg, []int32{1})
				initChanges := []sds.Change{
					{
						Seq:      0,
						TypeID:   16,
						TypeName: "objectfs.v1alpha1.Inode",
						Op:       sds.OpCreate,
						Key:      sds.NewKeyFromBytes(keyBytes),
						RawKey:   keyBytes,
						RawVal:   valBytes,
						Row:      rootInodeMsg,
					},
				}
				_ = v.sqliteDB.ApplyBatch(ctx, initChanges)
				v.applyChangesToSQLiteCacheLocked(initChanges)
			}
			v.rootInodeID = 1
			v.nextInode = erofs.DefaultInodeStride
			v.dirParents[1] = 1
		}
	}

	// Replay stream from snapPos
	if v.stream != nil {
		recovered := v.stream.RecoveredRecords()
		if len(recovered) > 0 {
			cr := sds.NewChangeReader(record.WithDecoderRegistry(v.metadataStream.Registry()))
			sr := sds.NewStreamReader("", v.streamID, sds.WithChangeReader(cr), sds.WithRecoveredRecords(recovered))
			changes, err := sr.FeedRecovered(snapPos)
			if err != nil {
				return fmt.Errorf("failed to recover SDS stream records: %w", err)
			}
			if len(changes) > 0 {
				if err := v.sqliteDB.ApplyBatch(ctx, changes); err != nil {
					return fmt.Errorf("failed to apply recovered SDS changes to SQLite: %w", err)
				}
				for _, ch := range changes {
					if ch.Seq > v.lastCommitSeq {
						v.lastCommitSeq = ch.Seq
					}
				}
			}
			sr.ChangeReader().DiscardPending()
		}
	}

	if v.sqliteCache != nil {
		v.sqliteCache.Clear()
	}
	if v.sqliteOverlay != nil {
		clear(v.sqliteOverlay)
		v.unappliedBytes = 0
	}
	if v.sqliteDB != nil {
		v.sqliteAppliedPos = v.sqliteDB.Position()
	}
	if v.sqliteApplier == nil {
		v.sqliteApplier = newSQLiteApplier(v, v.applierBatchSize, v.applierFaultHook)
		v.sqliteApplier.start()
	}

	v.updateNextInodeFromSQLiteLocked(ctx)
	return nil
}

func (v *Volume) LoadFromBackend(ctx context.Context) error {
	v.mu.Lock()
	defer v.mu.Unlock()

	if v.metadataStore == MetadataStoreSQLite {
		return v.loadFromBackendSQLiteLocked(ctx)
	}

	var snapPos uint64

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
					v.recoveredContent = make(map[string][]byte)

					// Read root xattrs for position and registry
					if rootXattrs, xErr := reader.GetXattrs(reader.GetRootNID()); xErr == nil {
						if posStr, ok := rootXattrs.Others["trusted.sds.position"]; ok && posStr != "" {
							if p, pErr := strconv.ParseUint(posStr, 10, 64); pErr == nil {
								snapPos = p
							}
						}
						if regStr, ok := rootXattrs.Others["trusted.sds.registry"]; ok && regStr != "" {
							var reg sdsv1.Registry
							if uErr := proto.Unmarshal([]byte(regStr), &reg); uErr == nil {
								if v.metadataStream != nil {
									_ = v.metadataStream.Registry().Import(&reg)
								}
							}
						}
					}
					if snapPos == 0 {
						trimmed := strings.TrimSuffix(latestSnapshotName, ".erofs")
						if p, pErr := strconv.ParseUint(trimmed, 10, 64); pErr == nil {
							snapPos = p
						}
					}
					v.lastCommitSeq = snapPos
				}
			}
		}
	}

	// SDS Stream replay from snapPos
	if v.stream != nil {
		recovered := v.stream.RecoveredRecords()
		if len(recovered) > 0 {
			cr := sds.NewChangeReader(record.WithDecoderRegistry(v.metadataStream.Registry()))
			sr := sds.NewStreamReader("", v.streamID, sds.WithChangeReader(cr), sds.WithRecoveredRecords(recovered))
			changes, err := sr.FeedRecovered(snapPos)
			if err != nil {
				return fmt.Errorf("failed to recover SDS stream records: %w", err)
			}
			for _, change := range changes {
				if err := v.ApplySDSChangeLocked(ctx, change); err != nil {
					return fmt.Errorf("failed to apply recovered SDS change: %w", err)
				}
				if change.Seq > v.lastCommitSeq {
					v.lastCommitSeq = change.Seq
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

// ListSnapshots returns all EROFS snapshot filenames for this volume sorted chronologically / by position.
func (v *Volume) ListSnapshots(ctx context.Context) ([]string, error) {
	v.mu.RLock()
	defer v.mu.RUnlock()

	metaPrefix := path.Join("volumes", v.volumeID, "meta") + "/"
	objects, err := v.backend.ListObjects(ctx, "", metaPrefix)
	if err != nil {
		return nil, err
	}

	type snapItem struct {
		name string
		pos  uint64
	}
	var snapshots []snapItem
	for _, obj := range objects {
		if strings.HasSuffix(obj, ".erofs") {
			base := path.Base(obj)
			trimmed := strings.TrimSuffix(base, ".erofs")
			pos, _ := strconv.ParseUint(trimmed, 10, 64)
			snapshots = append(snapshots, snapItem{name: base, pos: pos})
		}
	}
	sort.Slice(snapshots, func(i, j int) bool {
		if snapshots[i].pos != snapshots[j].pos {
			return snapshots[i].pos < snapshots[j].pos
		}
		return snapshots[i].name < snapshots[j].name
	})

	var res []string
	for _, s := range snapshots {
		res = append(res, s.name)
	}
	return res, nil
}

// GetSnapshotInfo returns metadata (position, created_at timestamp, size) for a snapshot.
func (v *Volume) GetSnapshotInfo(ctx context.Context, snapshotName string) (*pb.SnapshotInfo, error) {
	v.mu.RLock()
	defer v.mu.RUnlock()
	return v.getSnapshotInfoLocked(ctx, snapshotName)
}

func (v *Volume) getSnapshotInfoLocked(ctx context.Context, snapshotName string) (*pb.SnapshotInfo, error) {
	if v.backend == nil {
		return nil, fmt.Errorf("no backend configured")
	}

	trimmed := strings.TrimSuffix(snapshotName, ".erofs")
	pos, _ := strconv.ParseUint(trimmed, 10, 64)

	snapshotKey := path.Join("volumes", v.volumeID, "meta", snapshotName)
	var imgBuf bytes.Buffer
	if err := v.backend.GetObject(ctx, "", snapshotKey, 0, 0, &imgBuf); err != nil {
		return nil, fmt.Errorf("failed to get snapshot %s: %w", snapshotKey, err)
	}

	snapBytes := imgBuf.Bytes()
	info := &pb.SnapshotInfo{
		Name:     snapshotName,
		Position: pos,
		Size:     int64(len(snapBytes)),
	}

	readerAt := bytes.NewReader(snapBytes)
	if reader, err := erofs.NewReader(readerAt); err == nil {
		if rootXattrs, xErr := reader.GetXattrs(reader.GetRootNID()); xErr == nil {
			if posStr, ok := rootXattrs.Others["trusted.sds.position"]; ok && posStr != "" {
				if p, pErr := strconv.ParseUint(posStr, 10, 64); pErr == nil {
					info.Position = p
				}
			}
			if timeStr, ok := rootXattrs.Others["trusted.sds.snapshot_time"]; ok && timeStr != "" {
				if t, tErr := time.Parse(time.RFC3339Nano, timeStr); tErr == nil {
					info.CreatedAt = timestamppb.New(t)
				} else if t, tErr := time.Parse(time.RFC3339, timeStr); tErr == nil {
					info.CreatedAt = timestamppb.New(t)
				}
			}
		}
	}

	return info, nil
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

	var snapPos uint64
	if rootXattrs, xErr := reader.GetXattrs(reader.GetRootNID()); xErr == nil {
		if posStr, ok := rootXattrs.Others["trusted.sds.position"]; ok && posStr != "" {
			if p, pErr := strconv.ParseUint(posStr, 10, 64); pErr == nil {
				snapPos = p
			}
		}
		if regStr, ok := rootXattrs.Others["trusted.sds.registry"]; ok && regStr != "" {
			var reg sdsv1.Registry
			if uErr := proto.Unmarshal([]byte(regStr), &reg); uErr == nil {
				if v.metadataStream != nil {
					_ = v.metadataStream.Registry().Import(&reg)
				}
			}
		}
	}
	if snapPos == 0 {
		trimmed := strings.TrimSuffix(snapshotName, ".erofs")
		if p, pErr := strconv.ParseUint(trimmed, 10, 64); pErr == nil {
			snapPos = p
		}
	}
	v.lastCommitSeq = snapPos

	v.inodeCache.Clear()
	v.dirCache.Clear()
	v.dirtyInodes = make(map[uint64]LocalOffset)
	v.dirtyDirs = make(map[uint64]LocalOffset)
	v.recoveredContent = make(map[string][]byte)
	v.snapshotCutoff = NoOffset
	if v.localStore != nil {
		_ = v.localStore.DeleteAllAndReset()
	}

	return nil
}
