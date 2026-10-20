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

package fuse

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	pb "github.com/gke-labs/in-cluster-storage/pkg/api/objectfs/v1alpha1"
	"github.com/hanwen/go-fuse/v2/fuse"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
	"k8s.io/klog/v2"
)

// MaxNameLength is the maximum allowed byte length for a path component name
// (matching the POSIX NAME_MAX limit of 255 bytes advertised by statfs).
const MaxNameLength = 255

// DefaultRPCTimeout is the default deadline applied to ObjectFS controller RPCs (metadata, reads).
const DefaultRPCTimeout = 30 * time.Second

// DefaultSyncTimeout is the default deadline applied to sync operations (Fsync, Flush, Write).
const DefaultSyncTimeout = 10 * time.Minute

// DefaultConnectedEntryTimeout is the dentry cache timeout returned while the watcher is connected.
const DefaultConnectedEntryTimeout = 60 * time.Second

// DefaultConnectedAttrTimeout is the attribute cache timeout returned while the watcher is connected.
const DefaultConnectedAttrTimeout = 60 * time.Second

// DisconnectedTimeout is the entry and attribute timeout returned while the watcher is disconnected.
const DisconnectedTimeout = 1 * time.Second

// LongWatcherGapThreshold is the duration after which an ongoing watcher disconnect
// triggers a warning log.
const LongWatcherGapThreshold = 10 * time.Second

// DefaultPrefetchWindow is the number of chunks to prefetch ahead (32 chunks * 64KB = 2 MiB).
const DefaultPrefetchWindow = 32

type dentryKey struct {
	parent uint64
	name   string
}

// TrackedDentry represents a (parent, name) directory entry held in kernel cache.
type TrackedDentry struct {
	Parent uint64
	Name   string
}

type fileManifest struct {
	inode      uint64
	chunkSize  uint32
	totalSize  int64
	chunkSHAs  []string
	inlineData []byte
	loadedAt   time.Time
}

type prefetchHandle struct {
	cancel context.CancelFunc
	inode  uint64
}

type ObjectFS struct {
	fuse.RawFileSystem
	client                 pb.ObjectFSControllerClient
	volumeID               string
	writeMode              pb.WriteMode
	cache                  *NodeCache
	diskCache              *DiskBlobCache
	server                 *fuse.Server
	rpcTimeout             time.Duration
	syncTimeout            time.Duration
	syncOnClose            bool
	backgroundSyncInterval time.Duration
	syncTrigger            chan struct{}
	closedInodesMu         sync.Mutex
	closedInodes           map[uint64]struct{}
	errMu                  sync.Mutex
	lastLoggedErr          time.Time

	manifestMu         sync.RWMutex
	manifests          map[uint64]*fileManifest
	prefetchMu         sync.Mutex
	activePrefetches   map[uint64]*prefetchHandle
	lastReadChunk      map[uint64]int
	furthestPrefetched map[uint64]int

	statfsMu     sync.Mutex
	statfsCache  fuse.StatfsOut
	statfsExpiry time.Time
	statfsTTL    time.Duration

	entryTimeout     time.Duration
	attrTimeout      time.Duration
	watcherConnected atomic.Bool

	lookupMu      sync.Mutex
	lookups       map[uint64]uint64
	dentries      map[uint64]map[string]struct{}
	childDentries map[uint64]map[dentryKey]struct{}

	notifyHook func(notifyEvent)
}

var _ fuse.RawFileSystem = (*ObjectFS)(nil)

func NewObjectFS(client pb.ObjectFSControllerClient, volumeID string, writeMode pb.WriteMode, cache *NodeCache) *ObjectFS {
	return NewObjectFSWithTimeouts(client, volumeID, writeMode, cache, DefaultRPCTimeout, DefaultSyncTimeout)
}

func NewObjectFSWithTimeout(client pb.ObjectFSControllerClient, volumeID string, writeMode pb.WriteMode, cache *NodeCache, rpcTimeout time.Duration) *ObjectFS {
	return NewObjectFSWithTimeouts(client, volumeID, writeMode, cache, rpcTimeout, DefaultSyncTimeout)
}

func NewObjectFSWithTimeouts(client pb.ObjectFSControllerClient, volumeID string, writeMode pb.WriteMode, cache *NodeCache, rpcTimeout, syncTimeout time.Duration) *ObjectFS {
	if cache == nil {
		cache = NewNodeCache(128 * 1024 * 1024)
	}
	if rpcTimeout <= 0 {
		rpcTimeout = DefaultRPCTimeout
	}
	if syncTimeout <= 0 {
		syncTimeout = DefaultSyncTimeout
	}
	fs := &ObjectFS{
		RawFileSystem:          fuse.NewDefaultRawFileSystem(),
		client:                 client,
		volumeID:               volumeID,
		writeMode:              writeMode,
		cache:                  cache,
		rpcTimeout:             rpcTimeout,
		syncTimeout:            syncTimeout,
		syncOnClose:            true,
		backgroundSyncInterval: 30 * time.Second,
		syncTrigger:            make(chan struct{}, 1),
		closedInodes:           make(map[uint64]struct{}),
		manifests:              make(map[uint64]*fileManifest),
		activePrefetches:       make(map[uint64]*prefetchHandle),
		lastReadChunk:          make(map[uint64]int),
		furthestPrefetched:     make(map[uint64]int),
		statfsTTL:              time.Second,
		entryTimeout:           DefaultConnectedEntryTimeout,
		attrTimeout:            DefaultConnectedAttrTimeout,
		lookups:                make(map[uint64]uint64),
		dentries:               make(map[uint64]map[string]struct{}),
		childDentries:          make(map[uint64]map[dentryKey]struct{}),
	}
	fs.lookups[fuse.FUSE_ROOT_ID] = 1
	return fs
}

func (fs *ObjectFS) SetDiskBlobCache(dc *DiskBlobCache) {
	fs.diskCache = dc
}

func (fs *ObjectFS) DiskBlobCache() *DiskBlobCache {
	return fs.diskCache
}

func (fs *ObjectFS) invalidateManifest(inode uint64) {
	fs.manifestMu.Lock()
	delete(fs.manifests, inode)
	fs.manifestMu.Unlock()
}

func (fs *ObjectFS) invalidateAllManifests() {
	fs.manifestMu.Lock()
	fs.manifests = make(map[uint64]*fileManifest)
	fs.manifestMu.Unlock()
}

func (fs *ObjectFS) getOrLoadManifest(ctx context.Context, inode uint64) (*fileManifest, error) {
	fs.manifestMu.RLock()
	m, ok := fs.manifests[inode]
	fs.manifestMu.RUnlock()
	if ok && m != nil && time.Since(m.loadedAt) < 5*time.Minute {
		return m, nil
	}

	resp, err := fs.client.GetManifest(ctx, &pb.GetManifestRequest{
		VolumeId: fs.volumeID,
		Inode:    inode,
	})
	if err != nil {
		return nil, err
	}
	if resp.GetError() != 0 {
		return nil, syscall.Errno(resp.GetError())
	}

	cs := resp.GetChunkSize()
	if cs == 0 {
		cs = DefaultChunkSize
	}

	m = &fileManifest{
		inode:      resp.GetInode(),
		chunkSize:  cs,
		totalSize:  resp.GetTotalSize(),
		chunkSHAs:  resp.GetChunkSha256(),
		inlineData: resp.GetInlineData(),
		loadedAt:   time.Now(),
	}

	fs.manifestMu.Lock()
	if fs.manifests == nil {
		fs.manifests = make(map[uint64]*fileManifest)
	}
	fs.manifests[inode] = m
	fs.manifestMu.Unlock()

	return m, nil
}

func (fs *ObjectFS) startPrefetchOnOpen(inode, fh uint64) {
	if fs.diskCache == nil {
		return
	}

	manifest, err := fs.getOrLoadManifest(context.Background(), inode)
	if err != nil || manifest == nil || len(manifest.chunkSHAs) == 0 {
		return
	}

	fs.diskCache.RecordKnownBlobs(manifest.chunkSHAs)

	ctx, cancel := context.WithCancel(context.Background())
	fs.prefetchMu.Lock()
	if fs.activePrefetches == nil {
		fs.activePrefetches = make(map[uint64]*prefetchHandle)
	}
	fs.activePrefetches[fh] = &prefetchHandle{
		cancel: cancel,
		inode:  inode,
	}
	if fs.lastReadChunk == nil {
		fs.lastReadChunk = make(map[uint64]int)
	}
	fs.lastReadChunk[fh] = -1
	if fs.furthestPrefetched == nil {
		fs.furthestPrefetched = make(map[uint64]int)
	}

	limit := len(manifest.chunkSHAs)
	if limit > DefaultPrefetchWindow {
		limit = DefaultPrefetchWindow
	}
	fs.furthestPrefetched[fh] = limit
	fs.prefetchMu.Unlock()

	go fs.prefetchChunks(ctx, manifest, 0, limit)
}

func (fs *ObjectFS) prefetchChunks(ctx context.Context, manifest *fileManifest, start, end int) {
	if start < 0 {
		start = 0
	}
	if end > len(manifest.chunkSHAs) {
		end = len(manifest.chunkSHAs)
	}
	if start >= end {
		return
	}

	concurrency := 4
	sem := make(chan struct{}, concurrency)
	var wg sync.WaitGroup

	for i := start; i < end; i++ {
		select {
		case <-ctx.Done():
			return
		default:
		}

		sha := manifest.chunkSHAs[i]
		if sha == "" || fs.diskCache.HasBlob(sha) {
			continue
		}

		select {
		case <-ctx.Done():
			return
		case sem <- struct{}{}:
		}

		wg.Add(1)
		go func(chunkSHA string) {
			defer func() {
				<-sem
				wg.Done()
			}()
			if err := fs.diskCache.EnsureBlob(ctx, fs.client, chunkSHA); err != nil {
				klog.V(4).Infof("prefetch EnsureBlob failed for %s: %v", chunkSHA, err)
			}
		}(sha)
	}
	wg.Wait()
}

func (fs *ObjectFS) SetSyncOnClose(enabled bool) {
	fs.syncOnClose = enabled
}

func (fs *ObjectFS) SetBackgroundSyncInterval(d time.Duration) {
	fs.backgroundSyncInterval = d
}

func (fs *ObjectFS) StartBackgroundSync(ctx context.Context) {
	interval := fs.backgroundSyncInterval
	if interval <= 0 {
		interval = 30 * time.Second
	}
	ticker := time.NewTicker(interval)
	go func() {
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				syncCtx, cancel := context.WithTimeout(ctx, fs.syncTimeout)
				if err := fs.syncVolumeNonBlocking(syncCtx, pb.FsyncRequest_SYNC_TRIGGER_PERIODIC); err != nil {
					klog.Warningf("ObjectFS volume %s background sync failed: %v", fs.volumeID, err)
				}
				cancel()
			case <-fs.syncTrigger:
				syncCtx, cancel := context.WithTimeout(ctx, fs.syncTimeout)
				if err := fs.syncClosedInodes(syncCtx); err != nil {
					klog.Warningf("ObjectFS volume %s sync on close failed: %v", fs.volumeID, err)
				}
				cancel()
			}
		}
	}()
}

func (fs *ObjectFS) SetRPCTimeout(d time.Duration) {
	if d <= 0 {
		d = DefaultRPCTimeout
	}
	fs.rpcTimeout = d
}

func (fs *ObjectFS) SetSyncTimeout(d time.Duration) {
	if d <= 0 {
		d = DefaultSyncTimeout
	}
	fs.syncTimeout = d
}

func (fs *ObjectFS) String() string {
	return fmt.Sprintf("ObjectFS(%s)", fs.volumeID)
}

func (fs *ObjectFS) Init(server *fuse.Server) {
	fs.server = server
}

func (fs *ObjectFS) SetEntryTimeout(d time.Duration) {
	fs.entryTimeout = d
}

func (fs *ObjectFS) SetAttrTimeout(d time.Duration) {
	fs.attrTimeout = d
}

func (fs *ObjectFS) EntryTimeout() time.Duration {
	return fs.entryTimeout
}

func (fs *ObjectFS) AttrTimeout() time.Duration {
	return fs.attrTimeout
}

func (fs *ObjectFS) SetWatcherConnected(connected bool) {
	fs.watcherConnected.Store(connected)
}

func (fs *ObjectFS) IsWatcherConnected() bool {
	return fs.watcherConnected.Load()
}

func (fs *ObjectFS) currentEntryTimeout() time.Duration {
	if fs.IsWatcherConnected() {
		if fs.entryTimeout > 0 {
			return fs.entryTimeout
		}
		return DefaultConnectedEntryTimeout
	}
	return DisconnectedTimeout
}

func (fs *ObjectFS) currentAttrTimeout() time.Duration {
	if fs.IsWatcherConnected() {
		if fs.attrTimeout > 0 {
			return fs.attrTimeout
		}
		return DefaultConnectedAttrTimeout
	}
	return DisconnectedTimeout
}

func (fs *ObjectFS) trackDentryLocked(parent uint64, name string, childIno uint64) {
	if parent == 0 || name == "" || childIno == 0 {
		return
	}
	if fs.dentries == nil {
		fs.dentries = make(map[uint64]map[string]struct{})
	}
	names, ok := fs.dentries[parent]
	if !ok {
		names = make(map[string]struct{})
		fs.dentries[parent] = names
	}
	names[name] = struct{}{}

	if fs.childDentries == nil {
		fs.childDentries = make(map[uint64]map[dentryKey]struct{})
	}
	dk := dentryKey{parent: parent, name: name}
	dkMap, ok := fs.childDentries[childIno]
	if !ok {
		dkMap = make(map[dentryKey]struct{})
		fs.childDentries[childIno] = dkMap
	}
	dkMap[dk] = struct{}{}
}

// TrackedDentries returns a sorted slice of all (parent, name) pairs currently tracked as held by the kernel.
func (fs *ObjectFS) TrackedDentries() []TrackedDentry {
	fs.lookupMu.Lock()
	defer fs.lookupMu.Unlock()
	var res []TrackedDentry
	for parent, names := range fs.dentries {
		for name := range names {
			res = append(res, TrackedDentry{Parent: parent, Name: name})
		}
	}
	sort.Slice(res, func(i, j int) bool {
		if res[i].Parent != res[j].Parent {
			return res[i].Parent < res[j].Parent
		}
		return res[i].Name < res[j].Name
	})
	return res
}

// Forget is called by the kernel when discarding an entry from its dentry/inode cache.
func (fs *ObjectFS) Forget(nodeid, nlookup uint64) {
	if nodeid == 0 || nlookup == 0 {
		return
	}
	fs.lookupMu.Lock()
	defer fs.lookupMu.Unlock()
	if fs.lookups == nil {
		return
	}
	cur := fs.lookups[nodeid]
	if cur <= nlookup {
		delete(fs.lookups, nodeid)
		if fs.childDentries != nil {
			if dks, ok := fs.childDentries[nodeid]; ok {
				for dk := range dks {
					if names, ok := fs.dentries[dk.parent]; ok {
						delete(names, dk.name)
						if len(names) == 0 {
							delete(fs.dentries, dk.parent)
						}
					}
				}
				delete(fs.childDentries, nodeid)
			}
		}
	} else {
		fs.lookups[nodeid] = cur - nlookup
	}
}

// LookupCount returns the kernel lookup count currently tracked for the inode.
func (fs *ObjectFS) LookupCount(nodeid uint64) uint64 {
	fs.lookupMu.Lock()
	defer fs.lookupMu.Unlock()
	if fs.lookups == nil {
		return 0
	}
	return fs.lookups[nodeid]
}

// HeldInodes returns a sorted slice of all inode IDs currently tracked as held by the kernel.
func (fs *ObjectFS) HeldInodes() []uint64 {
	fs.lookupMu.Lock()
	defer fs.lookupMu.Unlock()
	if fs.lookups == nil {
		return nil
	}
	inodes := make([]uint64, 0, len(fs.lookups))
	for ino, count := range fs.lookups {
		if count > 0 {
			inodes = append(inodes, ino)
		}
	}
	sort.Slice(inodes, func(i, j int) bool { return inodes[i] < inodes[j] })
	return inodes
}

// SweepRevalidateInodes sends EntryNotify(parent, name) for every tracked dentry
// and InodeNotify(ino, -1, 0) for every held inode, invalidating kernel caches
// after a watcher reconnection.
func (fs *ObjectFS) SweepRevalidateInodes() {
	dentries := fs.TrackedDentries()
	inodes := fs.HeldInodes()

	klog.V(2).Infof("ObjectFS(%s): sweeping %d dentries and %d inodes on reconnect", fs.volumeID, len(dentries), len(inodes))

	for _, d := range dentries {
		fs.notifyEntry(d.Parent, d.Name)
	}
	for _, ino := range inodes {
		fs.notifyInode(ino, -1, 0)
	}
}

func (fs *ObjectFS) makeContext(cancel <-chan struct{}) (context.Context, context.CancelFunc) {
	return fs.makeContextWithTimeout(cancel, fs.rpcTimeout, DefaultRPCTimeout)
}

func (fs *ObjectFS) makeSyncContext(cancel <-chan struct{}) (context.Context, context.CancelFunc) {
	return fs.makeContextWithTimeout(cancel, fs.syncTimeout, DefaultSyncTimeout)
}

func (fs *ObjectFS) makeContextWithTimeout(cancel <-chan struct{}, timeout, defaultTimeout time.Duration) (context.Context, context.CancelFunc) {
	if timeout <= 0 {
		timeout = defaultTimeout
	}
	ctx, cancelFunc := context.WithTimeout(context.Background(), timeout)
	if cancel != nil {
		go func() {
			select {
			case <-cancel:
				cancelFunc()
			case <-ctx.Done():
			}
		}()
	}
	return ctx, cancelFunc
}

func (fs *ObjectFS) logRPCError(op string, err error) {
	if err == nil {
		return
	}
	st, ok := status.FromError(err)
	if !ok {
		return
	}
	if st.Code() == codes.Unavailable || st.Code() == codes.DeadlineExceeded {
		fs.errMu.Lock()
		now := time.Now()
		if now.Sub(fs.lastLoggedErr) >= 1*time.Second {
			fs.lastLoggedErr = now
			fs.errMu.Unlock()
			klog.Warningf("ObjectFS(%s): controller unreachable in %s: %v", fs.volumeID, op, err)
			return
		}
		fs.errMu.Unlock()
	}
}

func grpcErrorToStatus(err error) fuse.Status {
	if err == nil {
		return fuse.OK
	}
	st, ok := status.FromError(err)
	if !ok {
		return fuse.Status(syscall.EIO)
	}
	switch st.Code() {
	case codes.NotFound:
		return fuse.ENOENT
	case codes.AlreadyExists:
		return fuse.Status(syscall.EEXIST)
	case codes.InvalidArgument, codes.FailedPrecondition:
		return fuse.EINVAL
	case codes.PermissionDenied, codes.Unauthenticated:
		return fuse.EACCES
	case codes.Unimplemented:
		return fuse.ENOSYS
	case codes.DeadlineExceeded, codes.Unavailable:
		return fuse.Status(syscall.EIO)
	case codes.Canceled:
		return fuse.Status(syscall.EINTR)
	case codes.ResourceExhausted:
		return fuse.Status(syscall.ENOSPC)
	case codes.Aborted:
		return fuse.Status(syscall.EBUSY)
	default:
		return fuse.Status(syscall.EIO)
	}
}

func (fs *ObjectFS) grpcErrorToStatus(op string, err error) fuse.Status {
	fs.logRPCError(op, err)
	return grpcErrorToStatus(err)
}

func (fs *ObjectFS) fillAttrOut(attr *pb.EntryAttr, out *fuse.Attr) {
	if attr == nil || attr.GetInode() == nil {
		return
	}
	inoRow := attr.GetInode()
	out.Ino = inoRow.GetIno()
	out.Size = uint64(inoRow.GetSize())
	out.Mode = inoRow.GetMode()
	if inoRow.GetIsDir() {
		out.Mode |= syscall.S_IFDIR
		if inoRow.GetNlink() > 0 {
			out.Nlink = inoRow.GetNlink()
		} else {
			out.Nlink = 2
		}
	} else {
		if (out.Mode & syscall.S_IFMT) == 0 {
			out.Mode |= syscall.S_IFREG
		}
		out.Nlink = inoRow.GetNlink()
	}
	out.Rdev = inoRow.GetRdev()
	if inoRow.GetMtime() != nil {
		t := inoRow.GetMtime().AsTime()
		out.Mtime = uint64(t.Unix())
		out.Mtimensec = uint32(t.Nanosecond())
		out.Atime = out.Mtime
		out.Atimensec = out.Mtimensec
		out.Ctime = out.Mtime
		out.Ctimensec = out.Mtimensec
	}
	if inoRow.GetAtime() != nil {
		t := inoRow.GetAtime().AsTime()
		out.Atime = uint64(t.Unix())
		out.Atimensec = uint32(t.Nanosecond())
	}
	if inoRow.GetCtime() != nil {
		t := inoRow.GetCtime().AsTime()
		out.Ctime = uint64(t.Unix())
		out.Ctimensec = uint32(t.Nanosecond())
	}
	out.Owner = fuse.Owner{
		Uid: inoRow.GetUid(),
		Gid: inoRow.GetGid(),
	}

	isDir := inoRow.GetIsDir() || (out.Mode&syscall.S_IFMT) == syscall.S_IFDIR
	if fs.cache != nil && out.Ino != 0 {
		// Record attribute-derived positions only for regular files and symlinks.
		// A directory stat refreshes directory attributes but leaves child dentries
		// intact, so directory positions must only come from operations that update
		// dentries (this node's own mutations).
		if !isDir && attr.GetStreamPosition() > 0 {
			fs.cache.RecordPosition(out.Ino, attr.GetStreamPosition())
		}
		if entry, isDirty := fs.cache.GetDirty(out.Ino); isDirty {
			out.Size = uint64(entry.Size)
			out.Mtime = uint64(entry.ModTime.Unix())
			out.Mtimensec = uint32(entry.ModTime.Nanosecond())
			// Cache tracks ModTime but no separate inode change time; align Ctime with Mtime.
			out.Ctime = out.Mtime
			out.Ctimensec = out.Mtimensec
		}
	}
}

func (fs *ObjectFS) fillEntryOut(attr *pb.EntryAttr, out *fuse.EntryOut, parent uint64, name string) {
	fs.fillAttrOut(attr, &out.Attr)
	if attr != nil && attr.GetInode() != nil {
		out.NodeId = attr.GetInode().GetIno()
	}
	out.Generation = 1
	out.SetEntryTimeout(fs.currentEntryTimeout())
	out.SetAttrTimeout(fs.currentAttrTimeout())
	if out.NodeId != 0 {
		fs.lookupMu.Lock()
		if fs.lookups == nil {
			fs.lookups = make(map[uint64]uint64)
		}
		fs.lookups[out.NodeId]++
		if parent != 0 && name != "" {
			fs.trackDentryLocked(parent, name, out.NodeId)
		}
		fs.lookupMu.Unlock()
	}
}

func (fs *ObjectFS) Lookup(cancel <-chan struct{}, header *fuse.InHeader, name string, out *fuse.EntryOut) fuse.Status {
	if len(name) > MaxNameLength {
		return fuse.Status(syscall.ENAMETOOLONG)
	}
	ctx, cancelFunc := fs.makeContext(cancel)
	defer cancelFunc()

	resp, err := fs.client.Lookup(ctx, &pb.LookupRequest{
		VolumeId:    fs.volumeID,
		ParentInode: header.NodeId,
		Name:        name,
	})
	if err != nil {
		return fs.grpcErrorToStatus("Lookup", err)
	}
	if resp.GetError() != 0 {
		return fuse.Status(resp.GetError())
	}
	attr := resp.GetAttr()
	if attr == nil {
		return fuse.ENOENT
	}
	fs.fillEntryOut(attr, out, header.NodeId, name)
	return fuse.OK
}

func (fs *ObjectFS) GetAttr(cancel <-chan struct{}, input *fuse.GetAttrIn, out *fuse.AttrOut) fuse.Status {
	ctx, cancelFunc := fs.makeContext(cancel)
	defer cancelFunc()

	resp, err := fs.client.GetAttr(ctx, &pb.GetAttrRequest{
		VolumeId: fs.volumeID,
		Inode:    input.NodeId,
	})
	if err != nil {
		return fs.grpcErrorToStatus("GetAttr", err)
	}
	if resp.GetError() != 0 {
		return fuse.Status(resp.GetError())
	}
	attr := resp.GetAttr()
	if attr == nil {
		return fuse.ENOENT
	}
	fs.fillAttrOut(attr, &out.Attr)
	out.SetTimeout(fs.currentAttrTimeout())
	return fuse.OK
}

func (fs *ObjectFS) SetAttr(cancel <-chan struct{}, input *fuse.SetAttrIn, out *fuse.AttrOut) fuse.Status {
	ctx, cancelFunc := fs.makeContext(cancel)
	defer cancelFunc()

	var finalAttr *pb.EntryAttr

	if input.Valid&fuse.FATTR_SIZE != 0 {
		syncCtx, syncCancel := fs.makeSyncContext(cancel)
		if err := fs.syncFileToService(syncCtx, input.NodeId); err != nil {
			syncCancel()
			return fs.grpcErrorToStatus("Truncate:Sync", err)
		}
		syncCancel()
		truncGen := fs.cache.Truncate(input.NodeId, int64(input.Size), time.Now())
		resp, err := fs.client.TruncateFile(ctx, &pb.TruncateFileRequest{
			VolumeId: fs.volumeID,
			Inode:    input.NodeId,
			Size:     int64(input.Size),
		})
		if err != nil {
			return fs.grpcErrorToStatus("TruncateFile", err)
		}
		if resp.GetError() != 0 {
			return fuse.Status(resp.GetError())
		}
		if fs.cache != nil && resp.GetStreamPosition() > 0 {
			fs.cache.RecordPosition(input.NodeId, resp.GetStreamPosition())
		}
		fs.cache.MarkSizeClean(input.NodeId, truncGen)
		fs.invalidateManifest(input.NodeId)
		finalAttr = resp.GetAttr()
	}

	hasOtherAttrs := input.Valid&(fuse.FATTR_MODE|fuse.FATTR_UID|fuse.FATTR_GID|fuse.FATTR_ATIME|fuse.FATTR_ATIME_NOW|fuse.FATTR_MTIME|fuse.FATTR_MTIME_NOW|fuse.FATTR_CTIME) != 0

	if hasOtherAttrs {
		req := &pb.SetAttrRequest{
			VolumeId: fs.volumeID,
			Inode:    input.NodeId,
		}
		if input.Valid&fuse.FATTR_MODE != 0 {
			req.Mode = proto.Uint32(input.Mode)
		}
		if input.Valid&fuse.FATTR_UID != 0 {
			req.Uid = proto.Uint32(input.Uid)
		}
		if input.Valid&fuse.FATTR_GID != 0 {
			req.Gid = proto.Uint32(input.Gid)
		}
		if input.Valid&fuse.FATTR_ATIME_NOW != 0 {
			req.AtimeNow = true
		} else if input.Valid&fuse.FATTR_ATIME != 0 {
			req.Atime = timestamppb.New(time.Unix(int64(input.Atime), int64(input.Atimensec)))
		}
		if input.Valid&fuse.FATTR_MTIME_NOW != 0 {
			req.MtimeNow = true
		} else if input.Valid&fuse.FATTR_MTIME != 0 {
			req.Mtime = timestamppb.New(time.Unix(int64(input.Mtime), int64(input.Mtimensec)))
		}
		if input.Valid&fuse.FATTR_CTIME != 0 {
			req.Ctime = timestamppb.New(time.Unix(int64(input.Ctime), int64(input.Ctimensec)))
		}

		resp, err := fs.client.SetAttr(ctx, req)
		if err != nil {
			return fs.grpcErrorToStatus("SetAttr", err)
		}
		if resp.GetError() != 0 {
			return fuse.Status(resp.GetError())
		}
		finalAttr = resp.GetAttr()
	}

	if finalAttr == nil {
		resp, err := fs.client.GetAttr(ctx, &pb.GetAttrRequest{
			VolumeId: fs.volumeID,
			Inode:    input.NodeId,
		})
		if err != nil {
			return fs.grpcErrorToStatus("GetAttr", err)
		}
		if resp.GetError() != 0 {
			return fuse.Status(resp.GetError())
		}
		if resp.GetAttr() == nil {
			return fuse.ENOENT
		}
		finalAttr = resp.GetAttr()
	}

	fs.fillAttrOut(finalAttr, &out.Attr)
	if input.Valid&fuse.FATTR_SIZE != 0 {
		out.Attr.Size = input.Size
	}
	out.SetTimeout(fs.currentAttrTimeout())
	return fuse.OK
}

const (
	fallocAllocRange    uint32 = 0x00
	fallocKeepSize      uint32 = 0x01
	fallocPunchHole     uint32 = 0x02
	fallocCollapseRange uint32 = 0x08
	fallocZeroRange     uint32 = 0x10
	fallocInsertRange   uint32 = 0x20
	fallocUnshareRange  uint32 = 0x40
)

func (fs *ObjectFS) Fallocate(cancel <-chan struct{}, input *fuse.FallocateIn) fuse.Status {
	// 1. Check for unsupported modes: COLLAPSE_RANGE, INSERT_RANGE, UNSHARE_RANGE, and any unknown flags.
	if input.Mode&(fallocCollapseRange|fallocInsertRange|fallocUnshareRange) != 0 {
		return fuse.Status(syscall.EOPNOTSUPP)
	}
	const knownFlags = fallocKeepSize | fallocPunchHole | fallocCollapseRange | fallocZeroRange | fallocInsertRange | fallocUnshareRange
	if input.Mode&^knownFlags != 0 {
		return fuse.Status(syscall.EOPNOTSUPP)
	}

	offset := int64(input.Offset)
	length := int64(input.Length)
	if offset < 0 || length <= 0 || offset+length < 0 {
		return fuse.Status(syscall.EINVAL)
	}

	isPunchHole := input.Mode&fallocPunchHole != 0
	keepSize := input.Mode&fallocKeepSize != 0
	isZeroRange := input.Mode&fallocZeroRange != 0

	// Linux requires FALLOC_FL_PUNCH_HOLE to be used with FALLOC_FL_KEEP_SIZE.
	if isPunchHole && !keepSize {
		return fuse.Status(syscall.EINVAL)
	}

	// 2. Punch-hole and Zero-range operations.
	if isPunchHole || isZeroRange {
		punchKeepSize := keepSize
		if isPunchHole {
			punchKeepSize = true
		}

		syncCtx, syncCancel := fs.makeSyncContext(cancel)
		if err := fs.syncFileToService(syncCtx, input.NodeId); err != nil {
			syncCancel()
			return fs.grpcErrorToStatus("PunchHole:Sync", err)
		}
		syncCancel()

		ctx, cancelFunc := fs.makeContext(cancel)
		defer cancelFunc()

		resp, err := fs.client.PunchHole(ctx, &pb.PunchHoleRequest{
			VolumeId: fs.volumeID,
			Inode:    input.NodeId,
			Offset:   offset,
			Length:   length,
			KeepSize: punchKeepSize,
		})
		if err != nil {
			return fs.grpcErrorToStatus("PunchHole", err)
		}
		if resp.GetError() != 0 {
			return fuse.Status(resp.GetError())
		}
		if fs.cache != nil && resp.GetStreamPosition() > 0 {
			fs.cache.RecordPosition(input.NodeId, resp.GetStreamPosition())
		}

		punchGen := fs.cache.PunchHole(input.NodeId, offset, length, punchKeepSize, time.Now())
		fs.invalidateManifest(input.NodeId)
		fs.cache.MarkSizeClean(input.NodeId, punchGen)
		return fuse.OK
	}

	// 3. FALLOC_FL_KEEP_SIZE alone: no-op (no pre-allocation reservation).
	if input.Mode == fallocKeepSize {
		return fuse.OK
	}

	// 4. Mode 0 (allocate): extend file size to offset+len if target exceeds size (truncate-up with holes).
	if input.Mode == fallocAllocRange {
		syncCtx, syncCancel := fs.makeSyncContext(cancel)
		if err := fs.syncFileToService(syncCtx, input.NodeId); err != nil {
			syncCancel()
			return fs.grpcErrorToStatus("Fallocate:Sync", err)
		}
		syncCancel()

		ctx, cancelFunc := fs.makeContext(cancel)
		defer cancelFunc()

		var curSize int64
		if clientSize, hasSize := fs.cache.GetSize(input.NodeId); hasSize {
			curSize = clientSize
		} else {
			attrResp, err := fs.client.GetAttr(ctx, &pb.GetAttrRequest{
				VolumeId: fs.volumeID,
				Inode:    input.NodeId,
			})
			if err != nil {
				return fs.grpcErrorToStatus("Fallocate:GetAttr", err)
			}
			if attrResp.GetError() != 0 {
				return fuse.Status(attrResp.GetError())
			}
			curSize = attrResp.GetAttr().GetInode().GetSize()
		}

		targetSize := offset + length
		if targetSize > curSize {
			truncGen := fs.cache.Truncate(input.NodeId, targetSize, time.Now())
			resp, err := fs.client.TruncateFile(ctx, &pb.TruncateFileRequest{
				VolumeId: fs.volumeID,
				Inode:    input.NodeId,
				Size:     targetSize,
			})
			if err != nil {
				return fs.grpcErrorToStatus("Fallocate:TruncateFile", err)
			}
			if resp.GetError() != 0 {
				return fuse.Status(resp.GetError())
			}
			if fs.cache != nil && resp.GetStreamPosition() > 0 {
				fs.cache.RecordPosition(input.NodeId, resp.GetStreamPosition())
			}
			fs.cache.MarkSizeClean(input.NodeId, truncGen)
			fs.invalidateManifest(input.NodeId)
		}
		return fuse.OK
	}

	return fuse.Status(syscall.EOPNOTSUPP)
}

func (fs *ObjectFS) Mkdir(cancel <-chan struct{}, input *fuse.MkdirIn, name string, out *fuse.EntryOut) fuse.Status {
	if len(name) > MaxNameLength {
		return fuse.Status(syscall.ENAMETOOLONG)
	}
	ctx, cancelFunc := fs.makeContext(cancel)
	defer cancelFunc()

	resp, err := fs.client.Mkdir(ctx, &pb.MkdirRequest{
		VolumeId:    fs.volumeID,
		ParentInode: input.NodeId,
		Name:        name,
		Mode:        input.Mode,
		Uid:         input.Uid,
		Gid:         input.Gid,
	})
	if err != nil {
		return fs.grpcErrorToStatus("Mkdir", err)
	}
	if resp.GetError() != 0 {
		return fuse.Status(resp.GetError())
	}
	attr := resp.GetAttr()
	if attr == nil {
		return fuse.EIO
	}
	if fs.cache != nil && resp.GetStreamPosition() > 0 {
		fs.cache.RecordPosition(input.NodeId, resp.GetStreamPosition())
	}
	fs.fillEntryOut(attr, out, input.NodeId, name)
	return fuse.OK
}

func (fs *ObjectFS) Create(cancel <-chan struct{}, input *fuse.CreateIn, name string, out *fuse.CreateOut) fuse.Status {
	if len(name) > MaxNameLength {
		return fuse.Status(syscall.ENAMETOOLONG)
	}
	ctx, cancelFunc := fs.makeContext(cancel)
	defer cancelFunc()

	resp, err := fs.client.CreateFile(ctx, &pb.CreateFileRequest{
		VolumeId:    fs.volumeID,
		ParentInode: input.NodeId,
		Name:        name,
		Mode:        input.Mode,
		Uid:         input.Uid,
		Gid:         input.Gid,
	})
	if err != nil {
		return fs.grpcErrorToStatus("CreateFile", err)
	}
	if resp.GetError() != 0 {
		return fuse.Status(resp.GetError())
	}
	attr := resp.GetAttr()
	if attr == nil {
		return fuse.EIO
	}

	if fs.cache != nil && resp.GetStreamPosition() > 0 {
		fs.cache.RecordPosition(input.NodeId, resp.GetStreamPosition())
	}
	fs.cache.Put(attr.GetInode().GetIno(), []byte{}, time.Now(), "")
	fs.fillEntryOut(attr, &out.EntryOut, input.NodeId, name)
	out.OpenOut.Fh = resp.GetFh()
	out.OpenOut.OpenFlags |= fuse.FOPEN_KEEP_CACHE
	return fuse.OK
}

func (fs *ObjectFS) Mknod(cancel <-chan struct{}, input *fuse.MknodIn, name string, out *fuse.EntryOut) fuse.Status {
	if len(name) > MaxNameLength {
		return fuse.Status(syscall.ENAMETOOLONG)
	}
	ctx, cancelFunc := fs.makeContext(cancel)
	defer cancelFunc()

	resp, err := fs.client.CreateFile(ctx, &pb.CreateFileRequest{
		VolumeId:    fs.volumeID,
		ParentInode: input.NodeId,
		Name:        name,
		Mode:        input.Mode,
		Rdev:        input.Rdev,
		Uid:         input.Uid,
		Gid:         input.Gid,
	})
	if err != nil {
		return fs.grpcErrorToStatus("CreateFile", err)
	}
	if resp.GetError() != 0 {
		return fuse.Status(resp.GetError())
	}
	attr := resp.GetAttr()
	if attr == nil {
		return fuse.EIO
	}
	if fs.cache != nil && resp.GetStreamPosition() > 0 {
		fs.cache.RecordPosition(input.NodeId, resp.GetStreamPosition())
	}
	fs.fillEntryOut(attr, out, input.NodeId, name)
	return fuse.OK
}

func (fs *ObjectFS) Unlink(cancel <-chan struct{}, header *fuse.InHeader, name string) fuse.Status {
	if len(name) > MaxNameLength {
		return fuse.Status(syscall.ENAMETOOLONG)
	}
	ctx, cancelFunc := fs.makeContext(cancel)
	defer cancelFunc()

	resp, err := fs.client.Unlink(ctx, &pb.UnlinkRequest{
		VolumeId:    fs.volumeID,
		ParentInode: header.NodeId,
		Name:        name,
	})
	if err != nil {
		return fs.grpcErrorToStatus("Unlink", err)
	}
	if resp.GetError() != 0 {
		return fuse.Status(resp.GetError())
	}
	if fs.cache != nil && resp.GetStreamPosition() > 0 {
		fs.cache.RecordPosition(header.NodeId, resp.GetStreamPosition())
	}
	return fuse.OK
}

func (fs *ObjectFS) Rmdir(cancel <-chan struct{}, header *fuse.InHeader, name string) fuse.Status {
	if len(name) > MaxNameLength {
		return fuse.Status(syscall.ENAMETOOLONG)
	}
	ctx, cancelFunc := fs.makeContext(cancel)
	defer cancelFunc()

	resp, err := fs.client.Rmdir(ctx, &pb.RmdirRequest{
		VolumeId:    fs.volumeID,
		ParentInode: header.NodeId,
		Name:        name,
	})
	if err != nil {
		return fs.grpcErrorToStatus("Rmdir", err)
	}
	if resp.GetError() != 0 {
		return fuse.Status(resp.GetError())
	}
	if fs.cache != nil && resp.GetStreamPosition() > 0 {
		fs.cache.RecordPosition(header.NodeId, resp.GetStreamPosition())
	}
	return fuse.OK
}

func (fs *ObjectFS) Rename(cancel <-chan struct{}, input *fuse.RenameIn, oldName string, newName string) fuse.Status {
	if len(oldName) > MaxNameLength || len(newName) > MaxNameLength {
		return fuse.Status(syscall.ENAMETOOLONG)
	}
	ctx, cancelFunc := fs.makeContext(cancel)
	defer cancelFunc()

	resp, err := fs.client.Rename(ctx, &pb.RenameRequest{
		VolumeId:       fs.volumeID,
		OldParentInode: input.NodeId,
		OldName:        oldName,
		NewParentInode: input.Newdir,
		NewName:        newName,
	})
	if err != nil {
		return fs.grpcErrorToStatus("Rename", err)
	}
	if resp.GetError() != 0 {
		return fuse.Status(resp.GetError())
	}
	if fs.cache != nil && resp.GetStreamPosition() > 0 {
		pos := resp.GetStreamPosition()
		fs.cache.RecordPosition(input.NodeId, pos)
		fs.cache.RecordPosition(input.Newdir, pos)
		if attr := resp.GetAttr(); attr != nil && attr.GetInode() != nil {
			fs.cache.RecordPosition(attr.GetInode().GetIno(), pos)
		}
	}
	return fuse.OK
}

func (fs *ObjectFS) Open(cancel <-chan struct{}, input *fuse.OpenIn, out *fuse.OpenOut) fuse.Status {
	ctx, cancelFunc := fs.makeContext(cancel)
	defer cancelFunc()

	resp, err := fs.client.Open(ctx, &pb.OpenRequest{
		VolumeId: fs.volumeID,
		Inode:    input.NodeId,
		Flags:    input.Flags,
	})
	if err != nil {
		return fs.grpcErrorToStatus("Open", err)
	}
	if resp.GetError() != 0 {
		return fuse.Status(resp.GetError())
	}
	out.Fh = resp.GetFh()
	out.OpenFlags |= fuse.FOPEN_KEEP_CACHE

	if fs.diskCache != nil {
		fs.startPrefetchOnOpen(input.NodeId, out.Fh)
	}

	return fuse.OK
}

func (fs *ObjectFS) OpenDir(cancel <-chan struct{}, input *fuse.OpenIn, out *fuse.OpenOut) fuse.Status {
	out.Fh = input.NodeId
	return fuse.OK
}

func (fs *ObjectFS) ReadDir(cancel <-chan struct{}, input *fuse.ReadIn, out *fuse.DirEntryList) fuse.Status {
	ctx, cancelFunc := fs.makeContext(cancel)
	defer cancelFunc()

	resp, err := fs.client.ReadDir(ctx, &pb.ReadDirRequest{
		VolumeId: fs.volumeID,
		Inode:    input.NodeId,
	})
	if err != nil {
		return fs.grpcErrorToStatus("ReadDir", err)
	}
	if resp.GetError() != 0 {
		return fuse.Status(resp.GetError())
	}

	entries := resp.GetEntries()
	for i := int(input.Offset); i < len(entries); i++ {
		entry := entries[i]
		inoRow := entry.GetInode()
		mode := inoRow.GetMode()
		if inoRow.GetIsDir() {
			mode = (mode & ^uint32(syscall.S_IFMT)) | syscall.S_IFDIR
		} else if (mode & syscall.S_IFMT) == 0 {
			mode |= syscall.S_IFREG
		}
		if !out.AddDirEntry(fuse.DirEntry{
			Mode: mode,
			Name: entry.GetName(),
			Ino:  inoRow.GetIno(),
			Off:  uint64(i + 1),
		}) {
			break
		}
	}
	return fuse.OK
}

func (fs *ObjectFS) ReadDirPlus(cancel <-chan struct{}, input *fuse.ReadIn, out *fuse.DirEntryList) fuse.Status {
	ctx, cancelFunc := fs.makeContext(cancel)
	defer cancelFunc()

	resp, err := fs.client.ReadDir(ctx, &pb.ReadDirRequest{
		VolumeId: fs.volumeID,
		Inode:    input.NodeId,
	})
	if err != nil {
		return fs.grpcErrorToStatus("ReadDir", err)
	}
	if resp.GetError() != 0 {
		return fuse.Status(resp.GetError())
	}

	entries := resp.GetEntries()
	for i := int(input.Offset); i < len(entries); i++ {
		entry := entries[i]
		inoRow := entry.GetInode()
		mode := inoRow.GetMode()
		if inoRow.GetIsDir() {
			mode = (mode & ^uint32(syscall.S_IFMT)) | syscall.S_IFDIR
		} else if (mode & syscall.S_IFMT) == 0 {
			mode |= syscall.S_IFREG
		}
		entryOut := out.AddDirLookupEntry(fuse.DirEntry{
			Mode: mode,
			Name: entry.GetName(),
			Ino:  inoRow.GetIno(),
			Off:  uint64(i + 1),
		})
		if entryOut == nil {
			break
		}
		fs.fillEntryOut(entry, entryOut, input.NodeId, entry.GetName())
	}
	return fuse.OK
}

func (fs *ObjectFS) Symlink(cancel <-chan struct{}, header *fuse.InHeader, pointedTo string, linkName string, out *fuse.EntryOut) fuse.Status {
	if len(linkName) > MaxNameLength {
		return fuse.Status(syscall.ENAMETOOLONG)
	}
	ctx, cancelFunc := fs.makeContext(cancel)
	defer cancelFunc()

	resp, err := fs.client.Symlink(ctx, &pb.SymlinkRequest{
		VolumeId:    fs.volumeID,
		ParentInode: header.NodeId,
		Name:        linkName,
		Target:      pointedTo,
		Uid:         header.Uid,
		Gid:         header.Gid,
	})
	if err != nil {
		return fs.grpcErrorToStatus("Symlink", err)
	}
	if resp.GetError() != 0 {
		return fuse.Status(resp.GetError())
	}
	attr := resp.GetAttr()
	if attr == nil {
		return fuse.EIO
	}
	if fs.cache != nil && resp.GetStreamPosition() > 0 {
		fs.cache.RecordPosition(header.NodeId, resp.GetStreamPosition())
	}
	fs.fillEntryOut(attr, out, header.NodeId, linkName)
	return fuse.OK
}

func (fs *ObjectFS) Readlink(cancel <-chan struct{}, header *fuse.InHeader) ([]byte, fuse.Status) {
	ctx, cancelFunc := fs.makeContext(cancel)
	defer cancelFunc()

	resp, err := fs.client.Readlink(ctx, &pb.ReadlinkRequest{
		VolumeId: fs.volumeID,
		Inode:    header.NodeId,
	})
	if err != nil {
		return nil, fs.grpcErrorToStatus("Readlink", err)
	}
	if resp.GetError() != 0 {
		return nil, fuse.Status(resp.GetError())
	}
	return []byte(resp.GetTarget()), fuse.OK
}

func (fs *ObjectFS) Link(cancel <-chan struct{}, input *fuse.LinkIn, filename string, out *fuse.EntryOut) fuse.Status {
	if len(filename) > MaxNameLength {
		return fuse.Status(syscall.ENAMETOOLONG)
	}
	ctx, cancelFunc := fs.makeContext(cancel)
	defer cancelFunc()

	resp, err := fs.client.Link(ctx, &pb.LinkRequest{
		VolumeId:       fs.volumeID,
		OldInode:       input.Oldnodeid,
		NewParentInode: input.NodeId,
		NewName:        filename,
	})
	if err != nil {
		return fs.grpcErrorToStatus("Link", err)
	}
	if resp.GetError() != 0 {
		return fuse.Status(resp.GetError())
	}
	attr := resp.GetAttr()
	if attr == nil {
		return fuse.EIO
	}
	if fs.cache != nil && resp.GetStreamPosition() > 0 {
		pos := resp.GetStreamPosition()
		fs.cache.RecordPosition(input.NodeId, pos)
		fs.cache.RecordPosition(input.Oldnodeid, pos)
	}
	fs.fillEntryOut(attr, out, input.NodeId, filename)
	return fuse.OK
}

func (fs *ObjectFS) Read(cancel <-chan struct{}, input *fuse.ReadIn, buf []byte) (fuse.ReadResult, fuse.Status) {
	// Read serves data through four source tiers:
	// Tier 1: In-memory NodeCache (unflushed dirty writes must win so local updates are immediately visible).
	// Tier 2: File manifest inline data (small files stored directly within the metadata store).
	// Tier 3: Persistent on-disk blob cache (verified content-addressed blobs read from disk without network I/O).
	// Tier 4: Controller ReadFile RPC (fallback if not yet cached or manifest unavailable; fetched chunks are saved to disk cache).
	offset := int64(input.Offset)
	size := int64(input.Size)

	if size <= 0 {
		return fuse.ReadResultData([]byte{}), fuse.OK
	}

	if cachedBytes, ok := fs.cache.GetRange(input.NodeId, offset, size); ok {
		return fuse.ReadResultData(cachedBytes), fuse.OK
	}

	ctx, cancelFunc := fs.makeContext(cancel)
	defer cancelFunc()

	var manifest *fileManifest
	if fs.diskCache != nil {
		var mErr error
		manifest, mErr = fs.getOrLoadManifest(ctx, input.NodeId)
		if mErr != nil {
			klog.V(4).Infof("getOrLoadManifest failed for inode %d: %v", input.NodeId, mErr)
		}
	}

	cs := int64(DefaultChunkSize)
	if manifest != nil && manifest.chunkSize > 0 {
		cs = int64(manifest.chunkSize)
	}

	startChunk := int(offset / cs)
	endChunk := int((offset + size - 1) / cs)

	// Sequential read detection & prefetch extension
	if fs.diskCache != nil && manifest != nil {
		fs.prefetchMu.Lock()
		lastRead := fs.lastReadChunk[input.Fh]
		furthest := fs.furthestPrefetched[input.Fh]
		fs.lastReadChunk[input.Fh] = endChunk
		if startChunk >= lastRead && endChunk+DefaultPrefetchWindow > furthest {
			newEnd := endChunk + DefaultPrefetchWindow
			if newEnd > len(manifest.chunkSHAs) {
				newEnd = len(manifest.chunkSHAs)
			}
			fs.furthestPrefetched[input.Fh] = newEnd
			pHandle := fs.activePrefetches[input.Fh]
			fs.prefetchMu.Unlock()
			if pHandle != nil {
				go fs.prefetchChunks(ctx, manifest, furthest, newEnd)
			}
		} else {
			fs.prefetchMu.Unlock()
		}
	}

	clientSize, hasClientEntry := fs.cache.GetSize(input.NodeId)
	var totalSize int64
	if hasClientEntry {
		totalSize = clientSize
	} else if manifest != nil {
		totalSize = manifest.totalSize
	}

	numChunks := endChunk - startChunk + 1

	type chunkSlot struct {
		data []byte
	}
	slots := make([]chunkSlot, numChunks)

	type uncachedChunk struct {
		pIdx       int
		chunkIdx   int
		chunkStart int64
		sha        string
	}
	var uncached []uncachedChunk

	for i := startChunk; i <= endChunk; i++ {
		pIdx := i - startChunk

		// Tier 1: Check in-memory dirty cache (or memory cache)
		if cachedChunk, ok := fs.cache.GetChunk(input.NodeId, i); ok {
			slots[pIdx].data = cachedChunk
			continue
		}

		// Tier 2: Check inline data from manifest
		if manifest != nil && len(manifest.inlineData) > 0 && i == 0 {
			slots[pIdx].data = manifest.inlineData
			continue
		}

		// Tier 3: Check on-disk blob cache by SHA
		var chunkSHA string
		if manifest != nil && i < len(manifest.chunkSHAs) {
			chunkSHA = manifest.chunkSHAs[i]
		}
		if fs.diskCache != nil && chunkSHA != "" && fs.diskCache.HasBlob(chunkSHA) {
			data, err := fs.diskCache.ReadBlob(chunkSHA)
			if err == nil {
				slots[pIdx].data = data
				continue
			}
			klog.V(4).Infof("ReadBlob failed for cached sha %s: %v", chunkSHA, err)
		}

		uncached = append(uncached, uncachedChunk{
			pIdx:       pIdx,
			chunkIdx:   i,
			chunkStart: int64(i) * cs,
			sha:        chunkSHA,
		})
	}

	fetchChunk := func(fCtx context.Context, uc uncachedChunk) ([]byte, int64, fuse.Status, error) {
		// Tier 3 (disk fetch): fetch into disk cache if SHA is available
		if fs.diskCache != nil && uc.sha != "" {
			data, err := fs.diskCache.FetchBlob(fCtx, fs.client, uc.sha)
			if err == nil {
				return data, totalSize, fuse.OK, nil
			}
			klog.V(4).Infof("FetchBlob failed for sha %s, falling back to ReadFile: %v", uc.sha, err)
		}

		// Tier 4: Fallback to controller ReadFile RPC
		readGen := fs.cache.GetGeneration(input.NodeId)
		resp, err := fs.client.ReadFile(fCtx, &pb.ReadFileRequest{
			VolumeId: fs.volumeID,
			Inode:    input.NodeId,
			Offset:   uc.chunkStart,
			Size:     cs,
		})
		if err != nil {
			return nil, 0, fs.grpcErrorToStatus("ReadFile", err), err
		}
		if resp.GetError() != 0 {
			return nil, 0, fuse.Status(resp.GetError()), nil
		}
		fetchedData := resp.GetData()
		if (int64(len(fetchedData)) == cs || resp.GetEof()) && len(fetchedData) > 0 {
			// When disk cache is configured, clean chunks live ONLY on disk (and kernel page cache)
			// to avoid duplicating memory. In-memory NodeCache is only updated when disk cache is absent.
			if fs.diskCache != nil {
				sum := sha256.Sum256(fetchedData)
				sha := hex.EncodeToString(sum[:])
				if pErr := fs.diskCache.PutBlob(sha, fetchedData); pErr != nil {
					klog.V(4).Infof("cache PutBlob failed: %v", pErr)
				}
			} else {
				fs.cache.PutChunk(input.NodeId, uc.chunkIdx, uint32(cs), resp.GetTotalSize(), fetchedData, time.Now(), readGen)
			}
		}
		return fetchedData, resp.GetTotalSize(), fuse.OK, nil
	}

	if len(uncached) == 1 {
		u := uncached[0]
		data, tSize, st, err := fetchChunk(ctx, u)
		if st != fuse.OK {
			return nil, st
		}
		if err != nil {
			return nil, fs.grpcErrorToStatus("ReadFile", err)
		}
		if tSize > totalSize {
			totalSize = tSize
		}
		slots[u.pIdx].data = data
	} else if len(uncached) > 1 {
		var wg sync.WaitGroup
		var errMu sync.Mutex
		var firstErr error
		var firstStatus fuse.Status = fuse.OK

		fetchCtx, fetchCancel := context.WithCancel(ctx)
		defer fetchCancel()

		for _, u := range uncached {
			wg.Add(1)
			go func(uc uncachedChunk) {
				defer wg.Done()

				if fetchCtx.Err() != nil {
					return
				}

				data, tSize, st, err := fetchChunk(fetchCtx, uc)
				if st != fuse.OK || err != nil {
					errMu.Lock()
					if firstErr == nil && firstStatus == fuse.OK {
						firstErr = err
						firstStatus = st
						fetchCancel()
					}
					errMu.Unlock()
					return
				}

				errMu.Lock()
				if tSize > totalSize {
					totalSize = tSize
				}
				errMu.Unlock()

				slots[uc.pIdx].data = data
			}(u)
		}
		wg.Wait()

		if firstStatus != fuse.OK {
			return nil, firstStatus
		}
		if firstErr != nil {
			return nil, fs.grpcErrorToStatus("ReadFile", firstErr)
		}
		if err := ctx.Err(); err != nil {
			return nil, fs.grpcErrorToStatus("ReadFile", err)
		}
	}

	var result []byte
	for i := startChunk; i <= endChunk; i++ {
		pIdx := i - startChunk
		chunkStart := int64(i) * cs
		rStart := offset - chunkStart
		if rStart < 0 {
			rStart = 0
		}
		rEnd := offset + size - chunkStart
		if rEnd > cs {
			rEnd = cs
		}

		chunkData := slots[pIdx].data
		chunkLen := int64(len(chunkData))
		if totalSize > 0 {
			expectedLen := cs
			if int64(i+1)*cs > totalSize {
				expectedLen = totalSize - chunkStart
			}
			if expectedLen < 0 {
				expectedLen = 0
			}
			if chunkLen < expectedLen {
				padded := make([]byte, expectedLen)
				copy(padded, chunkData)
				chunkData = padded
				chunkLen = expectedLen
			}
		}

		if rStart < chunkLen {
			end := rEnd
			if end > chunkLen {
				end = chunkLen
			}
			result = append(result, chunkData[rStart:end]...)
		}
	}

	return fuse.ReadResultData(result), fuse.OK
}

func (fs *ObjectFS) Write(cancel <-chan struct{}, input *fuse.WriteIn, data []byte) (uint32, fuse.Status) {
	origLen := uint32(len(data))
	if input.WriteFlags&fuse.WRITE_CACHE != 0 {
		clientSize, hasSize := fs.cache.GetSize(input.NodeId)
		if !hasSize {
			ctx, cancelFunc := fs.makeContext(cancel)
			resp, err := fs.client.GetAttr(ctx, &pb.GetAttrRequest{
				VolumeId: fs.volumeID,
				Inode:    input.NodeId,
			})
			cancelFunc()
			if err != nil {
				return 0, fs.grpcErrorToStatus("Write:GetAttr", err)
			}
			if resp.GetError() != 0 {
				return 0, fuse.Status(resp.GetError())
			}
			if resp.GetAttr() == nil || resp.GetAttr().GetInode() == nil {
				return 0, fuse.EIO
			}
			clientSize = resp.GetAttr().GetInode().GetSize()
			hasSize = true
		}
		if hasSize {
			if int64(input.Offset) >= clientSize {
				// Mapped page writeback completely beyond EOF (e.g. truncated page) is discarded.
				klog.V(4).Infof("Write: discarding WRITE_CACHE write beyond EOF: inode=%d, offset=%d, length=%d, clientSize=%d", input.NodeId, input.Offset, len(data), clientSize)
				return origLen, fuse.OK
			}
			if int64(input.Offset)+int64(len(data)) > clientSize {
				// Page-aligned writeback must not grow the file beyond EOF.
				klog.V(4).Infof("Write: clamping WRITE_CACHE write to EOF: inode=%d, offset=%d, length=%d, clientSize=%d", input.NodeId, input.Offset, len(data), clientSize)
				data = data[:clientSize-int64(input.Offset)]
			}
		}
	}

	// If any chunk touched by write is not fully overwritten and not in cache, load existing chunk content into cache first
	cs := int64(DefaultChunkSize)
	startChunk := int64(input.Offset) / cs
	endChunk := (int64(input.Offset) + int64(len(data)) - 1) / cs

	for i := startChunk; i <= endChunk; i++ {
		chunkStart := i * cs
		chunkEnd := chunkStart + cs
		wStart := int64(input.Offset)
		if wStart < chunkStart {
			wStart = chunkStart
		}
		wEnd := int64(input.Offset) + int64(len(data))
		if wEnd > chunkEnd {
			wEnd = chunkEnd
		}

		if wStart > chunkStart || wEnd < chunkEnd {
			clientSize, hasSize := fs.cache.GetSize(input.NodeId)
			if hasSize && chunkStart >= clientSize {
				continue
			}
			if _, ok := fs.cache.GetChunk(input.NodeId, int(i)); !ok {
				ctx, cancelFunc := fs.makeSyncContext(cancel)
				readGen := fs.cache.GetGeneration(input.NodeId)
				resp, err := fs.client.ReadFile(ctx, &pb.ReadFileRequest{
					VolumeId: fs.volumeID,
					Inode:    input.NodeId,
					Offset:   chunkStart,
					Size:     cs,
				})
				cancelFunc()
				if err != nil {
					return 0, fs.grpcErrorToStatus("Write:ReadFile", err)
				}
				if resp.GetError() != 0 {
					return 0, fuse.Status(resp.GetError())
				}
				if len(resp.GetData()) > 0 {
					fs.cache.PutChunk(input.NodeId, int(i), DefaultChunkSize, resp.GetTotalSize(), resp.GetData(), time.Now(), readGen)
				}
			}
		}
	}

	if fs.cache.WouldDirtyOverflow(int64(len(data))) {
		syncCtx, cancelFunc := fs.makeSyncContext(cancel)
		if err := fs.syncVolumeBlocking(syncCtx); err != nil {
			cancelFunc()
			return 0, fs.grpcErrorToStatus("Write.Sync", err)
		}
		cancelFunc()
	}

	// Buffer write locally and mark dirty per chunk
	fs.cache.WriteAt(input.NodeId, int64(input.Offset), data, time.Now())
	return origLen, fuse.OK
}

// MaxSyncRunChunks is the maximum number of contiguous chunks coalesced into a single WriteFile RPC.
const MaxSyncRunChunks = 32

// MaxSyncRunBytes is the maximum total payload size for a coalesced WriteFile RPC,
// capped at 2 MiB to remain comfortably within the default 4 MiB gRPC receive limit.
const MaxSyncRunBytes = 2 * 1024 * 1024

func (fs *ObjectFS) pushDirtyChunksToStaging(ctx context.Context, inode uint64) (int64, [][]int, map[int]Generation, error) {
	dirtySize, sizeGen, hasDirtySize := fs.cache.GetDirtySize(inode)
	if hasDirtySize {
		resp, err := fs.client.TruncateFile(ctx, &pb.TruncateFileRequest{
			VolumeId: fs.volumeID,
			Inode:    inode,
			Size:     dirtySize,
		})
		if err != nil {
			fs.logRPCError("TruncateFile", err)
			return 0, nil, nil, err
		}
		if resp.GetError() != 0 {
			return 0, nil, nil, syscall.Errno(resp.GetError())
		}
		if fs.cache != nil && resp.GetStreamPosition() > 0 {
			fs.cache.RecordPosition(inode, resp.GetStreamPosition())
		}
		fs.cache.MarkSizeClean(inode, sizeGen)
	}

	dirtyChunks, chunkGens, chunkSize, entrySize, isDirty := fs.cache.GetDirtyChunks(inode)
	if !isDirty || len(dirtyChunks) == 0 {
		return 0, nil, nil, nil
	}
	if chunkSize == 0 {
		chunkSize = DefaultChunkSize
	}

	indices := make([]int, 0, len(dirtyChunks))
	for idx, chunkData := range dirtyChunks {
		chunkOffset := int64(idx) * int64(chunkSize)
		if chunkOffset >= entrySize {
			fs.cache.MarkChunkClean(inode, idx, chunkGens[idx])
			continue
		}
		if chunkOffset+int64(len(chunkData)) > entrySize {
			dirtyChunks[idx] = chunkData[:entrySize-chunkOffset]
		}
		indices = append(indices, idx)
	}
	if len(indices) == 0 {
		return 0, nil, nil, nil
	}
	sort.Ints(indices)

	// Coalesce contiguous chunk indices into runs capped at MaxSyncRunChunks (32 chunks)
	// and MaxSyncRunBytes (2 MiB).
	var runs [][]int
	var currentRun []int
	currentRunBytes := 0

	for _, idx := range indices {
		chunkData := dirtyChunks[idx]
		if len(currentRun) > 0 {
			prevIdx := currentRun[len(currentRun)-1]
			prevData := dirtyChunks[prevIdx]
			// A chunk can continue currentRun only if:
			// 1. It is contiguous with the previous chunk index.
			// 2. The previous chunk has full chunkSize length (otherwise concatenating would misalign byte offsets).
			// 3. Adding this chunk does not exceed MaxSyncRunChunks.
			// 4. Adding this chunk does not exceed MaxSyncRunBytes.
			if idx != prevIdx+1 || len(prevData) != int(chunkSize) || len(currentRun) >= MaxSyncRunChunks || currentRunBytes+len(chunkData) > MaxSyncRunBytes {
				runs = append(runs, currentRun)
				currentRun = nil
				currentRunBytes = 0
			}
		}
		currentRun = append(currentRun, idx)
		currentRunBytes += len(chunkData)
	}
	if len(currentRun) > 0 {
		runs = append(runs, currentRun)
	}

	var lastEpoch int64
	var uncleanedRuns [][]int
	var drained bool

	for i := 0; i < len(runs); i++ {
		run := runs[i]
		startIdx := run[0]
		chunkOffset := int64(startIdx) * int64(chunkSize)

		var data []byte
		if len(run) == 1 {
			data = dirtyChunks[startIdx]
		} else {
			runLen := 0
			for _, idx := range run {
				runLen += len(dirtyChunks[idx])
			}
			data = make([]byte, 0, runLen)
			for _, idx := range run {
				data = append(data, dirtyChunks[idx]...)
			}
		}

		resp, err := fs.client.WriteFile(ctx, &pb.WriteFileRequest{
			VolumeId:  fs.volumeID,
			Inode:     inode,
			Offset:    chunkOffset,
			Data:      data,
			WriteMode: fs.writeMode,
		})
		if err != nil {
			fs.logRPCError("WriteFile", err)
			return 0, nil, nil, err
		}
		if resp.GetError() == int32(syscall.ENOSPC) {
			// Controller staging cap reached. Issue a blocking Fsync to pack and commit
			// what has been staged so far (or whatever other inodes staged if this inode
			// hasn't pushed anything yet), mark any pushed runs clean, then retry this run.
			if !drained {
				drained = true
				fResp, fErr := fs.client.Fsync(ctx, &pb.FsyncRequest{
					VolumeId:     fs.volumeID,
					Inode:        inode,
					Blocking:     true,
					StagingEpoch: lastEpoch,
				})
				if fErr != nil {
					fs.logRPCError("Fsync(ENOSPC)", fErr)
					return 0, nil, nil, fErr
				}
				if fResp.GetError() != 0 {
					return 0, nil, nil, syscall.Errno(fResp.GetError())
				}
				if fs.cache != nil && fResp.GetStreamPosition() > 0 {
					fs.cache.RecordPosition(inode, fResp.GetStreamPosition())
				}
				for _, r := range uncleanedRuns {
					for _, idx := range r {
						fs.cache.MarkChunkClean(inode, idx, chunkGens[idx])
					}
				}
				uncleanedRuns = nil
				lastEpoch = fResp.GetStagingEpoch()

				// Retry the current run now that controller staging is cleared
				i--
				continue
			}
			return 0, nil, nil, syscall.ENOSPC
		}
		drained = false
		if resp.GetError() != 0 {
			return 0, nil, nil, syscall.Errno(resp.GetError())
		}
		lastEpoch = resp.GetStagingEpoch()
		uncleanedRuns = append(uncleanedRuns, run)
		if fs.diskCache != nil {
			for _, idx := range run {
				cData := dirtyChunks[idx]
				sum := sha256.Sum256(cData)
				sha := hex.EncodeToString(sum[:])
				if pErr := fs.diskCache.PutBlob(sha, cData); pErr != nil {
					klog.V(4).Infof("stage PutBlob failed: %v", pErr)
				}
			}
		}
	}

	return lastEpoch, uncleanedRuns, chunkGens, nil
}

type uncleanedItem struct {
	inode uint64
	runs  [][]int
	gens  map[int]Generation
}

func (fs *ObjectFS) syncVolumeBlocking(ctx context.Context) error {
	entries := fs.cache.GetDirtyEntries()
	if len(entries) == 0 {
		return nil
	}

	var allUncleaned []uncleanedItem
	var lastEpoch int64

	for _, entry := range entries {
		epoch, runs, gens, err := fs.pushDirtyChunksToStaging(ctx, entry.Inode)
		if err != nil {
			return err
		}
		if len(runs) > 0 {
			allUncleaned = append(allUncleaned, uncleanedItem{
				inode: entry.Inode,
				runs:  runs,
				gens:  gens,
			})
			lastEpoch = epoch
		}
	}

	if len(allUncleaned) == 0 {
		return nil
	}

	resp, err := fs.client.Fsync(ctx, &pb.FsyncRequest{
		VolumeId:     fs.volumeID,
		Inode:        1,
		Blocking:     true,
		StagingEpoch: lastEpoch,
	})
	if err != nil {
		fs.logRPCError("Fsync(volume)", err)
		return err
	}
	if resp.GetError() == int32(syscall.ESTALE) || (resp.GetStagingEpoch() != 0 && lastEpoch != 0 && resp.GetStagingEpoch() != lastEpoch) {
		return fs.syncVolumeBlocking(ctx)
	}
	if resp.GetError() != 0 {
		return syscall.Errno(resp.GetError())
	}

	for _, item := range allUncleaned {
		for _, run := range item.runs {
			for _, idx := range run {
				fs.cache.MarkChunkClean(item.inode, idx, item.gens[idx])
			}
		}
		fs.invalidateManifest(item.inode)
		if fs.cache != nil && resp.GetStreamPosition() > 0 {
			fs.cache.RecordPosition(item.inode, resp.GetStreamPosition())
		}
	}
	if fs.diskCache != nil {
		fs.cache.InvalidateAllClean()
	}
	return nil
}

func (fs *ObjectFS) syncVolumeNonBlocking(ctx context.Context, trigger pb.FsyncRequest_SyncTrigger) error {
	entries := fs.cache.GetDirtyEntries()
	if len(entries) == 0 {
		return nil
	}

	var allUncleaned []uncleanedItem
	var lastEpoch int64

	for _, entry := range entries {
		epoch, runs, gens, err := fs.pushDirtyChunksToStaging(ctx, entry.Inode)
		if err != nil {
			return err
		}
		if len(runs) > 0 {
			allUncleaned = append(allUncleaned, uncleanedItem{
				inode: entry.Inode,
				runs:  runs,
				gens:  gens,
			})
			lastEpoch = epoch
		}
	}

	if len(allUncleaned) == 0 {
		return nil
	}

	resp, err := fs.client.Fsync(ctx, &pb.FsyncRequest{
		VolumeId:     fs.volumeID,
		Inode:        1,
		Blocking:     false,
		StagingEpoch: lastEpoch,
		Trigger:      trigger,
	})
	if err != nil {
		fs.logRPCError("Fsync(volume)", err)
		return err
	}
	if resp.GetError() == int32(syscall.ESTALE) || (resp.GetStagingEpoch() != 0 && lastEpoch != 0 && resp.GetStagingEpoch() != lastEpoch) {
		return fs.syncVolumeNonBlocking(ctx, trigger)
	}
	if resp.GetError() != 0 {
		return syscall.Errno(resp.GetError())
	}

	for _, item := range allUncleaned {
		for _, run := range item.runs {
			for _, idx := range run {
				fs.cache.MarkChunkClean(item.inode, idx, item.gens[idx])
			}
		}
		fs.invalidateManifest(item.inode)
		if fs.cache != nil && resp.GetStreamPosition() > 0 {
			fs.cache.RecordPosition(item.inode, resp.GetStreamPosition())
		}
	}
	if fs.diskCache != nil {
		fs.cache.InvalidateAllClean()
	}
	return nil
}

func (fs *ObjectFS) syncClosedInodes(ctx context.Context) error {
	fs.closedInodesMu.Lock()
	if len(fs.closedInodes) == 0 {
		fs.closedInodesMu.Unlock()
		return nil
	}
	inodes := make([]uint64, 0, len(fs.closedInodes))
	for ino := range fs.closedInodes {
		inodes = append(inodes, ino)
	}
	fs.closedInodes = make(map[uint64]struct{})
	fs.closedInodesMu.Unlock()

	var errs []error
	for _, ino := range inodes {
		if err := fs.syncAndFsync(ctx, ino, false, pb.FsyncRequest_SYNC_TRIGGER_CLOSE); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

func (fs *ObjectFS) syncAndFsync(ctx context.Context, inode uint64, blocking bool, trigger pb.FsyncRequest_SyncTrigger) error {
	epoch, runs, chunkGens, err := fs.pushDirtyChunksToStaging(ctx, inode)
	if err != nil {
		return err
	}
	if len(runs) == 0 && !blocking {
		return nil
	}

	resp, err := fs.client.Fsync(ctx, &pb.FsyncRequest{
		VolumeId:     fs.volumeID,
		Inode:        inode,
		Blocking:     blocking,
		StagingEpoch: epoch,
		Trigger:      trigger,
	})
	if err != nil {
		fs.logRPCError("Fsync", err)
		return err
	}
	if resp.GetError() == int32(syscall.ESTALE) || (resp.GetStagingEpoch() != 0 && epoch != 0 && resp.GetStagingEpoch() != epoch) {
		// Controller restarted / staging epoch changed; re-push dirty chunks and retry Fsync
		newEpoch, newRuns, newGens, reErr := fs.pushDirtyChunksToStaging(ctx, inode)
		if reErr != nil {
			return reErr
		}
		retryResp, rErr := fs.client.Fsync(ctx, &pb.FsyncRequest{
			VolumeId:     fs.volumeID,
			Inode:        inode,
			Blocking:     blocking,
			StagingEpoch: newEpoch,
			Trigger:      trigger,
		})
		if rErr != nil {
			fs.logRPCError("Fsync(retry)", rErr)
			return rErr
		}
		if retryResp.GetError() != 0 {
			return syscall.Errno(retryResp.GetError())
		}
		for _, run := range newRuns {
			for _, idx := range run {
				fs.cache.MarkChunkClean(inode, idx, newGens[idx])
			}
		}
		fs.invalidateManifest(inode)
		if fs.cache != nil && retryResp.GetStreamPosition() > 0 {
			fs.cache.RecordPosition(inode, retryResp.GetStreamPosition())
		}
		if fs.diskCache != nil {
			fs.cache.InvalidateAllClean()
		}
		return nil
	}
	if resp.GetError() != 0 {
		return syscall.Errno(resp.GetError())
	}

	for _, run := range runs {
		for _, idx := range run {
			fs.cache.MarkChunkClean(inode, idx, chunkGens[idx])
		}
	}
	fs.invalidateManifest(inode)
	if fs.cache != nil && resp.GetStreamPosition() > 0 {
		fs.cache.RecordPosition(inode, resp.GetStreamPosition())
	}
	if fs.diskCache != nil {
		fs.cache.InvalidateAllClean()
	}
	return nil
}

func (fs *ObjectFS) syncFileToService(ctx context.Context, inode uint64) error {
	return fs.syncAndFsync(ctx, inode, true, pb.FsyncRequest_SYNC_TRIGGER_UNSPECIFIED)
}

func (fs *ObjectFS) Flush(cancel <-chan struct{}, input *fuse.FlushIn) fuse.Status {
	ctx, cancelFunc := fs.makeSyncContext(cancel)
	defer cancelFunc()

	_, _, _, err := fs.pushDirtyChunksToStaging(ctx, input.NodeId)
	if err != nil {
		return fs.grpcErrorToStatus("Flush", err)
	}
	return fuse.OK
}

func (fs *ObjectFS) Fsync(cancel <-chan struct{}, input *fuse.FsyncIn) fuse.Status {
	ctx, cancelFunc := fs.makeSyncContext(cancel)
	defer cancelFunc()

	if err := fs.syncAndFsync(ctx, input.NodeId, true, pb.FsyncRequest_SYNC_TRIGGER_UNSPECIFIED); err != nil {
		return fs.grpcErrorToStatus("Fsync", err)
	}
	return fuse.OK
}

func (fs *ObjectFS) Release(cancel <-chan struct{}, input *fuse.ReleaseIn) {
	fh := input.Fh
	fs.prefetchMu.Lock()
	if handle, ok := fs.activePrefetches[fh]; ok {
		handle.cancel()
		delete(fs.activePrefetches, fh)
	}
	delete(fs.lastReadChunk, fh)
	delete(fs.furthestPrefetched, fh)
	fs.prefetchMu.Unlock()

	if fs.syncOnClose {
		if fs.cache.IsInodeDirty(input.NodeId) {
			fs.closedInodesMu.Lock()
			if fs.closedInodes == nil {
				fs.closedInodes = make(map[uint64]struct{})
			}
			fs.closedInodes[input.NodeId] = struct{}{}
			fs.closedInodesMu.Unlock()
			select {
			case fs.syncTrigger <- struct{}{}:
			default:
			}
		}
	}

	ctx, cancelFunc := fs.makeContext(cancel)
	defer cancelFunc()

	resp, err := fs.client.Release(ctx, &pb.ReleaseRequest{
		VolumeId: fs.volumeID,
		Inode:    input.NodeId,
		Fh:       input.Fh,
	})
	if err != nil {
		fs.logRPCError("Release", err)
		klog.Warningf("Release RPC failed for volume %s, inode %d, fh %d: %v", fs.volumeID, input.NodeId, input.Fh, err)
		return
	}
	if resp.GetError() != 0 {
		klog.Warningf("Release returned error %d for volume %s, inode %d, fh %d", resp.GetError(), fs.volumeID, input.NodeId, input.Fh)
	}
}

func (fs *ObjectFS) StatFs(cancel <-chan struct{}, input *fuse.InHeader, out *fuse.StatfsOut) fuse.Status {
	fs.statfsMu.Lock()
	if time.Now().Before(fs.statfsExpiry) {
		*out = fs.statfsCache
		fs.statfsMu.Unlock()
		return fuse.OK
	}
	fs.statfsMu.Unlock()

	if fs.client == nil {
		return fuse.EIO
	}

	ctx, cancelFunc := fs.makeContext(cancel)
	defer cancelFunc()

	resp, err := fs.client.GetVolumeStats(ctx, &pb.GetVolumeStatsRequest{VolumeId: fs.volumeID})
	if err != nil {
		fs.logRPCError("GetVolumeStats", err)
		return fuse.EIO
	}
	if resp.GetError() != 0 {
		return fuse.EIO
	}

	const blockSize = 4096
	var total, avail uint64
	if resp.GetTotalBytes() > 0 {
		total = uint64(resp.GetTotalBytes())
	}
	if resp.GetAvailableBytes() > 0 {
		avail = uint64(resp.GetAvailableBytes())
	}
	var files, ffree uint64
	if resp.GetTotalInodes() > 0 {
		files = uint64(resp.GetTotalInodes())
	}
	if resp.GetFreeInodes() > 0 {
		ffree = uint64(resp.GetFreeInodes())
	}

	res := fuse.StatfsOut{
		Blocks:  total / blockSize,
		Bfree:   avail / blockSize,
		Bavail:  avail / blockSize,
		Bsize:   blockSize,
		Frsize:  blockSize,
		Files:   files,
		Ffree:   ffree,
		NameLen: 255,
	}

	ttl := fs.statfsTTL
	if ttl <= 0 {
		ttl = time.Second
	}

	fs.statfsMu.Lock()
	fs.statfsCache = res
	fs.statfsExpiry = time.Now().Add(ttl)
	fs.statfsMu.Unlock()

	*out = res
	return fuse.OK
}

func (fs *ObjectFS) Access(cancel <-chan struct{}, input *fuse.AccessIn) fuse.Status {
	return fuse.OK
}

type notifyEvent struct {
	Op     string // "inode", "entry", "delete"
	Inode  uint64
	Parent uint64
	Child  uint64
	Name   string
	Off    int64
	Length int64
}

func (fs *ObjectFS) notifyInode(ino uint64, off, length int64) {
	if fs.notifyHook != nil {
		fs.notifyHook(notifyEvent{Op: "inode", Inode: ino, Off: off, Length: length})
	}
	if fs.server != nil {
		if st := fs.server.InodeNotify(ino, off, length); !st.Ok() {
			if st == fuse.ENOENT {
				klog.V(4).Infof("InodeNotify(%d, off=%d, len=%d) returned ENOENT (kernel does not cache inode)", ino, off, length)
			} else {
				klog.V(2).Infof("InodeNotify(%d, off=%d, len=%d) returned status %v", ino, off, length, st)
			}
		}
	}
}

func (fs *ObjectFS) notifyEntry(parent uint64, name string) {
	if fs.notifyHook != nil {
		fs.notifyHook(notifyEvent{Op: "entry", Parent: parent, Name: name})
	}
	if fs.server != nil {
		if st := fs.server.EntryNotify(parent, name); !st.Ok() {
			if st == fuse.ENOENT {
				klog.V(4).Infof("EntryNotify(parent=%d, name=%q) returned ENOENT (kernel does not cache entry)", parent, name)
			} else {
				klog.V(2).Infof("EntryNotify(parent=%d, name=%q) returned status %v", parent, name, st)
			}
		}
	}
}

func (fs *ObjectFS) notifyDelete(parent, child uint64, name string) {
	if fs.notifyHook != nil {
		fs.notifyHook(notifyEvent{Op: "delete", Parent: parent, Child: child, Name: name})
	}
	if fs.server != nil {
		if st := fs.server.DeleteNotify(parent, child, name); !st.Ok() {
			if st == fuse.ENOENT {
				klog.V(4).Infof("DeleteNotify(parent=%d, child=%d, name=%q) returned ENOENT (kernel does not cache entry)", parent, child, name)
			} else {
				klog.V(2).Infof("DeleteNotify(parent=%d, child=%d, name=%q) returned status %v", parent, child, name, st)
			}
		}
	}
}

func handleWatchEvent(ev *pb.WatchVolumeResponse, cache *NodeCache, fs []*ObjectFS) {
	if ev == nil || ev.GetEventType() == pb.WatchEventType_EVENT_CHECKPOINT_REQUEST {
		return
	}
	ino := ev.GetInode()
	isDirty := ino != 0 && cache != nil && cache.IsInodeDirty(ino)

	eventPos := ev.GetStreamPosition()
	var targets []uint64
	if ino != 0 {
		targets = append(targets, ino)
	}
	if parent := ev.GetParentInode(); parent != 0 {
		targets = append(targets, parent)
	}
	if oldParent := ev.GetOldParentInode(); oldParent != 0 {
		targets = append(targets, oldParent)
	}

	skip := false
	if cache != nil && eventPos > 0 && len(targets) > 0 {
		allKnown := true
		for _, target := range targets {
			if eventPos > cache.KnownPosition(target) {
				allKnown = false
				break
			}
		}
		skip = allKnown
	}

	klog.V(4).Infof("handleWatchEvent: event_type=%v, inode=%d, parent=%d, name=%q, old_parent=%d, old_name=%q, pos=%d, dirty_skip=%v, pos_skip=%v",
		ev.GetEventType(), ino, ev.GetParentInode(), ev.GetName(), ev.GetOldParentInode(), ev.GetOldName(), eventPos, isDirty, skip)
	// Skip notifications for inodes the local cache holds dirty (the local writer wins locally; the #222/#263 rules)
	// or when event.position <= KnownPosition(ino) for every inode the event targets.
	if isDirty || skip {
		return
	}

	if ino != 0 {
		if cache != nil {
			cache.InvalidateIfNotDirty(ino)
		}
		for _, f := range fs {
			if f != nil {
				f.invalidateManifest(ino)
			}
		}
	}

	for _, f := range fs {
		if f == nil {
			continue
		}
		switch ev.GetEventType() {
		case pb.WatchEventType_EVENT_MODIFIED:
			if ino != 0 {
				f.notifyInode(ino, 0, -1)
			}
		case pb.WatchEventType_EVENT_CREATED:
			if parent := ev.GetParentInode(); parent != 0 && ev.GetName() != "" {
				f.notifyEntry(parent, ev.GetName())
			}
		case pb.WatchEventType_EVENT_DELETED:
			if parent := ev.GetParentInode(); parent != 0 && ev.GetName() != "" {
				if ino != 0 {
					f.notifyDelete(parent, ino, ev.GetName())
				} else {
					f.notifyEntry(parent, ev.GetName())
				}
			}
		case pb.WatchEventType_EVENT_RENAMED:
			oldParent := ev.GetOldParentInode()
			oldName := ev.GetOldName()
			newParent := ev.GetParentInode()
			newName := ev.GetName()

			if oldParent != 0 && oldName != "" {
				f.notifyEntry(oldParent, oldName)
				f.notifyInode(oldParent, -1, 0)
			}
			if newParent != 0 && newName != "" {
				f.notifyEntry(newParent, newName)
				if newParent != oldParent {
					f.notifyInode(newParent, -1, 0)
				}
			}
		}
	}

	if cache != nil && eventPos > 0 {
		for _, target := range targets {
			cache.RecordPosition(target, eventPos)
		}
	}
}

func StartWatcher(ctx context.Context, client pb.ObjectFSControllerClient, volumeID, nodeID string, cache *NodeCache, fs ...*ObjectFS) {
	go func() {
		defer func() {
			for _, f := range fs {
				if f != nil {
					f.SetWatcherConnected(false)
				}
			}
		}()

		backoff := 100 * time.Millisecond
		maxBackoff := 1 * time.Second
		hasDisconnected := false
		var disconnectTime time.Time
		loggedLongGap := false

		for {
			select {
			case <-ctx.Done():
				return
			default:
			}

			stream, err := client.WatchVolume(ctx, &pb.WatchVolumeRequest{
				VolumeId: volumeID,
				NodeId:   nodeID,
			})
			if err != nil {
				if !hasDisconnected {
					hasDisconnected = true
					disconnectTime = time.Now()
					loggedLongGap = false
				}
				for _, f := range fs {
					if f != nil {
						f.SetWatcherConnected(false)
					}
				}
				gap := time.Since(disconnectTime)
				if gap >= LongWatcherGapThreshold && !loggedLongGap {
					klog.Warningf("WatchVolume disconnected for volume %s, node %s for %v (exceeds %v threshold)", volumeID, nodeID, gap.Round(time.Millisecond), LongWatcherGapThreshold)
					loggedLongGap = true
				}
				klog.V(2).Infof("WatchVolume dial error for volume %s, node %s: %v; backing off %v", volumeID, nodeID, err, backoff)
				select {
				case <-ctx.Done():
					return
				case <-time.After(backoff):
				}
				backoff *= 2
				if backoff > maxBackoff {
					backoff = maxBackoff
				}
				continue
			}

			klog.V(2).Infof("WatchVolume subscription established for volume %s, node %s", volumeID, nodeID)
			if hasDisconnected {
				totalGap := time.Since(disconnectTime)
				klog.V(2).Infof("WatchVolume reconnected for volume %s, node %s after total gap of %v; invalidating clean cache and manifests", volumeID, nodeID, totalGap.Round(time.Millisecond))
				if cache != nil {
					cache.ResetKnownPositions()
					cache.InvalidateAllClean()
				}
				for _, f := range fs {
					if f != nil {
						f.invalidateAllManifests()
						f.SweepRevalidateInodes()
					}
				}
				hasDisconnected = false
				loggedLongGap = false
			}
			for _, f := range fs {
				if f != nil {
					f.SetWatcherConnected(true)
				}
			}
			backoff = 100 * time.Millisecond

			for {
				ev, err := stream.Recv()
				if err != nil {
					if !hasDisconnected {
						hasDisconnected = true
						disconnectTime = time.Now()
						loggedLongGap = false
					}
					for _, f := range fs {
						if f != nil {
							f.SetWatcherConnected(false)
						}
					}
					klog.V(2).Infof("WatchVolume stream error for volume %s, node %s: %v; backing off %v", volumeID, nodeID, err, backoff)
					break
				}
				handleWatchEvent(ev, cache, fs)
			}
			select {
			case <-ctx.Done():
				return
			case <-time.After(backoff):
			}
		}
	}()
}
