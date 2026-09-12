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
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"path"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	pb "github.com/gke-labs/in-cluster-storage/pkg/api/objectfs/v1alpha1"
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
	Inode   uint64    `json:"inode"`
	Path    string    `json:"path"`
	Name    string    `json:"name"`
	IsDir   bool      `json:"is_dir"`
	Mode    uint32    `json:"mode"`
	Size    int64     `json:"size"`
	ModTime time.Time `json:"mod_time"`
	Sha256  string    `json:"sha256,omitempty"`
	ETag    string    `json:"etag,omitempty"`
}

type FSNode struct {
	mu          sync.RWMutex
	inode       uint64
	name        string
	path        string
	isDir       bool
	mode        uint32
	size        int64
	modTime     time.Time
	data        []byte
	sha256      string
	redirectURL string
	etag        string
	isDirty     bool
	children    map[string]*FSNode
	parent      *FSNode
}

type Volume struct {
	mu           sync.RWMutex
	volumeID     string
	root         *FSNode
	nextInode    uint64
	backend      ObjectStorageBackend
	broadcaster  *EventBroadcaster
	maxInlineLen int64

	lastFlushedMetadata    *VolumeMetadata
	deletedPathsSinceFlush []string
}

func NewVolume(volumeID string, backend ObjectStorageBackend, broadcaster *EventBroadcaster) *Volume {
	v := &Volume{
		volumeID:     volumeID,
		nextInode:    1,
		backend:      backend,
		broadcaster:  broadcaster,
		maxInlineLen: 4 * 1024 * 1024, // 4MB default inline threshold
	}
	v.root = &FSNode{
		inode:    v.allocInode(),
		name:     "/",
		path:     "/",
		isDir:    true,
		mode:     0755 | syscall.S_IFDIR,
		modTime:  time.Now(),
		children: make(map[string]*FSNode),
	}
	return v
}

func (v *Volume) allocInode() uint64 {
	return atomic.AddUint64(&v.nextInode, 1) - 1
}

func cleanPath(p string) string {
	cleaned := path.Clean("/" + p)
	if !strings.HasPrefix(cleaned, "/") {
		cleaned = "/" + cleaned
	}
	return cleaned
}

func (n *FSNode) toEntryAttrLocked() *pb.EntryAttr {
	return &pb.EntryAttr{
		Inode:       n.inode,
		Path:        n.path,
		Name:        n.name,
		IsDir:       n.isDir,
		Size:        n.size,
		Mode:        n.mode,
		ModTime:     timestamppb.New(n.modTime),
		Sha256:      n.sha256,
		RedirectUrl: n.redirectURL,
	}
}

func (v *Volume) findNodeLocked(p string) (*FSNode, error) {
	p = cleanPath(p)
	if p == "/" {
		return v.root, nil
	}

	parts := strings.Split(strings.Trim(p, "/"), "/")
	curr := v.root
	for _, part := range parts {
		if !curr.isDir {
			return nil, fmt.Errorf("path component %s is not a directory: %w", curr.path, syscall.ENOTDIR)
		}
		child, ok := curr.children[part]
		if !ok {
			return nil, fmt.Errorf("file or directory %s not found: %w", p, syscall.ENOENT)
		}
		curr = child
	}
	return curr, nil
}

func (v *Volume) GetAttr(ctx context.Context, p string) (*pb.EntryAttr, error) {
	v.mu.RLock()
	defer v.mu.RUnlock()

	node, err := v.findNodeLocked(p)
	if err != nil {
		return nil, err
	}

	node.mu.RLock()
	defer node.mu.RUnlock()
	return node.toEntryAttrLocked(), nil
}

func (v *Volume) Lookup(ctx context.Context, parentPath, name string) (*pb.EntryAttr, error) {
	v.mu.RLock()
	defer v.mu.RUnlock()

	parent, err := v.findNodeLocked(parentPath)
	if err != nil {
		return nil, err
	}

	parent.mu.RLock()
	defer parent.mu.RUnlock()

	if !parent.isDir {
		return nil, fmt.Errorf("parent %s is not a directory: %w", parentPath, syscall.ENOTDIR)
	}

	child, ok := parent.children[name]
	if !ok {
		return nil, fmt.Errorf("child %s not found in %s: %w", name, parentPath, syscall.ENOENT)
	}

	child.mu.RLock()
	defer child.mu.RUnlock()
	return child.toEntryAttrLocked(), nil
}

func (v *Volume) ReadDir(ctx context.Context, p string) ([]*pb.EntryAttr, error) {
	v.mu.RLock()
	defer v.mu.RUnlock()

	node, err := v.findNodeLocked(p)
	if err != nil {
		return nil, err
	}

	node.mu.RLock()
	defer node.mu.RUnlock()

	if !node.isDir {
		return nil, fmt.Errorf("path %s is not a directory: %w", p, syscall.ENOTDIR)
	}

	var entries []*pb.EntryAttr
	for _, child := range node.children {
		child.mu.RLock()
		entries = append(entries, child.toEntryAttrLocked())
		child.mu.RUnlock()
	}
	return entries, nil
}

func (v *Volume) Mkdir(ctx context.Context, p string, mode uint32) (*pb.EntryAttr, error) {
	v.mu.Lock()
	defer v.mu.Unlock()

	p = cleanPath(p)
	if p == "/" {
		return nil, fmt.Errorf("cannot recreate root directory: %w", syscall.EEXIST)
	}

	parentPath := path.Dir(p)
	baseName := path.Base(p)

	parent, err := v.findNodeLocked(parentPath)
	if err != nil {
		return nil, fmt.Errorf("parent directory not found: %w", err)
	}

	parent.mu.Lock()
	defer parent.mu.Unlock()

	if !parent.isDir {
		return nil, fmt.Errorf("parent %s is not a directory: %w", parentPath, syscall.ENOTDIR)
	}

	if _, exists := parent.children[baseName]; exists {
		return nil, fmt.Errorf("directory %s already exists: %w", p, syscall.EEXIST)
	}

	if mode == 0 {
		mode = 0755
	}
	mode |= syscall.S_IFDIR

	now := time.Now()
	child := &FSNode{
		inode:    v.allocInode(),
		name:     baseName,
		path:     p,
		isDir:    true,
		mode:     mode,
		modTime:  now,
		children: make(map[string]*FSNode),
		parent:   parent,
	}
	parent.children[baseName] = child
	parent.modTime = now

	attr := child.toEntryAttrLocked()
	v.broadcaster.Broadcast(v.volumeID, &pb.WatchVolumeResponse{
		EventType: pb.WatchEventType_EVENT_CREATED,
		Path:      p,
		Attr:      attr,
	})

	return attr, nil
}

func (v *Volume) CreateFile(ctx context.Context, p string, mode uint32, initialContent []byte) (*pb.EntryAttr, error) {
	v.mu.Lock()
	defer v.mu.Unlock()

	p = cleanPath(p)
	if p == "/" {
		return nil, fmt.Errorf("cannot create file at root: %w", syscall.EISDIR)
	}

	parentPath := path.Dir(p)
	baseName := path.Base(p)

	parent, err := v.findNodeLocked(parentPath)
	if err != nil {
		return nil, fmt.Errorf("parent directory not found: %w", err)
	}

	parent.mu.Lock()
	defer parent.mu.Unlock()

	if !parent.isDir {
		return nil, fmt.Errorf("parent %s is not a directory: %w", parentPath, syscall.ENOTDIR)
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

	child, exists := parent.children[baseName]
	if exists {
		child.mu.Lock()
		defer child.mu.Unlock()
		if child.isDir {
			return nil, fmt.Errorf("cannot overwrite directory with file: %w", syscall.EISDIR)
		}
		child.mode = mode
		child.size = int64(len(dataCopy))
		child.data = dataCopy
		child.modTime = now
		child.sha256 = hashStr
		child.isDirty = true
		attr := child.toEntryAttrLocked()
		v.broadcaster.Broadcast(v.volumeID, &pb.WatchVolumeResponse{
			EventType: pb.WatchEventType_EVENT_MODIFIED,
			Path:      p,
			Attr:      attr,
		})
		return attr, nil
	}

	child = &FSNode{
		inode:   v.allocInode(),
		name:    baseName,
		path:    p,
		isDir:   false,
		mode:    mode,
		size:    int64(len(dataCopy)),
		modTime: now,
		data:    dataCopy,
		sha256:  hashStr,
		isDirty: true,
		parent:  parent,
	}
	parent.children[baseName] = child
	parent.modTime = now

	attr := child.toEntryAttrLocked()
	v.broadcaster.Broadcast(v.volumeID, &pb.WatchVolumeResponse{
		EventType: pb.WatchEventType_EVENT_CREATED,
		Path:      p,
		Attr:      attr,
	})

	return attr, nil
}

func (v *Volume) ReadFile(ctx context.Context, p string, offset, length int64) ([]byte, int64, string, error) {
	v.mu.RLock()
	node, err := v.findNodeLocked(p)
	v.mu.RUnlock()
	if err != nil {
		return nil, 0, "", err
	}

	node.mu.Lock()
	defer node.mu.Unlock()

	if node.isDir {
		return nil, 0, "", fmt.Errorf("cannot read directory as file: %w", syscall.EISDIR)
	}

	// Lazy load data from backend if not currently in memory
	if len(node.data) == 0 && node.size > 0 && v.backend != nil {
		data, err := v.backend.GetObject(ctx, v.volumeID, strings.TrimPrefix(node.path, "/"), 0, node.size)
		if err == nil {
			node.data = data
		}
	}

	total := node.size
	if node.redirectURL != "" && length > v.maxInlineLen {
		return nil, total, node.redirectURL, nil
	}

	if offset >= total {
		return []byte{}, total, "", nil
	}

	end := offset + length
	if length <= 0 || end > total {
		end = total
	}

	res := make([]byte, end-offset)
	copy(res, node.data[offset:end])
	return res, total, "", nil
}

func (v *Volume) WriteFile(ctx context.Context, p string, offset int64, data []byte, writeMode pb.WriteMode) (int64, int64, time.Time, error) {
	v.mu.RLock()
	node, err := v.findNodeLocked(p)
	v.mu.RUnlock()
	if err != nil {
		return 0, 0, time.Time{}, err
	}

	node.mu.Lock()
	defer node.mu.Unlock()

	if node.isDir {
		return 0, 0, time.Time{}, fmt.Errorf("cannot write to directory: %w", syscall.EISDIR)
	}

	neededLen := offset + int64(len(data))
	if neededLen > int64(len(node.data)) {
		newBuf := make([]byte, neededLen)
		copy(newBuf, node.data)
		node.data = newBuf
	}
	copy(node.data[offset:], data)
	node.size = int64(len(node.data))
	now := time.Now()
	node.modTime = now
	node.isDirty = true

	h := sha256.Sum256(node.data)
	node.sha256 = fmt.Sprintf("%x", h)

	attr := node.toEntryAttrLocked()
	v.broadcaster.Broadcast(v.volumeID, &pb.WatchVolumeResponse{
		EventType: pb.WatchEventType_EVENT_MODIFIED,
		Path:      p,
		Attr:      attr,
	})

	return int64(len(data)), node.size, now, nil
}

func (v *Volume) TruncateFile(ctx context.Context, p string, size int64) (*pb.EntryAttr, error) {
	v.mu.RLock()
	node, err := v.findNodeLocked(p)
	v.mu.RUnlock()
	if err != nil {
		return nil, err
	}

	node.mu.Lock()
	defer node.mu.Unlock()

	if node.isDir {
		return nil, fmt.Errorf("cannot truncate directory: %w", syscall.EISDIR)
	}

	if size < 0 {
		return nil, fmt.Errorf("invalid size %d: %w", size, syscall.EINVAL)
	}

	if size < int64(len(node.data)) {
		node.data = node.data[:size]
	} else if size > int64(len(node.data)) {
		newBuf := make([]byte, size)
		copy(newBuf, node.data)
		node.data = newBuf
	}
	node.size = size
	now := time.Now()
	node.modTime = now
	node.isDirty = true
	h := sha256.Sum256(node.data)
	node.sha256 = fmt.Sprintf("%x", h)

	attr := node.toEntryAttrLocked()
	v.broadcaster.Broadcast(v.volumeID, &pb.WatchVolumeResponse{
		EventType: pb.WatchEventType_EVENT_MODIFIED,
		Path:      p,
		Attr:      attr,
	})
	return attr, nil
}

func (v *Volume) Unlink(ctx context.Context, p string) error {
	v.mu.Lock()
	defer v.mu.Unlock()

	p = cleanPath(p)
	if p == "/" {
		return fmt.Errorf("cannot unlink root: %w", syscall.EBUSY)
	}

	parentPath := path.Dir(p)
	baseName := path.Base(p)

	parent, err := v.findNodeLocked(parentPath)
	if err != nil {
		return err
	}

	parent.mu.Lock()
	defer parent.mu.Unlock()

	child, ok := parent.children[baseName]
	if !ok {
		return fmt.Errorf("file %s not found: %w", p, syscall.ENOENT)
	}

	child.mu.RLock()
	if child.isDir {
		child.mu.RUnlock()
		return fmt.Errorf("cannot unlink directory %s: %w", p, syscall.EISDIR)
	}
	child.mu.RUnlock()

	delete(parent.children, baseName)
	parent.modTime = time.Now()
	v.deletedPathsSinceFlush = append(v.deletedPathsSinceFlush, p)

	v.broadcaster.Broadcast(v.volumeID, &pb.WatchVolumeResponse{
		EventType: pb.WatchEventType_EVENT_DELETED,
		Path:      p,
	})

	return nil
}

func (v *Volume) Rmdir(ctx context.Context, p string) error {
	v.mu.Lock()
	defer v.mu.Unlock()

	p = cleanPath(p)
	if p == "/" {
		return fmt.Errorf("cannot rmdir root: %w", syscall.EBUSY)
	}

	parentPath := path.Dir(p)
	baseName := path.Base(p)

	parent, err := v.findNodeLocked(parentPath)
	if err != nil {
		return err
	}

	parent.mu.Lock()
	defer parent.mu.Unlock()

	child, ok := parent.children[baseName]
	if !ok {
		return fmt.Errorf("directory %s not found: %w", p, syscall.ENOENT)
	}

	child.mu.Lock()
	defer child.mu.Unlock()

	if !child.isDir {
		return fmt.Errorf("cannot rmdir non-directory %s: %w", p, syscall.ENOTDIR)
	}

	if len(child.children) > 0 {
		return fmt.Errorf("directory %s not empty: %w", p, syscall.ENOTEMPTY)
	}

	delete(parent.children, baseName)
	parent.modTime = time.Now()

	v.broadcaster.Broadcast(v.volumeID, &pb.WatchVolumeResponse{
		EventType: pb.WatchEventType_EVENT_DELETED,
		Path:      p,
	})

	return nil
}

func (v *Volume) Rename(ctx context.Context, oldPath, newPath string) (*pb.EntryAttr, error) {
	v.mu.Lock()
	defer v.mu.Unlock()

	oldPath = cleanPath(oldPath)
	newPath = cleanPath(newPath)

	if oldPath == "/" || newPath == "/" {
		return nil, fmt.Errorf("cannot rename root: %w", syscall.EBUSY)
	}

	oldParentPath := path.Dir(oldPath)
	oldBaseName := path.Base(oldPath)
	newParentPath := path.Dir(newPath)
	newBaseName := path.Base(newPath)

	oldParent, err := v.findNodeLocked(oldParentPath)
	if err != nil {
		return nil, fmt.Errorf("old parent not found: %w", err)
	}

	newParent, err := v.findNodeLocked(newParentPath)
	if err != nil {
		return nil, fmt.Errorf("new parent not found: %w", err)
	}

	oldParent.mu.Lock()
	defer oldParent.mu.Unlock()

	child, ok := oldParent.children[oldBaseName]
	if !ok {
		return nil, fmt.Errorf("source %s not found: %w", oldPath, syscall.ENOENT)
	}

	if oldParent != newParent {
		newParent.mu.Lock()
		defer newParent.mu.Unlock()
	}

	if !newParent.isDir {
		return nil, fmt.Errorf("target parent %s is not a directory: %w", newParentPath, syscall.ENOTDIR)
	}

	delete(oldParent.children, oldBaseName)
	child.mu.Lock()
	child.name = newBaseName
	child.path = newPath
	child.parent = newParent
	child.modTime = time.Now()
	child.isDirty = true
	child.mu.Unlock()

	newParent.children[newBaseName] = child
	oldParent.modTime = time.Now()
	newParent.modTime = time.Now()
	v.deletedPathsSinceFlush = append(v.deletedPathsSinceFlush, oldPath)

	attr := child.toEntryAttrLocked()
	v.broadcaster.Broadcast(v.volumeID, &pb.WatchVolumeResponse{
		EventType: pb.WatchEventType_EVENT_RENAMED,
		Path:      newPath,
		OldPath:   oldPath,
		Attr:      attr,
	})

	return attr, nil
}

func (v *Volume) Fsync(ctx context.Context, p string) error {
	v.mu.RLock()
	_, err := v.findNodeLocked(p)
	v.mu.RUnlock()
	return err
}

func (v *Volume) FlushToBackend(ctx context.Context) error {
	v.mu.Lock()
	defer v.mu.Unlock()

	if v.backend == nil {
		return nil
	}

	// 1. Process deleted paths
	for _, delPath := range v.deletedPathsSinceFlush {
		key := strings.TrimPrefix(delPath, "/")
		_ = v.backend.DeleteObject(ctx, v.volumeID, key)
	}
	v.deletedPathsSinceFlush = nil

	// 2. Traverse tree to collect all entries and upload dirty files
	currentEntries := make(map[string]FileMetadata)
	var walk func(node *FSNode) error
	walk = func(node *FSNode) error {
		node.mu.Lock()
		defer node.mu.Unlock()

		meta := FileMetadata{
			Inode:   node.inode,
			Path:    node.path,
			Name:    node.name,
			IsDir:   node.isDir,
			Mode:    node.mode,
			Size:    node.size,
			ModTime: node.modTime,
			Sha256:  node.sha256,
			ETag:    node.etag,
		}

		if !node.isDir {
			needsUpload := node.isDirty || node.etag == ""
			if v.lastFlushedMetadata != nil {
				if lastEntry, exists := v.lastFlushedMetadata.Entries[node.path]; !exists || lastEntry.Sha256 != node.sha256 {
					needsUpload = true
				}
			}
			if needsUpload {
				key := strings.TrimPrefix(node.path, "/")
				etag, err := v.backend.PutObject(ctx, v.volumeID, key, node.data)
				if err != nil {
					return fmt.Errorf("failed to upload object %s: %w", key, err)
				}
				node.etag = etag
				meta.ETag = etag
				node.isDirty = false
			}
		}

		currentEntries[node.path] = meta

		if node.isDir {
			for _, child := range node.children {
				if err := walk(child); err != nil {
					return err
				}
			}
		}
		return nil
	}

	if err := walk(v.root); err != nil {
		return err
	}

	// 3. Write metadata file
	newMeta := VolumeMetadata{
		VolumeID:    v.volumeID,
		Version:     1,
		LastFlushed: time.Now(),
		NextInode:   v.nextInode,
		Entries:     currentEntries,
	}

	metaBytes, err := json.MarshalIndent(newMeta, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal metadata: %w", err)
	}

	if _, err := v.backend.PutObject(ctx, v.volumeID, MetadataFileName, metaBytes); err != nil {
		return fmt.Errorf("failed to write metadata file: %w", err)
	}

	v.lastFlushedMetadata = &newMeta
	return nil
}

func (v *Volume) LoadFromBackend(ctx context.Context) error {
	v.mu.Lock()
	defer v.mu.Unlock()

	if v.backend == nil {
		return nil
	}

	metaBytes, err := v.backend.GetObject(ctx, v.volumeID, MetadataFileName, 0, 0)
	if err != nil || len(metaBytes) == 0 {
		return nil
	}

	var meta VolumeMetadata
	if err := json.Unmarshal(metaBytes, &meta); err != nil {
		return fmt.Errorf("failed to parse volume metadata: %w", err)
	}

	if meta.NextInode > v.nextInode {
		v.nextInode = meta.NextInode
	}

	var paths []string
	for p := range meta.Entries {
		if p != "/" {
			paths = append(paths, p)
		}
	}
	sort.Slice(paths, func(i, j int) bool {
		return len(paths[i]) < len(paths[j])
	})

	for _, p := range paths {
		entry := meta.Entries[p]
		parentPath := path.Dir(p)
		baseName := path.Base(p)

		parent, err := v.findNodeLocked(parentPath)
		if err != nil {
			continue
		}

		child := &FSNode{
			inode:   entry.Inode,
			name:    baseName,
			path:    entry.Path,
			isDir:   entry.IsDir,
			mode:    entry.Mode,
			size:    entry.Size,
			modTime: entry.ModTime,
			sha256:  entry.Sha256,
			etag:    entry.ETag,
			parent:  parent,
			isDirty: false,
		}
		if child.isDir {
			child.children = make(map[string]*FSNode)
		}
		parent.children[baseName] = child
	}

	v.lastFlushedMetadata = &meta
	return nil
}
