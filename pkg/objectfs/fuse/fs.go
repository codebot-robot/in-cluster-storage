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
	"path"
	"syscall"
	"time"

	pb "github.com/gke-labs/in-cluster-storage/pkg/api/objectfs/v1alpha1"
	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type FSNode struct {
	fs.Inode
	client    pb.ObjectFSControllerClient
	volumeID  string
	path      string
	isDir     bool
	writeMode pb.WriteMode
	cache     *NodeCache
}

var _ fs.InodeEmbedder = (*FSNode)(nil)
var _ fs.NodeGetattrer = (*FSNode)(nil)
var _ fs.NodeSetattrer = (*FSNode)(nil)
var _ fs.NodeLookuper = (*FSNode)(nil)
var _ fs.NodeReaddirer = (*FSNode)(nil)
var _ fs.NodeMkdirer = (*FSNode)(nil)
var _ fs.NodeCreater = (*FSNode)(nil)
var _ fs.NodeUnlinker = (*FSNode)(nil)
var _ fs.NodeRmdirer = (*FSNode)(nil)
var _ fs.NodeRenamer = (*FSNode)(nil)
var _ fs.NodeOpener = (*FSNode)(nil)
var _ fs.FileReader = (*FSNode)(nil)
var _ fs.FileWriter = (*FSNode)(nil)
var _ fs.FileFlusher = (*FSNode)(nil)
var _ fs.FileFsyncer = (*FSNode)(nil)

func NewRootNode(client pb.ObjectFSControllerClient, volumeID string, writeMode pb.WriteMode, cache *NodeCache) *FSNode {
	if cache == nil {
		cache = NewNodeCache(128 * 1024 * 1024)
	}
	return &FSNode{
		client:    client,
		volumeID:  volumeID,
		path:      "/",
		isDir:     true,
		writeMode: writeMode,
		cache:     cache,
	}
}

func (n *FSNode) newNode(p string, isDir bool) *FSNode {
	return &FSNode{
		client:    n.client,
		volumeID:  n.volumeID,
		path:      p,
		isDir:     isDir,
		writeMode: n.writeMode,
		cache:     n.cache,
	}
}

func grpcErrorToErrno(err error) syscall.Errno {
	if err == nil {
		return fs.OK
	}
	st, ok := status.FromError(err)
	if !ok {
		return syscall.EIO
	}
	switch st.Code() {
	case codes.NotFound:
		return syscall.ENOENT
	case codes.AlreadyExists:
		return syscall.EEXIST
	case codes.InvalidArgument:
		return syscall.EINVAL
	case codes.PermissionDenied, codes.Unauthenticated:
		return syscall.EACCES
	case codes.Unimplemented:
		return syscall.ENOSYS
	default:
		return syscall.EIO
	}
}

func fillAttr(attr *pb.EntryAttr, out *fuse.Attr) {
	out.Ino = attr.GetInode()
	out.Size = uint64(attr.GetSize())
	out.Mode = attr.GetMode()
	if attr.GetIsDir() {
		out.Mode |= syscall.S_IFDIR
	} else {
		out.Mode |= syscall.S_IFREG
	}
	if attr.GetModTime() != nil {
		t := attr.GetModTime().AsTime()
		out.Mtime = uint64(t.Unix())
		out.Mtimensec = uint32(t.Nanosecond())
		out.Ctime = out.Mtime
		out.Ctimensec = out.Mtimensec
		out.Atime = out.Mtime
		out.Atimensec = out.Mtimensec
	}
}

func fillAttrOut(attr *pb.EntryAttr, out *fuse.AttrOut) {
	fillAttr(attr, &out.Attr)
}

func fillEntryOut(attr *pb.EntryAttr, out *fuse.EntryOut) {
	fillAttr(attr, &out.Attr)
	out.NodeId = attr.GetInode()
}

func (n *FSNode) Getattr(ctx context.Context, fh fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	resp, err := n.client.GetAttr(ctx, &pb.GetAttrRequest{
		VolumeId: n.volumeID,
		Path:     n.path,
	})
	if err != nil {
		return grpcErrorToErrno(err)
	}

	fillAttrOut(resp.GetAttr(), out)
	return fs.OK
}

func (n *FSNode) Setattr(ctx context.Context, fh fs.FileHandle, in *fuse.SetAttrIn, out *fuse.AttrOut) syscall.Errno {
	if sz, ok := in.GetSize(); ok {
		resp, err := n.client.TruncateFile(ctx, &pb.TruncateFileRequest{
			VolumeId: n.volumeID,
			Path:     n.path,
			Size:     int64(sz),
		})
		if err != nil {
			return grpcErrorToErrno(err)
		}
		n.cache.Invalidate(n.path)
		fillAttrOut(resp.GetAttr(), out)
		return fs.OK
	}

	// Fetch current attributes for non-size updates
	resp, err := n.client.GetAttr(ctx, &pb.GetAttrRequest{
		VolumeId: n.volumeID,
		Path:     n.path,
	})
	if err != nil {
		return grpcErrorToErrno(err)
	}
	fillAttrOut(resp.GetAttr(), out)
	return fs.OK
}

func (n *FSNode) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	resp, err := n.client.Lookup(ctx, &pb.LookupRequest{
		VolumeId:   n.volumeID,
		ParentPath: n.path,
		Name:       name,
	})
	if err != nil {
		return nil, grpcErrorToErrno(err)
	}

	attr := resp.GetAttr()
	fillEntryOut(attr, out)

	childPath := path.Join(n.path, name)
	childNode := n.newNode(childPath, attr.GetIsDir())

	mode := syscall.S_IFREG
	if attr.GetIsDir() {
		mode = syscall.S_IFDIR
	}

	stable := fs.StableAttr{
		Mode: uint32(mode),
		Ino:  attr.GetInode(),
	}
	inode := n.NewInode(ctx, childNode, stable)
	return inode, fs.OK
}

func (n *FSNode) Readdir(ctx context.Context) (fs.DirStream, syscall.Errno) {
	resp, err := n.client.ReadDir(ctx, &pb.ReadDirRequest{
		VolumeId: n.volumeID,
		Path:     n.path,
	})
	if err != nil {
		return nil, grpcErrorToErrno(err)
	}

	var list []fuse.DirEntry
	for _, entry := range resp.GetEntries() {
		mode := uint32(fuse.S_IFREG)
		if entry.GetIsDir() {
			mode = uint32(fuse.S_IFDIR)
		}
		list = append(list, fuse.DirEntry{
			Mode: mode,
			Name: entry.GetName(),
			Ino:  entry.GetInode(),
		})
	}
	return fs.NewListDirStream(list), fs.OK
}

func (n *FSNode) Mkdir(ctx context.Context, name string, mode uint32, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	childPath := path.Join(n.path, name)
	resp, err := n.client.Mkdir(ctx, &pb.MkdirRequest{
		VolumeId: n.volumeID,
		Path:     childPath,
		Mode:     mode,
	})
	if err != nil {
		return nil, grpcErrorToErrno(err)
	}

	attr := resp.GetAttr()
	fillEntryOut(attr, out)

	childNode := n.newNode(childPath, true)
	stable := fs.StableAttr{
		Mode: syscall.S_IFDIR,
		Ino:  attr.GetInode(),
	}
	return n.NewInode(ctx, childNode, stable), fs.OK
}

func (n *FSNode) Create(ctx context.Context, name string, flags uint32, mode uint32, out *fuse.EntryOut) (node *fs.Inode, fh fs.FileHandle, fuseFlags uint32, errno syscall.Errno) {
	childPath := path.Join(n.path, name)
	resp, err := n.client.CreateFile(ctx, &pb.CreateFileRequest{
		VolumeId: n.volumeID,
		Path:     childPath,
		Mode:     mode,
	})
	if err != nil {
		return nil, nil, 0, grpcErrorToErrno(err)
	}

	attr := resp.GetAttr()
	fillEntryOut(attr, out)

	childNode := n.newNode(childPath, false)
	stable := fs.StableAttr{
		Mode: syscall.S_IFREG,
		Ino:  attr.GetInode(),
	}
	childInode := n.NewInode(ctx, childNode, stable)
	return childInode, childNode, 0, fs.OK
}

func (n *FSNode) Unlink(ctx context.Context, name string) syscall.Errno {
	childPath := path.Join(n.path, name)
	_, err := n.client.Unlink(ctx, &pb.UnlinkRequest{
		VolumeId: n.volumeID,
		Path:     childPath,
	})
	if err != nil {
		return grpcErrorToErrno(err)
	}
	n.cache.Invalidate(childPath)
	return fs.OK
}

func (n *FSNode) Rmdir(ctx context.Context, name string) syscall.Errno {
	childPath := path.Join(n.path, name)
	_, err := n.client.Rmdir(ctx, &pb.RmdirRequest{
		VolumeId: n.volumeID,
		Path:     childPath,
	})
	if err != nil {
		return grpcErrorToErrno(err)
	}
	return fs.OK
}

func (n *FSNode) Rename(ctx context.Context, name string, newParent fs.InodeEmbedder, newName string, flags uint32) syscall.Errno {
	oldPath := path.Join(n.path, name)
	targetParent, ok := newParent.(*FSNode)
	if !ok {
		return syscall.EINVAL
	}
	newPath := path.Join(targetParent.path, newName)

	_, err := n.client.Rename(ctx, &pb.RenameRequest{
		VolumeId: n.volumeID,
		OldPath:  oldPath,
		NewPath:  newPath,
	})
	if err != nil {
		return grpcErrorToErrno(err)
	}
	n.cache.Invalidate(oldPath)
	n.cache.Invalidate(newPath)
	return fs.OK
}

func (n *FSNode) Open(ctx context.Context, flags uint32) (fh fs.FileHandle, fuseFlags uint32, errno syscall.Errno) {
	return n, 0, fs.OK
}

func (n *FSNode) Read(ctx context.Context, dest []byte, off int64) (fuse.ReadResult, syscall.Errno) {
	// Check node cache first
	if cached, ok := n.cache.Get(n.path); ok {
		if off >= cached.Size {
			return fuse.ReadResultData([]byte{}), fs.OK
		}
		end := off + int64(len(dest))
		if end > cached.Size {
			end = cached.Size
		}
		return fuse.ReadResultData(cached.Data[off:end]), fs.OK
	}

	resp, err := n.client.ReadFile(ctx, &pb.ReadFileRequest{
		VolumeId: n.volumeID,
		Path:     n.path,
		Offset:   off,
		Size:     int64(len(dest)),
	})
	if err != nil {
		return nil, grpcErrorToErrno(err)
	}

	// Cache small file content
	if off == 0 && resp.GetEof() && len(resp.GetData()) > 0 {
		n.cache.Put(n.path, resp.GetData(), time.Now(), "")
	}

	return fuse.ReadResultData(resp.GetData()), fs.OK
}

func (n *FSNode) Write(ctx context.Context, data []byte, off int64) (uint32, syscall.Errno) {
	resp, err := n.client.WriteFile(ctx, &pb.WriteFileRequest{
		VolumeId:  n.volumeID,
		Path:      n.path,
		Offset:    off,
		Data:      data,
		WriteMode: n.writeMode,
	})
	if err != nil {
		return 0, grpcErrorToErrno(err)
	}

	n.cache.Invalidate(n.path)
	return uint32(resp.GetBytesWritten()), fs.OK
}

func (n *FSNode) Flush(ctx context.Context) syscall.Errno {
	if n.writeMode == pb.WriteMode_WRITE_THROUGH_FSYNC {
		_, err := n.client.Fsync(ctx, &pb.FsyncRequest{
			VolumeId: n.volumeID,
			Path:     n.path,
		})
		if err != nil {
			return grpcErrorToErrno(err)
		}
	}
	return fs.OK
}

func (n *FSNode) Fsync(ctx context.Context, flags uint32) syscall.Errno {
	_, err := n.client.Fsync(ctx, &pb.FsyncRequest{
		VolumeId: n.volumeID,
		Path:     n.path,
	})
	if err != nil {
		return grpcErrorToErrno(err)
	}
	return fs.OK
}

// StartWatcher starts a background loop listening to push notifications and invalidating cache.
func StartWatcher(ctx context.Context, client pb.ObjectFSControllerClient, volumeID, nodeID string, cache *NodeCache) {
	go func() {
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
				time.Sleep(1 * time.Second)
				continue
			}

			for {
				ev, err := stream.Recv()
				if err != nil {
					break
				}
				if ev.GetPath() != "" {
					cache.Invalidate(ev.GetPath())
				}
				if ev.GetOldPath() != "" {
					cache.Invalidate(ev.GetOldPath())
				}
			}
			time.Sleep(500 * time.Millisecond)
		}
	}()
}
