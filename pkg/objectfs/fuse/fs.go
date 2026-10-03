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
	"fmt"
	"syscall"
	"time"

	pb "github.com/gke-labs/in-cluster-storage/pkg/api/objectfs/v1alpha1"
	"github.com/hanwen/go-fuse/v2/fuse"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type ObjectFS struct {
	fuse.RawFileSystem

	client    pb.ObjectFSControllerClient
	volumeID  string
	writeMode pb.WriteMode
	cache     *NodeCache
	server    *fuse.Server
}

var _ fuse.RawFileSystem = (*ObjectFS)(nil)

func NewObjectFS(client pb.ObjectFSControllerClient, volumeID string, writeMode pb.WriteMode, cache *NodeCache) *ObjectFS {
	if cache == nil {
		cache = NewNodeCache(128 * 1024 * 1024)
	}
	fs := &ObjectFS{
		RawFileSystem: fuse.NewDefaultRawFileSystem(),
		client:        client,
		volumeID:      volumeID,
		writeMode:     writeMode,
		cache:         cache,
	}
	return fs
}

func (fs *ObjectFS) String() string {
	return fmt.Sprintf("ObjectFS(%s)", fs.volumeID)
}

func (fs *ObjectFS) Init(server *fuse.Server) {
	fs.server = server
}

func makeContext(cancel <-chan struct{}) (context.Context, context.CancelFunc) {
	ctx, cancelFunc := context.WithCancel(context.Background())
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
	case codes.DeadlineExceeded:
		return fuse.Status(syscall.ETIMEDOUT)
	case codes.Canceled:
		return fuse.Status(syscall.EINTR)
	case codes.ResourceExhausted:
		return fuse.Status(syscall.ENOSPC)
	case codes.Aborted, codes.Unavailable:
		return fuse.Status(syscall.EBUSY)
	default:
		return fuse.Status(syscall.EIO)
	}
}

func (fs *ObjectFS) fillAttrOut(attr *pb.EntryAttr, out *fuse.Attr) {
	out.Ino = attr.GetInode()
	out.Size = uint64(attr.GetSize())
	out.Mode = attr.GetMode()
	if attr.GetIsDir() {
		out.Mode |= syscall.S_IFDIR
		out.Nlink = 2
	} else {
		out.Mode |= syscall.S_IFREG
		out.Nlink = 1
	}
	if attr.GetModTime() != nil {
		t := attr.GetModTime().AsTime()
		out.Mtime = uint64(t.Unix())
		out.Mtimensec = uint32(t.Nanosecond())
		out.Atime = out.Mtime
		out.Atimensec = out.Mtimensec
		out.Ctime = out.Mtime
		out.Ctimensec = out.Mtimensec
	}
	out.Owner = fuse.Owner{
		Uid: attr.GetUid(),
		Gid: attr.GetGid(),
	}
}

func (fs *ObjectFS) fillEntryOut(attr *pb.EntryAttr, out *fuse.EntryOut) {
	fs.fillAttrOut(attr, &out.Attr)
	out.NodeId = attr.GetInode()
	out.Generation = 1
	out.SetEntryTimeout(1 * time.Second)
	out.SetAttrTimeout(1 * time.Second)
}

func (fs *ObjectFS) Lookup(cancel <-chan struct{}, header *fuse.InHeader, name string, out *fuse.EntryOut) fuse.Status {
	ctx, cancelFunc := makeContext(cancel)
	defer cancelFunc()

	resp, err := fs.client.Lookup(ctx, &pb.LookupRequest{
		VolumeId:    fs.volumeID,
		ParentInode: header.NodeId,
		Name:        name,
	})
	if err != nil {
		return grpcErrorToStatus(err)
	}
	if resp.GetError() != 0 {
		return fuse.Status(resp.GetError())
	}
	attr := resp.GetAttr()
	if attr == nil {
		return fuse.ENOENT
	}
	fs.fillEntryOut(attr, out)
	return fuse.OK
}

func (fs *ObjectFS) GetAttr(cancel <-chan struct{}, input *fuse.GetAttrIn, out *fuse.AttrOut) fuse.Status {
	ctx, cancelFunc := makeContext(cancel)
	defer cancelFunc()

	resp, err := fs.client.GetAttr(ctx, &pb.GetAttrRequest{
		VolumeId: fs.volumeID,
		Inode:    input.NodeId,
	})
	if err != nil {
		return grpcErrorToStatus(err)
	}
	if resp.GetError() != 0 {
		return fuse.Status(resp.GetError())
	}
	attr := resp.GetAttr()
	if attr == nil {
		return fuse.ENOENT
	}
	fs.fillAttrOut(attr, &out.Attr)

	if entry, isDirty := fs.cache.GetDirty(input.NodeId); isDirty {
		out.Attr.Size = uint64(entry.Size)
		out.Attr.Mtime = uint64(entry.ModTime.Unix())
		out.Attr.Mtimensec = uint32(entry.ModTime.Nanosecond())
	}
	out.SetTimeout(1 * time.Second)
	return fuse.OK
}

func (fs *ObjectFS) SetAttr(cancel <-chan struct{}, input *fuse.SetAttrIn, out *fuse.AttrOut) fuse.Status {
	ctx, cancelFunc := makeContext(cancel)
	defer cancelFunc()

	if input.Valid&fuse.FATTR_SIZE != 0 {
		_ = fs.syncFileToService(ctx, input.NodeId)
		fs.cache.Truncate(input.NodeId, int64(input.Size), time.Now())
		resp, err := fs.client.TruncateFile(ctx, &pb.TruncateFileRequest{
			VolumeId: fs.volumeID,
			Inode:    input.NodeId,
			Size:     int64(input.Size),
		})
		if err != nil {
			return grpcErrorToStatus(err)
		}
		if resp.GetError() != 0 {
			return fuse.Status(resp.GetError())
		}
		if resp.GetAttr() != nil {
			fs.fillAttrOut(resp.GetAttr(), &out.Attr)
			out.Attr.Size = input.Size
			out.SetTimeout(1 * time.Second)
			return fuse.OK
		}
	}

	resp, err := fs.client.GetAttr(ctx, &pb.GetAttrRequest{
		VolumeId: fs.volumeID,
		Inode:    input.NodeId,
	})
	if err != nil {
		return grpcErrorToStatus(err)
	}
	if resp.GetError() != 0 {
		return fuse.Status(resp.GetError())
	}
	if resp.GetAttr() == nil {
		return fuse.ENOENT
	}
	fs.fillAttrOut(resp.GetAttr(), &out.Attr)
	if entry, isDirty := fs.cache.GetDirty(input.NodeId); isDirty {
		out.Attr.Size = uint64(entry.Size)
		out.Attr.Mtime = uint64(entry.ModTime.Unix())
		out.Attr.Mtimensec = uint32(entry.ModTime.Nanosecond())
	}
	out.SetTimeout(1 * time.Second)
	return fuse.OK
}

func (fs *ObjectFS) Mkdir(cancel <-chan struct{}, input *fuse.MkdirIn, name string, out *fuse.EntryOut) fuse.Status {
	ctx, cancelFunc := makeContext(cancel)
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
		return grpcErrorToStatus(err)
	}
	if resp.GetError() != 0 {
		return fuse.Status(resp.GetError())
	}
	attr := resp.GetAttr()
	if attr == nil {
		return fuse.EIO
	}
	fs.fillEntryOut(attr, out)
	return fuse.OK
}

func (fs *ObjectFS) Create(cancel <-chan struct{}, input *fuse.CreateIn, name string, out *fuse.CreateOut) fuse.Status {
	ctx, cancelFunc := makeContext(cancel)
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
		return grpcErrorToStatus(err)
	}
	if resp.GetError() != 0 {
		return fuse.Status(resp.GetError())
	}
	attr := resp.GetAttr()
	if attr == nil {
		return fuse.EIO
	}

	fs.cache.Put(attr.GetInode(), []byte{}, time.Now(), "")
	fs.fillEntryOut(attr, &out.EntryOut)
	out.OpenOut.Fh = attr.GetInode()
	return fuse.OK
}

func (fs *ObjectFS) Mknod(cancel <-chan struct{}, input *fuse.MknodIn, name string, out *fuse.EntryOut) fuse.Status {
	ctx, cancelFunc := makeContext(cancel)
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
		return grpcErrorToStatus(err)
	}
	if resp.GetError() != 0 {
		return fuse.Status(resp.GetError())
	}
	attr := resp.GetAttr()
	if attr == nil {
		return fuse.EIO
	}
	fs.fillEntryOut(attr, out)
	return fuse.OK
}

func (fs *ObjectFS) Unlink(cancel <-chan struct{}, header *fuse.InHeader, name string) fuse.Status {
	ctx, cancelFunc := makeContext(cancel)
	defer cancelFunc()

	resp, err := fs.client.Unlink(ctx, &pb.UnlinkRequest{
		VolumeId:    fs.volumeID,
		ParentInode: header.NodeId,
		Name:        name,
	})
	if err != nil {
		return grpcErrorToStatus(err)
	}
	if resp.GetError() != 0 {
		return fuse.Status(resp.GetError())
	}
	return fuse.OK
}

func (fs *ObjectFS) Rmdir(cancel <-chan struct{}, header *fuse.InHeader, name string) fuse.Status {
	ctx, cancelFunc := makeContext(cancel)
	defer cancelFunc()

	resp, err := fs.client.Rmdir(ctx, &pb.RmdirRequest{
		VolumeId:    fs.volumeID,
		ParentInode: header.NodeId,
		Name:        name,
	})
	if err != nil {
		return grpcErrorToStatus(err)
	}
	if resp.GetError() != 0 {
		return fuse.Status(resp.GetError())
	}
	return fuse.OK
}

func (fs *ObjectFS) Rename(cancel <-chan struct{}, input *fuse.RenameIn, oldName string, newName string) fuse.Status {
	ctx, cancelFunc := makeContext(cancel)
	defer cancelFunc()

	resp, err := fs.client.Rename(ctx, &pb.RenameRequest{
		VolumeId:       fs.volumeID,
		OldParentInode: input.NodeId,
		OldName:        oldName,
		NewParentInode: input.Newdir,
		NewName:        newName,
	})
	if err != nil {
		return grpcErrorToStatus(err)
	}
	if resp.GetError() != 0 {
		return fuse.Status(resp.GetError())
	}
	return fuse.OK
}

func (fs *ObjectFS) Open(cancel <-chan struct{}, input *fuse.OpenIn, out *fuse.OpenOut) fuse.Status {
	out.Fh = input.NodeId
	return fuse.OK
}

func (fs *ObjectFS) OpenDir(cancel <-chan struct{}, input *fuse.OpenIn, out *fuse.OpenOut) fuse.Status {
	out.Fh = input.NodeId
	return fuse.OK
}

func (fs *ObjectFS) ReadDir(cancel <-chan struct{}, input *fuse.ReadIn, out *fuse.DirEntryList) fuse.Status {
	ctx, cancelFunc := makeContext(cancel)
	defer cancelFunc()

	resp, err := fs.client.ReadDir(ctx, &pb.ReadDirRequest{
		VolumeId: fs.volumeID,
		Inode:    input.NodeId,
	})
	if err != nil {
		return grpcErrorToStatus(err)
	}
	if resp.GetError() != 0 {
		return fuse.Status(resp.GetError())
	}

	entries := resp.GetEntries()
	for i := int(input.Offset); i < len(entries); i++ {
		entry := entries[i]
		mode := uint32(syscall.S_IFREG)
		if entry.GetIsDir() {
			mode = uint32(syscall.S_IFDIR)
		}
		if !out.AddDirEntry(fuse.DirEntry{
			Mode: mode,
			Name: entry.GetName(),
			Ino:  entry.GetInode(),
			Off:  uint64(i + 1),
		}) {
			break
		}
	}
	return fuse.OK
}

func (fs *ObjectFS) ReadDirPlus(cancel <-chan struct{}, input *fuse.ReadIn, out *fuse.DirEntryList) fuse.Status {
	ctx, cancelFunc := makeContext(cancel)
	defer cancelFunc()

	resp, err := fs.client.ReadDir(ctx, &pb.ReadDirRequest{
		VolumeId: fs.volumeID,
		Inode:    input.NodeId,
	})
	if err != nil {
		return grpcErrorToStatus(err)
	}
	if resp.GetError() != 0 {
		return fuse.Status(resp.GetError())
	}

	entries := resp.GetEntries()
	for i := int(input.Offset); i < len(entries); i++ {
		entry := entries[i]
		mode := uint32(syscall.S_IFREG)
		if entry.GetIsDir() {
			mode = uint32(syscall.S_IFDIR)
		}
		entryOut := out.AddDirLookupEntry(fuse.DirEntry{
			Mode: mode,
			Name: entry.GetName(),
			Ino:  entry.GetInode(),
			Off:  uint64(i + 1),
		})
		if entryOut == nil {
			break
		}
		fs.fillEntryOut(entry, entryOut)
	}
	return fuse.OK
}

func (fs *ObjectFS) Read(cancel <-chan struct{}, input *fuse.ReadIn, buf []byte) (fuse.ReadResult, fuse.Status) {
	ctx, cancelFunc := makeContext(cancel)
	defer cancelFunc()

	offset := int64(input.Offset)
	size := int64(input.Size)

	if cachedBytes, ok := fs.cache.GetRange(input.NodeId, offset, size); ok {
		return fuse.ReadResultData(cachedBytes), fuse.OK
	}

	resp, err := fs.client.ReadFile(ctx, &pb.ReadFileRequest{
		VolumeId: fs.volumeID,
		Inode:    input.NodeId,
		Offset:   offset,
		Size:     size,
	})
	if err != nil {
		return nil, grpcErrorToStatus(err)
	}
	if resp.GetError() != 0 {
		return nil, fuse.Status(resp.GetError())
	}

	data := resp.GetData()
	if offset == 0 && resp.GetTotalSize() <= int64(len(data)) {
		fs.cache.Put(input.NodeId, data, time.Now(), "")
	}

	return fuse.ReadResultData(data), fuse.OK
}

func (fs *ObjectFS) Write(cancel <-chan struct{}, input *fuse.WriteIn, data []byte) (uint32, fuse.Status) {
	fs.cache.WriteAt(input.NodeId, int64(input.Offset), data, time.Now())
	return uint32(len(data)), fuse.OK
}

func (fs *ObjectFS) syncFileToService(ctx context.Context, inode uint64) error {
	entry, isDirty := fs.cache.GetDirty(inode)
	if !isDirty {
		return nil
	}

	resp, err := fs.client.WriteFile(ctx, &pb.WriteFileRequest{
		VolumeId:  fs.volumeID,
		Inode:     inode,
		Offset:    0,
		Data:      entry.Data,
		WriteMode: fs.writeMode,
	})
	if err != nil {
		return err
	}
	if resp.GetError() != 0 {
		return syscall.Errno(resp.GetError())
	}

	fs.cache.MarkClean(inode)
	return nil
}

func (fs *ObjectFS) Flush(cancel <-chan struct{}, input *fuse.FlushIn) fuse.Status {
	ctx, cancelFunc := makeContext(cancel)
	defer cancelFunc()

	if err := fs.syncFileToService(ctx, input.NodeId); err != nil {
		return grpcErrorToStatus(err)
	}
	return fuse.OK
}

func (fs *ObjectFS) Fsync(cancel <-chan struct{}, input *fuse.FsyncIn) fuse.Status {
	ctx, cancelFunc := makeContext(cancel)
	defer cancelFunc()

	if err := fs.syncFileToService(ctx, input.NodeId); err != nil {
		return grpcErrorToStatus(err)
	}

	resp, err := fs.client.Fsync(ctx, &pb.FsyncRequest{
		VolumeId: fs.volumeID,
		Inode:    input.NodeId,
	})
	if err != nil {
		return grpcErrorToStatus(err)
	}
	if resp.GetError() != 0 {
		return fuse.Status(resp.GetError())
	}
	return fuse.OK
}

func (fs *ObjectFS) Release(cancel <-chan struct{}, input *fuse.ReleaseIn) {
	ctx, cancelFunc := makeContext(cancel)
	defer cancelFunc()

	_ = fs.syncFileToService(ctx, input.NodeId)
}

func (fs *ObjectFS) StatFs(cancel <-chan struct{}, input *fuse.InHeader, out *fuse.StatfsOut) fuse.Status {
	out.Blocks = 1024 * 1024 * 1024
	out.Bfree = 1024 * 1024 * 1024
	out.Bavail = 1024 * 1024 * 1024
	out.Bsize = 4096
	out.Frsize = 4096
	out.Files = 1000000
	out.Ffree = 1000000
	out.NameLen = 255
	return fuse.OK
}

func (fs *ObjectFS) Access(cancel <-chan struct{}, input *fuse.AccessIn) fuse.Status {
	return fuse.OK
}

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
				if ev.GetInode() != 0 {
					cache.InvalidateIfNotDirty(ev.GetInode())
				}
			}
			time.Sleep(500 * time.Millisecond)
		}
	}()
}
