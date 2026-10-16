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

package main

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"syscall"
	"time"

	pb "github.com/gke-labs/in-cluster-storage/pkg/api/objectfs/v1alpha1"
	"github.com/gke-labs/in-cluster-storage/pkg/errutil"
	"golang.org/x/sys/unix"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
	"k8s.io/klog/v2"
)

type OpType string

const (
	OpMkdir        OpType = "mkdir"
	OpCreateFile   OpType = "create_file"
	OpWriteFile    OpType = "write_file"
	OpTruncateFile OpType = "truncate_file"
	OpSymlink      OpType = "symlink"
	OpUnlink       OpType = "unlink"
	OpRmdir        OpType = "rmdir"
	OpSetAttr      OpType = "set_attr"
)

type FileOp struct {
	Type   OpType
	Path   string // relative path from volume root (e.g. "dir/file.txt")
	Parent string // relative path of parent dir (e.g. "dir" or "")
	Name   string // base name (e.g. "file.txt")
	Mode   uint32
	Size   int64
	Mtime  time.Time
	Uid    uint32
	Gid    uint32
	Target string // for symlink
	Data   []byte // for create or write
	Offset int64  // for write
}

func isWhiteout(path string, info os.FileInfo) bool {
	sys, ok := info.Sys().(*syscall.Stat_t)
	if ok && (info.Mode()&os.ModeCharDevice != 0) && (sys.Rdev == 0) {
		return true
	}
	// Also check xattr for user-namespace overlayfs or unprivileged test environments
	buf := make([]byte, 16)
	if sz, err := unix.Getxattr(path, "user.overlay.whiteout", buf); err == nil && sz >= 0 {
		return true
	}
	if sz, err := unix.Getxattr(path, "trusted.overlay.whiteout", buf); err == nil && sz >= 0 {
		return true
	}
	return false
}

func isOpaqueDir(path string) bool {
	buf := make([]byte, 16)
	sz, err := unix.Getxattr(path, "trusted.overlay.opaque", buf)
	if err == nil && sz > 0 && string(buf[:sz]) == "y" {
		return true
	}
	sz, err = unix.Getxattr(path, "user.overlay.opaque", buf)
	if err == nil && sz > 0 && string(buf[:sz]) == "y" {
		return true
	}
	return false
}

func splitRelPath(relPath string) (string, string) {
	relPath = filepath.Clean(relPath)
	if relPath == "." || relPath == "" || relPath == "/" {
		return "", ""
	}
	parent := filepath.Dir(relPath)
	if parent == "." {
		parent = ""
	}
	return parent, filepath.Base(relPath)
}

func pathDepth(p string) int {
	clean := filepath.Clean(p)
	if clean == "." || clean == "" {
		return 0
	}
	return strings.Count(clean, string(filepath.Separator)) + 1
}

// ScanUpperLayer compares lowerDir and upperDir and produces an ordered list of FileOps.
// Renames appear as unlink+create; hard links are scanned as independent files.
func ScanUpperLayer(lowerDir, upperDir string) ([]FileOp, error) {
	if _, err := os.Stat(upperDir); os.IsNotExist(err) {
		return nil, nil
	}

	deletedInLower := make(map[string]bool)

	// Phase 1: Scan whiteouts and opaque directories for deletions
	deletions, err := scanWhiteoutsAndOpaqueDirs(lowerDir, upperDir, deletedInLower)
	if err != nil {
		return nil, err
	}

	// Phase 2: Scan creations, modifications, symlinks, and directory creations
	creations, writes, attrs, err := scanModificationsAndCreations(lowerDir, upperDir, deletedInLower)
	if err != nil {
		return nil, err
	}

	// Sort deletions: deepest paths first
	sort.Slice(deletions, func(i, j int) bool {
		return pathDepth(deletions[i].Path) > pathDepth(deletions[j].Path)
	})

	// Sort creations: shallowest paths first (so parents are created before children)
	sort.Slice(creations, func(i, j int) bool {
		return pathDepth(creations[i].Path) < pathDepth(creations[j].Path)
	})

	// Sort attrs: deepest directories last (bottom-up)
	sort.Slice(attrs, func(i, j int) bool {
		return pathDepth(attrs[i].Path) > pathDepth(attrs[j].Path)
	})

	var result []FileOp
	result = append(result, deletions...)
	result = append(result, creations...)
	result = append(result, writes...)
	result = append(result, attrs...)

	return result, nil
}

func scanWhiteoutsAndOpaqueDirs(lowerDir, upperDir string, deletedInLower map[string]bool) ([]FileOp, error) {
	var deletions []FileOp

	err := filepath.Walk(upperDir, func(path string, info os.FileInfo, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		relPath, relErr := filepath.Rel(upperDir, path)
		if relErr != nil {
			return relErr
		}
		if relPath == "." {
			return nil
		}

		if isWhiteout(path, info) {
			lowerPath := filepath.Join(lowerDir, relPath)
			if lInfo, lErr := os.Lstat(lowerPath); lErr == nil {
				if lInfo.IsDir() {
					// Recursively delete lower directory contents
					var subDirs []string
					wErr := filepath.Walk(lowerPath, func(subPath string, subInfo os.FileInfo, err error) error {
						if err != nil {
							return err
						}
						subRel, err := filepath.Rel(lowerDir, subPath)
						if err != nil {
							return err
						}
						if subRel == "." {
							return nil
						}
						if subInfo.IsDir() {
							subDirs = append(subDirs, subRel)
						} else {
							if !deletedInLower[subRel] {
								deletedInLower[subRel] = true
								p, n := splitRelPath(subRel)
								deletions = append(deletions, FileOp{
									Type:   OpUnlink,
									Path:   subRel,
									Parent: p,
									Name:   n,
								})
							}
						}
						return nil
					})
					if wErr != nil {
						return fmt.Errorf("walk lower %s: %w", lowerPath, wErr)
					}
					// Sort subdirs deepest first
					sort.Slice(subDirs, func(i, j int) bool {
						return pathDepth(subDirs[i]) > pathDepth(subDirs[j])
					})
					for _, sd := range subDirs {
						if !deletedInLower[sd] {
							deletedInLower[sd] = true
							p, n := splitRelPath(sd)
							deletions = append(deletions, FileOp{
								Type:   OpRmdir,
								Path:   sd,
								Parent: p,
								Name:   n,
							})
						}
					}
					if !deletedInLower[relPath] {
						deletedInLower[relPath] = true
						p, n := splitRelPath(relPath)
						deletions = append(deletions, FileOp{
							Type:   OpRmdir,
							Path:   relPath,
							Parent: p,
							Name:   n,
						})
					}
				} else {
					if !deletedInLower[relPath] {
						deletedInLower[relPath] = true
						p, n := splitRelPath(relPath)
						deletions = append(deletions, FileOp{
							Type:   OpUnlink,
							Path:   relPath,
							Parent: p,
							Name:   n,
						})
					}
				}
			} else {
				if !deletedInLower[relPath] {
					deletedInLower[relPath] = true
					p, n := splitRelPath(relPath)
					deletions = append(deletions, FileOp{
						Type:   OpUnlink,
						Path:   relPath,
						Parent: p,
						Name:   n,
					})
				}
			}
			return nil
		}

		if info.IsDir() && isOpaqueDir(path) {
			// Opaque directory: prune any entries in lowerDir under relPath not in upperDir
			lowerPath := filepath.Join(lowerDir, relPath)
			if lInfo, lErr := os.Stat(lowerPath); lErr == nil && lInfo.IsDir() {
				var opaqueDeletedDirs []string
				wErr := filepath.Walk(lowerPath, func(subPath string, subInfo os.FileInfo, err error) error {
					if err != nil {
						return err
					}
					subRel, err := filepath.Rel(lowerDir, subPath)
					if err != nil {
						return err
					}
					if subRel == "." || subRel == relPath {
						return nil
					}
					// Check if present in upperDir
					if _, uErr := os.Lstat(filepath.Join(upperDir, subRel)); os.IsNotExist(uErr) {
						if subInfo.IsDir() {
							opaqueDeletedDirs = append(opaqueDeletedDirs, subRel)
						} else {
							if !deletedInLower[subRel] {
								deletedInLower[subRel] = true
								p, n := splitRelPath(subRel)
								deletions = append(deletions, FileOp{
									Type:   OpUnlink,
									Path:   subRel,
									Parent: p,
									Name:   n,
								})
							}
						}
					}
					return nil
				})
				if wErr != nil {
					return fmt.Errorf("walk lower opaque %s: %w", lowerPath, wErr)
				}
				sort.Slice(opaqueDeletedDirs, func(i, j int) bool {
					return pathDepth(opaqueDeletedDirs[i]) > pathDepth(opaqueDeletedDirs[j])
				})
				for _, sd := range opaqueDeletedDirs {
					if !deletedInLower[sd] {
						deletedInLower[sd] = true
						p, n := splitRelPath(sd)
						deletions = append(deletions, FileOp{
							Type:   OpRmdir,
							Path:   sd,
							Parent: p,
							Name:   n,
						})
					}
				}
			}
		}

		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("walk upper for whiteouts: %w", err)
	}

	return deletions, nil
}

func scanModificationsAndCreations(lowerDir, upperDir string, deletedInLower map[string]bool) (creations, writes, attrs []FileOp, err error) {
	err = filepath.Walk(upperDir, func(path string, info os.FileInfo, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		relPath, relErr := filepath.Rel(upperDir, path)
		if relErr != nil {
			return relErr
		}
		if relPath == "." {
			return nil
		}

		if isWhiteout(path, info) {
			return nil
		}

		mode := uint32(info.Mode() & 07777)
		fileType := info.Mode() & os.ModeType

		var uid, gid uint32
		if sys, ok := info.Sys().(*syscall.Stat_t); ok {
			uid = sys.Uid
			gid = sys.Gid
		}

		parent, name := splitRelPath(relPath)
		lowerPath := filepath.Join(lowerDir, relPath)
		lStat, lErr := os.Lstat(lowerPath)
		existsInLower := (lErr == nil) && !deletedInLower[relPath]

		if info.IsDir() {
			if !existsInLower {
				creations = append(creations, FileOp{
					Type:   OpMkdir,
					Path:   relPath,
					Parent: parent,
					Name:   name,
					Mode:   mode,
					Uid:    uid,
					Gid:    gid,
				})
			}
			attrs = append(attrs, FileOp{
				Type:   OpSetAttr,
				Path:   relPath,
				Parent: parent,
				Name:   name,
				Mode:   mode,
				Mtime:  info.ModTime(),
				Uid:    uid,
				Gid:    gid,
			})
			return nil
		}

		if (info.Mode() & os.ModeSymlink) != 0 {
			target, rErr := os.Readlink(path)
			if rErr != nil {
				return fmt.Errorf("readlink %s: %w", path, rErr)
			}
			if existsInLower {
				if lStat.IsDir() {
					creations = append(creations, FileOp{Type: OpRmdir, Path: relPath, Parent: parent, Name: name})
				} else {
					creations = append(creations, FileOp{Type: OpUnlink, Path: relPath, Parent: parent, Name: name})
				}
			}
			creations = append(creations, FileOp{
				Type:   OpSymlink,
				Path:   relPath,
				Parent: parent,
				Name:   name,
				Target: target,
				Uid:    uid,
				Gid:    gid,
			})
			return nil
		}

		if fileType != 0 {
			// Special files (fifos, sockets, character/block devices) are skipped with a warning
			klog.Warningf("exclusive-local: skipping unsupported special file %s (mode %v)", relPath, info.Mode())
			return nil
		}

		// Regular file
		fileSize := info.Size()

		if !existsInLower || (lStat != nil && lStat.IsDir()) {
			if existsInLower && lStat.IsDir() {
				creations = append(creations, FileOp{Type: OpRmdir, Path: relPath, Parent: parent, Name: name})
			}

			// Read first 64 KiB chunk for initial_content
			f, oErr := os.Open(path)
			if oErr != nil {
				return oErr
			}

			initialChunkSize := int64(64 * 1024)
			if fileSize < initialChunkSize {
				initialChunkSize = fileSize
			}
			initBuf := make([]byte, initialChunkSize)
			if initialChunkSize > 0 {
				if _, err := io.ReadFull(f, initBuf); err != nil {
					errutil.CloseAndLog(f, "upper file")
					return fmt.Errorf("read initial content of %s: %w", path, err)
				}
			}

			creations = append(creations, FileOp{
				Type:   OpCreateFile,
				Path:   relPath,
				Parent: parent,
				Name:   name,
				Mode:   mode,
				Size:   fileSize,
				Uid:    uid,
				Gid:    gid,
				Data:   initBuf,
			})

			// Remaining chunks
			offset := int64(len(initBuf))
			for offset < fileSize {
				chunkLen := int64(64 * 1024)
				if offset+chunkLen > fileSize {
					chunkLen = fileSize - offset
				}
				chunkBuf := make([]byte, chunkLen)
				if _, err := io.ReadFull(f, chunkBuf); err != nil {
					errutil.CloseAndLog(f, "upper file")
					return fmt.Errorf("read chunk of %s at %d: %w", path, offset, err)
				}
				writes = append(writes, FileOp{
					Type:   OpWriteFile,
					Path:   relPath,
					Offset: offset,
					Data:   chunkBuf,
				})
				offset += chunkLen
			}
			errutil.CloseAndLog(f, "upper file")
		} else {
			// Modified file
			f, oErr := os.Open(path)
			if oErr != nil {
				return oErr
			}

			if lStat != nil && lStat.Size() != fileSize {
				writes = append(writes, FileOp{
					Type: OpTruncateFile,
					Path: relPath,
					Size: fileSize,
				})
			}

			offset := int64(0)
			for offset < fileSize {
				chunkLen := int64(64 * 1024)
				if offset+chunkLen > fileSize {
					chunkLen = fileSize - offset
				}
				chunkBuf := make([]byte, chunkLen)
				if _, err := io.ReadFull(f, chunkBuf); err != nil {
					errutil.CloseAndLog(f, "upper file")
					return fmt.Errorf("read chunk of modified file %s at %d: %w", path, offset, err)
				}
				writes = append(writes, FileOp{
					Type:   OpWriteFile,
					Path:   relPath,
					Offset: offset,
					Data:   chunkBuf,
				})
				offset += chunkLen
			}
			errutil.CloseAndLog(f, "upper file")
		}

		attrs = append(attrs, FileOp{
			Type:   OpSetAttr,
			Path:   relPath,
			Parent: parent,
			Name:   name,
			Mode:   mode,
			Mtime:  info.ModTime(),
			Uid:    uid,
			Gid:    gid,
		})

		return nil
	})
	if err != nil {
		return nil, nil, nil, fmt.Errorf("walk upper for modifications: %w", err)
	}

	return creations, writes, attrs, nil
}

// -----------------------------------------------------------------------------
// Checkpoint Replay
// -----------------------------------------------------------------------------

func resolveInode(ctx context.Context, client pb.ObjectFSControllerClient, volumeID string, pathInodes map[string]uint64, relPath string) (uint64, error) {
	relPath = filepath.Clean(relPath)
	if relPath == "." || relPath == "" || relPath == "/" {
		return 1, nil
	}
	if ino, ok := pathInodes[relPath]; ok {
		return ino, nil
	}
	parts := strings.Split(relPath, string(filepath.Separator))
	currIno := uint64(1)
	currPath := ""
	for _, part := range parts {
		if part == "" || part == "." {
			continue
		}
		if currPath == "" {
			currPath = part
		} else {
			currPath = currPath + "/" + part
		}
		if ino, ok := pathInodes[currPath]; ok {
			currIno = ino
			continue
		}
		resp, err := client.Lookup(ctx, &pb.LookupRequest{
			VolumeId:    volumeID,
			ParentInode: currIno,
			Name:        part,
		})
		if err != nil {
			return 0, fmt.Errorf("lookup %q under ino %d: %w", part, currIno, err)
		}
		if resp.GetError() != 0 {
			return 0, fmt.Errorf("lookup %q under ino %d returned error: %d", part, currIno, resp.GetError())
		}
		ino := resp.GetAttr().GetInode().GetIno()
		pathInodes[currPath] = ino
		currIno = ino
	}
	return currIno, nil
}

// ReplayOperations executes the scanned operations sequentially as RPCs against the controller.
func ReplayOperations(ctx context.Context, client pb.ObjectFSControllerClient, volumeID string, ops []FileOp) error {
	pathInodes := make(map[string]uint64)
	pathInodes[""] = 1
	pathInodes["."] = 1

	touchedInodes := make(map[uint64]bool)

	for _, op := range ops {
		switch op.Type {
		case OpUnlink:
			parentIno, err := resolveInode(ctx, client, volumeID, pathInodes, op.Parent)
			if err != nil {
				klog.V(4).Infof("resolveInode parent for unlink %s: %v (skipping)", op.Path, err)
				continue
			}
			resp, err := client.Unlink(ctx, &pb.UnlinkRequest{
				VolumeId:    volumeID,
				ParentInode: parentIno,
				Name:        op.Name,
			})
			if err != nil {
				return fmt.Errorf("unlink %s (parent %d, name %s): %w", op.Path, parentIno, op.Name, err)
			}
			// Treat ENOENT as success: if a previous cleanup removed lower/ but not upper/,
			// whiteouts without a lower counterpart replay as unlinks of names that may not exist.
			if resp.GetError() != 0 && resp.GetError() != int32(syscall.ENOENT) {
				return fmt.Errorf("unlink %s returned error: %d", op.Path, resp.GetError())
			}
			delete(pathInodes, op.Path)
			touchedInodes[parentIno] = true

		case OpRmdir:
			parentIno, err := resolveInode(ctx, client, volumeID, pathInodes, op.Parent)
			if err != nil {
				klog.V(4).Infof("resolveInode parent for rmdir %s: %v (skipping)", op.Path, err)
				continue
			}
			resp, err := client.Rmdir(ctx, &pb.RmdirRequest{
				VolumeId:    volumeID,
				ParentInode: parentIno,
				Name:        op.Name,
			})
			if err != nil {
				return fmt.Errorf("rmdir %s (parent %d, name %s): %w", op.Path, parentIno, op.Name, err)
			}
			// Treat ENOENT as success if the directory does not exist on controller.
			if resp.GetError() != 0 && resp.GetError() != int32(syscall.ENOENT) {
				return fmt.Errorf("rmdir %s returned error: %d", op.Path, resp.GetError())
			}
			delete(pathInodes, op.Path)
			touchedInodes[parentIno] = true

		case OpMkdir:
			parentIno, err := resolveInode(ctx, client, volumeID, pathInodes, op.Parent)
			if err != nil {
				return fmt.Errorf("resolve parent for mkdir %s: %w", op.Path, err)
			}
			mode := op.Mode
			if mode == 0 {
				mode = 0755
			}
			resp, err := client.Mkdir(ctx, &pb.MkdirRequest{
				VolumeId:    volumeID,
				ParentInode: parentIno,
				Name:        op.Name,
				Mode:        mode,
				Uid:         op.Uid,
				Gid:         op.Gid,
			})
			if err != nil {
				return fmt.Errorf("mkdir %s: %w", op.Path, err)
			}
			if resp.GetError() != 0 {
				return fmt.Errorf("mkdir %s returned error: %d", op.Path, resp.GetError())
			}
			newIno := resp.GetAttr().GetInode().GetIno()
			pathInodes[op.Path] = newIno
			touchedInodes[newIno] = true
			touchedInodes[parentIno] = true

		case OpCreateFile:
			parentIno, err := resolveInode(ctx, client, volumeID, pathInodes, op.Parent)
			if err != nil {
				return fmt.Errorf("resolve parent for create %s: %w", op.Path, err)
			}
			mode := op.Mode
			if mode == 0 {
				mode = 0644
			}
			resp, err := client.CreateFile(ctx, &pb.CreateFileRequest{
				VolumeId:       volumeID,
				ParentInode:    parentIno,
				Name:           op.Name,
				Mode:           mode,
				Uid:            op.Uid,
				Gid:            op.Gid,
				InitialContent: op.Data,
			})
			if err != nil {
				return fmt.Errorf("create file %s: %w", op.Path, err)
			}
			if resp.GetError() != 0 {
				return fmt.Errorf("create file %s returned error: %d", op.Path, resp.GetError())
			}
			newIno := resp.GetAttr().GetInode().GetIno()
			pathInodes[op.Path] = newIno
			touchedInodes[newIno] = true
			touchedInodes[parentIno] = true

		case OpSymlink:
			parentIno, err := resolveInode(ctx, client, volumeID, pathInodes, op.Parent)
			if err != nil {
				return fmt.Errorf("resolve parent for symlink %s: %w", op.Path, err)
			}
			resp, err := client.Symlink(ctx, &pb.SymlinkRequest{
				VolumeId:    volumeID,
				ParentInode: parentIno,
				Name:        op.Name,
				Target:      op.Target,
				Uid:         op.Uid,
				Gid:         op.Gid,
			})
			if err != nil {
				return fmt.Errorf("symlink %s: %w", op.Path, err)
			}
			if resp.GetError() != 0 {
				return fmt.Errorf("symlink %s returned error: %d", op.Path, resp.GetError())
			}
			newIno := resp.GetAttr().GetInode().GetIno()
			pathInodes[op.Path] = newIno
			touchedInodes[newIno] = true
			touchedInodes[parentIno] = true

		case OpTruncateFile:
			ino, err := resolveInode(ctx, client, volumeID, pathInodes, op.Path)
			if err != nil {
				return fmt.Errorf("resolve inode for truncate %s: %w", op.Path, err)
			}
			resp, err := client.TruncateFile(ctx, &pb.TruncateFileRequest{
				VolumeId: volumeID,
				Inode:    ino,
				Size:     op.Size,
			})
			if err != nil {
				return fmt.Errorf("truncate file %s: %w", op.Path, err)
			}
			if resp.GetError() != 0 {
				return fmt.Errorf("truncate file %s returned error: %d", op.Path, resp.GetError())
			}
			touchedInodes[ino] = true

		case OpWriteFile:
			ino, err := resolveInode(ctx, client, volumeID, pathInodes, op.Path)
			if err != nil {
				return fmt.Errorf("resolve inode for write %s: %w", op.Path, err)
			}
			resp, err := client.WriteFile(ctx, &pb.WriteFileRequest{
				VolumeId:  volumeID,
				Inode:     ino,
				Offset:    op.Offset,
				Data:      op.Data,
				WriteMode: pb.WriteMode_WRITE_THROUGH_FSYNC,
			})
			if err != nil {
				return fmt.Errorf("write file %s (offset %d): %w", op.Path, op.Offset, err)
			}
			if resp.GetError() != 0 {
				return fmt.Errorf("write file %s (offset %d) returned error: %d", op.Path, op.Offset, resp.GetError())
			}
			touchedInodes[ino] = true

		case OpSetAttr:
			ino, err := resolveInode(ctx, client, volumeID, pathInodes, op.Path)
			if err != nil {
				klog.V(4).Infof("resolve inode for setAttr %s: %v", op.Path, err)
				continue
			}
			req := &pb.SetAttrRequest{
				VolumeId: volumeID,
				Inode:    ino,
			}
			if op.Mode != 0 {
				req.Mode = proto.Uint32(op.Mode)
			}
			if op.Uid != 0 {
				req.Uid = proto.Uint32(op.Uid)
			}
			if op.Gid != 0 {
				req.Gid = proto.Uint32(op.Gid)
			}
			if !op.Mtime.IsZero() {
				req.Mtime = timestamppb.New(op.Mtime)
			}
			resp, err := client.SetAttr(ctx, req)
			if err != nil {
				return fmt.Errorf("setAttr %s: %w", op.Path, err)
			}
			if resp.GetError() != 0 {
				return fmt.Errorf("setAttr %s returned error: %d", op.Path, resp.GetError())
			}
			touchedInodes[ino] = true
		}
	}

	// Always fsync root inode 1
	touchedInodes[1] = true

	// Issue blocking fsync per touched inode
	for ino := range touchedInodes {
		fsyncResp, err := client.Fsync(ctx, &pb.FsyncRequest{
			VolumeId: volumeID,
			Inode:    ino,
			Blocking: true,
		})
		if err != nil {
			return fmt.Errorf("fsync ino %d: %w", ino, err)
		}
		if fsyncResp.GetError() != 0 {
			return fmt.Errorf("fsync ino %d returned error: %d", ino, fsyncResp.GetError())
		}
	}

	return nil
}
