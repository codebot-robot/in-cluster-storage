// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package controller

import (
	"fmt"
	"syscall"

	pb "github.com/gke-labs/in-cluster-storage/pkg/api/objectfs/v1alpha1"
	"google.golang.org/protobuf/proto"
)

const (
	InodeTypeName       = "objectfs.v1alpha1.Inode"
	FileChunkTypeName   = "objectfs.v1alpha1.FileChunk"
	VolumeStatsTypeName = "objectfs.v1alpha1.VolumeStats"
	VolumeStatsRowName  = "volume"
)

func inodeType(in *pb.Inode) string {
	mode := in.GetMode()
	if in.GetIsDir() || (mode&syscall.S_IFMT) == syscall.S_IFDIR {
		return "dir"
	}
	if len(in.GetSymlinkTarget()) > 0 || (mode&syscall.S_IFMT) == syscall.S_IFLNK {
		return "symlink"
	}
	if (mode&syscall.S_IFMT) == syscall.S_IFREG || (mode&(syscall.S_IFCHR|syscall.S_IFBLK|syscall.S_IFIFO|syscall.S_IFSOCK)) == 0 {
		return "file"
	}
	return "other"
}

func adjustInodeCount(s *pb.VolumeStats, kind string, delta int64) {
	switch kind {
	case "file":
		s.InodesFile += delta
	case "dir":
		s.InodesDir += delta
	case "symlink":
		s.InodesSymlink += delta
	case "other":
		s.InodesOther += delta
	}
}

// UpdateStatsForInodeChange updates VolumeStats for an Inode mutation.
func UpdateStatsForInodeChange(s *pb.VolumeStats, before, after *pb.Inode) {
	if before == nil && after != nil {
		if after.GetIno() > s.GetMaxIno() {
			s.MaxIno = after.GetIno()
		}
		adjustInodeCount(s, inodeType(after), 1)
		if inodeType(after) == "file" && after.GetSize() > 0 {
			s.LogicalBytes += after.GetSize()
		}
	} else if before != nil && after != nil {
		bKind, aKind := inodeType(before), inodeType(after)
		if bKind != aKind {
			adjustInodeCount(s, bKind, -1)
			adjustInodeCount(s, aKind, 1)
		}
		if aKind == "file" {
			s.LogicalBytes += (after.GetSize() - before.GetSize())
		}
	} else if before != nil && after == nil {
		bKind := inodeType(before)
		adjustInodeCount(s, bKind, -1)
		if bKind == "file" && before.GetSize() > 0 {
			s.LogicalBytes -= before.GetSize()
		}
	}
}

// UpdateStatsForFileChunkChange updates VolumeStats for a FileChunk mutation.
func UpdateStatsForFileChunkChange(s *pb.VolumeStats, before, after *pb.FileChunk) {
	if before == nil && after != nil {
		s.FileChunks++
	} else if before != nil && after == nil {
		s.FileChunks--
	}
}

// UpdateVolumeStats is the generic dispatcher for updating VolumeStats from before/after row changes.
func UpdateVolumeStats(stats, before, after proto.Message) error {
	s, ok := stats.(*pb.VolumeStats)
	if !ok {
		return fmt.Errorf("expected *pb.VolumeStats, got %T", stats)
	}

	if before == nil && after == nil {
		return nil
	}

	target := after
	if target == nil {
		target = before
	}

	switch target.(type) {
	case *pb.Inode:
		var bIn, aIn *pb.Inode
		if before != nil {
			var ok bool
			if bIn, ok = before.(*pb.Inode); !ok {
				return fmt.Errorf("expected before to be *pb.Inode, got %T", before)
			}
		}
		if after != nil {
			var ok bool
			if aIn, ok = after.(*pb.Inode); !ok {
				return fmt.Errorf("expected after to be *pb.Inode, got %T", after)
			}
		}
		UpdateStatsForInodeChange(s, bIn, aIn)
		return nil

	case *pb.FileChunk:
		var bChunk, aChunk *pb.FileChunk
		if before != nil {
			var ok bool
			if bChunk, ok = before.(*pb.FileChunk); !ok {
				return fmt.Errorf("expected before to be *pb.FileChunk, got %T", before)
			}
		}
		if after != nil {
			var ok bool
			if aChunk, ok = after.(*pb.FileChunk); !ok {
				return fmt.Errorf("expected after to be *pb.FileChunk, got %T", after)
			}
		}
		UpdateStatsForFileChunkChange(s, bChunk, aChunk)
		return nil

	case *pb.DirEntry:
		// DirEntry row changes do not affect VolumeStats
		return nil

	default:
		typeName := string(target.ProtoReflect().Descriptor().FullName())
		if typeName == VolumeStatsTypeName {
			return nil
		}
		return fmt.Errorf("unexpected message type for stats update: %T (%s)", target, typeName)
	}
}
