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
	"fmt"
	"sync"

	pb "github.com/gke-labs/in-cluster-storage/pkg/api/objectfs/v1alpha1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type Server struct {
	pb.UnimplementedObjectFSControllerServer
	mu          sync.RWMutex
	volumes     map[string]*Volume
	backend     ObjectStorageBackend
	broadcaster *EventBroadcaster
}

func NewServer(backend ObjectStorageBackend) *Server {
	if backend == nil {
		backend = NewMemoryBackend()
	}
	return &Server{
		volumes:     make(map[string]*Volume),
		backend:     backend,
		broadcaster: NewEventBroadcaster(),
	}
}

func (s *Server) getOrCreateVolume(volumeID string) *Volume {
	s.mu.Lock()
	defer s.mu.Unlock()

	vol, ok := s.volumes[volumeID]
	if !ok {
		vol = NewVolume(volumeID, s.backend, s.broadcaster)
		s.volumes[volumeID] = vol
	}
	return vol
}

func (s *Server) GetAttr(ctx context.Context, req *pb.GetAttrRequest) (*pb.GetAttrResponse, error) {
	if req.GetVolumeId() == "" {
		return nil, status.Error(codes.InvalidArgument, "volume_id is required")
	}
	vol := s.getOrCreateVolume(req.GetVolumeId())
	attr, err := vol.GetAttr(ctx, req.GetPath())
	if err != nil {
		return nil, status.Errorf(codes.NotFound, "failed to get attr: %v", err)
	}
	return &pb.GetAttrResponse{Attr: attr}, nil
}

func (s *Server) Lookup(ctx context.Context, req *pb.LookupRequest) (*pb.LookupResponse, error) {
	if req.GetVolumeId() == "" {
		return nil, status.Error(codes.InvalidArgument, "volume_id is required")
	}
	vol := s.getOrCreateVolume(req.GetVolumeId())
	attr, err := vol.Lookup(ctx, req.GetParentPath(), req.GetName())
	if err != nil {
		return nil, status.Errorf(codes.NotFound, "lookup failed: %v", err)
	}
	return &pb.LookupResponse{Attr: attr}, nil
}

func (s *Server) ReadDir(ctx context.Context, req *pb.ReadDirRequest) (*pb.ReadDirResponse, error) {
	if req.GetVolumeId() == "" {
		return nil, status.Error(codes.InvalidArgument, "volume_id is required")
	}
	vol := s.getOrCreateVolume(req.GetVolumeId())
	entries, err := vol.ReadDir(ctx, req.GetPath())
	if err != nil {
		return nil, status.Errorf(codes.NotFound, "readdir failed: %v", err)
	}
	return &pb.ReadDirResponse{Entries: entries}, nil
}

func (s *Server) Mkdir(ctx context.Context, req *pb.MkdirRequest) (*pb.MkdirResponse, error) {
	if req.GetVolumeId() == "" {
		return nil, status.Error(codes.InvalidArgument, "volume_id is required")
	}
	vol := s.getOrCreateVolume(req.GetVolumeId())
	attr, err := vol.Mkdir(ctx, req.GetPath(), req.GetMode())
	if err != nil {
		return nil, status.Errorf(codes.Internal, "mkdir failed: %v", err)
	}
	return &pb.MkdirResponse{Attr: attr}, nil
}

func (s *Server) CreateFile(ctx context.Context, req *pb.CreateFileRequest) (*pb.CreateFileResponse, error) {
	if req.GetVolumeId() == "" {
		return nil, status.Error(codes.InvalidArgument, "volume_id is required")
	}
	vol := s.getOrCreateVolume(req.GetVolumeId())
	attr, err := vol.CreateFile(ctx, req.GetPath(), req.GetMode(), req.GetInitialContent())
	if err != nil {
		return nil, status.Errorf(codes.Internal, "create file failed: %v", err)
	}
	return &pb.CreateFileResponse{Attr: attr}, nil
}

func (s *Server) ReadFile(ctx context.Context, req *pb.ReadFileRequest) (*pb.ReadFileResponse, error) {
	if req.GetVolumeId() == "" {
		return nil, status.Error(codes.InvalidArgument, "volume_id is required")
	}
	vol := s.getOrCreateVolume(req.GetVolumeId())
	data, totalSize, redirectURL, err := vol.ReadFile(ctx, req.GetPath(), req.GetOffset(), req.GetSize())
	if err != nil {
		return nil, status.Errorf(codes.NotFound, "read file failed: %v", err)
	}

	eof := (req.GetOffset() + int64(len(data))) >= totalSize
	return &pb.ReadFileResponse{
		Data:        data,
		RedirectUrl: redirectURL,
		Eof:         eof,
		TotalSize:   totalSize,
	}, nil
}

func (s *Server) WriteFile(ctx context.Context, req *pb.WriteFileRequest) (*pb.WriteFileResponse, error) {
	if req.GetVolumeId() == "" {
		return nil, status.Error(codes.InvalidArgument, "volume_id is required")
	}
	vol := s.getOrCreateVolume(req.GetVolumeId())
	bytesWritten, newSize, modTime, err := vol.WriteFile(ctx, req.GetPath(), req.GetOffset(), req.GetData(), req.GetWriteMode())
	if err != nil {
		return nil, status.Errorf(codes.Internal, "write file failed: %v", err)
	}
	return &pb.WriteFileResponse{
		BytesWritten: bytesWritten,
		NewSize:      newSize,
		ModTime:      timestamppb.New(modTime),
	}, nil
}

func (s *Server) TruncateFile(ctx context.Context, req *pb.TruncateFileRequest) (*pb.TruncateFileResponse, error) {
	if req.GetVolumeId() == "" {
		return nil, status.Error(codes.InvalidArgument, "volume_id is required")
	}
	vol := s.getOrCreateVolume(req.GetVolumeId())
	attr, err := vol.TruncateFile(ctx, req.GetPath(), req.GetSize())
	if err != nil {
		return nil, status.Errorf(codes.Internal, "truncate failed: %v", err)
	}
	return &pb.TruncateFileResponse{Attr: attr}, nil
}

func (s *Server) Unlink(ctx context.Context, req *pb.UnlinkRequest) (*pb.UnlinkResponse, error) {
	if req.GetVolumeId() == "" {
		return nil, status.Error(codes.InvalidArgument, "volume_id is required")
	}
	vol := s.getOrCreateVolume(req.GetVolumeId())
	if err := vol.Unlink(ctx, req.GetPath()); err != nil {
		return nil, status.Errorf(codes.Internal, "unlink failed: %v", err)
	}
	return &pb.UnlinkResponse{Success: true}, nil
}

func (s *Server) Rmdir(ctx context.Context, req *pb.RmdirRequest) (*pb.RmdirResponse, error) {
	if req.GetVolumeId() == "" {
		return nil, status.Error(codes.InvalidArgument, "volume_id is required")
	}
	vol := s.getOrCreateVolume(req.GetVolumeId())
	if err := vol.Rmdir(ctx, req.GetPath()); err != nil {
		return nil, status.Errorf(codes.Internal, "rmdir failed: %v", err)
	}
	return &pb.RmdirResponse{Success: true}, nil
}

func (s *Server) Rename(ctx context.Context, req *pb.RenameRequest) (*pb.RenameResponse, error) {
	if req.GetVolumeId() == "" {
		return nil, status.Error(codes.InvalidArgument, "volume_id is required")
	}
	vol := s.getOrCreateVolume(req.GetVolumeId())
	attr, err := vol.Rename(ctx, req.GetOldPath(), req.GetNewPath())
	if err != nil {
		return nil, status.Errorf(codes.Internal, "rename failed: %v", err)
	}
	return &pb.RenameResponse{Attr: attr}, nil
}

func (s *Server) Fsync(ctx context.Context, req *pb.FsyncRequest) (*pb.FsyncResponse, error) {
	if req.GetVolumeId() == "" {
		return nil, status.Error(codes.InvalidArgument, "volume_id is required")
	}
	vol := s.getOrCreateVolume(req.GetVolumeId())
	if err := vol.Fsync(ctx, req.GetPath()); err != nil {
		return nil, status.Errorf(codes.Internal, "fsync failed: %v", err)
	}
	return &pb.FsyncResponse{Success: true}, nil
}

func (s *Server) WatchVolume(req *pb.WatchVolumeRequest, stream pb.ObjectFSController_WatchVolumeServer) error {
	if req.GetVolumeId() == "" {
		return status.Error(codes.InvalidArgument, "volume_id is required")
	}

	ch := s.broadcaster.Subscribe(req.GetVolumeId())
	defer s.broadcaster.Unsubscribe(req.GetVolumeId(), ch)

	ctx := stream.Context()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case event, ok := <-ch:
			if !ok {
				return nil
			}
			if err := stream.Send(event); err != nil {
				return fmt.Errorf("failed to send event: %w", err)
			}
		}
	}
}
