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
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/container-storage-interface/spec/lib/go/csi"
	pb "github.com/gke-labs/in-cluster-storage/pkg/api/objectfs/v1alpha1"
	"github.com/gke-labs/in-cluster-storage/pkg/objectfs/controller"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

// TestNodePublish_AccessModeValidation verifies that exclusive-local mode requires SINGLE_NODE_SINGLE_WRITER (ReadWriteOncePod).
func TestNodePublish_AccessModeValidation(t *testing.T) {
	ctx := t.Context()
	driver, err := newDriver("test-node", "localhost:50051", t.TempDir(), 1024*1024, 10*1024*1024, 30*time.Second, 10*time.Minute, true, 30*time.Second)
	if err != nil {
		t.Fatalf("newDriver failed: %v", err)
	}
	defer func() { _ = driver.Close() }()

	tests := []struct {
		name       string
		mode       string
		accessMode csi.VolumeCapability_AccessMode_Mode
		wantErr    bool
		wantCode   codes.Code
	}{
		{
			name:       "exclusive-local with SINGLE_NODE_WRITER (rejected)",
			mode:       "exclusive-local",
			accessMode: csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER,
			wantErr:    true,
			wantCode:   codes.InvalidArgument,
		},
		{
			name:       "exclusive-local with MULTI_NODE_MULTI_WRITER (rejected)",
			mode:       "exclusive-local",
			accessMode: csi.VolumeCapability_AccessMode_MULTI_NODE_MULTI_WRITER,
			wantErr:    true,
			wantCode:   codes.InvalidArgument,
		},
		{
			name:       "exclusive-local without capability (rejected)",
			mode:       "exclusive-local",
			accessMode: csi.VolumeCapability_AccessMode_UNKNOWN,
			wantErr:    true,
			wantCode:   codes.InvalidArgument,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := &csi.NodePublishVolumeRequest{
				VolumeId:   "test-vol",
				TargetPath: filepath.Join(t.TempDir(), "target"),
				VolumeCapability: &csi.VolumeCapability{
					AccessMode: &csi.VolumeCapability_AccessMode{
						Mode: tt.accessMode,
					},
				},
				VolumeContext: map[string]string{
					"volumeID": "test-vol",
					"mode":     tt.mode,
				},
			}
			_, err := driver.NodePublishVolume(ctx, req)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("Expected error, got nil")
				}
				st, ok := status.FromError(err)
				if !ok || st.Code() != tt.wantCode {
					t.Errorf("Expected grpc error code %v, got %v (err: %v)", tt.wantCode, st.Code(), err)
				}
			}
		})
	}
}

// TestNodeGetCapabilities_IncludesSingleNodeMultiWriter verifies advertising capability for ReadWriteOncePod.
func TestNodeGetCapabilities_IncludesSingleNodeMultiWriter(t *testing.T) {
	ctx := t.Context()
	driver, err := newDriver("test-node", "localhost:50051", t.TempDir(), 1024*1024, 10*1024*1024, 30*time.Second, 10*time.Minute, true, 30*time.Second)
	if err != nil {
		t.Fatalf("newDriver failed: %v", err)
	}
	defer func() { _ = driver.Close() }()

	resp, err := driver.NodeGetCapabilities(ctx, &csi.NodeGetCapabilitiesRequest{})
	if err != nil {
		t.Fatalf("NodeGetCapabilities failed: %v", err)
	}

	found := false
	for _, cap := range resp.GetCapabilities() {
		if cap.GetRpc().GetType() == csi.NodeServiceCapability_RPC_SINGLE_NODE_MULTI_WRITER {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("Expected capability SINGLE_NODE_MULTI_WRITER in NodeGetCapabilities")
	}
}

// TestMaterialiseVolume verifies eager materialisation downloads directories and files.
func TestMaterialiseVolume(t *testing.T) {
	ctx := t.Context()
	server := controller.NewServer(nil)
	volumeID := "test-mat-vol"

	lis := bufconn.Listen(1024 * 1024)
	grpcServer := grpc.NewServer()
	pb.RegisterObjectFSControllerServer(grpcServer, server)
	go func() { _ = grpcServer.Serve(lis) }()
	defer grpcServer.Stop()

	conn, err := grpc.NewClient("passthrough://bufnet",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return lis.Dial()
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("grpc.NewClient failed: %v", err)
	}
	defer conn.Close()
	client := pb.NewObjectFSControllerClient(conn)

	// Create /dir/file.txt and /root_file.txt in volume
	mkdirResp, err := client.Mkdir(ctx, &pb.MkdirRequest{
		VolumeId:    volumeID,
		ParentInode: 1,
		Name:        "dir",
		Mode:        0755,
	})
	if err != nil || mkdirResp.GetError() != 0 {
		t.Fatalf("Mkdir dir failed: %v, resp: %v", err, mkdirResp)
	}
	dirIno := mkdirResp.GetAttr().GetInode().GetIno()

	_, err = client.CreateFile(ctx, &pb.CreateFileRequest{
		VolumeId:       volumeID,
		ParentInode:    dirIno,
		Name:           "file.txt",
		Mode:           0644,
		InitialContent: []byte("materialised content inside dir"),
	})
	if err != nil {
		t.Fatalf("CreateFile inside dir failed: %v", err)
	}

	_, err = client.CreateFile(ctx, &pb.CreateFileRequest{
		VolumeId:       volumeID,
		ParentInode:    1,
		Name:           "root_file.txt",
		Mode:           0644,
		InitialContent: []byte("root file data"),
	})
	if err != nil {
		t.Fatalf("CreateFile root_file failed: %v", err)
	}

	driver, err := newDriver("test-node", "passthrough://bufnet", t.TempDir(), 1024*1024, 10*1024*1024, 30*time.Second, 10*time.Minute, true, 30*time.Second)
	if err != nil {
		t.Fatalf("newDriver failed: %v", err)
	}
	defer func() { _ = driver.Close() }()

	lowerPath := filepath.Join(t.TempDir(), "lower")
	_ = os.MkdirAll(lowerPath, 0755)

	if err := driver.materialiseVolume(ctx, client, volumeID, lowerPath); err != nil {
		t.Fatalf("materialiseVolume failed: %v", err)
	}

	// Verify files in lowerPath
	data1, err := os.ReadFile(filepath.Join(lowerPath, "root_file.txt"))
	if err != nil || string(data1) != "root file data" {
		t.Errorf("root_file.txt: err=%v, content=%q", err, string(data1))
	}

	data2, err := os.ReadFile(filepath.Join(lowerPath, "dir", "file.txt"))
	if err != nil || string(data2) != "materialised content inside dir" {
		t.Errorf("dir/file.txt: err=%v, content=%q", err, string(data2))
	}
}

// TestMaterialiseVolume_Resumable verifies that materialiseVolume skips downloading files that
// are already present with matching size and mtime from a previous attempt.
func TestMaterialiseVolume_Resumable(t *testing.T) {
	ctx := t.Context()
	server := controller.NewServer(nil)
	volumeID := "test-resume-vol"

	lis := bufconn.Listen(1024 * 1024)
	grpcServer := grpc.NewServer()
	pb.RegisterObjectFSControllerServer(grpcServer, server)
	go func() { _ = grpcServer.Serve(lis) }()
	defer grpcServer.Stop()

	conn, err := grpc.NewClient("passthrough://bufnet",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return lis.Dial()
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("grpc.NewClient: %v", err)
	}
	defer conn.Close()
	client := pb.NewObjectFSControllerClient(conn)

	// Create file in volume
	createResp, err := client.CreateFile(ctx, &pb.CreateFileRequest{
		VolumeId:       volumeID,
		ParentInode:    1,
		Name:           "resumed_file.txt",
		Mode:           0644,
		InitialContent: []byte("resumed content"),
	})
	if err != nil || createResp.GetError() != 0 {
		t.Fatalf("CreateFile: %v, resp: %v", err, createResp)
	}
	fileMtime := createResp.GetAttr().GetInode().GetMtime().AsTime()

	driver, err := newDriver("test-node", "passthrough://bufnet", t.TempDir(), 1024*1024, 10*1024*1024, 30*time.Second, 10*time.Minute, true, 30*time.Second)
	if err != nil {
		t.Fatalf("newDriver: %v", err)
	}
	defer func() { _ = driver.Close() }()

	lowerPath := filepath.Join(t.TempDir(), "lower")
	_ = os.MkdirAll(lowerPath, 0755)

	// Pre-create the file in lowerPath with matching size and mtime
	targetFile := filepath.Join(lowerPath, "resumed_file.txt")
	if err := os.WriteFile(targetFile, []byte("resumed content"), 0644); err != nil {
		t.Fatalf("write target: %v", err)
	}
	if err := os.Chtimes(targetFile, fileMtime, fileMtime); err != nil {
		t.Fatalf("chtimes: %v", err)
	}
	initialStat, err := os.Stat(targetFile)
	if err != nil {
		t.Fatalf("stat: %v", err)
	}

	// Materialise
	if err := driver.materialiseVolume(ctx, client, volumeID, lowerPath); err != nil {
		t.Fatalf("materialiseVolume failed: %v", err)
	}

	afterStat, err := os.Stat(targetFile)
	if err != nil {
		t.Fatalf("stat after: %v", err)
	}
	// Verify mtime remained exactly the same
	if !afterStat.ModTime().Equal(initialStat.ModTime()) {
		t.Errorf("File was re-written rather than resumed: before=%v, after=%v", initialStat.ModTime(), afterStat.ModTime())
	}
}
