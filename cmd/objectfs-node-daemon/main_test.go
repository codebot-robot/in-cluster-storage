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
	"encoding/binary"
	"errors"
	"fmt"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/container-storage-interface/spec/lib/go/csi"
	pb "github.com/gke-labs/in-cluster-storage/pkg/api/objectfs/v1alpha1"
	"github.com/gke-labs/in-cluster-storage/pkg/objectfs/controller/controllertest"
	"github.com/gke-labs/in-cluster-storage/pkg/objectfs/fuseholder"
	"github.com/gke-labs/in-cluster-storage/pkg/objectfs/volumemode"
	"golang.org/x/sys/unix"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

func TestParseEndpoint(t *testing.T) {
	tests := []struct {
		endpoint  string
		wantProto string
		wantAddr  string
		wantErr   bool
	}{
		{"unix:///tmp/csi.sock", "unix", "/tmp/csi.sock", false},
		{"tcp://127.0.0.1:10000", "tcp", "127.0.0.1:10000", false},
		{"http://127.0.0.1:10000", "", "", true},
		{"invalid", "", "", true},
	}

	for _, tt := range tests {
		proto, addr, err := parseEndpoint(tt.endpoint)
		if (err != nil) != tt.wantErr {
			t.Errorf("parseEndpoint(%q) err = %v, wantErr %v", tt.endpoint, err, tt.wantErr)
		}
		if proto != tt.wantProto || addr != tt.wantAddr {
			t.Errorf("parseEndpoint(%q) = (%q, %q), want (%q, %q)", tt.endpoint, proto, addr, tt.wantProto, tt.wantAddr)
		}
	}
}

func TestNodePublishVolume_InvalidWriteModeFails(t *testing.T) {
	driver, err := newDriver("test-node", "localhost:50051", t.TempDir(), 1024*1024, 10*1024*1024, 30*time.Second, 10*time.Minute, true, 30*time.Second, "", true, "")
	if err != nil {
		t.Fatalf("newDriver: %v", err)
	}
	t.Cleanup(func() {
		if err := driver.Close(); err != nil {
			t.Errorf("driver.Close failed: %v", err)
		}
	})

	_, err = driver.NodePublishVolume(t.Context(), &csi.NodePublishVolumeRequest{
		VolumeId:      "test-vol",
		TargetPath:    t.TempDir(),
		VolumeContext: map[string]string{"writeMode": "writethrough"},
	})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("got status %v, want InvalidArgument", status.Code(err))
	}
}

func TestPluginInfo(t *testing.T) {
	driver, err := newDriver("test-node", "localhost:50051", t.TempDir(), 1024*1024, 10*1024*1024, 30*time.Second, 10*time.Minute, true, 30*time.Second, "", true, "")
	if err != nil {
		t.Fatalf("newDriver failed: %v", err)
	}
	defer func() { _ = driver.Close() }()
	ctx := t.Context()

	info, err := driver.GetPluginInfo(ctx, &csi.GetPluginInfoRequest{})
	if err != nil {
		t.Fatalf("GetPluginInfo failed: %v", err)
	}
	if info.GetName() != "objectfs.labs.gke.io" {
		t.Errorf("Expected driver name objectfs.labs.gke.io, got %s", info.GetName())
	}

	nodeInfo, err := driver.NodeGetInfo(ctx, &csi.NodeGetInfoRequest{})
	if err != nil {
		t.Fatalf("NodeGetInfo failed: %v", err)
	}
	if nodeInfo.GetNodeId() != "test-node" {
		t.Errorf("Expected node ID test-node, got %s", nodeInfo.GetNodeId())
	}
}

func TestNodeGetVolumeStats(t *testing.T) {
	ctx := t.Context()
	server := controllertest.NewTestServer(nil)
	volumeID := "test-csi-stats-vol"

	lis := bufconn.Listen(1024 * 1024)
	grpcServer := grpc.NewServer()
	pb.RegisterObjectFSControllerServer(grpcServer, server)
	go func() { _ = grpcServer.Serve(lis) }()
	defer grpcServer.Stop()

	driver, err := newDriver("test-node", "passthrough://bufnet", t.TempDir(), 1024*1024, 10*1024*1024, 30*time.Second, 10*time.Minute, true, 30*time.Second, "", true, "")
	if err != nil {
		t.Fatalf("newDriver failed: %v", err)
	}
	defer func() { _ = driver.Close() }()
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
	driver.client = pb.NewObjectFSControllerClient(conn)

	resp, err := driver.NodeGetVolumeStats(ctx, &csi.NodeGetVolumeStatsRequest{
		VolumeId: volumeID,
	})
	if err != nil {
		t.Fatalf("NodeGetVolumeStats failed: %v", err)
	}
	if len(resp.GetUsage()) != 2 {
		t.Fatalf("expected 2 usage entries (bytes and inodes), got %d", len(resp.GetUsage()))
	}
	bytesUsage := resp.GetUsage()[0]
	if bytesUsage.GetUnit() != csi.VolumeUsage_BYTES {
		t.Errorf("expected unit BYTES, got %v", bytesUsage.GetUnit())
	}
	inodesUsage := resp.GetUsage()[1]
	if inodesUsage.GetUnit() != csi.VolumeUsage_INODES {
		t.Errorf("expected unit INODES, got %v", inodesUsage.GetUnit())
	}
	if inodesUsage.GetUsed() != 1 { // root directory inode
		t.Errorf("expected 1 used inode, got %d", inodesUsage.GetUsed())
	}
}

func TestNodeDaemonSecurityFlags(t *testing.T) {
	// 1. Without insecure, non-existent client cert dir -> fails closed
	nonExistentDir := "/nonexistent/test/certs/client"
	driver, err := newDriver("test-node", "localhost:50051", t.TempDir(), 1024*1024, 10*1024*1024, 30*time.Second, 10*time.Minute, true, 30*time.Second, nonExistentDir, false, "")
	if err != nil {
		t.Fatalf("newDriver failed: %v", err)
	}
	defer func() {
		if closeErr := driver.Close(); closeErr != nil {
			t.Errorf("driver.Close failed: %v", closeErr)
		}
	}()

	_, err = driver.getClient()
	if err == nil {
		t.Fatalf("expected error from getClient with non-existent cert dir")
	}
	if !strings.Contains(err.Error(), nonExistentDir) {
		t.Errorf("expected error to name nonexistent cert dir %s, got: %v", nonExistentDir, err)
	}
}

func TestNodePublishVolume_ConsistencyModeValidation(t *testing.T) {
	driver, err := newDriver("test-node", "localhost:50051", t.TempDir(), 1024*1024, 10*1024*1024, 30*time.Second, 10*time.Minute, true, 30*time.Second, "", true, "")
	if err != nil {
		t.Fatalf("newDriver: %v", err)
	}
	t.Cleanup(func() {
		if err := driver.Close(); err != nil {
			t.Errorf("driver.Close failed: %v", err)
		}
	})

	_, err = driver.NodePublishVolume(t.Context(), &csi.NodePublishVolumeRequest{
		VolumeId:      "test-vol",
		TargetPath:    t.TempDir(),
		VolumeContext: map[string]string{"consistency": "close_to_open"},
	})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("got status %v, want InvalidArgument", status.Code(err))
	}
}

func TestNodePublishVolume_ExclusiveLocalMisspellingFails(t *testing.T) {
	driver, err := newDriver("test-node", "localhost:50051", t.TempDir(), 1024*1024, 10*1024*1024, 30*time.Second, 10*time.Minute, true, 30*time.Second, "", true, "")
	if err != nil {
		t.Fatalf("newDriver: %v", err)
	}
	t.Cleanup(func() {
		if err := driver.Close(); err != nil {
			t.Errorf("driver.Close failed: %v", err)
		}
	})

	_, err = driver.NodePublishVolume(t.Context(), &csi.NodePublishVolumeRequest{
		VolumeId:   "test-vol",
		TargetPath: t.TempDir(),
		VolumeContext: map[string]string{
			"mode":        "exclusive-local",
			"consistency": "close_to_open",
		},
	})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("got status %v, want InvalidArgument", status.Code(err))
	}
}

func TestNodePublishVolume_InvalidVolumeModeFails(t *testing.T) {
	driver, err := newDriver("test-node", "localhost:50051", t.TempDir(), 1024*1024, 10*1024*1024, 30*time.Second, 10*time.Minute, true, 30*time.Second, "", true, "")
	if err != nil {
		t.Fatalf("newDriver: %v", err)
	}
	t.Cleanup(func() {
		if err := driver.Close(); err != nil {
			t.Errorf("driver.Close failed: %v", err)
		}
	})

	_, err = driver.NodePublishVolume(t.Context(), &csi.NodePublishVolumeRequest{
		VolumeId:      "test-vol",
		TargetPath:    t.TempDir(),
		VolumeContext: map[string]string{"mode": "exclusive_local"},
	})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("got status %v, want InvalidArgument", status.Code(err))
	}
}

type fakeHolderClient struct {
	mountDescriptor int
	peerDescriptor  int
	mountRequest    fuseholder.MountRequest
	mountCalled     bool
	unmountTarget   string
	unmountCalled   bool
	unmountError    error
	closeCalled     bool
}

func (f *fakeHolderClient) Mount(request fuseholder.MountRequest) (int, error) {
	f.mountRequest = request
	f.mountCalled = true
	packet := make([]byte, 56)
	binary.LittleEndian.PutUint32(packet[0:4], 56)
	binary.LittleEndian.PutUint32(packet[4:8], 26) // _OP_INIT
	binary.LittleEndian.PutUint64(packet[8:16], 1)
	binary.LittleEndian.PutUint64(packet[16:24], 1)
	binary.LittleEndian.PutUint32(packet[40:44], 7)
	binary.LittleEndian.PutUint32(packet[44:48], 36)
	binary.LittleEndian.PutUint32(packet[48:52], 128*1024)
	if err := unix.Send(f.peerDescriptor, packet, 0); err != nil {
		return -1, fmt.Errorf("failed to send fake init: %w", err)
	}
	return f.mountDescriptor, nil
}

func (f *fakeHolderClient) Get(target string) (int, fuseholder.Entry, error) {
	return -1, fuseholder.Entry{}, fuseholder.ErrNotFound
}

func (f *fakeHolderClient) List() ([]fuseholder.Entry, error) {
	return nil, nil
}

func (f *fakeHolderClient) Unmount(target string, detach bool) error {
	f.unmountTarget = target
	f.unmountCalled = true
	return f.unmountError
}

func (f *fakeHolderClient) Close() error {
	f.closeCalled = true
	return nil
}

func TestNodePublishAndUnpublishVolume_WithHolderClient(t *testing.T) {
	ctx := t.Context()
	server := controllertest.NewTestServer(nil)
	volumeID := "test-fuse-publish-vol"

	listener := bufconn.Listen(1024 * 1024)
	grpcServer := grpc.NewServer()
	pb.RegisterObjectFSControllerServer(grpcServer, server)
	go func() {
		if err := grpcServer.Serve(listener); err != nil && !errors.Is(err, net.ErrClosed) {
			t.Logf("grpc server error: %v", err)
		}
	}()
	defer grpcServer.Stop()

	driver, err := newDriver("test-node", "passthrough://bufnet", t.TempDir(), 1024*1024, 10*1024*1024, 30*time.Second, 10*time.Minute, true, 30*time.Second, "", true, "/run/objectfs/fuse-holder.sock")
	if err != nil {
		t.Fatalf("newDriver failed: %v", err)
	}
	defer func() {
		if closeErr := driver.Close(); closeErr != nil {
			t.Errorf("driver.Close failed: %v", closeErr)
		}
	}()

	connection, err := grpc.NewClient("passthrough://bufnet",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return listener.Dial()
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("grpc.NewClient failed: %v", err)
	}
	defer connection.Close()
	driver.client = pb.NewObjectFSControllerClient(connection)

	socketDescriptors, err := unix.Socketpair(unix.AF_UNIX, unix.SOCK_STREAM, 0)
	if err != nil {
		t.Fatalf("unix.Socketpair failed: %v", err)
	}
	defer func() {
		if closeErr := unix.Close(socketDescriptors[1]); closeErr != nil {
			t.Errorf("failed to close peer descriptor: %v", closeErr)
		}
	}()

	fakeHolder := &fakeHolderClient{
		mountDescriptor: socketDescriptors[0],
		peerDescriptor:  socketDescriptors[1],
	}
	driver.dialHolder = func(socketPath string) (fuseHolderClient, error) {
		return fakeHolder, nil
	}

	targetDirectory := t.TempDir()
	publishResponse, err := driver.NodePublishVolume(ctx, &csi.NodePublishVolumeRequest{
		VolumeId:   volumeID,
		TargetPath: targetDirectory,
		VolumeContext: map[string]string{
			"mode": "fuse",
		},
	})
	if err != nil {
		t.Fatalf("NodePublishVolume failed: %v", err)
	}
	if publishResponse == nil {
		t.Fatal("expected non-nil NodePublishVolumeResponse")
	}

	if !fakeHolder.mountCalled {
		t.Error("expected holder.Mount to be called")
	}
	if fakeHolder.mountRequest.Target != targetDirectory {
		t.Errorf("expected target %s, got %s", targetDirectory, fakeHolder.mountRequest.Target)
	}
	if fakeHolder.mountRequest.VolumeID != volumeID {
		t.Errorf("expected volumeID %s, got %s", volumeID, fakeHolder.mountRequest.VolumeID)
	}
	if fakeHolder.mountRequest.Mode != "fuse" {
		t.Errorf("expected mode fuse, got %s", fakeHolder.mountRequest.Mode)
	}

	unpublishResponse, err := driver.NodeUnpublishVolume(ctx, &csi.NodeUnpublishVolumeRequest{
		VolumeId:   volumeID,
		TargetPath: targetDirectory,
	})
	if err != nil {
		t.Fatalf("NodeUnpublishVolume failed: %v", err)
	}
	if unpublishResponse == nil {
		t.Fatal("expected non-nil NodeUnpublishVolumeResponse")
	}

	if !fakeHolder.unmountCalled {
		t.Error("expected holder.Unmount to be called")
	}
	if fakeHolder.unmountTarget != targetDirectory {
		t.Errorf("expected unmount target %s, got %s", targetDirectory, fakeHolder.unmountTarget)
	}
}

func TestNodePublishVolume_SurvivesHolderRestart(t *testing.T) {
	ctx := t.Context()
	server := controllertest.NewTestServer(nil)

	listener := bufconn.Listen(1024 * 1024)
	grpcServer := grpc.NewServer()
	pb.RegisterObjectFSControllerServer(grpcServer, server)
	go func() {
		if err := grpcServer.Serve(listener); err != nil && !errors.Is(err, net.ErrClosed) {
			t.Logf("grpc server error: %v", err)
		}
	}()
	defer grpcServer.Stop()

	driver, err := newDriver("test-node", "passthrough://bufnet", t.TempDir(), 1024*1024, 10*1024*1024, 30*time.Second, 10*time.Minute, true, 30*time.Second, "", true, "/run/objectfs/fuse-holder.sock")
	if err != nil {
		t.Fatalf("newDriver failed: %v", err)
	}
	defer func() {
		if closeErr := driver.Close(); closeErr != nil {
			t.Errorf("driver.Close failed: %v", closeErr)
		}
	}()

	connection, err := grpc.NewClient("passthrough://bufnet",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return listener.Dial()
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("grpc.NewClient failed: %v", err)
	}
	defer connection.Close()
	driver.client = pb.NewObjectFSControllerClient(connection)

	callCount := 0
	driver.dialHolder = func(socketPath string) (fuseHolderClient, error) {
		callCount++
		socketDescriptors, err := unix.Socketpair(unix.AF_UNIX, unix.SOCK_STREAM, 0)
		if err != nil {
			return nil, fmt.Errorf("unix.Socketpair failed: %w", err)
		}
		t.Cleanup(func() {
			if closeErr := unix.Close(socketDescriptors[1]); closeErr != nil && !errors.Is(closeErr, unix.EBADF) {
				t.Errorf("failed to close peer descriptor: %v", closeErr)
			}
		})
		return &fakeHolderClient{
			mountDescriptor: socketDescriptors[0],
			peerDescriptor:  socketDescriptors[1],
		}, nil
	}

	target1 := t.TempDir()
	_, err = driver.NodePublishVolume(ctx, &csi.NodePublishVolumeRequest{
		VolumeId:   "vol-1",
		TargetPath: target1,
		VolumeContext: map[string]string{
			"mode": "fuse",
		},
	})
	if err != nil {
		t.Fatalf("first NodePublishVolume failed: %v", err)
	}

	target2 := t.TempDir()
	_, err = driver.NodePublishVolume(ctx, &csi.NodePublishVolumeRequest{
		VolumeId:   "vol-2",
		TargetPath: target2,
		VolumeContext: map[string]string{
			"mode": "fuse",
		},
	})
	if err != nil {
		t.Fatalf("second NodePublishVolume after holder restart failed: %v", err)
	}

	if callCount != 2 {
		t.Errorf("expected 2 holder dial calls, got %d", callCount)
	}

	_, err = driver.NodeUnpublishVolume(ctx, &csi.NodeUnpublishVolumeRequest{
		VolumeId:   "vol-1",
		TargetPath: target1,
	})
	if err != nil {
		t.Fatalf("unpublish vol-1 failed: %v", err)
	}
	_, err = driver.NodeUnpublishVolume(ctx, &csi.NodeUnpublishVolumeRequest{
		VolumeId:   "vol-2",
		TargetPath: target2,
	})
	if err != nil {
		t.Fatalf("unpublish vol-2 failed: %v", err)
	}
}

func TestNodeUnpublishVolume_HolderFailurePreservesMount(t *testing.T) {
	ctx := t.Context()
	server := controllertest.NewTestServer(nil)

	listener := bufconn.Listen(1024 * 1024)
	grpcServer := grpc.NewServer()
	pb.RegisterObjectFSControllerServer(grpcServer, server)
	go func() {
		if err := grpcServer.Serve(listener); err != nil && !errors.Is(err, net.ErrClosed) {
			t.Logf("grpc server error: %v", err)
		}
	}()
	defer grpcServer.Stop()

	driver, err := newDriver("test-node", "passthrough://bufnet", t.TempDir(), 1024*1024, 10*1024*1024, 30*time.Second, 10*time.Minute, true, 30*time.Second, "", true, "/run/objectfs/fuse-holder.sock")
	if err != nil {
		t.Fatalf("newDriver failed: %v", err)
	}
	defer func() {
		if closeErr := driver.Close(); closeErr != nil {
			t.Errorf("driver.Close failed: %v", closeErr)
		}
	}()

	connection, err := grpc.NewClient("passthrough://bufnet",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return listener.Dial()
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("grpc.NewClient failed: %v", err)
	}
	defer connection.Close()
	driver.client = pb.NewObjectFSControllerClient(connection)

	targetDirectory := t.TempDir()
	driver.mounts[targetDirectory] = &activeMount{
		volumeID: "test-vol",
		mode:     volumemode.String(pb.VolumeMode_FUSE),
	}

	expectedError := errors.New("injected unmount failure")
	driver.dialHolder = func(socketPath string) (fuseHolderClient, error) {
		return &fakeHolderClient{
			unmountError: expectedError,
		}, nil
	}

	_, err = driver.NodeUnpublishVolume(ctx, &csi.NodeUnpublishVolumeRequest{
		VolumeId:   "test-vol",
		TargetPath: targetDirectory,
	})
	if err == nil {
		t.Fatal("expected error from NodeUnpublishVolume when holder unmount fails, got nil")
	}

	driver.mu.Lock()
	mount, exists := driver.mounts[targetDirectory]
	driver.mu.Unlock()

	if !exists || mount == nil {
		t.Fatalf("expected mount %s to remain in mounts map on unmount failure, but it was removed", targetDirectory)
	}
}

func TestNodePublishVolume_RequiresFuseHolderSocket(t *testing.T) {
	driver, err := newDriver("test-node", "localhost:50051", t.TempDir(), 1024*1024, 10*1024*1024, 30*time.Second, 10*time.Minute, true, 30*time.Second, "", true, "")
	if err != nil {
		t.Fatalf("newDriver failed: %v", err)
	}
	t.Cleanup(func() {
		if closeErr := driver.Close(); closeErr != nil {
			t.Errorf("driver.Close failed: %v", closeErr)
		}
	})

	_, err = driver.NodePublishVolume(t.Context(), &csi.NodePublishVolumeRequest{
		VolumeId:   "test-vol",
		TargetPath: t.TempDir(),
		VolumeContext: map[string]string{
			"mode": "fuse",
		},
	})
	if err == nil {
		t.Fatal("expected error without fuse-holder-socket in FUSE mode, got nil")
	}
}
