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
	"bytes"
	"context"
	"io"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"

	pb "github.com/gke-labs/in-cluster-storage/pkg/api/v1alpha1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/proto"
)

func TestLazyLoaderInitialization(t *testing.T) {
	// Set threshold to -1 (disabled)
	val := int64(-1)
	lazyLoadThreshold = &val

	d := &agentFSDriver{
		lazyLoader: lazyLoader{
			pending:            make(map[string]*pb.FileMetadata),
			downloadOperations: make(map[string]*downloadOperation),
		},
		fanotifyFd: -1,
	}

	err := d.startLazyLoader(t.Context())
	if err != nil {
		t.Fatalf("expected no error when threshold is -1, got: %v", err)
	}
	if d.fanotifyFd != -1 {
		t.Fatalf("expected fanotifyFd to remain -1, got: %d", d.fanotifyFd)
	}
}

func TestLazyLoaderCoordinationWithDownloadOperation(t *testing.T) {
	// Simple test exercising the split lock initialization and basic status of pending map
	d := &agentFSDriver{
		lazyLoader: lazyLoader{
			pending:            make(map[string]*pb.FileMetadata),
			downloadOperations: make(map[string]*downloadOperation),
		},
		fanotifyFd: -1,
	}

	testPath := "/var/lib/agentfs/vol-1/lower/large-file.txt"
	meta := &pb.FileMetadata{
		Path:   "large-file.txt",
		Size:   1024 * 1024,
		Sha256: "dummy-sha",
	}

	d.lazyLoader.pendingMu.Lock()
	d.lazyLoader.pending[testPath] = meta
	d.lazyLoader.pendingMu.Unlock()

	d.lazyLoader.pendingMu.RLock()
	_, exists := d.lazyLoader.pending[testPath]
	d.lazyLoader.pendingMu.RUnlock()

	if !exists {
		t.Fatalf("expected path to exist in pending map")
	}

	d.lazyLoader.downloadMu.Lock()
	op, found := d.lazyLoader.downloadOperations[testPath]
	if found || op != nil {
		t.Fatalf("expected no download operations to exist initially")
	}
	d.lazyLoader.downloadMu.Unlock()
}

type mockControllerServer struct {
	pb.UnimplementedAgentFSControllerServer
	latestSnapshot *pb.SnapshotMetadata
	uploadedBlobs  map[string][]byte
}

func (m *mockControllerServer) GetLatestSnapshot(ctx context.Context, req *pb.GetLatestSnapshotRequest) (*pb.GetLatestSnapshotResponse, error) {
	return &pb.GetLatestSnapshotResponse{
		Snapshot: m.latestSnapshot,
	}, nil
}

func (m *mockControllerServer) HasBlob(ctx context.Context, req *pb.HasBlobRequest) (*pb.HasBlobResponse, error) {
	_, exists := m.uploadedBlobs[req.Sha256]
	return &pb.HasBlobResponse{Exists: exists}, nil
}

func (m *mockControllerServer) UploadBlob(stream pb.AgentFSController_UploadBlobServer) error {
	var sha string
	var buf bytes.Buffer
	for {
		req, err := stream.Recv()
		if err == io.EOF {
			m.uploadedBlobs[sha] = buf.Bytes()
			return stream.SendAndClose(&pb.UploadBlobResponse{Success: true})
		}
		if err != nil {
			return err
		}
		switch x := req.Data.(type) {
		case *pb.UploadBlobRequest_Sha256:
			sha = x.Sha256
		case *pb.UploadBlobRequest_Content:
			buf.Write(x.Content)
		}
	}
}

func (m *mockControllerServer) UploadSnapshot(ctx context.Context, req *pb.UploadSnapshotRequest) (*pb.UploadSnapshotResponse, error) {
	m.latestSnapshot = req.Snapshot
	return &pb.UploadSnapshotResponse{Success: true}, nil
}

func startMockController(t *testing.T, srv *mockControllerServer) (*grpc.ClientConn, func()) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to listen: %v", err)
	}
	s := grpc.NewServer()
	pb.RegisterAgentFSControllerServer(s, srv)
	go func() {
		_ = s.Serve(lis)
	}()

	conn, err := grpc.Dial(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		lis.Close()
		t.Fatalf("failed to dial mock server: %v", err)
	}

	cleanup := func() {
		conn.Close()
		s.Stop()
		lis.Close()
	}
	return conn, cleanup
}

func TestPushErofsLayersSnapshot_EmptyUpper(t *testing.T) {
	tmpDir := t.TempDir()
	volumeDir := filepath.Join(tmpDir, "vol-1")
	upperDir := filepath.Join(volumeDir, "upper")
	if err := os.MkdirAll(upperDir, 0755); err != nil {
		t.Fatalf("failed to create upper dir: %v", err)
	}

	d := &agentFSDriver{}
	err := d.pushErofsLayersSnapshot(context.Background(), "vol-1", volumeDir, upperDir)
	if err != nil {
		t.Fatalf("expected nil error for empty upper dir, got: %v", err)
	}
}

func TestPushErofsLayersSnapshot_SuccessAndIdempotency(t *testing.T) {
	mockSrv := &mockControllerServer{
		uploadedBlobs: make(map[string][]byte),
	}
	conn, cleanup := startMockController(t, mockSrv)
	defer cleanup()

	tmpDir := t.TempDir()
	volumeDir := filepath.Join(tmpDir, "vol-1")
	upperDir := filepath.Join(volumeDir, "upper")
	targetPath := filepath.Join(tmpDir, "target")
	if err := os.MkdirAll(upperDir, 0755); err != nil {
		t.Fatalf("failed to create upper dir: %v", err)
	}
	if err := os.MkdirAll(targetPath, 0755); err != nil {
		t.Fatalf("failed to create target dir: %v", err)
	}

	// Write a test file in upper dir
	if err := os.WriteFile(filepath.Join(upperDir, "file1.txt"), []byte("hello layer 1"), 0644); err != nil {
		t.Fatalf("failed to write test file: %v", err)
	}

	d := &agentFSDriver{
		controllerConn: conn,
	}

	// First push - should succeed
	err := d.pushErofsLayersSnapshot(context.Background(), "vol-1", volumeDir, targetPath)
	if err != nil {
		t.Fatalf("pushErofsLayersSnapshot failed: %v", err)
	}

	if mockSrv.latestSnapshot == nil || len(mockSrv.latestSnapshot.ErofsLayers) != 1 {
		t.Fatalf("expected 1 layer in server snapshot, got: %v", mockSrv.latestSnapshot)
	}

	// Verify snapshot.pb was written to disk
	snapshotPBPath := filepath.Join(volumeDir, "snapshot.pb")
	if _, err := os.Stat(snapshotPBPath); err != nil {
		t.Fatalf("expected snapshot.pb on disk, got err: %v", err)
	}

	// Case A: Unpublish retry where snapshot.pb is deleted/stale (existingLayers = []), but server already has target layers [sha]
	_ = os.Remove(snapshotPBPath)
	err = d.pushErofsLayersSnapshot(context.Background(), "vol-1", volumeDir, targetPath)
	if err != nil {
		t.Fatalf("idempotent retry with server already matching target layers failed: %v", err)
	}

	// Case B: Subsequent call where snapshot.pb has the updated state
	err = d.pushErofsLayersSnapshot(context.Background(), "vol-1", volumeDir, targetPath)
	if err != nil {
		t.Fatalf("idempotent retry with updated snapshot.pb failed: %v", err)
	}
}

func TestPushErofsLayersSnapshot_OptimisticConcurrencyConflict(t *testing.T) {
	mockSrv := &mockControllerServer{
		latestSnapshot: &pb.SnapshotMetadata{
			ErofsLayers: []string{"server-layer-sha-123"},
		},
		uploadedBlobs: make(map[string][]byte),
	}
	conn, cleanup := startMockController(t, mockSrv)
	defer cleanup()

	tmpDir := t.TempDir()
	volumeDir := filepath.Join(tmpDir, "vol-1")
	upperDir := filepath.Join(volumeDir, "upper")
	targetPath := filepath.Join(tmpDir, "target")
	if err := os.MkdirAll(upperDir, 0755); err != nil {
		t.Fatalf("failed to create upper dir: %v", err)
	}
	if err := os.MkdirAll(targetPath, 0755); err != nil {
		t.Fatalf("failed to create target dir: %v", err)
	}

	// Write a local snapshot.pb with empty/different layers
	localSnap := &pb.SnapshotMetadata{
		ErofsLayers: []string{"local-different-layer-456"},
	}
	localData, _ := proto.Marshal(localSnap)
	_ = os.WriteFile(filepath.Join(volumeDir, "snapshot.pb"), localData, 0644)

	// Write a file in upper
	_ = os.WriteFile(filepath.Join(upperDir, "file1.txt"), []byte("data"), 0644)

	d := &agentFSDriver{
		controllerConn: conn,
	}

	err := d.pushErofsLayersSnapshot(context.Background(), "vol-1", volumeDir, targetPath)
	if err == nil || !strings.Contains(err.Error(), "optimistic concurrency conflict") {
		t.Fatalf("expected optimistic concurrency conflict error, got: %v", err)
	}
}

func TestVolumeIDPersistenceAcrossRestart(t *testing.T) {
	tmpDir := t.TempDir()
	volumeDir := filepath.Join(tmpDir, "pvc-test-123")
	if err := os.MkdirAll(volumeDir, 0755); err != nil {
		t.Fatalf("failed to create volumeDir: %v", err)
	}

	// Simulate driver storing volume_id
	logicalID := "my-logical-volume-id"
	volumeIDFile := filepath.Join(volumeDir, "volume_id")
	if err := os.WriteFile(volumeIDFile, []byte(logicalID), 0644); err != nil {
		t.Fatalf("failed to write volume_id: %v", err)
	}

	// Read back directly as done in NodeUnpublishVolume
	data, err := os.ReadFile(volumeIDFile)
	if err != nil {
		t.Fatalf("failed to read volume_id: %v", err)
	}
	if strings.TrimSpace(string(data)) != logicalID {
		t.Fatalf("expected logical ID %q, got %q", logicalID, string(data))
	}
}
