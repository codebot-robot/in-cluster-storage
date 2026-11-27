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
	"bytes"
	"context"
	"errors"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	pb "github.com/gke-labs/in-cluster-storage/pkg/api/objectfs/v1alpha1"
	"github.com/gke-labs/in-cluster-storage/pkg/objectstore"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

// watchGateBackend intercepts ListObjects for a target stream ID and stalls until released,
// enabling tests to pause getOrCreateVolume during WatchVolume initialization.
type watchGateBackend struct {
	ObjectStorageBackend
	targetStreamID string
	gate           chan struct{}
	enteredGate    chan struct{}
	once           sync.Once
}

func (g *watchGateBackend) ListObjects(ctx context.Context, volumeID string, prefix string) ([]objectstore.ObjectEntry, error) {
	if g.targetStreamID != "" && strings.Contains(prefix, g.targetStreamID) {
		g.once.Do(func() {
			close(g.enteredGate)
		})
		if g.gate != nil {
			select {
			case <-g.gate:
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}
	}
	return g.ObjectStorageBackend.ListObjects(ctx, volumeID, prefix)
}

// newWatchTestClient starts an in-memory gRPC server running server over bufconn
// and returns a client connected to it, cleaned up when t finishes.
func newWatchTestClient(t *testing.T, server *Server) pb.ObjectFSControllerClient {
	t.Helper()

	listener := bufconn.Listen(1024 * 1024)
	grpcServer := grpc.NewServer()
	pb.RegisterObjectFSControllerServer(grpcServer, server)
	go func() {
		if err := grpcServer.Serve(listener); err != nil && !errors.Is(err, grpc.ErrServerStopped) {
			t.Errorf("grpcServer.Serve error: %v", err)
		}
	}()
	t.Cleanup(func() {
		grpcServer.Stop()
		if err := listener.Close(); err != nil {
			t.Errorf("listener.Close error: %v", err)
		}
	})

	conn, err := grpc.NewClient("passthrough://bufnet",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return listener.Dial()
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("Failed to dial bufnet: %v", err)
	}
	t.Cleanup(func() {
		if err := conn.Close(); err != nil {
			t.Errorf("conn.Close error: %v", err)
		}
	})

	return pb.NewObjectFSControllerClient(conn)
}

// TestWatchVolume_BookmarkGuaranteesSubscription verifies that WatchVolume registers
// the subscription with the broadcaster before sending the initial bookmark event.
func TestWatchVolume_BookmarkGuaranteesSubscription(t *testing.T) {
	ctx := t.Context()
	server := NewTestServer(t, NewMemoryBackend())
	volumeID := "test-watch-bookmark-vol"

	var bookmarkSent bool
	var subscriberCountAtSend int
	stream := &mockWatchStream{
		ctx: ctx,
		sendFunc: func(resp *pb.WatchVolumeResponse) error {
			if resp.GetEventType() == pb.WatchEventType_EVENT_BOOKMARK {
				bookmarkSent = true
				server.broadcaster.mu.RLock()
				subscriberCountAtSend = len(server.broadcaster.subscribers[volumeID])
				server.broadcaster.mu.RUnlock()
				return errors.New("bookmark received")
			}
			return nil
		},
	}

	err := server.WatchVolume(&pb.WatchVolumeRequest{
		VolumeId: volumeID,
		NodeId:   "node-1",
	}, stream)
	if err == nil || !strings.Contains(err.Error(), "bookmark received") {
		t.Fatalf("expected bookmark received error, got: %v", err)
	}
	if !bookmarkSent {
		t.Fatalf("bookmark was not sent")
	}
	if subscriberCountAtSend == 0 {
		t.Fatalf("broadcaster had no subscribers when bookmark was sent")
	}
}

// TestWatchVolume_BookmarkWaitsForSubscribeGate verifies that the initial bookmark
// is not sent until volume initialization completes and the subscription is registered.
func TestWatchVolume_BookmarkWaitsForSubscribeGate(t *testing.T) {
	ctx := t.Context()
	volumeID := "test-watch-gate-vol"
	targetStreamID := StreamIDForVolume(volumeID).String()

	gate := make(chan struct{})
	enteredGate := make(chan struct{})
	backend := &watchGateBackend{
		ObjectStorageBackend: NewMemoryBackend(),
		targetStreamID:       targetStreamID,
		gate:                 gate,
		enteredGate:          enteredGate,
	}

	server := NewTestServer(t, backend)
	client := newWatchTestClient(t, server)

	watchCtx, cancelWatch := context.WithTimeout(ctx, 30*time.Second)
	defer cancelWatch()

	stream, err := client.WatchVolume(watchCtx, &pb.WatchVolumeRequest{
		VolumeId:        volumeID,
		NodeId:          "node-gate",
		ConsistencyMode: pb.ConsistencyMode_STANDARD,
	})
	if err != nil {
		t.Fatalf("WatchVolume failed: %v", err)
	}

	type receiveResult struct {
		event *pb.WatchVolumeResponse
		err   error
	}
	receiveChannel := make(chan receiveResult, 1)
	go func() {
		event, receiveErr := stream.Recv()
		receiveChannel <- receiveResult{event: event, err: receiveErr}
	}()

	select {
	case <-enteredGate:
	case <-time.After(30 * time.Second):
		t.Fatalf("getOrCreateVolume did not reach gate after 30s guard")
	}

	select {
	case result := <-receiveChannel:
		t.Fatalf("stream.Recv() returned before getOrCreateVolume completed: %+v", result)
	case <-time.After(100 * time.Millisecond):
	}

	earlyEvent := &pb.WatchVolumeResponse{
		EventType: pb.WatchEventType_EVENT_CREATED,
		Inode:     100,
		Name:      "early.txt",
	}
	server.Broadcaster().Broadcast(volumeID, earlyEvent)

	close(gate)

	var bookmarkEvent *pb.WatchVolumeResponse
	select {
	case result := <-receiveChannel:
		if result.err != nil {
			t.Fatalf("stream.Recv() returned error after gate release: %v", result.err)
		}
		bookmarkEvent = result.event
	case <-time.After(30 * time.Second):
		t.Fatalf("stream.Recv() timed out waiting for bookmark after gate release")
	}

	if bookmarkEvent.GetEventType() != pb.WatchEventType_EVENT_BOOKMARK {
		t.Fatalf("Expected first event to be EVENT_BOOKMARK, got: %v", bookmarkEvent.GetEventType())
	}

	subscribedEvent := &pb.WatchVolumeResponse{
		EventType: pb.WatchEventType_EVENT_CREATED,
		Inode:     101,
		Name:      "subscribed.txt",
	}
	server.Broadcaster().Broadcast(volumeID, subscribedEvent)

	receivedEvent, err := stream.Recv()
	if err != nil {
		if errors.Is(watchCtx.Err(), context.DeadlineExceeded) {
			t.Fatalf("stream.Recv() timed out after 30s guard: %v", err)
		}
		t.Fatalf("stream.Recv() failed: %v", err)
	}
	if receivedEvent.GetInode() != subscribedEvent.GetInode() || receivedEvent.GetName() != subscribedEvent.GetName() {
		t.Fatalf("Expected subscribed event (%d, %s), got: (%d, %s)", subscribedEvent.GetInode(), subscribedEvent.GetName(), receivedEvent.GetInode(), receivedEvent.GetName())
	}
}

// TestWatchVolume_ConsistencyModeMismatch_SurfacesStatusError verifies that an early
// rejection before subscription (such as consistency mode mismatch) surfaces as a status error on Recv.
func TestWatchVolume_ConsistencyModeMismatch_SurfacesStatusError(t *testing.T) {
	ctx := t.Context()
	server := NewTestServer(t, NewMemoryBackend())
	client := newWatchTestClient(t, server)
	volumeID := "test-watch-mismatch-vol"

	volume, err := server.getOrCreateVolume(volumeID)
	if err != nil {
		t.Fatalf("getOrCreateVolume failed: %v", err)
	}
	volume.SetConsistencyMode(pb.ConsistencyMode_CLOSE_TO_OPEN)

	watchCtx, cancelWatch := context.WithTimeout(ctx, 30*time.Second)
	defer cancelWatch()

	stream, err := client.WatchVolume(watchCtx, &pb.WatchVolumeRequest{
		VolumeId:        volumeID,
		NodeId:          "node-mismatch",
		ConsistencyMode: pb.ConsistencyMode_STANDARD,
	})
	if err != nil {
		t.Fatalf("WatchVolume failed: %v", err)
	}

	_, receiveErr := stream.Recv()
	if receiveErr == nil {
		t.Fatalf("stream.Recv() succeeded, expected InvalidArgument error")
	}
	if status.Code(receiveErr) != codes.InvalidArgument {
		t.Fatalf("stream.Recv() status code = %v, want %v; err: %v", status.Code(receiveErr), codes.InvalidArgument, receiveErr)
	}
}

// TestWatchVolume_GracefulStopReachesShutdown verifies that BeginShutdown terminates
// open WatchVolume streams with Unavailable so GracefulStop returns promptly and
// Server.Shutdown drains staged volume data, and refuses watches arriving after shutdown begins.
func TestWatchVolume_GracefulStopReachesShutdown(t *testing.T) {
	ctx := t.Context()
	backend := NewMemoryBackend()
	server := NewTestServer(t, backend)
	volumeID := "test-watch-shutdown-vol"

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("net.Listen failed: %v", err)
	}
	grpcServer := grpc.NewServer()
	pb.RegisterObjectFSControllerServer(grpcServer, server)
	serveErrors := make(chan error, 1)
	go func() {
		serveErrors <- grpcServer.Serve(listener)
	}()
	t.Cleanup(func() {
		grpcServer.Stop()
		if err := listener.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
			t.Errorf("listener.Close error: %v", err)
		}
	})

	connection, err := grpc.NewClient(listener.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("grpc.NewClient failed: %v", err)
	}
	t.Cleanup(func() {
		if err := connection.Close(); err != nil {
			t.Errorf("connection.Close error: %v", err)
		}
	})
	client := pb.NewObjectFSControllerClient(connection)

	createResponse, err := client.CreateFile(ctx, &pb.CreateFileRequest{
		VolumeId:    volumeID,
		ParentInode: 1,
		Name:        "staged.txt",
		Mode:        0644,
	})
	if err != nil || createResponse.GetError() != 0 {
		t.Fatalf("CreateFile failed: %v (code %d)", err, createResponse.GetError())
	}
	stagedData := []byte("staged data drained after watch stream closes")
	writeResponse, err := client.WriteFile(ctx, &pb.WriteFileRequest{
		VolumeId:  volumeID,
		Inode:     createResponse.GetAttr().GetInode().GetIno(),
		Offset:    0,
		Data:      stagedData,
		WriteMode: pb.WriteMode_FSYNC,
	})
	if err != nil || writeResponse.GetError() != 0 {
		t.Fatalf("WriteFile failed: %v (code %d)", err, writeResponse.GetError())
	}

	watchStream, err := client.WatchVolume(ctx, &pb.WatchVolumeRequest{
		VolumeId: volumeID,
		NodeId:   "node-1",
	})
	if err != nil {
		t.Fatalf("WatchVolume failed: %v", err)
	}
	bookmark, err := watchStream.Recv()
	if err != nil || bookmark.GetEventType() != pb.WatchEventType_EVENT_BOOKMARK {
		t.Fatalf("initial WatchVolume.Recv() = (%v, %v), want EVENT_BOOKMARK", bookmark, err)
	}

	watchEnded := make(chan error, 1)
	go func() {
		_, receiveError := watchStream.Recv()
		watchEnded <- receiveError
	}()

	// Begin shutdown the same way the SIGTERM handler in cmd/objectfs-controller does.
	server.BeginShutdown()
	stopped := make(chan struct{})
	go func() {
		grpcServer.GracefulStop()
		close(stopped)
	}()

	select {
	case <-stopped:
	case <-time.After(time.Second):
		t.Fatal("GracefulStop did not return within 1s while WatchVolume stream was open")
	}
	if err := <-serveErrors; err != nil {
		t.Fatalf("grpcServer.Serve error: %v", err)
	}
	if receiveError := <-watchEnded; status.Code(receiveError) != codes.Unavailable {
		t.Fatalf("WatchVolume ended with status %v, want Unavailable (err: %v)", status.Code(receiveError), receiveError)
	}

	// A WatchVolume arriving after shutdown began is refused the same way.
	if lateError := server.WatchVolume(&pb.WatchVolumeRequest{VolumeId: volumeID, NodeId: "node-late"}, &mockWatchStream{ctx: ctx}); status.Code(lateError) != codes.Unavailable {
		t.Fatalf("post-shutdown WatchVolume status = %v, want Unavailable (err: %v)", status.Code(lateError), lateError)
	}

	if err := server.Shutdown(ctx); err != nil {
		t.Fatalf("server.Shutdown failed: %v", err)
	}

	recovered := NewTestServer(t, backend)
	readResponse, err := testReadFile(ctx, recovered, volumeID, "/staged.txt", 0, int64(len(stagedData)))
	if err != nil || readResponse.GetError() != 0 {
		t.Fatalf("ReadFile from fresh server failed: %v (code %d)", err, readResponse.GetError())
	}
	if !bytes.Equal(readResponse.GetData(), stagedData) {
		t.Fatalf("ReadFile data = %q, want %q", readResponse.GetData(), stagedData)
	}
}
