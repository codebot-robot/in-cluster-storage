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
	"errors"
	"net"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	pb "github.com/gke-labs/in-cluster-storage/pkg/api/objectfs/v1alpha1"
	walpb "github.com/gke-labs/in-cluster-storage/pkg/api/wal/v1alpha1"
	"github.com/gke-labs/in-cluster-storage/pkg/grpcutil"
	"github.com/gke-labs/in-cluster-storage/pkg/wal/buffer/buffertest"
	walclient "github.com/gke-labs/in-cluster-storage/pkg/wal/client"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

type blockingAppendWalServer struct {
	walpb.WalBufferServer
	sessionEnded chan struct{}

	mu              sync.Mutex
	blockNextRecord bool
	recordBlocked   chan struct{}
}

func (s *blockingAppendWalServer) armNextRecordBlock() <-chan struct{} {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.blockNextRecord = true
	s.recordBlocked = make(chan struct{})
	return s.recordBlocked
}

func (s *blockingAppendWalServer) Append(stream walpb.WalBuffer_AppendServer) error {
	wrapped := &blockingAppendStream{
		WalBuffer_AppendServer: stream,
		server:                 s,
	}
	err := s.WalBufferServer.Append(wrapped)
	select {
	case s.sessionEnded <- struct{}{}:
	default:
	}
	return err
}

type blockingAppendStream struct {
	walpb.WalBuffer_AppendServer
	server *blockingAppendWalServer
}

func (s *blockingAppendStream) Recv() (*walpb.AppendRequest, error) {
	request, err := s.WalBuffer_AppendServer.Recv()
	if err != nil {
		return request, err
	}
	if request.GetRecord() != nil {
		s.server.mu.Lock()
		shouldBlock := s.server.blockNextRecord
		blocked := s.server.recordBlocked
		if shouldBlock {
			s.server.blockNextRecord = false
		}
		s.server.mu.Unlock()
		if shouldBlock {
			close(blocked)
			<-s.Context().Done()
			return nil, s.Context().Err()
		}
	}
	return request, nil
}

func serveControllerForTest(t *testing.T, server *Server) *bufconn.Listener {
	t.Helper()
	listener := bufconn.Listen(4 * 1024 * 1024)
	grpcServer := grpc.NewServer(
		grpc.ChainUnaryInterceptor(grpcutil.UnaryServerInterceptor(), server.ResendUnaryServerInterceptor()),
	)
	pb.RegisterObjectFSControllerServer(grpcServer, server)
	serveDone := make(chan struct{})
	go func() {
		defer close(serveDone)
		if err := grpcServer.Serve(listener); err != nil && !errors.Is(err, grpc.ErrServerStopped) {
			t.Errorf("controller grpcServer.Serve failed: %v", err)
		}
	}()
	t.Cleanup(func() {
		grpcServer.Stop()
		<-serveDone
		if err := listener.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
			t.Errorf("controller listener.Close failed: %v", err)
		}
	})
	return listener
}

func dialControllerForTest(t *testing.T, listener *bufconn.Listener, withResendInterceptor bool) pb.ObjectFSControllerClient {
	t.Helper()
	dialOptions := []grpc.DialOption{
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return listener.Dial()
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	}
	if withResendInterceptor {
		dialOptions = append(dialOptions, grpc.WithChainUnaryInterceptor(grpcutil.ResendUnaryClientInterceptor()))
	}
	connection, err := grpc.NewClient("passthrough:///bufnet", dialOptions...)
	if err != nil {
		t.Fatalf("grpc.NewClient failed: %v", err)
	}
	t.Cleanup(func() {
		if err := connection.Close(); err != nil {
			t.Errorf("controller client connection.Close failed: %v", err)
		}
	})
	return pb.NewObjectFSControllerClient(connection)
}

func runMidRequestWriterLockLoss(t *testing.T, withClientResend bool) (pb.ObjectFSControllerClient, *pb.MkdirResponse, error) {
	t.Helper()
	ctx := t.Context()
	backend := NewMemoryBackend()
	clock := buffertest.NewFakeClock(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
	bufferTest, _ := buffertest.Start(ctx, t,
		buffertest.WithBackend(backend),
		buffertest.WithWriterLockGrace(15*time.Second),
		buffertest.WithClock(clock),
	)

	walServerA := &blockingAppendWalServer{
		WalBufferServer: bufferTest.Server,
		sessionEnded:    make(chan struct{}, 8),
	}
	walListenerA := bufconn.Listen(4 * 1024 * 1024)
	walGRPCServerA := grpc.NewServer()
	walpb.RegisterWalBufferServer(walGRPCServerA, walServerA)
	walServeDone := make(chan struct{})
	go func() {
		defer close(walServeDone)
		if err := walGRPCServerA.Serve(walListenerA); err != nil && !errors.Is(err, grpc.ErrServerStopped) {
			t.Errorf("walGRPCServerA.Serve failed: %v", err)
		}
	}()
	t.Cleanup(func() {
		walGRPCServerA.Stop()
		<-walServeDone
		if err := walListenerA.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
			t.Errorf("walListenerA.Close failed: %v", err)
		}
	})

	var connectionMu sync.Mutex
	var connectionCut bool
	var connectionsA []net.Conn
	dialOptionsA := []grpc.DialOption{
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			connectionMu.Lock()
			defer connectionMu.Unlock()
			if connectionCut {
				return nil, errors.New("connection cut")
			}
			connection, err := walListenerA.Dial()
			if err == nil {
				connectionsA = append(connectionsA, connection)
			}
			return connection, err
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	}

	startServer := func(clientID string, dialOptions []grpc.DialOption) *Server {
		walDirectory := t.TempDir()
		server := NewServer(backend,
			WithServerWAL(walDirectory, bufferTest.Target, walclient.Witness),
			WithServerWALClient(bufferTest.Client),
			WithServerMetadataDir(t.TempDir()),
			WithServerWithoutFsync(),
			WithServerStreamFactory(func(volumeID string) (walclient.Stream, error) {
				return walclient.Open(ctx, walDirectory, StreamIDForVolume(volumeID), bufferTest.Target,
					walclient.WithDialOptions(dialOptions...),
					walclient.WithoutFsync(),
					walclient.WithClientID(clientID),
				)
			}),
		)
		if err := server.Start(ctx); err != nil {
			t.Fatalf("server.Start (%s) failed: %v", clientID, err)
		}
		t.Cleanup(func() {
			if err := server.Close(); err != nil {
				t.Errorf("server.Close (%s) failed: %v", clientID, err)
			}
		})
		return server
	}

	const volumeID = "test-writer-lock-resend-volume"
	serverA := startServer("controller-A", dialOptionsA)
	controllerListener := serveControllerForTest(t, serverA)
	client := dialControllerForTest(t, controllerListener, withClientResend)

	// Open the volume on A and acquire the writer lock with an initial file.
	initialResponse, err := client.CreateFile(ctx, &pb.CreateFileRequest{
		VolumeId:       volumeID,
		ParentInode:    1,
		Name:           "from-a-initial.txt",
		Mode:           0644,
		InitialContent: []byte("initial"),
	})
	if err != nil || initialResponse.GetError() != 0 {
		t.Fatalf("initial CreateFile failed: err=%v errno=%v", err, syscall.Errno(initialResponse.GetError()))
	}

	// Synchronously guarantee the writer lock is lost while Mkdir is blocked:
	// Volume.Mkdir (OPERATION metadata durability, Witness WAL durability)
	// appends its WAL record locally and blocks in stream.Wait until the WAL
	// buffer witnesses that record. blockingAppendWalServer intercepts that
	// record in Recv, closes recordBlocked, and holds it without forwarding to
	// the underlying WalBufferServer until A's connection is cut. Because the
	// record cannot be witnessed, Mkdir is guaranteed to be inside the handler
	// and blocked on A's stream while we cut A, advance past the grace period,
	// let B take over and write, and heal A.
	recordBlocked := walServerA.armNextRecordBlock()

	type mkdirResult struct {
		response *pb.MkdirResponse
		err      error
	}
	resultCh := make(chan mkdirResult, 1)
	go func() {
		response, err := client.Mkdir(ctx, &pb.MkdirRequest{
			VolumeId:    volumeID,
			ParentInode: 1,
			Name:        "blocked-dir",
			Mode:        0755,
		})
		resultCh <- mkdirResult{response: response, err: err}
	}()

	select {
	case <-recordBlocked:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for Mkdir WAL record to reach stream server")
	}

	// Cut A's connection and wait for the buffer to record session end before advancing the clock.
	connectionMu.Lock()
	connectionCut = true
	for _, connection := range connectionsA {
		if err := connection.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
			t.Errorf("connection.Close failed: %v", err)
		}
	}
	connectionsA = nil
	connectionMu.Unlock()

	select {
	case <-walServerA.sessionEnded:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for controller A's Append session to end")
	}

	// Advance past the 15s grace period, let B take over, write a file, and release the lock cleanly.
	clock.Advance(16 * time.Second)
	serverB := startServer("controller-B", bufferTest.DialOpts)
	responseB, err := testCreateFile(ctx, serverB, volumeID, "/from-b.txt", 0644, []byte("from B"), 0, 0)
	if err != nil || responseB.GetError() != 0 {
		t.Fatalf("CreateFile on B failed: err=%v errno=%v", err, syscall.Errno(responseB.GetError()))
	}
	shutdownCtx, cancelShutdown := context.WithTimeout(ctx, 5*time.Second)
	defer cancelShutdown()
	if err := serverB.Shutdown(shutdownCtx, 5*time.Second, time.Second); err != nil {
		t.Fatalf("serverB.Shutdown failed: %v", err)
	}

	// Heal A so its blocked stream reconnects, discovers B's write, and fails with ErrWriterLockLost.
	connectionMu.Lock()
	connectionCut = false
	connectionMu.Unlock()

	select {
	case result := <-resultCh:
		return client, result.response, result.err
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for blocked Mkdir to complete")
		return nil, nil, nil
	}
}

func TestServer_ResendAfterMidRequestWriterLockLoss(t *testing.T) {
	const volumeID = "test-writer-lock-resend-volume"

	t.Run("with_client_interceptor", func(t *testing.T) {
		client, response, err := runMidRequestWriterLockLoss(t, true)
		if err != nil || response.GetError() != 0 {
			t.Fatalf("Mkdir with resend client interceptor failed: err=%v errno=%v", err, syscall.Errno(response.GetError()))
		}

		readDirResponse, err := client.ReadDir(t.Context(), &pb.ReadDirRequest{
			VolumeId: volumeID,
			Inode:    1,
		})
		if err != nil || readDirResponse.GetError() != 0 {
			t.Fatalf("ReadDir failed: err=%v errno=%v", err, syscall.Errno(readDirResponse.GetError()))
		}
		dirCount := 0
		sawFileFromB := false
		for _, entry := range readDirResponse.GetEntries() {
			if entry.GetName() == "blocked-dir" {
				dirCount++
			}
			if entry.GetName() == "from-b.txt" {
				sawFileFromB = true
			}
		}
		if dirCount != 1 {
			t.Fatalf("expected blocked-dir to exist once, found %d times", dirCount)
		}
		if !sawFileFromB {
			t.Fatalf("expected from-b.txt from controller B after reload")
		}
	})

	t.Run("without_client_interceptor", func(t *testing.T) {
		client, _, err := runMidRequestWriterLockLoss(t, false)
		delay, resend := grpcutil.ResendDelay(err)
		if !resend {
			t.Fatalf("expected resend status without client interceptor, got err=%v", err)
		}
		if delay != writerLockLostResendDelay {
			t.Fatalf("ResendDelay = %v, want %v", delay, writerLockLostResendDelay)
		}

		// A subsequent request on the already-lock-lost volume reloads via the handler
		// and returns its real errno unchanged rather than another resend status.
		lookupResponse, err := client.Lookup(t.Context(), &pb.LookupRequest{
			VolumeId:    volumeID,
			ParentInode: 1,
			Name:        "missing-after-loss",
		})
		if err != nil {
			t.Fatalf("Lookup after lock loss returned gRPC error %v, want ENOENT in response", err)
		}
		if lookupResponse.GetError() != int32(syscall.ENOENT) {
			t.Fatalf("Lookup after lock loss errno = %v, want ENOENT", syscall.Errno(lookupResponse.GetError()))
		}
	})
}

type injectableDegradedStream struct {
	walclient.Stream
	degraded atomic.Pointer[walclient.DegradedError]
}

func (s *injectableDegradedStream) Degraded() *walclient.DegradedError {
	if err := s.degraded.Load(); err != nil {
		return err
	}
	return s.Stream.Degraded()
}

func (s *injectableDegradedStream) Append(ctx context.Context, payload []byte) (uint64, error) {
	if err := s.degraded.Load(); err != nil {
		return 0, err
	}
	return s.Stream.Append(ctx, payload)
}

func TestServer_ResendInterceptorOnlyForLostLock(t *testing.T) {
	ctx := t.Context()
	backend := NewMemoryBackend()
	bufferTest, _ := buffertest.Start(ctx, t, buffertest.WithBackend(backend))
	walDirectory := t.TempDir()

	var streamsMu sync.Mutex
	wrappedStreams := make(map[string]*injectableDegradedStream)
	server := NewServer(backend,
		WithServerWAL(walDirectory, bufferTest.Target, walclient.Witness),
		WithServerWALClient(bufferTest.Client),
		WithServerMetadataDir(t.TempDir()),
		WithServerWithoutFsync(),
		WithServerStreamFactory(func(volumeID string) (walclient.Stream, error) {
			realStream, err := walclient.Open(ctx, walDirectory, StreamIDForVolume(volumeID), bufferTest.Target,
				walclient.WithDialOptions(bufferTest.DialOpts...),
				walclient.WithoutFsync(),
				walclient.WithClientID("controller-single"),
			)
			if err != nil {
				return nil, err
			}
			wrapped := &injectableDegradedStream{Stream: realStream}
			streamsMu.Lock()
			wrappedStreams[volumeID] = wrapped
			streamsMu.Unlock()
			return wrapped, nil
		}),
	)
	if err := server.Start(ctx); err != nil {
		t.Fatalf("server.Start failed: %v", err)
	}
	t.Cleanup(func() {
		if err := server.Close(); err != nil {
			t.Errorf("server.Close failed: %v", err)
		}
	})

	listener := serveControllerForTest(t, server)
	client := dialControllerForTest(t, listener, false)

	const healthyVolume = "healthy-volume"
	firstMkdir, err := client.Mkdir(ctx, &pb.MkdirRequest{
		VolumeId:    healthyVolume,
		ParentInode: 1,
		Name:        "existing-dir",
		Mode:        0755,
	})
	if err != nil || firstMkdir.GetError() != 0 {
		t.Fatalf("first Mkdir failed: err=%v errno=%v", err, syscall.Errno(firstMkdir.GetError()))
	}

	// Real ENOENT must be returned unchanged.
	lookupResponse, err := client.Lookup(ctx, &pb.LookupRequest{
		VolumeId:    healthyVolume,
		ParentInode: 1,
		Name:        "nonexistent",
	})
	if err != nil {
		t.Fatalf("Lookup returned gRPC error %v, want ENOENT in response", err)
	}
	if lookupResponse.GetError() != int32(syscall.ENOENT) {
		t.Fatalf("Lookup errno = %v, want ENOENT", syscall.Errno(lookupResponse.GetError()))
	}

	// Real EEXIST (mkdir twice) must be returned unchanged.
	secondMkdir, err := client.Mkdir(ctx, &pb.MkdirRequest{
		VolumeId:    healthyVolume,
		ParentInode: 1,
		Name:        "existing-dir",
		Mode:        0755,
	})
	if err != nil {
		t.Fatalf("second Mkdir returned gRPC error %v, want EEXIST in response", err)
	}
	if secondMkdir.GetError() != int32(syscall.EEXIST) {
		t.Fatalf("second Mkdir errno = %v, want EEXIST", syscall.Errno(secondMkdir.GetError()))
	}

	// Mutation on a volume degraded for a non-lock-loss reason must return EIO unchanged.
	const degradedVolume = "degraded-other-reason"
	setupDegraded, err := client.Mkdir(ctx, &pb.MkdirRequest{
		VolumeId:    degradedVolume,
		ParentInode: 1,
		Name:        "before-degrade",
		Mode:        0755,
	})
	if err != nil || setupDegraded.GetError() != 0 {
		t.Fatalf("setup Mkdir on degraded volume failed: err=%v errno=%v", err, syscall.Errno(setupDegraded.GetError()))
	}
	streamsMu.Lock()
	degradedStream := wrappedStreams[degradedVolume]
	streamsMu.Unlock()
	degradedStream.degraded.Store(&walclient.DegradedError{
		Cause: errors.New("segment store I/O error"),
		At:    time.Now(),
	})

	degradedMkdir, err := client.Mkdir(ctx, &pb.MkdirRequest{
		VolumeId:    degradedVolume,
		ParentInode: 1,
		Name:        "after-degrade",
		Mode:        0755,
	})
	if err != nil {
		t.Fatalf("Mkdir on non-lock-loss degraded volume returned gRPC error %v, want EIO in response", err)
	}
	if degradedMkdir.GetError() != int32(syscall.EIO) {
		t.Fatalf("Mkdir on non-lock-loss degraded volume errno = %v, want EIO", syscall.Errno(degradedMkdir.GetError()))
	}

	// Mutation on a stopped volume must return plain Unavailable (not resend).
	const stoppedVolume = "stopped-volume"
	setupStopped, err := client.Mkdir(ctx, &pb.MkdirRequest{
		VolumeId:    stoppedVolume,
		ParentInode: 1,
		Name:        "before-stop",
		Mode:        0755,
	})
	if err != nil || setupStopped.GetError() != 0 {
		t.Fatalf("setup Mkdir on stopped volume failed: err=%v errno=%v", err, syscall.Errno(setupStopped.GetError()))
	}
	server.mu.RLock()
	stopped := server.volumes[stoppedVolume]
	server.mu.RUnlock()
	if err := stopped.Stop(ctx); err != nil {
		t.Fatalf("stopped.Stop failed: %v", err)
	}

	_, err = client.Mkdir(ctx, &pb.MkdirRequest{
		VolumeId:    stoppedVolume,
		ParentInode: 1,
		Name:        "after-stop",
		Mode:        0755,
	})
	if status.Code(err) != codes.Unavailable {
		t.Fatalf("Mkdir on stopped volume status = %v, want Unavailable", err)
	}
	if _, resend := grpcutil.ResendDelay(err); resend {
		t.Fatalf("Mkdir on stopped volume unexpectedly returned resend status: %v", err)
	}
}
