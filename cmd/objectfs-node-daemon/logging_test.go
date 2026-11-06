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
	"errors"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/container-storage-interface/spec/lib/go/csi"
	pb "github.com/gke-labs/in-cluster-storage/pkg/api/objectfs/v1alpha1"
	"github.com/gke-labs/in-cluster-storage/pkg/grpcutil"
	"github.com/gke-labs/in-cluster-storage/pkg/objectfs/controller/controllertest"
	"github.com/gke-labs/in-cluster-storage/pkg/scoped"
	"github.com/go-logr/logr"
	"golang.org/x/sys/unix"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
	"k8s.io/klog/v2"
)

type sharedNodeLogState struct {
	mu      sync.Mutex
	entries []map[string]any
}

type nodeLoggingTestSink struct {
	state  *sharedNodeLogState
	values map[string]any
}

func newNodeLoggingTestSink() *nodeLoggingTestSink {
	return &nodeLoggingTestSink{
		state:  &sharedNodeLogState{},
		values: make(map[string]any),
	}
}

func (s *nodeLoggingTestSink) Init(info logr.RuntimeInfo) {}

func (s *nodeLoggingTestSink) Enabled(level int) bool {
	return true
}

func (s *nodeLoggingTestSink) Info(level int, message string, keysAndValues ...any) {
	entry := make(map[string]any, len(s.values)+len(keysAndValues)/2+1)
	for key, value := range s.values {
		entry[key] = value
	}
	for index := 0; index < len(keysAndValues)-1; index += 2 {
		if key, ok := keysAndValues[index].(string); ok {
			entry[key] = keysAndValues[index+1]
		}
	}
	entry["message"] = message

	s.state.mu.Lock()
	defer s.state.mu.Unlock()
	s.state.entries = append(s.state.entries, entry)
}

func (s *nodeLoggingTestSink) Error(err error, message string, keysAndValues ...any) {}

func (s *nodeLoggingTestSink) WithName(name string) logr.LogSink {
	return s
}

func (s *nodeLoggingTestSink) WithValues(keysAndValues ...any) logr.LogSink {
	newValues := make(map[string]any, len(s.values)+len(keysAndValues)/2)
	for key, value := range s.values {
		newValues[key] = value
	}
	for index := 0; index < len(keysAndValues)-1; index += 2 {
		if key, ok := keysAndValues[index].(string); ok {
			newValues[key] = keysAndValues[index+1]
		}
	}
	return &nodeLoggingTestSink{state: s.state, values: newValues}
}

func (s *nodeLoggingTestSink) Value(key string) any {
	s.state.mu.Lock()
	defer s.state.mu.Unlock()
	return s.values[key]
}

func (s *nodeLoggingTestSink) SawMount(expectedVolumeID, expectedTarget string) bool {
	s.state.mu.Lock()
	defer s.state.mu.Unlock()
	for _, entry := range s.state.entries {
		if entry["message"] == "started volume watcher" && entry["volume"] == expectedVolumeID && entry["target"] == expectedTarget {
			return true
		}
	}
	return false
}

func TestNodeDaemon_FuseMountContext_LoggerHasVolumeAndTarget(t *testing.T) {
	ctx := t.Context()
	sink := newNodeLoggingTestSink()
	rootLogger := logr.New(sink)
	klog.SetLogger(rootLogger)
	defer klog.ClearLogger()

	server := controllertest.NewTestServer(nil)
	volumeID := "test-fuse-logging-vol"

	listener := bufconn.Listen(1024 * 1024)
	grpcServer := grpc.NewServer()
	pb.RegisterObjectFSControllerServer(grpcServer, server)
	go func() {
		if err := grpcServer.Serve(listener); err != nil && !errors.Is(err, net.ErrClosed) {
			t.Logf("grpc serve error: %v", err)
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

	// Wait for watcher to establish subscription and emit log with volume and target
	deadline := time.Now().Add(5 * time.Second)
	sawMount := false
	for time.Now().Before(deadline) {
		if sink.SawMount(volumeID, targetDirectory) {
			sawMount = true
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if !sawMount {
		t.Fatalf("expected watcher log to carry volume=%q and target=%q", volumeID, targetDirectory)
	}

	if _, unpublishErr := driver.NodeUnpublishVolume(ctx, &csi.NodeUnpublishVolumeRequest{
		VolumeId:   volumeID,
		TargetPath: targetDirectory,
	}); unpublishErr != nil {
		t.Fatalf("NodeUnpublishVolume failed: %v", unpublishErr)
	}
}

func TestNodeDaemon_CSINodeServiceInterceptor(t *testing.T) {
	sink := newNodeLoggingTestSink()
	rootLogger := logr.New(sink)
	ctx := scoped.WithLog(t.Context(), rootLogger)

	interceptor := grpcutil.UnaryServerInterceptor()
	request := &csi.NodePublishVolumeRequest{
		VolumeId:   "vol-csi-456",
		TargetPath: "/test/target",
	}
	info := &grpc.UnaryServerInfo{
		FullMethod: "/csi.v1.Node/NodePublishVolume",
	}

	called := false
	handler := func(handlerCtx context.Context, req any) (any, error) {
		called = true
		logger := scoped.Log(handlerCtx)
		extractedSink, ok := logger.GetSink().(*nodeLoggingTestSink)
		if !ok {
			t.Fatalf("expected *nodeLoggingTestSink, got %T", logger.GetSink())
		}
		if got := extractedSink.Value("rpc.service"); got != "csi.v1.Node" {
			t.Fatalf("expected rpc.service=%q, got %v", "csi.v1.Node", got)
		}
		if got := extractedSink.Value("rpc.method"); got != "NodePublishVolume" {
			t.Fatalf("expected rpc.method=%q, got %v", "NodePublishVolume", got)
		}
		if got := extractedSink.Value("volume"); got != "vol-csi-456" {
			t.Fatalf("expected volume=%q, got %v", "vol-csi-456", got)
		}
		return &csi.NodePublishVolumeResponse{}, nil
	}

	_, err := interceptor(ctx, request, info, handler)
	if err != nil {
		t.Fatalf("interceptor returned error: %v", err)
	}
	if !called {
		t.Fatal("handler was not called")
	}
}
