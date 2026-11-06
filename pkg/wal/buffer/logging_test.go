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

package buffer

import (
	"context"
	"io"
	"sync"
	"testing"
	"time"

	pb "github.com/gke-labs/in-cluster-storage/pkg/api/wal/v1alpha1"
	"github.com/gke-labs/in-cluster-storage/pkg/objectstore/inmemorystorage"
	"github.com/gke-labs/in-cluster-storage/pkg/scoped"
	"github.com/go-logr/logr"
	"github.com/google/uuid"
	"google.golang.org/grpc/metadata"
)

type sharedLogState struct {
	mu      sync.Mutex
	entries []map[string]any
}

type loggingTestSink struct {
	state  *sharedLogState
	values map[string]any
}

func newLoggingTestSink() *loggingTestSink {
	return &loggingTestSink{
		state:  &sharedLogState{},
		values: make(map[string]any),
	}
}

func (s *loggingTestSink) Init(info logr.RuntimeInfo) {}

func (s *loggingTestSink) Enabled(level int) bool {
	return true
}

func (s *loggingTestSink) Info(level int, message string, keysAndValues ...any) {
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

func (s *loggingTestSink) Error(err error, message string, keysAndValues ...any) {}

func (s *loggingTestSink) WithName(name string) logr.LogSink {
	return s
}

func (s *loggingTestSink) WithValues(keysAndValues ...any) logr.LogSink {
	newValues := make(map[string]any, len(s.values)+len(keysAndValues)/2)
	for key, value := range s.values {
		newValues[key] = value
	}
	for index := 0; index < len(keysAndValues)-1; index += 2 {
		if key, ok := keysAndValues[index].(string); ok {
			newValues[key] = keysAndValues[index+1]
		}
	}
	return &loggingTestSink{state: s.state, values: newValues}
}

func (s *loggingTestSink) SawStream(expectedStreamID string) bool {
	s.state.mu.Lock()
	defer s.state.mu.Unlock()
	for _, entry := range s.state.entries {
		if value, ok := entry["stream"]; ok && value == expectedStreamID {
			return true
		}
	}
	return false
}

type mockAppendServer struct {
	mu          sync.Mutex
	ctx         context.Context
	streamID    uuid.UUID
	sentHello   bool
	ackReceived chan struct{}
}

func (m *mockAppendServer) SetHeader(metadata.MD) error  { return nil }
func (m *mockAppendServer) SendHeader(metadata.MD) error { return nil }
func (m *mockAppendServer) SetTrailer(metadata.MD)       {}

func (m *mockAppendServer) Context() context.Context {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.ctx
}

func (m *mockAppendServer) SendMsg(message any) error { return nil }
func (m *mockAppendServer) RecvMsg(message any) error { return nil }

func (m *mockAppendServer) Send(response *pb.AppendResponse) error {
	if response.GetHelloAck() != nil {
		select {
		case m.ackReceived <- struct{}{}:
		default:
		}
	}
	return nil
}

func (m *mockAppendServer) Recv() (*pb.AppendRequest, error) {
	m.mu.Lock()
	if !m.sentHello {
		m.sentHello = true
		m.mu.Unlock()
		return &pb.AppendRequest{
			Msg: &pb.AppendRequest_Hello{
				Hello: &pb.Hello{
					StreamId: m.streamID[:],
				},
			},
		}, nil
	}
	m.mu.Unlock()
	<-m.Context().Done()
	return nil, io.EOF
}

func TestAppend_LoggerHasStream(t *testing.T) {
	sink := newLoggingTestSink()
	rootLogger := logr.New(sink)
	baseContext, cancel := context.WithCancel(scoped.WithLog(t.Context(), rootLogger))
	defer cancel()

	backend := inmemorystorage.New()
	dataDirectory := t.TempDir()
	server, err := NewServer(baseContext, ServerConfig{
		Backend:       backend,
		DataDir:       dataDirectory,
		FlushInterval: 10 * time.Second,
	})
	if err != nil {
		t.Fatalf("failed to create server: %v", err)
	}
	defer func() {
		if closeErr := server.Close(); closeErr != nil {
			t.Errorf("failed to close server: %v", closeErr)
		}
	}()

	streamID := uuid.New()
	mockStream := &mockAppendServer{
		ctx:         baseContext,
		streamID:    streamID,
		ackReceived: make(chan struct{}, 1),
	}

	appendErrorChan := make(chan error, 1)
	go func() {
		appendErrorChan <- server.Append(mockStream)
	}()

	select {
	case <-mockStream.ackReceived:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for HelloAck")
	}

	if !sink.SawStream(streamID.String()) {
		t.Fatalf("expected sink to see log with stream=%q", streamID.String())
	}

	cancel()
	select {
	case <-appendErrorChan:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for Append to exit")
	}
}
