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

package grpcutil

import (
	"context"
	"sync"
	"testing"

	"github.com/gke-labs/in-cluster-storage/pkg/scoped"
	"github.com/go-logr/logr"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

type interceptorTestSink struct {
	mu     sync.Mutex
	values map[string]any
}

func newInterceptorTestSink() *interceptorTestSink {
	return &interceptorTestSink{values: make(map[string]any)}
}

func (s *interceptorTestSink) Init(info logr.RuntimeInfo) {}

func (s *interceptorTestSink) Enabled(level int) bool {
	return true
}

func (s *interceptorTestSink) Info(level int, message string, keysAndValues ...any) {}

func (s *interceptorTestSink) Error(err error, message string, keysAndValues ...any) {}

func (s *interceptorTestSink) WithName(name string) logr.LogSink {
	return s
}

func (s *interceptorTestSink) WithValues(keysAndValues ...any) logr.LogSink {
	s.mu.Lock()
	defer s.mu.Unlock()
	newValues := make(map[string]any, len(s.values)+len(keysAndValues)/2)
	for key, value := range s.values {
		newValues[key] = value
	}
	for index := 0; index < len(keysAndValues)-1; index += 2 {
		if key, ok := keysAndValues[index].(string); ok {
			newValues[key] = keysAndValues[index+1]
		}
	}
	return &interceptorTestSink{values: newValues}
}

func (s *interceptorTestSink) Value(key string) any {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.values[key]
}

type fakeVolumeRequest struct {
	volumeID string
}

func (f *fakeVolumeRequest) GetVolumeId() string {
	return f.volumeID
}

func TestUnaryServerInterceptor(t *testing.T) {
	sink := newInterceptorTestSink()
	rootLogger := logr.New(sink)
	ctx := scoped.WithLog(t.Context(), rootLogger)

	interceptor := UnaryServerInterceptor()
	info := &grpc.UnaryServerInfo{FullMethod: "/objectfs.v1alpha1.ObjectFSController/GetAttr"}
	request := &fakeVolumeRequest{volumeID: "vol-123"}

	called := false
	handler := func(handlerCtx context.Context, req any) (any, error) {
		called = true
		logger := scoped.Log(handlerCtx)
		extractedSink, ok := logger.GetSink().(*interceptorTestSink)
		if !ok {
			t.Fatalf("expected *interceptorTestSink, got %T", logger.GetSink())
		}
		if got := extractedSink.Value("rpc.service"); got != "objectfs.v1alpha1.ObjectFSController" {
			t.Fatalf("expected rpc.service=%q, got %v", "objectfs.v1alpha1.ObjectFSController", got)
		}
		if got := extractedSink.Value("rpc.method"); got != "GetAttr" {
			t.Fatalf("expected rpc.method=%q, got %v", "GetAttr", got)
		}
		if got := extractedSink.Value("volume"); got != "vol-123" {
			t.Fatalf("expected volume=%q, got %v", "vol-123", got)
		}
		return "ok", nil
	}

	result, err := interceptor(ctx, request, info, handler)
	if err != nil {
		t.Fatalf("interceptor returned error: %v", err)
	}
	if !called {
		t.Fatal("handler was not called")
	}
	if result != "ok" {
		t.Fatalf("unexpected result: %v", result)
	}
}

func TestUnaryServerInterceptor_NoVolume(t *testing.T) {
	sink := newInterceptorTestSink()
	rootLogger := logr.New(sink)
	ctx := scoped.WithLog(t.Context(), rootLogger)

	interceptor := UnaryServerInterceptor()
	info := &grpc.UnaryServerInfo{FullMethod: "/objectfs.v1alpha1.ObjectFSController/ListVolumes"}
	request := struct{}{}

	called := false
	handler := func(handlerCtx context.Context, req any) (any, error) {
		called = true
		logger := scoped.Log(handlerCtx)
		extractedSink, ok := logger.GetSink().(*interceptorTestSink)
		if !ok {
			t.Fatalf("expected *interceptorTestSink, got %T", logger.GetSink())
		}
		if got := extractedSink.Value("rpc.service"); got != "objectfs.v1alpha1.ObjectFSController" {
			t.Fatalf("expected rpc.service=%q, got %v", "objectfs.v1alpha1.ObjectFSController", got)
		}
		if got := extractedSink.Value("rpc.method"); got != "ListVolumes" {
			t.Fatalf("expected rpc.method=%q, got %v", "ListVolumes", got)
		}
		if got := extractedSink.Value("volume"); got != nil {
			t.Fatalf("expected nil volume, got %v", got)
		}
		return nil, nil
	}

	_, err := interceptor(ctx, request, info, handler)
	if err != nil {
		t.Fatalf("interceptor returned error: %v", err)
	}
	if !called {
		t.Fatal("handler was not called")
	}
}

type fakeServerStream struct {
	ctx context.Context
}

func (f *fakeServerStream) SetHeader(metadata.MD) error  { return nil }
func (f *fakeServerStream) SendHeader(metadata.MD) error { return nil }
func (f *fakeServerStream) SetTrailer(metadata.MD)       {}
func (f *fakeServerStream) Context() context.Context     { return f.ctx }
func (f *fakeServerStream) SendMsg(message any) error    { return nil }
func (f *fakeServerStream) RecvMsg(message any) error    { return nil }

func TestStreamServerInterceptor(t *testing.T) {
	sink := newInterceptorTestSink()
	rootLogger := logr.New(sink)
	ctx := scoped.WithLog(t.Context(), rootLogger)

	interceptor := StreamServerInterceptor()
	info := &grpc.StreamServerInfo{FullMethod: "/objectfs.v1alpha1.ObjectFSController/WatchVolume"}
	mockStream := &fakeServerStream{ctx: ctx}

	called := false
	handler := func(server any, stream grpc.ServerStream) error {
		called = true
		logger := scoped.Log(stream.Context())
		extractedSink, ok := logger.GetSink().(*interceptorTestSink)
		if !ok {
			t.Fatalf("expected *interceptorTestSink, got %T", logger.GetSink())
		}
		if got := extractedSink.Value("rpc.service"); got != "objectfs.v1alpha1.ObjectFSController" {
			t.Fatalf("expected rpc.service=%q, got %v", "objectfs.v1alpha1.ObjectFSController", got)
		}
		if got := extractedSink.Value("rpc.method"); got != "WatchVolume" {
			t.Fatalf("expected rpc.method=%q, got %v", "WatchVolume", got)
		}
		return nil
	}

	err := interceptor(nil, mockStream, info, handler)
	if err != nil {
		t.Fatalf("interceptor returned error: %v", err)
	}
	if !called {
		t.Fatal("handler was not called")
	}
}

func TestParseFullMethod(t *testing.T) {
	tests := []struct {
		fullMethod      string
		expectedService string
		expectedMethod  string
	}{
		{
			fullMethod:      "/pkg.Service/Method",
			expectedService: "pkg.Service",
			expectedMethod:  "Method",
		},
		{
			fullMethod:      "Method",
			expectedService: "",
			expectedMethod:  "Method",
		},
		{
			fullMethod:      "",
			expectedService: "",
			expectedMethod:  "",
		},
	}

	for _, testCase := range tests {
		t.Run(testCase.fullMethod, func(t *testing.T) {
			service, method := parseFullMethod(testCase.fullMethod)
			if service != testCase.expectedService {
				t.Errorf("parseFullMethod(%q) service = %q, want %q", testCase.fullMethod, service, testCase.expectedService)
			}
			if method != testCase.expectedMethod {
				t.Errorf("parseFullMethod(%q) method = %q, want %q", testCase.fullMethod, method, testCase.expectedMethod)
			}
		})
	}
}
