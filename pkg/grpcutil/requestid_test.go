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
	"bytes"
	"context"
	"errors"
	"net"
	"sync"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/test/bufconn"
)

type requestIDHealthServer struct {
	healthpb.UnimplementedHealthServer
	check func(context.Context) error
}

func (s *requestIDHealthServer) Check(ctx context.Context, _ *healthpb.HealthCheckRequest) (*healthpb.HealthCheckResponse, error) {
	if err := s.check(ctx); err != nil {
		return nil, err
	}
	return &healthpb.HealthCheckResponse{Status: healthpb.HealthCheckResponse_SERVING}, nil
}

func TestRequestIDInterceptors(t *testing.T) {
	var mu sync.Mutex
	var receivedKeys [][]byte
	resendOnce := true

	server := grpc.NewServer(grpc.UnaryInterceptor(RequestIDUnaryServerInterceptor()))
	healthpb.RegisterHealthServer(server, &requestIDHealthServer{
		check: func(ctx context.Context) error {
			key := RequestID(ctx)
			mu.Lock()
			receivedKeys = append(receivedKeys, key)
			shouldResend := resendOnce
			resendOnce = false
			mu.Unlock()
			if shouldResend {
				return ResendStatus("try again", minimumResendDelay)
			}
			return nil
		},
	})

	listener := bufconn.Listen(1024 * 1024)
	go func() {
		if err := server.Serve(listener); err != nil && !errors.Is(err, net.ErrClosed) {
			t.Errorf("grpc server exited with error: %v", err)
		}
	}()
	t.Cleanup(server.Stop)

	dial := func(dialOptions ...grpc.DialOption) healthpb.HealthClient {
		options := append([]grpc.DialOption{
			grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
				return listener.Dial()
			}),
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		}, dialOptions...)
		connection, err := grpc.NewClient("passthrough:///bufnet", options...)
		if err != nil {
			t.Fatalf("grpc.NewClient failed: %v", err)
		}
		t.Cleanup(func() {
			if closeErr := connection.Close(); closeErr != nil {
				t.Errorf("connection.Close failed: %v", closeErr)
			}
		})
		return healthpb.NewHealthClient(connection)
	}

	interceptedClient := dial(grpc.WithChainUnaryInterceptor(
		RequestIDUnaryClientInterceptor(),
		ResendUnaryClientInterceptor(),
	))

	if _, err := interceptedClient.Check(t.Context(), &healthpb.HealthCheckRequest{}); err != nil {
		t.Fatalf("first call failed: %v", err)
	}
	if _, err := interceptedClient.Check(t.Context(), &healthpb.HealthCheckRequest{}); err != nil {
		t.Fatalf("second call failed: %v", err)
	}

	plainClient := dial()
	if _, err := plainClient.Check(t.Context(), &healthpb.HealthCheckRequest{}); err != nil {
		t.Fatalf("plain client call failed: %v", err)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(receivedKeys) != 4 {
		t.Fatalf("expected 4 handler invocations, got %d", len(receivedKeys))
	}
	if len(receivedKeys[0]) != 16 {
		t.Fatalf("expected 16-byte request ID, got %d bytes", len(receivedKeys[0]))
	}
	if !bytes.Equal(receivedKeys[0], receivedKeys[1]) {
		t.Fatalf("expected same request ID across resend: first=%x, retry=%x", receivedKeys[0], receivedKeys[1])
	}
	if len(receivedKeys[2]) != 16 {
		t.Fatalf("expected 16-byte request ID on second call, got %d bytes", len(receivedKeys[2]))
	}
	if bytes.Equal(receivedKeys[0], receivedKeys[2]) {
		t.Fatalf("expected distinct request IDs across separate calls, both were %x", receivedKeys[0])
	}
	if !bytes.Equal(receivedKeys[0][:8], receivedKeys[2][:8]) {
		t.Fatalf("expected shared 8-byte process prefix: first=%x, second=%x", receivedKeys[0][:8], receivedKeys[2][:8])
	}
	if receivedKeys[3] != nil {
		t.Fatalf("expected nil RequestID for client without interceptor, got %x", receivedKeys[3])
	}
	if got := RequestID(t.Context()); got != nil {
		t.Fatalf("expected nil RequestID for bare context, got %x", got)
	}
}
