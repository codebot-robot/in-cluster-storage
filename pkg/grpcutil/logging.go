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
	"strings"

	"github.com/gke-labs/in-cluster-storage/pkg/scoped"
	"google.golang.org/grpc"
)

type volumeIDGetter interface {
	GetVolumeId() string
}

// parseFullMethod splits a gRPC FullMethod (e.g. "/pkg.Service/Method")
// into rpc.service ("pkg.Service") and rpc.method ("Method") per
// OpenTelemetry RPC semantic conventions.
func parseFullMethod(fullMethod string) (string, string) {
	trimmed := strings.TrimPrefix(fullMethod, "/")
	service, method, found := strings.Cut(trimmed, "/")
	if !found {
		return "", trimmed
	}
	return service, method
}

// UnaryServerInterceptor returns a grpc.UnaryServerInterceptor that derives a logger with
// rpc.service and rpc.method (per OpenTelemetry RPC semantic conventions) and volume
// (if present on the request) and attaches it to the request context.
func UnaryServerInterceptor() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, request any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		service, method := parseFullMethod(info.FullMethod)
		logger := scoped.Log(ctx).WithValues("rpc.service", service, "rpc.method", method)
		if getter, ok := request.(volumeIDGetter); ok {
			if volumeID := getter.GetVolumeId(); volumeID != "" {
				logger = logger.WithValues("volume", volumeID)
			}
		}
		return handler(scoped.WithLog(ctx, logger), request)
	}
}

type streamServerStream struct {
	grpc.ServerStream
	ctx context.Context
}

func (s *streamServerStream) Context() context.Context {
	return s.ctx
}

// StreamServerInterceptor returns a grpc.StreamServerInterceptor that derives a logger with
// rpc.service and rpc.method (per OpenTelemetry RPC semantic conventions) and attaches it
// to the stream context.
func StreamServerInterceptor() grpc.StreamServerInterceptor {
	return func(server any, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		ctx := stream.Context()
		service, method := parseFullMethod(info.FullMethod)
		logger := scoped.Log(ctx).WithValues("rpc.service", service, "rpc.method", method)
		wrapped := &streamServerStream{
			ServerStream: stream,
			ctx:          scoped.WithLog(ctx, logger),
		}
		return handler(server, wrapped)
	}
}
