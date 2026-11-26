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
	"crypto/rand"
	"encoding/binary"
	"fmt"
	"sync"
	"sync/atomic"

	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

// RequestIDMetadataKey is the gRPC binary metadata key carrying a call's
// idempotency key from client to server.
const RequestIDMetadataKey = "request-id-bin"

type requestIDContextKey struct{}

var (
	requestIDPrefixOnce sync.Once
	requestIDPrefix     [8]byte
	requestIDCounter    atomic.Uint64
)

func nextRequestID() []byte {
	requestIDPrefixOnce.Do(func() {
		if _, err := rand.Read(requestIDPrefix[:]); err != nil {
			panic(fmt.Sprintf("grpcutil: failed to read random bytes for request ID prefix: %v", err))
		}
	})
	var key [16]byte
	copy(key[:8], requestIDPrefix[:])
	binary.BigEndian.PutUint64(key[8:], requestIDCounter.Add(1))
	return key[:]
}

// RequestID returns the idempotency key attached to ctx by
// RequestIDUnaryServerInterceptor, or nil if the call carried none.
func RequestID(ctx context.Context) []byte {
	key, ok := ctx.Value(requestIDContextKey{}).([]byte)
	if !ok || len(key) == 0 {
		return nil
	}
	return bytes.Clone(key)
}

// RequestIDUnaryClientInterceptor returns a grpc.UnaryClientInterceptor that
// attaches a fresh idempotency key to each outgoing unary RPC as gRPC metadata.
// Chain it outside ResendUnaryClientInterceptor so re-sent attempts carry the
// same key.
func RequestIDUnaryClientInterceptor() grpc.UnaryClientInterceptor {
	return func(ctx context.Context, method string, request, reply any, connection *grpc.ClientConn, invoker grpc.UnaryInvoker, callOptions ...grpc.CallOption) error {
		ctx = metadata.AppendToOutgoingContext(ctx, RequestIDMetadataKey, string(nextRequestID()))
		return invoker(ctx, method, request, reply, connection, callOptions...)
	}
}

// RequestIDUnaryServerInterceptor returns a grpc.UnaryServerInterceptor that
// copies the idempotency key from incoming gRPC metadata into the handler
// context, where it can be read with RequestID.
func RequestIDUnaryServerInterceptor() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, request any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		if incomingMetadata, ok := metadata.FromIncomingContext(ctx); ok {
			if values := incomingMetadata.Get(RequestIDMetadataKey); len(values) > 0 && len(values[0]) > 0 {
				ctx = context.WithValue(ctx, requestIDContextKey{}, []byte(values[0]))
			}
		}
		return handler(ctx, request)
	}
}
