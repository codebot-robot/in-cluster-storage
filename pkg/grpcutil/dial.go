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
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/backoff"
)

// ConnectParams returns the standard connection parameters for controller and buffer clients.
// It bounds the reconnection backoff MaxDelay to 2 seconds (compared to gRPC's default 120 seconds)
// so that clients recover within a few seconds after a service outage.
func ConnectParams() grpc.ConnectParams {
	return grpc.ConnectParams{
		Backoff: backoff.Config{
			BaseDelay:  200 * time.Millisecond,
			Multiplier: 1.6,
			Jitter:     0.2,
			MaxDelay:   2 * time.Second,
		},
		MinConnectTimeout: 5 * time.Second,
	}
}

// DialOptions returns default gRPC dial options configuring the reconnect backoff parameters.
// Any extra options provided are appended.
func DialOptions(extra ...grpc.DialOption) []grpc.DialOption {
	opts := []grpc.DialOption{
		grpc.WithConnectParams(ConnectParams()),
	}
	return append(opts, extra...)
}
