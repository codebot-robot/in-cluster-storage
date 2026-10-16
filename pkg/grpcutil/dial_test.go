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
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func TestConnectParams(t *testing.T) {
	params := ConnectParams()

	if params.Backoff.BaseDelay != 200*time.Millisecond {
		t.Errorf("expected BaseDelay 200ms, got %v", params.Backoff.BaseDelay)
	}
	if params.Backoff.Multiplier != 1.6 {
		t.Errorf("expected Multiplier 1.6, got %v", params.Backoff.Multiplier)
	}
	if params.Backoff.Jitter != 0.2 {
		t.Errorf("expected Jitter 0.2, got %v", params.Backoff.Jitter)
	}
	if params.Backoff.MaxDelay != 2*time.Second {
		t.Errorf("expected MaxDelay 2s, got %v", params.Backoff.MaxDelay)
	}
	if params.MinConnectTimeout != 5*time.Second {
		t.Errorf("expected MinConnectTimeout 5s, got %v", params.MinConnectTimeout)
	}
}

func TestDialOptions(t *testing.T) {
	opts := DialOptions()
	if len(opts) != 1 {
		t.Fatalf("expected 1 dial option, got %d", len(opts))
	}

	extra := grpc.WithTransportCredentials(insecure.NewCredentials())
	optsWithExtra := DialOptions(extra)
	if len(optsWithExtra) != 2 {
		t.Fatalf("expected 2 dial options, got %d", len(optsWithExtra))
	}

	// Verify that grpc.NewClient succeeds with DialOptions.
	conn, err := grpc.NewClient("passthrough:///test", optsWithExtra...)
	if err != nil {
		t.Fatalf("grpc.NewClient failed with DialOptions: %v", err)
	}
	if err := conn.Close(); err != nil {
		t.Fatalf("failed to close conn: %v", err)
	}
}
