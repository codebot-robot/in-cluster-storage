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

package buffer

import (
	"context"
	"errors"
	"testing"
	"time"

	pb "github.com/gke-labs/in-cluster-storage/pkg/api/wal/v1alpha1"
	"github.com/gke-labs/in-cluster-storage/pkg/objectstore/inmemorystorage"
	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func newTestServer(t *testing.T) *Server {
	t.Helper()
	server, err := NewServer(t.Context(), ServerConfig{
		Backend:       inmemorystorage.New(),
		DataDir:       t.TempDir(),
		FlushInterval: 10 * time.Second,
		BatchMaxDelay: time.Millisecond,
	})
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	t.Cleanup(func() {
		if err := server.Close(); err != nil {
			t.Errorf("server.Close failed: %v", err)
		}
	})
	return server
}

func pollWaiters(t *testing.T, state *streamState, target int) {
	t.Helper()
	for deadline := time.Now().Add(5 * time.Second); time.Now().Before(deadline); time.Sleep(time.Millisecond) {
		state.mu.Lock()
		waiters := state.waiters
		state.mu.Unlock()
		if waiters == target {
			return
		}
	}
	t.Fatalf("waiters != %d", target)
}

func TestGrantDecision(t *testing.T) {
	for _, testCase := range []struct {
		name                  string
		holder, requester     string
		sessions, inFlight    int
		wantGrant, wantResume bool
	}{
		{"empty requester", "client-1", "", 1, 0, true, false},
		{"empty holder", "", "client-1", 0, 0, true, false},
		{"same client reacquires with session", "client-1", "client-1", 1, 0, true, true},
		{"same client reacquires after detach", "client-1", "client-1", 0, 1, true, true},
		{"another client waits while attached", "client-1", "client-2", 1, 0, false, false},
		{"another client waits on in flight", "client-1", "client-2", 0, 1, false, false},
		{"another client granted when free", "client-1", "client-2", 0, 0, true, false},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			gotGrant, gotResume := grantDecision(testCase.holder, testCase.sessions, testCase.inFlight, testCase.requester)
			if gotGrant != testCase.wantGrant || gotResume != testCase.wantResume {
				t.Fatalf("grantDecision(%q, %d, %d, %q) = (%v, %v), want (%v, %v)",
					testCase.holder, testCase.sessions, testCase.inFlight, testCase.requester, gotGrant, gotResume, testCase.wantGrant, testCase.wantResume)
			}
		})
	}
}

func TestWriterLock_GrantAndResume(t *testing.T) {
	server := newTestServer(t)
	ctx := t.Context()
	streamID := uuid.New()

	state, _, _, resumed, err := server.attachSession(ctx, streamID, 1, "client-1")
	if err != nil || resumed {
		t.Fatalf("attach client-1: err=%v, resumed=%v", err, resumed)
	}
	if _, _, _, resumed, err = server.attachSession(ctx, streamID, 1, "client-1"); err != nil || !resumed {
		t.Fatalf("second attach client-1: err=%v, resumed=%v", err, resumed)
	}
	var client2Resumed bool
	var client2Err error
	done := make(chan struct{})
	go func() {
		_, _, _, client2Resumed, client2Err = server.attachSession(ctx, streamID, 1, "client-2")
		close(done)
	}()
	pollWaiters(t, state, 1)
	server.detachSession(state, "client-1")
	pollWaiters(t, state, 1)
	server.detachSession(state, "client-1")
	<-done
	if client2Err != nil || client2Resumed {
		t.Fatalf("attach client-2: err=%v, resumed=%v", client2Err, client2Resumed)
	}
	server.detachSession(state, "client-2")
	if _, _, _, resumed, err = server.attachSession(ctx, streamID, 1, "client-2"); err != nil || !resumed {
		t.Fatalf("reacquire client-2: err=%v, resumed=%v", err, resumed)
	}
	server.detachSession(state, "client-2")
	if _, _, _, resumed, err = server.attachSession(ctx, streamID, 1, "client-1"); err != nil || resumed {
		t.Fatalf("acquire client-1: err=%v, resumed=%v", err, resumed)
	}
	server.detachSession(state, "client-1")
}

func TestWriterLock_InFlightWaitsUntilCommitted(t *testing.T) {
	server := newTestServer(t)
	ctx := t.Context()
	streamID := uuid.New()

	state, _, _, _, err := server.attachSession(ctx, streamID, 1, "client-1")
	if err != nil {
		t.Fatalf("attach: %v", err)
	}
	state.mu.Lock()
	state.inFlight = 1
	state.mu.Unlock()
	server.detachSession(state, "client-1")

	var witness uint64
	done := make(chan error, 1)
	go func() {
		var resumed bool
		_, witness, _, resumed, err = server.attachSession(ctx, streamID, 1, "client-2")
		if resumed {
			err = errors.New("expected resumed=false")
		}
		done <- err
	}()
	pollWaiters(t, state, 1)
	server.commitItems(ctx, []incomingItem{{streamID: streamID, streamSeq: 42, epoch: 1, payload: []byte("rec")}})
	if err := <-done; err != nil || witness != 42 {
		t.Fatalf("attach client-2: err=%v, witness=%d", err, witness)
	}
	server.detachSession(state, "client-2")
}

func TestWriterLock_CancelAndCloseWhileWaiting(t *testing.T) {
	server := newTestServer(t)
	ctx := t.Context()
	streamID := uuid.New()

	state, _, _, _, err := server.attachSession(ctx, streamID, 1, "client-1")
	if err != nil {
		t.Fatalf("attach: %v", err)
	}
	waitCtx, cancel := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() {
		_, _, _, _, err := server.attachSession(waitCtx, streamID, 1, "client-2")
		done <- err
	}()
	pollWaiters(t, state, 1)
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
	pollWaiters(t, state, 0)

	go func() {
		_, _, _, _, err := server.attachSession(ctx, streamID, 1, "client-2")
		done <- err
	}()
	pollWaiters(t, state, 1)
	if err := server.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := <-done; err == nil {
		t.Fatal("expected error on Close, got nil")
	}
	pollWaiters(t, state, 0)
}

func TestWriterLock_EmptyClientID(t *testing.T) {
	server := newTestServer(t)
	ctx := t.Context()
	streamID := uuid.New()

	state1, _, _, resumed1, err := server.attachSession(ctx, streamID, 1, "")
	if err != nil || resumed1 {
		t.Fatalf("attach 1: err=%v, resumed=%v", err, resumed1)
	}
	defer server.detachSession(state1, "")
	state2, _, _, resumed2, err := server.attachSession(ctx, streamID, 1, "")
	if err != nil || resumed2 {
		t.Fatalf("attach 2: err=%v, resumed=%v", err, resumed2)
	}
	defer server.detachSession(state2, "")
	state1.mu.Lock()
	defer state1.mu.Unlock()
	if state1.holder != "" || state1.holderSessions != 0 || state1.attachments != 2 {
		t.Fatalf("expected empty holder and attachments=2, got holder=%q, sessions=%d, attachments=%d",
			state1.holder, state1.holderSessions, state1.attachments)
	}
}

func TestWriterLock_FencedLeavesNoHolderOrSessionCount(t *testing.T) {
	server := newTestServer(t)
	ctx := t.Context()
	streamID := uuid.New()

	state, _, _, _, err := server.attachSession(ctx, streamID, 5, "client-1")
	if err != nil {
		t.Fatalf("initial attach: %v", err)
	}
	server.detachSession(state, "client-1")

	// Attempt attach with a lower epoch. The lock is evaluated and granted first,
	// but the epoch check fails and returns fencedStatus.
	_, _, _, _, err = server.attachSession(ctx, streamID, 2, "client-fenced")
	if err == nil {
		t.Fatal("expected fenced error, got nil")
	}

	state.mu.Lock()
	defer state.mu.Unlock()
	if state.holder != "client-1" {
		t.Fatalf("expected holder to remain client-1, got %q", state.holder)
	}
	if state.holderSessions != 0 {
		t.Fatalf("expected holderSessions 0, got %d", state.holderSessions)
	}
	if state.attachments != 0 {
		t.Fatalf("expected attachments 0, got %d", state.attachments)
	}
	if state.waiters != 0 {
		t.Fatalf("expected waiters 0, got %d", state.waiters)
	}
}

func TestAppend_WriterLockOverWire(t *testing.T) {
	ctx := t.Context()
	server, address, cleanup := startTestServer(t, inmemorystorage.New(), t.TempDir())
	t.Cleanup(func() {
		cleanup()
	})

	streamID := uuid.New()
	clientID1 := []byte("client-wire-1")
	clientID2 := []byte("client-wire-2")

	openAppend := func(sessionCtx context.Context, clientID []byte) (*grpc.ClientConn, *pb.HelloAck) {
		connection, err := grpc.NewClient(address, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			t.Fatalf("grpc.NewClient: %v", err)
		}
		stream, err := pb.NewWalBufferClient(connection).Append(sessionCtx)
		if err != nil {
			_ = connection.Close()
			t.Fatalf("Append: %v", err)
		}
		if err := stream.Send(&pb.AppendRequest{
			Msg: &pb.AppendRequest_Hello{
				Hello: &pb.Hello{
					StreamId: streamID[:],
					Epoch:    1,
					ClientId: clientID,
				},
			},
		}); err != nil {
			_ = connection.Close()
			t.Fatalf("Send Hello: %v", err)
		}
		response, err := stream.Recv()
		if err != nil {
			_ = connection.Close()
			t.Fatalf("Recv HelloAck: %v", err)
		}
		helloAck := response.GetHelloAck()
		if helloAck == nil {
			_ = connection.Close()
			t.Fatalf("expected HelloAck, got %v", response)
		}
		return connection, helloAck
	}

	// 1. Client 1 attaches: HelloAck has resumed=false.
	session1Ctx, cancel1 := context.WithCancel(ctx)
	conn1, ack1 := openAppend(session1Ctx, clientID1)
	defer func() {
		if err := conn1.Close(); err != nil {
			t.Errorf("conn1.Close failed: %v", err)
		}
	}()
	if ack1.GetResumed() {
		t.Fatalf("expected resumed=false on first attach, got true")
	}

	// 2. Client 1 attaches second session with same client ID: HelloAck has resumed=true.
	session2Ctx, cancel2 := context.WithCancel(ctx)
	conn2, ack2 := openAppend(session2Ctx, clientID1)
	defer func() {
		if err := conn2.Close(); err != nil {
			t.Errorf("conn2.Close failed: %v", err)
		}
	}()
	if !ack2.GetResumed() {
		t.Fatalf("expected resumed=true on second attach with same client ID, got false")
	}

	// 3. Client 1 leaves (cancel both sessions).
	cancel1()
	cancel2()

	server.mu.RLock()
	state := server.streams[streamID.String()]
	server.mu.RUnlock()

	for deadline := time.Now().Add(5 * time.Second); time.Now().Before(deadline); time.Sleep(time.Millisecond) {
		state.mu.Lock()
		sessions := state.holderSessions
		state.mu.Unlock()
		if sessions == 0 {
			break
		}
	}
	state.mu.Lock()
	if state.holderSessions != 0 {
		state.mu.Unlock()
		t.Fatalf("timed out waiting for client 1 sessions to detach")
	}
	state.mu.Unlock()

	// 4. Different client attaches after client 1 leaves: HelloAck has resumed=false.
	session3Ctx, cancel3 := context.WithCancel(ctx)
	defer cancel3()
	conn3, ack3 := openAppend(session3Ctx, clientID2)
	defer func() {
		if err := conn3.Close(); err != nil {
			t.Errorf("conn3.Close failed: %v", err)
		}
	}()
	if ack3.GetResumed() {
		t.Fatalf("expected resumed=false for different client, got true")
	}
}
