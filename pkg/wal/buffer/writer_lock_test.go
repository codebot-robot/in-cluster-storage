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
	"io"
	"strings"
	"sync"
	"testing"
	"time"

	pb "github.com/gke-labs/in-cluster-storage/pkg/api/wal/v1alpha1"
	"github.com/gke-labs/in-cluster-storage/pkg/objectstore/inmemorystorage"
	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

type fakeClock struct {
	mu  sync.Mutex
	now time.Time
}

func newFakeClock(start time.Time) *fakeClock {
	return &fakeClock{now: start}
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *fakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

func newTestServer(t *testing.T) *Server {
	t.Helper()
	return newTestServerWithGrace(t, 0, nil)
}

func newTestServerWithGrace(t *testing.T, grace time.Duration, clock Clock) *Server {
	t.Helper()
	server, err := NewServer(t.Context(), ServerConfig{
		Backend:         inmemorystorage.New(),
		DataDir:         t.TempDir(),
		FlushInterval:   10 * time.Second,
		BatchMaxDelay:   time.Millisecond,
		WriterLockGrace: grace,
		Clock:           clock,
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

func TestGrantDecision(t *testing.T) {
	t0 := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	grace := 15 * time.Second

	for _, testCase := range []struct {
		name                  string
		inputs                grantDecisionInputs
		requester             string
		now                   time.Time
		wantGrant, wantResume bool
		wantRefusal           grantRefusalReason
		wantNextWake          time.Time
	}{
		{"empty requester", grantDecisionInputs{holder: "client-1", attachedSessions: 1, lastSessionEnd: t0}, "", t0, true, false, grantRefusalNone, time.Time{}},
		{"empty holder", grantDecisionInputs{holder: "", lastSessionEnd: t0}, "client-1", t0, true, false, grantRefusalNone, time.Time{}},
		{"same client reacquires with session", grantDecisionInputs{holder: "client-1", attachedSessions: 1, lastSessionEnd: t0}, "client-1", t0, true, true, grantRefusalNone, time.Time{}},
		{"same client reacquires after detach", grantDecisionInputs{holder: "client-1", inFlight: 1, lastSessionEnd: t0}, "client-1", t0, true, true, grantRefusalNone, time.Time{}},
		{"another client refused while attached", grantDecisionInputs{holder: "client-1", attachedSessions: 1, lastSessionEnd: t0}, "client-2", t0, false, false, grantRefusalLockHeld, time.Time{}},
		{"another client refused on in flight", grantDecisionInputs{holder: "client-1", inFlight: 1, lastSessionEnd: t0}, "client-2", t0, false, false, grantRefusalRecordsInFlight, time.Time{}},
		{"another client granted when free", grantDecisionInputs{holder: "client-1", lastSessionEnd: t0}, "client-2", t0, true, false, grantRefusalNone, time.Time{}},
		{"another client refused inside grace period", grantDecisionInputs{holder: "client-1", lastSessionEnd: t0, gracePeriod: grace}, "client-2", t0.Add(5 * time.Second), false, false, grantRefusalGracePeriod, t0.Add(grace)},
		{"another client granted at grace expiry", grantDecisionInputs{holder: "client-1", lastSessionEnd: t0, gracePeriod: grace}, "client-2", t0.Add(grace), true, false, grantRefusalNone, time.Time{}},
		{"another client granted after grace period", grantDecisionInputs{holder: "client-1", lastSessionEnd: t0, gracePeriod: grace}, "client-2", t0.Add(20 * time.Second), true, false, grantRefusalNone, time.Time{}},
		{"same client granted inside grace period", grantDecisionInputs{holder: "client-1", lastSessionEnd: t0, gracePeriod: grace}, "client-1", t0.Add(5 * time.Second), true, true, grantRefusalNone, time.Time{}},
		{"same client granted after grace period", grantDecisionInputs{holder: "client-1", lastSessionEnd: t0, gracePeriod: grace}, "client-1", t0.Add(20 * time.Second), true, true, grantRefusalNone, time.Time{}},
		{"another client refused on in flight after grace period", grantDecisionInputs{holder: "client-1", inFlight: 1, lastSessionEnd: t0, gracePeriod: grace}, "client-2", t0.Add(20 * time.Second), false, false, grantRefusalRecordsInFlight, time.Time{}},
		{"another client refused on in flight inside grace period", grantDecisionInputs{holder: "client-1", inFlight: 1, lastSessionEnd: t0, gracePeriod: grace}, "client-2", t0.Add(5 * time.Second), false, false, grantRefusalRecordsInFlight, time.Time{}},
		{"zero lastSessionEnd with grace period is granted", grantDecisionInputs{holder: "client-1", gracePeriod: grace}, "client-2", t0, true, false, grantRefusalNone, time.Time{}},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			gotGrant, gotResume, gotRefusal, gotNextWake := grantDecision(testCase.inputs, testCase.requester, testCase.now)
			if gotGrant != testCase.wantGrant || gotResume != testCase.wantResume || gotRefusal != testCase.wantRefusal || !gotNextWake.Equal(testCase.wantNextWake) {
				t.Fatalf("grantDecision(%+v, %q, %v) = (%v, %v, %v, %v), want (%v, %v, %v, %v)",
					testCase.inputs, testCase.requester, testCase.now,
					gotGrant, gotResume, gotRefusal, gotNextWake,
					testCase.wantGrant, testCase.wantResume, testCase.wantRefusal, testCase.wantNextWake)
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

	// Another client while client-1 is attached is refused with Unavailable.
	if _, _, _, _, err = server.attachSession(ctx, streamID, 1, "client-2"); status.Code(err) != codes.Unavailable {
		t.Fatalf("expected Unavailable while attached, got %v", err)
	}

	server.detachSession(state, "client-1")

	// Another client while client-1 still has one attached session is refused with Unavailable.
	if _, _, _, _, err = server.attachSession(ctx, streamID, 1, "client-2"); status.Code(err) != codes.Unavailable {
		t.Fatalf("expected Unavailable while first session attached, got %v", err)
	}

	server.detachSession(state, "client-1")

	// Both sessions of client-1 detached: client-2 is granted.
	client2State, _, _, resumed, err := server.attachSession(ctx, streamID, 1, "client-2")
	if err != nil || resumed {
		t.Fatalf("attach client-2: err=%v, resumed=%v", err, resumed)
	}
	server.detachSession(client2State, "client-2")
	if _, _, _, resumed, err = server.attachSession(ctx, streamID, 1, "client-2"); err != nil || !resumed {
		t.Fatalf("reacquire client-2: err=%v, resumed=%v", err, resumed)
	}
	server.detachSession(client2State, "client-2")
	if _, _, _, resumed, err = server.attachSession(ctx, streamID, 1, "client-1"); err != nil || resumed {
		t.Fatalf("acquire client-1: err=%v, resumed=%v", err, resumed)
	}
	server.detachSession(state, "client-1")
}

func TestWriterLock_RefusedWhileHolderAttached(t *testing.T) {
	server := newTestServer(t)
	ctx := t.Context()
	streamID := uuid.New()

	state, _, _, _, err := server.attachSession(ctx, streamID, 1, "client-1")
	if err != nil {
		t.Fatalf("attach client-1: %v", err)
	}
	defer server.detachSession(state, "client-1")

	_, _, _, _, err = server.attachSession(ctx, streamID, 1, "client-2")
	if status.Code(err) != codes.Unavailable {
		t.Fatalf("expected codes.Unavailable, got %v", err)
	}
	if !strings.Contains(err.Error(), "stream writer lock is held by another client") {
		t.Fatalf("expected lock held message, got %v", err)
	}
	if strings.Contains(err.Error(), "client-1") || strings.Contains(err.Error(), "client-2") {
		t.Fatalf("message must not leak client IDs: %v", err)
	}
}

func TestWriterLock_InFlightRefusedUntilCommitted(t *testing.T) {
	server := newTestServer(t)
	ctx := t.Context()
	streamID := uuid.New()

	state, _, _, _, err := server.attachSession(ctx, streamID, 1, "client-1")
	if err != nil {
		t.Fatalf("attach client-1: %v", err)
	}
	state.mu.Lock()
	state.inFlight = 1
	state.mu.Unlock()
	server.detachSession(state, "client-1")

	// Another client while records are still in flight is refused with Unavailable.
	_, _, _, _, err = server.attachSession(ctx, streamID, 1, "client-2")
	if status.Code(err) != codes.Unavailable {
		t.Fatalf("expected codes.Unavailable, got %v", err)
	}
	if !strings.Contains(err.Error(), "stream writer lock is waiting for records of its previous holder to be committed") {
		t.Fatalf("expected records in flight message, got %v", err)
	}
	if strings.Contains(err.Error(), "client-1") || strings.Contains(err.Error(), "client-2") {
		t.Fatalf("message must not leak client IDs: %v", err)
	}

	// Commit the in-flight item.
	server.commitItems(ctx, []incomingItem{{streamID: streamID, streamSeq: 42, epoch: 1, payload: []byte("rec")}})

	// Client-2 connects again: granted, and watermark equals stream's watermark (42).
	client2State, witnessSeq, _, resumed, err := server.attachSession(ctx, streamID, 1, "client-2")
	if err != nil || resumed {
		t.Fatalf("attach client-2 after commit: err=%v, resumed=%v", err, resumed)
	}
	if witnessSeq != 42 {
		t.Fatalf("expected witness watermark 42, got %d", witnessSeq)
	}
	server.detachSession(client2State, "client-2")
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

func setupGraceTest(t *testing.T) (*Server, *fakeClock, *streamState, uuid.UUID) {
	t.Helper()
	clock := newFakeClock(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
	server := newTestServerWithGrace(t, 15*time.Second, clock)
	streamID := uuid.New()
	state, _, _, _, err := server.attachSession(t.Context(), streamID, 1, "client-1")
	if err != nil {
		t.Fatalf("attach client-1: %v", err)
	}
	server.detachSession(state, "client-1")
	return server, clock, state, streamID
}

func TestWriterLock_GracePeriodRefusesOtherClient(t *testing.T) {
	server, clock, state, streamID := setupGraceTest(t)
	ctx := t.Context()

	// Advance clock within grace period.
	clock.Advance(5 * time.Second)

	// Another client inside the grace period is refused with Unavailable and leaves waiters at zero.
	_, _, _, _, err := server.attachSession(ctx, streamID, 1, "client-2")
	if status.Code(err) != codes.Unavailable {
		t.Fatalf("expected codes.Unavailable, got %v", err)
	}
	if !strings.Contains(err.Error(), "stream writer lock is reserved for its previous holder") {
		t.Fatalf("expected reservation message, got %v", err)
	}
	if strings.Contains(err.Error(), "client-1") || strings.Contains(err.Error(), "client-2") {
		t.Fatalf("message must not leak client IDs: %v", err)
	}
	state.mu.Lock()
	waiters := state.waiters
	state.mu.Unlock()
	if waiters != 0 {
		t.Fatalf("expected waiters=0, got %d", waiters)
	}

	// The holder itself is still granted at once with resumed true.
	holderState, _, _, resumed, err := server.attachSession(ctx, streamID, 1, "client-1")
	if err != nil || !resumed {
		t.Fatalf("attach client-1 within grace: err=%v, resumed=%v", err, resumed)
	}
	server.detachSession(holderState, "client-1")

	// Advance clock past the grace period.
	clock.Advance(20 * time.Second)

	// After the clock passes the grace period the other client is granted with resumed false.
	client2State, _, _, resumed, err := server.attachSession(ctx, streamID, 1, "client-2")
	if err != nil || resumed {
		t.Fatalf("attach client-2 after grace period: err=%v, resumed=%v", err, resumed)
	}
	server.detachSession(client2State, "client-2")
}

func TestWriterLock_SameClientGrantedInsideGracePeriod(t *testing.T) {
	server, clock, state, streamID := setupGraceTest(t)

	// Advance clock within grace period.
	clock.Advance(5 * time.Second)

	// Same client reconnects: granted at once with resumed=true.
	_, _, _, resumed, err := server.attachSession(t.Context(), streamID, 1, "client-1")
	if err != nil || !resumed {
		t.Fatalf("attach client-1 within grace: err=%v, resumed=%v", err, resumed)
	}
	server.detachSession(state, "client-1")
}

func TestAppend_RefusedHelloOverWire(t *testing.T) {
	ctx := t.Context()
	_, address, cleanup := startTestServer(t, inmemorystorage.New(), t.TempDir())
	t.Cleanup(func() {
		cleanup()
	})

	streamID := uuid.New()
	clientID1 := []byte("client-wire-1")
	clientID2 := []byte("client-wire-2")

	// Client 1 connects and attaches.
	session1Ctx, cancel1 := context.WithCancel(ctx)
	defer cancel1()

	connection1, err := grpc.NewClient(address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("grpc.NewClient: %v", err)
	}
	defer func() {
		if err := connection1.Close(); err != nil {
			t.Errorf("connection1.Close: %v", err)
		}
	}()

	stream1, err := pb.NewWalBufferClient(connection1).Append(session1Ctx)
	if err != nil {
		t.Fatalf("Append 1: %v", err)
	}
	if err := stream1.Send(&pb.AppendRequest{
		Msg: &pb.AppendRequest_Hello{
			Hello: &pb.Hello{
				StreamId: streamID[:],
				Epoch:    1,
				ClientId: clientID1,
			},
		},
	}); err != nil {
		t.Fatalf("Send Hello 1: %v", err)
	}
	response1, err := stream1.Recv()
	if err != nil {
		t.Fatalf("Recv HelloAck 1: %v", err)
	}
	if response1.GetHelloAck() == nil {
		t.Fatalf("expected HelloAck, got %v", response1)
	}

	// Client 2 connects to the same stream: Hello is refused and ends RPC with Unavailable.
	connection2, err := grpc.NewClient(address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("grpc.NewClient: %v", err)
	}
	defer func() {
		if err := connection2.Close(); err != nil {
			t.Errorf("connection2.Close: %v", err)
		}
	}()

	stream2, err := pb.NewWalBufferClient(connection2).Append(ctx)
	if err != nil {
		t.Fatalf("Append 2: %v", err)
	}
	if err := stream2.Send(&pb.AppendRequest{
		Msg: &pb.AppendRequest_Hello{
			Hello: &pb.Hello{
				StreamId: streamID[:],
				Epoch:    1,
				ClientId: clientID2,
			},
		},
	}); err != nil {
		t.Fatalf("Send Hello 2: %v", err)
	}
	_, err = stream2.Recv()
	if status.Code(err) != codes.Unavailable {
		t.Fatalf("expected codes.Unavailable, got %v", err)
	}
	if !strings.Contains(err.Error(), "stream writer lock is held by another client") {
		t.Fatalf("expected lock held message, got %v", err)
	}
	if strings.Contains(err.Error(), "client-wire-1") || strings.Contains(err.Error(), "client-wire-2") {
		t.Fatalf("message must not leak client IDs: %v", err)
	}
}

func TestAppend_CleanlyEndedSessionExitsForwarder(t *testing.T) {
	ctx := t.Context()
	server, address, cleanup := startTestServer(t, inmemorystorage.New(), t.TempDir())
	t.Cleanup(func() {
		cleanup()
	})

	streamID := uuid.New()
	clientID := []byte("client-clean-close")

	connection, err := grpc.NewClient(address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("grpc.NewClient: %v", err)
	}
	defer func() {
		if err := connection.Close(); err != nil {
			t.Errorf("connection.Close: %v", err)
		}
	}()

	stream, err := pb.NewWalBufferClient(connection).Append(ctx)
	if err != nil {
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
		t.Fatalf("Send Hello: %v", err)
	}

	response, err := stream.Recv()
	if err != nil {
		t.Fatalf("Recv HelloAck: %v", err)
	}
	if response.GetHelloAck() == nil {
		t.Fatalf("expected HelloAck, got %v", response)
	}

	if err := stream.CloseSend(); err != nil {
		t.Fatalf("CloseSend: %v", err)
	}

	// Read until the stream closes (io.EOF).
	for {
		_, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatalf("Recv to end: %v", err)
		}
	}

	// Verify that waiters and attachments reach zero.
	server.mu.RLock()
	st := server.streams[streamID.String()]
	server.mu.RUnlock()

	if st == nil {
		t.Fatalf("stream state not found for %s", streamID)
	}

	for deadline := time.Now().Add(5 * time.Second); ; time.Sleep(time.Millisecond) {
		st.mu.Lock()
		waiters := st.waiters
		attachments := st.attachments
		st.mu.Unlock()

		if waiters == 0 && attachments == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for forwarder to exit: waiters=%d, attachments=%d", waiters, attachments)
		}
	}
}

func TestWriterLock_IdleEvictionKeepsStreamInsideGracePeriod(t *testing.T) {
	server, clock, _, _ := setupGraceTest(t)

	// Advance within grace period: eviction must keep the stream.
	clock.Advance(5 * time.Second)
	server.evictIdleStreams()
	if count := server.StreamCount(); count != 1 {
		t.Fatalf("expected stream retained during grace period, got count %d", count)
	}

	// Advance past grace period: eviction removes the idle stream.
	clock.Advance(15 * time.Second)
	server.evictIdleStreams()
	if count := server.StreamCount(); count != 0 {
		t.Fatalf("expected stream evicted after grace period, got count %d", count)
	}
}
