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
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
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

// session bundles the client stream and underlying gRPC connection for test sessions.
type session struct {
	stream     pb.WalBuffer_AppendClient
	connection *grpc.ClientConn
}

// close closes the underlying gRPC connection during test cleanup.
func (s *session) close(t *testing.T) {
	t.Helper()
	if err := s.connection.Close(); err != nil {
		t.Logf("connection close error: %v", err)
	}
}

// appendRecord sends a record on the stream and waits for the server acknowledgement.
// If Send fails because the server closed the stream with a status error, it calls Recv
// to retrieve the actual gRPC status error.
func (s *session) appendRecord(sequence uint64, payload []byte) (*pb.Ack, error) {
	if err := s.stream.Send(&pb.AppendRequest{
		Msg: &pb.AppendRequest_Record{
			Record: &pb.AppendRecord{StreamSeq: sequence, Payload: payload},
		},
	}); err != nil {
		response, receiveError := s.stream.Recv()
		if receiveError != nil {
			return nil, receiveError
		}
		return nil, fmt.Errorf("unexpected response on send error: %v", response)
	}

	response, err := s.stream.Recv()
	if err != nil {
		return nil, err
	}
	return response.GetAck(), nil
}

// openSession dials address and performs the Hello handshake presenting streamID and epoch.
func openSession(ctx context.Context, t *testing.T, address string, streamID uuid.UUID, epoch uint64) (*session, *pb.HelloAck, error) {
	t.Helper()
	connection, err := grpc.NewClient(address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial failed: %v", err)
	}

	stream, err := pb.NewWalBufferClient(connection).Append(ctx)
	if err != nil {
		if closeError := connection.Close(); closeError != nil {
			t.Logf("connection close error: %v", closeError)
		}
		return nil, nil, err
	}

	helloMessage := &pb.Hello{
		StreamId: streamID[:],
		Epoch:    epoch,
	}
	if err := stream.Send(&pb.AppendRequest{
		Msg: &pb.AppendRequest_Hello{Hello: helloMessage},
	}); err != nil {
		if closeError := connection.Close(); closeError != nil {
			t.Logf("connection close error: %v", closeError)
		}
		return nil, nil, err
	}

	response, err := stream.Recv()
	if err != nil {
		return &session{stream: stream, connection: connection}, nil, err
	}

	helloAck := response.GetHelloAck()
	if helloAck == nil {
		t.Fatalf("expected HelloAck response, got %v", response)
	}
	return &session{stream: stream, connection: connection}, helloAck, nil
}

// assertFencedError verifies that err is a gRPC status error with codes.FailedPrecondition
// carrying pb.Fenced in its details matching streamID, expectedWriterEpoch, and expectedAcceptedEpoch.
func assertFencedError(t *testing.T, err error, streamID uuid.UUID, expectedWriterEpoch, expectedAcceptedEpoch uint64) {
	t.Helper()
	if err == nil {
		t.Fatal("expected fenced error, got nil")
	}

	statusObject, ok := status.FromError(err)
	if !ok || statusObject.Code() != codes.FailedPrecondition {
		t.Fatalf("expected FailedPrecondition status, got code %v, err: %v", statusObject.Code(), err)
	}

	for _, detail := range statusObject.Details() {
		fenced, isFenced := detail.(*pb.Fenced)
		if isFenced {
			if !bytes.Equal(fenced.StreamId, streamID[:]) || fenced.WriterEpoch != expectedWriterEpoch || fenced.AcceptedEpoch != expectedAcceptedEpoch {
				t.Fatalf("fenced detail mismatch: got %+v, want streamID=%v, writerEpoch=%d, acceptedEpoch=%d",
					fenced, streamID, expectedWriterEpoch, expectedAcceptedEpoch)
			}
			return
		}
	}
	t.Fatalf("expected pb.Fenced detail in gRPC status details: %v", statusObject.Details())
}

// TestAppend_LowerEpochRefused verifies that a session presenting an epoch lower than the highest
// accepted epoch on the stream is refused during the Hello handshake with the fenced status.
func TestAppend_LowerEpochRefused(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()

	backend := inmemorystorage.New()
	_, address, cleanup := startTestServer(t, backend, t.TempDir())
	defer cleanup()

	streamID := uuid.New()

	firstSession, _, err := openSession(ctx, t, address, streamID, 5)
	if err != nil {
		t.Fatalf("first session with epoch 5 failed: %v", err)
	}
	defer firstSession.close(t)

	ack, err := firstSession.appendRecord(1, []byte("record-1"))
	if err != nil || ack == nil || ack.WitnessAckedStreamSeq < 1 {
		t.Fatalf("first session append failed: ack=%v, err=%v", ack, err)
	}

	secondSession, _, err := openSession(ctx, t, address, streamID, 2)
	if secondSession != nil {
		defer secondSession.close(t)
	}
	assertFencedError(t, err, streamID, 2, 5)
}

// TestAppend_EqualEpochAccepted verifies that an owner reconnecting with an epoch equal to the
// highest accepted epoch is accepted and can continue appending.
func TestAppend_EqualEpochAccepted(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()

	backend := inmemorystorage.New()
	_, address, cleanup := startTestServer(t, backend, t.TempDir())
	defer cleanup()

	streamID := uuid.New()

	firstSession, _, err := openSession(ctx, t, address, streamID, 5)
	if err != nil {
		t.Fatalf("first connection with epoch 5 failed: %v", err)
	}
	ack, err := firstSession.appendRecord(1, []byte("record-1"))
	if err != nil || ack == nil || ack.WitnessAckedStreamSeq < 1 {
		t.Fatalf("record 1 append failed: ack=%v, err=%v", ack, err)
	}
	firstSession.close(t)

	secondSession, helloAck, err := openSession(ctx, t, address, streamID, 5)
	if err != nil {
		t.Fatalf("reconnection with equal epoch 5 failed: %v", err)
	}
	defer secondSession.close(t)

	if helloAck.WitnessAckedStreamSeq < 1 {
		t.Fatalf("expected HelloAck witness watermark >= 1, got %d", helloAck.WitnessAckedStreamSeq)
	}

	secondAck, err := secondSession.appendRecord(2, []byte("record-2"))
	if err != nil || secondAck == nil || secondAck.WitnessAckedStreamSeq < 2 {
		t.Fatalf("record 2 append failed: ack=%v, err=%v", secondAck, err)
	}
}

// TestAppend_IdleSupersededSessionClosed verifies that an idle superseded session is closed
// promptly with the fenced status when a higher epoch is accepted, without waiting for the superseded
// session to send another message.
func TestAppend_IdleSupersededSessionClosed(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()

	backend := inmemorystorage.New()
	_, address, cleanup := startTestServer(t, backend, t.TempDir())
	defer cleanup()

	streamID := uuid.New()

	firstSession, _, err := openSession(ctx, t, address, streamID, 1)
	if err != nil {
		t.Fatalf("first session failed: %v", err)
	}
	defer firstSession.close(t)

	secondSession, _, err := openSession(ctx, t, address, streamID, 2)
	if err != nil {
		t.Fatalf("second session failed: %v", err)
	}
	defer secondSession.close(t)

	_, receiveError := firstSession.stream.Recv()
	assertFencedError(t, receiveError, streamID, 1, 2)
}

// TestAppend_InFlightRecordFromSupersededSessionDiscarded verifies that commitItems discards
// queued in-flight records from superseded sessions whose epoch is lower than the stream's
// accepted epoch, leaving nothing stored, the witness watermark unchanged, and inFlight decremented.
func TestAppend_InFlightRecordFromSupersededSessionDiscarded(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()

	backend := inmemorystorage.New()
	server, address, cleanup := startTestServer(t, backend, t.TempDir())
	defer cleanup()

	streamID := uuid.New()

	session, helloAck, err := openSession(ctx, t, address, streamID, 2)
	if err != nil {
		t.Fatalf("open session failed: %v", err)
	}
	defer session.close(t)

	if helloAck.WitnessAckedStreamSeq != 0 {
		t.Fatalf("expected HelloAck witness sequence 0, got %d", helloAck.WitnessAckedStreamSeq)
	}

	server.mu.RLock()
	stream := server.streams[streamID.String()]
	server.mu.RUnlock()

	if stream == nil {
		t.Fatalf("stream state not found for %s", streamID)
	}

	stream.mu.Lock()
	stream.inFlight = 1
	stream.mu.Unlock()

	server.commitItems(ctx, []incomingItem{
		{
			streamID:  streamID,
			streamSeq: 1,
			payload:   []byte("superseded-payload"),
			epoch:     1,
		},
	})

	server.mu.RLock()
	lastPosition := server.lastPosition
	server.mu.RUnlock()

	server.unflushedMu.Lock()
	unflushedCount := len(server.unflushedRecords)
	server.unflushedMu.Unlock()

	stream.mu.Lock()
	witnessSeq := stream.witnessSeq
	inFlight := stream.inFlight
	stream.mu.Unlock()

	if lastPosition != 0 {
		t.Fatalf("expected lastPosition 0, got %d", lastPosition)
	}
	if unflushedCount != 0 {
		t.Fatalf("expected 0 unflushed records, got %d", unflushedCount)
	}
	if witnessSeq != 0 {
		t.Fatalf("expected witness watermark 0, got %d", witnessSeq)
	}
	if inFlight != 0 {
		t.Fatalf("expected inFlight 0, got %d", inFlight)
	}
}

// TestAppend_EpochSurvivesBufferRestart verifies that after a buffer restart on the same data directory,
// a session with a lower epoch is refused with the Fenced detail and a session with an equal epoch is accepted.
func TestAppend_EpochSurvivesBufferRestart(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()

	backend := inmemorystorage.New()
	dataDir := t.TempDir()
	streamID := uuid.New()

	_, address1, cleanup1 := startTestServer(t, backend, dataDir)
	firstSession, _, err := openSession(ctx, t, address1, streamID, 2)
	if err != nil {
		t.Fatalf("first session with epoch 2 failed: %v", err)
	}
	firstSession.close(t)
	cleanup1()

	_, address2, cleanup2 := startTestServer(t, backend, dataDir)
	defer cleanup2()

	refusedSession, _, err := openSession(ctx, t, address2, streamID, 1)
	if refusedSession != nil {
		defer refusedSession.close(t)
	}
	assertFencedError(t, err, streamID, 1, 2)

	acceptedSession, _, err := openSession(ctx, t, address2, streamID, 2)
	if err != nil {
		t.Fatalf("session with epoch 2 failed after restart: %v", err)
	}
	defer acceptedSession.close(t)
}

// TestAppend_EpochSurvivesIdleStreamEviction verifies that after a stream is evicted for idleness,
// a session with a lower epoch is refused with the Fenced detail and a session with an equal epoch is accepted.
func TestAppend_EpochSurvivesIdleStreamEviction(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()

	backend := inmemorystorage.New()
	server, address, cleanup := startTestServer(t, backend, t.TempDir())
	defer cleanup()

	streamID := uuid.New()

	state, _, _, err := server.attachSession(ctx, streamID, 2)
	if err != nil {
		t.Fatalf("attachSession with epoch 2 failed: %v", err)
	}
	server.detachSession(state)

	server.evictIdleStreams()

	refusedSession, _, err := openSession(ctx, t, address, streamID, 1)
	if refusedSession != nil {
		defer refusedSession.close(t)
	}
	assertFencedError(t, err, streamID, 1, 2)

	acceptedSession, _, err := openSession(ctx, t, address, streamID, 2)
	if err != nil {
		t.Fatalf("session with epoch 2 failed after eviction: %v", err)
	}
	defer acceptedSession.close(t)
}

// TestAppend_SupersededWriterRecordCannotCommitDuringHandshake verifies that when an epoch-2
// Hello handshake is in progress and held between its save and its in-memory raise, a record
// submitted by an epoch-1 writer cannot commit without being reflected in the epoch-2 HelloAck
// witness watermark.
func TestAppend_SupersededWriterRecordCannotCommitDuringHandshake(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()

	backend := inmemorystorage.New()
	server, address, cleanup := startTestServer(t, backend, t.TempDir())
	defer cleanup()

	streamID := uuid.New()

	firstSession, _, err := openSession(ctx, t, address, streamID, 1)
	if err != nil {
		t.Fatalf("first session with epoch 1 failed: %v", err)
	}
	defer firstSession.close(t)

	server.mu.RLock()
	st := server.streams[streamID.String()]
	server.mu.RUnlock()
	if st == nil {
		t.Fatalf("stream state not found for %s", streamID)
	}

	// Hold the stream's epoch mutex from the test to simulate the handshake being held
	// between save and in-memory raise.
	st.epochMu.Lock()

	type helloResult struct {
		session  *session
		helloAck *pb.HelloAck
		err      error
	}
	resCh := make(chan helloResult, 1)
	go func() {
		s2, ack, err := openSession(ctx, t, address, streamID, 2)
		resCh <- helloResult{session: s2, helloAck: ack, err: err}
	}()

	// Bounded poll of attachments to ensure session 2 has attached.
	deadline := time.Now().Add(5 * time.Second)
	for {
		st.mu.Lock()
		attachments := st.attachments
		st.mu.Unlock()
		if attachments == 2 {
			break
		}
		if time.Now().After(deadline) {
			st.epochMu.Unlock()
			t.Fatal("timed out waiting for session 2 to attach")
		}
		time.Sleep(5 * time.Millisecond)
	}

	// Epoch-1 writer submits a record meanwhile.
	ack, err := firstSession.appendRecord(1, []byte("record-1"))
	if err != nil || ack == nil || ack.WitnessAckedStreamSeq < 1 {
		st.epochMu.Unlock()
		t.Fatalf("first session append failed: ack=%v, err=%v", ack, err)
	}

	// Release epoch mutex.
	st.epochMu.Unlock()

	res := <-resCh
	if res.err != nil {
		t.Fatalf("second session failed: %v", res.err)
	}
	if res.session != nil {
		defer res.session.close(t)
	}

	st.mu.Lock()
	streamWitnessWatermark := st.witnessSeq
	st.mu.Unlock()

	if res.helloAck.WitnessAckedStreamSeq != streamWitnessWatermark {
		t.Fatalf("HelloAck witness watermark %d does not equal stream witness watermark %d",
			res.helloAck.WitnessAckedStreamSeq, streamWitnessWatermark)
	}
}

// TestAppend_CorruptEpochFileReturnsInternalWithoutFilePath verifies that a corrupt epoch file
// causes the Hello handshake to fail with codes.Internal and a short message that does not leak
// the server's file path to the client.
func TestAppend_CorruptEpochFileReturnsInternalWithoutFilePath(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()

	dataDir := t.TempDir()
	streamID := uuid.New()

	// Pre-create a corrupt epoch file on disk
	streamDir := filepath.Join(dataDir, "streams", streamID.String())
	if err := os.MkdirAll(streamDir, 0755); err != nil {
		t.Fatalf("failed to create stream dir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(streamDir, "epoch"), []byte("corrupt-not-a-number"), 0644); err != nil {
		t.Fatalf("failed to write corrupt epoch file: %v", err)
	}

	backend := inmemorystorage.New()
	_, address, cleanup := startTestServer(t, backend, dataDir)
	defer cleanup()

	sess, _, err := openSession(ctx, t, address, streamID, 1)
	if sess != nil {
		defer sess.close(t)
	}
	if err == nil {
		t.Fatal("expected error on corrupt epoch file, got nil")
	}

	statusObj, ok := status.FromError(err)
	if !ok {
		t.Fatalf("expected gRPC status error, got: %v", err)
	}
	if statusObj.Code() != codes.Internal {
		t.Fatalf("expected codes.Internal, got %v: %v", statusObj.Code(), err)
	}
	if strings.Contains(statusObj.Message(), dataDir) || strings.Contains(statusObj.Message(), streamDir) {
		t.Fatalf("error message leaks server file path: %q", statusObj.Message())
	}
	if statusObj.Message() != "failed to read stream epoch" {
		t.Fatalf("expected short message %q, got %q", "failed to read stream epoch", statusObj.Message())
	}
}
