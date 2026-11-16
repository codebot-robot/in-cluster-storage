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
	"testing"

	pb "github.com/gke-labs/in-cluster-storage/pkg/api/wal/v1alpha1"
	"github.com/gke-labs/in-cluster-storage/pkg/objectstore/inmemorystorage"
	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

// openAppendStream connects to address and opens an append stream presenting the given streamID and epoch in Hello.
func openAppendStream(ctx context.Context, t *testing.T, address string, streamID uuid.UUID, epoch uint64) (pb.WalBuffer_AppendClient, *grpc.ClientConn, error) {
	connection, err := grpc.NewClient(address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial failed: %v", err)
	}

	stream, err := pb.NewWalBufferClient(connection).Append(ctx)
	if err != nil {
		_ = connection.Close()
		return nil, nil, err
	}

	helloMessage := &pb.Hello{
		StreamId: streamID[:],
		Epoch:    epoch,
	}
	if err := stream.Send(&pb.AppendRequest{
		Msg: &pb.AppendRequest_Hello{Hello: helloMessage},
	}); err != nil {
		_ = connection.Close()
		return nil, nil, err
	}

	response, err := stream.Recv()
	if err != nil {
		return stream, connection, err
	}
	if response.GetHelloAck() == nil {
		t.Fatalf("expected HelloAck response, got %v", response)
	}
	return stream, connection, nil
}

// sendRecord sends an AppendRecord on stream and returns the server's response.
func sendRecord(stream pb.WalBuffer_AppendClient, sequence uint64, payload []byte) (*pb.AppendResponse, error) {
	if err := stream.Send(&pb.AppendRequest{
		Msg: &pb.AppendRequest_Record{
			Record: &pb.AppendRecord{StreamSeq: sequence, Payload: payload},
		},
	}); err != nil {
		return nil, err
	}
	return stream.Recv()
}

// assertFencedError verifies that err is a gRPC status error with codes.FailedPrecondition
// carrying pb.Fenced in its details with matching streamID, writerEpoch, and acceptedEpoch.
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
		if fenced, ok := detail.(*pb.Fenced); ok {
			if !bytes.Equal(fenced.StreamId, streamID[:]) || fenced.WriterEpoch != expectedWriterEpoch || fenced.AcceptedEpoch != expectedAcceptedEpoch {
				t.Fatalf("fenced detail mismatch: got %+v, want streamID=%v, writerEpoch=%d, acceptedEpoch=%d", fenced, streamID, expectedWriterEpoch, expectedAcceptedEpoch)
			}
			return
		}
	}
	t.Fatalf("expected pb.Fenced detail in gRPC status: %v", statusObject.Details())
}

// TestAppend_HigherEpochTakesOverFirstWriterRefusedOnNextAppend tests that when a second writer
// with a higher epoch connects to the same stream, the first writer's subsequent append is refused
// with the typed status error carrying pb.Fenced.
func TestAppend_HigherEpochTakesOverFirstWriterRefusedOnNextAppend(t *testing.T) {
	ctx := t.Context()
	backend := inmemorystorage.New()
	_, address, cleanup := startTestServer(t, backend, t.TempDir())
	defer cleanup()

	streamID := uuid.New()

	// 1. Writer 1 connects with epoch 1 and appends record 1
	writer1Stream, writer1Conn, err := openAppendStream(ctx, t, address, streamID, 1)
	if err != nil {
		t.Fatalf("writer 1 open failed: %v", err)
	}
	defer func() { _ = writer1Conn.Close() }()

	response1, err := sendRecord(writer1Stream, 1, []byte("record-1"))
	if err != nil || response1.GetAck() == nil || response1.GetAck().WitnessAckedStreamSeq < 1 {
		t.Fatalf("writer 1 append failed: response=%v, err=%v", response1, err)
	}

	// 2. Writer 2 connects with higher epoch 2 and appends record 2
	writer2Stream, writer2Conn, err := openAppendStream(ctx, t, address, streamID, 2)
	if err != nil {
		t.Fatalf("writer 2 open failed: %v", err)
	}
	defer func() { _ = writer2Conn.Close() }()

	response2, err := sendRecord(writer2Stream, 2, []byte("record-2"))
	if err != nil || response2.GetAck() == nil || response2.GetAck().WitnessAckedStreamSeq < 2 {
		t.Fatalf("writer 2 append failed: response=%v, err=%v", response2, err)
	}

	// 3. Writer 1 attempts next append (record 3) - its next append must be refused with typed error
	_, recvErr := sendRecord(writer1Stream, 3, []byte("record-3-stale"))
	if recvErr == nil {
		_, recvErr = writer1Stream.Recv()
	}
	assertFencedError(t, recvErr, streamID, 1, 2)
}

// TestAppend_EqualEpochReconnects verifies that a writer reconnecting with the same epoch
// is accepted by the buffer.
func TestAppend_EqualEpochReconnects(t *testing.T) {
	ctx := t.Context()
	backend := inmemorystorage.New()
	_, address, cleanup := startTestServer(t, backend, t.TempDir())
	defer cleanup()

	streamID := uuid.New()

	// 1. Initial connection with epoch 5
	writerStream1, connection1, err := openAppendStream(ctx, t, address, streamID, 5)
	if err != nil {
		t.Fatalf("first connection failed: %v", err)
	}
	if _, err := sendRecord(writerStream1, 1, []byte("payload-1")); err != nil {
		t.Fatalf("send record 1 failed: %v", err)
	}
	_ = connection1.Close()

	// 2. Reconnection with the same epoch (5) must succeed
	writerStream2, connection2, err := openAppendStream(ctx, t, address, streamID, 5)
	if err != nil {
		t.Fatalf("reconnection with equal epoch failed: %v", err)
	}
	defer func() { _ = connection2.Close() }()

	response2, err := sendRecord(writerStream2, 2, []byte("payload-2"))
	if err != nil || response2.GetAck() == nil || response2.GetAck().WitnessAckedStreamSeq < 2 {
		t.Fatalf("expected ack >= 2 on reconnect, got response: %v, err: %v", response2, err)
	}
}

// TestAppend_LowerEpochRefusedAfterBufferRestart verifies that the highest accepted epoch
// survives a buffer restart on local disk, including when the buffer restarts immediately
// after accepting a higher epoch without appending records, and refuses lower epochs.
func TestAppend_LowerEpochRefusedAfterBufferRestart(t *testing.T) {
	ctx := t.Context()
	backend := inmemorystorage.New()
	dataDirectory := t.TempDir()
	streamID := uuid.New()

	// 1. Start buffer and accept session at epoch 10; crash/restart immediately after HelloAck
	server1, address1, cleanup1 := startTestServer(t, backend, dataDirectory)
	_, connection1, err := openAppendStream(ctx, t, address1, streamID, 10)
	if err != nil {
		t.Fatalf("open append stream with epoch 10 failed: %v", err)
	}
	_ = connection1.Close()
	cleanup1()
	_ = server1.Close()

	// 2. Restart buffer on the same data directory
	_, address2, cleanup2 := startTestServer(t, backend, dataDirectory)
	defer cleanup2()

	// 3. Connect with lower epoch 9 -> must be refused on initial handshake
	stream2, connection2, err := openAppendStream(ctx, t, address2, streamID, 9)
	if connection2 != nil {
		defer func() { _ = connection2.Close() }()
	}
	if err == nil {
		_, err = stream2.Recv()
	}
	assertFencedError(t, err, streamID, 9, 10)

	// 4. Equal epoch 10 reconnects successfully after restart
	stream3, connection3, err := openAppendStream(ctx, t, address2, streamID, 10)
	if err != nil {
		t.Fatalf("reconnect with equal epoch 10 failed: %v", err)
	}
	defer func() { _ = connection3.Close() }()

	if _, err := sendRecord(stream3, 1, []byte("payload-after-restart")); err != nil {
		t.Fatalf("append after restart failed: %v", err)
	}
}

// TestAppend_NoEpochWorksAsTodayWhenOnlyEpochZeroSeen verifies that a writer specifying no epoch
// (defaulting to 0) against a buffer that has only seen epoch 0 functions normally.
func TestAppend_NoEpochWorksAsTodayWhenOnlyEpochZeroSeen(t *testing.T) {
	ctx := t.Context()
	backend := inmemorystorage.New()
	_, address, cleanup := startTestServer(t, backend, t.TempDir())
	defer cleanup()

	streamID := uuid.New()

	// Connect with default epoch (0)
	writerStream, connection, err := openAppendStream(ctx, t, address, streamID, 0)
	if err != nil {
		t.Fatalf("open with epoch 0 failed: %v", err)
	}
	defer func() { _ = connection.Close() }()

	response, err := sendRecord(writerStream, 1, []byte("epoch-zero-payload"))
	if err != nil || response.GetAck() == nil || response.GetAck().WitnessAckedStreamSeq < 1 {
		t.Fatalf("expected ack >= 1, got response=%v, err=%v", response, err)
	}
}
