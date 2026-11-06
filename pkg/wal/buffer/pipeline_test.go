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
	"fmt"
	"os"
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

func openPipelinedStream(t *testing.T, address string) pb.WalBuffer_AppendClient {
	conn, err := grpc.NewClient(address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial failed: %v", err)
	}
	t.Cleanup(func() {
		if closeErr := conn.Close(); closeErr != nil {
			t.Errorf("connection close failed: %v", closeErr)
		}
	})

	client := pb.NewWalBufferClient(conn)
	stream, err := client.Append(t.Context())
	if err != nil {
		t.Fatalf("failed to start stream: %v", err)
	}

	streamID := uuid.New()
	if err := stream.Send(&pb.AppendRequest{
		Msg: &pb.AppendRequest_Hello{
			Hello: &pb.Hello{
				StreamId: streamID[:],
			},
		},
	}); err != nil {
		t.Fatalf("failed to send hello: %v", err)
	}

	response, err := stream.Recv()
	if err != nil {
		t.Fatalf("failed to recv hello ack: %v", err)
	}
	if response.GetHelloAck() == nil {
		t.Fatalf("expected HelloAck, got: %+v", response)
	}

	return stream
}

func TestAppend_PipelinedGroupCommit(t *testing.T) {
	backend := inmemorystorage.New()
	_, address, cleanup := startTestServerWithBatchDelay(t, backend, t.TempDir(), 20*time.Millisecond)
	defer cleanup()

	stream := openPipelinedStream(t, address)

	const recordCount = 100
	for i := uint64(1); i <= recordCount; i++ {
		if err := stream.Send(&pb.AppendRequest{
			Msg: &pb.AppendRequest_Record{
				Record: &pb.AppendRecord{
					StreamSeq: i,
					Payload:   []byte(fmt.Sprintf("pipeline-record-%d", i)),
				},
			},
		}); err != nil {
			t.Fatalf("failed to send record %d: %v", i, err)
		}
	}

	var lastWitnessAck uint64
	var ackCount int
	for lastWitnessAck < recordCount {
		ackResponse, err := stream.Recv()
		if err != nil {
			t.Fatalf("recv failed: %v", err)
		}
		ack := ackResponse.GetAck()
		if ack == nil {
			t.Fatalf("expected Ack message, got: %+v", ackResponse)
		}
		if ack.WitnessAckedStreamSeq < lastWitnessAck {
			t.Fatalf("watermark decreased: previous=%d, got=%d", lastWitnessAck, ack.WitnessAckedStreamSeq)
		}
		lastWitnessAck = ack.WitnessAckedStreamSeq
		ackCount++
	}

	if ackCount >= recordCount {
		t.Errorf("expected pipelined records to coalesce into fewer than %d acks, got %d", recordCount, ackCount)
	}
}

func TestAppend_CommitErrorEndsStreamWithInternal(t *testing.T) {
	backend := inmemorystorage.New()
	dataDir := t.TempDir()
	server, address, cleanup := startTestServerWithBatchDelay(t, backend, dataDir, 20*time.Millisecond)
	defer cleanup()

	stream := openPipelinedStream(t, address)

	// Force commit failures by closing the local segment store and removing its directory.
	if err := server.localStore.Close(); err != nil {
		t.Fatalf("localStore.Close failed: %v", err)
	}
	if err := os.RemoveAll(dataDir); err != nil {
		t.Fatalf("os.RemoveAll failed: %v", err)
	}

	// Send pipelined records without calling CloseSend, testing that an idle client
	// waiting for acknowledgements promptly receives codes.Internal on commit failure.
	for i := uint64(1); i <= 5; i++ {
		if err := stream.Send(&pb.AppendRequest{
			Msg: &pb.AppendRequest_Record{
				Record: &pb.AppendRecord{
					StreamSeq: i,
					Payload:   []byte(fmt.Sprintf("pipeline-record-%d", i)),
				},
			},
		}); err != nil {
			break
		}
	}

	// Recv must fail with codes.Internal
	var streamErr error
	for {
		_, err := stream.Recv()
		if err != nil {
			streamErr = err
			break
		}
	}

	if status.Code(streamErr) != codes.Internal {
		t.Fatalf("expected stream error with codes.Internal, got %v (code: %v)", streamErr, status.Code(streamErr))
	}
}
