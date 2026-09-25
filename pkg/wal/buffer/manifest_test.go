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
	"fmt"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	pb "github.com/gke-labs/in-cluster-storage/pkg/api/wal/v1alpha1"
	"github.com/gke-labs/in-cluster-storage/pkg/objectfs/blob"
	"github.com/gke-labs/in-cluster-storage/pkg/objectstore/filesystemstorage"
	"github.com/gke-labs/in-cluster-storage/pkg/objectstore/inmemorystorage"
	"github.com/gke-labs/in-cluster-storage/pkg/wal"
	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func TestManifestRoundTrip(t *testing.T) {
	ctx := t.Context()
	backend := inmemorystorage.New()

	// 1. Initial load should return empty manifest
	m, err := LoadManifest(ctx, backend)
	if err != nil {
		t.Fatalf("LoadManifest on empty backend failed: %v", err)
	}
	if m.LastPosition != 0 {
		t.Errorf("expected LastPosition 0, got %d", m.LastPosition)
	}
	if len(m.Segments) != 0 {
		t.Errorf("expected 0 segments, got %d", len(m.Segments))
	}
	if len(m.Streams) != 0 {
		t.Errorf("expected 0 streams, got %d", len(m.Streams))
	}

	// 2. Save and reload
	sid := uuid.New().String()
	m.LastPosition = 100
	m.Segments = []string{"wal/segments/000000000001-000000000100.wal"}
	m.Streams[sid] = StreamState{S3AckedStreamSeq: 50}

	if err := SaveManifest(ctx, backend, m); err != nil {
		t.Fatalf("SaveManifest failed: %v", err)
	}

	m2, err := LoadManifest(ctx, backend)
	if err != nil {
		t.Fatalf("LoadManifest failed: %v", err)
	}
	if m2.LastPosition != 100 {
		t.Errorf("expected LastPosition 100, got %d", m2.LastPosition)
	}
	if len(m2.Segments) != 1 || m2.Segments[0] != "wal/segments/000000000001-000000000100.wal" {
		t.Errorf("segments mismatch: %+v", m2.Segments)
	}
	if st, ok := m2.Streams[sid]; !ok || st.S3AckedStreamSeq != 50 {
		t.Errorf("stream state mismatch: %+v", m2.Streams)
	}
}

func TestReadSegmentFromBackend(t *testing.T) {
	ctx := t.Context()
	backend := inmemorystorage.New()

	streamID := uuid.New()
	recs := []*wal.LogRecord{
		{
			Position:  1,
			StreamID:  streamID,
			StreamSeq: 1,
			Payload:   []byte("payload-1"),
		},
		{
			Position:  2,
			StreamID:  streamID,
			StreamSeq: 2,
			Payload:   []byte("payload-2"),
		},
	}
	for _, r := range recs {
		r.CRC32C = r.ComputeCRC32C()
	}

	var segBuf bytes.Buffer
	for _, r := range recs {
		segBuf.Write(r.Encode())
	}

	segPath := "wal/segments/000000000001-000000000002.wal"
	if _, err := backend.PutObject(ctx, "", segPath, blob.NewByteStreamFromBytes(segBuf.Bytes())); err != nil {
		t.Fatalf("PutObject failed: %v", err)
	}

	loadedRecs, err := ReadSegmentFromBackend(ctx, backend, segPath)
	if err != nil {
		t.Fatalf("ReadSegmentFromBackend failed: %v", err)
	}
	if len(loadedRecs) != 2 {
		t.Fatalf("expected 2 records, got %d", len(loadedRecs))
	}
	for i, r := range loadedRecs {
		if r.Position != recs[i].Position {
			t.Errorf("record %d position mismatch: got %d, want %d", i, r.Position, recs[i].Position)
		}
		if r.StreamID != recs[i].StreamID {
			t.Errorf("record %d streamID mismatch", i)
		}
		if r.StreamSeq != recs[i].StreamSeq {
			t.Errorf("record %d streamSeq mismatch: got %d, want %d", i, r.StreamSeq, recs[i].StreamSeq)
		}
		if !bytes.Equal(r.Payload, recs[i].Payload) {
			t.Errorf("record %d payload mismatch: got %q, want %q", i, string(r.Payload), string(recs[i].Payload))
		}
	}
}

func TestArchivingToFilesystemStorage(t *testing.T) {
	ctx := t.Context()
	storeDir := t.TempDir()
	dataDir := t.TempDir()

	backend, err := filesystemstorage.New(storeDir)
	if err != nil {
		t.Fatalf("failed to create filesystem backend: %v", err)
	}

	srv, err := NewServer(ctx, ServerConfig{
		Backend:       backend,
		DataDir:       dataDir,
		FlushInterval: 10 * time.Second,
		BatchMaxDelay: 1 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("failed to create server: %v", err)
	}

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to listen: %v", err)
	}

	grpcServer := grpc.NewServer()
	pb.RegisterWalBufferServer(grpcServer, srv)
	go func() {
		_ = grpcServer.Serve(listener)
	}()
	defer func() {
		grpcServer.Stop()
		_ = srv.Close()
		_ = listener.Close()
	}()

	conn, err := grpc.NewClient(listener.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial failed: %v", err)
	}
	defer conn.Close()

	client := pb.NewWalBufferClient(conn)
	appendStream, err := client.Append(ctx)
	if err != nil {
		t.Fatalf("append failed: %v", err)
	}

	streamID := uuid.New()
	_ = appendStream.Send(&pb.AppendRequest{Msg: &pb.AppendRequest_Hello{Hello: &pb.Hello{StreamId: streamID[:]}}})
	_, _ = appendStream.Recv()

	for i := uint64(1); i <= 5; i++ {
		_ = appendStream.Send(&pb.AppendRequest{
			Msg: &pb.AppendRequest_Record{
				Record: &pb.AppendRecord{
					StreamSeq: i,
					Payload:   []byte(fmt.Sprintf("data-%d", i)),
				},
			},
		})
		_, err := appendStream.Recv()
		if err != nil {
			t.Fatalf("recv ack failed: %v", err)
		}
	}

	// Flush to filesystem object storage
	flushResp, err := client.Flush(ctx, &pb.FlushRequest{})
	if err != nil {
		t.Fatalf("flush failed: %v", err)
	}
	if flushResp.LastPosition != 5 {
		t.Errorf("expected LastPosition 5, got %d", flushResp.LastPosition)
	}

	// Verify files created in storeDir
	manifestPath := filepath.Join(storeDir, "wal", "manifest.json")
	if _, err := os.Stat(manifestPath); err != nil {
		t.Fatalf("manifest file does not exist at %s: %v", manifestPath, err)
	}

	segPath := filepath.Join(storeDir, "wal", "segments", "000000000001-000000000005.wal")
	if _, err := os.Stat(segPath); err != nil {
		t.Fatalf("segment file does not exist at %s: %v", segPath, err)
	}

	// Tail from position 1 to verify reading archived segment from backend
	tailStream, err := client.Tail(ctx, &pb.TailRequest{FromPosition: 1})
	if err != nil {
		t.Fatalf("tail failed: %v", err)
	}

	for i := uint64(1); i <= 5; i++ {
		resp, err := tailStream.Recv()
		if err != nil {
			t.Fatalf("tail recv %d failed: %v", i, err)
		}
		if resp.GetRecord().Position != i {
			t.Errorf("expected record position %d, got %d", i, resp.GetRecord().Position)
		}
		if string(resp.GetRecord().Payload) != fmt.Sprintf("data-%d", i) {
			t.Errorf("expected payload data-%d, got %s", i, string(resp.GetRecord().Payload))
		}
	}
}
