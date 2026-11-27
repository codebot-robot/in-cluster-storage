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
	"errors"
	"fmt"
	"io"
	"slices"
	"sync/atomic"
	"testing"
	"time"

	pb "github.com/gke-labs/in-cluster-storage/pkg/api/wal/v1alpha1"
	"github.com/gke-labs/in-cluster-storage/pkg/errutil"
	"github.com/gke-labs/in-cluster-storage/pkg/objectfs/blob"
	"github.com/gke-labs/in-cluster-storage/pkg/objectstore"
	"github.com/gke-labs/in-cluster-storage/pkg/objectstore/inmemorystorage"
	"github.com/gke-labs/in-cluster-storage/pkg/wal"
	"github.com/google/uuid"
)

type switchableBackend struct {
	objectstore.Backend
	unavailable atomic.Bool
}

func (b *switchableBackend) ListObjects(ctx context.Context, volumeID, prefix string) ([]objectstore.ObjectEntry, error) {
	if b.unavailable.Load() {
		return nil, fmt.Errorf("store outage: %w", objectstore.ErrUnavailable)
	}
	return b.Backend.ListObjects(ctx, volumeID, prefix)
}

func (b *switchableBackend) GetObject(ctx context.Context, volumeID, key string, offset, length int64, writer io.Writer) error {
	if b.unavailable.Load() {
		return fmt.Errorf("store outage: %w", objectstore.ErrUnavailable)
	}
	return b.Backend.GetObject(ctx, volumeID, key, offset, length, writer)
}

func (b *switchableBackend) PutObject(ctx context.Context, volumeID, key string, stream blob.ByteStream) (string, error) {
	if b.unavailable.Load() {
		return "", fmt.Errorf("store outage: %w", objectstore.ErrUnavailable)
	}
	return b.Backend.PutObject(ctx, volumeID, key, stream)
}

func TestOfflineStartup_ServesAndReconcilesOnFlush(t *testing.T) {
	ctx := t.Context()
	innerBackend := inmemorystorage.New()
	backend := &switchableBackend{Backend: innerBackend}
	dataDirectory := t.TempDir()
	streamID := uuid.New()

	handle1 := startTestServerHandle(t, backend, dataDirectory)
	for sequence := uint64(1); sequence <= 3; sequence++ {
		appendDirectRecord(t, handle1.srv, streamID, sequence, []byte(fmt.Sprintf("record-%d", sequence)))
	}
	if _, err := handle1.srv.Flush(ctx, &pb.FlushRequest{}); err != nil {
		t.Fatalf("Flush 1..3 failed: %v", err)
	}
	for sequence := uint64(4); sequence <= 5; sequence++ {
		appendDirectRecord(t, handle1.srv, streamID, sequence, []byte(fmt.Sprintf("record-%d", sequence)))
	}
	handle1.StopWithoutFlush()

	backend.unavailable.Store(true)
	server2, address, cleanup2 := startTestServer(t, backend, dataDirectory)
	defer func() {
		backend.unavailable.Store(false)
		cleanup2()
	}()

	clientSession, helloAck, err := openSession(ctx, t, address, streamID, 0)
	if err != nil {
		t.Fatalf("openSession failed: %v", err)
	}
	defer clientSession.close(t)
	if helloAck.GetWitnessAckedStreamSeq() != 5 || helloAck.GetS3AckedStreamSeq() != 0 {
		t.Fatalf("HelloAck watermarks = (witness=%d, s3=%d), want (5, 0)",
			helloAck.GetWitnessAckedStreamSeq(), helloAck.GetS3AckedStreamSeq())
	}

	ack, err := clientSession.appendRecord(6, []byte("record-6"))
	if err != nil {
		t.Fatalf("appendRecord 6 failed: %v", err)
	}
	if ack.GetWitnessAckedStreamSeq() != 6 || ack.GetS3AckedStreamSeq() != 0 || server2.LastPosition() != 6 {
		t.Fatalf("after append 6: witness=%d s3=%d position=%d, want 6, 0, and 6",
			ack.GetWitnessAckedStreamSeq(), ack.GetS3AckedStreamSeq(), server2.LastPosition())
	}
	if _, err := server2.Flush(ctx, &pb.FlushRequest{}); !errors.Is(err, objectstore.ErrUnavailable) {
		t.Fatalf("Flush during outage error = %v, want objectstore.ErrUnavailable", err)
	}

	backend.unavailable.Store(false)
	if _, err := server2.Flush(ctx, &pb.FlushRequest{}); err != nil {
		t.Fatalf("Flush after recovery failed: %v", err)
	}
	response, err := clientSession.stream.Recv()
	if err != nil || response.GetAck().GetS3AckedStreamSeq() != 6 {
		t.Fatalf("permanent watermark Ack = (%v, %v), want s3Seq 6", response, err)
	}

	segments, lastFlushed, err := ListSegmentsFromBackend(ctx, innerBackend)
	if err != nil {
		t.Fatalf("ListSegmentsFromBackend failed: %v", err)
	}
	wantSegments := []string{
		SegmentsPrefix + "000000000001-000000000003.wal",
		SegmentsPrefix + "000000000004-000000000006.wal",
	}
	if !slices.Equal(segments, wantSegments) || lastFlushed != 6 {
		t.Fatalf("segments = %v (lastFlushed=%d), want %v (lastFlushed=6)", segments, lastFlushed, wantSegments)
	}
	uploadedRecords, err := ReadSegmentFromBackend(ctx, innerBackend, wantSegments[1])
	if err != nil {
		t.Fatalf("ReadSegmentFromBackend failed: %v", err)
	}
	if len(uploadedRecords) != 3 || uploadedRecords[0].Position != 4 || uploadedRecords[2].Position != 6 {
		t.Fatalf("uploaded records count=%d range=%d..%d, want 3 (4..6)",
			len(uploadedRecords), uploadedRecords[0].Position, uploadedRecords[len(uploadedRecords)-1].Position)
	}

	ack7, err := clientSession.appendRecord(7, []byte("record-7"))
	if err != nil {
		t.Fatalf("appendRecord 7 failed: %v", err)
	}
	if ack7.GetWitnessAckedStreamSeq() != 7 || server2.LastPosition() != 7 {
		t.Fatalf("after append 7: witness=%d position=%d, want 7 and 7", ack7.GetWitnessAckedStreamSeq(), server2.LastPosition())
	}
	if _, err := server2.Flush(ctx, &pb.FlushRequest{}); err != nil {
		t.Fatalf("second Flush after reconciliation failed: %v", err)
	}
	response7, err := clientSession.stream.Recv()
	if err != nil || response7.GetAck().GetS3AckedStreamSeq() != 7 {
		t.Fatalf("permanent watermark Ack after record 7 = (%v, %v), want s3Seq 7", response7, err)
	}

	wantSegments = append(wantSegments, SegmentsPrefix+"000000000007-000000000007.wal")
	segments, lastFlushed, err = ListSegmentsFromBackend(ctx, innerBackend)
	if err != nil {
		t.Fatalf("ListSegmentsFromBackend after record 7 failed: %v", err)
	}
	if !slices.Equal(segments, wantSegments) || lastFlushed != 7 {
		t.Fatalf("segments after record 7 = %v (lastFlushed=%d), want %v (lastFlushed=7)", segments, lastFlushed, wantSegments)
	}
	uploadedRecord7, err := ReadSegmentFromBackend(ctx, innerBackend, wantSegments[2])
	if err != nil {
		t.Fatalf("ReadSegmentFromBackend record 7 failed: %v", err)
	}
	if len(uploadedRecord7) != 1 || uploadedRecord7[0].Position != 7 || uploadedRecord7[0].StreamSeq != 7 {
		t.Fatalf("uploaded record 7 = %v, want single record at position 7 seq 7", uploadedRecord7)
	}
}

func TestOfflineStartup_WatermarkFromStreamFiles(t *testing.T) {
	ctx := t.Context()
	backend := &switchableBackend{Backend: inmemorystorage.New()}
	dataDirectory := t.TempDir()
	streamA, streamB := uuid.New(), uuid.New()
	payload := bytes.Repeat([]byte("a"), 80)

	server1, err := NewServer(ctx, ServerConfig{
		Backend:        backend,
		DataDir:        dataDirectory,
		FlushInterval:  10 * time.Second,
		FlushBytes:     128,
		TailCacheBytes: 128,
		BatchMaxDelay:  2 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("NewServer 1 failed: %v", err)
	}
	for _, streamID := range []uuid.UUID{streamA, streamB} {
		for sequence := uint64(1); sequence <= 3; sequence++ {
			appendDirectRecord(t, server1, streamID, sequence, payload)
		}
		if _, err := server1.Flush(ctx, &pb.FlushRequest{}); err != nil {
			t.Fatalf("Flush failed: %v", err)
		}
	}
	if err := server1.Close(); err != nil {
		t.Fatalf("server1.Close failed: %v", err)
	}

	localStore, localRecords, err := wal.NewLogSegmentStore(dataDirectory, "log", 128)
	if err != nil {
		t.Fatalf("NewLogSegmentStore failed: %v", err)
	}
	errutil.CloseAndLog(localStore, "local segment store")
	for _, record := range localRecords {
		if record.StreamID == streamA {
			t.Fatalf("expected streamA records to have left local log, found position %d", record.Position)
		}
	}

	backend.unavailable.Store(true)
	_, address, cleanup2 := startTestServer(t, backend, dataDirectory)
	defer func() {
		backend.unavailable.Store(false)
		cleanup2()
	}()

	clientSession, helloAck, err := openSession(ctx, t, address, streamA, 0)
	if err != nil {
		t.Fatalf("openSession failed: %v", err)
	}
	defer clientSession.close(t)
	if got := helloAck.GetWitnessAckedStreamSeq(); got != 3 {
		t.Fatalf("HelloAck witness watermark = %d, want 3", got)
	}
}

func TestOfflineStartup_EmptyDataDirFailsWithUnavailable(t *testing.T) {
	backend := &switchableBackend{Backend: inmemorystorage.New()}
	backend.unavailable.Store(true)

	server, err := NewServer(t.Context(), ServerConfig{Backend: backend, DataDir: t.TempDir()})
	if server != nil {
		if closeError := server.Close(); closeError != nil {
			t.Errorf("server close error: %v", closeError)
		}
		t.Fatal("expected nil server on empty data directory during outage")
	}
	if !errors.Is(err, objectstore.ErrUnavailable) {
		t.Fatalf("NewServer error = %v, want objectstore.ErrUnavailable", err)
	}
}

func TestOfflineStartup_StaleDiskRefusesFlush(t *testing.T) {
	ctx := t.Context()
	innerBackend := inmemorystorage.New()
	backend := &switchableBackend{Backend: innerBackend}
	dataDirectory := t.TempDir()
	streamID := uuid.New()

	config := ServerConfig{Backend: backend, DataDir: dataDirectory, FlushInterval: 10 * time.Second, BatchMaxDelay: 2 * time.Millisecond}
	server1, err := NewServer(ctx, config)
	if err != nil {
		t.Fatalf("NewServer 1 failed: %v", err)
	}
	for sequence := uint64(1); sequence <= 3; sequence++ {
		appendDirectRecord(t, server1, streamID, sequence, []byte(fmt.Sprintf("record-%d", sequence)))
	}
	if _, err := server1.Flush(ctx, &pb.FlushRequest{}); err != nil {
		t.Fatalf("Flush failed: %v", err)
	}
	if err := server1.Close(); err != nil {
		t.Fatalf("server1.Close failed: %v", err)
	}

	extraSegment := blob.NewByteStreamFromBytes(wal.DefaultSegmentHeader().Encode())
	defer errutil.CloseAndLog(extraSegment, "extra segment")
	if _, err := innerBackend.PutObject(ctx, "", SegmentsPrefix+"000000000004-000000000005.wal", extraSegment); err != nil {
		t.Fatalf("PutObject extra segment failed: %v", err)
	}
	objectsBefore, err := innerBackend.ListObjects(ctx, "", "")
	if err != nil {
		t.Fatalf("ListObjects before failed: %v", err)
	}

	backend.unavailable.Store(true)
	server2, err := NewServer(ctx, config)
	if err != nil {
		t.Fatalf("NewServer 2 during outage failed: %v", err)
	}
	for sequence := uint64(4); sequence <= 6; sequence++ {
		appendDirectRecord(t, server2, streamID, sequence, []byte(fmt.Sprintf("stale-%d", sequence)))
	}

	backend.unavailable.Store(false)
	for attempt := 1; attempt <= 2; attempt++ {
		if _, err := server2.Flush(ctx, &pb.FlushRequest{}); err == nil {
			t.Fatalf("Flush attempt %d succeeded on stale disk, want error", attempt)
		}
	}
	if err := server2.Close(); err == nil {
		t.Fatal("server2.Close succeeded on stale disk, want shutdown flush error")
	}

	objectsAfter, err := innerBackend.ListObjects(ctx, "", "")
	if err != nil {
		t.Fatalf("ListObjects after failed: %v", err)
	}
	if !slices.Equal(objectsAfter, objectsBefore) {
		t.Fatalf("store objects changed on stale disk: got %v, want %v", objectsAfter, objectsBefore)
	}
}
