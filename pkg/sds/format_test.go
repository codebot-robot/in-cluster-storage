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

package sds

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	sdsv1 "github.com/gke-labs/in-cluster-storage/pkg/api/sds/v1"
	"github.com/gke-labs/in-cluster-storage/pkg/sds/record"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestStreamHeader_Golden(t *testing.T) {
	goldenPath := filepath.Join("testdata", "stream_header.golden")

	// Fixed timestamp for deterministic golden encoding
	hdr := &sdsv1.StreamHeader{
		FormatVersion:  1,
		CreatorVersion: "in-cluster-storage/1.0",
		CreatedAt:      &timestamppb.Timestamp{Seconds: 1792275600, Nanos: 0},
	}
	body, err := proto.Marshal(hdr)
	if err != nil {
		t.Fatalf("failed to marshal StreamHeader: %v", err)
	}

	actual, err := record.EncodeFrame(record.TypeIDStreamHeader, body)
	if err != nil {
		t.Fatalf("failed to encode frame: %v", err)
	}

	if _, err := os.Stat(goldenPath); os.IsNotExist(err) {
		if err := os.WriteFile(goldenPath, actual, 0644); err != nil {
			t.Fatalf("failed to write golden file: %v", err)
		}
	}

	expected, err := os.ReadFile(goldenPath)
	if err != nil {
		t.Fatalf("failed to read golden file %s: %v", goldenPath, err)
	}

	if !bytes.Equal(actual, expected) {
		t.Fatalf("stream header frame mismatch with golden file:\ngot:  %x\nwant: %x", actual, expected)
	}

	// Verify decoding the golden frame
	typeID, decodedBody, err := record.SplitFrame(expected)
	if err != nil {
		t.Fatalf("SplitFrame failed: %v", err)
	}
	if typeID != record.TypeIDStreamHeader {
		t.Fatalf("typeID = %d, want %d", typeID, record.TypeIDStreamHeader)
	}
	decodedHdr := &sdsv1.StreamHeader{}
	if err := proto.Unmarshal(decodedBody, decodedHdr); err != nil {
		t.Fatalf("Unmarshal StreamHeader failed: %v", err)
	}
	if decodedHdr.GetFormatVersion() != 1 {
		t.Errorf("FormatVersion = %d, want 1", decodedHdr.GetFormatVersion())
	}
	if decodedHdr.GetCreatorVersion() != "in-cluster-storage/1.0" {
		t.Errorf("CreatorVersion = %q, want %q", decodedHdr.GetCreatorVersion(), "in-cluster-storage/1.0")
	}
}

func TestStreamHeader_RejectsNewer(t *testing.T) {
	// Construct stream header with format_version = 2 (newer than current 1)
	newerHdr := &sdsv1.StreamHeader{
		FormatVersion:  2,
		CreatorVersion: "in-cluster-storage/2.0",
		CreatedAt:      timestamppb.Now(),
	}
	body, err := proto.Marshal(newerHdr)
	if err != nil {
		t.Fatalf("Marshal error: %v", err)
	}
	payload, err := record.EncodeFrame(record.TypeIDStreamHeader, body)
	if err != nil {
		t.Fatalf("EncodeFrame error: %v", err)
	}

	reader := NewChangeReader()
	_, err = reader.Feed(1, payload)
	if err == nil {
		t.Fatal("expected error for newer stream format version, got nil")
	}
	if !errors.Is(err, ErrUnsupportedStreamVersion) {
		t.Fatalf("expected ErrUnsupportedStreamVersion, got: %v", err)
	}
}

func TestStreamHeader_UnversionedUpgrade(t *testing.T) {
	// Construct an unversioned stream payload starting directly with Padding (TypeID 4)
	paddingPayload, err := record.EncodeFrame(record.TypeIDPadding, []byte("padding-body"))
	if err != nil {
		t.Fatalf("EncodeFrame error: %v", err)
	}

	// 1. Without flag: Feed at seq 1 must reject with ErrMissingStreamHeader
	readerWithoutFlag := NewChangeReader()
	_, err = readerWithoutFlag.Feed(1, paddingPayload)
	if err == nil {
		t.Fatal("expected error without acceptUnversioned, got nil")
	}
	if !errors.Is(err, ErrMissingStreamHeader) {
		t.Fatalf("expected ErrMissingStreamHeader, got: %v", err)
	}

	// 2. With flag: Feed at seq 1 must accept, log warning, and succeed
	readerWithFlag := NewChangeReaderWithOptions(true)
	changes, err := readerWithFlag.Feed(1, paddingPayload)
	if err != nil {
		t.Fatalf("unexpected error with acceptUnversioned=true: %v", err)
	}
	if len(changes) != 0 {
		t.Errorf("expected 0 changes from padding, got %d", len(changes))
	}
}

type formatTestAppender struct {
	payloads [][]byte
}

func (a *formatTestAppender) Append(ctx context.Context, payload []byte) (uint64, error) {
	a.payloads = append(a.payloads, payload)
	return uint64(len(a.payloads)), nil
}

func TestWriter_NonEmptyStreamDoesNotReEmitStreamHeader(t *testing.T) {
	ctx := t.Context()
	appender := &formatTestAppender{}

	// 1. Initial writer starts a new stream (emits StreamHeader at seq 1)
	w1 := NewWriter(appender)
	seq, err := w1.AppendPadding(ctx, 10)
	if err != nil {
		t.Fatalf("AppendPadding failed: %v", err)
	}
	if seq != 2 {
		t.Fatalf("expected seq 2 (1: StreamHeader, 2: Padding), got %d", seq)
	}
	if len(appender.payloads) != 2 {
		t.Fatalf("expected 2 payloads, got %d", len(appender.payloads))
	}
	typeID0, _, _ := record.SplitFrame(appender.payloads[0])
	if typeID0 != record.TypeIDStreamHeader {
		t.Fatalf("payload 0 type = %d, want TypeIDStreamHeader (%d)", typeID0, record.TypeIDStreamHeader)
	}

	// 2. Controller restarts: opens a new writer on the non-empty stream with WithStreamHeaderEmitted(true)
	w2 := NewWriter(appender, WithStreamHeaderEmitted(true))
	seq2, err := w2.AppendPadding(ctx, 20)
	if err != nil {
		t.Fatalf("w2.AppendPadding failed: %v", err)
	}
	if seq2 != 3 {
		t.Fatalf("expected seq 3, got %d", seq2)
	}
	if len(appender.payloads) != 3 {
		t.Fatalf("expected exactly 3 payloads (no spurious header), got %d", len(appender.payloads))
	}
	typeID2, _, _ := record.SplitFrame(appender.payloads[2])
	if typeID2 == record.TypeIDStreamHeader {
		t.Fatalf("spurious StreamHeader re-emitted on restart: payload 2 has type %d", typeID2)
	}
	if typeID2 != record.TypeIDPadding {
		t.Errorf("payload 2 type = %d, want TypeIDPadding (%d)", typeID2, record.TypeIDPadding)
	}
}
