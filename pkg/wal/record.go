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

package wal

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"

	pb "github.com/gke-labs/in-cluster-storage/pkg/api/wal/v1alpha1"
	"github.com/google/uuid"
)

const (
	// ClientHeaderSize is the fixed size of a client record header (36 bytes):
	// magic(4) + stream_id(16) + stream_seq(8) + length(4) + crc32c(4)
	ClientHeaderSize = 36

	// LogHeaderSize is the fixed size of a log record header (44 bytes):
	// magic(4) + position(8) + stream_id(16) + stream_seq(8) + length(4) + crc32c(4)
	LogHeaderSize = 44
)

var (
	// ClientMagicBytes is "WALC"
	ClientMagicBytes = [4]byte{'W', 'A', 'L', 'C'}

	// LogMagicBytes is "WALL"
	LogMagicBytes = [4]byte{'W', 'A', 'L', 'L'}

	// CastagnoliTable is the CRC32C lookup table.
	CastagnoliTable = crc32.MakeTable(crc32.Castagnoli)

	// ErrCorruptedRecord is returned when a record header magic is invalid.
	ErrCorruptedRecord = errors.New("corrupted WAL record header")

	// ErrChecksumMismatch is returned when the CRC32C of the record does not match the header.
	ErrChecksumMismatch = errors.New("record CRC32C checksum mismatch")

	// ErrTruncatedRecord is returned when record data ends unexpectedly.
	ErrTruncatedRecord = errors.New("truncated WAL record")
)

// ClientRecord represents a record stored in client segment files.
type ClientRecord struct {
	StreamID  uuid.UUID
	StreamSeq uint64
	Payload   []byte
	CRC32C    uint32
}

// ComputeCRC32C calculates the CRC32C checksum over the 36-byte header (with CRC zeroed) and payload.
func (r *ClientRecord) ComputeCRC32C() uint32 {
	var hdr [ClientHeaderSize]byte
	copy(hdr[0:4], ClientMagicBytes[:])
	copy(hdr[4:20], r.StreamID[:])
	binary.BigEndian.PutUint64(hdr[20:28], r.StreamSeq)
	binary.BigEndian.PutUint32(hdr[28:32], uint32(len(r.Payload)))
	binary.BigEndian.PutUint32(hdr[32:36], 0) // zeroed crc field

	h := crc32.New(CastagnoliTable)
	h.Write(hdr[:])
	h.Write(r.Payload)
	return h.Sum32()
}

// Encode serializes the ClientRecord to on-disk binary format:
// [4B magic "WALC"][16B stream_id][8B stream_seq][4B length][4B crc32c][payload]
func (r *ClientRecord) Encode() []byte {
	r.CRC32C = r.ComputeCRC32C()
	buf := make([]byte, ClientHeaderSize+len(r.Payload))
	copy(buf[0:4], ClientMagicBytes[:])
	copy(buf[4:20], r.StreamID[:])
	binary.BigEndian.PutUint64(buf[20:28], r.StreamSeq)
	binary.BigEndian.PutUint32(buf[28:32], uint32(len(r.Payload)))
	binary.BigEndian.PutUint32(buf[32:36], r.CRC32C)
	copy(buf[ClientHeaderSize:], r.Payload)
	return buf
}

// ToAppendProto converts to the gRPC AppendRecord format sent by clients.
func (r *ClientRecord) ToAppendProto() *pb.AppendRecord {
	if r.CRC32C == 0 {
		r.CRC32C = r.ComputeCRC32C()
	}
	return &pb.AppendRecord{
		StreamSeq: r.StreamSeq,
		Payload:   r.Payload,
		Crc32C:    r.CRC32C,
	}
}

// DecodeClientRecord reads and validates a single ClientRecord from an io.Reader.
func DecodeClientRecord(r io.Reader) (*ClientRecord, error) {
	var hdr [ClientHeaderSize]byte
	if _, err := io.ReadFull(r, hdr[:]); err != nil {
		if err == io.EOF {
			return nil, io.EOF
		}
		if errors.Is(err, io.ErrUnexpectedEOF) {
			return nil, ErrTruncatedRecord
		}
		return nil, err
	}

	if hdr[0] != ClientMagicBytes[0] || hdr[1] != ClientMagicBytes[1] || hdr[2] != ClientMagicBytes[2] || hdr[3] != ClientMagicBytes[3] {
		return nil, ErrCorruptedRecord
	}

	var streamID uuid.UUID
	copy(streamID[:], hdr[4:20])
	streamSeq := binary.BigEndian.Uint64(hdr[20:28])
	length := binary.BigEndian.Uint32(hdr[28:32])
	expectedCRC := binary.BigEndian.Uint32(hdr[32:36])

	payload := make([]byte, length)
	if length > 0 {
		if _, err := io.ReadFull(r, payload); err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
				return nil, ErrTruncatedRecord
			}
			return nil, err
		}
	}

	rec := &ClientRecord{
		StreamID:  streamID,
		StreamSeq: streamSeq,
		Payload:   payload,
		CRC32C:    expectedCRC,
	}

	if rec.ComputeCRC32C() != expectedCRC {
		return nil, ErrChecksumMismatch
	}

	return rec, nil
}

// LogRecord represents a record stored in witness log segments and permanent storage.
type LogRecord struct {
	Position  uint64
	StreamID  uuid.UUID
	StreamSeq uint64
	Payload   []byte
	CRC32C    uint32
}

// ComputeCRC32C calculates the CRC32C checksum over the 44-byte header (with CRC zeroed) and payload.
func (r *LogRecord) ComputeCRC32C() uint32 {
	var hdr [LogHeaderSize]byte
	copy(hdr[0:4], LogMagicBytes[:])
	binary.BigEndian.PutUint64(hdr[4:12], r.Position)
	copy(hdr[12:28], r.StreamID[:])
	binary.BigEndian.PutUint64(hdr[28:36], r.StreamSeq)
	binary.BigEndian.PutUint32(hdr[36:40], uint32(len(r.Payload)))
	binary.BigEndian.PutUint32(hdr[40:44], 0) // zeroed crc field

	h := crc32.New(CastagnoliTable)
	h.Write(hdr[:])
	h.Write(r.Payload)
	return h.Sum32()
}

// Encode serializes the LogRecord to on-disk binary format:
// [4B magic "WALL"][8B position][16B stream_id][8B stream_seq][4B length][4B crc32c][payload]
func (r *LogRecord) Encode() []byte {
	crc := r.CRC32C
	if crc == 0 {
		crc = r.ComputeCRC32C()
	}
	buf := make([]byte, LogHeaderSize+len(r.Payload))
	copy(buf[0:4], LogMagicBytes[:])
	binary.BigEndian.PutUint64(buf[4:12], r.Position)
	copy(buf[12:28], r.StreamID[:])
	binary.BigEndian.PutUint64(buf[28:36], r.StreamSeq)
	binary.BigEndian.PutUint32(buf[36:40], uint32(len(r.Payload)))
	binary.BigEndian.PutUint32(buf[40:44], crc)
	copy(buf[LogHeaderSize:], r.Payload)
	return buf
}

// ToProto converts LogRecord to pb.LogRecord.
func (r *LogRecord) ToProto() *pb.LogRecord {
	crc := r.CRC32C
	if crc == 0 {
		crc = r.ComputeCRC32C()
	}
	return &pb.LogRecord{
		Position:  r.Position,
		StreamId:  r.StreamID[:],
		StreamSeq: r.StreamSeq,
		Payload:   r.Payload,
		Crc32C:    crc,
	}
}

// FromProto converts a pb.LogRecord to LogRecord.
func FromProto(p *pb.LogRecord) (*LogRecord, error) {
	if p == nil {
		return nil, errors.New("nil protobuf record")
	}
	if len(p.StreamId) != 16 {
		return nil, fmt.Errorf("invalid stream_id length %d (expected 16)", len(p.StreamId))
	}
	var id uuid.UUID
	copy(id[:], p.StreamId)

	rec := &LogRecord{
		Position:  p.Position,
		StreamID:  id,
		StreamSeq: p.StreamSeq,
		Payload:   p.Payload,
		CRC32C:    p.Crc32C,
	}
	if rec.CRC32C == 0 {
		rec.CRC32C = rec.ComputeCRC32C()
	}
	return rec, nil
}

// DecodeLogRecord reads and validates a single LogRecord from an io.Reader.
func DecodeLogRecord(r io.Reader) (*LogRecord, error) {
	var hdr [LogHeaderSize]byte
	if _, err := io.ReadFull(r, hdr[:]); err != nil {
		if err == io.EOF {
			return nil, io.EOF
		}
		if errors.Is(err, io.ErrUnexpectedEOF) {
			return nil, ErrTruncatedRecord
		}
		return nil, err
	}

	if hdr[0] != LogMagicBytes[0] || hdr[1] != LogMagicBytes[1] || hdr[2] != LogMagicBytes[2] || hdr[3] != LogMagicBytes[3] {
		return nil, ErrCorruptedRecord
	}

	position := binary.BigEndian.Uint64(hdr[4:12])
	var streamID uuid.UUID
	copy(streamID[:], hdr[12:28])
	streamSeq := binary.BigEndian.Uint64(hdr[28:36])
	length := binary.BigEndian.Uint32(hdr[36:40])
	expectedCRC := binary.BigEndian.Uint32(hdr[40:44])

	payload := make([]byte, length)
	if length > 0 {
		if _, err := io.ReadFull(r, payload); err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
				return nil, ErrTruncatedRecord
			}
			return nil, err
		}
	}

	rec := &LogRecord{
		Position:  position,
		StreamID:  streamID,
		StreamSeq: streamSeq,
		Payload:   payload,
		CRC32C:    expectedCRC,
	}

	if rec.ComputeCRC32C() != expectedCRC {
		return nil, ErrChecksumMismatch
	}

	return rec, nil
}
