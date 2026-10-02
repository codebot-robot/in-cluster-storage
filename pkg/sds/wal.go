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

package sds

import (
	"context"
	"errors"
	"iter"

	"github.com/gke-labs/in-cluster-storage/pkg/sds/record"
	"github.com/gke-labs/in-cluster-storage/pkg/wal"
	walclient "github.com/gke-labs/in-cluster-storage/pkg/wal/client"
	"github.com/google/uuid"
)

// WALAppender implements record.Appender on top of walclient.Stream
// with a caller-selected durability level.
type WALAppender struct {
	stream walclient.Stream
	level  walclient.Level
}

// NewWALAppender creates a new WALAppender wrapping a walclient.Stream.
func NewWALAppender(stream walclient.Stream, level walclient.Level) *WALAppender {
	return &WALAppender{
		stream: stream,
		level:  level,
	}
}

// Stream returns the underlying walclient.Stream.
func (a *WALAppender) Stream() walclient.Stream {
	return a.stream
}

// Durability returns the configured durability level.
func (a *WALAppender) Durability() walclient.Level {
	return a.level
}

// Append writes payload to the underlying WAL stream and waits for the configured durability level.
func (a *WALAppender) Append(ctx context.Context, payload []byte) (uint64, error) {
	if a.stream == nil {
		return 0, errors.New("nil WAL stream")
	}
	seq, err := a.stream.Append(ctx, payload)
	if err != nil {
		return 0, err
	}
	if a.level == walclient.Local {
		return seq, nil
	}
	requestFlush := (a.level == walclient.Permanent)
	if err := a.stream.Wait(ctx, seq, a.level, requestFlush); err != nil {
		return 0, err
	}
	return seq, nil
}

// StreamReader reads a single structured data stream, processing locally recovered
// records from a writer restart and tailing remote records from a WAL buffer service
// via walclient.TailStream, feeding them into sds.ChangeReader.
type StreamReader struct {
	changeReader *ChangeReader
	recovered    []*wal.ClientRecord
	target       string
	streamID     uuid.UUID
	tailOpts     []walclient.Option
	lastErr      error
}

// StreamReaderOption configures a StreamReader.
type StreamReaderOption func(*StreamReader)

// WithChangeReader sets a custom ChangeReader.
func WithChangeReader(cr *ChangeReader) StreamReaderOption {
	return func(r *StreamReader) {
		r.changeReader = cr
	}
}

// WithRecoveredRecords supplies locally recovered records (e.g. from stream.RecoveredRecords()).
func WithRecoveredRecords(records []*wal.ClientRecord) StreamReaderOption {
	return func(r *StreamReader) {
		r.recovered = records
	}
}

// WithTailOptions passes options (e.g. WithGRPCClient, WithDialOptions) to walclient.TailStream.
func WithTailOptions(opts ...walclient.Option) StreamReaderOption {
	return func(r *StreamReader) {
		r.tailOpts = append(r.tailOpts, opts...)
	}
}

// NewStreamReader creates a new StreamReader.
func NewStreamReader(target string, streamID uuid.UUID, opts ...StreamReaderOption) *StreamReader {
	r := &StreamReader{
		target:   target,
		streamID: streamID,
	}
	for _, opt := range opts {
		opt(r)
	}
	if r.changeReader == nil {
		r.changeReader = NewChangeReader()
	}
	return r
}

// ChangeReader returns the underlying ChangeReader.
func (r *StreamReader) ChangeReader() *ChangeReader {
	return r.changeReader
}

// Registry returns the in-band type registry.
func (r *StreamReader) Registry() *record.Registry {
	return r.changeReader.Registry()
}

// Decoder returns the underlying record.Decoder.
func (r *StreamReader) Decoder() *record.Decoder {
	return r.changeReader.Decoder()
}

// Err returns the last error encountered during reading/tailing, if any.
func (r *StreamReader) Err() error {
	return r.lastErr
}

// FeedRecovered feeds all locally recovered records with StreamSeq > fromSeq into ChangeReader
// and returns any committed changes.
func (r *StreamReader) FeedRecovered(fromSeq uint64) ([]Change, error) {
	var allChanges []Change
	for _, rec := range r.recovered {
		if rec.StreamSeq <= fromSeq {
			continue
		}
		changes, err := r.changeReader.Feed(rec.StreamSeq, rec.Payload)
		if err != nil {
			r.lastErr = err
			return nil, err
		}
		allChanges = append(allChanges, changes...)
	}
	return allChanges, nil
}

// Read returns an iterator yielding (stream_seq, []Change) for each sequence number that committed changes.
// It first replays any locally recovered records where StreamSeq > fromSeq, then tails remote records
// from target starting after the highest seen sequence number.
func (r *StreamReader) Read(ctx context.Context, fromSeq uint64) (iter.Seq2[uint64, []Change], error) {
	iterator := func(yield func(uint64, []Change) bool) {
		r.lastErr = nil
		currentSeq := fromSeq

		// 1. Process locally recovered records
		for _, rec := range r.recovered {
			if rec.StreamSeq <= currentSeq {
				continue
			}
			changes, err := r.changeReader.Feed(rec.StreamSeq, rec.Payload)
			if err != nil {
				r.lastErr = err
				return
			}
			if rec.StreamSeq > currentSeq {
				currentSeq = rec.StreamSeq
			}
			if len(changes) > 0 {
				if !yield(rec.StreamSeq, changes) {
					return
				}
			}
		}

		// 2. Tail from remote buffer service if target or grpcClient is configured
		if r.target != "" || len(r.tailOpts) > 0 {
			tailIter, err := walclient.TailStream(ctx, r.target, r.streamID, currentSeq, r.tailOpts...)
			if err != nil {
				r.lastErr = err
				return
			}
			for seq, payload := range tailIter {
				changes, err := r.changeReader.Feed(seq, payload)
				if err != nil {
					r.lastErr = err
					return
				}
				if seq > currentSeq {
					currentSeq = seq
				}
				if len(changes) > 0 {
					if !yield(seq, changes) {
						return
					}
				}
			}
		}
	}

	return iterator, nil
}

// TailStreamReader creates a StreamReader and returns an iterator over committed changes.
func TailStreamReader(ctx context.Context, target string, streamID uuid.UUID, fromSeq uint64, opts ...StreamReaderOption) (iter.Seq2[uint64, []Change], error) {
	reader := NewStreamReader(target, streamID, opts...)
	return reader.Read(ctx, fromSeq)
}
