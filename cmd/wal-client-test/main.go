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

package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"time"

	pb "github.com/gke-labs/in-cluster-storage/pkg/api/wal/v1alpha1"
	"github.com/gke-labs/in-cluster-storage/pkg/wal/client"
	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintf(os.Stderr, "%v\n", err)
		os.Exit(1)
	}
}

func run(args []string) error {
	if len(args) < 1 {
		return fmt.Errorf("usage: wal-client-test <append|tail> [flags]")
	}

	cmd := args[0]
	switch cmd {
	case "append":
		return runAppend(args[1:])
	case "tail":
		return runTail(args[1:])
	default:
		return fmt.Errorf("unknown command: %s", cmd)
	}
}

func runAppend(args []string) error {
	fs := flag.NewFlagSet("append", flag.ContinueOnError)
	dir := fs.String("dir", "/data/wal", "Local WAL directory")
	streamIDStr := fs.String("stream-id", "", "Stream UUID")
	target := fs.String("target", "wal-buffer:50051", "WAL buffer service target")
	count := fs.Int("count", 10, "Number of records to append")
	waitLevelStr := fs.String("wait-level", "witness", "Durability level to wait for (local, witness, permanent)")
	doFlush := fs.Bool("flush", false, "Whether to call Flush to permanent storage at the end")
	holdOpen := fs.Duration("hold-open", 0, "Duration to hold stream open before exiting")
	if err := fs.Parse(args); err != nil {
		return err
	}

	var streamID uuid.UUID
	var err error
	if *streamIDStr != "" {
		streamID, err = uuid.Parse(*streamIDStr)
		if err != nil {
			return fmt.Errorf("invalid stream-id %q: %w", *streamIDStr, err)
		}
	} else {
		streamID = uuid.New()
	}

	var waitLevel client.Level
	var requestFlush bool
	switch *waitLevelStr {
	case "local":
		waitLevel = client.Local
	case "witness":
		waitLevel = client.Witness
	case "permanent", "s3":
		waitLevel = client.Permanent
		requestFlush = true
	default:
		return fmt.Errorf("invalid wait-level %q", *waitLevelStr)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	stream, err := client.Open(ctx, *dir, streamID, *target)
	if err != nil {
		return fmt.Errorf("failed to open stream: %w", err)
	}
	defer stream.Close()

	for i := 1; i <= *count; i++ {
		payload := []byte(fmt.Sprintf("wal-record-%s-%d", streamID.String(), i))
		seq, err := stream.Append(ctx, payload)
		if err != nil {
			return fmt.Errorf("append error on record %d: %w", i, err)
		}

		if err := stream.Wait(ctx, seq, waitLevel, requestFlush); err != nil {
			return fmt.Errorf("wait error on record %d: %w", i, err)
		}
	}

	if *doFlush {
		if err := stream.Flush(ctx); err != nil {
			return fmt.Errorf("flush error: %w", err)
		}
	}

	if *holdOpen > 0 {
		fmt.Printf("Holding stream open for %v...\n", *holdOpen)
		time.Sleep(*holdOpen)
	}

	local, witness, permanent := stream.Watermarks()
	fmt.Printf("SUCCESS stream_id=%s count=%d local=%d witness=%d permanent=%d\n", streamID.String(), *count, local, witness, permanent)
	return nil
}

func runTail(args []string) error {
	fs := flag.NewFlagSet("tail", flag.ContinueOnError)
	target := fs.String("target", "wal-buffer:50051", "WAL buffer service target")
	fromPos := fs.Uint64("from-pos", 1, "Starting position")
	streamIDStr := fs.String("stream-id", "", "Optional stream UUID to filter")
	fromSeq := fs.Uint64("from-seq", 0, "Optional stream_seq lower bound (with stream-id)")
	count := fs.Int("count", 10, "Number of records to read")
	if err := fs.Parse(args); err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	var streamIDBytes []byte
	if *streamIDStr != "" {
		sid, err := uuid.Parse(*streamIDStr)
		if err != nil {
			return fmt.Errorf("invalid stream-id %q: %w", *streamIDStr, err)
		}
		streamIDBytes = sid[:]
	}

	conn, err := grpc.NewClient(*target, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return fmt.Errorf("failed to dial target %s: %w", *target, err)
	}
	defer conn.Close()

	walClient := pb.NewWalBufferClient(conn)
	tailStream, err := walClient.Tail(ctx, &pb.TailRequest{
		FromPosition:  *fromPos,
		StreamId:      streamIDBytes,
		FromStreamSeq: *fromSeq,
	})
	if err != nil {
		return fmt.Errorf("tail RPC error: %w", err)
	}

	var lastPos uint64 = 0
	var lastSeq uint64 = 0
	for i := 1; i <= *count; i++ {
		resp, err := tailStream.Recv()
		if err != nil {
			return fmt.Errorf("tail recv error on record %d: %w", i, err)
		}
		rec := resp.GetRecord()
		if rec == nil {
			continue
		}
		if rec.Position <= lastPos {
			return fmt.Errorf("invalid position order: prev=%d, curr=%d", lastPos, rec.Position)
		}
		if len(streamIDBytes) > 0 && rec.StreamSeq <= lastSeq {
			return fmt.Errorf("invalid stream_seq order: prev=%d, curr=%d", lastSeq, rec.StreamSeq)
		}
		lastPos = rec.Position
		lastSeq = rec.StreamSeq
	}

	fmt.Printf("SUCCESS tailed=%d last_position=%d last_seq=%d\n", *count, lastPos, lastSeq)
	return nil
}
