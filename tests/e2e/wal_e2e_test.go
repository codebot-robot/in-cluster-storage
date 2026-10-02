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

package e2e

import (
	"bufio"
	"context"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	pb "github.com/gke-labs/in-cluster-storage/pkg/api/wal/v1alpha1"
	"github.com/gke-labs/in-cluster-storage/pkg/objectstore"
	"github.com/gke-labs/in-cluster-storage/pkg/objectstore/inmemorystorage"
	"github.com/gke-labs/in-cluster-storage/pkg/objectstore/s3storage"
	"github.com/gke-labs/in-cluster-storage/pkg/wal/buffer"
	"github.com/gke-labs/in-cluster-storage/pkg/wal/client"
	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func startBufferServerOnListener(t *testing.T, backend objectstore.Backend, dataDir string, ln net.Listener) (*buffer.Server, *grpc.Server) {
	t.Helper()
	ctx := t.Context()
	srv, err := buffer.NewServer(ctx, buffer.ServerConfig{
		Backend:       backend,
		DataDir:       dataDir,
		FlushInterval: 10 * time.Second,
		BatchMaxDelay: 2 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("failed to create WAL buffer server: %v", err)
	}

	grpcServer := grpc.NewServer()
	pb.RegisterWalBufferServer(grpcServer, srv)

	go func() {
		_ = grpcServer.Serve(ln)
	}()

	return srv, grpcServer
}

func startFakeS3(t *testing.T, bucket string) string {
	t.Helper()

	goBin, err := exec.LookPath("go")
	if err != nil {
		t.Skip("go not found on PATH; cannot build fakes3 for WAL S3 test")
	}

	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("runtime.Caller failed")
	}
	fakes3Dir := filepath.Join(filepath.Dir(thisFile), "..", "..", "fakes3")
	if _, err := os.Stat(filepath.Join(fakes3Dir, "go.mod")); err != nil {
		t.Fatalf("fakes3 module not found at %s: %v", fakes3Dir, err)
	}

	bin := filepath.Join(t.TempDir(), "fakes3")
	build := exec.Command(goBin, "build", "-o", bin, "./cmd/fakes3")
	build.Dir = fakes3Dir
	if out, err := build.CombinedOutput(); err != nil {
		t.Fatalf("building fakes3: %v\n%s", err, out)
	}

	cmd := exec.Command(bin, "--listen=127.0.0.1:0", "--buckets="+bucket, "--quiet")
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatalf("stdout pipe: %v", err)
	}
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		t.Fatalf("starting fakes3: %v", err)
	}
	t.Cleanup(func() {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
	})

	addrCh := make(chan string, 1)
	go func() {
		scanner := bufio.NewScanner(stdout)
		for scanner.Scan() {
			line := scanner.Text()
			if _, addr, found := strings.Cut(line, "listening on "); found {
				addrCh <- strings.TrimSpace(addr)
				return
			}
		}
		addrCh <- ""
	}()
	var endpoint string
	select {
	case endpoint = <-addrCh:
	case <-time.After(30 * time.Second):
		t.Fatal("timed out waiting for fakes3 to report its address")
	}
	if endpoint == "" {
		t.Fatal("fakes3 exited before reporting its address")
	}

	if os.Getenv("AWS_ACCESS_KEY_ID") == "" {
		t.Setenv("AWS_ACCESS_KEY_ID", "fakes3")
		t.Setenv("AWS_SECRET_ACCESS_KEY", "fakes3")
	}
	return endpoint
}

func TestWALE2E(t *testing.T) {
	if os.Getenv("RUN_E2E") == "" {
		t.Skip("Skipping WAL E2E test; RUN_E2E not set")
	}

	backend := inmemorystorage.New()
	dataDir := t.TempDir()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to listen: %v", err)
	}
	addr := ln.Addr().String()

	srv1, grpcServer1 := startBufferServerOnListener(t, backend, dataDir, ln)

	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()

	// Step 1: Open stream 1 and append 10 records with witness ack
	clientDir1 := t.TempDir()
	streamID1 := uuid.New()
	stream1, err := client.Open(ctx, clientDir1, streamID1, addr)
	if err != nil {
		t.Fatalf("open stream 1 failed: %v", err)
	}
	defer stream1.Close()

	for i := 1; i <= 10; i++ {
		payload := []byte(fmt.Sprintf("wal-record-%s-%d", streamID1.String(), i))
		seq, err := stream1.Append(ctx, payload)
		if err != nil {
			t.Fatalf("append %d failed: %v", i, err)
		}
		if err := stream1.Wait(ctx, seq, client.Witness, false); err != nil {
			t.Fatalf("wait %d failed: %v", i, err)
		}
	}

	// Step 2: Stop buffer server abruptly to test restart resilience
	t.Logf("Stopping WAL buffer server (simulating pod crash/restart)")
	grpcServer1.Stop()
	_ = srv1.Close()
	_ = ln.Close()

	// Rebind to the exact same TCP address
	ln2, err := net.Listen("tcp", addr)
	if err != nil {
		t.Fatalf("failed to re-listen on %s: %v", addr, err)
	}
	srv2, grpcServer2 := startBufferServerOnListener(t, backend, dataDir, ln2)
	defer func() {
		grpcServer2.Stop()
		_ = srv2.Close()
		_ = ln2.Close()
	}()

	// Wait for stream1 to reconnect and confirm durability after restart
	if err := stream1.Wait(ctx, 10, client.Witness, false); err != nil {
		t.Fatalf("failed waiting for stream 1 after buffer restart: %v", err)
	}

	// Step 3: Open stream 2 and append 10 records with permanent ack and flush
	clientDir2 := t.TempDir()
	streamID2 := uuid.New()
	stream2, err := client.Open(ctx, clientDir2, streamID2, addr)
	if err != nil {
		t.Fatalf("open stream 2 failed: %v", err)
	}
	defer stream2.Close()

	for i := 1; i <= 10; i++ {
		payload := []byte(fmt.Sprintf("wal-record-%s-%d", streamID2.String(), i))
		seq, err := stream2.Append(ctx, payload)
		if err != nil {
			t.Fatalf("append stream 2 record %d failed: %v", i, err)
		}
		if err := stream2.Wait(ctx, seq, client.Permanent, true); err != nil {
			t.Fatalf("wait stream 2 record %d failed: %v", i, err)
		}
	}

	if err := stream2.Flush(ctx); err != nil {
		t.Fatalf("flush stream 2 failed: %v", err)
	}

	// Step 4: Verify merged tail of all 20 records
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial %s failed: %v", addr, err)
	}
	defer conn.Close()

	walClient := pb.NewWalBufferClient(conn)
	tailStream, err := walClient.Tail(ctx, &pb.TailRequest{FromPosition: 1})
	if err != nil {
		t.Fatalf("tail RPC failed: %v", err)
	}

	var lastPos uint64
	for i := 1; i <= 20; i++ {
		resp, err := tailStream.Recv()
		if err != nil {
			t.Fatalf("tail recv record %d failed: %v", i, err)
		}
		rec := resp.GetRecord()
		if rec == nil {
			t.Fatalf("nil record in tail response %d", i)
		}
		if rec.Position <= lastPos {
			t.Fatalf("position out of order: prev=%d, curr=%d", lastPos, rec.Position)
		}
		lastPos = rec.Position
	}

	// Step 5: Verify per-stream tail for stream 1 starting from from_stream_seq=5 (expect seqs 6..10)
	stream1Iter, err := client.TailStream(ctx, addr, streamID1, 5)
	if err != nil {
		t.Fatalf("TailStream failed: %v", err)
	}

	var receivedSeqs []uint64
	for seq, payload := range stream1Iter {
		receivedSeqs = append(receivedSeqs, seq)
		expectedPayload := fmt.Sprintf("wal-record-%s-%d", streamID1.String(), seq)
		if string(payload) != expectedPayload {
			t.Errorf("expected payload %s, got %s", expectedPayload, string(payload))
		}
		if seq == 10 {
			break
		}
	}

	expectedSeqs := []uint64{6, 7, 8, 9, 10}
	if len(receivedSeqs) != len(expectedSeqs) {
		t.Fatalf("expected seqs %v, got %v", expectedSeqs, receivedSeqs)
	}
	for i, seq := range receivedSeqs {
		if seq != expectedSeqs[i] {
			t.Errorf("index %d: expected seq %d, got %d", i, expectedSeqs[i], seq)
		}
	}

	t.Logf("Successfully verified WAL E2E with in-process components, buffer restart, replay, and per-stream tail!")
}

func TestWALS3E2E(t *testing.T) {
	if os.Getenv("RUN_E2E") == "" {
		t.Skip("Skipping WAL S3 E2E test; RUN_E2E not set")
	}

	endpoint := startFakeS3(t, "wal-bucket")

	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()

	s3Backend, err := s3storage.New(ctx, s3storage.Config{
		Bucket:       "wal-bucket",
		Endpoint:     endpoint,
		UsePathStyle: true,
		Region:       "us-east-1",
	})
	if err != nil {
		t.Fatalf("failed to create s3 storage backend: %v", err)
	}

	dataDir := t.TempDir()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to listen: %v", err)
	}
	addr := ln.Addr().String()

	srv1, grpcServer1 := startBufferServerOnListener(t, s3Backend, dataDir, ln)

	// Step 1: Open stream 1 and append 10 records
	clientDir1 := t.TempDir()
	streamID1 := uuid.New()
	stream1, err := client.Open(ctx, clientDir1, streamID1, addr)
	if err != nil {
		t.Fatalf("open stream 1 failed: %v", err)
	}
	defer stream1.Close()

	for i := 1; i <= 10; i++ {
		payload := []byte(fmt.Sprintf("wal-record-%s-%d", streamID1.String(), i))
		seq, err := stream1.Append(ctx, payload)
		if err != nil {
			t.Fatalf("append %d failed: %v", i, err)
		}
		if err := stream1.Wait(ctx, seq, client.Witness, false); err != nil {
			t.Fatalf("wait %d failed: %v", i, err)
		}
	}

	// Step 2: Stop buffer server abruptly and restart pointing to same S3 backend and dataDir
	t.Logf("Stopping WAL buffer server with S3 backend")
	grpcServer1.Stop()
	_ = srv1.Close()
	_ = ln.Close()

	ln2, err := net.Listen("tcp", addr)
	if err != nil {
		t.Fatalf("failed to re-listen on %s: %v", addr, err)
	}
	srv2, grpcServer2 := startBufferServerOnListener(t, s3Backend, dataDir, ln2)
	defer func() {
		grpcServer2.Stop()
		_ = srv2.Close()
		_ = ln2.Close()
	}()

	if err := stream1.Wait(ctx, 10, client.Witness, false); err != nil {
		t.Fatalf("failed waiting for stream 1 after S3 buffer restart: %v", err)
	}

	// Step 3: Open stream 2 and append 10 records with permanent flush
	clientDir2 := t.TempDir()
	streamID2 := uuid.New()
	stream2, err := client.Open(ctx, clientDir2, streamID2, addr)
	if err != nil {
		t.Fatalf("open stream 2 failed: %v", err)
	}
	defer stream2.Close()

	for i := 1; i <= 10; i++ {
		payload := []byte(fmt.Sprintf("wal-record-%s-%d", streamID2.String(), i))
		seq, err := stream2.Append(ctx, payload)
		if err != nil {
			t.Fatalf("append stream 2 record %d failed: %v", i, err)
		}
		if err := stream2.Wait(ctx, seq, client.Permanent, true); err != nil {
			t.Fatalf("wait stream 2 record %d failed: %v", i, err)
		}
	}

	if err := stream2.Flush(ctx); err != nil {
		t.Fatalf("flush stream 2 failed: %v", err)
	}

	// Step 4: Verify merged tail
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial %s failed: %v", addr, err)
	}
	defer conn.Close()

	walClient := pb.NewWalBufferClient(conn)
	tailStream, err := walClient.Tail(ctx, &pb.TailRequest{FromPosition: 1})
	if err != nil {
		t.Fatalf("tail RPC failed: %v", err)
	}

	var lastPos uint64
	for i := 1; i <= 20; i++ {
		resp, err := tailStream.Recv()
		if err != nil {
			t.Fatalf("tail recv record %d failed: %v", i, err)
		}
		rec := resp.GetRecord()
		if rec == nil {
			t.Fatalf("nil record in tail response %d", i)
		}
		if rec.Position <= lastPos {
			t.Fatalf("position out of order: prev=%d, curr=%d", lastPos, rec.Position)
		}
		lastPos = rec.Position
	}

	// Step 5: Verify per-stream tail for stream 1 and stream 2
	stream1Iter, err := client.TailStream(ctx, addr, streamID1, 5)
	if err != nil {
		t.Fatalf("TailStream 1 failed: %v", err)
	}
	var s1Seqs []uint64
	for seq := range stream1Iter {
		s1Seqs = append(s1Seqs, seq)
		if seq == 10 {
			break
		}
	}
	if len(s1Seqs) != 5 {
		t.Fatalf("expected 5 records for stream 1 tail, got %v", s1Seqs)
	}

	stream2Iter, err := client.TailStream(ctx, addr, streamID2, 0)
	if err != nil {
		t.Fatalf("TailStream 2 failed: %v", err)
	}
	var s2Seqs []uint64
	for seq := range stream2Iter {
		s2Seqs = append(s2Seqs, seq)
		if seq == 10 {
			break
		}
	}
	if len(s2Seqs) != 10 {
		t.Fatalf("expected 10 records for stream 2 tail, got %v", s2Seqs)
	}

	t.Logf("Successfully verified WAL E2E with in-process S3 (fakes3) backend!")
}
