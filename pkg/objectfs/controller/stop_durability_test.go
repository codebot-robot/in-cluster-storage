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

package controller

import (
	"context"
	"errors"
	"testing"
	"time"

	pb "github.com/gke-labs/in-cluster-storage/pkg/api/objectfs/v1alpha1"
	walclient "github.com/gke-labs/in-cluster-storage/pkg/wal/client"
)

// stopTestStream embeds recordingStream and provides optional waitGate and waitEntered
// channels to coordinate durability barrier tests. waitEntered notifies the test that
// stream.Wait was entered, and waitGate blocks Wait until closed by the test.
type stopTestStream struct {
	recordingStream
	waitGate    chan struct{}
	waitEntered chan struct{}
}

// Wait records the wait call, signals waitEntered if configured, and blocks on waitGate until released or cancelled.
func (s *stopTestStream) Wait(ctx context.Context, sequence uint64, level walclient.Level, requestFlush bool) error {
	if err := s.recordingStream.Wait(ctx, sequence, level, requestFlush); err != nil {
		return err
	}
	if s.waitEntered != nil {
		select {
		case s.waitEntered <- struct{}{}:
		default:
		}
	}
	if s.waitGate != nil {
		select {
		case <-s.waitGate:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return nil
}

// setupStopDurabilityTest creates a server and volume with a configured stream and an initial file.
// Registers server cleanup on t. Takes no locks.
func setupStopDurabilityTest(t *testing.T, ctx context.Context, stream *stopTestStream) *Volume {
	server := NewTestServer(t, NewMemoryBackend(),
		WithServerStreamFactory(func(volumeID string) (walclient.Stream, error) {
			return stream, nil
		}),
		WithServerDurability(walclient.Witness),
	)
	t.Cleanup(func() {
		if err := server.Close(); err != nil {
			t.Errorf("server.Close failed: %v", err)
		}
	})
	createResponse, err := server.CreateFile(ctx, &pb.CreateFileRequest{
		VolumeId:    "test-stop-volume",
		ParentInode: 1,
		Name:        "file.txt",
		Mode:        0644,
	})
	if err != nil || createResponse.GetError() != 0 {
		t.Fatalf("CreateFile failed: %v", err)
	}
	volume, err := server.getOrCreateVolume("test-stop-volume")
	if err != nil {
		t.Fatalf("getOrCreateVolume failed: %v", err)
	}
	return volume
}

// runAsync executes action in a separate goroutine and returns a channel delivering its error result.
func runAsync(action func() error) <-chan error {
	resultChannel := make(chan error, 1)
	go func() {
		resultChannel <- action()
	}()
	return resultChannel
}

// awaitResult waits for a result channel or fails the test if description does not complete within the timeout.
func awaitResult(t *testing.T, resultChannel <-chan error, description string) error {
	t.Helper()
	select {
	case err := <-resultChannel:
		return err
	case <-time.After(5 * time.Second):
		t.Fatalf("timed out waiting for %s", description)
		return nil
	}
}

// TestVolumeStop_WaitsForMetadataDurability verifies that Volume.Stop waits for the newest
// commit sequence to reach its durability level, returning only after the wait completes.
func TestVolumeStop_WaitsForMetadataDurability(t *testing.T) {
	ctx := t.Context()
	stream := &stopTestStream{}
	volume := setupStopDurabilityTest(t, ctx, stream)

	expectedPosition := volume.Position()
	stream.waitCount.Store(0)
	stream.lastWaitSequence.Store(0)
	stream.waitEntered = make(chan struct{}, 1)
	stream.waitGate = make(chan struct{})

	stopChannel := runAsync(func() error { return volume.Stop(ctx) })

	select {
	case <-stream.waitEntered:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for Volume.Stop to enter stream.Wait")
	}

	select {
	case earlyError := <-stopChannel:
		t.Fatalf("Volume.Stop returned before wait gate was released: %v", earlyError)
	default:
	}

	close(stream.waitGate)
	if err := awaitResult(t, stopChannel, "Volume.Stop after gate release"); err != nil {
		t.Fatalf("Volume.Stop failed: %v", err)
	}

	if waits := stream.waitCount.Load(); waits != 1 {
		t.Fatalf("expected 1 durability wait from Volume.Stop, got %d", waits)
	}
	if lastWait := stream.lastWaitSequence.Load(); lastWait != expectedPosition {
		t.Fatalf("expected lastWait = %d, got %d", expectedPosition, lastWait)
	}
}

// TestVolumeStop_TimeoutAgainstUnacknowledgedStream verifies that Volume.Stop with a short deadline
// against a stream that never acknowledges returns an error wrapping context.DeadlineExceeded,
// and that a subsequent Stop call can wait again once the stream becomes responsive.
func TestVolumeStop_TimeoutAgainstUnacknowledgedStream(t *testing.T) {
	ctx := t.Context()
	stream := &stopTestStream{}
	volume := setupStopDurabilityTest(t, ctx, stream)

	expectedPosition := volume.Position()
	stream.waitCount.Store(0)
	stream.lastWaitSequence.Store(0)
	stream.waitGate = make(chan struct{})

	stopContext, stopCancel := context.WithTimeout(ctx, 50*time.Millisecond)
	defer stopCancel()

	stopError := volume.Stop(stopContext)
	if stopError == nil {
		t.Fatal("expected Volume.Stop to return error on unacknowledged stream, got nil")
	}
	if !errors.Is(stopError, context.DeadlineExceeded) {
		t.Fatalf("expected error wrapping context.DeadlineExceeded, got: %v", stopError)
	}
	if waits := stream.waitCount.Load(); waits != 1 {
		t.Fatalf("expected 1 durability wait from initial Volume.Stop, got %d", waits)
	}
	if lastWait := stream.lastWaitSequence.Load(); lastWait != expectedPosition {
		t.Fatalf("expected lastWait = %d, got %d", expectedPosition, lastWait)
	}

	// Verify retryability: unblock the gate and ensure a subsequent Stop call succeeds and waits again.
	close(stream.waitGate)
	if retryError := volume.Stop(ctx); retryError != nil {
		t.Fatalf("subsequent Volume.Stop failed: %v", retryError)
	}
	if waits := stream.waitCount.Load(); waits != 2 {
		t.Fatalf("expected 2 durability waits after retried Volume.Stop, got %d", waits)
	}
}

// TestVolumeClose_WithoutStopReturnsPromptly verifies that Volume.Close without Volume.Stop
// returns promptly even when stream.Wait would never acknowledge.
func TestVolumeClose_WithoutStopReturnsPromptly(t *testing.T) {
	ctx := t.Context()
	stream := &stopTestStream{}
	volume := setupStopDurabilityTest(t, ctx, stream)

	stream.waitCount.Store(0)
	stream.waitGate = make(chan struct{})

	closeChannel := runAsync(volume.Close)
	if err := awaitResult(t, closeChannel, "Volume.Close without Stop"); err != nil {
		t.Fatalf("Volume.Close failed: %v", err)
	}

	if waits := stream.waitCount.Load(); waits != 0 {
		t.Fatalf("expected 0 durability waits from Volume.Close, got %d", waits)
	}
}

// TestVolumeStop_AlreadyClosedStreamRecognisedBySentinel verifies that Volume.Stop
// against an already-closed stream returns an error wrapping walclient.ErrClosed,
// recognised by typed sentinel rather than message text.
func TestVolumeStop_AlreadyClosedStreamRecognisedBySentinel(t *testing.T) {
	ctx := t.Context()
	stream := &stopTestStream{}
	volume := setupStopDurabilityTest(t, ctx, stream)

	stream.setWaitError(walclient.ErrClosed)

	stopError := volume.Stop(ctx)
	if stopError == nil {
		t.Fatal("expected Volume.Stop on closed stream to return error, got nil")
	}
	if !errors.Is(stopError, walclient.ErrClosed) {
		t.Fatalf("expected error wrapping walclient.ErrClosed sentinel, got: %v", stopError)
	}
}
