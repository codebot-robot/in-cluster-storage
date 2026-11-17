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
	"testing"
	"time"
)

// TestRunnable verifies that Runnable starts once, errors on duplicate start or start after stop,
// and that Stop cancels the context and waits for the function to return.
// Requires no locks held.
func TestRunnable(t *testing.T) {
	var r Runnable
	ctx := t.Context()

	entered := make(chan struct{})
	release := make(chan struct{})
	done := make(chan struct{})

	err := r.Start(ctx, func(runCtx context.Context) {
		close(entered)
		select {
		case <-runCtx.Done():
		case <-release:
		}
		close(done)
	})
	if err != nil {
		t.Fatalf("first Start failed: %v", err)
	}

	// Starting a second time must return an error.
	if err := r.Start(ctx, func(context.Context) {}); err == nil {
		t.Fatal("expected second Start to return error, got nil")
	}

	// Wait for fn to enter.
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for Runnable fn to enter")
	}

	// Stop must cancel runCtx and wait for fn to return.
	r.Stop()

	select {
	case <-done:
	default:
		t.Fatal("expected Stop to wait for fn to return before completing")
	}

	// Starting after Stop must return an error.
	if err := r.Start(ctx, func(context.Context) {}); err == nil {
		t.Fatal("expected Start after Stop to return error, got nil")
	}

	// A Runnable stopped before ever being started must refuse Start and never run the function.
	var freshRunnable Runnable
	freshRunnable.Stop()
	freshFnRan := false
	if err := freshRunnable.Start(ctx, func(context.Context) {
		freshFnRan = true
	}); err == nil {
		t.Fatal("expected Start after Stop on never-started Runnable to return error, got nil")
	}
	if freshFnRan {
		t.Fatal("expected function to never run on stopped Runnable")
	}
}
