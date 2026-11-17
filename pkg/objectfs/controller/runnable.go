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
	"sync"
)

// Runnable manages the lifecycle of a single background task: start once with a context
// and function, error on duplicate start or start after stop, and stop cancels and waits.
type Runnable struct {
	// mu coordinates lifecycle transitions and guards started, closed, and cancel.
	mu sync.Mutex
	// started records whether Start has been called to prevent duplicate runs. Guarded by mu.
	started bool
	// closed records whether Stop has been called. Guarded by mu.
	closed bool
	// cancel cancels the running function's context when Stop is called. Guarded by mu.
	cancel context.CancelFunc
	// wg coordinates waiting for the background function to exit during Stop. Requires no lock.
	wg sync.WaitGroup
}

// Start starts fn in a background goroutine with a context derived from ctx.
// Returns an error if called more than once or after Stop.
// Requires no locks held.
func (r *Runnable) Start(ctx context.Context, fn func(ctx context.Context)) error {
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return errors.New("already closed")
	}
	if r.started {
		r.mu.Unlock()
		return errors.New("already started")
	}
	r.started = true
	runCtx, runCancel := context.WithCancel(ctx)
	r.cancel = runCancel
	r.wg.Add(1)
	r.mu.Unlock()

	go func() {
		defer r.wg.Done()
		fn(runCtx)
	}()
	return nil
}

// Stop cancels the running function's context and waits for its goroutine to exit.
// Requires no locks held.
func (r *Runnable) Stop() {
	r.mu.Lock()
	r.closed = true
	if r.cancel != nil {
		r.cancel()
	}
	r.mu.Unlock()
	r.wg.Wait()
}
