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

package buffertest

import (
	"testing"
	"time"
)

func TestBufferTest_StartCloseNoRemainingGoroutines(t *testing.T) {
	ctx := t.Context()
	bt, cleanup := Start(ctx, t, WithFlushInterval(10*time.Millisecond))
	if loops := bt.Server.ActiveLoops(); loops != 2 {
		t.Fatalf("expected 2 active loops running, got %d", loops)
	}
	cleanup()
	if loops := bt.Server.ActiveLoops(); loops != 0 {
		t.Fatalf("expected 0 active loops after buffertest cleanup, got %d", loops)
	}
}
