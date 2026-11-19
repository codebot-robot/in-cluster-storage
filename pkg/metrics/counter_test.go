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

package metrics

import (
	"sync"
	"testing"
)

func TestCounter_Basic(t *testing.T) {
	c := NewCounter("test_counter")
	if got := c.Name(); got != "test_counter" {
		t.Errorf("Name() = %q, want %q", got, "test_counter")
	}
	if got := c.Value(); got != 0 {
		t.Errorf("initial Value() = %d, want 0", got)
	}

	c.Inc()
	if got := c.Value(); got != 1 {
		t.Errorf("after Inc(), Value() = %d, want 1", got)
	}

	c.Inc()
	if got := c.Value(); got != 2 {
		t.Errorf("after second Inc(), Value() = %d, want 2", got)
	}
}

func TestCounter_Concurrent(t *testing.T) {
	c := NewCounter("concurrent_counter")
	const (
		goroutines = 50
		increments = 1000
	)

	var wg sync.WaitGroup
	wg.Add(goroutines)
	for i := 0; i < goroutines; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < increments; j++ {
				c.Inc()
			}
		}()
	}
	wg.Wait()

	want := int64(goroutines * increments)
	if got := c.Value(); got != want {
		t.Fatalf("Value() = %d, want %d", got, want)
	}
}
