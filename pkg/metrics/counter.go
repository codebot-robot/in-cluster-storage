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

// Package metrics provides minimal primitives for collecting application metrics.
package metrics

import "sync/atomic"

// Counter is a named, monotonically increasing 64-bit integer counter
// safe for concurrent use.
type Counter struct {
	// name is the identifier of the counter.
	name string

	// value holds the current counter value.
	value atomic.Int64
}

// NewCounter creates a new named Counter with an initial value of zero.
func NewCounter(name string) *Counter {
	return &Counter{
		name: name,
	}
}

// Name returns the counter's name.
func (c *Counter) Name() string {
	if c == nil {
		return ""
	}
	return c.name
}

// Value returns the current value of the counter.
func (c *Counter) Value() int64 {
	if c == nil {
		return 0
	}
	return c.value.Load()
}

// Inc increments the counter by 1.
func (c *Counter) Inc() {
	c.Add(1)
}

// Add increments the counter by delta. If delta is negative, Add panics.
func (c *Counter) Add(delta int64) {
	if delta < 0 {
		panic("metrics: counter value cannot decrease")
	}
	if c == nil {
		return
	}
	c.value.Add(delta)
}
