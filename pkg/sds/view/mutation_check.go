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

package view

import (
	"fmt"
	"hash/fnv"
	"reflect"
	"sync"

	"google.golang.org/protobuf/proto"
)

type mutationEntry struct {
	typeName string
	key      string
	hash     uint64
	msg      proto.Message
}

// mutationChecker tracks deterministic protobuf hashes of shared messages keyed by pointer identity.
//
// Note: mutationChecker retains a reference to every proto message pointer it observes without eviction
// (unbounded memory growth). It is strictly intended for test and debugging environments.
type mutationChecker struct {
	mu      sync.Mutex
	entries map[uintptr]mutationEntry
}

func newMutationChecker() *mutationChecker {
	return &mutationChecker{
		entries: make(map[uintptr]mutationEntry),
	}
}

func (c *mutationChecker) hashMsg(msg proto.Message) (uint64, error) {
	if msg == nil {
		return 0, nil
	}
	b, err := proto.MarshalOptions{Deterministic: true}.Marshal(msg)
	if err != nil {
		return 0, err
	}
	h := fnv.New64a()
	h.Write(b)
	return h.Sum64(), nil
}

func (c *mutationChecker) getPtr(msg proto.Message) uintptr {
	if msg == nil {
		return 0
	}
	val := reflect.ValueOf(msg)
	if val.Kind() == reflect.Pointer && !val.IsNil() {
		return val.Pointer()
	}
	return 0
}

// Record registers a message with its deterministic hash keyed by pointer.
// If the pointer is already tracked, it verifies the hash has not mutated.
func (c *mutationChecker) Record(msg proto.Message, typeName string, key any) {
	if c == nil || msg == nil {
		return
	}
	ptr := c.getPtr(msg)
	if ptr == 0 {
		return
	}
	h, err := c.hashMsg(msg)
	if err != nil {
		panic(fmt.Sprintf("view: failed to marshal %T for mutation check: %v", msg, err))
	}

	keyStr := fmt.Sprint(key)

	c.mu.Lock()
	defer c.mu.Unlock()

	if existing, exists := c.entries[ptr]; exists {
		if existing.hash != h {
			panic(fmt.Sprintf("view: row mutated in place! type=%s key=%s (recorded hash=%d, current hash=%d)", existing.typeName, existing.key, existing.hash, h))
		}
		return
	}

	c.entries[ptr] = mutationEntry{
		typeName: typeName,
		key:      keyStr,
		hash:     h,
		msg:      msg,
	}
}

// Check verifies that msg matches its registered hash.
// If the message has been mutated in place, it panics naming the type and key.
func (c *mutationChecker) Check(msg proto.Message) {
	if c == nil || msg == nil {
		return
	}
	ptr := c.getPtr(msg)
	if ptr == 0 {
		return
	}

	c.mu.Lock()
	existing, exists := c.entries[ptr]
	c.mu.Unlock()

	if !exists {
		return
	}

	h, err := c.hashMsg(msg)
	if err != nil {
		panic(fmt.Sprintf("view: failed to marshal %T for mutation check: %v", msg, err))
	}

	if existing.hash != h {
		panic(fmt.Sprintf("view: row mutated in place! type=%s key=%s (recorded hash=%d, current hash=%d)", existing.typeName, existing.key, existing.hash, h))
	}
}

// CheckAll checks all registered messages and panics if any have been mutated.
func (c *mutationChecker) CheckAll() {
	if c == nil {
		return
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	for ptr, entry := range c.entries {
		if entry.msg == nil {
			continue
		}
		h, err := c.hashMsg(entry.msg)
		if err != nil {
			panic(fmt.Sprintf("view: failed to marshal %T for mutation check: %v", entry.msg, err))
		}
		if entry.hash != h {
			panic(fmt.Sprintf("view: row mutated in place! type=%s key=%s pointer=0x%x (recorded hash=%d, current hash=%d)", entry.typeName, entry.key, ptr, entry.hash, h))
		}
	}
}
