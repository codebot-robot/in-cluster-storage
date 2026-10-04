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

// Package view provides a generic in-memory caching and write-behind applier layer
// wrapping any sds.LocalIndex.
package view

import (
	"bytes"
	"context"
	"iter"
	"sort"
	"sync"

	"github.com/gke-labs/in-cluster-storage/pkg/sds"
	"github.com/gke-labs/in-cluster-storage/pkg/sds/record"
	"google.golang.org/protobuf/proto"
)

// OverlayEntry represents a committed but unapplied row change held in memory.
type OverlayEntry struct {
	Op   sds.Op
	Row  proto.Message
	Seq  uint64
	Size int64
}

// Option configures a View.
type Option func(*View)

// WithBatchSize sets the maximum batch size for the background applier.
func WithBatchSize(batchSize int) Option {
	return func(v *View) {
		v.batchSize = batchSize
	}
}

// WithCacheLimits sets the maximum entry count and maximum bytes for the read cache.
func WithCacheLimits(capacity int, maxBytes int64) Option {
	return func(v *View) {
		v.cacheCapacity = capacity
		v.cacheMaxBytes = maxBytes
	}
}

// WithCacheDisabled enables or disables the read cache.
func WithCacheDisabled(disabled bool) Option {
	return func(v *View) {
		v.cacheDisabled = disabled
	}
}

// WithOverlayMaxBytes sets the maximum byte threshold for unapplied mutations before backpressure is applied.
func WithOverlayMaxBytes(maxBytes int64) Option {
	return func(v *View) {
		v.maxOverlayBytes = maxBytes
	}
}

// WithFaultHook sets a test fault injection hook for the background applier.
func WithFaultHook(hook func() error) Option {
	return func(v *View) {
		v.faultHook = hook
	}
}

// WithRegistry sets the in-band type registry for the View.
func WithRegistry(reg *record.Registry) Option {
	return func(v *View) {
		v.reg = reg
	}
}

// View wraps an underlying sds.LocalIndex with an unapplied mutations overlay,
// an LRU read cache, and an asynchronous write-behind applier.
type View struct {
	mu    sync.RWMutex
	index sds.LocalIndex
	reg   *record.Registry

	cache           *LRUCache[CacheKey, *CachedRow]
	cacheDisabled   bool
	cacheCapacity   int
	cacheMaxBytes   int64
	maxOverlayBytes int64
	unappliedBytes  int64

	overlay map[CacheKey]OverlayEntry
	queue   []sds.Change

	applier    *applier
	appliedPos uint64
	batchSize  int
	faultHook  func() error

	backpressureCond *sync.Cond
	flushCond        *sync.Cond
	closed           bool
}

// New creates and starts a new View wrapping index.
func New(index sds.LocalIndex, opts ...Option) *View {
	v := &View{
		index:           index,
		overlay:         make(map[CacheKey]OverlayEntry),
		batchSize:       defaultApplierBatchSize,
		cacheCapacity:   10000,
		cacheMaxBytes:   64 * 1024 * 1024,
		maxOverlayBytes: 64 * 1024 * 1024,
	}
	for _, opt := range opts {
		opt(v)
	}

	v.backpressureCond = sync.NewCond(&v.mu)
	v.flushCond = sync.NewCond(&v.mu)

	v.cache = NewLRUCacheWithLimits[CacheKey, *CachedRow](
		v.cacheCapacity,
		v.cacheMaxBytes,
		CacheSizeFn,
		nil,
	)

	if index != nil {
		v.appliedPos = index.Position()
	}

	v.applier = newApplier(v, v.batchSize, v.faultHook)
	v.applier.start()

	return v
}

// Index returns the underlying sds.LocalIndex.
func (v *View) Index() sds.LocalIndex {
	v.mu.RLock()
	defer v.mu.RUnlock()
	return v.index
}

// SetIndex replaces the underlying index (e.g. on restore).
func (v *View) SetIndex(index sds.LocalIndex) {
	v.mu.Lock()
	defer v.mu.Unlock()
	v.index = index
	if index != nil {
		v.appliedPos = index.Position()
	}
}

// Position returns the last applied stream sequence position.
func (v *View) Position() uint64 {
	v.mu.RLock()
	defer v.mu.RUnlock()
	if v.index != nil {
		pos := v.index.Position()
		if pos > v.appliedPos {
			return pos
		}
	}
	return v.appliedPos
}

// AppliedPosition returns the sequence position applied by the background applier.
func (v *View) AppliedPosition() uint64 {
	v.mu.RLock()
	defer v.mu.RUnlock()
	return v.appliedPos
}

// SetAppliedPosition sets the applied position (e.g. after snapshot restore or full replay).
func (v *View) SetAppliedPosition(pos uint64) {
	v.mu.Lock()
	defer v.mu.Unlock()
	v.appliedPos = pos
}

// Lag returns the number of sequence numbers between currentSeq and the last applied sequence.
func (v *View) Lag(currentSeq uint64) uint64 {
	v.mu.RLock()
	defer v.mu.RUnlock()
	if currentSeq > v.appliedPos {
		return currentSeq - v.appliedPos
	}
	return 0
}

// UnappliedBytes returns the approximate byte size of unapplied changes in the overlay.
func (v *View) UnappliedBytes() int64 {
	v.mu.RLock()
	defer v.mu.RUnlock()
	return v.unappliedBytes
}

// SyncRegistry synchronizes type definitions with the view and underlying index.
func (v *View) SyncRegistry(ctx context.Context, reg *record.Registry) error {
	v.mu.Lock()
	if reg != nil {
		if v.reg == nil {
			v.reg = record.NewRegistry()
		}
		_ = v.reg.Import(reg.Export())
	}
	idx := v.index
	v.mu.Unlock()

	if idx != nil && reg != nil {
		return idx.SyncRegistry(ctx, reg)
	}
	return nil
}

// ApplyChanges enqueues committed changes to the unapplied overlay and background applier.
func (v *View) ApplyChanges(changes []sds.Change) {
	if len(changes) == 0 {
		return
	}

	v.mu.Lock()
	defer v.mu.Unlock()

	normalized := make([]sds.Change, len(changes))
	for i, ch := range changes {
		typeName := ch.TypeName
		if typeName == "" && v.reg != nil {
			if def, _, ok := v.reg.LookupByID(ch.TypeID); ok {
				typeName = def.GetName()
			}
		}
		ch.TypeName = typeName
		if ch.Key.IsZero() && len(ch.RawKey) > 0 {
			ch.Key = sds.NewKeyFromBytes(ch.RawKey)
		}
		normalized[i] = ch

		ck := CacheKey{Table: typeName, Key: ch.Key}

		sz := int64(len(ch.RawKey) + len(ch.RawVal) + 64)
		if old, ok := v.overlay[ck]; ok {
			v.unappliedBytes -= old.Size
		}
		v.unappliedBytes += sz
		v.overlay[ck] = OverlayEntry{
			Op:   ch.Op,
			Row:  ch.Row,
			Seq:  ch.Seq,
			Size: sz,
		}

		if v.cache != nil && !v.cacheDisabled {
			v.cache.Remove(ck)
		}
	}

	if v.applier != nil {
		v.applier.enqueueLocked(normalized)
	}
}

// ApplyChangesSync applies changes synchronously to the underlying index and read cache.
func (v *View) ApplyChangesSync(ctx context.Context, changes []sds.Change) error {
	if len(changes) == 0 {
		return nil
	}

	v.mu.Lock()
	defer v.mu.Unlock()

	if v.index != nil {
		if err := v.index.ApplyBatch(ctx, changes); err != nil {
			return err
		}
	}

	var maxSeq uint64 = v.appliedPos
	for _, ch := range changes {
		typeName := ch.TypeName
		if typeName == "" && v.reg != nil {
			if def, _, ok := v.reg.LookupByID(ch.TypeID); ok {
				typeName = def.GetName()
			}
		}
		key := ch.Key
		if key.IsZero() && len(ch.RawKey) > 0 {
			key = sds.NewKeyFromBytes(ch.RawKey)
		}
		ck := CacheKey{Table: typeName, Key: key}

		if v.cache != nil && !v.cacheDisabled {
			switch ch.Op {
			case sds.OpCreate, sds.OpUpdate:
				if ch.Row != nil {
					v.cache.Put(ck, &CachedRow{Exists: true, Msg: ch.Row})
				} else {
					v.cache.Remove(ck)
				}
			case sds.OpDelete:
				v.cache.Put(ck, &CachedRow{Exists: false})
			}
		}
		if ch.Seq > maxSeq {
			maxSeq = ch.Seq
		}
	}

	if maxSeq > v.appliedPos {
		v.appliedPos = maxSeq
	}
	return nil
}

// Get retrieves a merged proto row by table type name and primary key.
// It checks overlay first, then read cache, and falls back to index.
func (v *View) Get(ctx context.Context, typeName string, key sds.Key) (proto.Message, bool, error) {
	ck := CacheKey{Table: typeName, Key: key}

	// 1. Check unapplied overlay
	v.mu.RLock()
	if entry, ok := v.overlay[ck]; ok {
		v.mu.RUnlock()
		if entry.Op == sds.OpDelete || entry.Row == nil {
			return nil, false, nil
		}
		return proto.Clone(entry.Row), true, nil
	}
	v.mu.RUnlock()

	// 2. Check read cache
	if v.cache != nil && !v.cacheDisabled {
		if row, ok := v.cache.Get(ck); ok {
			if !row.Exists || row.Msg == nil {
				return nil, false, nil
			}
			return proto.Clone(row.Msg), true, nil
		}
	}

	v.mu.RLock()
	idx := v.index
	v.mu.RUnlock()

	if idx == nil {
		return nil, false, nil
	}

	// 3. Query underlying index
	msg, ok, err := idx.Get(ctx, typeName, key)
	if err != nil {
		return nil, false, err
	}

	if v.cache != nil && !v.cacheDisabled {
		if !ok || msg == nil {
			v.cache.Put(ck, &CachedRow{Exists: false})
		} else {
			v.cache.Put(ck, &CachedRow{Exists: true, Msg: msg})
		}
	}

	return msg, ok, nil
}

// Scan yields merged proto rows matching keyPrefix in canonical key-byte order.
// It overlays unapplied in-memory mutations onto the underlying index scan.
func (v *View) Scan(ctx context.Context, typeName string, keyPrefix []byte) iter.Seq2[proto.Message, error] {
	return func(yield func(proto.Message, error) bool) {
		type rowEntry struct {
			key sds.Key
			msg proto.Message
		}
		merged := make(map[string]rowEntry)

		var keyFields []int32
		v.mu.RLock()
		if v.reg != nil {
			if def, _, ok := v.reg.LookupByName(typeName); ok {
				keyFields = def.GetKeyFields()
			}
		}
		idx := v.index
		v.mu.RUnlock()

		if idx != nil {
			for idxMsg, err := range idx.Scan(ctx, typeName, keyPrefix) {
				if err != nil {
					yield(nil, err)
					return
				}
				var k sds.Key
				if len(keyFields) > 0 {
					k, _ = sds.ExtractKey(idxMsg, keyFields)
				}
				if k.IsZero() {
					kBytes, err := proto.MarshalOptions{Deterministic: true}.Marshal(idxMsg)
					if err == nil {
						k = sds.NewKeyFromBytes(kBytes)
					}
				}
				if !k.IsZero() {
					merged[k.String()] = rowEntry{key: k, msg: idxMsg}
				}
			}
		}

		v.mu.RLock()
		for ck, entry := range v.overlay {
			if ck.Table != typeName {
				continue
			}
			if len(keyPrefix) > 0 && !bytes.HasPrefix(ck.Key.Bytes(), keyPrefix) {
				continue
			}
			kStr := ck.Key.String()
			if entry.Op == sds.OpDelete || entry.Row == nil {
				delete(merged, kStr)
			} else {
				merged[kStr] = rowEntry{key: ck.Key, msg: entry.Row}
			}
		}
		v.mu.RUnlock()

		rows := make([]rowEntry, 0, len(merged))
		for _, re := range merged {
			rows = append(rows, re)
		}
		sort.Slice(rows, func(i, j int) bool {
			return bytes.Compare(rows[i].key.Bytes(), rows[j].key.Bytes()) < 0
		})

		for _, re := range rows {
			if !yield(proto.Clone(re.msg), nil) {
				return
			}
		}
	}
}

// ScanSlice returns a slice of all merged proto rows matching keyPrefix in canonical key-byte order.
func (v *View) ScanSlice(ctx context.Context, typeName string, keyPrefix []byte) ([]proto.Message, error) {
	var result []proto.Message
	for msg, err := range v.Scan(ctx, typeName, keyPrefix) {
		if err != nil {
			return nil, err
		}
		result = append(result, msg)
	}
	return result, nil
}

// FlushTo blocks until the background applier has processed changes up to or beyond targetSeq.
func (v *View) FlushTo(ctx context.Context, targetSeq uint64) error {
	v.mu.Lock()
	defer v.mu.Unlock()

	if v.applier == nil {
		return nil
	}

	if ctxDone := ctx.Done(); ctxDone != nil {
		stopCancel := make(chan struct{})
		defer close(stopCancel)
		go func() {
			select {
			case <-ctxDone:
				v.mu.Lock()
				if v.flushCond != nil {
					v.flushCond.Broadcast()
				}
				v.mu.Unlock()
			case <-stopCancel:
			}
		}()
	}

	for v.appliedPos < targetSeq && !v.closed {
		if err := ctx.Err(); err != nil {
			return err
		}
		v.applier.wakeLocked()
		v.flushCond.Wait()
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	return nil
}

// Flush blocks until all currently queued changes in the view have been applied to the index.
func (v *View) Flush(ctx context.Context) error {
	v.mu.Lock()
	defer v.mu.Unlock()

	if v.applier == nil {
		return nil
	}

	if ctxDone := ctx.Done(); ctxDone != nil {
		stopCancel := make(chan struct{})
		defer close(stopCancel)
		go func() {
			select {
			case <-ctxDone:
				v.mu.Lock()
				if v.flushCond != nil {
					v.flushCond.Broadcast()
				}
				v.mu.Unlock()
			case <-stopCancel:
			}
		}()
	}

	for len(v.queue) > 0 && !v.closed {
		if err := ctx.Err(); err != nil {
			return err
		}
		v.applier.wakeLocked()
		v.flushCond.Wait()
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	return nil
}

// WaitBackpressure blocks if unapplied overlay bytes exceed maxOverlayBytes.
func (v *View) WaitBackpressure(ctx context.Context) error {
	v.mu.Lock()
	defer v.mu.Unlock()

	if ctxDone := ctx.Done(); ctxDone != nil {
		stopCancel := make(chan struct{})
		defer close(stopCancel)
		go func() {
			select {
			case <-ctxDone:
				v.mu.Lock()
				if v.backpressureCond != nil {
					v.backpressureCond.Broadcast()
				}
				v.mu.Unlock()
			case <-stopCancel:
			}
		}()
	}

	for v.maxOverlayBytes > 0 && v.unappliedBytes > v.maxOverlayBytes && !v.closed {
		if err := ctx.Err(); err != nil {
			return err
		}
		if v.applier != nil {
			v.applier.wakeLocked()
		}
		v.backpressureCond.Wait()
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	return nil
}

// CacheStats returns cache hit/miss and capacity metrics.
func (v *View) CacheStats() LRUCacheStats {
	if v.cache == nil {
		return LRUCacheStats{}
	}
	return v.cache.Stats()
}

// CacheResetStats resets cache metrics counters.
func (v *View) CacheResetStats() {
	if v.cache != nil {
		v.cache.ResetStats()
	}
}

// SetCacheLimits updates the entry count and byte capacity of the read cache.
func (v *View) SetCacheLimits(capacity int, maxBytes int64) {
	if v.cache != nil {
		v.cache.SetLimits(capacity, maxBytes)
	}
}

// SetCacheDisabled enables or disables read caching.
func (v *View) SetCacheDisabled(disabled bool) {
	v.mu.Lock()
	defer v.mu.Unlock()
	v.cacheDisabled = disabled
}

// ApplierFailures returns the total number of batch apply errors encountered.
func (v *View) ApplierFailures() uint64 {
	if v.applier != nil {
		return v.applier.failures.Load()
	}
	return 0
}

// IsDegraded reports whether the background applier is currently in a degraded state.
func (v *View) IsDegraded() bool {
	if v.applier != nil {
		return v.applier.isDegraded.Load()
	}
	return false
}

// SetFaultHook sets a fault injection hook for testing.
func (v *View) SetFaultHook(hook func() error) {
	if v.applier != nil {
		v.applier.faultHook = hook
	}
}

// ClearCache clears all items from the read cache.
func (v *View) ClearCache() {
	if v.cache != nil {
		v.cache.Clear()
	}
}

// Close stops the applier worker and closes the underlying index.
func (v *View) Close() error {
	v.mu.Lock()
	if v.closed {
		v.mu.Unlock()
		return nil
	}
	v.closed = true
	if v.flushCond != nil {
		v.flushCond.Broadcast()
	}
	if v.backpressureCond != nil {
		v.backpressureCond.Broadcast()
	}
	applier := v.applier
	idx := v.index
	v.mu.Unlock()

	if applier != nil {
		applier.stop()
	}

	if v.cache != nil {
		v.cache.Clear()
	}

	if idx != nil {
		return idx.Close()
	}
	return nil
}
