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
	"math"
	"math/rand"
	"sync/atomic"
	"time"

	sdsv1 "github.com/gke-labs/in-cluster-storage/pkg/api/sds/v1"
	"github.com/gke-labs/in-cluster-storage/pkg/sds"
	"google.golang.org/protobuf/proto"
	"k8s.io/klog/v2"
)

const (
	defaultApplierBatchSize   = 100
	defaultDegradedThreshold  = 3
	defaultApplierBaseBackoff = 5 * time.Millisecond
	defaultApplierMaxBackoff  = 1 * time.Second
)

// SQLiteOverlayEntry represents a committed but unapplied row change held in memory.
type SQLiteOverlayEntry struct {
	Op   sdsv1.OpRecord_Op
	Row  proto.Message
	Seq  uint64
	Size int64
}

// sqliteApplier is a background worker that applies overlay row changes to SQLite in log order in batches.
type sqliteApplier struct {
	v         *Volume
	batchSize int
	queue     []sds.Change
	wakeCh    chan struct{}
	stopCh    chan struct{}
	doneCh    chan struct{}
	stopped   bool

	failures       atomic.Uint64
	consecutiveErr atomic.Uint32
	isDegraded     atomic.Bool
	faultHook      func() error
}

func newSQLiteApplier(v *Volume, batchSize int, faultHook func() error) *sqliteApplier {
	if batchSize <= 0 {
		batchSize = defaultApplierBatchSize
	}
	return &sqliteApplier{
		v:         v,
		batchSize: batchSize,
		wakeCh:    make(chan struct{}, 1),
		stopCh:    make(chan struct{}),
		doneCh:    make(chan struct{}),
		faultHook: faultHook,
	}
}

func (a *sqliteApplier) start() {
	go a.loop()
}

func (a *sqliteApplier) stop() {
	a.v.mu.Lock()
	if !a.stopped {
		a.stopped = true
		close(a.stopCh)
		a.wakeLocked()
	}
	a.v.mu.Unlock()

	<-a.doneCh
}

func (a *sqliteApplier) wake() {
	select {
	case a.wakeCh <- struct{}{}:
	default:
	}
}

func (a *sqliteApplier) wakeLocked() {
	select {
	case a.wakeCh <- struct{}{}:
	default:
	}
}

func (a *sqliteApplier) enqueueLocked(changes []sds.Change) {
	if len(changes) == 0 {
		return
	}
	a.queue = append(a.queue, changes...)
	a.wakeLocked()
}

func (a *sqliteApplier) loop() {
	defer close(a.doneCh)

	for {
		// 1. Wait for work under volume mutex
		a.v.mu.Lock()
		for len(a.queue) == 0 && !a.stopped {
			a.v.mu.Unlock()
			select {
			case <-a.wakeCh:
			case <-a.stopCh:
			}
			a.v.mu.Lock()
		}

		if len(a.queue) == 0 && a.stopped {
			a.v.mu.Unlock()
			return
		}

		// Determine batch size: only cut batches at transaction boundaries!
		// Never split changes that share a Seq. If a single transaction is larger than batchSize,
		// apply it as one batch anyway.
		targetSize := a.batchSize
		if targetSize <= 0 {
			targetSize = defaultApplierBatchSize
		}
		batchLimit := 0
		for batchLimit < len(a.queue) {
			currentSeq := a.queue[batchLimit].Seq
			nextLimit := batchLimit + 1
			for nextLimit < len(a.queue) && a.queue[nextLimit].Seq == currentSeq {
				nextLimit++
			}
			batchLimit = nextLimit
			if batchLimit >= targetSize {
				break
			}
		}
		batch := a.queue[:batchLimit]

		// Coalesce repeated changes in batch by (table, key)
		type coalescedKey struct {
			typeName string
			key      sds.Key
		}
		type coalescedVal struct {
			change sds.Change
		}

		coalescedMap := make(map[coalescedKey]coalescedVal, len(batch))
		order := make([]coalescedKey, 0, len(batch))
		var batchMaxSeq uint64

		for _, ch := range batch {
			typeName := ch.TypeName
			if typeName == "" && a.v.metadataStream != nil {
				if def, _, ok := a.v.metadataStream.Registry().LookupByID(ch.TypeID); ok {
					typeName = def.GetName()
				}
			}
			if typeName == "" && a.v.sqliteDB != nil {
				if def, _, ok := a.v.sqliteDB.Registry().LookupByID(ch.TypeID); ok {
					typeName = def.GetName()
				}
			}
			key := ch.Key
			if key.IsZero() && len(ch.RawKey) > 0 {
				key = sds.NewKeyFromBytes(ch.RawKey)
			}
			ck := coalescedKey{typeName: typeName, key: key}
			if _, exists := coalescedMap[ck]; !exists {
				order = append(order, ck)
			}
			coalescedMap[ck] = coalescedVal{change: ch}
			if ch.Seq > batchMaxSeq {
				batchMaxSeq = ch.Seq
			}
		}

		coalescedChanges := make([]sds.Change, 0, len(coalescedMap))
		for _, ck := range order {
			cv := coalescedMap[ck]
			ch := cv.change
			ch.Seq = batchMaxSeq
			coalescedChanges = append(coalescedChanges, ch)
		}
		a.v.mu.Unlock()

		// 2. Apply batch to SQLite outside volume mutex
		var applyErr error
		if a.faultHook != nil {
			applyErr = a.faultHook()
		}
		if applyErr == nil && a.v.sqliteDB != nil && len(coalescedChanges) > 0 {
			applyErr = a.v.sqliteDB.ApplyBatch(context.Background(), coalescedChanges)
		}

		if applyErr != nil {
			a.failures.Add(1)
			fails := a.consecutiveErr.Add(1)
			if fails >= defaultDegradedThreshold && !a.isDegraded.Load() {
				a.isDegraded.Store(true)
				klog.Warningf("SQLite applier marked DEGRADED after %d consecutive failures: %v", fails, applyErr)
			}

			// Exponential backoff with jitter
			shift := min(fails, 8)
			backoffMs := float64(defaultApplierBaseBackoff/time.Millisecond) * math.Pow(2, float64(shift))
			if backoffMs > float64(defaultApplierMaxBackoff/time.Millisecond) {
				backoffMs = float64(defaultApplierMaxBackoff / time.Millisecond)
			}
			jitter := 0
			if int(backoffMs) > 1 {
				jitter = rand.Intn(int(backoffMs)/2 + 1)
			}
			backoffDur := time.Duration(int(backoffMs)+jitter) * time.Millisecond

			select {
			case <-time.After(backoffDur):
			case <-a.stopCh:
				return
			}
			a.v.mu.Lock()
			if a.stopped {
				a.v.mu.Unlock()
				return
			}
			a.v.mu.Unlock()
			// Do NOT remove batch from queue; retry on next iteration
			continue
		}

		// 3. Batch applied successfully! Clean up under volume mutex
		a.v.mu.Lock()
		if a.isDegraded.Load() {
			a.isDegraded.Store(false)
			klog.Infof("SQLite applier recovered from degraded state at seq %d", batchMaxSeq)
		}
		a.consecutiveErr.Store(0)

		// Dequeue the applied batch
		a.queue = a.queue[batchLimit:]

		// Update overlay and read cache for each coalesced key
		for ck, cv := range coalescedMap {
			cacheKey := SQLiteCacheKey{Table: ck.typeName, Key: ck.key}
			if overlayEntry, exists := a.v.sqliteOverlay[cacheKey]; exists {
				if overlayEntry.Seq <= batchMaxSeq {
					delete(a.v.sqliteOverlay, cacheKey)
					a.v.unappliedBytes -= overlayEntry.Size
					if a.v.unappliedBytes < 0 {
						a.v.unappliedBytes = 0
					}

					// Update read cache with clean entry
					if a.v.sqliteCache != nil && !a.v.sqliteCacheDisabled {
						switch cv.change.Op {
						case sds.OpCreate, sds.OpUpdate:
							if cv.change.Row != nil {
								a.v.sqliteCache.Put(cacheKey, &SQLiteCachedRow{Exists: true, Msg: cv.change.Row})
							} else {
								a.v.sqliteCache.Remove(cacheKey)
							}
						case sds.OpDelete:
							a.v.sqliteCache.Put(cacheKey, &SQLiteCachedRow{Exists: false})
						}
					}
				}
			}
		}

		if batchMaxSeq > a.v.sqliteAppliedPos {
			a.v.sqliteAppliedPos = batchMaxSeq
		}

		// Signal any blocked writers or flush waiters
		if a.v.backpressureCond != nil {
			a.v.backpressureCond.Broadcast()
		}
		if a.v.flushCond != nil {
			a.v.flushCond.Broadcast()
		}
		a.v.mu.Unlock()
	}
}
