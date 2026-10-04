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

package view_test

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/dynamicpb"

	"github.com/gke-labs/in-cluster-storage/pkg/sds"
	"github.com/gke-labs/in-cluster-storage/pkg/sds/memtable"
	"github.com/gke-labs/in-cluster-storage/pkg/sds/projection/sqlite"
	"github.com/gke-labs/in-cluster-storage/pkg/sds/view"
)

type indexFactory struct {
	name   string
	create func(t *testing.T, ctx context.Context, streamID string) (sds.LocalIndex, func())
}

func testFactories(t *testing.T) []indexFactory {
	return []indexFactory{
		{
			name: "SQLite",
			create: func(t *testing.T, ctx context.Context, streamID string) (sds.LocalIndex, func()) {
				dbPath := filepath.Join(t.TempDir(), "view_test.sqlite")
				db, err := sqlite.Open(ctx, dbPath,
					sqlite.WithStreamID(streamID),
					sqlite.WithJournalMode("WAL"),
					sqlite.WithSynchronous("NORMAL"),
				)
				if err != nil {
					t.Fatalf("sqlite.Open failed: %v", err)
				}
				return db, func() { _ = db.Close() }
			},
		},
		{
			name: "MemTable",
			create: func(t *testing.T, ctx context.Context, streamID string) (sds.LocalIndex, func()) {
				store := memtable.New(memtable.WithStreamID(streamID))
				return store, func() { _ = store.Close() }
			},
		},
	}
}

func protoField(name string, number int32, typeKind descriptorpb.FieldDescriptorProto_Type, label descriptorpb.FieldDescriptorProto_Label) *descriptorpb.FieldDescriptorProto {
	return &descriptorpb.FieldDescriptorProto{
		Name:   proto.String(name),
		Number: proto.Int32(number),
		Type:   typeKind.Enum(),
		Label:  label.Enum(),
	}
}

func dynamicDescriptor(t *testing.T, name string, fields []*descriptorpb.FieldDescriptorProto) protoreflect.MessageDescriptor {
	t.Helper()
	msgProto := &descriptorpb.DescriptorProto{
		Name:  proto.String(name),
		Field: fields,
	}
	fileProto := &descriptorpb.FileDescriptorProto{
		Name:        proto.String(name + ".proto"),
		Package:     proto.String("viewtest"),
		MessageType: []*descriptorpb.DescriptorProto{msgProto},
		Syntax:      proto.String("proto2"),
	}
	fds := &descriptorpb.FileDescriptorSet{
		File: []*descriptorpb.FileDescriptorProto{fileProto},
	}
	files, err := protodesc.NewFiles(fds)
	if err != nil {
		t.Fatalf("protodesc.NewFiles failed: %v", err)
	}
	d, err := files.FindDescriptorByName(protoreflect.FullName("viewtest." + name))
	if err != nil {
		t.Fatalf("FindDescriptorByName failed: %v", err)
	}
	return d.(protoreflect.MessageDescriptor)
}

type memoryTransport struct {
	payloads [][]byte
	seq      uint64
}

func (m *memoryTransport) Append(_ context.Context, payload []byte) (uint64, error) {
	m.seq++
	cp := make([]byte, len(payload))
	copy(cp, payload)
	m.payloads = append(m.payloads, cp)
	return m.seq, nil
}

// TestViewOverlayReadsDuringLag verifies that reads via Get and Scan see unapplied mutations immediately even when the applier is lagging or blocked.
func TestViewOverlayReadsDuringLag(t *testing.T) {
	for _, factory := range testFactories(t) {
		t.Run(factory.name, func(t *testing.T) {
			ctx := t.Context()
			streamID := uuid.New().String()
			idx, cleanup := factory.create(t, ctx, streamID)
			defer cleanup()

			var blockApplier atomic.Bool
			blockApplier.Store(true)

			faultHook := func() error {
				if blockApplier.Load() {
					return errors.New("simulated applier stall")
				}
				return nil
			}

			transport := &memoryTransport{}
			writer := sds.NewWriter(transport)

			userMD := dynamicDescriptor(t, "User", []*descriptorpb.FieldDescriptorProto{
				protoField("id", 1, descriptorpb.FieldDescriptorProto_TYPE_INT64, descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL),
				protoField("name", 2, descriptorpb.FieldDescriptorProto_TYPE_STRING, descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL),
			})
			_, err := writer.RegisterDescriptor(userMD, 1)
			if err != nil {
				t.Fatalf("RegisterDescriptor failed: %v", err)
			}

			v := view.New(idx,
				view.WithBatchSize(10),
				view.WithFaultHook(faultHook),
				view.WithRegistry(writer.Registry()),
			)
			defer v.Close()
			_ = v.SyncRegistry(ctx, writer.Registry())

			// 1. Create User 1 and User 2
			reader := sds.NewChangeReader()
			_ = reader.Registry().Import(writer.Registry().Export())
			u1 := dynamicpb.NewMessage(userMD)
			u1.Set(userMD.Fields().ByName("id"), protoreflect.ValueOfInt64(1))
			u1.Set(userMD.Fields().ByName("name"), protoreflect.ValueOfString("Alice"))
			seq1, _ := writer.Insert(ctx, u1)
			changes1, _ := reader.Feed(seq1, transport.payloads[len(transport.payloads)-1])
			v.ApplyChanges(changes1)

			u2 := dynamicpb.NewMessage(userMD)
			u2.Set(userMD.Fields().ByName("id"), protoreflect.ValueOfInt64(2))
			u2.Set(userMD.Fields().ByName("name"), protoreflect.ValueOfString("Bob"))
			seq2, _ := writer.Insert(ctx, u2)
			changes2, _ := reader.Feed(seq2, transport.payloads[len(transport.payloads)-1])
			v.ApplyChanges(changes2)

			// Verify applier is lagging (applied position is 0)
			if v.AppliedPosition() != 0 {
				t.Fatalf("expected applied position 0 during lag, got %d", v.AppliedPosition())
			}

			// Point lookup via Get should immediately return Alice from overlay
			k1, _ := sds.ExtractKey(u1, []int32{1})
			msg1, ok, err := v.Get(ctx, "viewtest.User", k1)
			if err != nil || !ok {
				t.Fatalf("v.Get(User 1) failed: ok=%v err=%v", ok, err)
			}
			dyn1 := msg1.(*dynamicpb.Message)
			if dyn1.Get(dyn1.Descriptor().Fields().ByName("name")).String() != "Alice" {
				t.Fatalf("name mismatch: %s", dyn1.Get(dyn1.Descriptor().Fields().ByName("name")).String())
			}

			// Scan should return both User 1 and User 2 in canonical order from overlay
			var scanUsers []string
			for msg, err := range v.Scan(ctx, "viewtest.User", nil) {
				if err != nil {
					t.Fatalf("v.Scan failed: %v", err)
				}
				dyn := msg.(*dynamicpb.Message)
				scanUsers = append(scanUsers, dyn.Get(dyn.Descriptor().Fields().ByName("name")).String())
			}
			if len(scanUsers) != 2 || scanUsers[0] != "Alice" || scanUsers[1] != "Bob" {
				t.Fatalf("unexpected scan results during lag: %v", scanUsers)
			}

			// 2. Unblock applier and flush
			blockApplier.Store(false)
			if err := v.FlushTo(ctx, seq2); err != nil {
				t.Fatalf("FlushTo failed: %v", err)
			}

			if v.AppliedPosition() < seq2 {
				t.Fatalf("applied position %d < seq2 %d after flush", v.AppliedPosition(), seq2)
			}

			// Reads should still return Alice and Bob cleanly from cache/index
			msg1After, ok, err := v.Get(ctx, "viewtest.User", k1)
			if err != nil || !ok {
				t.Fatalf("v.Get after flush failed: %v", err)
			}
			dyn1After := msg1After.(*dynamicpb.Message)
			if dyn1After.Get(dyn1After.Descriptor().Fields().ByName("name")).String() != "Alice" {
				t.Fatalf("name mismatch after flush: %s", dyn1After.Get(dyn1After.Descriptor().Fields().ByName("name")).String())
			}
		})
	}
}

// TestViewCoalescing verifies that multiple updates to the same row in a batch coalesce to the latest state.
func TestViewCoalescing(t *testing.T) {
	for _, factory := range testFactories(t) {
		t.Run(factory.name, func(t *testing.T) {
			ctx := t.Context()
			streamID := uuid.New().String()
			idx, cleanup := factory.create(t, ctx, streamID)
			defer cleanup()

			transport := &memoryTransport{}
			writer := sds.NewWriter(transport)

			counterMD := dynamicDescriptor(t, "Counter", []*descriptorpb.FieldDescriptorProto{
				protoField("id", 1, descriptorpb.FieldDescriptorProto_TYPE_INT64, descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL),
				protoField("val", 2, descriptorpb.FieldDescriptorProto_TYPE_INT64, descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL),
			})
			_, err := writer.RegisterDescriptor(counterMD, 1)
			if err != nil {
				t.Fatalf("RegisterDescriptor failed: %v", err)
			}

			v := view.New(idx,
				view.WithBatchSize(50),
				view.WithRegistry(writer.Registry()),
			)
			defer v.Close()
			_ = v.SyncRegistry(ctx, writer.Registry())

			reader := sds.NewChangeReader()
			_ = reader.Registry().Import(writer.Registry().Export())

			// Create counter id 1 val 0
			c := dynamicpb.NewMessage(counterMD)
			c.Set(counterMD.Fields().ByName("id"), protoreflect.ValueOfInt64(1))
			c.Set(counterMD.Fields().ByName("val"), protoreflect.ValueOfInt64(0))
			seq, _ := writer.Insert(ctx, c)
			ch, _ := reader.Feed(seq, transport.payloads[len(transport.payloads)-1])
			v.ApplyChanges(ch)

			// Rapidly update counter 1 from 1 to 20
			for i := int64(1); i <= 20; i++ {
				c.Set(counterMD.Fields().ByName("val"), protoreflect.ValueOfInt64(i))
				seq, _ = writer.Update(ctx, c)
				ch, _ = reader.Feed(seq, transport.payloads[len(transport.payloads)-1])
				v.ApplyChanges(ch)
			}

			if err := v.FlushTo(ctx, seq); err != nil {
				t.Fatalf("FlushTo failed: %v", err)
			}

			k1, _ := sds.ExtractKey(c, []int32{1})
			msg, ok, err := v.Get(ctx, "viewtest.Counter", k1)
			if err != nil || !ok {
				t.Fatalf("Get counter failed: %v", err)
			}
			dyn := msg.(*dynamicpb.Message)
			if dyn.Get(dyn.Descriptor().Fields().ByName("val")).Int() != 20 {
				t.Fatalf("expected val 20, got %d", dyn.Get(dyn.Descriptor().Fields().ByName("val")).Int())
			}
		})
	}
}

// TestViewFaultInjectionAndDegradedState verifies applier degraded state and recovery.
func TestViewFaultInjectionAndDegradedState(t *testing.T) {
	for _, factory := range testFactories(t) {
		t.Run(factory.name, func(t *testing.T) {
			ctx := t.Context()
			streamID := uuid.New().String()
			idx, cleanup := factory.create(t, ctx, streamID)
			defer cleanup()

			var failCount atomic.Int32
			failCount.Store(5) // Fail 5 times then recover

			faultHook := func() error {
				if failCount.Add(-1) >= 0 {
					return errors.New("simulated transient fault")
				}
				return nil
			}

			transport := &memoryTransport{}
			writer := sds.NewWriter(transport)

			itemMD := dynamicDescriptor(t, "Item", []*descriptorpb.FieldDescriptorProto{
				protoField("id", 1, descriptorpb.FieldDescriptorProto_TYPE_INT64, descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL),
				protoField("name", 2, descriptorpb.FieldDescriptorProto_TYPE_STRING, descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL),
			})
			_, _ = writer.RegisterDescriptor(itemMD, 1)

			v := view.New(idx,
				view.WithBatchSize(1),
				view.WithFaultHook(faultHook),
				view.WithRegistry(writer.Registry()),
			)
			defer v.Close()
			_ = v.SyncRegistry(ctx, writer.Registry())

			reader := sds.NewChangeReader()
			_ = reader.Registry().Import(writer.Registry().Export())
			it := dynamicpb.NewMessage(itemMD)
			it.Set(itemMD.Fields().ByName("id"), protoreflect.ValueOfInt64(1))
			it.Set(itemMD.Fields().ByName("name"), protoreflect.ValueOfString("FaultTest"))
			seq, _ := writer.Insert(ctx, it)
			ch, _ := reader.Feed(seq, transport.payloads[len(transport.payloads)-1])
			v.ApplyChanges(ch)

			// Wait for retry loop to recover and flush
			flushCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
			defer cancel()
			if err := v.FlushTo(flushCtx, seq); err != nil {
				t.Fatalf("FlushTo failed after retries: %v", err)
			}

			if v.ApplierFailures() < 5 {
				t.Fatalf("expected at least 5 failures, got %d", v.ApplierFailures())
			}
			if v.IsDegraded() {
				t.Fatalf("expected applier to be non-degraded after recovery")
			}
		})
	}
}

// TestViewBackpressure verifies that WaitBackpressure halts writers when unapplied bytes exceed limit.
func TestViewBackpressure(t *testing.T) {
	for _, factory := range testFactories(t) {
		t.Run(factory.name, func(t *testing.T) {
			ctx := t.Context()
			streamID := uuid.New().String()
			idx, cleanup := factory.create(t, ctx, streamID)
			defer cleanup()

			var holdApplier atomic.Bool
			holdApplier.Store(true)

			faultHook := func() error {
				if holdApplier.Load() {
					return errors.New("applier paused for backpressure test")
				}
				return nil
			}

			transport := &memoryTransport{}
			writer := sds.NewWriter(transport)

			dataMD := dynamicDescriptor(t, "Data", []*descriptorpb.FieldDescriptorProto{
				protoField("id", 1, descriptorpb.FieldDescriptorProto_TYPE_INT64, descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL),
				protoField("payload", 2, descriptorpb.FieldDescriptorProto_TYPE_BYTES, descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL),
			})
			_, _ = writer.RegisterDescriptor(dataMD, 1)

			v := view.New(idx,
				view.WithOverlayMaxBytes(500),
				view.WithFaultHook(faultHook),
				view.WithRegistry(writer.Registry()),
			)
			defer v.Close()
			_ = v.SyncRegistry(ctx, writer.Registry())

			reader := sds.NewChangeReader()
			_ = reader.Registry().Import(writer.Registry().Export())

			// Add entries to exceed 500 bytes limit
			largeBytes := make([]byte, 300)
			for i := int64(1); i <= 3; i++ {
				d := dynamicpb.NewMessage(dataMD)
				d.Set(dataMD.Fields().ByName("id"), protoreflect.ValueOfInt64(i))
				d.Set(dataMD.Fields().ByName("payload"), protoreflect.ValueOfBytes(largeBytes))
				seq, _ := writer.Insert(ctx, d)
				ch, _ := reader.Feed(seq, transport.payloads[len(transport.payloads)-1])
				v.ApplyChanges(ch)
			}

			if v.UnappliedBytes() <= 500 {
				t.Fatalf("expected unapplied bytes > 500, got %d", v.UnappliedBytes())
			}

			var unblocked atomic.Bool
			var wg sync.WaitGroup
			wg.Add(1)
			go func() {
				defer wg.Done()
				_ = v.WaitBackpressure(ctx)
				unblocked.Store(true)
			}()

			time.Sleep(50 * time.Millisecond)
			if unblocked.Load() {
				t.Fatalf("WaitBackpressure should have blocked while unapplied bytes exceeded limit")
			}

			// Release applier
			holdApplier.Store(false)
			wg.Wait()

			if !unblocked.Load() {
				t.Fatalf("WaitBackpressure failed to unblock after applier processed queue")
			}
		})
	}
}

// TestViewTruncateDeletesAndScanOrder verifies that tombstones in overlay and applied deletes
// suppress deleted chunks and correctly return live chunks during prefix scan and point lookups.
func TestViewTruncateDeletesAndScanOrder(t *testing.T) {
	for _, factory := range testFactories(t) {
		t.Run(factory.name, func(t *testing.T) {
			ctx := t.Context()
			streamID := uuid.New().String()
			idx, cleanup := factory.create(t, ctx, streamID)
			defer cleanup()

			transport := &memoryTransport{}
			writer := sds.NewWriter(transport)

			chunkMD := dynamicDescriptor(t, "FileChunk", []*descriptorpb.FieldDescriptorProto{
				protoField("ino", 1, descriptorpb.FieldDescriptorProto_TYPE_INT64, descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL),
				protoField("index", 2, descriptorpb.FieldDescriptorProto_TYPE_INT64, descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL),
				protoField("sha256", 3, descriptorpb.FieldDescriptorProto_TYPE_STRING, descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL),
			})
			_, _ = writer.RegisterDescriptor(chunkMD, 1, 2)

			v := view.New(idx,
				view.WithBatchSize(100),
				view.WithRegistry(writer.Registry()),
			)
			defer v.Close()
			_ = v.SyncRegistry(ctx, writer.Registry())

			reader := sds.NewChangeReader()
			_ = reader.Registry().Import(writer.Registry().Export())

			readOffset := 0
			consumeNewPayloads := func() {
				for readOffset < len(transport.payloads) {
					seq := uint64(readOffset + 1)
					ch, err := reader.Feed(seq, transport.payloads[readOffset])
					if err != nil {
						t.Fatalf("reader.Feed failed at seq %d: %v", seq, err)
					}
					if len(ch) > 0 {
						v.ApplyChanges(ch)
					}
					readOffset++
				}
			}

			// 1. Insert 20 chunks (indices 0..19) for ino 10
			ino := int64(10)
			for i := int64(0); i < 20; i++ {
				c := dynamicpb.NewMessage(chunkMD)
				c.Set(chunkMD.Fields().ByName("ino"), protoreflect.ValueOfInt64(ino))
				c.Set(chunkMD.Fields().ByName("index"), protoreflect.ValueOfInt64(i))
				c.Set(chunkMD.Fields().ByName("sha256"), protoreflect.ValueOfString(fmt.Sprintf("sha-chunk-%d", i)))
				_, _ = writer.Insert(ctx, c)
			}
			consumeNewPayloads()

			// Flush to index
			if err := v.Flush(ctx); err != nil {
				t.Fatalf("Flush failed: %v", err)
			}

			// 2. Truncate down: delete chunks 15..19
			tx1 := writer.Begin()
			for i := int64(15); i < 20; i++ {
				c := dynamicpb.NewMessage(chunkMD)
				c.Set(chunkMD.Fields().ByName("ino"), protoreflect.ValueOfInt64(ino))
				c.Set(chunkMD.Fields().ByName("index"), protoreflect.ValueOfInt64(i))
				if _, err := tx1.Delete(ctx, c); err != nil {
					t.Fatalf("tx1.Delete failed for chunk %d: %v", i, err)
				}
			}
			if _, err := tx1.Commit(ctx); err != nil {
				t.Fatalf("tx1.Commit failed: %v", err)
			}
			consumeNewPayloads()

			// 3. Truncate down again: delete chunks 12..14
			tx2 := writer.Begin()
			for i := int64(12); i < 15; i++ {
				c := dynamicpb.NewMessage(chunkMD)
				c.Set(chunkMD.Fields().ByName("ino"), protoreflect.ValueOfInt64(ino))
				c.Set(chunkMD.Fields().ByName("index"), protoreflect.ValueOfInt64(i))
				if _, err := tx2.Delete(ctx, c); err != nil {
					t.Fatalf("tx2.Delete failed for chunk %d: %v", i, err)
				}
			}
			if _, err := tx2.Commit(ctx); err != nil {
				t.Fatalf("tx2.Commit failed: %v", err)
			}
			consumeNewPayloads()

			// 4. Test Scan prefix while unapplied overlay holds tombstones
			prefixMsg := dynamicpb.NewMessage(chunkMD)
			prefixMsg.Set(chunkMD.Fields().ByName("ino"), protoreflect.ValueOfInt64(ino))
			prefixBytes, _ := sds.EncodeKeyPrefix(prefixMsg, 1)

			for idxMsg, err := range idx.Scan(ctx, "viewtest.FileChunk", prefixBytes) {
				if err != nil {
					t.Fatalf("idx.Scan error: %v", err)
				}
				k, _ := sds.ExtractKey(idxMsg, []int32{1, 2})
				dyn := idxMsg.(*dynamicpb.Message)
				t.Logf("[%s] idx.Scan row: index=%v key=%x", factory.name, dyn.Get(dyn.Descriptor().Fields().ByName("index")).Int(), k.Bytes())
			}

			scanChunks := make(map[int64]string)
			for msg, err := range v.Scan(ctx, "viewtest.FileChunk", prefixBytes) {
				if err != nil {
					t.Fatalf("Scan error: %v", err)
				}
				dyn := msg.(*dynamicpb.Message)
				idxVal := dyn.Get(dyn.Descriptor().Fields().ByName("index")).Int()
				shaVal := dyn.Get(dyn.Descriptor().Fields().ByName("sha256")).String()
				scanChunks[idxVal] = shaVal
			}

			if len(scanChunks) != 12 {
				t.Fatalf("expected 12 live chunks (0..11), got %d: %v", len(scanChunks), scanChunks)
			}
			if scanChunks[5] != "sha-chunk-5" {
				t.Fatalf("chunk 5 sha mismatch: %s", scanChunks[5])
			}
			if scanChunks[10] != "sha-chunk-10" {
				t.Fatalf("chunk 10 sha mismatch: %s", scanChunks[10])
			}
			if _, exists := scanChunks[13]; exists {
				t.Fatalf("deleted chunk 13 was returned in Scan")
			}

			// 5. Test point lookup via Get
			targetChunk := dynamicpb.NewMessage(chunkMD)
			targetChunk.Set(chunkMD.Fields().ByName("ino"), protoreflect.ValueOfInt64(ino))
			targetChunk.Set(chunkMD.Fields().ByName("index"), protoreflect.ValueOfInt64(10))
			k10, _ := sds.ExtractKey(targetChunk, []int32{1, 2})
			msg10, ok, err := v.Get(ctx, "viewtest.FileChunk", k10)
			if err != nil || !ok {
				t.Fatalf("Get chunk 10 failed: ok=%v err=%v", ok, err)
			}
			dyn10 := msg10.(*dynamicpb.Message)
			if dyn10.Get(dyn10.Descriptor().Fields().ByName("sha256")).String() != "sha-chunk-10" {
				t.Fatalf("chunk 10 sha mismatch on Get: %s", dyn10.Get(dyn10.Descriptor().Fields().ByName("sha256")).String())
			}

			// Get deleted chunk 13 -> must return !ok
			targetChunk.Set(chunkMD.Fields().ByName("index"), protoreflect.ValueOfInt64(13))
			k13, _ := sds.ExtractKey(targetChunk, []int32{1, 2})
			_, ok13, _ := v.Get(ctx, "viewtest.FileChunk", k13)
			if ok13 {
				t.Fatalf("Get chunk 13 returned true for deleted chunk")
			}

			// 6. Flush all and verify again
			if err := v.Flush(ctx); err != nil {
				t.Fatalf("Flush failed: %v", err)
			}

			scanChunksAfter := make(map[int64]string)
			for msg, err := range v.Scan(ctx, "viewtest.FileChunk", prefixBytes) {
				if err != nil {
					t.Fatalf("Scan error after flush: %v", err)
				}
				dyn := msg.(*dynamicpb.Message)
				idxVal := dyn.Get(dyn.Descriptor().Fields().ByName("index")).Int()
				shaVal := dyn.Get(dyn.Descriptor().Fields().ByName("sha256")).String()
				scanChunksAfter[idxVal] = shaVal
			}

			if len(scanChunksAfter) != 12 {
				t.Fatalf("expected 12 live chunks after flush, got %d", len(scanChunksAfter))
			}
		})
	}
}
