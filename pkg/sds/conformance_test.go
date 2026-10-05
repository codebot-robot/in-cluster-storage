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

package sds_test

import (
	"bytes"
	"context"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"testing"

	pb "github.com/gke-labs/in-cluster-storage/pkg/api/objectfs/v1alpha1"
	"github.com/gke-labs/in-cluster-storage/pkg/objectstore"
	"github.com/gke-labs/in-cluster-storage/pkg/objectstore/inmemorystorage"
	"github.com/gke-labs/in-cluster-storage/pkg/sds"
	"github.com/gke-labs/in-cluster-storage/pkg/sds/memtable"
	"github.com/gke-labs/in-cluster-storage/pkg/sds/projection/sqlite"
	"github.com/google/uuid"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/dynamicpb"
)

type localIndexFactory struct {
	name    string
	create  func(t *testing.T, ctx context.Context, streamID string) (sds.LocalIndex, func())
	publish func(ctx context.Context, idx sds.LocalIndex, backend objectstore.Backend) (string, uint64, error)
	restore func(ctx context.Context, backend objectstore.Backend, streamID string, maxPos uint64, targetDir string) (sds.LocalIndex, uint64, error)
}

func localIndexFactories(t *testing.T) []localIndexFactory {
	return []localIndexFactory{
		{
			name: "SQLite",
			create: func(t *testing.T, ctx context.Context, streamID string) (sds.LocalIndex, func()) {
				dbDir := t.TempDir()
				dbPath := filepath.Join(dbDir, "index.sqlite")
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
			publish: func(ctx context.Context, idx sds.LocalIndex, backend objectstore.Backend) (string, uint64, error) {
				return sqlite.PublishSnapshot(ctx, idx.(*sqlite.DB), backend, os.TempDir())
			},
			restore: func(ctx context.Context, backend objectstore.Backend, streamID string, maxPos uint64, targetDir string) (sds.LocalIndex, uint64, error) {
				dbPath := filepath.Join(targetDir, "restored.sqlite")
				return sqlite.RestoreSnapshot(ctx, backend, streamID, maxPos, dbPath)
			},
		},
		{
			name: "MemTable",
			create: func(t *testing.T, ctx context.Context, streamID string) (sds.LocalIndex, func()) {
				store := memtable.New(memtable.WithStreamID(streamID))
				return store, func() { _ = store.Close() }
			},
			publish: func(ctx context.Context, idx sds.LocalIndex, backend objectstore.Backend) (string, uint64, error) {
				return memtable.PublishSnapshot(ctx, idx.(*memtable.MemStore), backend, os.TempDir())
			},
			restore: func(ctx context.Context, backend objectstore.Backend, streamID string, maxPos uint64, targetDir string) (sds.LocalIndex, uint64, error) {
				return memtable.RestoreSnapshot(ctx, backend, streamID, maxPos, targetDir)
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
		Package:     proto.String("testpkg"),
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
	d, err := files.FindDescriptorByName(protoreflect.FullName("testpkg." + name))
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

// TestLocalIndexRandomLogsAndReplayConformance tests random change streams applied in batches vs full replay.
func TestLocalIndexRandomLogsAndReplayConformance(t *testing.T) {
	for _, factory := range localIndexFactories(t) {
		t.Run(factory.name, func(t *testing.T) {
			ctx := t.Context()
			streamID := uuid.New().String()

			transport := &memoryTransport{}
			writer := sds.NewWriter(transport)

			accountMD := dynamicDescriptor(t, "Account", []*descriptorpb.FieldDescriptorProto{
				protoField("account_id", 1, descriptorpb.FieldDescriptorProto_TYPE_INT64, descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL),
				protoField("owner", 2, descriptorpb.FieldDescriptorProto_TYPE_STRING, descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL),
				protoField("balance", 3, descriptorpb.FieldDescriptorProto_TYPE_DOUBLE, descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL),
			})
			_, err := writer.RegisterDescriptor(accountMD, 1)
			if err != nil {
				t.Fatalf("RegisterDescriptor failed: %v", err)
			}

			rng := rand.New(rand.NewSource(42))
			numAccounts := 30
			numOps := 150
			accountsMap := make(map[int64]*dynamicpb.Message)

			for i := 0; i < numOps; i++ {
				accID := int64(rng.Intn(numAccounts) + 1)
				bal := float64(rng.Intn(10000)) / 100.0

				acc := dynamicpb.NewMessage(accountMD)
				acc.Set(accountMD.Fields().ByName("account_id"), protoreflect.ValueOfInt64(accID))
				acc.Set(accountMD.Fields().ByName("owner"), protoreflect.ValueOfString(fmt.Sprintf("User-%d", accID)))
				acc.Set(accountMD.Fields().ByName("balance"), protoreflect.ValueOfFloat64(bal))

				oldAcc, exists := accountsMap[accID]
				if !exists {
					_, _ = writer.Create(ctx, acc)
					accountsMap[accID] = acc
				} else {
					if rng.Float64() < 0.7 {
						_, _ = writer.Update(ctx, oldAcc, acc)
						accountsMap[accID] = acc
					} else {
						_, _ = writer.Delete(ctx, oldAcc)
						delete(accountsMap, accID)
					}
				}
			}

			// Play into index 1 incrementally in batches
			idx1, cleanup1 := factory.create(t, ctx, streamID)
			defer cleanup1()
			if err := idx1.SyncRegistry(ctx, writer.Registry()); err != nil {
				t.Fatalf("SyncRegistry failed: %v", err)
			}

			reader1 := sds.NewChangeReader()
			for i, p := range transport.payloads {
				changes, err := reader1.Feed(uint64(i+1), p)
				if err != nil {
					t.Fatalf("reader1.Feed failed: %v", err)
				}
				if len(changes) > 0 {
					if err := idx1.ApplyBatch(ctx, changes); err != nil {
						t.Fatalf("idx1.ApplyBatch failed: %v", err)
					}
				}
			}

			// Play into index 2 all at once (full replay)
			idx2, cleanup2 := factory.create(t, ctx, streamID)
			defer cleanup2()
			if err := idx2.SyncRegistry(ctx, writer.Registry()); err != nil {
				t.Fatalf("SyncRegistry failed: %v", err)
			}

			reader2 := sds.NewChangeReader()
			var allChanges []sds.Change
			for i, p := range transport.payloads {
				changes, err := reader2.Feed(uint64(i+1), p)
				if err != nil {
					t.Fatalf("reader2.Feed failed: %v", err)
				}
				allChanges = append(allChanges, changes...)
			}
			if err := idx2.ApplyBatch(ctx, allChanges); err != nil {
				t.Fatalf("idx2.ApplyBatch failed: %v", err)
			}

			if idx1.Position() != idx2.Position() {
				t.Fatalf("Position mismatch: idx1 %d != idx2 %d", idx1.Position(), idx2.Position())
			}

			// Compare scan results between idx1 and idx2
			var rows1 []proto.Message
			for msg, err := range idx1.Scan(ctx, "testpkg.Account", nil) {
				if err != nil {
					t.Fatalf("idx1.Scan failed: %v", err)
				}
				rows1 = append(rows1, msg)
			}

			var rows2 []proto.Message
			for msg, err := range idx2.Scan(ctx, "testpkg.Account", nil) {
				if err != nil {
					t.Fatalf("idx2.Scan failed: %v", err)
				}
				rows2 = append(rows2, msg)
			}

			if len(rows1) != len(rows2) {
				t.Fatalf("Row count mismatch: idx1 has %d rows, idx2 has %d rows", len(rows1), len(rows2))
			}

			for i := range rows1 {
				k1, _ := sds.ExtractKey(rows1[i], []int32{1})
				k2, _ := sds.ExtractKey(rows2[i], []int32{1})
				if k1.String() != k2.String() {
					t.Fatalf("Row %d key mismatch: %v vs %v", i, k1, k2)
				}
			}
		})
	}
}

// TestLocalIndexIdempotentSuffixReapply tests that re-applying an already applied suffix is idempotent.
func TestLocalIndexIdempotentSuffixReapply(t *testing.T) {
	for _, factory := range localIndexFactories(t) {
		t.Run(factory.name, func(t *testing.T) {
			ctx := t.Context()
			streamID := uuid.New().String()

			transport := &memoryTransport{}
			writer := sds.NewWriter(transport)

			itemMD := dynamicDescriptor(t, "Item", []*descriptorpb.FieldDescriptorProto{
				protoField("id", 1, descriptorpb.FieldDescriptorProto_TYPE_INT64, descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL),
				protoField("title", 2, descriptorpb.FieldDescriptorProto_TYPE_STRING, descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL),
			})
			_, err := writer.RegisterDescriptor(itemMD, 1)
			if err != nil {
				t.Fatalf("RegisterDescriptor failed: %v", err)
			}

			for i := 1; i <= 10; i++ {
				it := dynamicpb.NewMessage(itemMD)
				it.Set(itemMD.Fields().ByName("id"), protoreflect.ValueOfInt64(int64(i)))
				it.Set(itemMD.Fields().ByName("title"), protoreflect.ValueOfString(fmt.Sprintf("Item %d", i)))
				writer.Create(ctx, it)
			}

			idx, cleanup := factory.create(t, ctx, streamID)
			defer cleanup()
			_ = idx.SyncRegistry(ctx, writer.Registry())

			reader := sds.NewChangeReader()
			var changesList [][]sds.Change
			for i, p := range transport.payloads {
				changes, _ := reader.Feed(uint64(i+1), p)
				changesList = append(changesList, changes)
				if len(changes) > 0 {
					_ = idx.ApplyBatch(ctx, changes)
				}
			}

			posBefore := idx.Position()

			// Re-apply the last 5 changes
			for i := 5; i < len(changesList); i++ {
				if len(changesList[i]) > 0 {
					if err := idx.ApplyBatch(ctx, changesList[i]); err != nil {
						t.Fatalf("re-applying changes failed: %v", err)
					}
				}
			}

			if idx.Position() != posBefore {
				t.Fatalf("Position changed after suffix reapply: %d vs %d", idx.Position(), posBefore)
			}

			// Verify row count is unchanged (10 items)
			count := 0
			for _, err := range idx.Scan(ctx, "testpkg.Item", nil) {
				if err != nil {
					t.Fatalf("Scan failed: %v", err)
				}
				count++
			}
			if count != 10 {
				t.Fatalf("expected 10 items, got %d", count)
			}
		})
	}
}

// TestLocalIndexScanOrderAndPrefix tests canonical key-byte ordering and prefix scanning.
func TestLocalIndexScanOrderAndPrefix(t *testing.T) {
	for _, factory := range localIndexFactories(t) {
		t.Run(factory.name, func(t *testing.T) {
			ctx := t.Context()
			streamID := uuid.New().String()

			transport := &memoryTransport{}
			writer := sds.NewWriter(transport)

			dirEntryMD := dynamicDescriptor(t, "DirEntry", []*descriptorpb.FieldDescriptorProto{
				protoField("parent_id", 1, descriptorpb.FieldDescriptorProto_TYPE_INT64, descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL),
				protoField("name", 2, descriptorpb.FieldDescriptorProto_TYPE_STRING, descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL),
				protoField("ino", 3, descriptorpb.FieldDescriptorProto_TYPE_INT64, descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL),
			})
			_, err := writer.RegisterDescriptor(dirEntryMD, 1, 2)
			if err != nil {
				t.Fatalf("RegisterDescriptor failed: %v", err)
			}

			p1Names := []string{"alpha.txt", "beta.txt", "gamma.txt"}
			for _, name := range p1Names {
				de := dynamicpb.NewMessage(dirEntryMD)
				de.Set(dirEntryMD.Fields().ByName("parent_id"), protoreflect.ValueOfInt64(1))
				de.Set(dirEntryMD.Fields().ByName("name"), protoreflect.ValueOfString(name))
				de.Set(dirEntryMD.Fields().ByName("ino"), protoreflect.ValueOfInt64(10))
				writer.Create(ctx, de)
			}

			p2Names := []string{"foo.txt", "bar.txt"}
			for _, name := range p2Names {
				de := dynamicpb.NewMessage(dirEntryMD)
				de.Set(dirEntryMD.Fields().ByName("parent_id"), protoreflect.ValueOfInt64(2))
				de.Set(dirEntryMD.Fields().ByName("name"), protoreflect.ValueOfString(name))
				de.Set(dirEntryMD.Fields().ByName("ino"), protoreflect.ValueOfInt64(20))
				writer.Create(ctx, de)
			}

			idx, cleanup := factory.create(t, ctx, streamID)
			defer cleanup()
			_ = idx.SyncRegistry(ctx, writer.Registry())

			reader := sds.NewChangeReader()
			for i, p := range transport.payloads {
				changes, _ := reader.Feed(uint64(i+1), p)
				if len(changes) > 0 {
					_ = idx.ApplyBatch(ctx, changes)
				}
			}

			// Prefix query for parent_id = 1
			p1PrefixMsg := dynamicpb.NewMessage(dirEntryMD)
			p1PrefixMsg.Set(dirEntryMD.Fields().ByName("parent_id"), protoreflect.ValueOfInt64(1))
			p1Prefix, err := sds.EncodeKeyPrefix(p1PrefixMsg, 1)
			if err != nil {
				t.Fatalf("EncodeKeyPrefix failed: %v", err)
			}

			var p1GotNames []string
			var lastKeyBytes []byte
			for msg, err := range idx.Scan(ctx, "testpkg.DirEntry", p1Prefix) {
				if err != nil {
					t.Fatalf("Scan failed: %v", err)
				}
				dyn := msg.(*dynamicpb.Message)
				name := dyn.Get(dyn.Descriptor().Fields().ByName("name")).String()
				p1GotNames = append(p1GotNames, name)

				k, _ := sds.ExtractKey(msg, []int32{1, 2})
				if len(lastKeyBytes) > 0 && bytes.Compare(lastKeyBytes, k.Bytes()) >= 0 {
					t.Fatalf("Scan results not in strict canonical key-byte order! %v >= %v", lastKeyBytes, k.Bytes())
				}
				lastKeyBytes = k.Bytes()
			}

			if len(p1GotNames) != 3 {
				t.Fatalf("expected 3 entries for parent 1, got %d", len(p1GotNames))
			}
		})
	}
}

// TestLocalIndexSnapshotPublishRestoreAndReplay tests snapshot publish, restore and replaying remainder.
func TestLocalIndexSnapshotPublishRestoreAndReplay(t *testing.T) {
	for _, factory := range localIndexFactories(t) {
		t.Run(factory.name, func(t *testing.T) {
			ctx := t.Context()
			streamID := uuid.New().String()
			backend := inmemorystorage.New()

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

			// Step 1: Write users 1..5
			for i := 1; i <= 5; i++ {
				u := dynamicpb.NewMessage(userMD)
				u.Set(userMD.Fields().ByName("id"), protoreflect.ValueOfInt64(int64(i)))
				u.Set(userMD.Fields().ByName("name"), protoreflect.ValueOfString(fmt.Sprintf("User-%d", i)))
				writer.Create(ctx, u)
			}

			liveIdx, cleanupLive := factory.create(t, ctx, streamID)
			defer cleanupLive()
			_ = liveIdx.SyncRegistry(ctx, writer.Registry())

			reader := sds.NewChangeReader()
			for i, p := range transport.payloads {
				changes, _ := reader.Feed(uint64(i+1), p)
				if len(changes) > 0 {
					_ = liveIdx.ApplyBatch(ctx, changes)
				}
			}

			snapKey, snapPos, err := factory.publish(ctx, liveIdx, backend)
			if err != nil {
				t.Fatalf("publish failed: %v", err)
			}
			if snapKey == "" || snapPos == 0 {
				t.Fatalf("invalid published snapshot key %q pos %d", snapKey, snapPos)
			}

			// Step 2: Write users 6..10
			for i := 6; i <= 10; i++ {
				u := dynamicpb.NewMessage(userMD)
				u.Set(userMD.Fields().ByName("id"), protoreflect.ValueOfInt64(int64(i)))
				u.Set(userMD.Fields().ByName("name"), protoreflect.ValueOfString(fmt.Sprintf("User-%d", i)))
				writer.Create(ctx, u)
			}

			// Apply to live index
			for i := int(snapPos); i < len(transport.payloads); i++ {
				changes, _ := reader.Feed(uint64(i+1), transport.payloads[i])
				if len(changes) > 0 {
					_ = liveIdx.ApplyBatch(ctx, changes)
				}
			}

			// Step 3: Restore snapshot into a new index and replay forward from snapPos
			restoredDir := t.TempDir()
			restoredIdx, restoredPos, err := factory.restore(ctx, backend, streamID, snapPos, restoredDir)
			if err != nil {
				t.Fatalf("restore failed: %v", err)
			}
			if restoredPos != snapPos {
				t.Fatalf("restoredPos %d != snapPos %d", restoredPos, snapPos)
			}

			replayReader := sds.NewChangeReader()
			_ = replayReader.Registry().Import(writer.Registry().Export())
			for i := int(snapPos); i < len(transport.payloads); i++ {
				changes, _ := replayReader.Feed(uint64(i+1), transport.payloads[i])
				if len(changes) > 0 {
					if err := restoredIdx.ApplyBatch(ctx, changes); err != nil {
						t.Fatalf("restoredIdx.ApplyBatch failed: %v", err)
					}
				}
			}

			// Compare live state vs restored + replayed state
			var liveRows []proto.Message
			for msg, err := range liveIdx.Scan(ctx, "testpkg.User", nil) {
				if err != nil {
					t.Fatalf("liveIdx.Scan failed: %v", err)
				}
				liveRows = append(liveRows, msg)
			}

			var restoredRows []proto.Message
			for msg, err := range restoredIdx.Scan(ctx, "testpkg.User", nil) {
				if err != nil {
					t.Fatalf("restoredIdx.Scan failed: %v", err)
				}
				restoredRows = append(restoredRows, msg)
			}

			if len(liveRows) != 10 || len(restoredRows) != 10 {
				t.Fatalf("expected 10 rows each, got live=%d restored=%d", len(liveRows), len(restoredRows))
			}
		})
	}
}

// TestDeltaCompleteFoldingConformance verifies that a consumer can compute
// row counts and field sums (e.g. sum of Inode.size) solely by folding the log
// of OpRecords with before-images, matching a full table scan in both MemTable and SQLite projections.
func TestDeltaCompleteFoldingConformance(t *testing.T) {
	for _, factory := range localIndexFactories(t) {
		t.Run(factory.name, func(t *testing.T) {
			ctx := t.Context()
			streamID := uuid.New().String()

			transport := &memoryTransport{}
			writer := sds.NewWriter(transport)

			if _, err := writer.RegisterTypeWithOptions(&pb.Inode{}, sds.WithKeyFields(1), sds.WithLogBeforeImages(true)); err != nil {
				t.Fatalf("Register Inode failed: %v", err)
			}
			if _, err := writer.RegisterTypeWithOptions(&pb.DirEntry{}, sds.WithKeyFields(1, 2)); err != nil {
				t.Fatalf("Register DirEntry failed: %v", err)
			}
			if _, err := writer.RegisterTypeWithOptions(&pb.FileChunk{}, sds.WithKeyFields(1, 2), sds.WithLogBeforeImages(true)); err != nil {
				t.Fatalf("Register FileChunk failed: %v", err)
			}

			// Perform an extensive sequence of filesystem lifecycle mutations:
			// 1. Root dir (ino 1)
			rootDir := &pb.Inode{Ino: proto.Uint64(1), IsDir: true, Nlink: 2, Mode: 0755}
			writer.Create(ctx, rootDir)

			// 2. Dir "docs" (ino 2)
			txMkdir := writer.Begin()
			docsDir := &pb.Inode{Ino: proto.Uint64(2), IsDir: true, Nlink: 2, Mode: 0755, ParentIno: proto.Uint64(1)}
			docsEntry := &pb.DirEntry{ParentIno: proto.Uint64(1), Name: proto.String("docs"), Ino: 2, IsDir: true}
			rootDirUp1 := proto.Clone(rootDir).(*pb.Inode)
			rootDirUp1.Nlink = 3
			txMkdir.Create(ctx, docsDir)
			txMkdir.Create(ctx, docsEntry)
			txMkdir.Update(ctx, rootDir, rootDirUp1)
			txMkdir.Commit(ctx)
			rootDir = rootDirUp1

			// 3. Create file1 (ino 3, size 100) with chunk 0
			txFile1 := writer.Begin()
			file1 := &pb.Inode{Ino: proto.Uint64(3), Size: 100, Nlink: 1, Mode: 0644, ParentIno: proto.Uint64(1)}
			file1Entry := &pb.DirEntry{ParentIno: proto.Uint64(1), Name: proto.String("file1.txt"), Ino: 3}
			file1Chunk0 := &pb.FileChunk{Ino: proto.Uint64(3), Index: proto.Uint32(0), Sha256: "sha-file1-c0"}
			txFile1.Create(ctx, file1)
			txFile1.Create(ctx, file1Entry)
			txFile1.Create(ctx, file1Chunk0)
			txFile1.Commit(ctx)

			// 4. Create file2 (ino 4, size 250) with chunks 0, 1
			txFile2 := writer.Begin()
			file2 := &pb.Inode{Ino: proto.Uint64(4), Size: 250, Nlink: 1, Mode: 0644, ParentIno: proto.Uint64(1)}
			file2Entry := &pb.DirEntry{ParentIno: proto.Uint64(1), Name: proto.String("file2.txt"), Ino: 4}
			file2Chunk0 := &pb.FileChunk{Ino: proto.Uint64(4), Index: proto.Uint32(0), Sha256: "sha-file2-c0"}
			file2Chunk1 := &pb.FileChunk{Ino: proto.Uint64(4), Index: proto.Uint32(1), Sha256: "sha-file2-c1"}
			txFile2.Create(ctx, file2)
			txFile2.Create(ctx, file2Entry)
			txFile2.Create(ctx, file2Chunk0)
			txFile2.Create(ctx, file2Chunk1)
			txFile2.Commit(ctx)

			// 5. Update file1 (setattr / size change: 100 -> 150)
			file1Up1 := proto.Clone(file1).(*pb.Inode)
			file1Up1.Size = 150
			file1Up1.Mode = 0600
			writer.Update(ctx, file1, file1Up1)
			file1 = file1Up1

			// 6. Overwrite file1 chunk 0
			file1Chunk0Up := proto.Clone(file1Chunk0).(*pb.FileChunk)
			file1Chunk0Up.Sha256 = "sha-file1-c0-modified"
			writer.Update(ctx, file1Chunk0, file1Chunk0Up)
			file1Chunk0 = file1Chunk0Up

			// 7. Truncate file2 down to 50 bytes (deletes chunk 1, updates chunk 0 and inode)
			txTrunc := writer.Begin()
			file2Up1 := proto.Clone(file2).(*pb.Inode)
			file2Up1.Size = 50
			file2Chunk0Up := proto.Clone(file2Chunk0).(*pb.FileChunk)
			file2Chunk0Up.Sha256 = "sha-file2-c0-truncated"
			txTrunc.Update(ctx, file2, file2Up1)
			txTrunc.Update(ctx, file2Chunk0, file2Chunk0Up)
			txTrunc.Delete(ctx, file2Chunk1)
			txTrunc.Commit(ctx)
			file2 = file2Up1
			file2Chunk0 = file2Chunk0Up

			// 8. Rename file1.txt over existing file2.txt (target replaced)
			txRename := writer.Begin()
			txRename.Delete(ctx, file1Entry)  // delete old dir entry file1.txt
			txRename.Delete(ctx, file2Entry)  // delete target dir entry file2.txt
			txRename.Delete(ctx, file2)       // target unlinked / deleted
			txRename.Delete(ctx, file2Chunk0) // target chunks deleted
			newFile2Entry := &pb.DirEntry{ParentIno: proto.Uint64(1), Name: proto.String("file2.txt"), Ino: 3}
			txRename.Create(ctx, newFile2Entry)
			file1Up2 := proto.Clone(file1).(*pb.Inode)
			file1Up2.ParentIno = proto.Uint64(1)
			txRename.Update(ctx, file1, file1Up2)
			txRename.Commit(ctx)
			file1 = file1Up2

			// 9. Unlink docs dir
			txRmdir := writer.Begin()
			txRmdir.Delete(ctx, docsEntry)
			txRmdir.Delete(ctx, docsDir)
			rootDirUp2 := proto.Clone(rootDir).(*pb.Inode)
			rootDirUp2.Nlink = 2
			txRmdir.Update(ctx, rootDir, rootDirUp2)
			txRmdir.Commit(ctx)
			rootDir = rootDirUp2

			// Play the entire log into the projection and simultaneously fold metrics from the log alone
			idx, cleanup := factory.create(t, ctx, streamID)
			defer cleanup()
			if err := idx.SyncRegistry(ctx, writer.Registry()); err != nil {
				t.Fatalf("SyncRegistry failed: %v", err)
			}

			logRowCounts := make(map[string]int)
			var logInodeSizeSum int64

			reader := sds.NewChangeReader()
			for i, p := range transport.payloads {
				changes, err := reader.Feed(uint64(i+1), p)
				if err != nil {
					t.Fatalf("reader.Feed failed at seq %d: %v", i+1, err)
				}
				for _, ch := range changes {
					// Fold from log alone
					switch ch.Op {
					case sds.OpCreate:
						logRowCounts[ch.TypeName]++
						if inode, ok := ch.Row.(*pb.Inode); ok {
							logInodeSizeSum += inode.GetSize()
						}
					case sds.OpUpdate:
						if inodeAfter, ok := ch.Row.(*pb.Inode); ok {
							inodeBefore, ok := ch.Before.(*pb.Inode)
							if !ok {
								t.Fatalf("expected Inode in Before for update change: %v", ch)
							}
							logInodeSizeSum += (inodeAfter.GetSize() - inodeBefore.GetSize())
						}
					case sds.OpDelete:
						logRowCounts[ch.TypeName]--
						if inodeBefore, ok := ch.Before.(*pb.Inode); ok {
							logInodeSizeSum -= inodeBefore.GetSize()
						}
					}
				}
				if len(changes) > 0 {
					if err := idx.ApplyBatch(ctx, changes); err != nil {
						t.Fatalf("idx.ApplyBatch failed: %v", err)
					}
				}
			}

			// Validate folded aggregates against a full scan of the projection
			tables := []string{"objectfs.v1alpha1.Inode", "objectfs.v1alpha1.DirEntry", "objectfs.v1alpha1.FileChunk"}
			for _, tbl := range tables {
				var scanCount int
				for msg, err := range idx.Scan(ctx, tbl, nil) {
					if err != nil {
						t.Fatalf("Scan %s failed: %v", tbl, err)
					}
					if msg != nil {
						scanCount++
					}
				}
				expectedCount := logRowCounts[tbl]
				if scanCount != expectedCount {
					t.Errorf("table %s row count mismatch: folded from log = %d, scan of projection = %d", tbl, expectedCount, scanCount)
				}
			}

			// Validate Inode.size sum
			var scanInodeSizeSum int64
			for msg, err := range idx.Scan(ctx, "objectfs.v1alpha1.Inode", nil) {
				if err != nil {
					t.Fatalf("Scan Inode failed: %v", err)
				}
				inode := msg.(*pb.Inode)
				scanInodeSizeSum += inode.GetSize()
			}

			if logInodeSizeSum != scanInodeSizeSum {
				t.Errorf("Inode size sum mismatch: folded from log = %d, scan of projection = %d", logInodeSizeSum, scanInodeSizeSum)
			}
		})
	}
}
