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

package sqlite

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/gke-labs/in-cluster-storage/pkg/objectstore/inmemorystorage"
	"github.com/gke-labs/in-cluster-storage/pkg/sds"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/dynamicpb"
)

type testPayloadAppender struct {
	payloads [][]byte
}

func (a *testPayloadAppender) Append(_ context.Context, p []byte) (uint64, error) {
	cp := make([]byte, len(p))
	copy(cp, p)
	a.payloads = append(a.payloads, cp)
	return uint64(len(a.payloads)), nil
}

func testBuildUserMD(t *testing.T) protoreflect.MessageDescriptor {
	t.Helper()
	msgProto := &descriptorpb.DescriptorProto{
		Name: proto.String("User"),
		Field: []*descriptorpb.FieldDescriptorProto{
			{
				Name:   proto.String("id"),
				Number: proto.Int32(1),
				Type:   descriptorpb.FieldDescriptorProto_TYPE_INT64.Enum(),
				Label:  descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum(),
			},
			{
				Name:   proto.String("name"),
				Number: proto.Int32(2),
				Type:   descriptorpb.FieldDescriptorProto_TYPE_STRING.Enum(),
				Label:  descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum(),
			},
		},
	}
	mainFile := &descriptorpb.FileDescriptorProto{
		Name:        proto.String("test.proto"),
		Package:     proto.String("testpkg"),
		MessageType: []*descriptorpb.DescriptorProto{msgProto},
		Syntax:      proto.String("proto3"),
	}
	fds := &descriptorpb.FileDescriptorSet{File: []*descriptorpb.FileDescriptorProto{mainFile}}
	files, err := protodesc.NewFiles(fds)
	if err != nil {
		t.Fatalf("protodesc.NewFiles failed: %v", err)
	}
	d, err := files.FindDescriptorByName("testpkg.User")
	if err != nil {
		t.Fatalf("FindDescriptorByName failed: %v", err)
	}
	return d.(protoreflect.MessageDescriptor)
}

func TestFactory_NewEmpty_MkdirError(t *testing.T) {
	ctx := t.Context()
	f := NewFactory()

	// Create a regular file so MkdirAll fails
	fileAsDir := filepath.Join(t.TempDir(), "not-a-dir")
	if err := os.WriteFile(fileAsDir, []byte("x"), 0644); err != nil {
		t.Fatalf("WriteFile failed: %v", err)
	}

	_, err := f.NewEmpty(ctx, "stream-1", fileAsDir)
	if err == nil {
		t.Fatal("expected error from NewEmpty with invalid dir, got nil")
	}
}

func TestFactory_NewEmpty_StaleFileRemovalError(t *testing.T) {
	ctx := t.Context()
	f := NewFactory()

	dir := filepath.Join(t.TempDir(), "dbdir")
	if err := os.MkdirAll(dir, 0755); err != nil {
		t.Fatalf("MkdirAll failed: %v", err)
	}

	// Create a non-empty directory at metadata.sqlite path so os.Remove fails
	badStalePath := filepath.Join(dir, "metadata.sqlite")
	if err := os.MkdirAll(filepath.Join(badStalePath, "child"), 0755); err != nil {
		t.Fatalf("MkdirAll failed: %v", err)
	}

	_, err := f.NewEmpty(ctx, "stream-1", dir)
	if err == nil {
		t.Fatal("expected error from NewEmpty when stale file removal fails, got nil")
	}
}

func TestFactory_RestoreSnapshot_MkdirError(t *testing.T) {
	ctx := t.Context()
	f := NewFactory()
	backend := inmemorystorage.New()

	fileAsDir := filepath.Join(t.TempDir(), "not-a-dir")
	if err := os.WriteFile(fileAsDir, []byte("x"), 0644); err != nil {
		t.Fatalf("WriteFile failed: %v", err)
	}

	key := SnapshotKey("stream-1", 10)
	_, _, err := f.RestoreSnapshot(ctx, backend, "stream-1", key, fileAsDir)
	if err == nil {
		t.Fatal("expected error from RestoreSnapshot with invalid dir, got nil")
	}
}

func TestFactory_RestoreSnapshot_StaleFileRemovalError(t *testing.T) {
	ctx := t.Context()
	f := NewFactory()
	backend := inmemorystorage.New()

	dir := filepath.Join(t.TempDir(), "restore-dir")
	if err := os.MkdirAll(dir, 0755); err != nil {
		t.Fatalf("MkdirAll failed: %v", err)
	}

	// Create a non-empty directory at metadata.sqlite path so os.Remove fails
	badStalePath := filepath.Join(dir, "metadata.sqlite")
	if err := os.MkdirAll(filepath.Join(badStalePath, "child"), 0755); err != nil {
		t.Fatalf("MkdirAll failed: %v", err)
	}

	key := SnapshotKey("stream-1", 10)
	_, _, err := f.RestoreSnapshot(ctx, backend, "stream-1", key, dir)
	if err == nil {
		t.Fatal("expected error from RestoreSnapshot when existing database removal fails, got nil")
	}
}

func TestRemoveDatabaseFiles(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "metadata.sqlite")
	walPath := dbPath + "-wal"
	shmPath := dbPath + "-shm"

	if err := os.WriteFile(dbPath, []byte("db"), 0644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(walPath, []byte("wal"), 0644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(shmPath, []byte("shm"), 0644); err != nil {
		t.Fatal(err)
	}

	if err := removeDatabaseFiles(dbPath); err != nil {
		t.Fatalf("removeDatabaseFiles failed: %v", err)
	}

	for _, p := range []string{dbPath, walPath, shmPath} {
		if _, err := os.Stat(p); !os.IsNotExist(err) {
			t.Errorf("expected %s to be removed, stat err: %v", p, err)
		}
	}
}

func TestFactory_RestoreSnapshot_RemovesStaleWAL(t *testing.T) {
	ctx := t.Context()
	backend := inmemorystorage.New()
	dir := t.TempDir()

	// Upload a dummy snapshot
	dummyDBPath := filepath.Join(t.TempDir(), "source.sqlite")
	sourceDB, err := Open(ctx, dummyDBPath, WithStreamID("stream-1"))
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer sourceDB.Close()

	key, pos, err := PublishSnapshot(ctx, sourceDB, backend, t.TempDir())
	if err != nil {
		t.Fatalf("PublishSnapshot failed: %v", err)
	}

	// Drop junk metadata.sqlite-wal and metadata.sqlite-shm into target dir
	walPath := filepath.Join(dir, "metadata.sqlite-wal")
	shmPath := filepath.Join(dir, "metadata.sqlite-shm")
	if err := os.WriteFile(walPath, []byte("junk wal frames"), 0644); err != nil {
		t.Fatalf("WriteFile failed: %v", err)
	}
	if err := os.WriteFile(shmPath, []byte("junk shm"), 0644); err != nil {
		t.Fatalf("WriteFile failed: %v", err)
	}

	f := NewFactory()
	db, restoredPos, err := f.RestoreSnapshot(ctx, backend, "stream-1", key, dir)
	if err != nil {
		t.Fatalf("RestoreSnapshot failed: %v", err)
	}
	defer db.Close()

	if restoredPos != pos {
		t.Errorf("restoredPos = %d, want %d", restoredPos, pos)
	}

	// Stale WAL file with junk content must be gone
	if data, err := os.ReadFile(walPath); err == nil {
		if string(data) == "junk wal frames" {
			t.Errorf("stale wal file with junk frames was not removed")
		}
	}
	if data, err := os.ReadFile(shmPath); err == nil {
		if string(data) == "junk shm" {
			t.Errorf("stale shm file with junk frames was not removed")
		}
	}
}

func TestLoadPosition_EmptyTable(t *testing.T) {
	ctx := t.Context()
	dbPath := filepath.Join(t.TempDir(), "pos_test.sqlite")
	db, err := Open(ctx, dbPath, WithStreamID("stream-1"))
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer db.Close()

	// When _stream_position table is empty, loadPosition returns found=false, nil
	sID, pos, found, err := db.loadPosition(ctx)
	if err != nil {
		t.Fatalf("loadPosition failed: %v", err)
	}
	if found || sID != "" || pos != 0 {
		t.Errorf("unexpected loadPosition result: sID=%s, pos=%d, found=%v", sID, pos, found)
	}

	// Set position and verify it returns found=true
	if err := db.SetPosition(42); err != nil {
		t.Fatalf("SetPosition failed: %v", err)
	}
	sID, pos, found, err = db.loadPosition(ctx)
	if err != nil {
		t.Fatalf("loadPosition failed: %v", err)
	}
	if !found || sID != "stream-1" || pos != 42 {
		t.Errorf("unexpected loadPosition after SetPosition: sID=%s, pos=%d, found=%v", sID, pos, found)
	}
}

func TestRestoreSnapshotKey_DownloadFailureCleansUp(t *testing.T) {
	ctx := t.Context()
	backend := inmemorystorage.New()

	targetDir := t.TempDir()
	targetPath := filepath.Join(targetDir, "restored.sqlite")

	key := SnapshotKey("stream-test", 42)
	// Key does not exist in backend, so download fails
	_, _, err := RestoreSnapshotKey(ctx, backend, "stream-test", key, targetPath)
	if err == nil {
		t.Fatal("expected download failure, got nil")
	}

	// Verify incomplete target file was removed
	if _, statErr := os.Stat(targetPath); !os.IsNotExist(statErr) {
		t.Fatalf("expected incomplete file %s to be removed, stat err: %v", targetPath, statErr)
	}
}

func TestBuildSnapshot_FeedFailureCleansUp(t *testing.T) {
	ctx := t.Context()
	backend := inmemorystorage.New()
	tempDir := t.TempDir()

	// Corrupt payload causes Feed to fail
	invalidPayloads := [][]byte{[]byte("corrupt-not-a-valid-sds-record")}

	_, _, err := BuildSnapshot(ctx, "stream-test", invalidPayloads, backend, tempDir)
	if err == nil {
		t.Fatal("expected feed failure from invalid payload, got nil")
	}

	// Verify temp directory has no leftover SQLite files
	entries, err := os.ReadDir(tempDir)
	if err != nil {
		t.Fatalf("ReadDir failed: %v", err)
	}
	if len(entries) != 0 {
		t.Fatalf("expected 0 files in tempDir after Feed failure, found %d", len(entries))
	}
}

func TestBuildSnapshot_PendingTransactionRejection(t *testing.T) {
	ctx := t.Context()
	backend := inmemorystorage.New()
	tempDir := t.TempDir()
	userMD := testBuildUserMD(t)

	appender := &testPayloadAppender{}
	writer := sds.NewWriter(appender)
	if _, err := writer.RegisterDescriptor(userMD, 1); err != nil {
		t.Fatalf("RegisterDescriptor failed: %v", err)
	}

	// Begin a transaction but do not commit it
	tx := writer.Begin()
	u := dynamicpb.NewMessage(userMD)
	u.Set(userMD.Fields().ByName("id"), protoreflect.ValueOfInt64(1))
	u.Set(userMD.Fields().ByName("name"), protoreflect.ValueOfString("Alice"))
	tx.Insert(ctx, u)

	_, _, err := BuildSnapshot(ctx, "stream-test", appender.payloads, backend, tempDir)
	if !errors.Is(err, ErrPendingTransaction) {
		t.Fatalf("expected ErrPendingTransaction, got %v", err)
	}

	entries, err := os.ReadDir(tempDir)
	if err != nil {
		t.Fatalf("ReadDir failed: %v", err)
	}
	if len(entries) != 0 {
		t.Fatalf("expected 0 leftover files in tempDir, found %d", len(entries))
	}
}

func TestPublishSnapshot_PendingTransactionRejection(t *testing.T) {
	ctx := t.Context()
	backend := inmemorystorage.New()
	dbDir := t.TempDir()
	dbPath := filepath.Join(dbDir, "test.sqlite")
	userMD := testBuildUserMD(t)

	db, err := Open(ctx, dbPath, WithStreamID("stream-test"))
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer db.Close()

	appender := &testPayloadAppender{}
	writer := sds.NewWriter(appender)
	if _, err := writer.RegisterDescriptor(userMD, 1); err != nil {
		t.Fatalf("RegisterDescriptor failed: %v", err)
	}
	if err := db.SyncRegistry(ctx, writer.Registry()); err != nil {
		t.Fatalf("SyncRegistry failed: %v", err)
	}

	// Begin an uncommitted transaction and feed to DB
	tx := writer.Begin()
	u := dynamicpb.NewMessage(userMD)
	u.Set(userMD.Fields().ByName("id"), protoreflect.ValueOfInt64(1))
	u.Set(userMD.Fields().ByName("name"), protoreflect.ValueOfString("Alice"))
	tx.Insert(ctx, u)

	for i, p := range appender.payloads {
		if _, err := db.Feed(ctx, uint64(i+1), p); err != nil {
			t.Fatalf("Feed failed: %v", err)
		}
	}

	snapDir := t.TempDir()
	_, _, err = PublishSnapshot(ctx, db, backend, snapDir)
	if !errors.Is(err, ErrPendingTransaction) {
		t.Fatalf("expected ErrPendingTransaction, got %v", err)
	}

	entries, err := os.ReadDir(snapDir)
	if err != nil {
		t.Fatalf("ReadDir failed: %v", err)
	}
	if len(entries) != 0 {
		t.Fatalf("expected 0 leftover files in snapDir, found %d", len(entries))
	}
}

func TestDB_Close_StatementsAndConnection(t *testing.T) {
	ctx := t.Context()
	dbPath := filepath.Join(t.TempDir(), "close_test.sqlite")
	userMD := testBuildUserMD(t)

	db, err := Open(ctx, dbPath, WithStreamID("stream-test"))
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	appender := &testPayloadAppender{}
	writer := sds.NewWriter(appender)
	if _, err := writer.RegisterDescriptor(userMD, 1); err != nil {
		t.Fatalf("RegisterDescriptor failed: %v", err)
	}
	if err := db.SyncRegistry(ctx, writer.Registry()); err != nil {
		t.Fatalf("SyncRegistry failed: %v", err)
	}

	// Ensure prepared statements are allocated
	tableName := TableName("testpkg.User")
	db.mu.Lock()
	stmts, err := db.ensureTableStmtsLocked(ctx, tableName)
	db.mu.Unlock()
	if err != nil {
		t.Fatalf("ensureTableStmtsLocked failed: %v", err)
	}
	if stmts == nil {
		t.Fatal("expected non-nil stmts")
	}

	if err := db.Close(); err != nil {
		t.Fatalf("db.Close failed: %v", err)
	}

	// Second close on already-closed DB is safe
	_ = db.Close()
}
