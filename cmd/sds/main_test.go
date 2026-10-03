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

package main

import (
	"bytes"
	"context"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	objectfspb "github.com/gke-labs/in-cluster-storage/pkg/api/objectfs/v1alpha1"
	pb "github.com/gke-labs/in-cluster-storage/pkg/api/wal/v1alpha1"
	"github.com/gke-labs/in-cluster-storage/pkg/objectfs/controller"
	"github.com/gke-labs/in-cluster-storage/pkg/objectstore"
	"github.com/gke-labs/in-cluster-storage/pkg/objectstore/inmemorystorage"
	"github.com/gke-labs/in-cluster-storage/pkg/sds"
	"github.com/gke-labs/in-cluster-storage/pkg/sds/record"
	"github.com/gke-labs/in-cluster-storage/pkg/wal"
	"github.com/gke-labs/in-cluster-storage/pkg/wal/buffer"
	walclient "github.com/gke-labs/in-cluster-storage/pkg/wal/client"
	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/dynamicpb"
)

type testServerHandle struct {
	srv        *buffer.Server
	addr       string
	grpcServer *grpc.Server
	listener   net.Listener
}

func (h *testServerHandle) StopGraceful() {
	h.grpcServer.Stop()
	_ = h.srv.Close()
	_ = h.listener.Close()
}

func startBufferServer(t *testing.T, backend objectstore.Backend, dataDir string) *testServerHandle {
	t.Helper()
	ctx := t.Context()
	srv, err := buffer.NewServer(ctx, buffer.ServerConfig{
		Backend:       backend,
		DataDir:       dataDir,
		FlushInterval: 10 * time.Second,
		BatchMaxDelay: 2 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("failed to start buffer server: %v", err)
	}

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to listen: %v", err)
	}

	grpcServer := grpc.NewServer()
	pb.RegisterWalBufferServer(grpcServer, srv)

	go func() {
		_ = grpcServer.Serve(listener)
	}()

	return &testServerHandle{
		srv:        srv,
		addr:       listener.Addr().String(),
		grpcServer: grpcServer,
		listener:   listener,
	}
}

func executeCommand(ctx context.Context, args ...string) (string, error) {
	cmd := NewRootCommand()
	cmd.SetContext(ctx)
	buf := new(bytes.Buffer)
	cmd.SetOut(buf)
	cmd.SetErr(buf)
	cmd.SetArgs(args)
	err := cmd.Execute()
	return buf.String(), err
}

func buildDynamicMessageDescriptor(t *testing.T, name string, fields []*descriptorpb.FieldDescriptorProto) protoreflect.MessageDescriptor {
	t.Helper()
	msgProto := &descriptorpb.DescriptorProto{
		Name:  proto.String(name),
		Field: fields,
	}

	mainFile := &descriptorpb.FileDescriptorProto{
		Name:        proto.String(name + ".proto"),
		Package:     proto.String("testpkg"),
		MessageType: []*descriptorpb.DescriptorProto{msgProto},
		Syntax:      proto.String("proto3"),
	}

	fds := &descriptorpb.FileDescriptorSet{
		File: []*descriptorpb.FileDescriptorProto{mainFile},
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

func field(name string, number int32, typeKind descriptorpb.FieldDescriptorProto_Type, label descriptorpb.FieldDescriptorProto_Label) *descriptorpb.FieldDescriptorProto {
	return &descriptorpb.FieldDescriptorProto{
		Name:   proto.String(name),
		Number: proto.Int32(number),
		Type:   typeKind.Enum(),
		Label:  label.Enum(),
	}
}

func TestSdsCatLiveServer(t *testing.T) {
	backend := inmemorystorage.New()
	handle := startBufferServer(t, backend, t.TempDir())
	defer handle.StopGraceful()

	clientDir := t.TempDir()
	streamID := uuid.New()

	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()

	walStream, err := walclient.Open(ctx, clientDir, streamID, handle.addr)
	if err != nil {
		t.Fatalf("failed to open WAL stream: %v", err)
	}
	defer walStream.Close()

	appender := sds.NewWALAppender(walStream, walclient.Witness)
	writer := sds.NewWriter(appender)

	fields := []*descriptorpb.FieldDescriptorProto{
		field("id", 1, descriptorpb.FieldDescriptorProto_TYPE_INT64, descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL),
		field("customer", 2, descriptorpb.FieldDescriptorProto_TYPE_STRING, descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL),
	}
	md := buildDynamicMessageDescriptor(t, "Order", fields)
	if _, err := writer.RegisterDescriptor(md, 1); err != nil {
		t.Fatalf("RegisterDescriptor failed: %v", err)
	}

	order := dynamicpb.NewMessage(md)
	order.Set(md.Fields().ByName("id"), protoreflect.ValueOfInt64(10))
	order.Set(md.Fields().ByName("customer"), protoreflect.ValueOfString("Alice"))
	if _, err := writer.Insert(ctx, order); err != nil {
		t.Fatalf("Insert failed: %v", err)
	}

	// Run sds cat with timeout context (since live tail stays open)
	catCtx, catCancel := context.WithTimeout(t.Context(), 600*time.Millisecond)
	defer catCancel()

	output, _ := executeCommand(catCtx, "cat", "--server", handle.addr, "--stream", streamID.String())

	lines := strings.Split(strings.TrimSpace(output), "\n")
	if len(lines) < 2 {
		t.Fatalf("expected at least 2 output lines, got %d: %q", len(lines), output)
	}

	// Line 1: TypeDefinition (TypeID 1)
	if !strings.Contains(lines[0], "1\t1\tsds.v1.TypeDefinition") || !strings.Contains(lines[0], `"name":"testpkg.Order"`) {
		t.Errorf("unexpected line 0: %s", lines[0])
	}

	// Line 2: OpRecord (TypeID 5)
	if !strings.Contains(lines[1], "2\t5\tsds.v1.OpRecord") || !strings.Contains(lines[1], `"typeId":16`) {
		t.Errorf("unexpected line 1: %s", lines[1])
	}
}

func TestSdsCatSegmentAndRegistry(t *testing.T) {
	tempDir := t.TempDir()
	streamID := uuid.New()

	// Build a type and registry
	fields := []*descriptorpb.FieldDescriptorProto{
		field("id", 1, descriptorpb.FieldDescriptorProto_TYPE_INT64, descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL),
		field("name", 2, descriptorpb.FieldDescriptorProto_TYPE_STRING, descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL),
	}
	md := buildDynamicMessageDescriptor(t, "User", fields)

	reg := record.NewRegistry()
	def, err := reg.RegisterDescriptor(md, 1)
	if err != nil {
		t.Fatalf("RegisterDescriptor failed: %v", err)
	}

	// Export registry to file
	regProto := reg.Export()
	regBytes, err := proto.Marshal(regProto)
	if err != nil {
		t.Fatalf("Marshal registry failed: %v", err)
	}
	registryFile := filepath.Join(tempDir, "registry.pb")
	if err := os.WriteFile(registryFile, regBytes, 0644); err != nil {
		t.Fatalf("WriteFile failed: %v", err)
	}

	// Create a raw application record (typeID = 16)
	userMsg := dynamicpb.NewMessage(md)
	userMsg.Set(md.Fields().ByName("id"), protoreflect.ValueOfInt64(100))
	userMsg.Set(md.Fields().ByName("name"), protoreflect.ValueOfString("Charlie"))
	userBody, err := proto.Marshal(userMsg)
	if err != nil {
		t.Fatalf("Marshal userMsg failed: %v", err)
	}
	rawPayload, err := record.EncodeFrame(def.GetId(), userBody)
	if err != nil {
		t.Fatalf("EncodeFrame failed: %v", err)
	}

	// Write into a sealed log segment file ("WALL")
	segmentFile := filepath.Join(tempDir, "000000000001-000000000001.wal")
	logRec := &wal.LogRecord{
		Position:  1,
		StreamID:  streamID,
		StreamSeq: 1,
		Payload:   rawPayload,
	}
	if err := os.WriteFile(segmentFile, logRec.Encode(), 0644); err != nil {
		t.Fatalf("WriteFile segment failed: %v", err)
	}

	// 1. Cat segment WITH registry
	outWithReg, err := executeCommand(t.Context(), "cat", "--segment", segmentFile, "--registry", registryFile)
	if err != nil {
		t.Fatalf("cat with registry failed: %v", err)
	}
	lines := strings.Split(strings.TrimSpace(outWithReg), "\n")
	if len(lines) != 1 {
		t.Fatalf("expected 1 line, got %d: %q", len(lines), outWithReg)
	}
	if !strings.Contains(lines[0], "1\t16\ttestpkg.User") || !strings.Contains(lines[0], `"name":"Charlie"`) {
		t.Errorf("unexpected output with registry: %s", lines[0])
	}

	// 2. Cat segment WITHOUT registry (must print type_id and raw bytes without failing)
	outNoReg, err := executeCommand(t.Context(), "cat", "--segment", segmentFile)
	if err != nil {
		t.Fatalf("cat without registry failed: %v", err)
	}
	linesNoReg := strings.Split(strings.TrimSpace(outNoReg), "\n")
	if len(linesNoReg) != 1 {
		t.Fatalf("expected 1 line, got %d: %q", len(linesNoReg), outNoReg)
	}
	if !strings.Contains(linesNoReg[0], "1\t16\tunknown\t") {
		t.Errorf("unexpected output without registry: %s", linesNoReg[0])
	}

	// 3. Cat segment with --stream filter
	otherStreamID := uuid.New()
	outFilter, err := executeCommand(t.Context(), "cat", "--segment", segmentFile, "--stream", otherStreamID.String())
	if err != nil {
		t.Fatalf("cat with stream filter failed: %v", err)
	}
	if strings.TrimSpace(outFilter) != "" {
		t.Errorf("expected empty output for non-matching stream filter, got: %q", outFilter)
	}

	outMatching, err := executeCommand(t.Context(), "cat", "--segment", segmentFile, "--stream", streamID.String(), "--registry", registryFile)
	if err != nil {
		t.Fatalf("cat with matching stream filter failed: %v", err)
	}
	if !strings.Contains(outMatching, "testpkg.User") {
		t.Errorf("expected matching output, got: %q", outMatching)
	}
}

func TestSdsCatValidationErrors(t *testing.T) {
	// Missing both --server and --segment
	_, err := executeCommand(t.Context(), "cat")
	if err == nil || !strings.Contains(err.Error(), "either --server or --segment must be specified") {
		t.Errorf("expected either --server or --segment error, got %v", err)
	}

	// Missing --stream when --server is specified
	_, err = executeCommand(t.Context(), "cat", "--server", "localhost:50051")
	if err == nil || !strings.Contains(err.Error(), "--stream is required") {
		t.Errorf("expected --stream required error, got %v", err)
	}

	// Specifying both --server and --segment
	_, err = executeCommand(t.Context(), "cat", "--server", "localhost:50051", "--stream", uuid.NewString(), "--segment", "foo.wal")
	if err == nil || !strings.Contains(err.Error(), "cannot specify both") {
		t.Errorf("expected cannot specify both error, got %v", err)
	}
}

func TestSdsCatObjectFSStream(t *testing.T) {
	ctx := t.Context()
	walDir := t.TempDir()
	backend := controller.NewMemoryBackend()
	volumeID := "cat-test-vol"

	server := controller.NewServer(backend, controller.WithServerWAL(walDir, "", walclient.Local))
	defer func() { _ = server.Close() }()

	_, err := server.Mkdir(ctx, &objectfspb.MkdirRequest{VolumeId: volumeID, ParentInode: 1, Name: "cats", Mode: 0755})
	if err != nil {
		t.Fatalf("Mkdir failed: %v", err)
	}

	vol := server.GetVolume(volumeID)
	if vol == nil || vol.Stream() == nil {
		t.Fatalf("Expected active volume stream")
	}

	files, err := filepath.Glob(filepath.Join(walDir, fmt.Sprintf("stream-%s-*.wal", vol.StreamID())))
	if err != nil || len(files) == 0 {
		t.Fatalf("No segment file found: %v", err)
	}

	out, err := executeCommand(ctx, "cat", "--segment", files[0])
	if err != nil {
		t.Fatalf("sds cat failed: %v", err)
	}

	if !strings.Contains(out, "objectfs.v1alpha1.Inode") {
		t.Errorf("expected Inode type definition in sds cat output, got: %s", out)
	}
	if !strings.Contains(out, "objectfs.v1alpha1.DirEntry") {
		t.Errorf("expected DirEntry type definition in sds cat output, got: %s", out)
	}
	if !strings.Contains(out, "sds.v1.OpRecord") {
		t.Errorf("expected OpRecord in sds cat output, got: %s", out)
	}
	if !strings.Contains(out, "sds.v1.TxCommit") {
		t.Errorf("expected TxCommit in sds cat output, got: %s", out)
	}
}
