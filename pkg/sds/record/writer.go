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

package record

import (
	"bytes"
	"context"
	"fmt"
	"sync"

	sdsv1 "github.com/gke-labs/in-cluster-storage/pkg/api/sds/v1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// Appender is the minimal transport interface required by Writer.
type Appender interface {
	Append(ctx context.Context, payload []byte) (seq uint64, err error)
}

// WriterOption configures a Writer.
type WriterOption func(*Writer)

// WithRegistry sets a custom Registry for the Writer.
func WithRegistry(reg *Registry) WriterOption {
	return func(w *Writer) {
		w.registry = reg
	}
}

// Writer provides high-level structured stream writing over an Appender transport.
// It manages type registrations and in-band TypeDefinition announcements.
type Writer struct {
	mu        sync.Mutex
	appender  Appender
	registry  *Registry
	announced map[uint32][]byte // typeID -> last announced fingerprint
}

// NewWriter creates a new Writer wrapping the given Appender.
func NewWriter(appender Appender, opts ...WriterOption) *Writer {
	w := &Writer{
		appender:  appender,
		registry:  NewRegistry(),
		announced: make(map[uint32][]byte),
	}
	for _, opt := range opts {
		opt(w)
	}
	return w
}

// Registry returns the Writer's Registry.
func (w *Writer) Registry() *Registry {
	return w.registry
}

// RegisterTypeWithOptions registers a Go proto.Message type with options.
func (w *Writer) RegisterTypeWithOptions(msg proto.Message, opts ...TypeOption) (uint32, error) {
	def, err := w.registry.RegisterMessageWithOptions(msg, opts...)
	if err != nil {
		return 0, err
	}
	return def.GetId(), nil
}

// RegisterDescriptorWithOptions registers a MessageDescriptor with options.
func (w *Writer) RegisterDescriptorWithOptions(md protoreflect.MessageDescriptor, opts ...TypeOption) (uint32, error) {
	def, err := w.registry.RegisterDescriptorWithOptions(md, opts...)
	if err != nil {
		return 0, err
	}
	return def.GetId(), nil
}

// RegisterType registers a Go proto.Message type with optional primary key field numbers.
func (w *Writer) RegisterType(msg proto.Message, keyFields ...int32) (uint32, error) {
	return w.RegisterTypeWithOptions(msg, WithKeyFields(keyFields...))
}

// RegisterDescriptor registers a MessageDescriptor with optional primary key field numbers.
func (w *Writer) RegisterDescriptor(md protoreflect.MessageDescriptor, keyFields ...int32) (uint32, error) {
	return w.RegisterDescriptorWithOptions(md, WithKeyFields(keyFields...))
}

// ensureAnnounced ensures that the TypeDefinition for typeID is emitted to the stream
// before any record of this type is written.
func (w *Writer) ensureAnnounced(ctx context.Context, typeID uint32) error {
	def, _, ok := w.registry.LookupByID(typeID)
	if !ok {
		return fmt.Errorf("%w: %d", ErrTypeNotRegistered, typeID)
	}

	lastFP, alreadyAnnounced := w.announced[typeID]
	if alreadyAnnounced && bytes.Equal(lastFP, def.GetFingerprint()) {
		return nil
	}

	// Emit TypeDefinition frame (TypeID = 1).
	defBytes, err := proto.Marshal(def)
	if err != nil {
		return fmt.Errorf("failed to marshal TypeDefinition for type ID %d: %w", typeID, err)
	}
	payload, err := EncodeFrame(TypeIDTypeDefinition, defBytes)
	if err != nil {
		return err
	}
	if _, err := w.appender.Append(ctx, payload); err != nil {
		return fmt.Errorf("failed to append TypeDefinition for type ID %d: %w", typeID, err)
	}

	w.announced[typeID] = def.GetFingerprint()
	return nil
}

// EnsureAnnounced ensures that the TypeDefinition for typeID is emitted to the stream
// before any record of this type is written.
func (w *Writer) EnsureAnnounced(ctx context.Context, typeID uint32) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.ensureAnnounced(ctx, typeID)
}

// AppendOp writes an OpRecord framework record (TypeID = 5).
// It ensures that the referenced table type ID is announced before writing.
func (w *Writer) AppendOp(ctx context.Context, op *sdsv1.OpRecord) (uint64, error) {
	if op == nil {
		return 0, fmt.Errorf("nil OpRecord")
	}

	w.mu.Lock()
	defer w.mu.Unlock()

	if err := w.ensureAnnounced(ctx, op.GetTypeId()); err != nil {
		return 0, err
	}

	body, err := proto.Marshal(op)
	if err != nil {
		return 0, fmt.Errorf("failed to marshal OpRecord: %w", err)
	}

	payload, err := EncodeFrame(TypeIDOpRecord, body)
	if err != nil {
		return 0, err
	}
	return w.appender.Append(ctx, payload)
}

// AppendTxCommit writes a TxCommit framework record (type ID = 2).
func (w *Writer) AppendTxCommit(ctx context.Context, commit *sdsv1.TxCommit) (uint64, error) {
	if commit == nil {
		return 0, fmt.Errorf("nil TxCommit")
	}
	body, err := proto.Marshal(commit)
	if err != nil {
		return 0, fmt.Errorf("failed to marshal TxCommit: %w", err)
	}
	payload, err := EncodeFrame(TypeIDTxCommit, body)
	if err != nil {
		return 0, err
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.appender.Append(ctx, payload)
}

// AppendSnapshotPointer writes a SnapshotPointer framework record (type ID = 3).
func (w *Writer) AppendSnapshotPointer(ctx context.Context, ptr *sdsv1.SnapshotPointer) (uint64, error) {
	if ptr == nil {
		return 0, fmt.Errorf("nil SnapshotPointer")
	}
	body, err := proto.Marshal(ptr)
	if err != nil {
		return 0, fmt.Errorf("failed to marshal SnapshotPointer: %w", err)
	}
	payload, err := EncodeFrame(TypeIDSnapshotPointer, body)
	if err != nil {
		return 0, err
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.appender.Append(ctx, payload)
}

// AppendPadding writes a Padding framework record (type ID = 4) with the specified byte length.
func (w *Writer) AppendPadding(ctx context.Context, length int) (uint64, error) {
	return w.AppendPaddingBytes(ctx, make([]byte, length))
}

// AppendPaddingBytes writes a Padding framework record (type ID = 4) with arbitrary payload bytes.
func (w *Writer) AppendPaddingBytes(ctx context.Context, body []byte) (uint64, error) {
	payload, err := EncodeFrame(TypeIDPadding, body)
	if err != nil {
		return 0, err
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.appender.Append(ctx, payload)
}

// AppendRaw writes an already-marshaled payload body with the given type ID.
func (w *Writer) AppendRaw(ctx context.Context, typeID uint32, body []byte) (uint64, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if typeID >= MinAppTypeID {
		if err := w.ensureAnnounced(ctx, typeID); err != nil {
			return 0, err
		}
	}

	payload, err := EncodeFrame(typeID, body)
	if err != nil {
		return 0, err
	}
	return w.appender.Append(ctx, payload)
}
