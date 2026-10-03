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

package sds

import (
	"bytes"
	"context"
	"fmt"
	"sync/atomic"

	sdsv1 "github.com/gke-labs/in-cluster-storage/pkg/api/sds/v1"
	"github.com/gke-labs/in-cluster-storage/pkg/sds/record"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// Writer provides Layer 2 relational row-change and transaction writing
// over a structured data stream using OpRecord framework records.
type Writer struct {
	recordWriter *record.Writer
	nextTxID     atomic.Uint64
}

// NewWriter creates a new Layer 2 Writer wrapping the given Appender transport.
func NewWriter(appender record.Appender, opts ...record.WriterOption) *Writer {
	return &Writer{
		recordWriter: record.NewWriter(appender, opts...),
	}
}

// Registry returns the in-band type registry.
func (w *Writer) Registry() *record.Registry {
	return w.recordWriter.Registry()
}

// RecordWriter returns the underlying Layer 1 record.Writer.
func (w *Writer) RecordWriter() *record.Writer {
	return w.recordWriter
}

// RegisterType registers a Go proto.Message type with optional primary key field numbers.
func (w *Writer) RegisterType(msg proto.Message, keyFields ...int32) (uint32, error) {
	return w.recordWriter.RegisterType(msg, keyFields...)
}

// RegisterDescriptor registers a MessageDescriptor with optional primary key field numbers.
func (w *Writer) RegisterDescriptor(md protoreflect.MessageDescriptor, keyFields ...int32) (uint32, error) {
	return w.recordWriter.RegisterDescriptor(md, keyFields...)
}

func (w *Writer) resolveOrRegister(msg proto.Message) (*sdsv1.TypeDefinition, error) {
	if msg == nil {
		return nil, fmt.Errorf("nil message")
	}
	md := msg.ProtoReflect().Descriptor()
	name := string(md.FullName())

	reg := w.recordWriter.Registry()
	def, _, exists := reg.LookupByName(name)
	if !exists {
		return reg.RegisterMessage(msg)
	}

	// Check if descriptor evolved
	currentFP, _, err := record.ComputeMessageFingerprint(md)
	if err != nil {
		return nil, err
	}
	if !bytes.Equal(currentFP, def.GetFingerprint()) {
		return reg.RegisterMessage(msg, def.GetKeyFields()...)
	}
	return def, nil
}

func (w *Writer) writeOp(ctx context.Context, op sdsv1.OpRecord_Op, txID uint64, msg proto.Message) (uint64, error) {
	if msg == nil {
		return 0, fmt.Errorf("nil message")
	}

	def, err := w.resolveOrRegister(msg)
	if err != nil {
		return 0, err
	}

	keyBytes, valBytes, err := SplitKeyAndNonKey(msg, def.GetKeyFields())
	if err != nil {
		return 0, err
	}

	opRec := &sdsv1.OpRecord{
		Op:     op,
		TypeId: def.GetId(),
		TxId:   txID,
		Key:    keyBytes,
	}
	if op != sdsv1.OpRecord_DELETE {
		opRec.Value = valBytes
	}

	return w.recordWriter.AppendOp(ctx, opRec)
}

// Insert writes an autocommit Create row change (tx_id = 0, OpRecord_CREATE).
func (w *Writer) Insert(ctx context.Context, msg proto.Message) (uint64, error) {
	return w.writeOp(ctx, sdsv1.OpRecord_CREATE, 0, msg)
}

// Create writes an autocommit Create row change (tx_id = 0, OpRecord_CREATE).
func (w *Writer) Create(ctx context.Context, msg proto.Message) (uint64, error) {
	return w.writeOp(ctx, sdsv1.OpRecord_CREATE, 0, msg)
}

// Update writes an autocommit Update row change (tx_id = 0, OpRecord_UPDATE).
func (w *Writer) Update(ctx context.Context, msg proto.Message) (uint64, error) {
	return w.writeOp(ctx, sdsv1.OpRecord_UPDATE, 0, msg)
}

// Delete writes an autocommit Delete row change (tx_id = 0, OpRecord_DELETE).
// msg must contain at least the primary key fields.
func (w *Writer) Delete(ctx context.Context, msg proto.Message) (uint64, error) {
	return w.writeOp(ctx, sdsv1.OpRecord_DELETE, 0, msg)
}

// Begin starts a new multi-record transaction.
func (w *Writer) Begin() *Tx {
	txID := w.nextTxID.Add(1)
	return &Tx{
		writer: w,
		txID:   txID,
	}
}

// AppendSnapshotPointer writes a SnapshotPointer framework record (type ID = 3).
func (w *Writer) AppendSnapshotPointer(ctx context.Context, ptr *sdsv1.SnapshotPointer) (uint64, error) {
	return w.recordWriter.AppendSnapshotPointer(ctx, ptr)
}

// AppendPadding writes a Padding framework record (type ID = 4) with the specified length.
func (w *Writer) AppendPadding(ctx context.Context, length int) (uint64, error) {
	return w.recordWriter.AppendPadding(ctx, length)
}

// AppendPaddingBytes writes a Padding framework record (type ID = 4) with arbitrary bytes.
func (w *Writer) AppendPaddingBytes(ctx context.Context, body []byte) (uint64, error) {
	return w.recordWriter.AppendPaddingBytes(ctx, body)
}

// Tx represents an in-progress transaction.
type Tx struct {
	writer   *Writer
	txID     uint64
	finished bool
	changes  []Change
}

// TxID returns the transaction ID.
func (tx *Tx) TxID() uint64 {
	return tx.txID
}

func (tx *Tx) recordChange(seq uint64, op sdsv1.OpRecord_Op, msg proto.Message) error {
	def, err := tx.writer.resolveOrRegister(msg)
	if err != nil {
		return err
	}
	keyBytes, valBytes, err := SplitKeyAndNonKey(msg, def.GetKeyFields())
	if err != nil {
		return err
	}
	ch := Change{
		Seq:      seq,
		TypeID:   def.GetId(),
		TypeName: def.GetName(),
		Op:       op,
		TxID:     tx.txID,
		Key:      NewKeyFromBytes(keyBytes),
		RawKey:   keyBytes,
		RawVal:   valBytes,
		Row:      msg,
	}
	tx.changes = append(tx.changes, ch)
	return nil
}

// Insert writes an OpRecord_CREATE row change belonging to this transaction.
func (tx *Tx) Insert(ctx context.Context, msg proto.Message) (uint64, error) {
	if tx.finished {
		return 0, fmt.Errorf("transaction %d already closed", tx.txID)
	}
	seq, err := tx.writer.writeOp(ctx, sdsv1.OpRecord_CREATE, tx.txID, msg)
	if err != nil {
		return 0, err
	}
	_ = tx.recordChange(seq, sdsv1.OpRecord_CREATE, msg)
	return seq, nil
}

// Create writes an OpRecord_CREATE row change belonging to this transaction.
func (tx *Tx) Create(ctx context.Context, msg proto.Message) (uint64, error) {
	if tx.finished {
		return 0, fmt.Errorf("transaction %d already closed", tx.txID)
	}
	seq, err := tx.writer.writeOp(ctx, sdsv1.OpRecord_CREATE, tx.txID, msg)
	if err != nil {
		return 0, err
	}
	_ = tx.recordChange(seq, sdsv1.OpRecord_CREATE, msg)
	return seq, nil
}

// Update writes an OpRecord_UPDATE row change belonging to this transaction.
func (tx *Tx) Update(ctx context.Context, msg proto.Message) (uint64, error) {
	if tx.finished {
		return 0, fmt.Errorf("transaction %d already closed", tx.txID)
	}
	seq, err := tx.writer.writeOp(ctx, sdsv1.OpRecord_UPDATE, tx.txID, msg)
	if err != nil {
		return 0, err
	}
	_ = tx.recordChange(seq, sdsv1.OpRecord_UPDATE, msg)
	return seq, nil
}

// Delete writes an OpRecord_DELETE row change belonging to this transaction.
func (tx *Tx) Delete(ctx context.Context, msg proto.Message) (uint64, error) {
	if tx.finished {
		return 0, fmt.Errorf("transaction %d already closed", tx.txID)
	}
	seq, err := tx.writer.writeOp(ctx, sdsv1.OpRecord_DELETE, tx.txID, msg)
	if err != nil {
		return 0, err
	}
	_ = tx.recordChange(seq, sdsv1.OpRecord_DELETE, msg)
	return seq, nil
}

// Commit commits this transaction by writing a TxCommit framework record.
func (tx *Tx) Commit(ctx context.Context) (uint64, error) {
	if tx.finished {
		return 0, fmt.Errorf("transaction %d already closed", tx.txID)
	}
	tx.finished = true
	commit := &sdsv1.TxCommit{
		TxId:       tx.txID,
		CommitTime: timestamppb.Now(),
	}
	commitSeq, err := tx.writer.recordWriter.AppendTxCommit(ctx, commit)
	if err != nil {
		return 0, err
	}
	for i := range tx.changes {
		tx.changes[i].Seq = commitSeq
	}
	return commitSeq, nil
}

// Changes returns all row changes made during this transaction with updated commit sequence numbers.
func (tx *Tx) Changes() []Change {
	return tx.changes
}
