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
	"fmt"

	sdsv1 "github.com/gke-labs/in-cluster-storage/pkg/api/sds/v1"
	"google.golang.org/protobuf/proto"
)

// Record represents a decoded record from a structured data stream.
type Record struct {
	TypeID  uint32
	Message proto.Message // nil for TypeIDPadding
	Raw     []byte        // The raw payload body bytes (excluding the varint type ID header)
}

// DecoderOption configures a Decoder.
type DecoderOption func(*Decoder)

// WithDecoderRegistry sets a custom Registry on the Decoder.
func WithDecoderRegistry(reg *Registry) DecoderOption {
	return func(d *Decoder) {
		d.registry = reg
	}
}

// Decoder decodes stream payloads in order and maintains the stream's type registry.
type Decoder struct {
	registry *Registry
}

// NewDecoder creates a new Decoder.
func NewDecoder(opts ...DecoderOption) *Decoder {
	d := &Decoder{
		registry: NewRegistry(),
	}
	for _, opt := range opts {
		opt(d)
	}
	return d
}

// Registry returns the Decoder's Registry.
func (d *Decoder) Registry() *Registry {
	return d.registry
}

// Decode parses a framed stream payload, updates the in-band type registry when TypeDefinition
// frames are encountered, and returns the decoded Record.
// Application records are instantiated using the compiled Go type from protoregistry when the
// registered fingerprint matches the compiled descriptor, or dynamicpb otherwise.
func (d *Decoder) Decode(payload []byte) (Record, error) {
	typeID, body, err := SplitFrame(payload)
	if err != nil {
		return Record{}, err
	}

	switch typeID {
	case TypeIDTypeDefinition:
		def := &sdsv1.TypeDefinition{}
		if err := proto.Unmarshal(body, def); err != nil {
			return Record{}, fmt.Errorf("failed to unmarshal TypeDefinition: %w", err)
		}
		if err := d.registry.Register(def); err != nil {
			return Record{}, err
		}
		return Record{TypeID: TypeIDTypeDefinition, Message: def, Raw: body}, nil

	case TypeIDTxCommit:
		commit := &sdsv1.TxCommit{}
		if err := proto.Unmarshal(body, commit); err != nil {
			return Record{}, fmt.Errorf("failed to unmarshal TxCommit: %w", err)
		}
		return Record{TypeID: TypeIDTxCommit, Message: commit, Raw: body}, nil

	case TypeIDSnapshotPointer:
		ptr := &sdsv1.SnapshotPointer{}
		if err := proto.Unmarshal(body, ptr); err != nil {
			return Record{}, fmt.Errorf("failed to unmarshal SnapshotPointer: %w", err)
		}
		return Record{TypeID: TypeIDSnapshotPointer, Message: ptr, Raw: body}, nil

	case TypeIDPadding:
		return Record{TypeID: TypeIDPadding, Message: nil, Raw: body}, nil

	default:
		// Application type (typeID >= 16)
		msgType, err := d.registry.ResolveMessageType(typeID)
		if err != nil {
			return Record{}, err
		}
		msg := msgType.New().Interface()
		if err := proto.Unmarshal(body, msg); err != nil {
			return Record{}, fmt.Errorf("failed to unmarshal record body for type ID %d: %w", typeID, err)
		}
		return Record{TypeID: typeID, Message: msg, Raw: body}, nil
	}
}

// DecodePayload is a convenience wrapper around Decode that returns the (typeID, proto.Message, error) tuple.
func (d *Decoder) DecodePayload(payload []byte) (uint32, proto.Message, error) {
	rec, err := d.Decode(payload)
	if err != nil {
		return 0, nil, err
	}
	return rec.TypeID, rec.Message, nil
}
