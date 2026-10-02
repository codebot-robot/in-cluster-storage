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
	"errors"
	"fmt"
	"slices"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/dynamicpb"
)

// ErrMissingPrimaryKey is returned when a row operation is missing one or more required primary key fields.
var ErrMissingPrimaryKey = errors.New("missing primary key field")

// ErrInvalidKeyBytes is returned when key bytes do not match the expected primary key definition.
var ErrInvalidKeyBytes = errors.New("key bytes do not match key_fields")

// ValidateKeyBytes validates that keyBytes represents a valid protobuf message for md where:
// 1. Every field number in keyFields is present and set.
// 2. No non-key fields are set.
// 3. No unknown fields are present.
func ValidateKeyBytes(md protoreflect.MessageDescriptor, keyFields []int32, keyBytes []byte) error {
	if md == nil {
		return errors.New("nil message descriptor")
	}
	if len(keyBytes) == 0 {
		if len(keyFields) == 0 {
			return nil
		}
		return fmt.Errorf("%w: empty key bytes for message %q with key_fields %v", ErrInvalidKeyBytes, md.FullName(), keyFields)
	}

	keyMsg := dynamicpb.NewMessage(md)
	if err := proto.Unmarshal(keyBytes, keyMsg); err != nil {
		return fmt.Errorf("%w: failed to unmarshal key proto bytes: %w", ErrInvalidKeyBytes, err)
	}

	fields := md.Fields()
	// Check all required key_fields are present
	for _, kf := range keyFields {
		f := fields.ByNumber(protoreflect.FieldNumber(kf))
		if f == nil {
			return fmt.Errorf("%w: key field %d not found in %q", ErrInvalidKeyBytes, kf, md.FullName())
		}
		if !keyMsg.Has(f) {
			return fmt.Errorf("%w: missing key field %d (%q) in message %q", ErrInvalidKeyBytes, kf, f.Name(), md.FullName())
		}
	}

	// Check that no non-key fields are set
	keyFieldMap := make(map[int32]bool, len(keyFields))
	for _, kf := range keyFields {
		keyFieldMap[kf] = true
	}

	var extraFields []string
	for i := 0; i < fields.Len(); i++ {
		f := fields.Get(i)
		if keyMsg.Has(f) && !keyFieldMap[int32(f.Number())] {
			extraFields = append(extraFields, fmt.Sprintf("%d (%q)", f.Number(), f.Name()))
		}
	}

	if len(extraFields) > 0 {
		return fmt.Errorf("%w: key bytes contain non-key fields %v in message %q", ErrInvalidKeyBytes, extraFields, md.FullName())
	}

	if len(keyMsg.GetUnknown()) > 0 {
		return fmt.Errorf("%w: key bytes contain unknown fields in message %q", ErrInvalidKeyBytes, md.FullName())
	}

	return nil
}

// PrimaryKey encapsulates the primary key field numbers and provides methods
// for extracting and encoding primary keys as canonical binary protobuf bytes.
type PrimaryKey struct {
	fieldNumbers []int32
}

// NewPrimaryKey creates a PrimaryKey for the given key field numbers.
func NewPrimaryKey(keyFields ...int32) *PrimaryKey {
	sorted := make([]int32, len(keyFields))
	copy(sorted, keyFields)
	slices.Sort(sorted)
	return &PrimaryKey{
		fieldNumbers: sorted,
	}
}

// FieldNumbers returns the sorted key field numbers.
func (pk *PrimaryKey) FieldNumbers() []int32 {
	return pk.fieldNumbers
}

// Key represents a primary key extracted from a row.
// It wraps the canonical binary proto representation of the key fields
// and is comparable so it can be used directly as a map key.
type Key struct {
	raw string
}

// NewKey creates a Key from a raw binary string representation.
func NewKey(repr string) Key {
	return Key{raw: repr}
}

// NewKeyFromBytes creates a Key from binary proto key bytes.
func NewKeyFromBytes(b []byte) Key {
	return Key{raw: string(b)}
}

// Bytes returns the binary proto bytes of the key.
func (k Key) Bytes() []byte {
	return []byte(k.raw)
}

// String returns the raw key representation.
func (k Key) String() string {
	return k.raw
}

// IsZero reports whether the key is empty.
func (k Key) IsZero() bool {
	return k.raw == ""
}

// Split splits a proto message into canonical binary proto key bytes and non-key value bytes.
func (pk *PrimaryKey) Split(msg proto.Message) (keyBytes []byte, valBytes []byte, err error) {
	if msg == nil {
		return nil, nil, fmt.Errorf("%w: nil message", ErrMissingPrimaryKey)
	}

	m := msg.ProtoReflect()
	md := m.Descriptor()
	fields := md.Fields()

	for _, kf := range pk.fieldNumbers {
		f := fields.ByNumber(protoreflect.FieldNumber(kf))
		if f == nil {
			return nil, nil, fmt.Errorf("key field number %d not found in %q", kf, md.FullName())
		}
		if !m.Has(f) {
			return nil, nil, fmt.Errorf("%w: field %d (%q) is not set in message %q", ErrMissingPrimaryKey, kf, f.Name(), md.FullName())
		}
	}

	keyMsg := m.Type().New()
	valMsg := m.Type().New()

	for i := 0; i < fields.Len(); i++ {
		f := fields.Get(i)
		if !m.Has(f) {
			continue
		}
		if slices.Contains(pk.fieldNumbers, int32(f.Number())) {
			keyMsg.Set(f, m.Get(f))
		} else {
			valMsg.Set(f, m.Get(f))
		}
	}

	marshalOpts := proto.MarshalOptions{Deterministic: true}
	keyBytes, err = marshalOpts.Marshal(keyMsg.Interface())
	if err != nil {
		return nil, nil, fmt.Errorf("failed to marshal key proto: %w", err)
	}

	valBytes, err = marshalOpts.Marshal(valMsg.Interface())
	if err != nil {
		return nil, nil, fmt.Errorf("failed to marshal value proto: %w", err)
	}

	return keyBytes, valBytes, nil
}

// Extract extracts the Key (as binary proto key bytes) from a message.
func (pk *PrimaryKey) Extract(msg proto.Message) (Key, error) {
	keyBytes, _, err := pk.Split(msg)
	if err != nil {
		return Key{}, err
	}
	return NewKeyFromBytes(keyBytes), nil
}

// ExtractKey is a convenience helper to extract a primary key from a proto.Message.
func ExtractKey(msg proto.Message, keyFields []int32) (Key, error) {
	return NewPrimaryKey(keyFields...).Extract(msg)
}

// SplitKeyAndNonKey is a convenience helper that splits a proto message into key and value proto bytes.
func SplitKeyAndNonKey(msg proto.Message, keyFields []int32) ([]byte, []byte, error) {
	return NewPrimaryKey(keyFields...).Split(msg)
}

// MergeKeyAndNonKey unmarshals key and value proto bytes into target message.
func MergeKeyAndNonKey(target proto.Message, keyBytes, valBytes []byte) error {
	if target == nil {
		return errors.New("nil target message")
	}
	opts := proto.UnmarshalOptions{Merge: true}
	if len(keyBytes) > 0 {
		if err := opts.Unmarshal(keyBytes, target); err != nil {
			return fmt.Errorf("failed to unmarshal key proto bytes: %w", err)
		}
	}
	if len(valBytes) > 0 {
		if err := opts.Unmarshal(valBytes, target); err != nil {
			return fmt.Errorf("failed to unmarshal value proto bytes: %w", err)
		}
	}
	return nil
}
