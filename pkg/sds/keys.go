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
)

// ErrMissingPrimaryKey is returned when a row operation is missing one or more required primary key fields.
var ErrMissingPrimaryKey = errors.New("missing primary key field")

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

	keyBytes, err = proto.Marshal(keyMsg.Interface())
	if err != nil {
		return nil, nil, fmt.Errorf("failed to marshal key proto: %w", err)
	}

	valBytes, err = proto.Marshal(valMsg.Interface())
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
