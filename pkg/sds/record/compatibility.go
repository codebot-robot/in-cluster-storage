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
	"slices"

	sdsv1 "github.com/gke-labs/in-cluster-storage/pkg/api/sds/v1"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// CheckCompatibility verifies whether newDef is a backwards- and forwards-compatible evolution
// of oldDef according to SDS Layer 1 schema evolution rules.
//
// Compatibility Rules:
//   - Message names must match.
//   - key_fields must match exactly in number, values, and order.
//   - Adding new fields is allowed.
//   - Renaming fields is allowed (proto uses numeric tags).
//   - Removing a field is allowed ONLY IF its field number is reserved in the new descriptor.
//   - Changing a field's number, kind/type, or cardinality (repeated/singular/map) is rejected.
//   - Changing oneof membership (moving into/out of oneof, or between oneofs) is rejected.
//   - Reusing a previously reserved field number for a new field is rejected.
//   - Nested message and enum types referenced by fields are checked recursively.
func CheckCompatibility(oldDef, newDef *sdsv1.TypeDefinition) error {
	if oldDef == nil || newDef == nil {
		return fmt.Errorf("nil TypeDefinition")
	}
	if oldDef.GetName() != newDef.GetName() {
		return fmt.Errorf("message name mismatch: old=%q, new=%q", oldDef.GetName(), newDef.GetName())
	}
	if !slices.Equal(oldDef.GetKeyFields(), newDef.GetKeyFields()) {
		return fmt.Errorf("key_fields mismatch: old=%v, new=%v", oldDef.GetKeyFields(), newDef.GetKeyFields())
	}
	if oldDef.GetDescriptors() == nil || newDef.GetDescriptors() == nil {
		return fmt.Errorf("missing descriptors in TypeDefinition")
	}

	oldFiles, err := protodesc.NewFiles(oldDef.GetDescriptors())
	if err != nil {
		return fmt.Errorf("failed to parse old descriptors: %w", err)
	}
	newFiles, err := protodesc.NewFiles(newDef.GetDescriptors())
	if err != nil {
		return fmt.Errorf("failed to parse new descriptors: %w", err)
	}

	oldDesc, err := oldFiles.FindDescriptorByName(protoreflect.FullName(oldDef.GetName()))
	if err != nil {
		return fmt.Errorf("message %q not found in old descriptors: %w", oldDef.GetName(), err)
	}
	newDesc, err := newFiles.FindDescriptorByName(protoreflect.FullName(newDef.GetName()))
	if err != nil {
		return fmt.Errorf("message %q not found in new descriptors: %w", newDef.GetName(), err)
	}

	oldMD, ok := oldDesc.(protoreflect.MessageDescriptor)
	if !ok {
		return fmt.Errorf("%q is not a message descriptor in old descriptors", oldDef.GetName())
	}
	newMD, ok := newDesc.(protoreflect.MessageDescriptor)
	if !ok {
		return fmt.Errorf("%q is not a message descriptor in new descriptors", newDef.GetName())
	}

	visited := make(map[string]bool)
	return checkMessageCompatibility(oldMD, newMD, visited)
}

// CheckMessageCompatibility verifies compatibility between two MessageDescriptors directly.
func CheckMessageCompatibility(oldMD, newMD protoreflect.MessageDescriptor) error {
	if oldMD == nil || newMD == nil {
		return fmt.Errorf("nil MessageDescriptor")
	}
	if oldMD.FullName() != newMD.FullName() {
		return fmt.Errorf("message name mismatch: old=%q, new=%q", oldMD.FullName(), newMD.FullName())
	}
	visited := make(map[string]bool)
	return checkMessageCompatibility(oldMD, newMD, visited)
}

func checkMessageCompatibility(oldMD, newMD protoreflect.MessageDescriptor, visited map[string]bool) error {
	fullName := string(oldMD.FullName())
	if visited[fullName] {
		return nil
	}
	visited[fullName] = true

	oldFields := oldMD.Fields()
	newFields := newMD.Fields()
	newReservedRanges := newMD.ReservedRanges()
	oldReservedRanges := oldMD.ReservedRanges()

	// 1. Check all fields from old message.
	for i := 0; i < oldFields.Len(); i++ {
		oldF := oldFields.Get(i)
		num := oldF.Number()
		newF := newFields.ByNumber(num)

		if newF == nil {
			// Field was removed: only allowed if field number is reserved in new descriptor.
			if !newReservedRanges.Has(num) {
				return fmt.Errorf("field %d (%q) was removed without reserving its field number in message %q",
					num, oldF.Name(), oldMD.FullName())
			}
			continue
		}

		// Field exists in both: check wire compatibility.
		if oldF.Kind() != newF.Kind() {
			return fmt.Errorf("field %d (%q) changed kind from %s to %s in message %q",
				num, oldF.Name(), oldF.Kind(), newF.Kind(), oldMD.FullName())
		}

		if oldF.Cardinality() != newF.Cardinality() {
			return fmt.Errorf("field %d (%q) changed cardinality from %s to %s in message %q",
				num, oldF.Name(), oldF.Cardinality(), newF.Cardinality(), oldMD.FullName())
		}

		if oldF.IsList() != newF.IsList() {
			return fmt.Errorf("field %d (%q) changed repeated/list status in message %q",
				num, oldF.Name(), oldMD.FullName())
		}

		if oldF.IsMap() != newF.IsMap() {
			return fmt.Errorf("field %d (%q) changed map status in message %q",
				num, oldF.Name(), oldMD.FullName())
		}

		if oldF.IsMap() {
			oldKey := oldF.MapKey()
			newKey := newF.MapKey()
			if oldKey.Kind() != newKey.Kind() {
				return fmt.Errorf("field %d (%q) map key changed kind from %s to %s in message %q",
					num, oldF.Name(), oldKey.Kind(), newKey.Kind(), oldMD.FullName())
			}
			oldVal := oldF.MapValue()
			newVal := newF.MapValue()
			if oldVal.Kind() != newVal.Kind() {
				return fmt.Errorf("field %d (%q) map value changed kind from %s to %s in message %q",
					num, oldF.Name(), oldVal.Kind(), newVal.Kind(), oldMD.FullName())
			}
			if oldVal.Kind() == protoreflect.MessageKind {
				if oldVal.Message().FullName() != newVal.Message().FullName() {
					return fmt.Errorf("field %d (%q) map value changed message type from %s to %s in message %q",
						num, oldF.Name(), oldVal.Message().FullName(), newVal.Message().FullName(), oldMD.FullName())
				}
				if err := checkMessageCompatibility(oldVal.Message(), newVal.Message(), visited); err != nil {
					return err
				}
			}
		}

		// Check oneof membership (excluding synthetic oneofs created for proto3 optional).
		oldOneof := oldF.ContainingOneof()
		if oldOneof != nil && oldOneof.IsSynthetic() {
			oldOneof = nil
		}
		newOneof := newF.ContainingOneof()
		if newOneof != nil && newOneof.IsSynthetic() {
			newOneof = nil
		}

		if (oldOneof == nil) != (newOneof == nil) {
			return fmt.Errorf("field %d (%q) oneof membership changed in message %q",
				num, oldF.Name(), oldMD.FullName())
		}
		if oldOneof != nil && newOneof != nil && oldOneof.Name() != newOneof.Name() {
			return fmt.Errorf("field %d (%q) moved from oneof %q to %q in message %q",
				num, oldF.Name(), oldOneof.Name(), newOneof.Name(), oldMD.FullName())
		}

		// Check nested message or enum targets.
		if oldF.Kind() == protoreflect.MessageKind && !oldF.IsMap() {
			if oldF.Message().FullName() != newF.Message().FullName() {
				return fmt.Errorf("field %d (%q) changed message type from %s to %s in message %q",
					num, oldF.Name(), oldF.Message().FullName(), newF.Message().FullName(), oldMD.FullName())
			}
			if err := checkMessageCompatibility(oldF.Message(), newF.Message(), visited); err != nil {
				return err
			}
		}

		if oldF.Kind() == protoreflect.EnumKind {
			if oldF.Enum().FullName() != newF.Enum().FullName() {
				return fmt.Errorf("field %d (%q) changed enum type from %s to %s in message %q",
					num, oldF.Name(), oldF.Enum().FullName(), newF.Enum().FullName(), oldMD.FullName())
			}
		}
	}

	// 2. Check all fields from new message (added fields).
	for i := 0; i < newFields.Len(); i++ {
		newF := newFields.Get(i)
		num := newF.Number()
		oldF := oldFields.ByNumber(num)

		if oldF == nil {
			// New field added: verify it doesn't reuse a field number that was reserved in the old message.
			if oldReservedRanges.Has(num) {
				return fmt.Errorf("new field %d (%q) reuses a field number reserved in older schema in message %q",
					num, newF.Name(), oldMD.FullName())
			}
		}
	}

	return nil
}
