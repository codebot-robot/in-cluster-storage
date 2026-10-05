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
	"errors"
	"fmt"
	"sort"
	"sync"

	sdsv1 "github.com/gke-labs/in-cluster-storage/pkg/api/sds/v1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
	"google.golang.org/protobuf/types/dynamicpb"
)

var (
	// ErrMissingDescriptors is returned when registering a new type without descriptors.
	ErrMissingDescriptors = errors.New("missing FileDescriptorSet for new type registration")
	// ErrTypeIDTooLow is returned when an application type definition uses an ID < 16.
	ErrTypeIDTooLow = errors.New("application type ID must be >= 16")
)

// TypeOption configures a TypeDefinition during registration.
type TypeOption func(*sdsv1.TypeDefinition)

// WithKeyFields specifies primary key field numbers.
func WithKeyFields(keyFields ...int32) TypeOption {
	return func(def *sdsv1.TypeDefinition) {
		kf := make([]int32, len(keyFields))
		copy(kf, keyFields)
		def.KeyFields = kf
	}
}

// WithLogBeforeImages specifies whether UPDATE and DELETE operations emit before_value.
func WithLogBeforeImages(logBeforeImages bool) TypeOption {
	return func(def *sdsv1.TypeDefinition) {
		def.LogBeforeImages = logBeforeImages
	}
}

type typeEntry struct {
	def          *sdsv1.TypeDefinition
	descriptor   protoreflect.MessageDescriptor
	resolvedType protoreflect.MessageType
}

// Registry maintains the in-band type definitions for a structured stream.
// It is thread-safe.
type Registry struct {
	mu     sync.RWMutex
	types  map[uint32]*typeEntry
	byName map[string]uint32
	nextID uint32
}

// NewRegistry creates a new empty Registry with IDs starting at MinAppTypeID (16).
func NewRegistry() *Registry {
	return &Registry{
		types:  make(map[uint32]*typeEntry),
		byName: make(map[string]uint32),
		nextID: MinAppTypeID,
	}
}

// AllocateID allocates the next available unused application type ID.
func (r *Registry) AllocateID() uint32 {
	r.mu.Lock()
	defer r.mu.Unlock()
	id := r.nextID
	r.nextID++
	return id
}

// Register applies or updates a TypeDefinition in the registry according to SDS rules:
//   - ID must be >= 16.
//   - Fingerprint matching the existing entry is an idempotent no-op.
//   - A new fingerprint for an existing ID is allowed only if it is a compatible schema evolution.
//   - Incompatible changes are rejected with an error.
func (r *Registry) Register(def *sdsv1.TypeDefinition) error {
	if def == nil {
		return fmt.Errorf("nil TypeDefinition")
	}
	if def.GetId() < MinAppTypeID {
		return fmt.Errorf("%w: got %d", ErrTypeIDTooLow, def.GetId())
	}
	if def.GetName() == "" {
		return fmt.Errorf("empty type name in TypeDefinition")
	}
	if len(def.GetFingerprint()) == 0 {
		return fmt.Errorf("empty fingerprint in TypeDefinition")
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	existing, ok := r.types[def.GetId()]
	if ok {
		// Idempotency check: same fingerprint is a no-op (rule 4).
		if bytes.Equal(existing.def.GetFingerprint(), def.GetFingerprint()) {
			return nil
		}

		// New fingerprint for existing ID: verify compatibility (rule 2).
		if err := CheckCompatibility(existing.def, def); err != nil {
			return fmt.Errorf("incompatible schema evolution for type ID %d (%s): %w", def.GetId(), def.GetName(), err)
		}

		// Parse the new descriptor and update entry.
		md, err := resolveMessageDescriptor(def)
		if err != nil {
			return err
		}

		defCopy := proto.Clone(def).(*sdsv1.TypeDefinition)
		r.types[def.GetId()] = &typeEntry{
			def:        defCopy,
			descriptor: md,
		}
		r.byName[def.GetName()] = def.GetId()
		return nil
	}

	// New type ID: must have descriptors.
	if def.GetDescriptors() == nil {
		return ErrMissingDescriptors
	}

	// Validate fingerprint.
	expectedFP, err := ComputeFingerprint(def.GetDescriptors())
	if err != nil {
		return fmt.Errorf("failed to compute fingerprint for type ID %d: %w", def.GetId(), err)
	}
	if !bytes.Equal(expectedFP, def.GetFingerprint()) {
		return fmt.Errorf("fingerprint mismatch for type ID %d (%s): provided %x, computed %x",
			def.GetId(), def.GetName(), def.GetFingerprint(), expectedFP)
	}

	md, err := resolveMessageDescriptor(def)
	if err != nil {
		return err
	}

	// Validate key fields.
	fields := md.Fields()
	for _, kf := range def.GetKeyFields() {
		f := fields.ByNumber(protoreflect.FieldNumber(kf))
		if f == nil {
			return fmt.Errorf("key_field %d not found in message %q", kf, def.GetName())
		}
		if f.IsList() {
			return fmt.Errorf("key_field %d (%q) in message %q cannot be repeated", kf, f.Name(), def.GetName())
		}
		if f.IsMap() {
			return fmt.Errorf("key_field %d (%q) in message %q cannot be a map", kf, f.Name(), def.GetName())
		}
		if f.Kind() == protoreflect.MessageKind || f.Kind() == protoreflect.GroupKind {
			return fmt.Errorf("key_field %d (%q) in message %q must be a scalar, got %s", kf, f.Name(), def.GetName(), f.Kind())
		}
	}

	defCopy := proto.Clone(def).(*sdsv1.TypeDefinition)
	r.types[def.GetId()] = &typeEntry{
		def:        defCopy,
		descriptor: md,
	}
	r.byName[def.GetName()] = def.GetId()

	if def.GetId() >= r.nextID {
		r.nextID = def.GetId() + 1
	}

	return nil
}

func resolveMessageDescriptor(def *sdsv1.TypeDefinition) (protoreflect.MessageDescriptor, error) {
	files, err := protodesc.NewFiles(def.GetDescriptors())
	if err != nil {
		return nil, fmt.Errorf("failed to parse descriptors for type ID %d: %w", def.GetId(), err)
	}
	d, err := files.FindDescriptorByName(protoreflect.FullName(def.GetName()))
	if err != nil {
		return nil, fmt.Errorf("message %q not found in descriptors for type ID %d: %w", def.GetName(), def.GetId(), err)
	}
	md, ok := d.(protoreflect.MessageDescriptor)
	if !ok {
		return nil, fmt.Errorf("%q is not a message descriptor", def.GetName())
	}
	return md, nil
}

// RegisterMessageWithOptions registers a proto.Message with options.
func (r *Registry) RegisterMessageWithOptions(msg proto.Message, opts ...TypeOption) (*sdsv1.TypeDefinition, error) {
	if msg == nil {
		return nil, fmt.Errorf("nil message")
	}
	return r.RegisterDescriptorWithOptions(msg.ProtoReflect().Descriptor(), opts...)
}

// RegisterDescriptorWithOptions registers a MessageDescriptor with options.
func (r *Registry) RegisterDescriptorWithOptions(md protoreflect.MessageDescriptor, opts ...TypeOption) (*sdsv1.TypeDefinition, error) {
	if md == nil {
		return nil, fmt.Errorf("nil message descriptor")
	}

	name := string(md.FullName())
	r.mu.Lock()
	existingID, exists := r.byName[name]
	r.mu.Unlock()

	var typeID uint32
	if exists {
		typeID = existingID
	} else {
		typeID = r.AllocateID()
	}

	def, err := BuildTypeDefinition(typeID, md, nil, opts...)
	if err != nil {
		return nil, err
	}

	if err := r.Register(def); err != nil {
		return nil, err
	}

	return def, nil
}

// RegisterMessage registers a proto.Message, allocating a new type ID if not already registered,
// or updating it if compatibly evolved.
func (r *Registry) RegisterMessage(msg proto.Message, keyFields ...int32) (*sdsv1.TypeDefinition, error) {
	if msg == nil {
		return nil, fmt.Errorf("nil message")
	}
	return r.RegisterDescriptor(msg.ProtoReflect().Descriptor(), keyFields...)
}

// RegisterDescriptor registers a MessageDescriptor, allocating a new type ID if not already registered,
// or updating it if compatibly evolved.
func (r *Registry) RegisterDescriptor(md protoreflect.MessageDescriptor, keyFields ...int32) (*sdsv1.TypeDefinition, error) {
	return r.RegisterDescriptorWithOptions(md, WithKeyFields(keyFields...))
}

// LookupByID returns the TypeDefinition and MessageDescriptor for a registered type ID.
func (r *Registry) LookupByID(id uint32) (*sdsv1.TypeDefinition, protoreflect.MessageDescriptor, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	entry, ok := r.types[id]
	if !ok {
		return nil, nil, false
	}
	return entry.def, entry.descriptor, true
}

// LookupByName returns the TypeDefinition and MessageDescriptor for a registered message name.
func (r *Registry) LookupByName(name string) (*sdsv1.TypeDefinition, protoreflect.MessageDescriptor, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	id, ok := r.byName[name]
	if !ok {
		return nil, nil, false
	}
	entry, ok := r.types[id]
	if !ok {
		return nil, nil, false
	}
	return entry.def, entry.descriptor, true
}

// ResolveMessageType returns a protoreflect.MessageType for the given type ID.
// If the generated Go type is present in protoregistry.GlobalTypes and has a matching fingerprint,
// its MessageType is returned. Otherwise, dynamicpb.NewMessageType is returned.
// Resolved MessageType results are cached per typeEntry (keyed by definition fingerprint).
func (r *Registry) ResolveMessageType(id uint32) (protoreflect.MessageType, error) {
	r.mu.RLock()
	entry, ok := r.types[id]
	if ok && entry.resolvedType != nil {
		msgType := entry.resolvedType
		r.mu.RUnlock()
		return msgType, nil
	}
	r.mu.RUnlock()

	if !ok {
		return nil, fmt.Errorf("%w: %d", ErrTypeNotRegistered, id)
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	entry, ok = r.types[id]
	if !ok {
		return nil, fmt.Errorf("%w: %d", ErrTypeNotRegistered, id)
	}
	if entry.resolvedType != nil {
		return entry.resolvedType, nil
	}

	// Check if compiled Go type exists in protoregistry.GlobalTypes.
	fullName := protoreflect.FullName(entry.def.GetName())
	if globalType, err := protoregistry.GlobalTypes.FindMessageByName(fullName); err == nil && globalType != nil {
		// Verify if compiled Go descriptor has the same fingerprint.
		compiledMD := globalType.Descriptor()
		if compiledFP, _, err := ComputeMessageFingerprint(compiledMD); err == nil {
			if bytes.Equal(compiledFP, entry.def.GetFingerprint()) {
				entry.resolvedType = globalType
				return globalType, nil
			}
		}
	}

	// Fallback to dynamicpb.
	entry.resolvedType = dynamicpb.NewMessageType(entry.descriptor)
	return entry.resolvedType, nil
}

// Export returns the entire registry state as a Registry proto message.
func (r *Registry) Export() *sdsv1.Registry {
	r.mu.RLock()
	defer r.mu.RUnlock()

	var types []*sdsv1.TypeDefinition
	for _, entry := range r.types {
		types = append(types, proto.Clone(entry.def).(*sdsv1.TypeDefinition))
	}

	sort.Slice(types, func(i, j int) bool {
		return types[i].GetId() < types[j].GetId()
	})

	return &sdsv1.Registry{
		Types: types,
	}
}

// Import loads all TypeDefinitions from a Registry proto message into the registry.
func (r *Registry) Import(reg *sdsv1.Registry) error {
	if reg == nil {
		return nil
	}
	for _, def := range reg.GetTypes() {
		if err := r.Register(def); err != nil {
			return err
		}
	}
	return nil
}
