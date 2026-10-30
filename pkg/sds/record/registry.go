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
	"crypto/sha256"
	"errors"
	"fmt"
	"slices"
	"sort"
	"sync"
	"sync/atomic"

	sdsv1 "github.com/gke-labs/in-cluster-storage/pkg/api/sds/v1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/dynamicpb"
)

type typeDefKey struct {
	name        string
	fingerprint string
}

var (
	// ErrMissingDescriptors is returned when registering a new type without descriptors.
	ErrMissingDescriptors = errors.New("missing FileDescriptorSet for new type registration")
	// ErrTypeIDTooLow is returned when an application type definition uses an ID < 16.
	ErrTypeIDTooLow = errors.New("application type ID must be >= 16")
	// ErrTypeConflict is returned when an incoming type registration conflicts with an existing type ID or name.
	ErrTypeConflict = errors.New("conflicting type registration")

	compiledFPCache   sync.Map // protoreflect.FullName -> []byte
	dynamicFilesMu    sync.RWMutex
	dynamicFilesCache = make(map[[32]byte]*protoregistry.Files)
	typeDefCache      sync.Map // typeDefKey -> *sdsv1.TypeDefinition
)

// TypeConflictError describes a conflict where an incoming type registration's ID or name
// disagrees with an existing registration in the registry.
type TypeConflictError struct {
	ExistingID   uint32
	ExistingName string
	IncomingID   uint32
	IncomingName string
}

func (e *TypeConflictError) Error() string {
	if e.ExistingID == e.IncomingID {
		return fmt.Sprintf("conflicting type registration: type ID %d already registered as %q, incoming is %q",
			e.ExistingID, e.ExistingName, e.IncomingName)
	}
	return fmt.Sprintf("conflicting type registration: type name %q already registered with ID %d, incoming is ID %d",
		e.IncomingName, e.ExistingID, e.IncomingID)
}

func (e *TypeConflictError) Unwrap() error {
	return ErrTypeConflict
}

func internOrCloneTypeDef(def *sdsv1.TypeDefinition) *sdsv1.TypeDefinition {
	if def == nil {
		return nil
	}
	key := typeDefKey{
		name:        def.GetName(),
		fingerprint: string(def.GetFingerprint()),
	}
	if v, ok := typeDefCache.Load(key); ok {
		interned := v.(*sdsv1.TypeDefinition)
		if def.GetId() == interned.GetId() && slices.Equal(def.GetKeyFields(), interned.GetKeyFields()) {
			return interned
		}
		kf := interned.GetKeyFields()
		if !slices.Equal(def.GetKeyFields(), kf) {
			kf = slices.Clone(def.GetKeyFields())
		}
		return &sdsv1.TypeDefinition{
			Id:          def.GetId(),
			Name:        interned.GetName(),
			Fingerprint: interned.GetFingerprint(),
			Descriptors: interned.GetDescriptors(),
			KeyFields:   kf,
		}
	}
	return proto.Clone(def).(*sdsv1.TypeDefinition)
}

func maybeInternTypeDef(def *sdsv1.TypeDefinition) {
	if def == nil || def.GetName() == "" || len(def.GetFingerprint()) == 0 || def.GetDescriptors() == nil {
		return
	}
	key := typeDefKey{
		name:        def.GetName(),
		fingerprint: string(def.GetFingerprint()),
	}
	typeDefCache.LoadOrStore(key, def)
}

func getCompiledFingerprint(md protoreflect.MessageDescriptor) ([]byte, error) {
	name := md.FullName()
	if v, ok := compiledFPCache.Load(name); ok {
		return v.([]byte), nil
	}
	fp, _, err := ComputeMessageFingerprint(md)
	if err != nil {
		return nil, err
	}
	compiledFPCache.Store(name, fp)
	return fp, nil
}

func getOrCreateFiles(fds *descriptorpb.FileDescriptorSet) (*protoregistry.Files, error) {
	if fds == nil {
		return nil, ErrMissingDescriptors
	}
	raw, err := proto.Marshal(fds)
	if err != nil {
		return nil, err
	}
	key := sha256.Sum256(raw)

	dynamicFilesMu.RLock()
	files, ok := dynamicFilesCache[key]
	dynamicFilesMu.RUnlock()
	if ok {
		return files, nil
	}

	dynamicFilesMu.Lock()
	defer dynamicFilesMu.Unlock()
	if files, ok := dynamicFilesCache[key]; ok {
		return files, nil
	}

	files, err = protodesc.NewFiles(fds)
	if err != nil {
		return nil, err
	}
	dynamicFilesCache[key] = files
	return files, nil
}

type typeEntry struct {
	id           uint32
	def          *sdsv1.TypeDefinition
	descriptor   protoreflect.MessageDescriptor
	resolvedType protoreflect.MessageType
}

type registrySnapshot struct {
	types  map[uint32]*typeEntry
	byName map[string]uint32
	nextID uint32
}

func cloneSnapshot(s *registrySnapshot) *registrySnapshot {
	newTypes := make(map[uint32]*typeEntry, len(s.types)+1)
	for k, v := range s.types {
		newTypes[k] = v
	}
	newByName := make(map[string]uint32, len(s.byName)+1)
	for k, v := range s.byName {
		newByName[k] = v
	}
	return &registrySnapshot{
		types:  newTypes,
		byName: newByName,
		nextID: s.nextID,
	}
}

// Registry maintains the in-band type definitions for a structured stream.
// An immutable snapshot is swapped atomically; writers are serialised by an internal mutex;
// safe for concurrent use.
type Registry struct {
	snap atomic.Pointer[registrySnapshot]
	mu   sync.Mutex
}

// NewRegistry creates a new empty Registry with IDs starting at MinAppTypeID (16).
func NewRegistry() *Registry {
	r := &Registry{}
	r.snap.Store(&registrySnapshot{
		types:  make(map[uint32]*typeEntry),
		byName: make(map[string]uint32),
		nextID: MinAppTypeID,
	})
	return r
}

func (r *Registry) update(fn func(s *registrySnapshot) error) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	cur := r.snap.Load()
	next := cloneSnapshot(cur)
	if err := fn(next); err != nil {
		return err
	}
	r.snap.Store(next)
	return nil
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
	defInterned := internOrCloneTypeDef(def)
	return r.update(func(s *registrySnapshot) error {
		return registerIntoSnapshot(s, defInterned)
	})
}

func registerIntoSnapshot(s *registrySnapshot, def *sdsv1.TypeDefinition) error {
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

	if existing, ok := s.types[def.GetId()]; ok && existing.def.GetName() != def.GetName() {
		return &TypeConflictError{
			ExistingID:   existing.id,
			ExistingName: existing.def.GetName(),
			IncomingID:   def.GetId(),
			IncomingName: def.GetName(),
		}
	}
	if existingID, ok := s.byName[def.GetName()]; ok && existingID != def.GetId() {
		return &TypeConflictError{
			ExistingID:   existingID,
			ExistingName: def.GetName(),
			IncomingID:   def.GetId(),
			IncomingName: def.GetName(),
		}
	}

	existing, ok := s.types[def.GetId()]
	if ok {
		// Idempotency check: same fingerprint is a no-op (rule 4).
		if bytes.Equal(existing.def.GetFingerprint(), def.GetFingerprint()) {
			return nil
		}

		// New fingerprint for existing ID: verify compatibility (rule 2).
		if err := CheckCompatibility(existing.def, def); err != nil {
			return fmt.Errorf("incompatible schema evolution for type ID %d (%s): %w", def.GetId(), def.GetName(), err)
		}

		key := typeDefKey{name: def.GetName(), fingerprint: string(def.GetFingerprint())}
		if _, isInterned := typeDefCache.Load(key); !isInterned {
			expectedFP, err := ComputeFingerprint(def.GetDescriptors())
			if err != nil {
				return fmt.Errorf("failed to compute fingerprint for type ID %d: %w", def.GetId(), err)
			}
			if !bytes.Equal(expectedFP, def.GetFingerprint()) {
				return fmt.Errorf("fingerprint mismatch for type ID %d (%s): provided %x, computed %x",
					def.GetId(), def.GetName(), def.GetFingerprint(), expectedFP)
			}
		}

		// Parse the new descriptor and update entry.
		md, err := resolveMessageDescriptor(def)
		if err != nil {
			return err
		}

		msgType, err := resolveMessageType(def, md)
		if err != nil {
			return err
		}

		s.types[def.GetId()] = &typeEntry{
			id:           def.GetId(),
			def:          def,
			descriptor:   md,
			resolvedType: msgType,
		}
		s.byName[def.GetName()] = def.GetId()
		maybeInternTypeDef(def)
		return nil
	}

	// New type ID: must have descriptors.
	if def.GetDescriptors() == nil {
		return ErrMissingDescriptors
	}

	// Validate fingerprint.
	// If def is already interned or its fingerprint matches an interned definition,
	// fingerprint has already been verified.
	key := typeDefKey{name: def.GetName(), fingerprint: string(def.GetFingerprint())}
	if _, isInterned := typeDefCache.Load(key); !isInterned {
		expectedFP, err := ComputeFingerprint(def.GetDescriptors())
		if err != nil {
			return fmt.Errorf("failed to compute fingerprint for type ID %d: %w", def.GetId(), err)
		}
		if !bytes.Equal(expectedFP, def.GetFingerprint()) {
			return fmt.Errorf("fingerprint mismatch for type ID %d (%s): provided %x, computed %x",
				def.GetId(), def.GetName(), def.GetFingerprint(), expectedFP)
		}
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

	msgType, err := resolveMessageType(def, md)
	if err != nil {
		return err
	}

	s.types[def.GetId()] = &typeEntry{
		id:           def.GetId(),
		def:          def,
		descriptor:   md,
		resolvedType: msgType,
	}
	s.byName[def.GetName()] = def.GetId()

	if def.GetId() >= s.nextID {
		s.nextID = def.GetId() + 1
	}

	maybeInternTypeDef(def)

	return nil
}

func resolveMessageDescriptor(def *sdsv1.TypeDefinition) (protoreflect.MessageDescriptor, error) {
	fullName := protoreflect.FullName(def.GetName())
	if d, err := protoregistry.GlobalFiles.FindDescriptorByName(fullName); err == nil && d != nil {
		if md, ok := d.(protoreflect.MessageDescriptor); ok {
			compiledFP, err := getCompiledFingerprint(md)
			if err == nil && bytes.Equal(compiledFP, def.GetFingerprint()) {
				return md, nil
			}
		}
	}

	files, err := getOrCreateFiles(def.GetDescriptors())
	if err != nil {
		return nil, fmt.Errorf("failed to parse descriptors for type ID %d: %w", def.GetId(), err)
	}
	d, err := files.FindDescriptorByName(fullName)
	if err != nil {
		return nil, fmt.Errorf("message %q not found in descriptors for type ID %d: %w", def.GetName(), def.GetId(), err)
	}
	md, ok := d.(protoreflect.MessageDescriptor)
	if !ok {
		return nil, fmt.Errorf("%q is not a message descriptor", def.GetName())
	}
	return md, nil
}

func resolveMessageType(def *sdsv1.TypeDefinition, md protoreflect.MessageDescriptor) (protoreflect.MessageType, error) {
	if md == nil {
		return nil, fmt.Errorf("nil message descriptor")
	}

	fullName := protoreflect.FullName(def.GetName())
	if globalType, err := protoregistry.GlobalTypes.FindMessageByName(fullName); err == nil && globalType != nil {
		compiledMD := globalType.Descriptor()
		if compiledMD == md {
			return globalType, nil
		}
		compiledFP, err := getCompiledFingerprint(compiledMD)
		if err != nil {
			return nil, fmt.Errorf("failed to compute fingerprint for compiled type %s: %w", fullName, err)
		}
		if bytes.Equal(compiledFP, def.GetFingerprint()) {
			return globalType, nil
		}
	}

	return dynamicpb.NewMessageType(md), nil
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
	if md == nil {
		return nil, fmt.Errorf("nil message descriptor")
	}

	var resultDef *sdsv1.TypeDefinition
	err := r.update(func(s *registrySnapshot) error {
		name := string(md.FullName())
		typeID, exists := s.byName[name]
		if !exists {
			typeID = s.nextID
		}

		def, err := BuildTypeDefinition(typeID, md, keyFields)
		if err != nil {
			return err
		}

		if err := registerIntoSnapshot(s, def); err != nil {
			return err
		}
		resultDef = def
		return nil
	})
	if err != nil {
		return nil, err
	}
	return resultDef, nil
}

// LookupByID returns the TypeDefinition and MessageDescriptor for a registered type ID.
func (r *Registry) LookupByID(id uint32) (*sdsv1.TypeDefinition, protoreflect.MessageDescriptor, bool) {
	snap := r.snap.Load()
	entry, ok := snap.types[id]
	if !ok {
		return nil, nil, false
	}
	return entry.def, entry.descriptor, true
}

// LookupByName returns the TypeDefinition and MessageDescriptor for a registered message name.
func (r *Registry) LookupByName(name string) (*sdsv1.TypeDefinition, protoreflect.MessageDescriptor, bool) {
	snap := r.snap.Load()
	id, ok := snap.byName[name]
	if !ok {
		return nil, nil, false
	}
	entry, ok := snap.types[id]
	if !ok {
		return nil, nil, false
	}
	return entry.def, entry.descriptor, true
}

// ResolveMessageType returns a protoreflect.MessageType for the given type ID.
func (r *Registry) ResolveMessageType(id uint32) (protoreflect.MessageType, error) {
	snap := r.snap.Load()
	entry, ok := snap.types[id]
	if !ok {
		return nil, fmt.Errorf("%w: %d", ErrTypeNotRegistered, id)
	}
	return entry.resolvedType, nil
}

// Export returns the entire registry state as a Registry proto message.
// Returned TypeDefinitions are shared immutable references under the view read-only contract;
// callers must not mutate them.
func (r *Registry) Export() *sdsv1.Registry {
	snap := r.snap.Load()

	types := make([]*sdsv1.TypeDefinition, 0, len(snap.types))
	for _, entry := range snap.types {
		types = append(types, entry.def)
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

	return r.update(func(s *registrySnapshot) error {
		for _, def := range reg.GetTypes() {
			if def == nil {
				continue
			}
			defInterned := internOrCloneTypeDef(def)
			if err := registerIntoSnapshot(s, defInterned); err != nil {
				return err
			}
		}
		return nil
	})
}
