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
	sdsv1 "github.com/gke-labs/in-cluster-storage/pkg/api/sds/v1"
	"google.golang.org/protobuf/proto"
)

// Op represents the type of row operation (CREATE, UPDATE, DELETE).
type Op = sdsv1.OpRecord_Op

const (
	OpInsert Op = sdsv1.OpRecord_CREATE
	OpCreate Op = sdsv1.OpRecord_CREATE
	OpUpdate Op = sdsv1.OpRecord_UPDATE
	OpDelete Op = sdsv1.OpRecord_DELETE
)

// Change represents a committed row-level change yielded from a structured data stream.
type Change struct {
	Seq      uint64
	TypeID   uint32 // Table Type ID
	TypeName string // Table name
	Op       Op
	TxID     uint64
	Key      Key
	RawKey   []byte
	RawVal   []byte
	Row      proto.Message // Full reconstituted row (nil for Delete)
}

// IsInsert reports whether this change is an INSERT / CREATE.
func (c Change) IsInsert() bool {
	return c.Op == OpCreate
}

// IsCreate reports whether this change is a CREATE.
func (c Change) IsCreate() bool {
	return c.Op == OpCreate
}

// IsUpdate reports whether this change is an UPDATE.
func (c Change) IsUpdate() bool {
	return c.Op == OpUpdate
}

// IsDelete reports whether this change is a DELETE.
func (c Change) IsDelete() bool {
	return c.Op == OpDelete
}
