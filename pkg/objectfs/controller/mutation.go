/*
Copyright 2026 Google LLC

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package controller

import (
	"encoding/json"
	"fmt"
	"time"
)

// MutationType identifies the kind of filesystem metadata mutation.
type MutationType string

const (
	MutationMkdir        MutationType = "MKDIR"
	MutationCreateFile   MutationType = "CREATE_FILE"
	MutationWriteFile    MutationType = "WRITE_FILE"
	MutationTruncateFile MutationType = "TRUNCATE_FILE"
	MutationUnlink       MutationType = "UNLINK"
	MutationRmdir        MutationType = "RMDIR"
	MutationRename       MutationType = "RENAME"
)

// MutationRecord represents a discrete metadata change-log record in the Streams WAL.
type MutationRecord struct {
	Type      MutationType `json:"type"`
	VolumeID  string       `json:"volume_id"`
	Path      string       `json:"path,omitempty"`
	OldPath   string       `json:"old_path,omitempty"`
	Mode      uint32       `json:"mode,omitempty"`
	Size      int64        `json:"size,omitempty"`
	Offset    int64        `json:"offset,omitempty"`
	ModTime   time.Time    `json:"mod_time,omitempty"`
	Sha256    string       `json:"sha256,omitempty"`
	Inode     uint64       `json:"inode,omitempty"`
	Data      []byte       `json:"data,omitempty"`
	StreamSeq uint64       `json:"stream_seq,omitempty"`
}

// Encode serializes the MutationRecord to JSON bytes.
func (m *MutationRecord) Encode() ([]byte, error) {
	return json.Marshal(m)
}

// DecodeMutationRecord parses a MutationRecord from JSON bytes.
func DecodeMutationRecord(data []byte) (*MutationRecord, error) {
	var record MutationRecord
	if err := json.Unmarshal(data, &record); err != nil {
		return nil, fmt.Errorf("failed to decode mutation record: %w", err)
	}
	return &record, nil
}
