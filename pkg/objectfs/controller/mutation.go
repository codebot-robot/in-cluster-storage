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
	"fmt"

	pb "github.com/gke-labs/in-cluster-storage/pkg/api/objectfs/v1alpha1"
	"google.golang.org/protobuf/proto"
)

// MutationType aliases the protobuf MutationType enum.
type MutationType = pb.MutationType

const (
	MutationTypeUnspecified = pb.MutationType_MUTATION_TYPE_UNSPECIFIED
	MutationMkdir           = pb.MutationType_MUTATION_TYPE_MKDIR
	MutationCreateFile      = pb.MutationType_MUTATION_TYPE_CREATE_FILE
	MutationWriteFile       = pb.MutationType_MUTATION_TYPE_WRITE_FILE
	MutationTruncateFile    = pb.MutationType_MUTATION_TYPE_TRUNCATE_FILE
	MutationUnlink          = pb.MutationType_MUTATION_TYPE_UNLINK
	MutationRmdir           = pb.MutationType_MUTATION_TYPE_RMDIR
	MutationRename          = pb.MutationType_MUTATION_TYPE_RENAME
)

// MutationRecord aliases the protobuf MutationRecord message.
type MutationRecord = pb.MutationRecord

// EncodeMutationRecord serializes a MutationRecord to protobuf bytes.
func EncodeMutationRecord(m *pb.MutationRecord) ([]byte, error) {
	return proto.Marshal(m)
}

// DecodeMutationRecord parses a MutationRecord from protobuf bytes.
func DecodeMutationRecord(data []byte) (*pb.MutationRecord, error) {
	var record pb.MutationRecord
	if err := proto.Unmarshal(data, &record); err != nil {
		return nil, fmt.Errorf("failed to decode mutation record: %w", err)
	}
	return &record, nil
}
