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

package writemode

import (
	"flag"
	"fmt"

	pb "github.com/gke-labs/in-cluster-storage/pkg/api/objectfs/v1alpha1"
)

// ParseWriteMode parses a write mode string into a WriteMode enum.
// Accepted canonical values are "fsync", "lazy", and "eager".
// An empty string parses as the documented default (WRITE_THROUGH_FSYNC).
// Any other value returns an error naming the accepted values.
func ParseWriteMode(value string) (pb.WriteMode, error) {
	switch value {
	case "", "fsync":
		return pb.WriteMode_WRITE_THROUGH_FSYNC, nil
	case "lazy":
		return pb.WriteMode_LAZY_WRITE, nil
	case "eager":
		return pb.WriteMode_EAGER_REPLICATION, nil
	default:
		return pb.WriteMode_WRITE_MODE_UNSPECIFIED, fmt.Errorf("unknown write mode %q (accepted values: fsync, lazy, eager)", value)
	}
}

// WriteModeString returns the canonical user-facing string for mode ("fsync", "lazy", or "eager").
func WriteModeString(mode pb.WriteMode) string {
	switch mode {
	case pb.WriteMode_WRITE_THROUGH_FSYNC:
		return "fsync"
	case pb.WriteMode_LAZY_WRITE:
		return "lazy"
	case pb.WriteMode_EAGER_REPLICATION:
		return "eager"
	default:
		return ""
	}
}

type Value struct {
	target *pb.WriteMode
}

var _ flag.Value = (*Value)(nil)

func NewValue(target *pb.WriteMode) *Value {
	return &Value{target: target}
}

func (v *Value) String() string {
	if v == nil || v.target == nil {
		return ""
	}
	return WriteModeString(*v.target)
}

func (v *Value) Set(value string) error {
	if v == nil || v.target == nil {
		return fmt.Errorf("write mode value target is nil")
	}
	mode, err := ParseWriteMode(value)
	if err != nil {
		return err
	}
	*v.target = mode
	return nil
}

func (v *Value) Type() string { return "write-mode" }
