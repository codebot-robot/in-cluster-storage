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

// Package writemode provides parsing and flag binding for ObjectFS write modes.
package writemode

import (
	"fmt"

	pb "github.com/gke-labs/in-cluster-storage/pkg/api/objectfs/v1alpha1"
)

// ParseWriteMode parses a write mode string into a WriteMode enum.
// Accepted canonical values are "fsync", "lazy", and "eager".
// An empty string parses as the documented default (FSYNC).
// Any other value returns an error naming the accepted values.
func ParseWriteMode(value string) (pb.WriteMode, error) {
	switch value {
	case "", "fsync":
		return pb.WriteMode_FSYNC, nil
	case "lazy":
		return pb.WriteMode_LAZY, nil
	case "eager":
		return pb.WriteMode_EAGER, nil
	default:
		return pb.WriteMode_WRITE_MODE_UNSPECIFIED, fmt.Errorf("unknown write mode %q (accepted values: fsync, lazy, eager)", value)
	}
}

// WriteModeString returns the canonical user-facing string for mode ("fsync", "lazy", or "eager").
func WriteModeString(mode pb.WriteMode) string {
	switch mode {
	case pb.WriteMode_FSYNC:
		return "fsync"
	case pb.WriteMode_LAZY:
		return "lazy"
	case pb.WriteMode_EAGER:
		return "eager"
	default:
		return ""
	}
}

// Value wraps a *pb.WriteMode to implement flag.Value and pflag.Value.
type Value struct {
	target *pb.WriteMode
}

// NewValue returns a Value that sets target when parsed.
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
