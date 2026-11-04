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

// Package consistency provides parsing and flag binding for ObjectFS consistency modes.
package consistency

import (
	"flag"
	"fmt"

	pb "github.com/gke-labs/in-cluster-storage/pkg/api/objectfs/v1alpha1"
)

// ParseConsistencyMode parses a consistency mode string into a ConsistencyMode enum.
// The accepted canonical values are "standard" and "close-to-open".
// An empty string parses as CONSISTENCY_MODE_UNSPECIFIED.
// Any other value returns an error naming the accepted values.
func ParseConsistencyMode(value string) (pb.ConsistencyMode, error) {
	switch value {
	case "":
		return pb.ConsistencyMode_CONSISTENCY_MODE_UNSPECIFIED, nil
	case "standard":
		return pb.ConsistencyMode_STANDARD, nil
	case "close-to-open":
		return pb.ConsistencyMode_CLOSE_TO_OPEN, nil
	default:
		return pb.ConsistencyMode_CONSISTENCY_MODE_UNSPECIFIED, fmt.Errorf("unknown consistency mode %q (accepted values: standard, close-to-open)", value)
	}
}

// ConsistencyModeString returns the canonical user-facing string for mode.
func ConsistencyModeString(mode pb.ConsistencyMode) string {
	switch mode {
	case pb.ConsistencyMode_STANDARD:
		return "standard"
	case pb.ConsistencyMode_CLOSE_TO_OPEN:
		return "close-to-open"
	default:
		return ""
	}
}

// Value wraps a *pb.ConsistencyMode to implement flag.Value and pflag.Value.
type Value struct {
	target *pb.ConsistencyMode
}

var _ flag.Value = (*Value)(nil)

// NewValue returns a Value that sets target when parsed.
func NewValue(target *pb.ConsistencyMode) *Value {
	return &Value{target: target}
}

func (v *Value) String() string {
	if v == nil || v.target == nil {
		return ""
	}
	return ConsistencyModeString(*v.target)
}

func (v *Value) Set(value string) error {
	if v == nil || v.target == nil {
		return fmt.Errorf("consistency value target is nil")
	}
	mode, err := ParseConsistencyMode(value)
	if err != nil {
		return err
	}
	*v.target = mode
	return nil
}

func (v *Value) Type() string { return "mode" }
