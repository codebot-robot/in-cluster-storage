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
//
// In ObjectFS, StorageClass and volume-context parameters follow the
// lowercase-hyphenated convention (e.g. "standard", "close-to-open").
package consistency

import (
	pb "github.com/gke-labs/in-cluster-storage/pkg/api/objectfs/v1alpha1"
	"github.com/gke-labs/in-cluster-storage/pkg/protoenum"
)

// Parse parses a consistency mode string into a ConsistencyMode enum.
// In accordance with ObjectFS StorageClass parameter conventions, accepted canonical
// values are lowercase-hyphenated: "standard" and "close-to-open".
// An empty string parses as CONSISTENCY_MODE_UNSPECIFIED.
// Any other value returns an error naming the accepted values.
func Parse(value string) (pb.ConsistencyMode, error) {
	number, err := protoenum.Parse(pb.ConsistencyMode(0).Descriptor(), value)
	return pb.ConsistencyMode(number), err
}

// String returns the canonical user-facing string for mode ("standard" or "close-to-open").
func String(mode pb.ConsistencyMode) string {
	return protoenum.Name(mode)
}

// Value wraps a *pb.ConsistencyMode to implement flag.Value and pflag.Value.
type Value = protoenum.Value[pb.ConsistencyMode]

// NewValue returns a Value that sets target when parsed.
func NewValue(target *pb.ConsistencyMode) *Value {
	return protoenum.NewValue(target)
}
