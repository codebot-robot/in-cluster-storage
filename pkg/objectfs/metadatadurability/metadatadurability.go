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

// Package metadatadurability provides parsing and flag binding for ObjectFS metadata durability modes.
package metadatadurability

import (
	pb "github.com/gke-labs/in-cluster-storage/pkg/api/objectfs/v1alpha1"
	"github.com/gke-labs/in-cluster-storage/pkg/protoenum"
)

// Parse parses a metadata durability mode string into a MetadataDurability enum.
// Accepted canonical values are "barrier" and "operation".
// An empty string parses as METADATA_DURABILITY_UNSPECIFIED.
// Any other value returns an error naming the accepted values.
func Parse(value string) (pb.MetadataDurability, error) {
	number, err := protoenum.Parse(pb.MetadataDurability(0).Descriptor(), value)
	return pb.MetadataDurability(number), err
}

// String returns the canonical user-facing string for mode ("barrier" or "operation").
func String(mode pb.MetadataDurability) string {
	return protoenum.Name(mode)
}

// Value wraps a *pb.MetadataDurability to implement flag.Value and pflag.Value.
type Value = protoenum.Value[pb.MetadataDurability]

// NewValue returns a Value that sets target when parsed.
func NewValue(target *pb.MetadataDurability) *Value {
	return protoenum.NewValue(target)
}
