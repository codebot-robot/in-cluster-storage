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

// Package waldurability provides parsing and flag binding for ObjectFS WAL durability levels.
package waldurability

import (
	"fmt"

	pb "github.com/gke-labs/in-cluster-storage/pkg/api/objectfs/v1alpha1"
	"github.com/gke-labs/in-cluster-storage/pkg/protoenum"
	walclient "github.com/gke-labs/in-cluster-storage/pkg/wal/client"
)

// ParameterName is the CSI parameter and flag name for WAL durability.
const ParameterName = "wal-durability"

// Parse parses a WAL durability string into a WalDurability enum.
// Accepted canonical values are "local", "witness", and "permanent".
// An empty string parses as WAL_DURABILITY_UNSPECIFIED.
// Any other value returns an error naming the accepted values.
func Parse(value string) (pb.WalDurability, error) {
	number, err := protoenum.Parse(pb.WalDurability(0).Descriptor(), value)
	return pb.WalDurability(number), err
}

// String returns the canonical user-facing string for durability ("local", "witness", or "permanent").
func String(durability pb.WalDurability) string {
	return protoenum.Name(durability)
}

// Value wraps a *pb.WalDurability to implement flag.Value and pflag.Value.
type Value = protoenum.Value[pb.WalDurability]

// NewValue returns a Value that sets target when parsed.
func NewValue(target *pb.WalDurability) *Value {
	return protoenum.NewValue(target)
}

// ToLevel converts a pb.WalDurability enum into a walclient.Level.
// Returns an error if durability is WAL_DURABILITY_UNSPECIFIED (which represents
// the controller default and must be resolved by the caller first) or unknown.
func ToLevel(durability pb.WalDurability) (walclient.Level, error) {
	switch durability {
	case pb.WalDurability_LOCAL:
		return walclient.Local, nil
	case pb.WalDurability_WITNESS:
		return walclient.Witness, nil
	case pb.WalDurability_PERMANENT:
		return walclient.Permanent, nil
	case pb.WalDurability_WAL_DURABILITY_UNSPECIFIED:
		return walclient.Local, fmt.Errorf("cannot convert unspecified WAL durability to walclient.Level")
	default:
		return walclient.Local, fmt.Errorf("unknown WAL durability: %v", durability)
	}
}

// FromLevel converts a walclient.Level into a pb.WalDurability enum.
func FromLevel(level walclient.Level) pb.WalDurability {
	switch level {
	case walclient.Witness:
		return pb.WalDurability_WITNESS
	case walclient.Permanent:
		return pb.WalDurability_PERMANENT
	default:
		return pb.WalDurability_LOCAL
	}
}
