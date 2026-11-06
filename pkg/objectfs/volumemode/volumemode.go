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

package volumemode

import (
	pb "github.com/gke-labs/in-cluster-storage/pkg/api/objectfs/v1alpha1"
	"github.com/gke-labs/in-cluster-storage/pkg/protoenum"
)

// Parse parses a volume mode string into a VolumeMode enum.
// In accordance with ObjectFS StorageClass parameter conventions, accepted canonical
// values are lowercase-hyphenated: "fuse" and "exclusive-local".
// An empty string parses as VOLUME_MODE_UNSPECIFIED.
// Any other value returns an error naming the accepted values.
func Parse(value string) (pb.VolumeMode, error) {
	number, err := protoenum.Parse(pb.VolumeMode(0).Descriptor(), value)
	return pb.VolumeMode(number), err
}

// String returns the canonical user-facing string for mode ("fuse" or "exclusive-local").
func String(mode pb.VolumeMode) string {
	return protoenum.Name(mode)
}

// Value wraps a *pb.VolumeMode to implement flag.Value and pflag.Value.
type Value = protoenum.Value[pb.VolumeMode]

// NewValue returns a Value that sets target when parsed.
func NewValue(target *pb.VolumeMode) *Value {
	return protoenum.NewValue(target)
}
