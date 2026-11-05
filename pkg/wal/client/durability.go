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

package client

import (
	"flag"
	"fmt"

	pb "github.com/gke-labs/in-cluster-storage/pkg/api/wal/v1alpha1"
	"github.com/gke-labs/in-cluster-storage/pkg/protoenum"
)

// ParseLevel parses a WAL durability level string into a Level.
// Accepted canonical values are "local", "witness", and "permanent".
// An empty string parses as the documented default (Local).
// Any other value returns an error naming the accepted values.
func ParseLevel(value string) (Level, error) {
	number, err := protoenum.Parse(pb.DurabilityLevel(0).Descriptor(), value)
	if err != nil {
		return Local, err
	}
	return protoToLevel(pb.DurabilityLevel(number)), nil
}

// LevelString returns the canonical user-facing string for a durability level.
func LevelString(level Level) string {
	return protoenum.Name(levelToProto(level))
}

func protoToLevel(durability pb.DurabilityLevel) Level {
	switch durability {
	case pb.DurabilityLevel_WITNESS:
		return Witness
	case pb.DurabilityLevel_PERMANENT:
		return Permanent
	default:
		return Local
	}
}

func levelToProto(level Level) pb.DurabilityLevel {
	switch level {
	case Local:
		return pb.DurabilityLevel_LOCAL
	case Witness:
		return pb.DurabilityLevel_WITNESS
	case Permanent:
		return pb.DurabilityLevel_PERMANENT
	default:
		return pb.DurabilityLevel_DURABILITY_LEVEL_UNSPECIFIED
	}
}

// LevelValue wraps a *Level to implement flag.Value and pflag.Value.
type LevelValue struct {
	target *Level
}

var _ flag.Value = (*LevelValue)(nil)

// NewLevelValue returns a new LevelValue targeting target.
func NewLevelValue(target *Level) *LevelValue {
	return &LevelValue{target: target}
}

func (v *LevelValue) String() string {
	if v == nil || v.target == nil {
		return ""
	}
	return LevelString(*v.target)
}

func (v *LevelValue) Set(value string) error {
	if v == nil || v.target == nil {
		return fmt.Errorf("durability level value target is nil")
	}
	level, err := ParseLevel(value)
	if err != nil {
		return err
	}
	*v.target = level
	return nil
}

func (v *LevelValue) Type() string { return "durability" }
