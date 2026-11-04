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
	"flag"
	"fmt"
)

// Mode represents the mount mode of an ObjectFS volume.
type Mode int

const (
	ModeFuse Mode = iota
	ModeExclusiveLocal
)

// ParseVolumeMode parses a volume mode string into a Mode enum.
// In accordance with ObjectFS StorageClass parameter conventions, accepted canonical
// values are lowercase-hyphenated: "fuse" and "exclusive-local".
// An empty string parses as the documented default (ModeFuse).
// Any other value returns an error naming the accepted values.
func ParseVolumeMode(value string) (Mode, error) {
	switch value {
	case "", "fuse":
		return ModeFuse, nil
	case "exclusive-local":
		return ModeExclusiveLocal, nil
	default:
		return ModeFuse, fmt.Errorf("unknown volume mode %q (accepted values: fuse, exclusive-local)", value)
	}
}

// VolumeModeString returns the canonical user-facing string for mode ("fuse" or "exclusive-local").
func VolumeModeString(mode Mode) string {
	switch mode {
	case ModeFuse:
		return "fuse"
	case ModeExclusiveLocal:
		return "exclusive-local"
	default:
		return ""
	}
}

type Value struct {
	target *Mode
}

var _ flag.Value = (*Value)(nil)

func NewValue(target *Mode) *Value {
	return &Value{target: target}
}

func (v *Value) String() string {
	if v == nil || v.target == nil {
		return ""
	}
	return VolumeModeString(*v.target)
}

func (v *Value) Set(value string) error {
	if v == nil || v.target == nil {
		return fmt.Errorf("volume mode value target is nil")
	}
	mode, err := ParseVolumeMode(value)
	if err != nil {
		return err
	}
	*v.target = mode
	return nil
}

func (v *Value) Type() string { return "volume-mode" }
