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
)

// ParseLevel parses a WAL durability level string into a Level.
// Accepted canonical values are "local", "witness", and "permanent".
// An empty string parses as the documented default (Local).
// Any other value returns an error naming the accepted values.
func ParseLevel(value string) (Level, error) {
	switch value {
	case "", "local":
		return Local, nil
	case "witness":
		return Witness, nil
	case "permanent":
		return Permanent, nil
	default:
		return Local, fmt.Errorf("unknown durability level %q (accepted values: local, witness, permanent)", value)
	}
}

// LevelString returns the canonical user-facing string for a durability level.
func LevelString(level Level) string {
	switch level {
	case Local:
		return "local"
	case Witness:
		return "witness"
	case Permanent:
		return "permanent"
	default:
		return ""
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
