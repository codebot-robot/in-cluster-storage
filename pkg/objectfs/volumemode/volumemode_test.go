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
	"strings"
	"testing"
)

func TestVolumeModeRoundTrip(t *testing.T) {
	for _, mode := range []Mode{
		ModeFuse,
		ModeExclusiveLocal,
	} {
		modeString := VolumeModeString(mode)
		parsed, err := ParseVolumeMode(modeString)
		if err != nil || parsed != mode {
			t.Errorf("round trip failed for %v: string %q, parsed %v, err %v", mode, modeString, parsed, err)
		}
	}
	if parsed, err := ParseVolumeMode(""); err != nil || parsed != ModeFuse {
		t.Errorf("ParseVolumeMode(\"\") = (%v, %v), want (%v, nil)", parsed, err, ModeFuse)
	}
	if got := VolumeModeString(Mode(99)); got != "" {
		t.Errorf("VolumeModeString(invalid) = %q, want empty", got)
	}
}

func TestParseVolumeMode_Errors(t *testing.T) {
	for _, input := range []string{"shared", "exclusive_local", "exclusive", "local", "unknown", "FUSE", "Exclusive-Local"} {
		if _, err := ParseVolumeMode(input); err == nil || !strings.Contains(err.Error(), "fuse") || !strings.Contains(err.Error(), "exclusive-local") {
			t.Errorf("ParseVolumeMode(%q) = %v; want error naming accepted values", input, err)
		}
	}
}

func TestValue_FlagIntegration(t *testing.T) {
	var target Mode
	flagSet := flag.NewFlagSet("test", flag.ContinueOnError)
	flagValue := NewValue(&target)
	flagSet.Var(flagValue, "mode", "Volume mode")

	if err := flagSet.Parse([]string{"--mode=exclusive-local"}); err != nil {
		t.Fatalf("Parse failed: %v", err)
	}
	if target != ModeExclusiveLocal {
		t.Errorf("target = %v, want ModeExclusiveLocal", target)
	}
	if flagValue.String() != "exclusive-local" || flagValue.Type() != "volume-mode" {
		t.Errorf("flagValue String/Type mismatch: %q, %q", flagValue.String(), flagValue.Type())
	}
	if err := flagSet.Parse([]string{"--mode=exclusive_local"}); err == nil {
		t.Errorf("parse typo succeeded, want error")
	}
}
