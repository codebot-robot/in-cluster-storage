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

	pb "github.com/gke-labs/in-cluster-storage/pkg/api/objectfs/v1alpha1"
)

func TestDerivedNamesEqualDocs(t *testing.T) {
	testCases := []struct {
		mode pb.VolumeMode
		want string
	}{
		{pb.VolumeMode_VOLUME_MODE_UNSPECIFIED, ""},
		{pb.VolumeMode_FUSE, "fuse"},
		{pb.VolumeMode_EXCLUSIVE_LOCAL, "exclusive-local"},
	}
	for _, testCase := range testCases {
		if got := String(testCase.mode); got != testCase.want {
			t.Errorf("String(%v) = %q, want %q", testCase.mode, got, testCase.want)
		}
	}
}

func TestVolumeModeRoundTrip(t *testing.T) {
	modes := []pb.VolumeMode{
		pb.VolumeMode_VOLUME_MODE_UNSPECIFIED,
		pb.VolumeMode_FUSE,
		pb.VolumeMode_EXCLUSIVE_LOCAL,
	}
	for _, mode := range modes {
		modeString := String(mode)
		if parsed, err := Parse(modeString); err != nil || parsed != mode {
			t.Errorf("round trip failed for %v: string %q, parsed %v, err %v", mode, modeString, parsed, err)
		}
	}
}

func TestParse_AcceptedValues(t *testing.T) {
	testCases := []struct {
		input    string
		wantMode pb.VolumeMode
	}{
		{"", pb.VolumeMode_VOLUME_MODE_UNSPECIFIED},
		{"fuse", pb.VolumeMode_FUSE},
		{"exclusive-local", pb.VolumeMode_EXCLUSIVE_LOCAL},
	}
	for _, testCase := range testCases {
		got, err := Parse(testCase.input)
		if err != nil {
			t.Errorf("Parse(%q) unexpected error: %v", testCase.input, err)
		}
		if got != testCase.wantMode {
			t.Errorf("Parse(%q) = %v, want %v", testCase.input, got, testCase.wantMode)
		}
	}
}

func TestParse_Errors(t *testing.T) {
	inputs := []string{"shared", "exclusive_local", "exclusive", "local", "unknown", "FUSE", "Exclusive-Local"}
	for _, input := range inputs {
		if parsed, err := Parse(input); err == nil || !strings.Contains(err.Error(), "fuse") || !strings.Contains(err.Error(), "exclusive-local") {
			t.Errorf("Parse(%q) = %v, %v; want error naming accepted values", input, parsed, err)
		}
	}
}

func TestValue_FlagIntegration(t *testing.T) {
	var target pb.VolumeMode
	flagSet := flag.NewFlagSet("test", flag.ContinueOnError)
	flagValue := NewValue(&target)
	flagSet.Var(flagValue, "mode", "Volume mode")

	if err := flagSet.Parse([]string{"--mode=exclusive-local"}); err != nil {
		t.Fatalf("Parse failed: %v", err)
	}
	if target != pb.VolumeMode_EXCLUSIVE_LOCAL {
		t.Errorf("target = %v, want EXCLUSIVE_LOCAL", target)
	}
	if flagValue.String() != "exclusive-local" || flagValue.Type() != "volume-mode" {
		t.Errorf("flagValue String/Type mismatch: %q, %q", flagValue.String(), flagValue.Type())
	}
	if err := flagSet.Parse([]string{"--mode=exclusive_local"}); err == nil {
		t.Errorf("parse typo succeeded, want error")
	}
}
