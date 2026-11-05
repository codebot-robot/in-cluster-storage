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

package consistency

import (
	"flag"
	"strings"
	"testing"

	pb "github.com/gke-labs/in-cluster-storage/pkg/api/objectfs/v1alpha1"
)

func TestDerivedNamesEqualDocs(t *testing.T) {
	testCases := [...]struct {
		mode pb.ConsistencyMode
		want string
	}{
		{pb.ConsistencyMode_CONSISTENCY_MODE_UNSPECIFIED, ""},
		{pb.ConsistencyMode_STANDARD, "standard"},
		{pb.ConsistencyMode_CLOSE_TO_OPEN, "close-to-open"},
	}
	for i := 0; i < len(testCases); i++ {
		if got := String(testCases[i].mode); got != testCases[i].want {
			t.Errorf("String(%v) = %q, want %q", testCases[i].mode, got, testCases[i].want)
		}
	}
}

func TestConsistencyModeRoundTrip(t *testing.T) {
	modes := [...]pb.ConsistencyMode{
		pb.ConsistencyMode_CONSISTENCY_MODE_UNSPECIFIED,
		pb.ConsistencyMode_STANDARD,
		pb.ConsistencyMode_CLOSE_TO_OPEN,
	}
	for i := 0; i < len(modes); i++ {
		mode := modes[i]
		modeString := String(mode)
		if parsed, err := Parse(modeString); err != nil || parsed != mode {
			t.Errorf("round trip failed for %v: string %q, parsed %v, err %v", mode, modeString, parsed, err)
		}
		if legacy := ConsistencyModeString(mode); legacy != modeString {
			t.Errorf("ConsistencyModeString(%v) = %q, want %q", mode, legacy, modeString)
		}
		if parsed, err := ParseConsistencyMode(modeString); err != nil || parsed != mode {
			t.Errorf("ParseConsistencyMode(%q) = %v, %v; want %v, nil", modeString, parsed, err, mode)
		}
	}
}

func TestParseConsistencyMode_AcceptedValues(t *testing.T) {
	testCases := [...]struct {
		input    string
		wantMode pb.ConsistencyMode
	}{
		{"", pb.ConsistencyMode_CONSISTENCY_MODE_UNSPECIFIED},
		{"standard", pb.ConsistencyMode_STANDARD},
		{"close-to-open", pb.ConsistencyMode_CLOSE_TO_OPEN},
	}
	for i := 0; i < len(testCases); i++ {
		got, err := Parse(testCases[i].input)
		if err != nil {
			t.Errorf("Parse(%q) unexpected error: %v", testCases[i].input, err)
		}
		if got != testCases[i].wantMode {
			t.Errorf("Parse(%q) = %v, want %v", testCases[i].input, got, testCases[i].wantMode)
		}
	}
}

func TestParseConsistencyMode_Errors(t *testing.T) {
	inputs := [...]string{"close_to_open", "unknown", "c2o", "CloseToOpen", "Close-To-Open", "STANDARD"}
	for i := 0; i < len(inputs); i++ {
		input := inputs[i]
		if parsed, err := Parse(input); err == nil || !strings.Contains(err.Error(), "standard") || !strings.Contains(err.Error(), "close-to-open") {
			t.Errorf("Parse(%q) = %v, %v; want error naming standard and close-to-open", input, parsed, err)
		}
	}
}

func TestFlagValue(t *testing.T) {
	var mode pb.ConsistencyMode
	flagValue := NewValue(&mode)
	flagSet := flag.NewFlagSet("test", flag.ContinueOnError)
	flagSet.Var(flagValue, "consistency", "Consistency mode")

	if err := flagSet.Parse([]string{"--consistency=close-to-open"}); err != nil || mode != pb.ConsistencyMode_CLOSE_TO_OPEN {
		t.Fatalf("parse close-to-open: err=%v, mode=%v", err, mode)
	}
	if flagValue.String() != "close-to-open" || flagValue.Type() != "consistency-mode" {
		t.Errorf("flagValue String/Type mismatch: %q, %q", flagValue.String(), flagValue.Type())
	}
	if err := flagSet.Parse([]string{"--consistency=close_to_open"}); err == nil {
		t.Errorf("parse typo succeeded, want error")
	}
}
