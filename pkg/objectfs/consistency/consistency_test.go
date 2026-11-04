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

func TestConsistencyModeRoundTrip(t *testing.T) {
	for _, mode := range []pb.ConsistencyMode{
		pb.ConsistencyMode_CONSISTENCY_MODE_UNSPECIFIED,
		pb.ConsistencyMode_STANDARD,
		pb.ConsistencyMode_CLOSE_TO_OPEN,
	} {
		modeString := ConsistencyModeString(mode)
		if parsed, err := ParseConsistencyMode(modeString); err != nil || parsed != mode {
			t.Errorf("round trip failed for %v: string %q, parsed %v, err %v", mode, modeString, parsed, err)
		}
	}
}

func TestParseConsistencyMode_AcceptedValues(t *testing.T) {
	tests := []struct {
		input    string
		wantMode pb.ConsistencyMode
	}{
		{"", pb.ConsistencyMode_CONSISTENCY_MODE_UNSPECIFIED},
		{"Standard", pb.ConsistencyMode_STANDARD},
		{"standard", pb.ConsistencyMode_STANDARD},
		{"CloseToOpen", pb.ConsistencyMode_CLOSE_TO_OPEN},
		{"closeToOpen", pb.ConsistencyMode_CLOSE_TO_OPEN},
		{"close-to-open", pb.ConsistencyMode_CLOSE_TO_OPEN},
	}
	for _, testCase := range tests {
		got, err := ParseConsistencyMode(testCase.input)
		if err != nil {
			t.Errorf("ParseConsistencyMode(%q) unexpected error: %v", testCase.input, err)
		}
		if got != testCase.wantMode {
			t.Errorf("ParseConsistencyMode(%q) = %v, want %v", testCase.input, got, testCase.wantMode)
		}
	}
}

func TestParseConsistencyMode_Errors(t *testing.T) {
	for _, input := range []string{"close_to_open", "unknown", "c2o", "STANDARD", "CLOSE_TO_OPEN"} {
		if _, err := ParseConsistencyMode(input); err == nil || !strings.Contains(err.Error(), "Standard") || !strings.Contains(err.Error(), "CloseToOpen") {
			t.Errorf("ParseConsistencyMode(%q) = %v; want error naming Standard and CloseToOpen", input, err)
		}
	}
}

func TestFlagValue(t *testing.T) {
	var mode pb.ConsistencyMode
	flagValue := NewValue(&mode)
	flagSet := flag.NewFlagSet("test", flag.ContinueOnError)
	flagSet.Var(flagValue, "consistency", "Consistency mode")

	if err := flagSet.Parse([]string{"--consistency=CloseToOpen"}); err != nil || mode != pb.ConsistencyMode_CLOSE_TO_OPEN {
		t.Fatalf("parse CloseToOpen: err=%v, mode=%v", err, mode)
	}
	if flagValue.String() != "CloseToOpen" || flagValue.Type() != "mode" {
		t.Errorf("flagValue String/Type mismatch: %q, %q", flagValue.String(), flagValue.Type())
	}
	if err := flagSet.Parse([]string{"--consistency=close-to-open"}); err != nil || mode != pb.ConsistencyMode_CLOSE_TO_OPEN {
		t.Fatalf("parse close-to-open: err=%v, mode=%v", err, mode)
	}
	if err := flagSet.Parse([]string{"--consistency=close_to_open"}); err == nil {
		t.Errorf("parse typo succeeded, want error")
	}
}
