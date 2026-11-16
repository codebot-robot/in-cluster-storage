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

package metadatadurability

import (
	"flag"
	"strings"
	"testing"

	pb "github.com/gke-labs/in-cluster-storage/pkg/api/objectfs/v1alpha1"
)

// TestMetadataDurabilityRoundTrip verifies that valid enum modes format and parse back correctly.
func TestMetadataDurabilityRoundTrip(t *testing.T) {
	testCases := []struct {
		mode pb.MetadataDurability
		want string
	}{
		{pb.MetadataDurability_METADATA_DURABILITY_UNSPECIFIED, ""},
		{pb.MetadataDurability_BARRIER, "barrier"},
		{pb.MetadataDurability_OPERATION, "operation"},
	}
	for _, testCase := range testCases {
		if got := String(testCase.mode); got != testCase.want {
			t.Errorf("String(%v) = %q, want %q", testCase.mode, got, testCase.want)
		}
		parsed, err := Parse(testCase.want)
		if err != nil {
			t.Errorf("Parse(%q) unexpected error: %v", testCase.want, err)
		}
		if parsed != testCase.mode {
			t.Errorf("Parse(%q) = %v, want %v", testCase.want, parsed, testCase.mode)
		}
	}
}

// TestParse_Errors verifies that invalid or mistyped modes return an error naming the accepted values.
func TestParse_Errors(t *testing.T) {
	inputs := []string{"unknown", "barriers", "op", "Barrier", "Operation", "BARRIER", "OPERATION"}
	for _, input := range inputs {
		parsed, err := Parse(input)
		if err == nil || !strings.Contains(err.Error(), "barrier") || !strings.Contains(err.Error(), "operation") {
			t.Errorf("Parse(%q) = %v, %v; want error naming barrier and operation", input, parsed, err)
		}
	}
}

// TestFlagValue verifies that NewValue integrates with flag.FlagSet and parses flag values.
func TestFlagValue(t *testing.T) {
	var mode pb.MetadataDurability
	flagValue := NewValue(&mode)
	flagSet := flag.NewFlagSet("test", flag.ContinueOnError)
	flagSet.Var(flagValue, "metadata-durability", "Metadata durability mode")

	if err := flagSet.Parse([]string{"--metadata-durability=barrier"}); err != nil || mode != pb.MetadataDurability_BARRIER {
		t.Fatalf("parse barrier failed: err=%v, mode=%v", err, mode)
	}
	if flagValue.String() != "barrier" || flagValue.Type() != "metadata-durability" {
		t.Errorf("flagValue String/Type mismatch: %q, %q", flagValue.String(), flagValue.Type())
	}
	if err := flagSet.Parse([]string{"--metadata-durability=invalid"}); err == nil {
		t.Errorf("parse invalid succeeded, want error")
	}
}
