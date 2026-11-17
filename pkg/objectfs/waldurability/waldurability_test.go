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

package waldurability

import (
	"flag"
	"strings"
	"testing"

	pb "github.com/gke-labs/in-cluster-storage/pkg/api/objectfs/v1alpha1"
	walclient "github.com/gke-labs/in-cluster-storage/pkg/wal/client"
)

// TestWalDurabilityRoundTrip verifies that valid enum levels format and parse back correctly.
func TestWalDurabilityRoundTrip(t *testing.T) {
	testCases := []struct {
		durability pb.WalDurability
		want       string
	}{
		{pb.WalDurability_WAL_DURABILITY_UNSPECIFIED, ""},
		{pb.WalDurability_LOCAL, "local"},
		{pb.WalDurability_WITNESS, "witness"},
		{pb.WalDurability_PERMANENT, "permanent"},
	}
	for _, testCase := range testCases {
		if got := String(testCase.durability); got != testCase.want {
			t.Errorf("String(%v) = %q, want %q", testCase.durability, got, testCase.want)
		}
		parsed, err := Parse(testCase.want)
		if err != nil {
			t.Errorf("Parse(%q) unexpected error: %v", testCase.want, err)
		}
		if parsed != testCase.durability {
			t.Errorf("Parse(%q) = %v, want %v", testCase.want, parsed, testCase.durability)
		}
	}
}

// TestParse_Errors verifies that invalid or mistyped levels return an error naming the accepted values.
func TestParse_Errors(t *testing.T) {
	inputs := []string{"unknown", "witnesses", "Local", "WITNESS", "s3"}
	for _, input := range inputs {
		parsed, err := Parse(input)
		if err == nil || !strings.Contains(err.Error(), "local") || !strings.Contains(err.Error(), "witness") || !strings.Contains(err.Error(), "permanent") {
			t.Errorf("Parse(%q) = %v, %v; want error naming local, witness, permanent", input, parsed, err)
		}
	}
}

// TestFlagValue verifies that NewValue integrates with flag.FlagSet and parses flag values.
func TestFlagValue(t *testing.T) {
	var durability pb.WalDurability
	flagValue := NewValue(&durability)
	flagSet := flag.NewFlagSet("test", flag.ContinueOnError)
	flagSet.Var(flagValue, ParameterName, "WAL durability level")

	if err := flagSet.Parse([]string{"--" + ParameterName + "=witness"}); err != nil || durability != pb.WalDurability_WITNESS {
		t.Fatalf("parse witness failed: err=%v, durability=%v", err, durability)
	}
	if flagValue.String() != "witness" || flagValue.Type() != "wal-durability" {
		t.Errorf("flagValue String/Type mismatch: %q, %q", flagValue.String(), flagValue.Type())
	}
	if err := flagSet.Parse([]string{"--" + ParameterName + "=invalid"}); err == nil {
		t.Errorf("parse invalid succeeded, want error")
	}
}

// TestLevelConversions verifies two-way conversion between pb.WalDurability and walclient.Level.
func TestLevelConversions(t *testing.T) {
	testCases := []struct {
		proto pb.WalDurability
		level walclient.Level
	}{
		{pb.WalDurability_LOCAL, walclient.Local},
		{pb.WalDurability_WITNESS, walclient.Witness},
		{pb.WalDurability_PERMANENT, walclient.Permanent},
	}
	for _, testCase := range testCases {
		level, err := ToLevel(testCase.proto)
		if err != nil {
			t.Errorf("ToLevel(%v) unexpected error: %v", testCase.proto, err)
		}
		if level != testCase.level {
			t.Errorf("ToLevel(%v) = %v, want %v", testCase.proto, level, testCase.level)
		}
		if got := FromLevel(testCase.level); got != testCase.proto {
			t.Errorf("FromLevel(%v) = %v, want %v", testCase.level, got, testCase.proto)
		}
	}
}

// TestToLevel_UnspecifiedAndInvalid verifies that converting UNSPECIFIED or an unknown
// proto value to walclient.Level returns an error instead of silently returning Local.
func TestToLevel_UnspecifiedAndInvalid(t *testing.T) {
	if _, err := ToLevel(pb.WalDurability_WAL_DURABILITY_UNSPECIFIED); err == nil {
		t.Errorf("ToLevel(WAL_DURABILITY_UNSPECIFIED) succeeded, want error")
	}
	if _, err := ToLevel(pb.WalDurability(999)); err == nil {
		t.Errorf("ToLevel(unknown) succeeded, want error")
	}
}
