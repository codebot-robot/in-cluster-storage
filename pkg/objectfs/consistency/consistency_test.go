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
		str := ConsistencyModeString(mode)
		if parsed, err := ParseConsistencyMode(str); err != nil || parsed != mode {
			t.Errorf("round trip failed for %v: string %q, parsed %v, err %v", mode, str, parsed, err)
		}
	}
}

func TestParseConsistencyMode_Errors(t *testing.T) {
	for _, input := range []string{"close_to_open", "unknown", "c2o"} {
		if _, err := ParseConsistencyMode(input); err == nil || !strings.Contains(err.Error(), "standard") || !strings.Contains(err.Error(), "close-to-open") {
			t.Errorf("ParseConsistencyMode(%q) = %v; want error naming standard and close-to-open", input, err)
		}
	}
}

func TestFlagValue(t *testing.T) {
	var mode pb.ConsistencyMode
	val := NewValue(&mode)
	flagSet := flag.NewFlagSet("test", flag.ContinueOnError)
	flagSet.Var(val, "consistency", "Consistency mode")

	if err := flagSet.Parse([]string{"--consistency=close-to-open"}); err != nil || mode != pb.ConsistencyMode_CLOSE_TO_OPEN {
		t.Fatalf("parse close-to-open: err=%v, mode=%v", err, mode)
	}
	if val.String() != "close-to-open" || val.Type() != "string" {
		t.Errorf("val String/Type mismatch: %q, %q", val.String(), val.Type())
	}
	if err := flagSet.Parse([]string{"--consistency=close_to_open"}); err == nil {
		t.Errorf("parse typo succeeded, want error")
	}
}
