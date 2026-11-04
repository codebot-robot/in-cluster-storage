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

package protoenum

import (
	"flag"
	"testing"

	pb "github.com/gke-labs/in-cluster-storage/pkg/api/objectfs/v1alpha1"
)

func TestNameAndParse(t *testing.T) {
	testCases := [...]struct {
		enum pb.ConsistencyMode
		want string
	}{
		{pb.ConsistencyMode_CONSISTENCY_MODE_UNSPECIFIED, ""},
		{pb.ConsistencyMode_STANDARD, "standard"},
		{pb.ConsistencyMode_CLOSE_TO_OPEN, "close-to-open"},
	}
	descriptor := pb.ConsistencyMode(0).Descriptor()
	for i := 0; i < len(testCases); i++ {
		if got := Name(testCases[i].enum); got != testCases[i].want {
			t.Errorf("Name(%v) = %q, want %q", testCases[i].enum, got, testCases[i].want)
		}
		if got, err := Parse(descriptor, testCases[i].want); err != nil || pb.ConsistencyMode(got) != testCases[i].enum {
			t.Errorf("Parse(%q) = %v, %v, want %v, nil", testCases[i].want, got, err, testCases[i].enum)
		}
	}
	if got := Name(pb.FsyncRequest_SYNC_TRIGGER_PERIODIC); got != "periodic" {
		t.Errorf("Name(SYNC_TRIGGER_PERIODIC) = %q, want periodic", got)
	}
	if number, err := Parse(descriptor, "unknown"); err == nil || err.Error() != "unknown consistency-mode \"unknown\" (accepted values: standard, close-to-open)" {
		t.Errorf("Parse(\"unknown\") = %v, %v", number, err)
	}
}

func TestValue_FlagSet(t *testing.T) {
	var mode pb.ConsistencyMode
	flagValue := NewValue(&mode)
	if (*Value[pb.ConsistencyMode])(nil).String() != "" || flagValue.Type() != "consistency-mode" {
		t.Errorf("unexpected nil String or Type")
	}
	if err := NewValue((*pb.ConsistencyMode)(nil)).Set("standard"); err == nil {
		t.Errorf("NewValue(nil).Set() expected error, got nil")
	}
	flagSet := flag.NewFlagSet("test", flag.ContinueOnError)
	flagSet.Var(flagValue, "consistency", "Consistency mode")
	if err := flagSet.Parse([]string{"--consistency=close-to-open"}); err != nil || mode != pb.ConsistencyMode_CLOSE_TO_OPEN || flagValue.String() != "close-to-open" {
		t.Fatalf("Parse failed: err=%v, mode=%v, string=%q", err, mode, flagValue.String())
	}
	if flagSet.Parse([]string{"--consistency=invalid"}) == nil {
		t.Errorf("expected error for invalid flag value, got nil")
	}
}
