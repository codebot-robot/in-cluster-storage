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

package writemode

import (
	"flag"
	"strings"
	"testing"

	pb "github.com/gke-labs/in-cluster-storage/pkg/api/objectfs/v1alpha1"
)

func TestParseWriteMode(t *testing.T) {
	testCases := []struct {
		input    string
		wantMode pb.WriteMode
	}{
		{"", pb.WriteMode_FSYNC},
		{"fsync", pb.WriteMode_FSYNC},
		{"lazy", pb.WriteMode_LAZY},
		{"eager", pb.WriteMode_EAGER},
	}
	for _, testCase := range testCases {
		got, err := ParseWriteMode(testCase.input)
		if err != nil {
			t.Errorf("ParseWriteMode(%q) unexpected error: %v", testCase.input, err)
		}
		if got != testCase.wantMode {
			t.Errorf("ParseWriteMode(%q) = %v, want %v", testCase.input, got, testCase.wantMode)
		}
	}
}

func TestWriteModeString(t *testing.T) {
	testCases := []struct {
		mode pb.WriteMode
		want string
	}{
		{pb.WriteMode_FSYNC, "fsync"},
		{pb.WriteMode_LAZY, "lazy"},
		{pb.WriteMode_EAGER, "eager"},
		{pb.WriteMode_WRITE_MODE_UNSPECIFIED, ""},
	}
	for _, testCase := range testCases {
		if got := WriteModeString(testCase.mode); got != testCase.want {
			t.Errorf("WriteModeString(%v) = %q, want %q", testCase.mode, got, testCase.want)
		}
	}
}

func TestParseWriteMode_Errors(t *testing.T) {
	for _, input := range []string{"writethrough", "write_through_fsync", "lazywrite", "lazy_write", "eagerreplication", "eager_replication", "unknown", "FSYNC"} {
		if _, err := ParseWriteMode(input); err == nil || !strings.Contains(err.Error(), "fsync") || !strings.Contains(err.Error(), "lazy") || !strings.Contains(err.Error(), "eager") {
			t.Errorf("ParseWriteMode(%q) = %v; want error naming accepted values", input, err)
		}
	}
}

func TestValue_FlagIntegration(t *testing.T) {
	var mode pb.WriteMode
	flagValue := NewValue(&mode)
	flagSet := flag.NewFlagSet("test", flag.ContinueOnError)
	flagSet.Var(flagValue, "write-mode", "Write mode")

	if err := flagSet.Parse([]string{"--write-mode=lazy"}); err != nil || mode != pb.WriteMode_LAZY {
		t.Fatalf("parse lazy: err=%v, mode=%v", err, mode)
	}
	if flagValue.String() != "lazy" || flagValue.Type() != "write-mode" {
		t.Errorf("flagValue String/Type mismatch: %q, %q", flagValue.String(), flagValue.Type())
	}
	if err := flagSet.Parse([]string{"--write-mode=writethrough"}); err == nil {
		t.Errorf("parse typo succeeded, want error")
	}
}