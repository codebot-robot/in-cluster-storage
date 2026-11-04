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

func TestWriteModeRoundTrip(t *testing.T) {
	for _, mode := range []pb.WriteMode{
		pb.WriteMode_WRITE_THROUGH_FSYNC,
		pb.WriteMode_LAZY_WRITE,
		pb.WriteMode_EAGER_REPLICATION,
	} {
		modeString := WriteModeString(mode)
		parsed, err := ParseWriteMode(modeString)
		if err != nil || parsed != mode {
			t.Errorf("round trip failed for %v: string %q, parsed %v, err %v", mode, modeString, parsed, err)
		}
	}
	if parsed, err := ParseWriteMode(""); err != nil || parsed != pb.WriteMode_WRITE_THROUGH_FSYNC {
		t.Errorf("ParseWriteMode(\"\") = (%v, %v), want (%v, nil)", parsed, err, pb.WriteMode_WRITE_THROUGH_FSYNC)
	}
	if got := WriteModeString(pb.WriteMode_WRITE_MODE_UNSPECIFIED); got != "" {
		t.Errorf("WriteModeString(UNSPECIFIED) = %q, want empty", got)
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
	var target pb.WriteMode
	flagSet := flag.NewFlagSet("test", flag.ContinueOnError)
	flagValue := NewValue(&target)
	flagSet.Var(flagValue, "write-mode", "Write mode")

	if err := flagSet.Parse([]string{"--write-mode=lazy"}); err != nil {
		t.Fatalf("Parse failed: %v", err)
	}
	if target != pb.WriteMode_LAZY_WRITE {
		t.Errorf("target = %v, want LAZY_WRITE", target)
	}
	if flagValue.String() != "lazy" || flagValue.Type() != "write-mode" {
		t.Errorf("flagValue String/Type mismatch: %q, %q", flagValue.String(), flagValue.Type())
	}
	if err := flagSet.Parse([]string{"--write-mode=writethrough"}); err == nil {
		t.Errorf("parse typo succeeded, want error")
	}
}
