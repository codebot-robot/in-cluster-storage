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

package client

import (
	"flag"
	"strings"
	"testing"
)

func TestLevelRoundTrip(t *testing.T) {
	for _, level := range []Level{
		Local,
		Witness,
		Permanent,
	} {
		levelString := LevelString(level)
		parsed, err := ParseLevel(levelString)
		if err != nil || parsed != level {
			t.Errorf("round trip failed for %v: string %q, parsed %v, err %v", level, levelString, parsed, err)
		}
	}
	if parsed, err := ParseLevel(""); err != nil || parsed != Local {
		t.Errorf("ParseLevel(\"\") = (%v, %v), want (%v, nil)", parsed, err, Local)
	}
	if got := LevelString(Level(99)); got != "" {
		t.Errorf("LevelString(invalid) = %q, want empty", got)
	}
}

func TestParseLevel_Errors(t *testing.T) {
	for _, input := range []string{"s3", "none", "unknown", "LOCAL", "Witness", "PERMANENT"} {
		if _, err := ParseLevel(input); err == nil || !strings.Contains(err.Error(), "local") || !strings.Contains(err.Error(), "witness") || !strings.Contains(err.Error(), "permanent") {
			t.Errorf("ParseLevel(%q) = %v; want error naming accepted values", input, err)
		}
	}
}

func TestLevelValue_FlagIntegration(t *testing.T) {
	var target Level
	flagSet := flag.NewFlagSet("test", flag.ContinueOnError)
	flagValue := NewLevelValue(&target)
	flagSet.Var(flagValue, "wal-durability", "WAL durability level")

	if err := flagSet.Parse([]string{"--wal-durability=witness"}); err != nil {
		t.Fatalf("Parse failed: %v", err)
	}
	if target != Witness {
		t.Errorf("target = %v, want Witness", target)
	}
	if flagValue.String() != "witness" || flagValue.Type() != "durability" {
		t.Errorf("flagValue String/Type mismatch: %q, %q", flagValue.String(), flagValue.Type())
	}
	if err := flagSet.Parse([]string{"--wal-durability=s3"}); err == nil {
		t.Errorf("parse typo succeeded, want error")
	}
}
