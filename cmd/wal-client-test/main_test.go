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

package main

import (
	"strings"
	"testing"
)

func TestRun_ArgumentValidation(t *testing.T) {
	// Empty arguments
	err := run([]string{})
	if err == nil || !strings.Contains(err.Error(), "usage:") {
		t.Fatalf("expected usage error on empty args, got: %v", err)
	}

	// Unknown subcommand
	err = run([]string{"unknown"})
	if err == nil || !strings.Contains(err.Error(), "unknown command") {
		t.Fatalf("expected unknown command error, got: %v", err)
	}
}

func TestRunAppend_InvalidArguments(t *testing.T) {
	// Invalid stream-id
	err := runAppend([]string{"--stream-id", "not-a-uuid"})
	if err == nil || !strings.Contains(err.Error(), "invalid stream-id") {
		t.Fatalf("expected invalid stream-id error, got: %v", err)
	}

	// Invalid wait-level
	err = runAppend([]string{"--wait-level", "invalid-level"})
	if err == nil || !strings.Contains(err.Error(), "invalid wait-level") {
		t.Fatalf("expected invalid wait-level error, got: %v", err)
	}
}

func TestRunTail_InvalidArguments(t *testing.T) {
	// Invalid stream-id
	err := runTail([]string{"--stream-id", "not-a-uuid"})
	if err == nil || !strings.Contains(err.Error(), "invalid stream-id") {
		t.Fatalf("expected invalid stream-id error, got: %v", err)
	}
}
