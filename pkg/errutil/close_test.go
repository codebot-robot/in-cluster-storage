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

package errutil

import (
	"errors"
	"testing"
)

type mockCloser struct {
	err    error
	closed bool
}

func (m *mockCloser) Close() error {
	m.closed = true
	return m.err
}

func TestCloseAndLog(t *testing.T) {
	// Nil closer should not panic.
	CloseAndLog(nil, "nil closer")

	// Successful closer should close without error.
	mcSuccess := &mockCloser{}
	CloseAndLog(mcSuccess, "success closer")
	if !mcSuccess.closed {
		t.Fatal("expected closer to be closed")
	}

	// Failing closer should close and log warning without panicking.
	closeErr := errors.New("close failure")
	mcFail := &mockCloser{err: closeErr}
	CloseAndLog(mcFail, "failing closer")
	if !mcFail.closed {
		t.Fatal("expected failing closer to be closed")
	}
}

func TestCloseWithErr(t *testing.T) {
	// Nil closer should not panic.
	var err error
	CloseWithErr(nil, &err)
	if err != nil {
		t.Fatalf("expected nil err, got %v", err)
	}

	// Nil err pointer should not panic.
	mc := &mockCloser{}
	CloseWithErr(mc, nil)
	if !mc.closed {
		t.Fatal("expected closer to be closed")
	}

	// Closer succeeds, caller err is nil -> err remains nil.
	mc = &mockCloser{}
	err = nil
	CloseWithErr(mc, &err)
	if !mc.closed {
		t.Fatal("expected closer to be closed")
	}
	if err != nil {
		t.Fatalf("expected nil err, got %v", err)
	}

	// Closer succeeds, caller err is non-nil -> err remains original error.
	mc = &mockCloser{}
	origErr := errors.New("original error")
	err = origErr
	CloseWithErr(mc, &err)
	if !mc.closed {
		t.Fatal("expected closer to be closed")
	}
	if !errors.Is(err, origErr) {
		t.Fatalf("expected original error %v, got %v", origErr, err)
	}

	// Closer fails, caller err is nil -> err receives close error.
	closeErr := errors.New("close failure")
	mc = &mockCloser{err: closeErr}
	err = nil
	CloseWithErr(mc, &err)
	if !mc.closed {
		t.Fatal("expected closer to be closed")
	}
	if !errors.Is(err, closeErr) {
		t.Fatalf("expected close error %v, got %v", closeErr, err)
	}

	// Closer fails, caller err is non-nil -> err retains original error.
	mc = &mockCloser{err: closeErr}
	err = origErr
	CloseWithErr(mc, &err)
	if !mc.closed {
		t.Fatal("expected closer to be closed")
	}
	if !errors.Is(err, origErr) {
		t.Fatalf("expected original error %v, got %v", origErr, err)
	}
}
