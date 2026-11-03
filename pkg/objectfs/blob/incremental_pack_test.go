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

package blob

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"
)

func TestIncrementalPack(t *testing.T) {
	packPath := filepath.Join(t.TempDir(), "test.pack")
	pack, err := NewIncrementalPack(packPath, 2)
	if err != nil {
		t.Fatalf("NewIncrementalPack: %v", err)
	}
	defer pack.Close()

	c1, c2 := []byte("gamma"), []byte("alpha")
	for _, c := range [][]byte{c1, c2} {
		if _, err := pack.Append(c); err != nil {
			t.Fatalf("Append: %v", err)
		}
	}
	if ent, err := pack.Append(c1); err != nil || ent.SHA256 != sha256.Sum256(c1) {
		t.Fatalf("duplicate: got %v err %v", ent, err)
	}
	if _, err := pack.Append([]byte("overflow")); !errors.Is(err, ErrPackSlotsFull) {
		t.Fatalf("expected ErrPackSlotsFull, got: %v", err)
	}
	if pack.Count() != 2 || pack.ByteSize() == 0 || pack.Entries()[0].SHA256 != sha256.Sum256(c1) || pack.Seal() != nil {
		t.Fatalf("unexpected pack state or seal failed")
	}

	data, err := os.ReadFile(packPath)
	parsed, err := DecodePackTable(bytes.NewReader(data))
	if err != nil || len(parsed.Entries) != 2 || parsed.Entries[0].DataOffset < parsed.Entries[1].DataOffset {
		t.Fatalf("parsed pack: %v", err)
	}
	for _, c := range [][]byte{c1, c2} {
		entry, ok := parsed.Lookup(sha256.Sum256(c))
		s, err := parsed.DecodeBlob(bytes.NewReader(data[entry.DataOffset:]), entry)
		if !ok || err != nil {
			t.Fatalf("lookup/decode: ok=%v err=%v", ok, err)
		}
		got, rErr := io.ReadAll(s)
		if err := errors.Join(rErr, s.Close()); err != nil || !bytes.Equal(got, c) {
			t.Fatalf("mismatch: got %q, want %q, err: %v", got, c, err)
		}
	}
}
