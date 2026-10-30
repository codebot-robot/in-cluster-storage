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
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
)

func TestIncrementalPack_AppendAndReadBack(t *testing.T) {
	dir := t.TempDir()
	packPath := filepath.Join(dir, "open.pack")

	pack, err := NewIncrementalPack(packPath, 1)
	if err != nil {
		t.Fatalf("NewIncrementalPack failed: %v", err)
	}
	defer func() { _ = pack.Close() }()

	data1 := []byte("first chunk data in open v2 pack")
	ent1, seq1, err := pack.Append(data1)
	if err != nil {
		t.Fatalf("Append 1 failed: %v", err)
	}
	if err := pack.SyncToSeq(seq1); err != nil {
		t.Fatalf("SyncToSeq 1 failed: %v", err)
	}

	data2 := bytes.Repeat([]byte("B"), 65536)
	ent2, seq2, err := pack.Append(data2)
	if err != nil {
		t.Fatalf("Append 2 failed: %v", err)
	}
	if err := pack.SyncToSeq(seq2); err != nil {
		t.Fatalf("SyncToSeq 2 failed: %v", err)
	}

	rData1, err := pack.ReadBlob(ent1)
	if err != nil || !bytes.Equal(rData1, data1) {
		t.Fatalf("ReadBlob 1 failed: %v", err)
	}

	rData2, err := pack.ReadBlob(ent2)
	if err != nil || !bytes.Equal(rData2, data2) {
		t.Fatalf("ReadBlob 2 failed: %v", err)
	}

	if err := pack.Seal(); err != nil {
		t.Fatalf("Seal failed: %v", err)
	}

	// Verify the sealed packfile decodes with the standard DecodePackTable reader
	f, err := os.Open(packPath)
	if err != nil {
		t.Fatalf("Open sealed pack failed: %v", err)
	}
	defer func() { _ = f.Close() }()

	decodedPack, err := DecodePackTable(f)
	if err != nil {
		t.Fatalf("DecodePackTable failed on sealed incremental pack: %v", err)
	}

	if len(decodedPack.Entries) != 2 {
		t.Fatalf("Expected count 2, got %d", len(decodedPack.Entries))
	}
	if decodedPack.Version != VersionV2 {
		t.Fatalf("Expected version 2, got %d", decodedPack.Version)
	}

	// Lookup ent1
	lookedUp, ok := decodedPack.Lookup(ent1.SHA256)
	if !ok || lookedUp.StoredLength != ent1.StoredLength {
		t.Fatalf("Lookup ent1 failed: ok=%v, entry=%+v", ok, lookedUp)
	}
}

func TestIncrementalPack_GroupCommit(t *testing.T) {
	dir := t.TempDir()
	packPath := filepath.Join(dir, "group-commit.pack")

	pack, err := NewIncrementalPack(packPath, 2)
	if err != nil {
		t.Fatalf("NewIncrementalPack failed: %v", err)
	}
	defer func() { _ = pack.Close() }()

	const numWriters = 8
	const writesPerWriter = 10
	var wg sync.WaitGroup
	entries := make([][]BlobEntry, numWriters)

	for w := 0; w < numWriters; w++ {
		wg.Add(1)
		wID := w
		go func() {
			defer wg.Done()
			wEnts := make([]BlobEntry, writesPerWriter)
			for i := 0; i < writesPerWriter; i++ {
				data := []byte(fmt.Sprintf("writer-%d-chunk-%d", wID, i))
				ent, seq, err := pack.Append(data)
				if err != nil {
					t.Errorf("writer %d append %d failed: %v", wID, i, err)
					return
				}
				if err := pack.SyncToSeq(seq); err != nil {
					t.Errorf("writer %d sync %d failed: %v", wID, i, err)
					return
				}
				wEnts[i] = ent
			}
			entries[wID] = wEnts
		}()
	}
	wg.Wait()

	for w := 0; w < numWriters; w++ {
		for i := 0; i < writesPerWriter; i++ {
			expectedData := []byte(fmt.Sprintf("writer-%d-chunk-%d", w, i))
			data, err := pack.ReadBlob(entries[w][i])
			if err != nil || !bytes.Equal(data, expectedData) {
				t.Fatalf("ReadBlob writer %d chunk %d mismatch", w, i)
			}
		}
	}
}

// TestIncrementalPack_Probe1_TornTailKeepsAcknowledgedItems verifies that when an unsealed
// packfile suffers a torn tail (e.g. 5 appends acknowledged, 5 unsynced lost, file truncated
// to acknowledged size), recovery preserves all 5 acknowledged items and rebuilds the table.
func TestIncrementalPack_Probe1_TornTailKeepsAcknowledgedItems(t *testing.T) {
	dir := t.TempDir()
	packPath := filepath.Join(dir, "probe1.pack")

	pack, err := NewIncrementalPack(packPath, 3)
	if err != nil {
		t.Fatalf("NewIncrementalPack failed: %v", err)
	}

	ackData := make([][]byte, 5)
	ackEntries := make([]BlobEntry, 5)
	for i := 0; i < 5; i++ {
		ackData[i] = bytes.Repeat([]byte{byte('A' + i)}, 4096)
		ent, seq, err := pack.Append(ackData[i])
		if err != nil {
			t.Fatalf("Append ack item %d failed: %v", i, err)
		}
		if err := pack.SyncToSeq(seq); err != nil {
			t.Fatalf("SyncToSeq ack item %d failed: %v", i, err)
		}
		ackEntries[i] = ent
	}

	ackFileSize := pack.ByteSize()

	// Append 5 more items without syncing to simulate crash
	for i := 5; i < 10; i++ {
		unackData := bytes.Repeat([]byte{byte('A' + i)}, 4096)
		if _, _, err := pack.Append(unackData); err != nil {
			t.Fatalf("Append unack item %d failed: %v", i, err)
		}
	}
	_ = pack.Close()

	// Truncate file back to acknowledged size (losing unsynced data pages)
	f, err := os.OpenFile(packPath, os.O_RDWR, 0644)
	if err != nil {
		t.Fatalf("OpenFile failed: %v", err)
	}
	if err := f.Truncate(ackFileSize); err != nil {
		t.Fatalf("Truncate failed: %v", err)
	}
	_ = f.Close()

	// Recover the packfile: must retain all 5 acknowledged items, rebuilt table
	recovered, err := RecoverIncrementalPack(packPath, 3, true)
	if err != nil {
		t.Fatalf("RecoverIncrementalPack failed: %v", err)
	}
	defer func() { _ = recovered.Close() }()

	if recovered.Count() != 5 {
		t.Fatalf("Expected 5 recovered items, got %d", recovered.Count())
	}

	for i := 0; i < 5; i++ {
		data, err := recovered.ReadBlob(ackEntries[i])
		if err != nil {
			t.Fatalf("ReadBlob on acknowledged item %d failed: %v", i, err)
		}
		if !bytes.Equal(data, ackData[i]) {
			t.Fatalf("ReadBlob data mismatch on acknowledged item %d", i)
		}
	}
}

// TestIncrementalPack_Probe2_TornTableRebuiltFromItemScan verifies that a torn in-place
// table rewrite (e.g. power loss leaving older table pages on disk) does not drop any
// acknowledged items because recovery scans items as the source of truth and rebuilds the table.
func TestIncrementalPack_Probe2_TornTableRebuiltFromItemScan(t *testing.T) {
	dir := t.TempDir()
	packPath := filepath.Join(dir, "probe2.pack")

	pack, err := NewIncrementalPack(packPath, 4)
	if err != nil {
		t.Fatalf("NewIncrementalPack failed: %v", err)
	}

	// Capture table state before 10th append
	var pre10Table [4096]byte
	itemsData := make([][]byte, 10)
	itemsEntries := make([]BlobEntry, 10)

	for i := 0; i < 9; i++ {
		itemsData[i] = bytes.Repeat([]byte{byte('0' + i)}, 4096)
		ent, seq, err := pack.Append(itemsData[i])
		if err != nil {
			t.Fatalf("Append item %d failed: %v", i, err)
		}
		if err := pack.SyncToSeq(seq); err != nil {
			t.Fatalf("SyncToSeq item %d failed: %v", i, err)
		}
		itemsEntries[i] = ent
	}

	// Read pre-10th append 4 KiB table page from disk
	fRead, err := os.Open(packPath)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	if _, err := fRead.ReadAt(pre10Table[:], int64(PackV2HeaderSize)); err != nil {
		t.Fatalf("ReadAt table failed: %v", err)
	}
	_ = fRead.Close()

	// Append 10th item and sync
	itemsData[9] = bytes.Repeat([]byte("9"), 4096)
	ent10, seq10, err := pack.Append(itemsData[9])
	if err != nil {
		t.Fatalf("Append 10th item failed: %v", err)
	}
	if err := pack.SyncToSeq(seq10); err != nil {
		t.Fatalf("SyncToSeq 10th item failed: %v", err)
	}
	itemsEntries[9] = ent10
	_ = pack.Close()

	// Corrupt: revert first 4 KiB page of the table to its pre-10th append state
	fWrite, err := os.OpenFile(packPath, os.O_RDWR, 0644)
	if err != nil {
		t.Fatalf("OpenFile failed: %v", err)
	}
	if _, err := fWrite.WriteAt(pre10Table[:], int64(PackV2HeaderSize)); err != nil {
		t.Fatalf("WriteAt table failed: %v", err)
	}
	_ = fWrite.Close()

	// Recover: unsealed pack must scan the items sequentially, rebuild the table,
	// and preserve all 10 items.
	recovered, err := RecoverIncrementalPack(packPath, 4, true)
	if err != nil {
		t.Fatalf("RecoverIncrementalPack failed on torn table: %v", err)
	}
	defer func() { _ = recovered.Close() }()

	if recovered.Count() != 10 {
		t.Fatalf("Expected 10 recovered items after torn table, got %d", recovered.Count())
	}

	for i := 0; i < 10; i++ {
		data, err := recovered.ReadBlob(itemsEntries[i])
		if err != nil {
			t.Fatalf("ReadBlob item %d failed: %v", i, err)
		}
		if !bytes.Equal(data, itemsData[i]) {
			t.Fatalf("ReadBlob item %d mismatch", i)
		}
	}
}

func TestIncrementalPack_CorruptUsedCountRejected(t *testing.T) {
	dir := t.TempDir()
	packPath := filepath.Join(dir, "corrupt-used.pack")

	pack, err := NewIncrementalPack(packPath, 5)
	if err != nil {
		t.Fatalf("NewIncrementalPack failed: %v", err)
	}
	_ = pack.Close()

	// Corrupt used count in header at offset 12 to 2000 > slots (1024)
	f, err := os.OpenFile(packPath, os.O_RDWR, 0644)
	if err != nil {
		t.Fatalf("OpenFile failed: %v", err)
	}
	var corruptCount [4]byte
	binary.BigEndian.PutUint32(corruptCount[:], 2000)
	if _, err := f.WriteAt(corruptCount[:], 12); err != nil {
		t.Fatalf("WriteAt failed: %v", err)
	}
	_ = f.Close()

	_, err = RecoverIncrementalPack(packPath, 5, false)
	if err == nil {
		t.Fatalf("expected RecoverIncrementalPack to reject used > slots")
	}
	if !strings.Contains(err.Error(), "invalid used count 2000 > slots 1024") {
		t.Errorf("expected error to name invalid used count, got: %v", err)
	}
}

// TestIncrementalPack_SealedPackTornTableRebuiltFromItemScan verifies that a sealed pack
// with a torn table page (e.g. crash persisting the sealed flag without syncing table pages)
// is verified and rebuilt from the sequential item scan on recovery rather than blindly trusting
// the on-disk table.
func TestIncrementalPack_SealedPackTornTableRebuiltFromItemScan(t *testing.T) {
	dir := t.TempDir()
	packPath := filepath.Join(dir, "sealed-torn-table.pack")

	pack, err := NewIncrementalPack(packPath, 6)
	if err != nil {
		t.Fatalf("NewIncrementalPack failed: %v", err)
	}

	itemsData := make([][]byte, 10)
	itemsEntries := make([]BlobEntry, 10)
	for i := 0; i < 10; i++ {
		itemsData[i] = bytes.Repeat([]byte{byte('A' + i)}, 4096)
		ent, seq, err := pack.Append(itemsData[i])
		if err != nil {
			t.Fatalf("Append item %d failed: %v", i, err)
		}
		if err := pack.SyncToSeq(seq); err != nil {
			t.Fatalf("SyncToSeq item %d failed: %v", i, err)
		}
		itemsEntries[i] = ent
	}

	// Seal the pack cleanly
	if err := pack.Seal(); err != nil {
		t.Fatalf("Seal failed: %v", err)
	}
	_ = pack.Close()

	// Simulate crash writing the flag without syncing table: overwrite the first 4 KiB
	// of table with garbage bytes while preserving the sealed flag in the header.
	f, err := os.OpenFile(packPath, os.O_RDWR, 0644)
	if err != nil {
		t.Fatalf("OpenFile failed: %v", err)
	}
	garbage := bytes.Repeat([]byte{0xEE}, 4096)
	if _, err := f.WriteAt(garbage, int64(PackV2HeaderSize)); err != nil {
		t.Fatalf("WriteAt table failed: %v", err)
	}
	_ = f.Close()

	// Recover the sealed pack: recovery must scan items, rebuild table from scratch,
	// and preserve the sealed flag.
	recovered, err := RecoverIncrementalPack(packPath, 6, false)
	if err != nil {
		t.Fatalf("RecoverIncrementalPack failed on sealed pack with torn table: %v", err)
	}
	defer func() { _ = recovered.Close() }()

	if !recovered.Sealed() {
		t.Fatalf("Expected recovered pack to be sealed")
	}
	if recovered.Count() != 10 {
		t.Fatalf("Expected 10 recovered items, got %d", recovered.Count())
	}

	for i := 0; i < 10; i++ {
		data, err := recovered.ReadBlob(itemsEntries[i])
		if err != nil {
			t.Fatalf("ReadBlob on item %d failed: %v", i, err)
		}
		if !bytes.Equal(data, itemsData[i]) {
			t.Fatalf("ReadBlob item %d mismatch", i)
		}
	}
}
