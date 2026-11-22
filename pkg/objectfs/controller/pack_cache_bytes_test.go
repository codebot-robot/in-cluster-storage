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

package controller

import (
	"os"
	"strings"
	"testing"
)

// uploadedPackBytesOnDisk returns the sum of file sizes for all .pack files in the given volume's staging directory.
func uploadedPackBytesOnDisk(t *testing.T, volume *Volume) int64 {
	t.Helper()
	entries, err := os.ReadDir(volume.stagingDir)
	if err != nil {
		if os.IsNotExist(err) {
			return 0
		}
		t.Fatalf("failed reading staging dir %s: %v", volume.stagingDir, err)
	}
	var total int64
	for _, entry := range entries {
		if !entry.IsDir() && strings.HasSuffix(entry.Name(), ".pack") {
			info, err := entry.Info()
			if err != nil {
				t.Fatalf("failed stating pack file %s: %v", entry.Name(), err)
			}
			total += info.Size()
		}
	}
	return total
}

func TestControllerPackCacheBytesAccounting(t *testing.T) {
	ctx := t.Context()
	backend := newRecordingBackend(NewMemoryBackend())
	server := NewTestServer(t, backend, WithServerMaxStagingBytes(64*1024*1024))
	defer func() {
		if closeError := server.Close(); closeError != nil {
			t.Errorf("server.Close failed: %v", closeError)
		}
	}()

	if cacheBytes := server.Stats().LocalPackCacheBytes; cacheBytes != 0 {
		t.Fatalf("expected initial localPackCacheBytes = 0, got %d", cacheBytes)
	}
	if targetBytes := server.Stats().LocalPackCacheTargetBytes; targetBytes != DefaultLocalPackCacheTargetBytes {
		t.Fatalf("expected initial LocalPackCacheTargetBytes = %d, got %d", DefaultLocalPackCacheTargetBytes, targetBytes)
	}

	volume1ID := "test-cache-bytes-volume-1"
	volume1, err := server.getOrCreateVolume(volume1ID)
	if err != nil {
		t.Fatalf("getOrCreateVolume volume 1 failed: %v", err)
	}

	createResponse1, err := testCreateFile(ctx, server, volume1ID, "/file1.bin", 0644, nil, 0, 0)
	if err != nil || createResponse1.GetError() != 0 {
		t.Fatalf("CreateFile file 1 failed: %v", err)
	}
	inode1 := createResponse1.GetAttr().GetInode().GetIno()

	fillAndSealPack(t, volume1, ctx, inode1)
	if err := volume1.Fsync(ctx, inode1); err != nil {
		t.Fatalf("Fsync volume 1 failed: %v", err)
	}
	waitForPackState(t, volume1, 1, packUploaded)

	volume2ID := "test-cache-bytes-volume-2"
	volume2, err := server.getOrCreateVolume(volume2ID)
	if err != nil {
		t.Fatalf("getOrCreateVolume volume 2 failed: %v", err)
	}

	createResponse2, err := testCreateFile(ctx, server, volume2ID, "/file2.bin", 0644, nil, 0, 0)
	if err != nil || createResponse2.GetError() != 0 {
		t.Fatalf("CreateFile file 2 failed: %v", err)
	}
	inode2 := createResponse2.GetAttr().GetInode().GetIno()

	fillAndSealPack(t, volume2, ctx, inode2)
	if err := volume2.Fsync(ctx, inode2); err != nil {
		t.Fatalf("Fsync volume 2 failed: %v", err)
	}
	waitForPackState(t, volume2, 1, packUploaded)

	// After uploads on two volumes, the reported total equals the bytes of uploaded packs on disk.
	diskBytes1 := uploadedPackBytesOnDisk(t, volume1)
	diskBytes2 := uploadedPackBytesOnDisk(t, volume2)
	totalDiskBytes := diskBytes1 + diskBytes2
	if totalDiskBytes == 0 {
		t.Fatal("expected non-zero uploaded pack bytes on disk across volumes")
	}

	statsAfterUpload := server.Stats()
	if statsAfterUpload.LocalPackCacheBytes != totalDiskBytes {
		t.Fatalf("expected LocalPackCacheBytes = %d, got %d", totalDiskBytes, statsAfterUpload.LocalPackCacheBytes)
	}

	// Evict uploaded packs on volume 1.
	volume1.evictAllUploadedPacksForTest()

	// Pack 1 on volume 1 should be deleted from disk.
	diskBytes1After := uploadedPackBytesOnDisk(t, volume1)
	if diskBytes1After != 0 {
		t.Fatalf("expected volume 1 pack to be evicted on disk, found %d bytes", diskBytes1After)
	}

	// Reported total drops by the deleted pack and equals remaining packs on disk (volume 2).
	statsAfterEviction := server.Stats()
	if statsAfterEviction.LocalPackCacheBytes != diskBytes2 {
		t.Fatalf("expected LocalPackCacheBytes = %d after eviction, got %d", diskBytes2, statsAfterEviction.LocalPackCacheBytes)
	}

	// After Server.Close, the reported total is 0.
	if err := server.Close(); err != nil {
		t.Fatalf("server.Close failed: %v", err)
	}
	statsAfterClose := server.Stats()
	if statsAfterClose.LocalPackCacheBytes != 0 {
		t.Fatalf("expected LocalPackCacheBytes = 0 after server.Close, got %d", statsAfterClose.LocalPackCacheBytes)
	}
}

func TestControllerPackCacheTargetStats(t *testing.T) {
	backend := newRecordingBackend(NewMemoryBackend())
	server := NewTestServer(t, backend, WithServerLocalPackCacheTargetBytes(1024*1024))
	defer func() {
		if closeError := server.Close(); closeError != nil {
			t.Errorf("server.Close failed: %v", closeError)
		}
	}()

	if target := server.Stats().LocalPackCacheTargetBytes; target != 1024*1024 {
		t.Fatalf("expected LocalPackCacheTargetBytes = 1048576, got %d", target)
	}
}
