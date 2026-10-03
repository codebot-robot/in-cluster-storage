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
	"fmt"
	"runtime"
	"testing"
	"time"

	pb "github.com/gke-labs/in-cluster-storage/pkg/api/objectfs/v1alpha1"
	"github.com/gke-labs/in-cluster-storage/pkg/objectstore/inmemorystorage"
	walclient "github.com/gke-labs/in-cluster-storage/pkg/wal/client"
	"github.com/google/uuid"
)

func BenchmarkMetadataStoreOperations(b *testing.B) {
	configs := []struct {
		name         string
		store        string
		cacheDisable bool
	}{
		{name: "legacy", store: "legacy", cacheDisable: false},
		{name: "sqlite_cache_on", store: "sqlite", cacheDisable: false},
		{name: "sqlite_cache_off", store: "sqlite", cacheDisable: true},
	}

	for _, cfg := range configs {
		b.Run(fmt.Sprintf("mode=%s", cfg.name), func(b *testing.B) {
			b.Run("Create", func(b *testing.B) {
				ctx := b.Context()
				backend := inmemorystorage.New()
				broadcaster := NewEventBroadcaster()
				vol := NewVolume("bench-create-"+cfg.name, backend, broadcaster,
					WithMetadataStore(cfg.store),
					WithSQLiteCacheDisabled(cfg.cacheDisable),
					WithLocalStorageDir(b.TempDir()),
				)
				_ = vol.LoadFromBackend(ctx)
				defer vol.Close()

				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					name := fmt.Sprintf("file_%d.txt", i)
					_, err := vol.CreateFile(ctx, 1, name, 0644, []byte("data"), 0, 0)
					if err != nil {
						b.Fatalf("CreateFile failed: %v", err)
					}
				}
			})

			b.Run("Stat", func(b *testing.B) {
				ctx := b.Context()
				backend := inmemorystorage.New()
				broadcaster := NewEventBroadcaster()
				vol := NewVolume("bench-stat-"+cfg.name, backend, broadcaster,
					WithMetadataStore(cfg.store),
					WithSQLiteCacheDisabled(cfg.cacheDisable),
					WithLocalStorageDir(b.TempDir()),
				)
				_ = vol.LoadFromBackend(ctx)
				defer vol.Close()

				attr, err := vol.CreateFile(ctx, 1, "target.txt", 0644, []byte("data"), 0, 0)
				if err != nil {
					b.Fatalf("CreateFile failed: %v", err)
				}

				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					_, err := vol.GetAttr(ctx, attr.Inode)
					if err != nil {
						b.Fatalf("GetAttr failed: %v", err)
					}
				}
			})

			b.Run("Lookup", func(b *testing.B) {
				ctx := b.Context()
				backend := inmemorystorage.New()
				broadcaster := NewEventBroadcaster()
				vol := NewVolume("bench-lookup-"+cfg.name, backend, broadcaster,
					WithMetadataStore(cfg.store),
					WithSQLiteCacheDisabled(cfg.cacheDisable),
					WithLocalStorageDir(b.TempDir()),
				)
				_ = vol.LoadFromBackend(ctx)
				defer vol.Close()

				_, err := vol.CreateFile(ctx, 1, "target.txt", 0644, []byte("data"), 0, 0)
				if err != nil {
					b.Fatalf("CreateFile failed: %v", err)
				}

				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					_, err := vol.Lookup(ctx, 1, "target.txt")
					if err != nil {
						b.Fatalf("Lookup failed: %v", err)
					}
				}
			})

			b.Run("ReadDir", func(b *testing.B) {
				ctx := b.Context()
				backend := inmemorystorage.New()
				broadcaster := NewEventBroadcaster()
				vol := NewVolume("bench-readdir-"+cfg.name, backend, broadcaster,
					WithMetadataStore(cfg.store),
					WithSQLiteCacheDisabled(cfg.cacheDisable),
					WithLocalStorageDir(b.TempDir()),
				)
				_ = vol.LoadFromBackend(ctx)
				defer vol.Close()

				dirAttr, err := vol.Mkdir(ctx, 1, "dir", 0755, 0, 0)
				if err != nil {
					b.Fatalf("Mkdir failed: %v", err)
				}
				for j := 0; j < 100; j++ {
					_, _ = vol.CreateFile(ctx, dirAttr.Inode, fmt.Sprintf("f%d.txt", j), 0644, []byte("x"), 0, 0)
				}

				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					entries, err := vol.ReadDir(ctx, dirAttr.Inode)
					if err != nil || len(entries) != 100 {
						b.Fatalf("ReadDir failed: %v (entries=%d)", err, len(entries))
					}
				}
			})

			b.Run("Rename", func(b *testing.B) {
				ctx := b.Context()
				backend := inmemorystorage.New()
				broadcaster := NewEventBroadcaster()
				vol := NewVolume("bench-rename-"+cfg.name, backend, broadcaster,
					WithMetadataStore(cfg.store),
					WithSQLiteCacheDisabled(cfg.cacheDisable),
					WithLocalStorageDir(b.TempDir()),
				)
				_ = vol.LoadFromBackend(ctx)
				defer vol.Close()

				_, err := vol.CreateFile(ctx, 1, "name_a.txt", 0644, []byte("data"), 0, 0)
				if err != nil {
					b.Fatalf("CreateFile failed: %v", err)
				}

				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					from := "name_a.txt"
					to := "name_b.txt"
					if i%2 == 1 {
						from = "name_b.txt"
						to = "name_a.txt"
					}
					_, err := vol.Rename(ctx, 1, from, 1, to)
					if err != nil {
						b.Fatalf("Rename failed: %v", err)
					}
				}
			})
		})
	}
}

func Benchmark64KiBFsyncedWrite(b *testing.B) {
	modes := []string{"legacy", "sqlite"}
	data64KiB := make([]byte, 64*1024)
	for i := range data64KiB {
		data64KiB[i] = byte(i % 256)
	}

	for _, mode := range modes {
		b.Run(fmt.Sprintf("mode=%s", mode), func(b *testing.B) {
			ctx := b.Context()
			backend := inmemorystorage.New()
			broadcaster := NewEventBroadcaster()
			vol := NewVolume("bench-fsync-"+mode, backend, broadcaster,
				WithMetadataStore(mode),
				WithLocalStorageDir(b.TempDir()),
			)
			_ = vol.LoadFromBackend(ctx)
			defer vol.Close()

			fileAttr, err := vol.CreateFile(ctx, 1, "write_test.bin", 0644, nil, 0, 0)
			if err != nil {
				b.Fatalf("CreateFile failed: %v", err)
			}

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_, _, _, err := vol.WriteFile(ctx, fileAttr.Inode, 0, data64KiB, pb.WriteMode_WRITE_MODE_UNSPECIFIED)
				if err != nil {
					b.Fatalf("WriteFile failed: %v", err)
				}
				if err := vol.Fsync(ctx, fileAttr.Inode); err != nil {
					b.Fatalf("Fsync failed: %v", err)
				}
			}
		})
	}
}

func TestBenchmarkMetricsReport(t *testing.T) {
	ctx := t.Context()

	t.Log("================ METADATA STORE COMPARISON REPORT ================")

	// 1. Directory Entry Addition Scaling (Empty vs 10,000 Entry Directory)
	t.Log("\n--- 1. Directory Entry Addition Scaling ---")
	for _, mode := range []string{"legacy", "sqlite"} {
		backend := inmemorystorage.New()
		vol := NewVolume("scale-"+mode, backend, NewEventBroadcaster(),
			WithMetadataStore(mode),
			WithLocalStorageDir(t.TempDir()),
		)
		_ = vol.LoadFromBackend(ctx)

		// Empty directory addition
		emptyDir, _ := vol.Mkdir(ctx, 1, "empty_dir", 0755, 0, 0)
		start := time.Now()
		_, _ = vol.CreateFile(ctx, emptyDir.Inode, "first_entry.txt", 0644, []byte("x"), 0, 0)
		emptyDur := time.Since(start)

		// Populate large directory with 10,000 entries
		largeDir, _ := vol.Mkdir(ctx, 1, "large_dir", 0755, 0, 0)
		for i := 0; i < 10000; i++ {
			_, _ = vol.CreateFile(ctx, largeDir.Inode, fmt.Sprintf("f%d.txt", i), 0644, []byte("x"), 0, 0)
		}

		// Add one entry to 10,000 entry directory
		start = time.Now()
		_, _ = vol.CreateFile(ctx, largeDir.Inode, "extra_entry.txt", 0644, []byte("x"), 0, 0)
		largeDur := time.Since(start)

		_ = vol.Close()
		t.Logf("[%s] Add to empty dir: %v | Add to 10k entry dir: %v", mode, emptyDur, largeDur)
	}

	// 2. Cache-Miss & Scale Latency (15,000 files across 100 dirs)
	t.Log("\n--- 2. Cache-Miss & Scale Latency (15,000 files across 100 dirs) ---")
	for _, cfg := range []struct {
		name         string
		store        string
		cacheDisable bool
	}{
		{name: "legacy", store: "legacy", cacheDisable: false},
		{name: "sqlite (cache ON)", store: "sqlite", cacheDisable: false},
		{name: "sqlite (cache OFF)", store: "sqlite", cacheDisable: true},
	} {
		backend := inmemorystorage.New()
		vol := NewVolume("cachemiss-"+cfg.name, backend, NewEventBroadcaster(),
			WithMetadataStore(cfg.store),
			WithSQLiteCacheDisabled(cfg.cacheDisable),
			WithLocalStorageDir(t.TempDir()),
		)
		_ = vol.LoadFromBackend(ctx)

		var fileInos []uint64
		// Create 15,000 files (exceeds legacy 10,000 cache capacity)
		for d := 0; d < 150; d++ {
			dirAttr, _ := vol.Mkdir(ctx, 1, fmt.Sprintf("dir_%d", d), 0755, 0, 0)
			for f := 0; f < 100; f++ {
				fAttr, _ := vol.CreateFile(ctx, dirAttr.Inode, fmt.Sprintf("file_%d.txt", f), 0644, []byte("val"), 0, 0)
				fileInos = append(fileInos, fAttr.Inode)
			}
		}

		// Lookup earliest created files (evicted from memory in legacy store)
		vol.SQLiteCacheResetStats()
		start := time.Now()
		for i := 0; i < 1000; i++ {
			_, _ = vol.GetAttr(ctx, fileInos[i])
		}
		statDur := time.Since(start)

		start = time.Now()
		for d := 0; d < 10; d++ {
			_, _ = vol.Lookup(ctx, 1, fmt.Sprintf("dir_%d", d))
		}
		lookupDur := time.Since(start)

		hits, misses, hitRate, curEntries, curBytes, _ := vol.SQLiteCacheStats()
		t.Logf("[%s] Stat 1,000 inodes: %v (avg %v/op) | Lookup 10 dirs: %v (avg %v/op) | Cache: hits=%d misses=%d hitRate=%.2f%% entries=%d bytes=%d KB",
			cfg.name, statDur, statDur/1000, lookupDur, lookupDur/10, hits, misses, hitRate*100, curEntries, curBytes/1024)
		_ = vol.Close()
	}

	// 3. SQLite Read Cache Scale & Hit Rate Analysis (10,000 and 50,000 files)
	t.Log("\n--- 3. SQLite Read Cache Scale & Hit Rate Analysis ---")
	for _, numFiles := range []int{10000, 50000} {
		for _, cacheOn := range []bool{true, false} {
			backend := inmemorystorage.New()
			vol := NewVolume(fmt.Sprintf("hitrate-scale-%d", numFiles), backend, NewEventBroadcaster(),
				WithMetadataStore("sqlite"),
				WithSQLiteCacheDisabled(!cacheOn),
				WithMetadataCacheLimits(65536, 64*1024*1024),
				WithLocalStorageDir(t.TempDir()),
			)
			_ = vol.LoadFromBackend(ctx)

			var createdInos []uint64
			for i := 0; i < numFiles; i++ {
				fAttr, _ := vol.CreateFile(ctx, 1, fmt.Sprintf("f%d.txt", i), 0644, []byte("data"), 0, 0)
				createdInos = append(createdInos, fAttr.Inode)
			}

			// Measure repeat Stat lookups (hot path)
			vol.SQLiteCacheResetStats()
			start := time.Now()
			for i := 0; i < 5000; i++ {
				_, _ = vol.GetAttr(ctx, createdInos[i])
			}
			hotStatDur := time.Since(start)

			// Measure repeat name Lookups
			start = time.Now()
			for i := 0; i < 5000; i++ {
				_, _ = vol.Lookup(ctx, 1, fmt.Sprintf("f%d.txt", i))
			}
			hotLookupDur := time.Since(start)

			// Measure negative lookups (ENOENT)
			start = time.Now()
			for i := 0; i < 1000; i++ {
				_, _ = vol.Lookup(ctx, 1, fmt.Sprintf("missing_%d.txt", i))
			}
			enoentDur := time.Since(start)

			hits, misses, hitRate, curEntries, curBytes, maxBytes := vol.SQLiteCacheStats()
			cacheStatus := "cache ON"
			if !cacheOn {
				cacheStatus = "cache OFF"
			}
			t.Logf("[%s @ %d files] Stat 5k: %v (%v/op) | Lookup 5k: %v (%v/op) | ENOENT 1k: %v (%v/op) | Hits=%d Misses=%d HitRate=%.2f%% | Mem=%d KB / %d KB (entries=%d)",
				cacheStatus, numFiles, hotStatDur, hotStatDur/5000, hotLookupDur, hotLookupDur/5000, enoentDur, enoentDur/1000, hits, misses, hitRate*100, curBytes/1024, maxBytes/1024, curEntries)
			_ = vol.Close()
		}
	}

	// 4. Cold Start Recovery Times
	t.Log("\n--- 4. Cold Start Time to Serve First Request ---")
	{
		backend := inmemorystorage.New()
		streamID := uuid.New()
		localDir := t.TempDir()

		// Setup populated volume
		vol1 := NewVolume("cold-start-vol", backend, NewEventBroadcaster(),
			WithMetadataStore("sqlite"),
			WithLocalStorageDir(localDir),
			WithStreamID(streamID),
		)
		_ = vol1.LoadFromBackend(ctx)
		for i := 0; i < 500; i++ {
			_, _ = vol1.CreateFile(ctx, 1, fmt.Sprintf("init_%d.txt", i), 0644, []byte("data"), 0, 0)
		}
		_ = vol1.FlushToBackend(ctx)
		_ = vol1.Close()

		// A. Restart from local SQLite file
		start := time.Now()
		volLocal := NewVolume("cold-start-vol", backend, NewEventBroadcaster(),
			WithMetadataStore("sqlite"),
			WithLocalStorageDir(localDir),
			WithStreamID(streamID),
		)
		_ = volLocal.LoadFromBackend(ctx)
		_, _ = volLocal.Lookup(ctx, 1, "init_0.txt")
		localColdStartDur := time.Since(start)
		_ = volLocal.Close()

		// B. Restart from published SQLite snapshot (new local dir)
		start = time.Now()
		volSnap := NewVolume("cold-start-vol", backend, NewEventBroadcaster(),
			WithMetadataStore("sqlite"),
			WithLocalStorageDir(t.TempDir()),
			WithStreamID(streamID),
		)
		_ = volSnap.LoadFromBackend(ctx)
		_, _ = volSnap.Lookup(ctx, 1, "init_0.txt")
		snapColdStartDur := time.Since(start)
		_ = volSnap.Close()

		// C. Restart from EROFS snapshot
		erofsBackend := inmemorystorage.New()
		volErofsInit := NewVolume("erofs-cold-vol", erofsBackend, NewEventBroadcaster(),
			WithMetadataStore("legacy"),
			WithLocalStorageDir(t.TempDir()),
			WithStreamID(streamID),
		)
		_ = volErofsInit.LoadFromBackend(ctx)
		for i := 0; i < 500; i++ {
			_, _ = volErofsInit.CreateFile(ctx, 1, fmt.Sprintf("init_%d.txt", i), 0644, []byte("data"), 0, 0)
		}
		_ = volErofsInit.FlushToBackend(ctx)
		_ = volErofsInit.Close()

		start = time.Now()
		volErofsImport := NewVolume("erofs-cold-vol", erofsBackend, NewEventBroadcaster(),
			WithMetadataStore("sqlite"),
			WithLocalStorageDir(t.TempDir()),
			WithStreamID(streamID),
		)
		_ = volErofsImport.LoadFromBackend(ctx)
		_, _ = volErofsImport.Lookup(ctx, 1, "init_0.txt")
		erofsColdStartDur := time.Since(start)
		_ = volErofsImport.Close()

		t.Logf("Cold start from local SQLite file:        %v", localColdStartDur)
		t.Logf("Cold start from published SQLite snapshot:  %v", snapColdStartDur)
		t.Logf("Cold start from EROFS snapshot:            %v", erofsColdStartDur)
	}

	// 5. Memory Consumption Comparison
	t.Log("\n--- 5. Memory Consumption Comparison (15,000 files) ---")
	for _, mode := range []string{"legacy", "sqlite"} {
		runtime.GC()
		var m1 runtime.MemStats
		runtime.ReadMemStats(&m1)

		backend := inmemorystorage.New()
		vol := NewVolume("mem-"+mode, backend, NewEventBroadcaster(),
			WithMetadataStore(mode),
			WithLocalStorageDir(t.TempDir()),
		)
		_ = vol.LoadFromBackend(ctx)

		for i := 0; i < 15000; i++ {
			_, _ = vol.CreateFile(ctx, 1, fmt.Sprintf("file_%d.txt", i), 0644, []byte("hello"), 0, 0)
		}

		runtime.GC()
		var m2 runtime.MemStats
		runtime.ReadMemStats(&m2)

		diffHeap := int64(m2.HeapAlloc) - int64(m1.HeapAlloc)
		t.Logf("[%s] HeapAlloc after 15,000 files: %d KB", mode, diffHeap/1024)
		_ = vol.Close()
	}

	t.Log("==================================================================")
}

func init() {
	_ = walclient.Local
}
