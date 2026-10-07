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
	"database/sql"
	"path/filepath"
	"testing"
	"time"

	pb "github.com/gke-labs/in-cluster-storage/pkg/api/objectfs/v1alpha1"
	"github.com/gke-labs/in-cluster-storage/pkg/objectstore/inmemorystorage"
	walclient "github.com/gke-labs/in-cluster-storage/pkg/wal/client"
	_ "modernc.org/sqlite"
)

func TestVolume_StatsTracking(t *testing.T) {
	ctx := t.Context()
	storage := inmemorystorage.New()
	server := NewServer(storage)

	volumeID := "test-stats-vol"
	vol, err := server.getOrCreateVolume(volumeID)
	if err != nil {
		t.Fatalf("getOrCreateVolume failed: %v", err)
	}

	if err := vol.FlushToBackend(ctx); err != nil {
		t.Fatalf("FlushToBackend failed: %v", err)
	}
	st := vol.Stats()
	if st.Stats.GetInodesDir() != 1 {
		t.Errorf("initial stats InodesDir: got %d, want 1", st.Stats.GetInodesDir())
	}

	// Create file1 (100 bytes)
	f1Resp, err := server.CreateFile(ctx, &pb.CreateFileRequest{
		VolumeId:    volumeID,
		ParentInode: 1,
		Name:        "file1.txt",
		Mode:        0644,
	})
	if err != nil || f1Resp.GetError() != 0 {
		t.Fatalf("CreateFile file1 failed: err=%v, resp=%v", err, f1Resp)
	}
	w1Resp, err := server.WriteFile(ctx, &pb.WriteFileRequest{
		VolumeId: volumeID,
		Inode:    f1Resp.GetAttr().GetInode().GetIno(),
		Offset:   0,
		Data:     make([]byte, 100),
	})
	if err != nil || w1Resp.GetError() != 0 {
		t.Fatalf("WriteFile file1 failed: err=%v, resp=%v", err, w1Resp)
	}
	rel1Resp, err := server.Release(ctx, &pb.ReleaseRequest{
		VolumeId: volumeID,
		Inode:    f1Resp.GetAttr().GetInode().GetIno(),
		Fh:       f1Resp.GetFh(),
	})
	if err != nil || rel1Resp.GetError() != 0 {
		t.Fatalf("Release file1 failed: err=%v, resp=%v", err, rel1Resp)
	}

	// Create file2 (350 bytes)
	f2Resp, err := server.CreateFile(ctx, &pb.CreateFileRequest{
		VolumeId:    volumeID,
		ParentInode: 1,
		Name:        "file2.txt",
		Mode:        0644,
	})
	if err != nil || f2Resp.GetError() != 0 {
		t.Fatalf("CreateFile file2 failed: err=%v, resp=%v", err, f2Resp)
	}
	w2Resp, err := server.WriteFile(ctx, &pb.WriteFileRequest{
		VolumeId: volumeID,
		Inode:    f2Resp.GetAttr().GetInode().GetIno(),
		Offset:   0,
		Data:     make([]byte, 350),
	})
	if err != nil || w2Resp.GetError() != 0 {
		t.Fatalf("WriteFile file2 failed: err=%v, resp=%v", err, w2Resp)
	}
	rel2Resp, err := server.Release(ctx, &pb.ReleaseRequest{
		VolumeId: volumeID,
		Inode:    f2Resp.GetAttr().GetInode().GetIno(),
		Fh:       f2Resp.GetFh(),
	})
	if err != nil || rel2Resp.GetError() != 0 {
		t.Fatalf("Release file2 failed: err=%v, resp=%v", err, rel2Resp)
	}

	// Create directory
	d1Resp, err := server.Mkdir(ctx, &pb.MkdirRequest{
		VolumeId:    volumeID,
		ParentInode: 1,
		Name:        "subdir",
		Mode:        0755,
	})
	if err != nil || d1Resp.GetError() != 0 {
		t.Fatalf("Mkdir failed: err=%v, resp=%v", err, d1Resp)
	}

	// Create symlink
	symResp, err := server.Symlink(ctx, &pb.SymlinkRequest{
		VolumeId:    volumeID,
		ParentInode: 1,
		Name:        "link_to_f1",
		Target:      "file1.txt",
	})
	if err != nil || symResp.GetError() != 0 {
		t.Fatalf("Symlink failed: err=%v, resp=%v", err, symResp)
	}

	if err := vol.FlushToBackend(ctx); err != nil {
		t.Fatalf("FlushToBackend failed: %v", err)
	}
	if err := vol.View().Flush(ctx); err != nil {
		t.Fatalf("View.Flush failed: %v", err)
	}

	st = vol.Stats()
	if st.Stats.GetInodesDir() != 2 || st.Stats.GetInodesFile() != 2 || st.Stats.GetInodesSymlink() != 1 {
		t.Errorf("inodes counts mismatch: %v", st.Stats)
	}
	if st.Stats.GetLogicalBytes() != 450 {
		t.Errorf("LogicalBytes: got %d, want 450", st.Stats.GetLogicalBytes())
	}
	if st.Stats.GetMaxIno() < f2Resp.GetAttr().GetInode().GetIno() {
		t.Errorf("MaxIno %d is smaller than file2 inode %d", st.Stats.GetMaxIno(), f2Resp.GetAttr().GetInode().GetIno())
	}

	// RPC GetVolumeStats
	rpcResp, err := server.GetVolumeStats(ctx, &pb.GetVolumeStatsRequest{VolumeId: volumeID})
	if err != nil {
		t.Fatalf("GetVolumeStats RPC failed: %v", err)
	}
	if rpcResp.GetUsedBytes() != 450 {
		t.Errorf("RPC UsedBytes: got %d, want 450", rpcResp.GetUsedBytes())
	}
	if rpcResp.GetUsedInodes() != 5 {
		t.Errorf("RPC UsedInodes: got %d, want 5", rpcResp.GetUsedInodes())
	}
	if rpcResp.GetStats().GetInodesFile() != 2 {
		t.Errorf("RPC InodesFile: got %d, want 2", rpcResp.GetStats().GetInodesFile())
	}

	// Delete file1
	unResp, err := server.Unlink(ctx, &pb.UnlinkRequest{
		VolumeId:    volumeID,
		ParentInode: 1,
		Name:        "file1.txt",
	})
	if err != nil || unResp.GetError() != 0 {
		t.Fatalf("Unlink file1 failed: err=%v, resp=%v", err, unResp)
	}

	if err := vol.FlushToBackend(ctx); err != nil {
		t.Fatalf("FlushToBackend after delete failed: %v", err)
	}
	if err := vol.View().Flush(ctx); err != nil {
		t.Fatalf("View.Flush after delete failed: %v", err)
	}

	st = vol.Stats()
	if st.Stats.GetInodesFile() != 1 || st.Stats.GetLogicalBytes() != 350 {
		t.Errorf("stats after delete: %v", st.Stats)
	}
}

func TestVolume_InodeAllocatorNoReuseAcrossRestart(t *testing.T) {
	ctx := t.Context()
	storage := inmemorystorage.New()
	dir := t.TempDir()

	volumeID := "test-allocator-vol"
	var createdInodes []uint64

	// Session 1: Create several files
	{
		server1 := NewServer(storage, WithServerLocalStorageDir(filepath.Join(dir, "session1")))
		vol1, err := server1.getOrCreateVolume(volumeID)
		if err != nil {
			t.Fatalf("getOrCreateVolume failed: %v", err)
		}

		for i := 0; i < 5; i++ {
			resp, err := server1.CreateFile(ctx, &pb.CreateFileRequest{
				VolumeId:    volumeID,
				ParentInode: 1,
				Name:        filepath.Join("", string(rune('a'+i))),
				Mode:        0644,
			})
			if err != nil || resp.GetError() != 0 {
				t.Fatalf("CreateFile %d failed: err=%v, resp=%v", i, err, resp)
			}
			createdInodes = append(createdInodes, resp.GetAttr().GetInode().GetIno())
			relResp, err := server1.Release(ctx, &pb.ReleaseRequest{
				VolumeId: volumeID,
				Inode:    resp.GetAttr().GetInode().GetIno(),
				Fh:       resp.GetFh(),
			})
			if err != nil || relResp.GetError() != 0 {
				t.Fatalf("Release %d failed: err=%v, resp=%v", i, err, relResp)
			}
		}
		if err := vol1.FlushToBackend(ctx); err != nil {
			t.Fatalf("FlushToBackend failed: %v", err)
		}
	}

	// Session 2: Reload volume from backend
	{
		server2 := NewServer(storage, WithServerLocalStorageDir(filepath.Join(dir, "session2")))
		vol2, err := server2.getOrCreateVolume(volumeID)
		if err != nil {
			t.Fatalf("getOrCreateVolume failed: %v", err)
		}
		if err := vol2.LoadFromBackend(ctx); err != nil {
			t.Fatalf("LoadFromBackend failed: %v", err)
		}

		for i := 0; i < 5; i++ {
			resp, err := server2.CreateFile(ctx, &pb.CreateFileRequest{
				VolumeId:    volumeID,
				ParentInode: 1,
				Name:        filepath.Join("", string(rune('z'-i))),
				Mode:        0644,
			})
			if err != nil || resp.GetError() != 0 {
				t.Fatalf("CreateFile session2 %d failed: err=%v, resp=%v", i, err, resp)
			}
			newIno := resp.GetAttr().GetInode().GetIno()
			for _, oldIno := range createdInodes {
				if newIno == oldIno {
					t.Fatalf("duplicate inode allocated across restart: %d (previously allocated: %v)", newIno, createdInodes)
				}
			}
		}
	}
}

func TestVolume_InodeAllocatorRecomputeWhenStatsDeleted(t *testing.T) {
	ctx := t.Context()
	storage := inmemorystorage.New()
	dir := t.TempDir()
	localDir := filepath.Join(dir, "local")

	volumeID := "test-deleted-stats-vol"

	// Session 1: create 5 files (inodes 8, 16, 24, 32, 40)
	var createdInodes []uint64
	{
		server1 := NewServer(storage,
			WithServerMetadataIndex("sqlite"),
			WithServerLocalStorageDir(localDir),
		)
		vol1, err := server1.getOrCreateVolume(volumeID)
		if err != nil {
			t.Fatalf("getOrCreateVolume failed: %v", err)
		}

		for i := 0; i < 5; i++ {
			resp, err := server1.CreateFile(ctx, &pb.CreateFileRequest{
				VolumeId:    volumeID,
				ParentInode: 1,
				Name:        filepath.Join("", string(rune('a'+i))),
				Mode:        0644,
			})
			if err != nil || resp.GetError() != 0 {
				t.Fatalf("CreateFile %d failed: err=%v, resp=%v", i, err, resp)
			}
			createdInodes = append(createdInodes, resp.GetAttr().GetInode().GetIno())
			relResp, err := server1.Release(ctx, &pb.ReleaseRequest{
				VolumeId: volumeID,
				Inode:    resp.GetAttr().GetInode().GetIno(),
				Fh:       resp.GetFh(),
			})
			if err != nil || relResp.GetError() != 0 {
				t.Fatalf("Release %d failed: err=%v, resp=%v", i, err, relResp)
			}
		}
		if err := vol1.FlushToBackend(ctx); err != nil {
			t.Fatalf("FlushToBackend failed: %v", err)
		}
		if err := server1.Close(); err != nil {
			t.Fatalf("server1.Close failed: %v", err)
		}
	}

	// Delete VolumeStats row from SQLite to simulate pre-existing index without stats
	dbFiles, err := filepath.Glob(filepath.Join(localDir, "*.sqlite"))
	if err != nil || len(dbFiles) == 0 {
		t.Fatalf("no sqlite file found in %s: %v", localDir, err)
	}
	db, err := sql.Open("sqlite", dbFiles[0])
	if err != nil {
		t.Fatalf("sql.Open failed: %v", err)
	}
	if _, err := db.Exec("DELETE FROM objectfs_v1alpha1_VolumeStats;"); err != nil {
		t.Fatalf("db.Exec DELETE failed: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("db.Close failed: %v", err)
	}

	// Session 2: reopen volume with same local directory
	{
		server2 := NewServer(storage,
			WithServerMetadataIndex("sqlite"),
			WithServerLocalStorageDir(localDir),
		)
		vol2, err := server2.getOrCreateVolume(volumeID)
		if err != nil {
			t.Fatalf("getOrCreateVolume run 2 failed: %v", err)
		}
		defer func() {
			if err := server2.Close(); err != nil {
				t.Errorf("server2.Close failed: %v", err)
			}
		}()

		st := vol2.Stats()
		if st.Stats.GetMaxIno() != 40 {
			t.Errorf("MaxIno after recomputing stats from scan: got %d, want 40", st.Stats.GetMaxIno())
		}

		// Next CreateFile must not reuse any existing inode (allocates 48)
		resp, err := server2.CreateFile(ctx, &pb.CreateFileRequest{
			VolumeId:    volumeID,
			ParentInode: 1,
			Name:        "new_file.txt",
			Mode:        0644,
		})
		if err != nil || resp.GetError() != 0 {
			t.Fatalf("CreateFile run 2 failed: err=%v, resp=%v", err, resp)
		}
		newIno := resp.GetAttr().GetInode().GetIno()
		for _, oldIno := range createdInodes {
			if newIno == oldIno {
				t.Fatalf("reused existing inode %d after stats row deletion! (existing: %v)", newIno, createdInodes)
			}
		}

		if err := vol2.FlushToBackend(ctx); err != nil {
			t.Fatalf("FlushToBackend run 2 failed: %v", err)
		}
		if err := vol2.View().Flush(ctx); err != nil {
			t.Fatalf("View.Flush run 2 failed: %v", err)
		}
		deadline := time.Now().Add(5 * time.Second)
		for {
			st = vol2.Stats()
			if st.Stats.GetMaxIno() == newIno || time.Now().After(deadline) {
				break
			}
			time.Sleep(5 * time.Millisecond)
		}
		if st.Stats.GetMaxIno() != newIno {
			t.Errorf("MaxIno after flush: got %d, want %d", st.Stats.GetMaxIno(), newIno)
		}
	}
}

func TestVolume_CrashWithUnappliedBacklogAndRecovery(t *testing.T) {
	ctx := t.Context()
	storage := inmemorystorage.New()
	dir := t.TempDir()
	walDir := filepath.Join(dir, "wal")

	volumeID := "test-crash-backlog-vol"
	var unappliedInodes []uint64

	// Session 1: Create files remaining in overlay
	{
		server1 := NewServer(storage,
			WithServerWAL(walDir, "", walclient.Local),
			WithServerLocalStorageDir(filepath.Join(dir, "session1")),
			WithServerMetadataApplierBatchSize(1000),
		)
		vol1, err := server1.getOrCreateVolume(volumeID)
		if err != nil {
			t.Fatalf("getOrCreateVolume failed: %v", err)
		}

		if _, err := vol1.CreateSnapshot(ctx); err != nil {
			t.Fatalf("CreateSnapshot failed: %v", err)
		}

		for i := 0; i < 5; i++ {
			resp, err := server1.CreateFile(ctx, &pb.CreateFileRequest{
				VolumeId:    volumeID,
				ParentInode: 1,
				Name:        filepath.Join("", string(rune('a'+i))),
				Mode:        0644,
			})
			if err != nil || resp.GetError() != 0 {
				t.Fatalf("CreateFile %d failed: err=%v, resp=%v", i, err, resp)
			}
			unappliedInodes = append(unappliedInodes, resp.GetAttr().GetInode().GetIno())
			relResp, err := server1.Release(ctx, &pb.ReleaseRequest{
				VolumeId: volumeID,
				Inode:    resp.GetAttr().GetInode().GetIno(),
				Fh:       resp.GetFh(),
			})
			if err != nil || relResp.GetError() != 0 {
				t.Fatalf("Release %d failed: err=%v, resp=%v", i, err, relResp)
			}
		}
		if err := server1.Close(); err != nil {
			t.Fatalf("server1.Close failed: %v", err)
		}
	}

	// Session 2: Recover from backend + WAL stream
	{
		server2 := NewServer(storage,
			WithServerWAL(walDir, "", walclient.Local),
			WithServerLocalStorageDir(filepath.Join(dir, "session2")),
		)
		_, err := server2.getOrCreateVolume(volumeID)
		if err != nil {
			t.Fatalf("getOrCreateVolume failed: %v", err)
		}
		defer func() {
			if err := server2.Close(); err != nil {
				t.Errorf("server2.Close failed: %v", err)
			}
		}()

		for i := 0; i < 5; i++ {
			resp, err := server2.CreateFile(ctx, &pb.CreateFileRequest{
				VolumeId:    volumeID,
				ParentInode: 1,
				Name:        filepath.Join("", string(rune('z'-i))),
				Mode:        0644,
			})
			if err != nil || resp.GetError() != 0 {
				t.Fatalf("CreateFile session2 %d failed: err=%v, resp=%v", i, err, resp)
			}
			newIno := resp.GetAttr().GetInode().GetIno()
			for _, oldIno := range unappliedInodes {
				if newIno == oldIno {
					t.Fatalf("duplicate inode allocated after crash recovery with unapplied backlog: %d (unapplied backlog had: %v)", newIno, unappliedInodes)
				}
			}
		}
	}
}
