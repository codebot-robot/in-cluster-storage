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
	"testing"

	pb "github.com/gke-labs/in-cluster-storage/pkg/api/objectfs/v1alpha1"
	"github.com/gke-labs/in-cluster-storage/pkg/objectstore/inmemorystorage"
)

func BenchmarkWriteFilePushLatency(b *testing.B) {
	ctx := b.Context()
	backend := inmemorystorage.New()
	server := NewServer(backend, WithServerMetadataDir(b.TempDir()))
	defer func() { _ = server.Close() }()

	volumeID := "bench-write-push-vol"
	cResp, err := testCreateFile(ctx, server, volumeID, "/push.bin", 0644, nil, 0, 0)
	if err != nil || cResp.GetError() != 0 {
		b.Fatalf("CreateFile failed: %v", err)
	}
	ino := cResp.GetAttr().GetInode().GetIno()

	chunk64K := make([]byte, 64*1024)

	b.Run("Sequential_64KiB", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			wResp, err := server.WriteFile(ctx, &pb.WriteFileRequest{
				VolumeId: volumeID,
				Inode:    ino,
				Offset:   int64(i%100) * 64 * 1024,
				Data:     chunk64K,
			})
			if err != nil || wResp.GetError() != 0 {
				b.Fatalf("WriteFile failed: %v, code=%d", err, wResp.GetError())
			}
		}
	})

	b.Run("ConcurrentGroupCommit_64KiB", func(b *testing.B) {
		b.ResetTimer()
		b.RunParallel(func(pbIter *testing.PB) {
			i := 0
			for pbIter.Next() {
				wResp, err := server.WriteFile(ctx, &pb.WriteFileRequest{
					VolumeId: volumeID,
					Inode:    ino,
					Offset:   int64(i%1000) * 64 * 1024,
					Data:     chunk64K,
				})
				if err != nil || wResp.GetError() != 0 {
					b.Fatalf("Concurrent WriteFile failed: %v", err)
				}
				i++
			}
		})
	})
}
