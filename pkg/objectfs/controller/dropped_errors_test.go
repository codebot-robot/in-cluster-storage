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
	"context"
	"errors"
	"iter"
	"sync/atomic"
	"testing"

	"github.com/gke-labs/in-cluster-storage/pkg/objectstore/inmemorystorage"
	"github.com/gke-labs/in-cluster-storage/pkg/sds"
	"google.golang.org/protobuf/proto"
)

type faultyRootApplyIndex struct {
	sds.LocalIndex
	failApply atomic.Bool
}

func (f *faultyRootApplyIndex) ApplyBatch(ctx context.Context, changes []sds.Change) error {
	if f.failApply.Load() {
		return errors.New("injected apply batch failure")
	}
	return f.LocalIndex.ApplyBatch(ctx, changes)
}

type faultyRootApplyFactory struct {
	sds.IndexFactory
	idx *faultyRootApplyIndex
}

func (f *faultyRootApplyFactory) NewEmpty(ctx context.Context, streamID string, dir string) (sds.LocalIndex, error) {
	inner, err := f.IndexFactory.NewEmpty(ctx, streamID, dir)
	if err != nil {
		return nil, err
	}
	f.idx.LocalIndex = inner
	return f.idx, nil
}

func (f *faultyRootApplyFactory) OpenLocal(ctx context.Context, streamID string, dir string) (sds.LocalIndex, bool, error) {
	inner, found, err := f.IndexFactory.OpenLocal(ctx, streamID, dir)
	if err != nil || !found {
		return nil, found, err
	}
	f.idx.LocalIndex = inner
	return f.idx, true, nil
}

func TestRegression_RootInodeInitFailure(t *testing.T) {
	baseFactory, err := sds.GetIndexFactory("memory")
	if err != nil {
		t.Fatalf("GetIndexFactory failed: %v", err)
	}

	faultyIdx := &faultyRootApplyIndex{}
	faultyIdx.failApply.Store(true)
	factory := &faultyRootApplyFactory{IndexFactory: baseFactory, idx: faultyIdx}

	// 1. NewVolume should fail when root inode initialization fails.
	vol, err := NewVolume("test-vol-root-fail", nil, NewEventBroadcaster(),
		WithIndexFactory(factory),
	)
	if err == nil {
		if vol != nil {
			_ = vol.Close()
		}
		t.Fatal("expected NewVolume to fail when root inode creation fails, got nil")
	}

	// 2. loadFromBackendMetadataLocked should also fail when root inode initialization fails.
	faultyIdx.failApply.Store(false)
	volSuccess, err := NewVolume("test-vol-root-reload", nil, NewEventBroadcaster(),
		WithIndexFactory(factory),
	)
	if err != nil {
		t.Fatalf("expected NewVolume to succeed when failApply is false, got: %v", err)
	}
	defer volSuccess.Close()

	// Clear metadataView to simulate empty uninitialized reload and activate failure
	volSuccess.mu.Lock()
	volSuccess.metadataView = nil
	faultyIdx.failApply.Store(true)
	err = volSuccess.loadFromBackendMetadataLocked(t.Context())
	volSuccess.mu.Unlock()
	if err == nil {
		t.Fatal("expected loadFromBackendMetadataLocked to fail when root inode creation fails, got nil")
	}
}

type faultyChunkScanIndex struct {
	sds.LocalIndex
	failChunkScan atomic.Bool
}

func (f *faultyChunkScanIndex) Scan(ctx context.Context, typeName string, prefix []byte) iter.Seq2[proto.Message, error] {
	if f.failChunkScan.Load() && typeName == "objectfs.v1alpha1.FileChunk" {
		return func(yield func(proto.Message, error) bool) {
			yield(nil, errors.New("injected FileChunk scan failure"))
		}
	}
	return f.LocalIndex.Scan(ctx, typeName, prefix)
}

type faultyChunkScanFactory struct {
	sds.IndexFactory
	idx *faultyChunkScanIndex
}

func (f *faultyChunkScanFactory) NewEmpty(ctx context.Context, streamID string, dir string) (sds.LocalIndex, error) {
	inner, err := f.IndexFactory.NewEmpty(ctx, streamID, dir)
	if err != nil {
		return nil, err
	}
	f.idx.LocalIndex = inner
	return f.idx, nil
}

func (f *faultyChunkScanFactory) OpenLocal(ctx context.Context, streamID string, dir string) (sds.LocalIndex, bool, error) {
	inner, found, err := f.IndexFactory.OpenLocal(ctx, streamID, dir)
	if err != nil || !found {
		return nil, found, err
	}
	f.idx.LocalIndex = inner
	return f.idx, true, nil
}

func TestRegression_ChunkListScanFailure(t *testing.T) {
	ctx := t.Context()
	backend := inmemorystorage.New()

	baseFactory, err := sds.GetIndexFactory("memory")
	if err != nil {
		t.Fatalf("GetIndexFactory failed: %v", err)
	}

	faultyIdx := &faultyChunkScanIndex{}
	factory := &faultyChunkScanFactory{IndexFactory: baseFactory, idx: faultyIdx}

	vol := MustNewVolume(t, "test-vol-chunk-fail", backend, NewEventBroadcaster(),
		WithIndexFactory(factory),
		WithChunkSize(1024),
		WithMetadataCacheDisabled(true),
	)
	defer vol.Close()

	if err := vol.LoadFromBackend(ctx); err != nil {
		t.Fatalf("LoadFromBackend failed: %v", err)
	}

	// Create file with content and flush overlay so chunks are in the index
	data := []byte("hello chunk failure test data")
	attr, err := vol.CreateFile(ctx, 1, "test.txt", 0644, data, 1000, 1000)
	if err != nil {
		t.Fatalf("CreateFile failed: %v", err)
	}
	ino := attr.GetInode().GetIno()

	if err := vol.FlushOverlay(ctx); err != nil {
		t.Fatalf("FlushOverlay failed: %v", err)
	}

	// Now inject failure on FileChunk scan
	faultyIdx.failChunkScan.Store(true)

	// Attempt to read file; it must fail rather than returning empty/proceeding
	_, _, _, err = vol.ReadFile(ctx, ino, 0, int64(len(data)))
	if err == nil {
		t.Fatal("expected ReadFile to fail when FileChunk scan fails, got nil")
	}
}
