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
	"bytes"
	"context"
	"testing"

	pb "github.com/gke-labs/in-cluster-storage/pkg/api/objectfs/v1alpha1"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

func findSpanAttribute(attributes []attribute.KeyValue, key string) (attribute.Value, bool) {
	for _, attributeItem := range attributes {
		if string(attributeItem.Key) == key {
			return attributeItem.Value, true
		}
	}
	return attribute.Value{}, false
}

func TestVolumeSyncTracing(t *testing.T) {
	exporter := tracetest.NewInMemoryExporter()
	tracerProvider := trace.NewTracerProvider(trace.WithSyncer(exporter))
	previousProvider := otel.GetTracerProvider()
	otel.SetTracerProvider(tracerProvider)
	t.Cleanup(func() {
		otel.SetTracerProvider(previousProvider)
		if err := tracerProvider.Shutdown(context.WithoutCancel(t.Context())); err != nil {
			t.Errorf("failed to shut down tracer provider: %v", err)
		}
	})

	volumeID := "test-sync-trace-volume"
	backend := NewMemoryBackend()
	broadcaster := NewEventBroadcaster()
	volume := MustNewVolume(t, volumeID, backend, broadcaster)
	t.Cleanup(func() {
		if err := volume.Close(); err != nil {
			t.Errorf("volume.Close failed: %v", err)
		}
	})

	ctx := t.Context()
	fileAttr, err := volume.CreateFile(ctx, 1, "data.bin", 0644, nil, 0, 0, 0)
	if err != nil {
		t.Fatalf("CreateFile failed: %v", err)
	}
	inodeID := fileAttr.GetInode().GetIno()

	// 64 KiB of data creates exactly 1 chunk (> maxInlineLen of 4 KiB)
	payload := bytes.Repeat([]byte("a"), 64*1024)
	if _, _, _, _, err := volume.WriteFileWithEpoch(ctx, inodeID, 0, payload, pb.WriteMode_LAZY); err != nil {
		t.Fatalf("WriteFileWithEpoch failed: %v", err)
	}

	trigger := pb.FsyncRequest_SYNC_TRIGGER_CLOSE
	if _, _, err := volume.FsyncWithEpoch(ctx, inodeID, true, 0, trigger); err != nil {
		t.Fatalf("FsyncWithEpoch failed: %v", err)
	}

	exportedSpans := exporter.GetSpans()
	if len(exportedSpans) != 5 {
		t.Fatalf("expected 5 exported spans (1 parent + 4 children), got %d: %+v", len(exportedSpans), exportedSpans)
	}

	var parentSpan *tracetest.SpanStub
	childSpans := make(map[string]tracetest.SpanStub)
	for index := range exportedSpans {
		span := exportedSpans[index]
		if span.Name == spanVolumeSync {
			parentSpan = &exportedSpans[index]
		} else {
			childSpans[span.Name] = span
		}
	}

	if parentSpan == nil {
		t.Fatalf("expected parent span %s not found", spanVolumeSync)
	}

	expectedChildren := []string{
		spanVolumeSyncSeal,
		spanVolumeSyncUpload,
		spanVolumeSyncAppend,
		spanVolumeSyncCommitWait,
	}

	parentSpanID := parentSpan.SpanContext.SpanID()
	for _, expectedName := range expectedChildren {
		childSpan, ok := childSpans[expectedName]
		if !ok {
			t.Fatalf("missing expected child span %q", expectedName)
		}
		if childSpan.Parent.SpanID() != parentSpanID {
			t.Errorf("child span %s parent span ID = %s, want %s", expectedName, childSpan.Parent.SpanID(), parentSpanID)
		}
	}

	// Verify parent attributes
	volumeAttribute, ok := findSpanAttribute(parentSpan.Attributes, attributeVolume)
	if !ok {
		t.Fatalf("missing attribute %s", attributeVolume)
	}
	if volumeAttribute.AsString() != volumeID {
		t.Errorf("%s = %q, want %q", attributeVolume, volumeAttribute.AsString(), volumeID)
	}

	triggerAttribute, ok := findSpanAttribute(parentSpan.Attributes, attributeSyncTrigger)
	if !ok {
		t.Fatalf("missing attribute %s", attributeSyncTrigger)
	}
	if triggerAttribute.AsString() != trigger.String() {
		t.Errorf("%s = %q, want %q", attributeSyncTrigger, triggerAttribute.AsString(), trigger.String())
	}

	bytesAttribute, ok := findSpanAttribute(parentSpan.Attributes, attributeSyncBytes)
	if !ok {
		t.Fatalf("missing attribute %s", attributeSyncBytes)
	}
	var packByteSize int64
	volume.stagingMu.Lock()
	for _, packInfo := range volume.packs {
		if packInfo.pack != nil {
			packByteSize = packInfo.pack.ByteSize()
		}
	}
	volume.stagingMu.Unlock()
	if packByteSize == 0 || bytesAttribute.AsInt64() != packByteSize {
		t.Errorf("%s = %d, want pack byte size %d", attributeSyncBytes, bytesAttribute.AsInt64(), packByteSize)
	}

	recordsAttribute, ok := findSpanAttribute(parentSpan.Attributes, attributeSyncRecords)
	if !ok {
		t.Fatalf("missing attribute %s", attributeSyncRecords)
	}
	// Exactly 3 WAL records: 1 FileChunk + 1 Inode update + 1 TxCommit
	if recordsAttribute.AsInt64() != 3 {
		t.Errorf("%s = %d, want 3", attributeSyncRecords, recordsAttribute.AsInt64())
	}

	packsAttribute, ok := findSpanAttribute(parentSpan.Attributes, attributeSyncPacks)
	if !ok {
		t.Fatalf("missing attribute %s", attributeSyncPacks)
	}
	if packsAttribute.AsInt64() != 1 {
		t.Errorf("%s = %d, want 1", attributeSyncPacks, packsAttribute.AsInt64())
	}
}
