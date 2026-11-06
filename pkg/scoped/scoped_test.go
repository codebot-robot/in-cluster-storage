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

package scoped

import (
	"context"
	"sync"
	"testing"

	"github.com/go-logr/logr"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

type testSink struct {
	mu     sync.Mutex
	values map[string]any
}

func newTestSink() *testSink {
	return &testSink{values: make(map[string]any)}
}

func (s *testSink) Init(info logr.RuntimeInfo) {}

func (s *testSink) Enabled(level int) bool {
	return true
}

func (s *testSink) Info(level int, message string, keysAndValues ...any) {}

func (s *testSink) Error(err error, message string, keysAndValues ...any) {}

func (s *testSink) WithName(name string) logr.LogSink {
	return s
}

func (s *testSink) WithValues(keysAndValues ...any) logr.LogSink {
	s.mu.Lock()
	defer s.mu.Unlock()
	newValues := make(map[string]any, len(s.values)+len(keysAndValues)/2)
	for key, value := range s.values {
		newValues[key] = value
	}
	for index := 0; index < len(keysAndValues)-1; index += 2 {
		if key, ok := keysAndValues[index].(string); ok {
			newValues[key] = keysAndValues[index+1]
		}
	}
	return &testSink{values: newValues}
}

func (s *testSink) Value(key string) any {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.values[key]
}

func TestLogDefault(t *testing.T) {
	ctx := t.Context()
	logger := Log(ctx)
	if logger.GetSink() == nil {
		t.Fatal("expected non-nil sink for default logger")
	}
}

func TestWithLog(t *testing.T) {
	sink := newTestSink()
	expectedLogger := logr.New(sink).WithValues("key", "value")

	ctx := WithLog(t.Context(), expectedLogger)
	retrievedLogger := Log(ctx)

	retrievedSink, ok := retrievedLogger.GetSink().(*testSink)
	if !ok {
		t.Fatalf("expected *testSink, got %T", retrievedLogger.GetSink())
	}
	if got := retrievedSink.Value("key"); got != "value" {
		t.Fatalf("expected key=value, got %v", got)
	}
}

func TestStartWithProvider(t *testing.T) {
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

	sink := newTestSink()
	ctx := WithLog(t.Context(), logr.New(sink))

	ctx, span := Start(ctx, "test_operation")
	span.SetAttributes(attribute.String("test.key", "test.value"))

	logger := Log(ctx)
	retrievedSink, ok := logger.GetSink().(*testSink)
	if !ok {
		t.Fatalf("expected *testSink, got %T", logger.GetSink())
	}
	expectedTraceID := span.SpanContext().TraceID().String()
	if got := retrievedSink.Value("trace_id"); got != expectedTraceID {
		t.Fatalf("expected trace_id=%s, got %v", expectedTraceID, got)
	}
	expectedSpanID := span.SpanContext().SpanID().String()
	if got := retrievedSink.Value("span_id"); got != expectedSpanID {
		t.Fatalf("expected span_id=%s, got %v", expectedSpanID, got)
	}

	span.End()

	exportedSpans := exporter.GetSpans()
	if len(exportedSpans) != 1 {
		t.Fatalf("expected 1 exported span, got %d", len(exportedSpans))
	}
	exportedSpan := exportedSpans[0]
	if exportedSpan.Name != "test_operation" {
		t.Fatalf("expected span name test_operation, got %s", exportedSpan.Name)
	}
	if len(exportedSpan.Attributes) != 1 {
		t.Fatalf("expected 1 attribute, got %d", len(exportedSpan.Attributes))
	}
	if key := exportedSpan.Attributes[0].Key; key != "test.key" {
		t.Fatalf("expected attribute key test.key, got %s", key)
	}
	if value := exportedSpan.Attributes[0].Value.AsString(); value != "test.value" {
		t.Fatalf("expected attribute value test.value, got %s", value)
	}
}

func TestStartWithoutProvider(t *testing.T) {
	sink := newTestSink()
	ctx := WithLog(t.Context(), logr.New(sink))

	ctx, span := Start(ctx, "test_operation")
	if span.IsRecording() {
		t.Fatal("expected span to be non-recording without provider")
	}

	logger := Log(ctx)
	retrievedSink, ok := logger.GetSink().(*testSink)
	if !ok {
		t.Fatalf("expected *testSink, got %T", logger.GetSink())
	}
	if got := retrievedSink.Value("trace_id"); got != nil {
		t.Fatalf("expected trace_id to be unset, got %v", got)
	}
	if got := retrievedSink.Value("span_id"); got != nil {
		t.Fatalf("expected span_id to be unset, got %v", got)
	}

	span.End()
}
