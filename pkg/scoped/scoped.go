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

// Package scoped provides context-backed logging and tracing helpers.
package scoped

import (
	"context"

	"github.com/go-logr/logr"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"
	"k8s.io/klog/v2"
)

const tracerName = "github.com/gke-labs/in-cluster-storage"

// StartSpan starts a span from the package tracer using the global provider.
// Callers must end the returned span.
func StartSpan(ctx context.Context, name string) (context.Context, trace.Span) {
	return otel.Tracer(tracerName).Start(ctx, name)
}

// Log returns the logger attached to the context, or the default klog logger if none is set.
// When the context contains a valid span context, the returned logger carries trace_id and span_id.
func Log(ctx context.Context) logr.Logger {
	logger := klog.FromContext(ctx)
	spanContext := trace.SpanContextFromContext(ctx)
	if spanContext.IsValid() {
		return logger.WithValues(
			"trace_id", spanContext.TraceID().String(),
			"span_id", spanContext.SpanID().String(),
		)
	}
	return logger
}

// WithLog returns a new context with the given logger attached.
func WithLog(ctx context.Context, logger logr.Logger) context.Context {
	return klog.NewContext(ctx, logger)
}
