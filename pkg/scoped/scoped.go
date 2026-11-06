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

// Package scoped provides context-backed logging helpers.
package scoped

import (
	"context"

	"github.com/go-logr/logr"
	"k8s.io/klog/v2"
)

// Log returns the logger attached to the context, or the default klog logger if none is set.
func Log(ctx context.Context) logr.Logger {
	return klog.FromContext(ctx)
}

// WithLog returns a new context with the given logger attached.
func WithLog(ctx context.Context, logger logr.Logger) context.Context {
	return klog.NewContext(ctx, logger)
}
