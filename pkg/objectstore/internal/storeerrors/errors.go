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

// Package storeerrors holds shared sentinel errors and transport classification
// helpers used by pkg/objectstore and its backend subpackages without creating
// an import cycle with objectstore.Open.
package storeerrors

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"syscall"
)

var (
	// ErrNotFound is returned when the requested object does not exist.
	ErrNotFound = errors.New("object not found")
	// ErrUnavailable is returned when the store could not be reached or answered
	// that it is temporarily unable to serve the request, so the same request may
	// succeed later.
	ErrUnavailable = errors.New("object store unavailable")
)

// IsCallerContextError reports whether err is attributable to the caller's
// context being cancelled or its deadline expiring, rather than the store's
// state.
func IsCallerContextError(ctx context.Context, err error) bool {
	if ctx != nil && ctx.Err() != nil {
		return true
	}
	return errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)
}

// IsHTTPStatusUnavailable reports whether an HTTP response status code
// indicates the store is temporarily unable to serve the request: 5xx server
// errors, 429 Too Many Requests, or 408 Request Timeout.
func IsHTTPStatusUnavailable(statusCode int) bool {
	return (statusCode >= http.StatusInternalServerError && statusCode < 600) ||
		statusCode == http.StatusTooManyRequests ||
		statusCode == http.StatusRequestTimeout
}

// IsTransportUnavailable reports whether err represents a network or transport
// failure while talking to the store (connection refused or reset, DNS
// failure, unexpected EOF, or transport timeout). Callers must check
// IsCallerContextError first so a cancelled or expired caller context is not
// mistaken for a transport timeout.
func IsTransportUnavailable(err error) bool {
	if err == nil {
		return false
	}

	if errors.Is(err, io.ErrUnexpectedEOF) ||
		errors.Is(err, net.ErrClosed) ||
		errors.Is(err, os.ErrDeadlineExceeded) {
		return true
	}

	if errors.Is(err, syscall.ECONNREFUSED) ||
		errors.Is(err, syscall.ECONNRESET) ||
		errors.Is(err, syscall.ECONNABORTED) ||
		errors.Is(err, syscall.EPIPE) ||
		errors.Is(err, syscall.ENETUNREACH) ||
		errors.Is(err, syscall.EHOSTUNREACH) ||
		errors.Is(err, syscall.ETIMEDOUT) {
		return true
	}

	var dnsError *net.DNSError
	if errors.As(err, &dnsError) {
		return true
	}

	var opError *net.OpError
	if errors.As(err, &opError) {
		return true
	}

	var timeoutError interface{ Timeout() bool }
	if errors.As(err, &timeoutError) && timeoutError.Timeout() {
		return true
	}

	var urlError *url.Error
	if errors.As(err, &urlError) && errors.Is(urlError.Err, io.EOF) {
		return true
	}

	return false
}
