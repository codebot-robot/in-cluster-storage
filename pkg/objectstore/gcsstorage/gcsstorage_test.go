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

package gcsstorage

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"syscall"
	"testing"
	"time"

	"cloud.google.com/go/storage"
	"github.com/gke-labs/in-cluster-storage/pkg/objectstore/internal/storeerrors"
	"google.golang.org/api/googleapi"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type stubTimeoutError struct{}

func (stubTimeoutError) Error() string { return "i/o timeout" }
func (stubTimeoutError) Timeout() bool { return true }

func TestClassifyError(t *testing.T) {
	cancelledCtx, cancel := context.WithCancel(t.Context())
	cancel()

	expiredCtx, expireCancel := context.WithDeadline(t.Context(), time.Now().Add(-time.Second))
	defer expireCancel()

	cases := []struct {
		name            string
		ctx             context.Context
		err             error
		wantUnavailable bool
	}{
		{
			name:            "googleapi 500 internal error",
			err:             &googleapi.Error{Code: http.StatusInternalServerError, Message: "backendError"},
			wantUnavailable: true,
		},
		{
			name:            "googleapi 502 bad gateway",
			err:             &googleapi.Error{Code: http.StatusBadGateway, Message: "bad gateway"},
			wantUnavailable: true,
		},
		{
			name:            "googleapi 503 service unavailable",
			err:             &googleapi.Error{Code: http.StatusServiceUnavailable, Message: "service unavailable"},
			wantUnavailable: true,
		},
		{
			name:            "googleapi 504 gateway timeout",
			err:             &googleapi.Error{Code: http.StatusGatewayTimeout, Message: "gateway timeout"},
			wantUnavailable: true,
		},
		{
			name:            "googleapi 429 too many requests",
			err:             &googleapi.Error{Code: http.StatusTooManyRequests, Message: "rate limit exceeded"},
			wantUnavailable: true,
		},
		{
			name:            "googleapi 408 request timeout",
			err:             &googleapi.Error{Code: http.StatusRequestTimeout, Message: "request timeout"},
			wantUnavailable: true,
		},
		{
			name: "googleapi 403 rateLimitExceeded throttling reason",
			err: &googleapi.Error{
				Code:   http.StatusForbidden,
				Errors: []googleapi.ErrorItem{{Reason: "rateLimitExceeded"}},
			},
			wantUnavailable: true,
		},
		{
			name:            "grpc unavailable",
			err:             status.Error(codes.Unavailable, "transport closing"),
			wantUnavailable: true,
		},
		{
			name:            "grpc resource exhausted throttling",
			err:             status.Error(codes.ResourceExhausted, "quota exceeded"),
			wantUnavailable: true,
		},
		{
			name:            "grpc internal",
			err:             status.Error(codes.Internal, "internal error"),
			wantUnavailable: true,
		},
		{
			name:            "grpc server deadline exceeded with live context",
			err:             status.Error(codes.DeadlineExceeded, "backend timed out"),
			wantUnavailable: true,
		},
		{
			name:            "connection refused",
			err:             &net.OpError{Op: "dial", Net: "tcp", Err: &os.SyscallError{Syscall: "connect", Err: syscall.ECONNREFUSED}},
			wantUnavailable: true,
		},
		{
			name:            "connection reset",
			err:             &net.OpError{Op: "read", Net: "tcp", Err: &os.SyscallError{Syscall: "read", Err: syscall.ECONNRESET}},
			wantUnavailable: true,
		},
		{
			name:            "dns failure",
			err:             &net.DNSError{Err: "no such host", Name: "storage.googleapis.com", IsNotFound: true},
			wantUnavailable: true,
		},
		{
			name:            "transport timeout",
			err:             &url.Error{Op: "Get", URL: "https://storage.googleapis.com/b/k", Err: stubTimeoutError{}},
			wantUnavailable: true,
		},
		{
			name:            "unexpected EOF mid-stream",
			err:             io.ErrUnexpectedEOF,
			wantUnavailable: true,
		},
		{
			name:            "storage object not exist",
			err:             storage.ErrObjectNotExist,
			wantUnavailable: false,
		},
		{
			name:            "googleapi 404 not found",
			err:             &googleapi.Error{Code: http.StatusNotFound, Message: "No such object"},
			wantUnavailable: false,
		},
		{
			name:            "googleapi 401 unauthenticated",
			err:             &googleapi.Error{Code: http.StatusUnauthorized, Message: "Invalid Credentials"},
			wantUnavailable: false,
		},
		{
			name: "googleapi 403 permission denied",
			err: &googleapi.Error{
				Code:   http.StatusForbidden,
				Errors: []googleapi.ErrorItem{{Reason: "forbidden"}},
			},
			wantUnavailable: false,
		},
		{
			name:            "googleapi 400 invalid request",
			err:             &googleapi.Error{Code: http.StatusBadRequest, Message: "Invalid argument"},
			wantUnavailable: false,
		},
		{
			name:            "grpc permission denied",
			err:             status.Error(codes.PermissionDenied, "permission denied"),
			wantUnavailable: false,
		},
		{
			name:            "grpc not found",
			err:             status.Error(codes.NotFound, "object not found"),
			wantUnavailable: false,
		},
		{
			name:            "grpc invalid argument",
			err:             status.Error(codes.InvalidArgument, "invalid bucket"),
			wantUnavailable: false,
		},
		{
			name:            "cancelled caller context",
			ctx:             cancelledCtx,
			err:             context.Canceled,
			wantUnavailable: false,
		},
		{
			name:            "expired caller context with grpc deadline exceeded",
			ctx:             expiredCtx,
			err:             status.Error(codes.DeadlineExceeded, "context deadline exceeded"),
			wantUnavailable: false,
		},
		{
			name:            "context deadline exceeded error with live context",
			err:             context.DeadlineExceeded,
			wantUnavailable: false,
		},
	}

	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			ctx := testCase.ctx
			if ctx == nil {
				ctx = t.Context()
			}
			classified := classifyError(ctx, testCase.err)
			if got := errors.Is(classified, storeerrors.ErrUnavailable); got != testCase.wantUnavailable {
				t.Fatalf("errors.Is(classified, ErrUnavailable) = %v, want %v (err=%v)", got, testCase.wantUnavailable, classified)
			}
			if !errors.Is(classified, testCase.err) {
				t.Fatalf("original error lost from chain: classified=%v original=%v", classified, testCase.err)
			}
		})
	}
}
