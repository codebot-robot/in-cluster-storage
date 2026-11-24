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

package s3storage

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

	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
	smithyhttp "github.com/aws/smithy-go/transport/http"
	"github.com/gke-labs/in-cluster-storage/pkg/objectstore/internal/storeerrors"
)

type stubTimeoutError struct{}

func (stubTimeoutError) Error() string { return "i/o timeout" }
func (stubTimeoutError) Timeout() bool { return true }

func httpResponseError(statusCode int, inner error) error {
	return &smithyhttp.ResponseError{
		Response: &smithyhttp.Response{
			Response: &http.Response{StatusCode: statusCode},
		},
		Err: inner,
	}
}

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
			name:            "http 500 internal error",
			err:             httpResponseError(http.StatusInternalServerError, &smithy.GenericAPIError{Code: "InternalError", Fault: smithy.FaultServer}),
			wantUnavailable: true,
		},
		{
			name:            "http 502 bad gateway",
			err:             httpResponseError(http.StatusBadGateway, errors.New("bad gateway")),
			wantUnavailable: true,
		},
		{
			name:            "http 503 service unavailable",
			err:             httpResponseError(http.StatusServiceUnavailable, &smithy.GenericAPIError{Code: "ServiceUnavailable", Fault: smithy.FaultServer}),
			wantUnavailable: true,
		},
		{
			name:            "http 504 gateway timeout",
			err:             httpResponseError(http.StatusGatewayTimeout, errors.New("gateway timeout")),
			wantUnavailable: true,
		},
		{
			name:            "http 429 too many requests",
			err:             httpResponseError(http.StatusTooManyRequests, &smithy.GenericAPIError{Code: "TooManyRequests"}),
			wantUnavailable: true,
		},
		{
			name:            "s3 SlowDown throttling code",
			err:             &smithy.GenericAPIError{Code: "SlowDown", Message: "Please reduce your request rate."},
			wantUnavailable: true,
		},
		{
			name:            "s3 ThrottlingException code",
			err:             &smithy.GenericAPIError{Code: "ThrottlingException", Message: "Rate exceeded"},
			wantUnavailable: true,
		},
		{
			name:            "s3 RequestTimeout code",
			err:             httpResponseError(http.StatusBadRequest, &smithy.GenericAPIError{Code: "RequestTimeout", Fault: smithy.FaultClient}),
			wantUnavailable: true,
		},
		{
			name: "connection refused wrapped in status-0 response error",
			err: httpResponseError(0, &url.Error{
				Op:  "Put",
				URL: "http://127.0.0.1:1/b/k",
				Err: &net.OpError{Op: "dial", Net: "tcp", Err: &os.SyscallError{Syscall: "connect", Err: syscall.ECONNREFUSED}},
			}),
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
			err:             &net.DNSError{Err: "no such host", Name: "s3.example.invalid", IsNotFound: true},
			wantUnavailable: true,
		},
		{
			name:            "transport timeout",
			err:             &url.Error{Op: "Put", URL: "https://s3.example.invalid/k", Err: stubTimeoutError{}},
			wantUnavailable: true,
		},
		{
			name:            "unexpected EOF mid-stream",
			err:             io.ErrUnexpectedEOF,
			wantUnavailable: true,
		},
		{
			name:            "not found NoSuchKey",
			err:             httpResponseError(http.StatusNotFound, &s3types.NoSuchKey{}),
			wantUnavailable: false,
		},
		{
			name:            "permission error AccessDenied",
			err:             httpResponseError(http.StatusForbidden, &smithy.GenericAPIError{Code: "AccessDenied", Fault: smithy.FaultClient}),
			wantUnavailable: false,
		},
		{
			name:            "authentication error 401",
			err:             httpResponseError(http.StatusUnauthorized, &smithy.GenericAPIError{Code: "InvalidAccessKeyId", Fault: smithy.FaultClient}),
			wantUnavailable: false,
		},
		{
			name:            "invalid request 400",
			err:             httpResponseError(http.StatusBadRequest, &smithy.GenericAPIError{Code: "InvalidArgument", Fault: smithy.FaultClient}),
			wantUnavailable: false,
		},
		{
			name:            "cancelled caller context",
			ctx:             cancelledCtx,
			err:             &smithy.CanceledError{Err: context.Canceled},
			wantUnavailable: false,
		},
		{
			name:            "expired caller context with timeout error",
			ctx:             expiredCtx,
			err:             &url.Error{Op: "Put", URL: "https://s3.example.invalid/k", Err: context.DeadlineExceeded},
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
