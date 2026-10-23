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

package main

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"

	pb "github.com/gke-labs/in-cluster-storage/pkg/api/v1alpha1"
	"google.golang.org/grpc"
)

type failCloseWriter struct {
	io.WriteCloser
	closeErr error
}

func (w *failCloseWriter) Close() error {
	_ = w.WriteCloser.Close()
	return w.closeErr
}

type mockDownloadStream struct {
	grpc.ClientStream
	data   []byte
	offset int
}

func (s *mockDownloadStream) Recv() (*pb.DownloadBlobResponse, error) {
	if s.offset >= len(s.data) {
		return nil, io.EOF
	}
	end := s.offset + 1024
	if end > len(s.data) {
		end = len(s.data)
	}
	chunk := s.data[s.offset:end]
	s.offset = end
	return &pb.DownloadBlobResponse{Content: chunk}, nil
}

type mockAgentFSClient struct {
	pb.AgentFSControllerClient
	data []byte
}

func (m *mockAgentFSClient) DownloadBlob(ctx context.Context, in *pb.DownloadBlobRequest, opts ...grpc.CallOption) (grpc.ServerStreamingClient[pb.DownloadBlobResponse], error) {
	return &mockDownloadStream{data: m.data}, nil
}

func TestControllerDownloaderDownloadBlobCloseError(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "cas-download-close-err-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tempDir)

	data := []byte("hello cas blob download close error test")
	destPath := filepath.Join(tempDir, "blobs", "target-blob")

	injectedErr := errors.New("simulated close/flush failure on cas writer")
	downloader := &controllerDownloader{
		client: &mockAgentFSClient{data: data},
		wrapWriter: func(wc io.WriteCloser) io.WriteCloser {
			return &failCloseWriter{WriteCloser: wc, closeErr: injectedErr}
		},
	}

	err = downloader.DownloadBlob(t.Context(), "test-sha", destPath)
	if err == nil {
		t.Fatal("expected DownloadBlob to return error on failing Close(), got nil")
	}
	if !errors.Is(err, injectedErr) {
		t.Fatalf("expected error to wrap injectedErr, got: %v", err)
	}

	// Verify that the destination file was not created
	if _, err := os.Stat(destPath); !os.IsNotExist(err) {
		t.Fatalf("expected destPath %s to not exist, got err: %v", destPath, err)
	}

	// Verify that the temporary file was cleaned up
	destDir := filepath.Dir(destPath)
	entries, err := os.ReadDir(destDir)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 0 {
		t.Fatalf("expected 0 files in destination directory on failure, got %d", len(entries))
	}

	// Verify retry without failing Close succeeds
	downloader.wrapWriter = nil
	downloader.client = &mockAgentFSClient{data: data}
	if err := downloader.DownloadBlob(t.Context(), "test-sha", destPath); err != nil {
		t.Fatalf("expected subsequent DownloadBlob to succeed, got: %v", err)
	}
	content, err := os.ReadFile(destPath)
	if err != nil {
		t.Fatalf("failed reading downloaded blob: %v", err)
	}
	if !bytes.Equal(content, data) {
		t.Fatalf("downloaded blob content mismatch")
	}
}
