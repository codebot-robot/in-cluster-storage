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
	"testing"

	"github.com/container-storage-interface/spec/lib/go/csi"
	pb "github.com/gke-labs/in-cluster-storage/pkg/api/objectfs/v1alpha1"
)

func TestParseEndpoint(t *testing.T) {
	tests := []struct {
		endpoint  string
		wantProto string
		wantAddr  string
		wantErr   bool
	}{
		{"unix:///tmp/csi.sock", "unix", "/tmp/csi.sock", false},
		{"tcp://127.0.0.1:10000", "tcp", "127.0.0.1:10000", false},
		{"http://127.0.0.1:10000", "", "", true},
		{"invalid", "", "", true},
	}

	for _, tt := range tests {
		proto, addr, err := parseEndpoint(tt.endpoint)
		if (err != nil) != tt.wantErr {
			t.Errorf("parseEndpoint(%q) err = %v, wantErr %v", tt.endpoint, err, tt.wantErr)
		}
		if proto != tt.wantProto || addr != tt.wantAddr {
			t.Errorf("parseEndpoint(%q) = (%q, %q), want (%q, %q)", tt.endpoint, proto, addr, tt.wantProto, tt.wantAddr)
		}
	}
}

func TestParseWriteMode(t *testing.T) {
	tests := []struct {
		input string
		want  pb.WriteMode
	}{
		{"lazy", pb.WriteMode_LAZY_WRITE},
		{"lazy_write", pb.WriteMode_LAZY_WRITE},
		{"eager", pb.WriteMode_EAGER_REPLICATION},
		{"eager_replication", pb.WriteMode_EAGER_REPLICATION},
		{"fsync", pb.WriteMode_WRITE_THROUGH_FSYNC},
		{"writethrough", pb.WriteMode_WRITE_THROUGH_FSYNC},
		{"unknown", pb.WriteMode_WRITE_THROUGH_FSYNC},
	}

	for _, tt := range tests {
		got := parseWriteMode(tt.input)
		if got != tt.want {
			t.Errorf("parseWriteMode(%q) = %v, want %v", tt.input, got, tt.want)
		}
	}
}

func TestPluginInfo(t *testing.T) {
	driver := newDriver("test-node", "localhost:50051", 1024*1024)
	ctx := t.Context()

	info, err := driver.GetPluginInfo(ctx, &csi.GetPluginInfoRequest{})
	if err != nil {
		t.Fatalf("GetPluginInfo failed: %v", err)
	}
	if info.GetName() != "objectfs.labs.gke.io" {
		t.Errorf("Expected driver name objectfs.labs.gke.io, got %s", info.GetName())
	}

	nodeInfo, err := driver.NodeGetInfo(ctx, &csi.NodeGetInfoRequest{})
	if err != nil {
		t.Fatalf("NodeGetInfo failed: %v", err)
	}
	if nodeInfo.GetNodeId() != "test-node" {
		t.Errorf("Expected node ID test-node, got %s", nodeInfo.GetNodeId())
	}
}
