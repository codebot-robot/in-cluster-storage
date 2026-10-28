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
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"net/url"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"

	pb "github.com/gke-labs/in-cluster-storage/pkg/api/objectfs/v1alpha1"
)

func TestParseCertificateIdentity(t *testing.T) {
	tests := []struct {
		name         string
		uri          string
		multipleURIs []string
		expectNS     string
		wantRole     Role
		wantUsername string
		wantErr      bool
	}{
		{
			name:         "node-daemon in expected namespace",
			uri:          "objectfs://node-daemon/kube-objectfs-system/objectfs-node-daemon",
			expectNS:     "kube-objectfs-system",
			wantRole:     RoleNode,
			wantUsername: "system:serviceaccount:kube-objectfs-system:objectfs-node-daemon",
		},
		{
			name:         "admin in expected namespace",
			uri:          "objectfs://admin/kube-objectfs-system/objectfs-admin",
			expectNS:     "kube-objectfs-system",
			wantRole:     RoleAdmin,
			wantUsername: "system:serviceaccount:kube-objectfs-system:objectfs-admin",
		},
		{
			name:         "tenant isolation: node-daemon in evil namespace gets RoleNone",
			uri:          "objectfs://node-daemon/evil/objectfs-node-daemon",
			expectNS:     "kube-objectfs-system",
			wantRole:     RoleNone,
			wantUsername: "system:serviceaccount:evil:objectfs-node-daemon",
		},
		{
			name:         "tenant isolation: admin in evil namespace gets RoleNone",
			uri:          "objectfs://admin/evil/objectfs-admin",
			expectNS:     "kube-objectfs-system",
			wantRole:     RoleNone,
			wantUsername: "system:serviceaccount:evil:objectfs-admin",
		},
		{
			name:         "unknown role gets RoleNone",
			uri:          "objectfs://attacker/kube-objectfs-system/attacker-sa",
			expectNS:     "kube-objectfs-system",
			wantRole:     RoleNone,
			wantUsername: "system:serviceaccount:kube-objectfs-system:attacker-sa",
		},
		{
			name:     "malformed uri path missing serviceaccount",
			uri:      "objectfs://admin/kube-objectfs-system",
			expectNS: "kube-objectfs-system",
			wantErr:  true,
		},
		{
			name:     "trailing slash rejected",
			uri:      "objectfs://admin/kube-objectfs-system/objectfs-admin/",
			expectNS: "kube-objectfs-system",
			wantErr:  true,
		},
		{
			name:     "extra path segment rejected",
			uri:      "objectfs://admin/kube-objectfs-system/objectfs-admin/extra",
			expectNS: "kube-objectfs-system",
			wantErr:  true,
		},
		{
			name:     "empty path segment rejected",
			uri:      "objectfs://admin//objectfs-admin",
			expectNS: "kube-objectfs-system",
			wantErr:  true,
		},
		{
			name:         "uppercase role gets RoleNone",
			uri:          "objectfs://ADMIN/kube-objectfs-system/objectfs-admin",
			expectNS:     "kube-objectfs-system",
			wantRole:     RoleNone,
			wantUsername: "system:serviceaccount:kube-objectfs-system:objectfs-admin",
		},
		{
			name:         "multiple objectfs URIs rejected",
			uri:          "objectfs://admin/kube-objectfs-system/objectfs-admin",
			multipleURIs: []string{"objectfs://admin/kube-objectfs-system/objectfs-admin", "objectfs://node-daemon/kube-objectfs-system/objectfs-node-daemon"},
			expectNS:     "kube-objectfs-system",
			wantErr:      true,
		},
		{
			name:     "missing URI SAN",
			uri:      "",
			expectNS: "kube-objectfs-system",
			wantErr:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cert := &x509.Certificate{}
			if len(tt.multipleURIs) > 0 {
				for _, uStr := range tt.multipleURIs {
					u, err := url.Parse(uStr)
					if err != nil {
						t.Fatalf("failed to parse URI %q: %v", uStr, err)
					}
					cert.URIs = append(cert.URIs, u)
				}
			} else if tt.uri != "" {
				u, err := url.Parse(tt.uri)
				if err != nil {
					t.Fatalf("failed to parse test URI %q: %v", tt.uri, err)
				}
				cert.URIs = []*url.URL{u}
			}

			id, err := ParseCertificateIdentity(cert, tt.expectNS)
			if (err != nil) != tt.wantErr {
				t.Fatalf("ParseCertificateIdentity() error = %v, wantErr %v", err, tt.wantErr)
			}
			if tt.wantErr {
				return
			}
			if id.Role != tt.wantRole {
				t.Errorf("got role %q, want %q", id.Role, tt.wantRole)
			}
			if id.Username != tt.wantUsername {
				t.Errorf("got username %q, want %q", id.Username, tt.wantUsername)
			}
		})
	}
}

func contextWithPeerCert(ctx context.Context, cert *x509.Certificate) context.Context {
	tlsInfo := credentials.TLSInfo{
		State: tls.ConnectionState{
			VerifiedChains: [][]*x509.Certificate{{cert}},
		},
	}
	p := &peer.Peer{
		AuthInfo: tlsInfo,
	}
	return peer.NewContext(ctx, p)
}

func TestServerAuthInterceptor(t *testing.T) {
	cfg := &AuthConfig{
		Namespace: "kube-objectfs-system",
		Insecure:  false,
	}
	interceptor := NewServerAuthInterceptor(cfg)

	nodeURI, _ := url.Parse("objectfs://node-daemon/kube-objectfs-system/objectfs-node-daemon")
	nodeCert := &x509.Certificate{URIs: []*url.URL{nodeURI}}

	adminURI, _ := url.Parse("objectfs://admin/kube-objectfs-system/objectfs-admin")
	adminCert := &x509.Certificate{URIs: []*url.URL{adminURI}}

	evilURI, _ := url.Parse("objectfs://admin/evil/objectfs-admin")
	evilCert := &x509.Certificate{URIs: []*url.URL{evilURI}}

	handler := func(ctx context.Context, req any) (any, error) {
		id, ok := AuthIdentityFromContext(ctx)
		if !ok {
			return nil, fmt.Errorf("missing AuthIdentity in context")
		}
		return id, nil
	}

	// 1. Missing peer info -> Unauthenticated
	_, err := interceptor.UnaryServerInterceptor()(t.Context(), nil, &grpc.UnaryServerInfo{FullMethod: "/objectfs.v1alpha1.ObjectFSController/GetAttr"}, handler)
	if status.Code(err) != codes.Unauthenticated {
		t.Errorf("expected Unauthenticated for missing peer info, got %v", err)
	}

	// 2. Node cert calling allowed method (GetAttr) -> succeeds
	ctxNode := contextWithPeerCert(t.Context(), nodeCert)
	res, err := interceptor.UnaryServerInterceptor()(ctxNode, nil, &grpc.UnaryServerInfo{FullMethod: "/objectfs.v1alpha1.ObjectFSController/GetAttr"}, handler)
	if err != nil {
		t.Fatalf("expected node calling GetAttr to succeed, got %v", err)
	}
	id := res.(AuthIdentity)
	if id.Role != RoleNode {
		t.Errorf("expected RoleNode, got %v", id.Role)
	}

	// 3. Node cert calling admin method (CreateSnapshot) -> PermissionDenied
	_, err = interceptor.UnaryServerInterceptor()(ctxNode, nil, &grpc.UnaryServerInfo{FullMethod: "/objectfs.v1alpha1.ObjectFSController/CreateSnapshot"}, handler)
	if status.Code(err) != codes.PermissionDenied {
		t.Errorf("expected PermissionDenied for node calling CreateSnapshot, got %v", err)
	}

	// 4. Admin cert calling admin method (CreateSnapshot) -> succeeds
	ctxAdmin := contextWithPeerCert(t.Context(), adminCert)
	_, err = interceptor.UnaryServerInterceptor()(ctxAdmin, nil, &grpc.UnaryServerInfo{FullMethod: "/objectfs.v1alpha1.ObjectFSController/CreateSnapshot"}, handler)
	if err != nil {
		t.Fatalf("expected admin calling CreateSnapshot to succeed, got %v", err)
	}

	// 5. Cross-namespace tenant cert calling GetAttr -> PermissionDenied
	ctxEvil := contextWithPeerCert(t.Context(), evilCert)
	_, err = interceptor.UnaryServerInterceptor()(ctxEvil, nil, &grpc.UnaryServerInfo{FullMethod: "/objectfs.v1alpha1.ObjectFSController/GetAttr"}, handler)
	if status.Code(err) != codes.PermissionDenied {
		t.Errorf("expected PermissionDenied for cross-namespace cert, got %v", err)
	}

	// 6. Insecure mode allows any caller without peer info
	insecureCfg := &AuthConfig{Insecure: true}
	insecureInterceptor := NewServerAuthInterceptor(insecureCfg)
	_, err = insecureInterceptor.UnaryServerInterceptor()(t.Context(), nil, &grpc.UnaryServerInfo{FullMethod: "/objectfs.v1alpha1.ObjectFSController/CreateSnapshot"}, func(ctx context.Context, req any) (any, error) {
		return "ok", nil
	})
	if err != nil {
		t.Errorf("expected insecure mode to allow all callers, got %v", err)
	}
}

func TestAuthMethodClassificationDriftGuard(t *testing.T) {
	// expectedClassification explicitly classifies every RPC exposed by ObjectFSController
	// as either RoleNode (node-allowed) or RoleAdmin (admin-only).
	expectedClassification := map[string]Role{
		// Filesystem operations (FUSE driver and checkpoint replay)
		pb.ObjectFSController_GetAttr_FullMethodName:      RoleNode,
		pb.ObjectFSController_SetAttr_FullMethodName:      RoleNode,
		pb.ObjectFSController_Lookup_FullMethodName:       RoleNode,
		pb.ObjectFSController_ReadDir_FullMethodName:      RoleNode,
		pb.ObjectFSController_Mkdir_FullMethodName:        RoleNode,
		pb.ObjectFSController_CreateFile_FullMethodName:   RoleNode,
		pb.ObjectFSController_ReadFile_FullMethodName:     RoleNode,
		pb.ObjectFSController_WriteFile_FullMethodName:    RoleNode,
		pb.ObjectFSController_TruncateFile_FullMethodName: RoleNode,
		pb.ObjectFSController_PunchHole_FullMethodName:    RoleNode,
		pb.ObjectFSController_Unlink_FullMethodName:       RoleNode,
		pb.ObjectFSController_Rmdir_FullMethodName:        RoleNode,
		pb.ObjectFSController_Rename_FullMethodName:       RoleNode,
		pb.ObjectFSController_Fsync_FullMethodName:        RoleNode,
		pb.ObjectFSController_Symlink_FullMethodName:      RoleNode,
		pb.ObjectFSController_Readlink_FullMethodName:     RoleNode,
		pb.ObjectFSController_Link_FullMethodName:         RoleNode,
		pb.ObjectFSController_Open_FullMethodName:         RoleNode,
		pb.ObjectFSController_Release_FullMethodName:      RoleNode,

		// Volume metadata, statistics, and sync (materialisation and health monitoring)
		pb.ObjectFSController_GetManifest_FullMethodName:    RoleNode,
		pb.ObjectFSController_GetVolumeStats_FullMethodName: RoleNode,

		// Invalidation and checkpoint event streams
		pb.ObjectFSController_WatchVolume_FullMethodName:   RoleNode,
		pb.ObjectFSController_AckCheckpoint_FullMethodName: RoleNode,

		// Exclusive-local volume lease management (#315)
		pb.ObjectFSController_AcquireExclusive_FullMethodName: RoleNode,
		pb.ObjectFSController_RenewExclusive_FullMethodName:   RoleNode,
		pb.ObjectFSController_ReleaseExclusive_FullMethodName: RoleNode,
		pb.ObjectFSController_GetExclusiveHold_FullMethodName: RoleNode,

		// Blob access and prefetch (disk cache and blob downloads)
		pb.ObjectFSController_GetBlob_FullMethodName:   RoleNode,
		pb.ObjectFSController_ListBlobs_FullMethodName: RoleNode,

		// Admin-only operations
		pb.ObjectFSController_ListVolumes_FullMethodName:    RoleAdmin,
		pb.ObjectFSController_ListSnapshots_FullMethodName:  RoleAdmin,
		pb.ObjectFSController_CreateSnapshot_FullMethodName: RoleAdmin,
		pb.ObjectFSController_Checkpoint_FullMethodName:     RoleAdmin,
	}

	service := pb.ObjectFSController_ServiceDesc
	discovered := make(map[string]bool)

	for _, m := range service.Methods {
		fullMethod := fmt.Sprintf("/%s/%s", service.ServiceName, m.MethodName)
		discovered[fullMethod] = true
	}
	for _, s := range service.Streams {
		fullMethod := fmt.Sprintf("/%s/%s", service.ServiceName, s.StreamName)
		discovered[fullMethod] = true
	}

	cfg := DefaultAuthConfig()

	// 1. Every method and stream in ObjectFSController_ServiceDesc must be in the classification table.
	for fullMethod := range discovered {
		role, ok := expectedClassification[fullMethod]
		if !ok {
			t.Errorf("RPC %s is defined in ObjectFSController_ServiceDesc but missing from expectedClassification table; new RPCs must be explicitly classified as RoleNode or RoleAdmin", fullMethod)
			continue
		}

		switch role {
		case RoleNode:
			if !nodeAllowedMethods[fullMethod] {
				t.Errorf("RPC %s is classified as RoleNode but missing from nodeAllowedMethods", fullMethod)
			}
			if err := cfg.Authorize(RoleNode, fullMethod); err != nil {
				t.Errorf("expected RoleNode authorized for %s, got %v", fullMethod, err)
			}
			if err := cfg.Authorize(RoleAdmin, fullMethod); err != nil {
				t.Errorf("expected RoleAdmin authorized for %s, got %v", fullMethod, err)
			}
		case RoleAdmin:
			if nodeAllowedMethods[fullMethod] {
				t.Errorf("RPC %s is classified as RoleAdmin but present in nodeAllowedMethods", fullMethod)
			}
			if err := cfg.Authorize(RoleNode, fullMethod); err == nil {
				t.Errorf("expected RoleNode denied for admin-only RPC %s, got nil error", fullMethod)
			}
			if err := cfg.Authorize(RoleAdmin, fullMethod); err != nil {
				t.Errorf("expected RoleAdmin authorized for %s, got %v", fullMethod, err)
			}
		default:
			t.Errorf("RPC %s has unexpected role classification %q (must be RoleNode or RoleAdmin)", fullMethod, role)
		}
	}

	// 2. No stale entries in expectedClassification table that are not in ObjectFSController_ServiceDesc.
	for fullMethod := range expectedClassification {
		if !discovered[fullMethod] {
			t.Errorf("expectedClassification table contains %s which is not defined in ObjectFSController_ServiceDesc Methods or Streams", fullMethod)
		}
	}

	// 3. No stale entries in nodeAllowedMethods that are not in ObjectFSController_ServiceDesc.
	for fullMethod := range nodeAllowedMethods {
		if !discovered[fullMethod] {
			t.Errorf("nodeAllowedMethods contains %s which is not defined in ObjectFSController_ServiceDesc Methods or Streams", fullMethod)
		}
	}
}
