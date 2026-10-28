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
	"crypto/x509"
	"fmt"
	"net/url"
	"strings"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
	"k8s.io/klog/v2"
)

// Role represents the authorization level granted to an authenticated caller.
type Role string

const (
	// RoleNone represents an unprivileged or unrecognized caller (all RPCs denied).
	RoleNone Role = "none"

	// RoleNode represents an authenticated node daemon (filesystem and CSI operations allowed).
	RoleNode Role = "node-daemon"

	// RoleAdmin represents an administrative caller (full access to all RPCs).
	RoleAdmin Role = "admin"
)

// nodeAllowedMethods is the set of gRPC methods permitted for the node-daemon role.
var nodeAllowedMethods = map[string]bool{
	// Filesystem operations (FUSE driver and checkpoint replay)
	"/objectfs.v1alpha1.ObjectFSController/GetAttr":      true,
	"/objectfs.v1alpha1.ObjectFSController/SetAttr":      true,
	"/objectfs.v1alpha1.ObjectFSController/Lookup":       true,
	"/objectfs.v1alpha1.ObjectFSController/ReadDir":      true,
	"/objectfs.v1alpha1.ObjectFSController/Mkdir":        true,
	"/objectfs.v1alpha1.ObjectFSController/CreateFile":   true,
	"/objectfs.v1alpha1.ObjectFSController/ReadFile":     true,
	"/objectfs.v1alpha1.ObjectFSController/WriteFile":    true,
	"/objectfs.v1alpha1.ObjectFSController/TruncateFile": true,
	"/objectfs.v1alpha1.ObjectFSController/PunchHole":    true,
	"/objectfs.v1alpha1.ObjectFSController/Unlink":       true,
	"/objectfs.v1alpha1.ObjectFSController/Rmdir":        true,
	"/objectfs.v1alpha1.ObjectFSController/Rename":       true,
	"/objectfs.v1alpha1.ObjectFSController/Fsync":        true,
	"/objectfs.v1alpha1.ObjectFSController/Symlink":      true,
	"/objectfs.v1alpha1.ObjectFSController/Readlink":     true,
	"/objectfs.v1alpha1.ObjectFSController/Link":         true,
	"/objectfs.v1alpha1.ObjectFSController/Open":         true,
	"/objectfs.v1alpha1.ObjectFSController/Release":      true,

	// Volume metadata, statistics, and sync (materialisation and health monitoring)
	"/objectfs.v1alpha1.ObjectFSController/GetManifest":    true,
	"/objectfs.v1alpha1.ObjectFSController/GetVolumeStats": true,

	// Invalidation and checkpoint event streams
	"/objectfs.v1alpha1.ObjectFSController/WatchVolume":   true,
	"/objectfs.v1alpha1.ObjectFSController/AckCheckpoint": true,

	// Exclusive-local volume lease management (#315)
	"/objectfs.v1alpha1.ObjectFSController/AcquireExclusive": true,
	"/objectfs.v1alpha1.ObjectFSController/RenewExclusive":   true,
	"/objectfs.v1alpha1.ObjectFSController/ReleaseExclusive": true,
	"/objectfs.v1alpha1.ObjectFSController/GetExclusiveHold": true,

	// Blob access and prefetch (disk cache and blob downloads)
	"/objectfs.v1alpha1.ObjectFSController/GetBlob":   true,
	"/objectfs.v1alpha1.ObjectFSController/ListBlobs": true,
}

// AuthConfig controls controller authentication and role-based authorization.
type AuthConfig struct {
	Namespace string
	Insecure  bool
}

// DefaultAuthConfig returns default authorization configuration.
func DefaultAuthConfig() *AuthConfig {
	return &AuthConfig{
		Namespace: "kube-objectfs-system",
		Insecure:  false,
	}
}

// Authorize determines whether a role is permitted to call fullMethod.
func (c *AuthConfig) Authorize(role Role, fullMethod string) error {
	if c.Insecure {
		return nil
	}

	switch role {
	case RoleAdmin:
		return nil
	case RoleNode:
		if nodeAllowedMethods[fullMethod] {
			return nil
		}
		return status.Errorf(codes.PermissionDenied, "role %s is not authorized to call %s", role, fullMethod)
	default:
		return status.Errorf(codes.PermissionDenied, "unauthorized caller (%s) cannot call %s", role, fullMethod)
	}
}

// AuthIdentity holds the identity extracted from a verified client certificate.
type AuthIdentity struct {
	Username       string
	Role           Role
	Namespace      string
	ServiceAccount string
}

type authIdentityKey struct{}

// AuthIdentityFromContext retrieves the authenticated identity from ctx.
func AuthIdentityFromContext(ctx context.Context) (AuthIdentity, bool) {
	val := ctx.Value(authIdentityKey{})
	if val == nil {
		return AuthIdentity{}, false
	}
	id, ok := val.(AuthIdentity)
	return id, ok
}

// ParseCertificateIdentity extracts caller identity and role from a client certificate's
// URI Subject Alternative Name (SAN) formatted as objectfs://<role>/<namespace>/<serviceaccount>.
func ParseCertificateIdentity(cert *x509.Certificate, expectedNamespace string) (AuthIdentity, error) {
	if cert == nil {
		return AuthIdentity{}, fmt.Errorf("client certificate is nil")
	}

	var matchURI *url.URL
	for _, u := range cert.URIs {
		if u != nil && u.Scheme == "objectfs" {
			if matchURI != nil {
				return AuthIdentity{}, fmt.Errorf("client certificate carries multiple objectfs:// URI SANs")
			}
			matchURI = u
		}
	}

	if matchURI == nil {
		return AuthIdentity{}, fmt.Errorf("client certificate missing objectfs:// URI SAN")
	}

	roleStr := matchURI.Host
	if strings.HasSuffix(matchURI.Path, "/") {
		return AuthIdentity{}, fmt.Errorf("invalid objectfs URI SAN %q: trailing slash not permitted", matchURI.String())
	}
	path := strings.TrimPrefix(matchURI.Path, "/")
	parts := strings.Split(path, "/")
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
		return AuthIdentity{}, fmt.Errorf("invalid objectfs URI SAN %q: expected format objectfs://<role>/<namespace>/<serviceaccount>", matchURI.String())
	}

	namespace := parts[0]
	serviceAccount := parts[1]
	username := fmt.Sprintf("system:serviceaccount:%s:%s", namespace, serviceAccount)

	// Validate namespace matching
	if expectedNamespace != "" && namespace != expectedNamespace {
		klog.Warningf("Client certificate identity namespace mismatch: certificate has namespace %q, controller expected %q", namespace, expectedNamespace)
		return AuthIdentity{
			Username:       username,
			Role:           RoleNone,
			Namespace:      namespace,
			ServiceAccount: serviceAccount,
		}, nil
	}

	var role Role
	switch roleStr {
	case "node-daemon":
		role = RoleNode
	case "admin":
		role = RoleAdmin
	default:
		role = RoleNone
	}

	return AuthIdentity{
		Username:       username,
		Role:           role,
		Namespace:      namespace,
		ServiceAccount: serviceAccount,
	}, nil
}

// ServerAuthInterceptor enforces client certificate verification and role-based authorization.
type ServerAuthInterceptor struct {
	cfg *AuthConfig
}

// NewServerAuthInterceptor creates an interceptor enforcing mTLS client certificate authorization.
func NewServerAuthInterceptor(cfg *AuthConfig) *ServerAuthInterceptor {
	if cfg == nil {
		cfg = DefaultAuthConfig()
	}
	return &ServerAuthInterceptor{cfg: cfg}
}

func (s *ServerAuthInterceptor) authenticateAndAuthorize(ctx context.Context, fullMethod string) (context.Context, error) {
	if s.cfg.Insecure {
		return ctx, nil
	}

	p, ok := peer.FromContext(ctx)
	if !ok || p.AuthInfo == nil {
		klog.Warningf("Peer authentication failed for RPC %s: missing peer info", fullMethod)
		return nil, status.Errorf(codes.Unauthenticated, "missing peer authentication")
	}

	tlsInfo, ok := p.AuthInfo.(credentials.TLSInfo)
	if !ok || len(tlsInfo.State.VerifiedChains) == 0 || len(tlsInfo.State.VerifiedChains[0]) == 0 {
		klog.Warningf("Client certificate authentication failed for RPC %s: missing verified client certificate", fullMethod)
		return nil, status.Errorf(codes.Unauthenticated, "missing verified client certificate")
	}

	leaf := tlsInfo.State.VerifiedChains[0][0]
	identity, err := ParseCertificateIdentity(leaf, s.cfg.Namespace)
	if err != nil {
		klog.Warningf("Client certificate identity extraction failed for RPC %s: %v", fullMethod, err)
		return nil, status.Errorf(codes.Unauthenticated, "invalid client certificate identity: %v", err)
	}

	if err := s.cfg.Authorize(identity.Role, fullMethod); err != nil {
		klog.Warningf("Authorization failed for user %q (role %s) calling %s: %v", identity.Username, identity.Role, fullMethod, err)
		return nil, err
	}

	return context.WithValue(ctx, authIdentityKey{}, identity), nil
}

// UnaryServerInterceptor returns a grpc.UnaryServerInterceptor enforcing client certificate auth.
func (s *ServerAuthInterceptor) UnaryServerInterceptor() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		newCtx, err := s.authenticateAndAuthorize(ctx, info.FullMethod)
		if err != nil {
			return nil, err
		}
		return handler(newCtx, req)
	}
}

type wrappedServerStream struct {
	grpc.ServerStream
	ctx context.Context
}

func (w *wrappedServerStream) Context() context.Context {
	return w.ctx
}

// StreamServerInterceptor returns a grpc.StreamServerInterceptor enforcing client certificate auth.
func (s *ServerAuthInterceptor) StreamServerInterceptor() grpc.StreamServerInterceptor {
	return func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		newCtx, err := s.authenticateAndAuthorize(ss.Context(), info.FullMethod)
		if err != nil {
			return err
		}
		return handler(srv, &wrappedServerStream{ServerStream: ss, ctx: newCtx})
	}
}
