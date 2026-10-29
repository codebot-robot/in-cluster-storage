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

package certsigner

import (
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
)

func parseCert(t *testing.T, pemData string) *x509.Certificate {
	t.Helper()
	block, _ := pem.Decode([]byte(pemData))
	if block == nil {
		t.Fatalf("failed to decode PEM block")
	}
	cert, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		t.Fatalf("failed to parse certificate: %v", err)
	}
	return cert
}

func TestGenerateCertificates(t *testing.T) {
	cfg := Config{
		Namespace:        "test-ns",
		ServiceName:      "test-controller",
		ControllerSecret: "test-controller-server",
		NodeDaemonSecret: "test-node-daemon-client",
		AdminSecret:      "test-admin-client",
	}

	secrets, err := GenerateCertificates(cfg, nil)
	if err != nil {
		t.Fatalf("GenerateCertificates failed: %v", err)
	}

	if len(secrets) != 3 {
		t.Fatalf("expected 3 secrets, got %d", len(secrets))
	}

	secMap := make(map[string]GeneratedSecret)
	for _, s := range secrets {
		secMap[s.Name] = s
	}

	// Verify server secret
	serverSec, ok := secMap[cfg.ControllerSecret]
	if !ok {
		t.Fatalf("missing server secret %s", cfg.ControllerSecret)
	}
	serverCert := parseCert(t, serverSec.Data["tls.crt"])
	caCert := parseCert(t, serverSec.Data["ca.crt"])

	if !caCert.IsCA {
		t.Errorf("expected caCert.IsCA to be true")
	}
	if caCert.Subject.CommonName != "objectfs-ca" {
		t.Errorf("expected CA CN objectfs-ca, got %q", caCert.Subject.CommonName)
	}
	if serverSec.Data["ca-previous.crt"] != serverSec.Data["ca.crt"] {
		t.Errorf("expected ca-previous.crt to mirror ca.crt on initial generation")
	}

	if serverCert.Subject.CommonName != cfg.ServiceName {
		t.Errorf("expected server CN %q, got %q", cfg.ServiceName, serverCert.Subject.CommonName)
	}
	dnsFound := false
	for _, dns := range serverCert.DNSNames {
		if dns == "test-controller.test-ns.svc.cluster.local" {
			dnsFound = true
			break
		}
	}
	if !dnsFound {
		t.Errorf("expected full cluster DNS name in server cert DNS names: %v", serverCert.DNSNames)
	}

	// Verify node-daemon client secret
	nodeSec, ok := secMap[cfg.NodeDaemonSecret]
	if !ok {
		t.Fatalf("missing node secret %s", cfg.NodeDaemonSecret)
	}
	nodeCert := parseCert(t, nodeSec.Data["tls.crt"])
	if nodeCert.Subject.CommonName != "objectfs-node-daemon" {
		t.Errorf("expected node CN objectfs-node-daemon, got %q", nodeCert.Subject.CommonName)
	}
	if len(nodeCert.URIs) != 1 || nodeCert.URIs[0].String() != "objectfs://node-daemon/test-ns/objectfs-node-daemon" {
		t.Errorf("expected node URI objectfs://node-daemon/test-ns/objectfs-node-daemon, got %v", nodeCert.URIs)
	}

	// Verify admin client secret
	adminSec, ok := secMap[cfg.AdminSecret]
	if !ok {
		t.Fatalf("missing admin secret %s", cfg.AdminSecret)
	}
	adminCert := parseCert(t, adminSec.Data["tls.crt"])
	if adminCert.Subject.CommonName != "objectfs-admin" {
		t.Errorf("expected admin CN objectfs-admin, got %q", adminCert.Subject.CommonName)
	}
	if len(adminCert.URIs) != 1 || adminCert.URIs[0].String() != "objectfs://admin/test-ns/objectfs-admin" {
		t.Errorf("expected admin URI objectfs://admin/test-ns/objectfs-admin, got %v", adminCert.URIs)
	}

	// Verify server cert is signed by the CA
	roots := x509.NewCertPool()
	roots.AddCert(caCert)

	if _, err := serverCert.Verify(x509.VerifyOptions{Roots: roots, KeyUsages: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}}); err != nil {
		t.Errorf("server certificate failed verification against CA: %v", err)
	}

	// Verify client certs are signed by the CA
	for _, cert := range []*x509.Certificate{nodeCert, adminCert} {
		if _, err := cert.Verify(x509.VerifyOptions{Roots: roots, KeyUsages: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}}); err != nil {
			t.Errorf("certificate %q failed verification against CA: %v", cert.Subject.CommonName, err)
		}
	}
}

func TestGenerateCertificates_PreviousCA(t *testing.T) {
	cfg := Config{
		Namespace:        "test-ns",
		ServiceName:      "test-controller",
		ControllerSecret: "test-controller-server",
		NodeDaemonSecret: "test-node-daemon-client",
		AdminSecret:      "test-admin-client",
	}

	initialSecrets, err := GenerateCertificates(cfg, nil)
	if err != nil {
		t.Fatalf("initial GenerateCertificates failed: %v", err)
	}
	initialCA := initialSecrets[0].Data["ca.crt"]

	// Rotate with previous CA provided
	rotatedSecrets, err := GenerateCertificates(cfg, []byte(initialCA))
	if err != nil {
		t.Fatalf("rotated GenerateCertificates failed: %v", err)
	}

	for _, sec := range rotatedSecrets {
		newCA := sec.Data["ca.crt"]
		prevCA := sec.Data["ca-previous.crt"]

		if newCA == initialCA {
			t.Errorf("expected fresh CA on rotation, got same CA")
		}
		if prevCA != initialCA {
			t.Errorf("expected previous CA to be preserved in ca-previous.crt")
		}

		newCert := parseCert(t, newCA)
		prevCert := parseCert(t, prevCA)

		pool := x509.NewCertPool()
		pool.AddCert(newCert)
		pool.AddCert(prevCert)

		leafCert := parseCert(t, sec.Data["tls.crt"])
		if _, err := leafCert.Verify(x509.VerifyOptions{Roots: pool, KeyUsages: []x509.ExtKeyUsage{x509.ExtKeyUsageAny}}); err != nil {
			t.Errorf("leaf certificate failed verification against trust pool with new and previous CA: %v", err)
		}
	}
}

func TestEnsureSecrets_Rotate(t *testing.T) {
	var mu sync.Mutex
	secretsStore := make(map[string]map[string]string)

	initialSecrets, err := GenerateCertificates(Config{
		Namespace:        "test-ns",
		ServiceName:      "test-controller",
		ControllerSecret: "server-sec",
		NodeDaemonSecret: "node-sec",
		AdminSecret:      "admin-sec",
	}, nil)
	if err != nil {
		t.Fatalf("initial cert generation failed: %v", err)
	}
	initialCA := initialSecrets[0].Data["ca.crt"]
	for _, s := range initialSecrets {
		secretsStore[s.Name] = s.Data
	}

	putCount := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()

		if r.Method == http.MethodGet {
			parts := strings.Split(r.URL.Path, "/")
			name := parts[len(parts)-1]
			data, ok := secretsStore[name]
			if !ok {
				w.WriteHeader(http.StatusNotFound)
				return
			}
			resp := map[string]any{
				"stringData": data,
			}
			w.WriteHeader(http.StatusOK)
			_ = json.NewEncoder(w).Encode(resp)
			return
		}
		if r.Method == http.MethodPut {
			var sec kubeSecret
			if err := json.NewDecoder(r.Body).Decode(&sec); err != nil {
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			secretsStore[sec.Metadata.Name] = sec.StringData
			putCount++
			w.WriteHeader(http.StatusOK)
			return
		}
		t.Errorf("unexpected %s request to %s", r.Method, r.URL.Path)
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer server.Close()

	cfg := Config{
		APIServerURL:     server.URL,
		Namespace:        "test-ns",
		ServiceName:      "test-controller",
		ControllerSecret: "server-sec",
		NodeDaemonSecret: "node-sec",
		AdminSecret:      "admin-sec",
	}

	if err := EnsureSecrets(t.Context(), cfg); err != nil {
		t.Fatalf("EnsureSecrets rotation failed: %v", err)
	}

	mu.Lock()
	defer mu.Unlock()

	if putCount != 3 {
		t.Errorf("expected 3 PUT requests to update existing secrets, got %d", putCount)
	}
	for _, name := range []string{"server-sec", "node-sec", "admin-sec"} {
		sec := secretsStore[name]
		if sec["ca-previous.crt"] != initialCA {
			t.Errorf("secret %s did not carry forward previous CA in ca-previous.crt", name)
		}
		if sec["ca.crt"] == initialCA {
			t.Errorf("secret %s has stale CA in ca.crt", name)
		}
	}
}

func TestEnsureSecrets_Create(t *testing.T) {
	var mu sync.Mutex
	createdSecrets := make(map[string]kubeSecret)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()

		if r.Method == http.MethodGet {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		if r.Method == http.MethodPost {
			var sec kubeSecret
			if err := json.NewDecoder(r.Body).Decode(&sec); err != nil {
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			createdSecrets[sec.Metadata.Name] = sec
			w.WriteHeader(http.StatusCreated)
			return
		}
		t.Errorf("unexpected %s request to %s", r.Method, r.URL.Path)
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer server.Close()

	cfg := Config{
		APIServerURL:     server.URL,
		Namespace:        "test-ns",
		ServiceName:      "test-controller",
		ControllerSecret: "server-sec",
		NodeDaemonSecret: "node-sec",
		AdminSecret:      "admin-sec",
	}

	if err := EnsureSecrets(t.Context(), cfg); err != nil {
		t.Fatalf("EnsureSecrets failed: %v", err)
	}

	mu.Lock()
	defer mu.Unlock()

	expected := []string{"server-sec", "node-sec", "admin-sec"}
	for _, exp := range expected {
		sec, ok := createdSecrets[exp]
		if !ok {
			t.Errorf("expected secret %s was not created", exp)
			continue
		}
		if sec.StringData["tls.key"] == "" || sec.StringData["tls.crt"] == "" || sec.StringData["ca.crt"] == "" || sec.StringData["ca-previous.crt"] == "" {
			t.Errorf("secret %s missing required keys (tls.key, tls.crt, ca.crt, ca-previous.crt)", exp)
		}
	}
}
