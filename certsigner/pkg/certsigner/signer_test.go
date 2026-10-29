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
	"encoding/pem"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
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
	cfg := Config{
		Namespace:        "test-ns",
		ServiceName:      "test-controller",
		ControllerSecret: "server-sec",
		NodeDaemonSecret: "node-sec",
		AdminSecret:      "admin-sec",
	}

	initialSecrets, err := GenerateCertificates(cfg, nil)
	if err != nil {
		t.Fatalf("initial cert generation failed: %v", err)
	}
	initialCA := initialSecrets[0].Data["ca.crt"]

	client := fake.NewClientset()
	for _, s := range initialSecrets {
		dataBytes := make(map[string][]byte, len(s.Data))
		for k, v := range s.Data {
			dataBytes[k] = []byte(v)
		}
		sec := &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{
				Name:      s.Name,
				Namespace: cfg.Namespace,
			},
			Type: corev1.SecretTypeTLS,
			Data: dataBytes,
		}
		if _, err := client.CoreV1().Secrets(cfg.Namespace).Create(t.Context(), sec, metav1.CreateOptions{}); err != nil {
			t.Fatalf("failed to seed initial secret %s: %v", s.Name, err)
		}
	}

	if err := EnsureSecrets(t.Context(), client, cfg); err != nil {
		t.Fatalf("EnsureSecrets rotation failed: %v", err)
	}

	for _, name := range []string{"server-sec", "node-sec", "admin-sec"} {
		sec, err := client.CoreV1().Secrets(cfg.Namespace).Get(t.Context(), name, metav1.GetOptions{})
		if err != nil {
			t.Fatalf("failed to get rotated secret %s: %v", name, err)
		}
		if string(sec.Data["ca-previous.crt"]) != initialCA {
			t.Errorf("secret %s did not carry forward previous CA in ca-previous.crt", name)
		}
		if string(sec.Data["ca.crt"]) == initialCA {
			t.Errorf("secret %s has stale CA in ca.crt", name)
		}
	}
}

func TestEnsureSecrets_Create(t *testing.T) {
	cfg := Config{
		Namespace:        "test-ns",
		ServiceName:      "test-controller",
		ControllerSecret: "server-sec",
		NodeDaemonSecret: "node-sec",
		AdminSecret:      "admin-sec",
	}

	client := fake.NewClientset()
	if err := EnsureSecrets(t.Context(), client, cfg); err != nil {
		t.Fatalf("EnsureSecrets failed: %v", err)
	}

	expected := []string{"server-sec", "node-sec", "admin-sec"}
	for _, exp := range expected {
		sec, err := client.CoreV1().Secrets(cfg.Namespace).Get(t.Context(), exp, metav1.GetOptions{})
		if err != nil {
			t.Errorf("expected secret %s was not created: %v", exp, err)
			continue
		}
		if len(sec.Data["tls.key"]) == 0 || len(sec.Data["tls.crt"]) == 0 || len(sec.Data["ca.crt"]) == 0 || len(sec.Data["ca-previous.crt"]) == 0 {
			t.Errorf("secret %s missing required keys (tls.key, tls.crt, ca.crt, ca-previous.crt)", exp)
		}
	}
}

func TestEnsureSecrets_NilClient(t *testing.T) {
	cfg := Config{
		Namespace:        "test-ns",
		ServiceName:      "test-controller",
		ControllerSecret: "server-sec",
		NodeDaemonSecret: "node-sec",
		AdminSecret:      "admin-sec",
	}
	err := EnsureSecrets(t.Context(), nil, cfg)
	if err == nil {
		t.Fatalf("expected error when running outside cluster with nil client, got nil")
	}
}
