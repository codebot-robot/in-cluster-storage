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

func parseCertificates(t *testing.T, pemData string) []*x509.Certificate {
	t.Helper()
	var certs []*x509.Certificate
	rest := []byte(pemData)
	for len(rest) > 0 {
		var block *pem.Block
		block, rest = pem.Decode(rest)
		if block == nil {
			break
		}
		if block.Type != "CERTIFICATE" {
			continue
		}
		cert, err := x509.ParseCertificate(block.Bytes)
		if err != nil {
			t.Fatalf("failed to parse certificate: %v", err)
		}
		certs = append(certs, cert)
	}
	return certs
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
	caCerts := parseCertificates(t, serverSec.Data["ca.crt"])
	if len(caCerts) != 1 {
		t.Fatalf("expected 1 CA cert in bundle on initial generation, got %d", len(caCerts))
	}
	caCert := caCerts[0]

	if !caCert.IsCA {
		t.Errorf("expected caCert.IsCA to be true")
	}
	if caCert.Subject.CommonName != "objectfs-ca" {
		t.Errorf("expected CA CN objectfs-ca, got %q", caCert.Subject.CommonName)
	}
	if _, ok := serverSec.Data["ca-previous.crt"]; ok {
		t.Errorf("ca-previous.crt should not be present; bundle is in ca.crt")
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
	initialCACert := parseCert(t, initialCA)

	// First rotation: previous CA provided
	rotated1, err := GenerateCertificates(cfg, []byte(initialCA))
	if err != nil {
		t.Fatalf("first rotated GenerateCertificates failed: %v", err)
	}

	for _, sec := range rotated1 {
		if _, ok := sec.Data["ca-previous.crt"]; ok {
			t.Errorf("expected ca-previous.crt to not be present")
		}

		caCerts := parseCertificates(t, sec.Data["ca.crt"])
		if len(caCerts) != 2 {
			t.Fatalf("expected 2 CA certs in bundle on rotation, got %d", len(caCerts))
		}
		newCACert := caCerts[0]
		prevCACert := caCerts[1]

		if newCACert.Equal(initialCACert) {
			t.Errorf("expected fresh CA on rotation, got same CA")
		}
		if !prevCACert.Equal(initialCACert) {
			t.Errorf("expected previous CA to be preserved as second cert in bundle")
		}

		pool := x509.NewCertPool()
		pool.AddCert(newCACert)
		pool.AddCert(prevCACert)

		leafCert := parseCert(t, sec.Data["tls.crt"])
		if _, err := leafCert.Verify(x509.VerifyOptions{Roots: pool, KeyUsages: []x509.ExtKeyUsage{x509.ExtKeyUsageAny}}); err != nil {
			t.Errorf("leaf certificate failed verification against trust pool with new and previous CA: %v", err)
		}
	}

	// Second rotation: keep 2, drop older
	rotated2, err := GenerateCertificates(cfg, []byte(rotated1[0].Data["ca.crt"]))
	if err != nil {
		t.Fatalf("second rotated GenerateCertificates failed: %v", err)
	}
	rotated1CACert := parseCertificates(t, rotated1[0].Data["ca.crt"])[0]

	for _, sec := range rotated2 {
		caCerts := parseCertificates(t, sec.Data["ca.crt"])
		if len(caCerts) != 2 {
			t.Fatalf("expected 2 CA certs in bundle on second rotation, got %d", len(caCerts))
		}
		if !caCerts[1].Equal(rotated1CACert) {
			t.Errorf("expected previous CA to be rotated1 CA")
		}
		for _, c := range caCerts {
			if c.Equal(initialCACert) {
				t.Errorf("expected oldest CA to be dropped from bundle")
			}
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
		if _, ok := sec.Data["ca-previous.crt"]; ok {
			t.Errorf("secret %s unexpectedly contains ca-previous.crt", name)
		}
		caCerts := parseCertificates(t, string(sec.Data["ca.crt"]))
		if len(caCerts) != 2 {
			t.Fatalf("expected 2 CA certs in bundle for secret %s, got %d", name, len(caCerts))
		}
		if !caCerts[1].Equal(parseCert(t, initialCA)) {
			t.Errorf("secret %s did not carry forward previous CA in ca.crt bundle", name)
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
		if len(sec.Data["tls.key"]) == 0 || len(sec.Data["tls.crt"]) == 0 || len(sec.Data["ca.crt"]) == 0 {
			t.Errorf("secret %s missing required keys (tls.key, tls.crt, ca.crt)", exp)
		}
		if _, ok := sec.Data["ca-previous.crt"]; ok {
			t.Errorf("secret %s unexpectedly contains ca-previous.crt", exp)
		}
		caCerts := parseCertificates(t, string(sec.Data["ca.crt"]))
		if len(caCerts) != 1 {
			t.Errorf("expected 1 CA cert in bundle on initial creation for secret %s, got %d", exp, len(caCerts))
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
