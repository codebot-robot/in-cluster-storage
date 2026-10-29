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
	"bytes"
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"io"
	"log"
	"math/big"
	"net"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"
)

// GeneratedSecret holds PEM-encoded TLS material destined for a Kubernetes Secret.
type GeneratedSecret struct {
	Name string
	Data map[string]string
}

// Config specifies the configuration for issuing per-directory TLS Secrets.
type Config struct {
	APIServerURL     string
	TokenPath        string
	CACertPath       string
	Namespace        string
	ServiceName      string
	ControllerSecret string
	NodeDaemonSecret string
	AdminSecret      string
}

// DefaultConfig returns Config populated from standard in-cluster environment variables and paths.
func DefaultConfig() Config {
	apiServerURL := ""
	host := os.Getenv("KUBERNETES_SERVICE_HOST")
	port := os.Getenv("KUBERNETES_SERVICE_PORT")
	if host != "" && port != "" {
		apiServerURL = fmt.Sprintf("https://%s:%s", host, port)
	} else {
		apiServerURL = "https://kubernetes.default.svc"
	}

	ns := os.Getenv("POD_NAMESPACE")
	if ns == "" {
		data, err := os.ReadFile("/var/run/secrets/kubernetes.io/serviceaccount/namespace")
		if err == nil {
			ns = strings.TrimSpace(string(data))
		} else if !os.IsNotExist(err) {
			log.Printf("Warning: failed to read namespace file: %v", err)
		}
	}
	if ns == "" {
		ns = "kube-objectfs-system"
	}

	return Config{
		APIServerURL:     apiServerURL,
		TokenPath:        "/var/run/secrets/kubernetes.io/serviceaccount/token",
		CACertPath:       "/var/run/secrets/kubernetes.io/serviceaccount/ca.crt",
		Namespace:        ns,
		ServiceName:      "objectfs-controller",
		ControllerSecret: "objectfs-controller-server",
		NodeDaemonSecret: "objectfs-node-daemon-client",
		AdminSecret:      "objectfs-admin-client",
	}
}

// GenerateCertificates generates an in-memory self-signed CA (10-year validity, key never stored)
// and creates leaf certificates (1-year validity) for the controller server and node/admin clients.
// If previousCA is provided, it is stored as ca-previous.crt to provide CA trust overlap during rotation;
// if previousCA is empty, ca-previous.crt mirrors the active CA so volume projections succeed.
func GenerateCertificates(cfg Config, previousCA []byte) ([]GeneratedSecret, error) {
	caKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		return nil, fmt.Errorf("failed to generate CA private key: %w", err)
	}

	caTemplate := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject: pkix.Name{
			CommonName: "objectfs-ca",
		},
		NotBefore:             time.Now().Add(-1 * time.Hour),
		NotAfter:              time.Now().Add(10 * 365 * 24 * time.Hour),
		IsCA:                  true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageCRLSign,
		BasicConstraintsValid: true,
	}

	caDer, err := x509.CreateCertificate(rand.Reader, caTemplate, caTemplate, &caKey.PublicKey, caKey)
	if err != nil {
		return nil, fmt.Errorf("failed to create CA certificate: %w", err)
	}
	caPem := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: caDer})

	// 1. Controller Server Certificate
	serverKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		return nil, fmt.Errorf("failed to generate server key: %w", err)
	}

	dnsNames := []string{
		cfg.ServiceName,
		fmt.Sprintf("%s.%s", cfg.ServiceName, cfg.Namespace),
		fmt.Sprintf("%s.%s.svc", cfg.ServiceName, cfg.Namespace),
		fmt.Sprintf("%s.%s.svc.cluster.local", cfg.ServiceName, cfg.Namespace),
		"localhost",
	}

	serverTemplate := &x509.Certificate{
		SerialNumber: big.NewInt(2),
		Subject: pkix.Name{
			CommonName: cfg.ServiceName,
		},
		DNSNames:    dnsNames,
		IPAddresses: []net.IP{net.ParseIP("127.0.0.1")},
		NotBefore:   time.Now().Add(-1 * time.Hour),
		NotAfter:    time.Now().Add(365 * 24 * time.Hour),
		KeyUsage:    x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}

	serverDer, err := x509.CreateCertificate(rand.Reader, serverTemplate, caTemplate, &serverKey.PublicKey, caKey)
	if err != nil {
		return nil, fmt.Errorf("failed to create server certificate: %w", err)
	}
	serverPem := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: serverDer})
	serverKeyPem := pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(serverKey)})

	// 2. Node Daemon Client Certificate
	nodeKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		return nil, fmt.Errorf("failed to generate node client key: %w", err)
	}

	nodeURI, err := url.Parse(fmt.Sprintf("objectfs://node-daemon/%s/objectfs-node-daemon", cfg.Namespace))
	if err != nil {
		return nil, fmt.Errorf("failed to parse node daemon URI SAN: %w", err)
	}

	nodeTemplate := &x509.Certificate{
		SerialNumber: big.NewInt(3),
		Subject: pkix.Name{
			CommonName: "objectfs-node-daemon",
		},
		URIs:        []*url.URL{nodeURI},
		NotBefore:   time.Now().Add(-1 * time.Hour),
		NotAfter:    time.Now().Add(365 * 24 * time.Hour),
		KeyUsage:    x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
	}

	nodeDer, err := x509.CreateCertificate(rand.Reader, nodeTemplate, caTemplate, &nodeKey.PublicKey, caKey)
	if err != nil {
		return nil, fmt.Errorf("failed to create node client certificate: %w", err)
	}
	nodePem := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: nodeDer})
	nodeKeyPem := pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(nodeKey)})

	// 3. Admin Client Certificate
	adminKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		return nil, fmt.Errorf("failed to generate admin client key: %w", err)
	}

	adminURI, err := url.Parse(fmt.Sprintf("objectfs://admin/%s/objectfs-admin", cfg.Namespace))
	if err != nil {
		return nil, fmt.Errorf("failed to parse admin URI SAN: %w", err)
	}

	adminTemplate := &x509.Certificate{
		SerialNumber: big.NewInt(4),
		Subject: pkix.Name{
			CommonName: "objectfs-admin",
		},
		URIs:        []*url.URL{adminURI},
		NotBefore:   time.Now().Add(-1 * time.Hour),
		NotAfter:    time.Now().Add(365 * 24 * time.Hour),
		KeyUsage:    x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
	}

	adminDer, err := x509.CreateCertificate(rand.Reader, adminTemplate, caTemplate, &adminKey.PublicKey, caKey)
	if err != nil {
		return nil, fmt.Errorf("failed to create admin client certificate: %w", err)
	}
	adminPem := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: adminDer})
	adminKeyPem := pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(adminKey)})

	previousCAPEM := string(caPem)
	if len(previousCA) > 0 {
		previousCAPEM = string(previousCA)
	}

	return []GeneratedSecret{
		{
			Name: cfg.ControllerSecret,
			Data: map[string]string{
				"tls.key":         string(serverKeyPem),
				"tls.crt":         string(serverPem),
				"ca.crt":          string(caPem),
				"ca-previous.crt": previousCAPEM,
			},
		},
		{
			Name: cfg.NodeDaemonSecret,
			Data: map[string]string{
				"tls.key":         string(nodeKeyPem),
				"tls.crt":         string(nodePem),
				"ca.crt":          string(caPem),
				"ca-previous.crt": previousCAPEM,
			},
		},
		{
			Name: cfg.AdminSecret,
			Data: map[string]string{
				"tls.key":         string(adminKeyPem),
				"tls.crt":         string(adminPem),
				"ca.crt":          string(caPem),
				"ca-previous.crt": previousCAPEM,
			},
		},
	}, nil
}

type kubeSecret struct {
	APIVersion string            `json:"apiVersion"`
	Kind       string            `json:"kind"`
	Metadata   kubeSecretMeta    `json:"metadata"`
	Type       string            `json:"type"`
	StringData map[string]string `json:"stringData"`
}

type kubeSecretMeta struct {
	Name      string `json:"name"`
	Namespace string `json:"namespace"`
}

func closeAndLog(c io.Closer, what string) {
	if c != nil {
		if err := c.Close(); err != nil {
			log.Printf("failed to close %s: %v", what, err)
		}
	}
}

// EnsureSecrets checks whether the required TLS Secrets exist in the configured namespace,
// creating them if absent or rotating them in place if present while carrying forward previous CA
// certificates in the trust bundle.
func EnsureSecrets(ctx context.Context, cfg Config) error {
	var rootCAs *x509.CertPool
	if cfg.CACertPath != "" {
		b, err := os.ReadFile(cfg.CACertPath)
		if err == nil {
			pool := x509.NewCertPool()
			if !pool.AppendCertsFromPEM(b) {
				return fmt.Errorf("failed to parse CA certificate from %s", cfg.CACertPath)
			}
			rootCAs = pool
		} else if !os.IsNotExist(err) {
			return fmt.Errorf("failed to read CA certificate from %s: %w", cfg.CACertPath, err)
		}
	}

	httpClient := &http.Client{
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{RootCAs: rootCAs},
		},
		Timeout: 10 * time.Second,
	}

	var token string
	if cfg.TokenPath != "" {
		b, err := os.ReadFile(cfg.TokenPath)
		if err == nil {
			token = strings.TrimSpace(string(b))
		} else if !os.IsNotExist(err) {
			return fmt.Errorf("failed to read serviceaccount token from %s: %w", cfg.TokenPath, err)
		}
	}

	apiServerURL := strings.TrimRight(cfg.APIServerURL, "/")
	secretNames := []string{cfg.ControllerSecret, cfg.NodeDaemonSecret, cfg.AdminSecret}

	var previousCA []byte
	existingSecrets := make(map[string]bool)

	for _, name := range secretNames {
		data, exists, err := getSecret(ctx, httpClient, apiServerURL, token, cfg.Namespace, name)
		if err != nil {
			return err
		}
		if exists {
			existingSecrets[name] = true
			if len(previousCA) == 0 && len(data["ca.crt"]) > 0 {
				previousCA = data["ca.crt"]
			}
		}
	}

	secrets, err := GenerateCertificates(cfg, previousCA)
	if err != nil {
		return err
	}

	for _, sec := range secrets {
		if existingSecrets[sec.Name] {
			if err := updateSecret(ctx, httpClient, apiServerURL, token, cfg.Namespace, sec); err != nil {
				return err
			}
			log.Printf("Successfully updated TLS Secret %s/%s (carried forward previous CA in trust bundle)", cfg.Namespace, sec.Name)
		} else {
			if err := createSecret(ctx, httpClient, apiServerURL, token, cfg.Namespace, sec); err != nil {
				return err
			}
			log.Printf("Successfully created TLS Secret %s/%s", cfg.Namespace, sec.Name)
		}
	}

	return nil
}

func getSecret(ctx context.Context, client *http.Client, apiServerURL, token, namespace, name string) (map[string][]byte, bool, error) {
	reqURL := fmt.Sprintf("%s/api/v1/namespaces/%s/secrets/%s", apiServerURL, namespace, name)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, reqURL, nil)
	if err != nil {
		return nil, false, fmt.Errorf("failed to create GET request for secret %s: %w", name, err)
	}
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}

	resp, err := client.Do(req)
	if err != nil {
		return nil, false, fmt.Errorf("failed to get secret %s/%s: %w", namespace, name, err)
	}
	defer closeAndLog(resp.Body, "secret get response body")

	if resp.StatusCode == http.StatusNotFound {
		return nil, false, nil
	}
	if resp.StatusCode != http.StatusOK {
		body, readErr := io.ReadAll(resp.Body)
		if readErr != nil {
			return nil, false, fmt.Errorf("unexpected status %d getting secret %s/%s: %w", resp.StatusCode, namespace, name, readErr)
		}
		return nil, false, fmt.Errorf("unexpected status %d getting secret %s/%s: %s", resp.StatusCode, namespace, name, strings.TrimSpace(string(body)))
	}

	var secretResp struct {
		Data       map[string]string `json:"data"`
		StringData map[string]string `json:"stringData"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&secretResp); err != nil {
		return nil, false, fmt.Errorf("failed to decode secret response for %s: %w", name, err)
	}

	decoded := make(map[string][]byte)
	for k, v := range secretResp.Data {
		b, decErr := base64.StdEncoding.DecodeString(v)
		if decErr == nil {
			decoded[k] = b
		} else {
			decoded[k] = []byte(v)
		}
	}
	for k, v := range secretResp.StringData {
		if _, ok := decoded[k]; !ok {
			decoded[k] = []byte(v)
		}
	}
	return decoded, true, nil
}

func updateSecret(ctx context.Context, client *http.Client, apiServerURL, token, namespace string, sec GeneratedSecret) error {
	secretObj := kubeSecret{
		APIVersion: "v1",
		Kind:       "Secret",
		Metadata: kubeSecretMeta{
			Name:      sec.Name,
			Namespace: namespace,
		},
		Type:       "kubernetes.io/tls",
		StringData: sec.Data,
	}

	payload, err := json.Marshal(secretObj)
	if err != nil {
		return fmt.Errorf("failed to marshal secret json: %w", err)
	}

	reqURL := fmt.Sprintf("%s/api/v1/namespaces/%s/secrets/%s", apiServerURL, namespace, sec.Name)
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, reqURL, bytes.NewReader(payload))
	if err != nil {
		return fmt.Errorf("failed to create PUT request for secret %s: %w", sec.Name, err)
	}
	req.Header.Set("Content-Type", "application/json")
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}

	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("failed to PUT secret %s/%s: %w", namespace, sec.Name, err)
	}
	defer closeAndLog(resp.Body, "secret update response body")

	if resp.StatusCode == http.StatusOK || resp.StatusCode == http.StatusCreated {
		return nil
	}

	body, readErr := io.ReadAll(resp.Body)
	if readErr != nil {
		return fmt.Errorf("failed to update secret %s/%s: HTTP %d (failed to read body: %w)", namespace, sec.Name, resp.StatusCode, readErr)
	}
	return fmt.Errorf("failed to update secret %s/%s: HTTP %d: %s", namespace, sec.Name, resp.StatusCode, strings.TrimSpace(string(body)))
}

func createSecret(ctx context.Context, client *http.Client, apiServerURL, token, namespace string, sec GeneratedSecret) error {
	secretObj := kubeSecret{
		APIVersion: "v1",
		Kind:       "Secret",
		Metadata: kubeSecretMeta{
			Name:      sec.Name,
			Namespace: namespace,
		},
		Type:       "kubernetes.io/tls",
		StringData: sec.Data,
	}

	payload, err := json.Marshal(secretObj)
	if err != nil {
		return fmt.Errorf("failed to marshal secret json: %w", err)
	}

	reqURL := fmt.Sprintf("%s/api/v1/namespaces/%s/secrets", apiServerURL, namespace)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, reqURL, bytes.NewReader(payload))
	if err != nil {
		return fmt.Errorf("failed to create POST request for secret %s: %w", sec.Name, err)
	}
	req.Header.Set("Content-Type", "application/json")
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}

	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("failed to POST secret %s/%s: %w", namespace, sec.Name, err)
	}
	defer closeAndLog(resp.Body, "secret create response body")

	if resp.StatusCode == http.StatusCreated || resp.StatusCode == http.StatusOK || resp.StatusCode == http.StatusConflict {
		return nil
	}

	body, readErr := io.ReadAll(resp.Body)
	if readErr != nil {
		return fmt.Errorf("failed to create secret %s/%s: HTTP %d (failed to read body: %w)", namespace, sec.Name, resp.StatusCode, readErr)
	}
	return fmt.Errorf("failed to create secret %s/%s: HTTP %d: %s", namespace, sec.Name, resp.StatusCode, strings.TrimSpace(string(body)))
}
