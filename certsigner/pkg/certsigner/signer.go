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
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"log"
	"math/big"
	"net"
	"net/url"
	"os"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

// GeneratedSecret holds PEM-encoded TLS material destined for a Kubernetes Secret.
type GeneratedSecret struct {
	Name string
	Data map[string]string
}

// Config specifies the configuration for issuing per-directory TLS Secrets.
type Config struct {
	Namespace        string
	ServiceName      string
	ControllerSecret string
	NodeDaemonSecret string
	AdminSecret      string
}

// DefaultConfig returns Config populated from standard in-cluster environment variables and paths.
func DefaultConfig() Config {
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
		Namespace:        ns,
		ServiceName:      "objectfs-controller",
		ControllerSecret: "objectfs-controller-server",
		NodeDaemonSecret: "objectfs-node-daemon-client",
		AdminSecret:      "objectfs-admin-client",
	}
}

// GenerateCertificates generates an in-memory self-signed CA (10-year validity, key never stored)
// and creates leaf certificates (1-year validity) for the controller server and node/admin clients.
// If previousCA is provided, the CA trust bundle (ca.crt) contains the current CA followed by
// the previous CA (keeping two; dropping older); if previousCA is empty, ca.crt contains only
// the current CA.
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

	var caBundle bytes.Buffer
	caBundle.Write(caPem)

	if len(previousCA) > 0 {
		block, _ := pem.Decode(previousCA)
		if block != nil && block.Type == "CERTIFICATE" {
			if !bytes.HasSuffix(caBundle.Bytes(), []byte("\n")) {
				caBundle.WriteByte('\n')
			}
			caBundle.Write(pem.EncodeToMemory(block))
		}
	}

	return []GeneratedSecret{
		{
			Name: cfg.ControllerSecret,
			Data: map[string]string{
				"tls.key": string(serverKeyPem),
				"tls.crt": string(serverPem),
				"ca.crt":  caBundle.String(),
			},
		},
		{
			Name: cfg.NodeDaemonSecret,
			Data: map[string]string{
				"tls.key": string(nodeKeyPem),
				"tls.crt": string(nodePem),
				"ca.crt":  caBundle.String(),
			},
		},
		{
			Name: cfg.AdminSecret,
			Data: map[string]string{
				"tls.key": string(adminKeyPem),
				"tls.crt": string(adminPem),
				"ca.crt":  caBundle.String(),
			},
		},
	}, nil
}

// EnsureSecrets checks whether the required TLS Secrets exist in the configured namespace,
// creating them if absent or rotating them in place if present while carrying forward previous CA
// certificates in the trust bundle.
func EnsureSecrets(ctx context.Context, client kubernetes.Interface, cfg Config) error {
	if client == nil {
		restConfig, err := rest.InClusterConfig()
		if err != nil {
			return fmt.Errorf("failed to build in-cluster config: %w", err)
		}
		client, err = kubernetes.NewForConfig(restConfig)
		if err != nil {
			return fmt.Errorf("failed to create kubernetes client: %w", err)
		}
	}

	secretNames := []string{cfg.ControllerSecret, cfg.NodeDaemonSecret, cfg.AdminSecret}

	var previousCA []byte
	existingSecrets := make(map[string]*corev1.Secret)

	for _, name := range secretNames {
		sec, err := client.CoreV1().Secrets(cfg.Namespace).Get(ctx, name, metav1.GetOptions{})
		if err != nil {
			if !apierrors.IsNotFound(err) {
				return fmt.Errorf("failed to get secret %s/%s: %w", cfg.Namespace, name, err)
			}
			continue
		}
		existingSecrets[name] = sec
		if len(previousCA) == 0 && len(sec.Data["ca.crt"]) > 0 {
			previousCA = sec.Data["ca.crt"]
		}
	}

	secrets, err := GenerateCertificates(cfg, previousCA)
	if err != nil {
		return err
	}

	for _, sec := range secrets {
		dataBytes := make(map[string][]byte, len(sec.Data))
		for k, v := range sec.Data {
			dataBytes[k] = []byte(v)
		}

		if existing, ok := existingSecrets[sec.Name]; ok {
			existing.Data = dataBytes
			existing.StringData = nil
			if _, err := client.CoreV1().Secrets(cfg.Namespace).Update(ctx, existing, metav1.UpdateOptions{}); err != nil {
				return fmt.Errorf("failed to update secret %s/%s: %w", cfg.Namespace, sec.Name, err)
			}
			log.Printf("Successfully updated TLS Secret %s/%s (carried forward previous CA in trust bundle)", cfg.Namespace, sec.Name)
		} else {
			secretObj := &corev1.Secret{
				ObjectMeta: metav1.ObjectMeta{
					Name:      sec.Name,
					Namespace: cfg.Namespace,
				},
				Type: corev1.SecretTypeTLS,
				Data: dataBytes,
			}
			if _, err := client.CoreV1().Secrets(cfg.Namespace).Create(ctx, secretObj, metav1.CreateOptions{}); err != nil {
				if !apierrors.IsAlreadyExists(err) {
					return fmt.Errorf("failed to create secret %s/%s: %w", cfg.Namespace, sec.Name, err)
				}
			}
			log.Printf("Successfully created TLS Secret %s/%s", cfg.Namespace, sec.Name)
		}
	}

	return nil
}
