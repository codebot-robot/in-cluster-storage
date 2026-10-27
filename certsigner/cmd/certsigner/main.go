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
	"context"
	"flag"
	"log"
	"time"

	"github.com/gke-labs/in-cluster-storage/certsigner/pkg/certsigner"
)

func main() {
	cfg := certsigner.DefaultConfig()

	flag.StringVar(&cfg.Namespace, "namespace", cfg.Namespace, "Kubernetes namespace for TLS secrets")
	flag.StringVar(&cfg.ServiceName, "service-name", cfg.ServiceName, "Service name for controller server TLS SANs")
	flag.StringVar(&cfg.APIServerURL, "apiserver-url", cfg.APIServerURL, "Kubernetes API server base URL")
	flag.Parse()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	log.Printf("Starting certsigner for namespace %s, service %s", cfg.Namespace, cfg.ServiceName)
	if err := certsigner.EnsureSecrets(ctx, cfg); err != nil {
		log.Fatalf("certsigner failed: %v", err)
	}
	log.Println("certsigner completed successfully")
}
