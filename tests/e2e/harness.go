// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package e2e

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/gke-labs/gke-labs-infra/ktesting/e2e"
)

type Harness struct {
	*e2e.Harness
	t *testing.T
}

func NewHarness(t *testing.T, clusterName string) *Harness {
	return &Harness{
		Harness: e2e.NewHarness(t, clusterName),
		t:       t,
	}
}

// SetupMultiNode sets up a Kind cluster with a control plane and the requested number of worker nodes.
func (h *Harness) SetupMultiNode(workerNodes int) {
	if workerNodes <= 0 {
		h.Setup()
		return
	}

	out, _ := exec.Command("kind", "get", "clusters").Output()
	clusters := strings.Fields(string(out))
	exists := false
	for _, c := range clusters {
		if c == h.ClusterName {
			exists = true
			break
		}
	}

	if !exists {
		var b strings.Builder
		b.WriteString("kind: Cluster\napiVersion: kind.x-k8s.io/v1alpha4\nnodes:\n- role: control-plane\n")
		for i := 0; i < workerNodes; i++ {
			b.WriteString("- role: worker\n")
		}
		configFile := filepath.Join(h.t.TempDir(), "kind-config.yaml")
		if err := os.WriteFile(configFile, []byte(b.String()), 0644); err == nil {
			cmd := exec.Command("kind", "create", "cluster", "--name", h.ClusterName, "--config", configFile)
			_ = cmd.Run()
		}
	}

	h.Setup()
}

// GetWorkerNodeNames returns the names of all worker Kubernetes nodes in the cluster.
func (h *Harness) GetWorkerNodeNames() []string {
	out, err := exec.Command("kubectl", "get", "nodes", "-o", "jsonpath={.items[*].metadata.name}").Output()
	if err != nil {
		return nil
	}
	var workers []string
	for _, node := range strings.Fields(string(out)) {
		if !strings.Contains(node, "control-plane") {
			workers = append(workers, node)
		}
	}
	return workers
}

// GetNodeNames returns the names of all Kubernetes nodes in the cluster.
func (h *Harness) GetNodeNames() []string {
	out, err := exec.Command("kubectl", "get", "nodes", "-o", "jsonpath={.items[*].metadata.name}").Output()
	if err != nil {
		return nil
	}
	return strings.Fields(string(out))
}

func (h *Harness) getPodNames(selector, namespace string) []string {
	args := []string{"get", "pods", "-n", namespace}
	if selector != "" {
		args = append(args, "-l", selector)
	}
	args = append(args, "-o", "jsonpath={.items[*].metadata.name}")
	out, err := exec.Command("kubectl", args...).Output()
	if err != nil {
		return nil
	}
	return strings.Fields(string(out))
}

func (h *Harness) getContainerLogs(pod, container, namespace string, previous bool) string {
	args := []string{"logs", pod, "-n", namespace}
	if container != "" {
		args = append(args, "-c", container)
	}
	if previous {
		args = append(args, "--previous")
	}
	out, err := exec.Command("kubectl", args...).CombinedOutput()
	if err != nil {
		return ""
	}
	return strings.TrimSpace(string(out))
}

func (h *Harness) DumpDiagnosticLogs(t *testing.T) {
	t.Log("======= DUMPING DIAGNOSTIC LOGS =======")

	// Pod overview
	if out, err := exec.Command("kubectl", "get", "pods", "-n", "default", "-o", "wide").CombinedOutput(); err == nil {
		t.Logf("Pods in default namespace:\n%s\n", strings.TrimSpace(string(out)))
	}

	// Events
	t.Logf("Events:\n%s\n", h.GetEvents("default"))

	// Controller Logs (current & previous)
	if logs := h.getContainerLogs("agentfs-controller-0", "agentfs-controller", "default", false); logs != "" {
		t.Logf("AgentFS Controller Logs:\n%s\n", logs)
	}
	if prevLogs := h.getContainerLogs("agentfs-controller-0", "agentfs-controller", "default", true); prevLogs != "" {
		t.Logf("AgentFS Controller Previous Logs:\n%s\n", prevLogs)
	}
	if logs := h.getContainerLogs("objectfs-controller-0", "objectfs-controller", "default", false); logs != "" {
		t.Logf("ObjectFS Controller Logs:\n%s\n", logs)
	}
	if prevLogs := h.getContainerLogs("objectfs-controller-0", "objectfs-controller", "default", true); prevLogs != "" {
		t.Logf("ObjectFS Controller Previous Logs:\n%s\n", prevLogs)
	}
	if logs := h.getContainerLogs("wal-buffer-0", "wal-buffer", "default", false); logs != "" {
		t.Logf("WAL Buffer Logs:\n%s\n", logs)
	}
	if prevLogs := h.getContainerLogs("wal-buffer-0", "wal-buffer", "default", true); prevLogs != "" {
		t.Logf("WAL Buffer Previous Logs:\n%s\n", prevLogs)
	}

	// Node Daemon Logs (current & previous, both node-daemon & node-driver-registrar containers)
	daemonPods := h.getPodNames("", "default")
	for _, pod := range daemonPods {
		if strings.Contains(pod, "node-daemon") || strings.Contains(pod, "agentfs") || strings.Contains(pod, "objectfs") {
			for _, container := range []string{"agentfs-node-daemon", "objectfs-node-daemon", "node-driver-registrar", "cas-node-daemon"} {
				if logs := h.getContainerLogs(pod, container, "default", false); logs != "" {
					t.Logf("Node Daemon (%s / %s) Logs:\n%s\n", pod, container, logs)
				}
				if prev := h.getContainerLogs(pod, container, "default", true); prev != "" {
					t.Logf("Node Daemon (%s / %s) Previous Logs:\n%s\n", pod, container, prev)
				}
			}
		}
	}

	// Any test pods: describe & logs
	allPods := h.getPodNames("", "default")
	for _, pod := range allPods {
		if strings.HasPrefix(pod, "layers-pod-") || strings.HasPrefix(pod, "test-pod-") || strings.HasPrefix(pod, "dev-vscode") || strings.HasPrefix(pod, "persistent-pod-") || strings.HasPrefix(pod, "failover-") || strings.HasPrefix(pod, "crash-") || strings.HasPrefix(pod, "outage-") || strings.HasPrefix(pod, "wal-") {
			if desc, err := exec.Command("kubectl", "describe", "pod", pod, "-n", "default").CombinedOutput(); err == nil {
				t.Logf("Pod %s Description:\n%s\n", pod, strings.TrimSpace(string(desc)))
			}
			if logs := h.getContainerLogs(pod, "", "default", false); logs != "" {
				t.Logf("Pod %s Logs:\n%s\n", pod, logs)
			}
		}
	}

	t.Log("=========================================")
}

func (h *Harness) DeletePodWithTimeout(t *testing.T, name, namespace string, timeout time.Duration) {
	t.Logf("Deleting Pod %s in namespace %s with a timeout of %v", name, namespace, timeout)

	done := make(chan struct{})
	go func() {
		h.DeletePod(name, namespace)
		close(done)
	}()

	select {
	case <-done:
		t.Logf("Successfully deleted Pod %s", name)
	case <-time.After(timeout):
		t.Errorf("TIMED OUT waiting for Pod %s to be deleted (timeout: %v)", name, timeout)
		h.DumpDiagnosticLogs(t)
		t.FailNow()
	}
}
