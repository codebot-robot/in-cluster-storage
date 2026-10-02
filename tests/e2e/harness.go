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
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/gke-labs/gke-labs-infra/ktesting/e2e"
)

type Harness struct {
	*e2e.Harness
}

func NewHarness(t *testing.T, clusterName string) *Harness {
	return &Harness{
		Harness: e2e.NewHarness(t, clusterName),
	}
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
		return strings.TrimSpace(string(out))
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

	// Node Daemon Logs (current & previous, both agentfs-node-daemon & node-driver-registrar containers)
	daemonPods := h.getPodNames("app=agentfs-node-daemon", "default")
	if len(daemonPods) == 0 {
		daemonPods = h.getPodNames("", "default")
	}
	for _, pod := range daemonPods {
		if strings.Contains(pod, "node-daemon") || strings.Contains(pod, "agentfs") {
			if logs := h.getContainerLogs(pod, "agentfs-node-daemon", "default", false); logs != "" {
				t.Logf("AgentFS Node Daemon (%s / agentfs-node-daemon) Logs:\n%s\n", pod, logs)
			}
			if prev := h.getContainerLogs(pod, "agentfs-node-daemon", "default", true); prev != "" {
				t.Logf("AgentFS Node Daemon (%s / agentfs-node-daemon) Previous Logs:\n%s\n", pod, prev)
			}
			if regLogs := h.getContainerLogs(pod, "node-driver-registrar", "default", false); regLogs != "" {
				t.Logf("AgentFS Node Daemon (%s / node-driver-registrar) Logs:\n%s\n", pod, regLogs)
			}
		}
	}

	// Any test pods: describe & logs
	allPods := h.getPodNames("", "default")
	for _, pod := range allPods {
		if strings.HasPrefix(pod, "layers-pod-") || strings.HasPrefix(pod, "test-pod-") {
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
