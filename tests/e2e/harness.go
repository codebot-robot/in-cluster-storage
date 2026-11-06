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
	"encoding/json"
	"fmt"
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

// CreateTestNamespace creates a uniquely named test namespace, tracks it for artifact collection,
// sets h.Namespace, and registers a t.Cleanup callback that dumps diagnostics on failure and
// deletes the namespace when the test completes.
func (h *Harness) CreateTestNamespace(prefix string) string {
	h.t.Helper()
	ns := fmt.Sprintf("%s-%d", prefix, time.Now().UnixNano())
	if len(ns) > 63 {
		ns = ns[:63]
	}
	h.RunCommand("kubectl", "create", "namespace", ns)
	h.TrackNamespace(ns)
	h.Namespace = ns

	h.t.Cleanup(func() {
		if h.t.Failed() {
			h.DumpDiagnosticLogs(h.t)
			h.CollectArtifacts(h.t.Name())
		}
		// Delete test namespace during teardown
		if err := exec.Command("kubectl", "delete", "namespace", ns, "--ignore-not-found").Run(); err != nil {
			h.t.Logf("Warning: failed to delete test namespace %s: %v", ns, err)
		}
	})
	return ns
}

// SetupMultiNode sets up a Kind cluster with a control plane and the requested number of worker nodes.
func (h *Harness) SetupMultiNode(workerNodes int) {
	if workerNodes <= 0 {
		h.Setup()
		return
	}

	out, err := exec.Command("kind", "get", "clusters").Output()
	if err != nil {
		h.t.Fatalf("failed to list kind clusters: %v", err)
	}
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
		if err := os.WriteFile(configFile, []byte(b.String()), 0644); err != nil {
			h.t.Fatalf("failed to write kind config %s: %v", configFile, err)
		}
		cmd := exec.Command("kind", "create", "cluster", "--name", h.ClusterName, "--config", configFile)
		if out, err := cmd.CombinedOutput(); err != nil {
			h.t.Fatalf("failed to create kind cluster %s: %v (output: %s)", h.ClusterName, err, strings.TrimSpace(string(out)))
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
		if previous {
			return ""
		}
		return fmt.Sprintf("failed to get logs: %v\n%s", err, strings.TrimSpace(string(out)))
	}
	return strings.TrimSpace(string(out))
}

func (h *Harness) DumpDiagnosticLogs(t *testing.T) {
	t.Log("======= DUMPING DIAGNOSTIC LOGS =======")

	// Dump test namespace if non-default
	if h.Namespace != "" && h.Namespace != "default" {
		if out, err := exec.Command("kubectl", "get", "pods", "-n", h.Namespace, "-o", "wide").CombinedOutput(); err != nil {
			t.Logf("Warning: failed to get pods in %s namespace: %v (out: %s)", h.Namespace, err, strings.TrimSpace(string(out)))
		} else {
			t.Logf("Pods in %s namespace:\n%s\n", h.Namespace, strings.TrimSpace(string(out)))
		}
		t.Logf("Events in %s:\n%s\n", h.Namespace, h.GetEvents(h.Namespace))

		testPods := h.getPodNames("", h.Namespace)
		for _, pod := range testPods {
			if desc, err := exec.Command("kubectl", "describe", "pod", pod, "-n", h.Namespace).CombinedOutput(); err != nil {
				t.Logf("Warning: failed to describe pod %s/%s: %v (out: %s)", h.Namespace, pod, err, strings.TrimSpace(string(desc)))
			} else {
				t.Logf("Pod %s/%s Description:\n%s\n", h.Namespace, pod, strings.TrimSpace(string(desc)))
			}
			if logs := h.getContainerLogs(pod, "", h.Namespace, false); logs != "" {
				t.Logf("Pod %s/%s Logs:\n%s\n", h.Namespace, pod, logs)
			}
		}
	}

	// Pod overview
	if out, err := exec.Command("kubectl", "get", "pods", "-n", "default", "-o", "wide").CombinedOutput(); err != nil {
		t.Logf("Warning: failed to get pods in default namespace: %v (out: %s)", err, strings.TrimSpace(string(out)))
	} else {
		t.Logf("Pods in default namespace:\n%s\n", strings.TrimSpace(string(out)))
	}

	// Events
	t.Logf("Events:\n%s\n", h.GetEvents("default"))

	// Controller Logs (current & previous)
	ctrlPods := h.getPodNames("app=objectfs-controller", "default")
	if len(ctrlPods) == 0 {
		ctrlPods = []string{"objectfs-controller-0"}
	}
	for _, pod := range ctrlPods {
		for _, container := range []string{"objectfs-controller", "csi-provisioner"} {
			if logs := h.getContainerLogs(pod, container, "default", false); logs != "" {
				t.Logf("ObjectFS Controller (%s / %s) Logs:\n%s\n", pod, container, logs)
			}
			if prevLogs := h.getContainerLogs(pod, container, "default", true); prevLogs != "" {
				t.Logf("ObjectFS Controller (%s / %s) Previous Logs:\n%s\n", pod, container, prevLogs)
			}
		}
	}
	if logs := h.getContainerLogs("wal-buffer-0", "wal-buffer", "default", false); logs != "" {
		t.Logf("WAL Buffer Logs:\n%s\n", logs)
	}
	if prevLogs := h.getContainerLogs("wal-buffer-0", "wal-buffer", "default", true); prevLogs != "" {
		t.Logf("WAL Buffer Previous Logs:\n%s\n", prevLogs)
	}

	// PersistentVolumes overview
	out, err := exec.Command("kubectl", "get", "pv").CombinedOutput()
	if err != nil {
		t.Logf("Warning: failed to get persistentvolumes: %v (out: %s)", err, strings.TrimSpace(string(out)))
	} else {
		t.Logf("PersistentVolumes:\n%s\n", strings.TrimSpace(string(out)))
	}

	// Node Daemon Logs (current & previous, both node-daemon & node-driver-registrar containers)
	daemonPods := h.getPodNames("", "default")
	for _, pod := range daemonPods {
		if strings.Contains(pod, "node-daemon") || strings.Contains(pod, "objectfs") {
			for _, container := range []string{"objectfs-node-daemon", "node-driver-registrar", "cas-node-daemon"} {
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
			if desc, err := exec.Command("kubectl", "describe", "pod", pod, "-n", "default").CombinedOutput(); err != nil {
				t.Logf("Warning: failed to describe pod %s: %v (out: %s)", pod, err, strings.TrimSpace(string(desc)))
			} else {
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

// GetPVForPVC returns the volume name (PV name) bound to the given PVC.
func (h *Harness) GetPVForPVC(pvcName, namespace string) (string, error) {
	h.t.Helper()
	out, err := exec.Command("kubectl", "get", "pvc", pvcName, "-n", namespace, "-o", "jsonpath={.spec.volumeName}").CombinedOutput()
	if err != nil {
		return "", fmt.Errorf("failed to get PV for PVC %s/%s: %w (output: %s)", namespace, pvcName, err, string(out))
	}
	pvName := strings.TrimSpace(string(out))
	if pvName == "" {
		return "", fmt.Errorf("PV for PVC %s/%s is empty", namespace, pvcName)
	}
	return pvName, nil
}

// DeletePVC deletes a PersistentVolumeClaim by name in the specified namespace.
func (h *Harness) DeletePVC(name, namespace string) {
	h.t.Helper()
	if err := exec.Command("kubectl", "delete", "pvc", name, "-n", namespace, "--ignore-not-found").Run(); err != nil {
		h.t.Logf("Warning: failed to delete pvc %s in namespace %s: %v", name, namespace, err)
	}
}

// WaitForPVDeleted waits until a PersistentVolume is completely deleted from the cluster.
func (h *Harness) WaitForPVDeleted(name string, timeout time.Duration) error {
	h.t.Helper()
	h.t.Logf("Waiting for persistentvolume %s to be deleted", name)
	start := time.Now()
	for {
		if time.Since(start) > timeout {
			desc, err := exec.Command("kubectl", "get", "persistentvolume", name, "-o", "yaml").CombinedOutput()
			if err != nil {
				h.t.Logf("Warning: failed to get persistentvolume %s yaml: %v (out: %s)", name, err, strings.TrimSpace(string(desc)))
			} else {
				h.t.Logf("Stuck PersistentVolume %s:\n%s\n", name, strings.TrimSpace(string(desc)))
			}
			return fmt.Errorf("timed out waiting for persistentvolume %s to be deleted after %s", name, timeout)
		}
		cmd := exec.Command("kubectl", "get", "persistentvolume", name, "--ignore-not-found", "-o", "jsonpath={.metadata.name}")
		out, err := cmd.CombinedOutput()
		outputStr := strings.TrimSpace(string(out))
		if err != nil {
			if strings.Contains(outputStr, "NotFound") || strings.Contains(outputStr, "not found") {
				h.t.Logf("PersistentVolume %s successfully deleted", name)
				return nil
			}
			h.t.Logf("Warning: error querying persistentvolume %s: %v (output: %s)", name, err, outputStr)
		} else if outputStr == "" {
			h.t.Logf("PersistentVolume %s successfully deleted", name)
			return nil
		}
		time.Sleep(2 * time.Second)
	}
}

func (h *Harness) kubectlOutput(description string, args ...string) (string, bool) {
	h.t.Helper()
	out, err := exec.Command("kubectl", args...).CombinedOutput()
	output := strings.TrimSpace(string(out))
	if err != nil {
		h.t.Logf("Warning: failed to %s: %v (output: %s)", description, err, output)
		return output, false
	}
	return output, true
}

// WaitForPodReady waits until a pod is ready by delegating to the embedded harness wait.
// On timeout, it logs pod description, PVC and PV state, and node daemon log tail for objectfs volumes.
func (h *Harness) WaitForPodReady(name, namespace string, timeout time.Duration) error {
	h.t.Helper()
	waitErr := h.Harness.WaitForPodReady(name, namespace, timeout)
	if waitErr == nil {
		return nil
	}

	if describe, ok := h.kubectlOutput(fmt.Sprintf("describe pod %s", name), "describe", "pod", name, "-n", namespace); ok {
		h.t.Logf("Pod %s description:\n%s\n", name, describe)
	}

	storage, ok := h.kubectlOutput(fmt.Sprintf("get persistent volume claims and persistent volumes in %s", namespace), "get", "pvc,pv", "-n", namespace, "-o", "wide")
	if ok {
		h.t.Logf("Persistent volume claims and persistent volumes in %s:\n%s\n", namespace, storage)
	}

	driver, _ := h.kubectlOutput(fmt.Sprintf("get volume driver for pod %s", name), "get", "pod", name, "-n", namespace, "-o", "jsonpath={.spec.volumes[*].csi.driver}")
	isObjectFSVolume := strings.Contains(driver, "objectfs")
	if !isObjectFSVolume {
		claim, _ := h.kubectlOutput(fmt.Sprintf("get persistent volume claims for pod %s", name), "get", "pod", name, "-n", namespace, "-o", "jsonpath={.spec.volumes[*].persistentVolumeClaim.claimName}")
		if claim != "" && strings.Contains(storage, "objectfs") {
			isObjectFSVolume = true
		}
	}

	if isObjectFSVolume {
		nodeName, ok := h.kubectlOutput(fmt.Sprintf("get node name for pod %s", name), "get", "pod", name, "-n", namespace, "-o", "jsonpath={.spec.nodeName}")
		if ok && nodeName != "" {
			daemonPods, ok := h.kubectlOutput(fmt.Sprintf("find node daemon pod for node %s", nodeName), "get", "pods", "-n", "default", "-l", "app=objectfs-node-daemon", "--field-selector", "spec.nodeName="+nodeName, "-o", "jsonpath={.items[*].metadata.name}")
			daemonPodNames := strings.Fields(daemonPods)
			if ok && len(daemonPodNames) > 0 {
				daemonLogs := h.getContainerLogs(daemonPodNames[0], "objectfs-node-daemon", "default", false)
				if daemonLogs != "" {
					lines := strings.Split(daemonLogs, "\n")
					if len(lines) > 50 {
						lines = lines[len(lines)-50:]
					}
					h.t.Logf("Node Daemon %s on %s tail logs:\n%s\n", daemonPodNames[0], nodeName, strings.Join(lines, "\n"))
				}
			}
		}
	}

	if phase, ok := h.kubectlOutput(fmt.Sprintf("get pod %s phase on timeout", name), "get", "pod", name, "-n", namespace, "-o", "jsonpath={.status.phase}"); ok && phase == "Pending" {
		if eventsOutput, ok := h.kubectlOutput(fmt.Sprintf("get events for pod %s", name), "get", "events", "-n", namespace, "--field-selector", "involvedObject.name="+name, "-o", "json"); ok {
			var eventsData struct {
				Items []struct {
					Reason  string `json:"reason"`
					Message string `json:"message"`
				} `json:"items"`
			}
			if err := json.Unmarshal([]byte(eventsOutput), &eventsData); err != nil {
				h.t.Logf("Warning: failed to parse events for pod %s: %v", name, err)
			} else {
				for _, item := range eventsData.Items {
					if item.Reason == "FailedScheduling" || item.Reason == "FailedMount" {
						return fmt.Errorf("%w: %s", waitErr, strings.TrimSpace(item.Message))
					}
				}
			}
		}
	}

	return waitErr
}

// resolveApCommand returns the command name and initial arguments to execute ap.
// It prefers an "ap" executable already found on PATH, falling back to
// "go run github.com/gke-labs/gke-labs-infra/ap@latest" as used by presubmits.
func resolveApCommand() (string, []string) {
	apPath, err := exec.LookPath("ap")
	if err != nil {
		return "go", []string{"run", "github.com/gke-labs/gke-labs-infra/ap@latest"}
	}
	return apPath, nil
}

// ApDeploy writes the provided manifest contents into a scratch directory inside
// the repository checkout (under a k8s/ path segment), invokes "ap deploy" to build,
// kind-load, rewrite, and apply the manifests, and removes the scratch directory
// during test cleanup.
func (h *Harness) ApDeploy(t *testing.T, manifests map[string]string) {
	t.Helper()

	repositoryRoot := h.GetGitRoot()
	scratchDirectory := filepath.Join(repositoryRoot, "tests", "e2e", "deploy-scratch", h.ClusterName)
	k8sDirectory := filepath.Join(scratchDirectory, "k8s")

	t.Cleanup(func() {
		if err := os.RemoveAll(scratchDirectory); err != nil {
			t.Errorf("failed to remove scratch directory %s: %v", scratchDirectory, err)
		}
	})

	if err := os.RemoveAll(scratchDirectory); err != nil {
		t.Errorf("failed to clean up existing scratch directory %s: %v", scratchDirectory, err)
	}
	if err := os.MkdirAll(k8sDirectory, 0755); err != nil {
		t.Fatalf("failed to create scratch directory %s: %v", k8sDirectory, err)
	}

	for name, content := range manifests {
		manifestFileName := name
		if !strings.HasSuffix(manifestFileName, ".yaml") && !strings.HasSuffix(manifestFileName, ".yml") {
			manifestFileName += ".yaml"
		}
		manifestPath := filepath.Join(k8sDirectory, manifestFileName)
		if err := os.WriteFile(manifestPath, []byte(content), 0644); err != nil {
			t.Fatalf("failed to write manifest %s: %v", manifestPath, err)
		}
	}

	commandName, initialArguments := resolveApCommand()
	arguments := append([]string{}, initialArguments...)

	namespace := h.Namespace
	if namespace == "" {
		namespace = "default"
	}

	arguments = append(arguments, "deploy", "--skip-push", "--cluster", fmt.Sprintf("kind://%s", h.ClusterName), "--namespace", namespace, k8sDirectory)

	command := exec.Command(commandName, arguments...)
	command.Dir = repositoryRoot
	command.Env = append(os.Environ(), "IMAGE_TAG=e2e", "IMAGE_PREFIX=images.local")
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("ap deploy failed: %v\nOutput: %s", err, string(output))
	}
}

// WaitForProvisionerReady verifies that the CSI provisioner is functional by creating
// a small probe PVC, waiting until it binds, and deleting it.
func (h *Harness) WaitForProvisionerReady(timeout time.Duration) error {
	h.t.Helper()
	h.t.Logf("Waiting up to %v for objectfs provisioner to be ready", timeout)
	startTime := time.Now()

	readyCommand := exec.Command("kubectl", "wait", "--for=condition=Ready", "pod/objectfs-controller-0", "-n", "default", fmt.Sprintf("--timeout=%s", timeout))
	if readyOutput, readyError := readyCommand.CombinedOutput(); readyError != nil {
		return fmt.Errorf("controller pod not ready: %w (output: %s)", readyError, strings.TrimSpace(string(readyOutput)))
	}

	probeClaimName := fmt.Sprintf("provisioner-probe-%d", time.Now().UnixNano())
	probeClaimYaml := fmt.Sprintf(`apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: %s
  namespace: default
spec:
  accessModes:
    - ReadWriteOnce
  storageClassName: objectfs-standard
  resources:
    requests:
      storage: 1Mi
`, probeClaimName)

	applyCommand := exec.Command("kubectl", "apply", "-f", "-")
	applyCommand.Stdin = strings.NewReader(probeClaimYaml)
	if applyOutput, applyError := applyCommand.CombinedOutput(); applyError != nil {
		return fmt.Errorf("failed to apply probe PVC %s: %w (output: %s)", probeClaimName, applyError, strings.TrimSpace(string(applyOutput)))
	}

	var lastPhase string
	for {
		if time.Since(startTime) > timeout {
			var eventMessages []string
			eventsCommand := exec.Command("kubectl", "get", "events", "-n", "default", "--field-selector", "involvedObject.name="+probeClaimName, "-o", "json")
			eventsOutput, eventsError := eventsCommand.CombinedOutput()
			if eventsError != nil {
				h.t.Logf("Warning: failed to get events for probe PVC %s: %v", probeClaimName, eventsError)
			} else {
				var eventsData struct {
					Items []struct {
						Reason  string `json:"reason"`
						Message string `json:"message"`
					} `json:"items"`
				}
				if jsonError := json.Unmarshal(eventsOutput, &eventsData); jsonError != nil {
					h.t.Logf("Warning: failed to parse events for probe PVC %s: %v", probeClaimName, jsonError)
				} else {
					for _, item := range eventsData.Items {
						eventMessages = append(eventMessages, fmt.Sprintf("%s: %s", item.Reason, strings.TrimSpace(item.Message)))
					}
				}
			}

			cleanupCommand := exec.Command("kubectl", "delete", "pvc", probeClaimName, "-n", "default", "--ignore-not-found")
			if cleanupOutput, cleanupError := cleanupCommand.CombinedOutput(); cleanupError != nil {
				h.t.Logf("Warning: failed to clean up probe PVC %s: %v (output: %s)", probeClaimName, cleanupError, strings.TrimSpace(string(cleanupOutput)))
			}

			if len(eventMessages) > 0 {
				return fmt.Errorf("timed out waiting %v for probe PVC %s to bind (last phase: %s): events: %s", timeout, probeClaimName, lastPhase, strings.Join(eventMessages, "; "))
			}
			return fmt.Errorf("timed out waiting %v for probe PVC %s to bind (last phase: %s)", timeout, probeClaimName, lastPhase)
		}

		phaseCommand := exec.Command("kubectl", "get", "pvc", probeClaimName, "-n", "default", "-o", "jsonpath={.status.phase}")
		phaseOutput, phaseError := phaseCommand.CombinedOutput()
		if phaseError != nil {
			h.t.Logf("Warning: failed to get phase for probe PVC %s: %v", probeClaimName, phaseError)
		} else {
			lastPhase = strings.TrimSpace(string(phaseOutput))
			if lastPhase == "Bound" {
				break
			}
		}
		time.Sleep(200 * time.Millisecond)
	}

	bindDuration := time.Since(startTime)

	deleteCommand := exec.Command("kubectl", "delete", "pvc", probeClaimName, "-n", "default", "--ignore-not-found")
	if deleteOutput, deleteError := deleteCommand.CombinedOutput(); deleteError != nil {
		return fmt.Errorf("failed to delete probe PVC %s: %w (output: %s)", probeClaimName, deleteError, strings.TrimSpace(string(deleteOutput)))
	}

	for {
		if time.Since(startTime) > timeout {
			return fmt.Errorf("timed out waiting for probe PVC %s to be deleted after %v", probeClaimName, timeout)
		}
		checkCommand := exec.Command("kubectl", "get", "pvc", probeClaimName, "-n", "default", "--ignore-not-found", "-o", "jsonpath={.metadata.name}")
		checkOutput, checkError := checkCommand.CombinedOutput()
		if checkError != nil {
			h.t.Logf("Warning: failed to query probe PVC %s deletion: %v", probeClaimName, checkError)
		} else if strings.TrimSpace(string(checkOutput)) == "" {
			break
		}
		time.Sleep(200 * time.Millisecond)
	}

	h.t.Logf("WaitForProvisionerReady: probe PVC %s bound in %v (total wait %v)", probeClaimName, bindDuration.Round(time.Millisecond), time.Since(startTime).Round(time.Millisecond))
	return nil
}
