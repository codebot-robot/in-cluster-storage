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

package e2e

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestObjectFSE2E(t *testing.T) {
	if os.Getenv("RUN_E2E") == "" {
		t.Skip("Skipping ObjectFS E2E test; RUN_E2E not set")
	}

	h := NewHarness(t, "objectfs-e2e")
	h.Setup()

	gitRoot := h.GetGitRoot()
	experimentRoot := gitRoot

	// Build docker images
	h.DockerBuild("objectfs-controller:e2e", filepath.Join(experimentRoot, "images/objectfs-controller/Dockerfile"), experimentRoot)
	h.DockerBuild("objectfs-node-daemon:e2e", filepath.Join(experimentRoot, "images/objectfs-node-daemon/Dockerfile"), experimentRoot)

	// Load images into Kind
	h.KindLoad("objectfs-controller:e2e")
	h.KindLoad("objectfs-node-daemon:e2e")

	// Read and adapt manifest
	manifestPath := filepath.Join(experimentRoot, "k8s/objectfs.yaml")
	b, err := os.ReadFile(manifestPath)
	if err != nil {
		t.Fatalf("Failed to read manifest: %v", err)
	}
	manifest := string(b)
	manifest = strings.ReplaceAll(manifest, "namespace: kube-objectfs-system", "namespace: default")
	manifest = strings.ReplaceAll(manifest, "image: objectfs-controller:latest", "image: objectfs-controller:e2e\n          imagePullPolicy: Never")
	manifest = strings.ReplaceAll(manifest, "image: objectfs-node-daemon:latest", "image: objectfs-node-daemon:e2e\n          imagePullPolicy: Never")

	// Apply manifest
	h.KubectlApplyContent("objectfs", manifest)

	// Wait for controller
	if err := h.WaitForStatefulSet("objectfs-controller", "default", 2*time.Minute); err != nil {
		t.Logf("Events:\n%s\n", h.GetEvents("default"))
		t.Fatalf("ObjectFS Controller failed to start: %v", err)
	}

	// Wait for node-daemon
	if err := h.WaitForDaemonSet("objectfs-node-daemon", "default", 2*time.Minute); err != nil {
		t.Logf("Events:\n%s\n", h.GetEvents("default"))
		t.Fatalf("ObjectFS Node Daemon failed to start: %v", err)
	}

	volumeID := "objectfs-test-vol"

	// Step 1: Run Pod 1 that writes a file
	pod1Name := "objectfs-pod-1"
	pod1Yaml := fmt.Sprintf(`
apiVersion: v1
kind: Pod
metadata:
  name: %s
spec:
  restartPolicy: Never
  containers:
    - name: app
      image: alpine
      command: ["/bin/sh", "-c", "echo 'hello objectfs' > /data/hello.txt && sync && sleep 10"]
      volumeMounts:
        - name: data
          mountPath: /data
  volumes:
    - name: data
      csi:
        driver: objectfs.labs.gke.io
        volumeAttributes:
          volumeID: %s
`, pod1Name, volumeID)

	t.Logf("Creating Pod 1")
	h.KubectlApplyContent(pod1Name, pod1Yaml)
	if err := h.WaitForPodReady(pod1Name, "default", 1*time.Minute); err != nil {
		t.Logf("Events:\n%s\n", h.GetEvents("default"))
		t.Logf("Node Daemon Logs:\n%s\n", h.GetPodLogs("app=objectfs-node-daemon", "default"))
		t.Fatalf("Test Pod 1 failed to start: %v", err)
	}

	time.Sleep(5 * time.Second)
	t.Logf("Deleting Pod 1")
	h.DeletePod(pod1Name, "default")

	// Step 2: Start Pod 2, read the file, verify content, create subdir and second file
	pod2Name := "objectfs-pod-2"
	pod2Yaml := fmt.Sprintf(`
apiVersion: v1
kind: Pod
metadata:
  name: %s
spec:
  restartPolicy: Never
  containers:
    - name: app
      image: alpine
      command: ["/bin/sh", "-c", "cat /data/hello.txt && mkdir /data/nested && echo 'nested data' > /data/nested/data.txt && echo 'updated objectfs' > /data/hello.txt && sync && sleep 10"]
      volumeMounts:
        - name: data
          mountPath: /data
  volumes:
    - name: data
      csi:
        driver: objectfs.labs.gke.io
        volumeAttributes:
          volumeID: %s
`, pod2Name, volumeID)

	t.Logf("Creating Pod 2")
	h.KubectlApplyContent(pod2Name, pod2Yaml)
	if err := h.WaitForPodReady(pod2Name, "default", 1*time.Minute); err != nil {
		t.Logf("Node Daemon Logs:\n%s\n", h.GetPodLogs("app=objectfs-node-daemon", "default"))
		t.Fatalf("Test Pod 2 failed to start: %v", err)
	}

	time.Sleep(5 * time.Second)
	logs2 := h.GetPodLogsByName(pod2Name, "default")
	if !strings.Contains(logs2, "hello objectfs") {
		t.Logf("Pod 2 Logs:\n%s\n", logs2)
		t.Fatalf("Pod 2 did not see 'hello objectfs'")
	}
	t.Logf("Pod 2 verified, deleting")
	h.DeletePod(pod2Name, "default")

	// Step 3: Start Pod 3 to verify updated file and nested directory
	pod3Name := "objectfs-pod-3"
	pod3Yaml := fmt.Sprintf(`
apiVersion: v1
kind: Pod
metadata:
  name: %s
spec:
  containers:
    - name: app
      image: alpine
      command: ["/bin/sh", "-c", "cat /data/hello.txt && cat /data/nested/data.txt && sleep 3600"]
      volumeMounts:
        - name: data
          mountPath: /data
  volumes:
    - name: data
      csi:
        driver: objectfs.labs.gke.io
        volumeAttributes:
          volumeID: %s
`, pod3Name, volumeID)

	t.Logf("Creating Pod 3")
	h.KubectlApplyContent(pod3Name, pod3Yaml)
	if err := h.WaitForPodReady(pod3Name, "default", 1*time.Minute); err != nil {
		t.Fatalf("Test Pod 3 failed to start: %v", err)
	}

	out1, err := h.RunInPod(pod3Name, "default", "cat", "/data/hello.txt")
	if err != nil {
		t.Fatalf("Failed to read hello.txt in Pod 3: %v", err)
	}
	if !strings.Contains(out1, "updated objectfs") {
		t.Fatalf("Expected hello.txt content 'updated objectfs', got: %s", out1)
	}

	out2, err := h.RunInPod(pod3Name, "default", "cat", "/data/nested/data.txt")
	if err != nil {
		t.Fatalf("Failed to read nested/data.txt in Pod 3: %v", err)
	}
	if !strings.Contains(out2, "nested data") {
		t.Fatalf("Expected nested/data.txt content 'nested data', got: %s", out2)
	}

	h.DeletePod(pod3Name, "default")
	t.Logf("Successfully verified ObjectFS multi-writer FUSE CSI driver!")
}
