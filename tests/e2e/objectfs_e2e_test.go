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
	t.Cleanup(func() {
		if t.Failed() {
			h.DumpDiagnosticLogs(t)
		}
	})

	gitRoot := h.GetGitRoot()
	experimentRoot := gitRoot

	// Build docker images
	h.DockerBuild("objectfs-controller:e2e", filepath.Join(experimentRoot, "images/objectfs-controller/Dockerfile"), experimentRoot)
	h.DockerBuild("objectfs-node-daemon:e2e", filepath.Join(experimentRoot, "images/objectfs-node-daemon/Dockerfile"), experimentRoot)
	h.DockerBuild("wal-buffer:e2e", filepath.Join(experimentRoot, "images/wal-buffer/Dockerfile"), experimentRoot)

	// Load images into Kind
	h.KindLoad("objectfs-controller:e2e")
	h.KindLoad("objectfs-node-daemon:e2e")
	h.KindLoad("wal-buffer:e2e")

	// Read and adapt manifests
	manifestPath := filepath.Join(experimentRoot, "k8s/objectfs.yaml")
	b, err := os.ReadFile(manifestPath)
	if err != nil {
		t.Fatalf("Failed to read manifest: %v", err)
	}
	manifest := string(b)
	manifest = strings.ReplaceAll(manifest, "namespace: kube-objectfs-system", "namespace: default")
	manifest = strings.ReplaceAll(manifest, "image: objectfs-controller:latest", "image: objectfs-controller:e2e\n          imagePullPolicy: Never")
	manifest = strings.ReplaceAll(manifest, "image: objectfs-node-daemon:latest", "image: objectfs-node-daemon:e2e\n          imagePullPolicy: Never")
	manifest = strings.ReplaceAll(manifest, `backend: ""`, `backend: "memory://"`)

	walManifestPath := filepath.Join(experimentRoot, "k8s/wal.yaml")
	walB, err := os.ReadFile(walManifestPath)
	if err != nil {
		t.Fatalf("Failed to read wal manifest: %v", err)
	}
	walManifest := string(walB)
	walManifest = strings.ReplaceAll(walManifest, "namespace: kube-objectfs-system", "namespace: default")
	walManifest = strings.ReplaceAll(walManifest, "image: wal-buffer:latest", "image: wal-buffer:e2e\n          imagePullPolicy: Never")

	// Apply WAL buffer and ObjectFS CSI driver / controller
	h.KubectlApplyContent("wal-buffer", walManifest)
	h.KubectlApplyContent("objectfs", manifest)

	// Wait for wal-buffer
	if err := h.WaitForStatefulSet("wal-buffer", "default", 2*time.Minute); err != nil {
		t.Logf("Events:\n%s\n", h.GetEvents("default"))
		t.Fatalf("WAL Buffer failed to start: %v", err)
	}

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

func TestObjectFSStatefulSetE2E(t *testing.T) {
	if os.Getenv("RUN_E2E") == "" {
		t.Skip("Skipping ObjectFS StatefulSet E2E test; RUN_E2E not set")
	}

	h := NewHarness(t, "objectfs-statefulset-e2e")
	h.Setup()
	t.Cleanup(func() {
		if t.Failed() {
			h.DumpDiagnosticLogs(t)
		}
	})

	gitRoot := h.GetGitRoot()
	experimentRoot := gitRoot

	// Build docker images
	h.DockerBuild("objectfs-controller:e2e", filepath.Join(experimentRoot, "images/objectfs-controller/Dockerfile"), experimentRoot)
	h.DockerBuild("objectfs-node-daemon:e2e", filepath.Join(experimentRoot, "images/objectfs-node-daemon/Dockerfile"), experimentRoot)
	h.DockerBuild("wal-buffer:e2e", filepath.Join(experimentRoot, "images/wal-buffer/Dockerfile"), experimentRoot)

	// Load images into Kind
	h.KindLoad("objectfs-controller:e2e")
	h.KindLoad("objectfs-node-daemon:e2e")
	h.KindLoad("wal-buffer:e2e")

	// Read and adapt manifests
	manifestPath := filepath.Join(experimentRoot, "k8s/objectfs.yaml")
	b, err := os.ReadFile(manifestPath)
	if err != nil {
		t.Fatalf("Failed to read manifest: %v", err)
	}
	manifest := string(b)
	manifest = strings.ReplaceAll(manifest, "namespace: kube-objectfs-system", "namespace: default")
	manifest = strings.ReplaceAll(manifest, "image: objectfs-controller:latest", "image: objectfs-controller:e2e\n          imagePullPolicy: Never")
	manifest = strings.ReplaceAll(manifest, "image: objectfs-node-daemon:latest", "image: objectfs-node-daemon:e2e\n          imagePullPolicy: Never")
	manifest = strings.ReplaceAll(manifest, `backend: ""`, `backend: "memory://"`)

	walManifestPath := filepath.Join(experimentRoot, "k8s/wal.yaml")
	walB, err := os.ReadFile(walManifestPath)
	if err != nil {
		t.Fatalf("Failed to read wal manifest: %v", err)
	}
	walManifest := string(walB)
	walManifest = strings.ReplaceAll(walManifest, "namespace: kube-objectfs-system", "namespace: default")
	walManifest = strings.ReplaceAll(walManifest, "image: wal-buffer:latest", "image: wal-buffer:e2e\n          imagePullPolicy: Never")

	// Apply WAL buffer and ObjectFS CSI driver / controller
	h.KubectlApplyContent("wal-buffer", walManifest)
	h.KubectlApplyContent("objectfs", manifest)

	// Wait for wal-buffer
	if err := h.WaitForStatefulSet("wal-buffer", "default", 2*time.Minute); err != nil {
		t.Logf("Events:\n%s\n", h.GetEvents("default"))
		t.Fatalf("WAL Buffer failed to start: %v", err)
	}

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

	// Deploy StatefulSet with Dynamic Volume Provisioning via StorageClass
	statefulSetYaml := `
apiVersion: v1
kind: ConfigMap
metadata:
  name: dev-vscode
data:
  settings.json: |
    {
      "editor.fontSize": 14
    }
---
apiVersion: v1
kind: Secret
metadata:
  name: dev-vscode
type: Opaque
data:
  password: cGFzc3dvcmQ=
---
apiVersion: v1
kind: Service
metadata:
  name: dev-vscode
spec:
  ports:
    - port: 80
      targetPort: 80
  selector:
    app: dev-vscode
---
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: dev-vscode
spec:
  serviceName: dev-vscode
  replicas: 1
  selector:
    matchLabels:
      app: dev-vscode
  template:
    metadata:
      labels:
        app: dev-vscode
    spec:
      initContainers:
        - name: init
          image: debian:bookworm-slim
          command: ["/bin/sh", "-c", "mkdir -p /vscode-server/workspace"]
          volumeMounts:
            - name: dev-vscode-data
              mountPath: /vscode-server
      containers:
        - name: app
          image: debian:bookworm-slim
          ports:
            - containerPort: 80
          volumeMounts:
            - name: dev-vscode-data
              mountPath: /vscode-server
          env:
            - name: PASSWORD
              valueFrom:
                secretKeyRef:
                  name: dev-vscode
                  key: password
          command: ["/bin/sh", "-c", "sleep infinity"]
  volumeClaimTemplates:
    - metadata:
        name: dev-vscode-data
      spec:
        storageClassName: objectfs-standard
        accessModes:
          - ReadWriteOnce
        resources:
          requests:
            storage: 10Gi
`
	t.Logf("Applying VSCode StatefulSet with ObjectFS StorageClass")
	h.KubectlApplyContent("dev-vscode", statefulSetYaml)

	podName := "dev-vscode-0"
	t.Logf("Waiting for StatefulSet pod %s to be ready", podName)
	if err := h.WaitForPodReady(podName, "default", 2*time.Minute); err != nil {
		t.Logf("Events:\n%s\n", h.GetEvents("default"))
		t.Logf("Controller Logs:\n%s\n", h.GetPodLogsByName("objectfs-controller-0", "default"))
		t.Logf("Node Daemon Logs:\n%s\n", h.GetPodLogs("app=objectfs-node-daemon", "default"))
		t.Fatalf("StatefulSet pod %s failed to start: %v", podName, err)
	}

	// Step 1: Write a file via kubectl exec inside the pod
	t.Logf("Writing data inside %s", podName)
	_, err = h.RunInPod(podName, "default", "/bin/sh", "-c", "echo 'hello from vscode statefulset' > /vscode-server/workspace/test.txt && sync")
	if err != nil {
		t.Fatalf("Failed to write test.txt in %s: %v", podName, err)
	}

	// Verify the file can be read back
	out, err := h.RunInPod(podName, "default", "cat", "/vscode-server/workspace/test.txt")
	if err != nil {
		t.Fatalf("Failed to read test.txt in %s: %v", podName, err)
	}
	if !strings.Contains(out, "hello from vscode statefulset") {
		t.Fatalf("Expected content 'hello from vscode statefulset', got: %s", out)
	}

	// Step 2: Delete the pod and verify persistence across StatefulSet pod recreation
	t.Logf("Deleting pod %s to test data persistence", podName)
	h.DeletePod(podName, "default")

	t.Logf("Waiting for recreated pod %s to be ready", podName)
	if err := h.WaitForPodReady(podName, "default", 2*time.Minute); err != nil {
		t.Logf("Events:\n%s\n", h.GetEvents("default"))
		t.Fatalf("Recreated pod %s failed to become ready: %v", podName, err)
	}

	// Verify persisted data after recreation
	t.Logf("Reading data from recreated pod %s", podName)
	outAfterRestart, err := h.RunInPod(podName, "default", "cat", "/vscode-server/workspace/test.txt")
	if err != nil {
		t.Fatalf("Failed to read test.txt after recreation in %s: %v", podName, err)
	}
	if !strings.Contains(outAfterRestart, "hello from vscode statefulset") {
		t.Fatalf("Expected content 'hello from vscode statefulset' after recreation, got: %s", outAfterRestart)
	}

	t.Logf("Successfully verified ObjectFS dynamic provisioning with StatefulSet!")
}

func TestObjectFSPersistentE2E(t *testing.T) {
	if os.Getenv("RUN_E2E") == "" {
		t.Skip("Skipping ObjectFS Persistent E2E test; RUN_E2E not set")
	}

	h := NewHarness(t, "objectfs-persistent-e2e")
	h.Setup()
	t.Cleanup(func() {
		if t.Failed() {
			h.DumpDiagnosticLogs(t)
		}
	})

	gitRoot := h.GetGitRoot()
	experimentRoot := gitRoot

	// Build docker images
	h.DockerBuild("objectfs-controller:e2e", filepath.Join(experimentRoot, "images/objectfs-controller/Dockerfile"), experimentRoot)
	h.DockerBuild("objectfs-node-daemon:e2e", filepath.Join(experimentRoot, "images/objectfs-node-daemon/Dockerfile"), experimentRoot)
	h.DockerBuild("wal-buffer:e2e", filepath.Join(experimentRoot, "images/wal-buffer/Dockerfile"), experimentRoot)
	h.DockerBuild("fakes3:e2e", filepath.Join(experimentRoot, "fakes3/images/fakes3/Dockerfile"), filepath.Join(experimentRoot, "fakes3"))

	// Load images into Kind
	h.KindLoad("objectfs-controller:e2e")
	h.KindLoad("objectfs-node-daemon:e2e")
	h.KindLoad("wal-buffer:e2e")
	h.KindLoad("fakes3:e2e")

	// Deploy FakeS3 service & deployment in default namespace
	fakes3Manifest := `
apiVersion: v1
kind: Service
metadata:
  name: fakes3
  namespace: default
spec:
  ports:
    - port: 9000
      targetPort: 9000
  selector:
    app: fakes3
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: fakes3
  namespace: default
spec:
  replicas: 1
  selector:
    matchLabels:
      app: fakes3
  template:
    metadata:
      labels:
        app: fakes3
    spec:
      containers:
        - name: fakes3
          image: fakes3:e2e
          imagePullPolicy: Never
          args: ["--listen=0.0.0.0:9000", "--buckets=objectfs-bucket", "--quiet"]
          ports:
            - containerPort: 9000
`
	t.Logf("Deploying FakeS3 backing object storage")
	h.KubectlApplyContent("fakes3", fakes3Manifest)
	if err := h.WaitForDeployment("fakes3", "default", 2*time.Minute); err != nil {
		t.Fatalf("FakeS3 deployment failed to start: %v", err)
	}

	// Read and adapt ObjectFS manifest with S3 backend
	s3BackendURL := "s3://objectfs-bucket?endpoint=http://fakes3:9000&region=us-east-1&s3ForcePathStyle=true"

	objectfsManifestPath := filepath.Join(experimentRoot, "k8s/objectfs.yaml")
	b, err := os.ReadFile(objectfsManifestPath)
	if err != nil {
		t.Fatalf("Failed to read objectfs manifest: %v", err)
	}
	objectfsManifest := string(b)
	objectfsManifest = strings.ReplaceAll(objectfsManifest, "namespace: kube-objectfs-system", "namespace: default")
	objectfsManifest = strings.ReplaceAll(objectfsManifest, "image: objectfs-controller:latest", "image: objectfs-controller:e2e\n          imagePullPolicy: Never")
	objectfsManifest = strings.ReplaceAll(objectfsManifest, "image: objectfs-node-daemon:latest", "image: objectfs-node-daemon:e2e\n          imagePullPolicy: Never")
	objectfsManifest = strings.ReplaceAll(objectfsManifest, `backend: ""`, fmt.Sprintf("backend: %q", s3BackendURL))

	// Inject AWS credential env vars into objectfs-controller container
	awsEnvController := `env:
            - name: AWS_ACCESS_KEY_ID
              value: "fakes3"
            - name: AWS_SECRET_ACCESS_KEY
              value: "fakes3"
            - name: AWS_REGION
              value: "us-east-1"
            - name: OBJECT_STORAGE_BACKEND`
	objectfsManifest = strings.ReplaceAll(objectfsManifest, "env:\n            - name: OBJECT_STORAGE_BACKEND", awsEnvController)

	// Read and adapt WAL buffer manifest
	walManifestPath := filepath.Join(experimentRoot, "k8s/wal.yaml")
	b, err = os.ReadFile(walManifestPath)
	if err != nil {
		t.Fatalf("Failed to read wal manifest: %v", err)
	}
	walManifest := string(b)
	walManifest = strings.ReplaceAll(walManifest, "namespace: kube-objectfs-system", "namespace: default")
	walManifest = strings.ReplaceAll(walManifest, "image: wal-buffer:latest", "image: wal-buffer:e2e\n          imagePullPolicy: Never")

	awsEnvWAL := `env:
            - name: AWS_ACCESS_KEY_ID
              value: "fakes3"
            - name: AWS_SECRET_ACCESS_KEY
              value: "fakes3"
            - name: AWS_REGION
              value: "us-east-1"
            - name: OBJECT_STORAGE_BACKEND`
	walManifest = strings.ReplaceAll(walManifest, "env:\n            - name: OBJECT_STORAGE_BACKEND", awsEnvWAL)

	t.Logf("Deploying ObjectFS and WAL Buffer with S3 backend and hostPath caching")
	h.KubectlApplyContent("objectfs", objectfsManifest)
	h.KubectlApplyContent("wal-buffer", walManifest)

	// Wait for wal-buffer
	if err := h.WaitForStatefulSet("wal-buffer", "default", 2*time.Minute); err != nil {
		t.Logf("Events:\n%s\n", h.GetEvents("default"))
		t.Fatalf("WAL Buffer failed to start: %v", err)
	}

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

	volumeID := "persistent-s3-test-vol"

	// Step 1: Write initial files to the persistent volume
	pod1Name := "persistent-pod-1"
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
      command: ["/bin/sh", "-c", "echo 'root-persistent-data' > /data/root.txt && mkdir /data/models && echo 'model-weights-persisted' > /data/models/weights.bin && sync && sleep 10"]
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

	t.Logf("Creating Pod 1 (writing data before controller restart)")
	h.KubectlApplyContent(pod1Name, pod1Yaml)
	if err := h.WaitForPodReady(pod1Name, "default", 1*time.Minute); err != nil {
		t.Logf("Events:\n%s\n", h.GetEvents("default"))
		t.Fatalf("Pod 1 failed to start: %v", err)
	}

	time.Sleep(5 * time.Second)
	t.Logf("Deleting Pod 1")
	h.DeletePod(pod1Name, "default")

	// Step 2: Restart ObjectFS Controller (simulating crash / pod restart)
	t.Logf("Restarting ObjectFS Controller pod to verify persistence across restarts")
	h.DeletePod("objectfs-controller-0", "default")

	// Wait for controller pod to be recreated and ready
	if err := h.WaitForStatefulSet("objectfs-controller", "default", 2*time.Minute); err != nil {
		t.Logf("Events:\n%s\n", h.GetEvents("default"))
		t.Fatalf("ObjectFS Controller failed to restart: %v", err)
	}

	// Step 3: Start Pod 2, read back existing data, and append new files
	pod2Name := "persistent-pod-2"
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
      command: ["/bin/sh", "-c", "cat /data/root.txt && cat /data/models/weights.bin && echo 'after-restart-file' > /data/models/config.json && echo 'updated-root-data' > /data/root.txt && sync && sleep 10"]
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

	t.Logf("Creating Pod 2 (reading data after controller restart and making further writes)")
	h.KubectlApplyContent(pod2Name, pod2Yaml)
	if err := h.WaitForPodReady(pod2Name, "default", 1*time.Minute); err != nil {
		t.Logf("Events:\n%s\n", h.GetEvents("default"))
		t.Logf("Controller Logs:\n%s\n", h.GetPodLogsByName("objectfs-controller-0", "default"))
		t.Fatalf("Pod 2 failed to start: %v", err)
	}

	time.Sleep(5 * time.Second)
	logs2 := h.GetPodLogsByName(pod2Name, "default")
	if !strings.Contains(logs2, "root-persistent-data") || !strings.Contains(logs2, "model-weights-persisted") {
		t.Fatalf("Pod 2 did not recover pre-restart data! Logs:\n%s", logs2)
	}
	t.Logf("Pod 2 successfully verified recovered data across controller restart!")
	h.DeletePod(pod2Name, "default")

	// Step 4: Restart ObjectFS Controller a second time
	t.Logf("Restarting ObjectFS Controller a second time")
	h.DeletePod("objectfs-controller-0", "default")
	if err := h.WaitForStatefulSet("objectfs-controller", "default", 2*time.Minute); err != nil {
		t.Logf("Events:\n%s\n", h.GetEvents("default"))
		t.Fatalf("ObjectFS Controller failed to restart (2nd time): %v", err)
	}

	// Step 5: Start Pod 3 to verify both original and post-restart modifications
	pod3Name := "persistent-pod-3"
	pod3Yaml := fmt.Sprintf(`
apiVersion: v1
kind: Pod
metadata:
  name: %s
spec:
  containers:
    - name: app
      image: alpine
      command: ["/bin/sh", "-c", "cat /data/root.txt && cat /data/models/config.json && sleep 3600"]
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

	t.Logf("Creating Pod 3 (verifying state after 2nd controller restart)")
	h.KubectlApplyContent(pod3Name, pod3Yaml)
	if err := h.WaitForPodReady(pod3Name, "default", 1*time.Minute); err != nil {
		t.Fatalf("Pod 3 failed to start: %v", err)
	}

	outRoot, err := h.RunInPod(pod3Name, "default", "cat", "/data/root.txt")
	if err != nil {
		t.Fatalf("Failed to read /data/root.txt in Pod 3: %v", err)
	}
	if !strings.Contains(outRoot, "updated-root-data") {
		t.Fatalf("Expected updated-root-data, got: %s", outRoot)
	}

	outConfig, err := h.RunInPod(pod3Name, "default", "cat", "/data/models/config.json")
	if err != nil {
		t.Fatalf("Failed to read /data/models/config.json in Pod 3: %v", err)
	}
	if !strings.Contains(outConfig, "after-restart-file") {
		t.Fatalf("Expected after-restart-file, got: %s", outConfig)
	}

	h.DeletePod(pod3Name, "default")
	t.Logf("Successfully verified ObjectFS persistent deployment with S3 backend, WAL buffer, and restart recovery!")
}

func TestObjectFSMultiNodeFailoverE2E(t *testing.T) {
	if os.Getenv("RUN_E2E") == "" {
		t.Skip("Skipping ObjectFS Multi-Node Failover E2E test; RUN_E2E not set")
	}

	h := NewHarness(t, "objectfs-multinode-e2e")
	h.SetupMultiNode(2)
	t.Cleanup(func() {
		if t.Failed() {
			h.DumpDiagnosticLogs(t)
		}
	})

	nodes := h.GetWorkerNodeNames()
	if len(nodes) < 2 {
		nodes = h.GetNodeNames()
	}
	if len(nodes) < 2 {
		t.Fatalf("Expected at least 2 nodes in kind cluster, found %d: %v", len(nodes), nodes)
	}
	node1 := nodes[0]
	node2 := nodes[1]
	t.Logf("Running 2-node failover test across Node 1 (%s) and Node 2 (%s)", node1, node2)

	gitRoot := h.GetGitRoot()
	experimentRoot := gitRoot

	// Build docker images
	h.DockerBuild("objectfs-controller:e2e", filepath.Join(experimentRoot, "images/objectfs-controller/Dockerfile"), experimentRoot)
	h.DockerBuild("objectfs-node-daemon:e2e", filepath.Join(experimentRoot, "images/objectfs-node-daemon/Dockerfile"), experimentRoot)
	h.DockerBuild("wal-buffer:e2e", filepath.Join(experimentRoot, "images/wal-buffer/Dockerfile"), experimentRoot)
	h.DockerBuild("fakes3:e2e", filepath.Join(experimentRoot, "fakes3/images/fakes3/Dockerfile"), filepath.Join(experimentRoot, "fakes3"))

	// Load images into Kind
	h.KindLoad("objectfs-controller:e2e")
	h.KindLoad("objectfs-node-daemon:e2e")
	h.KindLoad("wal-buffer:e2e")
	h.KindLoad("fakes3:e2e")

	// Deploy FakeS3
	fakes3Manifest := `
apiVersion: v1
kind: Service
metadata:
  name: fakes3
  namespace: default
spec:
  ports:
    - port: 9000
      targetPort: 9000
  selector:
    app: fakes3
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: fakes3
  namespace: default
spec:
  replicas: 1
  selector:
    matchLabels:
      app: fakes3
  template:
    metadata:
      labels:
        app: fakes3
    spec:
      containers:
        - name: fakes3
          image: fakes3:e2e
          imagePullPolicy: Never
          args: ["--listen=0.0.0.0:9000", "--buckets=objectfs-bucket", "--quiet"]
          ports:
            - containerPort: 9000
`
	t.Logf("Deploying FakeS3 backing object storage")
	h.KubectlApplyContent("fakes3", fakes3Manifest)
	if err := h.WaitForDeployment("fakes3", "default", 2*time.Minute); err != nil {
		t.Fatalf("FakeS3 deployment failed to start: %v", err)
	}

	s3BackendURL := "s3://objectfs-bucket?endpoint=http://fakes3:9000&region=us-east-1&s3ForcePathStyle=true"

	// ObjectFS manifest with S3 backend and pinned initially to node1
	objectfsManifestPath := filepath.Join(experimentRoot, "k8s/objectfs.yaml")
	b, err := os.ReadFile(objectfsManifestPath)
	if err != nil {
		t.Fatalf("Failed to read objectfs manifest: %v", err)
	}
	objectfsManifest := string(b)
	objectfsManifest = strings.ReplaceAll(objectfsManifest, "namespace: kube-objectfs-system", "namespace: default")
	objectfsManifest = strings.ReplaceAll(objectfsManifest, "image: objectfs-controller:latest", "image: objectfs-controller:e2e\n          imagePullPolicy: Never")
	objectfsManifest = strings.ReplaceAll(objectfsManifest, "image: objectfs-node-daemon:latest", "image: objectfs-node-daemon:e2e\n          imagePullPolicy: Never")
	objectfsManifest = strings.ReplaceAll(objectfsManifest, `backend: ""`, fmt.Sprintf("backend: %q", s3BackendURL))

	awsEnvController := `env:
            - name: AWS_ACCESS_KEY_ID
              value: "fakes3"
            - name: AWS_SECRET_ACCESS_KEY
              value: "fakes3"
            - name: AWS_REGION
              value: "us-east-1"
            - name: OBJECT_STORAGE_BACKEND`
	objectfsManifest = strings.ReplaceAll(objectfsManifest, "env:\n            - name: OBJECT_STORAGE_BACKEND", awsEnvController)

	// Pin controller to node1 initially and add toleration
	nodeSelectorNode1 := fmt.Sprintf("nodeSelector:\n        kubernetes.io/hostname: %s\n      tolerations:\n        - key: \"node-role.kubernetes.io/control-plane\"\n          operator: \"Exists\"\n          effect: \"NoSchedule\"\n      serviceAccountName: objectfs-controller", node1)
	objectfsManifest = strings.ReplaceAll(objectfsManifest, "serviceAccountName: objectfs-controller", nodeSelectorNode1)

	// WAL buffer manifest
	walManifestPath := filepath.Join(experimentRoot, "k8s/wal.yaml")
	b, err = os.ReadFile(walManifestPath)
	if err != nil {
		t.Fatalf("Failed to read wal manifest: %v", err)
	}
	walManifest := string(b)
	walManifest = strings.ReplaceAll(walManifest, "namespace: kube-objectfs-system", "namespace: default")
	walManifest = strings.ReplaceAll(walManifest, "image: wal-buffer:latest", "image: wal-buffer:e2e\n          imagePullPolicy: Never")

	awsEnvWAL := `env:
            - name: AWS_ACCESS_KEY_ID
              value: "fakes3"
            - name: AWS_SECRET_ACCESS_KEY
              value: "fakes3"
            - name: AWS_REGION
              value: "us-east-1"
            - name: OBJECT_STORAGE_BACKEND`
	walManifest = strings.ReplaceAll(walManifest, "env:\n            - name: OBJECT_STORAGE_BACKEND", awsEnvWAL)

	t.Logf("Deploying ObjectFS (on %s) and WAL Buffer", node1)
	h.KubectlApplyContent("objectfs", objectfsManifest)
	h.KubectlApplyContent("wal-buffer", walManifest)

	if err := h.WaitForStatefulSet("wal-buffer", "default", 2*time.Minute); err != nil {
		t.Fatalf("WAL Buffer failed to start: %v", err)
	}
	if err := h.WaitForStatefulSet("objectfs-controller", "default", 2*time.Minute); err != nil {
		t.Fatalf("ObjectFS Controller failed to start on %s: %v", node1, err)
	}
	if err := h.WaitForDaemonSet("objectfs-node-daemon", "default", 2*time.Minute); err != nil {
		t.Fatalf("ObjectFS Node Daemon failed to start: %v", err)
	}

	volumeID := "failover-s3-test-vol"

	// Step 1: Start a long-running writer pod on node1 that writes files and fsyncs
	writerPodName := "failover-writer-pod"
	writerPodYaml := fmt.Sprintf(`
apiVersion: v1
kind: Pod
metadata:
  name: %s
spec:
  nodeSelector:
    kubernetes.io/hostname: %s
  containers:
    - name: app
      image: alpine
      command: ["/bin/sh", "-c", "echo 'node1-file-content' > /data/file1.txt && mkdir /data/nested && echo 'nested-data-1' > /data/nested/data.txt && sync && sleep 3600"]
      volumeMounts:
        - name: data
          mountPath: /data
  volumes:
    - name: data
      csi:
        driver: objectfs.labs.gke.io
        volumeAttributes:
          volumeID: %s
`, writerPodName, node1, volumeID)

	t.Logf("Creating writer pod on %s (writing data while keeping pod alive)", node1)
	h.KubectlApplyContent(writerPodName, writerPodYaml)
	if err := h.WaitForPodReady(writerPodName, "default", 1*time.Minute); err != nil {
		t.Fatalf("Writer pod failed to start: %v", err)
	}

	time.Sleep(3 * time.Second)

	// Step 2: Force-kill controller pod while writer pod is STILL RUNNING (no pod deletion / no snapshot push)
	t.Logf("Force-killing ObjectFS Controller pod on %s while writer pod is still running", node1)
	h.RunCommand("kubectl", "delete", "pod", "objectfs-controller-0", "-n", "default", "--force", "--grace-period=0")

	// Step 3: Move controller to node2 by patching nodeSelector
	t.Logf("Moving ObjectFS Controller to Node 2 (%s)", node2)
	patchJSON := fmt.Sprintf(`{"spec":{"template":{"spec":{"nodeSelector":{"kubernetes.io/hostname":%q}}}}}`, node2)
	h.RunCommand("kubectl", "patch", "statefulset", "objectfs-controller", "-n", "default", "-p", patchJSON)

	// Wait for controller to be ready on node2
	if err := h.WaitForStatefulSet("objectfs-controller", "default", 2*time.Minute); err != nil {
		t.Fatalf("ObjectFS Controller failed to restart on %s: %v", node2, err)
	}

	// Step 4: Verify files are intact on node2, and perform new writes on node2
	readerPod2Name := "failover-reader-node2"
	readerPod2Yaml := fmt.Sprintf(`
apiVersion: v1
kind: Pod
metadata:
  name: %s
spec:
  nodeSelector:
    kubernetes.io/hostname: %s
  containers:
    - name: app
      image: alpine
      command: ["/bin/sh", "-c", "cat /data/file1.txt && cat /data/nested/data.txt && echo 'node2-file-content' > /data/file2.txt && sync && sleep 3600"]
      volumeMounts:
        - name: data
          mountPath: /data
  volumes:
    - name: data
      csi:
        driver: objectfs.labs.gke.io
        volumeAttributes:
          volumeID: %s
`, readerPod2Name, node2, volumeID)

	t.Logf("Creating reader/writer pod on Node 2 (%s) to verify recovery from wal-buffer", node2)
	h.KubectlApplyContent(readerPod2Name, readerPod2Yaml)
	if err := h.WaitForPodReady(readerPod2Name, "default", 1*time.Minute); err != nil {
		t.Fatalf("Pod on Node 2 failed to start: %v", err)
	}

	// Helper to retry reading a file in pod for up to 30s to tolerate node-daemon gRPC reconnect backoff
	retryCatInPod := func(pod, path string) (string, error) {
		deadline := time.Now().Add(30 * time.Second)
		var lastOut string
		var lastErr error
		for time.Now().Before(deadline) {
			lastOut, lastErr = h.RunInPod(pod, "default", "cat", path)
			if lastErr == nil {
				return lastOut, nil
			}
			if strings.Contains(strings.ToLower(lastOut), "resource busy") || strings.Contains(strings.ToLower(fmt.Sprintf("%v", lastErr)), "resource busy") {
				return lastOut, fmt.Errorf("unexpected EBUSY / Resource busy error during controller failover: %v (out: %s)", lastErr, lastOut)
			}
			time.Sleep(500 * time.Millisecond)
		}
		return lastOut, fmt.Errorf("timed out reading %s after 30s: last err=%v out=%s", path, lastErr, lastOut)
	}

	time.Sleep(1 * time.Second)
	outF1, err := retryCatInPod(readerPod2Name, "/data/file1.txt")
	if err != nil || !strings.Contains(outF1, "node1-file-content") {
		t.Fatalf("Expected 'node1-file-content' on Node 2, got: %q (err: %v)", outF1, err)
	}

	outNested, err := retryCatInPod(readerPod2Name, "/data/nested/data.txt")
	if err != nil || !strings.Contains(outNested, "nested-data-1") {
		t.Fatalf("Expected 'nested-data-1' on Node 2, got: %q (err: %v)", outNested, err)
	}
	t.Logf("Successfully verified data recovered from wal-buffer on Node 2!")

	// Step 5: Force-kill controller pod on node2 and move it BACK to node1 (whose hostPath segments are now stale!)
	t.Logf("Force-killing controller on Node 2 and moving BACK to Node 1 (%s) with stale local segments", node1)
	h.RunCommand("kubectl", "delete", "pod", "objectfs-controller-0", "-n", "default", "--force", "--grace-period=0")

	patchNode1JSON := fmt.Sprintf(`{"spec":{"template":{"spec":{"nodeSelector":{"kubernetes.io/hostname":%q}}}}}`, node1)
	h.RunCommand("kubectl", "patch", "statefulset", "objectfs-controller", "-n", "default", "-p", patchNode1JSON)

	// Repeatedly read file in writer pod while controller is restarting to verify no EBUSY errors
	readDone := make(chan struct{})
	readErrors := make(chan error, 100)
	go func() {
		defer close(readDone)
		for i := 0; i < 20; i++ {
			out, err := h.RunInPod(writerPodName, "default", "cat", "/data/file1.txt")
			if err != nil {
				if strings.Contains(strings.ToLower(out), "resource busy") || strings.Contains(strings.ToLower(fmt.Sprintf("%v", err)), "resource busy") {
					readErrors <- fmt.Errorf("unexpected EBUSY / Resource busy while controller restarting: %v (%s)", err, out)
				}
			}
			time.Sleep(200 * time.Millisecond)
		}
	}()

	if err := h.WaitForStatefulSet("objectfs-controller", "default", 2*time.Minute); err != nil {
		t.Fatalf("ObjectFS Controller failed to restart back on %s: %v", node1, err)
	}
	<-readDone
	close(readErrors)
	for rErr := range readErrors {
		t.Fatalf("Read during restart failed: %v", rErr)
	}

	// Step 6: Verify all files (node1 writes + node2 writes) are intact on node1 with retries
	outF2, err := retryCatInPod(writerPodName, "/data/file2.txt")
	if err != nil || !strings.Contains(outF2, "node2-file-content") {
		t.Fatalf("Expected 'node2-file-content' back on Node 1, got: %q (err: %v)", outF2, err)
	}

	// Write additional file back on node1 to ensure sequence numbering continues cleanly
	_, err = h.RunInPod(writerPodName, "default", "/bin/sh", "-c", "echo 'node1-final-content' > /data/final.txt && sync")
	if err != nil {
		t.Fatalf("Failed to write final.txt on Node 1: %v", err)
	}

	outFinal, err := retryCatInPod(writerPodName, "/data/final.txt")
	if err != nil || !strings.Contains(outFinal, "node1-final-content") {
		t.Fatalf("Expected 'node1-final-content', got: %q (err: %v)", outFinal, err)
	}

	// Step 7: Restart controller on Node 1 once more and verify all files persist without duplicate sequence errors
	t.Logf("Restarting controller on Node 1 once more to verify persistence after truncation")
	h.RunCommand("kubectl", "delete", "pod", "objectfs-controller-0", "-n", "default", "--force", "--grace-period=0")
	if err := h.WaitForStatefulSet("objectfs-controller", "default", 2*time.Minute); err != nil {
		t.Fatalf("ObjectFS Controller failed to restart on Node 1: %v", err)
	}

	outFinal2, err := retryCatInPod(writerPodName, "/data/final.txt")
	if err != nil || !strings.Contains(outFinal2, "node1-final-content") {
		t.Fatalf("Expected 'node1-final-content' after 2nd restart on Node 1, got: %q (err: %v)", outFinal2, err)
	}
	outF1Final, err := retryCatInPod(writerPodName, "/data/file1.txt")
	if err != nil || !strings.Contains(outF1Final, "node1-file-content") {
		t.Fatalf("Expected 'node1-file-content' after 2nd restart on Node 1, got: %q (err: %v)", outF1Final, err)
	}
	outF2Final, err := retryCatInPod(writerPodName, "/data/file2.txt")
	if err != nil || !strings.Contains(outF2Final, "node2-file-content") {
		t.Fatalf("Expected 'node2-file-content' after 2nd restart on Node 1, got: %q (err: %v)", outF2Final, err)
	}

	h.DeletePod(readerPod2Name, "default")
	h.DeletePod(writerPodName, "default")
	t.Logf("Successfully verified ObjectFS multi-node failover and stale segment recovery across nodes!")
}
