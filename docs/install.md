# ObjectFS Persistent Deployment & Installation Guide

This guide describes how to deploy **ObjectFS** in Kubernetes clusters with persistent cloud object storage (Google Cloud Storage or Amazon S3), a central Write-Ahead Log (WAL) buffer, and local `hostPath` caching.

> **Important:** The `objectfs-controller` and `wal-buffer` pods will not start (they exit with a clear error) until you set the `backend` field in the `objectfs-config` ConfigMap.

---

## Architecture Overview

ObjectFS is self-contained: because it acts as the cluster's storage provider, it does **not** depend on a pre-existing StorageClass or PVCs for its own control-plane storage. Local state on nodes is treated as an acceleration cache backed by `hostPath` volumes (`/var/lib/objectfs/...`). Authoritative state lives in cloud object storage and the central `wal-buffer`.

```
+-----------------------------------------------------------------------------------------+
|                                  Kubernetes Cluster                                     |
|                                                                                         |
|  +---------------------------+               +---------------------------------------+  |
|  |   objectfs-node-daemon    |               |          objectfs-controller          |  |
|  | (DaemonSet on every node) |               |             (StatefulSet)             |  |
|  | - FUSE driver (go-fuse)   | ────────────> | - Metadata Engine (SQLite + RAM Cache)|  |
|  | - Node read/write cache   |  gRPC (50051) | - hostPath: /var/lib/objectfs/        |  |
|  +---------------------------+               |   controller (wal, metadata)          |  |
|                                              +---------------------------------------+  |
|                                                                  │                      |
|                                                                  │ WAL Replication      |
|                                                                  v                      |
|                                              +---------------------------------------+  |
|                                              |              wal-buffer               |  |
|                                              |      (StatefulSet, 1 replica)         |  |
|                                              | - Central in-memory WAL buffer        |  |
|                                              | - hostPath: /var/lib/objectfs/        |  |
|                                              |   wal-buffer (segments cache)         |  |
|                                              +---------------------------------------+  |
|                                                                  │                      |
+------------------------------------------------------------------|----------------------+
                                                                   │ Flush / Commit
                                                                   v
+-----------------------------------------------------------------------------------------+
|                       Cloud Object Storage (GCS / S3 / MinIO)                           |
|  - Content-addressable data blobs: blobs/<sha256>, blobs/<packID>-<count>.pack          |
|  - Metadata snapshots: volumes/<volID>/meta/<stream_seq>.erofs                          |
|  - Durable WAL segment logs: wal/segments/<first_pos>-<last_pos>.wal                    |
+-----------------------------------------------------------------------------------------+
```

### Components

1. **`objectfs-node-daemon` (`DaemonSet`):** Runs on every node, mounting volumes via FUSE (`go-fuse`) and caching read/write data blocks.
2. **`objectfs-controller` (`StatefulSet`):** Central metadata server storing SQLite database files (`/var/lib/objectfs/controller/metadata/<volID>/metadata.sqlite`) and local WAL segment cache (`/var/lib/objectfs/controller/wal`). On startup, the controller replays local WAL segments and restores published snapshots from object storage. If the controller moves to a new node without local WAL history, it loads published metadata snapshots from cloud object storage; uncommitted remote WAL tailing across node moves will be completed in [#185](https://github.com/gke-labs/in-cluster-storage/issues/185).
3. **`wal-buffer` (`StatefulSet`):** Central high-throughput stream buffer service with local caching on `hostPath` (`/var/lib/objectfs/wal-buffer`) and background flushing to cloud object storage. Runs as a single replica (or 1 replica per node).
4. **Cloud Object Storage Backend:** Google Cloud Storage (`gs://...`) or Amazon S3 / S3-compatible store (`s3://...`).

---

## Prerequisites

- A Kubernetes cluster (v1.26+).
- `kubectl` configured with cluster administrator privileges.
- A Cloud Object Storage bucket (GCS or S3) with read/write access.

---

## 1. Configuring Cloud Object Storage

### Option A: Google Cloud Storage (GCS) with Workload Identity (GKE)

GKE Workload Identity allows Kubernetes workloads to securely access Google Cloud services without storing service account keys in cluster secrets.

#### Step 1: Create a GCS Bucket
```bash
export PROJECT_ID="your-gcp-project-id"
export BUCKET_NAME="your-objectfs-bucket"
export GSA_NAME="objectfs-storage-sa"
export NAMESPACE="kube-objectfs-system"

gcloud storage buckets create "gs://${BUCKET_NAME}" --project="${PROJECT_ID}" --location="us-central1"
```

#### Step 2: Create a Google Service Account (GSA) and Grant Bucket Permissions
```bash
gcloud iam service-accounts create "${GSA_NAME}" \
  --project="${PROJECT_ID}" \
  --description="Service account for ObjectFS controller and WAL buffer" \
  --display-name="ObjectFS Storage SA"

# Grant read/write access on the bucket
gcloud storage buckets add-iam-policy-binding "gs://${BUCKET_NAME}" \
  --member="serviceAccount:${GSA_NAME}@${PROJECT_ID}.iam.gserviceaccount.com" \
  --role="roles/storage.objectUser"
```

#### Step 3: Bind GSA to Kubernetes Service Accounts
Bind the Google Service Account to both `objectfs-controller` and `wal-buffer` Kubernetes ServiceAccounts:
```bash
gcloud iam service-accounts add-iam-policy-binding "${GSA_NAME}@${PROJECT_ID}.iam.gserviceaccount.com" \
  --project="${PROJECT_ID}" \
  --role="roles/iam.workloadIdentityUser" \
  --member="serviceAccount:${PROJECT_ID}.svc.id.goog[${NAMESPACE}/objectfs-controller]"

gcloud iam service-accounts add-iam-policy-binding "${GSA_NAME}@${PROJECT_ID}.iam.gserviceaccount.com" \
  --project="${PROJECT_ID}" \
  --role="roles/iam.workloadIdentityUser" \
  --member="serviceAccount:${PROJECT_ID}.svc.id.goog[${NAMESPACE}/wal-buffer]"
```

#### Step 4: Annotate Kubernetes ServiceAccounts
Annotate the ServiceAccounts in `k8s/objectfs.yaml` and `k8s/wal.yaml`:
```yaml
apiVersion: v1
kind: ServiceAccount
metadata:
  name: objectfs-controller
  namespace: kube-objectfs-system
  annotations:
    iam.gke.io/gcp-service-account: objectfs-storage-sa@your-gcp-project-id.iam.gserviceaccount.com
---
apiVersion: v1
kind: ServiceAccount
metadata:
  name: wal-buffer
  namespace: kube-objectfs-system
  annotations:
    iam.gke.io/gcp-service-account: objectfs-storage-sa@your-gcp-project-id.iam.gserviceaccount.com
```

---

### Option B: Amazon S3 or S3-Compatible Storage (MinIO, Ceph)

For Amazon S3 or S3-compatible object stores, configure backend URLs with query parameters:
```yaml
backend: "s3://my-bucket/prefix?region=us-east-1&endpoint=http://minio:9000&use_path_style=true"
```

Credentials can be passed via AWS IAM Roles for Service Accounts (IRSA) or environment variables (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_REGION`) mapped to the StatefulSets.

---

## 2. Deploying ObjectFS

### Step 1: Configure the Object Storage Backend
Update the `objectfs-config` ConfigMap in `k8s/objectfs.yaml`:
```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: objectfs-config
  namespace: kube-objectfs-system
data:
  backend: "gs://your-objectfs-bucket/data"
```

### Step 2: Deploy WAL Buffer and ObjectFS Driver
```bash
# Create namespace
kubectl create namespace kube-objectfs-system

# Deploy WAL buffer service
kubectl apply -f k8s/wal.yaml

# Deploy ObjectFS controller, CSI driver, and node daemon
kubectl apply -f k8s/objectfs.yaml
```

### Step 3: Verify Deployment
Check that all pods are running and the CSI driver is registered:
```bash
kubectl get pods -n kube-objectfs-system
kubectl get csidrivers
```

---

## 3. Storage Durability Levels & Write Modes

ObjectFS offers configurable durability levels and client write synchronization modes tailored to different workload performance and safety requirements.

### WAL Durability Levels (`--wal-durability`)

Because the controller's local disk is a node `hostPath` cache, durability must be set to `witness` or `permanent` for multi-node failure safety:

| Durability Level | Acknowledgement Point | Failure Mode & Recovery Guarantee | Recommended Use Case |
| :--- | :--- | :--- | :--- |
| `local` | Acknowledged after appending to the controller node's local hostPath disk. | **Node-bound:** If the controller pod moves to another node before flushing, local-only writes are inaccessible until the controller lands back on the original node. | Single-node testing and non-critical scratch workloads. |
| `witness` *(Default)* | Acknowledged after synchronous replication to the remote `wal-buffer` service. | **Crash Safe:** Survives controller restarts on the same node with local WAL cache intact. When the controller starts on a new node without local WAL history, recovery relies on published object storage snapshots; remote stream catch-up across node moves is tracked in [#185](https://github.com/gke-labs/in-cluster-storage/issues/185). | Standard production workloads requiring sub-millisecond latency and multi-pod crash safety. |
| `permanent` | Acknowledged after flushing and committing WAL segments to cloud object storage (GCS/S3). | **Disaster Safe:** Survives total cluster loss; all data is durably written to cloud object storage. | High-value data, archive outputs, and financial/audit logs. |

### Write Modes & Synchronization Guarantees

ObjectFS supports three client write modes:

1. **`WRITE_THROUGH_FSYNC` (Default):**
   - File writes are buffered in client node memory (`NodeCache`).
   - Synchronously transmitted to the controller and flushed to the WAL on `fsync(2)`, `fdatasync(2)`, or `close(2)`.
   - Guarantees POSIX compliance for transactional databases and applications relying on fsync.
2. **`LAZY_WRITE`:**
   - Writes are buffered asynchronously in node cache and flushed in background batches.
   - Maximizes throughput and minimizes network round-trips for batch processing, compilation caches, and scratch storage.
3. **`EAGER_REPLICATION`:**
   - Every `write(2)` syscall is streamed immediately to the central controller memory and change-log.
   - Provides immediate cross-node visibility for collaborative multi-writer workloads.

---

## 4. Verifying Installation & Controller Restart Persistence

To verify that ObjectFS is correctly configured and persistent across controller restarts:

### Step 1: Deploy a Test Workload
Create a test Pod using dynamic provisioning via the `objectfs-standard` StorageClass:

```yaml
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: verify-pvc
spec:
  accessModes:
    - ReadWriteMany
  storageClassName: objectfs-standard
  resources:
    requests:
      storage: 10Gi
---
apiVersion: v1
kind: Pod
metadata:
  name: verify-writer
spec:
  containers:
    - name: app
      image: alpine
      command:
        - /bin/sh
        - -c
        - |
          echo "objectfs-durability-test-data" > /data/persistence-check.txt
          sync
          echo "Wrote test data successfully"
          sleep 3600
      volumeMounts:
        - name: storage
          mountPath: /data
  volumes:
    - name: storage
      persistentVolumeClaim:
        claimName: verify-pvc
```

Apply the workload and check that the file was written:
```bash
kubectl apply -f verify.yaml
kubectl wait --for=condition=Ready pod/verify-writer --timeout=60s
kubectl exec verify-writer -- cat /data/persistence-check.txt
```

### Step 2: Delete the Writer Pod
```bash
kubectl delete pod verify-writer
```

### Step 3: Restart the ObjectFS Controller
Simulate controller crash or pod relocation by deleting the controller pod:
```bash
kubectl delete pod objectfs-controller-0 -n kube-objectfs-system
kubectl wait --for=condition=Ready pod/objectfs-controller-0 -n kube-objectfs-system --timeout=120s
```

### Step 4: Verify Data Read-Back
Launch a reader Pod to verify that the SQLite metadata index and WAL segments were restored from object storage and `wal-buffer`:

```yaml
apiVersion: v1
kind: Pod
metadata:
  name: verify-reader
spec:
  containers:
    - name: app
      image: alpine
      command:
        - /bin/sh
        - -c
        - |
          cat /data/persistence-check.txt
          sleep 3600
      volumeMounts:
        - name: storage
          mountPath: /data
  volumes:
    - name: storage
      persistentVolumeClaim:
        claimName: verify-pvc
```

```bash
kubectl apply -f verify-reader.yaml
kubectl wait --for=condition=Ready pod/verify-reader --timeout=60s
kubectl logs verify-reader
```

Expected output:
```
objectfs-durability-test-data
```

Clean up verification resources:
```bash
kubectl delete pod verify-reader
kubectl delete pvc verify-pvc
```
