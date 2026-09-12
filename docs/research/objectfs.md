# Research & Design: ObjectFS CSI Driver

This document outlines the architecture, design choices, implementation details, and roadmap for **ObjectFS**, a distributed multi-writer FUSE-based CSI storage driver for Kubernetes.

## Objective

ObjectFS enables multi-writer shared storage backed by object storage (such as S3 or Google Cloud Storage) across Kubernetes cluster nodes. It removes the need to distribute object storage credentials to every node by routing access through a central per-cluster service (`objectfs-controller`), providing metadata and small file caching, push notifications for synchronization, and FUSE mounting on nodes.

---

## Architectural Design

### 1. Central Cluster Service (`objectfs-controller`)
- **Credential Isolation:** The central controller interacts with the underlying object storage backend (e.g. S3 or GCS), avoiding distribution of cloud storage secrets across worker nodes.
- **In-Memory Metadata & Content Cache:** Maintains an in-memory representation of directory structures and files for fast metadata lookups and small-file caching.
- **Push Notifications via gRPC Streams:** Exposes `WatchVolume` gRPC streams so node daemons can receive live invalidation events when files or directories are created, updated, renamed, or deleted by concurrent writers across the cluster.
- **Large File Redirect URLs:** Capable of generating signed redirect URLs for direct high-throughput downloads from object storage when requested files exceed inline transfer limits.

### 2. Node CSI Driver & FUSE Client (`objectfs-node-daemon`)
- **FUSE Mount:** Mounts a virtual POSIX filesystem at `targetPath` for each pod volume using `go-fuse`.
- **Local Node Caching:** Automatically caches recently read file blocks and small files in memory/disk to reduce network round-trips.
- **Push Notification Listener:** Subscribes to the controller's `WatchVolume` stream and proactively invalidates local cache entries when external modifications occur.
- **Configurable Write-Through Modes:** Configurable via StorageClass or volume attributes:
  1. `WRITE_THROUGH_FSYNC`: Synchronously flushes data to backing storage on `fsync`/`flush`.
  2. `LAZY_WRITE`: Buffered asynchronous writes for maximum throughput with relaxed durability.
  3. `EAGER_REPLICATION`: Immediate memory replication to the central service to prevent data loss without paying full object store commit latency.

---

## Protocol Specification

ObjectFS defines a comprehensive gRPC service (`ObjectFSController` in `proto/objectfs.proto`) providing:
- `GetAttr`, `Lookup`, `ReadDir`, `Mkdir`
- `CreateFile`, `ReadFile`, `WriteFile`, `TruncateFile`
- `Unlink`, `Rmdir`, `Rename`, `Fsync`
- `WatchVolume` (streaming push notifications)

---

## Roadmap & TODO List

- [x] Protocol definition (`proto/objectfs.proto`) with gRPC service and push notification events.
- [x] In-memory volume manager and filesystem state machine (`pkg/objectfs/controller`).
- [x] Event broadcaster for real-time node synchronization (`pkg/objectfs/controller/events.go`).
- [x] Go-FUSE filesystem layer with local node cache (`pkg/objectfs/fuse`).
- [x] CSI Node & Identity driver implementation (`cmd/objectfs-node-daemon`).
- [x] Kubernetes deployment manifests and Dockerfiles (`k8s/objectfs.yaml`, `images/`).
- [x] End-to-end integration test harness (`tests/e2e/objectfs_e2e_test.go`).
- [ ] Connect pluggable S3/GCS object storage backends into `ObjectStorageBackend`.
- [ ] Support multipart upload streaming for files exceeding 100MB.
- [ ] Extended attribute (xattr) support.
