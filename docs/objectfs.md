# ObjectFS Architecture & Design

This document describes the design, architecture, metadata management, storage layout, caching mechanisms, and future roadmap for **ObjectFS** in the `in-cluster-storage` project.

---

## Overview

**ObjectFS** is a high-performance, POSIX-like distributed filesystem engineered to run natively on Kubernetes clusters. The ultimate source of truth and persistent backing store for ObjectFS is cloud object storage (such as Google Cloud Storage or Amazon S3).

ObjectFS is built around the fundamental architectural principle of **splitting metadata storage from object payload storage**:
1. **Metadata Layer:** Tracks the hierarchical directory tree, file inodes, attributes, permissions, and timestamps. Metadata updates are recorded in an append-only change-log using the **Streams** service ([`docs/streams.md`](streams.md)) for low-latency persistence, combined with periodic immutable **EROFS** snapshots to avoid long replay times during recovery.
2. **Object/Blob Layer:** Stores file contents as deduplicated, immutable content-addressable blobs identified by their SHA-256 digests. ObjectFS adopts a storage strategy inspired by **Git**: large files are written as standalone loose blobs, while smaller files are aggregated into indexed packfiles (`.pack`) to eliminate small-object metadata overhead and API throttling in cloud object storage.

Mounting on Kubernetes worker nodes is currently handled via a user-space **FUSE** driver (`go-fuse`) that communicates with a cluster controller service. ObjectFS provides near-instant volume readiness, intelligent local node caching, push-based invalidations, and high-throughput data access without requiring cloud storage credentials to be distributed to worker nodes.

---

## Architectural Design

```
+-----------------------------------------------------------------------------------------+
|                                    Kubernetes Node                                      |
|                                                                                         |
|  +--------------------+         POSIX Syscalls (read, write, lookup, mkdir, etc.)       |
|  | Application Pod    | -------------------------------------------------------------+  |
|  | (Standard Workload)|                                                              |  |
|  +--------------------+                                                              |  |
|                                                                                      |  |
|  +--------------------------------------------------------------------------------+  |  |
|  | Node CSI Driver & FUSE Client (objectfs-node-daemon)                           |  |  |
|  |                                                                                |  |  |
|  |  +-----------------------------+        +-----------------------------------+  |  |  |
|  |  | Inode / Path Mapper         |        | Local Node Cache (NodeCache)      |<-+  |  |
|  |  +-----------------------------+        | (In-Memory + Dirty Write Buffers) |     |  |
|  |                 |                       +-----------------------------------+     |  |
|  +-----------------|-----------------------------------------|-----------------------+  |
|                    | (gRPC: Lookup, GetAttr, Read, Write)     |                          |
|                    v                                         | (WatchVolume Stream)     |
|  +-----------------------------------------------------------v-----------------------+  |
|  | Enlightened App / CAS Socket (TODO): /.in-cluster-storage/api (SCM_RIGHTS FD)    |  |
+-----------------------------------------------------------------------------------------+
                                     |
                                     | Bidirectional gRPC
                                     v
+-----------------------------------------------------------------------------------------+
|                       Central Cluster Service (objectfs-controller)                     |
|                                                                                         |
|  +-----------------------------------------------------------------------------------+  |
|  | In-Memory Volume State & Metadata Engine                                          |  |
|  |  - Active Directory Tree & Inode Allocator                                        |  |
|  |  - EventBroadcaster (Push Invalidation to Node Watchers)                          |  |
|  +-----------------------------------------------------------------------------------+  |
|         |                                                       |                       |
|         | 1. Metadata Change-Log                                | 2. Blob Management    |
|         v                                                       v                       |
|  +------------------------------+             +--------------------------------------+  |
|  | Streams Layer (docs/streams) |             | Content-Addressable Blob Store       |  |
|  | (Append-Only WAL Engine)     |             | (Loose Blobs & Git-style Packfiles)  |  |
|  +------------------------------+             +--------------------------------------+  |
+-----------------|-----------------------------------------------|-----------------------+
                  |                                               |
                  | Flush / Commit                                | Put / Get / Range
                  v                                               v
+-----------------------------------------------------------------------------------------+
|                             Cloud Object Storage (GCS / S3)                             |
|                                                                                         |
|  Metadata (Snapshots & WAL):                                                            |
|    volumes/<volID>/meta/<timestamp>.erofs        (Immutable EROFS snapshot image)       |
|    volumes/<volID>/meta/latest                   (Pointer to latest snapshot)           |
|    wal/segments/<first_pos>-<last_pos>.wal       (Streams WAL segment logs)             |
|                                                                                         |
|  Data Blobs:                                                                            |
|    blobs/<sha256>                                (Standalone loose blob for > 64MB)     |
|    blobs/<packID>-<count>.pack                   (Packed small/medium blobs + index)    |
+-----------------------------------------------------------------------------------------+
```

### Key Components

1. **Central Controller Service (`objectfs-controller`, `pkg/objectfs/controller`):**
   - Runs as a central service or `StatefulSet` in the Kubernetes cluster.
   - Maintains a metadata engine for sub-millisecond lookup and directory traversal (currently in-memory; TODO: index only "hot" entries in memory while keeping the full metadata structure on local disk replicated from S3/GCS snapshots).
   - Manages credentials to backend cloud object storage (GCS/S3), isolating sensitive storage secrets from worker nodes.
   - Manages periodic consolidation of active volume states into read-only EROFS snapshots and flushes dirty blobs to object storage.
   - Hosts an `EventBroadcaster` that transmits live filesystem events (`EVENT_CREATED`, `EVENT_MODIFIED`, `EVENT_DELETED`, `EVENT_RENAMED`) over `WatchVolume` gRPC streams to connected node daemons.

2. **Node CSI Driver & FUSE Client (`objectfs-node-daemon`, `pkg/objectfs/fuse`):**
   - Deployed as a `DaemonSet` on every Kubernetes node.
   - Implements the CSI node interface (`NodePublishVolume`, `NodeUnpublishVolume`) and mounts a POSIX filesystem at the pod volume path via `go-fuse`.
   - Embeds a fast `NodeCache` supporting LRU read caching, dirty write buffering, and write-through synchronization modes (TODO: cache blobs in allocated fixed-size local scratch storage on an LRU basis; similarly, the controller should cache blobs locally).
   - Subscribes to the controller's `WatchVolume` notification stream to proactively invalidate cached node attributes and file blocks upon remote concurrent writes.

3. **Metadata Change-Log & Streams Integration (`pkg/wal`, `docs/streams.md`):**
   - Serves as the write-ahead log for all filesystem mutations (file creations, writes, truncates, renames, unlinks).
   - Enables fast multi-tier acknowledgements (`Local`, `Witness`, `Permanent`) for write operations before large EROFS snapshot generation.

4. **Content-Addressable Blob Storage (`pkg/objectfs/blob`):**
   - Handles storage, compression, and retrieval of file contents addressed exclusively by their cryptographic hash (SHA-256).
   - Partitions blobs into standalone loose objects (for files > 64MB) and consolidated packfiles (for files <= 64MB).

5. **Cloud Object Storage Backend (`pkg/objectstore`):**
   - Pluggable storage abstraction supporting Google Cloud Storage (GCS), Amazon S3, local filesystem emulation, and in-memory test backends.

---

## Metadata Architecture & Management

A core strength of ObjectFS is separating metadata operations from data payloads, eliminating the high latency and eventual-consistency hazards typical of naive object-storage-backed filesystems.

### 1. The Metadata Change-Log (Streams Layer)

Every filesystem operation that mutates state—such as `mkdir`, `create`, `write`, `truncate`, `rename`, or `unlink`—is modeled as a discrete record appended to the volume's metadata change-log.

```
+-----------------------------------------------------------------------------------+
|                            Metadata Mutation Stream                               |
|                                                                                   |
|  [Seq 101: MKDIR /models] ──> [Seq 102: CREATE /models/config.json (SHA: 0a1b...)]|
|                                                                                   |
|  ──> [Seq 103: WRITE /models/weights.bin (offset: 0, len: 4MB, SHA: 8f3e...)]     |
|                                                                                   |
|  ──> [Seq 104: RENAME /temp/out.tmp -> /models/out.final] ──> ...                 |
+-----------------------------------------------------------------------------------+
```

- **Low Latency & High Durability:** Mutations are immediately durable upon being written to the Streams buffer without waiting for full cloud object flush cycles.
- **Ordered Replay & Recovery:** If the controller crashes or restarts, the filesystem state is deterministically reconstructed by loading the last EROFS snapshot and replaying subsequent change-log records from the Streams layer.
- **Cross-Node Event Notification:** The change-log acts as the event source driving `WatchVolume` invalidation notifications across all cluster worker nodes.

> **Partial Writes on Large Files (TODO):** An important architectural trade-off is how to handle partial writes or in-place modifications to large files (e.g. updating a 4KB block in a 100MB file). Potential approaches include:
> - **Full Copy-on-Write Rewrite:** Simple and preserves single-blob integrity, but incurs write amplification on small edits to large files.
> - **Chunked Blobs / Extent Maps:** Splitting large files into fixed-size or Content-Defined Chunking (CDC) blocks, updating only dirty chunks and indexing chunk hashes in metadata.
> - **Append-Only Delta Log:** Recording write deltas in the Streams change-log and compacting them during periodic snapshot creation.

> **Note on Implementation Roadmap:** ObjectFS currently maintains volume state in memory and flushes snapshots periodically to object storage. Integrating the Streams layer ([`docs/streams.md`](streams.md)) as the live, authoritative change-log is the primary next step for ObjectFS metadata durability and multi-writer synchronization.

### 2. Periodic Snapshots in EROFS Format

To prevent the metadata change-log from growing indefinitely and to provide instant point-in-time recovery, ObjectFS periodically compiles the filesystem hierarchy into an immutable **EROFS (Enhanced Read-Only File System)** snapshot image.

#### Why EROFS?
- **Deterministic & Compact:** EROFS is a lightweight, read-only filesystem format supported in Linux kernels (>= 5.4). It organizes inodes, directory tables, and extended attributes with minimal space overhead.
- **Composefs Alignment:** ObjectFS follows the Composefs pattern: the EROFS snapshot contains directory hierarchies, file names, sizes, modes, and timestamps, but **contains no file data blocks**.
- **Digest Association via Extended Attributes:** Inode data associations are embedded directly in EROFS xattrs:
  - `user.digest`: The SHA-256 content hash of the blob.
  - `user.sha256`: Duplicate digest xattr for broad tool compatibility.

#### Snapshot Storage Hierarchy
Snapshots are written to cloud object storage under the volume metadata prefix:
- `volumes/<volumeID>/meta/<timestamp>.erofs` (e.g. `volumes/vol-1/meta/20260925T120000.000000Z.erofs`)
- `volumes/<volumeID>/meta/latest` (Text pointer containing the name of the most recent valid snapshot; TODO: eliminate this file in favor of direct snapshot discovery)

---

## Content-Addressable Blob Storage & Packfiles

ObjectFS stores all file data as content-addressable blobs. Content addressing provides automatic cross-file and cross-volume **data deduplication**: identical files or identical chunks uploaded across multiple containers share the same underlying storage.

### Git-Inspired Storage Model: Loose Blobs vs. Packfiles

In cloud object storage, storing millions of individual small objects introduces severe performance degradation due to API call overhead, rate-limiting, and cost amplification. ObjectFS resolves this using a model inspired by Git:

```
                                  File Write Request
                                          │
                         Is Blob Size > 64 MB (Large Threshold)?
                                   /              \
                            YES   /                \   NO
                                 /                  \
                                v                    v
                    [ Standalone Loose Blob ]   [ Aggregate into Packfile ]
                    blobs/<sha256>              blobs/<packID>-<N>.pack
```

1. **Standalone Loose Blobs (Large Files > 64MB):**
   - Stored directly as individual objects keyed by `blobs/<sha256>`.
   - Allows direct streaming, HTTP range queries, and signed cloud redirect URLs for high-throughput parallel reads.
2. **Packfiles (Small & Medium Files <= 64MB):**
   - Batched into a consolidated `.pack` file containing multiple blobs.
   - Named `blobs/<packID>-<count>.pack` (where `count` indicates the number of blobs).
   - Contains a sorted index table enabling $O(\log N)$ binary search lookup without reading data payloads.

### Binary Wire & File Formats (`OBJB`)

All loose blob files and packfiles share the `OBJB` binary format (big-endian integer fields).

#### 1. File Base Header (8 Bytes)
```
+--------------------+-------------------+--------------------+--------------------+
|    Magic (4B)      |   Version (1B)    |   FileType (1B)    |   Reserved (2B)    |
|   0x4F424A42       |       0x01        |  1 (Blob) / 2 (Pack|       0x0000       |
+--------------------+-------------------+--------------------+--------------------+
```

#### 2. Standalone Loose Blob Format (64-Byte Header + Payload)
```
+-----------------------------------------------------------------------------------+
| Base Header (8B) | Encoding (4B) | FinalLength (8B) | SHA256 (32B) | Reserved (12B)|
+-----------------------------------------------------------------------------------+
|                                 Data Payload                                      |
+-----------------------------------------------------------------------------------+
```
- **Encoding:** `0 = Raw Uncompressed`, `1 = zlib Compressed`, `2 = Chunked`.

#### 3. Packfile Format (`.pack`)
```
+-----------------------------------------------------------------------------------+
| Base Header (8B) | Count N (4B) | Reserved (4B)                                   |
+-----------------------------------------------------------------------------------+
| Table 1: Sorted SHA256 Array (N * 32 Bytes)                                       |
| [ SHA_0 (32B) ] [ SHA_1 (32B) ] ... [ SHA_N-1 (32B) ]                             |
+-----------------------------------------------------------------------------------+
| Table 2: Data Offset Table ((N + 1) * 4 Bytes)                                    |
| [ Offset_0 (4B) ] [ Offset_1 (4B) ] ... [ Offset_N (4B = EOF Offset) ]           |
+-----------------------------------------------------------------------------------+
| Data Payload Section (Contiguous Blob Items)                                      |
|   +-----------------------------------------------------------------------------+ |
|   | Item 0: Encoding (4B) | FinalLen (4B) | Reserved (8B) | Payload Data ...    | |
|   +-----------------------------------------------------------------------------+ |
|   | Item 1: Encoding (4B) | FinalLen (4B) | Reserved (8B) | Payload Data ...    | |
|   +-----------------------------------------------------------------------------+ |
+-----------------------------------------------------------------------------------+
```

- **Two-Phase Binary Search:** To read a blob from a packfile, ObjectFS reads only the index tables (Header + Table 1 + Table 2), executes binary search over the sorted SHA array to locate the item index `i`, retrieves the exact byte range `[Offset_i, Offset_{i+1})`, and streams only the requested payload.

---

## Mounting, Client Architecture & Caching

### 1. FUSE Filesystem Driver (`pkg/objectfs/fuse`)
The node daemon mounts ObjectFS volumes into container pod paths using `go-fuse`. The driver maps POSIX operations to gRPC calls:
- `Lookup` / `GetAttr` -> Retrieves inode attributes from the controller or local cache.
- `ReadDir` / `ReadDirPlus` -> Enumerates directory contents with populated inode attributes.
- `Read` -> Fetches byte ranges from the local cache or requests content streams from the controller.
- `Write` / `Truncate` -> Buffers changes locally and transmits modifications to the controller.
- `Fsync` / `Flush` -> Forces synchronization of dirty buffers to the cluster service.

### 2. Local Caching & Scratch Storage (`NodeCache`)
Each node daemon and controller maintains a local cache backed by memory and dedicated scratch disk storage:
- **Scratch Disk LRU Blob Caching (TODO):** Blobs and packfile byte ranges are cached in an allocated local scratch directory on disk. Eviction runs on an LRU basis to remain within fixed disk capacity limits. The controller similarly caches frequently requested blobs locally to reduce cloud storage egress.
- **Read Caching:** Small files and frequently accessed blocks are held in memory to satisfy repeated reads locally with zero network round-trips.
- **Dirty Write Buffering:** Writes are aggregated in memory to support high-throughput sequential and random modifications.
- **Cache Invalidation:** Subscribes to the controller's `WatchVolume` gRPC push notification stream. When another node modifies, renames, or deletes a file, the node daemon invalidates its local cache entries.

### 3. Configurable Write-Through Modes
ObjectFS supports three distinct write synchronization modes:
1. `WRITE_THROUGH_FSYNC`: Writes are buffered locally in node memory and synchronously flushed to the controller upon `fsync(2)` or `close(2)`.
2. `LAZY_WRITE`: Writes are buffered asynchronously and flushed in background batches, maximizing write throughput for scratch and temporary workloads.
3. `EAGER_REPLICATION`: Writes are streamed immediately to the central controller memory and change-log, providing cross-node visibility and crash resilience without waiting for object store commits.

---

## Protocol Specification

The gRPC contract between `objectfs-node-daemon` and `objectfs-controller` is defined in [`proto/objectfs.proto`](../proto/objectfs.proto):

| Category | RPC Method | Description |
| :--- | :--- | :--- |
| **Metadata Operations** | `GetAttr` | Fetches inode attributes for a path. |
| | `Lookup` | Finds a named child entry inside a parent directory. |
| | `ReadDir` | Lists entries within a directory. |
| | `Mkdir` | Creates a new directory. |
| | `Unlink` | Removes a file entry. |
| | `Rmdir` | Removes an empty directory. |
| | `Rename` | Atomically renames or moves a file or directory. |
| | `WatchVolume` | Server-streaming RPC pushing real-time filesystem change events to nodes. |
| **Data Operations** | `CreateFile` | Creates a new regular file with optional initial content. |
| | `ReadFile` | Reads byte slices or obtains direct download redirect URLs. |
| | `WriteFile` | Writes byte slices at specified offsets with configurable write modes. |
| | `TruncateFile` | Resizes a file to a specified length. |
| | `Fsync` | Flushes uncommitted file writes to durable storage. |
| **Blob & Snapshot APIs**| `ListBlobs` | Enumerates available content-addressable blob SHAs. |
| | `GetBlob` | Server-streaming download of raw or packed blob payloads. |
| | `ListVolumes` | Discovers available volumes across the cluster. |
| | `ListSnapshots` | Lists point-in-time EROFS snapshots for a volume. |
| | `CreateSnapshot` | Manually triggers an immediate EROFS snapshot compilation. |

---

## Roadmap & Next Steps (TODOs)

The following items represent the planned roadmap and architectural evolution for ObjectFS:

### 1. Integration with the Streams Layer (Metadata Change-Log)
- [x] **Wire Streams as Authoritative Change-Log:** Replace the current in-memory mutation log with the Streams service ([`docs/streams.md`](streams.md)). Every `mkdir`, `create`, `write`, `rename`, and `unlink` will be logged as an append-only Streams record with configurable durability (`Local`, `Witness`, `Permanent`).
- [ ] **Deterministic Crash Recovery:** Rebuild controller memory on startup by mounting the latest EROFS snapshot and replaying outstanding change-log records from the Streams log.
- [ ] **Multi-Writer Ordering:** Use the central Streams buffer sequence numbers to establish linearizable ordering across multiple concurrent node writers.
- [ ] **Eliminate "latest" Snapshot Pointer Object:** Remove the `volumes/<volID>/meta/latest` pointer file; discover the most recent valid snapshot via lexicographical listing or timestamp markers to avoid single-object update contention.
- [ ] **Tiered Metadata Caching (Hot in Memory, Cold on Disk):** Refactor the controller metadata engine so only "hot" active directories and inodes are kept in RAM, while the full metadata state resides on fast local disk (replicated from cloud EROFS snapshots) to scale to millions of files without unbounded memory usage.

### 2. Convergence with AgentFS (Kernel-Native EROFS Mounting)
- [ ] **Bypass User-Space FUSE Overhead:** Instead of routing all node I/O through a user-space FUSE daemon, converge with AgentFS by distributing the compiled EROFS metadata snapshot directly to worker nodes.
- [ ] **Kernel-Level Lazy-Loading & OverlayFS:** Mount the EROFS snapshot image directly as a read-only `lower` layer in an OverlayFS mount, combining kernel-native filesystem performance with on-demand blob fetching (via `fanotify`, `fscache`, or blob drivers) when file contents are accessed.

### 3. Enlightened Applications & CAS Unix Domain Socket (UDS) API
- [ ] **In-Filesystem Control Socket:** Expose a well-known Unix Domain Socket path inside the mounted volume (e.g. `/.in-cluster-storage/api` or `/.objectfs/api`).
- [ ] **Zero-Copy `SCM_RIGHTS` Blob Retrieval:** Allow enlightened applications (such as AI model runtimes, build systems, or data processors) to query cached blobs directly by SHA-256 over the socket. The node daemon transfers an open file descriptor (`SCM_RIGHTS`) of the locally cached blob directly to the application, enabling zero-copy `mmap`/`read` access without data copying through FUSE or user-space memory buffers.

### 4. Blob Storage, Caching & Write Handling
- [ ] **Local Scratch Storage & LRU Blob Caching:** Allocate a fixed-size local scratch disk volume on worker nodes and controller pods to cache downloaded loose blobs and packfile slices on an LRU basis, avoiding repeated network transfers from cloud storage.
- [ ] **Partial Write Strategies for Large Files:** Evaluate and design trade-offs for handling partial writes to large files: evaluate whether to perform full copy-on-write rewrites, introduce chunked blob index indirection (fixed-size chunks or Content-Defined Chunking / Rabin fingerprints), or record write deltas in the change-log.
- [ ] **Multipart Blob Uploading:** Implement parallel multipart uploads to S3 and GCS for large files (> 64MB) to maximize network bandwidth.
- [ ] **Signed Direct-Download Redirects:** Enhance `ReadFile` and `GetBlob` with presigned URLs for client-side direct parallel streaming from cloud object storage, bypassing controller network bottlenecks.
- [ ] **Packfile Compaction & Garbage Collection:** Periodically scan object storage packfiles to identify and purge unreferenced blobs from deleted files or pruned snapshots, rewriting active blobs into consolidated new packfiles.

### 5. Distributed Leasing & File Locking
- [ ] **File Locking Support (Advisory vs Mandatory):** Evaluate whether ObjectFS should support POSIX file locking (`fcntl(F_SETLK)` / `flock`). Explore trade-offs of centralized lock management in the controller (with heartbeat leases and failover cleanup) vs. client-side or distributed locking mechanisms.
- [ ] **Distributed Read/Write Leases:** Introduce delegation leases allowing node daemons to cache files locally for writes and reads with optimistic concurrency until invalidated by conflicting leases.

### 6. Extended Attributes (xattr) & POSIX ACLs
- [ ] Support full `getxattr`, `setxattr`, `listxattr`, and `removexattr` operations in the FUSE driver and controller, preserving security labels and custom application metadata across snapshots.
