# Streams (WAL) Architecture & Design

This document describes the design, implementation, durability semantics, wire formats, and roadmap for the **Streams** service (historically implemented under the `wal` package) in the `in-cluster-storage` project.

---

## Overview

**Streams** provides a lightweight, high-performance, tiered write-ahead log (WAL) and streaming ingestion system designed for Kubernetes clusters. It combines ultra-low latency local-disk commits, fast in-cluster network replication to a central witness buffer, and asynchronous batch flushing to permanent cloud object storage (e.g. Google Cloud Storage or Amazon S3).

Streams supports flexible acknowledgement modes depending on application consistency and durability requirements:
1. **Mode 1 — Write locally and ack (`Local`):** Acks immediately after appending and fsyncing to fast node-local disk (< 1 ms latency). Asynchronous replication streams records in the background to the central buffer service.
2. **Mode 2 — Ack when written to central service (`Witness`):** Acks once the record is transmitted over gRPC, micro-batched into a group commit, and fsynced to the central witness buffer's local disk (~2–10 ms latency).
3. **Mode 3 — Ack when flushed to permanent object storage (`Permanent`):** Acks only once the record is sealed into an immutable segment object and committed in cloud object storage (typical on-demand GCS/S3 write latency of ~200 ms – 2 s).

Streams enables workloads to achieve:
- **Sub-millisecond write latency:** Workloads can write locally and achieve immediate durability against local process crashes.
- **Resilient in-cluster durability:** Changes are asynchronously streamed over bidirectional gRPC to a central witness buffer (`wal-buffer`) that performs group commits with fsync.
- **Cost-effective permanent retention:** The buffer service aggregates multiple client streams into consolidated log segments and periodically flushes them to object storage along with an atomic manifest.
- **Unified catch-up and real-time tailing:** Consumers can tail merged records in global position order seamlessly across historical object storage segments, buffered local disk segments, and live in-flight memory commits.

---

## Architectural Design

```
+---------------------------------------------------------------------------------+
|                                Kubernetes Node                                  |
|                                                                                 |
|  +--------------------+        Local Disk Fsync                                 |
|  | Application / Pod  | -----------------------------> [ stream-<id>-*.wal ]   |
|  | (e.g. DB / Client) |                               (ClientSegmentStore)      |
|  +--------------------+                                         |               |
|            |                                                    | (S3 Ack GC)   |
|            | (Streams Client Library)                           v               |
|            +==================== gRPC Stream ===================+               |
+--------------------------------------|------------------------------------------+
                                       |
                                       | Bidirectional Append
                                       v
+---------------------------------------------------------------------------------+
|                       Central Witness Service (wal-buffer)                      |
|                                                                                 |
|  +---------------------------------------------------------------------------+  |
|  | Group Commit Engine (Batching & Monotonic Position Assignment)            |  |
|  +---------------------------------------------------------------------------+  |
|         |                                              |                        |
|         v (Local Fsync)                                v (Tail Subscribers)     |
|  [ log-*.wal ]                                   +-------------------+          |
|  (LogSegmentStore)                               | WalBuffer.Tail()  |          |
|         |                                        +-------------------+          |
|         v (Periodic / Size-triggered Flush)                                     |
+---------|-----------------------------------------------------------------------+
          |
          v
+---------------------------------------------------------------------------------+
|                             Permanent Object Storage                            |
|                                                                                 |
|  wal/manifest.json                                                              |
|  wal/segments/000000000001-000000000500.wal                                     |
|  wal/segments/000000000501-000000001000.wal                                     |
+---------------------------------------------------------------------------------+
```

### Key Components

1. **Client Library (`pkg/wal/client`):**
   - Embedded in writer pods or node daemons.
   - Manages a local `ClientSegmentStore` on node storage (`/data/wal/stream-<stream_id>-*.wal`).
   - Appends records locally with `fsync`, guarantees per-stream strictly increasing sequence numbers (`stream_seq`), and handles automatic backpressure if un-flushed data exceeds `maxRetainedBytes`.
   - Maintains a background streaming gRPC connection to `wal-buffer`, replaying unacknowledged records after disconnections or server restarts.
   - Automatically cleans up local segment files once object storage acknowledges durability (`DeleteSegmentsBeforeS3Ack`).

2. **Central Buffer Service (`cmd/wal-buffer`, `pkg/wal/buffer`):**
   - Deployed as a Kubernetes `StatefulSet` or Service (`wal-buffer`).
   - Merges concurrent appends from multiple independent client streams into a single globally ordered sequence of log records identified by a monotonic 64-bit `position`.
   - Uses a micro-batched **group commit loop** (`batchMaxDelay`, `batchMaxSize`) to fsync batches to local disk (`LogSegmentStore`).
   - Maintains an in-memory `Manifest` and periodically packages committed records into immutable segment objects in cloud storage (`wal/segments/<first_pos>-<last_pos>.wal`), atomically publishing `wal/manifest.json`.

3. **Unified Tail Reader (`WalBuffer.Tail`):**
   - Provides a continuous stream of merged records ordered by `position`.
   - Reads transparently across three tiers in chronological order:
     1. Flushed segment objects from object storage (`GetObject`).
     2. Committed log segments from the buffer service's local disk (`LogSegmentStore.ReadFrom`).
     3. Live in-flight commits via real-time waiter notifications.

---

## Durability Levels & Watermarks

Streams defines three progressive durability levels:

| Level | Watermark Name | Description | Latency Profile |
| :--- | :--- | :--- | :--- |
| **`Local`** | `localSeq` | Record is written and fsynced to the client node's local disk. | < 1 ms (NVMe / SSD) |
| **`Witness`** | `witnessSeq` | Record is received by the central buffer service, assigned a global `position`, and fsynced to the buffer's disk. | Network RTT + Group Commit Fsync (~2–10 ms) |
| **`Permanent`** | `s3Seq` | Record is included in a sealed segment uploaded to object storage and committed into `wal/manifest.json`. | Typical cloud object store write latency (~200 ms – 2 s on-demand `Flush()`, or 10–60 s periodic background flush) |

### Lifecycle of an Append

```
Client Appends Payload
       │
       ▼
1. Write & Fsync to local client segment ────> Returns localSeq immediately (Mode 1)
       │
       ▼
2. Send AppendRecord over gRPC stream
       │
       ▼
3. Server batches with other streams & fsyncs to local disk ──> Sends Ack(witnessSeq) (Mode 2)
       │
       ▼
4. Server uploads segment to Object Storage & updates manifest.json ──> Sends Ack(s3Seq) (Mode 3)
       │
       ▼
5. Client deletes local segments whose records <= s3Seq
```

### Client API Reference

See [`pkg/wal/client/client.go`](../pkg/wal/client/client.go) for Go client definitions (`Stream` interface, `Open`, `Append`, `Wait`, `Flush`, `Watermarks`, and `Close`).

---

## Wire & Storage Formats

Streams utilizes zero-overhead, length-prefixed binary records protected by CRC32C (Castagnoli) checksums, leveraging hardware-accelerated CPU instructions on AMD64 (`SSE4.2` / `CRC32`) and ARM64 (`PMULL` / `CRC32`).

All integer fields in record headers are serialized in **big-endian** format (network byte order). Big-endian is standard for network wire formats (e.g. TCP/IP), ensures that natural lexicographical `[]byte` ordering matches numeric ordering, and incurs no measurable CPU overhead as modern AMD64 and ARM64 processors provide single-cycle byte-swapping instructions (`BSWAP` / `REV`).

### 1. Client Record Format (`WALC`)

Stored in client node segment files (`stream-<uuid>-<firstSeq>.wal`):

```
+---------------+-------------------+--------------------+------------------+------------------+-------------------------+
|  Magic (4B)   |  StreamID (16B)   |  StreamSeq (8B)    |   Length (4B)    |   CRC32C (4B)    |     Payload (N bytes)   |
|   "WALC"      |    (UUIDv4)       |      (uint64)      |     (uint32)     |     (uint32)     |                         |
+---------------+-------------------+--------------------+------------------+------------------+-------------------------+
|<--------------------------------- Fixed 36-Byte Header ------------------------------------->|
```

- **Checksum:** Computed over the 36-byte header (with CRC32C field zeroed during calculation) and the payload bytes using Castagnoli CRC32C.
- **Torn Write Protection:** On startup/recovery, `ScanClientSegmentFile` detects trailing partial or corrupted records, creates a backup snapshot (`<file>.<timestamp>.recover`), and truncates the file back to the last clean record boundary.

### 2. Witness & Segment Log Record Format (`WALL`)

Stored in buffer service local cache segments and permanent object store segments (`wal/segments/<first>-<last>.wal`):

```
+---------------+--------------------+-------------------+--------------------+------------------+------------------+-------------------------+
|  Magic (4B)   |   Position (8B)    |  StreamID (16B)   |  StreamSeq (8B)    |   Length (4B)    |   CRC32C (4B)    |     Payload (N bytes)   |
|   "WALL"      |      (uint64)      |    (UUIDv4)       |      (uint64)      |     (uint32)     |     (uint32)     |                         |
+---------------+--------------------+-------------------+--------------------+------------------+------------------+-------------------------+
|<------------------------------------------ Fixed 44-Byte Header ------------------------------------------------->|
```

- **Position:** 64-bit monotonically increasing sequence number assigned globally by the buffer service.

### 3. Object Store Manifest (`wal/manifest.json`)

Stored at `wal/manifest.json` in object storage:

```json
{
  "segments": [
    "wal/segments/000000000001-000000000500.wal",
    "wal/segments/000000000501-000000001000.wal"
  ],
  "last_position": 1000,
  "streams": {
    "6ba7b810-9dad-11d1-80b4-00c04fd430c8": {
      "s3_acked_stream_seq": 450
    },
    "7c9e6679-7425-40de-944b-e07fc1f90ae7": {
      "s3_acked_stream_seq": 550
    }
  }
}
```

---

## Permanent Object Storage Archiving (GCS / S3)

Streams integrates with cloud object storage backends through the unified `pkg/objectstore` abstraction (`objectstore.Backend`).

### Supported Storage Backend Schemes

The buffer service (`wal-buffer`) accepts a `--backend` URL configured via CLI flag or container args:

| Scheme / URL Format | Backend Provider | Description & Parameters |
| :--- | :--- | :--- |
| `gs://<bucket>/<prefix>` or `gcs://<bucket>/<prefix>` | **Google Cloud Storage (GCS)** | Uses Google Cloud SDK (`cloud.google.com/go/storage`). Authenticates automatically via Application Default Credentials (ADC) or Kubernetes Workload Identity. |
| `s3://<bucket>/<prefix>?endpoint=<url>&region=<region>&use_path_style=true` | **Amazon S3 / MinIO** | Uses AWS SDK v2 (`github.com/aws/aws-sdk-go-v2/service/s3`). Supports standard AWS credentials, IRSA/EKS pod identities, and custom endpoints like MinIO (`endpoint=http://minio:9000`). |
| `file:///path/to/dir` | **Local / HostPath Filesystem** | Persists segments and manifest atomically directly into a filesystem directory. |
| `memory://` or `memory` | **In-Memory Storage** | Ephemeral storage used for unit testing and local development. |

### Buffer Archiving Configuration

The `wal-buffer` daemon exposes the following flags to tune archiving and cache behavior:

- `--backend`: Object storage backend URL (e.g. `gs://my-bucket/wal`, `s3://wal-bucket/logs?endpoint=http://minio:9000`, `file:///store`, or `memory://`).
- `--flush-interval`: Maximum duration between automatic segment flushes to permanent storage (default: `60s`).
- `--flush-bytes`: Accumulation byte threshold that triggers an immediate segment flush before the periodic interval (default: `64MB`).
- `--tail-cache-bytes`: Minimum bytes of committed segment files retained on the witness node's local disk after flushing to object storage to accelerate hot `Tail` reads (default: `64MB`).

### Archival Lifecycle

1. **Micro-batched Group Commit:** Incoming client records are validated, assigned global monotonic sequence numbers (`position`), and appended to the local `LogSegmentStore` with `fsync`.
2. **Segment Packaging:** When `flushBytes` accumulates or `flushInterval` elapses (or an explicit client `Flush` RPC is received), all unflushed `WALL` records are serialized into a sealed segment file: `wal/segments/<first_pos>-<last_pos>.wal`.
3. **Atomic Object Put:** The segment is uploaded via `Backend.PutObject` to cloud object storage.
4. **Manifest Publication:** The central manifest (`wal/manifest.json`) is updated with the new segment path, updated `last_position`, and latest per-stream `s3_acked_stream_seq` watermarks, then uploaded to object storage.
5. **Client Notification & GC:** Stream clients receive `Ack(s3Seq)` updates, permitting them to safely garbage-collect local client segment files (`stream-<id>-*.wal`) whose records have been durably committed to permanent object storage.

---

## gRPC Protocol Specification

The gRPC service contract is defined in [`proto/wal.proto`](../proto/wal.proto) (`WalBuffer` service, `Append`, `Flush`, and `Tail`).

### Connection Handshake & Replay Semantics

1. When opening the stream, the client sends `Hello(stream_id)`.
2. The server responds with `HelloAck(witness_acked_stream_seq, s3_acked_stream_seq)`.
3. If the server restarted, `witness_acked_stream_seq` might have reset to the last flushed watermark (`s3_acked_stream_seq`). The client automatically scans its retained records and replays all records where `stream_seq > witness_acked_stream_seq`.
4. Idempotency: The buffer server discards any incoming records with `stream_seq <= witnessSeq` and sends an updated `Ack`.

### Incarnation Safety & Tail Semantics

- **Positions above `manifest.last_position` are provisional:** In the event of a witness crash before flushing to object storage, provisional positions may be reassigned upon restart.
- **Tail clamping:** If a `Tail` request specifies `from_position > manifest.last_position + 1`, the server clamps it to `manifest.last_position + 1` and returns the effective starting position in `resumed_from`.
- **Consumer deduplication:** Consumers of `Tail` must deduplicate records based on `(stream_id, stream_seq)`.

---

## Potential Use Cases

### 1. Continuous Backup of SQL Database WALs
- **PostgreSQL / MySQL / SQLite:**
  - Database engines produce write-ahead logs (e.g. Postgres WAL segments, SQLite WAL frame pages, MySQL binary logs) required for crash recovery and point-in-time recovery (PITR).
  - Rather than uploading multi-megabyte completed segment files periodically (which risks losing up to several minutes of transactions in a disaster), database pods can write streaming log records to Streams with `Local` or `Witness` durability.
  - Achieves **near-zero Recovery Point Objective (RPO)** with minimal impact on database transaction commit latency.

### 2. Underpinning for FUSE-based ObjectFS
- **Distributed Multi-Writer Coordination:**
  - ObjectFS nodes require a low-latency append-only log to record filesystem mutations (file writes, directory creations, renames, unlinks) before aggregating them into immutable read-only layers (like EROFS or CAS blobs).
  - Streams provides the durable write-through channel and ordering foundation for cross-node cache invalidation and metadata synchronization.

### 3. Lightweight Change Data Capture (CDC) & Event Ingestion
- Ingests event streams, audit logs, and operational telemetry from microservices running across the cluster directly into object storage.
- Avoids the resource and operational overhead of provisioning and managing dedicated Kafka, Pulsar, or ZooKeeper clusters for in-cluster streaming.

### 4. Distributed State Machine Replication
- Serves as the shared append-only log for distributed controllers, leader election state synchronization, and reproducible workflow execution logs.

---

## Roadmap & TODO List

- [ ] **Package & Name Refactoring:** Rename `wal` package, proto services, CLI binaries, Docker images, and Kubernetes manifests to `streams` (e.g. `streams-buffer`, `streams-client`, `proto/streams.proto`).
- [ ] **Eliminate Manifest File:** Remove central `wal/manifest.json` from object storage to avoid atomic single-object contention, race conditions, and consistency bottlenecks; explore self-describing segments and prefix listing / atomic markers instead.
- [ ] **Opaque Stream Offsets (Hide Global Monotonic Positions):** Hide global buffer monotonic positions from individual stream clients. Clients should only reason about their own `stream_id` and `stream_seq`. Expose global positions only as opaque resumption tokens/cookies for consumers.
- [ ] **Stress & Chaos Testing:**
  - High-concurrency randomized multi-client benchmark tests to stress group commits and tail subscribers.
  - Chaos testing with simulated network partitions, abrupt client/server pod terminations (`SIGKILL`), and disk full conditions.
  - Property-based fuzzing for record serialization, segment parsing, and torn file recovery.
- [ ] **Security & Transport Encryption:**
  - Support TLS and mutual TLS (mTLS) authentication for gRPC endpoints.
  - Coherent security model with Kubernetes ServiceAccount token authentication or SPIFFE/SPIRE workload identities.
  - Optional client-side payload encryption (e.g. AES-GCM / ChaCha20-Poly1305) ensuring zero-knowledge storage in untrusted object buckets.
- [ ] **Multi-Tenant Stream Isolation & Quotas:**
  - Namespace and stream-level authorization policies.
  - Rate limiting, bandwidth quotas, and storage tiering per stream.
- [ ] **High-Availability (HA) Witness Buffering:**
  - Multi-replica witness clustering (e.g. Raft or quorum replication across multiple buffer pods) to eliminate single-pod buffer restarts during node upgrades.
- [ ] **Compaction & Retention Lifecycle:**
  - Automated segment compaction and retention policies (TTL or max-bytes) for archived stream segments in object storage.
