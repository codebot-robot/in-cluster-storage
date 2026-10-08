# Periodic Per-Stream Files from Aggregated WAL

This document describes the design and lifecycle for periodic per-stream WAL files in `wal-buffer`, enabling cheap per-stream history reads, fast crash recovery, and snapshot-then-trim retention policies.

---

## Background & Problem

The `wal-buffer` aggregates incoming records across all active streams into a unified write-ahead log. It commits batches with `fsync` locally and flushes aggregated segments to object storage under `wal/segments/<first_pos>-<last_pos>.wal`.

While aggregating all streams maximizes write throughput via group commits, reading historical records for a single stream (e.g. controller recovery after failover (#185), stream replay, or `sds cat`) previously required listing and scanning every aggregated segment since position $P$. The scan cost was proportional to the total cluster traffic across all streams rather than the specific stream being read.

## Architecture & Lifecycle

Instead of splitting aggregated segments asynchronously after flushing, the only synchronous write on the critical path is to the aggregated WAL file. As records arrive, `wal-buffer` asynchronously writes them off the critical path to per-stream local files, uses the local filesystem as a per-stream cache, and periodically uploads those per-stream files to object storage upon sealing.

```
+-----------------------------------------------------------------------------------+
|                               wal-buffer Service                                  |
|                                                                                   |
|  Incoming Records                                                                 |
|         |                                                                         |
|         +------------------------+                                                |
|         |                        |                                                |
|         v (Critical Path)        v (Off Critical Path Write-Behind)               |
|  [ Aggregated WAL ]       [ In-Memory Stream Buffer ]                             |
|  (Local Fsync Commit)            |                                                |
|         |                        v (Append to Local Cache)                        |
|         |                 streams/<uuid>/from<from>.wal (Open)                    |
|         |                        |                                                |
|         |                        | Seal (hourly / size threshold)                 |
|         |                        v                                                |
|         |                 streams/<uuid>/from<from>-to<to>.wal (Sealed)           |
|         |                        |                                                |
|         v (Flush)                v (Upload)                                       |
|  wal/segments/*.wal       streams/<uuid>/from<from>-to<to>.wal                    |
|  (Object Storage)         (Object Storage)                                        |
+-----------------------------------------------------------------------------------+
```

### 1. Write Path

1. **Aggregated Log (Source of Truth):**
   - Incoming records from all streams are batched, sequenced with a monotonic global `position`, written to `LogSegmentStore`, and `fsync`ed.
   - The witness acknowledgement (`witness_acked_stream_seq`) is returned to the client as soon as the aggregated batch fsync completes.
   - The aggregated log remains the authoritative source of truth for all records that have not yet been sealed into immutable per-stream files.

2. **Per-Stream Open File (Write-Behind Cache):**
   - Each committed record is concurrently queued into an in-memory per-stream buffer.
   - A write-behind worker appends queued records to an open file:
     `streams/<stream-id>/from<from>.wal`
   - File descriptors are not kept open continuously across thousands of streams; files are opened in append mode (`O_WRONLY|O_APPEND|O_CREATE`), written in micro-batches, and closed (or recycled via a bounded handle pool).
   - Appending is off the latency-critical path, so witness ACKs do not wait on per-stream disk operations.

### 2. Object Layout & Naming Convention

Directories are kept small by partitioning per stream:

- **Local Path:** `<dataDir>/streams/<stream-id>/`
- **Object Storage Prefix:** `streams/<stream-id>/`
- **Open File:** `from<from>.wal` (where `from` is zero-padded to 20 digits, e.g. `from00000000000000000001.wal`).
- **Sealed File:** `from<from>-to<to>.wal` (e.g. `from00000000000000000001-to00000000000000000100.wal`).

**Format Properties:**
- Both `<from>` and `<to>` are zero-padded 20-digit decimals (`%020d`), ensuring that lexicographical sorting in directory listings and object storage prefix listings strictly equals numeric sequence order.
- The state of the file is explicit from its name: a file matching `from*-to*.wal` is sealed and immutable; a bare `from*.wal` is open and active.
- Records in per-stream files use the existing binary `wal.LogRecord` format with Castagnoli CRC32C checksums, enabling zero-copy reuse of decoder, validation, and torn-tail recovery routines.

### 3. Sealing

Per-stream files are sealed when either of the following conditions is met:
- **Time Threshold:** The file has been open for the configured duration (default: 1 hour, configurable via `ServerConfig.StreamSealInterval`).
- **Size Threshold:** The file exceeds the configured size limit (default: 64 MB, configurable via `ServerConfig.StreamSealBytes`).
- **Shutdown / Flush:** Clean shutdown flushes and seals active stream files with uncommitted data.

**Sealing Sequence:**
1. Flush in-memory stream buffer to the open file and `fsync` (`f.Sync()`).
2. If the open file contains 0 records (idle stream), skip sealing to prevent generating empty objects in object storage.
3. Atomically rename the file from `from<from>.wal` to `from<from>-to<to>.wal`.
4. Upload `from<from>-to<to>.wal` to object storage at `streams/<stream-id>/from<from>-to<to>.wal`.
5. Open the next active file at `from<to+1>.wal`.

### 4. Crash Recovery

- **Sealed files are immutable and trusted:** If a local sealed file exists on startup, it is verified. If it was not yet uploaded to object storage, the uploader finishes uploading it.
- **Open files:** On buffer service restart:
  - If a trailing torn record exists due to an abrupt node crash, `ScanLogSegmentFile` truncates the file back to the last valid record boundary and backs up the corrupted version.
  - The highest valid `stream_seq` in the open file (or `to` of the latest sealed file if no open file exists) is determined.
  - Any subsequent records present in the local aggregated log or flushed aggregated segments are re-appended to catch up the per-stream file.
- **Idempotent uploads:** Uploading a sealed file is idempotent because the filename deterministically reflects the exact stream sequence range `[from, to]`.

### 5. Reads (`Tail` by `stream_id`)

When a client calls `Tail` specifying `stream_id` and `from_stream_seq`:
1. Find relevant files covering sequences $> \text{from\_stream\_seq}$ in sequential order:
   - Check local sealed files in `streams/<stream-id>/`.
   - Check object storage listing under `streams/<stream-id>/` for older files evicted locally.
   - Check the local open file for recent records.
   - Listen for live incoming records from the in-memory commit notifications.
2. Stream records directly without scanning irrelevant aggregated segments.

This eliminates the full-WAL scan overhead on controller failover and stream replay. Global (unfiltered) `Tail` requests serve as a position-ordered tooling/debug path (used only by `wal-client-test` and tests), while all stream consumers use the per-stream path.

### 6. Retention and Garbage Collection

- **Aggregated Segments:** An aggregated segment `wal/segments/<first>-<last>.wal` becomes safe to delete once every stream that contributed records to that segment has a sealed per-stream file covering at least up to its highest sequence in that segment.
- **Local Per-Stream Files:** Once uploaded to object storage, local sealed files can be evicted under an LRU disk space budget and re-downloaded on demand if needed.
- **Object Storage Retention ("Snapshot-Then-Trim"):**
  - **Default:** Retain all history indefinitely. No records are deleted automatically.
  - **Snapshot Guardrail:** Records MUST NEVER be deleted unless covered by a published snapshot (`stream_seq <= snapshot_position`).
  - **Explicit Policy:** Deletion of per-stream files in object storage only occurs when an explicit retention policy is configured and a snapshot covering that range has been durably published.
