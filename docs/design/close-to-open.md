# Design Note: Close-to-Open Consistency as a StorageClass Option

## 1. Semantics

A `consistency: close-to-open` StorageClass parameter guarantees that if process $P_1$ on node $N_1$ writes to file $F$ and successfully completes `close(2)` (serviced via FUSE `Release`), any subsequent `open(2)` of $F$ by process $P_2$ on node $N_2$ starting after $P_1$'s `close(2)` returns will observe all modifications made to $F$ prior to that close.

What it would **not** promise:
- **Concurrent writers:** POSIX close provides no multi-writer concurrency control. Multiple nodes writing concurrently to the same file or volume without application-level synchronization still exhibit last-writer-wins or interleaved chunk commits; close-to-open only establishes ordering when an open begins strictly *after* a prior close returns.
- **mmap writes:** Dirty pages dirtied via `mmap(2)` are flushed only upon explicit `msync(2)` or `munmap(2)`. The kernel does not flush dirty memory mappings on descriptor `close(2)`. Close-to-open does not flush un-synced dirty mappings.
- **Directory operations:** Directory mutations (`mkdir`, `rmdir`, `unlink`, `rename`, `create`) are linearizable and committed and acknowledged at the configured durability level (witness by default) via synchronous controller RPCs before returning. They are not gated on file close.
- **Symlinks:** Symlink creation and evaluation are metadata-only operations committed synchronously via controller RPCs, independent of file handle lifecycles.

## 2. Mechanism Options and Costs

### Option A: Close Blocks on Pack Upload and WAL Commit
FUSE `Release` triggers a synchronous chunk push and blocking `Fsync` RPC before returning. The controller packs dirty chunks, uploads them via `PutBlobs` to cloud storage, and commits metadata to the WAL.
- **Trade-offs:** Strongest durability model. However, every close incurs an object-store PUT and a WAL round trip. This creates high write amplification and pack fragmentation (violating the 64 MiB coalescing target and write-amplification guidelines) and defeats the coalesce window by forcing immediate commits on single-file closes.
- **4 KiB file:** Inlined into WAL transaction ($4\text{ KiB} \le \text{maxInlineLen}=4096$). **Blob count:** 0 blobs. **Close latency:** ~5–15 ms (gRPC push + WAL replication round trip). If non-inlined: 1 pack blob, ~35–95 ms.
- **64 MiB file:** **Blob count:** 1 pack blob (64 MiB). **Close latency:** ~300–850 ms (chunk push + object store PUT + WAL commit).

### Option B: Close Blocks on Metadata Commit Only (Staged Chunk Serving)
Close pushes dirty chunks to controller staging and commits metadata rows referencing staged chunks to WAL immediately. Packfile upload to object storage occurs asynchronously in the background. Remote nodes read un-packed chunks directly from controller staging memory via `ReadFile`.
- **Trade-offs:** Yields weaker durability than visibility, which the ObjectFS contract explicitly forbids ("no logged row ever references a blob that is not durable in cloud storage").
- **Controller restart hazard:** If the controller restarts or crashes before background pack upload completes, staged in-memory chunks are lost. Because the WAL already committed rows referencing these chunks, recovery produces dangling references and subsequent reads return `EIO` or silent corruption. This failure mode is **unacceptable** for a reliable filesystem.
- **4 KiB file:** Inlined into WAL. **Blob count:** 0 blobs. **Close latency:** ~5–15 ms.
- **64 MiB file:** **Blob count:** 0 blobs at close (1 uploaded asynchronously later). **Close latency:** ~150–320 ms (gRPC push + WAL commit).

### Option C: Asynchronous Close with Controller-Owned Open Barrier
Close remains asynchronous on writer node $N_1$, signaling the controller to flush staging. When node $N_2$ issues an `Open` RPC for that inode:
1. **Barrier Owns the Sync:** The open barrier cannot passively wait for a writer sync that might never arrive if the writer daemon is slow, crashed, or partitioned. If staged chunks exist or a sync is in-flight for that inode, the controller *itself* initiates and drives the immediate coalesced sync (using the immediate path from #331).
2. **Bounded Wait & Degradation:** The barrier needs its own bound rather than inheriting `--controller-sync-timeout` (10 minutes on both binaries, which is an unacceptable hang for `open(2)`). We propose a dedicated `--open-barrier-timeout` flag with a single-digit second default (e.g. 5 s), as it only needs to cover pack upload plus WAL ack for that inode (#338 makes close sync per-inode). If this timeout expires before sync completes, `Open` fails with `EIO` rather than exposing a stale pre-close view. If the volume is degraded (`walclient.DegradedError`, #327), the barrier immediately returns `EIO`.
3. **No Added Durability on Close:** Close-to-open provides visibility ordering, **not** crash durability. When `close(2)` returns on $N_1$, uncommitted data resides only in controller staging memory and the writer daemon's cache. If both crash before the sync commits, uncommitted data is lost despite successful close. Applications needing durability must still call `fsync(2)`.
4. **Reader Cache Invalidation:** Node $N_2$ may have cached attributes (e.g. `size=0`) and pages under `FOPEN_KEEP_CACHE` (up to `--attr-timeout=60s`). Asynchronous `WatchVolume` invalidations have no ordering guarantee against `Open` RPC return. Therefore, `OpenResponse` must return `EntryAttr attr` and `uint64 stream_position`. The reader daemon updates its attributes and purges cached kernel pages before answering FUSE `Open`.
- **4 KiB file:** **Close latency:** 0 ms. **Remote open latency:** ~5–15 ms (inlined into WAL, 0 blobs).
- **64 MiB file:** **Close latency:** 0 ms. **Remote open latency:** ~300–850 ms (only if opened immediately; 1 consolidated pack blob).

## 3. Plumbing

- **Delivery to Node Daemon:** Configured via a StorageClass parameter (e.g. `parameters: consistency: close-to-open`). The CSI provisioner copies StorageClass parameters into PV `spec.csi.volumeAttributes`. Kubelet passes these attributes in `NodePublishVolumeRequest.volume_context` to `objectfs-node-daemon`, matching existing patterns like `mode: exclusive-local` and `writeMode` in `cmd/objectfs-node-daemon/main.go`.
- **Volume vs. Mount Scope:** Scoped per volume through StorageClass/PV parameters. The volume consistency mode is registered with the controller during volume initialization or mount registration, allowing the controller to enforce the open barrier on that volume.
- **Mutability on Existing Volumes:** In Kubernetes, StorageClass parameters and PV `volumeAttributes` are immutable after creation. Changing the consistency mode of an existing volume requires manual PV attribute updates and pod recreation (remount). Runtime per-mount changes across nodes are discouraged because divergent writer/reader assumptions cause inconsistent semantics.

## 4. Interaction with Existing Modes

- **Exclusive-Local Mode (`mode: exclusive-local`):** Uses `ReadWriteOncePod` access via node-local OverlayFS. Remote nodes cannot mount or open the volume while in use. On unmount, `unmountCheckpoint` commits all upper-layer modifications to the controller before another node can mount. Cross-node close-to-open is **trivially satisfied** by construction.
- **Degraded Volumes (#327):** If the WAL stream fails and sets a `walclient.DegradedError`, all mutating RPCs return `EIO`. The open barrier detects volume degradation immediately and fails fast with `EIO` rather than hanging on blocked sync waiters.
- **Coalesce Window (#331):** #331 introduces `SyncTrigger.SYNC_TRIGGER_CLOSE` to begin coalesced syncs immediately upon file close. Option C leverages this immediate sync path so reader open barriers only wait for raw pack upload and WAL commit latency rather than the coalesce timer.
- **Durability Levels Matrix (`docs/objectfs.md`):**
  - **`permanent`:** Open blocks until cloud storage WAL seal completes (hundreds of ms to seconds), providing disaster-safe linearizability.
  - **`witness` (default):** Sync completes upon replication to `wal-buffer` disk (~2–10 ms + pack upload). Recommended pairing for close-to-open.
  - **`local`:** Persists to node-local WAL; survives controller restarts on the same node.
  - **`none` (ephemeral shared disk):** Packs upload to object store, but metadata commits only in controller memory. A controller crash loses metadata; close-to-open holds only within the lifetime of the controller process.

## 5. Recommendation

**Recommendation:** Offer `consistency: close-to-open` as an opt-in StorageClass parameter implementing **Option C (Open-Side Barrier)** paired with #331.
- **Reasoning:** Option B is rejected due to data loss and corruption risks upon controller restart. Option A severely degrades write throughput and causes object-storage pack fragmentation (violating write-amplification targets). Option C provides true close-to-open semantics for workloads handing off files between pods without penalizing writer performance or disrupting pack aggregation, provided that: (1) the controller open barrier actively owns and drives the sync (failing with `EIO` on timeout or degradation), and (2) `OpenResponse` returns fresh `EntryAttr` so the reader daemon invalidates stale cached attributes and page cache before answering the kernel. Workloads not requiring cross-node handoff should remain on standard eventual consistency (`consistency: standard`, default).
- **Verification Test:** A two-node E2E test in `tests/e2e/objectfs_e2e_test.go`:
  1. Pod 2 on Node 2 reads/stats the file first to populate its local cache with a stale size (`size=0` or empty content).
  2. Pod 1 on Node 1 writes new content (4 KiB inline and 16 MiB multi-chunk), closes file descriptors without calling `fsync(2)`, and signals Pod 2 on Node 2 via a synchronization channel (zero sleep).
  3. Pod 2 on Node 2 immediately executes `open(2)`, asserting that `open(2)` blocks until data is durable, completes without error, invalidates its stale cached attributes and page cache, and reads the identical bytes and sizes written by Pod 1.
- **Follow-up Implementation Budget:**
  - `proto/objectfs.proto`: ConsistencyMode enum in VolumeInfo, and EntryAttr/stream_position in OpenResponse (~25 lines).
  - `pkg/objectfs/controller`: `--open-barrier-timeout` flag, inode staging sync tracking, controller-owned sync trigger, and open barrier in `Open` (~85 lines).
  - `pkg/objectfs/fuse` / `cmd/objectfs-node-daemon`: CSI volume context parsing, passing consistency mode, and reader cache invalidation on `OpenResponse.Attr` (~45 lines).
  - `tests/e2e`: Unit tests and two-node E2E verification test including stale reader cache invalidation (~120 lines).
  - Total implementation budget: ~275 lines.
