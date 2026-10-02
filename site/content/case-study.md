---
title: "Case Study: ObjectFS"
kicker: "Real-World Application"
summary: "Expressing a POSIX filesystem as relational tables over a stream, projecting read-only EROFS images for high-performance Kubernetes workloads."
status_note: "In progress. Tracked under Issue #97 and sub-issues #94, #107, #108, #109."
next_url: "implementations/"
next_title: "Implementations & Roadmap"
---

[ObjectFS](https://github.com/gke-labs/in-cluster-storage/blob/main/docs/objectfs.md) is a distributed filesystem designed for Kubernetes workloads, providing read-write shared volumes backed by cloud object storage.

Originally built with a custom mutation log and bespoke snapshotting routines, ObjectFS is being refactored to run as a native **Structured Data Streams** application.

---

## The Filesystem as Relational Tables

Instead of treating filesystem metadata as opaque binary blocks or custom structures, ObjectFS models the directory tree as three standard Protobuf tables registered on an SDS stream:

### 1. The `Inode` Table
Represents file and directory metadata:
- Inode number (stable 64-bit ID, see [Issue #94](https://github.com/gke-labs/in-cluster-storage/issues/94))
- Mode (permissions, file vs directory vs symlink)
- UID / GID, timestamps (atime, mtime, ctime)
- File size and data location (blob digest reference or inline content)

### 2. The `DirEntry` Table
Represents directory hierarchies:
- Parent inode number
- Entry name (e.g. `"Makefile"`, `"src"`)
- Child inode number

### 3. The `Content` Table
Stores small inline file contents (up to the inline size threshold, e.g. 128KB). Larger file payloads are written directly to the content-addressed blob store (CAS) and referenced by SHA-256 digest in the inode's `user.digest` extended attribute.

---

## Atomic Filesystem Operations as Transactions

Complex filesystem operations naturally touch multiple tables. SDS transactions (`TxCommit`) ensure atomic execution:

- **`mkdir /a/b`:** A single transaction that:
  1. Inserts a new row in `Inode` with directory mode.
  2. Inserts a new row in `DirEntry` under parent inode `a`.
  3. Updates parent inode `a`’s modification timestamp and link count in `Inode`.
- **`rename /a/file.txt /b/file.txt`:** Atomically deletes the entry from parent `a`, inserts into parent `b`, and updates the target inode.

If a writer crashes mid-operation, uncommitted transaction rows are discarded by readers during recovery.

---

## Projections in ObjectFS

```
                                  ┌───► SQLite Snapshot (Serving Controller)
                                  │     • Fast path lookups, SQL directory queries
SDS Stream (Inodes + DirEntries) ─┤
                                  └───► EROFS Image (Container Nodes)
                                        • Kernel page cache, zero-copy FUSE reads
```

1. **SQLite Projection (Controller):**  
   The ObjectFS controller maintains an active SQLite projection of the filesystem stream. Finding all files matching a glob or scanning directory trees becomes standard SQL queries with indexed lookups (`WHERE parent_ino = ?`).
2. **EROFS Projection (Container Mounts):**  
   Periodically, a projector compiles an immutable [EROFS](https://erofs.docs.kernel.org/) filesystem image from the SQLite snapshot. Worker nodes mount this EROFS image with kernel-level page cache sharing and direct block-level caching.

---

## Migration Plan & Sub-Issues

The migration of ObjectFS onto SDS is actively tracked under master issue **[#97](https://github.com/gke-labs/in-cluster-storage/issues/97)**:

- **[#94](https://github.com/gke-labs/in-cluster-storage/issues/94):** Stable inode numbers across EROFS snapshots (prerequisite for keying the inode table).
- **[#107](https://github.com/gke-labs/in-cluster-storage/issues/107):** Express ObjectFS metadata as an SDS stream with Inode, DirEntry, and Content tables.
- **[#108](https://github.com/gke-labs/in-cluster-storage/issues/108):** Compile EROFS snapshots as a standard SDS projection.
- **[#109](https://github.com/gke-labs/in-cluster-storage/issues/109):** Replace the circular-buffer local store with the SQLite projection.
