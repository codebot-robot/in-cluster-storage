---
title: "Projections"
kicker: "Snapshots & Materializations"
summary: "Deriving specialized read representations—SQLite for OLTP, Parquet for OLAP, Iceberg for lakehouses, and EROFS for filesystems—from one authoritative stream."
status_note: "Design proposal with experimental Go projectors."
next_url: "/case-study/"
next_title: "Case Study: ObjectFS"
---

In Structured Data Streams, the stream is the single source of truth. A **projection** is a deterministic engine that consumes the stream and materializes its state as an immutable snapshot at position $P$:

```
                             ┌───► SQLite Snapshot (OLTP Point Lookups)
                             │
Structured Stream ──[Project]────► Parquet Directory (OLAP Scans / DuckDB)
                             │
                             ├───► Iceberg Table Commits (Data Lakehouse)
                             │
                             └───► EROFS Image (Filesystem Tree)
```

Every snapshot:
- Is taken at a position $P$ where no transaction is pending.
- Carries the complete **type registry in force at $P$** (the `TypeDefinition` of every active table).
- Is stored immutably in object storage named by position:  
  `streams/<stream-uuid>/snapshots/<format>/<position-20-digits>.<ext>`

---

## 1. SQLite Projection (OLTP)

SQLite provides low-latency local reads, point lookups by primary key, secondary indexes, and SQL expressions.

### Schema & Bookkeeping

Tables are dynamically generated from the registered Protobuf descriptors:
- Scalar fields become SQL columns (e.g. `int64` $\rightarrow$ `INTEGER`, `string` $\rightarrow$ `TEXT`).
- The fields listed in `key_fields` become the table's `PRIMARY KEY`.
- Two metadata bookkeeping tables are included:
  - `_stream_types`: persists `(id, name, fingerprint, descriptors, key_fields)`.
  - `_stream_position`: stores `(stream_id, position, snapshot_time)`.

### Idempotent Apply Rules

When replaying records into SQLite:
- `INSERT` $\rightarrow$ `INSERT OR REPLACE INTO table ...`
- `UPDATE` $\rightarrow$ `UPDATE table SET ... WHERE key`, falling back to `INSERT` if the row does not yet exist.
- `DELETE` $\rightarrow$ `DELETE FROM table WHERE key`.
- Autocommit records (`tx_id = 0`) execute in their own SQLite transaction; multi-row batches commit on `TxCommit`.

### Serving Architecture

A serving node downloads the latest SQLite snapshot from object storage, opens it in read-only mode, and establishes a live `Tail` gRPC stream to the storage buffer. New mutations are continuously applied into the local database with sub-millisecond lag.

---

## 2. Parquet Projection (OLAP)

Columnar formats like [Apache Parquet](https://parquet.apache.org/) are optimized for bulk analytical scans, aggregations, and vectorized execution engines like DuckDB, Polars, and Apache Spark.

### Layout & Self-Description

- One Parquet directory/file per table per snapshot.
- The `TypeDefinition` registry and stream sequence watermark are encoded directly into the Parquet file footer's `key_value_metadata`.
- Any external analytics engine can read the Parquet file without a sidecar catalog or out-of-band schema repository.

### Type Mapping

| Protobuf Field | SQLite Type | Parquet Physical / Logical Type |
| :--- | :--- | :--- |
| `bool` | `INTEGER` (0 or 1) | `BOOLEAN` |
| `int32` / `int64` / enums | `INTEGER` | `INT32` / `INT64` |
| `uint32` / `uint64` | `INTEGER` (two's complement) | `INT64` (`UINT_64` annotation) |
| `float` / `double` | `REAL` | `FLOAT` / `DOUBLE` |
| `string` | `TEXT` | `BYTE_ARRAY` (`UTF8`) |
| `bytes` | `BLOB` | `BYTE_ARRAY` |
| `google.protobuf.Timestamp` | `INTEGER` (microseconds) | `INT64` (`TIMESTAMP_MICROS`) |

### Incremental vs. Rewrite Snapshots

- **Append-only tables:** Snapshots can be generated incrementally—each snapshot writes only rows appended since the last checkpoint, and query engines scan the union.
- **Mutable tables:** Re-written periodically or handled via merge-on-read delete files.

---

## 3. Iceberg: The Lakehouse Upgrade

Writing the OLAP projection directly as an [Apache Iceberg](https://iceberg.apache.org/) table is a natural evolution of the Parquet projector:

1. **Snapshots as Commits:** Each stream snapshot translates to an Iceberg table commit with the stream position recorded in the commit summary.
2. **Delete Files:** Iceberg's native positional and equality delete files resolve mutations and deletes efficiently without rewriting full Parquet data files.
3. **Ecosystem Interoperability:** Any Iceberg-compatible query engine (Trino, Snowflake, BigQuery, Spark) queries the stream's projected history natively.

---

## 4. EROFS: Filesystems as Projections

The POSIX filesystem metadata tree in ObjectFS is also an SDS projection. Instead of building custom image assemblers, ObjectFS represents files and directories as stream tables:
- `Inode` table (permissions, size, timestamps, data pointers).
- `DirEntry` table (parent inode, name, child inode).

A snapshot compiler builds a read-only [EROFS](https://erofs.docs.kernel.org/) filesystem image. Directory contents are easily queried with standard SQL queries over SQLite, and the Linux kernel mounts the compiled EROFS image with native page cache sharing.
