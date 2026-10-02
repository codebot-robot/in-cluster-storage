---
title: "The Idea"
kicker: "Architecture & Fundamentals"
summary: "The log is the single source of truth. Databases, analytics tables, and filesystem images are snapshots of the stream materialized at a position."
status_note: "Design proposal with an experimental implementation."
next_url: "format/"
next_title: "The Wire Format"
---

Most data architectures suffer from the **dual-write problem**: applications write to an operational database, then attempt to emit events to Kafka, update search indexes, and export parquet files for analytics. Inevitably, pipelines desynchronize, race conditions occur, and rebuilding state requires complex migration scripts.

**Structured Data Streams (SDS)** turns this model inside out:

> **The append-only log is the authoritative source of truth.**  
> Every database, index, cache, and filesystem image is a **projection**—a snapshot of the stream materialized at a specific stream position $P$.

---

## Core Principles

```
+-----------------------------------------------------------------------------------+
|  Layer 3: Projections (snapshots at position P, each carrying the type registry)  |
|     SQLite (OLTP)      Parquet (OLAP)      EROFS (filesystem)      ...            |
+-----------------------------------------------------------------------------------+
|  Layer 2: Logical change log                                                      |
|     application messages as tables, row changes, transactions, schema evolution   |
+-----------------------------------------------------------------------------------+
|  Layer 1: Record typing                                                           |
|     payload = varint(type_id) body ; type 1 defines new types in-band             |
+-----------------------------------------------------------------------------------+
|  Layer 0: Streams transport (existing, unchanged)                                 |
|     WALC/WALL framing, CRC32C, stream_seq, Local/Witness/Permanent durability      |
+-----------------------------------------------------------------------------------+
```

### 1. The Log is Authoritative

In SDS, writes never go directly into a database file. Writers append typed mutation records (inserts, updates, deletes) to an ordered, durable stream. 

- There is exactly **one writer per stream**, avoiding distributed multi-master coordination.
- Payloads are opaque to the transport layer (Layer 0), allowing end-to-end encryption.
- Every record receives a monotonically increasing stream sequence number (`stream_seq`).

### 2. Snapshots are Projections at Position $P$

A snapshot is a derived, rebuildable materialization of all stream mutations up to a specific position $P$:

$$\text{State}(P) = \text{Fold}(\text{Records}[0 \dots P])$$

Because snapshots are strictly deterministic functions of the log, they are **interchangeable**:
- An **OLTP projection** produces an indexed SQLite database file for low-latency point lookups.
- An **OLAP projection** produces columnar Parquet files for analytical aggregations in DuckDB or Spark.
- A **Filesystem projection** produces read-only EROFS images for container file trees.

### 3. Restore = Latest Snapshot $\le P$ Plus Replay

To reconstruct state at any arbitrary position $Q$:
1. Locate the newest immutable snapshot taken at position $P \le Q$.
2. Restore or mount the snapshot at $P$.
3. Replay log records in the interval $(P, Q]$.

```
Stream:   [0 ................. P ............ Q ........... Head]
                               ▲               ▲
                      Snapshot at P       Target position Q
                               └─ Replay (P, Q] ─┘
```

Serving replicas can spin up instantly by downloading a snapshot, opening it read-only, and applying the live tail of the stream in memory.

### 4. Free Time Travel

Because snapshots are immutable objects stored by position (e.g. `streams/<uuid>/snapshots/sqlite/00000000000000012500.db`), querying past state requires no special engine features. Pick the snapshot at or before the desired historical timestamp, replay up to that timestamp, and execute queries.

### 5. Snapshots as the Retention Anchor

An append-only log cannot grow unbounded forever. In SDS, snapshots act as the **retention anchor**:
- Log segments older than the oldest active snapshot can be safely reclaimed and pruned from storage.
- The stream's schema definitions and type descriptors are preserved within the snapshot itself.
- A newly provisioned node never needs to read segments from the beginning of time.

---

## Comparison with Existing Systems

| Feature | SDS | Delta Lake / Iceberg | Kafka + Debezium |
| :--- | :--- | :--- | :--- |
| **Source of Truth** | Low-latency stream of proto records | Parquet files + JSON/Avro commit log | External database WAL |
| **OLTP Serving** | First-class (SQLite projection with live tail) | Non-existent (scan-heavy OLAP only) | Handled by upstream database |
| **OLAP Analytics** | Parquet snapshots / Iceberg commits | Native Parquet lakehouse | Sinks to Snowflake/BigQuery |
| **Filesystems** | Native (EROFS images) | Out of scope | Out of scope |
| **Schema In-Band** | Self-describing Protobuf `FileDescriptorSet` | Avro / JSON schemas | Schema Registry (out-of-band) |
| **Restore Model** | Snapshot $\le P$ + stream replay | Manifest replay | External DB snapshot + CDC offset |
