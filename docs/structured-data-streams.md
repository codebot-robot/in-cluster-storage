# Structured Data Streams: SQL, OLTP and OLAP as Snapshots over a Stream of Proto Records

> **Status:** design proposal. Nothing in this document is implemented yet. It builds on the existing Streams service ([`docs/streams.md`](streams.md)) and borrows the log-plus-snapshot pattern that ObjectFS already uses ([`docs/objectfs.md`](objectfs.md)).

---

## Overview

A Streams stream is an ordered, durable sequence of opaque payloads. This document proposes a **structured** payload format on top of it, so that a stream becomes a **logical change log for a relational dataset**: typed records describing row inserts, updates and deletes, grouped into transactions, with the schema carried in-band.

Once the log is the source of truth, every other representation is a **projection** of it, materialized at a known stream position:

| Projection | Format | Serves |
| :--- | :--- | :--- |
| **OLTP snapshot** | SQLite database file | Point lookups, small transactions, serving reads with indexes |
| **OLAP snapshot** | Parquet files (one per table) | Scans, aggregations, DuckDB / BigQuery / Spark |
| **Filesystem image** *(case study)* | EROFS image | The ObjectFS metadata tree, as today |

The log is authoritative and append-only. Snapshots are derived, rebuildable, and interchangeable: a consumer restores state from *any* snapshot at position `P` and replays the log from `P` forward. This is the architecture of Delta Lake (JSON action log plus parquet checkpoints) and Iceberg (metadata log plus manifests), with a durable low-latency log underneath and a first-class OLTP projection that those systems lack.

The design has four layers, each independent of the ones above it:

```
+-----------------------------------------------------------------------------------+
|  Layer 3: Projections (snapshots at position P, each carrying the type registry)  |
|     SQLite (OLTP)      Parquet (OLAP)      EROFS (filesystem)      ...            |
+-----------------------------------------------------------------------------------+
|  Layer 2: Logical change log                                                      |
|     tables as types, row changes, transaction commits, schema evolution           |
+-----------------------------------------------------------------------------------+
|  Layer 1: Record typing                                                           |
|     payload = varint(type_id) body ; type 1 defines new types in-band             |
+-----------------------------------------------------------------------------------+
|  Layer 0: Streams transport (existing, unchanged)                                 |
|     WALC/WALL framing, CRC32C, stream_seq, Local/Witness/Permanent durability      |
+-----------------------------------------------------------------------------------+
```

Layer 0 never inspects a payload. Everything above it is a client-side convention, which is also what allows payloads to be encrypted end to end (see [Encryption](#encryption)).

---

## Layer 1: Record Typing

### The problem

A stream that carries more than one kind of record needs a per-record type. The conventional answers are a `google.protobuf.Any` (a 40–60 byte type URL per record) or a `oneof` in a fixed envelope message (types fixed at compile time). Neither suits a change log where the record types are *tables*, which appear at runtime and must be readable from an archive years later without access to the source tree.

### The format

Every payload begins with a varint type id, followed by the record body:

```
payload := varint(type_id) body
```

| `type_id` | Meaning | Body |
| :--- | :--- | :--- |
| `0` | Reserved, never valid. Zeroed or torn data is detected immediately. | — |
| `1` | **TypeDefinition**: introduces a new type id for this stream. | `TypeDefinition` proto (schema fixed by the format version) |
| `2` | **TxCommit**: closes the current transaction. | `TxCommit` proto |
| `3` | **SnapshotPointer**: announces that a snapshot covering up to a position exists. | `SnapshotPointer` proto |
| `4` | **Padding**: no-op. | Arbitrary bytes |
| `5`–`15` | Reserved for the framework. | — |
| `16`+ | Application types, defined by a preceding `TypeDefinition`. | Per the definition's `encoding` |

Type ids `16`–`127` cost one byte per record; ids up to `16383` cost two.

```proto
message TypeDefinition {
  uint32 id = 1;                                      // >= 16; never reused within a stream
  string name = 2;                                    // fully-qualified message name, e.g. "shop.orders.v3"
  bytes  fingerprint = 3;                             // sha256 over the canonical serialized descriptors + encoding
  google.protobuf.FileDescriptorSet descriptors = 4;  // may be omitted if this fingerprint has already been defined
  enum Encoding { PROTO = 0; AVRO = 1; JSON = 2; RAW = 3; }
  Encoding encoding = 5;                              // how to decode bodies of this type
  uint32 replaces = 6;                                // previous id for the same logical type, if this is an evolution
}
```

### Rules

1. **Define before use.** A type's `TypeDefinition` precedes its first use in the same stream. This is inherent to any in-band scheme.
2. **Definitions are immutable and ids are never reused.** Schema evolution allocates a *new* id with `replaces` pointing at the old one. Every reader, wherever it starts, agrees on what an id means, which is what makes replay deterministic.
3. **Ids are scoped to a stream and allocated by its single writer.** Streams already have exactly one writer per `stream_id`, so no coordination is needed. Types are shared *across* streams by `fingerprint` or `name`, never by id.
4. **Definitions are idempotent by fingerprint.** A repeated definition with a matching fingerprint is a no-op; a mismatching one is a hard error. This lets a restarting writer simply re-emit its definitions instead of persisting which ids it has already announced (its local segments are trimmed after the `Permanent` ack, so it cannot always re-derive them).
5. **The registry travels with snapshots and cursors, not with periodic keyframes in the log.** See [Layer 3](#layer-3-projections-as-snapshots). A reader that starts from a snapshot at `P` has every definition in force at `P`; anything newer is in the log after `P`. A tailing consumer persists the handful of definitions it has seen alongside its position. The log therefore carries each definition once.

The accepted cost of rule 5 is that a lone segment pulled out of object storage is not decodable without the snapshot before it. Tooling that inspects segments takes a snapshot or registry as input.

### Why proto for the bodies

The type registry above is encoding-neutral (`Encoding`), but the default and the framework messages are protobuf, for reasons that matter specifically to an archive:

- **Structural decode without a schema.** Every proto field carries a tag and wire type, so a record can be walked field by field with no descriptor at all (`protoc --decode_raw`). If a registry is ever lost, the structure of a ten-year-old segment is still recoverable. Avro's positional encoding has no field boundaries and is unrecoverable without its exact writer schema.
- **Misparse fails loudly.** Decoding an Avro record with the wrong writer schema yields plausible garbage; a proto decoded against the wrong type yields unknown fields or a wire-type error.
- **One IDL, one toolchain.** The Streams and ObjectFS APIs, and `MutationRecord`, are already proto with first-party Go support. A log record can flow through `AppendRecord` and out of `Tail` as the same bytes.
- **Sparse records are smaller.** Unset fields cost nothing; a change log is sparse (deletes have no after-image, most updates touch two columns).
- **Unknown fields are preserved**, so a relay or compactor built against an older schema forwards newer fields intact.

Avro wins on dense tiny rows (roughly 25–35 % smaller than a naive proto `Value` list before compression) and on its logical-type vocabulary (`decimal`, `date`, `uuid`). Layer 2 closes the size gap by generating a proto message per table version, which is within a few bytes per field of Avro. Streams that must interoperate with Kafka / Debezium consumers can opt into `AVRO` bodies per type without changing anything else.

---

## Layer 2: The Logical Change Log

### Tables are types

A table version is registered as an application type whose message is generated by the framework from the table's column list:

```proto
// Generated for table "orders", schema version 3. Column ids become field
// numbers and are never renumbered; dropped columns become reserved.
message OrdersRow {
  int64  id = 1;
  string customer = 2;
  double total = 3;
  google.protobuf.Timestamp placed_at = 4;
  bytes  receipt_sha256 = 5;
}

message OrdersChange {
  enum Op { INSERT = 0; UPDATE = 1; DELETE = 2; }
  Op op = 1;
  OrdersRow key = 2;      // primary-key columns only
  OrdersRow before = 3;   // UPDATE/DELETE, optional; enables undo and CDC diffs
  OrdersRow after = 4;    // INSERT/UPDATE
}
```

The `TypeDefinition` for this type carries the `FileDescriptorSet` for `OrdersChange`, so any reader can decode rows with `dynamicpb` and no generated code. A row change on the wire is one varint plus one `OrdersChange`: no wrapper, no table name, no column names.

Because ids are never reused, **schema evolution is a new type id**: adding a column produces `OrdersChange` v4 as a new type with `replaces` set, and both types may appear in the same stream. Readers project both into the logical table `orders` by name. Compatible changes (new optional column, dropped column) are the norm; incompatible changes (type change on a column) are a new column id, exactly as in proto itself.

### Value types

The column type system is deliberately the **intersection** of what the projections represent losslessly:

| Logical type | proto field | SQLite | Parquet |
| :--- | :--- | :--- | :--- |
| `bool` | `bool` | `INTEGER` 0/1 | `BOOLEAN` |
| `int64` | `int64` / `sint64` | `INTEGER` | `INT64` |
| `double` | `double` | `REAL` | `DOUBLE` |
| `text` | `string` | `TEXT` | `BYTE_ARRAY` (UTF8) |
| `blob` | `bytes` | `BLOB` | `BYTE_ARRAY` |
| `timestamp` | `google.protobuf.Timestamp` | `INTEGER` (micros) | `INT64` (TIMESTAMP_MICROS) |
| nullable | proto3 `optional` | `NULL` | definition level |

Richer types (`decimal`, `date`, `uuid`, enums) are annotations on the column in the type definition, stored over one of the above. Large values (file contents, images) are **not** stored inline: they are written to the content-addressed blob store and referenced by SHA-256, the same rule ObjectFS is adopting for large file content.

### Transactions

Row changes between two `TxCommit` records belong to one transaction; the commit record closes it:

```proto
message TxCommit {
  google.protobuf.Timestamp commit_time = 1;
  uint64 tx_id = 2;            // optional application transaction id
  map<string, string> tags = 3; // optional: origin, request id, ...
}
```

This is the classic WAL layout (Postgres uses a commit record the same way). Rows carry no `tx_id`, which keeps single-row transactions at one small record plus a commit. A reader treats rows as pending until the commit arrives; on recovery, a trailing group with no commit is discarded. Snapshots are only ever taken at commit boundaries. A filesystem rename, which touches two directory rows and one inode row, is one transaction.

Cross-stream transactions are out of scope: a stream is the unit of atomicity and ordering, as it is today.

---

## Layer 3: Projections as Snapshots

### Definition

A **snapshot** is a materialization of a stream's state:

- taken at a commit boundary, at stream position `P` (the `stream_seq` of the last applied record);
- containing the **type registry in force at `P`** (every `TypeDefinition` seen so far), not merely the table schemas;
- stored in object storage under the stream, named by position so lexical order is stream order: `streams/<stream-uuid>/snapshots/<format>/<position 20 digits>.<ext>`;
- immutable once written.

Given a snapshot at `P`, a reader that wants the state at `Q >= P` restores the snapshot and replays records `(P, Q]`. This is the same contract the ObjectFS controller has with its EROFS image plus mutation log, and it gives **time travel** for free: pick the latest snapshot at or before `Q`.

Snapshots are also the **retention anchor**. Segments older than the oldest snapshot anyone still needs can be deleted; the log need not be retained from the beginning of time, and definitions from before the retention window survive in the snapshot registry.

### SQLite (OLTP)

- One database file per stream (or per shard if a stream is sharded; see open questions).
- Tables are created from the registry. Each column id is a column; the primary key from the type definition becomes the `PRIMARY KEY`; indexes are a property of the projection, not the log, and may differ between two SQLite snapshots of the same stream.
- Two bookkeeping tables: `_stream_types` (`id`, `name`, `fingerprint`, `descriptors BLOB`, `replaces`) and `_stream_position` (`stream_id`, `position`, `snapshot_time`).
- Apply rules: `INSERT` → `INSERT OR REPLACE`; `UPDATE` → `UPDATE ... WHERE key`, falling back to insert of `after` if the row is missing (idempotent replay); `DELETE` → `DELETE WHERE key`. Each `TxCommit` closes a SQLite transaction.
- Writing: build in a temp file, `PRAGMA journal_mode=OFF` during load, fsync, then upload. Serving: download or stream to local disk, open read-only, and keep applying the live tail into it from `Tail`.
- SQLite is the right OLTP shape because a single file is trivially snapshotted, has real indexes and a page cache, and is readable everywhere. Its WAL and page format are *not* used as the log format: the log is logical, the file is a projection.

### Parquet (OLAP)

- One parquet file per table per snapshot, under the position-named directory. The type registry and position go in the file footer's key/value metadata, so a parquet file is self-describing to DuckDB or Spark without any side channel.
- Append-only tables can be snapshotted **incrementally**: each snapshot writes only rows committed since the previous one, and readers union the files. Tables with updates and deletes need either a full rewrite per snapshot or a merge-on-read design with delete files, as Iceberg does; the first is the proposal's starting point and the second is an open question.
- Row-group sizing, sorting, and partitioning are projection choices. Two OLAP snapshots of the same log can be organized differently for different query shapes.

### EROFS (filesystem, case study)

ObjectFS today stores its metadata mutations as a proto `MutationRecord` in a stream and periodically compiles an EROFS image. Recast in this design, the filesystem is two tables, `inodes` and `dirents`, each a registered type; a `mkdir` is one transaction touching both; and the EROFS image is a third kind of projection alongside SQLite and Parquet, with `user.digest` xattrs carrying the blob SHA exactly as now. This unification is **deliberately tabled**: it would change the on-disk WAL format for a working system and belongs behind stream-format versioning rather than in this proposal. It is listed here because it demonstrates that the layering is general, and because it would let a directory tree be queried with SQL from the SQLite projection.

### SnapshotPointer

Snapshots are discoverable by listing the object-store prefix, so no manifest is required (the same manifest-free choice Streams made for segments). Optionally, the writer or the snapshotter appends a framework record so that the log itself says where its snapshots are:

```proto
message SnapshotPointer {
  uint64 position = 1;     // stream_seq the snapshot covers up to
  string format = 2;       // "sqlite", "parquet", "erofs"
  string location = 3;     // object key or URL
  bytes  registry_fingerprint = 4;
}
```

A reader tailing a stream then learns about new snapshots without polling the bucket. It is a pointer, never a copy of the snapshot's content.

---

## Reading

| Reader | Starts from | Needs |
| :--- | :--- | :--- |
| Full restore / new replica | Latest snapshot ≤ target position, then replay | Snapshot only |
| Serving node | Snapshot on local disk + live `Tail` | Snapshot + cursor |
| CDC / tailing consumer | Its saved cursor | Its saved cursor **and** the definitions it has seen |
| Ad-hoc analytics | A Parquet snapshot directly | Nothing else |
| Debugging (`streams cat`) | Any segment | A snapshot or registry file to resolve type ids |

A consumer that loses its registry restarts from the latest snapshot at or before its cursor. This is the only place a snapshot is strictly required for correctness, and it is exactly what snapshots are for.

---

## Encryption

The client-side encryption proposed for Streams (per-record AEAD, [issue #87](https://github.com/gke-labs/in-cluster-storage/issues/87)) composes cleanly because the type varint lives **inside** the payload: an observer sees neither record types nor their distribution, only sizes and timing. The `Padding` type exists so that sizes can be bucketed under encryption, and the key envelope for a stream and its `TypeDefinition`s are both "stream header" records that a snapshot carries forward.

---

## Alternatives Considered

| Alternative | Why not (here) |
| :--- | :--- |
| `google.protobuf.Any` per record | 40–60 bytes per record for the type URL; still no schema in the archive. |
| `oneof` in a fixed envelope | Same cost as the varint, but the set of types is fixed at compile time; tables appear at runtime. |
| Avro (single-object encoding + schema records) | No structural decode without the writer schema; silent misparse on schema mismatch; second IDL and third-party Go tooling. Retained as an opt-in body encoding for Kafka-facing streams. |
| Arrow IPC record batches as the log | Excellent for bulk ingestion and maps straight to Parquet, but hundreds of bytes of overhead per single-row change. A candidate opt-in encoding for batch-heavy streams. |
| SQLite session changesets | Almost exactly a `RowChange`, and worth reading, but engine-specific binary with no versioning; unsuitable as an archive format. |
| SQLite WAL / physical page log | Ties the log to one engine's page layout; cannot be projected into Parquet. |
| Riegeli | Solves the *file* problem well (descriptors in the header, chunk checksums, resync, transposition) and validates the in-band-schema approach, but is one type per file, has no streaming or append semantics, and has no Go implementation. Its role would be a sealed-segment format, which zstd on segments already covers. |
| Periodic type keyframes in the log | Redundant once snapshots carry the registry and consumers persist it with their cursor. Dropped. |

---

## Open Questions

- **Snapshot production.** Who takes snapshots: the writer (has the state in memory), a buffer-side compactor (has all streams, no application code), or a dedicated projector reading `Tail`? The projector is the most general and keeps the buffer schema-free; the writer is simplest for a single-tenant stream.
- **Cadence and cost.** Snapshot on a size threshold of log since last snapshot, on a timer, or on demand. Interaction with `Permanent` durability: a snapshot must not cover positions beyond the `s3Seq` watermark.
- **Sharding.** A stream is the unit of ordering; a large table may need many streams. How keys map to streams, and whether a Parquet projection can span streams, is undecided.
- **Mutable tables in Parquet.** Full rewrite per snapshot versus delete files and merge-on-read.
- **Compaction of the log itself.** Whether to ever rewrite segments (e.g. dropping superseded rows) or rely purely on snapshots plus retention. The latter is simpler and the proposal's default.
- **Column annotations.** Exact vocabulary for `decimal`, `date`, `uuid`, enums, and how they round-trip into SQLite (`DECIMAL` has no native type there).
- **Format versioning.** A stream header record identifying the payload format version, shared with the encryption envelope, so that Layer 1 can evolve.

---

## Roadmap

- [ ] Layer 1 in `pkg/wal`: `TypeDefinition` / `TxCommit` / `SnapshotPointer` / `Padding` protos, a typed `Append`/`Read` wrapper over `client.Stream` that maintains the registry, and `streams cat`.
- [ ] Layer 2: table type generation (column list → `FileDescriptorSet`), `dynamicpb` decoding, transaction grouping on read.
- [ ] SQLite projector: build, publish, restore, live tail apply.
- [ ] Parquet projector: append-only tables first.
- [ ] Conformance suite: random logs replayed into both projections must agree, from any snapshot position.
- [ ] Case study: express the ObjectFS `MutationRecord` as two tables and produce an EROFS image from the SQLite projection (no format change to the live system).
