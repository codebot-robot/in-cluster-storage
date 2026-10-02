---
title: "Why / FAQ"
kicker: "Design Decisions & Alternatives"
summary: "Detailed rationale behind design trade-offs: Protobuf vs Avro, Any vs oneof, Delta/Iceberg relationships, Kafka, and Riegeli."
status_note: "Derived from the SDS Design Proposal (docs/structured-data-streams.md)."
next_url: "/case-study/"
next_title: "Case Study: ObjectFS"
---

The design of Structured Data Streams prioritizes long-term archival integrity, zero-coordination schema evolution, and minimal serialization overhead. Below are the design decisions and trade-offs.

---

## Serialization & Encodings

### Why Protobuf instead of Apache Avro?

Bodies are strictly Protobuf with no alternative runtime encodings. While Avro is widely used in event streaming, Protobuf provides critical properties for long-lived change logs:

1. **Structural Decode Without a Schema:** Every Protobuf field carries a tag (field number) and wire type. A reader can inspect the structure of a ten-year-old segment without having its descriptor (`protoc --decode_raw`). Avro’s positional encoding has no field boundaries and cannot be parsed if the writer schema is lost.
2. **Misparse Fails Loudly:** Decoding an Avro record against an incorrect schema yields plausible garbage. Protobuf decodes against the wrong message type produce unknown fields or wire-type validation errors.
3. **One IDL & Toolchain:** The Streams service, ObjectFS APIs, and storage protocols already use Protobuf with first-party Go tooling.
4. **Sparse Mutations:** Protobuf omits unset fields completely. Change-log records (e.g. deletes containing only keys, or updates modifying a single column) are extremely compact.
5. **Unknown Fields are Preserved:** Intermediary nodes (compactors, relays) forwarding streams preserve unknown fields intact across versions without requiring coordination.

### Why not `google.protobuf.Any`?

`google.protobuf.Any` prepends a full type URL (such as `type.googleapis.com/shop.Order`) to each record, costing 40–60 bytes of overhead per row change. Furthermore, `Any` still requires an out-of-band schema repository to resolve definitions from historical archives. SDS’s 1–2 byte varint type ID with an in-band `TypeDefinition` is an order of magnitude smaller and fully self-contained.

### Why not a `oneof` inside a fixed envelope?

A top-level `oneof` message requires all possible message types to be known at compile time. In a multi-tenant storage system or dynamic change log, new tables are created by applications at runtime. Type IDs allocated in-band permit dynamic schema registration without recompiling or redeploying storage daemons.

### Why are there no periodic type keyframes in the log?

In early designs, writers periodically re-emitted `TypeDefinition` keyframes every $N$ segments. However, because snapshots already carry the complete type registry in force at position $P$, and tailing consumers maintain the definitions they have seen alongside their cursor, keyframes in the log were completely redundant. Omitting them keeps the stream clean and append-only.

---

## Ecosystem & Related Architectures

### How does this relate to Delta Lake and Apache Iceberg?

**Structured Data Streams is complementary to Iceberg and Delta Lake, not an alternative.**

- **Delta Lake & Iceberg** are OLAP table formats designed over Parquet files in object storage. They manage file manifests, partition pruning, and column statistics. However, they lack an authoritative, low-latency, append-only ingestion log and have no first-class OLTP or filesystem projection capabilities.
- **SDS** provides the low-latency, ordered proto stream and generates specialized snapshots for multiple engines. In fact, writing the OLAP projection as an Iceberg table is the natural path for integrating SDS into enterprise lakehouses.

### How does this compare to Kafka + Debezium CDC?

Traditional Change Data Capture (CDC) with Debezium captures WAL records from an external relational database (like PostgreSQL) and sends them to Kafka. In that architecture, the database is the source of truth, Kafka is a downstream transport, and schemas are stored in an external Confluent Schema Registry.

In SDS:
- The stream **is** the authoritative source of truth.
- Writers append directly to the stream.
- The schema is carried in-band via `TypeDefinition`.
- The SQLite database is simply a derived read cache/projection that can be wiped and re-materialized at will.

### Why not Google's Riegeli?

[Riegeli](https://github.com/google/riegeli) is an exceptional record file format featuring header descriptors, chunk checksums, and transposition. However, Riegeli is designed for static single-type files rather than append-only multi-type streaming logs, lacks a native Go implementation, and does not provide multi-engine projection semantics.

### Why not Arrow IPC batches as the log format?

Apache Arrow IPC batches excel at bulk vector ingestion and map cleanly to Parquet. However, the metadata overhead for small single-row OLTP transactions is hundreds of bytes per record. SDS uses lightweight Protobuf row mutations for low-latency writes and projects bulk Parquet files on snapshot boundaries.

### Why not SQLite session changesets or SQLite WAL?

- **SQLite Session Changesets** are engine-specific binary diffs tied to SQLite’s internal representation; they cannot be parsed by generic decoders or projected into Parquet.
- **SQLite WAL Files** tie storage to one specific engine’s 4KB physical page layout, making long-term archival, encryption, and cross-format projection impossible.

---

## Summary of Alternatives Considered

| Alternative | Verdict | Why Not Here |
| :--- | :--- | :--- |
| `google.protobuf.Any` per record | ❌ Rejected | 40–60 bytes overhead per record; missing schema in archives. |
| `oneof` in fixed envelope | ❌ Rejected | Fixed at compile time; cannot add application tables at runtime. |
| Generated per-table row messages | ❌ Rejected | Duplicates user schemas; requires code generation for every table. |
| Apache Avro | ❌ Rejected | No schema-free structural decode; silent misparse; duplicate toolchain. |
| Arrow IPC record batches as log | ❌ Rejected | Too much framing overhead for single-row OLTP mutations. |
| Periodic type keyframes in log | ❌ Rejected | Redundant because snapshots and cursors maintain the registry. |
| SQLite WAL as log | ❌ Rejected | Ties storage to physical disk pages; cannot project to Parquet. |
| Riegeli | ❌ Rejected | Static file format; no Go implementation; single-type per file. |
