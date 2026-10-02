# Structured Data Streams (SDS) Wire Format Specification

> **Version:** v0 (draft; may change incompatibly)  
> **Status:** Normative Specification  
> **Design Rationale & Architecture:** [`docs/structured-data-streams.md`](structured-data-streams.md)

---

## 1. Introduction and Terminology

Structured Data Streams (SDS) defines a structured payload encoding and relational change-log protocol layered over an append-only, ordered byte stream (Layer 0 Streams transport). SDS enables typed record multiplexing, in-band schema registration, atomic relational mutations, and snapshot-anchored projections.

The key words **MUST**, **MUST NOT**, **REQUIRED**, **SHALL**, **SHALL NOT**, **SHOULD**, **SHOULD NOT**, **RECOMMENDED**, **NOT RECOMMENDED**, **MAY**, and **OPTIONAL** in this document are to be interpreted as described in BCP 14 ([RFC 2119](https://datatracker.ietf.org/doc/html/rfc2119), [RFC 8174](https://datatracker.ietf.org/doc/html/rfc8174)).

### Layer Architecture

```
+-----------------------------------------------------------------------------------+
|  Layer 3: Projections (Snapshots at safe position P + current Type Registry)      |
|     SQLite (OLTP)      Parquet (OLAP)      EROFS (Filesystem)                     |
+-----------------------------------------------------------------------------------+
|  Layer 2: Logical Change Log                                                      |
|     OpRecord (CREATE/UPDATE/DELETE), PrimaryKey, Transactions, Safe Positions    |
+-----------------------------------------------------------------------------------+
|  Layer 1: Record Framing & Type Registry                                          |
|     payload = varint(type_id) body ; TypeDefinition in-band schema registration    |
+-----------------------------------------------------------------------------------+
|  Layer 0: Streams Transport (Opaque Byte Stream)                                  |
|     WALC/WALL framing, CRC32C, stream_seq, Local/Witness/Permanent Durability     |
+-----------------------------------------------------------------------------------+
```

---

## 2. Layer 1: Framing and Type ID Allocation

### 2.1 Wire Framing

Every stream payload **MUST** begin with a variable-length unsigned integer (`varint`) representing the `type_id`, followed immediately by the payload `body`:

$$\text{payload} := \text{varint}(\text{type\_id}) \mathbin{\Vert} \text{body}$$

- The `varint` **MUST** be encoded as a standard Protobuf unsigned varint (LEB128).
- The `body` **MUST** be serialized Protobuf wire bytes corresponding to the schema associated with `type_id`, except for `Padding` (type ID 4) where `body` is arbitrary bytes.
- Decoders **MUST** reject empty payloads or payloads containing malformed varints.

### 2.2 Type ID Allocation Table

| `type_id` | Name | Description | Body Schema |
| :--- | :--- | :--- | :--- |
| `0` | **Invalid** | Reserved. **MUST NOT** be used. Immediately detects uninitialized or torn byte buffers. | — |
| `1` | **TypeDefinition** | Registers or compatibly evolves an application message type in-band. | `sds.v1.TypeDefinition` |
| `2` | **TxCommit** | Commits an open multi-record transaction. | `sds.v1.TxCommit` |
| `3` | **SnapshotPointer** | Announces an immutable snapshot taken at a safe position. | `sds.v1.SnapshotPointer` |
| `4` | **Padding** | No-op used for fixed-bucket size obfuscation under encryption. | Arbitrary byte sequence |
| `5` | **OpRecord** | Relational row mutation (CREATE, UPDATE, DELETE). | `sds.v1.OpRecord` |
| `6`–`15` | **Reserved** | Reserved for future framework extensions. Writers **MUST NOT** emit these IDs. Decoders **MUST** reject unknown IDs in this range. | — |
| `16`+ | **Application Types** | Application-defined message types registered via `TypeDefinition`. | Registered application Protobuf message |

---

## 3. Layer 1: TypeDefinition and In-Band Registry

### 3.1 Protobuf Schema

```protobuf
syntax = "proto3";

package sds.v1;

import "google.protobuf/descriptor.proto";

message TypeDefinition {
  uint32 id = 1;                                      // Application type ID (MUST be >= 16)
  string name = 2;                                    // Fully-qualified message name, e.g. "shop.Order"
  bytes  fingerprint = 3;                             // 32-byte SHA-256 canonical descriptor digest
  google.protobuf.FileDescriptorSet descriptors = 4;  // Full transitive descriptors
  repeated int32 key_fields = 5;                      // Primary key field numbers in order
}
```

### 3.2 Registration Rules

1. **Define Before Use:** A `TypeDefinition` **MUST** appear in the stream prior to the first occurrence of its `id` (either as a raw application record or referenced inside `OpRecord.type_id`). Readers encountering an unregistered `type_id` **MUST** fail with a decode error.
2. **Stream Scope:** Type IDs are scoped strictly to an individual stream and allocated sequentially by its single writer. Writers **MUST NOT** allocate type IDs below 16 for application types.
3. **Idempotency by Fingerprint:** If a stream encounters a `TypeDefinition` for an existing `id` whose `fingerprint` exactly matches the existing registration, it **MUST** be treated as a no-op.
4. **Compatible Evolution:** If a `TypeDefinition` specifies an existing `id` with a new `fingerprint`, the new schema **MUST** be a strictly compatible evolution of the previous schema (see Section 4). Incompatible schema redefinitions **MUST** be rejected.
5. **Key Field Constraints:** If `key_fields` is specified:
   - Each field number listed **MUST** exist in the root message.
   - Key fields **MUST NOT** be `repeated` (lists) or `map` fields.
   - Key fields **MUST NOT** be message-typed or group-typed fields; they **MUST** be scalar primitive types (e.g. `int32`, `int64`, `uint32`, `uint64`, `sint32`, `sint64`, `fixed32`, `fixed64`, `string`, `bytes`, `bool`, `enum`).

### 3.3 Canonical Fingerprint Algorithm

The `fingerprint` field is a 32-byte SHA-256 cryptographic digest over the canonical binary serialization of the `FileDescriptorSet` containing all transitive dependencies of the message:

1. **Transitive Dependency Collection:** Starting from the target message's `ParentFile`, traverse and collect all unique `protoreflect.FileDescriptor` instances across all transitive imports.
2. **Proto Conversion:** Convert each unique `FileDescriptor` into a `google.protobuf.FileDescriptorProto` using standard descriptor reflection.
3. **Assembly:** Assemble all collected `FileDescriptorProto` messages into a `google.protobuf.FileDescriptorSet`.
4. **Lexicographical Sorting:** Sort the `file` slice within the `FileDescriptorSet` in ascending lexicographical order by file path/name (`FileDescriptorProto.name`).
5. **Deterministic Serialization:** Serialize the sorted `FileDescriptorSet` using deterministic Protobuf wire format (`proto.MarshalOptions{Deterministic: true}`). Deterministic serialization guarantees consistent field tag order and sorted map keys across implementations.
6. **SHA-256 Digest:** Compute the SHA-256 hash over the deterministic wire bytes.

Implementations **MUST** produce identical fingerprint byte sequences for identical schemas.

---

## 4. Precise Schema Compatibility Rules

When a `TypeDefinition` updates an existing type ID with a new fingerprint, the new descriptor set is evaluated against the existing descriptor set.

### 4.1 Compatibility Requirements

An evolution is compatible if and only if all of the following rules hold:

1. **Message Identity:** The fully qualified message name (`name`) **MUST** match the existing definition.
2. **Primary Key Immutability:** `key_fields` **MUST** match the existing definition exactly in field numbers, count, and order. Primary key fields cannot be altered across schema versions.
3. **Field Additions:** New fields **MAY** be added. The new field numbers **MUST NOT** have been previously reserved in an older version of the schema.
4. **Field Renames:** Field names **MAY** be changed. Protobuf identifies fields on the wire by numeric tag, so field renames are wire-compatible.
5. **Field Removals & Reservations:** A field **MAY** be removed **ONLY IF** its field number is explicitly marked as `reserved` (e.g. `reserved 3;` in `.proto`, appearing in `DescriptorProto.reserved_range`) in the new descriptor. Removing a field without reserving its number **MUST** be rejected.
6. **Field Type & Wire Compatibility:** Existing field numbers **MUST NOT** change their wire kind, type, or cardinality:
   - Scalar kinds (e.g. `int32`, `string`, `double`, `bytes`, `bool`) **MUST NOT** change kind.
   - A singular/optional field **MUST NOT** become `repeated` or `map`, and vice versa.
   - A map's key kind and value kind **MUST NOT** change.
7. **Oneof Integrity:** Moving an existing field into a `oneof`, out of a `oneof`, or between two distinct `oneof` definitions **MUST** be rejected (excluding synthetic oneofs created by proto3 optional syntax).
8. **Recursive Validation:** If a field references another message or enum type, that referenced type **MUST** be checked recursively under these same compatibility rules.

### 4.2 Comparison with `buf breaking`

The SDS compatibility rules align with the standard Protobuf wire and backward compatibility rules enforced by `buf breaking` (specifically under the `FILE` and `WIRE` categories):

| Rule / Change | SDS Compatibility | `buf breaking` Equivalent | Rationale |
| :--- | :--- | :--- | :--- |
| **Add optional field** | Allowed | Allowed | Unknown to old readers; default value to new readers. |
| **Rename field** | Allowed | `FIELD_SAME_NAME` (warn/reject depending on category), but allowed in `WIRE` | Wire format uses field tags; JSON/text decoders should be aware. |
| **Delete field without `reserved`** | **Rejected** | `FIELD_NO_DELETE` | Prevents subsequent reuse of the tag number causing silent data corruption. |
| **Delete field with `reserved`** | Allowed | Allowed | Safely retires the tag number across the archive lifecycle. |
| **Change field tag number** | **Rejected** | `FIELD_SAME_TYPE` / `FIELD_WIRE_TYPE` | Corrupts wire decoding for existing records. |
| **Change field type (e.g. int32 $\rightarrow$ string)** | **Rejected** | `FIELD_SAME_TYPE` / `FIELD_WIRE_TYPE` | Incompatible wire type or value representation. |
| **Change cardinality (singular $\leftrightarrow$ repeated)** | **Rejected** | `FIELD_SAME_LABEL` | Wire packing and structural decoding incompatibilities. |
| **Change oneof membership** | **Rejected** | `ONEOF_SAME_MEMBERS` | Alters field presence and serialization semantics. |
| **Reuse reserved field number** | **Rejected** | `RESERVED_ENUM_NO_DELETE` / `FIELD_NOT_RESERVED` | Prevents colliding with historical data. |

---

## 5. Layer 2: Logical Change Log & Row Operations

### 5.1 Protobuf Schema (`OpRecord`, Type ID 5)

Relational row changes are carried in `OpRecord` messages framed under `type_id = 5`:

```protobuf
syntax = "proto3";

package sds.v1;

message OpRecord {
  enum Op {
    CREATE = 0;
    UPDATE = 1;
    DELETE = 2;
  }
  Op     op = 1;
  uint32 type_id = 2; // Target table type ID (MUST be >= 16)
  uint64 tx_id = 3;   // 0 = autocommit; >0 = part of transaction
  bytes  key = 4;     // Canonical binary proto encoding of key fields
  bytes  value = 5;   // Binary proto encoding of non-key fields
}
```

### 5.2 Canonical Key Encoding

The `key` field in `OpRecord` **MUST** be encoded as follows:
- A new instance of the target message descriptor is populated containing **only** the fields listed in `TypeDefinition.key_fields`.
- All fields listed in `key_fields` **MUST** be present and set. If any primary key field is unset or missing, writer and reader **MUST** reject the operation.
- Non-key fields **MUST NOT** be set in `key`.
- Unknown fields **MUST NOT** be present in `key`.
- The key message **MUST** be serialized using deterministic Protobuf serialization (`proto.MarshalOptions{Deterministic: true}`).
- The resulting byte slice provides an exact, canonical representation of the primary key that is directly comparable for map keys, hash indexing, and lookup operations.

### 5.3 Value Encoding

The `value` field in `OpRecord` **MUST** be encoded as follows:
- For `CREATE` and `UPDATE` operations: `value` contains the serialized binary Protobuf bytes of all **non-key** fields of the target row message.
- For `DELETE` operations: `value` **MUST** be empty (0 bytes).

### 5.4 Reconstitution (Merging)

To reconstruct the complete row message from an `OpRecord`:
1. Instantiate a new empty message of the type identified by `type_id`.
2. Unmarshal `key` into the message using merge options (`proto.UnmarshalOptions{Merge: true}`).
3. If `value` is non-empty, unmarshal `value` into the same message using merge options.
4. The resulting message contains the fully populated row.

### 5.5 Operation Semantics

- **`CREATE` (`0`):** Inserts the row into the table. If a row with the same primary key already exists in the projection, `CREATE` replaces it (insert-or-replace semantics for idempotent replay).
- **`UPDATE` (`1`):** Replaces the non-key attributes of the row identified by `key`. `UPDATE` represents a complete replacement of non-key fields.
- **`DELETE` (`2`):** Removes the row identified by `key` from the table. `DELETE` payloads omit the `value` field. If the row does not exist, `DELETE` is an idempotent no-op.

---

## 6. Transactions and the Safe-Position Rule

### 6.1 Transaction Semantics

```protobuf
syntax = "proto3";

package sds.v1;

import "google.protobuf/timestamp.proto";

message TxCommit {
  uint64 tx_id = 1;
  google.protobuf.Timestamp commit_time = 2;
}
```

- **Autocommit (`tx_id = 0`):** An `OpRecord` with `tx_id = 0` is an autocommit transaction. It is applied immediately by consumers upon consumption.
- **Multi-Record Transactions (`tx_id > 0`):** Records with a non-zero `tx_id` belong to an atomic transaction. Consumers **MUST** buffer all records for `tx_id` as pending until a matching `TxCommit` (type ID 2) with the same `tx_id` arrives in the stream.
- **Transaction Closure:** When `TxCommit{tx_id: T}` is received, all buffered records for `T` are committed atomically in the order they were received.
- **Uncommitted Tail / Crash Recovery:** If the stream terminates or a consumer recovers from a checkpoint, any pending records with no matching `TxCommit` **MUST** be discarded.
- **Scope:** Transaction IDs are allocated by the stream writer and need only be unique among concurrently open transactions on that stream.

### 6.2 Safe-Position Rule

A stream sequence position $P$ is a **safe position** if and only if **no multi-record transaction is pending** after consuming the record at position $P$:

$$\text{Safe}(P) \iff \text{PendingTxCount}(P) == 0$$

- When an autocommit record (`tx_id = 0`) or a `TxCommit` record is processed and no other transactions remain open, the position of that record is a safe position.
- Non-relational framework records (`TypeDefinition`, `SnapshotPointer`, `Padding`) and raw application records processed when no transactions are pending also advance the safe position.
- **Snapshots MUST only be taken at safe positions.** A snapshot taken at a non-safe position would capture partial, uncommitted transaction state and violate atomic consistency.

---

## 7. Raw Application-Typed Records (Type IDs 16+)

Application message types registered via `TypeDefinition` **MAY** also be written directly to the stream as raw application records using their assigned `type_id`:

$$\text{payload} := \text{varint}(\text{type\_id}) \mathbin{\Vert} \text{serialized\_proto\_body}$$

- **Semantics:** A raw application record represents a standalone, keyless event or document. It does not carry relational row mutation semantics (`CREATE`/`UPDATE`/`DELETE`).
- **Projections Behavior:** Relational and tabular projections (such as SQLite or MemTable stores) **MUST** ignore raw application records or treat them as non-row stream events. Event-sourcing and streaming consumers decode them directly into the registered Protobuf message type using dynamic or compiled reflection.

---

## 8. Layer 3: The Snapshot Contract

### 8.1 Snapshot Structure

A snapshot is an immutable point-in-time materialization of the stream state:

1. **Safe Position Anchor:** A snapshot is materialized at an exact safe stream position $P$ (`stream_seq`).
2. **Type Registry State:** The snapshot **MUST** contain the complete in-band Type Registry in force at position $P$ (all `TypeDefinition` records seen up to $P$).
3. **Data State:** The snapshot contains the projected dataset state resulting from applying all committed transactions up to $P$.

```protobuf
syntax = "proto3";

package sds.v1;

message Registry {
  repeated TypeDefinition types = 1;
}

message SnapshotPointer {
  uint64 position = 1;             // Stream sequence position P
  string format = 2;               // Format identifier (e.g. "sqlite", "parquet", "erofs")
  string location = 3;             // Object storage URI or key
  bytes  registry_fingerprint = 4; // SHA-256 digest of the Registry proto
}
```

### 8.2 Object Storage Naming Convention

Snapshots are stored immutably in object storage using zero-padded 20-digit position naming:

$$\text{streams/}\langle\text{stream-uuid}\rangle\text{/snapshots/}\langle\text{format}\rangle\text{/}\langle\text{position:020d}\rangle.\langle\text{ext}\rangle$$

Examples:
- `streams/abc-123/snapshots/sqlite/00000000000000001000.db`
- `streams/abc-123/snapshots/parquet/00000000000000001000/`

Lexicographical sorting of snapshot keys strictly matches stream sequence order.

### 8.3 Restore and Replay Protocol

To reconstruct the dataset state at any target position $Q$:
1. Locate the latest snapshot with position $P \le Q$.
2. Restore the dataset state and in-band type registry from the snapshot at $P$.
3. Replay log records in the interval $(P, Q]$ using `ChangeReader`, applying committed changes into the state.
4. If $Q$ is a safe position, the resulting state is valid and consistent.

---

## 9. Conformance & Test Vectors

To verify compliant implementations across languages, the SDS test suite provides golden test vectors in `pkg/sds/testdata/vectors/`. Compliant decoders and readers **MUST** pass all conformance test vectors, including valid operations, transaction boundaries, schema evolution, and all error conditions.
