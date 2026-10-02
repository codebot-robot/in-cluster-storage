---
title: "The Wire Format"
kicker: "Framing & Encodings"
summary: "Layer 1 record typing and Layer 2 logical change log framing, with a byte-by-byte wire example."
status_note: "Draft specification. See the normative specification in docs/sds-spec.md."
next_url: "/projections/"
next_title: "Projections"
---

The Structured Data Streams wire format is split into two clean layers:
- **Layer 1 (Record Typing):** How different record kinds are multiplexed into one opaque stream.
- **Layer 2 (Logical Change Log):** How relational mutations (tables, rows, transactions) are represented.

> **Normative Specification:** A formal, normative specification document with golden test vectors is available in [`docs/sds-spec.md`](https://github.com/gke-labs/in-cluster-storage/blob/main/docs/sds-spec.md). This page provides an intuitive, high-level guide.

---

## Layer 1: Record Typing

Every record payload on the wire begins with a variable-length integer (`varint`) specifying its type ID, followed by the protobuf serialized body:

$$\text{payload} := \text{varint}(\text{type\_id}) \mathbin{\Vert} \text{body}$$

### System vs. Application Types

| `type_id` | Name | Description | Body Schema |
| :--- | :--- | :--- | :--- |
| `0` | *Reserved* | Never valid. Catches uninitialized or torn byte buffers immediately. | — |
| `1` | **TypeDefinition** | Registers or compatibly evolves a protobuf message type for this stream. | `TypeDefinition` |
| `2` | **TxCommit** | Commits a multi-record transaction. | `TxCommit` |
| `3` | **SnapshotPointer** | Announces the creation of an immutable snapshot. | `SnapshotPointer` |
| `4` | **Padding** | No-op used for fixed-bucket size obfuscation under encryption. | Arbitrary bytes |
| `5` | **OpRecord** | Relational row mutation (`CREATE`, `UPDATE`, `DELETE`). | `OpRecord` |
| `6`–`15` | *Reserved* | Reserved for future core framework extensions. | — |
| `16`+ | **Application Types** | Defined by a preceding `TypeDefinition`. Costs only 1 byte for IDs $\le 127$. | Bare registered user proto |

### Type Definition Schema

When a writer introduces a table, it writes a `TypeDefinition` (type ID `1`) before emitting any rows:

```protobuf
message TypeDefinition {
  uint32 id = 1;                                      // >= 16; immutable within a stream
  string name = 2;                                    // e.g. "shop.Order"
  bytes  fingerprint = 3;                             // SHA-256 hash over canonical descriptors
  google.protobuf.FileDescriptorSet descriptors = 4;  // Full descriptor set for zero-codegen decoders
  repeated int32 key_fields = 5;                      // Field numbers that form the Primary Key
}
```

### Key Invariants

1. **Define Before Use:** A `TypeDefinition` must precede any row referencing its `id`.
2. **Compatible In-Place Evolution:** Redefining an existing ID is allowed only if the new descriptors are backward/forward compatible (e.g. adding optional fields, reserving deleted numbers). Field numbers and primary key definitions are immutable.
3. **Idempotent by Fingerprint:** Re-emitting a known `(id, fingerprint)` is a safe no-op.
4. **Registry in Snapshots:** Type definitions are not periodically spammed as keyframes in the log. Instead, the current type registry travels inside snapshots and consumer cursor checkpoints.

---

## Layer 2: The Logical Change Log

Application tables are simply existing Protobuf messages. Relational mutations are represented by a framework `OpRecord` (type ID `5`):

```protobuf
message OpRecord {
  enum Op {
    CREATE = 0;
    UPDATE = 1;
    DELETE = 2;
  }
  Op     op = 1;
  uint32 type_id = 2; // Target table type ID (>= 16)
  uint64 tx_id = 3;   // 0 for autocommit; non-zero held pending until TxCommit
  bytes  key = 4;     // Canonical binary proto encoding of key fields
  bytes  value = 5;   // Binary proto encoding of non-key fields
}
```

### Transactions

- Single-row mutations set `tx_id = 0` (autocommit).
- Multi-row atomic batches share a non-zero `tx_id` and are finalized by a `TxCommit` record (type ID `2`):

```protobuf
message TxCommit {
  uint64 tx_id = 1;
  google.protobuf.Timestamp commit_time = 2;
}
```

---

## Worked Example: A Row Change on the Wire

Let us trace an application creating a new order row in table `shop.Order` (registered as type ID `16`).

### 1. The Application Protobuf Message

```protobuf
syntax = "proto3";
package shop;

message Order {
  int64  id = 1;         // Primary key (key_fields = [1])
  string customer = 2;
  double total = 3;
}
```

We insert: `Order{ id: 42, customer: "Alice", total: 19.99 }`.

### 2. Encoded Key and Value Fields

- **Key proto (`Order{ id: 42 }`):**
  - Field 1 (`id = 42`): tag `(1 << 3) | 0 = 0x08`, varint `42` (`0x2a`) $\rightarrow$ `08 2a` (2 bytes)
- **Value proto (`Order{ customer: "Alice", total: 19.99 }`):**
  - Field 2 (`customer = "Alice"`): tag `(2 << 3) | 2 = 0x12`, length `5` (`0x05`), UTF-8 `"Alice"` (`41 6c 69 63 65`) $\rightarrow$ `12 05 41 6c 69 63 65` (7 bytes)
  - Field 3 (`total = 19.99`): tag `(3 << 3) | 1 = 0x19`, 64-bit IEEE-754 float `19.99` (`71 3d 0a d7 a3 f0 33 40`) $\rightarrow$ `19 71 3d 0a d7 a3 f0 33 40` (9 bytes)

### 3. Encoded `OpRecord` Framework Record

- Frame Type ID: varint `5` (`0x05`)
- `type_id = 16`: tag `(2 << 3) | 0 = 0x10`, varint `16` (`0x10`) $\rightarrow$ `10 10`
- `key`: tag `(4 << 3) | 2 = 0x22`, length `2` (`0x02`), `08 2a` $\rightarrow$ `22 02 08 2a`
- `value`: tag `(5 << 3) | 2 = 0x2a`, length `16` (`0x10`), followed by value bytes $\rightarrow$ `2a 10 12 05 41 6c 69 63 65 19 71 3d 0a d7 a3 f0 33 40`

### 4. Complete Wire Payload

```
05 10 10 22 02 08 2a 2a 10 12 05 41 6c 69 63 65 19 71 3d 0a d7 a3 f0 33 40
```

<div class="byte-breakdown">
  <div class="byte-breakdown-title">Byte-by-Byte Wire Breakdown (25 bytes total)</div>
  <div class="byte-row">
    <div class="byte-hex">05</div>
    <div class="byte-desc"><strong>Layer 1 Type ID:</strong> Varint 5 (OpRecord framework record)</div>
  </div>
  <div class="byte-row">
    <div class="byte-hex">10 10</div>
    <div class="byte-desc"><strong>OpRecord.type_id:</strong> Tag 2 (varint), table type ID 16 (<code>shop.Order</code>)</div>
  </div>
  <div class="byte-row">
    <div class="byte-hex">22 02 08 2a</div>
    <div class="byte-desc"><strong>OpRecord.key:</strong> Tag 4, length 2 bytes, canonically encoded <code>Order.id = 42</code></div>
  </div>
  <div class="byte-row">
    <div class="byte-hex">2a 10 ...</div>
    <div class="byte-desc"><strong>OpRecord.value:</strong> Tag 5, length 16 bytes, non-key fields <code>customer="Alice"</code>, <code>total=19.99</code></div>
  </div>
</div>

### Why This is Powerful

1. **Minimal Overhead:** Only **5 bytes** of framing overhead (1 byte frame type ID + 2 bytes `type_id` + 2 bytes key/value length delimiters) for the entire record.
2. **Zero Code Generation for Generic Consumers:** Decoders walk the raw proto tags or parse into `dynamicpb.Message` using the in-band `FileDescriptorSet`.
3. **Canonical Key Indexing:** Key bytes are directly usable as map keys and index entries without decoding.
