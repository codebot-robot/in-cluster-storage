---
title: "The Wire Format"
kicker: "Framing & Encodings"
summary: "Layer 1 record typing and Layer 2 logical change log framing, with a byte-by-byte wire example."
status_note: "Draft specification. A normative spec and test vectors will land in Issue #106."
next_url: "/projections/"
next_title: "Projections"
---

The Structured Data Streams wire format is split into two clean layers:
- **Layer 1 (Record Typing):** How different record kinds are multiplexed into one opaque stream.
- **Layer 2 (Logical Change Log):** How relational mutations (tables, rows, transactions) are represented.

> **Normative Specification:** A formal, normative specification document with golden test vectors is tracked in [Issue #106](https://github.com/gke-labs/in-cluster-storage/issues/106). This page provides an intuitive, high-level guide.

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
| `5`–`15` | *Reserved* | Reserved for future core framework extensions. | — |
| `16`+ | **Application Types** | Defined by a preceding `TypeDefinition`. Costs only 1 byte for IDs $\le 127$. | `RowChange` wrapping user proto |

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

Application tables are simply existing Protobuf messages. Writers do not need generated table wrappers; mutations are represented by a generic framework envelope:

```protobuf
message RowChange {
  enum Op {
    INSERT = 0;
    UPDATE = 1;
    DELETE = 2;
  }
  Op     op = 1;
  uint64 tx_id = 2;   // 0 for autocommit; non-zero held pending until TxCommit
  bytes  before = 3;  // Key fields for DELETE/UPDATE (or full image for undo/CDC)
  bytes  after = 4;   // Full row image for INSERT/UPDATE
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

Let us trace an application inserting a new order row into table `shop.Order` (registered as type ID `16`).

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

### 2. Encoded `Order` Body (`after`)

In protobuf wire format:
- Field 1 (`id = 42`): tag `(1 << 3) | 0 = 0x08`, varint `42` (`0x2a`) $\rightarrow$ `08 2a` (2 bytes)
- Field 2 (`customer = "Alice"`): tag `(2 << 3) | 2 = 0x12`, length `5` (`0x05`), UTF-8 `"Alice"` (`41 6c 69 63 65`) $\rightarrow$ `12 05 41 6c 69 63 65` (7 bytes)
- Field 3 (`total = 19.99`): tag `(3 << 3) | 1 = 0x19`, 64-bit IEEE-754 float `19.99` (`71 3d 0a d7 a3 f0 33 40`) $\rightarrow$ `19 71 3d 0a d7 a3 f0 33 40` (9 bytes)

Total serialized `Order` length = **18 bytes**.

### 3. Encoded `RowChange` Envelope

- Field 1 (`op = INSERT = 0`): default value, 0 bytes on wire.
- Field 2 (`tx_id = 0`): default value (autocommit), 0 bytes on wire.
- Field 4 (`after`): tag `(4 << 3) | 2 = 0x22`, length `18` (`0x12`), followed by the 18 bytes above $\rightarrow$ `22 12 ...` (20 bytes total).

### 4. Complete Wire Payload

Prepending the Layer 1 type ID (`16 = 0x10`):

```
10 22 12 08 2a 12 05 41 6c 69 63 65 19 71 3d 0a d7 a3 f0 33 40
```

<div class="byte-breakdown">
  <div class="byte-breakdown-title">Byte-by-Byte Wire Breakdown (21 bytes total)</div>
  <div class="byte-row">
    <div class="byte-hex">10</div>
    <div class="byte-desc"><strong>Layer 1 Type ID:</strong> Varint 16 (resolves to registered table <code>shop.Order</code>)</div>
  </div>
  <div class="byte-row">
    <div class="byte-hex">22 12</div>
    <div class="byte-desc"><strong>RowChange.after:</strong> Tag 4 (wire type 2: length-delimited), length 18 bytes</div>
  </div>
  <div class="byte-row">
    <div class="byte-hex">08 2a</div>
    <div class="byte-desc"><strong>Order.id:</strong> Tag 1 (varint), value 42 (Primary Key)</div>
  </div>
  <div class="byte-row">
    <div class="byte-hex">12 05 41 6c 69 63 65</div>
    <div class="byte-desc"><strong>Order.customer:</strong> Tag 2 (length-delimited), length 5, string <code>"Alice"</code></div>
  </div>
  <div class="byte-row">
    <div class="byte-hex">19 71 3d 0a d7 a3 f0 33 40</div>
    <div class="byte-desc"><strong>Order.total:</strong> Tag 3 (64-bit fixed), float <code>19.99</code></div>
  </div>
</div>

### Why This is Powerful

1. **Minimal Overhead:** Only **3 bytes** of framing overhead (1 byte type ID + 2 bytes `RowChange` tag & length) for the entire record.
2. **Zero Code Generation for Generic Consumers:** Decoders walk the raw proto tags or parse into `dynamicpb.Message` using the in-band `FileDescriptorSet`.
3. **Sparse Updates:** An update touching only `total` serializes only fields 1 and 3 in `after`, keeping wire size minimal.
