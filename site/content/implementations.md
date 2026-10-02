---
title: "Implementations & Roadmap"
kicker: "Software & Specifications"
summary: "Reference Go implementation, architecture decisions, and active roadmap under Issue #97."
status_note: "Experimental development in the gke-labs/in-cluster-storage repository."
next_url: "idea/"
next_title: "Back to The Idea"
---

Structured Data Streams is being implemented as a first-party Go library and CLI tool in the [`gke-labs/in-cluster-storage`](https://github.com/gke-labs/in-cluster-storage) repository.

---

## Reference Go Implementation

The Go implementation is located in the root Go module of the repository:

- `proto/sds.proto`: Protocol buffer definitions for `TypeDefinition`, `RowChange`, `TxCommit`, and `SnapshotPointer`.
- `pkg/sds`: Pure-Go client library providing typed stream adapters, type registry management, dynamic protobuf decoders, and transaction buffers.
- `cmd/sds`: Command-line inspection tool (including `sds cat` for decoding and dumping stream segments without custom code generation).

### Key Implementation Principles

1. **Pure-Go Dependencies:**  
   Because all repository container images build with `CGO_ENABLED=0`, SDS uses pure-Go database engines (such as [`modernc.org/sqlite`](https://modernc.org/sqlite) and pure-Go Parquet libraries).
2. **Zero Code Generation for Generic Tools:**  
   Relays, dump tools, and generic projectors decode message payloads dynamically at runtime using `google.golang.org/protobuf/types/dynamicpb` backed by the descriptors in `TypeDefinition`.
3. **Stream Layering:**  
   Built cleanly on top of `pkg/wal`'s append and tailing APIs ([Issue #99](https://github.com/gke-labs/in-cluster-storage/issues/99)), keeping storage buffers completely oblivious to record schemas.

---

## Active Roadmap

Progress is tracked under master issue **[#97](https://github.com/gke-labs/in-cluster-storage/issues/97)**:

### Wave 1: Core Foundation & Tooling
- [x] **Website & Documentation:** (This website, [Issue #100](https://github.com/gke-labs/in-cluster-storage/issues/100))
- [ ] **Layer 1 Framing:** `varint(type_id)` framing and in-band type registry in `pkg/sds` ([Issue #98](https://github.com/gke-labs/in-cluster-storage/issues/98))
- [ ] **Stream Tail API:** Per-stream tailing with `stream_id` filter and `stream_seq` resume ([Issue #99](https://github.com/gke-labs/in-cluster-storage/issues/99))
- [ ] **Stable Inode Numbers:** Prerequisite for ObjectFS inode tables ([Issue #94](https://github.com/gke-labs/in-cluster-storage/issues/94))

### Wave 2: Layer 2 & Stream Adapters
- [ ] **Layer 2 Mutations:** `RowChange`, `TxCommit`, column derivation, dynamic protobuf decoding ([Issue #102](https://github.com/gke-labs/in-cluster-storage/issues/102))
- [ ] **Typed Stream Adapter & CLI:** `sds cat` inspect tool ([Issue #103](https://github.com/gke-labs/in-cluster-storage/issues/103))

### Wave 3: Projections & Specifications
- [ ] **SQLite Projector:** Materializing and serving SQLite snapshots ([Issue #104](https://github.com/gke-labs/in-cluster-storage/issues/104))
- [ ] **Parquet Projector:** Materializing append-only and mutable Parquet tables ([Issue #105](https://github.com/gke-labs/in-cluster-storage/issues/105))
- [ ] **Normative Spec v0:** Formal specification document and golden test vectors ([Issue #106](https://github.com/gke-labs/in-cluster-storage/issues/106))
- [ ] **ObjectFS Metadata Stream:** Inode, DirEntry, and Content tables ([Issue #107](https://github.com/gke-labs/in-cluster-storage/issues/107))

### Wave 4: Integration
- [ ] **ObjectFS EROFS Projection:** Compiling EROFS images as an SDS projection ([Issue #108](https://github.com/gke-labs/in-cluster-storage/issues/108))
- [ ] **ObjectFS Controller Migration:** Replacing circular-buffer store with SQLite projection ([Issue #109](https://github.com/gke-labs/in-cluster-storage/issues/109))

---

## Status & Honesty Note

> **Disclaimer:** Structured Data Streams is an experimental proposal and actively evolving protocol. We do not make performance or production-readiness claims until the conformance test suite passes and real-world benchmarks on ObjectFS are completed.
