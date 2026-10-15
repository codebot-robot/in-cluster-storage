# Structured Data Streams (SDS) Table Snapshot Format Specification

> **Version:** v1 (prototype)  
> **Status:** Prototype Specification  
> **Related Documents:** [`docs/sds-spec.md`](sds-spec.md), [`docs/structured-data-streams.md`](structured-data-streams.md)

---

## 1. Overview and Motivation

Structured Data Streams (SDS) streams relational log mutations (`CREATE`, `UPDATE`, `DELETE`) over an append-only, ordered byte transport. To support point-in-time recovery, fast volume startup, and random-access reads, SDS generates periodic snapshots at consistent transaction boundaries (safe stream positions $P$).

Historically, published snapshots used whole SQLite database files (`.sqlite`), and the local mutable index ran SQLite with write-ahead logging (WAL). While functional, this design created tight coupling to SQLite:
1. **Unnecessary overhead:** All SDS tables are simple binary key/value relations `(keydata BLOB PRIMARY KEY, valuedata BLOB)` storing canonical SDS keys and Protobuf wire bytes. Parsing SQL schemas and running transpiled C engines (~240 MB Go driver) is excessive for a snapshot reader.
2. **Restore friction:** Restoring an SQLite snapshot requires downloading the full file and opening an engine instance, while local index recovery involves replaying WAL pages.
3. **Write amplification:** Benchmarks revealed 4–40 KB of disk writes per small filesystem metadata operation in SQLite's WAL/B-tree structures.

The **Table Snapshot Format** establishes a sorted-table-of-protos format for published snapshots and local indexing. Published snapshots are standalone, immutable, checksummed block-based tables (SSTables) with embedded schema definitions. Restore is an instant ingest operation (hard-linking the table directly into the LSM engine's L0), and external tools can read snapshots directly using standalone block-based table readers without launching an LSM engine.

### Logical vs. Physical Format Split

We accept Pebble's table format as the **physical** format of published snapshots rather than reimplementing an LSM engine or table format from scratch. What SDS owns is the **logical** format:
- The unified keyspace layout: $\text{varint}(\text{type\_id}) \mathbin{\Vert} \text{canonical SDS key bytes}$.
- The Protobuf values (non-key wire bytes).
- The reserved metadata keys (`\x00position`, `\x00registry`, `\x00stream_id`).

### Compacted Table Invariant

**A published snapshot is exactly one compacted table:**
- Contains only live entries.
- Contains no deletion tombstones or range deletions.
- Each key appears exactly once.
- Strictly sorted in ascending lexicographical order (`bytes.Compare`).

`PublishSnapshot` satisfies this invariant by construction: it streams live rows from a point-in-time database snapshot iterator (`snap.NewIter`) into a fresh table writer, bypassing the source DB's internal LSM layers, deleted entries, and superseded versions.

---

## 2. Keyspace and Key Conventions

All entries—both framework metadata and application table records—live in a single, globally sorted byte keyspace:

$$\text{Entry Key} := \text{varint}(\text{type\_id}) \mathbin{\Vert} \text{canonical SDS key bytes}$$

$$\text{Entry Value} := \text{non-key Protobuf bytes}$$

- **`type_id`:** Protobuf unsigned varint (LEB128). Identifies the table schema registered in the stream registry.
- **Canonical SDS key:** Binary Protobuf wire encoding of primary key fields in strictly ascending field-number order, as extracted by `sds.PrimaryKey.Split()` / `sds.ExtractKey()`.
- **Non-key Protobuf bytes:** Binary Protobuf wire encoding of the remaining non-key fields. A complete row message is reconstituted by merging the key and non-key bytes using `sds.MergeKeyAndNonKey()`.

### Prefix Scanning and Lexicographical Sorting

Because tables are stored in lexicographical key order (`bytes.Compare`):
1. All rows for a given `type_id` share the prefix `varint(type_id)` and are stored contiguously.
2. Scanning an entire table is a seek to `varint(type_id)` followed by sequential block iteration until the prefix no longer matches.
3. Scans with a primary key prefix `P` seek to `varint(type_id) ‖ P` with an upper bound computed by incrementing the least-significant byte of the prefix.
4. Entries within each table retain strict canonical SDS key ordering.

---

## 3. Reserved Metadata Keys (Type ID 0)

Type ID `0` is invalid for application data records and is reserved for framework metadata. Because `varint(0)` encodes to a single byte `0x00`, and application type IDs are $\ge 16$ (encoded as varints with first byte $\ge 0x10$), **all metadata keys sort strictly before all application table entries**.

| Metadata Key | Format / Value | Description |
| :--- | :--- | :--- |
| `\x00position` | 8-byte big-endian `uint64` | The stream sequence position $P$ reflected by this snapshot. |
| `\x00registry` | Protobuf wire bytes (`sds.v1.Registry`) | The full type registry in force, containing all `TypeDefinition` messages. |
| `\x00stream_id` | UTF-8 string bytes | The unique stream identifier. |

By embedding the applied position and the complete type registry directly in the snapshot table, every snapshot file is completely **self-describing**, just like a stream segment. Readers require no external catalogs or out-of-band schema definitions.

---

## 4. Pinned Physical Format

Published snapshots are formatted as block-based Sorted String Tables (SSTables) following Pebble's block-based table format specification.

### Version Pinning: TableFormatPebblev4

Published snapshots pin **`sstable.TableFormatPebblev4`**:
- **Why v4?** `TableFormatPebblev4` is the newest **row-oriented** table format in Pebble. Pebble v5 and later switch to columnar blocks (`TableFormatPebblev5`), which require full column schemas and make an independent, standalone table reader significantly more complex. Pinning v4 keeps the classic, well-documented block layout while maximizing future compatibility runway before the engine's minimum supported format could reach it.
- **Local DB Format Major Version:** The local mutable Pebble database pins `Options.FormatMajorVersion = pebble.FormatVirtualSSTables` (the format version whose maximum table format is v4), ensuring consistent on-disk manifest and store behavior across engine upgrades.

### Physical Layout and Constants

```
+-------------------------------------------------------------+
| Data Block 0: Metadata Keys (\x00position, \x00registry...)  |
+-------------------------------------------------------------+
| Data Block 1: Application Rows (varint(type_id) || key)     |
+-------------------------------------------------------------+
| ...                                                         |
+-------------------------------------------------------------+
| Filter Block (Bloom filter: 10 bits/key)                    |
+-------------------------------------------------------------+
| Meta Index Block (properties, table metadata)               |
+-------------------------------------------------------------+
| Index Block (binary search index over data blocks)          |
+-------------------------------------------------------------+
| Footer (53 bytes: handles, version, magic)                  |
+-------------------------------------------------------------+
```

A from-scratch reader can parse the file without Pebble source code using the following constants:
- **Footer Length:** Exactly 53 bytes (`rocksDBFooterLen` = 1 byte format version + $2 \times 20$ bytes max block handles + 4 bytes version + 8 bytes magic).
- **Magic Bytes:** 8 bytes at the very end of the file (`buf[len-8:]`):  
  `\xf0\x9f\xaa\xb3\xf0\x9f\xaa\xb3` (hex: `0xf0 0x9f 0xaa 0xb3 0xf0 0x9f 0xaa 0xb3`, UTF-8 encoding of `🪳🪳`).
- **Table Format Version:** 4-byte little-endian `uint32` immediately preceding the magic bytes (`buf[len-12:len-8]`): `0x04, 0x00, 0x00, 0x00`.
- **Block Size:** 4096 bytes target block size.
- **Restart Interval:** 16 keys between delta-compression restart points.
- **Checksum:** 4-byte CRC32C checksum trailer on each block.
- **Filter Policy:** 10 bits per key Bloom filter (`bloom.FilterPolicy(10)`).
- **Compression:** Disabled (`sstable.NoCompression`) for local metadata index SSTables to maximize point-read throughput.

---

## 5. Reading Snapshots Without an Engine and Portability

### Portability Reality

Pebble table formats carry Pebble's magic number (`pebbleDBMagic` = `\xf0\x9f\xaa\xb3\xf0\x9f\xaa\xb3`), not RocksDB's magic number (`rocksDBMagic` = `\xf7\xcf\xf4\x85\xb7\x41\xe2\x88`). Consequently, standard RocksDB C++, Rust, and Java tooling will reject the file on magic number mismatch. Furthermore, Pebble's store-level directory format is not RocksDB-compatible at any format version supported by modern Pebble.

Snapshots are readable by:
1. Pebble's `sstable` Go package (`sstable.NewReader`, `sstable.NewMemReader`).
2. The SDS standalone `SnapshotReader` (`table.OpenSnapshotFile`, `table.OpenSnapshotBytes`).
3. Any independent reader implementing the documented 53-byte footer, restart point decoding, and CRC32C block parsing.

### Example: Standalone Reader in Go

```go
package main

import (
    "fmt"
    "github.com/gke-labs/in-cluster-storage/pkg/sds/projection/table"
)

func InspectSnapshot(path string) error {
    sr, err := table.OpenSnapshotFile(path)
    if err != nil {
        return err
    }
    defer sr.Close()

    fmt.Printf("Stream ID: %s\n", sr.StreamID())
    fmt.Printf("Applied Position: %d\n", sr.Position())
    fmt.Printf("Registered Tables: %v\n", sr.Registry().TypeNames())

    // Iterate through rows in a table
    for msg, err := range sr.Scan("objectfs.v1alpha1.Inode", nil) {
        if err != nil {
            return err
        }
        fmt.Printf("Row: %v\n", msg)
    }
    return nil
}
```

---

## 6. Object Storage Layout and Ingestion Lifecycle

### Canonical Key Path

Published snapshots are stored at:
```
streams/<stream_id>/snapshots/table/<position, 20 digits>.sst
```
Example: `streams/vol-42/snapshots/table/00000000000000010000.sst`

### Restore via Ingest

When a volume restores a table snapshot:
1. The `.sst` file is downloaded into the local volume staging directory.
2. A new empty local table index is initialized.
3. The engine ingests the table file via `db.Ingest([]string{path})`.
4. Ingestion hard-links the immutable SSTable directly into L0 in a single atomic metadata operation without deserializing, row parsing, or index reconstruction.
5. The local index immediately reflects position $P$ and all rows, ready for real-time stream replay of subsequent positions $P+1 \dots$

### Ingest Does Not Re-encode

Ingest links the SSTable file into the database as-is without re-encoding:
- SSTables are immutable.
- Reads dispatch based on each table file's individual footer and format version.
- Newer writes shadow older entries in newer tables.
- Subsequent LSM compactions eventually rewrite older tables in the DB's current format.

This guarantees that format upgrades do not cause restore delays or CPU spikes during snapshot ingestion.

### Engine WAL and Durability Configuration

By default, the table local index runs with `DisableWAL = true` and `writeOpts = pebble.NoSync`.
- **Justification:** The SDS stream is the single durable source of truth; applied stream position and table mutations commit in an atomic batch; on startup or crash recovery, replay begins from the persisted stream position.
