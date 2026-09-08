# Dumper Repository & Stream Format Specification

## 1. Repository Layout

Whether stored on local disk or in an S3 bucket, a Dumper repository has an identical logical hierarchy:

```text
<repository-root>/
├── config
├── snapshots/
│   ├── 7c4f9c3a
│   └── a8b12e4f
├── blobs/
│   ├── ab/
│   │   └── abcdef123456...
│   └── cd/
│       └── cde789012345...
└── locks/
    └── 3f8a10bc
```

### 1.1 `config` Object
The repository configuration contains encryption parameters and envelope key material:
```json
{
  "format_version": 1,
  "repository_id": "9b12a83f47ce560124a9...",
  "created_at": "2026-09-08T02:00:00Z",
  "envelope": {
    "salt_hex": "e4f8...",
    "encrypted_master_key_hex": "01af..."
  }
}
```

### 1.2 `snapshots/<id>` Object
Each snapshot is represented by an atomic JSON metadata file:
```json
{
  "id": "7c4f9c3a",
  "full_id": "7c4f9c3a98124b81023a...",
  "format_version": 1,
  "dumper_version": "0.1.0",
  "engine": "postgresql",
  "database": "production",
  "server_version": "PostgreSQL 17.4",
  "started_at": "2026-09-08T02:00:01Z",
  "completed_at": "2026-09-08T02:04:32Z",
  "duration_seconds": 271,
  "logical_bytes": 9348234234,
  "stored_bytes": 1284234234,
  "deduplicated_bytes": 8064000000,
  "table_count": 48,
  "compression": "default",
  "tag": "nightly",
  "blobs": [
    {
      "hash": "abcdef123456...",
      "raw_size": 2097152,
      "stored_size": 842100,
      "compression_tag": 1
    }
  ]
}
```

### 1.3 `blobs/<prefix>/<hash>`
Each blob is an immutable, encrypted, compressed chunk:
```text
[ Compression Tag (1B) | XChaCha20 Nonce (24B) | Ciphertext + Poly1305 Tag (N+16B) ]
```
Compression tags:
- `0x00`: Uncompressed
- `0x01`: Zstandard

### 1.4 `locks/<lock_id>`
Locks manage concurrent access:
```json
{
  "lock_id": "3f8a10bc",
  "lock_type": "Exclusive",
  "hostname": "backup-worker-01",
  "pid": 4120,
  "created_at": "2026-09-08T02:00:00Z"
}
```

---

## 2. Backup Stream Framing (`DMP1`)

The database backup stream is encoded in sequentially frame-delimited binary records. Seeking is never required for restore.

### Frame Layout
```text
┌──────────────┬────────────┬─────────────┬───────────┬──────────────┐
│ Record Type  │   Flags    │ Payload Len │  Payload  │ CRC32 (LE)   │
│   (1 byte)   │  (1 byte)  │   (4 bytes) │ (N bytes) │  (4 bytes)   │
└──────────────┴────────────┴─────────────┴───────────┴──────────────┘
```

### Stream Magic Header
Every stream starts with the 4-byte sequence: `b"DMP1"` (`0x44 0x4D 0x50 0x31`).

### Record Types
* `0x01` **Header**: Engine, database name, server version, start time.
* `0x02` **PreData**: Schema creation, custom types, extensions.
* `0x03` **TableSchema**: Column definitions, data types, nullability, CREATE TABLE DDL.
* `0x04` **TableDataSlice**: Streamed table data slices (COPY binary or row batches).
* `0x05` **Sequence**: Sequence state and current value.
* `0x06` **PostData**: Secondary indexes, foreign keys, unique constraints.
* `0x07` **Routine**: Views, functions, stored procedures, triggers.
* `0xFF` **Trailer**: Total record count, total uncompressed bytes, stream SHA-256 hash.
