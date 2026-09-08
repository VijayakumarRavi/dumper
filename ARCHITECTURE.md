# Dumper Architecture

Dumper is architected as an ultra-compact, resource-sensitive database backup appliance in a single static Rust binary.

## Architectural Layers

```text
┌────────────────────────────────────────────────────────┐
│                      CLI Layer                         │
│   (clap derive, signal handling, json/table formatting) │
└───────────────────────────┬────────────────────────────┘
                            │
                            ▼
┌────────────────────────────────────────────────────────┐
│                   Engine Pipeline                      │
│   Database Stream ──► Chunker ──► Zstd ──► XChaCha20   │
└───────────────┬────────────────────────┬───────────────┘
                │                        │
       ┌────────┴────────┐      ┌────────┴────────┐
       ▼                 ▼      ▼                 ▼
  PostgreSQL          MySQL   Local FS          S3 API
   Adapter           Adapter  Storage         (SigV4)
```

## Streaming & Memory Management

### 1. Zero Full-Buffer Staging
Dumper eliminates the standard pitfall of backup utilities: loading entire tables, databases, or S3 multipart parts into an unbounded `Vec<u8>`.

* **PostgreSQL Backup**:
  - Starts a `REPEATABLE READ READ ONLY` transaction.
  - Inspects catalogs for table and constraint metadata.
  - Calls `COPY <table> TO STDOUT (FORMAT binary)` directly.
  - Wire protocol packets (64 KiB slices) are framed into Dumper stream records (`TableDataSliceRecord`).
  - Frames stream directly into the bounded chunker.
* **MySQL Backup**:
  - Executes `START TRANSACTION WITH CONSISTENT SNAPSHOT`.
  - Queries `information_schema` and `SHOW CREATE TABLE`.
  - Streams rows in bounded batches (1,000 rows max per slice).
* **Chunking**:
  - The `StreamChunker` buffers data only up to the fixed chunk size (default: 2 MiB).
  - Memory usage is $O(\text{chunk\_size} \times \text{workers})$, independent of whether the database is 10 MB or 1 TB.
  - Typical RSS during backup remains strictly under 30–64 MB.

### 2. Backpressure
Every step of the pipeline is synchronous or bounded-async:
```text
Database Wire Stream ──► Encoder ──► Chunker (2 MiB) ──► Zstd ──► XChaCha20 ──► Storage PUT
```
If network bandwidth to S3 or disk I/O slows down, backpressure naturally halts consumption from the database connection socket. No intermediary queues or unbounded channels exist.

### 3. Concurrency Model
The default concurrency is intentionally conservative:
```text
workers = 1
```
This protects production databases from CPU and I/O starvation. Users can opt into higher concurrency using `--parallel N`.

## Storage Model & Crash Safety

### 1. Content Addressing & Deduplication
* Blobs are stored at `blobs/<prefix>/<sha256-hash>`.
* Before compressing and encrypting a chunk, Dumper computes its SHA-256 hash.
* If the blob already exists in storage, upload is skipped entirely, saving bandwidth, storage costs, and CPU time.
* Every snapshot is an independent set of references to immutable content-addressed blobs. There are no fragile incremental chains.

### 2. Atomic Commit State Machine
1. `STARTED`: Snapshot ID allocated, start timestamp recorded.
2. `WRITING`: Data blobs uploaded incrementally.
3. `FINALIZING`: Database transaction cleanly closed.
4. `COMMITTED`: Snapshot metadata written to `snapshots/<id>`.

If the process crashes or receives `SIGKILL` at any point prior to step 4, the uncommitted snapshot never appears in `dumper snapshots` or `dumper restore`. Pre-existing committed backups remain 100% valid and untouched.
