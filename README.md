# Dumper 🗄️

**Dumper** is a tiny, production-grade database backup, restore, verification, retention, and repository-management CLI written in Rust for:

- **PostgreSQL**
- **MySQL / MariaDB**

Inspired by the operational models of `restic`, `pgBackRest`, and `mysqldump`, Dumper is built for a focused mission:

> **A single small Rust binary that performs reliable database backups to local disk or S3-compatible object storage while consuming very little CPU, memory, and storage overhead.**

Designed to run safely inside edge containers (64–128 MB RAM, 0.25–1 vCPU) without impacting production database workloads.

---

## Key Highlights

- **Single Statically-Linked Binary**: Zero runtime dependencies. Does NOT require `pg_dump`, `pg_restore`, `mysqldump`, `mysql`, `openssl`, `gzip`, or the AWS CLI.
- **Wire Protocol Streaming**: Direct native communication with PostgreSQL and MySQL. Tables are extracted and restored using streaming wire protocol frames (`COPY` binary streams for PostgreSQL, streaming row batches for MySQL).
- **Constant Memory Footprint**: Bounded memory usage (< 30–64 MB RSS) regardless of whether backing up a 10 MB or a 100 GB database. No staging of entire tables or databases in RAM or temporary disk.
- **Content-Addressed Deduplication**: Chunks are content-hashed (SHA-256) and deduplicated across all snapshots. Identical data is stored only once.
- **Client-Side Authenticated Encryption**: Authenticated AEAD encryption (**XChaCha20-Poly1305**) with a master key derived via **Argon2id** password hashing. Keys never leave the client.
- **Streaming Zstandard Compression**: Multi-level zstd compression (`none`, `fast`, `default`, `max`).
- **S3 as a First-Class Backend**: Built-in AWS SigV4 signer supporting Amazon S3, MinIO, Cloudflare R2, Wasabi, Backblaze B2, and Garage.
- **Crash Safety**: Atomic snapshot commits. Partial or failed backups remain uncommitted and never corrupt previous snapshots.
- **Full Lifecycle**: `init`, `backup`, `snapshots`, `info`, `restore`, `verify`, `check`, `forget`, `prune`, `stats`, and `unlock`.

---

## Quick Start

### 1. Initialize Repository

#### Local Filesystem

```bash
dumper init --repository /mnt/backups/postgres
```

#### S3-Compatible Storage (AWS, MinIO, Garage, R2)

```bash
export DUMPER_REPOSITORY="s3://my-backups/postgres"
export DUMPER_PASSWORD="secure-repo-password"
export DUMPER_S3_ENDPOINT="https://s3.example.com"
export DUMPER_S3_ACCESS_KEY_ID="minioadmin"
export DUMPER_S3_SECRET_ACCESS_KEY="minioadmin"

dumper init
```

### 2. Backup a Database

#### PostgreSQL

```bash
dumper backup postgres://app_user:secret@localhost:5432/production_db
```

#### MySQL / MariaDB

```bash
dumper backup mysql://app_user:secret@localhost:3306/production_db
```

### 3. List Snapshots

```bash
dumper snapshots
```

Output:

```text
ID          DATE                  ENGINE        DATABASE      LOGICAL     STORED    
--------------------------------------------------------------------------------
7c4f9c3a    2026-09-08 02:00:01   postgresql    production    8.70 GiB    1.40 GiB
```

### 4. Verify Snapshot Data Integrity

```bash
# Verify hashes, authenticated encryption, and decompression
dumper verify 7c4f9c3a

# Full stream reconstruction dry-run
dumper verify 7c4f9c3a --restore-test
```

### 5. Restore Database

```bash
dumper restore 7c4f9c3a --target postgres://postgres:secret@recovery-host:5432/production_db
```

### 6. Retention and Garbage Collection

```bash
# Forget older snapshots based on retention policies
dumper forget --keep-last 7 --keep-daily 14 --keep-weekly 8 --keep-monthly 12 --prune
```

### 7. Repository Statistics

```bash
dumper stats
```

Output:

```text
Snapshots:             31
Unique blobs:           17,428
Repository size:       48.2 GiB
Logical backup size:   280.0 GiB
Deduplication ratio:   5.8x
```

---

## Configuration & Environment Variables

| Variable                      | Flag                     | Description                                  |
| ----------------------------- | ------------------------ | -------------------------------------------- |
| `DUMPER_REPOSITORY`           | `-r, --repository`       | Local directory path or `s3://bucket/prefix` |
| `DUMPER_PASSWORD`             | `--password`             | Repository encryption password               |
| `DUMPER_PASSWORD_FILE`        | `--password-file`        | Path to file containing password             |
| `DUMPER_S3_ENDPOINT`          | `--endpoint`             | S3 custom endpoint URL                       |
| `DUMPER_S3_REGION`            | `--region`               | S3 region (default: `us-east-1`)             |
| `DUMPER_S3_ACCESS_KEY_ID`     | `--s3-access-key-id`     | S3 access key ID                             |
| `DUMPER_S3_SECRET_ACCESS_KEY` | `--s3-secret-access-key` | S3 secret access key                         |
| `DUMPER_S3_SESSION_TOKEN`     | `--s3-session-token`     | Temporary AWS STS token                      |

---

## Exit Codes

Dumper returns stable exit codes for automated tooling and scripting:

- `0`: Success
- `1`: General runtime error
- `2`: CLI usage / configuration error
- `3`: Authentication error (invalid password)
- `4`: Database connection / execution error
- `5`: Repository / storage error
- `6`: Data integrity verification failure
- `7`: Database restore failure
- `8`: Process interrupted (SIGINT / SIGTERM)

---

## License

Dual-licensed under MIT or Apache 2.0.
