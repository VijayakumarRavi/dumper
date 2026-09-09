# Compatibility & Versioning Matrix

## 1. Versioning Strategy

Dumper maintains separate version counters for key system layers:

1. **Tool Version**: Follows Semantic Versioning (`MAJOR.MINOR.PATCH`).
2. **Repository Format Version**: Currently `1`. Incrementing this indicates a structural change to repository index or metadata layout. Future Dumper versions will maintain read compatibility with older format versions.
3. **Stream Format Version**: Currently `DMP1` (`version: 1`). Identifies the binary record layout of logical backups.

---

## 2. Supported Database Engines

| Engine         | Versions Tested & Supported     | Consistent Snapshot Mechanism                | Streaming Protocol                   |
| -------------- | ------------------------------- | -------------------------------------------- | ------------------------------------ |
| **PostgreSQL** | 13, 14, 15, 16, 17, 18+         | `REPEATABLE READ READ ONLY`                  | `COPY ... TO STDOUT (FORMAT binary)` |
| **MySQL**      | 8.0, 8.4 LTS, 9.0+              | `START TRANSACTION WITH CONSISTENT SNAPSHOT` | Streaming cursor query               |
| **MariaDB**    | 10.5, 10.6, 10.11 LTS, 11.4 LTS | `START TRANSACTION WITH CONSISTENT SNAPSHOT` | Streaming cursor query               |

---

## 3. Supported Storage Backends

| Backend                          | Protocol                | Compatibility Notes                                       |
| -------------------------------- | ----------------------- | --------------------------------------------------------- |
| **Local Filesystem**             | POSIX / Windows / macOS | Atomic rename writes; path traversal protection           |
| **Amazon Web Services (AWS S3)** | S3 REST / SigV4         | Standard S3, S3 Standard-IA, S3 Glacier Instant Retrieval |
| **MinIO**                        | S3 API                  | Fully tested; path-style addressing supported             |
| **Cloudflare R2**                | S3 API                  | Custom endpoint URL supported; SigV4                      |
| **Garage**                       | S3 API                  | Fully tested with devshell integration                    |
| **Wasabi**                       | S3 API                  | Supported across all regions                              |
| **Backblaze B2**                 | S3-Compatible API       | Supported with standard S3 keys                           |
| **Google Cloud Storage (GCS)**   | S3 Interoperability     | Supported with GCS HMAC access keys                       |

---

## 4. Unsupported Scope in v1

- **Physical WAL Replication / Barman-style PITR**: Dumper v1 is focused exclusively on fast, compact logical backups. Physical base backups may be considered in future releases.
- **Non-relational databases**: No MongoDB, Redis, or Cassandra support.
