# Compatibility & Versioning Matrix

## 1. Versioning Strategy

Dumper maintains separate version counters for key system layers:

1. **Tool Version**: Follows Semantic Versioning (`MAJOR.MINOR.PATCH`).
2. **Repository Format Version**: Currently `1`. Incrementing this indicates a structural change to repository index or metadata layout. Future Dumper versions will maintain read compatibility with older format versions.
3. **Stream Format Version**: Currently `DMP1` (`version: 1`). Identifies the binary record layout of logical backups.

---

## 2. Supported Database Engines

### Continuous Integration (CI) Matrix Validated

The following database engines and major versions are continuously tested in automated end-to-end matrix pipelines (running on every commit and PR across Linux, macOS, and Windows):

| Engine         | Versions Continuously Tested | Consistent Snapshot Mechanism                | Streaming Protocol                     | CI Verification Level                                                              |
| -------------- | ---------------------------- | -------------------------------------------- | -------------------------------------- | ---------------------------------------------------------------------------------- |
| **PostgreSQL** | 14, 15, 16, 17               | `REPEATABLE READ READ ONLY`                  | `COPY ... TO STDOUT (FORMAT binary)`   | Schema + data roundtrip, sequences, constraints, table drop + restore equality     |
| **MySQL**      | 8.0, 8.4 LTS                 | `START TRANSACTION WITH CONSISTENT SNAPSHOT` | Streaming cursor query (`mysql_async`) | Schema + batch inserts, table drop + restore equality                              |
| **MariaDB**    | 10.11 LTS, 11.4 LTS          | `START TRANSACTION WITH CONSISTENT SNAPSHOT` | Streaming cursor query (`mysql_async`) | Transaction rollback on failure, escaping roundtrip, table drop + restore equality |

### Target Wire-Protocol Compatibility (Not Continuously Tested in CI)

The following versions target the same wire protocols and SQL dialects, but are **not** continuously validated in automated CI test suites. Operators must validate backups and restores on a staging cluster before relying on them in production:

- **PostgreSQL 13 and 18+**: Expected to work via PostgreSQL wire protocol, but not covered by automated regression suites.
- **MySQL 9.0+**: Expected to work via MySQL wire protocol, but authentication plugins or syntax shifts have not been validated in CI.
- **MariaDB 10.5, 10.6**: Older LTS releases sharing the MariaDB wire protocol, not covered by automated regression suites.

---

## 3. Supported Storage Backends

### Continuous Integration (CI) Storage Validation

| Backend              | Protocol                | CI Testing Status  | Details                                                                                                                                                            |
| -------------------- | ----------------------- | :----------------: | ------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| **Local Filesystem** | POSIX / Windows / macOS | **Verified in CI** | Atomic rename writes, directory sharding (`data/xx/yyyy...`), path traversal protection                                                                            |
| **MinIO**            | S3 REST / SigV4         | **Verified in CI** | Automated container & process tests on Linux, macOS, Windows; multipart uploads, SigV4 authentication, path-style addressing, HTTP 403/404/429/503 error injection |

### S3-Compatible Cloud Providers (Target Supported via S3 API)

Dumper implements standard AWS SigV4 request signing over HTTP/HTTPS, supporting custom endpoints and path-style addressing. The following cloud providers expose S3-compatible APIs and are target architectures:

- **Amazon Web Services (AWS S3)** (Standard, S3 Standard-IA)
- **Cloudflare R2**
- **Garage** (Local devshell integration available)
- **Wasabi**
- **Backblaze B2**
- **Google Cloud Storage (GCS)** (via S3 interoperability keys)

> [!WARNING]
> **Provider Idiosyncrasy Disclaimer**: Automated CI test suites validate MinIO and the local filesystem. While Dumper uses standard S3 REST requests with AWS SigV4 signatures, third-party and commercial cloud providers possess vendor-specific idiosyncrasies (e.g. multipart part size minimums, eventual consistency windows, proprietary rate limit responses, header casing, or signed URL expiration policies). **MinIO test results do not constitute end-to-end certification of every commercial S3 provider.** Operators are required to execute full backup and restore drill cycles against their specific cloud provider and bucket configurations prior to production deployment.

---

## 4. Unsupported Scope in v1

- **Physical WAL Replication / Barman-style PITR**: Dumper v1 is focused exclusively on fast, compact logical backups. Physical base backups may be considered in future releases.
- **Non-relational databases**: No MongoDB, Redis, or Cassandra support.
