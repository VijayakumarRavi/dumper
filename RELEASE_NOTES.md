# Dumper v0.2.1

Dumper `v0.2.1` is a maintenance and security release bringing Garage S3 migration, critical security audit hardenings, restic-compatible hourly retention policies, and compiler modernization.

---

## What's New in v0.2.1

### 1. S3 Backend: Migration to Garage S3
- **Test Infrastructure & CI**: Migrated local S3 test harnesses (`tests/s3_storage_test.rs` and `tests/crash_safety_test.rs`) and GitHub Actions E2E workflows from MinIO to **Garage S3** (v1.3.1), providing a lightweight, modern, open-source S3 testing environment.
- **Nix Dev Shell**: Cleaned `flake.nix` by removing outdated insecure package overrides and bundling `garage` directly into the development shell.
- **Documentation**: Updated `COMPATIBILITY.md`, `README.md`, `THREAT_MODEL.md`, and CLI references to reflect Garage S3 compatibility and setup.

### 2. Security & Cryptographic Hardening (SEC-01 through SEC-10)
- **Snapshot Metadata Encryption**: Snapshot metadata files (`snapshots/<id>`) are now fully encrypted under the repository master key via authenticated AEAD envelopes (`XChaCha20-Poly1305`), preventing metadata leaks on shared storage backends.
- **Authenticated Lock Release**: `dumper unlock` now validates repository credentials and master keys before modifying lock records.
- **Custom TLS & CA Verification**:
  - Added `--s3-ca-cert <PATH>` CLI option for verifying S3 endpoints against custom private CA certificates.
  - Added `sslrootcert` parameter support in PostgreSQL connection strings for strict server certificate validation.
- **Atomic Single-Transaction Restores**: Added `--single-transaction` flag to PostgreSQL restore pipelines, executing schema and data recreation in a single transaction block (`BEGIN` / `COMMIT`) to prevent partial restoration on failures.
- **AEAD Tamper Proofing**: Moved the internal compression indicator byte into the authenticated plaintext payload prior to encryption, preventing unauthenticated oracle manipulation.
- **Strict POSIX Permissions**: Automatically enforces `0700` on directories and `0600` on files created in local filesystem repositories.

### 3. Retention Policies
- **Hourly Backup Retention**: Added `--keep-hourly <N>` to `dumper forget`, providing restic-compatible retention semantics to preserve the latest snapshot in each hourly window across the last N hours of backup activity.

### 4. Code Quality & Rust 1.98 Modernization
- Addressed Rust 1.98 `chunks_exact_to_as_chunks` clippy lints in PostgreSQL SSL certificate decoding.
- Maintained 100% test pass rate across all 39 unit, integration, and crash-safety tests.

---

## Upgrade & Compatibility

- **Repository Format**: Repositories created with `v0.2.0` remain compatible; existing plaintext snapshots can still be read and verified, while new snapshots will have encrypted metadata.
- **CLI Options**: Backward compatible with all existing flags; new `--single-transaction`, `--s3-ca-cert`, and `--keep-hourly` options are opt-in.

---

## Installation

### Binary Tarballs & Checksums
Pre-built standalone binaries with SHA-256 checksums are attached below for:
- Linux `x86_64` (glibc and musl)
- Linux `aarch64` (glibc and musl)
- macOS `x86_64` (Intel)
- macOS `aarch64` (Apple Silicon)
- Windows `x86_64`
