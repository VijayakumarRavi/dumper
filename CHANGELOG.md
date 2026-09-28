# Changelog

All notable changes to Dumper will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.2.1] - 2026-09-28

### Added

- `--keep-hourly <N>` retention policy option in `dumper forget` matching restic's hourly retention semantics.
- `--s3-ca-cert <PATH>` CLI option for verifying S3 endpoints against custom private CA certificates.
- `sslrootcert` connection URI parameter support in PostgreSQL adapter for strict TLS certificate validation.
- `--single-transaction` flag in PostgreSQL restore pipeline to run schema and data restore atomically in a single transaction.
- Native Garage S3 integration test harness (`TestGarageServer`) and GitHub Actions E2E service matrix.

### Changed

- Migrated local S3 test harnesses and CI workflows from MinIO to Garage S3 v1.3.1.
- Updated documentation (`COMPATIBILITY.md`, `README.md`, `THREAT_MODEL.md`, `Build Dumper.md`) and CLI help strings to reference Garage S3.
- Removed deprecated insecure package allowances from `flake.nix` and bundled `garage` in the Nix dev shell.
- Updated byte slice chunking in PostgreSQL certificate decoding to Rust 1.98 `as_chunks` idiom.

### Security

- **SEC-01 / SEC-02**: Encrypted snapshot metadata files (`snapshots/<id>`) under the repository master key via XChaCha20-Poly1305 authenticated envelopes.
- **SEC-03**: Enforced repository passphrase and master key authentication before allowing `dumper unlock`.
- **SEC-05**: Moved compression indicator byte into the authenticated AEAD plaintext payload prior to encryption.
- **SEC-10**: Automatically enforced strict POSIX permissions (`0700` directories, `0600` files) on local repository creation.

## [0.2.0] - 2026-09-25

### Added

- MySQL 8.0 and MariaDB 11.4 native streaming adapters.
- Cross-platform release CD packaging for Linux (gnu/musl), macOS (Intel/ARM), and Windows.
- Multi-engine crash safety and consistency test suites.

### Changed

- Improved memory boundedness and backpressure handling across streaming pipelines.

## [0.1.0] - 2026-09-10

### Added

- Initial release of Dumper CLI.
- Native PostgreSQL streaming backup and restore pipeline.
- Streaming DMP1 protocol with chunking, zstd compression, and XChaCha20-Poly1305 encryption.
- Local filesystem and S3-compatible repository backends.
- Snapshot management: `backup`, `restore`, `verify`, `check`, `snapshots`, `info`, `forget`, `prune`, `stats`, `unlock`.
