# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.1.1](https://github.com/VijayakumarRavi/dumper/compare/v0.1.0...v0.1.1) - 2026-09-10

### Added

- *(postgres)* add TLS connection support, target database override, and COPY sink backpressure
- *(mysql)* improve binary safety and constraints handling
- *(postgres)* exhaustive database schema extraction
- initial prototype implementation

### Fixed

- *(clippy)* use char array in path split for manual-pattern-char-comparison
- *(ci)* update cargo-deny configuration and grant checks write permission to audit workflow
- *(repository)* normalize object keys to forward slashes for cross-platform Windows compatibility
- *(postgres)* exclude constraint-backed indexes from standalone index extraction
- *(s3)* include port in host header for SigV4 and add S3/MinIO tests
- *(s3)* harden xml parsing for s3 object listing and entity decoding
- *(repository)* cleanup abandoned temporary files in local backend
- *(stream)* prioritize background upload errors over pipe disconnects in backup
- *(retention)* group snapshots by database and engine to prevent cross-database deletion
- *(s3)* standardize SigV4 canonical URI and request URL matching on empty keys
- *(repository)* eliminate full-blob downloads in prune and stats
- *(compression)* enforce bounded chunk decompression to prevent zip bomb OOM
- *(repository)* add periodic lock heartbeat lease renewal and corruption-safe unlock
- *(repository)* implement RAII lock guard and signal safety for repository locks
- *(postgres)* resolve sequence deadlock and preserve sequence metadata
- *(postgres)* preserve custom types, arrays, and numeric precision in table schema
- *(stream)* enforce trailer verification before clean stream EOF
- *(mysql)* propagate restore errors on row inserts and routines
- *(postgres)* propagate restore errors on constraints, sequences, and routines
- *(repository)* abort prune on unreadable snapshot to prevent data loss
- *(repository)* prevent distributed locking races
- *(s3)* add backoff jitter and fast-fail auth errors

### Other

- *(postgres)* set unix_socket_directories to temp dir for unprivileged runners
- *(postgres)* serialize cluster tests, harden TLS setup, and fix mariadbd pid
- serialize lifecycle tests, format compatibility doc, and improve release-plz diagnostics
- fix security audit by running cargo-audit directly and add pwsh shell for Windows packaging
- *(rust)* format code in postgres adapter and lifecycle/s3 tests
- *(compatibility)* align database matrix with CI testing and clarify MinIO vs cloud S3 scopes
- *(deps)* update mysql_async to 0.37 resolving cargo audit advisories
- *(lock)* add backup duration > TTL heartbeat renewal and concurrent backup+prune exclusion tests
- *(repository)* add 1000-blob scale test covering check, verify, and prune
- *(postgres)* prove TLS encryption via pg_stat_ssl and validate restored data
- *(release-plz)* remove continue-on-error masking and support custom token
- *(ci)* filter internal table OID not_null constraints and compare columns directly
- fix postgres healthchecks, minio startup, and release-plz permission handling
- *(docs)* format documentation with dprint and exclude internal directories
- add deep before-and-after data, schema, and constraint verification scripts
- adapt GitHub Actions workflows for Dumper CI/CD and matrix testing
- add deny.toml license policies and dprint formatting configuration
- *(s3)* remove useless format! macro in corrupt snapshot test
- *(docker)* copy static musl binary explicitly into scratch image
- *(repository)* add SIGKILL crash-safety tests for backup, S3 upload, and prune
- *(perf)* add 10 MB, 100 MB, and 1 GB memory RSS benchmark test
- *(postgres)* add TLS success and concurrent-write consistency tests
- fix clippy warnings and enforce rustfmt across workspace
- *(mysql)* batch row inserts, cache table columns, and wrap in transaction
- *(mysql)* fix _dir field matching in TestMysqlServer
- *(stream)* update memory_rss_benchmark to current StreamEncoder API
- *(stream)* switch to async memory-bounded stream pipeline
