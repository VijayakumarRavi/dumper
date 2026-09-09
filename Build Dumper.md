# Build Dumper

Build **Dumper**, a tiny production-grade database backup, restore, verification, retention, and repository-management CLI for:

- PostgreSQL
- MySQL
- MariaDB

Dumper is inspired by the operational model of:

- `restic`
- `pgBackRest`
- `mysqldump`

but has a much narrower goal:

> **A single small Rust binary that can perform reliable database backups to local disk or S3-compatible object storage while consuming very little CPU, memory, and storage overhead.**

The primary deployment target is a tiny edge container running alongside a production application.

The tool must be designed to run safely with approximately:

```text
CPU: 0.25–1 vCPU
RAM: 64–256 MB
Disk: small temporary working directory
Network: potentially slow or unreliable
```

The application must avoid materially affecting the CPU, memory, disk I/O, or database performance of the main workload.

---

# 1. Product name

The project name is:

```text
dumper
```

Binary:

```text
dumper
```

Examples:

```bash
dumper init
dumper backup postgres://...
dumper backup mysql://...
dumper snapshots
dumper restore ...
dumper check
dumper verify
dumper forget
dumper prune
```

Keep the CLI name lowercase and easy to type.

---

# 2. Non-negotiable design goals

The following requirements take priority over feature breadth:

1. Correctness
2. Crash safety
3. Data integrity
4. Low memory usage
5. Low CPU usage
6. Minimal disk usage
7. Small static binary
8. Minimal dependency tree
9. Simple operational model
10. Easy container deployment

Do NOT sacrifice these goals for convenience.

---

# 3. Rust requirements

Implement the entire application in Rust.

Target a current stable Rust toolchain.

Use:

```text
edition = "2024"
```

where supported by the selected Rust toolchain.

Prefer the standard library wherever practical.

Avoid large general-purpose frameworks.

Do not use a web framework.

Do not build an HTTP server.

Do not build a daemon.

Do not introduce a plugin architecture.

Do not use dynamic loading.

Do not spawn shell commands.

Do not depend on external binaries.

---

# 4. Single binary

The final deliverable must be a single executable:

```text
dumper
```

It must not require:

```text
pg_dump
pg_restore
mysqldump
mysql
openssl
gzip
zstd
aws
curl
bash
python
node
```

or any other external runtime dependency.

The goal is to support an image conceptually similar to:

```dockerfile
FROM scratch

COPY dumper /dumper

ENTRYPOINT ["/dumper"]
```

If a runtime CA certificate bundle is required for TLS connections, document the smallest practical way to handle it without turning the container into a general OS environment.

---

# 5. Dependency philosophy

Dependency count matters.

Before adding any crate, evaluate:

```text
binary size
compile time
transitive dependencies
runtime memory
runtime CPU
security maintenance
whether std can replace it
```

Do not add dependencies merely because they make development easier.

Keep the dependency tree intentionally small.

Use feature flags aggressively.

Disable unnecessary default features.

The final project should include a documented dependency audit:

```text
crate
purpose
why it is required
major runtime impact
whether std alternatives were considered
```

---

# 6. Architecture

Use a layered architecture:

```text
CLI
 │
 ▼
Backup / Restore Engine
 │
 ├───────────────┬──────────────────┐
 │               │                  │
 ▼               ▼                  ▼
PostgreSQL      MySQL/MariaDB     Repository
Adapter                              │
                                     │
                           ┌─────────┴─────────┐
                           ▼                   ▼
                     Local Filesystem       S3 API
```

The core backup engine must not know whether the repository is local disk or S3.

The repository layer must expose a small interface.

Conceptually:

```text
Repository
├── put_object()
├── get_object()
├── object_exists()
├── delete_object()
├── list_objects()
├── commit_snapshot()
└── read_snapshot()
```

Keep interfaces small.

---

# 7. S3 is a first-class backend

The primary remote backend is:

> **S3-compatible object storage**

It must work with:

- Amazon S3
- MinIO
- Cloudflare R2
- Wasabi
- Backblaze B2 S3 API
- other reasonably compatible S3 implementations

Do not hard-code AWS-specific assumptions where the standard S3 API can be used.

Support configuration for:

```text
endpoint
region
bucket
prefix
access key
secret key
session token
```

Environment variables should be supported.

Example:

```bash
DUMPER_S3_ENDPOINT=https://s3.example.com
DUMPER_S3_REGION=us-east-1
DUMPER_S3_BUCKET=db-backups
DUMPER_S3_PREFIX=/production
DUMPER_S3_ACCESS_KEY_ID=...
DUMPER_S3_SECRET_ACCESS_KEY=...
```

Prefer credentials from:

- environment
- secret files
- container secrets

Never require AWS CLI configuration.

---

# 8. S3 implementation strategy

Be very careful here.

A normal modern cloud SDK can introduce a large dependency graph and significant binary/runtime overhead.

The implementation should therefore evaluate using:

```text
HTTP client
+
SigV4 signing
+
minimal S3 API implementation
```

instead of automatically pulling in a large cloud SDK.

Use a minimal HTTP stack.

Prefer:

```text
hyper
http
rustls
```

or an equivalently small carefully selected stack.

Use the minimum features necessary.

Avoid accidentally enabling:

- HTTP/2
- compression
- tracing stacks
- DNS features
- service discovery
- unused cloud credential providers
- large AWS service abstractions

unless genuinely required.

Document the final decision.

---

# 9. S3 operations

Implement only the S3 functionality Dumper actually needs.

At minimum:

```text
PUT object
GET object
HEAD object
DELETE object
LIST objects
multipart upload
abort multipart upload
```

Use multipart upload for large objects.

Do not buffer a whole object in memory before uploading.

Streaming must be:

```text
database
  ↓
chunker
  ↓
compressor
  ↓
encryptor
  ↓
bounded buffer
  ↓
S3 multipart upload
```

Never:

```text
database
  ↓
Vec<u8> containing entire backup
  ↓
S3
```

---

# 10. S3 multipart behavior

Implement bounded multipart uploads.

Parameters should be configurable:

```text
part size
maximum in-flight parts
connection count
retry count
```

Default concurrency must be conservative.

Default:

```text
workers = 1
```

Do not create dozens of asynchronous tasks per backup.

A user must explicitly opt into higher concurrency.

Example:

```bash
dumper backup ... --parallel 2
```

The default behavior must prioritize:

> protecting the application and database

over:

> maximum S3 throughput

---

# 11. S3 retries

Implement bounded retries for transient failures.

Retry appropriate failures such as:

```text
timeouts
connection resets
HTTP 429
HTTP 500
HTTP 502
HTTP 503
HTTP 504
```

Use exponential backoff with jitter.

Do not retry forever.

Make retry behavior configurable.

Example:

```text
--retry 5
```

A failed multipart upload must be safely aborted when practical.

---

# 12. S3 consistency and repository safety

Do not assume that a partially uploaded snapshot is valid.

Use an atomic-ish commit model:

```text
upload chunks
     ↓
write snapshot metadata
     ↓
final commit marker
```

Only snapshots with valid committed metadata should appear in:

```bash
dumper snapshots
```

A crashed process must not cause an incomplete backup to appear as a valid snapshot.

---

# 13. Repository format

Design a custom Dumper repository format.

Do NOT directly copy restic's repository format.

The format should be:

- content-addressed
- deduplicated
- encrypted
- compressed
- integrity protected
- append-friendly
- crash safe
- database agnostic

Example conceptual structure:

```text
s3://bucket/prefix/
    config
    snapshots/
    blobs/
    indexes/
```

Actual object naming is up to the implementation.

Use content hashes as object identifiers.

Example:

```text
blobs/ab/cdef1234...
```

Do not create millions of deeply nested prefixes unnecessarily.

---

# 14. Deduplication

The repository must deduplicate identical content across snapshots.

For example:

```text
snapshot A
snapshot B
snapshot C
```

should reference the same immutable blobs where content is unchanged.

Avoid fragile backup chains.

This must NOT be:

```text
full
  ↓
incremental
  ↓
incremental
  ↓
incremental
```

where losing one backup breaks all subsequent backups.

Every snapshot must be independently restorable.

Deduplication should happen at the blob/chunk level.

---

# 15. Chunking

Implement a streaming chunker.

Do not chunk based only on fixed-size whole-database buffers.

Evaluate:

```text
fixed-size chunks
content-defined chunking
```

Choose the simplest method that provides useful deduplication without excessive CPU cost.

Target approximately:

```text
1–8 MiB
```

chunk sizes, with sensible configurable defaults.

Chunks must be processed incrementally.

Memory consumption must be bounded.

---

# 16. Compression

Compression must be streaming.

Prefer:

```text
zstd
```

if the crate/runtime overhead is acceptable.

Otherwise evaluate a lighter alternative.

Expose:

```bash
--compression none
--compression fast
--compression default
--compression max
```

The default must favor:

```text
low CPU
good storage reduction
```

rather than maximum compression ratio.

Do not compress data that is already compressed when doing so is obviously wasteful.

Where practical, detect or track compression benefit per chunk.

---

# 17. Encryption

Encrypt repository contents client-side before they leave the machine.

Use authenticated encryption.

Preferred construction:

```text
XChaCha20-Poly1305
```

or another well-established AEAD construction with an appropriately reviewed Rust implementation.

Never invent cryptography.

Do not implement custom encryption.

Keys must not be derived directly from the user password.

Use a strong password-based KDF such as:

```text
Argon2id
```

or another modern password KDF.

Support:

```bash
dumper init
```

creating the repository encryption metadata.

Support:

```bash
dumper key ...
```

or an equivalent key-management command if required.

Never print secrets.

---

# 18. Key management

The repository must separate:

```text
repository metadata
encryption key material
user password
```

Support password-based repository unlocking.

Do not store the plaintext password in the repository.

Design the repository format so that key rotation is possible without rewriting every backup blob.

---

# 19. Integrity

Every blob must be integrity verifiable.

Use:

```text
cryptographic hash
+
authenticated encryption
```

where appropriate.

Detect:

- modified blobs
- corrupted blobs
- truncated blobs
- missing blobs
- invalid snapshot metadata
- wrong encryption credentials

Provide:

```bash
dumper check
```

and:

```bash
dumper verify <snapshot>
```

---

# 20. Database abstraction

Define a database adapter interface.

Conceptually:

```rust
trait DatabaseBackup {
    fn backup(...);
    fn restore(...);
    fn inspect(...);
}
```

The repository must not know whether the source is PostgreSQL or MySQL.

The database adapter is responsible for converting database contents into a Dumper logical backup stream.

---

# 21. PostgreSQL support

Support modern PostgreSQL versions.

Do not invoke:

```text
pg_dump
pg_restore
psql
```

Implement the PostgreSQL interaction natively in Rust.

Use the PostgreSQL wire protocol.

The implementation must produce a consistent logical backup.

Use database-native mechanisms such as:

- transactions
- repeatable-read snapshots
- COPY
- catalog queries

where appropriate.

Do not hold the complete database in memory.

---

# 22. PostgreSQL backup scope

At minimum, support:

```text
database metadata
schemas
tables
columns
data
sequences
indexes
constraints
foreign keys
views
materialized views where practical
functions
procedures where applicable
triggers
extensions metadata
roles/ownership metadata where practical
large objects where practical
```

The backup should preserve enough metadata to recreate a functioning database.

Do not promise byte-for-byte physical equivalence.

This is a logical backup system.

---

# 23. PostgreSQL consistency

The backup must represent a consistent database state.

A backup must not produce:

```text
table A from time T1
table B from time T2
```

because of ordinary concurrent writes.

Use an appropriate consistent snapshot strategy.

Do not block the production database unnecessarily.

Minimize locking.

Document exactly what locking is performed.

---

# 24. PostgreSQL streaming

Table data must be streamed.

For example:

```text
PostgreSQL COPY
      ↓
decoder
      ↓
small bounded buffer
      ↓
Dumper record stream
      ↓
chunker
```

Never:

```text
COPY
 ↓
Vec containing entire table
```

Large tables must have effectively constant memory requirements.

---

# 25. MySQL support

Support:

- MySQL 8+
- practical MariaDB versions

Do not invoke:

```text
mysqldump
mysql
```

Use the native MySQL protocol.

---

# 26. MySQL consistency

Implement a consistent backup strategy appropriate for transactional databases.

Prefer:

```text
START TRANSACTION WITH CONSISTENT SNAPSHOT
```

and an appropriate isolation level.

Use appropriate metadata handling.

Do not unnecessarily lock the whole database.

Document behavior for:

- InnoDB
- non-transactional tables
- MyISAM or equivalent engines

Do not pretend all MySQL storage engines provide identical consistency guarantees.

---

# 27. MySQL backup scope

At minimum support:

```text
databases
tables
columns
data
indexes
foreign keys
views
triggers
routines
events
generated columns
```

where supported.

---

# 28. Database connection behavior

Connections must be:

- TLS-capable
- timeout-controlled
- cancellable
- bounded
- cleanly closed

Provide configurable:

```text
connect timeout
query timeout
socket timeout
```

Defaults must be conservative.

Handle:

```text
SIGTERM
SIGINT
```

and cleanly abort active database operations.

---

# 29. Backup stream format

Do not simply generate a giant SQL string.

Define a compact internal record format.

Conceptually:

```text
Header
Schema records
Table metadata records
Data records
Index records
Constraint records
Routine records
Trailer
```

The format must be:

- versioned
- streamable
- self-describing enough for restore
- forward-compatible
- endian-safe
- corruption detectable

Do not require seeking within the backup stream.

A restore operation should be able to process the stream sequentially.

---

# 30. Restore architecture

Restore must also be streaming.

Example:

```text
S3
 ↓
download blob
 ↓
decrypt
 ↓
decompress
 ↓
decode record stream
 ↓
database adapter
 ↓
PostgreSQL/MySQL
```

Do not download the whole snapshot first.

Large backups must be restorable with bounded memory.

---

# 31. Restore commands

Example:

```bash
dumper restore <snapshot-id> \
  --target postgres://user@host/db
```

and:

```bash
dumper restore <snapshot-id> \
  --target mysql://user@host/db
```

Support sensible options:

```text
--database
--schema
--table
--drop
--create
--jobs
--parallel
```

only where they can be implemented without compromising correctness or memory usage.

---

# 32. Snapshot metadata

Every snapshot must contain:

```text
snapshot ID
repository format version
Dumper version
database type
database name
database server version
server identity
start time
end time
duration
logical object counts
logical data size
compressed size
stored size
deduplicated bytes
encryption mode
compression mode
```

Example:

```text
Snapshot:       7c4f9c3a
Database:       production
Engine:         PostgreSQL
Version:        17.6

Started:        2026-09-08 02:00:01
Completed:      2026-09-08 02:04:32
Duration:       4m31s

Database size:  8.7 GB
Stored:         1.4 GB
Deduplicated:   7.3 GB
```

---

# 33. Snapshot IDs

Use short human-friendly IDs while maintaining strong underlying identifiers.

Example:

```text
7c4f9c3a
```

Internally use cryptographically strong identifiers/hashes as appropriate.

---

# 34. CLI design

CLI should feel familiar to `restic`.

Commands:

```text
dumper
├── init
├── backup
├── snapshots
├── info
├── list
├── restore
├── verify
├── check
├── forget
├── prune
├── stats
├── config
└── version
```

Potential future commands can be added later.

---

# 35. Repository initialization

Example:

```bash
dumper init \
  --repository s3://my-bucket/postgres
```

For a custom S3 endpoint:

```bash
dumper init \
  --repository s3://my-bucket/postgres \
  --endpoint https://minio.example.com
```

The repository should contain:

```text
repository format version
encryption metadata
repository ID
creation time
configuration defaults
```

Do not store secrets in plaintext.

---

# 36. Connection strings

Accept normal database URLs.

Examples:

```text
postgres://user:password@db.example.com:5432/app
mysql://user:password@db.example.com:3306/app
```

Support safer credential methods.

Avoid printing full URLs back to the terminal because they may contain passwords.

Mask secrets in errors and logs.

---

# 37. Environment variable support

Support:

```text
DUMPER_REPOSITORY
DUMPER_PASSWORD_FILE
DUMPER_PASSWORD
DUMPER_S3_ENDPOINT
DUMPER_S3_REGION
DUMPER_S3_BUCKET
DUMPER_S3_PREFIX
DUMPER_S3_ACCESS_KEY_ID
DUMPER_S3_SECRET_ACCESS_KEY
DUMPER_S3_SESSION_TOKEN
```

Also consider standard S3 environment naming where useful.

Never log environment variables containing secrets.

---

# 38. Local repository backend

Implement local filesystem storage as the first backend.

Example:

```bash
dumper init --repository /backup
```

The local backend and S3 backend must use the exact same logical repository format.

This is important for:

```text
local development
migration
testing
disaster recovery
```

---

# 39. S3 repository layout

Design S3 layout so that:

- listing snapshots is cheap
- fetching a snapshot is cheap
- old snapshots can be pruned
- garbage collection does not require downloading every blob
- object names are deterministic
- repository metadata is small

Use separate namespaces for:

```text
config
snapshots
blobs
indexes
locks
```

Example conceptual layout:

```text
s3://bucket/prefix/
    config
    snapshots/<id>
    blobs/<hash-prefix>/<hash>
    indexes/<generation>
    locks/<lock-id>
```

The exact structure is implementation-defined.

---

# 40. Repository locking

Implement repository locking.

Purpose:

Prevent destructive concurrent operations such as:

```text
prune
forget
restore
backup
```

from corrupting repository state.

Support:

```bash
dumper unlock
```

for stale locks.

For S3, implement a lease/lock mechanism suitable for object storage.

Do not assume filesystem locking semantics exist on S3.

Avoid permanent stale locks.

---

# 41. Crash safety

The most important repository requirement is:

> A failed backup must never destroy a previous successful backup.

Test:

```text
SIGKILL
SIGTERM
network failure
S3 timeout
S3 permission error
disk full
database disconnect
process crash
container restart
```

A partially completed backup must remain clearly uncommitted.

Repository recovery must be deterministic.

---

# 42. Retention

Implement retention rules.

Example:

```bash
dumper forget \
  --keep-last 7 \
  --keep-daily 14 \
  --keep-weekly 8 \
  --keep-monthly 12
```

Semantics:

```text
forget = remove snapshot references
prune  = garbage collect unreferenced data
```

Never conflate the two.

---

# 43. Garbage collection

Implement:

```bash
dumper prune
```

Pruning must:

1. Determine live snapshots
2. Determine referenced blobs
3. Determine unreferenced blobs
4. Delete only unreferenced objects
5. Safely handle concurrent repository changes

For S3, minimize LIST operations because they can be expensive and slow.

Consider maintaining repository indexes to avoid full object-store scans.

---

# 44. Verification commands

Support:

```bash
dumper check
```

for repository-level validation.

Support:

```bash
dumper verify <snapshot-id>
```

for snapshot-level validation.

Verification must test:

```text
metadata integrity
blob existence
hashes
authentication tags
decryption
decompression
record-stream validity
```

Optionally provide:

```bash
dumper verify <snapshot-id> --restore-test
```

which reconstructs the logical stream without modifying the production database.

---

# 45. Stats

Provide:

```bash
dumper stats
```

Example output:

```text
Snapshots:             31
Unique blobs:           17,428
Repository size:       482 GB
Logical backup size:   2.8 TB
Deduplication ratio:   5.8x
```

Do not calculate expensive statistics on every command.

Allow lightweight and full statistics modes.

---

# 46. Logging

Default output should be extremely lightweight.

Example:

```text
Backing up PostgreSQL database 'production'
Snapshot: 7c4f9c3a

Progress:
  3.8 GB / 8.7 GB
  44%
  28 MB/s

Completed
Duration: 5m11s
Stored:   1.2 GB
```

Do not update progress hundreds of times per second.

Throttle progress output.

Support:

```bash
--quiet
--verbose
--json
```

JSON output should be stable enough for automation.

---

# 47. Exit codes

Define stable exit codes:

```text
0  success
1  general error
2  usage/configuration error
3  authentication error
4  database error
5  repository error
6  integrity error
7  restore error
8  interrupted
```

Document them.

---

# 48. Resource governor

Design Dumper as a resource-sensitive application.

Provide:

```text
--parallel
--max-memory
--bandwidth-limit
--compression
```

where appropriate.

Default:

```text
parallel = 1
```

Memory use must remain bounded.

Do not allow a flag to accidentally create thousands of concurrent tasks.

---

# 49. Memory requirements

This is a core acceptance criterion.

The implementation must NOT use:

```rust
Vec<u8>
```

or equivalent structures that grow with:

- database size
- table size
- snapshot size
- S3 object size

Memory should scale approximately with:

```text
fixed buffers
+
chunk size
+
small metadata
+
limited concurrency
```

A 10 MB database and a 100 GB database should have broadly similar memory behavior.

Target ordinary operation:

```text
< 30 MB RSS
```

where practical.

A sustained RSS of 64 MB or less should be considered the maximum design target for the common one-worker case.

Document actual measurements rather than pretending the target is guaranteed.

---

# 50. CPU requirements

The backup must be intentionally conservative.

Default compression level must not consume a full CPU core unless the user explicitly requests it.

Avoid:

- excessive task scheduling
- excessive hashing
- unnecessary copies
- repeated allocations
- giant temporary buffers
- aggressive polling

Prefer zero-copy or low-copy paths where practical.

Reuse buffers.

Use bounded channels.

Avoid creating one task per database row or chunk.

---

# 51. Disk requirements

The normal backup process should not require a full temporary copy of the database.

Ideal flow:

```text
DB
 ↓
stream
 ↓
chunk
 ↓
compress
 ↓
encrypt
 ↓
upload
```

Temporary disk should be optional rather than mandatory.

For local repositories, write incrementally.

For S3, stream into multipart uploads.

Do not stage a complete database dump on disk unless explicitly requested.

---

# 52. Database load minimization

The backup tool must be designed not to become the database's second application.

Avoid:

```text
SELECT * ORDER BY ...
```

on huge tables when ordering is unnecessary.

Use efficient native mechanisms such as:

```text
COPY
streaming result sets
server-side cursors
```

where appropriate.

Avoid unnecessary metadata queries.

Avoid repeatedly querying system catalogs.

Do not hold long-running heavyweight locks.

Document backup impact.

---

# 53. Backpressure

Backpressure is mandatory.

If S3 slows down:

```text
database reader
      ↓
must slow down
```

rather than accumulating data in RAM.

Likewise:

```text
database
 ↓
chunker
 ↓
compressor
 ↓
encryptor
 ↓
network
```

must behave like a bounded pipeline.

No stage may unboundedly queue work for another stage.

---

# 54. Async vs sync design

Do not automatically assume "async Rust is faster."

Choose the model that produces the lowest:

```text
memory
CPU
binary size
complexity
```

while still providing good streaming network/database behavior.

If async is required for efficient S3 and database streaming, use a lightweight runtime with minimal enabled features.

Do not spawn unnecessary executor threads.

The default runtime should be configured for low resource usage.

---

# 55. Networking

Support TLS.

Prefer:

```text
rustls
```

over native OS OpenSSL linkage when this reduces deployment dependencies.

Avoid a requirement for system OpenSSL libraries.

Connection pools must be tiny.

Default:

```text
database connections = 1
S3 connections = 1–2
```

Increase only when requested.

---

# 56. Security

Dumper handles credentials and potentially extremely sensitive data.

Requirements:

- never log passwords
- never log secret keys
- never expose credentials in panic messages
- sanitize database URLs
- sanitize S3 URLs
- secure secret files
- avoid writing credentials to disk
- use TLS
- authenticate repository contents
- validate repository metadata
- reject malformed input
- prevent path traversal for local repositories
- never execute shell commands
- never require root

---

# 57. Panic policy

CLI operations must not panic on ordinary user errors.

Examples:

```text
wrong password
database unavailable
S3 unavailable
corrupt snapshot
invalid repository
invalid URL
```

Return structured errors.

Provide useful human-readable context.

Avoid giant error backtraces in normal CLI output.

Provide a debug mode for detailed diagnostics.

---

# 58. Testing strategy

Use unit tests for:

```text
chunking
hashing
encryption
compression
record encoding
repository metadata
snapshot encoding
retention
S3 signing
```

Use integration tests for:

```text
PostgreSQL
MySQL
MariaDB
MinIO
local filesystem
```

Use failure injection for:

```text
network failure
S3 500
S3 timeout
database disconnect
process termination
corrupted object
missing object
wrong key
```

---

# 59. Testcontainers / integration environments

Use containers only for the test environment.

Do not make runtime operation depend on containers.

Integration test matrix should include:

```text
PostgreSQL
MySQL
MariaDB
MinIO
```

Run end-to-end scenarios:

```text
init
backup
list
verify
restore
compare
forget
prune
verify again
```

---

# 60. Restore correctness tests

For every test database:

1. Create database
2. Insert representative data
3. Perform backup
4. Destroy/recreate target
5. Restore
6. Validate schema
7. Validate row counts
8. Validate data
9. Validate indexes/constraints where applicable
10. Validate routines/triggers where applicable

Include:

```text
Unicode
NULL
binary values
large values
JSON
timestamps
arrays for PostgreSQL
generated columns for MySQL
```

---

# 61. Large database tests

Generate test databases:

```text
10 MB
100 MB
1 GB
```

Measure:

```text
RSS
CPU
throughput
network usage
temporary disk
repository size
deduplication
restore speed
```

The most important graph is:

```text
database size → peak RSS
```

Peak RSS should be approximately flat.

---

# 62. Benchmark suite

Create benchmarks for:

```text
chunking
hashing
compression
encryption
record encoding
S3 upload
S3 download
PostgreSQL extraction
MySQL extraction
restore
```

Do not optimize based purely on microbenchmarks.

Measure complete backup behavior.

---

# 63. Binary size

Track release binary size.

The project should continuously report:

```text
unstripped size
stripped size
compressed release artifact size
```

Use:

```text
release profile optimizations
LTO
strip
panic = "abort"
```

where appropriate.

Do not use unsafe binary-size tricks that compromise debugging or reliability.

---

# 64. Static linking

Prefer a self-contained Linux release.

Investigate:

```text
musl
```

for portable static builds.

The release pipeline should produce something that can run in a minimal container without requiring a distro-specific runtime.

Also produce builds for common targets as appropriate.

---

# 65. Container

Build a minimal image.

Preferred concept:

```dockerfile
FROM scratch

COPY dumper /dumper

ENTRYPOINT ["/dumper"]
```

If runtime CA certificates are necessary, include only the required certificate material.

Provide a non-root container example.

Example:

```text
USER 65532:65532
```

where appropriate.

---

# 66. Configuration

Keep configuration simple.

CLI flags should work without requiring a config file.

Optional config file can be introduced later.

Environment variables should be sufficient for Kubernetes/Docker secrets.

Do not require YAML unless there is a strong reason.

Prefer:

```text
CLI
+
environment variables
+
secret files
```

for v1.

---

# 67. Cron / Kubernetes usage

Dumper must be suitable for:

```text
cron
systemd timers
Kubernetes CronJob
Docker
Nomad
```

Example:

```bash
dumper backup \
  --repository s3://backups/postgres \
  postgres://backup@postgres/production
```

This should be a complete non-interactive operation.

---

# 68. JSON output

Provide machine-readable output.

Example:

```json
{
  "event": "backup_complete",
  "snapshot_id": "7c4f9c3a",
  "database": "production",
  "engine": "postgresql",
  "input_bytes": 9348234234,
  "stored_bytes": 1284234234,
  "deduplicated_bytes": 8064000000,
  "duration_seconds": 311
}
```

Do not emit secrets.

---

# 69. Database identity

Record enough information to detect accidentally restoring a backup into the wrong database environment.

Record:

```text
database engine
database name
server version
optional stable server identity
```

Do not rely on hostname alone because containers frequently change hostnames.

---

# 70. Repository portability

A backup repository must be portable.

The repository should not depend on:

```text
machine ID
hostname
filesystem paths
container ID
local database installation
```

A repository copied from:

```text
S3
```

to:

```text
local disk
```

should remain logically usable.

---

# 71. Repository migration

Design so that future commands such as:

```bash
dumper copy
dumper export
dumper migrate
```

can be implemented without changing the repository format.

Do not build migration functionality initially unless necessary.

---

# 72. Versioning

Version everything that matters.

Repository:

```text
format version
```

Backup stream:

```text
stream version
```

Metadata:

```text
schema version
```

The tool version must not be the same thing as the repository format version.

Old repositories must be readable by newer versions where possible.

---

# 73. Compatibility policy

Document:

```text
repository compatibility
backup stream compatibility
database version compatibility
restore compatibility
```

Do not silently break old repositories.

If an incompatible change is required:

```text
dumper check
```

must identify it clearly.

---

# 74. Observability without bloat

Do not add Prometheus, OpenTelemetry, or a metrics server to the core application.

The primary observability mechanism should be:

```text
CLI output
JSON output
exit codes
```

Optional integrations may come later.

---

# 75. Repository performance

The repository must remain usable with:

```text
100
1,000
10,000
100,000+
```

snapshots/chunks.

Avoid loading millions of object references into RAM.

Indexing must be incremental.

Repository metadata must remain compact.

---

# 76. S3 cost awareness

S3 API calls can cost money.

Avoid unnecessary:

```text
HEAD
LIST
GET
PUT
```

operations.

Do not perform one HTTP request per tiny logical object.

Use sufficiently large immutable blobs.

Batch metadata where possible.

Multipart uploads should use sensible part sizes.

Repository design should optimize both:

```text
runtime efficiency
```

and:

```text
object-storage API cost
```

---

# 77. S3 object immutability

Once a blob is written and verified, treat it as immutable.

Never modify an existing blob.

Snapshots are metadata references to immutable content.

This simplifies:

```text
deduplication
integrity
concurrency
crash recovery
```

---

# 78. Snapshot commit model

Use a clear state machine:

```text
STARTED
   ↓
WRITING
   ↓
FINALIZING
   ↓
COMMITTED
```

Only:

```text
COMMITTED
```

snapshots should be visible to users.

On failure:

```text
ABORTED
```

or effectively unreachable.

Never expose half-written snapshots as valid backups.

---

# 79. Garbage collection safety

Prune must never delete a blob that may still belong to a committed snapshot.

Treat snapshot metadata as the source of truth.

Use a repository locking strategy that prevents:

```text
backup
```

and:

```text
prune
```

from racing destructively.

---

# 80. Database backup format philosophy

Do not attempt to replicate every obscure feature of `pg_dump` or `mysqldump` immediately.

Build a robust core first.

The first production milestone should provide excellent support for common:

```text
tables
columns
rows
schema
indexes
constraints
views
triggers
routines
```

Then expand coverage.

Never pretend a feature is supported when it is not.

The CLI should clearly report unsupported database objects.

---

# 81. Physical PostgreSQL backup

Do NOT implement PostgreSQL physical/WAL backup in v1.

Do not attempt to become a replacement for:

```text
pgBackRest
WAL-G
Barman
```

The initial product is:

> **logical PostgreSQL + MySQL/MariaDB backup with content-addressed encrypted S3 storage.**

Physical/WAL backup can be designed separately later.

---

# 82. UX principles

The tool must be:

```text
boring
predictable
safe
fast enough
easy to automate
```

Avoid unnecessary cleverness.

A database administrator should immediately understand:

```bash
dumper backup ...
dumper snapshots
dumper verify ...
dumper restore ...
dumper forget ...
dumper prune
```

from:

```bash
dumper --help
```

---

# 83. Example workflow

## Initialize repository

```bash
dumper init \
  --repository s3://prod-backups/postgres
```

## Backup PostgreSQL

```bash
dumper backup \
  --repository s3://prod-backups/postgres \
  postgres://backup@postgres:5432/production
```

## Backup MySQL

```bash
dumper backup \
  --repository s3://prod-backups/mysql \
  mysql://backup@mysql:3306/production
```

## List snapshots

```bash
dumper snapshots \
  --repository s3://prod-backups/postgres
```

## Verify

```bash
dumper verify \
  --repository s3://prod-backups/postgres \
  7c4f9c3a
```

## Restore

```bash
dumper restore \
  --repository s3://prod-backups/postgres \
  7c4f9c3a \
  --target postgres://restore@postgres-recovery/production
```

## Retention

```bash
dumper forget \
  --repository s3://prod-backups/postgres \
  --keep-last 7 \
  --keep-daily 14 \
  --keep-weekly 8 \
  --keep-monthly 12
```

## Garbage collection

```bash
dumper prune \
  --repository s3://prod-backups/postgres
```

---

# 84. Suggested Rust crate categories

Evaluate crates in these categories, choosing the smallest reliable implementation:

```text
CLI argument parsing
PostgreSQL protocol/client
MySQL protocol/client
HTTP
TLS
S3 SigV4
hashing
AEAD encryption
password KDF
compression
serialization
```

Do not blindly choose popular crates.

For each dependency, explicitly document:

```text
crate
version
features enabled
why it exists
transitive dependency impact
runtime impact
```

Where possible, use:

```text
std::io::Read
std::io::Write
std::io::BufRead
```

and streaming abstractions instead of collecting data into memory.

---

# 85. Rust implementation quality

Use idiomatic Rust.

Avoid:

- unnecessary `Arc`
- unnecessary `Mutex`
- excessive cloning
- excessive heap allocation
- giant enums where simpler representations work
- unbounded channels
- task-per-record designs
- global mutable state

Prefer:

```text
ownership
borrowing
small structs
streaming traits
bounded channels
RAII
typed errors
```

Use `unsafe` only when absolutely necessary and justify every use.

Prefer zero unsafe code.

---

# 86. Error model

Create typed application errors.

Categories should include:

```text
CliError
ConfigError
DatabaseError
RepositoryError
S3Error
CryptoError
FormatError
IntegrityError
RestoreError
Interrupted
```

Map them to documented exit codes.

Errors must identify:

```text
what failed
where it failed
what the user can do next
```

without exposing credentials.

---

# 87. Logging and debug mode

Normal:

```bash
dumper backup ...
```

should be concise.

Debug:

```bash
RUST_LOG=debug dumper backup ...
```

or an equivalent minimal debug mode may be supported.

Do not force a heavyweight logging stack into the release architecture unless justified.

---

# 88. Documentation

Provide:

```text
README.md
ARCHITECTURE.md
REPOSITORY_FORMAT.md
SECURITY.md
THREAT_MODEL.md
OPERATIONS.md
COMPATIBILITY.md
```

Document:

- backup semantics
- consistency model
- encryption
- S3 behavior
- retention
- crash recovery
- memory behavior
- CPU behavior
- restore limitations
- supported database versions

---

# 89. Threat model

Document what Dumper protects against:

```text
stolen S3 credentials
stolen repository storage
accidental deletion
corrupted objects
partial uploads
wrong passwords
malicious repository modification
```

Clearly document what it does NOT protect against.

Do not oversell security.

---

# 90. Security review requirements

Before release:

- audit dependencies
- run `cargo audit`
- run formatting/lints
- test malformed repositories
- test corrupted encrypted objects
- test malformed S3 responses
- test malicious metadata
- test path traversal
- test credential leakage in logs
- test command-line process arguments where feasible

Do not claim production readiness until these have been exercised.

---

# 91. CI

CI should run:

```text
cargo fmt --check
cargo clippy
cargo test
integration tests
cargo audit
release build
binary-size report
```

Build reproducible release artifacts where practical.

---

# 92. Release targets

Prioritize:

```text
Linux x86_64
Linux aarch64
```

because these are the most useful for containers and edge environments.

Add other targets later.

---

# 93. Performance acceptance criteria

The first production milestone should aim for:

```text
Single worker
<= 64 MB RSS
No memory proportional to database size
Very low idle CPU
Conservative backup CPU usage
No full temporary database dump
S3 streaming uploads
S3 streaming restores
```

A successful 100 GB backup must not require 100 GB of temporary local storage.

A successful 100 GB backup must not require 100 GB of RAM.

---

# 94. Failure acceptance criteria

The following must all be safe:

```text
kill -9 during upload
kill -9 during database read
network outage during S3 upload
database disconnect
wrong S3 credentials
wrong repository password
disk full
corrupt object
missing object
interrupted prune
```

Expected result:

```text
previous committed snapshots remain valid
```

---

# 95. Development strategy

Do NOT build everything at once.

Implement in stages.

## Stage 1 — repository core

Build:

```text
repository format
local filesystem backend
chunking
compression
encryption
snapshot metadata
check
```

## Stage 2 — S3

Build:

```text
S3 endpoint support
SigV4
GET
PUT
HEAD
LIST
DELETE
multipart upload
retry handling
S3 locking
```

## Stage 3 — PostgreSQL

Build:

```text
connection
consistent snapshot
schema extraction
COPY streaming
backup stream
restore
verification
```

## Stage 4 — MySQL/MariaDB

Build:

```text
connection
consistent snapshot
metadata extraction
streaming rows
backup stream
restore
verification
```

## Stage 5 — lifecycle

Build:

```text
forget
retention
prune
stats
```

## Stage 6 — optimization

Measure:

```text
RSS
CPU
binary size
throughput
S3 request count
database load
```

Then optimize based on real measurements.

---

# 96. Do not over-engineer

Do NOT initially build:

```text
GUI
web UI
daemon
scheduler
Kubernetes operator
database monitoring
multi-node coordination
distributed repository
custom cloud control plane
Prometheus endpoint
OpenTelemetry
```

The product is a CLI.

---

# 97. Final acceptance scenario

The canonical acceptance test is:

Run Dumper in a container with:

```text
CPU limit: 0.5
Memory limit: 128 MB
```

Connect to a production-like PostgreSQL database.

Store the repository in MinIO/S3.

Run:

```bash
dumper backup ...
```

Then:

```bash
dumper snapshots
dumper verify ...
dumper restore ...
dumper check
dumper forget ...
dumper prune
```

The system passes only if:

- backup is correct
- restore is correct
- data integrity is verified
- previous snapshots survive failures
- S3 is used without staging the entire backup locally
- memory stays bounded
- CPU usage is conservative
- the application remains operational
- the final container contains essentially only the Dumper binary and required CA certificates

---

# 98. Most important engineering principle

Every design decision must be evaluated through this question:

> **"Does this feature justify the memory, CPU, binary-size, dependency, complexity, and operational cost it introduces?"**

When there is a choice between:

```text
more features
```

and:

```text
smaller + safer + more predictable
```

choose:

```text
smaller + safer + more predictable
```

Dumper should feel like a piece of infrastructure you can forget is running.

The ideal outcome is:

```text
64–128 MB container
single Rust binary
one database connection
one or two S3 connections
bounded memory
low CPU
no temp dump
encrypted deduplicated snapshots
simple CLI
```

The design should resemble:

```text
                ┌──────────────────────┐
                │        Dumper          │
                │                      │
PostgreSQL ────►│  streaming backup    │
MySQL ─────────►│        engine        │
                │          │           │
                │          ▼           │
                │  chunk/compress      │
                │          │           │
                │          ▼           │
                │       encrypt        │
                │          │           │
                └──────────┼───────────┘
                           │
                           ▼
                    Local / S3 repo
                           │
                    ┌──────┴──────┐
                    ▼             ▼
                snapshots       blobs
                    │             │
                    └──────┬──────┘
                           ▼
                    verify / restore
```

Do not lose sight of the original mission:

> **Dumper is a tiny database backup appliance in a single Rust binary, not a cloud platform disguised as a CLI.**
