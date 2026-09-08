---
trigger: always_on
---

# Dumper — Coding Agent Rules - Part 1

# 1. Mission

Dumper is a **small, reliable database backup and restore CLI** for:

* PostgreSQL
* MySQL
* MariaDB

Primary storage:

* S3-compatible object storage
* Local filesystem

Primary design constraints:

* Single binary
* Low CPU
* Low memory
* Minimal dependencies
* Streaming operation
* Safe unattended execution
* Strong backup integrity
* Reliable restore

The project is infrastructure software. **Correctness and recoverability matter more than feature count.**

---

# 2. Do Not Over-Engineer

Before adding code, ask:

> Is this required for correctness, security, reliability, compatibility, or an important operational capability?

If the answer is no, do not add it.

Do not add:

* web UI
* HTTP server
* daemon mode
* scheduler
* Kubernetes operator
* monitoring server
* Prometheus endpoint
* OpenTelemetry
* plugin architecture
* distributed control plane
* complex configuration framework
* physical PostgreSQL/WAL backup
* PITR
* unrelated database engines

Keep the project a CLI.

---

# 3. Preserve the Existing Architecture

Do not rewrite working components without evidence.

The preferred architecture is:

```text
CLI
 ↓
Backup / Restore Engine
 ↓
Database Adapter
 ↓
Streaming DMP1 Pipeline
 ↓
Chunker
 ↓
Compression
 ↓
Encryption
 ↓
Repository
 ├── Local filesystem
 └── S3
```

Database adapters must remain independent from repository implementations.

Repository implementations must remain independent from database-specific logic.

Avoid cross-layer coupling.

---

# 4. Read Before Changing

Before modifying a subsystem, inspect the existing implementation and relevant documentation.

Important files:

```text
README.md
ARCHITECTURE.md
REPOSITORY_FORMAT.md
SECURITY.md
THREAT_MODEL.md
OPERATIONS.md
COMPATIBILITY.md
Cargo.toml
flake.nix
Dockerfile
src/
tests/
```

Do not assume the documentation is wrong.

If implementation and documentation disagree:

1. determine which behavior is intentional
2. fix the implementation or documentation
3. add a test preventing the mismatch from returning

Do not silently change documented behavior.

---

# 5. Correctness First

Priority order:

```text
1. Data correctness
2. Restore correctness
3. Repository integrity
4. Crash safety
5. Security
6. Database consistency
7. S3 reliability
8. Resource usage
9. Performance
10. CLI polish
```

Never optimize something before establishing that the backup and restore are correct.

---

# 6. Never Stage the Entire Database

Never load an entire:

* database
* table
* backup
* snapshot
* S3 object

into memory.

Never create architecture that requires:

```rust
Vec<u8>
```

to grow with database size.

Use bounded streaming.

Preferred model:

```text
database
 → bounded buffer
 → DMP1
 → chunk
 → compress
 → encrypt
 → storage
```

Restore must reverse this process without full-buffer staging.

---

# 7. Memory Must Be Bounded

Memory must be approximately independent of database size.

Bad:

```text
database size → memory usage
```

Good:

```text
fixed buffers
+ chunk size
+ bounded concurrency
+ small metadata
```

Avoid:

* unbounded channels
* unbounded queues
* task-per-row
* task-per-chunk
* growing caches
* loading all snapshot metadata into RAM unnecessarily

Default concurrency remains:

```text
1
```

Do not increase default concurrency to improve benchmark numbers.

---

# 8. Backpressure Is Mandatory

Every streaming stage must provide backpressure.

If S3 slows down:

```text
S3
 ↓
encryption
 ↓
compression
 ↓
database
```

must naturally slow the database reader.

Do not buffer large amounts of data to hide slow storage.

Never introduce an unbounded queue between pipeline stages.

---

# 9. Prefer Simple Rust

Prefer:

* ownership
* borrowing
* small structs
* simple traits
* iterators
* `Read` / `Write`
* bounded buffers
* RAII
* typed errors

Avoid unnecessary:

* `Arc`
* `Mutex`
* cloning
* heap allocations
* global state
* interior mutability
* macros
* procedural abstractions

Use `unsafe` only when there is a measurable and documented reason.

Prefer zero `unsafe`.

---

# 10. Dependency Discipline

Do not add a crate because it is convenient.

Before adding a dependency, consider:

```text
Can std solve this?
Does this crate materially improve reliability?
How many transitive dependencies does it add?
Does it increase binary size?
Does it increase runtime memory?
Does it increase compile time?
Is it actively maintained?
```

Prefer small, focused crates.

Disable unused default features.

Do not pull a large SDK when a small protocol implementation is sufficient.

---

# 11. S3 Rules

S3 is a first-class backend.

Support S3-compatible storage through the existing repository abstraction.

Do not couple repository logic to AWS-specific features unnecessarily.

Do not add the full AWS SDK unless there is a demonstrated correctness requirement.

Preserve:

* streaming uploads
* multipart uploads
* bounded concurrency
* retries
* exponential backoff
* custom endpoints
* SigV4
* S3-compatible behavior

Never retry forever.

Never create unlimited concurrent S3 requests.

---

# 12. S3 Request Cost Matters

Avoid unnecessary:

```text
HEAD
GET
PUT
LIST
DELETE
```

requests.

Remember that S3 operations can be:

* slow
* rate-limited
* billable

Do not design algorithms that repeatedly scan the entire repository unless unavoidable.

Prefer repository indexes and metadata.

---

# 13. S3 Failure Behavior

Assume S3 can fail at any time.

Handle:

```text
timeouts
connection resets
429
500
502
503
504
```

with bounded retries.

Do not retry:

```text
authentication failures
authorization failures
invalid requests
```

unless there is a clear transient reason.

Errors must remain typed and actionable.

---

# 14. Repository Safety

Existing committed snapshots are sacred.

A failed operation must never invalidate a previous committed snapshot.

Prefer:

```text
orphaned blob
```

over:

```text
deleted valid blob
```

Repository state must favor safety over aggressive cleanup.

Do not mutate immutable content blobs.

---

# 15. Snapshot Commit Semantics

Only fully completed snapshots may become visible.

The intended conceptual lifecycle is:

```text
STARTED
 ↓
WRITING
 ↓
FINALIZING
 ↓
COMMITTED
```

If the process fails before `COMMITTED`:

```text
snapshot must not be restorable
previous committed snapshots must remain valid
```

Do not weaken atomic commit semantics to simplify implementation.

---

# 16. Crash Safety

Always consider:

```text
SIGTERM
SIGINT
SIGKILL
OOM kill
container restart
power failure
network failure
database disconnect
disk full
```

Before changing repository code, ask:

> What happens if the process dies immediately after this line?

Especially for:

* blob writes
* snapshot metadata
* locks
* prune
* multipart uploads
* restore

---

# 17. Prune Is Dangerous

`prune` is destructive.

Never trade safety for speed.

Before deleting a blob, establish that no committed snapshot references it.

If uncertain:

```text
do not delete
```

A leaked object costs money.

A deleted backup costs much more.

---

# 18. Locking

Do not introduce a complicated distributed locking architecture.

Use the existing repository locking design.

All destructive or repository-mutating operations must respect locking.

Test:

```text
backup + prune
backup + forget
restore + prune
two prune operations
stale locks
SIGKILL with lock held
```

Do not automatically force-unlock another process.

---

# 19. Database Adapters

Keep PostgreSQL and MySQL/MariaDB adapters independent.

Do not build a fake generic abstraction that hides database-specific consistency semantics.

Database-specific behavior belongs in the adapter.

The common layer should operate on the resulting backup stream.

---

# 20. PostgreSQL Rules

Do not call:

```text
pg_dump
pg_restore
psql
```

or shell out to external database tools.

Use the native protocol implementation.

Maintain consistent snapshot semantics.

Prefer PostgreSQL-native streaming mechanisms such as:

```text
COPY
```

where appropriate.

Do not unnecessarily lock production tables.

Do not change backup semantics without adding a consistency test.

---

# 21. MySQL / MariaDB Rules

Do not call:

```text
mysqldump
mysql
```

or any external database utility.

Use the native protocol implementation.

Maintain consistent transaction snapshot behavior.

Do not assume MySQL and MariaDB behave identically.

When behavior differs:

```text
detect
handle explicitly
test explicitly
document explicitly
```

---

# 22. Do Not Silently Lose Database Objects

If a database object is unsupported and its omission could affect restore correctness:

```text
fail clearly
```

or use an explicitly documented best-effort mode.

Never silently produce a backup that looks complete while dropping important objects.

Maintain an accurate compatibility matrix.

---

# 23. DMP1 Format Stability

Treat the DMP1 format as a public internal protocol.

Do not casually modify:

* frame structure
* record types
* encoding
* trailer semantics
* version semantics

If a format change is required:

1. increment version
2. document it
3. add compatibility tests
4. preserve old-read support where practical

Never silently reinterpret existing backups.

---

# 24. Parser Safety

All parsers must treat input as hostile.

This includes:

```text
DMP1
repository metadata
snapshot metadata
S3 XML
CLI input
database metadata
```

Never trust lengths from input.

Never allocate enormous buffers based solely on an input length.

Reject:

```text
overflow
truncation
invalid enum values
invalid lengths
invalid hashes
invalid versions
```

No parser should panic on malformed external input.

---

# 25. Cryptography

Do not invent cryptography.

Do not change established cryptographic primitives for cosmetic reasons.

Current cryptographic architecture uses:

```text
Argon2id
XChaCha20-Poly1305
SHA-256
CRC32
```

Preserve that approach unless a concrete security issue is demonstrated.

Do not log:

* passwords
* encryption keys
* S3 secret keys
* session tokens

Never weaken authenticated encryption for performance.

---

# 26. Encryption Ordering

Maintain correct ordering for deduplication.

Conceptually:

```text
plaintext stream
 ↓
chunk
 ↓
compress
 ↓
content identity/hash
 ↓
encrypt with unique nonce
 ↓
store
```

Do not design encryption in a way that makes legitimate deduplication impossible.

Do not reuse AEAD nonces with the same key.

---

# 27. Secrets

Secrets must not appear in:

```text
logs
errors
panic messages
JSON output
progress output
debug output
repository metadata
```

Database URLs must be sanitized.

S3 secrets must never be printed.

When adding logging, explicitly inspect whether sensitive values can reach the log path.

---

# 28. Error Handling

Normal user/runtime failures must not panic.

Use typed errors.

Errors should answer:

```text
what failed
which operation failed
whether it is retryable
what the operator should do
```

without leaking secrets.

Do not add verbose stack traces to normal CLI output.

---

# 29. Exit Codes Are API

Treat exit codes as a compatibility contract.

Do not casually change them.

Scripts and CronJobs depend on them.

Any new error category should be mapped deliberately.

---

# 30. CLI Stability

Do not rename existing commands or flags without a strong reason.

Do not break:

```text
backup
restore
verify
check
snapshots
info
forget
prune
stats
unlock
```

without compatibility consideration.

Human output may evolve.

Machine-readable `--json` output should remain stable.

---

# 31. JSON Output

JSON output is for automation.

Never write human prose into a JSON stream.

Never include secrets.

Prefer stable field names.

When changing JSON fields:

* preserve existing fields
* avoid unnecessary renames
* document meaningful changes

---