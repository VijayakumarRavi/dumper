---
trigger: always_on
---

# Dumper — Coding Agent Rules - Part 1

# 32. Progress Reporting

Progress reporting must never become a resource bottleneck.

Do not refresh progress continuously.

Throttle updates.

Do not create a separate thread/task merely to redraw a progress bar unless necessary.

`--quiet` must actually be quiet.

---

# 33. Resource-Constrained Container Rules

Assume the application may run under:

```text
128 MiB RAM
0.5 CPU
```

Do not assume:

```text
4 CPU
2 GB RAM
fast local disk
```

as the normal environment.

The existing deployment model explicitly targets constrained Kubernetes jobs.

---

# 34. Performance Rules

Do not optimize based on intuition.

Measure first.

When optimizing:

```text
1. establish baseline
2. make one change
3. benchmark
4. compare RSS
5. compare CPU
6. compare throughput
7. keep the change only if it is materially better
```

Never trade correctness for a benchmark win.

---

# 35. Database Load

Remember that Dumper runs beside production workloads.

Avoid:

```text
full-table unnecessary scans
excessive concurrency
unnecessary metadata queries
long heavyweight locks
large read-ahead buffers
```

The backup process should naturally slow when downstream storage is slow.

---

# 36. Testing Rules

Every bug fix should include a regression test.

Every new behavior should include a test.

Prefer integration tests for:

```text
database behavior
S3 behavior
repository behavior
restore behavior
```

Do not replace real integration tests with mocks when the behavior depends on actual database/S3 semantics.

---

# 37. Failure Testing

Important code paths must be tested under failure.

At minimum:

```text
database disconnect
S3 timeout
S3 500/503
wrong credentials
corrupt blob
missing blob
wrong password
SIGTERM
SIGKILL
disk full
stale lock
partial multipart upload
```

The expected outcome must be explicit.

---

# 38. Restore Tests Matter More Than Backup Tests

A backup is not successful merely because it completed.

The actual acceptance criterion is:

```text
backup
 ↓
verify
 ↓
restore
 ↓
validate
```

Whenever possible, integration tests should perform the entire round trip.

---

# 39. Do Not Trust Row Counts Alone

Restore validation must check actual values and structure.

Test:

```text
schema
columns
data
sequences
indexes
constraints
views
triggers
routines
```

where supported.

A database containing the correct row count but missing constraints is not a correct restore.

---

# 40. Documentation Must Match Reality

Do not claim:

```text
fully supported
production-ready
constant memory
fully tested
```

unless tests demonstrate the claim.

When behavior changes, update the relevant documentation.

Do not modify documentation merely to make the implementation look better.

---

# 41. Compatibility

Do not expand the supported-version matrix just because something happens to work locally.

A version belongs in the supported matrix only after meaningful testing.

Keep:

```text
backup tested
restore tested
verification tested
```

distinct where useful.

---

# 42. Repository Compatibility

Never break old repositories accidentally.

Before changing repository structures:

```text
inspect format version
add compatibility tests
consider old repositories
```

A newer binary should reject an unsupported repository clearly rather than corrupting it.

---

# 43. No Automatic Destructive Recovery

Never automatically run:

```text
unlock --force
prune
delete
repair
```

in response to an error.

Automatic recovery must not make a bad situation worse.

---

# 44. Small Commits

Make focused changes.

Prefer:

```text
fix PostgreSQL restore bug
add S3 retry test
fix snapshot commit race
add corruption test
```

over:

```text
refactor everything
```

Do not mix unrelated formatting/restructuring with functional changes.

---

# 45. Avoid Unnecessary Refactoring

Do not refactor existing code merely because you prefer a different style.

Refactor when:

```text
correctness requires it
testing requires it
resource usage requires it
maintainability is genuinely impaired
```

Otherwise leave working code alone.

---

# 46. No Drive-By Changes

Do not modify unrelated:

```text
dependencies
formatting
documentation
CI
Docker
shell scripts
```

while fixing another problem unless the change is necessary.

Keep diffs reviewable.

---

# 47. Formatting and Lints

Before finishing changes:

```bash
cargo fmt --check
cargo clippy
cargo test
```

Fix warnings introduced by your changes.

Do not silence warnings with broad `allow` attributes without justification.

---

# 48. Dependency Changes

When adding or changing a dependency:

1. explain why
2. enable only required features
3. inspect the dependency tree
4. run tests
5. check release binary size
6. consider runtime impact

Do not add a large dependency for a tiny utility.

---

# 49. Security-Sensitive Changes

For changes involving:

```text
crypto
authentication
S3 signing
repository format
secret handling
permissions
locking
```

add focused tests before considering the change complete.

Do not rely solely on manual testing.

---

# 50. Release Readiness

Before calling a change release-ready:

```text
cargo fmt --check
cargo clippy
cargo test
integration tests
security/dependency audit
real database backup
real database restore
S3 test
failure-path test
```

For major repository changes, test migration/compatibility.

---

# 51. Final Review Questions

Before completing any task, ask:

### Correctness

```text
Can this lose database data?
Can this create an incomplete backup?
Can this restore incorrect data?
```

### Crash safety

```text
What happens after SIGKILL here?
What happens after a network failure here?
```

### Memory

```text
Can input size cause memory usage to grow?
```

### CPU

```text
Can this accidentally use all available CPU?
```

### S3

```text
What happens if S3 disappears halfway through?
```

### Security

```text
Can a secret reach an error/log/output path?
```

### Compatibility

```text
Can this break an existing repository or script?
```

### Complexity

```text
Is this more complicated than necessary?
```

---

# 52. Golden Rule

When in doubt:

```text
simple > clever
bounded > unbounded
streaming > buffering
safe > fast
tested > assumed
small > feature-rich
recoverable > convenient
```

The goal is not to build the biggest backup system.

The goal is to build a backup system that can be trusted.
