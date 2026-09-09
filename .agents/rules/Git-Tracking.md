---
trigger: always_on
---

# Git Change Tracking Rules

## 1. Git Is Part of the Engineering Process

All source-code changes must be tracked through Git.

Never treat the working tree as disposable scratch space.

Every meaningful implementation change must be represented by a commit.

The Git history must make it possible to answer:

```text id="l3b9wq"
What changed?
Why did it change?
Which issue/problem did it fix?
What tests prove it?
Can the change be reverted safely?
```

---

# 2. Start Every Task by Inspecting Git State

Before modifying anything, run:

```bash
git status --short
git branch --show-current
git log -5 --oneline
```

Understand the current state before making changes.

Never assume the working tree is clean.

---

# 3. Never Destroy Existing User Changes

If `git status` shows modifications that were not created by you:

```text id="1l7d9v"
DO NOT overwrite them
DO NOT reset them
DO NOT checkout them
DO NOT stash them automatically
```

Work around existing changes when possible.

Never run destructive commands such as:

```bash
git reset --hard
git checkout -- .
git clean -fd
```

unless explicitly instructed by the user.

---

# 4. Keep Changes Focused

One commit should represent one logical change.

Good:

```text id="yq5xet"
fix PostgreSQL sequence restoration
add PostgreSQL sequence regression test
fix S3 retry classification
add S3 retry tests
```

Bad:

```text id="w2nm28"
production readiness changes
```

containing unrelated changes across the entire project.

---

# 5. Commit After Logical Milestones

Do not wait until the entire production-readiness effort is finished.

Create commits after meaningful, working milestones.

Recommended structure:

```text id="l3t0js"
1. fix streaming backup pipeline
2. fix streaming restore pipeline
3. harden DMP1 decoder
4. improve PostgreSQL metadata restore
5. improve MySQL/MariaDB restore
6. harden S3 retries
7. harden repository locking
8. add repository corruption tests
9. add crash-safety tests
10. add database integration tests
11. add resource benchmarks
12. update documentation
```

Not every task needs exactly one commit, but commits should remain logically coherent.

---

# 6. Every Bug Fix Gets a Regression Test

When fixing a bug:

```text id="x2a4lq"
1. reproduce the bug
2. add a failing test
3. fix the bug
4. verify the test passes
5. commit both together
```

Do not commit a bug fix without its regression test unless a test is genuinely impossible.

If a test cannot be written, document why.

---

# 7. Tests and Implementation Belong Together

Do not create:

```text id="rj1y9r"
commit 1: implementation
commit 2: "tests later"
```

for a normal bug fix.

Prefer:

```text id="o5de4z"
commit: fix X and add regression test
```

so that the history remains bisectable.

---

# 8. Commits Must Build

Every commit should ideally leave the repository in a usable state.

At minimum, the code should compile.

For functional changes, run the relevant tests before committing.

Avoid commits that intentionally leave:

```text id="t4m3fw"
broken compilation
missing imports
failing core tests
half-written migration
```

unless the task explicitly requires a staged series where intermediate commits cannot build.

Prefer atomic, buildable commits.

---

# 9. Commit Message Format

Use Conventional Commit-style messages.

Format:

```text
<type>(<scope>): <short description>
```

Examples:

```text id="n4ekiw"
fix(postgres): restore sequences after table data

test(postgres): add concurrent backup consistency test

fix(s3): stop retrying authentication failures

test(repository): verify prune never deletes live blobs

perf(stream): remove full-buffer backup staging

fix(stream): reject oversized DMP1 payloads

test(mysql): validate generated column restore

docs(operations): document interrupted backup recovery
```

Use these common types:

```text id="l6uoh4"
feat
fix
test
perf
refactor
docs
build
ci
security
```

Use `security` for security-specific fixes where appropriate.

---

# 10. Commit Subject Rules

Keep the subject:

* concise
* imperative
* specific

Prefer:

```text
fix(s3): classify 403 responses as non-retryable
```

over:

```text
fixed some S3 stuff
```

Prefer:

```text
fix(stream): bound DMP1 payload allocation
```

over:

```text
improve stream safety
```

---

# 11. Commit Body for Important Changes

For significant or security-sensitive changes, include a short commit body.

Example:

```text
fix(repository): make prune snapshot-safe

Prune now calculates live blob references from committed snapshots
before deleting unreferenced objects.

This prevents a concurrent backup from losing referenced data.

Tests:
- prune with multiple snapshots
- backup/prune contention
- orphan blob cleanup
```

Do not write essay-length commit messages.

---

# 12. Reference the Problem

When there is an issue, ticket, bug report, or implementation-plan item, reference it in the commit body.

Example:

```text
Refs: production-readiness #12
Fixes: #34
```

Use the repository's actual issue/PR numbering conventions.

Do not invent issue numbers.

---

# 13. Don't Mix Refactoring With Bug Fixes

Avoid commits like:

```text id="33a5uv"
fix postgres restore
+ rename 40 structs
+ reorganize modules
+ format unrelated files
+ replace a crate
```

Instead separate unrelated changes.

This makes:

```text id="vx5jzx"
review
revert
git bisect
```

much easier.

---

# 14. Formatting Changes

If formatting is required:

```bash
cargo fmt
```

include the formatting changes in the relevant commit when possible.

Do not create enormous formatting-only commits unless explicitly needed.

Never mix a massive formatting rewrite with a security or correctness fix.

---

# 15. Dependency Changes

Dependency changes deserve their own logical commit when substantial.

Example:

```text id="o8s77v"
build: add minimal rustls dependency for S3 TLS
```

The commit should explain why the dependency is necessary.

Before committing:

```bash
cargo tree
cargo audit
```

where appropriate.

---

# 16. Repository Format Changes

Any change to:

```text id="m2j0f7"
repository format
snapshot format
DMP1 stream format
encryption envelope
blob encoding
```

must be isolated and very clearly committed.

Example:

```text id="8od9ah"
feat(repository): bump repository format to v2
```

The commit body must explain:

```text
what changed
why it changed
compatibility impact
migration/read compatibility
tests added
```

Do not bury format changes inside unrelated fixes.

---

# 17. Never Rewrite History Unexpectedly

Do not run:

```bash
git rebase
git reset
git commit --amend
git push --force
```

unless explicitly requested or clearly required by the workflow.

The agent should assume existing commits are valuable audit history.

Never rewrite published history merely to make the log look cleaner.

---

# 18. Never Force Push

Never execute:

```bash
git push --force
git push --force-with-lease
```

unless explicitly requested.

Production infrastructure projects need a trustworthy history.

---

# 19. Inspect the Diff Before Every Commit

Before committing:

```bash
git status
git diff
git diff --check
```

Verify:

```text id="uj0kby"
only intended files changed
no secrets
no credentials
no debug artifacts
no generated junk
no accidental deletes
```

---

# 20. Secret Protection

Before committing, check for accidental secrets.

Never commit:

```text id="abq4gc"
database passwords
S3 secret keys
AWS session tokens
repository passwords
private keys
test credentials that are actually sensitive
```

Be especially careful with:

```text id="cju2mu"
.env
config files
logs
test fixtures
shell history
debug output
```

Use clearly fake credentials in tests.

---

# 21. Generated Files

Do not commit:

```text id="nx6fcy"
target/
temporary dumps
large database files
local S3 data
MinIO data directories
coverage output
benchmark artifacts
editor files
```

unless explicitly required.

Keep `.gitignore` aligned with the project.

---

# 22. Large Files

Do not commit database dumps, huge test artifacts, binaries, or S3 object fixtures unless explicitly required.

Use generated test data instead.

Repository test fixtures should remain small.

---

# 23. Git Diff Is a Security Review

For sensitive changes, inspect the final diff specifically for:

```text id="m10w7w"
credential exposure
crypto changes
authentication bypass
permission changes
unsafe deserialization
unbounded allocation
destructive repository operations
```

Do not rely solely on tests.

---

# 24. Commit Tests With Functional Changes

When adding a production-hardening change:

```text id="3bn1ne"
implementation
+
regression test
+
necessary documentation
```

should generally be committed together.

This ensures a commit tells a complete engineering story.

---

# 25. Documentation Changes

Documentation updates caused by behavior changes should normally be in the same commit.

Example:

```text id="kqaqfa"
fix(s3): classify authentication errors correctly

```

may also update the S3 behavior documentation in that same commit.

Do not leave the documentation knowingly incorrect.

---

# 26. Changelog

If the project maintains a changelog:

* update it for user-visible changes
* mention breaking changes
* mention repository-format changes
* mention important security fixes

Do not add noise for every internal refactor.

---

# 27. Git Bisect Compatibility

Write commits so that `git bisect` remains useful.

When practical:

```text id="p7mk95"
each commit builds
each commit has sensible behavior
```

Avoid combining dozens of unrelated changes into a single commit.

This is especially important for bugs involving:

```text id="x1at9x"
database restore
repository corruption
S3 behavior
memory regressions
```

---

# 28. Before a Risky Change, Record the Baseline

Before modifying major resource-sensitive or repository-sensitive code, record:

```text id="c3c6xx"
current tests
current binary size
current RSS where known
current repository format
current dependency state
```

Do not needlessly create commits just to record measurements, but ensure the Git history can identify the implementation state associated with important measurements.

---

# 29. Avoid "WIP" Commits on Main

Do not create commits such as:

```text id="1oe2k0"
WIP
fix later
temporary
try this
debug
```

unless the repository workflow explicitly uses temporary branches/commits.

Prefer completing the logical change before committing.

---

# 30. Branching

Use a dedicated branch for substantial work.

Suggested names:

```text id="giq9qf"
feature/production-hardening
fix/postgres-restore
fix/s3-retry
test/database-integration
perf/streaming-memory
```

Do not develop large changes directly on the main branch unless that is explicitly the repository workflow.

---

# 31. Keep Main Stable

Do not merge code into the main branch that knowingly breaks:

```text id="5jcj7v"
build
tests
backup
restore
repository compatibility
```

For production-hardening work, the main branch should remain usable.

---

# 32. Tag Meaningful Releases

When a production-ready release is actually reached, create a version tag consistent with the project's semantic versioning.

Example:

```bash
git tag -a v1.0.0 -m "Dumper 1.0.0"
```

Only tag a release after the documented production-readiness gates pass.

Do not call a prototype release `1.0.0` merely because the CLI works.

