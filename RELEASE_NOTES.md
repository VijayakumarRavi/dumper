# Dumper v0.2.2

Dumper `v0.2.2` is a bugfix release addressing snapshot table column alignment, displaying snapshot timestamps in local system time, adopting Restic-compatible listing UX, and supporting short snapshot prefix resolution.

---

## What's New in v0.2.2

### 1. Snapshot Table Column Alignment Fix

- **Header Alignment**: Corrected the column spacing in `dumper snapshots`. Previously, 16-hex-character snapshot IDs pushed all subsequent headers (`DATE`, `ENGINE`, `DATABASE`, `LOGICAL`, `STORED`) 6 spaces out of alignment with their respective column values. The ID column is now properly budgeted to 16 characters.

### 2. Local Timezone Display

- **System Timezone Formatting**: Snapshot timestamps in `dumper snapshots` and `dumper info` are now automatically converted and displayed in the system's local timezone (e.g., IST, EST, CEST) using `chrono::Local`.
- **Automatic UTC Fallback**: If the local timezone is unavailable (such as in minimal container environments without timezone data), Dumper gracefully falls back to UTC.

### 3. Restic-Compatible Listing UX

- **Column Header Modernization**: The `DATE` column header is now named `Time`, and all headers use clean Title Case (`ID`, `Time`, `Engine`, `Database`, `Logical`, `Stored`).
- **Footer Metadata**: Added a closing separator line along with `Timestamps shown in local time` and a snapshot count summary (`N snapshots`), matching Restic's CLI conventions.

### 4. Short-Prefix Snapshot Resolution

- **8-Character Prefix Support**: `dumper verify <ID>` and `dumper restore <ID>` now resolve snapshots by their 8-character short prefix in addition to exact IDs and full SHA-256 hashes, improving command-line ergonomics.

### 5. CI & Script Robustness

- **Garage S3 Setup**: Fixed bash subshell expansion syntax in GitHub Actions CI workflow.
- **Pipefail Safety**: Prevented SIGPIPE (141) under pipefail when extracting snapshot IDs from JSON output in E2E validation scripts.

---

## Upgrade & Compatibility

- **Fully Backward Compatible**: No repository format changes. Existing repositories and snapshots work without modification.
- **Machine-Readable API Stability**: The `--json` flag output remains stable and continues to emit ISO-8601/RFC3339 UTC timestamps.

---

## Installation

### Binary Tarballs & Checksums

Pre-built standalone binaries with SHA-256 checksums are attached below for:

- Linux `x86_64` (glibc and musl)
- Linux `aarch64` (glibc and musl)
- macOS `x86_64` (Intel)
- macOS `aarch64` (Apple Silicon)
- Windows `x86_64`
