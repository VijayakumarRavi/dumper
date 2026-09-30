# Dumper v0.2.3

Dumper `v0.2.3` is a critical bugfix release resolving an intermittent snapshot metadata decoding failure during `dumper check`, `dumper snapshots`, and snapshot restore/verification operations.

---

## What's New in v0.2.3

### 1. Robust Snapshot Metadata Decryption & Heuristic Fix

- **Root Cause**: When snapshot metadata files are committed, they are encrypted with XChaCha20-Poly1305 and prepended with a 24-byte random nonce. In approximately 1 out of 256 backups (~0.39%), the first byte of this random nonce happened to be `0x7B` (ASCII `{`). A naive backward-compatibility check previously treated any file starting with `{` as legacy plaintext JSON, skipping decryption and passing raw ciphertext to the JSON parser, which caused `dumper check` to fail intermittently with `key must be a string at line 1 column 2`.
- **Decrypt-First Architecture**: Snapshot metadata decoding now attempts authenticated AEAD decryption under the repository master key first. Only if decryption fails (or payload is unencrypted legacy data) does it gracefully fall back to JSON parsing for backward compatibility.
- **Verification Guarantee**: Repositories containing both encrypted and legacy unencrypted snapshots are verified seamlessly, and snapshot verification no longer experiences random failures.

---

## Upgrade & Compatibility

- **Fully Backward Compatible**: No repository format changes or migrations required. Existing repositories and all existing snapshots continue to work without modification.
- **Immediate Resolution**: Resolves intermittent verification errors on existing repositories where snapshots were created with nonces starting with `0x7B`.

---

## Installation

### Binary Tarballs & Checksums

Pre-built standalone binaries with SHA-256 checksums are attached below for:

- Linux `x86_64` (glibc and musl)
- Linux `aarch64` (glibc and musl)
- macOS `x86_64` (Intel)
- macOS `aarch64` (Apple Silicon)
- Windows `x86_64`
