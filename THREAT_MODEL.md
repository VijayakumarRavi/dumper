# Threat Model

## 1. System Overview & Trust Boundaries

Dumper operates as a client-side backup appliance. The primary trust boundary is the machine or container running the `dumper` process.

```text
               TRUSTED ZONE                       │          UNTRUSTED ZONE
┌──────────────────────────────────────────────┐  │
│  Dumper Process                              │  │
│  ├── Database Adapter                        │  │
│  ├── Streaming Pipeline                      │  │
│  └── Client-Side Encryption (XChaCha20)      │  │
└──────────────────────▲───────────────────────┘  │
                       │ TLS                      │
                       ▼                          │
            ┌─────────────────────┐               │
            │ Production Database │               │
            │ (PostgreSQL/MySQL)  │               │
            └─────────────────────┘               │
                                                  │
                       ───────────────────────────┼──► Remote Object Storage / Disk
                                                  │    (S3 / MinIO / Local Directory)
                                                  │
```

---

## 2. Adversary Assumptions & Defenses

### 2.1 Compromised Object Storage / Stolen S3 Credentials

- **Threat**: An adversary gains full administrative read and write access to the S3 bucket or storage repository.
- **Protection**:
  - All content blobs are encrypted with authenticated encryption (**XChaCha20-Poly1305**) before leaving the host.
  - The repository encryption key is protected using **Argon2id** password derivation with random salt.
  - An attacker reading the bucket cannot decrypt table schemas, row contents, or snapshot structures.
  - Blob paths are content-addressed hashes (`blobs/ab/cdef...`), revealing zero database or table names.

### 2.2 Tampering & Malicious Repository Modification

- **Threat**: An adversary injects malicious bytes, alters snapshot metadata, truncates blobs, or swaps chunks.
- **Protection**:
  - Authenticated AEAD tags (**Poly1305**) guarantee that any single-bit alteration in ciphertext immediately aborts decryption with an integrity error.
  - Each record within the stream is protected with individual **CRC-32** checksums.
  - End-to-end stream integrity is validated against a final **SHA-256** digest.
  - Verification (`dumper verify` and `dumper check`) flags missing or modified blobs.

### 2.3 Network Interception (Man-in-the-Middle)

- **Threat**: Traffic on the local or WAN network is intercepted.
- **Protection**:
  - All database and S3 connections use TLS via `rustls`.
  - Sensitive database credentials are never transmitted in cleartext.

### 2.4 Brute-Force Password Attacks

- **Threat**: An attacker with a copy of `config` attempts offline dictionary or brute-force password guessing.
- **Protection**:
  - Argon2id with 32 MiB memory cost and 3 iterations requires significant memory per attempt, rendering GPU and ASIC cracking economically prohibitive.

---

## 3. Out of Scope (What Dumper Does Not Protect Against)

1. **Compromised Host / Container**: If an attacker achieves root access or can inspect the memory of the running Dumper process, they can extract the master key from RAM while a backup is in flight.
2. **Bucket Obliteration**: If an attacker with S3 credentials executes `DeleteBucket` or deletes all objects, Dumper cannot restore lost data unless bucket versioning / S3 Object Lock is enabled at the cloud provider level.
3. **Database Compromise**: Dumper logical backups do not clean or sanitize compromised or malicious SQL data already present in the production database.
