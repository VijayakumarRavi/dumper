# Security Architecture & Cryptography

Dumper treats all remote storage backends and intermediate networks as completely untrusted. Data is encrypted and authenticated client-side before leaving the application boundary.

## Cryptographic Primitives

Dumper relies strictly on modern, standard, battle-tested cryptographic algorithms:

1. **Password-Based Key Derivation**: **Argon2id** (RFC 9106)
   - Salt: 16 cryptographically random bytes generated via OS CSPRNG (`getrandom` / `OsRng`).
   - Configuration: 32 MiB memory cost ($m=32768$), 3 iterations ($t=3$), 1 lane ($p=1$).
   - Derives a 256-bit Key Encryption Key (KEK).
2. **Authenticated Encryption with Associated Data (AEAD)**: **XChaCha20-Poly1305**
   - 256-bit key.
   - 192-bit (24-byte) random nonce generated per blob. The large 192-bit nonce space allows safe random nonce generation with zero risk of nonce collisions across trillions of chunks.
   - 128-bit (16-byte) Poly1305 authentication tag verified in constant time prior to decompression or processing.
3. **Content Addressing & Integrity**: **SHA-256**
   - Standard 256-bit SHA-2 hash used for content-based deduplication addressing and end-to-end stream verification.
4. **Data Framing Checksums**: **CRC-32**
   - Hardware-accelerated CRC32-C / IEEE for frame demarcation and corruption detection.

## Envelope Encryption & Key Management

- The repository master key is a randomly generated 256-bit key.
- The master key is wrapped using the Argon2id-derived KEK and stored in `config`.
- **Key Rotation**: To change the repository password, only the `config` envelope is re-encrypted with a new KEK. Data blobs never need to be downloaded or re-encrypted.
- **Plaintext Secrets**: Plaintext passwords and secret keys are never written to disk or stored in the repository.

## Credential Sanitization

- All error messages, logs, and progress events run through `sanitize_secrets()`.
- Connection strings (`postgres://user:pass@host/db`) have their password segments masked (`user:*****@host`).
- AWS / S3 access keys and secret keys are never printed to terminal or standard error streams.
