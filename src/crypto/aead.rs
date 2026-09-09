use crate::error::DumperError;
use chacha20poly1305::{
    aead::{Aead, KeyInit},
    XChaCha20Poly1305, XNonce,
};
use rand::{rngs::OsRng, RngCore};

pub const NONCE_LEN: usize = 24;
pub const TAG_LEN: usize = 16;

/// Encrypt data with XChaCha20-Poly1305 using a 24-byte random nonce.
/// Format: `[nonce (24B) | ciphertext + poly1305_tag (len + 16B)]`
pub fn encrypt_blob(key: &[u8; 32], plaintext: &[u8]) -> Result<Vec<u8>, DumperError> {
    let cipher = XChaCha20Poly1305::new_from_slice(key)
        .map_err(|e| DumperError::Crypto(format!("Invalid cipher key: {}", e)))?;

    let mut nonce_bytes = [0u8; NONCE_LEN];
    OsRng.fill_bytes(&mut nonce_bytes);
    let nonce = XNonce::from_slice(&nonce_bytes);

    let ciphertext = cipher
        .encrypt(nonce, plaintext)
        .map_err(|e| DumperError::Crypto(format!("AEAD encryption failed: {}", e)))?;

    let mut output = Vec::with_capacity(NONCE_LEN + ciphertext.len());
    output.extend_from_slice(&nonce_bytes);
    output.extend_from_slice(&ciphertext);
    Ok(output)
}

/// Decrypt data with XChaCha20-Poly1305 and verify integrity tag.
pub fn decrypt_blob(key: &[u8; 32], payload: &[u8]) -> Result<Vec<u8>, DumperError> {
    if payload.len() < NONCE_LEN + TAG_LEN {
        return Err(DumperError::Integrity(
            "Encrypted payload is shorter than minimum nonce + tag length".into(),
        ));
    }

    let (nonce_bytes, ciphertext) = payload.split_at(NONCE_LEN);
    let nonce = XNonce::from_slice(nonce_bytes);
    let cipher = XChaCha20Poly1305::new_from_slice(key)
        .map_err(|e| DumperError::Crypto(format!("Invalid cipher key: {}", e)))?;

    let plaintext = cipher.decrypt(nonce, ciphertext).map_err(|_| {
        DumperError::Integrity(
            "Decryption failed: authentication tag mismatch or corrupted data".into(),
        )
    })?;

    Ok(plaintext)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_encrypt_decrypt_roundtrip() {
        let key = [0x42u8; 32];
        let message = b"Hello, Dumper secure streaming database backup!";

        let encrypted = encrypt_blob(&key, message).unwrap();
        assert_ne!(encrypted, message);
        assert_eq!(encrypted.len(), NONCE_LEN + message.len() + TAG_LEN);

        let decrypted = decrypt_blob(&key, &encrypted).unwrap();
        assert_eq!(decrypted, message);
    }

    #[test]
    fn test_corrupted_data_detection() {
        let key = [0x42u8; 32];
        let message = b"Confidential DB records";
        let mut encrypted = encrypt_blob(&key, message).unwrap();

        // Corrupt one byte
        let last_idx = encrypted.len() - 1;
        encrypted[last_idx] ^= 0xFF;

        let res = decrypt_blob(&key, &encrypted);
        assert!(res.is_err());
    }

    #[test]
    fn test_wrong_key_fails() {
        let key1 = [0x42u8; 32];
        let key2 = [0x99u8; 32];
        let message = b"Secret data";

        let encrypted = encrypt_blob(&key1, message).unwrap();
        let res = decrypt_blob(&key2, &encrypted);
        assert!(res.is_err());
    }
}
