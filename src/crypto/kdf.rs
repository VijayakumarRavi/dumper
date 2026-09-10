use crate::error::DumperError;
use argon2::{Algorithm, Argon2, Params, Version};
use rand::{rngs::OsRng, RngCore};

pub const SALT_LEN: usize = 16;
pub const KEY_LEN: usize = 32;

pub fn generate_salt() -> [u8; SALT_LEN] {
    let mut salt = [0u8; SALT_LEN];
    OsRng.fill_bytes(&mut salt);
    salt
}

/// Derive a 256-bit encryption key from a password and salt using Argon2id.
/// Designed to run comfortably within 32MB RAM containers.
pub fn derive_key(password: &str, salt: &[u8]) -> Result<zeroize::Zeroizing<[u8; KEY_LEN]>, DumperError> {
    if password.is_empty() {
        return Err(DumperError::Authentication(
            "Password cannot be empty".into(),
        ));
    }

    // Parameters: 32MB memory (32768 KB), 3 iterations, 1 lane
    let params = Params::new(32768, 3, 1, Some(KEY_LEN))
        .map_err(|e| DumperError::Crypto(format!("Invalid Argon2 parameters: {}", e)))?;

    let argon2 = Argon2::new(Algorithm::Argon2id, Version::V0x13, params);

    let mut key = zeroize::Zeroizing::new([0u8; KEY_LEN]);
    argon2
        .hash_password_into(password.as_bytes(), salt, &mut *key)
        .map_err(|e| DumperError::Crypto(format!("Key derivation failed: {}", e)))?;

    Ok(key)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_derive_key() {
        let salt = generate_salt();
        let key1 = derive_key("correct-horse-battery-staple", &salt).unwrap();
        let key2 = derive_key("correct-horse-battery-staple", &salt).unwrap();
        let key3 = derive_key("wrong-password", &salt).unwrap();

        assert_eq!(key1, key2);
        assert_ne!(key1, key3);
        assert_eq!(key1.len(), KEY_LEN);
    }
}
