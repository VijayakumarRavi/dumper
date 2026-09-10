use rand::{rngs::OsRng, RngCore};
use serde::{Deserialize, Serialize};

use crate::crypto::aead::{decrypt_blob, encrypt_blob};
use crate::crypto::kdf::{derive_key, generate_salt, KEY_LEN, SALT_LEN};
use crate::error::DumperError;

use zeroize::Zeroizing;

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct KeyEnvelope {
    pub salt_hex: String,
    pub encrypted_master_key_hex: String,
}

impl KeyEnvelope {
    /// Create a new key envelope with a fresh master key and protect it with password.
    pub fn create(password: &str) -> Result<(Self, Zeroizing<[u8; KEY_LEN]>), DumperError> {
        let salt = generate_salt();
        let kek = derive_key(password, &salt)?;

        let mut master_key = Zeroizing::new([0u8; KEY_LEN]);
        OsRng.fill_bytes(&mut *master_key);

        let encrypted = encrypt_blob(&kek, &*master_key)?;

        let envelope = Self {
            salt_hex: hex::encode(salt),
            encrypted_master_key_hex: hex::encode(encrypted),
        };

        Ok((envelope, master_key))
    }

    /// Unlock the repository master key using user password.
    pub fn unlock(&self, password: &str) -> Result<Zeroizing<[u8; KEY_LEN]>, DumperError> {
        let salt = hex::decode(&self.salt_hex)
            .map_err(|e| DumperError::Format(format!("Invalid salt hex: {}", e)))?;
        if salt.len() != SALT_LEN {
            return Err(DumperError::Format("Stored salt has invalid length".into()));
        }

        let encrypted_master = hex::decode(&self.encrypted_master_key_hex)
            .map_err(|e| DumperError::Format(format!("Invalid encrypted master key hex: {}", e)))?;

        let kek = derive_key(password, &salt)?;
        let decrypted = Zeroizing::new(decrypt_blob(&kek, &encrypted_master).map_err(|_| {
            DumperError::Authentication("Failed to unlock repository: incorrect password".into())
        })?);

        if decrypted.len() != KEY_LEN {
            return Err(DumperError::Integrity(
                "Decrypted master key has invalid length".into(),
            ));
        }

        let mut master_key = Zeroizing::new([0u8; KEY_LEN]);
        master_key.copy_from_slice(&decrypted);
        Ok(master_key)
    }

    /// Rotate repository password without changing the underlying master key.
    pub fn rotate_password(
        &mut self,
        current_password: &str,
        new_password: &str,
    ) -> Result<(), DumperError> {
        let master_key = self.unlock(current_password)?;
        let new_salt = generate_salt();
        let new_kek = derive_key(new_password, &new_salt)?;
        let new_encrypted = encrypt_blob(&new_kek, &*master_key)?;

        self.salt_hex = hex::encode(new_salt);
        self.encrypted_master_key_hex = hex::encode(new_encrypted);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_envelope_lifecycle() {
        let password = "my-secure-password";
        let (mut envelope, master_key) = KeyEnvelope::create(password).unwrap();

        // Unlock with correct password
        let unlocked = envelope.unlock(password).unwrap();
        assert_eq!(unlocked, master_key);

        // Unlock with wrong password
        let failed = envelope.unlock("wrong-password");
        assert!(failed.is_err());

        // Rotate password
        envelope
            .rotate_password(password, "brand-new-password")
            .unwrap();
        assert!(envelope.unlock(password).is_err());
        let unlocked_new = envelope.unlock("brand-new-password").unwrap();
        assert_eq!(unlocked_new, master_key);
    }

    #[test]
    fn test_key_zeroization_on_drop() {
        use zeroize::Zeroize;
        let mut key = [0x55u8; 32];
        key.zeroize();
        assert_eq!(key, [0u8; 32]);

        let buffer = zeroize::Zeroizing::new([0xAAu8; 32]);
        assert_eq!(*buffer, [0xAAu8; 32]);
        drop(buffer);
    }
}
