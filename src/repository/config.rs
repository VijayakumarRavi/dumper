use crate::crypto::envelope::KeyEnvelope;
use crate::error::DumperError;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

pub const REPO_FORMAT_VERSION: u32 = 1;
pub const CONFIG_FILE_PATH: &str = "config";

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct RepositoryConfig {
    pub format_version: u32,
    pub repository_id: String,
    pub created_at: DateTime<Utc>,
    pub envelope: KeyEnvelope,
}

impl RepositoryConfig {
    pub fn new(password: &str) -> Result<(Self, [u8; 32]), DumperError> {
        let (envelope, master_key) = KeyEnvelope::create(password)?;
        let repository_id = hex::encode(rand::random::<[u8; 16]>());

        let config = Self {
            format_version: REPO_FORMAT_VERSION,
            repository_id,
            created_at: Utc::now(),
            envelope,
        };

        Ok((config, master_key))
    }

    pub fn unlock(&self, password: &str) -> Result<[u8; 32], DumperError> {
        if self.format_version > REPO_FORMAT_VERSION {
            return Err(DumperError::Format(format!(
                "Repository format version {} is newer than supported version {}",
                self.format_version, REPO_FORMAT_VERSION
            )));
        }
        self.envelope.unlock(password)
    }
}
