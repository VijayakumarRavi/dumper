use chrono::{DateTime, Duration, Utc};
use serde::{Deserialize, Serialize};
use crate::error::DumperError;
use crate::repository::backend::StorageBackend;

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub enum LockType {
    Shared,
    Exclusive,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct LockInfo {
    pub lock_id: String,
    pub lock_type: LockType,
    pub hostname: String,
    pub pid: u32,
    pub created_at: DateTime<Utc>,
}

pub struct RepositoryLock {
    pub info: LockInfo,
    pub path: String,
}

impl RepositoryLock {
    pub async fn acquire<B: StorageBackend>(
        backend: &B,
        lock_type: LockType,
    ) -> Result<Self, DumperError> {
        let existing_locks = Self::list_active_locks(backend).await?;

        // Conflict check
        for existing in &existing_locks {
            match lock_type {
                LockType::Exclusive => {
                    // Exclusive lock conflicts with ANY existing lock
                    return Err(DumperError::Repository(format!(
                        "Repository is locked by {} (pid: {}, created: {})",
                        existing.hostname, existing.pid, existing.created_at
                    )));
                }
                LockType::Shared => {
                    // Shared lock conflicts only with Exclusive locks
                    if existing.lock_type == LockType::Exclusive {
                        return Err(DumperError::Repository(format!(
                            "Repository is exclusively locked by {} (pid: {}, created: {})",
                            existing.hostname, existing.pid, existing.created_at
                        )));
                    }
                }
            }
        }

        let lock_id = hex::encode(rand::random::<[u8; 8]>());
        let hostname = std::env::var("HOSTNAME").unwrap_or_else(|_| "localhost".to_string());
        let pid = std::process::id();

        let info = LockInfo {
            lock_id: lock_id.clone(),
            lock_type: lock_type.clone(),
            hostname,
            pid,
            created_at: Utc::now(),
        };

        let path = format!("locks/{}", lock_id);
        let data = serde_json::to_vec(&info)?;
        backend.put_object(&path, &data).await?;

        // Phase 2: Verify lock acquisition
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
        
        let verify_locks = Self::list_active_locks(backend).await?;
        for existing in &verify_locks {
            if existing.lock_id == lock_id {
                continue;
            }
            
            let is_conflict = match lock_type {
                LockType::Exclusive => true,
                LockType::Shared => existing.lock_type == LockType::Exclusive,
            };
            
            if is_conflict {
                // Yield to older locks or same timestamp but lower lock ID (tie-breaker)
                if existing.created_at < info.created_at || (existing.created_at == info.created_at && existing.lock_id < info.lock_id) {
                    let _ = backend.delete_object(&path).await;
                    return Err(DumperError::Repository(format!(
                        "Concurrent lock acquisition detected. Yielded to lock by {} (pid: {})",
                        existing.hostname, existing.pid
                    )));
                }
            }
        }

        Ok(Self { info, path })
    }

    pub async fn release<B: StorageBackend>(&self, backend: &B) -> Result<(), DumperError> {
        backend.delete_object(&self.path).await
    }

    pub async fn list_active_locks<B: StorageBackend>(backend: &B) -> Result<Vec<LockInfo>, DumperError> {
        let keys = backend.list_objects("locks").await?;
        let mut active = Vec::new();
        let now = Utc::now();

        for key in keys {
            if let Ok(data) = backend.get_object(&key).await {
                if let Ok(info) = serde_json::from_slice::<LockInfo>(&data) {
                    // Consider locks older than 2 hours stale
                    if now.signed_duration_since(info.created_at) < Duration::hours(2) {
                        active.push(info);
                    }
                }
            }
        }

        Ok(active)
    }

    pub async fn unlock_all<B: StorageBackend>(backend: &B, force: bool) -> Result<usize, DumperError> {
        let keys = backend.list_objects("locks").await?;
        let mut removed = 0;
        let now = Utc::now();

        for key in keys {
            if let Ok(data) = backend.get_object(&key).await {
                if let Ok(info) = serde_json::from_slice::<LockInfo>(&data) {
                    let is_stale = now.signed_duration_since(info.created_at) >= Duration::hours(2);
                    if is_stale || force {
                        let _ = backend.delete_object(&key).await;
                        removed += 1;
                    }
                }
            }
        }

        Ok(removed)
    }
}
