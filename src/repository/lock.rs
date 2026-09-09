use crate::error::DumperError;
use crate::repository::backend::StorageBackend;
use chrono::{DateTime, Duration, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

type CleanupClosure = Box<dyn Fn() -> Pin<Box<dyn Future<Output = ()> + Send>> + Send + Sync>;

static NEXT_REGISTRATION_ID: AtomicU64 = AtomicU64::new(1);
static ACTIVE_CLEANUPS: std::sync::LazyLock<Mutex<HashMap<u64, CleanupClosure>>> =
    std::sync::LazyLock::new(|| Mutex::new(HashMap::new()));

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

pub struct RepositoryLockGuard<B: StorageBackend + 'static> {
    lock: Option<RepositoryLock>,
    backend: Arc<B>,
    registration_id: u64,
}

impl<B: StorageBackend + 'static> std::ops::Deref for RepositoryLockGuard<B> {
    type Target = RepositoryLock;
    fn deref(&self) -> &Self::Target {
        self.lock.as_ref().expect("Lock has already been released")
    }
}

impl<B: StorageBackend + 'static> RepositoryLockGuard<B> {
    pub async fn release(&mut self) -> Result<(), DumperError> {
        if let Some(lock) = self.lock.take() {
            if let Ok(mut map) = ACTIVE_CLEANUPS.lock() {
                map.remove(&self.registration_id);
            }
            lock.release(&*self.backend).await?;
        }
        Ok(())
    }
}

impl<B: StorageBackend + 'static> Drop for RepositoryLockGuard<B> {
    fn drop(&mut self) {
        if let Some(lock) = self.lock.take() {
            if let Ok(mut map) = ACTIVE_CLEANUPS.lock() {
                map.remove(&self.registration_id);
            }
            let backend = self.backend.clone();
            let path = lock.path.clone();
            if let Ok(handle) = tokio::runtime::Handle::try_current() {
                handle.spawn(async move {
                    let _ = backend.delete_object(&path).await;
                });
            }
        }
    }
}

impl RepositoryLock {
    pub async fn acquire<B: StorageBackend + 'static>(
        backend: Arc<B>,
        lock_type: LockType,
    ) -> Result<RepositoryLockGuard<B>, DumperError> {
        let lock = Self::acquire_raw(&*backend, lock_type).await?;
        let reg_id = NEXT_REGISTRATION_ID.fetch_add(1, Ordering::Relaxed);

        let backend_for_cleanup = backend.clone();
        let path_for_cleanup = lock.path.clone();
        let cleanup_fn: CleanupClosure = Box::new(move || {
            let b = backend_for_cleanup.clone();
            let p = path_for_cleanup.clone();
            Box::pin(async move {
                let _ = b.delete_object(&p).await;
            })
        });

        if let Ok(mut map) = ACTIVE_CLEANUPS.lock() {
            map.insert(reg_id, cleanup_fn);
        }

        Ok(RepositoryLockGuard {
            lock: Some(lock),
            backend,
            registration_id: reg_id,
        })
    }

    pub async fn acquire_raw<B: StorageBackend>(
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
                if existing.created_at < info.created_at
                    || (existing.created_at == info.created_at && existing.lock_id < info.lock_id)
                {
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

    pub async fn cleanup_all_active() {
        let cleanups: Vec<CleanupClosure> = {
            if let Ok(mut map) = ACTIVE_CLEANUPS.lock() {
                map.drain().map(|(_, v)| v).collect()
            } else {
                Vec::new()
            }
        };

        for cleanup in cleanups {
            cleanup().await;
        }
    }

    pub async fn list_active_locks<B: StorageBackend>(
        backend: &B,
    ) -> Result<Vec<LockInfo>, DumperError> {
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

    pub async fn unlock_all<B: StorageBackend>(
        backend: &B,
        force: bool,
    ) -> Result<usize, DumperError> {
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
