use dumper::repository::engine::RepositoryEngine;
use dumper::repository::lock::{LockType, RepositoryLock};
use tempfile::tempdir;

#[tokio::test]
async fn test_lock_exclusion() {
    let dir = tempdir().unwrap();
    let backend = std::sync::Arc::new(
        dumper::repository::local::LocalBackend::new(dir.path().to_str().unwrap())
            .await
            .unwrap(),
    );

    let _engine = RepositoryEngine::init(backend.clone(), "password123")
        .await
        .unwrap();

    // Acquire shared lock
    let mut lock1 = RepositoryLock::acquire(backend.clone(), LockType::Shared)
        .await
        .unwrap();

    // Second shared lock should succeed
    let mut lock2 = RepositoryLock::acquire(backend.clone(), LockType::Shared)
        .await
        .unwrap();

    // Exclusive lock should fail
    let lock3 = RepositoryLock::acquire(backend.clone(), LockType::Exclusive).await;
    assert!(lock3.is_err());
    let err_str = match lock3 {
        Err(e) => e.to_string(),
        Ok(_) => panic!("Expected error"),
    };
    assert!(
        err_str.contains("Repository is locked by")
            || err_str.contains("Concurrent lock acquisition")
    );

    lock1.release().await.unwrap();
    lock2.release().await.unwrap();

    // Now exclusive lock should succeed
    let mut lock4 = RepositoryLock::acquire(backend.clone(), LockType::Exclusive)
        .await
        .unwrap();

    // Shared lock should fail now
    let lock5 = RepositoryLock::acquire(backend.clone(), LockType::Shared).await;
    assert!(lock5.is_err());

    lock4.release().await.unwrap();
}

#[tokio::test]
async fn test_lock_raii_guard_drop_cleanup() {
    let dir = tempdir().unwrap();
    let backend = std::sync::Arc::new(
        dumper::repository::local::LocalBackend::new(dir.path().to_str().unwrap())
            .await
            .unwrap(),
    );

    // Helper that acquires a lock and returns Err without calling release()
    async fn failing_operation(
        backend: std::sync::Arc<dumper::repository::local::LocalBackend>,
    ) -> Result<(), &'static str> {
        let _guard = RepositoryLock::acquire(backend, LockType::Exclusive)
            .await
            .unwrap();
        // Simulate error leading to early exit and guard drop
        Err("simulated worker failure")
    }

    let res = failing_operation(backend.clone()).await;
    assert!(res.is_err());

    // Allow tokio runtime to execute the background drop cleanup task
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    // Verify lock is automatically cleaned up and a new lock can be acquired
    let mut new_lock = RepositoryLock::acquire(backend.clone(), LockType::Exclusive).await;
    assert!(
        new_lock.is_ok(),
        "RepositoryLock must be released on Drop when operation fails early"
    );
    new_lock.as_mut().unwrap().release().await.unwrap();
}

#[tokio::test]
async fn test_lock_cleanup_all_active_signal_safety() {
    let dir = tempdir().unwrap();
    let backend = std::sync::Arc::new(
        dumper::repository::local::LocalBackend::new(dir.path().to_str().unwrap())
            .await
            .unwrap(),
    );

    // Acquire multiple active locks
    let _lock1 = RepositoryLock::acquire(backend.clone(), LockType::Shared)
        .await
        .unwrap();
    let _lock2 = RepositoryLock::acquire(backend.clone(), LockType::Shared)
        .await
        .unwrap();

    // Verify locks are active in storage
    let active_before = RepositoryLock::list_active_locks(backend.as_ref())
        .await
        .unwrap();
    assert_eq!(active_before.len(), 2);

    // Simulate signal handler cleanup
    RepositoryLock::cleanup_all_active().await;

    // Verify all active locks are removed
    let active_after = RepositoryLock::list_active_locks(backend.as_ref())
        .await
        .unwrap();
    assert_eq!(
        active_after.len(),
        0,
        "cleanup_all_active must remove all registered active locks on interrupt"
    );

    // New exclusive lock can be acquired immediately
    let mut exclusive = RepositoryLock::acquire(backend.clone(), LockType::Exclusive)
        .await
        .unwrap();
    exclusive.release().await.unwrap();
}

#[tokio::test]
async fn test_lock_heartbeat_renewal() {
    let dir = tempdir().unwrap();
    let backend = std::sync::Arc::new(
        dumper::repository::local::LocalBackend::new(dir.path().to_str().unwrap())
            .await
            .unwrap(),
    );

    // Acquire lock with rapid 40ms heartbeat interval
    let mut lock = RepositoryLock::acquire_with_heartbeat_interval(
        backend.clone(),
        LockType::Shared,
        std::time::Duration::from_millis(40),
    )
    .await
    .unwrap();

    let initial_hb = lock.info.last_heartbeat.unwrap();

    // Poll for heartbeat update (giving up to 1s for background task to tick under heavy test load)
    use dumper::repository::backend::StorageBackend;
    let mut advanced = false;
    for _ in 0..20 {
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        if let Ok(data) = backend.get_object(&lock.path).await {
            if let Ok(info) = serde_json::from_slice::<dumper::repository::lock::LockInfo>(&data) {
                if let Some(hb) = info.last_heartbeat {
                    if hb > initial_hb {
                        advanced = true;
                        break;
                    }
                }
            }
        }
    }

    assert!(advanced, "Heartbeat timestamp must advance over time");

    lock.release().await.unwrap();
}

#[tokio::test]
async fn test_corrupted_lock_file_unlock_force() {
    let dir = tempdir().unwrap();
    let backend = std::sync::Arc::new(
        dumper::repository::local::LocalBackend::new(dir.path().to_str().unwrap())
            .await
            .unwrap(),
    );

    // Write a corrupted lock file
    use dumper::repository::backend::StorageBackend;
    backend
        .put_object("locks/corrupted_lock_xyz", b"MALFORMED_JSON_BYTES")
        .await
        .unwrap();

    // Non-force unlock must not remove corrupted lock file
    let removed_normal = RepositoryLock::unlock_all(backend.as_ref(), false)
        .await
        .unwrap();
    assert_eq!(removed_normal, 0);
    assert!(backend
        .object_exists("locks/corrupted_lock_xyz")
        .await
        .unwrap());

    // Force unlock MUST remove corrupted lock file
    let removed_force = RepositoryLock::unlock_all(backend.as_ref(), true)
        .await
        .unwrap();
    assert_eq!(removed_force, 1);
    assert!(!backend
        .object_exists("locks/corrupted_lock_xyz")
        .await
        .unwrap());
}
