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

#[tokio::test]
async fn test_backup_exceeds_lock_ttl_with_heartbeat_renewal() {
    use chrono::{Duration as ChronoDuration, Utc};
    use dumper::repository::backend::StorageBackend;
    use dumper::repository::lock::LockInfo;

    let dir = tempdir().unwrap();
    let backend = std::sync::Arc::new(
        dumper::repository::local::LocalBackend::new(dir.path().to_str().unwrap())
            .await
            .unwrap(),
    );

    let now = Utc::now();
    // Simulate a lock created 3 hours ago (far exceeding normal 2-hour default TTL),
    // but with an active heartbeat updated 10 seconds ago.
    let long_running_lock_info = LockInfo {
        lock_id: "long_running_backup_lock".into(),
        lock_type: LockType::Shared,
        hostname: "worker-backup-node-1".into(),
        pid: 9999,
        created_at: now - ChronoDuration::hours(3),
        last_heartbeat: Some(now - ChronoDuration::seconds(10)),
    };

    let lock_path = format!("locks/{}", long_running_lock_info.lock_id);
    let lock_bytes = serde_json::to_vec(&long_running_lock_info).unwrap();
    backend.put_object(&lock_path, &lock_bytes).await.unwrap();

    // 1. Verify that because the heartbeat is fresh (< 15 min), the lock remains ACTIVE
    let active_locks = RepositoryLock::list_active_locks(backend.as_ref())
        .await
        .unwrap();
    assert_eq!(active_locks.len(), 1);
    assert_eq!(active_locks[0].lock_id, "long_running_backup_lock");

    // 2. An exclusive lock attempt (e.g. prune) MUST fail
    let exclusive_attempt = RepositoryLock::acquire(backend.clone(), LockType::Exclusive).await;
    assert!(
        exclusive_attempt.is_err(),
        "Prune exclusive lock must be blocked by long-running backup with fresh heartbeat"
    );

    // 3. Regular unlock_all (non-force) must NOT remove this active lock
    let cleaned = RepositoryLock::unlock_all(backend.as_ref(), false)
        .await
        .unwrap();
    assert_eq!(
        cleaned, 0,
        "Active lock with fresh heartbeat must not be cleaned up"
    );

    // 4. Now simulate process crash / heartbeat abandoned: last_heartbeat is older than 15-minute TTL
    let stale_lock_info = LockInfo {
        lock_id: "long_running_backup_lock".into(),
        lock_type: LockType::Shared,
        hostname: "worker-backup-node-1".into(),
        pid: 9999,
        created_at: now - ChronoDuration::hours(3),
        last_heartbeat: Some(now - ChronoDuration::minutes(20)),
    };
    let stale_bytes = serde_json::to_vec(&stale_lock_info).unwrap();
    backend.put_object(&lock_path, &stale_bytes).await.unwrap();

    // The lock is now recognized as inactive by list_active_locks
    let active_after_abandon = RepositoryLock::list_active_locks(backend.as_ref())
        .await
        .unwrap();
    assert_eq!(
        active_after_abandon.len(),
        0,
        "Lock with stale heartbeat (>15 min) must not be listed as active"
    );

    // unlock_all cleans up stale lock safely without requiring --force
    let cleaned_stale = RepositoryLock::unlock_all(backend.as_ref(), false)
        .await
        .unwrap();
    assert_eq!(
        cleaned_stale, 1,
        "Stale lock must be reclaimed by unlock_all"
    );

    // Exclusive lock can now be acquired
    let mut exclusive_success = RepositoryLock::acquire(backend.clone(), LockType::Exclusive)
        .await
        .unwrap();
    exclusive_success.release().await.unwrap();
}

#[tokio::test]
async fn test_concurrent_backup_and_prune_mutual_exclusion() {
    let dir = tempdir().unwrap();
    let backend = std::sync::Arc::new(
        dumper::repository::local::LocalBackend::new(dir.path().to_str().unwrap())
            .await
            .unwrap(),
    );

    let _engine = RepositoryEngine::init(backend.clone(), "test-pass-concurrent")
        .await
        .unwrap();

    // Step 1: Process A starts a backup (acquires Shared lock)
    let mut backup_lock_a = RepositoryLock::acquire(backend.clone(), LockType::Shared)
        .await
        .expect("Process A backup lock acquire");

    // Step 2: Process C starts a second backup concurrently (acquires Shared lock)
    // Both backups are permitted concurrently
    let mut backup_lock_c = RepositoryLock::acquire(backend.clone(), LockType::Shared)
        .await
        .expect("Process C concurrent backup lock acquire");

    // Step 3: Process B attempts prune (requests Exclusive lock)
    // Must be rejected because backup locks are held
    let prune_attempt = RepositoryLock::acquire(backend.clone(), LockType::Exclusive).await;
    assert!(
        prune_attempt.is_err(),
        "Prune must fail to acquire Exclusive lock while backups are running"
    );
    let err_msg = prune_attempt.err().unwrap().to_string();
    assert!(
        err_msg.contains("Repository is locked by")
            || err_msg.contains("Concurrent lock acquisition"),
        "Error should indicate repository lock conflict, got: {}",
        err_msg
    );

    // Step 4: Process C finishes backup and releases lock
    backup_lock_c.release().await.unwrap();

    // Prune attempt still fails because Process A is still running
    let prune_attempt2 = RepositoryLock::acquire(backend.clone(), LockType::Exclusive).await;
    assert!(
        prune_attempt2.is_err(),
        "Prune must still fail while Process A backup lock is held"
    );

    // Step 5: Process A finishes backup and releases lock
    backup_lock_a.release().await.unwrap();

    // Step 6: Process B can now successfully acquire Exclusive lock for prune
    let mut prune_lock = RepositoryLock::acquire(backend.clone(), LockType::Exclusive)
        .await
        .expect("Prune lock acquire should succeed after all backups release");

    // While prune is running, new backups must be blocked
    let new_backup_attempt = RepositoryLock::acquire(backend.clone(), LockType::Shared).await;
    assert!(
        new_backup_attempt.is_err(),
        "New backup must be blocked while prune holds Exclusive lock"
    );

    // Prune finishes and releases lock
    prune_lock.release().await.unwrap();

    // New backup can now proceed
    let mut post_prune_backup = RepositoryLock::acquire(backend.clone(), LockType::Shared)
        .await
        .unwrap();
    post_prune_backup.release().await.unwrap();
}
