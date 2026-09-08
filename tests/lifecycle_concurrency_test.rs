use dumper::repository::engine::RepositoryEngine;
use dumper::repository::lock::{LockType, RepositoryLock};
use tempfile::tempdir;

#[tokio::test]
async fn test_lock_exclusion() {
    let dir = tempdir().unwrap();
    let backend = std::sync::Arc::new(dumper::repository::local::LocalBackend::new(dir.path().to_str().unwrap()).await.unwrap());
    
    // We just test the repository lock, we don't strictly need engine initialization
    // but we can initialize it to be safe
    let _engine = RepositoryEngine::init(
        backend.clone(),
        "password123",
    ).await.unwrap();

    // Acquire shared lock
    let lock1 = RepositoryLock::acquire(backend.as_ref(), LockType::Shared).await.unwrap();

    // Second shared lock should succeed
    let lock2 = RepositoryLock::acquire(backend.as_ref(), LockType::Shared).await.unwrap();

    // Exclusive lock should fail
    let lock3 = RepositoryLock::acquire(backend.as_ref(), LockType::Exclusive).await;
    assert!(lock3.is_err());
    let err_str = match lock3 {
        Err(e) => e.to_string(),
        Ok(_) => panic!("Expected error"),
    };
    assert!(err_str.contains("Repository is locked by") || err_str.contains("Concurrent lock acquisition"));

    lock1.release(backend.as_ref()).await.unwrap();
    lock2.release(backend.as_ref()).await.unwrap();

    // Now exclusive lock should succeed
    let lock4 = RepositoryLock::acquire(backend.as_ref(), LockType::Exclusive).await.unwrap();
    
    // Shared lock should fail now
    let lock5 = RepositoryLock::acquire(backend.as_ref(), LockType::Shared).await;
    assert!(lock5.is_err());
    
    lock4.release(backend.as_ref()).await.unwrap();
}
