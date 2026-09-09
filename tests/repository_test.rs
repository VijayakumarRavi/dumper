use dumper::cli::CompressionLevel;
use dumper::repository::backend::StorageBackend;
use dumper::repository::engine::RepositoryEngine;
use dumper::repository::local::LocalBackend;
use dumper::repository::snapshot::SnapshotMetadata;
use sha2::{Digest, Sha256};
use std::sync::Arc;

#[tokio::test]
async fn test_full_repository_lifecycle_and_deduplication() {
    let temp_dir = tempfile::tempdir().unwrap();
    let backend = Arc::new(LocalBackend::new(temp_dir.path()).await.unwrap());
    let password = "production-grade-password-123";

    // 1. Initialize Repository
    let engine = RepositoryEngine::init(backend.clone(), password)
        .await
        .unwrap();

    // 2. Prepare Sample Database Data Blocks
    let block_a =
        b"TABLE users (id INT, email TEXT); INSERT INTO users VALUES (1, 'alice@example.com');";
    let block_b = b"TABLE orders (id INT, user_id INT); INSERT INTO orders VALUES (101, 1);";

    let hash_a = hex::encode(Sha256::digest(block_a));
    let hash_b = hex::encode(Sha256::digest(block_b));

    // 3. Backup 1: Store block A and block B
    let (ref_a1, dedup_a1) = engine
        .put_chunk(block_a, &hash_a, CompressionLevel::Default)
        .await
        .unwrap();
    assert!(!dedup_a1, "First upload must not be deduplicated");

    let (ref_b1, dedup_b1) = engine
        .put_chunk(block_b, &hash_b, CompressionLevel::Default)
        .await
        .unwrap();
    assert!(!dedup_b1, "First upload must not be deduplicated");

    let snap1 = SnapshotMetadata {
        id: "snap0001".into(),
        full_id: "full_snapshot_hash_0001".into(),
        format_version: 1,
        dumper_version: "0.1.0".into(),
        engine: "postgresql".into(),
        database: "production".into(),
        server_version: "17.4".into(),
        started_at: chrono::Utc::now(),
        completed_at: chrono::Utc::now(),
        duration_seconds: 2,
        logical_bytes: (block_a.len() + block_b.len()) as u64,
        stored_bytes: ref_a1.stored_size + ref_b1.stored_size,
        deduplicated_bytes: 0,
        table_count: 2,
        compression: "default".into(),
        tag: Some("v1-initial".into()),
        blobs: vec![ref_a1, ref_b1],
    };
    engine.commit_snapshot(&snap1).await.unwrap();

    // 4. Backup 2: Store block A again (unchanged table) and block C (new table)
    let block_c = b"TABLE audit_log (event TEXT); INSERT INTO audit_log VALUES ('user_created');";
    let hash_c = hex::encode(Sha256::digest(block_c));

    let (ref_a2, dedup_a2) = engine
        .put_chunk(block_a, &hash_a, CompressionLevel::Default)
        .await
        .unwrap();
    assert!(
        dedup_a2,
        "Identical block A MUST be deduplicated across backups!"
    );
    assert_eq!(
        ref_a2.stored_size, 0,
        "Deduplicated block stores 0 new bytes"
    );

    let (ref_c2, dedup_c2) = engine
        .put_chunk(block_c, &hash_c, CompressionLevel::Default)
        .await
        .unwrap();
    assert!(!dedup_c2, "New block C must be stored");

    let snap2 = SnapshotMetadata {
        id: "snap0002".into(),
        full_id: "full_snapshot_hash_0002".into(),
        format_version: 1,
        dumper_version: "0.1.0".into(),
        engine: "postgresql".into(),
        database: "production".into(),
        server_version: "17.4".into(),
        started_at: chrono::Utc::now(),
        completed_at: chrono::Utc::now(),
        duration_seconds: 1,
        logical_bytes: (block_a.len() + block_c.len()) as u64,
        stored_bytes: ref_c2.stored_size,
        deduplicated_bytes: block_a.len() as u64,
        table_count: 2,
        compression: "default".into(),
        tag: Some("v2-incremental-state".into()),
        blobs: vec![ref_a2, ref_c2],
    };
    engine.commit_snapshot(&snap2).await.unwrap();

    // 5. Verification
    let snapshots = engine.list_snapshots().await.unwrap();
    assert_eq!(snapshots.len(), 2);

    let v1 = engine.verify_snapshot(&snap1).await.unwrap();
    assert_eq!(v1, 2);

    let v2 = engine.verify_snapshot(&snap2).await.unwrap();
    assert_eq!(v2, 2);

    // 6. Delete snapshot 1 (forget) and Prune
    engine.delete_snapshot(&snap1.id).await.unwrap();
    let remaining_snaps = engine.list_snapshots().await.unwrap();
    assert_eq!(remaining_snaps.len(), 1);
    assert_eq!(remaining_snaps[0].id, "snap0002");

    // Block B was only in snapshot 1, so prune should delete block B, keeping block A and C
    let (deleted_blobs, _deleted_bytes) = engine.prune().await.unwrap();
    assert_eq!(
        deleted_blobs, 1,
        "Only unreferenced block B should be pruned"
    );

    // Verify snapshot 2 is still 100% intact after prune!
    let v2_after_prune = engine.verify_snapshot(&snap2).await.unwrap();
    assert_eq!(v2_after_prune, 2);

    // Ensure block A can still be decrypted and read
    let retrieved_a = engine.get_chunk(&hash_a).await.unwrap();
    assert_eq!(retrieved_a, block_a);

    // Ensure block C can still be decrypted and read
    let retrieved_c = engine.get_chunk(&hash_c).await.unwrap();
    assert_eq!(retrieved_c, block_c);
}

#[tokio::test]
async fn test_prune_safety_on_corrupt_or_unreadable_snapshot() {
    let temp_dir = tempfile::tempdir().unwrap();
    let backend = Arc::new(LocalBackend::new(temp_dir.path()).await.unwrap());
    let engine = RepositoryEngine::init(backend.clone(), "test-pass")
        .await
        .unwrap();

    let block = b"Critical live table data";
    let hash = hex::encode(Sha256::digest(block));
    let (blob_ref, _) = engine
        .put_chunk(block, &hash, CompressionLevel::Default)
        .await
        .unwrap();

    let snap = SnapshotMetadata {
        id: "snap_valid".into(),
        full_id: "snap_valid_full".into(),
        format_version: 1,
        dumper_version: "0.1.0".into(),
        engine: "postgresql".into(),
        database: "prod".into(),
        server_version: "17".into(),
        started_at: chrono::Utc::now(),
        completed_at: chrono::Utc::now(),
        duration_seconds: 1,
        logical_bytes: block.len() as u64,
        stored_bytes: blob_ref.stored_size,
        deduplicated_bytes: 0,
        table_count: 1,
        compression: "default".into(),
        tag: None,
        blobs: vec![blob_ref],
    };
    engine.commit_snapshot(&snap).await.unwrap();

    // Intentionally create an unreadable / corrupted snapshot in the snapshots directory
    use dumper::repository::backend::StorageBackend;
    backend
        .put_object("snapshots/corrupt_snap", b"NOT_VALID_JSON")
        .await
        .unwrap();

    // prune() MUST abort with an error and must NOT delete any blobs!
    let prune_res = engine.prune().await;
    assert!(
        prune_res.is_err(),
        "prune() must fail when any snapshot cannot be parsed"
    );

    // Verify blob is still intact in storage
    let fetched = engine.get_chunk(&hash).await.unwrap();
    assert_eq!(fetched, block);
}

#[tokio::test]
async fn test_prune_does_not_download_blob_payloads() {
    use dumper::repository::backend::StorageBackend;
    use std::sync::atomic::{AtomicUsize, Ordering};

    struct CountingBackend {
        inner: LocalBackend,
        blob_get_object_calls: AtomicUsize,
        blob_get_object_size_calls: AtomicUsize,
    }

    impl StorageBackend for CountingBackend {
        async fn put_object<'a>(
            &'a self,
            path: &'a str,
            data: &'a [u8],
        ) -> Result<(), dumper::error::DumperError> {
            self.inner.put_object(path, data).await
        }
        async fn get_object<'a>(
            &'a self,
            path: &'a str,
        ) -> Result<Vec<u8>, dumper::error::DumperError> {
            if path.starts_with("blobs/") {
                self.blob_get_object_calls.fetch_add(1, Ordering::SeqCst);
            }
            self.inner.get_object(path).await
        }
        async fn get_object_size<'a>(
            &'a self,
            path: &'a str,
        ) -> Result<u64, dumper::error::DumperError> {
            if path.starts_with("blobs/") {
                self.blob_get_object_size_calls
                    .fetch_add(1, Ordering::SeqCst);
            }
            self.inner.get_object_size(path).await
        }
        async fn object_exists<'a>(
            &'a self,
            path: &'a str,
        ) -> Result<bool, dumper::error::DumperError> {
            self.inner.object_exists(path).await
        }
        async fn delete_object<'a>(
            &'a self,
            path: &'a str,
        ) -> Result<(), dumper::error::DumperError> {
            self.inner.delete_object(path).await
        }
        async fn list_objects<'a>(
            &'a self,
            prefix: &'a str,
        ) -> Result<Vec<String>, dumper::error::DumperError> {
            self.inner.list_objects(prefix).await
        }
        async fn count_temp_files(&self) -> Result<usize, dumper::error::DumperError> {
            self.inner.count_temp_files().await
        }
        async fn cleanup_temp_files(&self) -> Result<usize, dumper::error::DumperError> {
            self.inner.cleanup_temp_files().await
        }
    }

    let temp_dir = tempfile::tempdir().unwrap();
    let local = LocalBackend::new(temp_dir.path()).await.unwrap();
    let counting_backend = Arc::new(CountingBackend {
        inner: local,
        blob_get_object_calls: AtomicUsize::new(0),
        blob_get_object_size_calls: AtomicUsize::new(0),
    });

    let engine = RepositoryEngine::init(counting_backend.clone(), "test-pass")
        .await
        .unwrap();

    // Store a referenced chunk and commit snapshot
    let (ref_1, _) = engine
        .put_chunk(b"live chunk", "hash_live", CompressionLevel::Default)
        .await
        .unwrap();
    let snap = SnapshotMetadata {
        id: "snap1".into(),
        full_id: "snap1_full".into(),
        format_version: 1,
        dumper_version: "0.1.0".into(),
        engine: "test".into(),
        database: "test".into(),
        server_version: "1.0".into(),
        started_at: chrono::Utc::now(),
        completed_at: chrono::Utc::now(),
        duration_seconds: 1,
        logical_bytes: 100,
        stored_bytes: 100,
        deduplicated_bytes: 0,
        table_count: 1,
        compression: "default".into(),
        tag: None,
        blobs: vec![ref_1],
    };
    engine.commit_snapshot(&snap).await.unwrap();

    // Store an orphaned / unreferenced chunk
    let (ref_orphan, _) = engine
        .put_chunk(b"orphan chunk", "hash_orphan", CompressionLevel::Default)
        .await
        .unwrap();
    assert!(ref_orphan.stored_size > 0);

    // Reset counters before prune
    counting_backend
        .blob_get_object_calls
        .store(0, Ordering::SeqCst);
    counting_backend
        .blob_get_object_size_calls
        .store(0, Ordering::SeqCst);

    // Run prune
    let (deleted_count, deleted_bytes) = engine.prune().await.unwrap();
    assert_eq!(
        deleted_count, 1,
        "Exactly one orphaned blob should be pruned"
    );
    assert!(
        deleted_bytes > 0,
        "Deleted bytes should be accurately recorded"
    );

    // VERIFICATION: get_object must NOT have been called for any blob!
    assert_eq!(
        counting_backend
            .blob_get_object_calls
            .load(Ordering::SeqCst),
        0,
        "prune() must NEVER download blob payloads using get_object()!"
    );
    assert_eq!(
        counting_backend
            .blob_get_object_size_calls
            .load(Ordering::SeqCst),
        1,
        "prune() should use lightweight get_object_size() instead"
    );
}

#[tokio::test]
async fn test_abandoned_temp_files_cleanup_and_detection() {
    let temp_dir = tempfile::tempdir().unwrap();
    let backend = Arc::new(LocalBackend::new(temp_dir.path()).await.unwrap());
    let engine = RepositoryEngine::init(backend.clone(), "test-pass")
        .await
        .unwrap();

    // 1. Manually create abandoned .tmp_* files inside blobs/ and snapshots/ simulating crashed operations
    let blobs_dir = temp_dir.path().join("blobs");
    let snaps_dir = temp_dir.path().join("snapshots");
    tokio::fs::create_dir_all(&blobs_dir).await.unwrap();
    tokio::fs::create_dir_all(&snaps_dir).await.unwrap();

    let fake_tmp1 = blobs_dir.join(".tmp_1111_2222");
    let fake_tmp2 = snaps_dir.join(".tmp_3333_4444");
    tokio::fs::write(&fake_tmp1, b"abandoned partial blob data")
        .await
        .unwrap();
    tokio::fs::write(&fake_tmp2, b"abandoned partial snapshot data")
        .await
        .unwrap();

    // 2. Detection: backend count_temp_files must find both
    assert_eq!(backend.count_temp_files().await.unwrap(), 2);

    // 3. Prune: pruning the repository must clean up abandoned temporary files
    let (_deleted_blobs, _freed) = engine.prune().await.unwrap();

    // 4. Verify that temp files are now gone
    assert_eq!(backend.count_temp_files().await.unwrap(), 0);
    assert!(!fake_tmp1.exists());
    assert!(!fake_tmp2.exists());
}
