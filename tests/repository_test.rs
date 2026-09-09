use std::sync::Arc;
use sha2::{Digest, Sha256};
use dumper::cli::CompressionLevel;
use dumper::repository::engine::RepositoryEngine;
use dumper::repository::local::LocalBackend;
use dumper::repository::snapshot::SnapshotMetadata;

#[tokio::test]
async fn test_full_repository_lifecycle_and_deduplication() {
    let temp_dir = tempfile::tempdir().unwrap();
    let backend = Arc::new(LocalBackend::new(temp_dir.path()).await.unwrap());
    let password = "production-grade-password-123";

    // 1. Initialize Repository
    let engine = RepositoryEngine::init(backend.clone(), password).await.unwrap();

    // 2. Prepare Sample Database Data Blocks
    let block_a = b"TABLE users (id INT, email TEXT); INSERT INTO users VALUES (1, 'alice@example.com');";
    let block_b = b"TABLE orders (id INT, user_id INT); INSERT INTO orders VALUES (101, 1);";

    let hash_a = hex::encode(Sha256::digest(block_a));
    let hash_b = hex::encode(Sha256::digest(block_b));

    // 3. Backup 1: Store block A and block B
    let (ref_a1, dedup_a1) = engine.put_chunk(block_a, &hash_a, CompressionLevel::Default).await.unwrap();
    assert!(!dedup_a1, "First upload must not be deduplicated");

    let (ref_b1, dedup_b1) = engine.put_chunk(block_b, &hash_b, CompressionLevel::Default).await.unwrap();
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

    let (ref_a2, dedup_a2) = engine.put_chunk(block_a, &hash_a, CompressionLevel::Default).await.unwrap();
    assert!(dedup_a2, "Identical block A MUST be deduplicated across backups!");
    assert_eq!(ref_a2.stored_size, 0, "Deduplicated block stores 0 new bytes");

    let (ref_c2, dedup_c2) = engine.put_chunk(block_c, &hash_c, CompressionLevel::Default).await.unwrap();
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
    assert_eq!(deleted_blobs, 1, "Only unreferenced block B should be pruned");

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
    let engine = RepositoryEngine::init(backend.clone(), "test-pass").await.unwrap();

    let block = b"Critical live table data";
    let hash = hex::encode(Sha256::digest(block));
    let (blob_ref, _) = engine.put_chunk(block, &hash, CompressionLevel::Default).await.unwrap();

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
    backend.put_object("snapshots/corrupt_snap", b"NOT_VALID_JSON").await.unwrap();

    // prune() MUST abort with an error and must NOT delete any blobs!
    let prune_res = engine.prune().await;
    assert!(prune_res.is_err(), "prune() must fail when any snapshot cannot be parsed");

    // Verify blob is still intact in storage
    let fetched = engine.get_chunk(&hash).await.unwrap();
    assert_eq!(fetched, block);
}

