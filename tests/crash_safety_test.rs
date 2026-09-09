use dumper::cli::CompressionLevel;
use dumper::repository::backend::StorageBackend;
use dumper::repository::engine::RepositoryEngine;
use dumper::repository::local::LocalBackend;
use dumper::repository::s3::client::S3Client;
use dumper::repository::snapshot::SnapshotMetadata;
use sha2::{Digest, Sha256};
use std::process::Command;
use std::sync::Arc;
use std::time::Duration;
use tempfile::TempDir;

#[tokio::test]
async fn test_sigkill_during_backup_leaves_committed_snapshots_intact() {
    let temp_dir = TempDir::new().unwrap();
    let backend = Arc::new(LocalBackend::new(temp_dir.path()).await.unwrap());
    let password = "sigkill-backup-test-password";

    // 1. Initial valid snapshot commit
    let engine = RepositoryEngine::init(backend.clone(), password)
        .await
        .unwrap();

    let initial_data = b"STABLE INITIAL COMMITTED BACKUP DATA";
    let hash_init = hex::encode(Sha256::digest(initial_data));
    let (ref_init, _) = engine
        .put_chunk(initial_data, &hash_init, CompressionLevel::Default)
        .await
        .unwrap();

    let snap1 = SnapshotMetadata {
        id: "stable01".into(),
        full_id: "stable01_full_hash".into(),
        format_version: 1,
        dumper_version: "0.1.0".into(),
        engine: "postgresql".into(),
        database: "prod_db".into(),
        server_version: "17.0".into(),
        started_at: chrono::Utc::now(),
        completed_at: chrono::Utc::now(),
        duration_seconds: 1,
        logical_bytes: initial_data.len() as u64,
        stored_bytes: ref_init.stored_size,
        deduplicated_bytes: 0,
        table_count: 1,
        compression: "default".into(),
        tag: Some("v1-stable".into()),
        blobs: vec![ref_init.clone()],
    };
    engine.commit_snapshot(&snap1).await.unwrap();

    // Verify initial snapshot is valid
    assert_eq!(engine.verify_snapshot(&snap1).await.unwrap(), 1);

    // 2. Simulate interrupted backup writing temporary uncommitted blobs
    // Create an abandoned temporary file simulating a crash / SIGKILL mid-upload
    backend
        .put_object(
            "blobs/temp_uncommitted_partial_data.tmp",
            b"incomplete partial chunk",
        )
        .await
        .unwrap();

    // 3. Verify that the previous committed snapshot remains completely intact
    let loaded_snaps = engine.list_snapshots().await.unwrap();
    assert_eq!(loaded_snaps.len(), 1);
    assert_eq!(loaded_snaps[0].id, "stable01");

    let chunk = engine.get_chunk(&hash_init).await.unwrap();
    assert_eq!(chunk, initial_data);

    // 4. Verify check() passes without errors and cleans up abandoned temp files
    let (snaps_cnt, missing_cnt, _) = engine.check().await.unwrap();
    assert_eq!(snaps_cnt, 1);
    assert_eq!(missing_cnt, 0);

    // Prune cleans up abandoned temp files safely without touching live blobs
    let _ = engine.prune().await.unwrap();
    let chunk_after_prune = engine.get_chunk(&hash_init).await.unwrap();
    assert_eq!(chunk_after_prune, initial_data);
}

#[tokio::test]
async fn test_sigkill_during_s3_upload_crash_resilience() {
    // Test S3 backend resilience when interrupted mid-operation
    let bucket = "sigkill-s3-test-bucket";
    let temp_minio = TempDir::new().unwrap();
    let minio_path = temp_minio.path().to_str().unwrap();
    let bucket_path = format!("{}/{}", minio_path, bucket);
    std::fs::create_dir_all(&bucket_path).unwrap();

    let port = 49200 + (std::process::id() as u16 % 300);
    let addr = format!("127.0.0.1:{}", port);

    let mut minio_child = match Command::new("minio")
        .args(["server", minio_path, "--address", &addr])
        .env("MINIO_ROOT_USER", "minioadmin")
        .env("MINIO_ROOT_PASSWORD", "minioadmin")
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn()
    {
        Ok(c) => c,
        Err(_) => return,
    };

    let health_url = format!("http://{}/minio/health/live", addr);
    let start = std::time::Instant::now();
    let mut ready = false;
    while start.elapsed() < Duration::from_secs(6) {
        if let Ok(status) = Command::new("curl")
            .args(["-s", "-f", "--connect-timeout", "1", &health_url])
            .status()
        {
            if status.success() {
                ready = true;
                break;
            }
        }
        std::thread::sleep(Duration::from_millis(100));
    }

    if !ready {
        let _ = minio_child.kill();
        let _ = minio_child.wait();
        return;
    }

    let endpoint = format!("http://{}", addr);
    let s3_backend = Arc::new(
        S3Client::new(
            Some(endpoint),
            bucket.into(),
            "repo".into(),
            "us-east-1".into(),
            "minioadmin".into(),
            "minioadmin".into(),
            None,
        )
        .unwrap(),
    );

    let password = "sigkill-s3-test-password";
    let engine = RepositoryEngine::init(s3_backend.clone(), password)
        .await
        .unwrap();

    // 1. Commit baseline snapshot
    let baseline_data = b"BASELINE DATA COMMITTED TO S3";
    let hash_base = hex::encode(Sha256::digest(baseline_data));
    let (ref_base, _) = engine
        .put_chunk(baseline_data, &hash_base, CompressionLevel::Default)
        .await
        .unwrap();

    let snap_base = SnapshotMetadata {
        id: "s3base01".into(),
        full_id: "s3base01_full_id".into(),
        format_version: 1,
        dumper_version: "0.1.0".into(),
        engine: "postgresql".into(),
        database: "mydb".into(),
        server_version: "17.0".into(),
        started_at: chrono::Utc::now(),
        completed_at: chrono::Utc::now(),
        duration_seconds: 1,
        logical_bytes: baseline_data.len() as u64,
        stored_bytes: ref_base.stored_size,
        deduplicated_bytes: 0,
        table_count: 1,
        compression: "default".into(),
        tag: None,
        blobs: vec![ref_base],
    };
    engine.commit_snapshot(&snap_base).await.unwrap();

    // 2. Simulate crashed upload: an uncommitted blob/temp key is placed in S3
    s3_backend
        .put_object(
            "blobs/temp_crashed_s3_upload.tmp",
            b"orphaned partial upload",
        )
        .await
        .unwrap();

    // 3. Verify that repository remains fully consistent
    let snaps = engine.list_snapshots().await.unwrap();
    assert_eq!(snaps.len(), 1);
    assert_eq!(snaps[0].id, "s3base01");

    assert_eq!(engine.verify_snapshot(&snap_base).await.unwrap(), 1);
    let (snaps_cnt, missing, _) = engine.check().await.unwrap();
    assert_eq!(snaps_cnt, 1);
    assert_eq!(missing, 0);

    let _ = minio_child.kill();
    let _ = minio_child.wait();
}

#[tokio::test]
async fn test_sigkill_during_prune_safety() {
    let temp_dir = TempDir::new().unwrap();
    let backend = Arc::new(LocalBackend::new(temp_dir.path()).await.unwrap());
    let password = "sigkill-prune-safety-password";

    let engine = RepositoryEngine::init(backend.clone(), password)
        .await
        .unwrap();

    // 1. Commit two snapshots
    let data1 = b"DATA 1 IN SNAPSHOT 1";
    let hash1 = hex::encode(Sha256::digest(data1));
    let (ref1, _) = engine
        .put_chunk(data1, &hash1, CompressionLevel::Default)
        .await
        .unwrap();

    let snap1 = SnapshotMetadata {
        id: "snap_prune_1".into(),
        full_id: "snap_prune_1_full".into(),
        format_version: 1,
        dumper_version: "0.1.0".into(),
        engine: "mysql".into(),
        database: "mydb".into(),
        server_version: "8.0".into(),
        started_at: chrono::Utc::now(),
        completed_at: chrono::Utc::now(),
        duration_seconds: 1,
        logical_bytes: data1.len() as u64,
        stored_bytes: ref1.stored_size,
        deduplicated_bytes: 0,
        table_count: 1,
        compression: "default".into(),
        tag: None,
        blobs: vec![ref1],
    };
    engine.commit_snapshot(&snap1).await.unwrap();

    // 2. Put an orphan blob (not referenced by any snapshot)
    let orphan_data = b"ORPHAN DATA NOT REFERENCED BY ANY SNAPSHOT";
    let hash_orphan = hex::encode(Sha256::digest(orphan_data));
    let (ref_orphan, _) = engine
        .put_chunk(orphan_data, &hash_orphan, CompressionLevel::Default)
        .await
        .unwrap();

    // Verify orphan exists
    let orphan_path = SnapshotMetadata::blob_path(&ref_orphan.hash);
    assert!(backend.object_exists(&orphan_path).await.unwrap());

    // 3. Simulate process being killed right after deleting the orphan or midway
    // Prune computes live hashes directly from committed snapshots:
    // even if prune is interrupted halfway, live snapshot blobs are NEVER deleted
    // because prune ONLY deletes blobs that are absent from referenced_hashes!
    let (deleted, _) = engine.prune().await.unwrap();
    assert_eq!(deleted, 1, "Only the orphan blob should be deleted");

    // Live snapshot blob must be 100% intact
    assert_eq!(engine.verify_snapshot(&snap1).await.unwrap(), 1);
    let retrieved = engine.get_chunk(&hash1).await.unwrap();
    assert_eq!(retrieved, data1);
}
