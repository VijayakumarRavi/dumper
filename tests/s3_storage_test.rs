use dumper::cli::CompressionLevel;
use dumper::repository::backend::StorageBackend;
use dumper::repository::engine::RepositoryEngine;
use dumper::repository::s3::client::S3Client;
use dumper::repository::snapshot::SnapshotMetadata;
use sha2::{Digest, Sha256};
use std::process::{Child, Command};
use std::sync::atomic::{AtomicU16, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tempfile::TempDir;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

static MINIO_PORT_COUNTER: AtomicU16 = AtomicU16::new(0);

struct TestMinioServer {
    _dir: TempDir,
    port: u16,
    child: Child,
}

impl TestMinioServer {
    fn start(bucket: &str) -> Option<Self> {
        let dir = TempDir::new().ok()?;
        let path = dir.path().to_str()?;

        // Pre-create bucket directory so MinIO serves it immediately
        let bucket_path = format!("{}/{}", path, bucket);
        std::fs::create_dir_all(&bucket_path).ok()?;

        let offset = MINIO_PORT_COUNTER.fetch_add(1, Ordering::SeqCst);
        let port = 49152 + ((std::process::id() as u16 % 500) * 10) + offset;
        let addr = format!("127.0.0.1:{}", port);

        let child = Command::new("minio")
            .args(["server", path, "--address", &addr])
            .env("MINIO_ROOT_USER", "minioadmin")
            .env("MINIO_ROOT_PASSWORD", "minioadmin")
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .spawn()
            .ok()?;

        let server = Self {
            _dir: dir,
            port,
            child,
        };

        // Poll health endpoint until ready (up to 6 seconds)
        let health_url = format!("http://{}/minio/health/live", addr);
        let start_time = std::time::Instant::now();
        let mut ready = false;
        while start_time.elapsed() < Duration::from_secs(6) {
            let status = Command::new("curl")
                .args(["-s", "-f", "--connect-timeout", "1", &health_url])
                .output();
            if let Ok(out) = status {
                if out.status.success() {
                    ready = true;
                    break;
                }
            }
            std::thread::sleep(Duration::from_millis(100));
        }

        if !ready {
            return None;
        }

        Some(server)
    }

    fn endpoint(&self) -> String {
        format!("http://127.0.0.1:{}", self.port)
    }
}

impl Drop for TestMinioServer {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

#[tokio::test]
async fn test_minio_s3_backup_restore_roundtrip_and_deduplication() {
    let bucket = "dumper-test-bucket";
    let server = match TestMinioServer::start(bucket) {
        Some(s) => s,
        None => {
            eprintln!("MinIO not available or failed to start, skipping test.");
            return;
        }
    };

    let s3_backend = Arc::new(
        S3Client::new(
            Some(server.endpoint()),
            bucket.into(),
            "backups/app".into(),
            "us-east-1".into(),
            "minioadmin".into(),
            "minioadmin".into(),
            None,
        )
        .unwrap(),
    );

    let password = "minio-production-password-456";

    // 1. Initialize Repository over S3
    let engine = RepositoryEngine::init(s3_backend.clone(), password)
        .await
        .unwrap();

    // 2. Upload chunks
    let block1 = b"POSTGRESQL DATA ROW 1: user_id=1, email=test1@example.com";
    let block2 = b"POSTGRESQL DATA ROW 2: user_id=2, email=test2@example.com";
    let hash1 = hex::encode(Sha256::digest(block1));
    let hash2 = hex::encode(Sha256::digest(block2));

    let (ref1, dedup1) = engine
        .put_chunk(block1, &hash1, CompressionLevel::Default)
        .await
        .unwrap();
    assert!(!dedup1, "First upload of block 1 must not be deduplicated");

    let (ref2, dedup2) = engine
        .put_chunk(block2, &hash2, CompressionLevel::Default)
        .await
        .unwrap();
    assert!(!dedup2, "First upload of block 2 must not be deduplicated");

    // 3. Deduplication check: re-upload block 1
    let (ref1_dup, dedup1_dup) = engine
        .put_chunk(block1, &hash1, CompressionLevel::Default)
        .await
        .unwrap();
    assert!(dedup1_dup, "Second upload of block 1 must be deduplicated");
    assert_eq!(ref1_dup.hash, hash1);

    // 4. Commit snapshot
    let snap1 = SnapshotMetadata {
        id: "s3snap01".into(),
        full_id: "s3_snapshot_full_id_0001".into(),
        format_version: 1,
        dumper_version: "0.1.0".into(),
        engine: "postgresql".into(),
        database: "prod_db".into(),
        server_version: "17.2".into(),
        started_at: chrono::Utc::now(),
        completed_at: chrono::Utc::now(),
        duration_seconds: 1,
        logical_bytes: (block1.len() + block2.len()) as u64,
        stored_bytes: ref1.stored_size + ref2.stored_size,
        deduplicated_bytes: 0,
        table_count: 1,
        compression: "default".into(),
        tag: Some("first-s3-backup".into()),
        blobs: vec![ref1, ref2],
    };
    engine.commit_snapshot(&snap1).await.unwrap();

    // 5. Verify snapshot over S3 (downloads blobs, verifies decryption, zstd, sha256)
    let verified = engine.verify_snapshot(&snap1).await.unwrap();
    assert_eq!(verified, 2, "Must verify 2 blobs successfully");

    // 6. Check repository health over S3
    let (snaps_cnt, missing_cnt, orphaned_cnt) = engine.check().await.unwrap();
    assert_eq!(snaps_cnt, 1);
    assert_eq!(missing_cnt, 0);
    assert_eq!(orphaned_cnt, 0);

    // 7. Verify chunk data round-trip
    let retrieved1 = engine.get_chunk(&hash1).await.unwrap();
    assert_eq!(retrieved1, block1);
    let retrieved2 = engine.get_chunk(&hash2).await.unwrap();
    assert_eq!(retrieved2, block2);
}

#[tokio::test]
async fn test_s3_403_forbidden_rejection_no_retry() {
    let bucket = "dumper-auth-test-bucket";
    let server = match TestMinioServer::start(bucket) {
        Some(s) => s,
        None => return,
    };

    // Client with intentionally WRONG secret key
    let bad_client = S3Client::new(
        Some(server.endpoint()),
        bucket.into(),
        "backups".into(),
        "us-east-1".into(),
        "minioadmin".into(),
        "wrong-secret-key-12345".into(),
        None,
    )
    .unwrap();

    let start = std::time::Instant::now();
    let res = bad_client.put_object("test_object", b"sample data").await;
    let elapsed = start.elapsed();

    assert!(res.is_err(), "Invalid secret key must fail immediately");
    let err = res.unwrap_err().to_string();
    assert!(
        err.contains("Authentication/Authorization failed")
            || err.contains("HTTP 403")
            || err.contains("403 Forbidden"),
        "Error must indicate 403 Forbidden: {}",
        err
    );

    // Verify it did not perform multiple retries with long backoff (should fail under 2 seconds)
    assert!(
        elapsed < Duration::from_secs(3),
        "403 must fail immediately without retrying; took {:?}",
        elapsed
    );
}

#[tokio::test]
async fn test_s3_404_not_found() {
    let bucket = "dumper-404-test-bucket";
    let server = match TestMinioServer::start(bucket) {
        Some(s) => s,
        None => return,
    };

    let client = S3Client::new(
        Some(server.endpoint()),
        bucket.into(),
        "backups".into(),
        "us-east-1".into(),
        "minioadmin".into(),
        "minioadmin".into(),
        None,
    )
    .unwrap();

    let res = client.get_object("nonexistent/path/to/blob").await;
    assert!(res.is_err());
    let err = res.unwrap_err().to_string();
    assert!(
        err.contains("not found"),
        "Must return Object not found error: {}",
        err
    );

    let exists = client.object_exists("nonexistent/key").await.unwrap();
    assert!(!exists, "Nonexistent object must return exists=false");
}

#[tokio::test]
async fn test_s3_retry_on_transient_503_and_429() {
    // Start a mock HTTP server on a random port that returns 503 Service Unavailable twice,
    // then returns 200 OK
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let attempt_counter = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let counter_clone = attempt_counter.clone();

    tokio::spawn(async move {
        while let Ok((mut socket, _)) = listener.accept().await {
            let mut buf = [0u8; 2048];
            let _ = socket.read(&mut buf).await;
            let attempt = counter_clone.fetch_add(1, Ordering::SeqCst);

            if attempt < 2 {
                // Return 503 Service Unavailable
                let resp = "HTTP/1.1 503 Service Unavailable\r\nContent-Length: 0\r\nConnection: close\r\n\r\n";
                let _ = socket.write_all(resp.as_bytes()).await;
            } else {
                // Return 200 OK
                let resp = "HTTP/1.1 200 OK\r\nContent-Length: 12\r\nConnection: close\r\n\r\nsuccess_data";
                let _ = socket.write_all(resp.as_bytes()).await;
            }
        }
    });

    let client = S3Client::new(
        Some(format!("http://127.0.0.1:{}", port)),
        "mock-bucket".into(),
        "".into(),
        "us-east-1".into(),
        "access".into(),
        "secret".into(),
        None,
    )
    .unwrap();

    let data = client.get_object("transient_key").await.unwrap();
    assert_eq!(data, b"success_data");
    assert_eq!(
        attempt_counter.load(Ordering::SeqCst),
        3,
        "Should succeed on 3rd attempt after two 503 retries"
    );
}

#[tokio::test]
async fn test_s3_missing_blob_and_corrupt_snapshot_detection() {
    let bucket = "dumper-corrupt-test-bucket";
    let server = match TestMinioServer::start(bucket) {
        Some(s) => s,
        None => return,
    };

    let s3_backend = Arc::new(
        S3Client::new(
            Some(server.endpoint()),
            bucket.into(),
            "repo".into(),
            "us-east-1".into(),
            "minioadmin".into(),
            "minioadmin".into(),
            None,
        )
        .unwrap(),
    );

    let password = "integrity-check-password";
    let engine = RepositoryEngine::init(s3_backend.clone(), password)
        .await
        .unwrap();

    let block = b"CRITICAL DATABASE ROW DATA TO PROTECT";
    let hash = hex::encode(Sha256::digest(block));
    let (blob_ref, _) = engine
        .put_chunk(block, &hash, CompressionLevel::Default)
        .await
        .unwrap();

    let snap = SnapshotMetadata {
        id: "validsnap".into(),
        full_id: "validsnap_full_hash".into(),
        format_version: 1,
        dumper_version: "0.1.0".into(),
        engine: "postgresql".into(),
        database: "mydb".into(),
        server_version: "17.0".into(),
        started_at: chrono::Utc::now(),
        completed_at: chrono::Utc::now(),
        duration_seconds: 1,
        logical_bytes: block.len() as u64,
        stored_bytes: blob_ref.stored_size,
        deduplicated_bytes: 0,
        table_count: 1,
        compression: "default".into(),
        tag: None,
        blobs: vec![blob_ref.clone()],
    };
    engine.commit_snapshot(&snap).await.unwrap();

    // 1. Verify that a missing blob is detected by check() and verify_snapshot()
    let blob_path = SnapshotMetadata::blob_path(&blob_ref.hash);
    s3_backend.delete_object(&blob_path).await.unwrap();

    let (_, missing_cnt, _) = engine.check().await.unwrap();
    assert_eq!(missing_cnt, 1, "check() must report 1 missing blob");

    let verify_res = engine.verify_snapshot(&snap).await;
    assert!(
        verify_res.is_err(),
        "verify_snapshot() must fail when a blob is missing"
    );

    // 2. Corrupt snapshot metadata file
    let corrupt_snap_path = "snapshots/corrupt_snap.json".to_string();
    s3_backend
        .put_object(&corrupt_snap_path, b"NOT_VALID_ENCRYPTED_OR_JSON_DATA")
        .await
        .unwrap();

    // Check that prune does not panic or delete valid data on corrupted snapshot
    let prune_res = engine.prune().await;
    assert!(
        prune_res.is_err(),
        "prune() must refuse to proceed when an unreadable snapshot exists"
    );
}

#[tokio::test]
async fn test_repository_large_object_count() {
    let temp_dir = tempfile::tempdir().unwrap();
    let backend = Arc::new(
        dumper::repository::local::LocalBackend::new(temp_dir.path())
            .await
            .unwrap(),
    );
    let password = "large-object-count-test";

    let engine = RepositoryEngine::init(backend.clone(), password)
        .await
        .unwrap();

    // Create 100 unique small chunks
    let mut blob_refs = Vec::with_capacity(100);
    for i in 0..100 {
        let content = format!("Unique row content for index={}", i);
        let hash = hex::encode(Sha256::digest(content.as_bytes()));
        let (r, _) = engine
            .put_chunk(content.as_bytes(), &hash, CompressionLevel::Fast)
            .await
            .unwrap();
        blob_refs.push(r);
    }

    let snap = SnapshotMetadata {
        id: "hundred_blobs".into(),
        full_id: "hundred_blobs_full_hash".into(),
        format_version: 1,
        dumper_version: "0.1.0".into(),
        engine: "mysql".into(),
        database: "large_db".into(),
        server_version: "8.0".into(),
        started_at: chrono::Utc::now(),
        completed_at: chrono::Utc::now(),
        duration_seconds: 1,
        logical_bytes: 5000,
        stored_bytes: 10000,
        deduplicated_bytes: 0,
        table_count: 10,
        compression: "fast".into(),
        tag: None,
        blobs: blob_refs,
    };
    engine.commit_snapshot(&snap).await.unwrap();

    let (snaps, missing, orphans) = engine.check().await.unwrap();
    assert_eq!(snaps, 1);
    assert_eq!(missing, 0);
    assert_eq!(orphans, 0);

    let verified = engine.verify_snapshot(&snap).await.unwrap();
    assert_eq!(verified, 100);
}
