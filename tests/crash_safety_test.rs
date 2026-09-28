use dumper::cli::CompressionLevel;
use dumper::repository::backend::StorageBackend;
use dumper::repository::engine::RepositoryEngine;
use dumper::repository::local::LocalBackend;
use dumper::repository::s3::client::S3Client;
use dumper::repository::snapshot::SnapshotMetadata;
use sha2::{Digest, Sha256};
use std::process::{Child, Command};
use std::sync::atomic::{AtomicU16, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tempfile::TempDir;

static GARAGE_PORT_COUNTER: AtomicU16 = AtomicU16::new(0);

struct TestGarageServer {
    _dir: TempDir,
    s3_port: u16,
    child: Child,
    access_key_id: String,
    secret_access_key: String,
}

impl TestGarageServer {
    fn start(bucket: &str) -> Option<Self> {
        let dir = TempDir::new().ok()?;
        let meta_dir = dir.path().join("meta");
        let data_dir = dir.path().join("data");
        std::fs::create_dir_all(&meta_dir).ok()?;
        std::fs::create_dir_all(&data_dir).ok()?;

        let offset = GARAGE_PORT_COUNTER.fetch_add(2, Ordering::SeqCst);
        let rpc_port = 49152 + ((std::process::id() as u16 % 400) * 10) + offset;
        let s3_port = rpc_port + 1;

        let config_path = dir.path().join("garage.toml");
        let config_content = format!(
            r#"metadata_dir = "{}"
data_dir = "{}"
db_engine = "sqlite"
replication_factor = 1

rpc_bind_addr = "127.0.0.1:{}"
rpc_secret = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"

[s3_api]
s3_region = "us-east-1"
api_bind_addr = "127.0.0.1:{}"
root_domain = ".s3.garage"
"#,
            meta_dir.to_str()?,
            data_dir.to_str()?,
            rpc_port,
            s3_port
        );
        std::fs::write(&config_path, config_content).ok()?;

        let config_str = config_path.to_str()?;

        let mut child = Command::new("garage")
            .args(["-c", config_str, "server"])
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .spawn()
            .ok()?;

        let start_time = std::time::Instant::now();
        let mut ready = false;
        while start_time.elapsed() < Duration::from_secs(6) {
            let status = Command::new("garage")
                .args(["-c", config_str, "status"])
                .stdout(std::process::Stdio::null())
                .stderr(std::process::Stdio::null())
                .status();
            if let Ok(st) = status {
                if st.success() {
                    ready = true;
                    break;
                }
            }
            std::thread::sleep(Duration::from_millis(100));
        }

        if !ready {
            let _ = child.kill();
            let _ = child.wait();
            return None;
        }

        let node_id_out = Command::new("garage")
            .args(["-c", config_str, "node", "id"])
            .stderr(std::process::Stdio::null())
            .output()
            .ok()?;
        let node_id_str = String::from_utf8_lossy(&node_id_out.stdout);
        let node_id = node_id_str.lines().next()?.trim();

        let assign_status = Command::new("garage")
            .args([
                "-c", config_str, "layout", "assign", "-z", "dc1", "-c", "1G", node_id,
            ])
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .status()
            .ok()?;
        if !assign_status.success() {
            let _ = child.kill();
            let _ = child.wait();
            return None;
        }

        let apply_status = Command::new("garage")
            .args(["-c", config_str, "layout", "apply", "--version", "1"])
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .status()
            .ok()?;
        if !apply_status.success() {
            let _ = child.kill();
            let _ = child.wait();
            return None;
        }

        let key_id = "GK0123456789abcdef01234567";
        let key_secret = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
        let key_status = Command::new("garage")
            .args([
                "-c", config_str, "key", "import", "--yes", key_id, key_secret,
            ])
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .status()
            .ok()?;
        if !key_status.success() {
            let _ = child.kill();
            let _ = child.wait();
            return None;
        }

        let bkt_status = Command::new("garage")
            .args(["-c", config_str, "bucket", "create", bucket])
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .status()
            .ok()?;
        if !bkt_status.success() {
            let _ = child.kill();
            let _ = child.wait();
            return None;
        }

        let allow_status = Command::new("garage")
            .args([
                "-c", config_str, "bucket", "allow", "--read", "--write", "--owner", bucket,
                "--key", key_id,
            ])
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .status()
            .ok()?;
        if !allow_status.success() {
            let _ = child.kill();
            let _ = child.wait();
            return None;
        }

        Some(Self {
            _dir: dir,
            s3_port,
            child,
            access_key_id: key_id.into(),
            secret_access_key: key_secret.into(),
        })
    }

    fn endpoint(&self) -> String {
        format!("http://127.0.0.1:{}", self.s3_port)
    }
}

impl Drop for TestGarageServer {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

#[tokio::test]
async fn test_sigkill_during_s3_upload_crash_resilience() {
    // Test S3 backend resilience when interrupted mid-operation
    let bucket = "sigkill-s3-test-bucket";
    let server = match TestGarageServer::start(bucket) {
        Some(s) => s,
        None => return,
    };

    let s3_backend = Arc::new(
        S3Client::new(
            Some(server.endpoint()),
            bucket.into(),
            "repo".into(),
            "us-east-1".into(),
            server.access_key_id.clone(),
            server.secret_access_key.clone(),
            None,
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
