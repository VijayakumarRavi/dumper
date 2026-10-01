use crate::cli::CompressionLevel;
use crate::compression::{compress_data, decompress_data};
use crate::crypto::aead::{decrypt_blob, encrypt_blob};
use crate::error::DumperError;
use crate::repository::backend::StorageBackend;
use crate::repository::config::{RepositoryConfig, CONFIG_FILE_PATH};
use crate::repository::snapshot::{BlobReference, SnapshotMetadata};
use sha2::{Digest, Sha256};
use std::collections::HashSet;
use std::sync::Arc;

pub struct RepositoryEngine<B: StorageBackend> {
    backend: Arc<B>,
    master_key: zeroize::Zeroizing<[u8; 32]>,
    pub config: RepositoryConfig,
}

impl<B: StorageBackend> Clone for RepositoryEngine<B> {
    fn clone(&self) -> Self {
        Self {
            backend: Arc::clone(&self.backend),
            master_key: self.master_key.clone(),
            config: self.config.clone(),
        }
    }
}

impl<B: StorageBackend> RepositoryEngine<B> {
    /// Initialize a new repository with password
    pub async fn init(backend: Arc<B>, password: &str) -> Result<Self, DumperError> {
        if backend.object_exists(CONFIG_FILE_PATH).await? {
            return Err(DumperError::Repository(
                "Repository is already initialized (config file exists)".into(),
            ));
        }

        let (config, master_key) = RepositoryConfig::new(password)?;
        let config_bytes = serde_json::to_vec_pretty(&config)?;
        backend.put_object(CONFIG_FILE_PATH, &config_bytes).await?;

        Ok(Self {
            backend,
            master_key,
            config,
        })
    }

    /// Open and unlock an existing repository with password
    pub async fn open(backend: Arc<B>, password: &str) -> Result<Self, DumperError> {
        if !backend.object_exists(CONFIG_FILE_PATH).await? {
            return Err(DumperError::Repository(
                "Repository is not initialized (missing config file). Run 'dumper init' first."
                    .into(),
            ));
        }

        let config_bytes = backend.get_object(CONFIG_FILE_PATH).await?;
        let config: RepositoryConfig = serde_json::from_slice(&config_bytes)?;
        let master_key = config.unlock(password)?;

        Ok(Self {
            backend,
            master_key,
            config,
        })
    }

    /// Store a data chunk. Deduplicates if blob already exists.
    /// Returns (BlobReference, was_deduplicated)
    pub async fn put_chunk(
        &self,
        raw_data: &[u8],
        hash_hex: &str,
        compression: CompressionLevel,
    ) -> Result<(BlobReference, bool), DumperError> {
        let blob_path = SnapshotMetadata::blob_path(hash_hex);

        // Deduplication check: if blob exists in storage, skip upload!
        if self.backend.object_exists(&blob_path).await? {
            let blob_ref = BlobReference {
                hash: hash_hex.to_string(),
                raw_size: raw_data.len() as u64,
                stored_size: 0, // 0 newly stored bytes
                compression_tag: 0,
            };
            return Ok((blob_ref, true));
        }

        // 1. Compress
        let (comp_tag, compressed) = compress_data(compression, raw_data)?;

        // 2. Encrypt: SEC-05: Put compression tag inside authenticated plaintext envelope
        let mut payload_to_encrypt = Vec::with_capacity(1 + compressed.len());
        payload_to_encrypt.push(comp_tag);
        payload_to_encrypt.extend_from_slice(&compressed);

        let encrypted = encrypt_blob(&self.master_key, &payload_to_encrypt)?;

        // 3. Upload
        self.backend.put_object(&blob_path, &encrypted).await?;

        let blob_ref = BlobReference {
            hash: hash_hex.to_string(),
            raw_size: raw_data.len() as u64,
            stored_size: encrypted.len() as u64,
            compression_tag: comp_tag,
        };

        Ok((blob_ref, false))
    }

    /// Fetch and verify a data chunk
    pub async fn get_chunk(&self, hash_hex: &str) -> Result<Vec<u8>, DumperError> {
        let blob_path = SnapshotMetadata::blob_path(hash_hex);
        let stored_payload = self.backend.get_object(&blob_path).await?;

        if stored_payload.is_empty() {
            return Err(DumperError::Integrity(format!(
                "Stored blob '{}' is empty",
                blob_path
            )));
        }

        // Decrypt: SEC-05: Authenticate before decompressing
        let (comp_tag, compressed) = match decrypt_blob(&self.master_key, &stored_payload) {
            Ok(decrypted) => {
                if decrypted.is_empty() {
                    return Err(DumperError::Integrity(
                        "Decrypted chunk payload is empty".into(),
                    ));
                }
                (decrypted[0], decrypted[1..].to_vec())
            }
            Err(e) => {
                // Backward compatibility for legacy format: [comp_tag (1B) | encrypted_data]
                if stored_payload.len() > 1 {
                    let legacy_tag = stored_payload[0];
                    let legacy_encrypted = &stored_payload[1..];
                    if let Ok(decrypted) = decrypt_blob(&self.master_key, legacy_encrypted) {
                        (legacy_tag, decrypted)
                    } else {
                        return Err(e);
                    }
                } else {
                    return Err(e);
                }
            }
        };

        // Decompress
        let raw_data = decompress_data(comp_tag, &compressed)?;

        // Verify SHA-256 matches expected content address
        let actual_hash = hex::encode(Sha256::digest(&raw_data));
        if actual_hash != hash_hex {
            return Err(DumperError::Integrity(format!(
                "Hash mismatch for blob '{}': expected {}, got {}",
                blob_path, hash_hex, actual_hash
            )));
        }

        Ok(raw_data)
    }

    /// Helper to decode snapshot metadata, supporting both authenticated encrypted format
    /// (SEC-01) and legacy plaintext JSON for backward compatibility.
    fn decode_snapshot_data(&self, data: &[u8]) -> Result<SnapshotMetadata, DumperError> {
        // Attempt decryption under repository master key first (standard encrypted format)
        match decrypt_blob(&self.master_key, data) {
            Ok(decrypted) => serde_json::from_slice::<SnapshotMetadata>(&decrypted).map_err(|e| {
                DumperError::Format(format!("Failed to parse snapshot metadata: {}", e))
            }),
            Err(decrypt_err) => {
                // If decryption failed, check if this is a legacy unencrypted JSON snapshot
                match serde_json::from_slice::<SnapshotMetadata>(data) {
                    Ok(snapshot) => Ok(snapshot),
                    Err(_) => Err(decrypt_err),
                }
            }
        }
    }

    /// Commit a snapshot atomically with encryption under repository master key (SEC-01)
    pub async fn commit_snapshot(&self, snapshot: &SnapshotMetadata) -> Result<(), DumperError> {
        let path = SnapshotMetadata::snapshot_path(&snapshot.id);
        if self.backend.object_exists(&path).await? {
            return Err(DumperError::Repository(format!(
                "Snapshot ID collision detected: snapshot '{}' already exists",
                snapshot.id
            )));
        }
        let json_bytes = serde_json::to_vec(snapshot)?;
        let encrypted = encrypt_blob(&self.master_key, &json_bytes)?;
        self.backend.put_object(&path, &encrypted).await
    }

    /// List all committed snapshots, sorted newest first
    pub async fn list_snapshots(&self) -> Result<Vec<SnapshotMetadata>, DumperError> {
        let keys = self.backend.list_objects("snapshots").await?;

        let futures = keys.into_iter().map(|key| async move {
            let data = self.backend.get_object(&key).await.map_err(|e| {
                DumperError::Repository(format!("Failed to read snapshot '{}': {}", key, e))
            })?;
            self.decode_snapshot_data(&data).map_err(|e| {
                DumperError::Format(format!(
                    "Failed to parse snapshot metadata in '{}': {}",
                    key, e
                ))
            })
        });

        let mut snapshots = futures_util::future::try_join_all(futures).await?;

        snapshots.sort_by_key(|b| std::cmp::Reverse(b.started_at));
        Ok(snapshots)
    }

    /// Find snapshot by short (prefix) or full ID.
    ///
    /// Attempts direct lookup by snapshot path first, then falls back to searching
    /// known snapshot files. Unparseable/corrupted snapshot files during search do not
    /// prevent finding a valid snapshot (SEC-07).
    pub async fn find_snapshot(&self, id_query: &str) -> Result<SnapshotMetadata, DumperError> {
        // 1. Direct path lookup: if id_query is an exact snapshot ID
        let direct_path = SnapshotMetadata::snapshot_path(id_query);
        if self
            .backend
            .object_exists(&direct_path)
            .await
            .unwrap_or(false)
        {
            let data = self.backend.get_object(&direct_path).await?;
            let snapshot = self.decode_snapshot_data(&data).map_err(|e| {
                DumperError::Format(format!(
                    "Failed to parse snapshot metadata in '{}': {}",
                    direct_path, e
                ))
            })?;
            if snapshot.id == id_query
                || snapshot.id.starts_with(id_query)
                || snapshot.full_id == id_query
                || snapshot.full_id.starts_with(id_query)
            {
                return Ok(snapshot);
            }
        }

        // 2. Search all snapshot keys by prefix or full ID, ignoring corrupt files so they
        // don't cause a Denial of Service for valid restores.
        let keys = self.backend.list_objects("snapshots").await?;
        for key in keys {
            if let Ok(data) = self.backend.get_object(&key).await {
                if let Ok(s) = self.decode_snapshot_data(&data) {
                    if s.id == id_query
                        || s.id.starts_with(id_query)
                        || s.full_id == id_query
                        || s.full_id.starts_with(id_query)
                    {
                        return Ok(s);
                    }
                }
            }
        }
        Err(DumperError::Repository(format!(
            "Snapshot '{}' not found in repository",
            id_query
        )))
    }

    /// Delete snapshot metadata (does not remove underlying blobs; prune does that)
    pub async fn delete_snapshot(&self, id: &str) -> Result<(), DumperError> {
        let path = SnapshotMetadata::snapshot_path(id);
        self.backend.delete_object(&path).await
    }

    /// Garbage collect unreferenced blobs
    pub async fn prune(&self) -> Result<(usize, u64), DumperError> {
        let snapshots = self.list_snapshots().await?;

        // 1. Build set of referenced hashes
        let mut referenced_hashes = HashSet::new();
        for s in snapshots {
            for b in s.blobs {
                referenced_hashes.insert(b.hash);
            }
        }

        // 2. Scan all existing blobs
        let all_blobs = self.backend.list_objects("blobs").await?;
        let mut deleted_count = 0;
        let mut deleted_bytes = 0u64;

        for blob_key in all_blobs {
            let hash = blob_key.split(['/', '\\']).next_back().unwrap_or("");
            if !referenced_hashes.contains(hash) {
                if let Ok(size) = self.backend.get_object_size(&blob_key).await {
                    deleted_bytes += size;
                }
                let _ = self.backend.delete_object(&blob_key).await;
                deleted_count += 1;
            }
        }

        // 3. Clean up abandoned temporary files (e.g. from interrupted uploads or crashes)
        let _ = self.backend.cleanup_temp_files().await;

        Ok((deleted_count, deleted_bytes))
    }

    /// Verify a snapshot's blobs, encryption, decompression, and hashes
    pub async fn verify_snapshot(&self, snapshot: &SnapshotMetadata) -> Result<usize, DumperError> {
        use futures_util::{StreamExt, TryStreamExt};

        let stream = futures_util::stream::iter(snapshot.blobs.iter().enumerate()).map(
            |(i, blob_ref)| async move {
                let _data = self.get_chunk(&blob_ref.hash).await.map_err(|e| {
                    DumperError::Integrity(format!(
                        "Verification failed on blob {} ({}/{}): {}",
                        blob_ref.hash,
                        i + 1,
                        snapshot.blobs.len(),
                        e
                    ))
                })?;
                Ok::<(), DumperError>(())
            },
        );

        stream
            .buffer_unordered(16)
            .try_for_each(|_| async { Ok(()) })
            .await?;
        Ok(snapshot.blobs.len())
    }

    /// Check repository-wide consistency: missing blobs, orphaned blobs, metadata validity
    pub async fn check(&self) -> Result<(usize, usize, usize), DumperError> {
        let snapshots = self.list_snapshots().await?;
        let mut referenced_hashes = HashSet::new();
        let mut missing_count = 0;

        for s in &snapshots {
            for b in &s.blobs {
                referenced_hashes.insert(b.hash.clone());
                let blob_path = SnapshotMetadata::blob_path(&b.hash);
                if !self.backend.object_exists(&blob_path).await? {
                    missing_count += 1;
                }
            }
        }

        let all_blobs = self.backend.list_objects("blobs").await?;
        let mut orphaned_count = 0;

        for blob_key in &all_blobs {
            let hash = blob_key.split(['/', '\\']).next_back().unwrap_or("");
            if !referenced_hashes.contains(hash) {
                orphaned_count += 1;
            }
        }

        Ok((snapshots.len(), missing_count, orphaned_count))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::repository::local::LocalBackend;

    #[tokio::test]
    async fn test_repository_engine_e2e() {
        let temp_dir = tempfile::tempdir().unwrap();
        let backend = Arc::new(LocalBackend::new(temp_dir.path()).await.unwrap());
        let password = "test-repo-password";

        // 1. Init
        let engine = RepositoryEngine::init(backend.clone(), password)
            .await
            .unwrap();

        // 2. Put chunk 1
        let chunk1 = b"Sample database row data block 1";
        let hash1 = hex::encode(Sha256::digest(chunk1));
        let (ref1, dedup1) = engine
            .put_chunk(chunk1, &hash1, CompressionLevel::Default)
            .await
            .unwrap();
        assert!(!dedup1);
        assert_eq!(ref1.hash, hash1);

        // 3. Put chunk 1 again -> must be deduplicated!
        let (ref1_dup, dedup1_dup) = engine
            .put_chunk(chunk1, &hash1, CompressionLevel::Default)
            .await
            .unwrap();
        assert!(dedup1_dup);
        assert_eq!(ref1_dup.hash, hash1);

        // 4. Read chunk 1
        let retrieved = engine.get_chunk(&hash1).await.unwrap();
        assert_eq!(retrieved, chunk1);

        // 5. Commit snapshot
        let snapshot = SnapshotMetadata {
            id: "12345678".into(),
            full_id: "12345678abcdef".into(),
            format_version: 1,
            dumper_version: "0.1.0".into(),
            engine: "postgresql".into(),
            database: "app_db".into(),
            server_version: "17.0".into(),
            started_at: chrono::Utc::now(),
            completed_at: chrono::Utc::now(),
            duration_seconds: 1,
            logical_bytes: chunk1.len() as u64,
            stored_bytes: ref1.stored_size,
            deduplicated_bytes: 0,
            table_count: 1,
            compression: "default".into(),
            tag: None,
            blobs: vec![ref1],
        };
        engine.commit_snapshot(&snapshot).await.unwrap();

        // Duplicate snapshot ID must be rejected to prevent overwrites
        assert!(engine.commit_snapshot(&snapshot).await.is_err());

        // SEC-01: Verify that raw stored snapshot bytes on disk are encrypted (not plain JSON)
        let raw_snap_bytes = backend.get_object("snapshots/12345678").await.unwrap();
        assert!(serde_json::from_slice::<SnapshotMetadata>(&raw_snap_bytes).is_err());
        let decrypted_bytes = decrypt_blob(&engine.master_key, &raw_snap_bytes).unwrap();
        assert_eq!(decrypted_bytes, serde_json::to_vec(&snapshot).unwrap());

        // SEC-05: Tampering with stored blob triggers AEAD authentication failure before decompression
        let blob_path = SnapshotMetadata::blob_path(&hash1);
        let mut tampered_blob = backend.get_object(&blob_path).await.unwrap();
        tampered_blob[0] ^= 0xFF; // flip byte
        let tampered_hash = "ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff";
        let tampered_path = SnapshotMetadata::blob_path(tampered_hash);
        backend
            .put_object(&tampered_path, &tampered_blob)
            .await
            .unwrap();
        assert!(engine.get_chunk(tampered_hash).await.is_err());
        backend.delete_object(&tampered_path).await.unwrap();

        // 6. List snapshots
        let list = engine.list_snapshots().await.unwrap();
        assert_eq!(list.len(), 1);
        assert_eq!(list[0].id, "12345678");

        // 7. Verify snapshot
        let verified_blobs = engine.verify_snapshot(&snapshot).await.unwrap();
        assert_eq!(verified_blobs, 1);

        // 8. Check repository
        let (snapshots_cnt, missing_cnt, orphaned_cnt) = engine.check().await.unwrap();
        assert_eq!(snapshots_cnt, 1);
        assert_eq!(missing_cnt, 0);
        assert_eq!(orphaned_cnt, 0);
    }

    #[tokio::test]
    async fn test_decode_snapshot_with_nonce_starting_with_brace() {
        use chacha20poly1305::{
            aead::{Aead, KeyInit},
            XChaCha20Poly1305, XNonce,
        };

        let temp_dir = tempfile::tempdir().unwrap();
        let backend = Arc::new(LocalBackend::new(temp_dir.path()).await.unwrap());
        let password = "test-nonce-brace-password";
        let engine = RepositoryEngine::init(backend.clone(), password)
            .await
            .unwrap();

        let snapshot = SnapshotMetadata {
            id: "snap_brace".into(),
            full_id: "snap_brace_full_id_12345".into(),
            format_version: 1,
            dumper_version: crate::VERSION.into(),
            engine: "postgresql".into(),
            database: "test_db".into(),
            server_version: "16".into(),
            started_at: chrono::Utc::now(),
            completed_at: chrono::Utc::now(),
            duration_seconds: 5,
            logical_bytes: 1024,
            stored_bytes: 512,
            deduplicated_bytes: 0,
            table_count: 2,
            compression: "default".into(),
            tag: None,
            blobs: vec![],
        };

        let json_bytes = serde_json::to_vec(&snapshot).unwrap();

        // Construct encrypted snapshot with nonce[0] == b'{' (0x7B)
        // This simulates the 1-in-256 random chance where the AEAD nonce starts with ASCII '{'
        let cipher = XChaCha20Poly1305::new_from_slice(&engine.master_key[..]).unwrap();
        let mut nonce_bytes = [0u8; 24];
        nonce_bytes[0] = b'{';
        nonce_bytes[1] = 0xAA; // Arbitrary non-JSON byte
        let nonce = XNonce::from_slice(&nonce_bytes);
        let ciphertext = cipher.encrypt(nonce, json_bytes.as_slice()).unwrap();

        let mut encrypted_payload = Vec::with_capacity(24 + ciphertext.len());
        encrypted_payload.extend_from_slice(&nonce_bytes);
        encrypted_payload.extend_from_slice(&ciphertext);

        // Verify the raw encrypted payload starts with '{'
        assert_eq!(encrypted_payload[0], b'{');

        // Store this snapshot in repository
        let path = SnapshotMetadata::snapshot_path(&snapshot.id);
        backend.put_object(&path, &encrypted_payload).await.unwrap();

        // 1. decode_snapshot_data must successfully decrypt and decode
        let decoded = engine.decode_snapshot_data(&encrypted_payload).unwrap();
        assert_eq!(decoded.id, "snap_brace");
        assert_eq!(decoded.database, "test_db");

        // 2. list_snapshots must successfully parse this snapshot without failing
        let snapshots = engine.list_snapshots().await.unwrap();
        assert_eq!(snapshots.len(), 1);
        assert_eq!(snapshots[0].id, "snap_brace");

        // 3. check() must succeed with zero errors
        let (total_snaps, missing_blobs, orphaned_blobs) = engine.check().await.unwrap();
        assert_eq!(total_snaps, 1);
        assert_eq!(missing_blobs, 0);
        assert_eq!(orphaned_blobs, 0);

        // 4. find_snapshot must find it
        let found = engine.find_snapshot("snap_brace").await.unwrap();
        assert_eq!(found.id, "snap_brace");
    }

    #[tokio::test]
    async fn test_decode_legacy_unencrypted_snapshot() {
        let temp_dir = tempfile::tempdir().unwrap();
        let backend = Arc::new(LocalBackend::new(temp_dir.path()).await.unwrap());
        let password = "test-legacy-password";
        let engine = RepositoryEngine::init(backend.clone(), password)
            .await
            .unwrap();

        let legacy_snapshot = SnapshotMetadata {
            id: "legacy_snap".into(),
            full_id: "legacy_snap_full_id_67890".into(),
            format_version: 1,
            dumper_version: "0.1.0".into(),
            engine: "mysql".into(),
            database: "legacy_db".into(),
            server_version: "8.0".into(),
            started_at: chrono::Utc::now(),
            completed_at: chrono::Utc::now(),
            duration_seconds: 10,
            logical_bytes: 2048,
            stored_bytes: 1024,
            deduplicated_bytes: 0,
            table_count: 5,
            compression: "default".into(),
            tag: None,
            blobs: vec![],
        };

        // Write as plaintext JSON (legacy pre-SEC-01 format)
        let json_bytes = serde_json::to_vec_pretty(&legacy_snapshot).unwrap();
        assert!(json_bytes.starts_with(b"{\n") || json_bytes.starts_with(b"{"));
        let path = SnapshotMetadata::snapshot_path(&legacy_snapshot.id);
        backend.put_object(&path, &json_bytes).await.unwrap();

        // 1. decode_snapshot_data must fallback to plaintext JSON parsing and succeed
        let decoded = engine.decode_snapshot_data(&json_bytes).unwrap();
        assert_eq!(decoded.id, "legacy_snap");
        assert_eq!(decoded.database, "legacy_db");

        // 2. list_snapshots must successfully list legacy snapshot
        let snapshots = engine.list_snapshots().await.unwrap();
        assert_eq!(snapshots.len(), 1);
        assert_eq!(snapshots[0].id, "legacy_snap");

        // 3. check() must succeed
        let (total_snaps, missing_blobs, orphaned_blobs) = engine.check().await.unwrap();
        assert_eq!(total_snaps, 1);
        assert_eq!(missing_blobs, 0);
        assert_eq!(orphaned_blobs, 0);
    }
}
