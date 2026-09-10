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

        // 2. Encrypt
        let encrypted = encrypt_blob(&self.master_key, &compressed)?;

        // Prepend 1-byte compression tag to stored payload: [compression_tag (1B) | encrypted_data]
        let mut stored_payload = Vec::with_capacity(1 + encrypted.len());
        stored_payload.push(comp_tag);
        stored_payload.extend_from_slice(&encrypted);

        // 3. Upload
        self.backend.put_object(&blob_path, &stored_payload).await?;

        let blob_ref = BlobReference {
            hash: hash_hex.to_string(),
            raw_size: raw_data.len() as u64,
            stored_size: stored_payload.len() as u64,
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

        let comp_tag = stored_payload[0];
        let encrypted = &stored_payload[1..];

        // Decrypt
        let compressed = decrypt_blob(&self.master_key, encrypted)?;

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

    /// Commit a snapshot atomically
    pub async fn commit_snapshot(&self, snapshot: &SnapshotMetadata) -> Result<(), DumperError> {
        let path = SnapshotMetadata::snapshot_path(&snapshot.id);
        if self.backend.object_exists(&path).await? {
            return Err(DumperError::Repository(format!(
                "Snapshot ID collision detected: snapshot '{}' already exists",
                snapshot.id
            )));
        }
        let data = serde_json::to_vec_pretty(snapshot)?;
        self.backend.put_object(&path, &data).await
    }

    /// List all committed snapshots, sorted newest first
    pub async fn list_snapshots(&self) -> Result<Vec<SnapshotMetadata>, DumperError> {
        let keys = self.backend.list_objects("snapshots").await?;
        let mut snapshots = Vec::new();

        for key in keys {
            let data = self.backend.get_object(&key).await.map_err(|e| {
                DumperError::Repository(format!("Failed to read snapshot '{}': {}", key, e))
            })?;
            let snapshot = serde_json::from_slice::<SnapshotMetadata>(&data).map_err(|e| {
                DumperError::Format(format!(
                    "Failed to parse snapshot metadata in '{}': {}",
                    key, e
                ))
            })?;
            snapshots.push(snapshot);
        }

        snapshots.sort_by_key(|b| std::cmp::Reverse(b.started_at));
        Ok(snapshots)
    }

    /// Find snapshot by short (prefix) or full ID
    pub async fn find_snapshot(&self, id_query: &str) -> Result<SnapshotMetadata, DumperError> {
        let snapshots = self.list_snapshots().await?;
        for s in snapshots {
            if s.id == id_query || s.full_id == id_query || s.full_id.starts_with(id_query) {
                return Ok(s);
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
        for (i, blob_ref) in snapshot.blobs.iter().enumerate() {
            let _data = self.get_chunk(&blob_ref.hash).await.map_err(|e| {
                DumperError::Integrity(format!(
                    "Verification failed on blob {} ({}/{}): {}",
                    blob_ref.hash,
                    i + 1,
                    snapshot.blobs.len(),
                    e
                ))
            })?;
        }
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
}
