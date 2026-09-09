use crate::error::DumperError;
use crate::repository::backend::StorageBackend;
use std::path::{Path, PathBuf};
use tokio::fs::{self, File};
use tokio::io::AsyncWriteExt;

pub struct LocalBackend {
    base_path: PathBuf,
}

impl LocalBackend {
    pub async fn new<P: AsRef<Path>>(path: P) -> Result<Self, DumperError> {
        let base_path = path.as_ref().to_path_buf();
        fs::create_dir_all(&base_path).await.map_err(|e| {
            DumperError::Repository(format!("Failed to create repository directory: {}", e))
        })?;
        let canonical = fs::canonicalize(&base_path).await.map_err(|e| {
            DumperError::Repository(format!(
                "Failed to canonicalize repository directory: {}",
                e
            ))
        })?;
        Ok(Self {
            base_path: canonical,
        })
    }

    fn resolve_path(&self, rel_path: &str) -> Result<PathBuf, DumperError> {
        // Prevent path traversal
        let clean_rel = rel_path.trim_start_matches('/');
        if clean_rel.contains("..") {
            return Err(DumperError::Repository(format!(
                "Path traversal attempt detected: '{}'",
                rel_path
            )));
        }
        Ok(self.base_path.join(clean_rel))
    }
}

impl StorageBackend for LocalBackend {
    async fn put_object(&self, path: &str, data: &[u8]) -> Result<(), DumperError> {
        let target_path = self.resolve_path(path)?;
        if let Some(parent) = target_path.parent() {
            fs::create_dir_all(parent).await.map_err(|e| {
                DumperError::Repository(format!("Failed to create parent directory: {}", e))
            })?;
        }

        // Atomic write: write to a temporary file in the same directory, sync, then rename
        let tmp_filename = format!(".tmp_{}_{}", std::process::id(), rand::random::<u64>());
        let tmp_path = target_path
            .parent()
            .unwrap_or(&self.base_path)
            .join(&tmp_filename);

        let write_res = async {
            let mut file = File::create(&tmp_path).await.map_err(|e| {
                DumperError::Repository(format!("Failed to create temp file {:?}: {}", tmp_path, e))
            })?;

            file.write_all(data).await.map_err(|e| {
                DumperError::Repository(format!("Failed to write temp file {:?}: {}", tmp_path, e))
            })?;

            file.sync_all().await.map_err(|e| {
                DumperError::Repository(format!("Failed to sync temp file {:?}: {}", tmp_path, e))
            })?;

            drop(file);

            fs::rename(&tmp_path, &target_path).await.map_err(|e| {
                DumperError::Repository(format!(
                    "Failed to rename temp file {:?} to {:?}: {}",
                    tmp_path, target_path, e
                ))
            })?;

            Ok::<(), DumperError>(())
        }
        .await;

        if let Err(e) = write_res {
            let _ = fs::remove_file(&tmp_path).await;
            return Err(e);
        }

        Ok(())
    }

    async fn get_object(&self, path: &str) -> Result<Vec<u8>, DumperError> {
        let target_path = self.resolve_path(path)?;
        fs::read(&target_path).await.map_err(|e| {
            DumperError::Repository(format!("Failed to read object '{}': {}", path, e))
        })
    }

    async fn get_object_size(&self, path: &str) -> Result<u64, DumperError> {
        let target_path = self.resolve_path(path)?;
        let meta = fs::metadata(&target_path).await.map_err(|e| {
            DumperError::Repository(format!(
                "Failed to get metadata for object '{}': {}",
                path, e
            ))
        })?;
        Ok(meta.len())
    }

    async fn object_exists(&self, path: &str) -> Result<bool, DumperError> {
        let target_path = self.resolve_path(path)?;
        Ok(fs::try_exists(&target_path).await.unwrap_or(false))
    }

    async fn delete_object(&self, path: &str) -> Result<(), DumperError> {
        let target_path = self.resolve_path(path)?;
        if fs::try_exists(&target_path).await.unwrap_or(false) {
            fs::remove_file(&target_path).await.map_err(|e| {
                DumperError::Repository(format!("Failed to delete object '{}': {}", path, e))
            })?;
        }
        Ok(())
    }

    async fn list_objects(&self, prefix: &str) -> Result<Vec<String>, DumperError> {
        let prefix_clean = prefix.trim_start_matches('/').replace('\\', "/");
        let mut results = Vec::new();
        let target_dir = self.base_path.join(&prefix_clean);

        if !fs::try_exists(&target_dir).await.unwrap_or(false) {
            // It might be a prefix of file names, or directory doesn't exist
            let search_dir = target_dir.parent().unwrap_or(&self.base_path);
            if fs::try_exists(search_dir).await.unwrap_or(false) {
                let mut entries = fs::read_dir(search_dir).await?;
                while let Some(entry) = entries.next_entry().await? {
                    let full_path = entry.path();
                    if let Ok(rel) = full_path.strip_prefix(&self.base_path) {
                        let rel_str = rel.to_string_lossy().replace('\\', "/");
                        if rel_str.starts_with(&prefix_clean) && entry.file_type().await?.is_file()
                        {
                            results.push(rel_str);
                        }
                    }
                }
            }
            return Ok(results);
        }

        // Recursive directory traversal
        let mut dirs = vec![target_dir];
        while let Some(dir) = dirs.pop() {
            let mut entries = match fs::read_dir(&dir).await {
                Ok(e) => e,
                Err(_) => continue,
            };
            while let Some(entry) = entries.next_entry().await? {
                let file_type = entry.file_type().await?;
                let full_path = entry.path();
                if file_type.is_dir() {
                    dirs.push(full_path);
                } else if file_type.is_file() {
                    if let Ok(rel) = full_path.strip_prefix(&self.base_path) {
                        let rel_str = rel.to_string_lossy().replace('\\', "/");
                        // Ignore hidden temp files
                        if !entry.file_name().to_string_lossy().starts_with(".tmp_") {
                            results.push(rel_str);
                        }
                    }
                }
            }
        }

        results.sort();
        Ok(results)
    }

    async fn count_temp_files(&self) -> Result<usize, DumperError> {
        let mut count = 0;
        let mut dirs = vec![self.base_path.clone()];
        while let Some(dir) = dirs.pop() {
            let mut entries = match fs::read_dir(&dir).await {
                Ok(e) => e,
                Err(_) => continue,
            };
            while let Some(entry) = entries.next_entry().await.map_err(|e| {
                DumperError::Repository(format!("Failed to read directory {:?}: {}", dir, e))
            })? {
                let file_type = entry.file_type().await.map_err(|e| {
                    DumperError::Repository(format!("Failed to inspect file type: {}", e))
                })?;
                let path = entry.path();
                if file_type.is_dir() {
                    dirs.push(path);
                } else if file_type.is_file()
                    && entry.file_name().to_string_lossy().starts_with(".tmp_")
                {
                    count += 1;
                }
            }
        }
        Ok(count)
    }

    async fn cleanup_temp_files(&self) -> Result<usize, DumperError> {
        let mut cleaned = 0;
        let mut dirs = vec![self.base_path.clone()];
        while let Some(dir) = dirs.pop() {
            let mut entries = match fs::read_dir(&dir).await {
                Ok(e) => e,
                Err(_) => continue,
            };
            while let Some(entry) = entries.next_entry().await.map_err(|e| {
                DumperError::Repository(format!("Failed to read directory {:?}: {}", dir, e))
            })? {
                let file_type = entry.file_type().await.map_err(|e| {
                    DumperError::Repository(format!("Failed to inspect file type: {}", e))
                })?;
                let path = entry.path();
                if file_type.is_dir() {
                    dirs.push(path);
                } else if file_type.is_file()
                    && entry.file_name().to_string_lossy().starts_with(".tmp_")
                    && fs::remove_file(&path).await.is_ok()
                {
                    cleaned += 1;
                }
            }
        }
        Ok(cleaned)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_local_backend_crud() {
        let temp_dir = tempfile::tempdir().unwrap();
        let backend = LocalBackend::new(temp_dir.path()).await.unwrap();

        // 1. Put object
        let key = "blobs/12/3456abcdef";
        let data = b"Chunk payload data here";
        backend.put_object(key, data).await.unwrap();

        // 2. Exists
        assert!(backend.object_exists(key).await.unwrap());
        assert!(!backend.object_exists("blobs/nonexistent").await.unwrap());

        // 3. Get object
        let fetched = backend.get_object(key).await.unwrap();
        assert_eq!(fetched, data);

        // 4. List objects
        let list = backend.list_objects("blobs").await.unwrap();
        assert_eq!(list, vec!["blobs/12/3456abcdef".to_string()]);

        // 5. Delete object
        backend.delete_object(key).await.unwrap();
        assert!(!backend.object_exists(key).await.unwrap());
    }

    #[tokio::test]
    async fn test_path_traversal_prevention() {
        let temp_dir = tempfile::tempdir().unwrap();
        let backend = LocalBackend::new(temp_dir.path()).await.unwrap();

        let malicious = "../../../etc/passwd";
        let res = backend.put_object(malicious, b"danger").await;
        assert!(res.is_err());
    }

    #[tokio::test]
    async fn test_temp_file_counting_and_cleanup() {
        let temp_dir = tempfile::tempdir().unwrap();
        let backend = LocalBackend::new(temp_dir.path()).await.unwrap();

        let sub_dir = temp_dir.path().join("blobs/ab");
        fs::create_dir_all(&sub_dir).await.unwrap();

        let tmp1 = sub_dir.join(".tmp_111");
        let tmp2 = temp_dir.path().join(".tmp_222");
        fs::write(&tmp1, b"partial").await.unwrap();
        fs::write(&tmp2, b"partial").await.unwrap();

        assert_eq!(backend.count_temp_files().await.unwrap(), 2);
        assert_eq!(backend.cleanup_temp_files().await.unwrap(), 2);
        assert_eq!(backend.count_temp_files().await.unwrap(), 0);
    }
}
