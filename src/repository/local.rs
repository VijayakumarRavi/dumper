use crate::error::DumperError;
use crate::repository::backend::StorageBackend;
use std::path::{Path, PathBuf};
use tokio::fs;
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
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let _ = std::fs::set_permissions(&base_path, std::fs::Permissions::from_mode(0o700));
        }
        let canonical = fs::canonicalize(&base_path).await.map_err(|e| {
            DumperError::Repository(format!(
                "Failed to canonicalize repository directory: {}",
                e
            ))
        })?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let _ = std::fs::set_permissions(&canonical, std::fs::Permissions::from_mode(0o700));
        }
        Ok(Self {
            base_path: canonical,
        })
    }

    fn resolve_path(&self, rel_path: &str) -> Result<PathBuf, DumperError> {
        // Prevent path traversal across Unix and Windows
        let p = Path::new(rel_path);
        if rel_path.starts_with('/')
            || rel_path.starts_with('\\')
            || rel_path.contains(':')
            || rel_path.contains("..")
            || p.is_absolute()
            || p.has_root()
            || p.components().any(|c| {
                matches!(
                    c,
                    std::path::Component::Prefix(_)
                        | std::path::Component::ParentDir
                        | std::path::Component::RootDir
                )
            })
        {
            return Err(DumperError::Repository(format!(
                "Path traversal attempt detected: '{}'",
                rel_path
            )));
        }

        let joined = self.base_path.join(rel_path);
        if !joined.starts_with(&self.base_path) {
            return Err(DumperError::Repository(format!(
                "Path traversal attempt detected: '{}'",
                rel_path
            )));
        }
        Ok(joined)
    }
}

#[cfg(unix)]
fn is_pid_alive(pid: u32) -> bool {
    unsafe {
        if libc::kill(pid as libc::pid_t, 0) == 0 {
            true
        } else {
            std::io::Error::last_os_error().raw_os_error() == Some(libc::EPERM)
        }
    }
}

#[cfg(not(unix))]
fn is_pid_alive(_pid: u32) -> bool {
    false
}

fn get_host_id() -> String {
    if let Ok(h) = std::env::var("HOSTNAME") {
        let sanitized = sanitize_hostname(&h);
        if !sanitized.is_empty() {
            return sanitized;
        }
    }
    #[cfg(unix)]
    {
        let mut buf = [0u8; 256];
        unsafe {
            if libc::gethostname(buf.as_mut_ptr() as *mut libc::c_char, buf.len()) == 0 {
                if let Ok(s) =
                    std::ffi::CStr::from_ptr(buf.as_ptr() as *const libc::c_char).to_str()
                {
                    let sanitized = sanitize_hostname(s);
                    if !sanitized.is_empty() {
                        return sanitized;
                    }
                }
            }
        }
    }
    "host".to_string()
}

fn sanitize_hostname(s: &str) -> String {
    s.chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '-' {
                c
            } else {
                '-'
            }
        })
        .collect()
}

fn secure_dir_tree(path: &Path, base_path: &Path) {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mut curr = path;
        while curr.starts_with(base_path) {
            if let Ok(meta) = std::fs::metadata(curr) {
                if meta.is_dir() {
                    let _ = std::fs::set_permissions(curr, std::fs::Permissions::from_mode(0o700));
                }
            }
            if curr == base_path {
                break;
            }
            if let Some(parent) = curr.parent() {
                curr = parent;
            } else {
                break;
            }
        }
    }
}

impl StorageBackend for LocalBackend {
    async fn put_object(&self, path: &str, data: &[u8]) -> Result<(), DumperError> {
        let target_path = self.resolve_path(path)?;
        if let Some(parent) = target_path.parent() {
            fs::create_dir_all(parent).await.map_err(|e| {
                DumperError::Repository(format!("Failed to create parent directory: {}", e))
            })?;
            secure_dir_tree(parent, &self.base_path);
        }

        // Atomic write: write to a temporary file in the same directory, sync, then rename
        // SEC-10: Include sanitized host identifier so processes on shared filesystems (NFS/PVC)
        // do not clean up each other's in-flight temporary files.
        let tmp_filename = format!(
            ".tmp_{}_{}_{}",
            get_host_id(),
            std::process::id(),
            rand::random::<u64>()
        );
        let tmp_path = target_path
            .parent()
            .unwrap_or(&self.base_path)
            .join(&tmp_filename);

        let write_res = async {
            #[allow(unused_mut)]
            let mut open_options = tokio::fs::OpenOptions::new();
            open_options.write(true).create(true).truncate(true);
            #[cfg(unix)]
            open_options.mode(0o600);

            let mut file = open_options.open(&tmp_path).await.map_err(|e| {
                DumperError::Repository(format!("Failed to create temp file {:?}: {}", tmp_path, e))
            })?;

            file.write_all(data).await.map_err(|e| {
                DumperError::Repository(format!("Failed to write temp file {:?}: {}", tmp_path, e))
            })?;

            file.sync_all().await.map_err(|e| {
                DumperError::Repository(format!("Failed to sync temp file {:?}: {}", tmp_path, e))
            })?;

            drop(file);

            if let Err(rename_err) = fs::rename(&tmp_path, &target_path).await {
                // On Windows, rename fails if target already exists. Remove and retry.
                if fs::try_exists(&target_path).await.unwrap_or(false) {
                    let _ = fs::remove_file(&target_path).await;
                    fs::rename(&tmp_path, &target_path).await.map_err(|e| {
                        DumperError::Repository(format!(
                            "Failed to rename temp file {:?} to {:?}: {}",
                            tmp_path, target_path, e
                        ))
                    })?;
                } else {
                    return Err(DumperError::Repository(format!(
                        "Failed to rename temp file {:?} to {:?}: {}",
                        tmp_path, target_path, rename_err
                    )));
                }
            }

            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                let _ =
                    std::fs::set_permissions(&target_path, std::fs::Permissions::from_mode(0o600));
            }

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
                {
                    // Do not delete in-flight temporary files written by active processes
                    let fname = entry.file_name().to_string_lossy().to_string();
                    let mut in_flight = false;

                    if let Ok(meta) = entry.metadata().await {
                        if let Ok(modified) = meta.modified() {
                            if let Ok(elapsed) = modified.elapsed() {
                                if elapsed.as_secs() < 3600 {
                                    let parts: Vec<&str> =
                                        fname.trim_start_matches(".tmp_").split('_').collect();
                                    if parts.len() >= 3 {
                                        // Format: .tmp_{host}_{pid}_{rand}
                                        let file_host = parts[0];
                                        let pid_str = parts[1];
                                        let current_host = get_host_id();

                                        if file_host == current_host {
                                            if let Ok(pid) = pid_str.parse::<u32>() {
                                                if pid == std::process::id() || is_pid_alive(pid) {
                                                    in_flight = true;
                                                }
                                            }
                                        } else {
                                            // Remote host on shared filesystem: cannot check PID locally,
                                            // so treat recent files (< 3600s) as in-flight
                                            in_flight = true;
                                        }
                                    } else if let Some(pid_str) = parts.first() {
                                        // Legacy format: .tmp_{pid}_{rand}
                                        if let Ok(pid) = pid_str.parse::<u32>() {
                                            if pid == std::process::id() || is_pid_alive(pid) {
                                                in_flight = true;
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }

                    if !in_flight && fs::remove_file(&path).await.is_ok() {
                        cleaned += 1;
                    }
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

        let malicious_paths = [
            "../../../etc/passwd",
            "..\\..\\..\\windows\\system32",
            "/etc/passwd",
            "\\windows\\system32",
            "C:\\test\\file",
            "C:test\\file",
            "\\\\server\\share\\test",
        ];
        for malicious in malicious_paths {
            let res = backend.put_object(malicious, b"danger").await;
            assert!(res.is_err(), "Expected error for path: {}", malicious);
        }
    }

    #[tokio::test]
    async fn test_overwrite_existing_object() {
        let temp_dir = tempfile::tempdir().unwrap();
        let backend = LocalBackend::new(temp_dir.path()).await.unwrap();

        backend
            .put_object("blobs/test", b"version 1")
            .await
            .unwrap();
        backend
            .put_object("blobs/test", b"version 2")
            .await
            .unwrap();
        assert_eq!(
            backend.get_object("blobs/test").await.unwrap(),
            b"version 2"
        );
    }

    #[tokio::test]
    async fn test_temp_file_counting_and_cleanup() {
        let temp_dir = tempfile::tempdir().unwrap();
        let backend = LocalBackend::new(temp_dir.path()).await.unwrap();

        let sub_dir = temp_dir.path().join("blobs/ab");
        fs::create_dir_all(&sub_dir).await.unwrap();

        let tmp1 = sub_dir.join(".tmp_99999998_111");
        let tmp2 = temp_dir.path().join(".tmp_99999999_222");
        fs::write(&tmp1, b"partial").await.unwrap();
        fs::write(&tmp2, b"partial").await.unwrap();

        assert_eq!(backend.count_temp_files().await.unwrap(), 2);
        assert_eq!(backend.cleanup_temp_files().await.unwrap(), 2);
        assert_eq!(backend.count_temp_files().await.unwrap(), 0);
    }

    #[tokio::test]
    async fn test_in_flight_temp_file_not_deleted() {
        let temp_dir = tempfile::tempdir().unwrap();
        let backend = LocalBackend::new(temp_dir.path()).await.unwrap();

        let sub_dir = temp_dir.path().join("blobs/ab");
        fs::create_dir_all(&sub_dir).await.unwrap();

        // In-flight temp file with current PID and recent timestamp
        let in_flight = sub_dir.join(format!(".tmp_{}_99999", std::process::id()));
        // Abandoned temp file with non-existent PID (e.g. 99999999)
        let abandoned = sub_dir.join(".tmp_99999999_12345");

        fs::write(&in_flight, b"in-flight data").await.unwrap();
        fs::write(&abandoned, b"abandoned data").await.unwrap();

        assert_eq!(backend.count_temp_files().await.unwrap(), 2);
        // Only abandoned file should be cleaned up; in-flight file preserved!
        let cleaned = backend.cleanup_temp_files().await.unwrap();
        assert_eq!(cleaned, 1);
        assert!(in_flight.exists());
        assert!(!abandoned.exists());
    }

    #[tokio::test]
    async fn test_multi_host_temp_file_race_condition() {
        let temp_dir = tempfile::tempdir().unwrap();
        let backend = LocalBackend::new(temp_dir.path()).await.unwrap();

        let current_host = get_host_id();
        let remote_host = format!("{}-remote-node", current_host);

        let sub_dir = temp_dir.path().join("blobs/cd");
        fs::create_dir_all(&sub_dir).await.unwrap();

        // 1. In-flight file from local host: active pid
        let local_active =
            sub_dir.join(format!(".tmp_{}_{}_1001", current_host, std::process::id()));
        // 2. Abandoned file from local host: dead pid (99999999)
        let local_dead = sub_dir.join(format!(".tmp_{}_99999999_1002", current_host));
        // 3. In-flight file from remote host: recent timestamp
        let remote_active = sub_dir.join(format!(".tmp_{}_99999999_1003", remote_host));

        fs::write(&local_active, b"local active").await.unwrap();
        fs::write(&local_dead, b"local dead").await.unwrap();
        fs::write(&remote_active, b"remote active").await.unwrap();

        assert_eq!(backend.count_temp_files().await.unwrap(), 3);

        // cleanup_temp_files should ONLY clean up local_dead!
        // local_active is active on this host, and remote_active is < 3600s so it cannot be touched.
        let cleaned = backend.cleanup_temp_files().await.unwrap();
        assert_eq!(cleaned, 1);
        assert!(local_active.exists());
        assert!(!local_dead.exists());
        assert!(remote_active.exists());
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn test_local_backend_permissions() {
        use std::os::unix::fs::PermissionsExt;
        let temp_dir = tempfile::tempdir().unwrap();
        let repo_dir = temp_dir.path().join("repo");
        let backend = LocalBackend::new(&repo_dir).await.unwrap();

        // 1. Verify repo directory is 0700 (rwx------)
        let meta = std::fs::metadata(&repo_dir).unwrap();
        assert_eq!(meta.permissions().mode() & 0o777, 0o700);

        // 2. Put an object into nested directories
        backend
            .put_object("blobs/ab/test_chunk", b"classified")
            .await
            .unwrap();

        // 3. Verify subdirectories are 0700 (rwx------)
        let sub_dir = repo_dir.join("blobs").join("ab");
        let sub_meta = std::fs::metadata(&sub_dir).unwrap();
        assert_eq!(sub_meta.permissions().mode() & 0o777, 0o700);

        // 4. Verify created file is 0600 (rw-------)
        let file_path = sub_dir.join("test_chunk");
        let file_meta = std::fs::metadata(&file_path).unwrap();
        assert_eq!(file_meta.permissions().mode() & 0o777, 0o600);
    }
}
