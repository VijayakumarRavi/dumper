use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct BlobReference {
    pub hash: String,
    pub raw_size: u64,
    pub stored_size: u64,
    pub compression_tag: u8,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct SnapshotMetadata {
    pub id: String,      // Short 8-char identifier
    pub full_id: String, // Full SHA-256 identifier
    pub format_version: u32,
    pub dumper_version: String,
    pub engine: String,
    pub database: String,
    pub server_version: String,
    pub started_at: DateTime<Utc>,
    pub completed_at: DateTime<Utc>,
    pub duration_seconds: u64,
    pub logical_bytes: u64,
    pub stored_bytes: u64,
    pub deduplicated_bytes: u64,
    pub table_count: usize,
    pub compression: String,
    pub tag: Option<String>,
    pub blobs: Vec<BlobReference>,
}

impl SnapshotMetadata {
    pub fn blob_path(hash: &str) -> String {
        let prefix = if hash.len() >= 2 { &hash[..2] } else { "xx" };
        format!("blobs/{}/{}", prefix, hash)
    }

    pub fn snapshot_path(id: &str) -> String {
        format!("snapshots/{}", id)
    }
}
