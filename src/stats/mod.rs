use std::collections::HashSet;
use serde::Serialize;
use crate::error::DumperError;
use crate::repository::backend::StorageBackend;
use crate::repository::snapshot::SnapshotMetadata;
use crate::ui::progress::format_bytes;

#[derive(Serialize, Debug)]
pub struct RepositoryStats {
    pub total_snapshots: usize,
    pub unique_blobs: usize,
    pub repository_stored_bytes: u64,
    pub total_logical_bytes: u64,
    pub deduplication_ratio: f64,
}

pub async fn compute_stats<B: StorageBackend>(
    backend: &B,
    snapshots: &[SnapshotMetadata],
    full_scan: bool,
) -> Result<RepositoryStats, DumperError> {
    let mut unique_hashes = HashSet::new();
    let mut total_logical = 0u64;
    let mut stored_bytes_from_refs = 0u64;

    for s in snapshots {
        total_logical += s.logical_bytes;
        for b in &s.blobs {
            if unique_hashes.insert(b.hash.clone()) {
                stored_bytes_from_refs += b.stored_size;
            }
        }
    }

    let actual_stored_bytes = if full_scan {
        let all_blobs = backend.list_objects("blobs").await?;
        let mut total = 0u64;
        for blob_key in all_blobs {
            if let Ok(data) = backend.get_object(&blob_key).await {
                total += data.len() as u64;
            }
        }
        total
    } else {
        stored_bytes_from_refs
    };

    let dedup_ratio = if actual_stored_bytes > 0 {
        total_logical as f64 / actual_stored_bytes as f64
    } else {
        1.0
    };

    Ok(RepositoryStats {
        total_snapshots: snapshots.len(),
        unique_blobs: unique_hashes.len(),
        repository_stored_bytes: actual_stored_bytes,
        total_logical_bytes: total_logical,
        deduplication_ratio: (dedup_ratio * 10.0).round() / 10.0,
    })
}

pub fn print_stats_table(stats: &RepositoryStats) {
    println!("Snapshots:             {}", stats.total_snapshots);
    println!("Unique blobs:          {}", stats.unique_blobs);
    println!("Repository size:       {}", format_bytes(stats.repository_stored_bytes));
    println!("Logical backup size:   {}", format_bytes(stats.total_logical_bytes));
    println!("Deduplication ratio:   {:.1}x", stats.deduplication_ratio);
}
