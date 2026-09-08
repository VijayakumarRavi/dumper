use std::io::{self, BufRead};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use clap::Parser;
use sha2::Digest;
use dumper::cli::{Cli, Commands};
use dumper::database::{AnyDatabaseAdapter, RestoreOptions};
use dumper::error::{sanitize_secrets, DumperError};
use dumper::repository::backend::StorageBackend;
use dumper::repository::engine::RepositoryEngine;
use dumper::repository::local::LocalBackend;
use dumper::repository::lock::{LockType, RepositoryLock};
use dumper::repository::s3::client::S3Client;
use dumper::repository::snapshot::{BlobReference, SnapshotMetadata};
use dumper::retention::evaluate_retention;
use dumper::stats::{compute_stats, print_stats_table};
use dumper::stream::decoder::StreamDecoder;
use dumper::stream::encoder::StreamEncoder;
use dumper::ui::progress::{format_bytes, format_duration, ProgressEvent, ProgressReporter};

static INTERRUPTED: AtomicBool = AtomicBool::new(false);

#[tokio::main(flavor = "current_thread")]
async fn main() {
    // Setup signal handler for SIGINT (Ctrl+C) and SIGTERM
    tokio::spawn(async {
        if tokio::signal::ctrl_c().await.is_ok() {
            INTERRUPTED.store(true, Ordering::SeqCst);
            eprintln!("\nReceived interrupt signal. Aborting cleanly...");
            std::process::exit(dumper::error::exit_codes::INTERRUPTED);
        }
    });

    let cli = Cli::parse();
    let reporter = ProgressReporter::new(cli.quiet, cli.json);

    if let Err(err) = run(cli, &reporter).await {
        let code = err.exit_code();
        if reporter_is_json() {
            let event = ProgressEvent {
                event: "error",
                snapshot_id: None,
                database: None,
                engine: None,
                processed_bytes: None,
                stored_bytes: None,
                deduplicated_bytes: None,
                duration_seconds: None,
                message: Some(&err.to_string()),
            };
            reporter.emit_event(&event);
        } else {
            eprintln!("dumper: {}", err);
        }
        std::process::exit(code);
    }
}

fn reporter_is_json() -> bool {
    std::env::args().any(|a| a == "--json")
}

async fn run(cli: Cli, reporter: &ProgressReporter) -> Result<(), DumperError> {
    if let Commands::Version = cli.command {
        println!("dumper version {}", env!("CARGO_PKG_VERSION"));
        println!("repository format version: 1");
        println!("rustc target: {}", std::env::consts::ARCH);
        return Ok(());
    }

    let repo_url = cli.repository.as_deref().ok_or_else(|| {
        DumperError::Cli("Missing repository path or URL. Provide --repository or set DUMPER_REPOSITORY".into())
    })?;

    // Create storage backend: Local or S3
    if repo_url.starts_with("s3://") {
        let (bucket, prefix) = S3Client::parse_s3_url(repo_url)?;
        let access_key = cli.access_key_id.clone().or_else(|| std::env::var("AWS_ACCESS_KEY_ID").ok()).unwrap_or_default();
        let secret_key = cli.secret_access_key.clone().or_else(|| std::env::var("AWS_SECRET_ACCESS_KEY").ok()).unwrap_or_default();
        let s3_backend = Arc::new(S3Client::new(
            cli.endpoint.clone(),
            bucket,
            prefix,
            cli.region.clone(),
            access_key,
            secret_key,
            cli.session_token.clone(),
        )?);
        execute_command_with_backend(s3_backend, &cli.command, &cli, repo_url, reporter).await
    } else {
        let local_backend = Arc::new(LocalBackend::new(repo_url).await?);
        execute_command_with_backend(local_backend, &cli.command, &cli, repo_url, reporter).await
    }
}

async fn execute_command_with_backend<B: StorageBackend + 'static>(
    backend: Arc<B>,
    command: &Commands,
    cli: &Cli,
    repo_url: &str,
    reporter: &ProgressReporter,
) -> Result<(), DumperError> {
    match command {
        Commands::Init => {
            let password = resolve_password(cli, true)?;
            let _engine = RepositoryEngine::init(backend, &password).await?;
            reporter.log_info(&format!("Successfully initialized repository at '{}'", sanitize_secrets(repo_url)));
            Ok(())
        }
        Commands::Unlock(args) => {
            let removed = RepositoryLock::unlock_all(&*backend, args.force).await?;
            reporter.log_info(&format!("Removed {} lock(s)", removed));
            Ok(())
        }
        _ => {
            // All other commands require opening and unlocking repository with password
            let password = resolve_password(cli, false)?;
            let engine = RepositoryEngine::open(backend.clone(), &password).await?;
            dispatch_engine_command(backend, engine, command, cli, reporter).await
        }
    }
}

async fn dispatch_engine_command<B: StorageBackend + 'static>(
    backend: Arc<B>,
    engine: RepositoryEngine<B>,
    command: &Commands,
    cli: &Cli,
    reporter: &ProgressReporter,
) -> Result<(), DumperError> {
    match command {
        Commands::Backup(args) => {
            let start_time = chrono::Utc::now();
            let lock = RepositoryLock::acquire(&*backend, LockType::Shared).await?;

            let db_adapter = AnyDatabaseAdapter::from_url(&args.database_url)?;

            let meta = db_adapter.inspect().await?;
            reporter.log_info(&format!(
                "Backing up {} database '{}' (version: {})",
                meta.engine, meta.database, meta.server_version
            ));

            let snapshot_short_id = hex::encode(rand::random::<[u8; 4]>());
            let full_id = hex::encode(rand::random::<[u8; 16]>());

            let mut stored_blobs: Vec<BlobReference> = Vec::new();
            let mut total_stored_bytes = 0u64;
            let mut total_dedup_bytes = 0u64;

            // Piping streaming encoder -> chunker -> repository
            let compression_level = cli.compression;
            let mut intermediate_buffer = Vec::new();
            let mut encoder = StreamEncoder::new(&mut intermediate_buffer);

            let backup_stats = db_adapter.backup(&mut encoder).await?;
            let (logical_bytes, _) = encoder.finish()?;

            // Chunk and upload with bounded chunker
            let chunk_size = 2 * 1024 * 1024; // 2 MiB
            for chunk_slice in intermediate_buffer.chunks(chunk_size) {
                let hash_bytes = sha2::Sha256::digest(chunk_slice);
                let hash_hex = hex::encode(hash_bytes);
                let (blob_ref, was_dedup) = engine.put_chunk(chunk_slice, &hash_hex, compression_level).await?;

                if was_dedup {
                    total_dedup_bytes += blob_ref.raw_size;
                } else {
                    total_stored_bytes += blob_ref.stored_size;
                }
                stored_blobs.push(blob_ref);
            }

            let completed_time = chrono::Utc::now();
            let duration_seconds = (completed_time - start_time).num_seconds().max(0) as u64;

            let snapshot = SnapshotMetadata {
                id: snapshot_short_id.clone(),
                full_id,
                format_version: 1,
                dumper_version: env!("CARGO_PKG_VERSION").into(),
                engine: backup_stats.engine.clone(),
                database: backup_stats.database.clone(),
                server_version: backup_stats.server_version.clone(),
                started_at: start_time,
                completed_at: completed_time,
                duration_seconds,
                logical_bytes,
                stored_bytes: total_stored_bytes,
                deduplicated_bytes: total_dedup_bytes,
                table_count: backup_stats.tables_backed_up,
                compression: format!("{:?}", compression_level).to_lowercase(),
                tag: args.tag.clone(),
                blobs: stored_blobs,
            };

            // Commit snapshot metadata atomically
            engine.commit_snapshot(&snapshot).await?;
            lock.release(&*backend).await?;

            let event = ProgressEvent {
                event: "backup_complete",
                snapshot_id: Some(&snapshot_short_id),
                database: Some(&backup_stats.database),
                engine: Some(&backup_stats.engine),
                processed_bytes: Some(logical_bytes),
                stored_bytes: Some(total_stored_bytes),
                deduplicated_bytes: Some(total_dedup_bytes),
                duration_seconds: Some(duration_seconds),
                message: Some("Backup completed successfully"),
            };
            reporter.emit_event(&event);

            if !cli.json && !cli.quiet {
                println!();
                println!("Snapshot:       {}", snapshot_short_id);
                println!("Database:       {}", backup_stats.database);
                println!("Engine:         {}", backup_stats.engine);
                println!("Duration:       {}", format_duration(duration_seconds));
                println!("Logical size:   {}", format_bytes(logical_bytes));
                println!("Stored size:    {}", format_bytes(total_stored_bytes));
                println!("Deduplicated:   {}", format_bytes(total_dedup_bytes));
            }

            Ok(())
        }

        Commands::Snapshots => {
            let snapshots = engine.list_snapshots().await?;
            if cli.json {
                let json_str = serde_json::to_string_pretty(&snapshots)?;
                println!("{}", json_str);
            } else if snapshots.is_empty() {
                println!("No snapshots found in repository.");
            } else {
                println!("{:<10}  {:<20}  {:<12}  {:<12}  {:<10}  {:<10}", "ID", "DATE", "ENGINE", "DATABASE", "LOGICAL", "STORED");
                println!("{}", "-".repeat(80));
                for s in snapshots {
                    let date_str = s.started_at.format("%Y-%m-%d %H:%M:%S").to_string();
                    println!(
                        "{:<10}  {:<20}  {:<12}  {:<12}  {:<10}  {:<10}",
                        s.id,
                        date_str,
                        s.engine,
                        s.database,
                        format_bytes(s.logical_bytes),
                        format_bytes(s.stored_bytes)
                    );
                }
            }
            Ok(())
        }

        Commands::Info(args) => {
            let snapshot = engine.find_snapshot(&args.snapshot_id).await?;
            if cli.json {
                let json_str = serde_json::to_string_pretty(&snapshot)?;
                println!("{}", json_str);
            } else {
                println!("Snapshot ID:       {}", snapshot.id);
                println!("Full Hash:         {}", snapshot.full_id);
                println!("Database Engine:   {}", snapshot.engine);
                println!("Database Name:     {}", snapshot.database);
                println!("Server Version:    {}", snapshot.server_version);
                println!("Started At:        {}", snapshot.started_at);
                println!("Completed At:      {}", snapshot.completed_at);
                println!("Duration:          {}", format_duration(snapshot.duration_seconds));
                println!("Logical Size:      {}", format_bytes(snapshot.logical_bytes));
                println!("Stored Size:       {}", format_bytes(snapshot.stored_bytes));
                println!("Deduplicated:      {}", format_bytes(snapshot.deduplicated_bytes));
                println!("Blob Chunks:       {}", snapshot.blobs.len());
                println!("Compression:       {}", snapshot.compression);
                if let Some(tag) = snapshot.tag {
                    println!("Tag:               {}", tag);
                }
            }
            Ok(())
        }

        Commands::Restore(args) => {
            let snapshot = engine.find_snapshot(&args.snapshot_id).await?;
            let lock = RepositoryLock::acquire(&*backend, LockType::Shared).await?;

            reporter.log_info(&format!(
                "Restoring snapshot '{}' ({}) into target database...",
                snapshot.id, snapshot.database
            ));

            let db_adapter = AnyDatabaseAdapter::from_url(&args.target)?;

            // Stream reconstruct all chunks into a unified reader
            let mut reconstructed_stream = Vec::new();
            for (i, blob_ref) in snapshot.blobs.iter().enumerate() {
                reporter.log_info(&format!("Fetching chunk {}/{} ({})", i + 1, snapshot.blobs.len(), blob_ref.hash));
                let chunk_bytes = engine.get_chunk(&blob_ref.hash).await?;
                reconstructed_stream.extend_from_slice(&chunk_bytes);
            }

            let mut decoder = StreamDecoder::new(&reconstructed_stream[..]);
            let options = RestoreOptions {
                target_database_override: args.database.clone(),
                drop_existing: args.drop_existing,
            };

            let stats = db_adapter.restore(&mut decoder, &options).await?;
            lock.release(&*backend).await?;

            reporter.log_info(&format!(
                "Restore completed successfully! Processed {} records, restored {} tables.",
                stats.records_processed, stats.tables_restored
            ));
            Ok(())
        }

        Commands::Verify(args) => {
            let snapshot = engine.find_snapshot(&args.snapshot_id).await?;
            reporter.log_info(&format!("Verifying snapshot '{}' ({} chunks)...", snapshot.id, snapshot.blobs.len()));

            let count = engine.verify_snapshot(&snapshot).await?;
            reporter.log_info(&format!("Verified {} blob(s): hashes, decryption, and decompression OK", count));

            if args.restore_test {
                reporter.log_info("Running stream reconstruction test...");
                let mut stream_bytes = Vec::new();
                for b in &snapshot.blobs {
                    let chunk = engine.get_chunk(&b.hash).await?;
                    stream_bytes.extend_from_slice(&chunk);
                }
                let mut decoder = StreamDecoder::new(&stream_bytes[..]);
                let mut records_seen = 0u64;
                while let Some(_rec) = decoder.read_next_record()? {
                    records_seen += 1;
                }
                reporter.log_info(&format!("Stream test passed: {} records decoded with verified CRC32", records_seen));
            }

            Ok(())
        }

        Commands::Check => {
            reporter.log_info("Performing repository integrity check...");
            let (snapshots_count, missing, orphaned) = engine.check().await?;

            println!("Repository Integrity Check:");
            println!("  Committed Snapshots: {}", snapshots_count);
            println!("  Missing Blobs:       {}", missing);
            println!("  Orphaned Blobs:      {}", orphaned);

            if missing > 0 {
                return Err(DumperError::Integrity(format!(
                    "Repository check FAILED: {} missing blob(s) detected!",
                    missing
                )));
            }

            reporter.log_info("Repository integrity check PASSED.");
            Ok(())
        }

        Commands::Forget(args) => {
            let snapshots = engine.list_snapshots().await?;
            let plan = evaluate_retention(&snapshots, args);

            if cli.json {
                let keep_ids: Vec<&str> = plan.keep.iter().map(|s| s.id.as_str()).collect();
                let remove_ids: Vec<&str> = plan.remove.iter().map(|s| s.id.as_str()).collect();
                println!("{}", serde_json::json!({ "keep": keep_ids, "remove": remove_ids }));
            } else {
                println!("Retention policy evaluation:");
                println!("  Keeping:   {} snapshot(s)", plan.keep.len());
                for s in &plan.keep {
                    println!("    + {} ({})", s.id, s.started_at);
                }
                println!("  Removing:  {} snapshot(s)", plan.remove.len());
                for s in &plan.remove {
                    println!("    - {} ({})", s.id, s.started_at);
                }
            }

            if !args.dry_run {
                let lock = RepositoryLock::acquire(&*backend, LockType::Exclusive).await?;
                for s in plan.remove {
                    engine.delete_snapshot(&s.id).await?;
                }
                lock.release(&*backend).await?;

                if args.prune {
                    reporter.log_info("Pruning unreferenced blobs...");
                    let prune_lock = RepositoryLock::acquire(&*backend, LockType::Exclusive).await?;
                    let (deleted_count, deleted_bytes) = engine.prune().await?;
                    prune_lock.release(&*backend).await?;
                    reporter.log_info(&format!(
                        "Pruned {} unreferenced blob(s) ({})",
                        deleted_count, format_bytes(deleted_bytes)
                    ));
                }
            }

            Ok(())
        }

        Commands::Prune => {
            reporter.log_info("Acquiring exclusive lock for prune...");
            let lock = RepositoryLock::acquire(&*backend, LockType::Exclusive).await?;
            let (deleted_count, deleted_bytes) = engine.prune().await?;
            lock.release(&*backend).await?;

            reporter.log_info(&format!(
                "Prune complete: deleted {} unreferenced blob(s), freed {}",
                deleted_count, format_bytes(deleted_bytes)
            ));
            Ok(())
        }

        Commands::Stats(args) => {
            let snapshots = engine.list_snapshots().await?;
            let stats = compute_stats(&*backend, &snapshots, args.full).await?;

            if cli.json {
                println!("{}", serde_json::to_string_pretty(&stats)?);
            } else {
                print_stats_table(&stats);
            }
            Ok(())
        }

        _ => Ok(()),
    }
}

fn resolve_password(cli: &Cli, is_init: bool) -> Result<String, DumperError> {
    if let Some(ref pass) = cli.password {
        return Ok(pass.clone());
    }
    if let Ok(pass) = std::env::var("DUMPER_PASSWORD") {
        if !pass.is_empty() {
            return Ok(pass);
        }
    }
    if let Some(ref file_path) = cli.password_file {
        let content = std::fs::read_to_string(file_path).map_err(|e| {
            DumperError::Config(format!("Failed to read password file '{}': {}", file_path, e))
        })?;
        return Ok(content.trim().to_string());
    }
    if let Ok(file_path) = std::env::var("DUMPER_PASSWORD_FILE") {
        let content = std::fs::read_to_string(&file_path).map_err(|e| {
            DumperError::Config(format!("Failed to read password file '{}': {}", file_path, e))
        })?;
        return Ok(content.trim().to_string());
    }

    // Interactive prompt
    let prompt = if is_init {
        "Enter new repository password: "
    } else {
        "Enter repository password: "
    };

    eprint!("{}", prompt);
    let mut input = String::new();
    let stdin = io::stdin();
    stdin.lock().read_line(&mut input).map_err(|e| {
        DumperError::Authentication(format!("Failed to read password from stdin: {}", e))
    })?;

    let trimmed = input.trim().to_string();
    if trimmed.is_empty() {
        return Err(DumperError::Authentication("Password cannot be empty".into()));
    }
    Ok(trimmed)
}
