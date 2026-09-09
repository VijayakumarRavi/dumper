use clap::{Args, Parser, Subcommand, ValueEnum};

#[derive(Parser, Debug)]
#[command(
    name = "dumper",
    about = "A tiny, production-grade database backup, restore, and repository-management CLI",
    version,
    arg_required_else_help = true
)]
pub struct Cli {
    #[arg(
        short = 'r',
        long = "repository",
        env = "DUMPER_REPOSITORY",
        help = "Repository location (local path or s3://bucket/prefix)"
    )]
    pub repository: Option<String>,

    #[arg(
        long = "password-file",
        env = "DUMPER_PASSWORD_FILE",
        help = "Path to file containing repository encryption password"
    )]
    pub password_file: Option<String>,

    #[arg(
        long = "password",
        env = "DUMPER_PASSWORD",
        help = "Repository encryption password (avoid passing via CLI flags in production)"
    )]
    pub password: Option<String>,

    #[arg(
        long = "endpoint",
        env = "DUMPER_S3_ENDPOINT",
        help = "Custom S3 endpoint URL (e.g. MinIO, Cloudflare R2, Garage)"
    )]
    pub endpoint: Option<String>,

    #[arg(
        long = "region",
        env = "DUMPER_S3_REGION",
        default_value = "us-east-1",
        help = "S3 region"
    )]
    pub region: String,

    #[arg(
        long = "s3-access-key-id",
        env = "DUMPER_S3_ACCESS_KEY_ID",
        help = "S3 access key ID"
    )]
    pub access_key_id: Option<String>,

    #[arg(
        long = "s3-secret-access-key",
        env = "DUMPER_S3_SECRET_ACCESS_KEY",
        help = "S3 secret access key"
    )]
    pub secret_access_key: Option<String>,

    #[arg(
        long = "s3-session-token",
        env = "DUMPER_S3_SESSION_TOKEN",
        help = "S3 temporary session token"
    )]
    pub session_token: Option<String>,

    #[arg(
        short = 'q',
        long = "quiet",
        help = "Quiet mode (suppress progress output)"
    )]
    pub quiet: bool,

    #[arg(
        short = 'v',
        long = "verbose",
        help = "Verbose mode (more detailed diagnostic logs)"
    )]
    pub verbose: bool,

    #[arg(long = "json", help = "Emit machine-readable JSON output")]
    pub json: bool,

    #[arg(
        long = "parallel",
        default_value = "1",
        help = "Concurrency worker count (conservative default = 1)"
    )]
    pub parallel: usize,

    #[arg(
        long = "compression",
        default_value = "default",
        value_enum,
        help = "Compression algorithm and level"
    )]
    pub compression: CompressionLevel,

    #[command(subcommand)]
    pub command: Commands,
}

#[derive(ValueEnum, Clone, Copy, Debug, PartialEq, Eq)]
pub enum CompressionLevel {
    None,
    Fast,
    Default,
    Max,
}

#[derive(Subcommand, Debug)]
pub enum Commands {
    #[command(about = "Initialize a new repository")]
    Init,

    #[command(about = "Stream a consistent database backup into the repository")]
    Backup(BackupArgs),

    #[command(about = "List committed snapshots in the repository")]
    Snapshots,

    #[command(about = "Show detailed metadata for a snapshot")]
    Info(SnapshotIdArgs),

    #[command(about = "Stream restore a snapshot to a target database")]
    Restore(RestoreArgs),

    #[command(about = "Verify snapshot data integrity (hashes, encryption, decompression)")]
    Verify(VerifyArgs),

    #[command(about = "Perform repository-wide consistency and integrity check")]
    Check,

    #[command(about = "Apply retention rules and remove older snapshot references")]
    Forget(ForgetArgs),

    #[command(about = "Garbage collect unreferenced repository blobs")]
    Prune,

    #[command(about = "Show repository statistics and deduplication ratio")]
    Stats(StatsArgs),

    #[command(about = "Remove stale repository locks")]
    Unlock(UnlockArgs),

    #[command(about = "Show version and build details")]
    Version,
}

#[derive(Args, Debug)]
pub struct BackupArgs {
    #[arg(
        help = "Database URL (postgres://user:pass@host:port/db or mysql://user:pass@host:port/db)"
    )]
    pub database_url: String,

    #[arg(long = "tag", help = "Optional user tag for snapshot metadata")]
    pub tag: Option<String>,
}

#[derive(Args, Debug)]
pub struct SnapshotIdArgs {
    #[arg(help = "Snapshot ID (short 8-char or full hex)")]
    pub snapshot_id: String,
}

#[derive(Args, Debug)]
pub struct RestoreArgs {
    #[arg(help = "Snapshot ID to restore")]
    pub snapshot_id: String,

    #[arg(
        long = "target",
        required = true,
        help = "Target database connection URL (postgres://... or mysql://...)"
    )]
    pub target: String,

    #[arg(long = "database", help = "Optional target database name override")]
    pub database: Option<String>,

    #[arg(long = "drop-existing", help = "Drop tables before restoring them")]
    pub drop_existing: bool,
}

#[derive(Args, Debug)]
pub struct VerifyArgs {
    #[arg(help = "Snapshot ID to verify")]
    pub snapshot_id: String,

    #[arg(
        long = "restore-test",
        help = "Fully decode and reconstruct record stream without writing to a database"
    )]
    pub restore_test: bool,
}

#[derive(Args, Debug, Clone)]
pub struct ForgetArgs {
    #[arg(long = "keep-last", help = "Keep the most recent N snapshots")]
    pub keep_last: Option<usize>,

    #[arg(
        long = "keep-daily",
        help = "Keep 1 snapshot per day for the last N days with backups"
    )]
    pub keep_daily: Option<usize>,

    #[arg(
        long = "keep-weekly",
        help = "Keep 1 snapshot per week for the last N weeks with backups"
    )]
    pub keep_weekly: Option<usize>,

    #[arg(
        long = "keep-monthly",
        help = "Keep 1 snapshot per month for the last N months with backups"
    )]
    pub keep_monthly: Option<usize>,

    #[arg(
        long = "database",
        help = "Only apply retention policy to snapshots of this database"
    )]
    pub database: Option<String>,

    #[arg(
        long = "tag",
        help = "Only apply retention policy to snapshots with this tag"
    )]
    pub tag: Option<String>,

    #[arg(
        long = "prune",
        help = "Automatically prune unreferenced blobs after forgetting"
    )]
    pub prune: bool,

    #[arg(
        long = "dry-run",
        help = "Simulate retention without actually deleting snapshot records"
    )]
    pub dry_run: bool,
}

#[derive(Args, Debug)]
pub struct StatsArgs {
    #[arg(
        long = "full",
        help = "Scan all blob objects to compute exact storage size"
    )]
    pub full: bool,
}

#[derive(Args, Debug)]
pub struct UnlockArgs {
    #[arg(long = "force", help = "Force removal of locks even if not expired")]
    pub force: bool,
}
