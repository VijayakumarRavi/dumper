use std::io::Write;
use crate::error::DumperError;
use crate::stream::decoder::StreamDecoder;
use crate::stream::encoder::StreamEncoder;

pub mod mysql;
pub mod postgres;

pub struct DatabaseMeta {
    pub engine: String,
    pub database: String,
    pub server_version: String,
    pub table_names: Vec<(String, String)>, // (schema, table)
}

pub struct BackupStats {
    pub engine: String,
    pub database: String,
    pub server_version: String,
    pub tables_backed_up: usize,
    pub rows_backed_up: u64,
    pub logical_bytes: u64,
}

pub struct RestoreOptions {
    pub target_database_override: Option<String>,
    pub drop_existing: bool,
}

pub struct RestoreStats {
    pub tables_restored: usize,
    pub records_processed: u64,
}

pub trait DatabaseAdapter: Send + Sync {
    fn inspect(&self) -> impl std::future::Future<Output = Result<DatabaseMeta, DumperError>> + Send;

    fn backup<W: Write + Send>(
        &self,
        encoder: &mut StreamEncoder<W>,
    ) -> impl std::future::Future<Output = Result<BackupStats, DumperError>> + Send;

    fn restore<R: std::io::Read + Send>(
        &self,
        decoder: &mut StreamDecoder<R>,
        options: &RestoreOptions,
    ) -> impl std::future::Future<Output = Result<RestoreStats, DumperError>> + Send;
}

pub enum AnyDatabaseAdapter {
    Postgres(postgres::PostgresAdapter),
    Mysql(mysql::MysqlAdapter),
}

impl AnyDatabaseAdapter {
    pub fn from_url(url: &str) -> Result<Self, DumperError> {
        if url.starts_with("postgres://") || url.starts_with("postgresql://") {
            Ok(Self::Postgres(postgres::PostgresAdapter::new(url)))
        } else if url.starts_with("mysql://") || url.starts_with("mariadb://") {
            Ok(Self::Mysql(mysql::MysqlAdapter::new(url)))
        } else {
            Err(DumperError::Config(format!(
                "Unsupported database scheme in URL '{}'. Must be postgres:// or mysql://",
                crate::error::sanitize_secrets(url)
            )))
        }
    }

    pub async fn inspect(&self) -> Result<DatabaseMeta, DumperError> {
        match self {
            Self::Postgres(a) => a.inspect().await,
            Self::Mysql(a) => a.inspect().await,
        }
    }

    pub async fn backup<W: Write + Send>(
        &self,
        encoder: &mut StreamEncoder<W>,
    ) -> Result<BackupStats, DumperError> {
        match self {
            Self::Postgres(a) => a.backup(encoder).await,
            Self::Mysql(a) => a.backup(encoder).await,
        }
    }

    pub async fn restore<R: std::io::Read + Send>(
        &self,
        decoder: &mut StreamDecoder<R>,
        options: &RestoreOptions,
    ) -> Result<RestoreStats, DumperError> {
        match self {
            Self::Postgres(a) => a.restore(decoder, options).await,
            Self::Mysql(a) => a.restore(decoder, options).await,
        }
    }
}
