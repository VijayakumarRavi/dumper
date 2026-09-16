use crate::error::DumperError;
use crate::stream::decoder::StreamDecoder;
use crate::stream::encoder::StreamEncoder;
use std::collections::HashMap;
use tokio::io::{AsyncRead, AsyncWrite};

pub mod mysql;
pub mod postgres;

#[derive(Debug, Clone)]
pub struct DatabaseMeta {
    pub engine: String,
    pub database: String,
    pub server_version: String,
    pub table_names: Vec<(String, String)>, // (schema, table)
}

#[derive(Debug, Clone)]
pub struct BackupStats {
    pub engine: String,
    pub database: String,
    pub server_version: String,
    pub tables_backed_up: usize,
    pub rows_backed_up: u64,
    pub logical_bytes: u64,
}

#[derive(Debug, Clone)]
pub struct RestoreOptions {
    pub target_database_override: Option<String>,
    pub drop_existing: bool,
}

#[derive(Debug, Clone)]
pub struct RestoreStats {
    pub tables_restored: usize,
    pub records_processed: u64,
}

pub trait DatabaseAdapter: Send + Sync {
    fn inspect(
        &self,
    ) -> impl std::future::Future<Output = Result<DatabaseMeta, DumperError>> + Send;

    fn backup<W: AsyncWrite + Unpin + Send>(
        &self,
        encoder: &mut StreamEncoder<W>,
    ) -> impl std::future::Future<Output = Result<BackupStats, DumperError>> + Send;

    fn restore<R: AsyncRead + Unpin + Send>(
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
        let trimmed = url.trim();
        if trimmed.starts_with("postgres://") || trimmed.starts_with("postgresql://") {
            Ok(Self::Postgres(postgres::PostgresAdapter::new(trimmed)))
        } else if trimmed.starts_with("mysql://") || trimmed.starts_with("mariadb://") {
            Ok(Self::Mysql(mysql::MysqlAdapter::new(trimmed)))
        } else {
            let params = parse_key_value_pairs(trimmed);
            if !params.is_empty() {
                if is_mysql_key_value(&params) {
                    let mysql_url = build_mysql_url_from_params(&params)?;
                    Ok(Self::Mysql(mysql::MysqlAdapter::new(&mysql_url)))
                } else if is_postgres_key_value(&params, trimmed)
                    || trimmed.parse::<tokio_postgres::Config>().is_ok()
                {
                    Ok(Self::Postgres(postgres::PostgresAdapter::new(trimmed)))
                } else {
                    Err(DumperError::Config(format!(
                        "Could not determine database type from connection string '{}'.\n\
                         Supported formats:\n\
                         - PostgreSQL URI: postgres://[user[:pass]@]host[:port]/dbname\n\
                         - PostgreSQL key-value (libpq): host=... port=... dbname=... user=...\n\
                         - MySQL URI: mysql://[user[:pass]@]host[:port]/database\n\
                         - MySQL key-value: host=... port=... database=... user=...",
                        crate::error::sanitize_secrets(url)
                    )))
                }
            } else {
                Err(DumperError::Config(format!(
                    "Unsupported database connection string '{}'. Must start with postgres://, mysql://, or specify key-value parameters (e.g. host=... dbname=...)",
                    crate::error::sanitize_secrets(url)
                )))
            }
        }
    }

    pub async fn inspect(&self) -> Result<DatabaseMeta, DumperError> {
        match self {
            Self::Postgres(a) => a.inspect().await,
            Self::Mysql(a) => a.inspect().await,
        }
    }

    pub async fn backup<W: AsyncWrite + Unpin + Send>(
        &self,
        encoder: &mut StreamEncoder<W>,
    ) -> Result<BackupStats, DumperError> {
        match self {
            Self::Postgres(a) => a.backup(encoder).await,
            Self::Mysql(a) => a.backup(encoder).await,
        }
    }

    pub async fn restore<R: AsyncRead + Unpin + Send>(
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

/// Parses key-value connection strings.
/// Supports both space-separated (`host=localhost port=5432 ...`) and
/// semicolon-separated (`Host=localhost;Port=3306;...`) formats, with optional
/// single or double quotes around values containing spaces.
pub fn parse_key_value_pairs(s: &str) -> HashMap<String, String> {
    let mut map = HashMap::new();
    let chars: Vec<char> = s.chars().collect();
    let len = chars.len();
    let mut i = 0;

    while i < len {
        // Skip leading whitespace and semicolons
        while i < len && (chars[i].is_whitespace() || chars[i] == ';') {
            i += 1;
        }
        if i >= len {
            break;
        }

        // Read key (until '=' or delimiter or whitespace)
        let key_start = i;
        while i < len && chars[i] != '=' && !chars[i].is_whitespace() && chars[i] != ';' {
            i += 1;
        }
        let key = chars[key_start..i]
            .iter()
            .collect::<String>()
            .trim()
            .to_lowercase();

        // Skip whitespace before '='
        while i < len && chars[i].is_whitespace() {
            i += 1;
        }

        if i >= len || chars[i] != '=' {
            continue;
        }
        i += 1; // skip '='

        // Skip whitespace after '='
        while i < len && chars[i].is_whitespace() {
            i += 1;
        }
        if i >= len {
            if !key.is_empty() {
                map.insert(key, String::new());
            }
            break;
        }

        // Read value (could be quoted with ' or ")
        let mut value = String::new();
        if chars[i] == '\'' || chars[i] == '"' {
            let quote = chars[i];
            i += 1; // skip quote
            while i < len {
                if chars[i] == '\\' && i + 1 < len {
                    value.push(chars[i + 1]);
                    i += 2;
                } else if chars[i] == quote {
                    i += 1; // skip closing quote
                    break;
                } else {
                    value.push(chars[i]);
                    i += 1;
                }
            }
        } else {
            // Unquoted value: ends at whitespace or ';'
            while i < len && !chars[i].is_whitespace() && chars[i] != ';' {
                value.push(chars[i]);
                i += 1;
            }
        }

        if !key.is_empty() {
            map.insert(key, value);
        }
    }

    map
}

/// Checks if parsed key-value parameters represent a MySQL connection.
pub fn is_mysql_key_value(map: &HashMap<String, String>) -> bool {
    if map.contains_key("database") || map.contains_key("db") {
        return true;
    }
    if map.contains_key("ssl-mode") || map.contains_key("ssl_mode") {
        return true;
    }
    if map.contains_key("uid") || map.contains_key("pwd") || map.contains_key("server") {
        return true;
    }
    if let Some(port) = map.get("port") {
        if port == "3306" {
            return true;
        }
    }
    false
}

/// Constructs a MySQL URI (`mysql://...`) from parsed key-value parameters.
pub fn build_mysql_url_from_params(map: &HashMap<String, String>) -> Result<String, DumperError> {
    let host = map
        .get("host")
        .or_else(|| map.get("hostname"))
        .or_else(|| map.get("server"))
        .map(|s| s.as_str())
        .unwrap_or("127.0.0.1");

    let port = map.get("port").map(|s| s.as_str()).unwrap_or("3306");

    let db = map
        .get("database")
        .or_else(|| map.get("db"))
        .or_else(|| map.get("dbname"))
        .map(|s| s.as_str())
        .unwrap_or("");

    let user = map
        .get("user")
        .or_else(|| map.get("username"))
        .or_else(|| map.get("uid"))
        .map(|s| s.as_str());

    let password = map
        .get("password")
        .or_else(|| map.get("pass"))
        .or_else(|| map.get("pwd"))
        .map(|s| s.as_str());

    let auth = match (user, password) {
        (Some(u), Some(p)) => {
            let u_enc: String = url::form_urlencoded::byte_serialize(u.as_bytes()).collect();
            let p_enc: String = url::form_urlencoded::byte_serialize(p.as_bytes()).collect();
            format!("{}:{}@", u_enc, p_enc)
        }
        (Some(u), None) => {
            let u_enc: String = url::form_urlencoded::byte_serialize(u.as_bytes()).collect();
            format!("{}@", u_enc)
        }
        _ => String::new(),
    };

    let mut url = format!("mysql://{}{}:{}/{}", auth, host, port, db);

    let ssl_mode = map
        .get("ssl-mode")
        .or_else(|| map.get("ssl_mode"))
        .or_else(|| map.get("sslmode"));

    if let Some(mode) = ssl_mode {
        url.push_str(&format!("?ssl-mode={}", mode));
    }

    Ok(url)
}

/// Checks if parsed key-value parameters represent a PostgreSQL libpq connection.
pub fn is_postgres_key_value(map: &HashMap<String, String>, raw: &str) -> bool {
    if map.contains_key("dbname") {
        return true;
    }
    if let Some(port) = map.get("port") {
        if port == "5432" {
            return true;
        }
    }
    if map.contains_key("hostaddr")
        || map.contains_key("channel_binding")
        || map.contains_key("application_name")
    {
        return true;
    }
    if (map.contains_key("host") || map.contains_key("user"))
        && raw.parse::<tokio_postgres::Config>().is_ok()
    {
        return true;
    }
    false
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_from_url_postgres_uri() {
        let adapter =
            AnyDatabaseAdapter::from_url("postgres://postgres:secret@localhost:5432/mydb").unwrap();
        match adapter {
            AnyDatabaseAdapter::Postgres(_) => {}
            _ => panic!("Expected Postgres adapter"),
        }
    }

    #[test]
    fn test_from_url_mysql_uri() {
        let adapter =
            AnyDatabaseAdapter::from_url("mysql://root:secret@127.0.0.1:3306/mydb").unwrap();
        match adapter {
            AnyDatabaseAdapter::Mysql(_) => {}
            _ => panic!("Expected Mysql adapter"),
        }
    }

    #[test]
    fn test_from_url_libpq_key_value() {
        let conn_str =
            "host=localhost port=5432 user=admin password=secret dbname=mydb sslmode=require";
        let adapter = AnyDatabaseAdapter::from_url(conn_str).unwrap();
        match adapter {
            AnyDatabaseAdapter::Postgres(_) => {}
            _ => panic!("Expected Postgres adapter for libpq connection string"),
        }
    }

    #[test]
    fn test_from_url_libpq_with_quotes() {
        let conn_str = "host='db.example.com' user=\"my user\" password='p@ss word' dbname=app";
        let adapter = AnyDatabaseAdapter::from_url(conn_str).unwrap();
        match adapter {
            AnyDatabaseAdapter::Postgres(_) => {}
            _ => panic!("Expected Postgres adapter for quoted libpq connection string"),
        }
    }

    #[test]
    fn test_from_url_mysql_key_value() {
        let conn_str = "host=127.0.0.1 port=3306 user=root password=secret database=mydb";
        let adapter = AnyDatabaseAdapter::from_url(conn_str).unwrap();
        match adapter {
            AnyDatabaseAdapter::Mysql(_) => {}
            _ => panic!("Expected Mysql adapter for MySQL connection string"),
        }
    }

    #[test]
    fn test_from_url_mysql_semicolon_format() {
        let conn_str = "Server=127.0.0.1;Port=3306;Database=mydb;Uid=root;Pwd=secret;";
        let adapter = AnyDatabaseAdapter::from_url(conn_str).unwrap();
        match adapter {
            AnyDatabaseAdapter::Mysql(_) => {}
            _ => panic!("Expected Mysql adapter for semicolon connection string"),
        }
    }

    #[test]
    fn test_from_url_unsupported_string() {
        let res = AnyDatabaseAdapter::from_url("sqlite://test.db");
        assert!(res.is_err());
        let err = match res {
            Err(e) => e,
            Ok(_) => panic!("Expected error"),
        };
        assert!(err
            .to_string()
            .contains("Unsupported database connection string"));

        let res_kv = AnyDatabaseAdapter::from_url("foo=bar baz=qux");
        assert!(res_kv.is_err());
        let err_kv = match res_kv {
            Err(e) => e,
            Ok(_) => panic!("Expected error"),
        };
        assert!(err_kv
            .to_string()
            .contains("Could not determine database type"));
    }

    #[test]
    fn test_build_mysql_url_special_chars() {
        let mut map = HashMap::new();
        map.insert("host".into(), "localhost".into());
        map.insert("port".into(), "3306".into());
        map.insert("user".into(), "user@domain".into());
        map.insert("password".into(), "p@ss#123".into());
        map.insert("database".into(), "test_db".into());

        let url = build_mysql_url_from_params(&map).unwrap();
        assert_eq!(
            url,
            "mysql://user%40domain:p%40ss%23123@localhost:3306/test_db"
        );
    }
}
