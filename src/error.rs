use std::fmt;

/// Exit codes as defined in the Dumper specification.
pub mod exit_codes {
    pub const SUCCESS: i32 = 0;
    pub const GENERAL_ERROR: i32 = 1;
    pub const USAGE_OR_CONFIG_ERROR: i32 = 2;
    pub const AUTHENTICATION_ERROR: i32 = 3;
    pub const DATABASE_ERROR: i32 = 4;
    pub const REPOSITORY_ERROR: i32 = 5;
    pub const INTEGRITY_ERROR: i32 = 6;
    pub const RESTORE_ERROR: i32 = 7;
    pub const INTERRUPTED: i32 = 8;
}

#[derive(Debug)]
pub enum DumperError {
    Cli(String),
    Config(String),
    Authentication(String),
    Database(String),
    Repository(String),
    S3(String),
    Crypto(String),
    Format(String),
    Integrity(String),
    Restore(String),
    Interrupted,
    Io(std::io::Error),
}

impl DumperError {
    pub fn exit_code(&self) -> i32 {
        match self {
            DumperError::Cli(_) | DumperError::Config(_) => exit_codes::USAGE_OR_CONFIG_ERROR,
            DumperError::Authentication(_) => exit_codes::AUTHENTICATION_ERROR,
            DumperError::Database(_) => exit_codes::DATABASE_ERROR,
            DumperError::Repository(_) | DumperError::S3(_) => exit_codes::REPOSITORY_ERROR,
            DumperError::Crypto(_) | DumperError::Integrity(_) => exit_codes::INTEGRITY_ERROR,
            DumperError::Restore(_) => exit_codes::RESTORE_ERROR,
            DumperError::Interrupted => exit_codes::INTERRUPTED,
            DumperError::Format(_) => exit_codes::GENERAL_ERROR,
            DumperError::Io(_) => exit_codes::GENERAL_ERROR,
        }
    }
}

impl fmt::Display for DumperError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DumperError::Cli(msg) => write!(f, "CLI error: {}", sanitize_secrets(msg)),
            DumperError::Config(msg) => write!(f, "Configuration error: {}", sanitize_secrets(msg)),
            DumperError::Authentication(msg) => {
                write!(f, "Authentication error: {}", sanitize_secrets(msg))
            }
            DumperError::Database(msg) => write!(f, "Database error: {}", sanitize_secrets(msg)),
            DumperError::Repository(msg) => {
                write!(f, "Repository error: {}", sanitize_secrets(msg))
            }
            DumperError::S3(msg) => write!(f, "S3 storage error: {}", sanitize_secrets(msg)),
            DumperError::Crypto(msg) => write!(f, "Cryptography error: {}", sanitize_secrets(msg)),
            DumperError::Format(msg) => write!(f, "Format error: {}", sanitize_secrets(msg)),
            DumperError::Integrity(msg) => {
                write!(f, "Integrity verification error: {}", sanitize_secrets(msg))
            }
            DumperError::Restore(msg) => write!(f, "Restore error: {}", sanitize_secrets(msg)),
            DumperError::Interrupted => write!(f, "Operation interrupted by signal"),
            DumperError::Io(err) => write!(f, "I/O error: {}", sanitize_secrets(&err.to_string())),
        }
    }
}

impl std::error::Error for DumperError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            DumperError::Io(err) => Some(err),
            _ => None,
        }
    }
}

impl From<std::io::Error> for DumperError {
    fn from(err: std::io::Error) -> Self {
        DumperError::Io(err)
    }
}

impl From<serde_json::Error> for DumperError {
    fn from(err: serde_json::Error) -> Self {
        DumperError::Format(err.to_string())
    }
}

/// Sanitizes potential passwords or access keys in database URLs or text.
pub fn sanitize_secrets(input: &str) -> String {
    // Check if input contains a database URL with a password pattern: scheme://user:password@host
    let mut result = input.to_string();
    if let Ok(mut url) = url::Url::parse(input) {
        if url.password().is_some() {
            let _ = url.set_password(Some("*****"));
            return url.to_string();
        }
    }

    // Heuristic regex-like substitution for passwords in connection strings or keys
    let schemes = [
        "postgres://",
        "postgresql://",
        "mysql://",
        "mariadb://",
        "http://",
        "https://",
    ];
    for scheme in &schemes {
        if let Some(pos) = result.find(scheme) {
            let rest = &result[pos + scheme.len()..];
            if let Some(at_pos) = rest.find('@') {
                let user_info = &rest[..at_pos];
                if let Some(colon_pos) = user_info.find(':') {
                    let sanitized = format!(
                        "{}{}{}:*****{}",
                        &result[..pos],
                        scheme,
                        &user_info[..colon_pos],
                        &rest[at_pos..]
                    );
                    result = sanitized;
                }
            }
        }
    }

    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sanitize_database_url() {
        let raw = "postgres://admin:super_secret_123@db.example.com:5432/mydb";
        let sanitized = sanitize_secrets(raw);
        assert!(!sanitized.contains("super_secret_123"));
        assert!(sanitized.contains("admin:*****@db.example.com"));

        let raw_mysql = "mysql://user:pass@127.0.0.1:3306/app";
        let sanitized_mysql = sanitize_secrets(raw_mysql);
        assert!(!sanitized_mysql.contains("pass"));
        assert!(sanitized_mysql.contains("user:*****@127.0.0.1"));
    }

    #[test]
    fn test_exit_codes() {
        assert_eq!(
            DumperError::Authentication("bad pass".into()).exit_code(),
            3
        );
        assert_eq!(
            DumperError::Database("connection failed".into()).exit_code(),
            4
        );
        assert_eq!(
            DumperError::Integrity("hash mismatch".into()).exit_code(),
            6
        );
        assert_eq!(DumperError::Interrupted.exit_code(), 8);
    }
}
