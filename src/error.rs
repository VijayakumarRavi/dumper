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
    // 1. Direct database URL with a password pattern: scheme://user:password@host
    if let Ok(mut url) = url::Url::parse(input) {
        if url.password().is_some() {
            let _ = url.set_password(Some("*****"));
            return url.to_string();
        }
    }

    let mut result = input.to_string();

    // 2. Embedded URLs with credentials
    let schemes = [
        "postgres://",
        "postgresql://",
        "mysql://",
        "mariadb://",
        "http://",
        "https://",
    ];

    for scheme in &schemes {
        let mut search_from = 0;
        while let Some(rel_pos) = result[search_from..].find(scheme) {
            let pos = search_from + rel_pos;
            let after_scheme = pos + scheme.len();
            let rest = &result[after_scheme..];

            // Look for '@' before any whitespace, quotes, or another scheme
            let end_candidate = rest
                .find(|c: char| c.is_whitespace() || c == '"' || c == '\'' || c == '`')
                .unwrap_or(rest.len());
            let candidate = &rest[..end_candidate];

            if let Some(at_pos) = candidate.find('@') {
                let user_info = &candidate[..at_pos];
                if let Some(colon_pos) = user_info.find(':') {
                    let sanitized = format!(
                        "{}{}{}:*****{}",
                        &result[..pos],
                        scheme,
                        &user_info[..colon_pos],
                        &result[after_scheme + at_pos..]
                    );
                    search_from = pos + scheme.len() + colon_pos + 6; // advance past ":*****@"
                    result = sanitized;
                    continue;
                }
            }
            search_from = after_scheme;
        }
    }

    // 3. Sanitize S3 XML error tags containing sensitive signing or token information
    let xml_tags = ["CanonicalRequest", "StringToSign", "BinarySecurityToken"];
    for tag in &xml_tags {
        let open_tag = format!("<{}>", tag);
        let close_tag = format!("</{}>", tag);
        let mut search_from = 0;
        while let Some(rel_start) = result[search_from..].find(&open_tag) {
            let start = search_from + rel_start;
            let inner_start = start + open_tag.len();
            if let Some(rel_end) = result[inner_start..].find(&close_tag) {
                let inner_end = inner_start + rel_end;
                result = format!("{}*****{}", &result[..inner_start], &result[inner_end..]);
                search_from = inner_start + 5 + close_tag.len();
            } else {
                break;
            }
        }
    }

    // 4. Sanitize key-value pairs (libpq, MySQL, S3 headers / config)
    let secret_keys = [
        "aws_secret_access_key",
        "secret_access_key",
        "secret_key",
        "x-amz-security-token",
        "session_token",
        "api_key",
        "password",
        "passwd",
        "pwd",
        "secret",
        "token",
    ];

    sanitize_key_values(&result, &secret_keys)
}

fn sanitize_key_values(input: &str, keys: &[&str]) -> String {
    let mut result = input.to_string();

    for &key in keys {
        let mut search_from = 0;
        while search_from < result.len() {
            let lower_sub = result[search_from..].to_lowercase();
            let Some(rel_pos) = lower_sub.find(key) else {
                break;
            };
            let key_pos = search_from + rel_pos;

            // Ensure key is preceded by start of string or a non-alphanumeric delimiter
            if key_pos > 0 {
                if let Some(prev_char) = result[..key_pos].chars().next_back() {
                    if prev_char.is_ascii_alphanumeric() || prev_char == '_' || prev_char == '-' {
                        search_from = key_pos + key.len();
                        continue;
                    }
                }
            }

            // After the key, allow optional whitespace, then require '=' or ':'
            let after_key = key_pos + key.len();
            let rem = &result[after_key..];
            let trimmed = rem.trim_start();
            let whitespace_len = rem.len() - trimmed.len();

            let (_sep_char, sep_offset) = if trimmed.starts_with('=') {
                ('=', whitespace_len + 1)
            } else if trimmed.starts_with(':') {
                (':', whitespace_len + 1)
            } else {
                search_from = after_key;
                continue;
            };

            let after_sep = after_key + sep_offset;
            let val_rem = &result[after_sep..];
            let val_trimmed = val_rem.trim_start();
            let val_whitespace_len = val_rem.len() - val_trimmed.len();
            let val_start = after_sep + val_whitespace_len;

            if val_start >= result.len() {
                break;
            }

            // Determine value extent (quoted or unquoted)
            let (val_end, replacement) = if result[val_start..].starts_with('\'') {
                let quote_start = val_start + 1;
                if let Some(close_quote) = result[quote_start..].find('\'') {
                    (quote_start + close_quote + 1, "'*****'")
                } else {
                    (result.len(), "'*****'")
                }
            } else if result[val_start..].starts_with('"') {
                let quote_start = val_start + 1;
                if let Some(close_quote) = result[quote_start..].find('"') {
                    (quote_start + close_quote + 1, "\"*****\"")
                } else {
                    (result.len(), "\"*****\"")
                }
            } else {
                // Unquoted value: ends at next whitespace, comma, semicolon, ampersand, or end of string
                let end_offset = result[val_start..]
                    .find(|c: char| c.is_whitespace() || c == ',' || c == ';' || c == '&')
                    .unwrap_or(result[val_start..].len());
                (val_start + end_offset, "*****")
            };

            // Don't re-sanitize if already masked
            if &result[val_start..val_end] == replacement || &result[val_start..val_end] == "*****"
            {
                search_from = val_end;
                continue;
            }

            let sanitized = format!(
                "{}{}{}",
                &result[..val_start],
                replacement,
                &result[val_end..]
            );
            search_from = val_start + replacement.len();
            result = sanitized;
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
    fn test_sanitize_multiple_database_urls() {
        let raw = "Replication failed between postgres://admin:secret1@host1:5432/db and postgres://backup:secret2@host2:5432/db";
        let sanitized = sanitize_secrets(raw);
        assert!(!sanitized.contains("secret1"));
        assert!(!sanitized.contains("secret2"));
        assert!(sanitized.contains("admin:*****@host1"));
        assert!(sanitized.contains("backup:*****@host2"));
    }

    #[test]
    fn test_sanitize_key_value_connection_strings() {
        // SEC-08: libpq key-value format and variations
        let libpq = "host=localhost port=5432 user=postgres password=supersecret dbname=mydb";
        let sanitized = sanitize_secrets(libpq);
        assert!(!sanitized.contains("supersecret"));
        assert!(sanitized.contains("password=*****"));
        assert!(sanitized.contains("user=postgres"));

        let quoted_single = "host=localhost password = 'super secret with spaces' dbname=mydb";
        let sanitized_single = sanitize_secrets(quoted_single);
        assert!(!sanitized_single.contains("super secret with spaces"));
        assert!(sanitized_single.contains("password = '*****'"));

        let quoted_double = "user=admin password=\"my_pass_123\" host=127.0.0.1";
        let sanitized_double = sanitize_secrets(quoted_double);
        assert!(!sanitized_double.contains("my_pass_123"));
        assert!(sanitized_double.contains("password=\"*****\""));

        // Case-insensitive matching
        let uppercase = "PASSWORD=secret123";
        assert_eq!(sanitize_secrets(uppercase), "PASSWORD=*****");

        // Substring boundary: has_password should not be sanitized
        let flag = "has_password=true";
        assert_eq!(sanitize_secrets(flag), "has_password=true");
    }

    #[test]
    fn test_sanitize_s3_error_payloads() {
        // SEC-08: S3 XML error with CanonicalRequest & StringToSign
        let s3_xml = "<Error><Code>SignatureDoesNotMatch</Code><Message>Signature mismatch</Message><CanonicalRequest>GET\n/bucket\n\nx-amz-security-token:AQoDYXdzEJr123456\n</CanonicalRequest><StringToSign>AWS4-HMAC-SHA256\n20260928T120000Z\n...</StringToSign></Error>";
        let sanitized = sanitize_secrets(s3_xml);
        assert!(!sanitized.contains("AQoDYXdzEJr123456"));
        assert!(sanitized.contains("<CanonicalRequest>*****</CanonicalRequest>"));
        assert!(sanitized.contains("<StringToSign>*****</StringToSign>"));

        // Header style tokens
        let header =
            "Failed request with x-amz-security-token: AQoDYXdzEJr123456 and secret_key=abcdef";
        let sanitized_header = sanitize_secrets(header);
        assert!(!sanitized_header.contains("AQoDYXdzEJr123456"));
        assert!(!sanitized_header.contains("abcdef"));
        assert!(sanitized_header.contains("x-amz-security-token: *****"));
        assert!(sanitized_header.contains("secret_key=*****"));
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
