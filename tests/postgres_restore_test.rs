use dumper::database::postgres::PostgresAdapter;
use dumper::database::{DatabaseAdapter, RestoreOptions};
use dumper::stream::decoder::StreamDecoder;
use dumper::stream::encoder::StreamEncoder;
use dumper::stream::format::*;
use std::process::Command;
use tempfile::TempDir;

static PG_PORT_COUNTER: std::sync::atomic::AtomicU16 = std::sync::atomic::AtomicU16::new(0);

static PG_TEST_MUTEX: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

struct TestPgServer {
    dir: TempDir,
    port: u16,
}

impl TestPgServer {
    fn start() -> Option<Self> {
        let dir = TempDir::new().ok()?;
        let path = dir.path().to_str()?;
        let offset = PG_PORT_COUNTER.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        let port = 54300 + ((std::process::id() as u16 % 200) * 10) + offset;

        let init_status = Command::new("initdb")
            .args(["-D", path, "--no-sync", "-A", "trust", "-U", "postgres"])
            .output()
            .ok()?;
        if !init_status.status.success() {
            eprintln!(
                "initdb failed: stdout={}, stderr={}",
                String::from_utf8_lossy(&init_status.stdout),
                String::from_utf8_lossy(&init_status.stderr)
            );
            return None;
        }

        use std::io::Write;
        if let Ok(mut conf_file) = std::fs::OpenOptions::new()
            .append(true)
            .open(format!("{}/postgresql.conf", path))
        {
            let _ = writeln!(conf_file, "\nunix_socket_directories = '{}'", path);
            let _ = writeln!(conf_file, "listen_addresses = '127.0.0.1'");
        }

        let log_file = format!("{}/pg.log", path);
        let start_status = Command::new("pg_ctl")
            .args([
                "-D",
                path,
                "-l",
                &log_file,
                "-o",
                &format!("-p {} -k '{}' -c listen_addresses='127.0.0.1'", port, path),
                "-w",
                "start",
            ])
            .output()
            .ok()?;
        if !start_status.status.success() {
            let pg_log = std::fs::read_to_string(&log_file).unwrap_or_default();
            eprintln!(
                "pg_ctl start failed: stdout={}, stderr={}, log={}",
                String::from_utf8_lossy(&start_status.stdout),
                String::from_utf8_lossy(&start_status.stderr),
                pg_log
            );
            return None;
        }

        Some(Self { dir, port })
    }

    fn start_with_ssl() -> Result<Self, String> {
        let dir = TempDir::new().map_err(|e| format!("TempDir::new failed: {}", e))?;
        let path = dir
            .path()
            .to_str()
            .ok_or_else(|| "dir.path() to_str failed".to_string())?;
        let offset = PG_PORT_COUNTER.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        let port = 54300 + ((std::process::id() as u16 % 200) * 10) + offset;

        let init_status = Command::new("initdb")
            .args(["-D", path, "--no-sync", "-A", "trust", "-U", "postgres"])
            .output()
            .map_err(|e| format!("initdb execution error: {}", e))?;
        if !init_status.status.success() {
            return Err(format!(
                "initdb failed: stdout={}, stderr={}",
                String::from_utf8_lossy(&init_status.stdout),
                String::from_utf8_lossy(&init_status.stderr)
            ));
        }

        // Generate self-signed cert and key using explicit RSA 2048
        let openssl_status = Command::new("openssl")
            .args([
                "req",
                "-new",
                "-newkey",
                "rsa:2048",
                "-x509",
                "-days",
                "1",
                "-nodes",
                "-out",
                &format!("{}/server.crt", path),
                "-keyout",
                &format!("{}/server.key", path),
                "-subj",
                "/CN=127.0.0.1",
            ])
            .output()
            .map_err(|e| format!("openssl execution error: {}", e))?;
        if !openssl_status.status.success() {
            return Err(format!(
                "openssl failed: stdout={}, stderr={}",
                String::from_utf8_lossy(&openssl_status.stdout),
                String::from_utf8_lossy(&openssl_status.stderr)
            ));
        }

        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(
                format!("{}/server.key", path),
                std::fs::Permissions::from_mode(0o600),
            )
            .map_err(|e| format!("chmod 0600 on server.key failed: {}", e))?;
        }

        let ssl_conf = format!(
            "\nunix_socket_directories = '{}'\nlisten_addresses = '127.0.0.1'\nssl = on\nssl_cert_file = '{}/server.crt'\nssl_key_file = '{}/server.key'\n",
            path, path, path
        );
        use std::io::Write;
        let mut conf_file = std::fs::OpenOptions::new()
            .append(true)
            .open(format!("{}/postgresql.conf", path))
            .map_err(|e| format!("failed to open postgresql.conf: {}", e))?;
        conf_file
            .write_all(ssl_conf.as_bytes())
            .map_err(|e| format!("failed to append ssl configuration: {}", e))?;

        let log_file = format!("{}/pg.log", path);
        let start_status = Command::new("pg_ctl")
            .args([
                "-D",
                path,
                "-l",
                &log_file,
                "-o",
                &format!("-p {} -k '{}' -c listen_addresses='127.0.0.1'", port, path),
                "-w",
                "start",
            ])
            .output()
            .map_err(|e| format!("pg_ctl execution error: {}", e))?;
        if !start_status.status.success() {
            let pg_log = std::fs::read_to_string(&log_file)
                .unwrap_or_else(|e| format!("<could not read pg.log: {}>", e));
            return Err(format!(
                "pg_ctl start failed: stdout={}, stderr={}, pg.log={}",
                String::from_utf8_lossy(&start_status.stdout),
                String::from_utf8_lossy(&start_status.stderr),
                pg_log
            ));
        }

        Ok(Self { dir, port })
    }

    fn url(&self) -> String {
        format!("postgres://postgres@127.0.0.1:{}/postgres", self.port)
    }

    fn createdb(&self, dbname: &str) -> bool {
        Command::new("createdb")
            .args([
                "-h",
                "127.0.0.1",
                "-p",
                &self.port.to_string(),
                "-U",
                "postgres",
                dbname,
            ])
            .status()
            .map(|s| s.success())
            .unwrap_or(false)
    }
}

impl Drop for TestPgServer {
    fn drop(&mut self) {
        if let Some(path) = self.dir.path().to_str() {
            let _ = Command::new("pg_ctl")
                .args(["-D", path, "-m", "immediate", "stop"])
                .output();
        }
    }
}

#[tokio::test]
async fn test_postgres_restore_propagates_postdata_error() {
    let _test_guard = PG_TEST_MUTEX.lock().await;
    let pg = match TestPgServer::start() {
        Some(server) => server,
        None => {
            eprintln!("PostgreSQL not available or failed to start, skipping test.");
            return;
        }
    };

    let adapter = PostgresAdapter::new(&pg.url());

    // Construct a backup stream containing an invalid PostData constraint
    let mut buffer = Vec::new();
    {
        let mut encoder = StreamEncoder::new(&mut buffer);

        let header = StreamHeader {
            version: 1,
            engine: "postgresql".into(),
            database: "postgres".into(),
            server_version: "18".into(),
            dumper_version: "0.1.0".into(),
            start_time: 1700000000,
        };
        encoder
            .write_record(&StreamRecord::Header(header))
            .await
            .unwrap();

        // Create a valid table
        let schema = TableSchemaRecord {
            schema_name: "public".into(),
            table_name: "items".into(),
            columns: vec![TableColumnMeta {
                name: "id".into(),
                data_type: "integer".into(),
                is_nullable: false,
                default_val: None,
            }],
            create_sql: "CREATE TABLE public.items (id integer NOT NULL);".into(),
        };
        encoder
            .write_record(&StreamRecord::TableSchema(schema))
            .await
            .unwrap();

        // PostData with completely invalid SQL syntax
        let invalid_post = PostDataRecord {
            schema_name: "public".into(),
            table_name: "items".into(),
            name: "broken_constraint".into(),
            sql: "ALTER TABLE public.items ADD CONSTRAINT invalid_syn TAX ERROR;".into(),
        };
        encoder
            .write_record(&StreamRecord::PostData(invalid_post))
            .await
            .unwrap();

        encoder.finish().await.unwrap();
    }

    // Attempt to restore stream
    let mut decoder = StreamDecoder::new(&buffer[..]);
    let options = RestoreOptions {
        target_database_override: None,
        drop_existing: true,
    };

    let res = adapter.restore(&mut decoder, &options).await;
    match res {
        Err(e) => {
            let err_msg = e.to_string();
            assert!(
                err_msg.contains("Failed to execute post-data"),
                "Error must clearly identify post-data failure: {}",
                err_msg
            );
        }
        Ok(_) => panic!("Restore must fail when PostData contains invalid constraint or fails"),
    }
}

#[tokio::test]
async fn test_postgres_custom_types_and_precision_preserved() {
    let _test_guard = PG_TEST_MUTEX.lock().await;
    let pg = match TestPgServer::start() {
        Some(server) => server,
        None => {
            eprintln!("PostgreSQL not available or failed to start, skipping test.");
            return;
        }
    };

    let adapter = PostgresAdapter::new(&pg.url());

    // Setup source schema with ENUM, VARCHAR(50), NUMERIC(10, 2), ARRAY
    {
        let (client, conn) = tokio_postgres::connect(&pg.url(), tokio_postgres::NoTls)
            .await
            .unwrap();
        tokio::spawn(async move {
            let _ = conn.await;
        });
        client.batch_execute("
            CREATE TYPE priority_level AS ENUM ('low', 'medium', 'high');
            CREATE TABLE tasks (
                id integer PRIMARY KEY,
                title varchar(50) NOT NULL,
                budget numeric(10, 2) DEFAULT 100.50,
                tags text[],
                priority priority_level
            );
            INSERT INTO tasks (id, title, budget, tags, priority) VALUES (1, 'Test Task', 123.45, ARRAY['dev', 'rust'], 'high');
        ").await.unwrap();
    }

    // Backup
    let mut buffer = Vec::new();
    {
        let mut encoder = StreamEncoder::new(&mut buffer);
        let stats = adapter.backup(&mut encoder).await.unwrap();
        assert_eq!(stats.tables_backed_up, 1);
        encoder.finish().await.unwrap();
    }

    // Inspect stream records: ensure 'priority_level' is used, NOT 'USER-DEFINED', and VARCHAR/NUMERIC have bounds
    {
        let mut decoder = StreamDecoder::new(&buffer[..]);
        let mut found_schema = false;
        while let Some(rec) = decoder.read_next_record().await.unwrap() {
            if let StreamRecord::TableSchema(s) = rec {
                if s.table_name == "tasks" {
                    found_schema = true;
                    assert!(
                        s.create_sql.contains("priority_level"),
                        "create_sql must contain exact enum name: {}",
                        s.create_sql
                    );
                    assert!(
                        !s.create_sql.contains("USER-DEFINED"),
                        "create_sql must NOT contain USER-DEFINED: {}",
                        s.create_sql
                    );
                    assert!(
                        s.create_sql.contains("character varying(50)")
                            || s.create_sql.contains("varchar(50)"),
                        "VARCHAR length must be preserved: {}",
                        s.create_sql
                    );
                    assert!(
                        s.create_sql.contains("numeric(10,2)")
                            || s.create_sql.contains("numeric(10, 2)"),
                        "NUMERIC precision must be preserved: {}",
                        s.create_sql
                    );
                }
            }
        }
        assert!(found_schema);
    }

    // Drop table and type in database
    {
        let (client, conn) = tokio_postgres::connect(&pg.url(), tokio_postgres::NoTls)
            .await
            .unwrap();
        tokio::spawn(async move {
            let _ = conn.await;
        });
        client
            .batch_execute("DROP TABLE tasks CASCADE; DROP TYPE priority_level;")
            .await
            .unwrap();
    }

    // Restore
    let mut decoder = StreamDecoder::new(&buffer[..]);
    let options = RestoreOptions {
        target_database_override: None,
        drop_existing: true,
    };
    let restore_stats = adapter.restore(&mut decoder, &options).await.unwrap();
    assert_eq!(restore_stats.tables_restored, 1);

    // Verify restored row
    {
        let (client, conn) = tokio_postgres::connect(&pg.url(), tokio_postgres::NoTls)
            .await
            .unwrap();
        tokio::spawn(async move {
            let _ = conn.await;
        });
        match client
            .query_opt("SELECT title, priority::text FROM tasks", &[])
            .await
        {
            Ok(Some(row)) => {
                let title: String = row.get(0);
                let priority: String = row.get(1);
                assert_eq!(title, "Test Task");
                assert_eq!(priority, "high");
            }
            Ok(None) => panic!("No rows found in restored tasks table!"),
            Err(e) => panic!("Querying tasks failed: {}", e),
        }
    }
}

#[tokio::test]
async fn test_postgres_sequence_and_serial_restoration_roundtrip() {
    let _test_guard = PG_TEST_MUTEX.lock().await;
    let pg = match TestPgServer::start() {
        Some(server) => server,
        None => {
            eprintln!("PostgreSQL not available or failed to start, skipping test.");
            return;
        }
    };

    let adapter = PostgresAdapter::new(&pg.url());

    // Setup source schema with serial column and a standalone sequence
    {
        let (client, conn) = tokio_postgres::connect(&pg.url(), tokio_postgres::NoTls)
            .await
            .unwrap();
        tokio::spawn(async move {
            let _ = conn.await;
        });
        client
            .batch_execute(
                "
                CREATE TABLE orders (
                    id serial PRIMARY KEY,
                    description text NOT NULL
                );
                INSERT INTO orders (description) VALUES ('Item A'), ('Item B'), ('Item C');
                CREATE SEQUENCE global_tx_seq START WITH 100 INCREMENT BY 5;
                SELECT nextval('global_tx_seq');
            ",
            )
            .await
            .unwrap();
    }

    // Backup
    let mut buffer = Vec::new();
    {
        let mut encoder = StreamEncoder::new(&mut buffer);
        let stats = adapter.backup(&mut encoder).await.unwrap();
        assert_eq!(stats.tables_backed_up, 1);
        encoder.finish().await.unwrap();
    }

    // Drop table and standalone sequence
    {
        let (client, conn) = tokio_postgres::connect(&pg.url(), tokio_postgres::NoTls)
            .await
            .unwrap();
        tokio::spawn(async move {
            let _ = conn.await;
        });
        client
            .batch_execute("DROP TABLE orders CASCADE; DROP SEQUENCE IF EXISTS global_tx_seq;")
            .await
            .unwrap();
    }

    // Restore
    let mut decoder = StreamDecoder::new(&buffer[..]);
    let options = RestoreOptions {
        target_database_override: None,
        drop_existing: true,
    };
    let restore_stats = adapter.restore(&mut decoder, &options).await.unwrap();
    assert_eq!(restore_stats.tables_restored, 1);

    // Verify restored serial sequence behavior and standalone sequence
    {
        let (client, conn) = tokio_postgres::connect(&pg.url(), tokio_postgres::NoTls)
            .await
            .unwrap();
        tokio::spawn(async move {
            let _ = conn.await;
        });

        // Insert new order without specifying id; serial must assign 4
        let row = client
            .query_one(
                "INSERT INTO orders (description) VALUES ('Item D') RETURNING id;",
                &[],
            )
            .await
            .unwrap();
        let new_id: i32 = row.get(0);
        assert_eq!(
            new_id, 4,
            "Serial sequence must resume at 4 after 3 initial rows"
        );

        // Check standalone sequence nextval; previous was 100, increment is 5, next must be 105
        let seq_row = client
            .query_one("SELECT nextval('global_tx_seq');", &[])
            .await
            .unwrap();
        let next_val: i64 = seq_row.get(0);
        assert_eq!(
            next_val, 105,
            "Standalone sequence must resume with correct increment and state"
        );
    }
}

#[tokio::test]
async fn test_postgres_restore_target_database_override() {
    let _test_guard = PG_TEST_MUTEX.lock().await;
    let pg = match TestPgServer::start() {
        Some(server) => server,
        None => {
            eprintln!("PostgreSQL not available or failed to start, skipping test.");
            return;
        }
    };

    assert!(pg.createdb("staging_db"), "Failed to create staging_db");

    let adapter = PostgresAdapter::new(&pg.url());

    // Construct a backup stream with table 'products'
    let mut buffer = Vec::new();
    {
        let mut encoder = StreamEncoder::new(&mut buffer);

        let header = StreamHeader {
            version: 1,
            engine: "postgresql".into(),
            database: "postgres".into(),
            server_version: "18".into(),
            dumper_version: "0.1.0".into(),
            start_time: 1700000000,
        };
        encoder
            .write_record(&StreamRecord::Header(header))
            .await
            .unwrap();

        let schema = TableSchemaRecord {
            schema_name: "public".into(),
            table_name: "products".into(),
            columns: vec![
                TableColumnMeta {
                    name: "id".into(),
                    data_type: "integer".into(),
                    is_nullable: false,
                    default_val: None,
                },
                TableColumnMeta {
                    name: "name".into(),
                    data_type: "text".into(),
                    is_nullable: true,
                    default_val: None,
                },
            ],
            create_sql: "CREATE TABLE public.products (id integer PRIMARY KEY, name text);".into(),
        };
        encoder
            .write_record(&StreamRecord::TableSchema(schema))
            .await
            .unwrap();

        encoder.finish().await.unwrap();
    }

    let mut decoder = StreamDecoder::new(&buffer[..]);
    let options = RestoreOptions {
        target_database_override: Some("staging_db".into()),
        drop_existing: true,
    };

    let stats = adapter.restore(&mut decoder, &options).await.unwrap();
    assert_eq!(stats.tables_restored, 1);

    // Verify staging_db has the products table
    let staging_url = format!("postgres://postgres@127.0.0.1:{}/staging_db", pg.port);
    let (staging_client, conn) = tokio_postgres::connect(&staging_url, tokio_postgres::NoTls)
        .await
        .unwrap();
    tokio::spawn(async move {
        let _ = conn.await;
    });

    let row = staging_client
        .query_one("SELECT to_regclass('public.products')::text;", &[])
        .await
        .unwrap();
    let regclass: Option<String> = row.get(0);
    assert_eq!(regclass.as_deref(), Some("products"));

    // Verify default postgres database does NOT have the products table
    let (default_client, conn2) = tokio_postgres::connect(&pg.url(), tokio_postgres::NoTls)
        .await
        .unwrap();
    tokio::spawn(async move {
        let _ = conn2.await;
    });
    let row_default = default_client
        .query_one("SELECT to_regclass('public.products')::text;", &[])
        .await
        .unwrap();
    let regclass_default: Option<String> = row_default.get(0);
    assert_eq!(
        regclass_default, None,
        "Original database must not receive restored table when target_database_override is set"
    );
}

#[tokio::test]
async fn test_postgres_tls_rejection_on_sslmode_require() {
    let _test_guard = PG_TEST_MUTEX.lock().await;
    let is_initdb_available = std::process::Command::new("initdb")
        .arg("--version")
        .output()
        .map(|o| o.status.success())
        .unwrap_or(false);

    let pg = match TestPgServer::start() {
        Some(server) => server,
        None => {
            if is_initdb_available {
                panic!("initdb is available but TestPgServer::start() failed");
            }
            eprintln!("PostgreSQL not available or failed to start, skipping test.");
            return;
        }
    };

    // 1. sslmode=require must fail because local test server does not have SSL enabled
    let require_url = format!("{}?sslmode=require", pg.url());
    let require_adapter = PostgresAdapter::new(&require_url);
    let require_res = require_adapter.inspect().await;
    assert!(
        require_res.is_err(),
        "sslmode=require must fail on non-SSL server"
    );
    let err = require_res.unwrap_err().to_string();
    assert!(
        err.contains("PostgreSQL TLS connection failed")
            || err.contains("server does not support TLS"),
        "Error must indicate TLS failure: {}",
        err
    );

    // 2. sslmode=disable must succeed
    let disable_url = format!("{}?sslmode=disable", pg.url());
    let disable_adapter = PostgresAdapter::new(&disable_url);
    assert!(
        disable_adapter.inspect().await.is_ok(),
        "sslmode=disable must succeed on non-SSL server"
    );

    // 3. sslmode=prefer must gracefully fall back to NoTls on non-SSL server
    let prefer_url = format!("{}?sslmode=prefer", pg.url());
    let prefer_adapter = PostgresAdapter::new(&prefer_url);
    assert!(
        prefer_adapter.inspect().await.is_ok(),
        "sslmode=prefer must succeed on non-SSL server"
    );
}

#[tokio::test]
async fn test_postgres_tls_success_roundtrip() {
    let _test_guard = PG_TEST_MUTEX.lock().await;
    let is_initdb_available = std::process::Command::new("initdb")
        .arg("--version")
        .output()
        .map(|o| o.status.success())
        .unwrap_or(false);

    let pg = match TestPgServer::start_with_ssl() {
        Ok(server) => server,
        Err(err) => {
            if is_initdb_available {
                panic!(
                    "initdb is available but TestPgServer::start_with_ssl() failed: {}",
                    err
                );
            } else {
                eprintln!("PostgreSQL not installed, skipping test.");
                return;
            }
        }
    };

    // 1. Verify that sslmode=require succeeds and actually establishes TLS encryption
    let target_db = "postgres_tls_test";
    assert!(pg.createdb(target_db));

    let db_tls_url = format!(
        "postgres://postgres@127.0.0.1:{}/{}?sslmode=require",
        pg.port, target_db
    );
    let target_adapter = PostgresAdapter::new(&db_tls_url);

    // Explicitly query pg_stat_ssl on the active backend connection
    let (is_ssl, tls_ver, tls_cipher) = target_adapter
        .query_ssl_stat()
        .await
        .expect("query_ssl_stat on sslmode=require connection");
    assert!(
        is_ssl,
        "pg_stat_ssl.ssl must be true for sslmode=require connection"
    );
    assert!(
        tls_ver.is_some(),
        "pg_stat_ssl.version must be recorded for TLS connection"
    );
    assert!(
        tls_cipher.is_some(),
        "pg_stat_ssl.cipher must be recorded for TLS connection"
    );
    eprintln!(
        "PROVEN: PostgreSQL TLS active: version={}, cipher={}",
        tls_ver.as_deref().unwrap_or("unknown"),
        tls_cipher.as_deref().unwrap_or("unknown")
    );

    // Verify unencrypted connection reports ssl = false
    let db_plain_url = format!(
        "postgres://postgres@127.0.0.1:{}/{}?sslmode=disable",
        pg.port, target_db
    );
    let plain_adapter = PostgresAdapter::new(&db_plain_url);
    let (plain_ssl, _, _) = plain_adapter
        .query_ssl_stat()
        .await
        .expect("query_ssl_stat on sslmode=disable connection");
    assert!(
        !plain_ssl,
        "pg_stat_ssl.ssl must be false for sslmode=disable connection"
    );

    let stats_inspect = target_adapter.inspect().await.unwrap();
    assert_eq!(stats_inspect.database, target_db);

    // Create table and insert rows using setup client
    let setup_url = format!(
        "postgres://postgres@127.0.0.1:{}/{}?sslmode=disable",
        pg.port, target_db
    );
    {
        let (client, conn) = tokio_postgres::connect(&setup_url, tokio_postgres::NoTls)
            .await
            .unwrap();
        tokio::spawn(async move {
            let _ = conn.await;
        });
        client
            .batch_execute(
                "
            CREATE TABLE tls_data (id int PRIMARY KEY, note text);
            INSERT INTO tls_data VALUES (1, 'secure data'), (2, 'encrypted row');
        ",
            )
            .await
            .unwrap();
    }

    // Backup via TLS (sslmode=require)
    let mut backup_buf = Vec::new();
    {
        let mut encoder = StreamEncoder::new(&mut backup_buf);
        let backup_stats = target_adapter.backup(&mut encoder).await.unwrap();
        assert_eq!(backup_stats.tables_backed_up, 1);
        encoder.finish().await.unwrap();
    }

    // Drop table
    {
        let (client, conn) = tokio_postgres::connect(&setup_url, tokio_postgres::NoTls)
            .await
            .unwrap();
        tokio::spawn(async move {
            let _ = conn.await;
        });
        client.batch_execute("DROP TABLE tls_data;").await.unwrap();
    }

    // Restore via TLS (sslmode=require)
    {
        let mut decoder = StreamDecoder::new(&backup_buf[..]);
        let restore_opts = RestoreOptions {
            target_database_override: None,
            drop_existing: false,
        };
        let restore_stats = target_adapter
            .restore(&mut decoder, &restore_opts)
            .await
            .unwrap();
        assert_eq!(restore_stats.tables_restored, 1);
    }

    // Validate restored rows match original data exactly
    {
        let (client, conn) = tokio_postgres::connect(&setup_url, tokio_postgres::NoTls)
            .await
            .unwrap();
        tokio::spawn(async move {
            let _ = conn.await;
        });
        let rows = client
            .query("SELECT id, note FROM tls_data ORDER BY id", &[])
            .await
            .unwrap();
        assert_eq!(rows.len(), 2);
        let r1_id: i32 = rows[0].get(0);
        let r1_note: String = rows[0].get(1);
        let r2_id: i32 = rows[1].get(0);
        let r2_note: String = rows[1].get(1);
        assert_eq!((r1_id, r1_note.as_str()), (1, "secure data"));
        assert_eq!((r2_id, r2_note.as_str()), (2, "encrypted row"));
    }

    // 2. sslmode=verify-full must fail because self-signed certificate is not in WebPKI CA roots
    let verify_url = format!(
        "postgres://postgres@127.0.0.1:{}/{}?sslmode=verify-full",
        pg.port, target_db
    );
    let verify_adapter = PostgresAdapter::new(&verify_url);
    let res = verify_adapter.inspect().await;
    assert!(
        res.is_err(),
        "sslmode=verify-full must reject self-signed certificate"
    );
}

#[tokio::test]
async fn test_postgres_concurrent_write_consistency() {
    let _test_guard = PG_TEST_MUTEX.lock().await;
    let pg = match TestPgServer::start() {
        Some(server) => server,
        None => return,
    };

    let target_db = "concurrent_tx_db";
    assert!(pg.createdb(target_db));
    let db_url = format!("postgres://postgres@127.0.0.1:{}/{}", pg.port, target_db);
    let adapter = PostgresAdapter::new(&db_url);

    // Create table and insert initial batch
    {
        let (client, conn) = tokio_postgres::connect(&db_url, tokio_postgres::NoTls)
            .await
            .unwrap();
        tokio::spawn(async move {
            let _ = conn.await;
        });
        client
            .batch_execute(
                "
            CREATE TABLE accounts (id int PRIMARY KEY, balance numeric(10, 2));
            INSERT INTO accounts (id, balance) SELECT g, 100.00 FROM generate_series(1, 100) g;
        ",
            )
            .await
            .unwrap();
    }

    // Spawn a concurrent writer task that updates and inserts rows continuously
    let stop_writer = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let stop_clone = stop_writer.clone();
    let write_url = db_url.clone();
    let writer_handle = tokio::spawn(async move {
        let (client, conn) = tokio_postgres::connect(&write_url, tokio_postgres::NoTls)
            .await
            .unwrap();
        tokio::spawn(async move {
            let _ = conn.await;
        });
        let mut i = 101;
        while !stop_clone.load(std::sync::atomic::Ordering::Relaxed) {
            let _ = client
                .execute(
                    "INSERT INTO accounts (id, balance) VALUES ($1, 50.00) ON CONFLICT (id) DO NOTHING",
                    &[&i],
                )
                .await;
            let _ = client
                .execute(
                    "UPDATE accounts SET balance = balance + 1.00 WHERE id = 1",
                    &[],
                )
                .await;
            i += 1;
            tokio::time::sleep(std::time::Duration::from_millis(2)).await;
        }
    });

    // Run backup while concurrent writes are occurring
    let mut backup_buf = Vec::new();
    let backup_stats = {
        let mut encoder = StreamEncoder::new(&mut backup_buf);
        let stats = adapter.backup(&mut encoder).await.unwrap();
        encoder.finish().await.unwrap();
        stats
    };

    // Stop writer
    stop_writer.store(true, std::sync::atomic::Ordering::Relaxed);
    let _ = writer_handle.await;

    assert_eq!(backup_stats.tables_backed_up, 1);

    // Restore into a fresh database to verify consistency
    let restore_db = "restored_tx_db";
    assert!(pg.createdb(restore_db));
    let restore_url = format!("postgres://postgres@127.0.0.1:{}/{}", pg.port, restore_db);
    let restore_adapter = PostgresAdapter::new(&restore_url);

    let mut decoder = StreamDecoder::new(&backup_buf[..]);
    let restore_stats = restore_adapter
        .restore(
            &mut decoder,
            &RestoreOptions {
                target_database_override: None,
                drop_existing: false,
            },
        )
        .await
        .unwrap();
    assert_eq!(restore_stats.tables_restored, 1);

    // Verify row integrity in restored database
    let (client, conn) = tokio_postgres::connect(&restore_url, tokio_postgres::NoTls)
        .await
        .unwrap();
    tokio::spawn(async move {
        let _ = conn.await;
    });
    let count_row = client
        .query_one("SELECT count(*) FROM accounts", &[])
        .await
        .unwrap();
    let count: i64 = count_row.get(0);
    assert!(
        count >= 100,
        "Must have restored at least initial 100 rows: {}",
        count
    );
}
