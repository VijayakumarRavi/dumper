use dumper::database::postgres::PostgresAdapter;
use dumper::database::{DatabaseAdapter, RestoreOptions};
use dumper::stream::decoder::StreamDecoder;
use dumper::stream::encoder::StreamEncoder;
use dumper::stream::format::*;
use std::process::Command;
use tempfile::TempDir;

struct TestPgServer {
    dir: TempDir,
    port: u16,
}

impl TestPgServer {
    fn start() -> Option<Self> {
        let dir = TempDir::new().ok()?;
        let path = dir.path().to_str()?;
        let port = 54300 + (std::process::id() % 1000) as u16;

        let init_status = Command::new("initdb")
            .args(["-D", path, "--no-sync", "-A", "trust", "-U", "postgres"])
            .output()
            .ok()?;
        if !init_status.status.success() {
            return None;
        }

        let log_file = format!("{}/pg.log", path);
        let start_status = Command::new("pg_ctl")
            .args([
                "-D",
                path,
                "-l",
                &log_file,
                "-o",
                &format!("-p {}", port),
                "-w",
                "start",
            ])
            .output()
            .ok()?;
        if !start_status.status.success() {
            return None;
        }

        Some(Self { dir, port })
    }

    fn url(&self) -> String {
        format!("postgres://postgres@127.0.0.1:{}/postgres", self.port)
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
