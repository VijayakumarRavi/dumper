use std::process::Command;
use tempfile::TempDir;
use dumper::database::{DatabaseAdapter, RestoreOptions};
use dumper::database::postgres::PostgresAdapter;
use dumper::stream::encoder::StreamEncoder;
use dumper::stream::decoder::StreamDecoder;
use dumper::stream::format::*;

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
            .output().ok()?;
        if !init_status.status.success() {
            return None;
        }

        let log_file = format!("{}/pg.log", path);
        let start_status = Command::new("pg_ctl")
            .args(["-D", path, "-l", &log_file, "-o", &format!("-p {}", port), "-w", "start"])
            .output().ok()?;
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
        encoder.write_record(&StreamRecord::Header(header)).await.unwrap();

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
        encoder.write_record(&StreamRecord::TableSchema(schema)).await.unwrap();

        // PostData with completely invalid SQL syntax
        let invalid_post = PostDataRecord {
            schema_name: "public".into(),
            table_name: "items".into(),
            name: "broken_constraint".into(),
            sql: "ALTER TABLE public.items ADD CONSTRAINT invalid_syn TAX ERROR;".into(),
        };
        encoder.write_record(&StreamRecord::PostData(invalid_post)).await.unwrap();

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
