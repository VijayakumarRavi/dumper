use std::process::{Child, Command};
use tempfile::TempDir;
use dumper::database::{DatabaseAdapter, RestoreOptions};
use dumper::database::mysql::MysqlAdapter;
use dumper::stream::encoder::StreamEncoder;
use dumper::stream::decoder::StreamDecoder;
use dumper::stream::format::*;

struct TestMysqlServer {
    _dir: TempDir,
    child: Child,
    port: u16,
}

impl TestMysqlServer {
    fn start() -> Option<Self> {
        let dir = TempDir::new().ok()?;
        let path = dir.path().to_str()?;
        let port = 53300 + (std::process::id() % 1000) as u16;

        let install_status = Command::new("mariadb-install-db")
            .args(["--datadir", path, "--auth-root-authentication-method=normal"])
            .output().ok()?;
        if !install_status.status.success() {
            return None;
        }

        let child = Command::new("mariadbd")
            .args([
                format!("--datadir={}", path),
                format!("--port={}", port),
                format!("--socket={}/mysql.sock", path),
                "--bind-address=127.0.0.1".into(),
            ])
            .spawn().ok()?;

        // Wait up to 5 seconds for server to respond
        for _ in 0..50 {
            std::thread::sleep(std::time::Duration::from_millis(100));
            let check = Command::new("mariadb")
                .args(["-h", "127.0.0.1", "-P", &port.to_string(), "-u", "root", "-e", "CREATE DATABASE IF NOT EXISTS testdb"])
                .output();
            if let Ok(out) = check {
                if out.status.success() {
                    return Some(Self { dir, child, port });
                }
            }
        }

        None
    }

    fn url(&self) -> String {
        format!("mysql://root@127.0.0.1:{}/testdb", self.port)
    }
}

impl Drop for TestMysqlServer {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

#[tokio::test]
async fn test_mysql_restore_propagates_insert_error() {
    let mysql = match TestMysqlServer::start() {
        Some(server) => server,
        None => {
            eprintln!("MariaDB/MySQL not available or failed to start, skipping test.");
            return;
        }
    };

    let adapter = MysqlAdapter::new(&mysql.url());

    // Construct a backup stream with a table and an invalid data row
    let mut buffer = Vec::new();
    {
        let mut encoder = StreamEncoder::new(&mut buffer);

        let header = StreamHeader {
            version: 1,
            engine: "mysql".into(),
            database: "testdb".into(),
            server_version: "11.4".into(),
            dumper_version: "0.1.0".into(),
            start_time: 1700000000,
        };
        encoder.write_record(&StreamRecord::Header(header)).await.unwrap();

        // Create table with integer primary key
        let schema = TableSchemaRecord {
            schema_name: "testdb".into(),
            table_name: "users".into(),
            columns: vec![TableColumnMeta {
                name: "id".into(),
                data_type: "int".into(),
                is_nullable: false,
                default_val: None,
            }],
            create_sql: "CREATE TABLE `users` (`id` int NOT NULL, PRIMARY KEY (`id`));".into(),
        };
        encoder.write_record(&StreamRecord::TableSchema(schema)).await.unwrap();

        // Invalid row data: string "not_a_valid_integer" for INT NOT NULL column in strict mode
        // Or duplicate PK row in batch
        #[allow(dead_code)]
        #[derive(serde::Serialize)]
        enum MysqlValue {
            Null,
            String(String),
            Bytes(Vec<u8>),
        }
        let batch: Vec<Vec<MysqlValue>> = vec![
            vec![MysqlValue::String("1".into())],
            vec![MysqlValue::String("1".into())], // Duplicate primary key 1!
        ];
        let slice_data = serde_json::to_vec(&batch).unwrap();
        let data_slice = TableDataSliceRecord {
            schema_name: "testdb".into(),
            table_name: "users".into(),
            slice_seq: 1,
            is_last: true,
            data: slice_data,
        };
        encoder.write_record(&StreamRecord::TableDataSlice(data_slice)).await.unwrap();

        encoder.finish().await.unwrap();
    }

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
                err_msg.contains("Failed to insert row into table"),
                "Error must clearly indicate row insert failure: {}",
                err_msg
            );
        }
        Ok(_) => panic!("Restore must fail when row insert fails due to duplicate key or invalid data"),
    }
}
