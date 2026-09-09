use dumper::database::mysql::MysqlAdapter;
use dumper::database::{DatabaseAdapter, RestoreOptions};
use dumper::stream::decoder::StreamDecoder;
use dumper::stream::encoder::StreamEncoder;
use dumper::stream::format::*;
use std::process::{Child, Command};
use tempfile::TempDir;

struct TestMysqlServer {
    _dir: TempDir,
    child: Child,
    port: u16,
}

static PORT_COUNTER: std::sync::atomic::AtomicU16 = std::sync::atomic::AtomicU16::new(0);

impl TestMysqlServer {
    fn start() -> Option<Self> {
        let dir = TempDir::new().ok()?;
        let path = dir.path().to_str()?;
        let offset = PORT_COUNTER.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        let port = 53300 + ((std::process::id() as u16 % 200) * 10) + offset;

        let install_status = Command::new("mariadb-install-db")
            .args([
                "--datadir",
                path,
                "--auth-root-authentication-method=normal",
            ])
            .output()
            .ok()?;
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
            .spawn()
            .ok()?;

        // Wait up to 5 seconds for server to respond
        for _ in 0..50 {
            std::thread::sleep(std::time::Duration::from_millis(100));
            let check = Command::new("mariadb")
                .args([
                    "-h",
                    "127.0.0.1",
                    "-P",
                    &port.to_string(),
                    "-u",
                    "root",
                    "-e",
                    "CREATE DATABASE IF NOT EXISTS testdb",
                ])
                .output();
            if let Ok(out) = check {
                if out.status.success() {
                    return Some(Self {
                        _dir: dir,
                        child,
                        port,
                    });
                }
            }
        }

        None
    }

    fn url(&self) -> String {
        format!("mysql://root@127.0.0.1:{}/testdb", self.port)
    }

    fn query(&self, sql: &str) -> String {
        let out = Command::new("mariadb")
            .args([
                "-h",
                "127.0.0.1",
                "-P",
                &self.port.to_string(),
                "-u",
                "root",
                "-D",
                "testdb",
                "-N",
                "-s",
                "-e",
                sql,
            ])
            .output()
            .expect("failed to execute mariadb client command");
        String::from_utf8_lossy(&out.stdout).trim().to_string()
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
        encoder
            .write_record(&StreamRecord::Header(header))
            .await
            .unwrap();

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
        encoder
            .write_record(&StreamRecord::TableSchema(schema))
            .await
            .unwrap();

        use dumper::database::mysql::MysqlValue;
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
        encoder
            .write_record(&StreamRecord::TableDataSlice(data_slice))
            .await
            .unwrap();

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
        Ok(_) => {
            panic!("Restore must fail when row insert fails due to duplicate key or invalid data")
        }
    }
}

#[tokio::test]
async fn test_mysql_restore_batch_inserts_and_escaping_roundtrip() {
    let mysql = match TestMysqlServer::start() {
        Some(server) => server,
        None => {
            eprintln!("MariaDB/MySQL not available or failed to start, skipping test.");
            return;
        }
    };

    let adapter = MysqlAdapter::new(&mysql.url());
    use dumper::database::mysql::MysqlValue;

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
        encoder
            .write_record(&StreamRecord::Header(header))
            .await
            .unwrap();

        let schema = TableSchemaRecord {
            schema_name: "testdb".into(),
            table_name: "messages".into(),
            columns: vec![
                TableColumnMeta {
                    name: "id".into(),
                    data_type: "int".into(),
                    is_nullable: false,
                    default_val: None,
                },
                TableColumnMeta {
                    name: "content".into(),
                    data_type: "text".into(),
                    is_nullable: true,
                    default_val: None,
                },
                TableColumnMeta {
                    name: "tag".into(),
                    data_type: "varchar(50)".into(),
                    is_nullable: true,
                    default_val: None,
                },
            ],
            create_sql: "CREATE TABLE `messages` (`id` int NOT NULL, `content` text, `tag` varchar(50), PRIMARY KEY (`id`));".into(),
        };
        encoder
            .write_record(&StreamRecord::TableSchema(schema))
            .await
            .unwrap();

        // 450 rows to span multiple 200-row batches
        let mut rows = Vec::new();
        // Row 1 with complex special chars: quotes, backslash, null byte, newlines, tabs
        let special_content = "Special 'quotes' and \"double quotes\" and backslash \\ and \\0 null and \n newline \r cr \t tab \x1A ctrlz";
        rows.push(vec![
            MysqlValue::String("1".into()),
            MysqlValue::String(special_content.into()),
            MysqlValue::String("special".into()),
        ]);

        // Row 2 with null tag
        rows.push(vec![
            MysqlValue::String("2".into()),
            MysqlValue::String("Second message".into()),
            MysqlValue::Null,
        ]);

        // Rows 3..450
        for i in 3..=450 {
            rows.push(vec![
                MysqlValue::String(i.to_string()),
                MysqlValue::String(format!("Message body for item {}", i)),
                MysqlValue::String("batch".into()),
            ]);
        }

        let slice_data = serde_json::to_vec(&rows).unwrap();
        encoder
            .write_record(&StreamRecord::TableDataSlice(TableDataSliceRecord {
                schema_name: "testdb".into(),
                table_name: "messages".into(),
                slice_seq: 1,
                is_last: true,
                data: slice_data,
            }))
            .await
            .unwrap();

        encoder.finish().await.unwrap();
    }

    let mut decoder = StreamDecoder::new(&buffer[..]);
    let options = RestoreOptions {
        target_database_override: None,
        drop_existing: true,
    };

    let stats = adapter.restore(&mut decoder, &options).await.unwrap();
    assert_eq!(stats.tables_restored, 1);

    // Verify row count
    let count = mysql.query("SELECT count(*) FROM messages;");
    assert_eq!(count, "450", "All 450 batched rows must be restored");

    // Verify Row 1 special chars
    let row1_content = mysql.query("SELECT content FROM messages WHERE id = 1;");
    assert!(
        row1_content.contains("'quotes'"),
        "Single quotes must be preserved"
    );
    assert!(
        row1_content.contains("\"double quotes\""),
        "Double quotes must be preserved"
    );
    assert!(row1_content.contains('\\'), "Backslashes must be preserved");

    // Verify Row 2 null column
    let row2_tag_is_null = mysql.query("SELECT tag IS NULL FROM messages WHERE id = 2;");
    assert_eq!(row2_tag_is_null, "1", "Null column must remain NULL");
}

#[tokio::test]
async fn test_mysql_restore_transaction_rollback_on_failure() {
    let mysql = match TestMysqlServer::start() {
        Some(server) => server,
        None => {
            eprintln!("MariaDB/MySQL not available or failed to start, skipping test.");
            return;
        }
    };

    let adapter = MysqlAdapter::new(&mysql.url());
    use dumper::database::mysql::MysqlValue;

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
        encoder
            .write_record(&StreamRecord::Header(header))
            .await
            .unwrap();

        let schema = TableSchemaRecord {
            schema_name: "testdb".into(),
            table_name: "items".into(),
            columns: vec![TableColumnMeta {
                name: "id".into(),
                data_type: "int".into(),
                is_nullable: false,
                default_val: None,
            }],
            create_sql: "CREATE TABLE `items` (`id` int NOT NULL, PRIMARY KEY (`id`));".into(),
        };
        encoder
            .write_record(&StreamRecord::TableSchema(schema))
            .await
            .unwrap();

        // Slice 1: rows 1..=5 (valid)
        let slice1_rows: Vec<Vec<MysqlValue>> = (1..=5)
            .map(|i| vec![MysqlValue::String(i.to_string())])
            .collect();
        encoder
            .write_record(&StreamRecord::TableDataSlice(TableDataSliceRecord {
                schema_name: "testdb".into(),
                table_name: "items".into(),
                slice_seq: 1,
                is_last: false,
                data: serde_json::to_vec(&slice1_rows).unwrap(),
            }))
            .await
            .unwrap();

        // Slice 2: rows 6, 7, 8, 9, 1 (1 is duplicate, triggers failure)
        let slice2_rows: Vec<Vec<MysqlValue>> = vec![
            vec![MysqlValue::String("6".into())],
            vec![MysqlValue::String("7".into())],
            vec![MysqlValue::String("8".into())],
            vec![MysqlValue::String("9".into())],
            vec![MysqlValue::String("1".into())], // Duplicate primary key
        ];
        encoder
            .write_record(&StreamRecord::TableDataSlice(TableDataSliceRecord {
                schema_name: "testdb".into(),
                table_name: "items".into(),
                slice_seq: 2,
                is_last: true,
                data: serde_json::to_vec(&slice2_rows).unwrap(),
            }))
            .await
            .unwrap();

        encoder.finish().await.unwrap();
    }

    let mut decoder = StreamDecoder::new(&buffer[..]);
    let options = RestoreOptions {
        target_database_override: None,
        drop_existing: true,
    };

    let res = adapter.restore(&mut decoder, &options).await;
    assert!(
        res.is_err(),
        "Restore must fail due to duplicate key in Slice 2"
    );

    // Because Slice 2 failed and was rolled back, none of Slice 2's rows (6, 7, 8, 9) should exist.
    // Only Slice 1's 5 rows should exist.
    let count = mysql.query("SELECT count(*) FROM items;");
    assert_eq!(
        count, "5",
        "Transaction rollback in failed slice must leave exactly 5 rows from committed slice 1"
    );
}
