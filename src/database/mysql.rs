use tokio::io::{AsyncWrite, AsyncRead};
use mysql_async::prelude::*;
use mysql_async::{Conn, Opts, Pool};
use crate::database::{BackupStats, DatabaseAdapter, DatabaseMeta, RestoreOptions, RestoreStats};
use crate::error::DumperError;
use crate::stream::decoder::StreamDecoder;
use crate::stream::encoder::StreamEncoder;
use crate::stream::format::*;
use serde::{Serialize, Deserialize};

#[derive(Serialize, Deserialize)]
enum MysqlValue {
    Null,
    String(String),
    Bytes(Vec<u8>),
}

pub struct MysqlAdapter {
    url: String,
}

impl MysqlAdapter {
    pub fn new(url: &str) -> Self {
        Self {
            url: url.to_string(),
        }
    }

    async fn get_conn(&self) -> Result<Conn, DumperError> {
        let opts = Opts::from_url(&self.url)
            .map_err(|e| DumperError::Database(format!("Invalid MySQL connection URL: {}", e)))?;
        let pool = Pool::new(opts);
        let conn = pool
            .get_conn()
            .await
            .map_err(|e| DumperError::Database(format!("MySQL connection failed: {}", e)))?;
        Ok(conn)
    }
}

impl DatabaseAdapter for MysqlAdapter {
    async fn inspect(&self) -> Result<DatabaseMeta, DumperError> {
        let mut conn = self.get_conn().await?;

        let version: String = conn
            .query_first("SELECT VERSION()")
            .await
            .map_err(|e| DumperError::Database(e.to_string()))?
            .unwrap_or_else(|| "Unknown MySQL".into());

        let database: String = conn
            .query_first("SELECT DATABASE()")
            .await
            .map_err(|e| DumperError::Database(e.to_string()))?
            .unwrap_or_else(|| "default".into());

        let tables: Vec<String> = conn
            .query_map(
                "SELECT table_name FROM information_schema.tables \
                 WHERE table_schema = DATABASE() AND table_type = 'BASE TABLE' \
                 ORDER BY table_name",
                |table_name: String| table_name,
            )
            .await
            .map_err(|e| DumperError::Database(e.to_string()))?;

        let table_names = tables.into_iter().map(|t| (database.clone(), t)).collect();

        Ok(DatabaseMeta {
            engine: "mysql".into(),
            database,
            server_version: version,
            table_names,
        })
    }

    async fn backup<W: AsyncWrite + Unpin + Send>(
        &self,
        encoder: &mut StreamEncoder<W>,
    ) -> Result<BackupStats, DumperError> {
        let mut conn = self.get_conn().await?;

        // Consistent snapshot via REPEATABLE READ
        conn.query_drop("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ;")
            .await
            .map_err(|e| DumperError::Database(e.to_string()))?;
        conn.query_drop("START TRANSACTION WITH CONSISTENT SNAPSHOT;")
            .await
            .map_err(|e| DumperError::Database(e.to_string()))?;

        let meta = self.inspect().await?;

        // 1. Header
        let header = StreamHeader {
            version: STREAM_VERSION,
            engine: meta.engine.clone(),
            database: meta.database.clone(),
            server_version: meta.server_version.clone(),
            dumper_version: env!("CARGO_PKG_VERSION").into(),
            start_time: chrono::Utc::now().timestamp(),
        };
        encoder.write_record(&StreamRecord::Header(header)).await?;

        // 2. Tables & Schema
        let mut tables_backed_up = 0;
        let mut total_rows = 0u64;

        for (db, table) in &meta.table_names {
            // Get CREATE TABLE statement
            let show_create_query = format!("SHOW CREATE TABLE `{}`", table);
            let show_row: Option<(String, String)> = conn
                .query_first(&show_create_query)
                .await
                .map_err(|e| DumperError::Database(e.to_string()))?;

            let create_sql = show_row
                .map(|(_, sql)| format!("{};", sql))
                .unwrap_or_default();

            let cols_query = format!("SELECT column_name FROM information_schema.columns WHERE table_schema = DATABASE() AND table_name = '{}' AND extra NOT LIKE '%GENERATED%' ORDER BY ordinal_position", table);
            let col_names: Vec<String> = conn.query_map(&cols_query, |c: String| c).await.unwrap_or_default();
            let columns = col_names.iter().map(|c| TableColumnMeta {
                name: c.clone(),
                data_type: "".into(),
                is_nullable: true,
                default_val: None,
            }).collect();

            encoder.write_record(&StreamRecord::TableSchema(TableSchemaRecord {
                schema_name: db.clone(),
                table_name: table.clone(),
                columns,
                create_sql,
            })).await?;

            // Stream rows as chunked JSON/TSV data
            let select_query = if col_names.is_empty() {
                format!("SELECT * FROM `{}`", table)
            } else {
                let quoted_cols = col_names.iter().map(|c| format!("`{}`", c)).collect::<Vec<_>>().join(", ");
                format!("SELECT {} FROM `{}`", quoted_cols, table)
            };
            
            let mut result_stream = conn
                .query_iter(&select_query)
                .await
                .map_err(|e| DumperError::Database(e.to_string()))?;

            let mut batch_rows = Vec::new();
            let mut slice_seq = 0u64;

            while let Ok(Some(row)) = result_stream.next().await {
                let mut row_values = Vec::new();
                for col_idx in 0..row.len() {
                    let val: mysql_async::Value = row.get(col_idx).unwrap();
                    let mval = match val {
                        mysql_async::Value::NULL => MysqlValue::Null,
                        mysql_async::Value::Bytes(b) => {
                            if let Ok(s) = String::from_utf8(b.clone()) {
                                MysqlValue::String(s)
                            } else {
                                MysqlValue::Bytes(b)
                            }
                        }
                        mysql_async::Value::Int(i) => MysqlValue::String(i.to_string()),
                        mysql_async::Value::UInt(u) => MysqlValue::String(u.to_string()),
                        mysql_async::Value::Float(f) => MysqlValue::String(f.to_string()),
                        mysql_async::Value::Double(d) => MysqlValue::String(d.to_string()),
                        mysql_async::Value::Date(y, m, d, h, i, s, u) => {
                            let mut dt = format!("{:04}-{:02}-{:02}", y, m, d);
                            if h != 0 || i != 0 || s != 0 || u != 0 {
                                dt.push_str(&format!(" {:02}:{:02}:{:02}", h, i, s));
                                if u != 0 {
                                    dt.push_str(&format!(".{:06}", u));
                                }
                            }
                            MysqlValue::String(dt)
                        }
                        mysql_async::Value::Time(is_neg, d, h, m, s, u) => {
                            let sign = if is_neg { "-" } else { "" };
                            let hours = h as u32 + d * 24;
                            let mut t = format!("{}{:02}:{:02}:{:02}", sign, hours, m, s);
                            if u != 0 {
                                t.push_str(&format!(".{:06}", u));
                            }
                            MysqlValue::String(t)
                        }
                    };
                    row_values.push(mval);
                }
                batch_rows.push(row_values);
                total_rows += 1;

                if batch_rows.len() >= 1000 {
                    slice_seq += 1;
                    let data = serde_json::to_vec(&batch_rows)?;
                    encoder.write_record(&StreamRecord::TableDataSlice(TableDataSliceRecord {
                        schema_name: db.clone(),
                        table_name: table.clone(),
                        slice_seq,
                        is_last: false,
                        data,
                    })).await?;
                    batch_rows.clear();
                }
            }

            // Flush final batch
            if !batch_rows.is_empty() {
                slice_seq += 1;
                let data = serde_json::to_vec(&batch_rows)?;
                encoder.write_record(&StreamRecord::TableDataSlice(TableDataSliceRecord {
                    schema_name: db.clone(),
                    table_name: table.clone(),
                    slice_seq,
                    is_last: false,
                    data,
                })).await?;
            }

            // End slice
            encoder.write_record(&StreamRecord::TableDataSlice(TableDataSliceRecord {
                schema_name: db.clone(),
                table_name: table.clone(),
                slice_seq: slice_seq + 1,
                is_last: true,
                data: Vec::new(),
            })).await?;

            tables_backed_up += 1;
        }

        // 3. Views
        let views: Vec<(String, String)> = conn
            .query_map(
                "SELECT table_name, view_definition FROM information_schema.views \
                 WHERE table_schema = DATABASE()",
                |(t, v): (String, String)| (t, v),
            )
            .await
            .unwrap_or_default();

        for (vname, vdef) in views {
            let sql = format!("CREATE OR REPLACE VIEW `{}` AS {};", vname, vdef);
            encoder.write_record(&StreamRecord::Routine(RoutineRecord {
                schema_name: meta.database.clone(),
                name: vname,
                routine_type: "VIEW".into(),
                sql,
            })).await?;
        }

        // 4. Triggers
        let triggers: Vec<String> = conn
            .query_map("SHOW TRIGGERS", |mut row: mysql_async::Row| {
                let stmt: String = row.take("Statement").unwrap();
                let trigger: String = row.take("Trigger").unwrap();
                let timing: String = row.take("Timing").unwrap();
                let event: String = row.take("Event").unwrap();
                let table: String = row.take("Table").unwrap();
                format!("CREATE TRIGGER `{}` {} {} ON `{}` FOR EACH ROW {}", trigger, timing, event, table, stmt)
            })
            .await
            .unwrap_or_default();
            
        for trig in triggers {
            encoder.write_record(&StreamRecord::PostData(PostDataRecord {
                schema_name: meta.database.clone(),
                table_name: "".into(),
                name: "".into(),
                sql: format!("{};", trig),
            })).await?;
        }

        // 5. Routines (Procedures and Functions)
        let procs: Vec<String> = conn
            .query_map("SHOW PROCEDURE STATUS WHERE Db = DATABASE()", |mut row: mysql_async::Row| {
                row.take("Name").unwrap()
            })
            .await
            .unwrap_or_default();

        for p in procs {
            let q = format!("SHOW CREATE PROCEDURE `{}`", p);
            if let Ok(Some(mut row)) = conn.query_first::<mysql_async::Row, _>(&q).await {
                if let Some(def) = row.take("Create Procedure") {
                    let def: String = def;
                    encoder.write_record(&StreamRecord::Routine(RoutineRecord {
                        schema_name: meta.database.clone(),
                        name: p.clone(),
                        routine_type: "PROCEDURE".into(),
                        sql: format!("{};", def),
                    })).await?;
                }
            }
        }

        let funcs: Vec<String> = conn
            .query_map("SHOW FUNCTION STATUS WHERE Db = DATABASE()", |mut row: mysql_async::Row| {
                row.take("Name").unwrap()
            })
            .await
            .unwrap_or_default();

        for f in funcs {
            let q = format!("SHOW CREATE FUNCTION `{}`", f);
            if let Ok(Some(mut row)) = conn.query_first::<mysql_async::Row, _>(&q).await {
                if let Some(def) = row.take("Create Function") {
                    let def: String = def;
                    encoder.write_record(&StreamRecord::Routine(RoutineRecord {
                        schema_name: meta.database.clone(),
                        name: f.clone(),
                        routine_type: "FUNCTION".into(),
                        sql: format!("{};", def),
                    })).await?;
                }
            }
        }

        let _ = conn.query_drop("COMMIT;").await;

        Ok(BackupStats {
            engine: meta.engine,
            database: meta.database,
            server_version: meta.server_version,
            tables_backed_up,
            rows_backed_up: total_rows,
            logical_bytes: encoder.bytes_written(),
        })
    }

    async fn restore<R: AsyncRead + Unpin + Send>(
        &self,
        decoder: &mut StreamDecoder<R>,
        options: &RestoreOptions,
    ) -> Result<RestoreStats, DumperError> {
        let mut conn = self.get_conn().await?;

        // Disable integrity checks for restore
        conn.query_drop("SET FOREIGN_KEY_CHECKS = 0; SET UNIQUE_CHECKS = 0; SET SQL_MODE = 'NO_AUTO_VALUE_ON_ZERO';")
            .await
            .map_err(|e| DumperError::Restore(format!("Failed to set restore session variables: {}", e)))?;

        let mut tables_restored = 0;
        let mut records_processed = 0u64;

        while let Some(record) = decoder.read_next_record().await? {
            records_processed += 1;
            match record {
                StreamRecord::Header(h) => {
                    if h.engine != "mysql" && h.engine != "mariadb" {
                        return Err(DumperError::Restore(format!(
                            "Cannot restore a '{}' backup into a MySQL target database",
                            h.engine
                        )));
                    }
                }
                StreamRecord::TableSchema(s) => {
                    if options.drop_existing {
                        let drop_sql = format!("DROP TABLE IF EXISTS `{}`;", s.table_name);
                        let _ = conn.query_drop(&drop_sql).await;
                    }
                    conn.query_drop(&s.create_sql).await.map_err(|e| {
                        DumperError::Restore(format!("Failed to create table {}: {}", s.table_name, e))
                    })?;
                    tables_restored += 1;
                }
                StreamRecord::TableDataSlice(d) => {
                    if !d.data.is_empty() {
                        let batch_rows: Vec<Vec<MysqlValue>> = serde_json::from_slice(&d.data)?;
                        
                        // We must fetch the schema from a cache if we want columns, but since TableSchema precedes TableDataSlice,
                        // we can query the target database for columns, OR we can just use the schema record if we cache it.
                        // For simplicity, we just rely on `INSERT INTO table VALUES` unless we need to query columns.
                        // Actually, if we just query the table columns now:
                        let cols_query = format!("SELECT column_name FROM information_schema.columns WHERE table_schema = DATABASE() AND table_name = '{}' AND extra NOT LIKE '%GENERATED%' ORDER BY ordinal_position", d.table_name);
                        let col_names: Vec<String> = conn.query_map(&cols_query, |c: String| format!("`{}`", c)).await.unwrap_or_default();
                        
                        let insert_prefix = if col_names.is_empty() {
                            format!("INSERT INTO `{}` VALUES", d.table_name)
                        } else {
                            format!("INSERT INTO `{}` ({}) VALUES", d.table_name, col_names.join(", "))
                        };

                        for row in batch_rows {
                            let values_str = row
                                .into_iter()
                                .map(|val| match val {
                                    MysqlValue::String(v) => format!("'{}'", v.replace('\\', "\\\\").replace('\'', "\\'")),
                                    MysqlValue::Bytes(b) => format!("X'{}'", hex::encode(&b)),
                                    MysqlValue::Null => "NULL".into(),
                                })
                                .collect::<Vec<_>>()
                                .join(", ");
                            let insert_sql = format!("{} ({});", insert_prefix, values_str);
                            conn.query_drop(&insert_sql).await.map_err(|e| {
                                DumperError::Restore(format!(
                                    "Failed to insert row into table '{}': {}",
                                    d.table_name, e
                                ))
                            })?;
                        }
                    }
                }
                StreamRecord::PostData(p) => {
                    conn.query_drop(&p.sql).await.map_err(|e| {
                        DumperError::Restore(format!("Failed to execute post-data in MySQL: {}", e))
                    })?;
                }
                StreamRecord::Routine(r) => {
                    conn.query_drop(&r.sql).await.map_err(|e| {
                        DumperError::Restore(format!("Failed to create routine/view '{}' in MySQL: {}", r.name, e))
                    })?;
                }
                StreamRecord::Trailer(_) => break,
                _ => {}
            }
        }

        Ok(RestoreStats {
            tables_restored,
            records_processed,
        })
    }
}
