use std::io::Write;
use mysql_async::prelude::*;
use mysql_async::{Conn, Opts, Pool};
use crate::database::{BackupStats, DatabaseAdapter, DatabaseMeta, RestoreOptions, RestoreStats};
use crate::error::DumperError;
use crate::stream::decoder::StreamDecoder;
use crate::stream::encoder::StreamEncoder;
use crate::stream::format::*;

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

    async fn backup<W: Write + Send>(
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
        encoder.write_record(&StreamRecord::Header(header))?;

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

            encoder.write_record(&StreamRecord::TableSchema(TableSchemaRecord {
                schema_name: db.clone(),
                table_name: table.clone(),
                columns: Vec::new(),
                create_sql,
            }))?;

            // Stream rows as chunked JSON/TSV data
            let select_query = format!("SELECT * FROM `{}`", table);
            let mut result_stream = conn
                .query_iter(&select_query)
                .await
                .map_err(|e| DumperError::Database(e.to_string()))?;

            let mut batch_rows = Vec::new();
            let mut slice_seq = 0u64;

            while let Ok(Some(row)) = result_stream.next().await {
                let mut row_values = Vec::new();
                for col_idx in 0..row.len() {
                    let val_opt: Option<String> = row.get(col_idx);
                    row_values.push(val_opt);
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
                    }))?;
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
                }))?;
            }

            // End slice
            encoder.write_record(&StreamRecord::TableDataSlice(TableDataSliceRecord {
                schema_name: db.clone(),
                table_name: table.clone(),
                slice_seq: slice_seq + 1,
                is_last: true,
                data: Vec::new(),
            }))?;

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
            }))?;
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

    async fn restore<R: std::io::Read + Send>(
        &self,
        decoder: &mut StreamDecoder<R>,
        options: &RestoreOptions,
    ) -> Result<RestoreStats, DumperError> {
        let mut conn = self.get_conn().await?;

        let mut tables_restored = 0;
        let mut records_processed = 0u64;

        while let Some(record) = decoder.read_next_record()? {
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
                        let batch_rows: Vec<Vec<Option<String>>> = serde_json::from_slice(&d.data)?;
                        for row in batch_rows {
                            let values_str = row
                                .into_iter()
                                .map(|val| match val {
                                    Some(v) => format!("'{}'", v.replace('\'', "\\'")),
                                    None => "NULL".into(),
                                })
                                .collect::<Vec<_>>()
                                .join(", ");
                            let insert_sql = format!("INSERT INTO `{}` VALUES ({});", d.table_name, values_str);
                            let _ = conn.query_drop(&insert_sql).await;
                        }
                    }
                }
                StreamRecord::Routine(r) => {
                    let _ = conn.query_drop(&r.sql).await;
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
