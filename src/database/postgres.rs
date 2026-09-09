use crate::database::{BackupStats, DatabaseAdapter, DatabaseMeta, RestoreOptions, RestoreStats};
use crate::error::DumperError;
use crate::stream::decoder::StreamDecoder;
use crate::stream::encoder::StreamEncoder;
use crate::stream::format::*;
use futures_util::pin_mut;
use futures_util::stream::StreamExt;
use futures_util::SinkExt;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio_postgres::{Client, NoTls};
use tokio_postgres_rustls::MakeRustlsConnect;

pub struct PostgresAdapter {
    url: String,
}

fn create_rustls_connector() -> MakeRustlsConnect {
    let mut root_store = rustls::RootCertStore::empty();
    root_store.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
    let provider = std::sync::Arc::new(rustls::crypto::ring::default_provider());
    let client_config = rustls::ClientConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()
        .expect("valid TLS protocol versions")
        .with_root_certificates(root_store)
        .with_no_client_auth();
    MakeRustlsConnect::new(client_config)
}

impl PostgresAdapter {
    pub fn new(url: &str) -> Self {
        Self {
            url: url.to_string(),
        }
    }

    async fn connect_with_db(&self, db_override: Option<&str>) -> Result<Client, DumperError> {
        let mut config: tokio_postgres::Config = self.url.parse().map_err(|e| {
            DumperError::Database(format!(
                "Invalid PostgreSQL connection URL '{}': {}",
                self.url, e
            ))
        })?;

        if let Some(target_db) = db_override {
            config.dbname(target_db);
        }

        let ssl_mode = config.get_ssl_mode();
        let client = if ssl_mode == tokio_postgres::config::SslMode::Disable {
            let (client, connection) = config.connect(NoTls).await.map_err(|e| {
                DumperError::Database(format!("PostgreSQL connection failed: {}", e))
            })?;
            tokio::spawn(async move {
                if let Err(e) = connection.await {
                    eprintln!("PostgreSQL connection error: {}", e);
                }
            });
            client
        } else {
            let tls = create_rustls_connector();
            match config.connect(tls).await {
                Ok((client, connection)) => {
                    tokio::spawn(async move {
                        if let Err(e) = connection.await {
                            eprintln!("PostgreSQL connection error: {}", e);
                        }
                    });
                    client
                }
                Err(e) => {
                    let err_str = e.to_string();
                    if ssl_mode != tokio_postgres::config::SslMode::Require
                        && (err_str.contains("server does not support TLS")
                            || err_str.contains("SSL is not supported")
                            || err_str.contains("server does not support SSL"))
                    {
                        let (client, connection) = config.connect(NoTls).await.map_err(|e| {
                            DumperError::Database(format!("PostgreSQL connection failed: {}", e))
                        })?;
                        tokio::spawn(async move {
                            if let Err(e) = connection.await {
                                eprintln!("PostgreSQL connection error: {}", e);
                            }
                        });
                        client
                    } else {
                        return Err(DumperError::Database(format!(
                            "PostgreSQL TLS connection failed: {}",
                            e
                        )));
                    }
                }
            }
        };

        Ok(client)
    }

    async fn connect(&self) -> Result<Client, DumperError> {
        self.connect_with_db(None).await
    }
}

impl DatabaseAdapter for PostgresAdapter {
    async fn inspect(&self) -> Result<DatabaseMeta, DumperError> {
        let client = self.connect().await?;

        let version_row = client
            .query_one("SELECT version()", &[])
            .await
            .map_err(|e| DumperError::Database(e.to_string()))?;
        let full_version: String = version_row.get(0);
        let server_version = full_version
            .split_whitespace()
            .take(2)
            .collect::<Vec<_>>()
            .join(" ");

        let db_row = client
            .query_one("SELECT current_database()", &[])
            .await
            .map_err(|e| DumperError::Database(e.to_string()))?;
        let database: String = db_row.get(0);

        let table_rows = client
            .query(
                "SELECT n.nspname, c.relname \
                 FROM pg_class c \
                 JOIN pg_namespace n ON n.oid = c.relnamespace \
                 WHERE c.relkind = 'r' \
                   AND n.nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast') \
                   AND n.nspname NOT LIKE 'pg_temp_%' \
                 ORDER BY n.nspname, c.relname",
                &[],
            )
            .await
            .map_err(|e| DumperError::Database(e.to_string()))?;

        let mut table_names = Vec::new();
        for row in table_rows {
            let schema: String = row.get(0);
            let table: String = row.get(1);
            table_names.push((schema, table));
        }

        Ok(DatabaseMeta {
            engine: "postgresql".into(),
            database,
            server_version,
            table_names,
        })
    }

    async fn backup<W: AsyncWrite + Unpin + Send>(
        &self,
        encoder: &mut StreamEncoder<W>,
    ) -> Result<BackupStats, DumperError> {
        let client = self.connect().await?;

        // 1. Consistent Snapshot via Repeatable Read isolation
        client
            .batch_execute("BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY;")
            .await
            .map_err(|e| {
                DumperError::Database(format!(
                    "Failed to start repeatable-read transaction: {}",
                    e
                ))
            })?;

        let meta = self.inspect().await?;

        // 2. Stream Header
        let header = StreamHeader {
            version: STREAM_VERSION,
            engine: meta.engine.clone(),
            database: meta.database.clone(),
            server_version: meta.server_version.clone(),
            dumper_version: env!("CARGO_PKG_VERSION").into(),
            start_time: chrono::Utc::now().timestamp(),
        };
        encoder.write_record(&StreamRecord::Header(header)).await?;

        // 3. User Schemas
        let schema_rows = client
            .query(
                "SELECT nspname FROM pg_namespace \
                 WHERE nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast') \
                   AND nspname NOT LIKE 'pg_temp_%' \
                 ORDER BY nspname",
                &[],
            )
            .await
            .map_err(|e| DumperError::Database(e.to_string()))?;

        for row in schema_rows {
            let schema: String = row.get(0);
            if schema != "public" {
                encoder
                    .write_record(&StreamRecord::PreData(PreDataRecord {
                        name: schema.clone(),
                        sql: format!("CREATE SCHEMA IF NOT EXISTS \"{}\";", schema),
                    }))
                    .await?;
            }
        }

        // 3.5 Custom Types (ENUM, DOMAIN)
        let type_rows = client
            .query(
                "SELECT n.nspname, t.typname, 
                  CASE 
                    WHEN t.typtype = 'e' THEN 'CREATE TYPE \"' || n.nspname || '\".\"' || t.typname || '\" AS ENUM (' || 
                      (SELECT string_agg(quote_literal(enumlabel), ', ') FROM pg_enum WHERE enumtypid = t.oid) || ');'
                    WHEN t.typtype = 'd' THEN 'CREATE DOMAIN \"' || n.nspname || '\".\"' || t.typname || '\" AS ' || format_type(t.typbasetype, t.typtypmod) || 
                      COALESCE(' DEFAULT ' || t.typdefault, '') || ';'
                  END as def
                FROM pg_type t
                JOIN pg_namespace n ON n.oid = t.typnamespace
                WHERE t.typtype IN ('e', 'd') 
                  AND n.nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
                  AND n.nspname NOT LIKE 'pg_temp_%'",
                &[],
            )
            .await
            .unwrap_or_default();

        for row in type_rows {
            let schema: String = row.get(0);
            let name: String = row.get(1);
            let def: String = row.get(2);
            encoder
                .write_record(&StreamRecord::PreData(PreDataRecord {
                    name: format!("{}.{}", schema, name),
                    sql: def,
                }))
                .await?;
        }

        // 3.6 Sequence Definitions
        let seq_ddl_rows = client
            .query(
                "SELECT schemaname, sequencename, \
                 'CREATE SEQUENCE IF NOT EXISTS \"' || schemaname || '\".\"' || sequencename || '\"' || \
                 ' AS ' || data_type || \
                 ' INCREMENT BY ' || increment_by || \
                 ' MINVALUE ' || min_value || \
                 ' MAXVALUE ' || max_value || \
                 ' START WITH ' || start_value || \
                 CASE WHEN cycle THEN ' CYCLE' ELSE ' NO CYCLE' END || \
                 ' CACHE ' || cache_size || ';' \
                 FROM pg_sequences \
                 WHERE schemaname NOT IN ('pg_catalog', 'information_schema') \
                 ORDER BY schemaname, sequencename",
                &[],
            )
            .await
            .unwrap_or_default();

        for srow in seq_ddl_rows {
            let schema: String = srow.get(0);
            let seq: String = srow.get(1);
            let sql: String = srow.get(2);
            encoder
                .write_record(&StreamRecord::PreData(PreDataRecord {
                    name: format!("{}.{}", schema, seq),
                    sql,
                }))
                .await?;
        }

        // 4. Tables and Streaming COPY Data
        let mut tables_backed_up = 0;
        let total_rows = 0u64;

        for (schema, table) in &meta.table_names {
            // Columns metadata
            let col_rows = client
                .query(
                    "SELECT \
                        a.attname::text, \
                        format_type(a.atttypid, a.atttypmod), \
                        not a.attnotnull, \
                        pg_get_expr(d.adbin, d.adrelid) \
                     FROM pg_attribute a \
                     JOIN pg_class c ON c.oid = a.attrelid \
                     JOIN pg_namespace n ON n.oid = c.relnamespace \
                     LEFT JOIN pg_attrdef d ON d.adrelid = a.attrelid AND d.adnum = a.attnum \
                     WHERE n.nspname = $1 AND c.relname = $2 \
                       AND a.attnum > 0 AND NOT a.attisdropped \
                     ORDER BY a.attnum",
                    &[schema, table],
                )
                .await
                .map_err(|e| DumperError::Database(e.to_string()))?;

            let mut columns = Vec::new();
            let mut col_defs = Vec::new();

            for crow in col_rows {
                let col_name: String = crow.get(0);
                let data_type: String = crow.get(1);
                let is_nullable: bool = crow.get(2);
                let default_val: Option<String> = crow.get(3);

                let mut def = format!("\"{}\" {}", col_name, data_type);
                if !is_nullable {
                    def.push_str(" NOT NULL");
                }
                if let Some(ref d) = default_val {
                    def.push_str(&format!(" DEFAULT {}", d));
                }
                col_defs.push(def);

                columns.push(TableColumnMeta {
                    name: col_name,
                    data_type,
                    is_nullable,
                    default_val,
                });
            }

            let create_sql = format!(
                "CREATE TABLE IF NOT EXISTS \"{}\".\"{}\" ({});",
                schema,
                table,
                col_defs.join(", ")
            );

            encoder
                .write_record(&StreamRecord::TableSchema(TableSchemaRecord {
                    schema_name: schema.clone(),
                    table_name: table.clone(),
                    columns,
                    create_sql,
                }))
                .await?;

            // Stream COPY Data directly
            let copy_sql = format!(
                "COPY \"{}\".\"{}\" TO STDOUT (FORMAT binary)",
                schema, table
            );
            let copy_out = client.copy_out(&copy_sql).await.map_err(|e| {
                DumperError::Database(format!("COPY OUT failed for {}.{}: {}", schema, table, e))
            })?;

            pin_mut!(copy_out);
            let mut slice_seq = 0u64;

            while let Some(chunk_res) = copy_out.next().await {
                let chunk = chunk_res
                    .map_err(|e| DumperError::Database(format!("COPY read error: {}", e)))?;
                slice_seq += 1;
                encoder
                    .write_record(&StreamRecord::TableDataSlice(TableDataSliceRecord {
                        schema_name: schema.clone(),
                        table_name: table.clone(),
                        slice_seq,
                        is_last: false,
                        data: chunk.to_vec(),
                    }))
                    .await?;
            }

            // Signal end of table slice
            encoder
                .write_record(&StreamRecord::TableDataSlice(TableDataSliceRecord {
                    schema_name: schema.clone(),
                    table_name: table.clone(),
                    slice_seq: slice_seq + 1,
                    is_last: true,
                    data: Vec::new(),
                }))
                .await?;

            tables_backed_up += 1;
        }

        // 5. Sequences
        let seq_rows = client
            .query(
                "SELECT n.nspname, c.relname \
                 FROM pg_class c \
                 JOIN pg_namespace n ON n.oid = c.relnamespace \
                 WHERE c.relkind = 'S' \
                   AND n.nspname NOT IN ('pg_catalog', 'information_schema') \
                 ORDER BY n.nspname, c.relname",
                &[],
            )
            .await
            .unwrap_or_default();

        for srow in seq_rows {
            let schema: String = srow.get(0);
            let seq: String = srow.get(1);
            let seq_val_sql = format!(
                "SELECT last_value, is_called FROM \"{}\".\"{}\"",
                schema, seq
            );
            let val_row = client.query_one(&seq_val_sql, &[]).await.map_err(|e| {
                DumperError::Database(format!(
                    "Failed to query sequence {}.{}: {}",
                    schema, seq, e
                ))
            })?;
            let last_value: i64 = val_row.get(0);
            let is_called: bool = val_row.get(1);
            encoder
                .write_record(&StreamRecord::Sequence(SequenceRecord {
                    schema_name: schema,
                    sequence_name: seq,
                    last_value,
                    is_called,
                }))
                .await?;
        }

        // 6. Views & Materialized Views
        let view_rows = client
            .query(
                "SELECT n.nspname, c.relname, pg_get_viewdef(c.oid), c.relkind
                 FROM pg_class c
                 JOIN pg_namespace n ON n.oid = c.relnamespace
                 WHERE c.relkind IN ('v', 'm')
                   AND n.nspname NOT IN ('pg_catalog', 'information_schema')
                 ORDER BY n.nspname, c.relname",
                &[],
            )
            .await
            .unwrap_or_default();

        for vrow in view_rows {
            let schema: String = vrow.get(0);
            let view_name: String = vrow.get(1);
            let view_def: String = vrow.get(2);
            let relkind: i8 = vrow.get(3);
            let sql = if relkind == b'm' as i8 {
                format!(
                    "CREATE MATERIALIZED VIEW \"{}\".\"{}\" AS {}",
                    schema, view_name, view_def
                )
            } else {
                format!(
                    "CREATE OR REPLACE VIEW \"{}\".\"{}\" AS {}",
                    schema, view_name, view_def
                )
            };
            let r_type = if relkind == b'm' as i8 {
                "MATERIALIZED_VIEW"
            } else {
                "VIEW"
            };
            encoder
                .write_record(&StreamRecord::Routine(RoutineRecord {
                    schema_name: schema,
                    name: view_name,
                    routine_type: r_type.into(),
                    sql,
                }))
                .await?;
        }

        // 7. Functions
        let func_rows = client
            .query(
                "SELECT n.nspname, p.proname, pg_get_functiondef(p.oid)
                 FROM pg_proc p
                 JOIN pg_namespace n ON n.oid = p.pronamespace
                 WHERE n.nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
                   AND n.nspname NOT LIKE 'pg_temp_%'",
                &[],
            )
            .await
            .unwrap_or_default();

        for row in func_rows {
            let schema: String = row.get(0);
            let func_name: String = row.get(1);
            if let Some(func_def) = row.get::<_, Option<String>>(2) {
                encoder
                    .write_record(&StreamRecord::Routine(RoutineRecord {
                        schema_name: schema,
                        name: func_name,
                        routine_type: "FUNCTION".into(),
                        sql: func_def,
                    }))
                    .await?;
            }
        }

        // 8. Secondary Indexes
        let index_rows = client
            .query(
                "SELECT schemaname, tablename, indexname, indexdef \
                 FROM pg_indexes \
                 WHERE schemaname NOT IN ('pg_catalog', 'information_schema') \
                   AND indexname NOT LIKE '%_pkey' \
                 ORDER BY schemaname, tablename, indexname",
                &[],
            )
            .await
            .unwrap_or_default();

        for irow in index_rows {
            let schema: String = irow.get(0);
            let table: String = irow.get(1);
            let index_name: String = irow.get(2);
            let index_def: String = irow.get(3);
            encoder
                .write_record(&StreamRecord::PostData(PostDataRecord {
                    schema_name: schema,
                    table_name: table,
                    name: index_name,
                    sql: format!("{};", index_def),
                }))
                .await?;
        }

        // 9. Constraints (Primary, Foreign, Unique, Check)
        let constraint_rows = client
            .query(
                "SELECT n.nspname, c.relname, con.conname, pg_get_constraintdef(con.oid), con.contype
                 FROM pg_constraint con
                 JOIN pg_class c ON c.oid = con.conrelid
                 JOIN pg_namespace n ON n.oid = c.relnamespace
                 WHERE n.nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
                   AND n.nspname NOT LIKE 'pg_temp_%'
                   AND con.contype IN ('p', 'f', 'u', 'c')
                 ORDER BY con.contype DESC", // Primary keys 'p', then others
                &[],
            )
            .await
            .unwrap_or_default();

        for row in constraint_rows {
            let schema: String = row.get(0);
            let table: String = row.get(1);
            let name: String = row.get(2);
            let def: String = row.get(3);
            encoder
                .write_record(&StreamRecord::PostData(PostDataRecord {
                    schema_name: schema.clone(),
                    table_name: table.clone(),
                    name: name.clone(),
                    sql: format!(
                        "ALTER TABLE \"{}\".\"{}\" ADD CONSTRAINT \"{}\" {};",
                        schema, table, name, def
                    ),
                }))
                .await?;
        }

        // 10. Triggers
        let trigger_rows = client
            .query(
                "SELECT n.nspname, c.relname, t.tgname, pg_get_triggerdef(t.oid)
                 FROM pg_trigger t
                 JOIN pg_class c ON t.tgrelid = c.oid
                 JOIN pg_namespace n ON c.relnamespace = n.oid
                 WHERE NOT t.tgisinternal
                   AND n.nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast')",
                &[],
            )
            .await
            .unwrap_or_default();

        for row in trigger_rows {
            let schema: String = row.get(0);
            let table: String = row.get(1);
            let name: String = row.get(2);
            let def: String = row.get(3);
            encoder
                .write_record(&StreamRecord::PostData(PostDataRecord {
                    schema_name: schema,
                    table_name: table,
                    name,
                    sql: format!("{};", def),
                }))
                .await?;
        }

        // Commit/End read transaction
        let _ = client.batch_execute("COMMIT;").await;

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
        let client = self
            .connect_with_db(options.target_database_override.as_deref())
            .await?;

        let mut tables_restored = 0;
        let mut records_processed = 0u64;

        type ActiveCopySink = (
            String,
            String,
            std::pin::Pin<Box<tokio_postgres::CopyInSink<bytes::Bytes>>>,
        );
        let mut active_copy_sink: Option<ActiveCopySink> = None;

        while let Some(record) = decoder.read_next_record().await? {
            records_processed += 1;
            match record {
                StreamRecord::Header(h) => {
                    // Check compatibility
                    if h.engine != "postgresql" {
                        return Err(DumperError::Restore(format!(
                            "Cannot restore a '{}' backup into a PostgreSQL target database",
                            h.engine
                        )));
                    }
                }
                StreamRecord::PreData(p) => {
                    client.batch_execute(&p.sql).await.map_err(|e| {
                        DumperError::Restore(format!("Failed to execute pre-data DDL: {}", e))
                    })?;
                }
                StreamRecord::TableSchema(s) => {
                    if options.drop_existing {
                        let drop_sql = format!(
                            "DROP TABLE IF EXISTS \"{}\".\"{}\" CASCADE;",
                            s.schema_name, s.table_name
                        );
                        let _ = client.batch_execute(&drop_sql).await;
                    }

                    // Pre-create any sequences referenced in column defaults to avoid relation does not exist error
                    for col in &s.columns {
                        if let Some(ref default_expr) = col.default_val {
                            if let Some(create_seq_sql) =
                                extract_sequence_from_default(default_expr, &s.schema_name)
                            {
                                let _ = client.batch_execute(&create_seq_sql).await;
                            }
                        }
                    }

                    client.batch_execute(&s.create_sql).await.map_err(|e| {
                        let msg = if let Some(d) = e.as_db_error() {
                            format!("{}: {}", d.message(), d.detail().unwrap_or(""))
                        } else {
                            e.to_string()
                        };
                        DumperError::Restore(format!(
                            "Failed to create table {}.{}: {} (SQL: {})",
                            s.schema_name, s.table_name, msg, s.create_sql
                        ))
                    })?;
                    tables_restored += 1;
                }
                StreamRecord::TableDataSlice(d) => {
                    // Ensure active COPY sink is initialized for this table
                    let needs_new_sink = match active_copy_sink {
                        Some((ref s, ref t, _)) => s != &d.schema_name || t != &d.table_name,
                        None => true,
                    };

                    if needs_new_sink && !d.is_last {
                        let copy_sql = format!(
                            "COPY \"{}\".\"{}\" FROM STDIN (FORMAT binary)",
                            d.schema_name, d.table_name
                        );
                        let sink = Box::pin(client.copy_in(&copy_sql).await.map_err(|e| {
                            DumperError::Restore(format!(
                                "COPY IN initialization failed for {}.{}: {}",
                                d.schema_name, d.table_name, e
                            ))
                        })?);
                        active_copy_sink =
                            Some((d.schema_name.clone(), d.table_name.clone(), sink));
                    }

                    if let Some((_, _, ref mut sink)) = active_copy_sink {
                        if !d.data.is_empty() {
                            sink.as_mut()
                                .feed(bytes::Bytes::from(d.data))
                                .await
                                .map_err(|e| {
                                    DumperError::Restore(format!(
                                        "COPY IN data write failed: {}",
                                        e
                                    ))
                                })?;
                            sink.as_mut().flush().await.map_err(|e| {
                                DumperError::Restore(format!("COPY IN flush failed: {}", e))
                            })?;
                        }
                    }

                    if d.is_last {
                        if let Some((_, _, mut sink)) = active_copy_sink.take() {
                            sink.as_mut().finish().await.map_err(|e| {
                                DumperError::Restore(format!(
                                    "COPY IN finish failed for {}.{}: {}",
                                    d.schema_name, d.table_name, e
                                ))
                            })?;
                        }
                    }
                }
                StreamRecord::Sequence(seq) => {
                    let seq_sql = format!(
                        "CREATE SEQUENCE IF NOT EXISTS \"{}\".\"{}\"; SELECT setval('\"{}\".\"{}\"', {}, {});",
                        seq.schema_name, seq.sequence_name, seq.schema_name, seq.sequence_name, seq.last_value, seq.is_called
                    );
                    client.batch_execute(&seq_sql).await.map_err(|e| {
                        let msg = if let Some(d) = e.as_db_error() {
                            format!("{}: {}", d.message(), d.detail().unwrap_or(""))
                        } else {
                            e.to_string()
                        };
                        DumperError::Restore(format!(
                            "Failed to create and set sequence {}.{}: {}",
                            seq.schema_name, seq.sequence_name, msg
                        ))
                    })?;
                }
                StreamRecord::PostData(post) => {
                    client.batch_execute(&post.sql).await.map_err(|e| {
                        DumperError::Restore(format!(
                            "Failed to execute post-data constraint/index '{}': {}",
                            post.name, e
                        ))
                    })?;
                }
                StreamRecord::Routine(routine) => {
                    client.batch_execute(&routine.sql).await.map_err(|e| {
                        DumperError::Restore(format!(
                            "Failed to execute routine SQL for '{}': {}",
                            routine.name, e
                        ))
                    })?;
                }
                StreamRecord::Trailer(_) => {
                    break;
                }
            }
        }

        Ok(RestoreStats {
            tables_restored,
            records_processed,
        })
    }
}

fn extract_sequence_from_default(default_expr: &str, default_schema: &str) -> Option<String> {
    let start = default_expr.find("nextval('")?;
    let rest = &default_expr[start + 9..];
    let end = rest.find('\'')?;
    let seq_ref = &rest[..end];

    if seq_ref.contains('.') {
        let parts: Vec<&str> = seq_ref.splitn(2, '.').collect();
        let s = parts[0].trim_matches('"');
        let q = parts[1].trim_matches('"');
        Some(format!(
            "CREATE SEQUENCE IF NOT EXISTS \"{}\".\"{}\";",
            s, q
        ))
    } else {
        let q = seq_ref.trim_matches('"');
        Some(format!(
            "CREATE SEQUENCE IF NOT EXISTS \"{}\".\"{}\";",
            default_schema, q
        ))
    }
}
