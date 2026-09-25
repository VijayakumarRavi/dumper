use crate::database::{BackupStats, DatabaseAdapter, DatabaseMeta, RestoreOptions, RestoreStats};
use crate::error::DumperError;
use crate::stream::decoder::StreamDecoder;
use crate::stream::encoder::StreamEncoder;
use crate::stream::format::*;
use futures_util::pin_mut;
use futures_util::stream::StreamExt;
use futures_util::SinkExt;
use std::collections::{BTreeSet, HashMap, HashSet};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio_postgres::{Client, NoTls};
use tokio_postgres_rustls::MakeRustlsConnect;

pub struct PostgresAdapter {
    url: String,
}

/// Quotes a PostgreSQL identifier using double quotes with internal double quotes doubled.
pub fn quote_pg_identifier(id: &str) -> String {
    format!("\"{}\"", id.replace('"', "\"\""))
}

/// Quotes a PostgreSQL literal string using single quotes with internal single quotes doubled.
pub fn quote_pg_literal(val: &str) -> String {
    format!("'{}'", val.replace('\'', "''"))
}

#[derive(Debug)]
struct NoCertificateVerification(std::sync::Arc<rustls::crypto::CryptoProvider>);

impl rustls::client::danger::ServerCertVerifier for NoCertificateVerification {
    fn verify_server_cert(
        &self,
        _end_entity: &rustls::pki_types::CertificateDer<'_>,
        _intermediates: &[rustls::pki_types::CertificateDer<'_>],
        _server_name: &rustls::pki_types::ServerName<'_>,
        _ocsp_response: &[u8],
        _now: rustls::pki_types::UnixTime,
    ) -> Result<rustls::client::danger::ServerCertVerified, rustls::Error> {
        Ok(rustls::client::danger::ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &rustls::pki_types::CertificateDer<'_>,
        dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls12_signature(
            message,
            cert,
            dss,
            &self.0.signature_verification_algorithms,
        )
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &rustls::pki_types::CertificateDer<'_>,
        dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls13_signature(
            message,
            cert,
            dss,
            &self.0.signature_verification_algorithms,
        )
    }

    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        self.0.signature_verification_algorithms.supported_schemes()
    }
}

fn create_rustls_connector(verify: bool) -> MakeRustlsConnect {
    let provider = std::sync::Arc::new(rustls::crypto::ring::default_provider());
    if verify {
        let mut root_store = rustls::RootCertStore::empty();
        root_store.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
        let client_config = rustls::ClientConfig::builder_with_provider(provider)
            .with_safe_default_protocol_versions()
            .expect("valid TLS protocol versions")
            .with_root_certificates(root_store)
            .with_no_client_auth();
        MakeRustlsConnect::new(client_config)
    } else {
        let client_config = rustls::ClientConfig::builder_with_provider(provider.clone())
            .with_safe_default_protocol_versions()
            .expect("valid TLS protocol versions")
            .dangerous()
            .with_custom_certificate_verifier(std::sync::Arc::new(NoCertificateVerification(
                provider,
            )))
            .with_no_client_auth();
        MakeRustlsConnect::new(client_config)
    }
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
                crate::error::sanitize_secrets(&self.url),
                e
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
            let lower_url = self.url.to_lowercase();
            let verify = lower_url.contains("sslmode=verify-ca")
                || lower_url.contains("sslmode=verify-full");
            let tls = create_rustls_connector(verify);
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

    /// Queries `pg_stat_ssl` to verify whether the active connection is encrypted with TLS,
    /// returning `(ssl_active, tls_version, tls_cipher)`.
    pub async fn query_ssl_stat(
        &self,
    ) -> Result<(bool, Option<String>, Option<String>), DumperError> {
        let client = self.connect().await?;
        let row = client
            .query_one(
                "SELECT ssl, version, cipher FROM pg_stat_ssl WHERE pid = pg_backend_pid()",
                &[],
            )
            .await
            .map_err(|e| DumperError::Database(format!("Failed to query pg_stat_ssl: {}", e)))?;
        let ssl: bool = row.get(0);
        let version: Option<String> = row.get(1);
        let cipher: Option<String> = row.get(2);
        Ok((ssl, version, cipher))
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
                   AND n.nspname NOT LIKE 'pg_%' \
                   AND n.nspname != 'information_schema' \
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

        // 2. Query server metadata and table list within the snapshot transaction
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
                   AND n.nspname NOT LIKE 'pg_%' \
                   AND n.nspname != 'information_schema' \
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

        // 3. Stream Header
        let header = StreamHeader {
            version: STREAM_VERSION,
            engine: "postgresql".into(),
            database: database.clone(),
            server_version: server_version.clone(),
            dumper_version: env!("CARGO_PKG_VERSION").into(),
            start_time: chrono::Utc::now().timestamp(),
        };
        encoder.write_record(&StreamRecord::Header(header)).await?;

        // 3. Extensions (e.g. citext, pgcrypto, uuid-ossp)
        let ext_rows = client
            .query(
                "SELECT extname FROM pg_extension WHERE extname != 'plpgsql' ORDER BY extname",
                &[],
            )
            .await
            .map_err(|e| {
                DumperError::Database(format!("Failed to query PostgreSQL extensions: {}", e))
            })?;

        for erow in ext_rows {
            let extname: String = erow.get(0);
            encoder
                .write_record(&StreamRecord::PreData(PreDataRecord {
                    name: format!("extension:{}", extname),
                    sql: format!(
                        "CREATE EXTENSION IF NOT EXISTS {};",
                        quote_pg_identifier(&extname)
                    ),
                }))
                .await?;
        }

        // 4. User Schemas
        let schema_rows = client
            .query(
                "SELECT nspname FROM pg_namespace \
                 WHERE nspname NOT LIKE 'pg_%' \
                   AND nspname != 'information_schema' \
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
                        sql: format!(
                            "CREATE SCHEMA IF NOT EXISTS {};",
                            quote_pg_identifier(&schema)
                        ),
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
                  AND n.nspname NOT LIKE 'pg_%'
                  AND n.nspname != 'information_schema'
                  AND NOT EXISTS (
                    SELECT 1 FROM pg_depend d
                    WHERE d.classid = 'pg_type'::regclass
                      AND d.objid = t.oid
                      AND d.deptype = 'e'
                  )",
                &[],
            )
            .await
            .map_err(|e| {
                DumperError::Database(format!("Failed to query custom types (ENUM/DOMAIN): {:?}", e))
            })?;

        for row in type_rows {
            let schema: String = row.get(0);
            let name: String = row.get(1);
            if let Some(def) = row.get::<_, Option<String>>(2) {
                encoder
                    .write_record(&StreamRecord::PreData(PreDataRecord {
                        name: format!("{}.{}", schema, name),
                        sql: def,
                    }))
                    .await?;
            }
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
                 WHERE schemaname NOT LIKE 'pg_%' AND schemaname != 'information_schema' \
                 ORDER BY schemaname, sequencename",
                &[],
            )
            .await
            .map_err(|e| {
                DumperError::Database(format!("Failed to query sequence definitions: {}", e))
            })?;

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

        for (schema, table) in &table_names {
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

                let mut def = format!("{} {}", quote_pg_identifier(&col_name), data_type);
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
                "CREATE TABLE IF NOT EXISTS {}.{} ({});",
                quote_pg_identifier(schema),
                quote_pg_identifier(table),
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
                "COPY {}.{} TO STDOUT (FORMAT binary)",
                quote_pg_identifier(schema),
                quote_pg_identifier(table)
            );
            let copy_out = client.copy_out(&copy_sql).await.map_err(|e| {
                DumperError::Database(format!("COPY OUT failed for {}.{}: {}", schema, table, e))
            })?;

            pin_mut!(copy_out);
            let mut slice_seq = 0u64;

            while let Some(chunk_res) = copy_out.next().await {
                let chunk = chunk_res
                    .map_err(|e| DumperError::Database(format!("COPY read error: {}", e)))?;
                for sub_chunk in chunk.chunks(1024 * 1024) {
                    slice_seq += 1;
                    encoder
                        .write_record(&StreamRecord::TableDataSlice(TableDataSliceRecord {
                            schema_name: schema.clone(),
                            table_name: table.clone(),
                            slice_seq,
                            is_last: false,
                            data: sub_chunk.to_vec(),
                        }))
                        .await?;
                }
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
                   AND n.nspname NOT LIKE 'pg_%' AND n.nspname != 'information_schema' \
                 ORDER BY n.nspname, c.relname",
                &[],
            )
            .await
            .map_err(|e| {
                DumperError::Database(format!("Failed to query sequence values: {}", e))
            })?;

        for srow in seq_rows {
            let schema: String = srow.get(0);
            let seq: String = srow.get(1);
            let seq_val_sql = format!(
                "SELECT last_value, is_called FROM {}.{}",
                quote_pg_identifier(&schema),
                quote_pg_identifier(&seq)
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

        // 6. Views & Materialized Views (topologically ordered by dependency)
        let view_rows = client
            .query(
                "SELECT c.oid::int8, n.nspname, c.relname, pg_get_viewdef(c.oid), c.relkind
                 FROM pg_class c
                 JOIN pg_namespace n ON n.oid = c.relnamespace
                 WHERE c.relkind IN ('v', 'm')
                   AND n.nspname NOT LIKE 'pg_%' AND n.nspname != 'information_schema'
                 ORDER BY n.nspname, c.relname",
                &[],
            )
            .await
            .map_err(|e| {
                DumperError::Database(format!(
                    "Failed to query views and materialized views: {}",
                    e
                ))
            })?;

        let raw_views: Vec<ViewMeta> = view_rows
            .into_iter()
            .map(|vrow| {
                let oid: i64 = vrow.get(0);
                let schema: String = vrow.get(1);
                let view_name: String = vrow.get(2);
                let view_def: String = vrow.get(3);
                let relkind: i8 = vrow.get(4);
                ViewMeta {
                    oid,
                    schema,
                    name: view_name,
                    definition: view_def,
                    is_materialized: relkind == b'm' as i8,
                }
            })
            .collect();

        let dep_rows = client
            .query(
                "SELECT DISTINCT
                    r.ev_class::int8 AS view_oid,
                    d.refobjid::int8 AS ref_view_oid
                 FROM pg_depend d
                 JOIN pg_rewrite r ON r.oid = d.objid
                 JOIN pg_class c1 ON c1.oid = r.ev_class AND c1.relkind IN ('v', 'm')
                 JOIN pg_class c2 ON c2.oid = d.refobjid AND c2.relkind IN ('v', 'm')
                 WHERE d.classid = 'pg_rewrite'::regclass
                   AND d.refclassid = 'pg_class'::regclass
                   AND r.ev_class <> d.refobjid",
                &[],
            )
            .await
            .map_err(|e| {
                DumperError::Database(format!(
                    "Failed to query view dependencies from pg_depend: {}",
                    e
                ))
            })?;

        let deps: Vec<(i64, i64)> = dep_rows
            .into_iter()
            .map(|row| (row.get(0), row.get(1)))
            .collect();

        let ordered_views = sort_views_topologically(raw_views, &deps);

        for view in ordered_views {
            let sql = if view.is_materialized {
                format!(
                    "CREATE MATERIALIZED VIEW {}.{} AS {}",
                    quote_pg_identifier(&view.schema),
                    quote_pg_identifier(&view.name),
                    view.definition
                )
            } else {
                format!(
                    "CREATE OR REPLACE VIEW {}.{} AS {}",
                    quote_pg_identifier(&view.schema),
                    quote_pg_identifier(&view.name),
                    view.definition
                )
            };
            let r_type = if view.is_materialized {
                "MATERIALIZED_VIEW"
            } else {
                "VIEW"
            };
            encoder
                .write_record(&StreamRecord::Routine(RoutineRecord {
                    schema_name: view.schema,
                    name: view.name,
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
                 WHERE p.prokind IN ('f', 'p')
                   AND n.nspname NOT LIKE 'pg_%'
                   AND n.nspname != 'information_schema'
                   AND NOT EXISTS (
                     SELECT 1 FROM pg_depend d
                     WHERE d.classid = 'pg_proc'::regclass
                       AND d.objid = p.oid
                       AND d.deptype = 'e'
                   )",
                &[],
            )
            .await
            .map_err(|e| DumperError::Database(format!("Failed to query functions: {}", e)))?;

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
                "SELECT n.nspname, c.relname, i.relname, pg_get_indexdef(i.oid) \
                 FROM pg_index x \
                 JOIN pg_class c ON c.oid = x.indrelid \
                 JOIN pg_class i ON i.oid = x.indexrelid \
                 JOIN pg_namespace n ON n.oid = c.relnamespace \
                 WHERE n.nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast') \
                   AND n.nspname NOT LIKE 'pg_temp_%' \
                   AND NOT EXISTS ( \
                       SELECT 1 FROM pg_constraint con WHERE con.conindid = i.oid \
                   ) \
                 ORDER BY n.nspname, c.relname, i.relname",
                &[],
            )
            .await
            .map_err(|e| {
                DumperError::Database(format!("Failed to query secondary indexes: {}", e))
            })?;

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
            .map_err(|e| {
                DumperError::Database(format!(
                    "Failed to query constraints (PK/FK/UNIQUE/CHECK): {}",
                    e
                ))
            })?;

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
                   AND n.nspname NOT LIKE 'pg_%' AND n.nspname != 'information_schema'",
                &[],
            )
            .await
            .map_err(|e| DumperError::Database(format!("Failed to query triggers: {}", e)))?;

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
            engine: "postgresql".into(),
            database,
            server_version,
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
                    let trimmed = p.sql.trim();
                    if trimmed.to_ascii_uppercase().starts_with("CREATE SCHEMA")
                        && (p.name.starts_with("pg_")
                            || p.name.starts_with("\"pg_")
                            || p.name.contains("pg_temp")
                            || p.name.contains("pg_toast"))
                    {
                        eprintln!("Skipping restore of reserved system schema '{}'", p.name);
                        continue;
                    }
                    client.batch_execute(&p.sql).await.map_err(|e| {
                        let detail = if let Some(dbe) = e.as_db_error() {
                            format!(
                                "{}: {} (code: {:?})",
                                dbe.message(),
                                dbe.detail().unwrap_or(""),
                                dbe.code()
                            )
                        } else {
                            e.to_string()
                        };
                        DumperError::Restore(format!(
                            "Failed to execute pre-data DDL for '{}' (SQL: '{}'): {}",
                            p.name, p.sql, detail
                        ))
                    })?;
                }
                StreamRecord::TableSchema(s) => {
                    if options.drop_existing {
                        let drop_sql = format!(
                            "DROP TABLE IF EXISTS {}.{} CASCADE;",
                            quote_pg_identifier(&s.schema_name),
                            quote_pg_identifier(&s.table_name)
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

                    if needs_new_sink {
                        if let Some((old_s, old_t, mut old_sink)) = active_copy_sink.take() {
                            old_sink.as_mut().finish().await.map_err(|e| {
                                DumperError::Restore(format!(
                                    "COPY IN finish failed for {}.{}: {}",
                                    old_s, old_t, e
                                ))
                            })?;
                        }
                        if !d.is_last {
                            let copy_sql = format!(
                                "COPY {}.{} FROM STDIN (FORMAT binary)",
                                quote_pg_identifier(&d.schema_name),
                                quote_pg_identifier(&d.table_name)
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
                    let regclass = format!(
                        "{}.{}",
                        quote_pg_identifier(&seq.schema_name),
                        quote_pg_identifier(&seq.sequence_name)
                    );
                    let seq_sql = format!(
                        "CREATE SEQUENCE IF NOT EXISTS {}; SELECT setval({}, {}, {});",
                        regclass,
                        quote_pg_literal(&regclass),
                        seq.last_value,
                        seq.is_called
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

        // Finalize any active sink that was left unclosed before stream end
        if let Some((s, t, mut sink)) = active_copy_sink.take() {
            sink.as_mut().finish().await.map_err(|e| {
                DumperError::Restore(format!("COPY IN finish failed for {}.{}: {}", s, t, e))
            })?;
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
            "CREATE SEQUENCE IF NOT EXISTS {}.{};",
            quote_pg_identifier(s),
            quote_pg_identifier(q)
        ))
    } else {
        let q = seq_ref.trim_matches('"');
        Some(format!(
            "CREATE SEQUENCE IF NOT EXISTS {}.{};",
            quote_pg_identifier(default_schema),
            quote_pg_identifier(q)
        ))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ViewMeta {
    pub oid: i64,
    pub schema: String,
    pub name: String,
    pub definition: String,
    pub is_materialized: bool,
}

/// Topologically sorts PostgreSQL views by dependency order using Kahn's algorithm.
///
/// `deps` contains pairs `(view_oid, ref_view_oid)` where `view_oid` depends on `ref_view_oid`.
/// Referenced views are ordered before dependent views so they can be created sequentially
/// during restore without `relation does not exist` errors.
///
/// Ties between views with equal dependency priority are resolved alphabetically by `(schema, name)`.
/// If a cyclic dependency is detected, remaining views fall back to alphabetical ordering with a warning.
pub fn sort_views_topologically(views: Vec<ViewMeta>, deps: &[(i64, i64)]) -> Vec<ViewMeta> {
    if views.is_empty() {
        return views;
    }

    let view_map: HashMap<i64, ViewMeta> = views.into_iter().map(|v| (v.oid, v)).collect();

    let mut in_deps: HashMap<i64, HashSet<i64>> = HashMap::new();
    let mut out_deps: HashMap<i64, Vec<i64>> = HashMap::new();

    for &oid in view_map.keys() {
        in_deps.entry(oid).or_default();
        out_deps.entry(oid).or_default();
    }

    for &(view_oid, ref_view_oid) in deps {
        if view_map.contains_key(&view_oid)
            && view_map.contains_key(&ref_view_oid)
            && view_oid != ref_view_oid
        {
            in_deps.entry(view_oid).or_default().insert(ref_view_oid);
            out_deps.entry(ref_view_oid).or_default().push(view_oid);
        }
    }

    // Ready set: views with 0 incoming dependencies.
    // Stored as (schema, name, oid) in BTreeSet for deterministic alphabetical tie-breaking.
    let mut ready: BTreeSet<(String, String, i64)> = BTreeSet::new();
    for (&oid, dep_set) in &in_deps {
        if dep_set.is_empty() {
            let v = &view_map[&oid];
            ready.insert((v.schema.clone(), v.name.clone(), oid));
        }
    }

    let mut sorted = Vec::with_capacity(view_map.len());
    let mut visited: HashSet<i64> = HashSet::new();

    while let Some((_, _, oid)) = ready.pop_first() {
        visited.insert(oid);
        sorted.push(view_map[&oid].clone());

        if let Some(dependents) = out_deps.get(&oid) {
            for &dep_oid in dependents {
                if let Some(pending) = in_deps.get_mut(&dep_oid) {
                    pending.remove(&oid);
                    if pending.is_empty() && !visited.contains(&dep_oid) {
                        let dep_view = &view_map[&dep_oid];
                        ready.insert((dep_view.schema.clone(), dep_view.name.clone(), dep_oid));
                    }
                }
            }
        }
    }

    // If there's a cycle or unvisited views, fall back to alphabetical for remaining
    if sorted.len() < view_map.len() {
        eprintln!(
            "Warning: circular dependency detected among PostgreSQL views; falling back to alphabetical order for remaining views"
        );
        let mut remaining: Vec<_> = view_map
            .values()
            .filter(|v| !visited.contains(&v.oid))
            .cloned()
            .collect();
        remaining.sort_by(|a, b| (&a.schema, &a.name).cmp(&(&b.schema, &b.name)));
        sorted.extend(remaining);
    }

    sorted
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_quote_pg_identifier() {
        assert_eq!(quote_pg_identifier("users"), "\"users\"");
        assert_eq!(quote_pg_identifier("user\"name"), "\"user\"\"name\"");
        assert_eq!(quote_pg_identifier("public.users"), "\"public.users\"");
    }

    #[test]
    fn test_quote_pg_literal() {
        assert_eq!(quote_pg_literal("hello"), "'hello'");
        assert_eq!(quote_pg_literal("O'Reilly"), "'O''Reilly'");
        assert_eq!(quote_pg_literal(""), "''");
    }

    #[test]
    fn test_topological_sort_views_empty() {
        let sorted = sort_views_topologically(vec![], &[]);
        assert!(sorted.is_empty());
    }

    #[test]
    fn test_topological_sort_views_independent_sorted_alphabetically() {
        let v1 = ViewMeta {
            oid: 10,
            schema: "public".into(),
            name: "zeta_view".into(),
            definition: "SELECT 1".into(),
            is_materialized: false,
        };
        let v2 = ViewMeta {
            oid: 20,
            schema: "public".into(),
            name: "alpha_view".into(),
            definition: "SELECT 2".into(),
            is_materialized: false,
        };
        let sorted = sort_views_topologically(vec![v1, v2], &[]);
        assert_eq!(sorted[0].name, "alpha_view");
        assert_eq!(sorted[1].name, "zeta_view");
    }

    #[test]
    fn test_topological_sort_views_dependency_order() {
        // alpha_view depends on zeta_view.
        // Alphabetically, alpha_view would be first.
        // But topologically, zeta_view must precede alpha_view.
        let alpha = ViewMeta {
            oid: 10,
            schema: "public".into(),
            name: "alpha_view".into(),
            definition: "SELECT * FROM zeta_view".into(),
            is_materialized: false,
        };
        let zeta = ViewMeta {
            oid: 20,
            schema: "public".into(),
            name: "zeta_view".into(),
            definition: "SELECT 1".into(),
            is_materialized: false,
        };
        // Dependency: view 10 (alpha) depends on view 20 (zeta)
        let deps = vec![(10, 20)];
        let sorted = sort_views_topologically(vec![alpha, zeta], &deps);
        assert_eq!(sorted.len(), 2);
        assert_eq!(sorted[0].name, "zeta_view");
        assert_eq!(sorted[1].name, "alpha_view");
    }

    #[test]
    fn test_topological_sort_views_diamond_dependency() {
        let base = ViewMeta {
            oid: 1,
            schema: "public".into(),
            name: "base".into(),
            definition: "SELECT 1".into(),
            is_materialized: false,
        };
        let mid_b = ViewMeta {
            oid: 2,
            schema: "public".into(),
            name: "mid_b".into(),
            definition: "SELECT * FROM base".into(),
            is_materialized: false,
        };
        let mid_a = ViewMeta {
            oid: 3,
            schema: "public".into(),
            name: "mid_a".into(),
            definition: "SELECT * FROM base".into(),
            is_materialized: false,
        };
        let top = ViewMeta {
            oid: 4,
            schema: "public".into(),
            name: "top".into(),
            definition: "SELECT * FROM mid_a, mid_b".into(),
            is_materialized: false,
        };

        let deps = vec![
            (2, 1), // mid_b depends on base
            (3, 1), // mid_a depends on base
            (4, 2), // top depends on mid_b
            (4, 3), // top depends on mid_a
        ];

        let sorted = sort_views_topologically(vec![top, mid_b, base, mid_a], &deps);
        assert_eq!(sorted.len(), 4);
        assert_eq!(sorted[0].name, "base");
        assert_eq!(sorted[1].name, "mid_a");
        assert_eq!(sorted[2].name, "mid_b");
        assert_eq!(sorted[3].name, "top");
    }

    #[test]
    fn test_topological_sort_views_cycle_fallback() {
        let v1 = ViewMeta {
            oid: 1,
            schema: "public".into(),
            name: "view_b".into(),
            definition: "SELECT 1".into(),
            is_materialized: false,
        };
        let v2 = ViewMeta {
            oid: 2,
            schema: "public".into(),
            name: "view_a".into(),
            definition: "SELECT 1".into(),
            is_materialized: false,
        };
        // Circular dependency
        let deps = vec![(1, 2), (2, 1)];
        let sorted = sort_views_topologically(vec![v1, v2], &deps);
        assert_eq!(sorted.len(), 2);
        assert_eq!(sorted[0].name, "view_a");
        assert_eq!(sorted[1].name, "view_b");
    }
}
