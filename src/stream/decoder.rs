use tokio::io::{AsyncRead, AsyncReadExt};
use crc32fast::Hasher as CrcHasher;
use crate::error::DumperError;
use crate::stream::format::*;
use sha2::{Digest, Sha256};

pub struct StreamDecoder<R: AsyncRead + Unpin + Send> {
    reader: R,
    magic_checked: bool,
    records_read: u64,
    bytes_read: u64,
    reached_eof: bool,
    trailer_verified: bool,
    stream_hasher: Sha256,
}

impl<R: AsyncRead + Unpin + Send> StreamDecoder<R> {
    pub fn new(reader: R) -> Self {
        Self {
            reader,
            magic_checked: false,
            records_read: 0,
            bytes_read: 0,
            reached_eof: false,
            trailer_verified: false,
            stream_hasher: Sha256::new(),
        }
    }

    async fn check_magic(&mut self) -> Result<(), DumperError> {
        if !self.magic_checked {
            let mut magic = [0u8; 4];
            self.reader
                .read_exact(&mut magic).await
                .map_err(|e| DumperError::Format(format!("Failed to read stream magic: {}", e)))?;
            if &magic != STREAM_MAGIC {
                return Err(DumperError::Format(format!(
                    "Invalid stream magic: expected {:?}, got {:?}",
                    STREAM_MAGIC, magic
                )));
            }
            self.stream_hasher.update(STREAM_MAGIC);
            self.bytes_read += 4;
            self.magic_checked = true;
        }
        Ok(())
    }

    /// Read next record from the stream. Returns `None` at EOF after valid Trailer.
    pub async fn read_next_record(&mut self) -> Result<Option<StreamRecord>, DumperError> {
        if self.reached_eof {
            return Ok(None);
        }

        self.check_magic().await?;

        let mut header = [0u8; 6]; // type (1B) + flags (1B) + len (4B)
        match self.reader.read_exact(&mut header).await {
            Ok(_) => {}
            Err(ref e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                if self.trailer_verified {
                    self.reached_eof = true;
                    return Ok(None);
                } else {
                    return Err(DumperError::Integrity(
                        "Stream ended prematurely before Trailer record was received".into(),
                    ));
                }
            }
            Err(e) => return Err(DumperError::Io(e)),
        }

        let type_byte = header[0];
        let flags = header[1];
        let len_bytes = [header[2], header[3], header[4], header[5]];
        let payload_len = u32::from_le_bytes(len_bytes) as usize;

        // Bounded payload limit to prevent malicious memory allocation
        const MAX_PAYLOAD_SIZE: usize = 8 * 1024 * 1024; // 8 MiB
        if payload_len > MAX_PAYLOAD_SIZE {
            return Err(DumperError::Integrity(format!(
                "Payload size {} exceeds maximum allowed frame limit",
                payload_len
            )));
        }

        let mut payload = vec![0u8; payload_len];
        self.reader.read_exact(&mut payload).await.map_err(|e| {
            if e.kind() == std::io::ErrorKind::UnexpectedEof {
                DumperError::Integrity(format!(
                    "Stream truncated while reading payload for record {}",
                    self.records_read
                ))
            } else {
                DumperError::Io(e)
            }
        })?;

        let mut crc_bytes = [0u8; 4];
        self.reader.read_exact(&mut crc_bytes).await.map_err(|e| {
            if e.kind() == std::io::ErrorKind::UnexpectedEof {
                DumperError::Integrity(format!(
                    "Stream truncated while reading CRC for record {}",
                    self.records_read
                ))
            } else {
                DumperError::Io(e)
            }
        })?;
        let expected_crc = u32::from_le_bytes(crc_bytes);

        // Verify CRC
        let mut crc_hasher = CrcHasher::new();
        crc_hasher.update(&[type_byte, flags]);
        crc_hasher.update(&len_bytes);
        crc_hasher.update(&payload);
        let calculated_crc = crc_hasher.finalize();

        if calculated_crc != expected_crc {
            return Err(DumperError::Integrity(format!(
                "CRC32 mismatch in record {}: expected 0x{:08x}, calculated 0x{:08x}",
                self.records_read, expected_crc, calculated_crc
            )));
        }

        self.records_read += 1;
        self.bytes_read += 6 + payload_len as u64 + 4;

        let record_type = RecordType::try_from(type_byte)
            .map_err(|b| DumperError::Format(format!("Unknown record type 0x{:02x}", b)))?;

        let pre_trailer_hash = if record_type == RecordType::Trailer {
            Some(hex::encode(self.stream_hasher.clone().finalize()))
        } else {
            None
        };

        self.stream_hasher.update([type_byte, flags]);
        self.stream_hasher.update(len_bytes);
        self.stream_hasher.update(&payload);
        self.stream_hasher.update(crc_bytes);

        let record = match record_type {
            RecordType::Header => {
                let h: StreamHeader = serde_json::from_slice(&payload)?;
                StreamRecord::Header(h)
            }
            RecordType::PreData => {
                let p: PreDataRecord = serde_json::from_slice(&payload)?;
                StreamRecord::PreData(p)
            }
            RecordType::TableSchema => {
                let s: TableSchemaRecord = serde_json::from_slice(&payload)?;
                StreamRecord::TableSchema(s)
            }
            RecordType::TableDataSlice => {
                let d: TableDataSliceRecord = serde_json::from_slice(&payload)?;
                StreamRecord::TableDataSlice(d)
            }
            RecordType::Sequence => {
                let s: SequenceRecord = serde_json::from_slice(&payload)?;
                StreamRecord::Sequence(s)
            }
            RecordType::PostData => {
                let p: PostDataRecord = serde_json::from_slice(&payload)?;
                StreamRecord::PostData(p)
            }
            RecordType::Routine => {
                let r: RoutineRecord = serde_json::from_slice(&payload)?;
                StreamRecord::Routine(r)
            }
            RecordType::Trailer => {
                let t: StreamTrailer = serde_json::from_slice(&payload)?;
                let expected_hash = pre_trailer_hash.unwrap();
                if expected_hash != t.stream_hash_hex {
                    return Err(DumperError::Integrity(format!(
                        "Stream SHA-256 hash mismatch: expected {}, calculated {}",
                        t.stream_hash_hex, expected_hash
                    )));
                }
                if t.total_records != self.records_read - 1 {
                    return Err(DumperError::Integrity(format!(
                        "Stream total records mismatch: expected {}, calculated {}",
                        t.total_records, self.records_read - 1
                    )));
                }
                self.trailer_verified = true;
                self.reached_eof = true;
                StreamRecord::Trailer(t)
            }
        };

        Ok(Some(record))
    }

    pub fn records_read(&self) -> u64 {
        self.records_read
    }

    pub fn bytes_read(&self) -> u64 {
        self.bytes_read
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::stream::encoder::StreamEncoder;

    #[tokio::test]
    async fn test_stream_encode_decode_roundtrip() {
        let mut buffer = Vec::new();
        {
            let mut encoder = StreamEncoder::new(&mut buffer);

            let header = StreamHeader {
                version: 1,
                engine: "postgresql".into(),
                database: "production_app".into(),
                server_version: "17.4".into(),
                dumper_version: "0.1.0".into(),
                start_time: 1700000000,
            };
            encoder.write_record(&StreamRecord::Header(header)).await.unwrap();

            let schema = TableSchemaRecord {
                schema_name: "public".into(),
                table_name: "users".into(),
                columns: vec![TableColumnMeta {
                    name: "id".into(),
                    data_type: "integer".into(),
                    is_nullable: false,
                    default_val: None,
                }],
                create_sql: "CREATE TABLE public.users (id integer NOT NULL);".into(),
            };
            encoder.write_record(&StreamRecord::TableSchema(schema)).await.unwrap();

            let data_slice = TableDataSliceRecord {
                schema_name: "public".into(),
                table_name: "users".into(),
                slice_seq: 1,
                is_last: true,
                data: vec![1, 2, 3, 4, 5, 6, 7, 8],
            };
            encoder.write_record(&StreamRecord::TableDataSlice(data_slice)).await.unwrap();

            encoder.finish().await.unwrap();
        }

        // Now decode
        let mut decoder = StreamDecoder::new(&buffer[..]);
        let r1 = decoder.read_next_record().await.unwrap().unwrap();
        match r1 {
            StreamRecord::Header(h) => {
                assert_eq!(h.database, "production_app");
                assert_eq!(h.engine, "postgresql");
            }
            _ => panic!("Expected Header"),
        }

        let r2 = decoder.read_next_record().await.unwrap().unwrap();
        match r2 {
            StreamRecord::TableSchema(s) => {
                assert_eq!(s.table_name, "users");
            }
            _ => panic!("Expected TableSchema"),
        }

        let r3 = decoder.read_next_record().await.unwrap().unwrap();
        match r3 {
            StreamRecord::TableDataSlice(d) => {
                assert_eq!(d.data, vec![1, 2, 3, 4, 5, 6, 7, 8]);
                assert!(d.is_last);
            }
            _ => panic!("Expected TableDataSlice"),
        }

        let r4 = decoder.read_next_record().await.unwrap().unwrap();
        match r4 {
            StreamRecord::Trailer(t) => {
                assert_eq!(t.total_records, 3); // Header, Schema, Data
                assert_eq!(decoder.records_read(), 4); // Including trailer
            }
            _ => panic!("Expected Trailer"),
        }

        let r5 = decoder.read_next_record().await.unwrap();
        assert!(r5.is_none());
    }
}
