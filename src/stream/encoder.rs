use crate::error::DumperError;
use crate::stream::format::*;
use crc32fast::Hasher as CrcHasher;
use sha2::{Digest, Sha256};
use tokio::io::{AsyncWrite, AsyncWriteExt};

pub struct StreamEncoder<W: AsyncWrite + Unpin + Send> {
    writer: W,
    record_count: u64,
    total_bytes_written: u64,
    hasher: Sha256,
    magic_written: bool,
}

impl<W: AsyncWrite + Unpin + Send> StreamEncoder<W> {
    pub fn new(writer: W) -> Self {
        Self {
            writer,
            record_count: 0,
            total_bytes_written: 0,
            hasher: Sha256::new(),
            magic_written: false,
        }
    }

    async fn ensure_magic(&mut self) -> Result<(), DumperError> {
        if !self.magic_written {
            self.writer.write_all(STREAM_MAGIC).await?;
            self.hasher.update(STREAM_MAGIC);
            self.total_bytes_written += STREAM_MAGIC.len() as u64;
            self.magic_written = true;
        }
        Ok(())
    }

    pub async fn write_raw_record(
        &mut self,
        record_type: RecordType,
        flags: u8,
        payload: &[u8],
    ) -> Result<(), DumperError> {
        if payload.len() > MAX_PAYLOAD_SIZE {
            return Err(DumperError::Format(format!(
                "Payload size {} exceeds maximum allowed frame limit {}",
                payload.len(),
                MAX_PAYLOAD_SIZE
            )));
        }

        self.ensure_magic().await?;

        let mut crc_hasher = CrcHasher::new();
        let type_byte = record_type as u8;
        crc_hasher.update(&[type_byte, flags]);

        let len_bytes = (payload.len() as u32).to_le_bytes();
        crc_hasher.update(&len_bytes);
        crc_hasher.update(payload);
        let crc = crc_hasher.finalize();

        // Write header
        self.writer.write_all(&[type_byte, flags]).await?;
        self.writer.write_all(&len_bytes).await?;
        self.writer.write_all(payload).await?;
        self.writer.write_all(&crc.to_le_bytes()).await?;

        // Update stream SHA-256
        self.hasher.update([type_byte, flags]);
        self.hasher.update(len_bytes);
        self.hasher.update(payload);
        self.hasher.update(crc.to_le_bytes());

        let frame_size = 1 + 1 + 4 + payload.len() + 4;
        self.total_bytes_written += frame_size as u64;
        self.record_count += 1;

        Ok(())
    }

    pub async fn write_record(&mut self, record: &StreamRecord) -> Result<(), DumperError> {
        match record {
            StreamRecord::Header(h) => {
                let payload = serde_json::to_vec(h)?;
                self.write_raw_record(RecordType::Header, 0, &payload).await
            }
            StreamRecord::PreData(p) => {
                let payload = serde_json::to_vec(p)?;
                self.write_raw_record(RecordType::PreData, 0, &payload)
                    .await
            }
            StreamRecord::TableSchema(s) => {
                let payload = serde_json::to_vec(s)?;
                self.write_raw_record(RecordType::TableSchema, 0, &payload)
                    .await
            }
            StreamRecord::TableDataSlice(d) => {
                let payload = serde_json::to_vec(d)?;
                self.write_raw_record(RecordType::TableDataSlice, 0, &payload)
                    .await
            }
            StreamRecord::Sequence(s) => {
                let payload = serde_json::to_vec(s)?;
                self.write_raw_record(RecordType::Sequence, 0, &payload)
                    .await
            }
            StreamRecord::PostData(p) => {
                let payload = serde_json::to_vec(p)?;
                self.write_raw_record(RecordType::PostData, 0, &payload)
                    .await
            }
            StreamRecord::Routine(r) => {
                let payload = serde_json::to_vec(r)?;
                self.write_raw_record(RecordType::Routine, 0, &payload)
                    .await
            }
            StreamRecord::Trailer(t) => {
                let payload = serde_json::to_vec(t)?;
                self.write_raw_record(RecordType::Trailer, 0, &payload)
                    .await
            }
        }
    }

    pub async fn finish(mut self) -> Result<(u64, String), DumperError> {
        let hash_hex = hex::encode(self.hasher.clone().finalize());
        let trailer = StreamTrailer {
            total_records: self.record_count,
            total_logical_bytes: self.total_bytes_written,
            stream_hash_hex: hash_hex.clone(),
        };
        self.write_record(&StreamRecord::Trailer(trailer)).await?;
        self.writer.flush().await?;
        Ok((self.total_bytes_written, hash_hex))
    }

    pub fn records_written(&self) -> u64 {
        self.record_count
    }

    pub fn bytes_written(&self) -> u64 {
        self.total_bytes_written
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::stream::format::{StreamHeader, StreamRecord, RecordType, STREAM_MAGIC, MAX_PAYLOAD_SIZE};

    #[tokio::test]
    async fn test_encoder_initial_state() {
        let mut buffer = Vec::new();
        let encoder = StreamEncoder::new(&mut buffer);
        assert_eq!(encoder.records_written(), 0);
        assert_eq!(encoder.bytes_written(), 0);
    }

    #[tokio::test]
    async fn test_encoder_magic_and_raw_record() {
        let mut buffer = Vec::new();
        let mut encoder = StreamEncoder::new(&mut buffer);

        let payload = b"test_payload";
        encoder
            .write_raw_record(RecordType::Header, 0x01, payload)
            .await
            .unwrap();

        assert_eq!(encoder.records_written(), 1);
        let frame_size = 1 + 1 + 4 + payload.len() + 4;
        assert_eq!(
            encoder.bytes_written(),
            (STREAM_MAGIC.len() + frame_size) as u64
        );

        // Check magic
        assert_eq!(&buffer[0..4], STREAM_MAGIC);
        // Check type byte
        assert_eq!(buffer[4], RecordType::Header as u8);
        // Check flags
        assert_eq!(buffer[5], 0x01);
        // Check length
        let len_bytes = [buffer[6], buffer[7], buffer[8], buffer[9]];
        assert_eq!(u32::from_le_bytes(len_bytes) as usize, payload.len());
        // Check payload
        let payload_start = 10;
        let payload_end = payload_start + payload.len();
        assert_eq!(&buffer[payload_start..payload_end], payload);
        // We don't strictly check the exact CRC32 value here, just that it exists
        assert_eq!(buffer.len(), payload_end + 4);
    }

    #[tokio::test]
    async fn test_write_record() {
        let mut buffer = Vec::new();
        let mut encoder = StreamEncoder::new(&mut buffer);

        let header = StreamHeader {
            version: 1,
            engine: "mysql".into(),
            database: "test_db".into(),
            server_version: "8.0".into(),
            dumper_version: "1.0".into(),
            start_time: 123456789,
        };

        let record = StreamRecord::Header(header.clone());
        encoder.write_record(&record).await.unwrap();

        assert_eq!(encoder.records_written(), 1);
        assert!(encoder.bytes_written() > 0);

        // Verify the payload is JSON encoded StreamHeader
        let json_payload = serde_json::to_vec(&header).unwrap();

        // Find payload in buffer
        let payload_start = 10;
        let payload_end = payload_start + json_payload.len();
        assert_eq!(&buffer[payload_start..payload_end], json_payload.as_slice());
    }

    #[tokio::test]
    async fn test_encoder_finish() {
        let mut buffer = Vec::new();
        let expected_bytes;
        let expected_hash;
        {
            let mut encoder = StreamEncoder::new(&mut buffer);
            encoder
                .write_raw_record(RecordType::PreData, 0x00, b"predata")
                .await
                .unwrap();

            let res = encoder.finish().await.unwrap();
            expected_bytes = res.0;
            expected_hash = res.1;
        }

        assert!(expected_bytes > 0);
        assert!(!expected_hash.is_empty());
        assert_eq!(expected_hash.len(), 64); // SHA-256 is 64 hex chars
    }

    #[tokio::test]
    async fn test_encoder_payload_too_large() {
        let mut buffer = Vec::new();
        let mut encoder = StreamEncoder::new(&mut buffer);

        let payload = vec![0; MAX_PAYLOAD_SIZE + 1];
        let err = encoder
            .write_raw_record(RecordType::Header, 0x00, &payload)
            .await
            .unwrap_err();

        match err {
            DumperError::Format(msg) => {
                assert!(msg.contains("exceeds maximum allowed frame limit"));
            }
            _ => panic!("Expected Format error"),
        }
    }
}
