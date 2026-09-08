use tokio::io::{AsyncWrite, AsyncWriteExt};
use crc32fast::Hasher as CrcHasher;
use sha2::{Digest, Sha256};
use crate::error::DumperError;
use crate::stream::format::*;

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

    pub async fn write_raw_record(&mut self, record_type: RecordType, flags: u8, payload: &[u8]) -> Result<(), DumperError> {
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
                self.write_raw_record(RecordType::PreData, 0, &payload).await
            }
            StreamRecord::TableSchema(s) => {
                let payload = serde_json::to_vec(s)?;
                self.write_raw_record(RecordType::TableSchema, 0, &payload).await
            }
            StreamRecord::TableDataSlice(d) => {
                let payload = serde_json::to_vec(d)?;
                self.write_raw_record(RecordType::TableDataSlice, 0, &payload).await
            }
            StreamRecord::Sequence(s) => {
                let payload = serde_json::to_vec(s)?;
                self.write_raw_record(RecordType::Sequence, 0, &payload).await
            }
            StreamRecord::PostData(p) => {
                let payload = serde_json::to_vec(p)?;
                self.write_raw_record(RecordType::PostData, 0, &payload).await
            }
            StreamRecord::Routine(r) => {
                let payload = serde_json::to_vec(r)?;
                self.write_raw_record(RecordType::Routine, 0, &payload).await
            }
            StreamRecord::Trailer(t) => {
                let payload = serde_json::to_vec(t)?;
                self.write_raw_record(RecordType::Trailer, 0, &payload).await
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
