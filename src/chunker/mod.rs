use crate::error::DumperError;
use sha2::{Digest, Sha256};
use std::io::Write;

pub const DEFAULT_CHUNK_SIZE: usize = 2 * 1024 * 1024; // 2 MiB

/// Streaming bounded chunker that chunks data into fixed-size segments
/// and calls a handler for each chunk, ensuring bounded RAM usage.
pub struct StreamChunker<F>
where
    F: FnMut(&[u8], &str) -> Result<(), DumperError>,
{
    buffer: Vec<u8>,
    chunk_size: usize,
    handler: F,
    total_bytes: u64,
    chunks_count: u64,
}

impl<F> StreamChunker<F>
where
    F: FnMut(&[u8], &str) -> Result<(), DumperError>,
{
    pub fn new(chunk_size: usize, handler: F) -> Self {
        Self {
            buffer: Vec::with_capacity(chunk_size),
            chunk_size,
            handler,
            total_bytes: 0,
            chunks_count: 0,
        }
    }

    pub fn with_default_size(handler: F) -> Self {
        Self::new(DEFAULT_CHUNK_SIZE, handler)
    }

    fn flush_chunk(&mut self) -> Result<(), DumperError> {
        if self.buffer.is_empty() {
            return Ok(());
        }

        let hash_bytes = Sha256::digest(&self.buffer);
        let hash_hex = hex::encode(hash_bytes);

        (self.handler)(&self.buffer, &hash_hex)?;
        self.chunks_count += 1;
        self.buffer.clear();
        Ok(())
    }

    pub fn finish(mut self) -> Result<(u64, u64), DumperError> {
        self.flush_chunk()?;
        Ok((self.total_bytes, self.chunks_count))
    }
}

impl<F> Write for StreamChunker<F>
where
    F: FnMut(&[u8], &str) -> Result<(), DumperError>,
{
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let mut written = 0;
        while written < buf.len() {
            let space = self.chunk_size - self.buffer.len();
            let to_take = (buf.len() - written).min(space);

            self.buffer
                .extend_from_slice(&buf[written..written + to_take]);
            written += to_take;
            self.total_bytes += to_take as u64;

            if self.buffer.len() >= self.chunk_size {
                self.flush_chunk()
                    .map_err(|e| std::io::Error::other(e.to_string()))?;
            }
        }
        Ok(written)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        // Intentionally do not flush partial chunks on write flushes to maintain chunk boundary consistency,
        // finish() handles the final partial chunk.
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_chunker_exact_split() {
        let mut hashes = Vec::new();
        let chunk_size = 100;
        let mut chunker = StreamChunker::new(chunk_size, |data, hash| {
            assert!(data.len() <= 100);
            hashes.push((data.len(), hash.to_string()));
            Ok(())
        });

        let input = vec![42u8; 250];
        chunker.write_all(&input).unwrap();
        let (total_bytes, chunk_count) = chunker.finish().unwrap();

        assert_eq!(total_bytes, 250);
        assert_eq!(chunk_count, 3);
        assert_eq!(hashes.len(), 3);
        assert_eq!(hashes[0].0, 100);
        assert_eq!(hashes[1].0, 100);
        assert_eq!(hashes[2].0, 50);
    }
}
