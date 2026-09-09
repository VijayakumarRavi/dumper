use crate::cli::CompressionLevel;
use crate::error::DumperError;
use std::io::Read;

pub const COMPRESSION_TAG_NONE: u8 = 0x00;
pub const COMPRESSION_TAG_ZSTD: u8 = 0x01;

pub fn compress_data(level: CompressionLevel, input: &[u8]) -> Result<(u8, Vec<u8>), DumperError> {
    match level {
        CompressionLevel::None => Ok((COMPRESSION_TAG_NONE, input.to_vec())),
        CompressionLevel::Fast => {
            let compressed = zstd::encode_all(input, 1).map_err(|e| {
                DumperError::Crypto(format!("Zstd compression (fast) failed: {}", e))
            })?;
            // If compression didn't save space, keep original
            if compressed.len() >= input.len() {
                Ok((COMPRESSION_TAG_NONE, input.to_vec()))
            } else {
                Ok((COMPRESSION_TAG_ZSTD, compressed))
            }
        }
        CompressionLevel::Default => {
            let compressed = zstd::encode_all(input, 3).map_err(|e| {
                DumperError::Crypto(format!("Zstd compression (default) failed: {}", e))
            })?;
            if compressed.len() >= input.len() {
                Ok((COMPRESSION_TAG_NONE, input.to_vec()))
            } else {
                Ok((COMPRESSION_TAG_ZSTD, compressed))
            }
        }
        CompressionLevel::Max => {
            let compressed = zstd::encode_all(input, 9).map_err(|e| {
                DumperError::Crypto(format!("Zstd compression (max) failed: {}", e))
            })?;
            if compressed.len() >= input.len() {
                Ok((COMPRESSION_TAG_NONE, input.to_vec()))
            } else {
                Ok((COMPRESSION_TAG_ZSTD, compressed))
            }
        }
    }
}

pub const MAX_DECOMPRESSED_CHUNK_SIZE: usize = 32 * 1024 * 1024; // 32 MiB maximum safety limit

pub fn decompress_data(tag: u8, data: &[u8]) -> Result<Vec<u8>, DumperError> {
    decompress_data_bounded(tag, data, MAX_DECOMPRESSED_CHUNK_SIZE)
}

pub fn decompress_data_bounded(
    tag: u8,
    data: &[u8],
    max_size: usize,
) -> Result<Vec<u8>, DumperError> {
    match tag {
        COMPRESSION_TAG_NONE => {
            if data.len() > max_size {
                return Err(DumperError::Integrity(format!(
                    "Uncompressed chunk size {} exceeds safety limit of {} bytes",
                    data.len(),
                    max_size
                )));
            }
            Ok(data.to_vec())
        }
        COMPRESSION_TAG_ZSTD => {
            let decoder = zstd::Decoder::new(data).map_err(|e| {
                DumperError::Integrity(format!("Zstd decoder initialization failed: {}", e))
            })?;
            let mut decompressed = Vec::new();
            // Read at most max_size + 1 bytes to detect and abort oversized decompression bombs
            let mut bounded_reader = decoder.take((max_size + 1) as u64);
            bounded_reader
                .read_to_end(&mut decompressed)
                .map_err(|e| DumperError::Integrity(format!("Zstd decompression failed: {}", e)))?;

            if decompressed.len() > max_size {
                return Err(DumperError::Integrity(format!(
                    "Decompressed chunk size exceeds safety limit of {} bytes (possible decompression bomb)",
                    max_size
                )));
            }
            Ok(decompressed)
        }
        other => Err(DumperError::Format(format!(
            "Unsupported compression tag: 0x{:02x}",
            other
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_compression_roundtrip() {
        let sample = b"The quick brown fox jumps over the lazy dog repeatedly. ".repeat(50);

        for level in [
            CompressionLevel::None,
            CompressionLevel::Fast,
            CompressionLevel::Default,
            CompressionLevel::Max,
        ] {
            let (tag, compressed) = compress_data(level, &sample).unwrap();
            let decompressed = decompress_data(tag, &compressed).unwrap();
            assert_eq!(decompressed, sample);
        }
    }

    #[test]
    fn test_decompression_bomb_rejected() {
        // Create 2 MB of zeros, which compresses to only ~60 bytes with Zstd
        let large_zeros = vec![0u8; 2 * 1024 * 1024];
        let compressed = zstd::encode_all(&large_zeros[..], 3).unwrap();
        assert!(
            compressed.len() < 1000,
            "2 MB of zeros must compress to <1 KB"
        );

        // Attempting to decompress with an allowed maximum of 1 MB must fail
        let result = decompress_data_bounded(COMPRESSION_TAG_ZSTD, &compressed, 1024 * 1024);
        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("possible decompression bomb"),
            "Error must identify decompression bomb: {}",
            err_msg
        );

        // Decompressing with an allowed maximum of 2 MB must succeed
        let ok_result = decompress_data_bounded(COMPRESSION_TAG_ZSTD, &compressed, 2 * 1024 * 1024);
        assert!(ok_result.is_ok());
        assert_eq!(ok_result.unwrap().len(), 2 * 1024 * 1024);
    }
}
