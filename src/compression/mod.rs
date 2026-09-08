use crate::cli::CompressionLevel;
use crate::error::DumperError;
use std::io::Read;

pub const COMPRESSION_TAG_NONE: u8 = 0x00;
pub const COMPRESSION_TAG_ZSTD: u8 = 0x01;

pub fn compress_data(level: CompressionLevel, input: &[u8]) -> Result<(u8, Vec<u8>), DumperError> {
    match level {
        CompressionLevel::None => Ok((COMPRESSION_TAG_NONE, input.to_vec())),
        CompressionLevel::Fast => {
            let compressed = zstd::encode_all(input, 1)
                .map_err(|e| DumperError::Crypto(format!("Zstd compression (fast) failed: {}", e)))?;
            // If compression didn't save space, keep original
            if compressed.len() >= input.len() {
                Ok((COMPRESSION_TAG_NONE, input.to_vec()))
            } else {
                Ok((COMPRESSION_TAG_ZSTD, compressed))
            }
        }
        CompressionLevel::Default => {
            let compressed = zstd::encode_all(input, 3)
                .map_err(|e| DumperError::Crypto(format!("Zstd compression (default) failed: {}", e)))?;
            if compressed.len() >= input.len() {
                Ok((COMPRESSION_TAG_NONE, input.to_vec()))
            } else {
                Ok((COMPRESSION_TAG_ZSTD, compressed))
            }
        }
        CompressionLevel::Max => {
            let compressed = zstd::encode_all(input, 9)
                .map_err(|e| DumperError::Crypto(format!("Zstd compression (max) failed: {}", e)))?;
            if compressed.len() >= input.len() {
                Ok((COMPRESSION_TAG_NONE, input.to_vec()))
            } else {
                Ok((COMPRESSION_TAG_ZSTD, compressed))
            }
        }
    }
}

pub fn decompress_data(tag: u8, data: &[u8]) -> Result<Vec<u8>, DumperError> {
    match tag {
        COMPRESSION_TAG_NONE => Ok(data.to_vec()),
        COMPRESSION_TAG_ZSTD => {
            let mut decoder = zstd::Decoder::new(data)
                .map_err(|e| DumperError::Integrity(format!("Zstd decoder initialization failed: {}", e)))?;
            let mut decompressed = Vec::new();
            decoder
                .read_to_end(&mut decompressed)
                .map_err(|e| DumperError::Integrity(format!("Zstd decompression failed: {}", e)))?;
            Ok(decompressed)
        }
        other => Err(DumperError::Format(format!("Unsupported compression tag: 0x{:02x}", other))),
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
}
