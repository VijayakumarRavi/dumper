use dumper::error::DumperError;
use dumper::stream::decoder::StreamDecoder;

#[tokio::test]
async fn test_fuzz_truncated_header() {
    let data = vec![0u8; 10]; // too short for any valid stream
    let mut decoder = StreamDecoder::new(data.as_slice());
    let res = decoder.read_next_record().await;
    assert!(matches!(
        res,
        Err(DumperError::Io(_)) | Err(DumperError::Integrity(_)) | Err(DumperError::Format(_))
    ));
}

#[tokio::test]
async fn test_fuzz_invalid_magic() {
    let mut data = vec![0u8; 100];
    // Put fake magic that doesn't match
    data[0..4].copy_from_slice(b"BAD!");
    let mut decoder = StreamDecoder::new(data.as_slice());
    let res = decoder.read_next_record().await;
    assert!(matches!(res, Err(DumperError::Format(_))));
}

#[tokio::test]
async fn test_fuzz_oversized_payload() {
    let mut data = Vec::new();
    // Magic
    data.extend_from_slice(b"DMP1");

    // Type and Flags
    data.push(0x01); // Header
    data.push(0x00);

    // Payload length: 10 MiB (exceeds 8 MiB limit)
    let len: u32 = 10 * 1024 * 1024;
    data.extend_from_slice(&len.to_le_bytes());

    // Some random payload
    data.extend_from_slice(&[0u8; 100]);

    let mut decoder = StreamDecoder::new(data.as_slice());
    let res = decoder.read_next_record().await;
    assert!(
        matches!(res, Err(DumperError::Integrity(ref s)) if s.contains("exceeds maximum allowed frame limit"))
    );
}

#[tokio::test]
async fn test_truncated_stream_missing_trailer_fails_with_integrity_error() {
    use dumper::stream::encoder::StreamEncoder;
    use dumper::stream::format::*;

    let mut buffer = Vec::new();
    {
        let mut encoder = StreamEncoder::new(&mut buffer);
        let header = StreamHeader {
            version: 1,
            engine: "postgresql".into(),
            database: "app".into(),
            server_version: "17".into(),
            dumper_version: "0.1.0".into(),
            start_time: 1700000000,
        };
        encoder
            .write_record(&StreamRecord::Header(header))
            .await
            .unwrap();
        // Intentionally DO NOT write trailer / finish encoder!
    }

    let mut decoder = StreamDecoder::new(&buffer[..]);
    let r1 = decoder.read_next_record().await.unwrap();
    assert!(r1.is_some());

    // Second read encounters EOF before Trailer record was seen
    let r2 = decoder.read_next_record().await;
    assert!(
        matches!(r2, Err(DumperError::Integrity(ref s)) if s.contains("before Trailer record was received")),
        "Stream ending before trailer MUST return Integrity error, got: {:?}",
        r2
    );
}

#[tokio::test]
async fn test_truncated_payload_fails_with_integrity_error() {
    use dumper::stream::encoder::StreamEncoder;
    use dumper::stream::format::*;

    let mut buffer = Vec::new();
    {
        let mut encoder = StreamEncoder::new(&mut buffer);
        let header = StreamHeader {
            version: 1,
            engine: "postgresql".into(),
            database: "app".into(),
            server_version: "17".into(),
            dumper_version: "0.1.0".into(),
            start_time: 1700000000,
        };
        encoder
            .write_record(&StreamRecord::Header(header))
            .await
            .unwrap();
        encoder.finish().await.unwrap();
    }

    // Truncate buffer in the middle of a record (e.g. drop last 10 bytes)
    let truncated_len = buffer.len() - 10;
    let truncated = &buffer[..truncated_len];

    let mut decoder = StreamDecoder::new(truncated);
    let mut found_error = false;
    while let Ok(Some(_)) = decoder.read_next_record().await {}
    // Next read should trigger Integrity error
    let res = decoder.read_next_record().await;
    if let Err(DumperError::Integrity(ref s)) = res {
        if s.contains("truncated") || s.contains("before Trailer") || s.contains("CRC32") {
            found_error = true;
        }
    }
    assert!(
        found_error,
        "Truncated payload or record must fail with Integrity error, got: {:?}",
        res
    );
}

#[tokio::test]
async fn test_decompression_bomb_in_repository_chunk_rejected() {
    use dumper::compression::{decompress_data, COMPRESSION_TAG_ZSTD, MAX_DECOMPRESSED_CHUNK_SIZE};

    // Create 33 MiB of zeros (exceeds MAX_DECOMPRESSED_CHUNK_SIZE of 32 MiB)
    // 33 MiB of zeros compresses to ~35 KiB in Zstd!
    let bomb_raw = vec![0u8; MAX_DECOMPRESSED_CHUNK_SIZE + 1024 * 1024];
    let compressed_bomb = zstd::encode_all(&bomb_raw[..], 1).unwrap();
    assert!(
        compressed_bomb.len() < 100 * 1024,
        "33 MiB of zeros must compress to under 100 KB in zstd"
    );

    // Decompressing must be rejected with an Integrity error
    let res = decompress_data(COMPRESSION_TAG_ZSTD, &compressed_bomb);
    assert!(res.is_err());
    match res {
        Err(DumperError::Integrity(ref s)) => {
            assert!(
                s.contains("possible decompression bomb"),
                "Error must identify decompression bomb: {}",
                s
            );
        }
        other => panic!(
            "Expected Integrity error for decompression bomb, got {:?}",
            other
        ),
    }
}
