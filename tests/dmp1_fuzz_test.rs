use dumper::error::DumperError;
use dumper::stream::decoder::StreamDecoder;

#[tokio::test]
async fn test_fuzz_truncated_header() {
    let data = vec![0u8; 10]; // too short for any valid stream
    let mut decoder = StreamDecoder::new(data.as_slice());
    let res = decoder.read_next_record().await;
    assert!(matches!(res, Err(DumperError::Io(_)) | Err(DumperError::Integrity(_)) | Err(DumperError::Format(_))));
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
    data.extend_from_slice(&vec![0u8; 100]);
    
    let mut decoder = StreamDecoder::new(data.as_slice());
    let res = decoder.read_next_record().await;
    assert!(matches!(res, Err(DumperError::Integrity(ref s)) if s.contains("exceeds maximum allowed frame limit")));
}
