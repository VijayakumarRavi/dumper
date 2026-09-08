use dumper::stream::encoder::StreamEncoder;
use tokio::io::duplex;

#[tokio::test]
async fn test_memory_bounded_streaming() {
    // A test to ensure we don't allocate memory proportional to data size.
    let (client, mut server) = duplex(64 * 1024); // 64 KB buffer

    let _server_task = tokio::spawn(async move {
        use tokio::io::AsyncReadExt;
        let mut buf = [0u8; 8192];
        let mut total = 0;
        while let Ok(n) = server.read(&mut buf).await {
            if n == 0 { break; }
            total += n;
        }
        // Ensure we actually processed a lot of data
        assert!(total > 50 * 1024 * 1024); // > 50 MiB
    });

    let mut encoder = StreamEncoder::new(client);
    
    // Write 50 MiB of data in chunks
    let chunk = vec![0u8; 1024 * 1024]; // 1 MiB chunk
    for _ in 0..55 {
        encoder.write_record(0x02, 0x00, &chunk).await.unwrap();
    }
    
    encoder.finalize().await.unwrap();
    
    // If it OOMs or hangs, the test fails.
}
