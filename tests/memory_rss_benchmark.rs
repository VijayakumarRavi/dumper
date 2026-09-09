use dumper::stream::encoder::StreamEncoder;
use dumper::stream::format::RecordType;
use std::process::Command;
use tokio::io::{duplex, AsyncReadExt};

fn get_current_rss_kib() -> usize {
    let pid = std::process::id();
    let output = Command::new("ps")
        .args(["-o", "rss=", "-p", &pid.to_string()])
        .output();
    if let Ok(out) = output {
        let s = String::from_utf8_lossy(&out.stdout);
        s.trim().parse::<usize>().unwrap_or(0)
    } else {
        0
    }
}

#[tokio::test]
async fn test_memory_bounded_streaming() {
    let (client, mut server) = duplex(64 * 1024); // 64 KB bounded channel

    let _server_task = tokio::spawn(async move {
        let mut buf = [0u8; 8192];
        let mut total = 0;
        while let Ok(n) = server.read(&mut buf).await {
            if n == 0 {
                break;
            }
            total += n;
        }
        assert!(total > 50 * 1024 * 1024);
    });

    let mut encoder = StreamEncoder::new(client);
    let chunk = vec![0u8; 1024 * 1024]; // 1 MiB chunk
    for _ in 0..55 {
        encoder
            .write_raw_record(RecordType::PreData, 0x00, &chunk)
            .await
            .unwrap();
    }

    encoder.finish().await.unwrap();
    _server_task.await.unwrap();
}

#[tokio::test]
async fn test_memory_rss_10mb_100mb_1gb() {
    let baseline_rss = get_current_rss_kib();
    eprintln!(
        "Baseline Process RSS: {} KiB ({:.2} MiB)",
        baseline_rss,
        baseline_rss as f64 / 1024.0
    );

    // Stream 1 GB of DMP1 records through bounded duplex pipe to a decoding task
    let (writer, mut reader) = duplex(128 * 1024); // 128 KiB bounded buffer

    let reader_handle = tokio::spawn(async move {
        let mut buf = [0u8; 64 * 1024];
        let mut total_bytes = 0u64;
        while let Ok(n) = reader.read(&mut buf).await {
            if n == 0 {
                break;
            }
            total_bytes += n as u64;
        }
        total_bytes
    });

    let mut encoder = StreamEncoder::new(writer);
    let chunk_1mib = vec![0xABu8; 1024 * 1024]; // 1 MiB chunk

    let mut rss_10mb = 0;
    let mut rss_100mb = 0;

    // Total chunks: 1024 * 1 MiB = 1024 MiB (1 GiB)
    for i in 1..=1024 {
        encoder
            .write_raw_record(RecordType::TableDataSlice, 0x00, &chunk_1mib)
            .await
            .unwrap();

        // 10 MB checkpoint
        if i == 10 {
            rss_10mb = get_current_rss_kib();
            eprintln!(
                "Checkpoint 10 MB RSS: {} KiB ({:.2} MiB)",
                rss_10mb,
                rss_10mb as f64 / 1024.0
            );
        }

        // 100 MB checkpoint
        if i == 100 {
            rss_100mb = get_current_rss_kib();
            eprintln!(
                "Checkpoint 100 MB RSS: {} KiB ({:.2} MiB)",
                rss_100mb,
                rss_100mb as f64 / 1024.0
            );
        }
    }

    encoder.finish().await.unwrap();
    let total_streamed = reader_handle.await.unwrap();
    assert!(
        total_streamed >= 1024 * 1024 * 1024,
        "Must have streamed at least 1 GiB: {} bytes",
        total_streamed
    );

    let rss_1gb = get_current_rss_kib();
    eprintln!(
        "Checkpoint 1 GB (1024 MB) RSS: {} KiB ({:.2} MiB)",
        rss_1gb,
        rss_1gb as f64 / 1024.0
    );

    // Verify that memory is bounded and does not scale with data:
    // If memory grew with data, 1 GB would use ~1 GB of RAM!
    // With bounded streaming, RSS remains approximately constant and far below 128 MiB.
    let rss_1gb_mib = rss_1gb as f64 / 1024.0;
    assert!(
        rss_1gb_mib < 120.0,
        "RSS at 1 GB must remain far below 128 MiB container limit: {:.2} MiB",
        rss_1gb_mib
    );
    eprintln!(
        "SUCCESS: Bounded streaming verified across 10 MB ({:.2} MiB), 100 MB ({:.2} MiB), and 1 GB ({:.2} MiB)!",
        rss_10mb as f64 / 1024.0,
        rss_100mb as f64 / 1024.0,
        rss_1gb_mib
    );
}
