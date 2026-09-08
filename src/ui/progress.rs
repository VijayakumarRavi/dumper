use std::io::{self, Write};
use std::time::{Duration, Instant};
use serde::Serialize;

pub struct ProgressReporter {
    quiet: bool,
    json: bool,
    last_update: Instant,
    start_time: Instant,
    throttle_interval: Duration,
}

#[derive(Serialize)]
pub struct ProgressEvent<'a> {
    pub event: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub snapshot_id: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub database: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub engine: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub processed_bytes: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stored_bytes: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub deduplicated_bytes: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub duration_seconds: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message: Option<&'a str>,
}

impl ProgressReporter {
    pub fn new(quiet: bool, json: bool) -> Self {
        let now = Instant::now();
        Self {
            quiet,
            json,
            last_update: now,
            start_time: now,
            throttle_interval: Duration::from_millis(500), // Max 2 updates per second
        }
    }

    pub fn log_info(&self, message: &str) {
        if self.quiet {
            return;
        }
        if self.json {
            let event = ProgressEvent {
                event: "info",
                snapshot_id: None,
                database: None,
                engine: None,
                processed_bytes: None,
                stored_bytes: None,
                deduplicated_bytes: None,
                duration_seconds: None,
                message: Some(message),
            };
            if let Ok(json_str) = serde_json::to_string(&event) {
                println!("{}", json_str);
            }
        } else {
            println!("{}", message);
        }
    }

    pub fn report_progress(&mut self, processed_bytes: u64, stage: &str) {
        if self.quiet {
            return;
        }

        let now = Instant::now();
        if now.duration_since(self.last_update) < self.throttle_interval {
            return;
        }
        self.last_update = now;

        let elapsed = now.duration_since(self.start_time).as_secs_f64();
        let speed = if elapsed > 0.0 {
            (processed_bytes as f64) / elapsed
        } else {
            0.0
        };

        if self.json {
            let event = ProgressEvent {
                event: "progress",
                snapshot_id: None,
                database: None,
                engine: None,
                processed_bytes: Some(processed_bytes),
                stored_bytes: None,
                deduplicated_bytes: None,
                duration_seconds: Some(elapsed as u64),
                message: Some(stage),
            };
            if let Ok(json_str) = serde_json::to_string(&event) {
                println!("{}", json_str);
            }
        } else {
            print!(
                "\r[{}] Processed: {} ({}/s)   ",
                stage,
                format_bytes(processed_bytes),
                format_bytes(speed as u64)
            );
            let _ = io::stdout().flush();
        }
    }

    pub fn emit_event(&self, event: &ProgressEvent) {
        if self.quiet && event.event != "summary" && event.event != "error" {
            return;
        }
        if self.json {
            if let Ok(json_str) = serde_json::to_string(event) {
                println!("{}", json_str);
            }
        } else if let Some(msg) = event.message {
            println!("{}", msg);
        }
    }
}

pub fn format_bytes(bytes: u64) -> String {
    const KIB: u64 = 1024;
    const MIB: u64 = 1024 * KIB;
    const GIB: u64 = 1024 * MIB;
    const TIB: u64 = 1024 * GIB;

    if bytes >= TIB {
        format!("{:.2} TiB", bytes as f64 / TIB as f64)
    } else if bytes >= GIB {
        format!("{:.2} GiB", bytes as f64 / GIB as f64)
    } else if bytes >= MIB {
        format!("{:.2} MiB", bytes as f64 / MIB as f64)
    } else if bytes >= KIB {
        format!("{:.2} KiB", bytes as f64 / KIB as f64)
    } else {
        format!("{} B", bytes)
    }
}

pub fn format_duration(secs: u64) -> String {
    let hours = secs / 3600;
    let mins = (secs % 3600) / 60;
    let s = secs % 60;

    if hours > 0 {
        format!("{}h {}m {}s", hours, mins, s)
    } else if mins > 0 {
        format!("{}m {}s", mins, s)
    } else {
        format!("{}s", s)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_format_bytes() {
        assert_eq!(format_bytes(500), "500 B");
        assert_eq!(format_bytes(1024), "1.00 KiB");
        assert_eq!(format_bytes(10 * 1024 * 1024), "10.00 MiB");
        assert_eq!(format_bytes(5 * 1024 * 1024 * 1024), "5.00 GiB");
    }

    #[test]
    fn test_format_duration() {
        assert_eq!(format_duration(45), "45s");
        assert_eq!(format_duration(125), "2m 5s");
        assert_eq!(format_duration(3665), "1h 1m 5s");
    }
}
