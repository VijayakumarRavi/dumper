use crate::repository::snapshot::SnapshotMetadata;
use crate::ui::progress::format_bytes;
use chrono::{DateTime, Local, Utc};

/// Format a UTC DateTime in the system's local timezone if available,
/// falling back to UTC if the system timezone cannot be determined.
pub fn format_snapshot_date(utc_time: &DateTime<Utc>) -> String {
    utc_time
        .with_timezone(&Local)
        .format("%Y-%m-%d %H:%M:%S")
        .to_string()
}

/// Format the snapshots list as an aligned, human-readable table matching restic's style.
pub fn format_snapshots_table(snapshots: &[SnapshotMetadata]) -> String {
    let mut out = String::new();
    out.push_str(&format!(
        "{:<16}  {:<20}  {:<12}  {:<14}  {:<10}  {}\n",
        "ID", "Time", "Engine", "Database", "Logical", "Stored"
    ));
    out.push_str(&format!("{}\n", "-".repeat(90)));

    for s in snapshots {
        let date_str = format_snapshot_date(&s.started_at);
        out.push_str(&format!(
            "{:<16}  {:<20}  {:<12}  {:<14}  {:<10}  {}\n",
            s.id,
            date_str,
            s.engine,
            s.database,
            format_bytes(s.logical_bytes),
            format_bytes(s.stored_bytes)
        ));
    }

    out.push_str(&format!("{}\n", "-".repeat(90)));
    out.push_str("Timestamps shown in local time\n");
    let count = snapshots.len();
    if count == 1 {
        out.push_str("1 snapshot\n");
    } else {
        out.push_str(&format!("{} snapshots\n", count));
    }

    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    fn make_test_snapshot(id: &str, started_at: DateTime<Utc>) -> SnapshotMetadata {
        SnapshotMetadata {
            id: id.to_string(),
            full_id: "0123456789abcdef0123456789abcdef".into(),
            format_version: 1,
            dumper_version: "0.2.1".into(),
            engine: "postgresql".into(),
            database: "openbao".into(),
            server_version: "16.2".into(),
            started_at,
            completed_at: started_at,
            duration_seconds: 5,
            logical_bytes: 1024 * 1024 + 30 * 1024, // ~1.03 MiB
            stored_bytes: 341 * 1024 + 120,         // ~341.12 KiB
            deduplicated_bytes: 0,
            table_count: 10,
            compression: "default".into(),
            tag: None,
            blobs: Vec::new(),
        }
    }

    #[test]
    fn test_format_snapshot_date_produces_standard_length() {
        let utc_dt = Utc.with_ymd_and_hms(2026, 9, 29, 3, 31, 2).unwrap();
        let formatted = format_snapshot_date(&utc_dt);
        // Formatted date string should always be YYYY-MM-DD HH:MM:SS (19 chars)
        assert_eq!(formatted.len(), 19);
        assert!(chrono::NaiveDateTime::parse_from_str(&formatted, "%Y-%m-%d %H:%M:%S").is_ok());
    }

    #[test]
    fn test_snapshots_table_column_alignment() {
        let utc_dt = Utc.with_ymd_and_hms(2026, 9, 29, 3, 31, 2).unwrap();
        let s1 = make_test_snapshot("aeac215a407add9e", utc_dt);
        let mut s2 = make_test_snapshot("1caf59a82f9528e7", utc_dt);
        s2.engine = "mysql".into();
        s2.database = "vaultwarden".into();
        s2.logical_bytes = 3 * 1024 * 1024 + 800 * 1024;
        s2.stored_bytes = 898 * 1024;

        let table = format_snapshots_table(&[s1, s2]);
        let lines: Vec<&str> = table.lines().collect();
        assert_eq!(lines.len(), 7); // header, top separator, 2 rows, bottom separator, tz note, count

        let header = lines[0];
        let top_separator = lines[1];
        let row1 = lines[2];
        let row2 = lines[3];
        let bottom_separator = lines[4];
        let tz_note = lines[5];
        let count_line = lines[6];

        assert_eq!(header.len(), 88, "Header length mismatch: '{}'", header);
        assert_eq!(top_separator.len(), 90, "Top separator length mismatch");
        assert_eq!(row1.len(), 92, "Row 1 length mismatch: '{}'", row1);
        assert_eq!(row2.len(), 92, "Row 2 length mismatch: '{}'", row2);
        assert_eq!(
            bottom_separator.len(),
            90,
            "Bottom separator length mismatch"
        );
        assert_eq!(tz_note, "Timestamps shown in local time");
        assert_eq!(count_line, "2 snapshots");

        // Verify column start indices match exactly between header and rows:
        // ID: index 0
        assert!(header.starts_with("ID"));
        assert!(row1.starts_with("aeac215a407add9e"));
        assert!(row2.starts_with("1caf59a82f9528e7"));

        // Time: index 18
        assert_eq!(&header[18..22], "Time");
        // Row dates start at index 18
        assert_eq!(&row1[18..28], "2026-09-29");
        assert_eq!(&row2[18..28], "2026-09-29");

        // Engine: index 40
        assert_eq!(&header[40..46], "Engine");
        assert!(row1[40..].starts_with("postgresql"));
        assert!(row2[40..].starts_with("mysql"));

        // Database: index 54
        assert_eq!(&header[54..62], "Database");
        assert!(row1[54..].starts_with("openbao"));
        assert!(row2[54..].starts_with("vaultwarden"));

        // Logical: index 70
        assert_eq!(&header[70..77], "Logical");
        assert_eq!(&row1[70..78], "1.03 MiB");
        assert_eq!(&row2[70..78], "3.78 MiB");

        // Stored: index 82
        assert_eq!(&header[82..88], "Stored");
        assert!(row1[82..].starts_with("341.12 KiB"));
        assert!(row2[82..].starts_with("898.00 KiB"));
    }
}
