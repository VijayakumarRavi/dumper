use crate::cli::ForgetArgs;
use crate::repository::snapshot::SnapshotMetadata;
use chrono::Datelike;
use std::collections::{BTreeMap, BTreeSet, HashSet};

pub struct RetentionPlan<'a> {
    pub keep: Vec<&'a SnapshotMetadata>,
    pub remove: Vec<&'a SnapshotMetadata>,
}

pub fn evaluate_retention<'a>(
    snapshots: &'a [SnapshotMetadata],
    policy: &ForgetArgs,
) -> RetentionPlan<'a> {
    if snapshots.is_empty() {
        return RetentionPlan {
            keep: Vec::new(),
            remove: Vec::new(),
        };
    }

    // If no retention flag was set, keep everything by default
    if policy.keep_last.is_none()
        && policy.keep_hourly.is_none()
        && policy.keep_daily.is_none()
        && policy.keep_weekly.is_none()
        && policy.keep_monthly.is_none()
    {
        return RetentionPlan {
            keep: snapshots.iter().collect(),
            remove: Vec::new(),
        };
    }

    // Filter snapshots based on database and tag if provided.
    // Snapshots that do not match the filter are NOT subject to deletion; they are kept.
    let mut candidates: Vec<&'a SnapshotMetadata> = Vec::new();
    let mut untouched: Vec<&'a SnapshotMetadata> = Vec::new();

    for s in snapshots {
        let db_matches = match policy.database {
            Some(ref db) => &s.database == db,
            None => true,
        };
        let tag_matches = match policy.tag {
            Some(ref t) => s.tag.as_ref() == Some(t),
            None => true,
        };

        if db_matches && tag_matches {
            candidates.push(s);
        } else {
            untouched.push(s);
        }
    }

    // Group candidates by (engine, database) so multi-database repositories do not cross-contaminate
    let mut groups: BTreeMap<(&str, &str, Option<&str>), Vec<&'a SnapshotMetadata>> =
        BTreeMap::new();
    for s in candidates {
        groups
            .entry((&s.engine, &s.database, s.tag.as_deref()))
            .or_default()
            .push(s);
    }

    let mut kept_ids: HashSet<String> = HashSet::new();

    for (_group_key, mut group_snapshots) in groups {
        // Sort group snapshots by started_at descending (most recent first)
        group_snapshots.sort_by_key(|b| std::cmp::Reverse(b.started_at));

        // 1. Keep Last N
        if let Some(n) = policy.keep_last {
            for s in group_snapshots.iter().take(n) {
                kept_ids.insert(s.id.clone());
            }
        }

        // 2. Keep Hourly N
        if let Some(n) = policy.keep_hourly {
            let mut seen_hours = BTreeSet::new();
            for s in &group_snapshots {
                let hour_key = s.started_at.format("%Y-%m-%d %H").to_string();
                if seen_hours.len() < n && seen_hours.insert(hour_key) {
                    kept_ids.insert(s.id.clone());
                }
            }
        }

        // 3. Keep Daily N
        if let Some(n) = policy.keep_daily {
            let mut seen_days = BTreeSet::new();
            for s in &group_snapshots {
                let day_key = s.started_at.format("%Y-%m-%d").to_string();
                if seen_days.len() < n && seen_days.insert(day_key) {
                    kept_ids.insert(s.id.clone());
                }
            }
        }

        // 4. Keep Weekly N
        if let Some(n) = policy.keep_weekly {
            let mut seen_weeks = BTreeSet::new();
            for s in &group_snapshots {
                let week_key = format!(
                    "{}-W{:02}",
                    s.started_at.year(),
                    s.started_at.iso_week().week()
                );
                if seen_weeks.len() < n && seen_weeks.insert(week_key) {
                    kept_ids.insert(s.id.clone());
                }
            }
        }

        // 5. Keep Monthly N
        if let Some(n) = policy.keep_monthly {
            let mut seen_months = BTreeSet::new();
            for s in &group_snapshots {
                let month_key = s.started_at.format("%Y-%m").to_string();
                if seen_months.len() < n && seen_months.insert(month_key) {
                    kept_ids.insert(s.id.clone());
                }
            }
        }
    }

    let mut keep = Vec::new();
    let mut remove = Vec::new();

    for s in snapshots {
        if kept_ids.contains(&s.id) || untouched.iter().any(|u| u.id == s.id) {
            keep.push(s);
        } else {
            remove.push(s);
        }
    }

    RetentionPlan { keep, remove }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{DateTime, Duration, TimeZone, Utc};

    fn make_snapshot_at(
        id: &str,
        db: &str,
        engine: &str,
        tag: Option<&str>,
        time: DateTime<Utc>,
    ) -> SnapshotMetadata {
        SnapshotMetadata {
            id: id.into(),
            full_id: format!("full_{}", id),
            format_version: 1,
            dumper_version: "0.1.0".into(),
            engine: engine.into(),
            database: db.into(),
            server_version: "17".into(),
            started_at: time,
            completed_at: time,
            duration_seconds: 1,
            logical_bytes: 100,
            stored_bytes: 100,
            deduplicated_bytes: 0,
            table_count: 1,
            compression: "default".into(),
            tag: tag.map(|t| t.into()),
            blobs: Vec::new(),
        }
    }

    fn make_snapshot(
        id: &str,
        db: &str,
        engine: &str,
        tag: Option<&str>,
        age_days: i64,
    ) -> SnapshotMetadata {
        make_snapshot_at(id, db, engine, tag, Utc::now() - Duration::days(age_days))
    }

    #[test]
    fn test_keep_last() {
        let s1 = make_snapshot("s1", "test", "postgresql", None, 0);
        let s2 = make_snapshot("s2", "test", "postgresql", None, 1);
        let s3 = make_snapshot("s3", "test", "postgresql", None, 2);
        let list = vec![s1, s2, s3];

        let policy = ForgetArgs {
            keep_last: Some(2),
            keep_hourly: None,
            keep_daily: None,
            keep_weekly: None,
            keep_monthly: None,
            database: None,
            tag: None,
            prune: false,
            dry_run: false,
        };

        let plan = evaluate_retention(&list, &policy);
        assert_eq!(plan.keep.len(), 2);
        assert_eq!(plan.remove.len(), 1);
        assert_eq!(plan.keep[0].id, "s1");
        assert_eq!(plan.keep[1].id, "s2");
        assert_eq!(plan.remove[0].id, "s3");
    }

    #[test]
    fn test_keep_hourly_basic() {
        let t1 = Utc.with_ymd_and_hms(2026, 9, 28, 10, 0, 0).unwrap();
        let t2 = Utc.with_ymd_and_hms(2026, 9, 28, 11, 0, 0).unwrap();
        let t3 = Utc.with_ymd_and_hms(2026, 9, 28, 12, 0, 0).unwrap();
        let t4 = Utc.with_ymd_and_hms(2026, 9, 28, 13, 0, 0).unwrap();

        let s1 = make_snapshot_at("s1", "test", "postgresql", None, t1);
        let s2 = make_snapshot_at("s2", "test", "postgresql", None, t2);
        let s3 = make_snapshot_at("s3", "test", "postgresql", None, t3);
        let s4 = make_snapshot_at("s4", "test", "postgresql", None, t4);
        let list = vec![s4, s3, s2, s1];

        // Keep last 2 hours
        let policy = ForgetArgs {
            keep_last: None,
            keep_hourly: Some(2),
            keep_daily: None,
            keep_weekly: None,
            keep_monthly: None,
            database: None,
            tag: None,
            prune: false,
            dry_run: false,
        };

        let plan = evaluate_retention(&list, &policy);
        assert_eq!(plan.keep.len(), 2);
        assert_eq!(plan.remove.len(), 2);
        assert_eq!(plan.keep[0].id, "s4"); // 13:00
        assert_eq!(plan.keep[1].id, "s3"); // 12:00
        assert_eq!(plan.remove[0].id, "s2"); // 11:00
        assert_eq!(plan.remove[1].id, "s1"); // 10:00
    }

    #[test]
    fn test_keep_hourly_multiple_in_same_hour() {
        // Hour 14 has two snapshots: 14:10 and 14:40
        let t_14_early = Utc.with_ymd_and_hms(2026, 9, 28, 14, 10, 0).unwrap();
        let t_14_late = Utc.with_ymd_and_hms(2026, 9, 28, 14, 40, 0).unwrap();

        // Hour 13 has two snapshots: 13:15 and 13:50
        let t_13_early = Utc.with_ymd_and_hms(2026, 9, 28, 13, 15, 0).unwrap();
        let t_13_late = Utc.with_ymd_and_hms(2026, 9, 28, 13, 50, 0).unwrap();

        // Hour 12 has one snapshot: 12:00
        let t_12 = Utc.with_ymd_and_hms(2026, 9, 28, 12, 0, 0).unwrap();

        let s_14_early = make_snapshot_at("s_14_early", "test", "postgresql", None, t_14_early);
        let s_14_late = make_snapshot_at("s_14_late", "test", "postgresql", None, t_14_late);
        let s_13_early = make_snapshot_at("s_13_early", "test", "postgresql", None, t_13_early);
        let s_13_late = make_snapshot_at("s_13_late", "test", "postgresql", None, t_13_late);
        let s_12 = make_snapshot_at("s_12", "test", "postgresql", None, t_12);

        let list = vec![s_14_late, s_14_early, s_13_late, s_13_early, s_12];

        // Keep 2 hourly snapshots: must select the newest snapshot in each hour (14:40 and 13:50)
        let policy = ForgetArgs {
            keep_last: None,
            keep_hourly: Some(2),
            keep_daily: None,
            keep_weekly: None,
            keep_monthly: None,
            database: None,
            tag: None,
            prune: false,
            dry_run: false,
        };

        let plan = evaluate_retention(&list, &policy);
        assert_eq!(plan.keep.len(), 2);
        assert_eq!(plan.remove.len(), 3);

        let kept_ids: Vec<&str> = plan.keep.iter().map(|s| s.id.as_str()).collect();
        assert!(
            kept_ids.contains(&"s_14_late"),
            "Must keep latest snapshot from hour 14"
        );
        assert!(
            kept_ids.contains(&"s_13_late"),
            "Must keep latest snapshot from hour 13"
        );

        let removed_ids: Vec<&str> = plan.remove.iter().map(|s| s.id.as_str()).collect();
        assert!(
            removed_ids.contains(&"s_14_early"),
            "Must remove earlier snapshot in hour 14"
        );
        assert!(
            removed_ids.contains(&"s_13_early"),
            "Must remove earlier snapshot in hour 13"
        );
        assert!(
            removed_ids.contains(&"s_12"),
            "Must remove snapshot from older hour 12"
        );
    }

    #[test]
    fn test_keep_hourly_combined_with_keep_last() {
        let t_14_early = Utc.with_ymd_and_hms(2026, 9, 28, 14, 10, 0).unwrap();
        let t_14_late = Utc.with_ymd_and_hms(2026, 9, 28, 14, 40, 0).unwrap();
        let t_13 = Utc.with_ymd_and_hms(2026, 9, 28, 13, 0, 0).unwrap();

        let s1 = make_snapshot_at("s1", "test", "postgresql", None, t_14_late);
        let s2 = make_snapshot_at("s2", "test", "postgresql", None, t_14_early);
        let s3 = make_snapshot_at("s3", "test", "postgresql", None, t_13);

        let list = vec![s1, s2, s3];

        // keep-last 2 keeps s1 and s2. keep-hourly 1 keeps s1. Combined should keep both s1 and s2.
        let policy = ForgetArgs {
            keep_last: Some(2),
            keep_hourly: Some(1),
            keep_daily: None,
            keep_weekly: None,
            keep_monthly: None,
            database: None,
            tag: None,
            prune: false,
            dry_run: false,
        };

        let plan = evaluate_retention(&list, &policy);
        assert_eq!(plan.keep.len(), 2);
        assert_eq!(plan.remove.len(), 1);
        assert_eq!(plan.remove[0].id, "s3");
    }

    #[test]
    fn test_keep_hourly_combined_with_keep_daily() {
        // Day 1
        let d1_morning = Utc.with_ymd_and_hms(2026, 9, 27, 10, 0, 0).unwrap();
        let d1_evening = Utc.with_ymd_and_hms(2026, 9, 27, 18, 0, 0).unwrap();

        // Day 2
        let d2_h8 = Utc.with_ymd_and_hms(2026, 9, 28, 8, 0, 0).unwrap();
        let d2_h9 = Utc.with_ymd_and_hms(2026, 9, 28, 9, 0, 0).unwrap();
        let d2_h10 = Utc.with_ymd_and_hms(2026, 9, 28, 10, 0, 0).unwrap();

        let s_d1_m = make_snapshot_at("d1_m", "test", "postgresql", None, d1_morning);
        let s_d1_e = make_snapshot_at("d1_e", "test", "postgresql", None, d1_evening);
        let s_d2_8 = make_snapshot_at("d2_8", "test", "postgresql", None, d2_h8);
        let s_d2_9 = make_snapshot_at("d2_9", "test", "postgresql", None, d2_h9);
        let s_d2_10 = make_snapshot_at("d2_10", "test", "postgresql", None, d2_h10);

        let list = vec![s_d2_10, s_d2_9, s_d2_8, s_d1_e, s_d1_m];

        // Keep 2 hourly (d2_10, d2_9) + keep 2 daily (d2_10, d1_e)
        // Kept union: d2_10, d2_9, d1_e
        // Removed: d2_8, d1_m
        let policy = ForgetArgs {
            keep_last: None,
            keep_hourly: Some(2),
            keep_daily: Some(2),
            keep_weekly: None,
            keep_monthly: None,
            database: None,
            tag: None,
            prune: false,
            dry_run: false,
        };

        let plan = evaluate_retention(&list, &policy);
        assert_eq!(plan.keep.len(), 3);
        assert_eq!(plan.remove.len(), 2);

        let kept_ids: Vec<&str> = plan.keep.iter().map(|s| s.id.as_str()).collect();
        assert!(kept_ids.contains(&"d2_10"));
        assert!(kept_ids.contains(&"d2_9"));
        assert!(kept_ids.contains(&"d1_e"));

        let removed_ids: Vec<&str> = plan.remove.iter().map(|s| s.id.as_str()).collect();
        assert!(removed_ids.contains(&"d2_8"));
        assert!(removed_ids.contains(&"d1_m"));
    }

    #[test]
    fn test_retention_grouping_multi_database() {
        // Multi-database repository: db_users, db_billing, db_logs
        let u1 = make_snapshot("u1", "db_users", "postgresql", None, 0);
        let u2 = make_snapshot("u2", "db_users", "postgresql", None, 1);
        let u3 = make_snapshot("u3", "db_users", "postgresql", None, 2);

        let b1 = make_snapshot("b1", "db_billing", "postgresql", None, 0);
        let b2 = make_snapshot("b2", "db_billing", "postgresql", None, 1);
        let b3 = make_snapshot("b3", "db_billing", "postgresql", None, 2);

        let l1 = make_snapshot("l1", "db_logs", "mysql", None, 0);
        let l2 = make_snapshot("l2", "db_logs", "mysql", None, 1);
        let l3 = make_snapshot("l3", "db_logs", "mysql", None, 2);

        let list = vec![u1, u2, u3, b1, b2, b3, l1, l2, l3];

        // Keep last 2 snapshots PER database
        let policy = ForgetArgs {
            keep_last: Some(2),
            keep_hourly: None,
            keep_daily: None,
            keep_weekly: None,
            keep_monthly: None,
            database: None,
            tag: None,
            prune: false,
            dry_run: false,
        };

        let plan = evaluate_retention(&list, &policy);
        // Each database must keep 2 and remove 1 -> total 6 kept, 3 removed
        assert_eq!(plan.keep.len(), 6, "Must keep 2 per database (total 6)");
        assert_eq!(
            plan.remove.len(),
            3,
            "Must remove 1 oldest per database (total 3)"
        );

        let removed_ids: Vec<&str> = plan.remove.iter().map(|s| s.id.as_str()).collect();
        assert!(removed_ids.contains(&"u3"), "Oldest user snapshot removed");
        assert!(
            removed_ids.contains(&"b3"),
            "Oldest billing snapshot removed"
        );
        assert!(removed_ids.contains(&"l3"), "Oldest logs snapshot removed");
    }

    #[test]
    fn test_retention_filter_by_database() {
        let u1 = make_snapshot("u1", "db_users", "postgresql", None, 0);
        let u2 = make_snapshot("u2", "db_users", "postgresql", None, 1);
        let b1 = make_snapshot("b1", "db_billing", "postgresql", None, 0);
        let b2 = make_snapshot("b2", "db_billing", "postgresql", None, 1);

        let list = vec![u1, u2, b1, b2];

        // Only apply policy to db_users
        let policy = ForgetArgs {
            keep_last: Some(1),
            keep_hourly: None,
            keep_daily: None,
            keep_weekly: None,
            keep_monthly: None,
            database: Some("db_users".into()),
            tag: None,
            prune: false,
            dry_run: false,
        };

        let plan = evaluate_retention(&list, &policy);
        // db_users keeps u1, removes u2
        // db_billing is untouched (keeps b1 and b2)
        assert_eq!(plan.keep.len(), 3);
        assert_eq!(plan.remove.len(), 1);
        assert_eq!(plan.remove[0].id, "u2");
    }

    #[test]
    fn test_retention_filter_by_tag() {
        let p1 = make_snapshot("p1", "db", "postgresql", Some("prod"), 0);
        let p2 = make_snapshot("p2", "db", "postgresql", Some("prod"), 1);
        let s1 = make_snapshot("s1", "db", "postgresql", Some("staging"), 0);

        let list = vec![p1, p2, s1];

        let policy = ForgetArgs {
            keep_last: Some(1),
            keep_hourly: None,
            keep_daily: None,
            keep_weekly: None,
            keep_monthly: None,
            database: None,
            tag: Some("prod".into()),
            prune: false,
            dry_run: false,
        };

        let plan = evaluate_retention(&list, &policy);
        assert_eq!(plan.keep.len(), 2);
        assert_eq!(plan.remove.len(), 1);
        assert_eq!(plan.remove[0].id, "p2");
    }
}
