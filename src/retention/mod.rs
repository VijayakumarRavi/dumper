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
    let mut groups: BTreeMap<(&str, &str), Vec<&'a SnapshotMetadata>> = BTreeMap::new();
    for s in candidates {
        groups.entry((&s.engine, &s.database)).or_default().push(s);
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

        // 2. Keep Daily N
        if let Some(n) = policy.keep_daily {
            let mut seen_days = BTreeSet::new();
            for s in &group_snapshots {
                let day_key = s.started_at.format("%Y-%m-%d").to_string();
                if seen_days.len() < n && seen_days.insert(day_key) {
                    kept_ids.insert(s.id.clone());
                }
            }
        }

        // 3. Keep Weekly N
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

        // 4. Keep Monthly N
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
    use chrono::{Duration, Utc};

    fn make_snapshot(
        id: &str,
        db: &str,
        engine: &str,
        tag: Option<&str>,
        age_days: i64,
    ) -> SnapshotMetadata {
        let time = Utc::now() - Duration::days(age_days);
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

    #[test]
    fn test_keep_last() {
        let s1 = make_snapshot("s1", "test", "postgresql", None, 0);
        let s2 = make_snapshot("s2", "test", "postgresql", None, 1);
        let s3 = make_snapshot("s3", "test", "postgresql", None, 2);
        let list = vec![s1, s2, s3];

        let policy = ForgetArgs {
            keep_last: Some(2),
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
