use std::collections::{BTreeSet, HashSet};
use chrono::Datelike;
use crate::cli::ForgetArgs;
use crate::repository::snapshot::SnapshotMetadata;

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

    let mut kept_ids: HashSet<String> = HashSet::new();

    // 1. Keep Last N
    if let Some(n) = policy.keep_last {
        for s in snapshots.iter().take(n) {
            kept_ids.insert(s.id.clone());
        }
    }

    // 2. Keep Daily N
    if let Some(n) = policy.keep_daily {
        let mut seen_days = BTreeSet::new();
        for s in snapshots {
            let day_key = s.started_at.format("%Y-%m-%d").to_string();
            if seen_days.len() < n || seen_days.contains(&day_key) {
                if !seen_days.contains(&day_key) {
                    seen_days.insert(day_key);
                    kept_ids.insert(s.id.clone());
                }
            }
        }
    }

    // 3. Keep Weekly N
    if let Some(n) = policy.keep_weekly {
        let mut seen_weeks = BTreeSet::new();
        for s in snapshots {
            let week_key = format!("{}-W{:02}", s.started_at.year(), s.started_at.iso_week().week());
            if seen_weeks.len() < n || seen_weeks.contains(&week_key) {
                if !seen_weeks.contains(&week_key) {
                    seen_weeks.insert(week_key);
                    kept_ids.insert(s.id.clone());
                }
            }
        }
    }

    // 4. Keep Monthly N
    if let Some(n) = policy.keep_monthly {
        let mut seen_months = BTreeSet::new();
        for s in snapshots {
            let month_key = s.started_at.format("%Y-%m").to_string();
            if seen_months.len() < n || seen_months.contains(&month_key) {
                if !seen_months.contains(&month_key) {
                    seen_months.insert(month_key);
                    kept_ids.insert(s.id.clone());
                }
            }
        }
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

    let mut keep = Vec::new();
    let mut remove = Vec::new();

    for s in snapshots {
        if kept_ids.contains(&s.id) {
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

    fn make_snapshot(id: &str, age_days: i64) -> SnapshotMetadata {
        let time = Utc::now() - Duration::days(age_days);
        SnapshotMetadata {
            id: id.into(),
            full_id: format!("full_{}", id),
            format_version: 1,
            dumper_version: "0.1.0".into(),
            engine: "postgresql".into(),
            database: "test".into(),
            server_version: "17".into(),
            started_at: time,
            completed_at: time,
            duration_seconds: 1,
            logical_bytes: 100,
            stored_bytes: 100,
            deduplicated_bytes: 0,
            table_count: 1,
            compression: "default".into(),
            tag: None,
            blobs: Vec::new(),
        }
    }

    #[test]
    fn test_keep_last() {
        let s1 = make_snapshot("s1", 0);
        let s2 = make_snapshot("s2", 1);
        let s3 = make_snapshot("s3", 2);
        let list = vec![s1, s2, s3];

        let policy = ForgetArgs {
            keep_last: Some(2),
            keep_daily: None,
            keep_weekly: None,
            keep_monthly: None,
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
}
