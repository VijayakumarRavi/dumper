# Dumper Operations & Deployment Guide

Dumper is designed for predictable, unattended execution in containerized and resource-constrained environments.

---

## 1. Kubernetes CronJob Deployment

Example production manifest deploying Dumper as a nightly Kubernetes CronJob with strict resource bounds (128 MiB RAM, 0.5 CPU limit):

```yaml
apiVersion: batch/v1
kind: CronJob
metadata:
  name: dumper-postgres-nightly
  namespace: database
spec:
  schedule: "0 2 * * *" # Daily at 02:00 UTC
  concurrencyPolicy: Forbid
  successfulJobsHistoryLimit: 3
  failedJobsHistoryLimit: 5
  jobTemplate:
    spec:
      backoffLimit: 2
      template:
        spec:
          restartPolicy: OnFailure
          securityContext:
            runAsNonRoot: true
            runAsUser: 65532
            runAsGroup: 65532
          containers:
            - name: dumper
              image: ghcr.io/yourorg/dumper:latest
              args:
                - "backup"
                - "$(DATABASE_URL)"
                - "--tag"
                - "nightly"
                - "--json"
              env:
                - name: DUMPER_REPOSITORY
                  value: "s3://prod-backups/postgres-main"
                - name: DUMPER_PASSWORD
                  valueFrom:
                    secretKeyRef:
                      name: dumper-secrets
                      key: repo-password
                - name: DATABASE_URL
                  valueFrom:
                    secretKeyRef:
                      name: db-credentials
                      key: url
                - name: DUMPER_S3_ENDPOINT
                  value: "https://s3.us-east-1.amazonaws.com"
                - name: DUMPER_S3_REGION
                  value: "us-east-1"
                - name: DUMPER_S3_ACCESS_KEY_ID
                  valueFrom:
                    secretKeyRef:
                      name: s3-credentials
                      key: access-key-id
                - name: DUMPER_S3_SECRET_ACCESS_KEY
                  valueFrom:
                    secretKeyRef:
                      name: s3-credentials
                      key: secret-access-key
              resources:
                requests:
                  memory: "64Mi"
                  cpu: "100m"
                limits:
                  memory: "128Mi"
                  cpu: "500m"
```

---

## 2. Retention & Pruning CronJob

Schedule retention policy evaluation and garbage collection once per week:

```bash
dumper forget \
  --keep-last 7 \
  --keep-hourly 24 \
  --keep-daily 14 \
  --keep-weekly 8 \
  --keep-monthly 12 \
  --prune \
  --json
```

---

## 3. Disaster Recovery Runbook

### Step 1: Verify Repository Accessibility

```bash
dumper check
```

### Step 2: Identify the Snapshot to Restore

```bash
dumper snapshots
```

### Step 3: Run Stream Integrity Dry-Run

```bash
dumper verify <snapshot-id> --restore-test
```

### Step 4: Stream Restore into Target Database

#### Option A: In-Place Transactional Restore (`--single-transaction`)
For PostgreSQL, use `--single-transaction` to execute the entire restore within an explicit `BEGIN ... COMMIT` block. If any error occurs during restoration, all changes are automatically rolled back, preventing half-restored states:

```bash
dumper restore <snapshot-id> \
  --target postgres://postgres:password@recovery-host:5432/production \
  --drop-existing \
  --single-transaction
```

#### Option B: Zero-Downtime Atomic Staging Database Rename Pattern (Recommended)
Restoring directly over an active production database risks exposing intermediate states or causing downtime while tables are dropped and re-populated. The recommended pattern is to restore into a temporary staging database, validate it, and atomically swap it with the production database:

**1. Create the staging database:**
```bash
createdb -h recovery-host -U postgres production_staging
```

**2. Restore snapshot into staging:**
```bash
dumper restore <snapshot-id> \
  --target postgres://postgres:password@recovery-host:5432/production_staging \
  --single-transaction
```

**3. Validate staging data integrity:**
Run application smoke tests or automated verification queries against `production_staging`.

**4. Perform atomic database swap (PostgreSQL):**
Terminate active user connections to `production` and rename the databases:
```sql
-- Terminate active client connections
SELECT pg_terminate_backend(pid)
FROM pg_stat_activity
WHERE datname = 'production' AND pid <> pg_backend_pid();

-- Atomic swap via database rename
ALTER DATABASE production RENAME TO production_old;
ALTER DATABASE production_staging RENAME TO production;

-- Drop old database after confirming application stability
DROP DATABASE production_old;
```

**MySQL / MariaDB Table Swap Pattern:**
For MySQL / MariaDB (which do not support `ALTER DATABASE RENAME`), restore into a staging schema and execute an atomic multi-table rename:
```sql
RENAME TABLE
  production.users  TO production_old.users,
  staging.users     TO production.users,
  production.orders TO production_old.orders,
  staging.orders    TO production.orders;
```


---

## 4. Stale Lock Troubleshooting

If a backup worker process crashed unexpectedly or was hard-killed (`SIGKILL` / out-of-memory killer outside Dumper):

1. Check active locks:
   `dumper check`
2. Remove expired locks:
   `dumper unlock`
3. Force unlock all locks if confirmed inactive:
   `dumper unlock --force`
