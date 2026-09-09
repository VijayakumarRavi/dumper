#!/usr/bin/env bash
set -euo pipefail

# End-to-end PostgreSQL backup & restore validation
# Tests data integrity, schema structures, foreign keys, unique constraints,
# arrays, numeric types, and sequence restoration.

DB_URL="${1:-postgres://postgres:testpassword@127.0.0.1:5432/e2e_pg}"
REPO="${2:-/tmp/dumper-pg-e2e-repo}"
DUMPER_BIN="${3:-./target/release/dumper}"

echo "=== [E2E Postgres] Validating against $DB_URL ==="

WORKDIR=$(mktemp -d)
trap 'rm -rf "$WORKDIR"' EXIT

export DUMPER_PASSWORD="${DUMPER_PASSWORD:-e2e-test-password}"

# 1. Setup rich schema
psql "$DB_URL" << 'EOSQL'
DROP TABLE IF EXISTS employees CASCADE;
DROP TABLE IF EXISTS departments CASCADE;
DROP TABLE IF EXISTS audit_log CASCADE;

CREATE TABLE departments (
    dept_id serial PRIMARY KEY,
    name varchar(50) UNIQUE NOT NULL,
    budget numeric(12,2) NOT NULL,
    created_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE employees (
    emp_id serial PRIMARY KEY,
    dept_id int NOT NULL REFERENCES departments(dept_id) ON DELETE CASCADE,
    full_name text NOT NULL,
    email varchar(100) UNIQUE NOT NULL,
    salary numeric(10,2) NOT NULL,
    tags text[] DEFAULT ARRAY['staff'],
    is_active boolean DEFAULT true,
    hire_date date NOT NULL
);

CREATE TABLE audit_log (
    log_id serial PRIMARY KEY,
    event text NOT NULL,
    metadata jsonb
);

INSERT INTO departments (name, budget) VALUES
    ('Engineering', 500000.00),
    ('Marketing', 150000.50),
    ('Finance', 250000.75);

INSERT INTO employees (dept_id, full_name, email, salary, tags, is_active, hire_date) VALUES
    (1, 'Alice Smith', 'alice@example.com', 95000.50, ARRAY['lead', 'backend'], true, '2022-01-15'),
    (1, 'Bob Jones', 'bob@example.com', 82000.00, ARRAY['frontend', 'ui'], true, '2022-06-01'),
    (2, 'Carol White', 'carol@example.com', 72000.25, ARRAY['marketing', 'seo'], true, '2023-03-10'),
    (3, 'David Brown', 'david@example.com', 88000.00, ARRAY['finance', 'tax'], false, '2021-11-20');

INSERT INTO audit_log (event, metadata) VALUES
    ('init_schema', '{"version": 1, "status": "ok"}'),
    ('seed_employees', '{"count": 4}');
EOSQL

# 2. Export BEFORE snapshots of data and schema metadata
echo "--> Exporting BEFORE state..."
psql "$DB_URL" -t -A -F $'\t' -c \
    "SELECT dept_id, name, budget FROM departments ORDER BY dept_id;" > "$WORKDIR/departments_before.tsv"

psql "$DB_URL" -t -A -F $'\t' -c \
    "SELECT emp_id, dept_id, full_name, email, salary, tags, is_active, hire_date FROM employees ORDER BY emp_id;" > "$WORKDIR/employees_before.tsv"

psql "$DB_URL" -t -A -F $'\t' -c \
    "SELECT log_id, event, metadata::text FROM audit_log ORDER BY log_id;" > "$WORKDIR/audit_before.tsv"

psql "$DB_URL" -t -A -F $'\t' -c \
    "SELECT table_name, constraint_name, constraint_type FROM information_schema.table_constraints WHERE table_schema='public' ORDER BY table_name, constraint_name;" > "$WORKDIR/constraints_before.tsv"

psql "$DB_URL" -t -A -F $'\t' -c \
    "SELECT sequencename, last_value FROM pg_sequences WHERE schemaname='public' ORDER BY sequencename;" > "$WORKDIR/sequences_before.tsv"

# 3. Perform backup
echo "--> Performing Dumper backup..."
if [[ "$REPO" == s3://* ]]; then
    "$DUMPER_BIN" backup "$DB_URL" --tag "e2e-pg"
    SNAP_ID=$("$DUMPER_BIN" --json snapshots | awk -F'"' '/"id":/{print $4; exit}')
    "$DUMPER_BIN" verify "$SNAP_ID" --restore-test
else
    "$DUMPER_BIN" -r "$REPO" init
    "$DUMPER_BIN" -r "$REPO" backup "$DB_URL" --tag "e2e-pg"
    SNAP_ID=$("$DUMPER_BIN" -r "$REPO" --json snapshots | awk -F'"' '/"id":/{print $4; exit}')
    "$DUMPER_BIN" -r "$REPO" verify "$SNAP_ID" --restore-test
fi

# 4. Drop all tables
echo "--> Dropping public schema tables..."
psql "$DB_URL" -c "DROP TABLE employees CASCADE; DROP TABLE departments CASCADE; DROP TABLE audit_log CASCADE;"

# 5. Restore backup
echo "--> Restoring snapshot $SNAP_ID..."
if [[ "$REPO" == s3://* ]]; then
    "$DUMPER_BIN" restore "$SNAP_ID" --target "$DB_URL"
else
    "$DUMPER_BIN" -r "$REPO" restore "$SNAP_ID" --target "$DB_URL"
fi

# 6. Export AFTER snapshots of data and schema metadata
echo "--> Exporting AFTER state and verifying..."
psql "$DB_URL" -t -A -F $'\t' -c \
    "SELECT dept_id, name, budget FROM departments ORDER BY dept_id;" > "$WORKDIR/departments_after.tsv"

psql "$DB_URL" -t -A -F $'\t' -c \
    "SELECT emp_id, dept_id, full_name, email, salary, tags, is_active, hire_date FROM employees ORDER BY emp_id;" > "$WORKDIR/employees_after.tsv"

psql "$DB_URL" -t -A -F $'\t' -c \
    "SELECT log_id, event, metadata::text FROM audit_log ORDER BY log_id;" > "$WORKDIR/audit_after.tsv"

psql "$DB_URL" -t -A -F $'\t' -c \
    "SELECT table_name, constraint_name, constraint_type FROM information_schema.table_constraints WHERE table_schema='public' ORDER BY table_name, constraint_name;" > "$WORKDIR/constraints_after.tsv"

psql "$DB_URL" -t -A -F $'\t' -c \
    "SELECT sequencename, last_value FROM pg_sequences WHERE schemaname='public' ORDER BY sequencename;" > "$WORKDIR/sequences_after.tsv"

# 7. Compare exact equality
echo "--> Comparing before and after data..."
diff -u "$WORKDIR/departments_before.tsv" "$WORKDIR/departments_after.tsv"
diff -u "$WORKDIR/employees_before.tsv" "$WORKDIR/employees_after.tsv"
diff -u "$WORKDIR/audit_before.tsv" "$WORKDIR/audit_after.tsv"
diff -u "$WORKDIR/constraints_before.tsv" "$WORKDIR/constraints_after.tsv"
diff -u "$WORKDIR/sequences_before.tsv" "$WORKDIR/sequences_after.tsv"

# 8. Test inserting a new employee to verify restored sequences don't collide
echo "--> Testing sequence progression after restore..."
psql "$DB_URL" -c "INSERT INTO employees (dept_id, full_name, email, salary, hire_date) VALUES (1, 'New Guy', 'newguy@example.com', 60000.00, '2026-09-09');"
NEW_ID=$(psql "$DB_URL" -t -A -c "SELECT emp_id FROM employees WHERE email='newguy@example.com';")
if [ "$NEW_ID" -ne 5 ]; then
    echo "ERROR: Restored sequence produced ID $NEW_ID, expected 5!"
    exit 1
fi

echo "=== [PASS] PostgreSQL E2E Data, Schema & Sequence Integrity 100% Verified ==="
