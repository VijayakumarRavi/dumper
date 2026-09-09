#!/usr/bin/env bash
set -euo pipefail

# End-to-end MySQL / MariaDB backup & restore validation
# Tests data integrity, schema structures, foreign keys, unique constraints,
# decimals, auto-increment, and exact row equality before and after restore.

DB_URL="${1:-mysql://root:testpassword@127.0.0.1:3306/e2e_mysql}"
REPO="${2:-/tmp/dumper-mysql-e2e-repo}"
CLIENT_CMD="${3:-mysql}"
DUMPER_BIN="${4:-./target/release/dumper}"

echo "=== [E2E MySQL/MariaDB] Validating against $DB_URL ==="

# Parse URL
PROTO="${DB_URL%%://*}://"
URL_NO_PROTO="${DB_URL#$PROTO}"
USER_PASS="${URL_NO_PROTO%@*}"
HOST_PORT_DB="${URL_NO_PROTO#*@}"
DB_USER="${USER_PASS%:*}"
DB_PASS="${USER_PASS#*:}"
HOST_PORT="${HOST_PORT_DB%/*}"
DB_NAME="${HOST_PORT_DB#*/}"
DB_HOST="${HOST_PORT%:*}"
DB_PORT="${HOST_PORT#*:}"

if [ -n "$DB_PASS" ]; then
    PASS_ARG="-p$DB_PASS"
else
    PASS_ARG=""
fi

run_sql() {
    if [ -n "$PASS_ARG" ]; then
        "$CLIENT_CMD" -h "$DB_HOST" -P "$DB_PORT" -u "$DB_USER" "$PASS_ARG" "$DB_NAME" "$@"
    else
        "$CLIENT_CMD" -h "$DB_HOST" -P "$DB_PORT" -u "$DB_USER" "$DB_NAME" "$@"
    fi
}

WORKDIR=$(mktemp -d)
trap 'rm -rf "$WORKDIR"' EXIT

export DUMPER_PASSWORD="${DUMPER_PASSWORD:-e2e-test-password}"

# 1. Setup rich schema
echo "--> Creating tables and inserting test records..."
run_sql << 'EOSQL'
DROP TABLE IF EXISTS product_reviews;
DROP TABLE IF EXISTS products;
DROP TABLE IF EXISTS categories;

CREATE TABLE categories (
    category_id INT AUTO_INCREMENT PRIMARY KEY,
    code VARCHAR(20) NOT NULL UNIQUE,
    name VARCHAR(100) NOT NULL,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
) ENGINE=InnoDB;

CREATE TABLE products (
    product_id INT AUTO_INCREMENT PRIMARY KEY,
    category_id INT NOT NULL,
    sku VARCHAR(30) NOT NULL UNIQUE,
    title VARCHAR(150) NOT NULL,
    price DECIMAL(10,2) NOT NULL,
    stock INT NOT NULL DEFAULT 0,
    is_active TINYINT(1) DEFAULT 1,
    description TEXT,
    FOREIGN KEY (category_id) REFERENCES categories(category_id) ON DELETE CASCADE
) ENGINE=InnoDB;

CREATE TABLE product_reviews (
    review_id INT AUTO_INCREMENT PRIMARY KEY,
    product_id INT NOT NULL,
    reviewer_name VARCHAR(50) NOT NULL,
    rating TINYINT NOT NULL,
    comment TEXT,
    FOREIGN KEY (product_id) REFERENCES products(product_id) ON DELETE CASCADE
) ENGINE=InnoDB;

INSERT INTO categories (code, name) VALUES
    ('ELEC', 'Electronics'),
    ('HOME', 'Home & Kitchen'),
    ('BOOK', 'Books');

INSERT INTO products (category_id, sku, title, price, stock, is_active, description) VALUES
    (1, 'ELEC-001', 'Wireless Headphones', 79.99, 50, 1, 'Noise cancelling over-ear headphones'),
    (1, 'ELEC-002', 'Mechanical Keyboard', 129.50, 25, 1, 'RGB tactile switches with braided cable'),
    (2, 'HOME-001', 'Pour-Over Coffee Maker', 24.95, 100, 1, 'Glass carafe with reusable filter'),
    (3, 'BOOK-001', 'Designing Data-Intensive Applications', 45.00, 15, 1, 'The big idea behind reliable systems');

INSERT INTO product_reviews (product_id, reviewer_name, rating, comment) VALUES
    (1, 'Alice', 5, 'Super clear audio, very comfortable!'),
    (1, 'Bob', 4, 'Great battery life, slightly heavy.'),
    (2, 'Carol', 5, 'Typing feels amazing on this keyboard.'),
    (4, 'David', 5, 'Essential reading for distributed systems engineers.');
EOSQL

# 2. Export BEFORE snapshots
echo "--> Exporting BEFORE state..."
run_sql -N -s -e \
    "SELECT category_id, code, name FROM categories ORDER BY category_id;" > "$WORKDIR/categories_before.tsv"

run_sql -N -s -e \
    "SELECT product_id, category_id, sku, title, price, stock, is_active, description FROM products ORDER BY product_id;" > "$WORKDIR/products_before.tsv"

run_sql -N -s -e \
    "SELECT review_id, product_id, reviewer_name, rating, comment FROM product_reviews ORDER BY review_id;" > "$WORKDIR/reviews_before.tsv"

run_sql -N -s -e \
    "SELECT table_name, constraint_name, constraint_type FROM information_schema.table_constraints WHERE table_schema='$DB_NAME' ORDER BY table_name, constraint_name;" > "$WORKDIR/constraints_before.tsv"

run_sql -N -s -e \
    "SELECT table_name, column_name, data_type, is_nullable FROM information_schema.columns WHERE table_schema='$DB_NAME' ORDER BY table_name, ordinal_position;" > "$WORKDIR/columns_before.tsv"

# 3. Perform backup
echo "--> Performing Dumper backup..."
if [[ "$REPO" == s3://* ]]; then
    "$DUMPER_BIN" backup "$DB_URL" --tag "e2e-mysql"
    SNAP_ID=$("$DUMPER_BIN" --json snapshots | awk -F'"' '/"id":/{print $4; exit}')
    "$DUMPER_BIN" verify "$SNAP_ID" --restore-test
else
    "$DUMPER_BIN" -r "$REPO" init
    "$DUMPER_BIN" -r "$REPO" backup "$DB_URL" --tag "e2e-mysql"
    SNAP_ID=$("$DUMPER_BIN" -r "$REPO" --json snapshots | awk -F'"' '/"id":/{print $4; exit}')
    "$DUMPER_BIN" -r "$REPO" verify "$SNAP_ID" --restore-test
fi

# 4. Drop all tables
echo "--> Dropping tables..."
run_sql -e "DROP TABLE product_reviews; DROP TABLE products; DROP TABLE categories;"

# 5. Restore backup
echo "--> Restoring snapshot $SNAP_ID..."
if [[ "$REPO" == s3://* ]]; then
    "$DUMPER_BIN" restore "$SNAP_ID" --target "$DB_URL"
else
    "$DUMPER_BIN" -r "$REPO" restore "$SNAP_ID" --target "$DB_URL"
fi

# 6. Export AFTER snapshots
echo "--> Exporting AFTER state and verifying..."
run_sql -N -s -e \
    "SELECT category_id, code, name FROM categories ORDER BY category_id;" > "$WORKDIR/categories_after.tsv"

run_sql -N -s -e \
    "SELECT product_id, category_id, sku, title, price, stock, is_active, description FROM products ORDER BY product_id;" > "$WORKDIR/products_after.tsv"

run_sql -N -s -e \
    "SELECT review_id, product_id, reviewer_name, rating, comment FROM product_reviews ORDER BY review_id;" > "$WORKDIR/reviews_after.tsv"

run_sql -N -s -e \
    "SELECT table_name, constraint_name, constraint_type FROM information_schema.table_constraints WHERE table_schema='$DB_NAME' ORDER BY table_name, constraint_name;" > "$WORKDIR/constraints_after.tsv"

run_sql -N -s -e \
    "SELECT table_name, column_name, data_type, is_nullable FROM information_schema.columns WHERE table_schema='$DB_NAME' ORDER BY table_name, ordinal_position;" > "$WORKDIR/columns_after.tsv"

# 7. Compare exact equality
echo "--> Comparing before and after data..."
diff -u "$WORKDIR/categories_before.tsv" "$WORKDIR/categories_after.tsv"
diff -u "$WORKDIR/products_before.tsv" "$WORKDIR/products_after.tsv"
diff -u "$WORKDIR/reviews_before.tsv" "$WORKDIR/reviews_after.tsv"
diff -u "$WORKDIR/constraints_before.tsv" "$WORKDIR/constraints_after.tsv"
diff -u "$WORKDIR/columns_before.tsv" "$WORKDIR/columns_after.tsv"

# 8. Test auto-increment progression after restore
echo "--> Testing auto-increment progression after restore..."
run_sql -e "INSERT INTO products (category_id, sku, title, price, stock, is_active, description) VALUES (1, 'ELEC-003', 'USB-C Hub', 39.99, 40, 1, '7-in-1 multi-port adapter');"
NEW_ID=$(run_sql -N -s -e "SELECT product_id FROM products WHERE sku='ELEC-003';")
if [ "$NEW_ID" -ne 5 ]; then
    echo "ERROR: Auto-increment produced ID $NEW_ID, expected 5!"
    exit 1
fi

echo "=== [PASS] MySQL/MariaDB E2E Data, Schema & Auto-Increment Integrity 100% Verified ==="
