#!/bin/bash
# Setup baseline MySQL with GRANT-based authorization

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
EXPERIMENTS_DIR="$(dirname "$SCRIPT_DIR")"
FRAMEWORK_DIR="$EXPERIMENTS_DIR/framework"

# Configuration
MYSQL_HOST="${MYSQL_HOST:-127.0.0.1}"
MYSQL_PORT="${MYSQL_PORT:-13306}"
MYSQL_USER="${MYSQL_USER:-root}"
MYSQL_PASSWORD="${MYSQL_PASSWORD:-}"

AUTH_SPEC="${AUTH_SPEC:-$FRAMEWORK_DIR/auth_spec_example.json}"

# Colors
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

echo -e "${GREEN}=== Setting up Baseline MySQL ===${NC}"
echo "MySQL: ${MYSQL_USER}@${MYSQL_HOST}:${MYSQL_PORT}"
echo "Auth Spec: ${AUTH_SPEC}"
echo ""

# Check if auth spec exists
if [ ! -f "$AUTH_SPEC" ]; then
    echo "Error: Auth spec not found: $AUTH_SPEC"
    exit 1
fi

# Generate SQL statements (CREATE USER + GRANT) using framework
echo -e "${YELLOW}Generating SQL statements from auth spec...${NC}"
AUTH_SQL=$(python3 "$FRAMEWORK_DIR/translate_to_grants.py" "$AUTH_SPEC" --mode setup)

if [ $? -ne 0 ]; then
    echo "Error: Failed to generate SQL statements from auth spec"
    exit 1
fi

# Create setup SQL script
SETUP_SQL=$(mktemp)
cat > "$SETUP_SQL" <<'EOF'
-- Create database and tables
CREATE DATABASE IF NOT EXISTS abac_test;

CREATE TABLE IF NOT EXISTS abac_test.employees (
    id INT PRIMARY KEY,
    name VARCHAR(100),
    department VARCHAR(50)
);

CREATE TABLE IF NOT EXISTS abac_test.projects (
    id INT PRIMARY KEY,
    name VARCHAR(100),
    classification VARCHAR(50)
);

CREATE TABLE IF NOT EXISTS abac_test.sensitive_data (
    id INT PRIMARY KEY,
    info TEXT
);

-- Insert test data
INSERT INTO abac_test.employees (id, name, department) VALUES
    (1, 'Alice', 'HR'),
    (2, 'Bob', 'IT'),
    (3, 'Charlie', 'Finance')
ON DUPLICATE KEY UPDATE name=VALUES(name), department=VALUES(department);

INSERT INTO abac_test.projects (id, name, classification) VALUES
    (1, 'Project 1', 'Public'),
    (2, 'Project 2', 'Internal'),
    (3, 'Project 3', 'Confidential')
ON DUPLICATE KEY UPDATE name=VALUES(name), classification=VALUES(classification);

INSERT INTO abac_test.sensitive_data (id, info) VALUES
    (1, 'Sensitive data 1'),
    (2, 'Sensitive data 2'),
    (3, 'Sensitive data 3')
ON DUPLICATE KEY UPDATE info=VALUES(info);

EOF

# Append authorization SQL (CREATE USER + GRANT statements)
echo "$AUTH_SQL" >> "$SETUP_SQL"

# Execute setup
echo -e "${YELLOW}Executing setup SQL...${NC}"
if [ -n "$MYSQL_PASSWORD" ]; then
    mysql -h"$MYSQL_HOST" -P"$MYSQL_PORT" -u"$MYSQL_USER" -p"$MYSQL_PASSWORD" < "$SETUP_SQL"
else
    mysql -h"$MYSQL_HOST" -P"$MYSQL_PORT" -u"$MYSQL_USER" < "$SETUP_SQL"
fi

rm "$SETUP_SQL"

echo -e "${GREEN}Baseline MySQL setup complete!${NC}"

