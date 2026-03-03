#!/bin/bash
# Cleanup Baseline MySQL setup - removes users and tables
# This allows you to reinitialize the setup cleanly
# 
# Optionally stops and removes the container if STOP_CONTAINER=true

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
EXPERIMENTS_DIR="$(dirname "$SCRIPT_DIR")"
FRAMEWORK_DIR="$EXPERIMENTS_DIR/framework"

# Configuration
MYSQL_HOST="${MYSQL_HOST:-127.0.0.1}"
MYSQL_PORT="${MYSQL_PORT:-13306}"
MYSQL_USER="${MYSQL_USER:-root}"
MYSQL_PASSWORD="${MYSQL_PASSWORD:-}"
STOP_CONTAINER="${STOP_CONTAINER:-false}"

# Try to find MySQL container by port mapping
MYSQL_CONTAINER=""
if command -v docker >/dev/null 2>&1; then
    # Find container with port mapping matching MYSQL_PORT:3306
    MYSQL_CONTAINER=$(docker ps --format "{{.Names}}" --filter "publish=${MYSQL_PORT}" 2>/dev/null | head -n1)
    if [ -z "$MYSQL_CONTAINER" ]; then
        # Try alternative: find container with mysql-baseline in name
        MYSQL_CONTAINER=$(docker ps -a --format "{{.Names}}" | grep -i "mysql.*baseline\|baseline.*mysql" | head -n1)
    fi
fi

# Colors
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

echo -e "${YELLOW}=== Cleaning up Baseline MySQL ===${NC}"
echo "MySQL: ${MYSQL_USER}@${MYSQL_HOST}:${MYSQL_PORT}"
if [ -n "$MYSQL_CONTAINER" ]; then
    echo "Container: ${MYSQL_CONTAINER}"
fi
echo ""

# Function to execute SQL
execute_sql() {
    local sql_file="$1"
    
    if [ -n "$MYSQL_CONTAINER" ]; then
        # Use docker exec to avoid host permission issues
        # Try without password first
        if docker exec -i "$MYSQL_CONTAINER" mysql -u"$MYSQL_USER" < "$sql_file" 2>/dev/null; then
            return 0
        fi
        # If that failed and password is provided, try with password
        if [ -n "$MYSQL_PASSWORD" ]; then
            docker exec -i -e MYSQL_PWD="$MYSQL_PASSWORD" "$MYSQL_CONTAINER" mysql -u"$MYSQL_USER" < "$sql_file" 2>/dev/null && return 0
        fi
    else
        # Direct connection
        if [ -n "$MYSQL_PASSWORD" ]; then
            mysql -h"$MYSQL_HOST" -P"$MYSQL_PORT" -u"$MYSQL_USER" -p"$MYSQL_PASSWORD" < "$sql_file" 2>/dev/null && return 0
        else
            mysql -h"$MYSQL_HOST" -P"$MYSQL_PORT" -u"$MYSQL_USER" < "$sql_file" 2>/dev/null && return 0
        fi
    fi
    
    return 1
}

# Cleanup SQL - remove users, tables, and database
CLEANUP_SQL=$(mktemp)
cat > "$CLEANUP_SQL" <<'EOF'
-- Remove demo data/users
DROP TABLE IF EXISTS abac_test.employees;
DROP TABLE IF EXISTS abac_test.projects;
DROP TABLE IF EXISTS abac_test.sensitive_data;

-- Remove demo data and schema
DROP DATABASE IF EXISTS abac_test;

-- Remove demo users (DROP USER automatically revokes all privileges)
DROP USER IF EXISTS 'user_alice'@'%';
DROP USER IF EXISTS 'user_bob'@'%';
DROP USER IF EXISTS 'user_charlie'@'%';
EOF

if execute_sql "$CLEANUP_SQL"; then
    echo -e "${GREEN}Baseline MySQL data cleanup complete!${NC}"
else
    echo -e "${YELLOW}Warning: Some cleanup operations may have failed (users/tables may not exist)${NC}"
fi
rm "$CLEANUP_SQL"

# Optionally stop and remove container
if [ "$STOP_CONTAINER" = "true" ] && [ -n "$MYSQL_CONTAINER" ]; then
    echo ""
    echo -e "${YELLOW}Stopping and removing container: ${MYSQL_CONTAINER}...${NC}"
    docker stop "$MYSQL_CONTAINER" 2>/dev/null || true
    docker rm "$MYSQL_CONTAINER" 2>/dev/null || true
    echo -e "${GREEN}Container stopped and removed${NC}"
fi

echo ""

