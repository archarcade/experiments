#!/bin/bash
# Setup Cedar MySQL with Cedar policies and attributes

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
EXPERIMENTS_DIR="$(dirname "$SCRIPT_DIR")"
FRAMEWORK_DIR="$EXPERIMENTS_DIR/framework"
BRIEF_DIR="$EXPERIMENTS_DIR/context/brief"

# Configuration
MYSQL_HOST="${MYSQL_HOST:-127.0.0.1}"
MYSQL_PORT="${MYSQL_PORT:-13307}"
MYSQL_USER="${MYSQL_USER:-root}"
MYSQL_PASSWORD="${MYSQL_PASSWORD:-}"

CEDAR_AGENT_URL="${CEDAR_AGENT_URL:-http://localhost:8280}"
AUTH_SPEC="${AUTH_SPEC:-$FRAMEWORK_DIR/auth_spec_example.json}"

# Try to find MySQL container by port mapping
MYSQL_CONTAINER=""
if command -v docker >/dev/null 2>&1; then
    # Find container with port mapping matching MYSQL_PORT:3306
    MYSQL_CONTAINER=$(docker ps --format "{{.Names}}" --filter "publish=${MYSQL_PORT}" 2>/dev/null | head -n1)
    if [ -z "$MYSQL_CONTAINER" ]; then
        # Try alternative: find container with mysql-cedar in name
        MYSQL_CONTAINER=$(docker ps --format "{{.Names}}" | grep -i "mysql.*cedar\|cedar.*mysql" | head -n1)
    fi
fi

# Detect if we're using docker-compose (containers on same network)
# If mysql-cedar and cedar-agent are on the same network, use container name instead of localhost
USE_CONTAINER_NAME=false
if [ -n "$MYSQL_CONTAINER" ]; then
    # Check if cedar-agent container exists and is on the same network
    if docker ps --format '{{.Names}}' | grep -q "^cedar-agent$"; then
        MYSQL_NETWORKS=$(docker inspect "$MYSQL_CONTAINER" --format '{{range $key, $value := .NetworkSettings.Networks}}{{$key}} {{end}}' 2>/dev/null || echo "")
        CEDAR_NETWORKS=$(docker inspect cedar-agent --format '{{range $key, $value := .NetworkSettings.Networks}}{{$key}} {{end}}' 2>/dev/null || echo "")
        
        # Check if they share a network (not just "bridge")
        for mysql_net in $MYSQL_NETWORKS; do
            for cedar_net in $CEDAR_NETWORKS; do
                if [ "$mysql_net" = "$cedar_net" ] && [ "$mysql_net" != "bridge" ]; then
                    USE_CONTAINER_NAME=true
                    break 2
                fi
            done
        done
        
        # Also check if they're both on the mysql-experiments network (from docker-compose)
        if echo "$MYSQL_NETWORKS" | grep -q "mysql-experiments" && echo "$CEDAR_NETWORKS" | grep -q "mysql-experiments"; then
            USE_CONTAINER_NAME=true
        fi
    fi
fi

# Store original URL for host access (Python script runs on host)
CEDAR_AGENT_URL_HOST="${CEDAR_AGENT_URL}"

# If using container name, update CEDAR_AGENT_URL to use container name for MySQL plugin config
# Note: This URL is for the MySQL container to reach Cedar agent, so use container internal port (8180)
if [ "$USE_CONTAINER_NAME" = true ]; then
    CEDAR_AGENT_URL_FOR_MYSQL="http://cedar-agent:8180"
    echo -e "${YELLOW}Detected docker-compose network, MySQL will use: ${CEDAR_AGENT_URL_FOR_MYSQL}${NC}"
else
    CEDAR_AGENT_URL_FOR_MYSQL="${CEDAR_AGENT_URL}"
fi

# Colors
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

echo -e "${GREEN}=== Setting up Cedar MySQL ===${NC}"
echo "MySQL: ${MYSQL_USER}@${MYSQL_HOST}:${MYSQL_PORT}"
if [ "$USE_CONTAINER_NAME" = true ]; then
    echo "Cedar Agent (host): ${CEDAR_AGENT_URL_HOST}"
    echo "Cedar Agent (MySQL): ${CEDAR_AGENT_URL_FOR_MYSQL}"
else
    echo "Cedar Agent: ${CEDAR_AGENT_URL_HOST}"
fi
echo "Auth Spec: ${AUTH_SPEC}"
echo ""

# Check if auth spec exists
if [ ! -f "$AUTH_SPEC" ]; then
    echo -e "${RED}Error: Auth spec not found: $AUTH_SPEC${NC}"
    exit 1
fi

# Check if Cedar agent is accessible
echo -e "${YELLOW}Checking Cedar agent...${NC}"
if [ "$USE_CONTAINER_NAME" = true ] && [ -n "$MYSQL_CONTAINER" ]; then
    # When using docker-compose, verify Cedar agent container exists and is running
    if ! docker ps --format '{{.Names}}' | grep -q "^cedar-agent$"; then
        echo -e "${RED}Error: Cedar agent container 'cedar-agent' not found or not running${NC}"
        echo ""
        echo "If using docker-compose, ensure all services are running:"
        echo "  docker-compose up -d"
        echo ""
        echo "Check status with:"
        echo "  docker-compose ps"
        echo ""
        exit 1
    fi
    
    # Check Cedar agent health status (if available)
    CEDAR_HEALTH=$(docker inspect cedar-agent --format '{{.State.Health.Status}}' 2>/dev/null || echo "")
    if [ "$CEDAR_HEALTH" = "healthy" ]; then
        echo -e "${GREEN}Cedar agent container is healthy${NC}"
    elif [ "$CEDAR_HEALTH" = "starting" ]; then
        echo -e "${YELLOW}Cedar agent container is starting (health check in progress)${NC}"
    elif [ -n "$CEDAR_HEALTH" ]; then
        echo -e "${YELLOW}Warning: Cedar agent container health status: ${CEDAR_HEALTH}${NC}"
    else
        echo -e "${GREEN}Cedar agent container is running${NC}"
    fi
    
    # Check from host (for Python script) - this is required for the setup script
    if ! curl -s -f "${CEDAR_AGENT_URL_HOST}/v1/" > /dev/null 2>&1; then
        echo -e "${YELLOW}Warning: Cedar agent not accessible from host at ${CEDAR_AGENT_URL_HOST}${NC}"
        echo "  The Python setup script needs host access. Checking if port mapping is correct..."
        echo "  If using docker-compose, verify port 8280 is mapped: docker-compose ps"
    else
        echo -e "${GREEN}Cedar agent accessible from host${NC}"
    fi
else
    # Check from host (for host-to-container communication)
    if ! curl -s -f "${CEDAR_AGENT_URL_HOST}/v1/" > /dev/null 2>&1; then
        echo -e "${RED}Error: Cedar agent not accessible at ${CEDAR_AGENT_URL_HOST}${NC}"
        echo ""
        echo "The Cedar agent must be started with the MySQL schema files before setup."
        echo "Please run:"
        echo "  ./start_cedar_agent.sh"
        echo ""
        echo "Or start it manually with:"
        echo "  docker run -d --name cedar-agent-experiments -p 8280:8180 \\"
        echo "    -v \"\$(pwd)/experiments/context/mysql_schemas:/app/mysql_schemas:ro\" \\"
        echo "    ghcr.io/archarcade/cedar-agent:latest \\"
        echo "    -l info -s /app/mysql_schemas/schema.json \\"
        echo "    -d /app/mysql_schemas/data.json \\"
        echo "    --policies /app/mysql_schemas/policies.json \\"
        echo "    --addr 0.0.0.0"
        echo ""
        exit 1
    else
        echo -e "${GREEN}Cedar agent accessible${NC}"
    fi
fi

# Function to execute SQL - tries docker exec first, falls back to direct connection
execute_sql() {
    local sql_file="$1"
    local show_errors="${2:-false}"  # Second parameter: show errors (default: false)
    local error_output
    
    # Try docker exec first (avoids host permission issues)
    if [ -n "$MYSQL_CONTAINER" ]; then
        # Try without password first (many containers don't have root password initially)
        if [ "$show_errors" = "true" ]; then
            if docker exec -i "$MYSQL_CONTAINER" mysql -u"$MYSQL_USER" < "$sql_file" 2>&1; then
                return 0
            fi
        else
            error_output=$(docker exec -i "$MYSQL_CONTAINER" mysql -u"$MYSQL_USER" < "$sql_file" 2>&1)
            if [ $? -eq 0 ]; then
                return 0
            fi
            # Check if error is just from SELECT output (not a real error)
            if echo "$error_output" | grep -q "ERROR"; then
                echo "$error_output" >&2
                return 1
            fi
            # If no ERROR found, assume success (SELECT statements produce output)
            return 0
        fi
        
        # If that failed and password is provided, try with password
        if [ -n "$MYSQL_PASSWORD" ]; then
            # Use MYSQL_PWD environment variable to avoid password in command line
            if [ "$show_errors" = "true" ]; then
                if docker exec -i -e MYSQL_PWD="$MYSQL_PASSWORD" "$MYSQL_CONTAINER" mysql -u"$MYSQL_USER" < "$sql_file" 2>&1; then
                    return 0
                fi
            else
                error_output=$(docker exec -i -e MYSQL_PWD="$MYSQL_PASSWORD" "$MYSQL_CONTAINER" mysql -u"$MYSQL_USER" < "$sql_file" 2>&1)
                if [ $? -eq 0 ]; then
                    return 0
                fi
                # Check if error is just from SELECT output (not a real error)
                if echo "$error_output" | grep -q "ERROR"; then
                    echo "$error_output" >&2
                    return 1
                fi
                # If no ERROR found, assume success
                return 0
            fi
        fi
    fi
    
    # Fallback to direct connection (after root@'%' is created, this should work)
    # Try without password first
    if [ "$show_errors" = "true" ]; then
        if mysql -h"$MYSQL_HOST" -P"$MYSQL_PORT" -u"$MYSQL_USER" < "$sql_file" 2>&1; then
            return 0
        fi
    else
        error_output=$(mysql -h"$MYSQL_HOST" -P"$MYSQL_PORT" -u"$MYSQL_USER" < "$sql_file" 2>&1)
        if [ $? -eq 0 ]; then
            return 0
        fi
        # Check if error is just from SELECT output (not a real error)
        if echo "$error_output" | grep -q "ERROR"; then
            echo "$error_output" >&2
            return 1
        fi
        return 0
    fi
    
    # Try with password if provided
    if [ -n "$MYSQL_PASSWORD" ]; then
        if [ "$show_errors" = "true" ]; then
            if mysql -h"$MYSQL_HOST" -P"$MYSQL_PORT" -u"$MYSQL_USER" -p"$MYSQL_PASSWORD" < "$sql_file" 2>&1; then
                return 0
            fi
        else
            error_output=$(mysql -h"$MYSQL_HOST" -P"$MYSQL_PORT" -u"$MYSQL_USER" -p"$MYSQL_PASSWORD" < "$sql_file" 2>&1)
            if [ $? -eq 0 ]; then
                return 0
            fi
            # Check if error is just from SELECT output (not a real error)
            if echo "$error_output" | grep -q "ERROR"; then
                echo "$error_output" >&2
                return 1
            fi
            return 0
        fi
    fi
    
    return 1
}

# IMPORTANT: Order of operations must match README.md flow:
# 1. Setup Cedar plugins FIRST (so DDL audit plugin can capture CREATE events)
# 2. THEN create entities (users and tables) - DDL plugin will propagate them to Cedar agent
# 3. Wait for propagation
# 4. THEN populate attributes and policies

# Setup Cedar plugins FIRST (if init SQL exists)
if [ -f "$BRIEF_DIR/cedar_init.sql" ]; then
    echo -e "${YELLOW}Setting up Cedar plugins...${NC}"
    
    # Install missing runtime dependencies if using Docker container
    if [ -n "$MYSQL_CONTAINER" ]; then
        echo -e "${YELLOW}Checking for required runtime dependencies...${NC}"
        
        # Verify that required runtime dependencies are preinstalled
        echo "Verifying preinstalled runtime dependencies..."
        if ! docker exec "$MYSQL_CONTAINER" sh -c "ldconfig -p | grep -q libcurl.so.4" 2>/dev/null; then
            echo -e "${RED}Error: libcurl.so.4 not found. The Docker image may not have been built with the updated dependencies.${NC}"
            echo "Please rebuild the MySQL Docker image with the latest Dockerfile."
            exit 1
        fi
        if ! docker exec "$MYSQL_CONTAINER" sh -c "ldconfig -p | grep -q libjsoncpp" 2>/dev/null; then
            echo -e "${RED}Error: libjsoncpp not found. The Docker image may not have been built with the updated dependencies.${NC}"
            echo "Please rebuild the MySQL Docker image with the latest Dockerfile."
            exit 1
        fi
        echo "✓ All required runtime dependencies verified."
    fi
    
    # Check if plugins are already installed (make script idempotent)
    echo -e "${YELLOW}Checking if plugins are already installed...${NC}"
    PLUGIN_CHECK_SQL=$(mktemp)
    cat > "$PLUGIN_CHECK_SQL" <<'EOF'
SELECT PLUGIN_NAME FROM INFORMATION_SCHEMA.PLUGINS 
WHERE PLUGIN_NAME IN ('ddl_audit', 'cedar_authorization');
EOF
    
    INSTALLED_PLUGINS=""
    # Use the same connection logic as execute_sql
    if [ -n "$MYSQL_CONTAINER" ]; then
        # Try without password first
        INSTALLED_PLUGINS=$(docker exec -i "$MYSQL_CONTAINER" mysql -u"$MYSQL_USER" -N -s < "$PLUGIN_CHECK_SQL" 2>/dev/null || echo "")
        # If that failed and password is provided, try with password
        if [ -z "$INSTALLED_PLUGINS" ] && [ -n "$MYSQL_PASSWORD" ]; then
            INSTALLED_PLUGINS=$(docker exec -i -e MYSQL_PWD="$MYSQL_PASSWORD" "$MYSQL_CONTAINER" mysql -u"$MYSQL_USER" -N -s < "$PLUGIN_CHECK_SQL" 2>/dev/null || echo "")
        fi
    else
        # Direct connection
        if [ -n "$MYSQL_PASSWORD" ]; then
            INSTALLED_PLUGINS=$(mysql -h"$MYSQL_HOST" -P"$MYSQL_PORT" -u"$MYSQL_USER" -p"$MYSQL_PASSWORD" -N -s < "$PLUGIN_CHECK_SQL" 2>/dev/null || echo "")
        else
            INSTALLED_PLUGINS=$(mysql -h"$MYSQL_HOST" -P"$MYSQL_PORT" -u"$MYSQL_USER" -N -s < "$PLUGIN_CHECK_SQL" 2>/dev/null || echo "")
        fi
    fi
    rm "$PLUGIN_CHECK_SQL"
    
    # Create a modified version of cedar_init.sql with the correct Cedar agent URL
    PLUGIN_SQL=$(mktemp)
    
    # Replace localhost:8280 with the Cedar agent URL for MySQL (container name if using docker-compose)
    sed -e "s|http://localhost:8280|${CEDAR_AGENT_URL_FOR_MYSQL}|g" \
        -e "s|localhost:8280|$(echo "$CEDAR_AGENT_URL_FOR_MYSQL" | sed -e 's|http://||' -e 's|https://||' -e 's|/.*||')|g" \
        "$BRIEF_DIR/cedar_init.sql" > "$PLUGIN_SQL"
    
    # Remove INSTALL PLUGIN commands if plugins are already installed
    if echo "$INSTALLED_PLUGINS" | grep -q "ddl_audit"; then
        echo "Plugin 'ddl_audit' is already installed, skipping installation."
        sed -i '/^INSTALL PLUGIN ddl_audit/d' "$PLUGIN_SQL"
    fi
    
    if echo "$INSTALLED_PLUGINS" | grep -q "cedar_authorization"; then
        echo "Plugin 'cedar_authorization' is already installed, skipping installation."
        sed -i '/^INSTALL PLUGIN cedar_authorization/d' "$PLUGIN_SQL"
    fi
    
    # Execute the SQL (configuration commands will still run even if plugins are already installed)
    # Capture output to check for "already exists" errors
    PLUGIN_OUTPUT=$(mktemp)
    if [ -n "$MYSQL_CONTAINER" ]; then
        # Try without password first
        docker exec -i "$MYSQL_CONTAINER" mysql -u"$MYSQL_USER" < "$PLUGIN_SQL" > "$PLUGIN_OUTPUT" 2>&1 || {
            # If that failed and password is provided, try with password
            if [ -n "$MYSQL_PASSWORD" ]; then
                docker exec -i -e MYSQL_PWD="$MYSQL_PASSWORD" "$MYSQL_CONTAINER" mysql -u"$MYSQL_USER" < "$PLUGIN_SQL" > "$PLUGIN_OUTPUT" 2>&1 || true
            fi
        }
    else
        # Direct connection
        if [ -n "$MYSQL_PASSWORD" ]; then
            mysql -h"$MYSQL_HOST" -P"$MYSQL_PORT" -u"$MYSQL_USER" -p"$MYSQL_PASSWORD" < "$PLUGIN_SQL" > "$PLUGIN_OUTPUT" 2>&1 || true
        else
            mysql -h"$MYSQL_HOST" -P"$MYSQL_PORT" -u"$MYSQL_USER" < "$PLUGIN_SQL" > "$PLUGIN_OUTPUT" 2>&1 || true
        fi
    fi
    
    # Check for errors (but ignore "already exists" errors)
    if grep -q "ERROR" "$PLUGIN_OUTPUT"; then
        # Check if all errors are "already exists" - if so, that's fine
        ERROR_LINES=$(grep "ERROR" "$PLUGIN_OUTPUT" || true)
        NON_EXISTS_ERRORS=$(echo "$ERROR_LINES" | grep -v "already exists" || true)
        
        if [ -n "$NON_EXISTS_ERRORS" ]; then
            # There are real errors, not just "already exists"
            echo -e "${RED}Error: Failed to setup Cedar plugins${NC}"
            echo ""
            echo "Error output:"
            cat "$PLUGIN_OUTPUT"
            echo ""
            echo "Troubleshooting:"
            echo "1. Check if the SQL file has errors:"
            echo "   cat $PLUGIN_SQL"
            echo ""
            if [ -n "$MYSQL_CONTAINER" ]; then
                echo "2. Try running manually to see full output:"
                echo "   docker exec -i $MYSQL_CONTAINER mysql -u root < $PLUGIN_SQL"
                echo ""
                echo "3. Check MySQL error logs:"
                echo "   docker logs $MYSQL_CONTAINER | tail -30"
                echo ""
                echo "4. Check if plugins are already installed:"
                echo "   docker exec -i $MYSQL_CONTAINER mysql -u root -e \"SELECT PLUGIN_NAME FROM INFORMATION_SCHEMA.PLUGINS WHERE PLUGIN_NAME LIKE '%cedar%' OR PLUGIN_NAME LIKE '%ddl%';\""
                echo ""
                echo "5. Check if plugin files exist:"
                echo "   docker exec $MYSQL_CONTAINER ls -la /usr/local/mysql/lib/plugin/ | grep -E 'ddl_audit|cedar_authorization'"
                echo ""
                echo "6. Check if dependencies are installed:"
                echo "   docker exec $MYSQL_CONTAINER ldconfig -p | grep -E 'libcurl|libjsoncpp'"
            fi
            rm "$PLUGIN_SQL" "$PLUGIN_OUTPUT"
            exit 1
        else
            # Only "already exists" errors - that's fine, plugins are already installed
            echo "Plugins are already installed (some 'already exists' warnings are expected)."
        fi
    else
        # No errors, show output if there's any useful info
        if [ -s "$PLUGIN_OUTPUT" ]; then
            cat "$PLUGIN_OUTPUT"
        fi
    fi
    rm "$PLUGIN_SQL" "$PLUGIN_OUTPUT"
    
    # Verify plugins are installed
    echo -e "${YELLOW}Verifying plugins are installed...${NC}"
    if [ -n "$MYSQL_CONTAINER" ]; then
        docker exec -i "$MYSQL_CONTAINER" mysql -u root -e "SELECT PLUGIN_NAME, PLUGIN_STATUS FROM INFORMATION_SCHEMA.PLUGINS WHERE PLUGIN_NAME IN ('ddl_audit', 'cedar_authorization');" 2>/dev/null || true
    fi
else
    echo -e "${YELLOW}Note: cedar_init.sql not found, skipping Cedar plugin setup${NC}"
    echo "  Expected location: $BRIEF_DIR/cedar_init.sql"
fi

# Step 2: Create entities (users and tables) AFTER plugins are installed
# This ensures the DDL audit plugin captures CREATE events and propagates entities to Cedar agent
echo -e "${YELLOW}Creating entities (users and tables)...${NC}"
if [ -n "$MYSQL_CONTAINER" ]; then
    echo "Using Docker container: $MYSQL_CONTAINER"
fi

SETUP_SQL=$(mktemp)
# Create SQL - always create root@'%' to allow external connections
# If password is provided, use it; otherwise create without password
cat > "$SETUP_SQL" <<'SETUP_EOF'
-- Ensure root can connect from any host (fixes Docker bridge network issue)
-- First, try to create root@'%' without password (for containers without root password)
CREATE USER IF NOT EXISTS 'root'@'%';
GRANT ALL PRIVILEGES ON *.* TO 'root'@'%' WITH GRANT OPTION;
SETUP_EOF

# If password is provided, also set it for root@'%'
if [ -n "$MYSQL_PASSWORD" ]; then
    cat >> "$SETUP_SQL" <<EOF
-- Set password for root@'%' if password was provided
ALTER USER 'root'@'%' IDENTIFIED BY '${MYSQL_PASSWORD}';
EOF
fi

cat >> "$SETUP_SQL" <<'SETUP_EOF'
FLUSH PRIVILEGES;
SETUP_EOF

cat >> "$SETUP_SQL" <<'EOF'

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

-- Create users (if they don't exist)
-- These will be propagated to Cedar agent by the DDL audit plugin
CREATE USER IF NOT EXISTS 'user_alice'@'%' IDENTIFIED BY '';
CREATE USER IF NOT EXISTS 'user_bob'@'%' IDENTIFIED BY '';
CREATE USER IF NOT EXISTS 'user_charlie'@'%' IDENTIFIED BY '';
EOF

if ! execute_sql "$SETUP_SQL"; then
    echo -e "${RED}Error: Failed to execute setup SQL${NC}"
    echo ""
    echo "Troubleshooting steps:"
    echo ""
    if [ -n "$MYSQL_CONTAINER" ]; then
        echo "1. Try connecting directly to the container:"
        echo "   docker exec -it $MYSQL_CONTAINER mysql -u root"
        echo ""
        echo "2. If that works, manually run the setup SQL:"
        echo "   docker exec -i $MYSQL_CONTAINER mysql -u root < $SETUP_SQL"
        echo ""
    else
        echo "1. Check if MySQL container is running:"
        echo "   docker ps | grep mysql"
        echo ""
        echo "2. Try connecting directly:"
if [ -n "$MYSQL_PASSWORD" ]; then
            echo "   mysql -h $MYSQL_HOST -P $MYSQL_PORT -u $MYSQL_USER -p"
else
            echo "   mysql -h $MYSQL_HOST -P $MYSQL_PORT -u $MYSQL_USER"
        fi
        echo ""
    fi
    echo "3. If you see 'Host is not allowed to connect' error, run:"
    echo "   CREATE USER IF NOT EXISTS 'root'@'%';"
    echo "   GRANT ALL PRIVILEGES ON *.* TO 'root'@'%' WITH GRANT OPTION;"
    echo "   FLUSH PRIVILEGES;"
    rm "$SETUP_SQL"
    exit 1
fi
rm "$SETUP_SQL"

# Step 3: Setup Cedar policies and attributes
# Note: Entities are now created in MySQL. The DDL audit plugin will propagate them
# to Cedar agent. The translate_to_cedar.py script will wait for entities to appear
# before setting attributes.
# Note: The Cedar agent should already be initialized with base schema from mysql_schemas/schema.json
# This script adds custom attributes, creates entities, assigns attributes, and creates policies
echo -e "${YELLOW}Setting up Cedar policies and attributes...${NC}"
python3 "$FRAMEWORK_DIR/translate_to_cedar.py" "$AUTH_SPEC" "${CEDAR_AGENT_URL_HOST}/v1" "MySQL"

if [ $? -ne 0 ]; then
    echo -e "${RED}Error: Failed to setup Cedar policies and attributes${NC}"
    echo "Make sure the Cedar agent is running with the MySQL schema files."
    exit 1
fi

echo -e "${GREEN}Cedar MySQL setup complete!${NC}"

