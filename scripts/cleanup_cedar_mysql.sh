#!/bin/bash
# Cleanup Cedar MySQL setup - removes entities, policies, users, tables, and plugins
# This allows you to reinitialize the setup cleanly
# 
# Optionally stops and removes containers if STOP_CONTAINERS=true

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
STOP_CONTAINERS="${STOP_CONTAINERS:-false}"

# Try to find MySQL container by port mapping
MYSQL_CONTAINER=""
if command -v docker >/dev/null 2>&1; then
    # Find container with port mapping matching MYSQL_PORT:3306
    MYSQL_CONTAINER=$(docker ps --format "{{.Names}}" --filter "publish=${MYSQL_PORT}" 2>/dev/null | head -n1)
    if [ -z "$MYSQL_CONTAINER" ]; then
        # Try alternative: find container with mysql-cedar in name
        MYSQL_CONTAINER=$(docker ps -a --format "{{.Names}}" | grep -i "mysql.*cedar\|cedar.*mysql" | head -n1)
    fi
fi

# Try to find Cedar Agent container
CEDAR_AGENT_CONTAINER=""
if command -v docker >/dev/null 2>&1; then
    CEDAR_AGENT_CONTAINER=$(docker ps -a --format "{{.Names}}" | grep -i "cedar.*agent\|agent.*cedar" | head -n1)
    if [ -z "$CEDAR_AGENT_CONTAINER" ]; then
        CEDAR_AGENT_CONTAINER=$(docker ps -a --format "{{.Names}}" --filter "publish=8280" 2>/dev/null | head -n1)
    fi
fi

# Colors
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

echo -e "${YELLOW}=== Cleaning up Cedar MySQL Setup ===${NC}"
echo "MySQL: ${MYSQL_USER}@${MYSQL_HOST}:${MYSQL_PORT}"
if [ -n "$MYSQL_CONTAINER" ]; then
    echo "MySQL Container: ${MYSQL_CONTAINER}"
fi
echo "Cedar Agent: ${CEDAR_AGENT_URL}"
if [ -n "$CEDAR_AGENT_CONTAINER" ]; then
    echo "Cedar Agent Container: ${CEDAR_AGENT_CONTAINER}"
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

# Step 1: Delete policies from Cedar agent
echo -e "${YELLOW}[1/5] Deleting policies from Cedar agent...${NC}"
if [ -f "$AUTH_SPEC" ]; then
    # Extract policy IDs from auth spec
    if command -v python3 >/dev/null 2>&1; then
        POLICY_IDS=$(python3 -c "
import json
import sys
try:
    with open('$AUTH_SPEC') as f:
        spec = json.load(f)
    # Generate policy IDs based on auth spec structure
    policies = []
    for req in spec.get('requirements', []):
        policy_id = req.get('policy_id') or req.get('name', '').lower().replace(' ', '_') + '_access'
        if policy_id:
            policies.append(policy_id)
    # Also check for common policy names
    common_policies = ['manager_access', 'employee_access', 'intern_access', 'sensitiv_data_access']
    for p in common_policies:
        if p not in policies:
            policies.append(p)
    print(' '.join(policies))
except Exception as e:
    print('manager_access employee_access intern_access sensitiv_data_access', file=sys.stderr)
    sys.exit(0)
" 2>/dev/null || echo "manager_access employee_access intern_access sensitiv_data_access")
    else
        # Fallback: use common policy names
        POLICY_IDS="manager_access employee_access intern_access sensitiv_data_access"
    fi
    
    BASE_URL="${CEDAR_AGENT_URL}"
    if ! echo "$BASE_URL" | grep -q "/v1$"; then
        if [ "${BASE_URL: -1}" = "/" ]; then
            BASE_URL="${BASE_URL}v1"
        else
            BASE_URL="${BASE_URL}/v1"
        fi
    fi
    
    for policy_id in $POLICY_IDS; do
        if curl -s -X DELETE "${BASE_URL}/policies/${policy_id}" >/dev/null 2>&1; then
            echo "  Deleted policy: ${policy_id}"
        else
            echo "  Policy ${policy_id} not found or already deleted (skipping)"
        fi
    done
else
    echo "  Warning: Auth spec not found, skipping policy deletion"
fi

# Step 2: Delete entities from Cedar agent
echo -e "${YELLOW}[2/5] Deleting entities from Cedar agent...${NC}"
BASE_URL="${CEDAR_AGENT_URL}"
if ! echo "$BASE_URL" | grep -q "/v1$"; then
    if [ "${BASE_URL: -1}" = "/" ]; then
        BASE_URL="${BASE_URL}v1"
    else
        BASE_URL="${BASE_URL}/v1"
    fi
fi

# Try to delete all entities at once
if curl -s -X DELETE "${BASE_URL}/data" >/dev/null 2>&1; then
    echo "  Deleted all entities"
else
    echo "  Warning: Failed to delete all entities (may not exist)"
    # Try deleting individual entities
    ENTITIES="user_alice user_bob user_charlie abac_test.employees abac_test.projects abac_test.sensitive_data"
    for entity_id in $ENTITIES; do
        if curl -s -X DELETE "${BASE_URL}/data/single/${entity_id}" >/dev/null 2>&1; then
            echo "  Deleted entity: ${entity_id}"
        fi
    done
fi

# Step 3: Delete MySQL users and tables
echo -e "${YELLOW}[3/5] Deleting MySQL users and tables...${NC}"
if [ -f "$BRIEF_DIR/cedar_delete.sql" ]; then
    if execute_sql "$BRIEF_DIR/cedar_delete.sql"; then
        echo "  Deleted users and tables"
    else
        echo "  Warning: Some users/tables may not exist (skipping)"
    fi
else
    # Fallback: create cleanup SQL inline
    CLEANUP_SQL=$(mktemp)
    cat > "$CLEANUP_SQL" <<'EOF'
-- Remove demo data/users
DROP TABLE IF EXISTS abac_test.employees;
DROP TABLE IF EXISTS abac_test.projects;
DROP TABLE IF EXISTS abac_test.sensitive_data;

-- Remove demo data and schema
DROP DATABASE IF EXISTS abac_test;

-- Remove demo users
DROP USER IF EXISTS 'user_alice'@'%';
DROP USER IF EXISTS 'user_bob'@'%';
DROP USER IF EXISTS 'user_charlie'@'%';
EOF
    if execute_sql "$CLEANUP_SQL"; then
        echo "  Deleted users and tables"
    else
        echo "  Warning: Some users/tables may not exist (skipping)"
    fi
    rm "$CLEANUP_SQL"
fi

# Step 4: Uninstall Cedar plugins
echo -e "${YELLOW}[4/5] Uninstalling Cedar plugins...${NC}"
if [ -f "$BRIEF_DIR/cedar_deinit.sql" ]; then
    DEINIT_SQL=$(mktemp)
    # Check if plugins are installed before trying to uninstall
    PLUGIN_CHECK_SQL=$(mktemp)
    cat > "$PLUGIN_CHECK_SQL" <<'EOF'
SELECT PLUGIN_NAME FROM INFORMATION_SCHEMA.PLUGINS 
WHERE PLUGIN_NAME IN ('ddl_audit', 'cedar_authorization');
EOF
    
    INSTALLED_PLUGINS=""
    if [ -n "$MYSQL_CONTAINER" ]; then
        INSTALLED_PLUGINS=$(docker exec -i "$MYSQL_CONTAINER" mysql -u"$MYSQL_USER" -N -s < "$PLUGIN_CHECK_SQL" 2>/dev/null || echo "")
        if [ -z "$INSTALLED_PLUGINS" ] && [ -n "$MYSQL_PASSWORD" ]; then
            INSTALLED_PLUGINS=$(docker exec -i -e MYSQL_PWD="$MYSQL_PASSWORD" "$MYSQL_CONTAINER" mysql -u"$MYSQL_USER" -N -s < "$PLUGIN_CHECK_SQL" 2>/dev/null || echo "")
        fi
    else
        if [ -n "$MYSQL_PASSWORD" ]; then
            INSTALLED_PLUGINS=$(mysql -h"$MYSQL_HOST" -P"$MYSQL_PORT" -u"$MYSQL_USER" -p"$MYSQL_PASSWORD" -N -s < "$PLUGIN_CHECK_SQL" 2>/dev/null || echo "")
        else
            INSTALLED_PLUGINS=$(mysql -h"$MYSQL_HOST" -P"$MYSQL_PORT" -u"$MYSQL_USER" -N -s < "$PLUGIN_CHECK_SQL" 2>/dev/null || echo "")
        fi
    fi
    rm "$PLUGIN_CHECK_SQL"
    
    # Only uninstall plugins that are actually installed
    cat > "$DEINIT_SQL" <<'EOF'
SET GLOBAL ddl_audit_enabled = OFF;
SET GLOBAL cedar_authorization_url = DEFAULT;
EOF
    
    if echo "$INSTALLED_PLUGINS" | grep -q "ddl_audit"; then
        echo "UNINSTALL PLUGIN ddl_audit;" >> "$DEINIT_SQL"
    fi
    
    if echo "$INSTALLED_PLUGINS" | grep -q "cedar_authorization"; then
        echo "UNINSTALL PLUGIN cedar_authorization;" >> "$DEINIT_SQL"
    fi
    
    if execute_sql "$DEINIT_SQL"; then
        echo "  Uninstalled plugins"
    else
        echo "  Warning: Plugins may not be installed (skipping)"
    fi
    rm "$DEINIT_SQL"
else
    echo "  Warning: cedar_deinit.sql not found, skipping plugin uninstallation"
fi

# Step 5: Optional - Remove schema attributes (commented out by default)
# Uncomment if you want to remove custom schema attributes as well
# echo -e "${YELLOW}[5/5] Removing schema attributes...${NC}"
# BASE_URL="${CEDAR_AGENT_URL}"
# if ! echo "$BASE_URL" | grep -q "/v1$"; then
#     if [ "${BASE_URL: -1}" = "/" ]; then
#         BASE_URL="${BASE_URL}v1"
#     else
#         BASE_URL="${BASE_URL}/v1"
#     fi
# fi
# 
# ATTRIBUTES="user_role clearance_level data_classification"
# for attr in $ATTRIBUTES; do
#     if [ "$attr" = "data_classification" ]; then
#         ENTITY_TYPE="Table"
#     else
#         ENTITY_TYPE="User"
#     fi
#     if curl -s -X DELETE "${BASE_URL}/schema/attribute" \
#         -H "Content-Type: application/json" \
#         -d "{\"entity_type\":\"${ENTITY_TYPE}\",\"namespace\":\"\",\"name\":\"${attr}\"}" >/dev/null 2>&1; then
#         echo "  Removed attribute: ${ENTITY_TYPE}.${attr}"
#     fi
# done

# Optionally stop and remove containers
if [ "$STOP_CONTAINERS" = "true" ]; then
    echo ""
    echo -e "${YELLOW}[6/6] Stopping and removing containers...${NC}"
    
    if [ -n "$MYSQL_CONTAINER" ]; then
        echo -e "${YELLOW}Stopping MySQL container: ${MYSQL_CONTAINER}...${NC}"
        docker stop "$MYSQL_CONTAINER" 2>/dev/null || true
        docker rm "$MYSQL_CONTAINER" 2>/dev/null || true
        echo -e "${GREEN}MySQL container stopped and removed${NC}"
    fi
    
    if [ -n "$CEDAR_AGENT_CONTAINER" ]; then
        echo -e "${YELLOW}Stopping Cedar Agent container: ${CEDAR_AGENT_CONTAINER}...${NC}"
        docker stop "$CEDAR_AGENT_CONTAINER" 2>/dev/null || true
        docker rm "$CEDAR_AGENT_CONTAINER" 2>/dev/null || true
        echo -e "${GREEN}Cedar Agent container stopped and removed${NC}"
    fi
fi

echo ""
echo -e "${GREEN}Cleanup complete!${NC}"
echo ""
echo "You can now reinitialize the setup by running:"
echo "  ./experiments/scripts/setup_all.sh"
echo ""

