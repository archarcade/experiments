#!/bin/bash
# Master setup script - sets up both baseline and Cedar MySQL instances

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Configuration
BASELINE_MYSQL_HOST="${BASELINE_MYSQL_HOST:-127.0.0.1}"
BASELINE_MYSQL_PORT="${BASELINE_MYSQL_PORT:-13306}"
BASELINE_MYSQL_USER="${BASELINE_MYSQL_USER:-root}"
BASELINE_MYSQL_PASSWORD="${BASELINE_MYSQL_PASSWORD:-}"

CEDAR_MYSQL_HOST="${CEDAR_MYSQL_HOST:-127.0.0.1}"
CEDAR_MYSQL_PORT="${CEDAR_MYSQL_PORT:-13307}"
CEDAR_MYSQL_USER="${CEDAR_MYSQL_USER:-root}"
CEDAR_MYSQL_PASSWORD="${CEDAR_MYSQL_PASSWORD:-}"

CEDAR_AGENT_URL="${CEDAR_AGENT_URL:-http://localhost:8280}"

# Colors
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

echo -e "${GREEN}=== Setting up Experiment Environment ===${NC}"
echo ""

# Check if Cedar agent is running (required for Cedar MySQL setup)
CEDAR_AGENT_URL="${CEDAR_AGENT_URL:-http://localhost:8280}"
if ! curl -s -f "${CEDAR_AGENT_URL}/v1/" > /dev/null 2>&1; then
    echo -e "${RED}Error: Cedar agent is not running.${NC}"
    echo "Please start it first with: ./start_cedar_agent.sh"
    echo ""
    echo "The Cedar agent must be initialized with MySQL schema files before setup."
    exit 1
fi

# Setup Baseline MySQL
echo -e "${YELLOW}[1/2] Setting up Baseline MySQL...${NC}"
export MYSQL_HOST="$BASELINE_MYSQL_HOST"
export MYSQL_PORT="$BASELINE_MYSQL_PORT"
export MYSQL_USER="$BASELINE_MYSQL_USER"
export MYSQL_PASSWORD="$BASELINE_MYSQL_PASSWORD"
"$SCRIPT_DIR/setup_baseline_mysql.sh"

echo ""

# Setup Cedar MySQL
echo -e "${YELLOW}[2/2] Setting up Cedar MySQL...${NC}"
export MYSQL_HOST="$CEDAR_MYSQL_HOST"
export MYSQL_PORT="$CEDAR_MYSQL_PORT"
export MYSQL_USER="$CEDAR_MYSQL_USER"
export MYSQL_PASSWORD="$CEDAR_MYSQL_PASSWORD"
export CEDAR_AGENT_URL="$CEDAR_AGENT_URL"
"$SCRIPT_DIR/setup_cedar_mysql.sh"

echo ""
echo -e "${GREEN}=== Setup Complete ===${NC}"
echo ""
echo "Both MySQL instances are ready for experiments."
echo ""
echo "To run Performance Overhead experiment:"
echo "  ./run_performance_overhead.sh"

