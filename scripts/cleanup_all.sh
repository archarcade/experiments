#!/bin/bash
# Cleanup both Baseline and Cedar MySQL setups
# This allows you to reinitialize the setup cleanly
# 
# This script cleans up:
#   1. Data, users, tables, policies from MySQL instances
#   2. Optionally stops and removes Docker containers (if STOP_CONTAINERS=true)

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Configuration: Set STOP_CONTAINERS=true to also stop/remove containers
STOP_CONTAINERS="${STOP_CONTAINERS:-false}"

# Colors
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
BLUE='\033[0;34m'
NC='\033[0m'

echo -e "${BLUE}=== Cleaning up Experiment Environment ===${NC}"
echo ""

# Cleanup Baseline MySQL
echo -e "${YELLOW}[1/3] Cleaning up Baseline MySQL data...${NC}"
if [ -f "$SCRIPT_DIR/cleanup_baseline_mysql.sh" ]; then
    bash "$SCRIPT_DIR/cleanup_baseline_mysql.sh"
else
    echo "  Warning: cleanup_baseline_mysql.sh not found, skipping baseline cleanup"
fi

echo ""

# Cleanup Cedar MySQL
echo -e "${YELLOW}[2/3] Cleaning up Cedar MySQL data...${NC}"
if [ -f "$SCRIPT_DIR/cleanup_cedar_mysql.sh" ]; then
    bash "$SCRIPT_DIR/cleanup_cedar_mysql.sh"
else
    echo -e "${RED}Error: cleanup_cedar_mysql.sh not found${NC}"
    exit 1
fi

echo ""

# Optionally stop and remove containers
if [ "$STOP_CONTAINERS" = "true" ]; then
    echo -e "${YELLOW}[3/3] Stopping and removing containers...${NC}"
    if [ -f "$SCRIPT_DIR/stop_containers.sh" ]; then
        bash "$SCRIPT_DIR/stop_containers.sh"
    else
        echo "  Warning: stop_containers.sh not found, skipping container cleanup"
    fi
else
    echo -e "${YELLOW}[3/3] Skipping container cleanup (set STOP_CONTAINERS=true to stop containers)${NC}"
fi

echo ""
echo -e "${GREEN}=== Cleanup Complete ===${NC}"
echo ""
echo "Both MySQL instances have been cleaned up."
if [ "$STOP_CONTAINERS" != "true" ]; then
    echo ""
    echo "Note: Containers are still running. To stop them, run:"
    echo "  STOP_CONTAINERS=true ./cleanup_all.sh"
    echo "  # or"
    echo "  ./stop_containers.sh"
fi
echo ""
echo "To reinitialize the setup, run:"
echo "  ./experiments/scripts/setup_all.sh"
echo ""

