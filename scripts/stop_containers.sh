#!/bin/bash
# Stop and remove Docker containers used for experiments
# This script stops and removes:
#   - mysql-baseline (Baseline MySQL)
#   - mysql-cedar (Cedar MySQL)
#   - cedar-agent-experiments or cedar-agent (Cedar Agent)

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Container names (try both common names)
BASELINE_CONTAINER="${BASELINE_CONTAINER:-mysql-baseline}"
CEDAR_MYSQL_CONTAINER="${CEDAR_MYSQL_CONTAINER:-mysql-cedar}"
CEDAR_AGENT_CONTAINER="${CEDAR_AGENT_CONTAINER:-cedar-agent-experiments}"

# Colors
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
BLUE='\033[0;34m'
NC='\033[0m'

echo -e "${BLUE}=== Stopping and Removing Experiment Containers ===${NC}"
echo ""

# Check if Docker is available
if ! command -v docker >/dev/null 2>&1; then
    echo -e "${RED}Error: Docker is not installed or not in PATH${NC}"
    exit 1
fi

# Function to stop and remove a container
stop_and_remove_container() {
    local container_name="$1"
    local description="$2"
    
    # Check if container exists (running or stopped)
    if docker ps -a --format '{{.Names}}' | grep -q "^${container_name}$"; then
        echo -e "${YELLOW}Stopping ${description} (${container_name})...${NC}"
        docker stop "$container_name" 2>/dev/null || true
        
        echo -e "${YELLOW}Removing ${description} (${container_name})...${NC}"
        docker rm "$container_name" 2>/dev/null || true
        
        echo -e "${GREEN}✓ ${description} stopped and removed${NC}"
        return 0
    else
        # Try alternative names
        local found_container=""
        
        # For baseline MySQL, try finding by port
        if [ "$container_name" = "$BASELINE_CONTAINER" ]; then
            found_container=$(docker ps -a --format "{{.Names}}" --filter "publish=13306" 2>/dev/null | head -n1 || echo "")
            if [ -z "$found_container" ]; then
                found_container=$(docker ps -a --format "{{.Names}}" | grep -i "mysql.*baseline\|baseline.*mysql" | head -n1 || echo "")
            fi
        fi
        
        # For Cedar MySQL, try finding by port
        if [ "$container_name" = "$CEDAR_MYSQL_CONTAINER" ]; then
            found_container=$(docker ps -a --format "{{.Names}}" --filter "publish=13307" 2>/dev/null | head -n1 || echo "")
            if [ -z "$found_container" ]; then
                found_container=$(docker ps -a --format "{{.Names}}" | grep -i "mysql.*cedar\|cedar.*mysql" | head -n1 || echo "")
            fi
        fi
        
        # For Cedar Agent, try alternative names
        if [ "$container_name" = "$CEDAR_AGENT_CONTAINER" ]; then
            found_container=$(docker ps -a --format "{{.Names}}" | grep -i "cedar.*agent\|agent.*cedar" | head -n1 || echo "")
            if [ -z "$found_container" ]; then
                found_container=$(docker ps -a --format "{{.Names}}" --filter "publish=8280" 2>/dev/null | head -n1 || echo "")
            fi
        fi
        
        if [ -n "$found_container" ]; then
            echo -e "${YELLOW}Found container with different name: ${found_container}${NC}"
            echo -e "${YELLOW}Stopping ${description} (${found_container})...${NC}"
            docker stop "$found_container" 2>/dev/null || true
            
            echo -e "${YELLOW}Removing ${description} (${found_container})...${NC}"
            docker rm "$found_container" 2>/dev/null || true
            
            echo -e "${GREEN}✓ ${description} stopped and removed${NC}"
            return 0
        else
            echo -e "${YELLOW}  ${description} container not found (may already be stopped/removed)${NC}"
            return 1
        fi
    fi
}

# Stop and remove containers
echo -e "${YELLOW}[1/3] Stopping Baseline MySQL container...${NC}"
stop_and_remove_container "$BASELINE_CONTAINER" "Baseline MySQL"

echo ""

echo -e "${YELLOW}[2/3] Stopping Cedar MySQL container...${NC}"
stop_and_remove_container "$CEDAR_MYSQL_CONTAINER" "Cedar MySQL"

echo ""

echo -e "${YELLOW}[3/3] Stopping Cedar Agent container...${NC}"
stop_and_remove_container "$CEDAR_AGENT_CONTAINER" "Cedar Agent"

echo ""
echo -e "${GREEN}=== Container Cleanup Complete ===${NC}"
echo ""
echo "All experiment containers have been stopped and removed."
echo ""
echo "To start containers again, run:"
echo "  # Start Baseline MySQL:"
echo "  docker run -d --name mysql-baseline -e MYSQL_ROOT_PASSWORD=rootpass -p 13306:3306 mysql:8.0.43"
echo ""
echo "  # Start Cedar MySQL:"
echo "  docker run -d --name mysql-cedar -e MYSQL_ROOT_PASSWORD=rootpass -p 13307:3306 ghcr.io/archarcade/mysql:latest"
echo ""
echo "  # Start Cedar Agent:"
echo "  ./start_cedar_agent.sh"
echo ""

