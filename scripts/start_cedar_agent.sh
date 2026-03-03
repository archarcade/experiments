#!/bin/bash
# Start Cedar Agent with MySQL schema initialization
# This script ensures the Cedar agent is started with the proper schema, policies, and data files

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
EXPERIMENTS_DIR="$(dirname "$SCRIPT_DIR")"
MYSQL_SCHEMAS_DIR="$EXPERIMENTS_DIR/context/mysql_schemas"

CEDAR_AGENT_PORT="${CEDAR_AGENT_PORT:-8280}"
CEDAR_AGENT_IMAGE="${CEDAR_AGENT_IMAGE:-ghcr.io/archarcade/cedar-agent:latest}"
CEDAR_AGENT_NAME="${CEDAR_AGENT_NAME:-cedar-agent-experiments}"
LOG_LEVEL="${LOG_LEVEL:-info}"

# Colors
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

echo -e "${GREEN}=== Starting Cedar Agent ===${NC}"
echo "Port: ${CEDAR_AGENT_PORT}"
echo "Schema directory: ${MYSQL_SCHEMAS_DIR}"
echo ""

# Check if schema files exist
if [ ! -f "$MYSQL_SCHEMAS_DIR/schema.json" ]; then
    echo -e "${RED}Error: schema.json not found at $MYSQL_SCHEMAS_DIR/schema.json${NC}"
    exit 1
fi

if [ ! -f "$MYSQL_SCHEMAS_DIR/policies.json" ]; then
    echo -e "${RED}Error: policies.json not found at $MYSQL_SCHEMAS_DIR/policies.json${NC}"
    exit 1
fi

if [ ! -f "$MYSQL_SCHEMAS_DIR/data.json" ]; then
    echo -e "${YELLOW}Warning: data.json not found, creating empty one...${NC}"
    echo "[]" > "$MYSQL_SCHEMAS_DIR/data.json"
fi

# Check if container already exists
if docker ps -a --format '{{.Names}}' | grep -q "^${CEDAR_AGENT_NAME}$"; then
    echo -e "${YELLOW}Container ${CEDAR_AGENT_NAME} already exists.${NC}"
    read -p "Stop and remove existing container? (y/n) " -n 1 -r
    echo
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        echo "Stopping and removing existing container..."
        docker stop "$CEDAR_AGENT_NAME" 2>/dev/null || true
        docker rm "$CEDAR_AGENT_NAME" 2>/dev/null || true
    else
        echo "Using existing container. Start it with: docker start $CEDAR_AGENT_NAME"
        exit 0
    fi
fi

# Start Cedar agent with mounted schema files
# Note: Cedar agent defaults to internal port 8180, so we map host port to container port 8180
echo -e "${YELLOW}Starting Cedar agent container...${NC}"
docker run -d \
  --name "$CEDAR_AGENT_NAME" \
  -p "${CEDAR_AGENT_PORT}:8180" \
  -v "${MYSQL_SCHEMAS_DIR}:/app/mysql_schemas:ro" \
  "$CEDAR_AGENT_IMAGE" \
  -l "$LOG_LEVEL" \
  -s /app/mysql_schemas/schema.json \
  -d /app/mysql_schemas/data.json \
  --policies /app/mysql_schemas/policies.json \
  --addr 0.0.0.0

# Wait for agent to be ready
echo -e "${YELLOW}Waiting for Cedar agent to be ready...${NC}"
MAX_RETRIES=30
RETRY_COUNT=0
while [ $RETRY_COUNT -lt $MAX_RETRIES ]; do
    if curl -s -f "http://localhost:${CEDAR_AGENT_PORT}/v1/" > /dev/null 2>&1; then
        echo -e "${GREEN}Cedar agent is ready!${NC}"
        echo "Health check: http://localhost:${CEDAR_AGENT_PORT}/v1/"
        echo "API endpoint: http://localhost:${CEDAR_AGENT_PORT}/v1"
        exit 0
    fi
    RETRY_COUNT=$((RETRY_COUNT + 1))
    sleep 1
done

echo -e "${RED}Error: Cedar agent failed to start within ${MAX_RETRIES} seconds${NC}"
echo "Check logs with: docker logs $CEDAR_AGENT_NAME"
exit 1

