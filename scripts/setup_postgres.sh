#!/bin/bash
# Setup script for PostgreSQL benchmarking environment
# This script provides instructions for building and using PostgreSQL images

set -e

POSTGRES_SRC="${POSTGRES_SRC_DIR:-}"

echo "PostgreSQL Setup Instructions"
echo "============================="
echo ""

if [ -n "$POSTGRES_SRC" ] && [ -d "$POSTGRES_SRC" ]; then
    echo "✓ Found PostgreSQL source directory: $POSTGRES_SRC"
    echo ""

    echo "To build and push PostgreSQL images to your registry:"
    echo ""

    echo "1. Baseline PostgreSQL: Use official image (no build needed)"
    echo "   # Just use postgres:17.7 directly"
    echo ""

    echo "2. Build Cedar PostgreSQL image:"
    echo "   cd $POSTGRES_SRC"
    echo "   export REGISTRY=your-registry.com"
    echo "   export TAG=latest"
    echo "   ./build_and_push.sh"
    echo ""

    echo "3. Set environment variables for docker-compose:"
    echo "   export POSTGRES_CEDAR_IMAGE=your-registry.com/postgres-cedar:latest"
    echo ""

else
    echo "PostgreSQL source directory not set or not found."
    echo ""
    echo "Set POSTGRES_SRC_DIR to the path of the PostgreSQL source with Cedar hooks:"
    echo "   export POSTGRES_SRC_DIR=/path/to/postgres-cedar-auth"
    echo "   $0"
    echo ""
    echo "Or use the pre-built Docker image directly:"
    echo "   docker pull ghcr.io/archarcade/postgres-cedar:latest"
    exit 1
fi

echo "PostgreSQL setup instructions complete!"
echo ""
echo "After building and pushing images, you can:"
echo ""
echo "1. Set environment variables:"
echo "   export POSTGRES_BASELINE_IMAGE=your-registry.com/postgres-baseline:latest"
echo "   export POSTGRES_CEDAR_IMAGE=your-registry.com/postgres-cedar:latest"
echo ""
echo "2. Start PostgreSQL services:"
echo "   docker compose up postgres-baseline postgres-cedar -d"
echo ""
echo "3. Run pgbench tests:"
echo "   make e8-pgbench-baseline  # Test baseline PostgreSQL"
echo "   make e8-pgbench-cedar     # Test Cedar PostgreSQL"
echo "   make e8-pgbench-compare   # Compare both systems"
