#!/bin/bash
# Standalone performance overhead experiment (E1 equivalent)
# Measures end-to-end overhead for different SQL operation types
# For the primary workflow, use: make e1-overhead

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
EXPERIMENTS_DIR="$(dirname "$SCRIPT_DIR")"
FRAMEWORK_DIR="$EXPERIMENTS_DIR/framework"
RESULTS_DIR="$EXPERIMENTS_DIR/results/performance_overhead"
QUERIES_DIR="$EXPERIMENTS_DIR/queries"

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
ITERATIONS="${ITERATIONS:-100}"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Create results directory
mkdir -p "$RESULTS_DIR"

echo -e "${GREEN}=== Performance Overhead Experiment ===${NC}"
echo "Baseline MySQL: ${BASELINE_MYSQL_HOST}:${BASELINE_MYSQL_PORT}"
echo "Cedar MySQL: ${CEDAR_MYSQL_HOST}:${CEDAR_MYSQL_PORT}"
echo "Iterations: ${ITERATIONS}"
echo ""

# Function to run query and measure latency
run_query() {
    local mysql_host=$1
    local mysql_port=$2
    local mysql_user=$3
    local mysql_password=$4
    local query=$5
    local output_file=$6
    
    # Build mysql command
    local mysql_cmd="mysql -h${mysql_host} -P${mysql_port} -u${mysql_user}"
    if [ -n "$mysql_password" ]; then
        mysql_cmd="$mysql_cmd -p${mysql_password}"
    fi
    
    # Measure time using Python for precision
    cd "$EXPERIMENTS_DIR" && uv run python3 <<EOF
import subprocess
import time
import sys
import os

# Add framework directory to path
framework_dir = "$FRAMEWORK_DIR"
if framework_dir not in sys.path:
    sys.path.insert(0, framework_dir)

mysql_cmd = "$mysql_cmd".split()
query = """$query"""

times = []
for i in range($ITERATIONS):
    start = time.perf_counter()
    try:
        result = subprocess.run(
            mysql_cmd,
            input=query.encode(),
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            timeout=30
        )
        elapsed = (time.perf_counter() - start) * 1000  # Convert to ms
        if result.returncode == 0:
            times.append(elapsed)
        else:
            error_msg = result.stderr.decode()
            # Ignore "duplicate key" errors for INSERT operations (expected behavior)
            if "Duplicate entry" not in error_msg:
                print(f"Query failed: {error_msg}", file=sys.stderr)
                sys.exit(1)
    except subprocess.TimeoutExpired:
        print(f"Query timed out on iteration {i}", file=sys.stderr)
        sys.exit(1)

# Write results
with open("$output_file", "w") as f:
    for t in times:
        f.write(f"{t}\n")

# Print statistics
times.sort()
n = len(times)
median = times[n//2] if n > 0 else 0
p95 = times[int(n * 0.95)] if n > 0 else 0
p99 = times[int(n * 0.99)] if n > 0 else 0
mean = sum(times) / n if n > 0 else 0

print(f"Mean: {mean:.2f}ms, Median: {median:.2f}ms, p95: {p95:.2f}ms, p99: {p99:.2f}ms")
EOF
}

# Function to test operation type
test_operation() {
    local operation_name=$1
    local query_file=$2
    
    echo -e "${YELLOW}Testing: ${operation_name}${NC}"
    
    # Read query from file
    if [ ! -f "$query_file" ]; then
        echo -e "${RED}Error: Query file not found: ${query_file}${NC}"
        return 1
    fi
    local query=$(cat "$query_file")
    
    # Test baseline MySQL
    echo "  Running against baseline MySQL..."
    local baseline_file="${RESULTS_DIR}/baseline_${operation_name}.csv"
    run_query "$BASELINE_MYSQL_HOST" "$BASELINE_MYSQL_PORT" "$BASELINE_MYSQL_USER" "$BASELINE_MYSQL_PASSWORD" "$query" "$baseline_file" > "${RESULTS_DIR}/baseline_${operation_name}_stats.txt"
    local baseline_stats=$(cat "${RESULTS_DIR}/baseline_${operation_name}_stats.txt")
    echo "    Baseline: $baseline_stats"
    
    # Test Cedar MySQL
    echo "  Running against Cedar MySQL..."
    local cedar_file="${RESULTS_DIR}/cedar_${operation_name}.csv"
    run_query "$CEDAR_MYSQL_HOST" "$CEDAR_MYSQL_PORT" "$CEDAR_MYSQL_USER" "$CEDAR_MYSQL_PASSWORD" "$query" "$cedar_file" > "${RESULTS_DIR}/cedar_${operation_name}_stats.txt"
    local cedar_stats=$(cat "${RESULTS_DIR}/cedar_${operation_name}_stats.txt")
    echo "    Cedar: $cedar_stats"
    
    echo ""
}

# Check prerequisites
echo -e "${YELLOW}Checking prerequisites...${NC}"

# Check if MySQL clients are accessible
if ! command -v mysql &> /dev/null; then
    echo -e "${RED}Error: mysql client not found${NC}"
    exit 1
fi

# Check if Python3 is available
if ! command -v python3 &> /dev/null; then
    echo -e "${RED}Error: python3 not found${NC}"
    exit 1
fi

# Test MySQL connections
echo "Testing MySQL connections..."
if ! mysql -h"$BASELINE_MYSQL_HOST" -P"$BASELINE_MYSQL_PORT" -u"$BASELINE_MYSQL_USER" ${BASELINE_MYSQL_PASSWORD:+-p"$BASELINE_MYSQL_PASSWORD"} -e "SELECT 1" &>/dev/null; then
    echo -e "${RED}Error: Cannot connect to baseline MySQL${NC}"
    exit 1
fi

if ! mysql -h"$CEDAR_MYSQL_HOST" -P"$CEDAR_MYSQL_PORT" -u"$CEDAR_MYSQL_USER" ${CEDAR_MYSQL_PASSWORD:+-p"$CEDAR_MYSQL_PASSWORD"} -e "SELECT 1" &>/dev/null; then
    echo -e "${RED}Error: Cannot connect to Cedar MySQL${NC}"
    exit 1
fi

# Verify connections are correct by checking for Cedar plugins
echo "Verifying MySQL instance identities..."
BASELINE_PLUGINS=$(mysql -h"$BASELINE_MYSQL_HOST" -P"$BASELINE_MYSQL_PORT" -u"$BASELINE_MYSQL_USER" ${BASELINE_MYSQL_PASSWORD:+-p"$BASELINE_MYSQL_PASSWORD"} -N -s -e "SELECT COUNT(*) FROM INFORMATION_SCHEMA.PLUGINS WHERE PLUGIN_NAME IN ('ddl_audit', 'cedar_authorization');" 2>/dev/null || echo "0")
CEDAR_PLUGINS=$(mysql -h"$CEDAR_MYSQL_HOST" -P"$CEDAR_MYSQL_PORT" -u"$CEDAR_MYSQL_USER" ${CEDAR_MYSQL_PASSWORD:+-p"$CEDAR_MYSQL_PASSWORD"} -N -s -e "SELECT COUNT(*) FROM INFORMATION_SCHEMA.PLUGINS WHERE PLUGIN_NAME IN ('ddl_audit', 'cedar_authorization');" 2>/dev/null || echo "0")

if [ "$BASELINE_PLUGINS" -gt 0 ]; then
    echo -e "${RED}WARNING: Baseline MySQL (${BASELINE_MYSQL_HOST}:${BASELINE_MYSQL_PORT}) appears to have Cedar plugins installed!${NC}"
    echo "  This suggests the connections might be swapped."
    echo "  Found $BASELINE_PLUGINS Cedar plugin(s) in baseline instance."
fi

if [ "$CEDAR_PLUGINS" -eq 0 ]; then
    echo -e "${RED}WARNING: Cedar MySQL (${CEDAR_MYSQL_HOST}:${CEDAR_MYSQL_PORT}) does not appear to have Cedar plugins installed!${NC}"
    echo "  This suggests the connections might be swapped or Cedar is not properly configured."
    echo "  Found $CEDAR_PLUGINS Cedar plugin(s) in Cedar instance."
fi

if [ "$BASELINE_PLUGINS" -eq 0 ] && [ "$CEDAR_PLUGINS" -gt 0 ]; then
    echo -e "${GREEN}✓ Connection verification passed: Baseline has no Cedar plugins, Cedar has Cedar plugins${NC}"
else
    echo -e "${YELLOW}⚠ Connection verification warning: Please verify the MySQL instances are correctly configured${NC}"
fi

echo -e "${GREEN}Prerequisites OK${NC}"
echo ""

# Create queries directory if it doesn't exist
mkdir -p "$QUERIES_DIR"

# Generate queries dynamically using the framework
echo -e "${YELLOW}Generating dynamic queries...${NC}"
cd "$EXPERIMENTS_DIR" && uv run python3 <<PYTHON_EOF
import sys
import os

# Add framework directory to path
framework_dir = "$FRAMEWORK_DIR"
if framework_dir not in sys.path:
    sys.path.insert(0, framework_dir)

from query_generator import get_query_generator

# Initialize query generator with a fixed seed for reproducibility
qgen = get_query_generator(seed=42)

# Generate queries dynamically
queries = {
    'simple_select': qgen.generate_select_query('abac_test.employees', limit=1),
    'select_join': qgen.generate_select_query('abac_test.employees', limit=1, 
                                              with_join='abac_test.projects'),
    'insert': qgen.generate_insert_query('abac_test.employees', cleanup=True),
    'update': qgen.generate_update_query('abac_test.employees', ensure_exists=True),
    'delete': qgen.generate_delete_query('abac_test.employees', ensure_exists=True),
    'create_table': qgen.generate_ddl_query('abac_test.test_table', if_not_exists=True)
}

# Write queries to files
queries_dir = "$QUERIES_DIR"
for op_name, query in queries.items():
    query_file = os.path.join(queries_dir, f"{op_name}.sql")
    with open(query_file, 'w') as f:
        f.write(query)
    print(f"Generated {op_name}.sql")

PYTHON_EOF

# Test each operation type
echo -e "${GREEN}Starting performance tests...${NC}"
echo ""

# Simple SELECT
test_operation "simple_select" "$QUERIES_DIR/simple_select.sql"

# SELECT with JOIN
test_operation "select_join" "$QUERIES_DIR/select_join.sql"

# INSERT
test_operation "insert" "$QUERIES_DIR/insert.sql"

# UPDATE
test_operation "update" "$QUERIES_DIR/update.sql"

# DELETE
test_operation "delete" "$QUERIES_DIR/delete.sql"

# CREATE TABLE (DDL)
test_operation "create_table" "$QUERIES_DIR/create_table.sql"

# Generate summary report
echo -e "${GREEN}Generating summary report...${NC}"
cd "$EXPERIMENTS_DIR" && uv run python3 <<EOF
import os
import csv
import statistics

results_dir = "$RESULTS_DIR"
operations = ["simple_select", "select_join", "insert", "update", "delete", "create_table"]

print("\n" + "="*80)
print("PERFORMANCE OVERHEAD SUMMARY")
print("="*80)
print(f"{'Operation':<20} {'Baseline (ms)':<15} {'Cedar (ms)':<15} {'Overhead (ms)':<15} {'Overhead (%)':<15}")
print("-"*80)

summary_data = []

for op in operations:
    baseline_file = os.path.join(results_dir, f"baseline_{op}.csv")
    cedar_file = os.path.join(results_dir, f"cedar_{op}.csv")
    
    if not os.path.exists(baseline_file) or not os.path.exists(cedar_file):
        continue
    
    # Read latencies
    with open(baseline_file) as f:
        baseline_times = [float(line.strip()) for line in f if line.strip()]
    with open(cedar_file) as f:
        cedar_times = [float(line.strip()) for line in f if line.strip()]
    
    if not baseline_times or not cedar_times:
        continue
    
    baseline_median = statistics.median(baseline_times)
    cedar_median = statistics.median(cedar_times)
    overhead_abs = cedar_median - baseline_median
    overhead_pct = (overhead_abs / baseline_median * 100) if baseline_median > 0 else 0
    
    print(f"{op:<20} {baseline_median:<15.2f} {cedar_median:<15.2f} {overhead_abs:<15.2f} {overhead_pct:<15.2f}")
    
    summary_data.append({
        'operation': op,
        'baseline_median': baseline_median,
        'cedar_median': cedar_median,
        'overhead_abs': overhead_abs,
        'overhead_pct': overhead_pct
    })

# Write summary CSV
summary_file = os.path.join(results_dir, "summary.csv")
with open(summary_file, 'w', newline='') as f:
    writer = csv.DictWriter(f, fieldnames=['operation', 'baseline_median', 'cedar_median', 'overhead_abs', 'overhead_pct'])
    writer.writeheader()
    writer.writerows(summary_data)

print("-"*80)
print(f"\nDetailed results saved to: {results_dir}")
print(f"Summary CSV: {summary_file}")
EOF

echo ""
echo -e "${GREEN}Experiment complete!${NC}"
echo "Results saved to: $RESULTS_DIR"

