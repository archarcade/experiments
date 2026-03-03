#!/bin/bash

# System Information Collection Script
# Collects comprehensive system information for experimental reproducibility
# Outputs: JSON and human-readable text formats
#
# Usage:
#   ./collect_system_info.sh [OUTPUT_DIR]
#
# Arguments:
#   OUTPUT_DIR  Optional. Base directory for output (default: experiments/results/system_info)
#               If provided, system info will be saved to OUTPUT_DIR/system_info/

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
EXPERIMENTS_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

# Parse arguments
if [ -n "$1" ]; then
    # Use provided output directory
    BASE_OUTPUT_DIR="$1"
    OUTPUT_DIR="$BASE_OUTPUT_DIR/system_info"
else
    # Default to experiments/results/system_info
    OUTPUT_DIR="$EXPERIMENTS_DIR/results/system_info"
fi

TIMESTAMP=$(date -u +"%Y-%m-%dT%H:%M:%SZ")

# Create output directory
mkdir -p "$OUTPUT_DIR"

JSON_FILE="$OUTPUT_DIR/system_info_${TIMESTAMP}.json"
TEXT_FILE="$OUTPUT_DIR/system_info_${TIMESTAMP}.txt"

# Initialize JSON structure
cat > "$JSON_FILE" <<EOF
{
  "timestamp": "$TIMESTAMP",
  "system": {},
  "hardware": {},
  "software": {},
  "docker": {},
  "network": {},
  "configuration": {}
}
EOF

# Function to safely get command output or return "N/A"
safe_exec() {
    local cmd="$1"
    local result
    result=$(eval "$cmd" 2>/dev/null || echo "N/A")
    echo "$result" | tr -d '\n' | sed 's/"/\\"/g'
}

# Function to get JSON value
get_json_value() {
    local key="$1"
    local value="$2"
    python3 -c "import json, sys; data=json.load(open('$JSON_FILE')); data['$key']='$value'; json.dump(data, open('$JSON_FILE', 'w'), indent=2)"
}

# Function to set nested JSON value
set_json_nested() {
    local section="$1"
    local key="$2"
    local value="$3"
    python3 <<PYTHON
import json
with open('$JSON_FILE', 'r') as f:
    data = json.load(f)
data['$section']['$key'] = '$value'
with open('$JSON_FILE', 'w') as f:
    json.dump(data, f, indent=2)
PYTHON
}

# Function to append to text file
append_text() {
    echo "$1" >> "$TEXT_FILE"
}

# Start text file
cat > "$TEXT_FILE" <<EOF
========================================
System Information Report
Generated: $TIMESTAMP
========================================

EOF

# ============================================
# HARDWARE INFORMATION
# ============================================
append_text "HARDWARE INFORMATION"
append_text "===================="

# CPU Information
if [[ "$OSTYPE" == "darwin"* ]]; then
    CPU_MODEL=$(sysctl -n machdep.cpu.brand_string 2>/dev/null || echo "N/A")
    CPU_CORES=$(sysctl -n hw.ncpu 2>/dev/null || echo "N/A")
    CPU_THREADS=$(sysctl -n hw.logicalcpu 2>/dev/null || echo "N/A")
    CPU_PHYSICAL=$(sysctl -n hw.physicalcpu 2>/dev/null || echo "N/A")
else
    CPU_MODEL=$(grep -m1 "model name" /proc/cpuinfo | cut -d: -f2 | sed 's/^ *//' || echo "N/A")
    CPU_CORES=$(nproc 2>/dev/null || grep -c "^processor" /proc/cpuinfo 2>/dev/null || echo "N/A")
    CPU_THREADS=$(nproc 2>/dev/null || echo "N/A")
    CPU_PHYSICAL=$(grep "physical id" /proc/cpuinfo | sort -u | wc -l 2>/dev/null || echo "N/A")
fi

append_text "CPU Model: $CPU_MODEL"
append_text "CPU Cores (Physical): $CPU_PHYSICAL"
append_text "CPU Cores (Logical): $CPU_CORES"
append_text "CPU Threads: $CPU_THREADS"
append_text ""

set_json_nested "hardware" "cpu_model" "$CPU_MODEL"
set_json_nested "hardware" "cpu_cores_physical" "$CPU_PHYSICAL"
set_json_nested "hardware" "cpu_cores_logical" "$CPU_CORES"
set_json_nested "hardware" "cpu_threads" "$CPU_THREADS"

# Memory Information
if [[ "$OSTYPE" == "darwin"* ]]; then
    MEM_TOTAL=$(sysctl -n hw.memsize 2>/dev/null | awk '{printf "%.2f", $1/1024/1024/1024}' || echo "N/A")
    MEM_TOTAL="${MEM_TOTAL} GB"
    MEM_AVAILABLE=$(vm_stat | grep "Pages free" | awk '{print $3}' | sed 's/\.//' | awk '{printf "%.2f", $1*4096/1024/1024/1024}' || echo "N/A")
    MEM_AVAILABLE="${MEM_AVAILABLE} GB"
else
    MEM_TOTAL=$(free -h | grep "Mem:" | awk '{print $2}' || echo "N/A")
    MEM_AVAILABLE=$(free -h | grep "Mem:" | awk '{print $7}' || echo "N/A")
fi

append_text "Memory Total: $MEM_TOTAL"
append_text "Memory Available: $MEM_AVAILABLE"
append_text ""

set_json_nested "hardware" "memory_total" "$MEM_TOTAL"
set_json_nested "hardware" "memory_available" "$MEM_AVAILABLE"

# Storage Information
if [[ "$OSTYPE" == "darwin"* ]]; then
    DISK_TOTAL=$(df -h / | tail -1 | awk '{print $2}' || echo "N/A")
    DISK_AVAILABLE=$(df -h / | tail -1 | awk '{print $4}' || echo "N/A")
    DISK_TYPE=$(diskutil info / | grep "Media Type" | cut -d: -f2 | sed 's/^ *//' || echo "N/A")
else
    DISK_TOTAL=$(df -h / | tail -1 | awk '{print $2}' || echo "N/A")
    DISK_AVAILABLE=$(df -h / | tail -1 | awk '{print $4}' || echo "N/A")
    DISK_TYPE=$(lsblk -d -o name,rota 2>/dev/null | grep -v "NAME" | head -1 | awk '{if ($2=="0") print "SSD"; else print "HDD"}' || echo "N/A")
fi

append_text "Disk Total: $DISK_TOTAL"
append_text "Disk Available: $DISK_AVAILABLE"
append_text "Disk Type: $DISK_TYPE"
append_text ""

set_json_nested "hardware" "disk_total" "$DISK_TOTAL"
set_json_nested "hardware" "disk_available" "$DISK_AVAILABLE"
set_json_nested "hardware" "disk_type" "$DISK_TYPE"

# ============================================
# SOFTWARE INFORMATION
# ============================================
append_text ""
append_text "SOFTWARE INFORMATION"
append_text "===================="

# Operating System
if [[ "$OSTYPE" == "darwin"* ]]; then
    OS_NAME=$(sw_vers -productName 2>/dev/null || echo "macOS")
    OS_VERSION=$(sw_vers -productVersion 2>/dev/null || echo "N/A")
    OS_BUILD=$(sw_vers -buildVersion 2>/dev/null || echo "N/A")
    KERNEL_VERSION=$(uname -r || echo "N/A")
else
    OS_NAME=$(lsb_release -si 2>/dev/null || cat /etc/os-release 2>/dev/null | grep "^NAME=" | cut -d= -f2 | tr -d '"' || echo "N/A")
    OS_VERSION=$(lsb_release -sr 2>/dev/null || cat /etc/os-release 2>/dev/null | grep "^VERSION_ID=" | cut -d= -f2 | tr -d '"' || echo "N/A")
    OS_BUILD="N/A"
    KERNEL_VERSION=$(uname -r || echo "N/A")
fi

append_text "OS Name: $OS_NAME"
append_text "OS Version: $OS_VERSION"
append_text "OS Build: $OS_BUILD"
append_text "Kernel Version: $KERNEL_VERSION"
append_text ""

set_json_nested "software" "os_name" "$OS_NAME"
set_json_nested "software" "os_version" "$OS_VERSION"
set_json_nested "software" "os_build" "$OS_BUILD"
set_json_nested "software" "kernel_version" "$KERNEL_VERSION"

# Docker Information
if command -v docker &> /dev/null; then
    DOCKER_VERSION=$(docker --version 2>/dev/null | cut -d' ' -f3 | tr -d ',' || echo "N/A")
    # Try docker compose (plugin version) - outputs format like "Docker Compose version v2.x.x"
    DOCKER_COMPOSE_VERSION=$(docker compose version 2>/dev/null | grep -oE 'v?[0-9]+\.[0-9]+\.[0-9]+' | head -1 || echo "N/A")
    # Fallback to docker-compose (standalone) - outputs format like "docker-compose version 1.29.2"
    if [[ "$DOCKER_COMPOSE_VERSION" == "N/A" ]] || [[ -z "$DOCKER_COMPOSE_VERSION" ]]; then
        DOCKER_COMPOSE_VERSION=$(docker-compose version 2>/dev/null | grep -oE '[0-9]+\.[0-9]+\.[0-9]+' | head -1 || echo "N/A")
    fi
    # Remove 'v' prefix if present for consistency
    DOCKER_COMPOSE_VERSION="${DOCKER_COMPOSE_VERSION#v}"
    append_text "Docker Version: $DOCKER_VERSION"
    append_text "Docker Compose Version: $DOCKER_COMPOSE_VERSION"
    set_json_nested "software" "docker_version" "$DOCKER_VERSION"
    set_json_nested "software" "docker_compose_version" "$DOCKER_COMPOSE_VERSION"
else
    append_text "Docker: Not installed"
    set_json_nested "software" "docker_version" "Not installed"
fi
append_text ""

# Python Information
if command -v python3 &> /dev/null; then
    PYTHON_VERSION=$(python3 --version 2>&1 | cut -d' ' -f2 || echo "N/A")
    PYTHON_PATH=$(which python3 || echo "N/A")
    append_text "Python Version: $PYTHON_VERSION"
    append_text "Python Path: $PYTHON_PATH"
    set_json_nested "software" "python_version" "$PYTHON_VERSION"
    set_json_nested "software" "python_path" "$PYTHON_PATH"
else
    append_text "Python3: Not installed"
    set_json_nested "software" "python_version" "Not installed"
fi
append_text ""

# MySQL Client Information
if command -v mysql &> /dev/null; then
    MYSQL_CLIENT_VERSION=$(mysql --version 2>&1 | grep -oP '\d+\.\d+\.\d+' | head -1 || echo "N/A")
    append_text "MySQL Client Version: $MYSQL_CLIENT_VERSION"
    set_json_nested "software" "mysql_client_version" "$MYSQL_CLIENT_VERSION"
else
    append_text "MySQL Client: Not installed"
    set_json_nested "software" "mysql_client_version" "Not installed"
fi
append_text ""

# MySQL Server Versions
append_text "MySQL Server Versions:"
MYSQL_BASELINE_VERSION="8.0.43"  # Baseline MySQL version (from mysql:8.0.43 image)
MYSQL_CEDAR_VERSION="N/A"

# Try to detect Cedar MySQL version if container is running
if command -v docker &> /dev/null && command -v mysql &> /dev/null; then
    # Check MySQL Cedar version (port 13307)
    if docker ps --filter "name=mysql-cedar" --format "{{.Names}}" 2>/dev/null | grep -q mysql-cedar; then
        # Try to get version from MySQL query
        MYSQL_CEDAR_VERSION=$(mysql -h127.0.0.1 -P13307 -uroot -prootpass -e "SELECT VERSION();" 2>/dev/null | tail -1 | grep -oP '\d+\.\d+\.\d+' | head -1 || echo "N/A")
        # Fallback: try to get from Docker image tag
        if [[ "$MYSQL_CEDAR_VERSION" == "N/A" ]]; then
            MYSQL_CEDAR_VERSION=$(docker inspect mysql-cedar --format='{{.Config.Image}}' 2>/dev/null | grep -oP '\d+\.\d+\.\d+' | head -1 || echo "N/A")
        fi
    fi
fi

append_text "  MySQL Baseline Server: $MYSQL_BASELINE_VERSION"
append_text "  MySQL Cedar Server: ${MYSQL_CEDAR_VERSION}"
set_json_nested "software" "mysql_baseline_server_version" "$MYSQL_BASELINE_VERSION"
set_json_nested "software" "mysql_cedar_server_version" "$MYSQL_CEDAR_VERSION"
append_text ""

# Bash Version
BASH_VERSION=$(bash --version | head -1 | grep -oP '\d+\.\d+\.\d+' | head -1 || echo "N/A")
append_text "Bash Version: $BASH_VERSION"
set_json_nested "software" "bash_version" "$BASH_VERSION"
append_text ""

# ============================================
# DOCKER CONTAINER INFORMATION
# ============================================
append_text ""
append_text "DOCKER CONTAINER INFORMATION"
append_text "============================="

if command -v docker &> /dev/null; then
    # Check for running containers
    RUNNING_CONTAINERS=$(docker ps --format "{{.Names}}" 2>/dev/null || echo "")
    
    if [[ -n "$RUNNING_CONTAINERS" ]]; then
        append_text "Running Containers:"
        while IFS= read -r container; do
            if [[ -n "$container" ]]; then
                CONTAINER_IMAGE=$(docker inspect "$container" --format='{{.Config.Image}}' 2>/dev/null || echo "N/A")
                CONTAINER_STATUS=$(docker inspect "$container" --format='{{.State.Status}}' 2>/dev/null || echo "N/A")
                append_text "  - $container (Image: $CONTAINER_IMAGE, Status: $CONTAINER_STATUS)"
            fi
        done <<< "$RUNNING_CONTAINERS"
        
        # Check for MySQL containers specifically
        MYSQL_BASELINE=$(docker ps --filter "name=mysql-baseline" --format "{{.Names}}" 2>/dev/null || echo "")
        MYSQL_CEDAR=$(docker ps --filter "name=mysql-cedar" --format "{{.Names}}" 2>/dev/null || echo "")
        CEDAR_AGENT=$(docker ps --filter "name=cedar-agent" --format "{{.Names}}" 2>/dev/null || echo "")
        
        append_text ""
        append_text "Experiment-related Containers:"
        append_text "  MySQL Baseline: ${MYSQL_BASELINE:-Not running}"
        append_text "  MySQL Cedar: ${MYSQL_CEDAR:-Not running}"
        append_text "  Cedar Agent: ${CEDAR_AGENT:-Not running}"
        
        set_json_nested "docker" "mysql_baseline_running" "$([ -n "$MYSQL_BASELINE" ] && echo "true" || echo "false")"
        set_json_nested "docker" "mysql_cedar_running" "$([ -n "$MYSQL_CEDAR" ] && echo "true" || echo "false")"
        set_json_nested "docker" "cedar_agent_running" "$([ -n "$CEDAR_AGENT" ] && echo "true" || echo "false")"
    else
        append_text "No running containers found"
        set_json_nested "docker" "containers_running" "false"
    fi
else
    append_text "Docker not available"
    set_json_nested "docker" "available" "false"
fi
append_text ""

# ============================================
# NETWORK INFORMATION
# ============================================
append_text ""
append_text "NETWORK INFORMATION"
append_text "==================="

# Network interfaces
if [[ "$OSTYPE" == "darwin"* ]]; then
    NETWORK_INTERFACES=$(ifconfig | grep -E "^[a-z]" | awk '{print $1}' | tr -d ':' || echo "N/A")
else
    NETWORK_INTERFACES=$(ip -o link show | awk -F': ' '{print $2}' || echo "N/A")
fi

append_text "Network Interfaces: $NETWORK_INTERFACES"

# Check if ports are in use
check_port() {
    local port=$1
    local name=$2
    if command -v lsof &> /dev/null; then
        if lsof -i :$port &>/dev/null; then
            append_text "  Port $port ($name): In use"
            set_json_nested "network" "port_${port}_in_use" "true"
        else
            append_text "  Port $port ($name): Available"
            set_json_nested "network" "port_${port}_in_use" "false"
        fi
    elif command -v netstat &> /dev/null; then
        if netstat -an | grep -q ":$port " 2>/dev/null; then
            append_text "  Port $port ($name): In use"
            set_json_nested "network" "port_${port}_in_use" "true"
        else
            append_text "  Port $port ($name): Available"
            set_json_nested "network" "port_${port}_in_use" "false"
        fi
    else
        append_text "  Port $port ($name): Cannot check"
        set_json_nested "network" "port_${port}_in_use" "unknown"
    fi
}

append_text ""
append_text "Port Status:"
check_port 13306 "MySQL Baseline"
check_port 13307 "MySQL Cedar"
check_port 8280 "Cedar Agent"

append_text ""

# ============================================
# SYSTEM CONFIGURATION
# ============================================
append_text ""
append_text "SYSTEM CONFIGURATION"
append_text "===================="

# CPU Governor (Linux only)
if [[ "$OSTYPE" != "darwin"* ]]; then
    if [[ -f /sys/devices/system/cpu/cpu0/cpufreq/scaling_governor ]]; then
        CPU_GOVERNOR=$(cat /sys/devices/system/cpu/cpu0/cpufreq/scaling_governor 2>/dev/null || echo "N/A")
        append_text "CPU Governor: $CPU_GOVERNOR"
        set_json_nested "configuration" "cpu_governor" "$CPU_GOVERNOR"
    else
        append_text "CPU Governor: Not available (may require root)"
        set_json_nested "configuration" "cpu_governor" "Not available"
    fi
else
    append_text "CPU Governor: N/A (macOS)"
    set_json_nested "configuration" "cpu_governor" "N/A (macOS)"
fi

# Timezone
TIMEZONE=$(date +%Z 2>/dev/null || echo "N/A")
append_text "Timezone: $TIMEZONE"
set_json_nested "configuration" "timezone" "$TIMEZONE"

# Locale
LOCALE=$(locale 2>/dev/null | grep "LANG=" | cut -d= -f2 | head -1 || echo "N/A")
append_text "Locale: $LOCALE"
set_json_nested "configuration" "locale" "$LOCALE"

# Ulimit settings
ULIMIT_FILES=$(ulimit -n 2>/dev/null || echo "N/A")
ULIMIT_PROCESSES=$(ulimit -u 2>/dev/null || echo "N/A")
append_text "Max Open Files: $ULIMIT_FILES"
append_text "Max Processes: $ULIMIT_PROCESSES"
set_json_nested "configuration" "ulimit_files" "$ULIMIT_FILES"
set_json_nested "configuration" "ulimit_processes" "$ULIMIT_PROCESSES"

append_text ""

# ============================================
# ENVIRONMENT VARIABLES (Experiment-related)
# ============================================
append_text ""
append_text "EXPERIMENT ENVIRONMENT VARIABLES"
append_text "================================"

ENV_VARS=("MYSQL_HOST" "MYSQL_PORT" "MYSQL_USER" "MYSQL_PASSWORD" "BASELINE_MYSQL_PASSWORD" "CEDAR_MYSQL_PASSWORD" "CEDAR_MYSQL_PORT" "CEDAR_AGENT_URL" "ITERATIONS" "AUTH_SPEC")

for var in "${ENV_VARS[@]}"; do
    value="${!var}"
    if [[ -n "$value" ]]; then
        # Mask passwords
        if [[ "$var" == *"PASSWORD"* ]]; then
            append_text "$var: [SET]"
            set_json_nested "configuration" "${var,,}" "[SET]"
        else
            append_text "$var: $value"
            set_json_nested "configuration" "${var,,}" "$value"
        fi
    else
        append_text "$var: [NOT SET]"
        set_json_nested "configuration" "${var,,}" "[NOT SET]"
    fi
done

append_text ""

# ============================================
# SUMMARY
# ============================================
append_text ""
append_text "========================================="
append_text "Collection Complete"
append_text "========================================="
append_text ""
append_text "JSON Output: $JSON_FILE"
append_text "Text Output: $TEXT_FILE"
append_text ""

# Create a symlink to latest
ln -sf "$(basename "$JSON_FILE")" "$OUTPUT_DIR/system_info_latest.json" 2>/dev/null || true
ln -sf "$(basename "$TEXT_FILE")" "$OUTPUT_DIR/system_info_latest.txt" 2>/dev/null || true

echo "System information collected successfully!"
echo "JSON: $JSON_FILE"
echo "Text: $TEXT_FILE"
echo ""
echo "Latest files:"
echo "  $OUTPUT_DIR/system_info_latest.json"
echo "  $OUTPUT_DIR/system_info_latest.txt"

