# Failure Resilience Experiments Setup Guide

This guide covers the setup and prerequisites for running the failure
resilience experiments (E7 and E8). These experiments produce Figure 5,
Table 3, and the security verification results in the paper.

## Prerequisites

The failure resilience experiments require two additional tools:

1. **Toxiproxy** - For injecting network latency and simulating agent unavailability
2. **Vegeta** - For stress testing the Cedar agent

## Installing Toxiproxy

Toxiproxy is a TCP proxy for simulating network conditions. It's used to inject delays and simulate failures.

### Option 1: Using Docker (Recommended)

The easiest way to run Toxiproxy is using Docker:

```bash
docker run -d --name toxiproxy -p 8474:8474 ghcr.io/shopify/toxiproxy:latest
```

This will:
- Start Toxiproxy in detached mode (`-d`)
- Expose the control API on port 8474 (default)
- Use the official Shopify Toxiproxy image

**Verify it's running:**
```bash
docker ps | grep toxiproxy
curl http://localhost:8474/version
```

**Stop Toxiproxy:**
```bash
docker stop toxiproxy
docker rm toxiproxy
```

### Option 2: Using Binary

1. **Download Toxiproxy:**
   - Visit: https://github.com/shopify/toxiproxy/releases
   - Download the appropriate binary for your platform (Linux, macOS, Windows)

2. **Install:**
   ```bash
   # For Linux (example)
   wget https://github.com/shopify/toxiproxy/releases/download/v2.5.0/toxiproxy-linux-amd64.tar.gz
   tar -xzf toxiproxy-linux-amd64.tar.gz
   sudo mv toxiproxy /usr/local/bin/
   ```

3. **Run Toxiproxy server:**
   ```bash
   toxiproxy-server
   ```

   The server will start on port 8474 by default.

**Verify it's running:**
```bash
curl http://localhost:8474/version
```

### Option 3: Using Package Manager

**Ubuntu/Debian:**
```bash
# Add Toxiproxy repository (if available)
# Or download from releases page
```

**macOS (Homebrew):**
```bash
brew install toxiproxy
toxiproxy-server
```

## Installing Vegeta

Vegeta is an HTTP load testing tool used to stress test the Cedar agent.

### Option 1: Using Installation Script (Recommended for Debian/Ubuntu)

A convenient installation script is provided that automatically downloads and installs the latest version:

```bash
cd experiments/scripts
./install_vegeta.sh
```

This script will:
- Detect your system architecture (amd64 or arm64)
- Fetch the latest Vegeta version from GitHub
- Download and install Vegeta to `/usr/local/bin`
- Verify the installation

### Option 2: Using Package Manager

**Ubuntu/Debian:**
```bash
sudo apt-get update
sudo apt-get install vegeta
```

**macOS (Homebrew):**
```bash
brew install vegeta
```

**Arch Linux:**
```bash
sudo pacman -S vegeta
```

### Option 3: Using Binary (Manual Installation)

1. **Download Vegeta:**
   - Visit: https://github.com/tsenart/vegeta/releases
   - Download the appropriate binary for your platform

2. **Install:**
   ```bash
   # For Linux (example)
   wget https://github.com/tsenart/vegeta/releases/download/v12.11.1/vegeta-12.11.1-linux-amd64.tar.gz
   tar -xzf vegeta-12.11.1-linux-amd64.tar.gz
   sudo mv vegeta /usr/local/bin/
   ```

   Or using the automated method from the latest release:
   ```bash
   VEGETA_VERSION=$(curl -s "https://api.github.com/repos/tsenart/vegeta/releases/latest" | grep -Po '"tag_name": "v\K[0-9.]+')
   curl -Lo vegeta.tar.gz "https://github.com/tsenart/vegeta/releases/latest/download/vegeta_${VEGETA_VERSION}_linux_amd64.tar.gz"
   mkdir vegeta-temp
   tar xf vegeta.tar.gz -C vegeta-temp
   sudo mv vegeta-temp/vegeta /usr/local/bin
   rm -rf vegeta.tar.gz vegeta-temp
   ```

### Option 4: Using Go

If you have Go installed:
```bash
go install github.com/tsenart/vegeta/v12@latest
```

**Verify installation:**
```bash
vegeta --version
```

## Configuration

### Toxiproxy Configuration

The experiments use Toxiproxy to proxy requests to the Cedar agent. Ensure your `config.yaml` has the correct settings:

```yaml
failure_tests:
  proxy:
    enabled: true
    name: "cedar_agent_proxy"
    host: "127.0.0.1"
    listen_port: 8182  # Port where proxy listens (different from control API)
    control_api: "http://127.0.0.1:8474"  # Toxiproxy control API
    upstream_host: "127.0.0.1"
    upstream_port: 8181  # Actual Cedar agent port
```

**Important Notes:**
- `control_api` (port 8474) is where Toxiproxy's HTTP API runs
- `listen_port` should be different from the control API port (e.g., 8182)
- `upstream_port` is the actual Cedar agent port
- MySQL should connect to the proxy's `listen_port`, not directly to the agent

### Updating MySQL Configuration

After setting up the Toxiproxy proxy, you need to configure MySQL to use the proxy instead of connecting directly to the Cedar agent.

**If using Docker:**
Update the Cedar MySQL container's plugin configuration to point to the proxy:
```sql
SET GLOBAL ddl_audit_cedar_url = 'http://host.docker.internal:8182';
```

**If running locally:**
```sql
SET GLOBAL ddl_audit_cedar_url = 'http://127.0.0.1:8182';
```

## Running the Experiments

Once Toxiproxy and Vegeta are installed:

1. **Start Toxiproxy:**
   ```bash
   docker run -d --name toxiproxy -p 8474:8474 ghcr.io/shopify/toxiproxy:latest
   ```

2. **Verify Toxiproxy is accessible:**
   ```bash
   curl http://localhost:8474/version
   ```

3. **Run the experiments:**
   ```bash
   # Via Makefile (recommended):
   make e7-failure     # Agent delay benchmark + agent stress test
   make e8-semantics   # Fail-closed and monotonicity verification

   # Or individually via CLI:
   uv run python cli.py failure agent-delay-benchmark --config config.yaml
   uv run python cli.py failure agent-stress-test --config config.yaml
   uv run python cli.py failure agent-unavailability-test --config config.yaml
   uv run python cli.py failure mysql-under-stress --config config.yaml --rps 400
   ```

## Troubleshooting

### Toxiproxy Connection Refused

**Error:** `Connection refused` when trying to connect to Toxiproxy

**Solution:**
1. Verify Toxiproxy is running:
   ```bash
   docker ps | grep toxiproxy
   # or
   curl http://localhost:8474/version
   ```

2. Check if port 8474 is accessible:
   ```bash
   netstat -tuln | grep 8474
   # or
   ss -tuln | grep 8474
   ```

3. If using Docker, ensure the port is mapped correctly:
   ```bash
   docker run -d --name toxiproxy -p 8474:8474 ghcr.io/shopify/toxiproxy:latest
   ```

### Vegeta Not Found

**Error:** `vegeta: command not found`

**Solution:**
1. Verify Vegeta is installed:
   ```bash
   which vegeta
   vegeta --version
   ```

2. If not installed, follow the installation steps above

3. Ensure Vegeta is in your PATH:
   ```bash
   echo $PATH | grep -q /usr/local/bin || export PATH=$PATH:/usr/local/bin
   ```

### Proxy Creation Fails

**Error:** `Could not create toxiproxy proxy`

**Possible causes:**
1. Toxiproxy is not running
2. Port conflict (listen_port already in use)
3. Incorrect control_api URL in config

**Solution:**
1. Check if the listen_port is already in use:
   ```bash
   lsof -i :8182
   # or
   netstat -tuln | grep 8182
   ```

2. Change `listen_port` in config.yaml to an unused port

3. Verify the control_api URL is correct (should be `http://127.0.0.1:8474`)

### MySQL Can't Reach Proxy

**Error:** MySQL queries fail when proxy is enabled

**Solution:**
1. Ensure MySQL is configured to use the proxy URL (not direct agent URL)
2. If using Docker, use `host.docker.internal` instead of `127.0.0.1`
3. Verify the proxy is listening on the correct port:
   ```bash
   curl http://localhost:8182/v1/is_authorized
   ```

## Additional Resources

- **Toxiproxy Documentation:** https://github.com/shopify/toxiproxy
- **Vegeta Documentation:** https://github.com/tsenart/vegeta
- **Toxiproxy API Reference:** https://github.com/shopify/toxiproxy#http-api

## Quick Reference

**Start Toxiproxy:**
```bash
docker run -d --name toxiproxy -p 8474:8474 ghcr.io/shopify/toxiproxy:latest
```

**Check Toxiproxy:**
```bash
curl http://localhost:8474/version
```

**List proxies:**
```bash
curl http://localhost:8474/proxies
```

**Test Vegeta:**
```bash
echo "GET http://localhost:8181/v1/is_authorized" | vegeta attack -rate=10 -duration=1s | vegeta report
```

