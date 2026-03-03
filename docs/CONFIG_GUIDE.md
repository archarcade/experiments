# Configuration Guide: Cedar Agent URL for Docker

## Problem

When MySQL runs inside a Docker container, it cannot access Cedar agent at `localhost:8280` because `localhost` inside the container refers to the container itself, not the host machine.

## Solution

The framework now supports separate URLs:
- **Host URL**: Used by the CLI (running on host) to communicate with Cedar agent
- **Container URL**: Used by MySQL plugins (inside container) to reach Cedar agent

## Configuration

### Option 1: Auto-detection (Recommended)

Leave `url_for_container` as `null` in your config. The framework will automatically detect:

1. **Docker Compose / Same Network**: If MySQL and Cedar agent are on the same Docker network, it uses `http://cedar-agent:8180`
2. **Docker Desktop (Mac/Windows)**: Falls back to `http://host.docker.internal:8280`
3. **Not in Docker**: Uses the host URL

### Option 2: Manual Configuration

Set `url_for_container` explicitly in your `config.yaml`:

```yaml
cedar_agent:
  url: http://localhost:8280  # For CLI (host access)
  url_for_container: http://cedar-agent:8180  # For MySQL container
```

## Example Configurations

### Docker Compose (Same Network)

```yaml
cedar_agent:
  url: http://localhost:8280
  url_for_container: null  # Auto-detects: http://cedar-agent:8180
```

### Docker Desktop (Mac/Windows)

```yaml
cedar_agent:
  url: http://localhost:8280
  url_for_container: http://host.docker.internal:8280
```

### Linux with Bridge Network

```yaml
cedar_agent:
  url: http://localhost:8280
  url_for_container: http://172.17.0.1:8280  # Or your host IP
```

### Not Using Docker

```yaml
cedar_agent:
  url: http://localhost:8280
  url_for_container: null  # Will use host URL
```

## Usage

1. **Copy the default config**:
   ```bash
   cp experiments/config.yaml my_config.yaml
   ```

2. **Edit if needed** (usually auto-detection works):
   ```yaml
   cedar_agent:
     url: http://localhost:8280
     url_for_container: null  # Auto-detect
   ```

3. **Run setup with config**:
   ```bash
   uv run python3 cli.py setup-cedar framework/auth_spec_example.json --config my_config.yaml
   ```

## How It Works

When you run `setup-cedar`:

1. The CLI detects if MySQL is running in a Docker container
2. If `url_for_container` is not set, it auto-detects:
   - Checks if `cedar-agent` container exists
   - Checks if they're on the same Docker network
   - Falls back to `host.docker.internal` for Docker Desktop
3. The detected URL is used to replace `localhost:8280` in `cedar_init.sql`
4. MySQL plugins are configured with the container-accessible URL

## Troubleshooting

### Entities not found (404 errors)

If you see warnings like:
```
Warning: Failed to set User user_alice.user_role: 404
```

This means MySQL container cannot reach Cedar agent. Check:

1. **Cedar agent is running**:
   ```bash
   docker ps | grep cedar-agent
   curl http://localhost:8280/v1/
   ```

2. **Containers are on same network** (if using docker-compose):
   ```bash
   docker network inspect mysql-experiments
   ```

3. **Test from MySQL container**:
   ```bash
   docker exec mysql-cedar curl -s http://cedar-agent:8180/v1/
   # Or if not on same network:
   docker exec mysql-cedar curl -s http://host.docker.internal:8280/v1/
   ```

4. **Check MySQL plugin configuration**:
   ```bash
   docker exec mysql-cedar mysql -uroot -e "SHOW VARIABLES LIKE 'ddl_audit%';"
   docker exec mysql-cedar mysql -uroot -e "SHOW VARIABLES LIKE 'cedar_authorization%';"
   ```

The `ddl_audit_cedar_url` should be the container-accessible URL (not `localhost:8280`).

