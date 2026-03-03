# Experiment Scripts

Helper scripts for setting up, running, and tearing down the ARCADE
experiment environment. For most workflows, use the Makefile targets
instead of running these scripts directly.

## Recommended Workflow

```bash
# Full setup via Makefile (preferred)
make setup          # deps + containers + baseline + cedar

# Full paper reproduction
make paper          # runs all experiments, analysis, visualization

# Teardown
make teardown       # stop containers
make clean-all      # stop containers + remove volumes
```

## Prerequisites

- Docker and Docker Compose v2
- Python 3.10+ with `uv` package manager
- MySQL command-line client (for setup scripts)
- sysbench 1.0+ (for E3 concurrency and E9 TPC-C)
- Vegeta (for E7 stress test) — install via `./install_vegeta.sh`
- Toxiproxy (for E7 delay injection) — see `docs/FAILURE_RESILIENCE_SETUP.md`

## Script Inventory

### Setup Scripts

| Script | Purpose | Makefile Equivalent |
|--------|---------|---------------------|
| `setup_all.sh` | Run both MySQL setups | `make setup-baseline setup-cedar` |
| `setup_baseline_mysql.sh` | Baseline MySQL with GRANTs | `make setup-baseline` |
| `setup_cedar_mysql.sh` | Cedar MySQL with plugins + policies | `make setup-cedar` |
| `setup_postgres.sh` | PostgreSQL setup instructions | (handled by docker-compose) |
| `start_cedar_agent.sh` | Start Cedar agent with schema | (handled by docker-compose) |

### Cleanup Scripts

| Script | Purpose | Makefile Equivalent |
|--------|---------|---------------------|
| `cleanup_all.sh` | Clean both MySQL setups | — |
| `cleanup_baseline_mysql.sh` | Remove baseline users/tables | — |
| `cleanup_cedar_mysql.sh` | Remove Cedar policies/entities/plugins | — |
| `stop_containers.sh` | Stop all experiment containers | `make teardown` |

### Experiment Scripts

| Script | Purpose | Makefile Equivalent |
|--------|---------|---------------------|
| `run_performance_overhead.sh` | Standalone per-query overhead test | `make e1-overhead` |
| `collect_system_info.sh` | Collect hardware/software info for reproducibility | — |
| `install_vegeta.sh` | Install Vegeta HTTP load tester | — |

### Paper Reproduction Scripts

| Script | Purpose |
|--------|---------|
| `reproduce_vldb_results.sh` | Reproduce all VLDB paper results (Tables 1--4, Figures 3--7) |
| `verify_paper_results.sh` | Verify all expected output files exist |

## Configuration

Scripts use environment variables for configuration. Default values match
the `docker-compose.yml` and `config.yaml` settings:

| Variable | Default | Description |
|----------|---------|-------------|
| `MYSQL_HOST` | `127.0.0.1` | MySQL host |
| `MYSQL_PORT` | `13306` (baseline), `13307` (cedar) | MySQL port |
| `MYSQL_USER` | `root` | MySQL user |
| `MYSQL_PASSWORD` | (from config.yaml) | MySQL password |
| `CEDAR_AGENT_URL` | `http://localhost:8280` | Cedar agent URL |

## Troubleshooting

### Cannot connect to MySQL
```bash
docker compose ps                              # check containers are running
docker compose logs mysql-baseline             # check baseline logs
docker compose logs mysql-cedar                # check cedar logs
mysql -h127.0.0.1 -P13306 -uroot -prootpass   # test baseline connection
mysql -h127.0.0.1 -P13307 -uroot -prootpass   # test cedar connection
```

### Cedar agent not accessible
```bash
docker compose ps                      # check cedar-agent is running
curl http://localhost:8280/v1/         # test health endpoint
docker compose logs cedar-agent        # check agent logs
```

### Permission denied errors
```bash
chmod +x *.sh                          # ensure scripts are executable
make setup-cedar                       # re-run cedar setup
curl http://localhost:8280/v1/policies | python3 -m json.tool  # check policies
```
