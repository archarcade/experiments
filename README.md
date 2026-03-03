# ARCADE Experiment Artifact

Experiment framework for evaluating ARCADE: Retrofitting Databases with
Attribute-based Access Control using External Authorization Engines
(PVLDB Volume 19, VLDB 2026).

---

## Table of Contents

1. [Overview](#overview)
2. [Hardware Requirements](#hardware-requirements)
3. [Software Requirements](#software-requirements)
4. [Getting Started](#getting-started)
5. [Kick-the-Tires (15 minutes)](#kick-the-tires-15-minutes)
6. [Full Reproduction (4--6 hours)](#full-reproduction-4-6-hours)
7. [Individual Experiments](#individual-experiments)
8. [Expected Outputs](#expected-outputs)
9. [Documentation](#documentation)
10. [Troubleshooting](#troubleshooting)

---

## Overview

This artifact evaluates decoupled authorization for MySQL and PostgreSQL
via the Cedar policy decision point (PDP). It measures performance
overhead, scalability, and security guarantees of the ARCADE system.

**Key claims evaluated:**

| Claim | Experiment | Paper Element |
|-------|-----------|---------------|
| Per-query overhead 0--5.5% | E1 | Table 1 |
| Overhead dominated by `checking permissions` | E2 | Table 2 |
| Concurrency scales to 16 threads | E3 | Figures 3--4 |
| Policy scaling flat beyond 10 policies | E4 | Figure 6 |
| Agent handles ~1,000 RPS | E7 | Figure 5 |
| Network delay overhead additive | E7 | Table 3 |
| TPC-C overhead 1.3% (PG), 3.9% (MySQL) | E9 | Figure 7 |
| pgbench overhead 3.6% TPS | E11 | Table 4 |
| Zero fail-closed / monotonicity violations | E8 | Security text |

**Artifact structure:**
```
experiments/
├── cli.py              # Main CLI entry point
├── config.yaml         # Experiment configuration
├── Makefile            # Convenience targets
├── docker-compose.yml  # Container orchestration
├── framework/          # Python experiment framework
├── scripts/            # Setup, teardown, and reproduction scripts
├── context/            # Cedar schema and policies
└── docs/               # Detailed documentation
```

---

## Hardware Requirements

**Minimum (smoke test):**
- 4 CPU cores, 8 GB RAM, 20 GB disk

**Recommended (full reproduction):**
- 8+ CPU cores, 16+ GB RAM, 50 GB disk, SSD storage

**Ideal (low-noise measurements):**
- Dedicated machine, CPU governor set to "performance", no other workloads

---

## Software Requirements

- **OS**: Linux (Ubuntu 20.04+) or macOS 12+
- **Docker**: 20.10+ with Docker Compose v2
- **Python**: 3.10+ with `uv` package manager
- **sysbench**: 1.0+ (for E3 concurrency and E9 TPC-C)

### Installation

```bash
# Install uv (Python package manager)
curl -LsSf https://astral.sh/uv/install.sh | sh

# Install sysbench
# Ubuntu: apt install sysbench
# macOS:  brew install sysbench
```

---

## Getting Started

### 1. Install dependencies

```bash
make deps
# or: uv sync
```

### 2. Start containers

```bash
make containers
# or: docker compose up -d
```

### 3. Set up databases

```bash
make setup
# or step by step:
#   make setup-baseline
#   make setup-cedar
```

### 4. Verify setup

```bash
docker compose ps
curl http://localhost:8280/v1/
mysql -h 127.0.0.1 -P 13306 -u root -prootpass -e "SELECT 1"
mysql -h 127.0.0.1 -P 13307 -u root -prootpass -e "SELECT 1"
```

---

## Kick-the-Tires (15 minutes)

Quick verification that the artifact works:

```bash
make smoke
```

This runs a reduced version:
- 100 iterations (instead of 50,000)
- 3 runs (instead of 10)
- ~15 minutes total

**Success criteria:**
- All commands complete without errors
- `analysis/*/smoke/query_by_query_overhead.csv` contains overhead data
- Console output shows overhead percentages

---

## Full Reproduction (4--6 hours)

### One Command

```bash
make paper
```

This runs all experiments (E1--E11), analysis, visualization, and artifact
generation.

### Using the Reproduction Script

```bash
# Full reproduction with verification
scripts/reproduce_vldb_results.sh

# Skip setup (if already done)
scripts/reproduce_vldb_results.sh --skip-setup

# Individual paper elements
scripts/reproduce_vldb_results.sh TABLE1    # Table 1 only
scripts/reproduce_vldb_results.sh FIGURE7   # Figure 7 only
```

### Step by Step

```bash
make setup                 # environment setup
make bench-user-setup      # benchmark users
make workload              # generate workload
make e1-overhead           # Table 1  (~2 hours)
make e2-breakdown          # Table 2  (~30 min)
make e3-concurrency        # Figures 3--4 (~1 hour)
make e4-policy-scaling     # Figure 6 (~1 hour)
make e7-failure            # Figure 5 + Table 3 (~30 min)
make e8-semantics          # Security verification (~15 min)
make e9-tpcc               # Figure 7 (~1 hour)
make e11-pgbench           # Table 4 (~30 min)
make analyze viz           # analysis and visualization
make paper-artifacts       # TikZ-compatible CSVs
```

---

## Individual Experiments

| Target | Experiment | Time |
|--------|-----------|------|
| `make e1-overhead` | Per-query overhead (10 ABBA runs) | ~2h |
| `make e2-breakdown` | MySQL stage overhead breakdown | ~30m |
| `make e3-concurrency` | sysbench concurrency (1--16 threads) | ~1h |
| `make e4-policy-scaling` | Policy count scaling (1--10,000) | ~1h |
| `make e5-analytic` | Analytic / join-heavy queries | ~30m |
| `make e6-concurrency-contention` | Concurrency contention (1--32 threads) | ~1h |
| `make e7-failure` | Agent delay + stress test | ~30m |
| `make e8-semantics` | Fail-closed + monotonicity | ~15m |
| `make e9-tpcc` | TPC-C (MySQL + PostgreSQL) | ~1h |
| `make e10-ddl` | DDL operations testing | ~15m |
| `make e11-pgbench` | pgbench TPC-B | ~30m |

---

## Expected Outputs

### Paper Figure/Table Mapping

| Paper Element | Output File |
|---------------|-------------|
| Table 1 | `analysis/*/benchmark/query_by_query_overhead.tex` |
| Table 2 | `analysis/*/benchmark/profiling/mysql_perf_schema_diff.csv` |
| Figure 3 | `paper_artifacts/*/concurrency_comparison_str.csv` |
| Figure 4 | `paper_artifacts/*/concurrency_comparison_str.csv` |
| Figure 5 | `analysis/*/failure/agent_stress_comprehensive.png` |
| Table 3 | `analysis/*/failure/agent_delay_impact.tex` |
| Figure 6 | `paper_artifacts/*/policy_scaling_boxplot_stats.csv` |
| Figure 7 | `paper_artifacts/*/tpcc_summary.csv` |
| Table 4 | `analysis/*/pgbench/pgbench_summary.tex` |

### Verification

```bash
# Check all required outputs exist
make verify-paper

# Or use the detailed verification script
scripts/verify_paper_results.sh
```

### Copying Artifacts to the Paper

```bash
TAG=$(grep experiment_tag config.yaml | awk '{print $2}')
cp paper_artifacts/$TAG/concurrency_comparison_str.csv    ../vldb/paper/figures/data/
cp paper_artifacts/$TAG/tpcc_summary.csv                  ../vldb/paper/figures/data/
cp paper_artifacts/$TAG/policy_scaling_boxplot_stats.csv   ../vldb/paper/figures/data/
```

---

## Documentation

| Document | Description |
|----------|-------------|
| [docs/SETUP.md](docs/SETUP.md) | Experimental setup and methodology |
| [docs/PAPER_RESULTS.md](docs/PAPER_RESULTS.md) | Paper figures/tables to experiment mapping |
| [docs/EXPERIMENT_CATALOG.md](docs/EXPERIMENT_CATALOG.md) | Detailed experiment descriptions |
| [docs/AUTH_SPEC_FORMAT.md](docs/AUTH_SPEC_FORMAT.md) | Authorization specification format |
| [docs/CONFIG_GUIDE.md](docs/CONFIG_GUIDE.md) | Docker network configuration |
| [docs/FAILURE_RESILIENCE_SETUP.md](docs/FAILURE_RESILIENCE_SETUP.md) | Toxiproxy and Vegeta setup |

---

## Troubleshooting

### Container issues

```bash
docker compose logs mysql-baseline
docker compose logs mysql-cedar
docker compose logs cedar-agent
docker compose restart
```

### Connection refused

```bash
sleep 30
docker compose ps
docker compose logs mysql-cedar | tail -20
```

### Permission denied (Cedar)

```bash
make setup-cedar
curl http://localhost:8280/v1/policies | python3 -m json.tool
```

### Low reproducibility

If results vary significantly:
1. Ensure no other workloads on the machine
2. Set CPU governor: `cpupower frequency-set -g performance`
3. Increase iterations in `config.yaml`

### Memory issues

Reduce concurrency or memory limits in `docker-compose.yml`.
