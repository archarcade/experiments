# Experimental Setup and Methodology

This document describes the experimental setup and statistical methodology used
in the ARCADE evaluation (PVLDB Volume 19, VLDB 2026).

## Test Environment

**Hardware** (as used in the paper):
- CPU: 8-core hyper-threaded Intel Xeon (Skylake)
- RAM: 32 GB
- OS: Ubuntu 18.04

**Software**:
- MySQL 8.0.43 (with Cedar authorization plugin)
- PostgreSQL 17 (with `pg_authorization` extension)
- Cedar agent (Docker, release mode)
- sysbench 1.0+ (concurrency and TPC-C benchmarks)
- pgbench (PostgreSQL TPC-B benchmark)
- Toxiproxy (fault injection)
- Vegeta (HTTP stress testing)

**Deployment**: All components (MySQL, PostgreSQL, Cedar agent) run as Docker
containers on the same host. This co-located deployment matches the recommended
production configuration.

## Workloads

| Workload | Tool | Description |
|----------|------|-------------|
| Custom ABAC | `cli.py` | 60% SELECT, 15% INSERT, 15% UPDATE, 10% DELETE |
| TPC-C | `sysbench-tpcc` | Industry-standard OLTP (10 warehouses, 16 threads) |
| TPC-B | `pgbench` | PostgreSQL standard (scale=20, 4 clients) |

The custom ABAC workload exercises per-query authorization hooks. Queries are
executed using per-query principals to ensure Cedar authorization is invoked on
every query.

## Statistical Methodology

| Parameter | Value |
|-----------|-------|
| Independent runs | 10 per configuration |
| Ordering | ABBA (alternating baseline-Cedar pairs) |
| Reported statistic | Median |
| Confidence intervals | 95% bootstrap (10,000 iterations) |
| Significance test | Wilcoxon signed-rank (alpha = 0.05) |
| Effect size | Cliff's delta |
| Multiple comparison correction | Bonferroni |

Latency is reported as percentiles (p50, p95, p99) to characterize both
typical and tail behavior.

## Baseline Comparison

Each configuration is compared against unmodified databases with semantically
equivalent GRANT-based permissions. The baseline represents the best-case
native authorization overhead.

## Reproducibility Notes

- All experiments run on warm systems with no other user workloads.
- CPU frequency governor should be set to "performance" for low-noise results.
- Fixed random seeds ensure deterministic workload generation.
- Docker images are pinned to specific versions for reproducibility.
- Results directories are organized by experiment tag (configured in `config.yaml`).

## Quick Reference

```bash
# Full setup
make setup

# Quick verification (~15 min)
make smoke

# Full paper reproduction (~4-6 hours)
make paper

# Analyze and visualize
make analyze viz

# Generate paper-ready artifacts
make paper-artifacts
```

See the [main README](../README.md) for detailed instructions and
[EXPERIMENT_CATALOG.md](EXPERIMENT_CATALOG.md) for per-experiment details.
