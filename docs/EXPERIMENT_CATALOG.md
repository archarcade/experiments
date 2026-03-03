# Experiment Catalog

This document enumerates every experiment implemented in `experiments/`,
explains what each experiment measures, how it maps to the VLDB paper
(ARCADE, PVLDB Vol. 19), and what outputs it produces.

See [PAPER_RESULTS.md](PAPER_RESULTS.md) for the mapping from paper
figures/tables to experiments.

---

## Quick Reference

| ID | Experiment | Makefile Target | Paper Element |
|----|-----------|-----------------|---------------|
| E1 | Per-query overhead | `make e1-overhead` | Table 1 |
| E2 | Overhead breakdown | `make e2-breakdown` | Table 2 |
| E3 | Concurrency scaling | `make e3-concurrency` | Figures 3--4 |
| E4 | Policy scaling | `make e4-policy-scaling` | Figure 6 |
| E5 | Analytic queries | `make e5-analytic` | (supporting) |
| E6 | Concurrency contention | `make e6-concurrency-contention` | (supporting) |
| E7 | Failure resilience | `make e7-failure` | Figure 5, Table 3 |
| E8 | Semantic correctness | `make e8-semantics` | Security verification |
| E9 | TPC-C macrobenchmark | `make e9-tpcc` | Figure 7 |
| E10 | DDL operations | `make e10-ddl` | (supporting) |
| E11 | pgbench (TPC-B) | `make e11-pgbench` | Table 4 |

**Suites**:
- `make smoke` — quick verification (~15 min)
- `make paper` — full evaluation (~4--6 hours)
- `make reproduce` — full run + analysis + visualization + report

---

## E1 — Per-Query Overhead (Microbenchmark)

**Paper element**: Table 1 (per-operation latency overhead)

**What it measures**: End-to-end latency per SQL operation type (SELECT,
INSERT, UPDATE, DELETE, JOIN), comparing baseline MySQL (GRANTs) against
Cedar-enabled MySQL across 10 ABBA-ordered runs.

**Why it matters**: This is the primary overhead claim. Without E1, the
per-query cost of decoupled authorization cannot be quantified.

**How to run**:
```bash
make workload
make e1-overhead
```
Or directly:
```bash
uv run python cli.py generate-workload --config config.yaml
uv run python cli.py multi-run benchmark --config config.yaml --ordering abba
```

**Outputs**:
- `analysis/<tag>/benchmark/query_by_query_overhead.{csv,tex}`
- `analysis/<tag>/benchmark/latency_cdf.png`

**Expected results**: Overhead of 0--5.5% per operation, not statistically
significant at alpha=0.05. Authorization cost ~0.06 ms per query.

---

## E2 — Overhead Breakdown

**Paper element**: Table 2 (MySQL stage overhead breakdown)

**What it measures**: Uses MySQL Performance Schema to attribute overhead
to internal stages (`checking permissions`, `starting`, `waiting for
handler commit`, etc.).

**Why it matters**: Reviewers require attribution — does overhead come
from the plugin, the network call, or the agent evaluation?

**How to run**:
```bash
make workload
make e2-breakdown
```

**Outputs**:
- `analysis/<tag>/benchmark/overhead_breakdown.{csv,tex}`
- `analysis/<tag>/benchmark/profiling/mysql_perf_schema_diff.csv`

**Expected results**: `checking permissions` dominates (~60.7%), consistent
with the design where Cedar adds work only on the authorization slow path.

---

## E3 — Concurrency Scaling (sysbench OLTP)

**Paper element**: Figure 3 (throughput), Figure 4 (latency percentiles)

**What it measures**: Throughput (QPS) and tail latency under increasing
concurrency (1--16 threads) using sysbench OLTP read-write workload.

**Why it matters**: Systems venues require at least one macrobenchmark
under concurrent load to demonstrate production viability.

**How to run**:
```bash
make e3-concurrency
```
Or directly:
```bash
uv run python cli.py concurrency-benchmark --config config.yaml
```

**Outputs**:
- `analysis/<tag>/concurrency/concurrency_throughput.{csv,png}`
- `analysis/<tag>/concurrency/concurrency_latency.{csv,png}`
- `paper_artifacts/<tag>/concurrency_comparison_str.csv` (after `make paper-artifacts`)

**Expected results**: Baseline ~26,629 QPS at 16 threads; Cedar ~22,467
QPS (15.6% reduction). Overhead increases with concurrency due to agent
contention.

---

## E4 — Policy Scaling

**Paper element**: Figure 6 (authorization latency vs. policy count boxplot)

**What it measures**: Cedar authorization latency as policy set size
increases from 1 to 10,000 policies.

**Why it matters**: Decoupled systems must demonstrate that overhead does
not grow unboundedly with policy count.

**How to run**:
```bash
make workload
make e4-policy-scaling
```
Or directly:
```bash
uv run python cli.py policy-scaling --config config.yaml
```

**Outputs**:
- `analysis/<tag>/policy_scaling/policy_scaling.{csv,tex,png}`
- `paper_artifacts/<tag>/policy_scaling_boxplot_stats.csv` (after `make paper-artifacts`)

**Expected results**: ~3x jump from 1 to 10 policies (Cedar indexing),
then flat (< 1.02x increase from 10 to 10,000 policies).

---

## E5 — Analytic / Join-Heavy Queries

**Paper element**: Supporting (not a standalone figure)

**What it measures**: Overhead when queries are complex (multi-join +
aggregation), representing analytic workloads.

**Why it matters**: Demonstrates that as query complexity grows,
authorization overhead becomes a smaller fraction of total query time.

**How to run**:
```bash
make workload
make e5-analytic
```

**Outputs**:
- `results/<tag>/analytic/...`
- `analysis/<tag>/analytic/...`

**Expected results**: Absolute overhead similar to E1, but relative
overhead lower because base query time is higher.

---

## E6 — Concurrency Contention

**Paper element**: Supporting (reinforces E3)

**What it measures**: Same as E3 but extended to 32 threads, focusing on
authorization-path contention and agent saturation effects.

**How to run**:
```bash
make e6-concurrency-contention
```

**Outputs**: Same structure as E3.

**Expected results**: If the agent is provisioned adequately, trends track
E3. At very high thread counts, sharper throughput drops may emerge from
agent queuing.

---

## E7 — Failure Resilience

**Paper element**: Figure 5 (agent stress test), Table 3 (network delay)

**What it measures**: Two sub-experiments:

1. **Agent delay benchmark**: Query latency under injected network delays
   (0--500 ms via Toxiproxy). Verifies that overhead is additive and
   predictable (resolution time stays ~5--7 ms).

2. **Agent stress test**: Cedar agent behavior at 100--3,200 RPS using
   Vegeta. Identifies the saturation point (~1,000 RPS with p99 < 15 ms;
   failures from ~1,400 RPS).

**Prerequisites**: Toxiproxy and Vegeta must be installed. See
[FAILURE_RESILIENCE_SETUP.md](FAILURE_RESILIENCE_SETUP.md).

**How to run**:
```bash
make e7-failure
```
Or individually:
```bash
uv run python cli.py failure agent-delay-benchmark --config config.yaml
uv run python cli.py failure agent-stress-test --config config.yaml
```

**Outputs**:
- `analysis/<tag>/failure/agent_delay_comprehensive.png`
- `analysis/<tag>/failure/agent_delay_impact.tex`
- `analysis/<tag>/failure/agent_stress_comprehensive.png`
- `results/<tag>/failure/agent_stress_test/summary.csv`

---

## E8 — Semantic Correctness and Monotonicity

**Paper element**: Security verification (mentioned in evaluation text)

**What it measures**: Two security properties:

1. **Fail-closed**: Under agent failures (network, protocol, agent crash,
   configuration errors, overload), queries that should be denied are
   denied. Verified via Toxiproxy fault injection.

2. **Monotonicity (additive permissions)**: If baseline GRANTs allow a
   query, Cedar must also allow it. Cedar must never deny what baseline
   allows.

**How to run**:
```bash
make workload
make e8-semantics
```

**Outputs**:
- `results/<tag>/semantics/semantic_correctness_results.json`
- `results/<tag>/semantics/monotonicity_results.json`
- `analysis/<tag>/semantics/robustness_summary.{csv,tex}`

**Expected results**: Zero violations across all scenarios.

---

## E9 — TPC-C Macrobenchmark

**Paper element**: Figure 7 (cross-database comparison)

**What it measures**: TPC-C throughput (TPM) on both PostgreSQL and MySQL
using sysbench-tpcc (10 warehouses, 16 threads, 10 runs).

**Why it matters**: Industry-standard OLTP benchmark validating that
overhead is acceptable for complex transactions and generalizes across
database engines.

**How to run**:
```bash
make e9-tpcc
```
This runs both MySQL (`e9-tpcc-fresh`) and PostgreSQL (`e9-tpcc-postgres-fresh`).

**Outputs**:
- `analysis/<tag>/tpcc/tpcc_summary.{csv,tex}`
- `paper_artifacts/<tag>/tpcc_summary.csv` (after `make paper-artifacts`)

**Expected results**: PostgreSQL ~1.3% overhead, MySQL ~3.9% overhead.

---

## E10 — DDL Operations

**Paper element**: Supporting (DDL coverage)

**What it measures**: Validates that DDL operations (CREATE/ALTER/DROP)
are correctly authorized under Cedar.

**How to run**:
```bash
make e10-ddl
```

**Outputs**:
- `results/<tag>/ddl/ddl_comprehensive_results.json`

**Expected results**: Baseline and Cedar agree on allowed/denied
operations under equivalent authorization specs.

---

## E11 — pgbench (TPC-B)

**Paper element**: Table 4 (PostgreSQL TPC-B comparison)

**What it measures**: PostgreSQL TPS and latency using pgbench
(scale=20, 4 clients, 10 runs).

**Why it matters**: Demonstrates generality beyond MySQL and provides
a simpler transaction benchmark alongside TPC-C.

**How to run**:
```bash
make e11-pgbench
```

**Outputs**:
- `analysis/<tag>/pgbench/pgbench_summary.{csv,tex}`

**Expected results**: ~3.6% TPS overhead, ~3.9% latency overhead.

---

## Profiling (Supporting Diagnostic)

**Paper element**: Supports E2 attribution and anomaly investigation.

**What it measures**:
- **MySQL**: Performance Schema stage/wait deltas
- **PostgreSQL**: EXPLAIN (ANALYZE, FORMAT JSON) plan vs. execution times

**How to run**:
```bash
make profile-mysql
make profile-postgres
```

**Outputs**:
- `analysis/<tag>/benchmark/profiling/mysql_perf_schema_diff.csv`
- `analysis/<tag>/benchmark/profiling/postgres_explain_diff.csv`
