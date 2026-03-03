# VLDB Paper Results Mapping

This document maps every figure and table in the ARCADE evaluation
(Section 7 of the PVLDB paper) to the experiment that produces it,
the Makefile target to run, and the output files to check.

## Overview

| Paper Element | Description | Experiment | Target | Key Output |
|---------------|-------------|------------|--------|------------|
| Table 1 | Per-operation latency overhead | E1 | `make e1-overhead` | `analysis/*/benchmark/query_by_query_overhead.tex` |
| Table 2 | MySQL stage overhead breakdown | E2 | `make e2-breakdown` | `analysis/*/benchmark/overhead_breakdown.tex` |
| Figure 3 | Throughput vs. concurrency | E3 | `make e3-concurrency` | `paper_artifacts/*/concurrency_comparison_str.csv` |
| Figure 4 | Latency percentiles vs. concurrency | E3 | `make e3-concurrency` | `paper_artifacts/*/concurrency_comparison_str.csv` |
| Figure 5 | Cedar agent stress test | E7 | `make e7-failure` | `analysis/*/failure/agent_stress_comprehensive.png` |
| Table 3 | Network delay sensitivity | E7 | `make e7-failure` | `analysis/*/failure/agent_delay_impact.tex` |
| Figure 6 | Policy scaling (boxplot) | E4 | `make e4-policy-scaling` | `paper_artifacts/*/policy_scaling_boxplot_stats.csv` |
| Figure 7 | TPC-C cross-database comparison | E9 | `make e9-tpcc` | `paper_artifacts/*/tpcc_summary.csv` |
| Table 4 | pgbench TPC-B | E11 | `make e11-pgbench` | `analysis/*/pgbench/pgbench_summary.tex` |
| (text) | Fail-closed and monotonicity | E8 | `make e8-semantics` | `analysis/*/semantics/robustness_summary.tex` |

---

## Detailed Result Descriptions

### Table 1: Per-Operation Latency Overhead (MySQL)

Per-query latency across 10 paired ABBA runs. Reports median with 95%
bootstrap confidence intervals.

**Paper values** (reference):

| Op | Baseline (ms) | Cedar (ms) | Overhead |
|----|---------------|------------|----------|
| SELECT | 1.38 [1.36--1.42] | 1.44 [1.42--1.46] | +4.5% |
| INSERT | 1.25 [1.23--1.28] | 1.28 [1.26--1.32] | +2.8% |
| UPDATE | 5.24 [5.20--5.26] | 5.24 [5.20--5.28] | ~0% |
| DELETE | 1.33 [1.31--1.39] | 1.40 [1.39--1.43] | +5.5% |
| JOIN | 1.45 [1.44--1.50] | 1.51 [1.49--1.53] | +3.9% |

**Reproduce**:
```bash
make workload
make e1-overhead
make analyze
```

**Outputs**:
- `analysis/<tag>/benchmark/query_by_query_overhead.csv`
- `analysis/<tag>/benchmark/query_by_query_overhead.tex`
- `analysis/<tag>/benchmark/latency_cdf.png`

---

### Table 2: MySQL Stage Overhead Breakdown

Performance Schema stage timings showing where authorization overhead
originates. The `checking permissions` stage dominates (60.7%).

**Reproduce**:
```bash
make workload
make e2-breakdown
```

**Outputs**:
- `analysis/<tag>/benchmark/overhead_breakdown.csv`
- `analysis/<tag>/benchmark/overhead_breakdown.tex`
- `analysis/<tag>/benchmark/profiling/mysql_perf_schema_diff.csv`

---

### Figures 3--4: Concurrency Scaling (sysbench OLTP)

Throughput (QPS) and latency percentiles (p50, p95) at 1--16 threads.
The paper's TikZ figures read data from CSV files.

**Paper values** (reference):

| Threads | Baseline QPS | Cedar QPS | Overhead |
|---------|-------------|-----------|----------|
| 1 | 2,701 | 2,592 | 4.0% |
| 16 | 26,629 | 22,467 | 15.6% |

**Reproduce**:
```bash
make e3-concurrency
make paper-artifacts
```

**Outputs**:
- `paper_artifacts/<tag>/concurrency_comparison.csv`
- `paper_artifacts/<tag>/concurrency_comparison_str.csv` (TikZ input)
- `analysis/<tag>/concurrency/concurrency_throughput.csv`
- `analysis/<tag>/concurrency/concurrency_latency.csv`
- `analysis/<tag>/concurrency/concurrency_throughput.png`
- `analysis/<tag>/concurrency/concurrency_latency.png`

The TikZ files in `vldb/paper/figures/tikz/concurrency_throughput.tex` and
`concurrency_latency.tex` read `concurrency_comparison_str.csv`.

---

### Figure 5: Cedar Agent Stress Test

Dual-panel plot showing latency percentiles (log scale) and success/failure
rates at 100--3,200 RPS.

**Key observations from the paper**:
- 0--1,000 RPS: p99 < 15 ms
- 1,200 RPS: p50 ~ 1,044 ms, 0% failures
- >= 1,400 RPS: connection exhaustion, 10.5% failures

**Reproduce**:
```bash
make e7-failure
make analyze
```

**Outputs**:
- `analysis/<tag>/failure/agent_stress_comprehensive.png`
- `results/<tag>/failure/agent_stress_test/summary.csv`

The TikZ file `vldb/paper/figures/tikz/agent_stress_test.tex` reads
hardcoded data (or the summary CSV can be used for updating).

---

### Table 3: Network Delay Sensitivity

Injected delay (0--500 ms via Toxiproxy) vs. query time. Resolution time
(query time minus delay) stays constant at ~5--7 ms.

**Reproduce**:
```bash
make e7-failure
make analyze
```

**Outputs**:
- `analysis/<tag>/failure/agent_delay_impact.tex`
- `analysis/<tag>/failure/agent_delay_comprehensive.png`

---

### Figure 6: Policy Scaling (Boxplot)

Authorization latency vs. policy count (1--10,000 policies). Shows
~3x jump from 1 to 10 policies, then flat.

**Paper values** (reference):
- 1 policy: ~0.48 ms
- 10 policies: ~1.6 ms
- 10,000 policies: ~1.60 ms (< 1.02x vs 10 policies)

**Reproduce**:
```bash
make workload
make e4-policy-scaling
make paper-artifacts
```

**Outputs**:
- `paper_artifacts/<tag>/policy_scaling.csv`
- `paper_artifacts/<tag>/policy_scaling_boxplot_stats.csv` (TikZ input)
- `analysis/<tag>/policy_scaling/policy_scaling.csv`
- `analysis/<tag>/policy_scaling/policy_scaling.png`

The TikZ file `vldb/paper/figures/tikz/policy_scaling_boxplot.tex` reads
`policy_scaling_boxplot_stats.csv`.

---

### Figure 7: TPC-C Cross-Database Comparison

Grouped bar chart comparing TPC-C throughput (TPM) for PostgreSQL and MySQL,
baseline vs. Cedar.

**Paper values** (reference):

| Database | Baseline TPM | Cedar TPM | Overhead |
|----------|-------------|-----------|----------|
| PostgreSQL | 76,694 +/- 2,876 | 75,709 +/- 4,114 | 1.3% |
| MySQL | 10,011 +/- 1,421 | 9,634 +/- 1,501 | 3.9% |

**Reproduce**:
```bash
make e9-tpcc
make paper-artifacts
```

**Outputs**:
- `paper_artifacts/<tag>/tpcc_summary.csv`
- `paper_artifacts/<tag>/tpcc_summary.tex`
- `analysis/<tag>/tpcc/tpcc_summary.csv`

The TikZ file `vldb/paper/figures/tikz/cross_database_comparison.tex` reads
`tpcc_summary.csv`.

---

### Table 4: PostgreSQL TPC-B (pgbench)

TPS and latency for PostgreSQL baseline vs. Cedar (scale=20, 4 clients,
10 runs).

**Paper values** (reference):

| Metric | Baseline | Cedar | Overhead |
|--------|----------|-------|----------|
| TPS | 7,737 +/- 83 | 7,457 +/- 129 | +3.6% |
| Latency | 0.517 ms | 0.537 ms | +3.9% |

**Reproduce**:
```bash
make e11-pgbench
make analyze
```

**Outputs**:
- `analysis/<tag>/pgbench/pgbench_summary.csv`
- `analysis/<tag>/pgbench/pgbench_summary.tex`

---

### Security Verification (Fail-Closed and Monotonicity)

The paper states zero fail-closed violations and zero additive-permissions
violations across all tested failure categories (network, protocol, agent,
configuration, overload).

**Reproduce**:
```bash
make workload
make e8-semantics
make analyze
```

**Outputs**:
- `results/<tag>/semantics/semantic_correctness_results.json`
- `results/<tag>/semantics/monotonicity_results.json`
- `analysis/<tag>/semantics/robustness_summary.csv`
- `analysis/<tag>/semantics/robustness_summary.tex`

---

## Reproducing All Paper Results

To reproduce every result in the evaluation section:

```bash
# One-command full reproduction
make paper

# Or step by step:
make setup
make bench-user-setup
make workload
make e1-overhead          # Table 1
make e2-breakdown         # Table 2
make e3-concurrency       # Figures 3--4
make e4-policy-scaling    # Figure 6
make e7-failure           # Figure 5, Table 3
make e8-semantics         # Security verification
make e9-tpcc              # Figure 7
make e11-pgbench          # Table 4
make analyze viz
make paper-artifacts      # Generate TikZ-compatible CSVs
```

## Copying Artifacts to the Paper

After running experiments, the TikZ-compatible CSVs in `paper_artifacts/<tag>/`
can be copied to `vldb/paper/figures/data/`:

```bash
TAG=$(grep experiment_tag config.yaml | awk '{print $2}')
cp paper_artifacts/$TAG/concurrency_comparison_str.csv ../vldb/paper/figures/data/
cp paper_artifacts/$TAG/tpcc_summary.csv               ../vldb/paper/figures/data/
cp paper_artifacts/$TAG/policy_scaling_boxplot_stats.csv ../vldb/paper/figures/data/
```

## Verification

To verify all expected outputs are present:

```bash
make verify-paper
```

This checks for every output file listed in this document.
