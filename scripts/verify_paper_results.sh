#!/bin/bash
# verify_paper_results.sh
#
# Verifies that all expected output files for the ARCADE VLDB paper exist.
# Reports which paper elements have their outputs and which are missing.
#
# Usage:
#   ./verify_paper_results.sh              # uses tag from config.yaml
#   ./verify_paper_results.sh <tag>        # use specific experiment tag

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
EXPERIMENTS_DIR="$(dirname "$SCRIPT_DIR")"

cd "$EXPERIMENTS_DIR"

GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
BOLD='\033[1m'
NC='\033[0m'

if [ -n "$1" ]; then
    TAG="$1"
else
    TAG=$(grep "experiment_tag:" config.yaml | awk '{print $2}')
fi

echo -e "${BOLD}ARCADE VLDB Paper — Output Verification${NC}"
echo "Experiment tag: $TAG"
echo ""

PASS=0
FAIL=0
WARN=0

check_required() {
    local path="$1"
    local label="$2"
    if ls $path 1>/dev/null 2>&1; then
        echo -e "  ${GREEN}✓${NC} $label"
        PASS=$((PASS + 1))
    else
        echo -e "  ${RED}✗${NC} $label"
        echo -e "    ${RED}Expected: $path${NC}"
        FAIL=$((FAIL + 1))
    fi
}

check_optional() {
    local path="$1"
    local label="$2"
    if ls $path 1>/dev/null 2>&1; then
        echo -e "  ${GREEN}✓${NC} $label"
        PASS=$((PASS + 1))
    else
        echo -e "  ${YELLOW}⊘${NC} $label (optional)"
        WARN=$((WARN + 1))
    fi
}

# ── Table 1: Per-operation latency overhead ──────────────────────────

echo -e "${BOLD}Table 1: Per-Operation Latency Overhead (E1)${NC}"
check_required "analysis/$TAG/benchmark/query_by_query_overhead.csv" "Overhead CSV"
check_required "analysis/$TAG/benchmark/query_by_query_overhead.tex" "Overhead LaTeX"
check_optional "analysis/$TAG/benchmark/latency_cdf.png"             "Latency CDF plot"
echo ""

# ── Table 2: Overhead breakdown ──────────────────────────────────────

echo -e "${BOLD}Table 2: MySQL Stage Overhead Breakdown (E2)${NC}"
check_required "analysis/$TAG/benchmark/profiling/mysql_perf_schema_diff.csv" "Profiling diff CSV"
check_optional "analysis/$TAG/benchmark/overhead_breakdown.csv"                "Breakdown CSV"
check_optional "analysis/$TAG/benchmark/overhead_breakdown.tex"                "Breakdown LaTeX"
echo ""

# ── Figures 3-4: Concurrency scaling ────────────────────────────────

echo -e "${BOLD}Figures 3-4: Concurrency Scaling (E3)${NC}"
check_required "analysis/$TAG/concurrency/concurrency_throughput.csv" "Throughput CSV"
check_required "analysis/$TAG/concurrency/concurrency_latency.csv"   "Latency CSV"
check_optional "analysis/$TAG/concurrency/concurrency_throughput.png" "Throughput plot"
check_optional "analysis/$TAG/concurrency/concurrency_latency.png"   "Latency plot"
check_optional "paper_artifacts/$TAG/concurrency_comparison_str.csv"  "TikZ data CSV"
echo ""

# ── Figure 5: Agent stress test ─────────────────────────────────────

echo -e "${BOLD}Figure 5: Cedar Agent Stress Test (E7)${NC}"
check_required "analysis/$TAG/failure/agent_stress_comprehensive.png"           "Stress test plot"
check_optional "results/$TAG/failure/agent_stress_test/summary.csv"             "Raw summary CSV"
echo ""

# ── Table 3: Network delay sensitivity ──────────────────────────────

echo -e "${BOLD}Table 3: Network Delay Sensitivity (E7)${NC}"
check_required "analysis/$TAG/failure/agent_delay_impact.tex"          "Delay impact LaTeX"
check_optional "analysis/$TAG/failure/agent_delay_comprehensive.png"   "Delay plot"
echo ""

# ── Figure 6: Policy scaling ────────────────────────────────────────

echo -e "${BOLD}Figure 6: Policy Scaling (E4)${NC}"
check_required "analysis/$TAG/policy_scaling/policy_scaling.csv"                 "Scaling CSV"
check_optional "analysis/$TAG/policy_scaling/policy_scaling.png"                 "Scaling plot"
check_optional "analysis/$TAG/policy_scaling/policy_scaling.tex"                 "Scaling LaTeX"
check_optional "paper_artifacts/$TAG/policy_scaling_boxplot_stats.csv"           "TikZ boxplot stats"
echo ""

# ── Figure 7: TPC-C cross-database ──────────────────────────────────

echo -e "${BOLD}Figure 7: TPC-C Cross-Database Comparison (E9)${NC}"
check_required "analysis/$TAG/tpcc/tpcc_summary.csv"               "TPC-C summary CSV"
check_required "analysis/$TAG/tpcc/tpcc_summary.tex"               "TPC-C summary LaTeX"
check_optional "paper_artifacts/$TAG/tpcc_summary.csv"              "TikZ data CSV"
echo ""

# ── Table 4: pgbench TPC-B ──────────────────────────────────────────

echo -e "${BOLD}Table 4: pgbench TPC-B (E11)${NC}"
check_required "analysis/$TAG/pgbench/pgbench_summary.csv"         "pgbench CSV"
check_required "analysis/$TAG/pgbench/pgbench_summary.tex"         "pgbench LaTeX"
echo ""

# ── Security verification ───────────────────────────────────────────

echo -e "${BOLD}Security Verification (E8)${NC}"
check_required "results/$TAG/semantics/semantic_correctness_results.json" "Correctness results"
check_optional "results/$TAG/semantics/monotonicity_results.json"         "Monotonicity results"
check_required "analysis/$TAG/semantics/robustness_summary.tex"           "Robustness LaTeX"
echo ""

# ── Supporting experiments ───────────────────────────────────────────

echo -e "${BOLD}Supporting Experiments${NC}"
check_optional "results/$TAG/ddl/ddl_comprehensive_results.json"     "E10: DDL results"
check_optional "results/$TAG/analytic"                                "E5: Analytic results"
echo ""

# ── Summary ──────────────────────────────────────────────────────────

echo "─────────────────────────────────────────────────"
TOTAL=$((PASS + FAIL + WARN))
echo -e "${GREEN}Passed:   $PASS / $TOTAL${NC}"
if [ $FAIL -gt 0 ]; then
    echo -e "${RED}Failed:   $FAIL / $TOTAL${NC}"
fi
if [ $WARN -gt 0 ]; then
    echo -e "${YELLOW}Optional: $WARN / $TOTAL${NC}"
fi
echo ""

if [ $FAIL -eq 0 ]; then
    echo -e "${GREEN}${BOLD}All required outputs are present.${NC}"
    exit 0
else
    echo -e "${RED}${BOLD}$FAIL required output(s) missing.${NC}"
    echo ""
    echo "To generate missing outputs, run:"
    echo "  make paper          # full reproduction"
    echo "  make analyze viz    # re-run analysis only"
    echo "  make paper-artifacts  # generate TikZ CSVs"
    exit 1
fi
