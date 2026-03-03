#!/bin/bash
# reproduce_vldb_results.sh
#
# Reproduces all results from the ARCADE VLDB paper (Section 7: Evaluation).
# Maps each paper figure/table to the corresponding experiment and verifies
# outputs upon completion.
#
# Usage:
#   ./reproduce_vldb_results.sh              # full reproduction (~4-6 hours)
#   ./reproduce_vldb_results.sh --quick      # smoke test (~15 min)
#   ./reproduce_vldb_results.sh --skip-setup # skip environment setup
#   ./reproduce_vldb_results.sh TABLE1       # reproduce only Table 1
#   ./reproduce_vldb_results.sh FIGURE5      # reproduce only Figure 5
#
# Paper elements that can be specified individually:
#   TABLE1   — Per-operation latency overhead (E1)
#   TABLE2   — MySQL stage overhead breakdown (E2)
#   FIGURE3  — Throughput vs. concurrency (E3)
#   FIGURE4  — Latency vs. concurrency (E3)
#   FIGURE5  — Cedar agent stress test (E7)
#   TABLE3   — Network delay sensitivity (E7)
#   FIGURE6  — Policy scaling boxplot (E4)
#   FIGURE7  — TPC-C cross-database comparison (E9)
#   TABLE4   — pgbench TPC-B (E11)
#   SECURITY — Fail-closed and monotonicity (E8)

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
EXPERIMENTS_DIR="$(dirname "$SCRIPT_DIR")"

cd "$EXPERIMENTS_DIR"

GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
BLUE='\033[0;34m'
BOLD='\033[1m'
NC='\033[0m'

SKIP_SETUP=false
QUICK=false
TARGET=""

for arg in "$@"; do
    case "$arg" in
        --skip-setup) SKIP_SETUP=true ;;
        --quick)      QUICK=true ;;
        --help|-h)
            head -25 "$0" | tail -24
            exit 0
            ;;
        *)            TARGET="$arg" ;;
    esac
done

TAG=$(grep "experiment_tag:" config.yaml | awk '{print $2}')

log_section() {
    echo ""
    echo -e "${BLUE}${BOLD}════════════════════════════════════════════════════════${NC}"
    echo -e "${BLUE}${BOLD}  $1${NC}"
    echo -e "${BLUE}${BOLD}════════════════════════════════════════════════════════${NC}"
    echo ""
}

log_step() {
    echo -e "${YELLOW}▶ $1${NC}"
}

log_done() {
    echo -e "${GREEN}✓ $1${NC}"
}

log_skip() {
    echo -e "${YELLOW}⊘ Skipping: $1${NC}"
}

check_output() {
    local path="$1"
    local label="$2"
    if ls $path 1>/dev/null 2>&1; then
        log_done "$label: $(ls $path 2>/dev/null | head -1)"
    else
        echo -e "${RED}✗ Missing: $label ($path)${NC}"
    fi
}

run_target() {
    local target="$1"
    local match="$2"
    if [ -n "$TARGET" ] && [ "$TARGET" != "$match" ]; then
        return 1
    fi
    return 0
}

# ─────────────────────────────────────────────────────────────────────
# Setup
# ─────────────────────────────────────────────────────────────────────

if [ "$QUICK" = true ]; then
    log_section "ARCADE VLDB Results — Smoke Test (~15 min)"
    make smoke
    log_done "Smoke test complete"
    exit 0
fi

log_section "ARCADE VLDB Paper Results Reproduction"
echo "Experiment tag: $TAG"
echo "Start time: $(date)"
echo ""

if [ "$SKIP_SETUP" = false ] && [ -z "$TARGET" ]; then
    log_step "Setting up environment..."
    make setup
    make bench-user-setup
    log_done "Environment ready"
fi

# ─────────────────────────────────────────────────────────────────────
# Workload generation (needed by E1, E2, E4, E5, E8)
# ─────────────────────────────────────────────────────────────────────

if [ -z "$TARGET" ] || [[ "$TARGET" =~ ^(TABLE1|TABLE2|FIGURE6|SECURITY)$ ]]; then
    log_step "Generating workload..."
    make workload
    log_done "Workload generated"
fi

# ─────────────────────────────────────────────────────────────────────
# Table 1: Per-operation latency overhead (E1)
# ─────────────────────────────────────────────────────────────────────

if run_target TABLE1 TABLE1 || [ -z "$TARGET" ]; then
    log_section "Table 1: Per-Operation Latency Overhead (E1)"
    log_step "Running E1: Per-query overhead (10 ABBA runs)..."
    make e1-overhead
    log_done "E1 complete"
fi

# ─────────────────────────────────────────────────────────────────────
# Table 2: MySQL stage overhead breakdown (E2)
# ─────────────────────────────────────────────────────────────────────

if run_target TABLE2 TABLE2 || [ -z "$TARGET" ]; then
    log_section "Table 2: MySQL Stage Overhead Breakdown (E2)"
    log_step "Running E2: Overhead breakdown (profiling)..."
    make e2-breakdown
    log_done "E2 complete"
fi

# ─────────────────────────────────────────────────────────────────────
# Figures 3-4: Concurrency scaling (E3)
# ─────────────────────────────────────────────────────────────────────

if run_target FIGURE3 FIGURE3 || run_target FIGURE4 FIGURE4 || [ -z "$TARGET" ]; then
    log_section "Figures 3-4: Concurrency Scaling (E3)"
    log_step "Running E3: sysbench OLTP concurrency (1-16 threads, 10 runs)..."
    make e3-concurrency
    log_done "E3 complete"
fi

# ─────────────────────────────────────────────────────────────────────
# Figure 6: Policy scaling (E4)
# ─────────────────────────────────────────────────────────────────────

if run_target FIGURE6 FIGURE6 || [ -z "$TARGET" ]; then
    log_section "Figure 6: Policy Scaling (E4)"
    log_step "Running E4: Policy scaling (1-10,000 policies, 10 runs)..."
    make e4-policy-scaling
    log_done "E4 complete"
fi

# ─────────────────────────────────────────────────────────────────────
# Figure 5 + Table 3: Failure resilience (E7)
# ─────────────────────────────────────────────────────────────────────

if run_target FIGURE5 FIGURE5 || run_target TABLE3 TABLE3 || [ -z "$TARGET" ]; then
    log_section "Figure 5 + Table 3: Failure Resilience (E7)"
    log_step "Running E7: Agent delay benchmark + stress test..."
    make e7-failure
    log_done "E7 complete"
fi

# ─────────────────────────────────────────────────────────────────────
# Security verification (E8)
# ─────────────────────────────────────────────────────────────────────

if run_target SECURITY SECURITY || [ -z "$TARGET" ]; then
    log_section "Security Verification (E8)"
    log_step "Running E8: Fail-closed + monotonicity tests..."
    make e8-semantics
    log_done "E8 complete"
fi

# ─────────────────────────────────────────────────────────────────────
# Figure 7: TPC-C cross-database comparison (E9)
# ─────────────────────────────────────────────────────────────────────

if run_target FIGURE7 FIGURE7 || [ -z "$TARGET" ]; then
    log_section "Figure 7: TPC-C Cross-Database Comparison (E9)"
    log_step "Running E9: TPC-C on MySQL and PostgreSQL (10 warehouses, 16 threads)..."
    make e9-tpcc
    log_done "E9 complete"
fi

# ─────────────────────────────────────────────────────────────────────
# Table 4: pgbench TPC-B (E11)
# ─────────────────────────────────────────────────────────────────────

if run_target TABLE4 TABLE4 || [ -z "$TARGET" ]; then
    log_section "Table 4: pgbench TPC-B (E11)"
    log_step "Running E11: pgbench (scale=20, 4 clients, 10 runs)..."
    make e11-pgbench
    log_done "E11 complete"
fi

# ─────────────────────────────────────────────────────────────────────
# Analysis, visualization, and artifact generation
# ─────────────────────────────────────────────────────────────────────

if [ -z "$TARGET" ]; then
    log_section "Post-Processing"

    log_step "Running analysis..."
    make analyze
    log_done "Analysis complete"

    log_step "Generating visualizations..."
    make viz
    log_done "Visualizations complete"

    log_step "Generating paper artifacts (TikZ-compatible CSVs)..."
    make paper-artifacts
    log_done "Paper artifacts complete"
fi

# ─────────────────────────────────────────────────────────────────────
# Verification
# ─────────────────────────────────────────────────────────────────────

log_section "Output Verification"

echo "Checking outputs for tag: $TAG"
echo ""

echo -e "${BOLD}Paper Tables:${NC}"
check_output "analysis/$TAG/benchmark/query_by_query_overhead.tex" "Table 1 (per-query overhead)"
check_output "analysis/$TAG/benchmark/overhead_breakdown.tex"      "Table 2 (overhead breakdown)"
check_output "analysis/$TAG/failure/agent_delay_impact.tex"        "Table 3 (network delay)"
check_output "analysis/$TAG/pgbench/pgbench_summary.tex"           "Table 4 (pgbench TPC-B)"

echo ""
echo -e "${BOLD}Paper Figures:${NC}"
check_output "analysis/$TAG/concurrency/concurrency_throughput.png" "Figure 3 (concurrency throughput)"
check_output "analysis/$TAG/concurrency/concurrency_latency.png"   "Figure 4 (concurrency latency)"
check_output "analysis/$TAG/failure/agent_stress_comprehensive.png" "Figure 5 (agent stress test)"
check_output "analysis/$TAG/policy_scaling/policy_scaling.png"      "Figure 6 (policy scaling)"
check_output "analysis/$TAG/tpcc/tpcc_summary.csv"                 "Figure 7 (TPC-C comparison)"

echo ""
echo -e "${BOLD}TikZ Data (for paper compilation):${NC}"
check_output "paper_artifacts/$TAG/concurrency_comparison_str.csv"    "concurrency_comparison_str.csv"
check_output "paper_artifacts/$TAG/policy_scaling_boxplot_stats.csv"  "policy_scaling_boxplot_stats.csv"
check_output "paper_artifacts/$TAG/tpcc_summary.csv"                  "tpcc_summary.csv"

echo ""
echo -e "${BOLD}Security Verification:${NC}"
check_output "analysis/$TAG/semantics/robustness_summary.tex"        "Robustness summary"

echo ""
echo "End time: $(date)"
log_done "Reproduction complete"
