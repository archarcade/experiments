# Makefile for Decoupled Authorization Experiments
# USENIX Artifact Evaluation Ready
#
# Usage:
#   make smoke     - Quick verification (~15 minutes)
#   make paper     - Full paper experiments (several hours)
#   make clean     - Remove generated results
#
# Prerequisites:
#   - Docker and Docker Compose
#   - Python 3.10+ with uv
#   - Running MySQL containers (baseline and cedar)
#   - Running Cedar agent container

.PHONY: all smoke paper paper-runtime clean setup deps containers containers-fresh analyze visualizations help \
	env-fresh-bench-users \
	profile-mysql profile-postgres workload e1-overhead e2-breakdown e3-concurrency \
	e4-policy-scaling e5-analytic e6-concurrency-contention e7-failure \
	e8-semantics e8-semantics-test e8-monotonicity-test \
	e9-env-fresh e9-tpcc e9-tpcc-sysbench e9-tpcc-postgres e9-tpcc-postgres-fresh e9-tpcc-cleanup e9-tpcc-fresh e9-tpcc-mysql e9-tpcc-profile \
	e10-ddl e10-ddl-test e10-tpcc-ddl \
	e11-pgbench e11-pgbench-baseline e11-pgbench-cedar e11-pgbench-strace e11-pgbench-perf e11-pgbench-compare e11-pgbench-no-cache e11-pgbench-profile \
	comprehensive-breakdown \
	bench-user-setup bench-user-verify bench-user-check cache-stats cache-recommend paper-artifacts paper-artifacts-csv \
	teardown 	clean-all report viz kick-the-tires reproduce verify paper-artifacts-csv

# Configuration
PYTHON := uv run python
CLI := $(PYTHON) cli.py
CONFIG := config.yaml
TAG := $(shell grep "experiment_tag:" $(CONFIG) | awk '{print $$2}')

# Optional: set to 1 to skip PNG generation when generating paper artifacts
SKIP_PLOTS ?= 1

# Default target
all: help

help:
	@echo "Decoupled Authorization Experiments"
	@echo "===================================="
	@echo ""
	@echo "Targets:"
	@echo "  make smoke      - Quick verification (~15 min)"
	@echo "  make paper      - Full paper experiments (~4-6 hours)"
	@echo "  make analyze    - Analyze all results"
	@echo "  make report     - Generate LaTeX summary report"
	@echo "  make viz        - Generate all visualizations"
	@echo "  make clean      - Remove generated results"
	@echo ""
	@echo "Setup:"
	@echo "  make deps       - Install Python dependencies"
	@echo "  make containers - Start Docker containers"
	@echo "  make setup      - Full setup (deps + containers + Cedar)"
	@echo ""
	@echo "Individual experiments:"
	@echo "  make e1-overhead       - E1: Query-by-query overhead"
	@echo "  make e2-breakdown      - E2: Unified overhead breakdown (with profiling)"
	@echo "  make e3-concurrency    - E3: Concurrency scaling"
	@echo "  make e4-policy-scaling - E4: Policy count scaling"
	@echo "  make e5-analytic       - E5: Analytic / Join-heavy workload"
	@echo "  make e6-concurrency-contention - E6: Multi-user contention"
	@echo "  make e7-failure        - E7: Failure resilience (Performance)"
	@echo "  make e8-semantics      - E8: Semantic correctness & Monotonicity"
	@echo "  make e9-tpcc           - E9: TPC-C macrobenchmarks"
	@echo "  make e9-tpcc-postgres  - E9: TPC-C on PostgreSQL"
	@echo "  make e9-tpcc-cleanup   - E9: TPC-C cleanup only"
	@echo "  make e9-tpcc-fresh     - E9: TPC-C fresh run (cleanup + prepare)"
	@echo "  make e9-tpcc-profile   - E9: TPC-C with internal profiling"
	@echo "  make e10-ddl           - E10: DDL operations testing"
	@echo "  make e11-pgbench       - E11: PostgreSQL generality (pgbench)"
	@echo "  make e11-pgbench-strace - E11: pgbench with strace profiling"
	@echo "  make e11-pgbench-perf   - E11: pgbench with perf profiling"
	@echo "  make e11-pgbench-profile - E11: pgbench with internal profiling"
	@echo ""
	@echo "Profiling (Internal latency breakdown):"
	@echo "  make profile-mysql      - Profile MySQL internal stages (diff)"
	@echo "  make profile-postgres   - Profile PostgreSQL planning/exec (diff)"
	@echo "  make comprehensive-breakdown - Unified overhead report for current tag"
	@echo ""
	@echo "Authorization Verification (Deep Analysis Fixes):"
	@echo "  make bench-user-setup   - Create benchmark users for auth verification"
	@echo "  make bench-user-verify  - Verify benchmark user access levels"
	@echo "  make bench-user-check   - Check Cedar agent health and stats"
	@echo ""
	@echo "Cache Analysis:"
	@echo "  make cache-stats        - Get Cedar authorization cache statistics"
	@echo "  make cache-recommend RPS=<n> COMBOS=<m> - Get cache config recommendations"
	@echo ""
	@echo "Artifact Generation:"
	@echo "  make paper-artifacts    - Generate paper-ready tables and figures"

# =============================================================================
# Setup
# =============================================================================

deps:
	@echo "Starting dependencies installation at $$(date)"
	@echo "Installing Python dependencies..."
	uv sync
	@echo "✓ Dependencies installed"

containers:
	@echo "Starting Docker containers..."
	docker compose up -d
	@echo "Waiting for containers to be ready (this may take 30-60s for DB initialization)..."
	@for i in $$(seq 1 60); do \
		RUNNING=$$(docker compose ps -q); \
		if [ -n "$$RUNNING" ]; then \
			STATUSES=$$(docker inspect --format='{{if .State.Health}}{{.State.Health.Status}}{{else}}{{.State.Status}}{{end}}' $$RUNNING); \
			NON_HEALTHY=$$(echo "$$STATUSES" | grep -vE "healthy|running" | wc -l); \
			if [ $$NON_HEALTHY -eq 0 ]; then \
				echo "✓ All containers are healthy"; \
				break; \
			fi; \
		fi; \
		if [ $$i -eq 60 ]; then \
			echo "⚠️  Warning: Some containers are not healthy after 60s. Continuing anyway..."; \
		else \
			printf "."; \
			sleep 1; \
		fi; \
	done
	@echo ""
	@echo "✓ Containers started"

# Fresh container start: stop/remove compose services but keep persistent volumes.
# This is useful now that DB state is persisted on the mounted drive and we
# want a predictable clean container state per run.
containers-fresh:
	@echo "Restarting Docker containers (fresh containers, keep volumes)..."
	docker compose down --remove-orphans
	@$(MAKE) containers

setup-baseline:
	@echo "Setting up baseline MySQL..."
	$(CLI) setup-baseline --config $(CONFIG)
	@echo "✓ Baseline setup complete"

setup-cedar:
	@echo "Setting up Cedar MySQL and agent..."
	$(CLI) setup-cedar --config $(CONFIG)
	@echo "✓ Cedar setup complete"

setup: deps containers setup-baseline setup-cedar
	@echo "✓ Full setup complete"

env-fresh-bench-users: containers-fresh setup-baseline setup-cedar bench-user-setup
	@echo "✓ Environment refreshed + bench users setup"

# =============================================================================
# Benchmark User Management (Authorization Verification)
# =============================================================================
# These targets help ensure Cedar authorization is actually being invoked
# during benchmarks (addresses the negative overhead anomaly from deep analysis)

bench-user-setup:
	@echo "Setting up benchmark users for authorization verification..."
	$(CLI) bench-user setup --config $(CONFIG) --target both --db-type mysql
	@echo "✓ Benchmark users created"

bench-user-verify:
	@echo "Verifying benchmark user access levels..."
	$(CLI) bench-user verify --config $(CONFIG) --target baseline --db-type mysql
	$(CLI) bench-user verify --config $(CONFIG) --target cedar --db-type mysql
	@echo "✓ Benchmark user verification complete"

bench-user-check:
	@echo "Checking Cedar agent health and readiness..."
	$(CLI) bench-user check-cedar --config $(CONFIG)
	$(CLI) bench-user get-stats --config $(CONFIG)
	@echo "✓ Cedar agent check complete"

# =============================================================================
# Cache Analysis (P3-9 from Deep Experimental Analysis)
# =============================================================================

cache-stats:
	@echo "Getting Cedar authorization cache statistics..."
	$(CLI) bench-user cache-stats --config $(CONFIG) --db-type mysql
	@echo "✓ Cache stats retrieved"

cache-recommend:
	@echo "Getting cache configuration recommendations..."
	@echo "Using configuration from $(CONFIG)..."
	@# Allow overriding via make variables (optional)
	@EXTRA_ARGS=""; \
	if [ -n "$(RPS)" ]; then EXTRA_ARGS="$$EXTRA_ARGS --rps $(RPS)"; fi; \
	if [ -n "$(COMBOS)" ]; then EXTRA_ARGS="$$EXTRA_ARGS --unique-combos $(COMBOS)"; fi; \
	$(CLI) bench-user cache-recommend --config $(CONFIG) $$EXTRA_ARGS

# =============================================================================
# Paper Artifacts Generation
# =============================================================================

paper-artifacts:
	@echo "Generating paper artifacts (tables, figures)..."
	@EXTRA_ARGS=""; \
	if [ "$(SKIP_PLOTS)" = "1" ]; then EXTRA_ARGS="$$EXTRA_ARGS --skip-plots"; fi; \
	$(CLI) generate-artifacts --config $(CONFIG) $$EXTRA_ARGS
	@echo "✓ Paper artifacts generated in paper_artifacts/$(TAG)/"

paper-artifacts-csv:
	@echo "Generating paper artifact CSVs (TikZ data only, skip plots)..."
	$(MAKE) paper-artifacts SKIP_PLOTS=1


# =============================================================================
# Smoke Test (Quick Verification)
# =============================================================================

smoke: deps
	@echo "=============================================="
	@echo "SMOKE TEST - Quick Verification (~15 minutes)"
	@echo "=============================================="
	$(CLI) suite smoke --config $(CONFIG)
	@echo ""
	@echo "✓ Smoke test complete"
	@echo "Check results in: analysis/$(TAG)/smoke/"

# =============================================================================
# Full Paper Experiments
# =============================================================================

paper: setup bench-user-setup workload e1-overhead e2-breakdown e3-concurrency e4-policy-scaling e5-analytic e6-concurrency-contention e7-failure e8-semantics e9-tpcc e10-ddl e11-pgbench analyze viz paper-artifacts
	@echo "=============================================="
	@echo "✓ All paper experiments complete"
	@echo "Paper artifacts available in: paper_artifacts/$(TAG)/"
	@echo "=============================================="
	@echo "Finished all paper experiments at $$(date)"

paper-runtime: setup bench-user-setup workload e8-semantics e7-failure e1-overhead e2-breakdown e5-analytic e4-policy-scaling env-fresh-bench-users e10-ddl e11-pgbench env-fresh-bench-users e9-tpcc e6-concurrency-contention env-fresh-bench-users e3-concurrency analyze viz paper-artifacts
	@echo "=============================================="
	@echo "✓ All paper experiments complete (runtime-ordered)"
	@echo "Paper artifacts available in: paper_artifacts/$(TAG)/"
	@echo "=============================================="
	@echo "Finished all paper experiments at $$(date)"

# =============================================================================
# Individual Experiments
# =============================================================================

# Workload Generation target
workload:
	@echo "Generating workload..."
	$(CLI) generate-workload --config $(CONFIG)
	@echo "✓ Workload generated"

# E1: Query-by-Query Overhead targets
e1-overhead: workload
	@echo "Running E1: Query-by-Query Overhead..."
	$(CLI) multi-run benchmark --config $(CONFIG) --ordering abba
	@echo "✓ E1 complete"

# E2: Overhead Breakdown targets
e2-breakdown: workload
	@echo "Running E2: Unified Overhead Breakdown (with profiling)..."
	@$(MAKE) profile-mysql
	$(CLI) comprehensive-breakdown --results-dir results/$(TAG)/benchmark --analysis-dir analysis/$(TAG)/benchmark
	@echo "✓ E2 complete"

comprehensive-breakdown:
	@echo "Generating comprehensive breakdown for current tag..."
	$(CLI) comprehensive-breakdown --config $(CONFIG)

# E3: Concurrency Scaling targets
e3-concurrency:
	@echo "Running E3: Concurrency Scaling..."
	$(CLI) concurrency-benchmark --config $(CONFIG)
	@echo "✓ E3 complete"

# E4: Policy Count Scaling targets
e4-policy-scaling: workload
	@echo "Running E4: Policy Count Scaling..."
	$(CLI) policy-scaling --config $(CONFIG)
	@echo "✓ E4 complete"

# E5: Analytic / Join-heavy Workload targets
e5-analytic: workload
	@echo "Running E5: Analytic / Join-heavy Workload..."
	$(CLI) analytic-benchmark --config $(CONFIG)
	@echo "✓ E5 complete"

# E6: Multi-user Concurrency Contention targets
e6-concurrency-contention:
	@echo "Running E6: Multi-user Concurrency Contention..."
	$(CLI) concurrency-benchmark --config $(CONFIG) --threads 1,4,8,16,32 --target both
	@echo "✓ E6 complete"

# E7: Failure Resilience targets
e7-failure:
	@echo "Running E7: Failure Resilience (Performance)..."
	$(CLI) failure agent-delay-benchmark --config $(CONFIG)
	$(CLI) failure agent-stress-test --config $(CONFIG)
	@echo "✓ E7 complete"

# E8: Semantic Correctness targets
e8-semantics: e8-semantics-test e8-monotonicity-test

e8-semantics-test:
	@echo "Running E8: Semantic Correctness Testing..."
	@test -d workloads/$(TAG)/benchmark || (echo "Please generate workload first: make workload" && exit 1)
	$(CLI) semantics test --config $(CONFIG) --workload-dir workloads/$(TAG)/benchmark
	@echo "✓ E8 semantic correctness complete"

e8-monotonicity-test:
	@echo "Running E8: Monotonicity Testing..."
	@test -d workloads/$(TAG)/benchmark || (echo "Please generate workload first: make workload" && exit 1)
	$(CLI) semantics monotonicity --config $(CONFIG) --workload-dir workloads/$(TAG)/benchmark
	@echo "✓ E8 monotonicity testing complete"

# E9: TPC-C Macrobenchmark targets
# With persistent volumes, we explicitly restart containers and re-run DB setup
# to ensure a predictable environment for each run.
e9-env-fresh: containers-fresh setup-baseline setup-cedar
	@echo "✓ E9 environment fresh start complete"

e9-tpcc: e9-tpcc-fresh e9-tpcc-postgres-fresh


e9-tpcc-sysbench:
	@echo "Running E9: TPC-C with sysbench-tpcc (MySQL)..."
	$(CLI) tpcc sysbench-tpcc --config $(CONFIG)
	@echo "✓ E9 sysbench-tpcc complete"

e9-tpcc-postgres:
	@echo "Running E9: TPC-C with sysbench-tpcc (PostgreSQL)..."
	$(CLI) tpcc sysbench-tpcc-postgres --config $(CONFIG) --prepare --run
	@echo "✓ E9 sysbench-tpcc-postgres complete"

e9-tpcc-postgres-fresh:
	@echo "Running E9: TPC-C with fresh data (cleanup + prepare + run) (PostgreSQL)..."
	$(CLI) tpcc sysbench-tpcc-postgres --config $(CONFIG) --cleanup --prepare --run
	@echo "✓ E9 TPC-C fresh run complete"

e9-tpcc-postgres-rerun:
	@echo "Running E9: TPC-C with rerun (PostgreSQL)..."
	$(CLI) tpcc sysbench-tpcc-postgres --config $(CONFIG) --run
	@echo "✓ E9 TPC-C rerun complete"

e9-tpcc-cleanup:
	@echo "Cleaning up E9: TPC-C sysbench-tpcc tables..."
	$(CLI) tpcc sysbench-tpcc --config $(CONFIG) --cleanup --no-prepare --no-run
	@echo "✓ E9 TPC-C cleanup complete"

e9-tpcc-fresh:
	@echo "Running E9: TPC-C with fresh data (cleanup + prepare + run)..."
	$(CLI) tpcc sysbench-tpcc --config $(CONFIG) --cleanup --prepare --run
	@echo "✓ E9 TPC-C fresh run complete"

e9-tpcc-mysql:
	@echo "Running E9: TPC-C with tpcc-mysql..."
	$(CLI) tpcc tpcc-mysql --config $(CONFIG)
	@echo "✓ E9 tpcc-mysql complete"

e9-tpcc-profile:
	@echo "Running E9: TPC-C with profiling..."
	$(CLI) tpcc tpcc-mysql --config $(CONFIG) --profile
	$(CLI) comprehensive-breakdown --results-dir results/$(TAG)/tpcc --analysis-dir analysis/$(TAG)/tpcc
	@echo "✓ E9 TPC-C profiling complete"

# E10: DDL Operations targets
e10-ddl: e10-ddl-test e10-tpcc-ddl

e10-ddl-test:
	@echo "Running E10: DDL Operations Testing..."
	$(CLI) ddl test --config $(CONFIG) --suite comprehensive
	@echo "✓ E10 DDL testing complete"

e10-tpcc-ddl:
	@echo "Running E10: TPC-C DDL Schema Testing..."
	$(CLI) ddl tpcc-schema --config $(CONFIG) --tpcc-tool sysbench-tpcc
	@echo "✓ E10 TPC-C DDL testing complete"

# E11: PostgreSQL Cross-database targets
e11-pgbench: e11-pgbench-compare e11-pgbench-no-cache

e11-pgbench-baseline:
	@echo "Running E11: PostgreSQL Baseline pgbench..."
	$(CLI) pgbench run --config $(CONFIG) --db-system postgres-baseline
	@echo "✓ E11 PostgreSQL baseline complete"

e11-pgbench-cedar:
	@echo "Running E11: PostgreSQL Cedar pgbench..."
	$(CLI) pgbench run --config $(CONFIG) --db-system postgres-cedar
	@echo "✓ E11 PostgreSQL Cedar complete"

e11-pgbench-strace:
	@echo "Running E11: PostgreSQL pgbench with strace profiling..."
	$(CLI) pgbench run --config $(CONFIG) --db-system postgres-cedar --strace --strace-duration 10
	@echo "✓ E11 PostgreSQL strace complete"

e11-pgbench-perf:
	@echo "Running E11: PostgreSQL pgbench with perf profiling (Baseline vs Cedar, Cache Enabled)..."
	$(CLI) pgbench compare --config $(CONFIG) --perf --perf-duration 10 --cache
	@echo "✓ E11 PostgreSQL perf complete"

e11-pgbench-perf-record:
	@echo "Running E11: PostgreSQL pgbench with detailed perf record (Baseline vs Cedar, Cache Enabled)..."
	$(CLI) pgbench compare --config $(CONFIG) --perf-record --perf-duration 10 --cache
	@echo "✓ E11 PostgreSQL perf record complete"

e11-pgbench-compare:
	@echo "Running E11: PostgreSQL Baseline vs Cedar Comparison..."
	$(CLI) pgbench compare --config $(CONFIG)
	@echo "✓ E11 PostgreSQL comparison complete"

e11-pgbench-no-cache:
	@echo "Running E11: PostgreSQL pgbench with caches disabled (Baseline vs Cedar)..."
	$(CLI) pgbench no-cache --config $(CONFIG)
	@echo "✓ E11 PostgreSQL no-cache complete"

e11-pgbench-profile:
	@echo "Running E11: PostgreSQL pgbench with profiling..."
	$(CLI) pgbench compare --config $(CONFIG) --profile
	$(CLI) comprehensive-breakdown --results-dir results/$(TAG)/pgbench --analysis-dir analysis/$(TAG)/pgbench
	@echo "✓ E11 PostgreSQL profiling complete"

# =============================================================================
# Profiling (Internal Latency Breakdown)
# =============================================================================

profile-mysql: workload
	@echo "Profiling MySQL baseline..."
	$(CLI) profile mysql --config $(CONFIG) --target baseline --experiment benchmark
	@echo "Profiling MySQL Cedar..."
	$(CLI) profile mysql --config $(CONFIG) --target cedar --experiment benchmark
	@echo "Generating MySQL profiling diff..."
	$(CLI) profile diff \
		--baseline-profile analysis/$(TAG)/benchmark/profiling/mysql_baseline_perf_schema.json \
		--cedar-profile analysis/$(TAG)/benchmark/profiling/mysql_cedar_perf_schema.json \
		--output analysis/$(TAG)/benchmark/profiling/mysql_perf_schema_diff.csv
	@echo "✓ MySQL profiling complete (see analysis/$(TAG)/benchmark/profiling/mysql_perf_schema_diff.csv)"

profile-postgres: workload
	@echo "Profiling PostgreSQL baseline..."
	$(CLI) profile postgres --config $(CONFIG) --target postgres-baseline --experiment benchmark --sample-n 200
	@echo "Profiling PostgreSQL Cedar..."
	$(CLI) profile postgres --config $(CONFIG) --target postgres-cedar --experiment benchmark --sample-n 200
	@echo "Generating PostgreSQL profiling diff..."
	$(CLI) profile diff \
		--baseline-profile analysis/$(TAG)/benchmark/profiling/postgres_postgres-baseline_explain.json \
		--cedar-profile analysis/$(TAG)/benchmark/profiling/postgres_postgres-cedar_explain.json \
		--output analysis/$(TAG)/benchmark/profiling/postgres_explain_diff.csv
	@echo "✓ PostgreSQL profiling complete (see analysis/$(TAG)/benchmark/profiling/postgres_explain_diff.csv)"

# =============================================================================
# Analysis and Visualization
# =============================================================================

analyze:
	@echo "Analyzing all results..."
	$(CLI) analyze-results --config $(CONFIG) --include-extra
	@echo "✓ Analysis complete"

visualizations: viz

viz:
	@echo "Generating visualizations..."
	@# Generate visualizations for any analysis directory that contains known CSV inputs
	@if [ -d "analysis" ]; then \
		find analysis -type f \( \
			-name "baseline_latencies.csv" -o \
			-name "cedar_latencies.csv" -o \
			-name "policy_scaling.csv" -o \
			-name "concurrency_throughput.csv" -o \
			-name "concurrency_latency.csv" -o \
			-name "pgbench_summary.csv" -o \
			-name "tpcc_summary.csv" \
		\) -exec sh -c 'dirname "$$1"' _ {} \; | sort -u | \
		while IFS= read -r d; do \
			python3 -c "from framework.visualizations import generate_all_visualizations; from pathlib import Path; import sys; generate_all_visualizations(Path(sys.argv[1]))" "$$d"; \
		done; \
	fi
	@echo "✓ Visualizations generated"

report:
	@echo "Generating experiment report..."
	$(CLI) report --config $(CONFIG) --format latex
	@echo "✓ Report generated"

# =============================================================================
# Cleanup
# =============================================================================

clean:
	@echo "Safely archiving generated files..."
	@TIMESTAMP=$$(date +%Y%m%d_%H%M%S); \
	BACKUP_DIR="backups/backup_$$TIMESTAMP"; \
	mkdir -p backups; \
	if [ -d results ] || [ -d analysis ] || [ -d workloads ] || [ -d report ]; then \
		mkdir -p $$BACKUP_DIR; \
		[ -d results ] && mv results $$BACKUP_DIR/ 2>/dev/null || true; \
		[ -d analysis ] && mv analysis $$BACKUP_DIR/ 2>/dev/null || true; \
		[ -d workloads ] && mv workloads $$BACKUP_DIR/ 2>/dev/null || true; \
		[ -d report ] && mv report $$BACKUP_DIR/ 2>/dev/null || true; \
		echo "✓ Data moved to $$BACKUP_DIR"; \
	else \
		echo "No data to clean."; \
	fi

teardown:
	@echo "Tearing down containers..."
	docker compose down
	@echo "✓ Teardown complete"

clean-all: clean
	@echo "Stopping containers..."
	docker compose down -v
	@echo "✓ Full cleanup complete"

# =============================================================================
# Artifact Evaluation Targets
# =============================================================================

# Kick-the-tires: Quick check that everything works
kick-the-tires: smoke
	@echo "Kick-the-tires evaluation complete!"
	@echo "Check the following outputs:"
	@echo "  - analysis/$(TAG)/smoke/query_by_query_overhead.csv"
	@echo "  - analysis/$(TAG)/smoke/latency_cdf.png"

# Full reproduction: Reproduce all paper results
reproduce: paper analyze viz report
	@echo "Full reproduction complete!"
	@echo ""
	@echo "Paper outputs available in:"
	@echo "  - report/report.tex (summary)"
	@echo "  - analysis/*/benchmark/*.png (figures)"
	@echo "  - analysis/*/benchmark/*.tex (tables)"

# Verify outputs against expected results
verify:
	@echo "Verifying outputs..."
	@test -f analysis/*/benchmark/query_by_query_overhead.csv || (echo "Missing: overhead table" && exit 1)
	@test -f analysis/*/benchmark/latency_cdf.png || (echo "Missing: CDF plot" && exit 1)
	@echo "✓ All expected outputs present"

# Stricter check for paper-ready outputs (aligned with RESULTS.md / visualizations_and_results.md)
verify-paper:
	@echo "Verifying paper outputs..."
	@test -f analysis/*/benchmark/query_by_query_overhead.csv || (echo "Missing: O1 query-by-query overhead CSV" && exit 1)
	@test -f analysis/*/benchmark/query_by_query_overhead.tex || (echo "Missing: O1 query-by-query overhead LaTeX" && exit 1)
	@test -f analysis/*/benchmark/latency_cdf.png || (echo "Missing: O3 latency CDF plot" && exit 1)
	@# Policy scaling (if experiment was run)
	@test -f analysis/*/policy_scaling/policy_scaling.csv || (echo "Missing: O5 policy scaling CSV" && exit 1)
	@test -f analysis/*/policy_scaling/policy_scaling.png || (echo "Missing: O4 policy scaling plot" && exit 1)
	@# Concurrency (if experiment was run)
	@test -f analysis/*/concurrency/concurrency_throughput.csv || (echo "Missing: O7 concurrency throughput CSV" && exit 1)
	@test -f analysis/*/concurrency/concurrency_throughput.png || (echo "Missing: O6 concurrency throughput plot" && exit 1)
	@test -f analysis/*/concurrency/concurrency_latency.csv || (echo "Missing: concurrency latency CSV" && exit 1)
	@test -f analysis/*/concurrency/concurrency_latency.png || (echo "Missing: concurrency latency plot" && exit 1)
	@# Failure resilience (if configured)
	@test -f analysis/*/failure/agent_delay_comprehensive.png || (echo "Missing: O8 agent delay comprehensive plot" && exit 1)
	@test -f analysis/*/failure/agent_delay_impact.tex || (echo "Missing: O9 agent delay impact table" && exit 1)
	@test -f analysis/*/failure/agent_stress_comprehensive.png || (echo "Missing: O10 agent stress plot" && exit 1)
	@# Macrobenchmarks
	@test -f analysis/*/pgbench/pgbench_summary.tex || (echo "Missing: pgbench summary LaTeX" && exit 1)
	@test -f analysis/*/tpcc/tpcc_summary.tex || (echo "Missing: TPC-C summary LaTeX" && exit 1)
	@# Security
	@test -f analysis/*/semantics/robustness_summary.tex || (echo "Missing: Robustness summary LaTeX" && exit 1)
	@echo "✓ Paper outputs present"

