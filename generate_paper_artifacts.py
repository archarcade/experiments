#!/usr/bin/env python3
# ruff: noqa: E402

"""
Orchestration script to generate all paper artifacts (tables, plots) from experiment results.
Scans `experiments/results` and produces outputs in `experiments/paper_artifacts/<experiment_tag>`.

USENIX Security Paper Artifacts:
- E1: Query-by-query overhead tables and CDF plots
- E2: Overhead breakdown waterfall charts
- E3/E6: Concurrency scaling and contention analysis
- E4: Policy scaling tables and plots
- E5: Analytic query complexity analysis
- E7: Failure resilience plots
- E8: Security properties verification table
- E9: TPC-C macrobenchmark comparison
- E10: DDL operations analysis
- E11: pgbench comparison

Usage:
    uv run python3 experiments/generate_paper_artifacts.py --config config.yaml
    uv run python3 experiments/generate_paper_artifacts.py  # uses default config
"""

import argparse
import csv
import json
import logging
import statistics
import sys
from pathlib import Path

# Ensure experiments module is in path
current_file = Path(__file__).resolve()
experiments_root = current_file.parent
if str(experiments_root) not in sys.path:
    sys.path.append(str(experiments_root))

from framework import analysis_analytic, analysis_ddl, analysis_tpcc, visualizations
from framework.config import Config, load_config_file
from framework.tikz_data_export import (
    write_concurrency_comparison_str_csv,
    write_policy_scaling_boxplot_stats_csv,
)

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def process_benchmark_results(experiment_dir: Path, output_dir: Path) -> None:
    """Process E1 benchmark results and generate overhead tables."""
    benchmark_dir = experiment_dir / "benchmark"
    if not benchmark_dir.exists():
        return

    logger.info("  Processing benchmark results (E1)")

    # Look for results.json files
    for results_file in benchmark_dir.glob("**/results.json"):
        # Generate overhead summary table
        tex_path = output_dir / "overhead_summary.tex"
        result = visualizations.latex_table_overhead_summary(results_file, tex_path)
        if result:
            logger.info("    Generated overhead_summary.tex")
        break  # Use first results file found


def process_profiling_results(experiment_dir: Path, output_dir: Path) -> None:
    """Process E2 profiling results and generate overhead breakdown."""
    # Look for profiling data in various locations
    profiling_locations = [
        experiment_dir / "benchmark" / "profiling",
        experiment_dir / "profiling",
    ]

    for profiling_dir in profiling_locations:
        if not profiling_dir.exists():
            continue

        mysql_diff = profiling_dir / "mysql_perf_schema_diff.csv"
        if mysql_diff.exists():
            logger.info("  Processing profiling results (E2)")

            # Generate overhead breakdown waterfall
            result = visualizations.generate_overhead_breakdown_waterfall(
                mysql_diff, output_dir / "overhead_breakdown_waterfall.png"
            )
            if result:
                logger.info("    Generated overhead_breakdown_waterfall.png")
            return


def process_concurrency_results(experiment_dir: Path, output_dir: Path) -> None:
    """Process E3/E6 concurrency and contention results."""
    concurrency_dir = experiment_dir / "concurrency"
    if not concurrency_dir.exists():
        return

    logger.info("  Processing concurrency results (E3/E6)")
    agg_rows = []

    def scan_conc_summaries(path_root, system_name):
        if not path_root.exists():
            return
        for p in path_root.glob("threads_*/summary.json"):
            try:
                data = json.loads(p.read_text())
                threads = int(p.parent.name.replace("threads_", ""))
                agg_rows.append(
                    {
                        "threads": threads,
                        "system": system_name,
                        "qps": data.get("qps", 0)
                        or data.get("qps_median", 0)
                        or data.get("throughput", 0),
                        "p95": data.get("lat_p95_ms", 0)
                        or data.get("lat_p95_median", 0)
                        or data.get("p95_latency", 0),
                        "p99": data.get("lat_p99_ms")
                        or data.get("lat_p99_median", 0)
                        or data.get("p99_latency")
                        or 0,
                        "p50": data.get("lat_avg_ms", 0)
                        or data.get("lat_p50_ms", 0)
                        or data.get("lat_avg_median", 0),
                        "qps_std": data.get("qps_std", 0)
                        or data.get("throughput_std", 0),
                    }
                )
            except Exception as e:
                logger.warning(f"Failed to parse {p}: {e}")

    scan_conc_summaries(concurrency_dir / "baseline", "baseline")
    scan_conc_summaries(concurrency_dir / "cedar", "cedar")

    if agg_rows:
        by_threads = {}
        for r in agg_rows:
            th = r["threads"]
            if th not in by_threads:
                by_threads[th] = {"threads": th}
            sys = r["system"]
            by_threads[th][f"{sys}_qps"] = r["qps"]
            by_threads[th][f"{sys}_qps_std"] = r["qps_std"]
            by_threads[th][f"{sys}_p95_ms"] = r["p95"]
            by_threads[th][f"{sys}_p99_ms"] = r["p99"]
            by_threads[th][f"{sys}_p50_ms"] = r["p50"]

        csv_path = output_dir / "concurrency_comparison.csv"
        fieldnames = [
            "threads",
            "baseline_qps",
            "baseline_qps_std",
            "cedar_qps",
            "cedar_qps_std",
            "baseline_p95_ms",
            "cedar_p95_ms",
            "baseline_p99_ms",
            "cedar_p99_ms",
            "baseline_p50_ms",
            "cedar_p50_ms",
        ]

        with csv_path.open("w", newline="") as f:
            w = csv.DictWriter(f, fieldnames=fieldnames)
            w.writeheader()
            for th in sorted(by_threads.keys()):
                w.writerow(by_threads[th])
        logger.info("    Generated concurrency_comparison.csv")

        # Generate LaTeX table
        tex_path = output_dir / "concurrency_comparison.tex"
        visualizations.latex_table_concurrency_comparison(csv_path, tex_path)
        logger.info("    Generated concurrency_comparison.tex")

        # Generate TikZ-compatible CSV with string thread counts
        tikz_csv_path = output_dir / "concurrency_comparison_str.csv"
        write_concurrency_comparison_str_csv(by_threads, tikz_csv_path)
        logger.info("    Generated concurrency_comparison_str.csv (TikZ format)")


def process_policy_scaling_results(experiment_dir: Path, output_dir: Path) -> None:
    """Process E4 policy scaling results."""
    policy_dir = experiment_dir / "policy_scaling"
    if not policy_dir.exists():
        return

    logger.info("  Processing policy scaling results (E4)")
    agg_rows = []

    for p in policy_dir.glob("policies_*/results.json"):
        try:
            count = int(p.parent.name.replace("policies_", ""))
            data = json.loads(p.read_text())

            rows = []
            if isinstance(data, dict):
                rows = (
                    data.get("cedar", [])
                    if "cedar" in data
                    else data.get("baseline", [])
                )
            elif isinstance(data, list):
                rows = data

            cedar_rows = [
                r for r in rows if isinstance(r, dict) and r.get("system") == "cedar"
            ]
            if not cedar_rows and rows:
                cedar_rows = rows

            # Support both single-run and multi-run policy scaling results
            if isinstance(data, dict) and data.get("multi_run"):
                latencies = []
                for run in data.get("runs", []) or []:
                    if not isinstance(run, dict):
                        continue
                    latencies.extend(
                        [
                            float(r.get("latency_ms", 0))
                            for r in (run.get("cedar", []) or [])
                            if isinstance(r, dict) and r.get("success")
                        ]
                    )
            else:
                latencies = [
                    float(r.get("latency_ms", 0))
                    for r in cedar_rows
                    if isinstance(r, dict)
                ]

            if latencies:
                med = statistics.median(latencies)
                p95 = (
                    statistics.quantiles(latencies, n=100)[94]
                    if len(latencies) > 100
                    else 0
                )
                p99 = (
                    statistics.quantiles(latencies, n=100)[98]
                    if len(latencies) > 100
                    else 0
                )
                std = statistics.stdev(latencies) if len(latencies) > 1 else 0.0
                agg_rows.append(
                    {
                        "policy_count": count,
                        "median_ms": med,
                        "p95_ms": p95,
                        "p99_ms": p99,
                        "stddev_ms": std,
                    }
                )
        except Exception as e:
            logger.warning(f"Failed to process {p}: {e}")

    if agg_rows:
        csv_path = output_dir / "policy_scaling.csv"
        agg_rows.sort(key=lambda x: x["policy_count"])
        with csv_path.open("w", newline="") as f:
            w = csv.DictWriter(
                f,
                fieldnames=[
                    "policy_count",
                    "median_ms",
                    "p95_ms",
                    "p99_ms",
                    "stddev_ms",
                ],
            )
            w.writeheader()
            w.writerows(agg_rows)
        logger.info("    Generated policy_scaling.csv")

        # Generate LaTeX table
        tex_path = output_dir / "policy_scaling.tex"
        visualizations.latex_table_policy_scaling(csv_path, tex_path)
        logger.info("    Generated policy_scaling.tex")

        # Generate line plot
        plot_path = output_dir / "policy_scaling.png"
        visualizations.generate_policy_scaling_plot(csv_path, plot_path)
        logger.info("    Generated policy_scaling.png")

        # Generate box plot for variance visualization
        boxplot_path = output_dir / "policy_scaling_boxplot.png"
        visualizations.generate_policy_scaling_boxplot(policy_dir, boxplot_path)
        logger.info("    Generated policy_scaling_boxplot.png")

        # Generate TikZ-compatible boxplot stats CSV
        boxplot_stats_path = output_dir / "policy_scaling_boxplot_stats.csv"
        result = write_policy_scaling_boxplot_stats_csv(policy_dir, boxplot_stats_path)
        if result:
            logger.info("    Generated policy_scaling_boxplot_stats.csv (TikZ format)")


def process_analytic_results(experiment_dir: Path, output_dir: Path) -> None:
    """Process E5 analytic query results."""
    analytic_dir = experiment_dir / "analytic"
    benchmark_dir = experiment_dir / "benchmark"

    # Try analytic dir first, then benchmark dir
    for search_dir in [analytic_dir, benchmark_dir]:
        if not search_dir.exists():
            continue

        analytic_results = analysis_analytic.collect_analytic_results(search_dir)
        if analytic_results:
            logger.info("  Processing analytic query results (E5)")

            csv_path = output_dir / "analytic_summary.csv"
            analysis_analytic.write_analytic_summary_csv(analytic_results, csv_path)
            logger.info("    Generated analytic_summary.csv")

            tex_path = output_dir / "analytic_summary.tex"
            analysis_analytic.write_analytic_summary_table_tex(
                analytic_results, tex_path
            )
            logger.info("    Generated analytic_summary.tex")

            # Generate overhead ratio plot
            plot_path = output_dir / "analytic_overhead_ratio.png"
            result = analysis_analytic.generate_overhead_ratio_plot(
                analytic_results, plot_path
            )
            if result:
                logger.info("    Generated analytic_overhead_ratio.png")
            return


def process_failure_results(experiment_dir: Path, output_dir: Path) -> None:
    """Process E7 failure resilience results."""
    failure_dir = experiment_dir / "failure"
    if not failure_dir.exists():
        return

    logger.info("  Processing failure resilience results (E7)")
    # Already handled by existing visualizations - just log
    logger.info("    Failure visualizations generated by viz target")


def process_semantics_results(experiment_dir: Path, output_dir: Path) -> None:
    """Process E8 semantic correctness results."""
    semantics_dir = experiment_dir / "semantics"
    if not semantics_dir.exists():
        return

    robustness_csv = semantics_dir / "robustness_summary.csv"
    if robustness_csv.exists():
        logger.info("  Processing semantic correctness results (E8)")

        tex_path = output_dir / "security_properties.tex"
        result = visualizations.generate_security_properties_table(
            robustness_csv, tex_path
        )
        if result:
            logger.info("    Generated security_properties.tex")


def process_tpcc_results(experiment_dir: Path, output_dir: Path) -> None:
    """Process E9 TPC-C results."""
    tpcc_dir = experiment_dir / "tpcc"
    if not tpcc_dir.exists():
        return

    logger.info("  Processing TPC-C results (E9)")
    tpcc_rows = analysis_tpcc.collect_tpcc_results(tpcc_dir)
    if tpcc_rows:
        analysis_tpcc.write_tpcc_summary_table_tex(
            tpcc_rows, output_dir / "tpcc_summary.tex"
        )
        analysis_tpcc.write_tpcc_summary_csv(tpcc_rows, output_dir / "tpcc_summary.csv")
        logger.info("    Generated tpcc_summary.csv and tpcc_summary.tex")


def process_ddl_results(experiment_dir: Path, output_dir: Path) -> None:
    """Process E10 DDL operations results."""
    ddl_dir = experiment_dir / "ddl"
    if not ddl_dir.exists():
        return

    logger.info("  Processing DDL operations results (E10)")
    ddl_rows = analysis_ddl.collect_ddl_results(ddl_dir)
    if ddl_rows:
        analysis_ddl.write_ddl_summary_csv(ddl_rows, output_dir / "ddl_summary.csv")
        analysis_ddl.write_ddl_summary_table_tex(
            ddl_rows, output_dir / "ddl_summary.tex"
        )
        logger.info("    Generated ddl_summary.csv and ddl_summary.tex")


def process_pgbench_results(experiment_dir: Path, output_dir: Path) -> None:
    """Process E11 pgbench results."""
    pgbench_dir = experiment_dir / "pgbench"
    if not pgbench_dir.exists():
        return

    logger.info("  Processing pgbench results (E11)")
    # Already handled by existing analysis_pgbench module
    logger.info("    pgbench visualizations generated by viz target")


def process_cross_database_comparison(output_dir: Path) -> None:
    """Generate cross-database comparison (MySQL vs PostgreSQL)."""
    tpcc_csv = output_dir / "tpcc_summary.csv"
    pgbench_csv = output_dir / "pgbench_summary.csv"

    if tpcc_csv.exists() or pgbench_csv.exists():
        logger.info("  Generating cross-database comparison")
        result = visualizations.generate_cross_database_comparison(
            tpcc_csv if tpcc_csv.exists() else None,
            pgbench_csv if pgbench_csv.exists() else None,
            output_dir / "cross_database_comparison.png",
        )
        if result:
            logger.info("    Generated cross_database_comparison.png")


def generate_unified_summary(output_dir: Path) -> None:
    """Generate unified summary table for paper."""
    logger.info("  Generating unified summary table")
    result = visualizations.generate_unified_summary_table(
        output_dir, output_dir / "unified_summary.tex"
    )
    if result:
        logger.info("    Generated unified_summary.tex")


def main():
    parser = argparse.ArgumentParser(
        description="Generate paper artifacts from experiment results",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    uv run python generate_paper_artifacts.py --config config.yaml
    uv run python generate_paper_artifacts.py --experiment-tag mysql_cache_on
    uv run python generate_paper_artifacts.py --results-dir ./results --output-dir ./paper_artifacts
        """,
    )
    parser.add_argument(
        "--config",
        "-c",
        type=str,
        default=None,
        help="Path to YAML/JSON config file (optional, uses defaults if not provided)",
    )
    parser.add_argument(
        "--experiment-tag",
        "-e",
        type=str,
        default=None,
        help="Specific experiment tag to process (default: all tags in results dir)",
    )
    parser.add_argument(
        "--results-dir",
        "-r",
        type=str,
        default=None,
        help="Results directory (default: from config or ./results)",
    )
    parser.add_argument(
        "--output-dir",
        "-o",
        type=str,
        default=None,
        help="Output directory for artifacts (default: from config or ./paper_artifacts)",
    )
    parser.add_argument(
        "--skip-plots",
        action="store_true",
        help="Skip PNG figure generation (generate only CSV and LaTeX tables)",
    )

    args = parser.parse_args()

    # Set environment variable to skip plots if requested
    if args.skip_plots:
        import os

        os.environ["CEDAR_SKIP_PLOTS"] = "1"
        logger.info("Skipping PNG generation (--skip-plots enabled)")

    # Load config file if provided
    cfg: Config = load_config_file(args.config)

    # Determine results directory: CLI > config > default
    if args.results_dir:
        results_root = Path(args.results_dir)
    elif cfg.output.results_dir:
        results_root = Path(cfg.output.results_dir)
    else:
        results_root = experiments_root / "results"

    # Determine output directory: CLI > default
    if args.output_dir:
        artifacts_root = Path(args.output_dir)
    else:
        artifacts_root = experiments_root / "paper_artifacts"

    if not results_root.exists():
        logger.error(f"Results directory not found: {results_root}")
        sys.exit(1)

    logger.info(f"Using config: {args.config or 'default'}")
    logger.info(f"Scanning results in: {results_root}")
    logger.info(f"Output directory: {artifacts_root}")

    # Determine which experiment directories to process
    if args.experiment_tag:
        # Process specific experiment tag
        experiment_dirs = [results_root / args.experiment_tag]
        if not experiment_dirs[0].exists():
            logger.error(f"Experiment directory not found: {experiment_dirs[0]}")
            sys.exit(1)
    else:
        # Process all experiment directories
        experiment_dirs = [
            d
            for d in results_root.iterdir()
            if d.is_dir() and not d.name.startswith(".")
        ]

    for experiment_dir in experiment_dirs:
        experiment_tag = experiment_dir.name
        logger.info(f"Processing experiment: {experiment_tag}")

        # Create output directory for this experiment
        output_dir = artifacts_root / experiment_tag
        output_dir.mkdir(parents=True, exist_ok=True)

        # Process each experiment type
        process_benchmark_results(experiment_dir, output_dir)  # E1
        process_profiling_results(experiment_dir, output_dir)  # E2
        process_concurrency_results(experiment_dir, output_dir)  # E3/E6
        process_policy_scaling_results(experiment_dir, output_dir)  # E4
        process_analytic_results(experiment_dir, output_dir)  # E5
        process_failure_results(experiment_dir, output_dir)  # E7
        process_semantics_results(experiment_dir, output_dir)  # E8
        process_tpcc_results(experiment_dir, output_dir)  # E9
        process_ddl_results(experiment_dir, output_dir)  # E10
        process_pgbench_results(experiment_dir, output_dir)  # E11

        # Generate cross-cutting artifacts
        process_cross_database_comparison(output_dir)
        generate_unified_summary(output_dir)

        # Generate all visualizations
        logger.info("  Generating visualizations...")
        outputs = visualizations.generate_all_visualizations(output_dir, output_dir)
        for name, path in outputs.items():
            if path:
                logger.info(f"    Generated {name}: {path.name}")

    logger.info("Paper artifact generation complete!")


if __name__ == "__main__":
    main()
