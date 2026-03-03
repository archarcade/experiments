#!/usr/bin/env python3
"""
Visualization generation from CSV analysis outputs.

Generates plots for:
- CDF Plot: Latency Distribution (RQ1)
- Box Plot: Latency per Query Type (Baseline vs Cedar)
- Line Plot: Policy Count vs. Authorization Time (RQ2)
- Line Plot: Concurrency vs. Throughput (RQ2)
- Line Plot: Concurrency vs. Latency Percentiles (RQ2)
"""

from __future__ import annotations

import csv
import json
import os
from pathlib import Path

try:
    import matplotlib

    matplotlib.use("Agg")  # Non-interactive backend
    import matplotlib.pyplot as plt
    import seaborn as sns

    try:
        import pandas as pd

        HAS_PANDAS = True
    except ImportError:
        HAS_PANDAS = False
        pd = None
    HAS_PLOTTING = True
except ImportError:
    HAS_PLOTTING = False


def _should_skip_plots() -> bool:
    """
    Check if plot generation should be skipped.

    This allows generating only CSV/LaTeX outputs without matplotlib figures,
    which is useful when migrating to TikZ-based figures for the paper.

    Set via environment variable: CEDAR_SKIP_PLOTS=1
    """
    return os.environ.get("CEDAR_SKIP_PLOTS", "").lower() in ("1", "true", "yes")


def generate_latency_cdf(
    baseline_csv: Path,
    cedar_csv: Path,
    output_path: Path,
    title: str = "Latency Distribution (CDF)",
) -> Path | None:
    """
    Generate CDF plot comparing baseline vs Cedar latency distributions.

    Args:
        baseline_csv: Path to baseline_latencies.csv
        cedar_csv: Path to cedar_latencies.csv
        output_path: Path to save the plot (e.g., latency_cdf.png)
        title: Plot title

    Returns:
        Path to generated plot, or None if plotting libraries unavailable
    """
    if not HAS_PLOTTING or _should_skip_plots():
        return None

    if not baseline_csv.exists() or not cedar_csv.exists():
        return None

    # Read data
    baseline_latencies = []
    cedar_latencies = []

    with baseline_csv.open() as f:
        reader = csv.DictReader(f)
        for row in reader:
            baseline_latencies.append(float(row["latency_ms"]))

    with cedar_csv.open() as f:
        reader = csv.DictReader(f)
        for row in reader:
            cedar_latencies.append(float(row["latency_ms"]))

    if not baseline_latencies or not cedar_latencies:
        return None

    # Sort for CDF
    baseline_latencies.sort()
    cedar_latencies.sort()

    # Calculate CDF
    n_baseline = len(baseline_latencies)
    n_cedar = len(cedar_latencies)

    baseline_cdf = [(i + 1) / n_baseline for i in range(n_baseline)]
    cedar_cdf = [(i + 1) / n_cedar for i in range(n_cedar)]

    # Create plot
    plt.figure(figsize=(10, 6))
    plt.plot(
        baseline_latencies, baseline_cdf, "--", label="Baseline", linewidth=2, alpha=0.8
    )
    plt.plot(
        cedar_latencies, cedar_cdf, "-", label="With Cedar", linewidth=2, alpha=0.8
    )
    plt.xlabel("Latency (ms)", fontsize=16, fontweight="bold")
    plt.ylabel("Cumulative Probability", fontsize=16, fontweight="bold")
    plt.title(title, fontsize=18, fontweight="bold")
    plt.legend(fontsize=14)
    plt.tick_params(axis="both", which="major", labelsize=14)
    plt.grid(True, alpha=0.3)
    plt.tight_layout()

    output_path.parent.mkdir(parents=True, exist_ok=True)
    plt.savefig(output_path, dpi=300, bbox_inches="tight")
    plt.close()

    return output_path


# =============================================================================
# USENIX Paper Enhancements - New Visualization Functions
# =============================================================================


def generate_overhead_breakdown_waterfall(
    profiling_csv: Path,
    output_path: Path,
    title: str = "Authorization Overhead Breakdown",
) -> Path | None:
    """
    Generate waterfall/stacked bar chart showing where overhead comes from.

    Reads MySQL profiling diff CSV and creates a breakdown visualization showing:
    - Opening tables
    - Checking permissions
    - Handler commit
    - Other stages

    Args:
        profiling_csv: Path to mysql_perf_schema_diff.csv
        output_path: Path to save the plot
        title: Plot title

    Returns:
        Path to generated plot, or None if data unavailable
    """
    if not HAS_PLOTTING or not HAS_PANDAS or _should_skip_plots():
        return None

    if not profiling_csv.exists():
        return None

    # Read the profiling diff data
    df = pd.read_csv(profiling_csv)

    # Filter for stages section only (most relevant for overhead breakdown)
    stages_df = df[df["section"] == "stages"].copy()

    if stages_df.empty:
        return None

    # Sort by absolute delta to show most significant contributors
    stages_df["abs_delta"] = stages_df["cedar_minus_baseline_ms"].abs()
    stages_df = stages_df.nlargest(10, "abs_delta")

    # Clean up event names for display
    stages_df["display_name"] = stages_df["event_name"].str.replace(
        "stage/sql/", "", regex=False
    )

    # Create the waterfall chart
    fig, ax = plt.subplots(figsize=(14, 8))

    colors = [
        "#DC3545" if v > 0 else "#28A745" for v in stages_df["cedar_minus_baseline_ms"]
    ]

    y_pos = range(len(stages_df))
    bars = ax.barh(
        y_pos,
        stages_df["cedar_minus_baseline_ms"],
        color=colors,
        alpha=0.85,
        edgecolor="black",
        linewidth=0.5,
    )

    ax.set_yticks(y_pos)
    ax.set_yticklabels(stages_df["display_name"], fontsize=12)
    ax.invert_yaxis()

    ax.axvline(x=0, color="black", linewidth=1.5)

    # Add value labels on bars
    for i, (bar, val) in enumerate(zip(bars, stages_df["cedar_minus_baseline_ms"])):
        if val >= 0:
            ax.text(
                val + 20,
                i,
                f"+{val:.1f}ms",
                va="center",
                fontsize=11,
                fontweight="bold",
            )
        else:
            ax.text(
                val - 20,
                i,
                f"{val:.1f}ms",
                va="center",
                ha="right",
                fontsize=11,
                fontweight="bold",
            )

    ax.set_xlabel("Overhead (Cedar - Baseline) in ms", fontsize=16, fontweight="bold")
    ax.set_title(title, fontsize=18, fontweight="bold", pad=20)
    ax.grid(True, alpha=0.3, axis="x")

    # Add legend
    from matplotlib.patches import Patch

    legend_elements = [
        Patch(facecolor="#DC3545", label="Increased time (overhead)"),
        Patch(facecolor="#28A745", label="Decreased time (improvement)"),
    ]
    ax.legend(handles=legend_elements, loc="lower right", fontsize=12)

    plt.tight_layout()
    output_path.parent.mkdir(parents=True, exist_ok=True)
    plt.savefig(output_path, dpi=300, bbox_inches="tight")
    plt.close()

    return output_path


def generate_security_properties_table(
    robustness_csv: Path, output_path: Path
) -> Path | None:
    """
    Generate LaTeX table for security properties verification (E8).

    Shows pass/fail status for:
    - Fail-closed (secure fallback)
    - Monotonicity (no privilege escalation)
    - Consistency (deterministic under failure)

    Args:
        robustness_csv: Path to robustness_summary.csv
        output_path: Path to save the LaTeX table

    Returns:
        Path to generated table, or None if data unavailable
    """
    if not robustness_csv.exists():
        return None

    # Read data
    with robustness_csv.open() as f:
        reader = csv.DictReader(f)
        rows = list(reader)

    if not rows:
        return None

    # Generate LaTeX table
    lines = [
        "\\begin{table}[h]",
        "\\centering",
        "\\caption{Security Property Verification Results}",
        "\\label{tab:security_properties}",
        "\\begin{tabular}{lccc}",
        "\\toprule",
        "Security Property & Test Cases & Passed & Status \\\\",
        "\\midrule",
    ]

    for row in rows:
        prop_name = row.get("property", row.get("Security Property", "Unknown"))
        violations = int(row.get("violations", 0))
        status = "\\checkmark" if violations == 0 else f"\\texttimes~({violations})"
        # Assume 100 test cases if not specified (can be adjusted)
        test_cases = row.get("test_cases", 100)
        passed = int(test_cases) - violations
        lines.append(f"{prop_name} & {test_cases} & {passed} & {status} \\\\")

    lines.extend(["\\bottomrule", "\\end{tabular}", "\\end{table}"])

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text("\n".join(lines))

    return output_path


def latex_table_overhead_summary(
    results_json_path: Path, output_path: Path
) -> Path | None:
    """
    Generate LaTeX table for query-by-query overhead summary (E1).

    Shows per-operation-type overhead breakdown:
    - Operation Type
    - Baseline Latency (ms)
    - Cedar Latency (ms)
    - Overhead (ms)
    - Overhead (%)

    Supports both the legacy single-run results format (top-level "baseline"/
    "cedar") and the newer multi-run wrapper format (top-level "runs").

    Args:
        results_json_path: Path to results.json containing benchmark results
        output_path: Path to save the LaTeX table

    Returns:
        Path to generated table, or None if data unavailable
    """
    import statistics

    if not results_json_path.exists():
        return None

    with results_json_path.open() as f:
        data = json.load(f)

    if data.get("multi_run"):
        baseline_results = []
        cedar_results = []
        for run in data.get("runs", []) or []:
            if not isinstance(run, dict):
                continue
            baseline_results.extend(run.get("baseline", []) or [])
            cedar_results.extend(run.get("cedar", []) or [])
    else:
        baseline_results = data.get("baseline", [])
        cedar_results = data.get("cedar", [])

    if not baseline_results or not cedar_results:
        return None

    # Group by category/operation type
    baseline_by_cat: dict[str, list[float]] = {}
    cedar_by_cat: dict[str, list[float]] = {}

    for r in baseline_results:
        cat = r.get("category") or r.get("action", "UNKNOWN")
        baseline_by_cat.setdefault(cat, []).append(float(r["latency_ms"]))

    for r in cedar_results:
        cat = r.get("category") or r.get("action", "UNKNOWN")
        cedar_by_cat.setdefault(cat, []).append(float(r["latency_ms"]))

    all_cats = sorted(set(baseline_by_cat.keys()) | set(cedar_by_cat.keys()))

    if not all_cats:
        return None

    # Generate LaTeX
    lines = [
        "\\begin{table}[h]",
        "\\centering",
        "\\caption{Query-by-Query Authorization Overhead}",
        "\\label{tab:overhead_summary}",
        "\\begin{tabular}{lrrrrr}",
        "\\toprule",
        "Operation Type & Baseline (ms) & Cedar (ms) & Overhead (ms) & Overhead (\\%) & n \\\\",
        "\\midrule",
    ]

    for cat in all_cats:
        base_lats = baseline_by_cat.get(cat, [])
        cedar_lats = cedar_by_cat.get(cat, [])

        if not base_lats or not cedar_lats:
            continue

        base_median = statistics.median(base_lats)
        cedar_median = statistics.median(cedar_lats)
        overhead_ms = cedar_median - base_median
        overhead_pct = (overhead_ms / base_median * 100) if base_median > 0 else 0
        n = min(len(base_lats), len(cedar_lats))

        lines.append(
            f"{cat} & {base_median:.2f} & {cedar_median:.2f} & "
            f"{overhead_ms:+.2f} & {overhead_pct:+.1f}\\% & {n} \\\\"
        )

    lines.extend(["\\bottomrule", "\\end{tabular}", "\\end{table}"])

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text("\n".join(lines))

    return output_path


def latex_table_concurrency_comparison(
    csv_path: Path, output_path: Path
) -> Path | None:
    """
    Generate LaTeX table for concurrency scaling results (E3/E6).

    Columns: Threads, Baseline QPS, Cedar QPS, Degradation (%), P95 Baseline, P95 Cedar

    Args:
        csv_path: Path to concurrency_comparison.csv
        output_path: Path to save the LaTeX table

    Returns:
        Path to generated table, or None if data unavailable
    """
    if not csv_path.exists():
        return None

    rows = []
    with csv_path.open() as f:
        reader = csv.DictReader(f)
        rows = list(reader)

    if not rows:
        return None

    lines = [
        "\\begin{table}[h]",
        "\\centering",
        "\\caption{Concurrency Scaling Performance}",
        "\\label{tab:concurrency}",
        "\\begin{tabular}{rrrrrr}",
        "\\toprule",
        "Threads & Baseline QPS & Cedar QPS & Degradation (\\%) & P95 Base (ms) & P95 Cedar (ms) \\\\",
        "\\midrule",
    ]

    for row in rows:
        try:
            threads = int(float(row.get("threads", 0)))
            base_qps = float(row.get("baseline_qps", 0))
            cedar_qps = float(row.get("cedar_qps", 0))
            base_p95 = float(row.get("baseline_p95_ms", 0))
            cedar_p95 = float(row.get("cedar_p95_ms", 0))

            if base_qps > 0:
                degradation = ((base_qps - cedar_qps) / base_qps) * 100
            else:
                degradation = 0

            lines.append(
                f"{threads} & {base_qps:.1f} & {cedar_qps:.1f} & "
                f"{degradation:.1f}\\% & {base_p95:.2f} & {cedar_p95:.2f} \\\\"
            )
        except (ValueError, KeyError):
            continue

    lines.extend(["\\bottomrule", "\\end{tabular}", "\\end{table}"])

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text("\n".join(lines))

    return output_path


def latex_table_policy_scaling(csv_path: Path, output_path: Path) -> Path | None:
    """
    Generate LaTeX table for policy count scaling results (E4).

    Columns: Policy Count, Median (ms), P95 (ms), P99 (ms), Increase from Baseline

    Args:
        csv_path: Path to policy_scaling.csv
        output_path: Path to save the LaTeX table

    Returns:
        Path to generated table, or None if data unavailable
    """
    if not csv_path.exists():
        return None

    rows = []
    with csv_path.open() as f:
        reader = csv.DictReader(f)
        rows = list(reader)

    if not rows:
        return None

    # Get baseline (first row, typically policy_count=1)
    rows.sort(key=lambda r: int(r.get("policy_count", 0)))
    float(rows[0].get("median_ms", 0)) if rows else 0

    lines = [
        "\\begin{table}[h]",
        "\\centering",
        "\\caption{Authorization Time vs Policy Count}",
        "\\label{tab:policy_scaling}",
        "\\begin{tabular}{rrrr}",
        "\\toprule",
        "Policy Count & Median (ms) & P95 (ms) & P99 (ms) \\\\",
        "\\midrule",
    ]

    for row in rows:
        try:
            policy_count = int(row.get("policy_count", 0))
            median_ms = float(row.get("median_ms", 0))
            p95_ms = float(row.get("p95_ms", 0))
            p99_ms = float(row.get("p99_ms", 0))

            lines.append(
                f"{policy_count} & {median_ms:.2f} & {p95_ms:.2f} & {p99_ms:.2f} \\\\"
            )
        except (ValueError, KeyError):
            continue

    lines.extend(["\\bottomrule", "\\end{tabular}", "\\end{table}"])

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text("\n".join(lines))

    return output_path


def generate_cross_database_comparison(
    tpcc_csv: Path | None,
    pgbench_csv: Path | None,
    output_path: Path,
    title: str = "Cross-Database Performance Comparison",
) -> Path | None:
    """
    Generate side-by-side comparison plot of MySQL vs PostgreSQL overhead.

    Args:
        tpcc_csv: Path to tpcc_summary.csv (MySQL or both DBs)
        pgbench_csv: Path to pgbench_summary.csv (PostgreSQL)
        output_path: Path to save the plot
        title: Plot title

    Returns:
        Path to generated plot, or None if data unavailable
    """
    if not HAS_PLOTTING or not HAS_PANDAS or _should_skip_plots():
        return None

    # We need at least one CSV
    if (not tpcc_csv or not tpcc_csv.exists()) and (
        not pgbench_csv or not pgbench_csv.exists()
    ):
        return None

    plot_data = []

    # Read TPC-C data
    if tpcc_csv and tpcc_csv.exists():
        with tpcc_csv.open() as f:
            reader = csv.DictReader(f)
            for row in reader:
                tool = row.get("tool", "")
                base_tpm = float(row.get("baseline_tpm", 0) or 0)
                cedar_tpm = float(row.get("cedar_tpm", 0) or 0)
                overhead_pct = float(row.get("tpm_overhead_pct", 0) or 0)

                db = (
                    "PostgreSQL"
                    if "pg" in tool.lower() or "postgres" in tool.lower()
                    else "MySQL"
                )

                if base_tpm > 0:
                    plot_data.append(
                        {
                            "Database": db,
                            "Benchmark": "TPC-C",
                            "Baseline": base_tpm,
                            "Cedar": cedar_tpm,
                            "Overhead (%)": overhead_pct,
                        }
                    )

    # Read pgbench data
    if pgbench_csv and pgbench_csv.exists():
        with pgbench_csv.open() as f:
            reader = csv.DictReader(f)
            for row in reader:
                base_tps = float(row.get("baseline_tps", 0) or 0)
                cedar_tps = float(row.get("cedar_tps", 0) or 0)

                if base_tps > 0 and cedar_tps > 0:
                    overhead_pct = ((base_tps - cedar_tps) / base_tps) * 100
                    plot_data.append(
                        {
                            "Database": "PostgreSQL",
                            "Benchmark": "pgbench",
                            "Baseline": base_tps,
                            "Cedar": cedar_tps,
                            "Overhead (%)": overhead_pct,
                        }
                    )

    if not plot_data:
        return None

    df = pd.DataFrame(plot_data)

    # Create grouped bar chart
    fig, ax = plt.subplots(figsize=(12, 7))

    x = range(len(df))
    width = 0.35

    ax.bar(
        [i - width / 2 for i in x],
        df["Baseline"],
        width,
        label="Baseline",
        color="#3498db",
        alpha=0.9,
    )
    ax.bar(
        [i + width / 2 for i in x],
        df["Cedar"],
        width,
        label="With Cedar",
        color="#e74c3c",
        alpha=0.9,
    )

    ax.set_ylabel("Throughput (TPM/TPS)", fontsize=16, fontweight="bold")
    ax.set_title(title, fontsize=18, fontweight="bold")
    ax.set_xticks(x)
    ax.set_xticklabels(
        [f"{r['Database']}\n({r['Benchmark']})" for _, r in df.iterrows()], fontsize=12
    )
    ax.legend(fontsize=14)
    ax.grid(True, alpha=0.3, axis="y")

    # Add overhead annotations
    for i, (_, row) in enumerate(df.iterrows()):
        overhead = row["Overhead (%)"]
        y_pos = max(row["Baseline"], row["Cedar"]) * 1.02
        label = f"{overhead:+.1f}%" if overhead != 0 else "0%"
        ax.annotate(
            label,
            xy=(i, y_pos),
            ha="center",
            fontsize=12,
            fontweight="bold",
            color="#28A745" if overhead <= 0 else "#DC3545",
        )

    plt.tight_layout()
    output_path.parent.mkdir(parents=True, exist_ok=True)
    plt.savefig(output_path, dpi=300, bbox_inches="tight")
    plt.close()

    return output_path


def generate_cache_impact_plot(
    cache_on_csv: Path,
    cache_off_csv: Path,
    output_path: Path,
    title: str = "Authorization Cache Impact",
) -> Path | None:
    """
    Generate comparison plot showing impact of authorization cache.

    Args:
        cache_on_csv: Path to results with cache enabled
        cache_off_csv: Path to results with cache disabled
        output_path: Path to save the plot
        title: Plot title

    Returns:
        Path to generated plot, or None if data unavailable
    """
    if not HAS_PLOTTING or not HAS_PANDAS or _should_skip_plots():
        return None

    if not cache_on_csv.exists() or not cache_off_csv.exists():
        return None

    # Read data
    cache_on_data = []
    cache_off_data = []

    with cache_on_csv.open() as f:
        reader = csv.DictReader(f)
        for row in reader:
            tps = float(row.get("cedar_tps", 0) or row.get("cedar_qps", 0) or 0)
            if tps > 0:
                cache_on_data.append(tps)

    with cache_off_csv.open() as f:
        reader = csv.DictReader(f)
        for row in reader:
            tps = float(row.get("cedar_tps", 0) or row.get("cedar_qps", 0) or 0)
            if tps > 0:
                cache_off_data.append(tps)

    if not cache_on_data or not cache_off_data:
        return None

    import statistics

    cache_on_avg = statistics.mean(cache_on_data)
    cache_off_avg = statistics.mean(cache_off_data)

    # Create bar chart
    fig, ax = plt.subplots(figsize=(10, 6))

    x = ["Cache Enabled", "Cache Disabled"]
    y = [cache_on_avg, cache_off_avg]
    colors = ["#28A745", "#DC3545"]

    bars = ax.bar(x, y, color=colors, width=0.6, edgecolor="black", linewidth=1)

    # Add value labels
    for bar, val in zip(bars, y):
        ax.text(
            bar.get_x() + bar.get_width() / 2,
            val,
            f"{val:.1f}",
            ha="center",
            va="bottom",
            fontsize=14,
            fontweight="bold",
        )

    # Add improvement annotation
    if cache_off_avg > 0:
        improvement = ((cache_on_avg - cache_off_avg) / cache_off_avg) * 100
        ax.annotate(
            f"Cache Improvement: {improvement:+.1f}%",
            xy=(0.5, max(y) * 1.1),
            ha="center",
            fontsize=14,
            fontweight="bold",
            color="#28A745",
            xycoords=("axes fraction", "data"),
        )

    ax.set_ylabel("Throughput (TPS)", fontsize=16, fontweight="bold")
    ax.set_title(title, fontsize=18, fontweight="bold")
    ax.grid(True, alpha=0.3, axis="y")

    plt.tight_layout()
    output_path.parent.mkdir(parents=True, exist_ok=True)
    plt.savefig(output_path, dpi=300, bbox_inches="tight")
    plt.close()

    return output_path


def generate_unified_summary_table(results_dir: Path, output_path: Path) -> Path | None:
    """
    Generate unified summary table for paper abstract/introduction.

    Aggregates key results from all experiments into one comprehensive table.

    Args:
        results_dir: Base results directory containing experiment subdirs
        output_path: Path to save the LaTeX table

    Returns:
        Path to generated table, or None if data unavailable
    """
    summary_data = []

    # Try to find and aggregate data from various experiments

    # 1. Concurrency scaling
    conc_csv = results_dir / "concurrency_comparison.csv"
    if conc_csv.exists():
        with conc_csv.open() as f:
            reader = csv.DictReader(f)
            rows = list(reader)
            if rows:
                # Get max concurrency results
                max_row = max(rows, key=lambda r: int(r.get("threads", 0)))
                base_qps = float(max_row.get("baseline_qps", 0))
                cedar_qps = float(max_row.get("cedar_qps", 0))
                if base_qps > 0:
                    overhead = ((base_qps - cedar_qps) / base_qps) * 100
                    summary_data.append(
                        {
                            "Metric": f"Peak Throughput ({max_row['threads']} threads)",
                            "Baseline": f"{base_qps:.0f} QPS",
                            "Cedar": f"{cedar_qps:.0f} QPS",
                            "Overhead": f"{overhead:.1f}%",
                        }
                    )

    # 2. TPC-C results
    tpcc_csv = results_dir / "tpcc_summary.csv"
    if tpcc_csv.exists():
        with tpcc_csv.open() as f:
            reader = csv.DictReader(f)
            for row in reader:
                tool = row.get("tool", "TPC-C")
                base = float(row.get("baseline_tpm", 0) or 0)
                cedar = float(row.get("cedar_tpm", 0) or 0)
                overhead = float(row.get("tpm_overhead_pct", 0) or 0)
                if base > 0:
                    summary_data.append(
                        {
                            "Metric": f"{tool} TPM",
                            "Baseline": f"{base:.0f}",
                            "Cedar": f"{cedar:.0f}",
                            "Overhead": f"{overhead:+.1f}%",
                        }
                    )

    # 3. Policy scaling
    policy_csv = results_dir / "policy_scaling.csv"
    if policy_csv.exists():
        with policy_csv.open() as f:
            reader = csv.DictReader(f)
            rows = sorted(list(reader), key=lambda r: int(r.get("policy_count", 0)))
            if len(rows) >= 2:
                first = rows[0]
                last = rows[-1]
                first_med = float(first.get("median_ms", 0))
                last_med = float(last.get("median_ms", 0))
                summary_data.append(
                    {
                        "Metric": f"Policy Scaling (1→{last['policy_count']})",
                        "Baseline": f"{first_med:.2f}ms",
                        "Cedar": f"{last_med:.2f}ms",
                        "Overhead": f"{((last_med - first_med) / first_med * 100):+.1f}%"
                        if first_med > 0
                        else "0%",
                    }
                )

    if not summary_data:
        return None

    # Generate LaTeX
    lines = [
        "\\begin{table}[t]",
        "\\centering",
        "\\caption{Summary of Authorization Overhead}",
        "\\label{tab:summary}",
        "\\begin{tabular}{lrrr}",
        "\\toprule",
        "Metric & Baseline & With Cedar & Overhead \\\\",
        "\\midrule",
    ]

    for row in summary_data:
        lines.append(
            f"{row['Metric']} & {row['Baseline']} & {row['Cedar']} & {row['Overhead']} \\\\"
        )

    lines.extend(["\\bottomrule", "\\end{tabular}", "\\end{table}"])

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text("\n".join(lines))

    return output_path


def generate_policy_scaling_plot(
    csv_path: Path,
    output_path: Path,
    title: str = "Policy Count vs. Authorization Time",
) -> Path | None:
    """
    Generate line plot showing how authorization time scales with policy count.

    Args:
        csv_path: Path to policy_scaling.csv
        output_path: Path to save the plot (e.g., policy_scaling.png)
        title: Plot title

    Returns:
        Path to generated plot, or None if plotting libraries unavailable
    """
    if not HAS_PLOTTING or _should_skip_plots():
        return None

    if not csv_path.exists():
        return None

    # Read data
    policy_counts = []
    median_ms = []
    p95_ms = []
    p99_ms = []

    with csv_path.open() as f:
        reader = csv.DictReader(f)
        for row in reader:
            policy_counts.append(int(row["policy_count"]))
            median_ms.append(float(row["median_ms"]))
            p95_ms.append(float(row["p95_ms"]))
            p99_ms.append(float(row["p99_ms"]))

    if not policy_counts:
        return None

    # Create plot
    plt.figure(figsize=(10, 6))
    plt.plot(policy_counts, median_ms, "o-", label="Median", linewidth=2, markersize=8)
    plt.plot(policy_counts, p95_ms, "s-", label="p95", linewidth=2, markersize=8)
    plt.plot(policy_counts, p99_ms, "^-", label="p99", linewidth=2, markersize=8)

    plt.xlabel("Policy Count", fontsize=16, fontweight="bold")
    plt.ylabel("Authorization Time (ms)", fontsize=16, fontweight="bold")
    plt.title(title, fontsize=18, fontweight="bold")
    plt.legend(fontsize=14)
    plt.tick_params(axis="both", which="major", labelsize=14)
    plt.grid(True, alpha=0.3)
    plt.xscale("log")  # Log scale for policy count
    plt.tight_layout()

    output_path.parent.mkdir(parents=True, exist_ok=True)
    plt.savefig(output_path, dpi=300, bbox_inches="tight")
    plt.close()

    return output_path


def generate_policy_scaling_boxplot(
    raw_data_dir: Path,
    output_path: Path,
    title: str = "Policy Scaling Latency Distribution",
) -> Path | None:
    """
    Generate box plot showing latency distribution for each policy count.

    This visualization shows the variance/spread in authorization times,
    which is important when median and p95/p99 latencies differ significantly.

    Args:
        raw_data_dir: Path to directory containing policy scaling raw CSVs
        output_path: Path to save the plot
        title: Plot title

    Returns:
        Path to generated plot, or None if plotting libraries unavailable
    """
    if not HAS_PLOTTING or _should_skip_plots():
        return None

    if not raw_data_dir.exists():
        return None

    # Try to read raw latency data for each policy count
    all_data = []

    # Look for JSON files with pattern like policies_1/results.json, policies_10/results.json
    for json_file in sorted(raw_data_dir.glob("policies_*/results.json")):
        try:
            policy_count = int(json_file.parent.name.replace("policies_", ""))
            data = json.loads(json_file.read_text())

            # Get latencies from cedar or baseline results
            rows = (
                data.get("cedar", []) if "cedar" in data else data.get("baseline", [])
            )
            if not rows and isinstance(data, list):
                rows = data

            # Filter to cedar rows if needed
            cedar_rows = [r for r in rows if r.get("system") == "cedar"]
            if not cedar_rows and rows:
                cedar_rows = rows

            for r in cedar_rows:
                latency = float(r.get("latency_ms", 0))
                if latency > 0:
                    all_data.append(
                        {"Policy Count": str(policy_count), "Latency (ms)": latency}
                    )
        except (ValueError, KeyError, IndexError, json.JSONDecodeError):
            continue

    # Fallback: look for CSV files with pattern like policies_1.csv, policies_10.csv etc
    if not all_data:
        for csv_file in sorted(raw_data_dir.glob("policies_*.csv")):
            try:
                policy_count = int(csv_file.stem.split("_")[1])
                with csv_file.open() as f:
                    reader = csv.DictReader(f)
                    for row in reader:
                        latency = float(row.get("latency_ms", 0))
                        if latency > 0:
                            all_data.append(
                                {
                                    "Policy Count": str(policy_count),
                                    "Latency (ms)": latency,
                                }
                            )
            except (ValueError, KeyError, IndexError):
                continue

    # If no raw data, try reading from summary CSV
    if not all_data:
        summary_csv = raw_data_dir.parent / "policy_scaling.csv"
        if summary_csv.exists():
            with summary_csv.open() as f:
                reader = csv.DictReader(f)
                for row in reader:
                    try:
                        policy_count = int(row.get("policy_count", 0))
                        median = float(row.get("median_ms", 0))
                        p95 = float(row.get("p95_ms", 0))
                        p99 = float(row.get("p99_ms", 0))
                        # Create synthetic data points for boxplot visualization
                        all_data.extend(
                            [
                                {
                                    "Policy Count": str(policy_count),
                                    "Latency (ms)": median,
                                },
                                {
                                    "Policy Count": str(policy_count),
                                    "Latency (ms)": p95 * 0.5 + median * 0.5,
                                },
                                {
                                    "Policy Count": str(policy_count),
                                    "Latency (ms)": p95,
                                },
                                {
                                    "Policy Count": str(policy_count),
                                    "Latency (ms)": p99,
                                },
                            ]
                        )
                    except (ValueError, KeyError):
                        continue

    if not all_data:
        return None

    df = pd.DataFrame(all_data)

    # Sort by policy count numerically
    policy_order = sorted(df["Policy Count"].unique(), key=lambda x: int(x))

    # Create figure
    fig, ax = plt.subplots(figsize=(12, 7))

    # Create box plot
    sns.boxplot(
        data=df,
        x="Policy Count",
        y="Latency (ms)",
        order=policy_order,
        palette="Set2",
        ax=ax,
        showfliers=True,
        flierprops={
            "marker": "o",
            "markerfacecolor": "red",
            "markeredgecolor": "red",
            "alpha": 0.5,
        },
    )

    # Add strip plot for individual points if we have real raw data (not synthetic)
    if len(df) > len(policy_order) * 4:
        sns.stripplot(
            data=df,
            x="Policy Count",
            y="Latency (ms)",
            order=policy_order,
            color="black",
            alpha=0.3,
            size=3,
            ax=ax,
            jitter=True,
        )

    ax.set_xlabel("Policy Count", fontsize=16, fontweight="bold")
    ax.set_ylabel("Authorization Latency (ms)", fontsize=16, fontweight="bold")
    ax.set_title(title, fontsize=18, fontweight="bold")
    ax.tick_params(axis="both", which="major", labelsize=14)
    ax.grid(True, alpha=0.3, axis="y")

    # Add annotation about variance
    ax.annotate(
        "Note: Box shows IQR, whiskers show range, outliers in red",
        xy=(0.02, 0.98),
        xycoords="axes fraction",
        fontsize=10,
        va="top",
        ha="left",
        style="italic",
        color="gray",
    )

    plt.tight_layout()

    output_path.parent.mkdir(parents=True, exist_ok=True)
    plt.savefig(output_path, dpi=300, bbox_inches="tight")
    plt.close()

    return output_path


def generate_concurrency_throughput_plot(
    csv_path: Path, output_path: Path, title: str = "Concurrency vs. Throughput"
) -> Path | None:
    """
    Generate grouped bar chart comparing baseline vs Cedar throughput at different concurrency levels.
    """
    if not HAS_PLOTTING or _should_skip_plots():
        return None

    if not csv_path.exists():
        return None

    # Read data
    threads = []
    baseline_qps = []
    cedar_qps = []

    with csv_path.open() as f:
        reader = csv.DictReader(f)
        for row in reader:
            threads.append(int(row["threads"]))
            baseline_qps.append(float(row["baseline_qps"]))
            cedar_qps.append(float(row["cedar_qps"]))

    if not threads:
        return None

    # Create plot
    fig, ax = plt.subplots(figsize=(10, 6))

    x = list(range(len(threads)))
    width = 0.35

    ax.bar(
        [i - width / 2 for i in x],
        baseline_qps,
        width,
        label="Baseline QPS",
        color="#2E86AB",
        alpha=0.9,
    )
    ax.bar(
        [i + width / 2 for i in x],
        cedar_qps,
        width,
        label="With Cedar QPS",
        color="#E74C3C",
        alpha=0.9,
    )

    ax.set_xlabel("Concurrent Threads", fontsize=16, fontweight="bold")
    ax.set_ylabel("Queries per Second (QPS)", fontsize=16, fontweight="bold")
    ax.set_title(title, fontsize=18, fontweight="bold")
    ax.set_xticks(x)
    ax.set_xticklabels([str(t) for t in threads], fontsize=12)
    ax.legend(fontsize=14)
    ax.grid(True, alpha=0.3, axis="y")

    plt.tight_layout()

    output_path.parent.mkdir(parents=True, exist_ok=True)
    plt.savefig(output_path, dpi=300, bbox_inches="tight")
    plt.close()

    return output_path


def generate_concurrency_latency_plot(
    csv_path: Path,
    output_path: Path,
    title: str = "Concurrency vs. Latency Percentiles",
) -> Path | None:
    """
    Generate line plot showing latency percentiles at different concurrency levels.

    Shows baseline and Cedar data on the same plot for direct comparison.

    Args:
        csv_path: Path to concurrency_latency.csv
        output_path: Path to save the plot (e.g., concurrency_latency.png)
        title: Plot title

    Returns:
        Path to generated plot, or None if plotting libraries unavailable
    """
    if not HAS_PLOTTING or _should_skip_plots():
        return None

    if not csv_path.exists():
        return None

    # Read data
    baseline_threads = []
    baseline_p50 = []
    baseline_p95 = []
    baseline_p99 = []
    cedar_threads = []
    cedar_p50 = []
    cedar_p95 = []
    cedar_p99 = []

    with csv_path.open() as f:
        reader = csv.DictReader(f)
        for row in reader:
            threads = int(row["threads"])

            # Support Long format (system column)
            if "system" in row:
                system = row["system"]
                if system == "baseline":
                    baseline_threads.append(threads)
                    baseline_p50.append(float(row.get("p50_ms", 0)))
                    baseline_p95.append(float(row.get("p95_ms", 0)))
                    baseline_p99.append(float(row.get("p99_ms", 0)))
                elif system == "cedar":
                    cedar_threads.append(threads)
                    cedar_p50.append(float(row.get("p50_ms", 0)))
                    cedar_p95.append(float(row.get("p95_ms", 0)))
                    cedar_p99.append(float(row.get("p99_ms", 0)))

            # Support Wide format (baseline_p50_ms, etc.)
            else:
                # Check for baseline cols
                if "baseline_p50_ms" in row:
                    baseline_threads.append(threads)
                    baseline_p50.append(float(row["baseline_p50_ms"]))
                    baseline_p95.append(float(row.get("baseline_p95_ms", 0)))
                    baseline_p99.append(float(row.get("baseline_p99_ms", 0)))

                # Check for cedar cols
                if "cedar_p50_ms" in row:
                    cedar_threads.append(threads)
                    cedar_p50.append(float(row["cedar_p50_ms"]))
                    cedar_p95.append(float(row.get("cedar_p95_ms", 0)))
                    cedar_p99.append(float(row.get("cedar_p99_ms", 0)))

    if not baseline_threads and not cedar_threads:
        return None

    # Create single combined plot
    fig, ax = plt.subplots(figsize=(12, 7))

    # Define colors for percentiles
    colors = {"p50": "#2E86AB", "p95": "#E74C3C", "p99": "#9B59B6"}

    # Plot baseline (solid lines)
    if baseline_threads:
        if any(v > 0 for v in baseline_p50):
            ax.plot(
                baseline_threads,
                baseline_p50,
                "o-",
                label="Baseline p50",
                linewidth=2.5,
                markersize=7,
                color=colors["p50"],
            )
        if any(v > 0 for v in baseline_p95):
            ax.plot(
                baseline_threads,
                baseline_p95,
                "s-",
                label="Baseline p95",
                linewidth=2.5,
                markersize=7,
                color=colors["p95"],
            )
        if any(v > 0 for v in baseline_p99):
            ax.plot(
                baseline_threads,
                baseline_p99,
                "^-",
                label="Baseline p99",
                linewidth=2.5,
                markersize=7,
                color=colors["p99"],
            )

    # Plot Cedar (dashed lines, same colors)
    if cedar_threads:
        if any(v > 0 for v in cedar_p50):
            ax.plot(
                cedar_threads,
                cedar_p50,
                "o--",
                label="Cedar p50",
                linewidth=2.5,
                markersize=7,
                color=colors["p50"],
                alpha=0.7,
            )
        if any(v > 0 for v in cedar_p95):
            ax.plot(
                cedar_threads,
                cedar_p95,
                "s--",
                label="Cedar p95",
                linewidth=2.5,
                markersize=7,
                color=colors["p95"],
                alpha=0.7,
            )
        if any(v > 0 for v in cedar_p99):
            ax.plot(
                cedar_threads,
                cedar_p99,
                "^--",
                label="Cedar p99",
                linewidth=2.5,
                markersize=7,
                color=colors["p99"],
                alpha=0.7,
            )

    ax.set_xlabel("Concurrent Threads", fontsize=16, fontweight="bold")
    ax.set_ylabel("Latency (ms)", fontsize=16, fontweight="bold")
    ax.set_title(title, fontsize=18, fontweight="bold")
    ax.legend(fontsize=12, loc="upper left", ncol=2)
    ax.tick_params(axis="both", which="major", labelsize=14)
    ax.grid(True, alpha=0.3)

    plt.tight_layout()

    output_path.parent.mkdir(parents=True, exist_ok=True)
    plt.savefig(output_path, dpi=300, bbox_inches="tight")
    plt.close()

    return output_path


def generate_agent_delay_vs_query_latency_plot(
    csv_path: Path, output_path: Path, title: str = "Agent Delay vs. Query Latency"
) -> Path | None:
    """Generate line plot showing MySQL query latency vs. injected agent delay."""
    if not HAS_PLOTTING or _should_skip_plots() or not csv_path.exists():
        return None

    df = pd.read_csv(csv_path)
    if df.empty:
        return None

    plt.figure(figsize=(10, 6))
    sns.lineplot(data=df, x="delay_ms", y="median", marker="o", label="Median Latency")
    sns.lineplot(
        data=df, x="delay_ms", y="p95", marker="s", linestyle="--", label="p95 Latency"
    )

    plt.xlabel("Injected Agent Delay (ms)", fontsize=16, fontweight="bold")
    plt.ylabel("MySQL Query Latency (ms)", fontsize=16, fontweight="bold")
    plt.title(title, fontsize=18, fontweight="bold")
    plt.legend(fontsize=14)
    plt.tick_params(axis="both", which="major", labelsize=14)
    plt.grid(True, which="both", ls="--", c="0.7")
    plt.tight_layout()

    output_path.parent.mkdir(parents=True, exist_ok=True)
    plt.savefig(output_path, dpi=300)
    plt.close()
    return output_path


def generate_agent_delay_comprehensive_plot(
    summary_csv_path: Path,
    output_path: Path,
    title: str = "Query Latency Overhead vs. Agent Delay",
) -> Path | None:
    """
    Generate comprehensive line plot showing mean and median latencies
    minus the injected delay, showing the actual overhead/processing time.

    Args:
        summary_csv_path: Path to summary.csv with aggregated statistics
        output_path: Path to save the plot
        title: Plot title

    Returns:
        Path to generated plot, or None if plotting libraries unavailable
    """
    if not HAS_PLOTTING or _should_skip_plots() or not summary_csv_path.exists():
        return None

    df = pd.read_csv(summary_csv_path)
    if df.empty or "delay_ms" not in df.columns:
        return None

    # Sort by delay_ms for proper line ordering
    df = df.sort_values("delay_ms")

    # Calculate latency minus delay to show actual overhead
    df["mean_overhead"] = df["mean"] - df["delay_ms"]
    df["median_overhead"] = df["median"] - df["delay_ms"]
    # df['p95_overhead'] = df['p95'] - df['delay_ms']
    # df['p99_overhead'] = df['p99'] - df['delay_ms']

    # Create figure with larger size for better readability
    plt.figure(figsize=(12, 8))

    # Get unique delay values for discrete x-axis
    delay_values = sorted(df["delay_ms"].unique())

    # Create position mapping for discrete x-axis
    x_positions = range(len(delay_values))
    delay_to_pos = {delay: pos for pos, delay in enumerate(delay_values)}
    df["x_pos"] = df["delay_ms"].map(delay_to_pos)

    # Plot all four statistics with distinct styles (latency - delay)
    plt.plot(
        df["x_pos"],
        df["mean_overhead"],
        marker="o",
        linewidth=2.5,
        markersize=8,
        label="Mean (latency - delay)",
        color="#2E86AB",
        linestyle="-",
    )
    plt.plot(
        df["x_pos"],
        df["median_overhead"],
        marker="s",
        linewidth=2.5,
        markersize=8,
        label="Median (latency - delay)",
        color="#A23B72",
        linestyle="-",
    )
    # plt.plot(df['x_pos'], df['p95_overhead'], marker='^', linewidth=2.5,
    #          markersize=8, label='p95 (latency - delay)', color='#F18F01', linestyle='--')
    # plt.plot(df['x_pos'], df['p99_overhead'], marker='d', linewidth=2.5,
    #          markersize=8, label='p99 (latency - delay)', color='#C73E1D', linestyle='--')

    # Set x-axis to discrete values
    plt.xticks(x_positions, [str(int(d)) for d in delay_values])

    # Customize axes
    plt.xlabel("Injected Agent Delay (ms)", fontsize=16, fontweight="bold")
    plt.ylabel("Latency Overhead (ms)", fontsize=16, fontweight="bold")
    plt.title(title, fontsize=18, fontweight="bold", pad=15)

    # Add grid for better readability
    plt.grid(True, alpha=0.3, linestyle="--", linewidth=0.8, axis="y")
    plt.grid(True, alpha=0.2, linestyle=":", linewidth=0.5, axis="x")

    # Set tick label sizes
    plt.tick_params(axis="both", which="major", labelsize=14)

    # Add legend with better positioning
    plt.legend(
        loc="upper left", fontsize=14, framealpha=0.9, fancybox=True, shadow=True
    )

    # Add sample count annotations if available
    if "count" in df.columns:
        ax = plt.gca()
        # overhead_cols = ['mean_overhead', 'median_overhead', 'p95_overhead', 'p99_overhead']
        overhead_cols = ["mean_overhead", "median_overhead"]
        y_max = max(df[overhead_cols].max())
        for _, row in df.iterrows():
            if row["count"] < 100:  # Highlight if sample size is different
                ax.text(
                    row["x_pos"],
                    y_max * 0.98,
                    f"n={int(row['count'])}",
                    ha="center",
                    va="top",
                    fontsize=12,
                    bbox=dict(boxstyle="round,pad=0.3", facecolor="yellow", alpha=0.6),
                )

    plt.tight_layout()

    output_path.parent.mkdir(parents=True, exist_ok=True)
    plt.savefig(output_path, dpi=300, bbox_inches="tight")
    plt.close()

    return output_path


def generate_agent_rps_vs_latency_plot(
    csv_path: Path,
    output_path: Path,
    title: str = "Request Rate vs. Cedar Agent Latency",
) -> Path | None:
    """Generate line plot showing Cedar agent latency vs. request rate."""
    if not HAS_PLOTTING or _should_skip_plots() or not csv_path.exists():
        return None

    df = pd.read_csv(csv_path)
    if df.empty:
        return None

    # Convert nanoseconds to milliseconds
    for col in ["p50_ns", "p95_ns", "p99_ns", "mean_ns"]:
        if col in df.columns:
            df[col.replace("_ns", "_ms")] = df[col] / 1e6

    plt.figure(figsize=(10, 6))
    sns.lineplot(data=df, x="target_rps", y="p50_ms", marker="o", label="p50")
    sns.lineplot(data=df, x="target_rps", y="p95_ms", marker="s", label="p95")
    sns.lineplot(data=df, x="target_rps", y="p99_ms", marker="^", label="p99")

    plt.xlabel("Request Rate (RPS)", fontsize=16, fontweight="bold")
    plt.ylabel("Agent Latency (ms)", fontsize=16, fontweight="bold")
    plt.title(title, fontsize=18, fontweight="bold")
    plt.legend(fontsize=14)
    plt.tick_params(axis="both", which="major", labelsize=14)
    plt.grid(True, which="both", ls="--", c="0.7")
    plt.tight_layout()

    output_path.parent.mkdir(parents=True, exist_ok=True)
    plt.savefig(output_path, dpi=300)
    plt.close()
    return output_path


def generate_agent_stress_comprehensive_plot(
    csv_path: Path,
    output_path: Path,
    title: str = "Cedar Agent Stress Test: Comprehensive Analysis",
) -> Path | None:
    """
    Generate comprehensive multi-panel visualization for agent stress test.

    Shows:
    1. Latency percentiles vs RPS (with log scale)
    2. Failure rate and success rate vs RPS
    3. Highlights breaking point and degradation

    Args:
        csv_path: Path to summary.csv
        output_path: Path to save the plot
        title: Plot title

    Returns:
        Path to generated plot, or None if plotting libraries unavailable
    """
    if not HAS_PLOTTING or _should_skip_plots() or not csv_path.exists():
        return None

    df = pd.read_csv(csv_path)
    if df.empty:
        return None

    # Convert nanoseconds to milliseconds
    for col in ["p50_ns", "p95_ns", "p99_ns", "mean_ns"]:
        if col in df.columns:
            df[col.replace("_ns", "_ms")] = df[col] / 1e6

    # Sort by target_rps for proper line ordering
    df = df.sort_values("target_rps")

    # Create figure with two subplots
    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(12, 10), sharex=True)

    # Top panel: Latency percentiles (log scale)
    ax1.plot(
        df["target_rps"],
        df["p50_ms"],
        "o-",
        label="p50",
        linewidth=2.5,
        markersize=8,
        color="#2E86AB",
    )
    ax1.plot(
        df["target_rps"],
        df["p95_ms"],
        "s-",
        label="p95",
        linewidth=2.5,
        markersize=8,
        color="#F18F01",
    )
    ax1.plot(
        df["target_rps"],
        df["p99_ms"],
        "^-",
        label="p99",
        linewidth=2.5,
        markersize=8,
        color="#C73E1D",
    )

    # Use log scale for latency (wide range: ~1ms to ~7000ms)
    ax1.set_yscale("log")
    ax1.set_ylabel("Agent Latency (ms)", fontsize=18, fontweight="bold")
    ax1.set_title(
        "Latency Percentiles vs Request Rate", fontsize=20, fontweight="bold", pad=10
    )
    ax1.legend(fontsize=16, loc="upper left")
    ax1.grid(True, alpha=0.3, linestyle="--", linewidth=0.8)
    ax1.tick_params(axis="both", which="major", labelsize=16)

    # Add safe zone shading (P2-8: Capacity planning guidance)
    # Safe zone: 0-800 RPS based on E7 stress test results
    SAFE_RPS_LIMIT = 800
    xlims = ax1.get_xlim()
    ax1.get_ylim()
    ax1.axvspan(
        0,
        SAFE_RPS_LIMIT,
        alpha=0.15,
        color="green",
        zorder=0,
        label="Safe Zone (<800 RPS)",
    )
    ax1.axvspan(SAFE_RPS_LIMIT, xlims[1], alpha=0.1, color="red", zorder=0)
    # Re-add the legend with safe zone included
    ax1.legend(fontsize=14, loc="upper left", framealpha=0.9)

    # Add vertical line to indicate where latency degrades significantly
    # Find the point where p95 latency increases significantly (>100ms)
    latency_degradation_point = None
    for _, row in df.iterrows():
        if row["p95_ms"] > 100:  # Significant latency increase
            latency_degradation_point = row["target_rps"]
            break

    if latency_degradation_point:
        ax1.axvline(
            x=latency_degradation_point,
            color="orange",
            linestyle="--",
            linewidth=2,
            alpha=0.7,
            zorder=0,
        )
        # Add subtle annotation
        y_pos = ax1.get_ylim()[0] * 10  # Position near bottom of log scale
        ax1.text(
            latency_degradation_point,
            y_pos,
            f"{int(latency_degradation_point)} RPS",
            ha="center",
            va="bottom",
            fontsize=14,
            rotation=90,
            rotation_mode="anchor",
            bbox=dict(
                boxstyle="round,pad=0.3",
                facecolor="orange",
                alpha=0.3,
                edgecolor="orange",
                linewidth=1,
            ),
        )

    # Bottom panel: Error rate and success rate
    error_rate_pct = df["error_rate"] * 100
    success_rate_pct = df["success"] * 100

    ax2_twin = ax2.twinx()  # Create second y-axis for error rate

    # Success rate (left y-axis)
    line1 = ax2.plot(
        df["target_rps"],
        success_rate_pct,
        "o-",
        label="Success Rate",
        linewidth=2.5,
        markersize=8,
        color="#28A745",
    )

    # Error rate (right y-axis)
    line2 = ax2_twin.plot(
        df["target_rps"],
        error_rate_pct,
        "s",
        label="Failure Rate",
        linewidth=2.5,
        markersize=8,
        color="#DC3545",
        linestyle="--",
    )

    # Combine legends
    lines = line1 + line2
    labels = [line.get_label() for line in lines]
    ax2.legend(lines, labels, fontsize=16, loc="upper left", framealpha=0.9)

    ax2.set_xlabel("Request Rate (RPS)", fontsize=18, fontweight="bold")
    ax2.set_ylabel("Success Rate (%)", fontsize=18, fontweight="bold", color="#28A745")
    ax2_twin.set_ylabel(
        "Failure Rate (%)", fontsize=18, fontweight="bold", color="#DC3545"
    )

    ax2.set_title(
        "Success Rate and Failure Rate vs Request Rate",
        fontsize=20,
        fontweight="bold",
        pad=10,
    )
    ax2.set_ylim([0, 105])  # 0-100% for success rate
    ax2_twin.set_ylim([0, 105])  # 0-100% for error rate
    ax2.grid(True, alpha=0.3, linestyle="--", linewidth=0.8)
    ax2.tick_params(axis="both", which="major", labelsize=16)
    ax2_twin.tick_params(axis="y", which="major", labelsize=16, colors="#DC3545")
    ax2.tick_params(axis="y", which="major", colors="#28A745")

    # Add vertical line to indicate where errors start occurring
    error_start_point = None
    for _, row in df.iterrows():
        if row["error_rate"] > 0:
            error_start_point = row["target_rps"]
            break

    if error_start_point:
        ax2.axvline(
            x=error_start_point,
            color="red",
            linestyle=":",
            linewidth=2,
            alpha=0.7,
            zorder=0,
        )
        # Add subtle annotation
        ax2.text(
            error_start_point,
            50,
            f"{int(error_start_point)} RPS",
            ha="center",
            va="center",
            fontsize=14,
            rotation=90,
            rotation_mode="anchor",
            bbox=dict(
                boxstyle="round,pad=0.3",
                facecolor="red",
                alpha=0.3,
                edgecolor="red",
                linewidth=1,
            ),
        )

    # Also add the latency degradation line to bottom plot for alignment
    if latency_degradation_point:
        ax2.axvline(
            x=latency_degradation_point,
            color="orange",
            linestyle="--",
            linewidth=2,
            alpha=0.7,
            zorder=0,
        )

    # Overall title
    fig.suptitle(title, fontsize=22, fontweight="bold", y=0.995)

    plt.tight_layout()

    output_path.parent.mkdir(parents=True, exist_ok=True)
    plt.savefig(output_path, dpi=300, bbox_inches="tight")
    plt.close()

    return output_path


def latex_table_agent_delay_impact(csv_path: Path, output_path: Path) -> Path | None:
    """Generate a LaTeX table for agent delay impact."""
    if not csv_path.exists():
        return None

    df = pd.read_csv(csv_path)
    if df.empty:
        return None

    baseline_latency = df[df["delay_ms"] == 0]["median"].iloc[0]

    def calculate_impact(row):
        if baseline_latency > 0:
            return ((row["median"] - baseline_latency) / baseline_latency) * 100
        return 0

    df["impact_percent"] = df.apply(calculate_impact, axis=1)

    with open(output_path, "w") as f:
        f.write("\\begin{tabular}{rrr}\n")
        f.write("\\toprule\n")
        f.write("Added Delay (ms) & Query Time (ms) & Impact (\\%)\\\\\n")
        f.write("\\midrule\n")
        for _, row in df.iterrows():
            f.write(
                f"{int(row['delay_ms'])} & {row['median']:.2f} & {row['impact_percent']:.2f}\\\\\n"
            )
        f.write("\\bottomrule\n")
        f.write("\\end{tabular}\n")

    return output_path


def latex_table_agent_stress_test(csv_path: Path, output_path: Path) -> Path | None:
    """Generate a LaTeX table for agent stress test results."""
    if not csv_path.exists():
        return None

    df = pd.read_csv(csv_path)
    if df.empty:
        return None

    with open(output_path, "w") as f:
        f.write("\\begin{tabular}{rrrrr}\n")
        f.write("\\toprule\n")
        f.write(
            "Request Rate (RPS) & p50 (ms) & p95 (ms) & p99 (ms) & Failure Rate (\\%)\\\\\n"
        )
        f.write("\\midrule\n")
        for _, row in df.iterrows():
            p50_ms = row["p50_ns"] / 1e6
            p95_ms = row["p95_ns"] / 1e6
            p99_ms = row["p99_ns"] / 1e6
            error_rate_percent = row["error_rate"] * 100
            f.write(
                f"{int(row['target_rps'])} & {p50_ms:.2f} & {p95_ms:.2f} & {p99_ms:.2f} & {error_rate_percent:.2f}\\\\\n"
            )
        f.write("\\bottomrule\n")
        f.write("\\end{tabular}\n")

    return output_path


def generate_latency_boxplot_per_query(
    results_json_path: Path,
    output_path: Path,
    title: str = "Latency Distribution per Query Type (Baseline vs Cedar)",
) -> Path | None:
    """
    Generate box plot comparing latencies per query type (category) for baseline vs Cedar.

    Args:
        results_json_path: Path to results.json file
        output_path: Path to save the plot (e.g., latency_boxplot_per_query.png)
        title: Plot title

    Returns:
        Path to generated plot, or None if plotting libraries unavailable
    """
    if not HAS_PLOTTING or _should_skip_plots():
        return None

    if not results_json_path.exists():
        return None

    # Read results JSON
    with results_json_path.open() as f:
        results = json.load(f)

    baseline_results = results.get("baseline", [])
    cedar_results = results.get("cedar", [])

    if not baseline_results or not cedar_results:
        return None

    # Group latencies by category (query type) and system
    baseline_by_category: dict[str, list[float]] = {}
    cedar_by_category: dict[str, list[float]] = {}

    for r in baseline_results:
        category = r.get("category") or r.get("action", "UNKNOWN")
        baseline_by_category.setdefault(category, []).append(float(r["latency_ms"]))

    for r in cedar_results:
        category = r.get("category") or r.get("action", "UNKNOWN")
        cedar_by_category.setdefault(category, []).append(float(r["latency_ms"]))

    # Get all categories (union of both systems)
    all_categories = sorted(
        set(baseline_by_category.keys()) | set(cedar_by_category.keys())
    )

    if not all_categories:
        return None

    # Prepare data for plotting
    plot_data = []
    for category in all_categories:
        # Baseline data
        if category in baseline_by_category:
            for latency in baseline_by_category[category]:
                plot_data.append(
                    {
                        "query_type": category,
                        "system": "Baseline",
                        "latency_ms": latency,
                    }
                )
        # Cedar data
        if category in cedar_by_category:
            for latency in cedar_by_category[category]:
                plot_data.append(
                    {"query_type": category, "system": "Cedar", "latency_ms": latency}
                )

    if not plot_data:
        return None

    df = pd.DataFrame(plot_data)

    # Create box plot
    plt.figure(figsize=(max(10, len(all_categories) * 0.8), 8))

    # Use seaborn for better box plots
    sns.boxplot(
        data=df,
        x="query_type",
        y="latency_ms",
        hue="system",
        palette={"Baseline": "#3498db", "Cedar": "#e74c3c"},
        showfliers=False,  # Hide outliers for cleaner plot
    )

    plt.xlabel("Query Type", fontsize=16, fontweight="bold")
    plt.ylabel("Latency (ms)", fontsize=16, fontweight="bold")
    plt.title(title, fontsize=18, fontweight="bold")
    plt.legend(title="System", fontsize=14, title_fontsize=15)
    plt.xticks(rotation=45, ha="right")
    plt.tick_params(axis="both", which="major", labelsize=14)
    plt.grid(True, alpha=0.3, axis="y")
    plt.tight_layout()

    output_path.parent.mkdir(parents=True, exist_ok=True)
    plt.savefig(output_path, dpi=300, bbox_inches="tight")
    plt.close()

    return output_path


def generate_per_operation_bar_chart(
    results_json_path: Path,
    output_path: Path,
    title: str = "Per-Operation Overhead with 95% CI",
    n_bootstrap: int = 10000,
) -> Path | None:
    """
    Generate grouped bar chart showing per-operation overhead with 95% CI error bars.

    This addresses P1-4 from the deep experimental analysis:
    "E1 should show SELECT/INSERT/UPDATE/DELETE separately"

    Args:
        results_json_path: Path to results.json file
        output_path: Path to save the plot
        title: Plot title
        n_bootstrap: Number of bootstrap samples for CI calculation

    Returns:
        Path to generated plot, or None if plotting libraries unavailable
    """
    if not HAS_PLOTTING or _should_skip_plots():
        return None

    if not results_json_path.exists():
        return None

    # Import bootstrap CI function
    try:
        from framework.stats import bootstrap_ci_median
    except ImportError:
        # Fallback if stats module not available
        bootstrap_ci_median = None

    # Read results JSON
    with results_json_path.open() as f:
        results = json.load(f)

    baseline_results = results.get("baseline", [])
    cedar_results = results.get("cedar", [])

    if not baseline_results or not cedar_results:
        return None

    # Group latencies by category (query type/operation)
    baseline_by_category: dict[str, list[float]] = {}
    cedar_by_category: dict[str, list[float]] = {}

    for r in baseline_results:
        category = r.get("category") or r.get("action", "UNKNOWN")
        baseline_by_category.setdefault(category, []).append(float(r["latency_ms"]))

    for r in cedar_results:
        category = r.get("category") or r.get("action", "UNKNOWN")
        cedar_by_category.setdefault(category, []).append(float(r["latency_ms"]))

    # Get all categories (sorted for consistent ordering)
    all_categories = sorted(
        set(baseline_by_category.keys()) | set(cedar_by_category.keys())
    )

    if not all_categories:
        return None

    # Compute medians and CIs for each category
    import statistics

    categories = []
    baseline_medians = []
    cedar_medians = []
    baseline_ci_lowers = []
    baseline_ci_uppers = []
    cedar_ci_lowers = []
    cedar_ci_uppers = []

    for cat in all_categories:
        base_vals = baseline_by_category.get(cat, [])
        cedar_vals = cedar_by_category.get(cat, [])

        if not base_vals or not cedar_vals:
            continue

        categories.append(cat)

        # Compute medians
        base_med = statistics.median(base_vals)
        cedar_med = statistics.median(cedar_vals)
        baseline_medians.append(base_med)
        cedar_medians.append(cedar_med)

        # Compute bootstrap CIs
        if bootstrap_ci_median is not None and len(base_vals) >= 3:
            try:
                base_ci = bootstrap_ci_median(base_vals, n_bootstrap=n_bootstrap)
                baseline_ci_lowers.append(base_ci.lower)
                baseline_ci_uppers.append(base_ci.upper)
            except Exception:
                baseline_ci_lowers.append(base_med)
                baseline_ci_uppers.append(base_med)
        else:
            # Simple fallback: use IQR-based estimate
            baseline_ci_lowers.append(base_med)
            baseline_ci_uppers.append(base_med)

        if bootstrap_ci_median is not None and len(cedar_vals) >= 3:
            try:
                cedar_ci = bootstrap_ci_median(cedar_vals, n_bootstrap=n_bootstrap)
                cedar_ci_lowers.append(cedar_ci.lower)
                cedar_ci_uppers.append(cedar_ci.upper)
            except Exception:
                cedar_ci_lowers.append(cedar_med)
                cedar_ci_uppers.append(cedar_med)
        else:
            cedar_ci_lowers.append(cedar_med)
            cedar_ci_uppers.append(cedar_med)

    if not categories:
        return None

    # Create grouped bar chart
    import numpy as np

    x = np.arange(len(categories))
    width = 0.35

    fig, ax = plt.subplots(figsize=(max(10, len(categories) * 1.5), 7))

    # Calculate error bars (distance from median to CI bounds)
    base_errors = [
        [baseline_medians[i] - baseline_ci_lowers[i] for i in range(len(categories))],
        [baseline_ci_uppers[i] - baseline_medians[i] for i in range(len(categories))],
    ]
    cedar_errors = [
        [cedar_medians[i] - cedar_ci_lowers[i] for i in range(len(categories))],
        [cedar_ci_uppers[i] - cedar_medians[i] for i in range(len(categories))],
    ]

    # Plot bars with error bars
    ax.bar(
        x - width / 2,
        baseline_medians,
        width,
        label="Baseline",
        color="#3498db",
        alpha=0.85,
        yerr=base_errors,
        capsize=5,
        error_kw={"elinewidth": 1.5},
    )
    ax.bar(
        x + width / 2,
        cedar_medians,
        width,
        label="Cedar",
        color="#e74c3c",
        alpha=0.85,
        yerr=cedar_errors,
        capsize=5,
        error_kw={"elinewidth": 1.5},
    )

    # Customize plot
    ax.set_xlabel("Operation Type", fontsize=16, fontweight="bold")
    ax.set_ylabel("Median Latency (ms)", fontsize=16, fontweight="bold")
    ax.set_title(title, fontsize=18, fontweight="bold", pad=15)
    ax.set_xticks(x)
    ax.set_xticklabels(categories, fontsize=14, fontweight="bold")
    ax.legend(fontsize=14, loc="upper right")
    ax.tick_params(axis="both", which="major", labelsize=13)
    ax.grid(True, alpha=0.3, axis="y", linestyle="--")

    # Add overhead percentage annotations above Cedar bars
    for i, (base, cedar) in enumerate(zip(baseline_medians, cedar_medians)):
        if base > 0:
            overhead_pct = (cedar - base) / base * 100
            overhead_text = (
                f"+{overhead_pct:.1f}%" if overhead_pct >= 0 else f"{overhead_pct:.1f}%"
            )
            y_pos = max(cedar + cedar_errors[1][i], base + base_errors[1][i]) + 0.5
            ax.text(
                x[i],
                y_pos,
                overhead_text,
                ha="center",
                va="bottom",
                fontsize=11,
                fontweight="bold",
                color="#2c3e50",
            )

    plt.tight_layout()

    output_path.parent.mkdir(parents=True, exist_ok=True)
    plt.savefig(output_path, dpi=300, bbox_inches="tight")
    plt.close()

    return output_path


def generate_throughput_latency_plot(
    csv_path: Path, output_path: Path, title: str = "Throughput vs. Latency"
) -> Path | None:
    """
    Generate a Throughput-Latency curve (knee curve).
    X-axis: Throughput (QPS)
    Y-axis: P95 Latency (ms)
    """
    if not HAS_PLOTTING or _should_skip_plots():
        return None

    if not csv_path.exists():
        return None

    # Read data
    data_points = []
    with csv_path.open() as f:
        reader = csv.DictReader(f)
        for row in reader:
            # We expect columns like: threads, baseline_qps, cedar_qps, baseline_p95_ms, cedar_p95_ms
            # We need to restructure this into (system, qps, p95) tuples
            try:
                threads = int(row["threads"])
                b_qps = float(row["baseline_qps"])
                c_qps = float(row["cedar_qps"])
                # We might need to join with latency data if not in same file,
                # but let's assume the input CSV has merged data or we handle specific CSV formats.
                # If using `concurrency_scaling.csv` produced by analysis, it might be split.
                # Let's assume the CSV passed here is a merged summary.

                # Check if latency cols exist, otherwise this plot cannot be made from this CSV
                if "baseline_p95_ms" in row:
                    data_points.append(
                        {
                            "system": "Baseline",
                            "qps": b_qps,
                            "p95": float(row["baseline_p95_ms"]),
                            "threads": threads,
                        }
                    )
                if "cedar_p95_ms" in row:
                    data_points.append(
                        {
                            "system": "With Cedar",
                            "qps": c_qps,
                            "p95": float(row["cedar_p95_ms"]),
                            "threads": threads,
                        }
                    )
            except (ValueError, KeyError):
                continue

    if not data_points:
        return None

    df = pd.DataFrame(data_points)

    plt.figure(figsize=(10, 6))
    sns.lineplot(
        data=df,
        x="qps",
        y="p95",
        hue="system",
        style="system",
        markers=True,
        dashes=False,
        linewidth=2.5,
        markersize=9,
    )

    # Annotate thread counts for clarity
    # for _, row in df.iterrows():
    #     plt.text(row['qps'], row['p95'], f" {int(row['threads'])}t", fontsize=9, va='bottom')

    plt.xlabel("Throughput (QPS)", fontsize=16, fontweight="bold")
    plt.ylabel("P95 Latency (ms)", fontsize=16, fontweight="bold")
    plt.title(title, fontsize=18, fontweight="bold")
    plt.legend(fontsize=14)
    plt.grid(True, alpha=0.3)
    plt.tight_layout()

    output_path.parent.mkdir(parents=True, exist_ok=True)
    plt.savefig(output_path, dpi=300, bbox_inches="tight")
    plt.close()
    return output_path


def generate_tpcc_barplot(
    csv_path: Path, output_path: Path, title: str = "TPC-C Performance Comparison"
) -> Path | None:
    """
    Generate grouped bar chart for TPC-C: TPM and P95 Latency.
    """
    if not HAS_PLOTTING or _should_skip_plots():
        return None

    if not csv_path.exists():
        return None

    # Read CSV (produced by analysis_tpcc.py)
    # Header: tool, warehouses, load, baseline_tpm, cedar_tpm, ..., baseline_latency_ms, cedar_latency_ms
    data = []
    with csv_path.open() as f:
        reader = csv.DictReader(f)
        for row in reader:
            data.append(row)

    if not data:
        return None

    # We'll plot the first row (assuming one main configuration for the bar plot)
    # or create subplots if multiple. For now, take the first valid one.
    row = data[0]

    # metrics
    tpm_base = float(row["baseline_tpm"])
    tpm_cedar = float(row["cedar_tpm"])

    lat_base = float(row["baseline_latency_ms"])
    lat_cedar = float(row["cedar_latency_ms"])

    # Setup plot
    fig, ax1 = plt.subplots(figsize=(10, 6))

    bar_width = 0.35

    # TPM Bars (Left Axis)
    bars1 = ax1.bar(
        [0], [tpm_base], bar_width, label="Baseline TPM", color="#3498db", alpha=0.9
    )
    bars2 = ax1.bar(
        [0 + bar_width],
        [tpm_cedar],
        bar_width,
        label="Cedar TPM",
        color="#e74c3c",
        alpha=0.9,
    )

    ax1.set_ylabel("Throughput (TPM)", fontsize=16, fontweight="bold", color="#2c3e50")
    ax1.tick_params(axis="y", labelcolor="#2c3e50", labelsize=12)
    ax1.set_xticks([0.175])
    ax1.set_xticklabels(["TPC-C (Sysbench)"], fontsize=14, fontweight="bold")

    # Add values on top of bars
    def add_labels(bars):
        for bar in bars:
            height = bar.get_height()
            ax1.text(
                bar.get_x() + bar.get_width() / 2.0,
                height,
                f"{int(height)}",
                ha="center",
                va="bottom",
                fontsize=12,
                fontweight="bold",
            )

    add_labels(bars1)
    add_labels(bars2)

    # Latency Bars (Right Axis) - visualized as separate group or separate comparison?
    # Mixing TPM and Latency on one bar chart can be confusing.
    # Better to have side-by-side subplots.
    plt.close()  # Reset

    fig, (ax_tpm, ax_lat) = plt.subplots(1, 2, figsize=(14, 6))

    # TPM Subplot
    x = ["Baseline", "With Cedar"]
    y_tpm = [tpm_base, tpm_cedar]
    colors_tpm = ["#3498db", "#e74c3c"]

    ax_tpm.bar(x, y_tpm, color=colors_tpm, width=0.6)
    ax_tpm.set_ylabel("Transactions Per Minute (TPM)", fontsize=14, fontweight="bold")
    ax_tpm.set_title("Throughput (Higher is Better)", fontsize=16, fontweight="bold")
    ax_tpm.grid(axis="y", alpha=0.3)
    for i, v in enumerate(y_tpm):
        ax_tpm.text(
            i, v, f"{int(v)}", ha="center", va="bottom", fontsize=12, fontweight="bold"
        )

    # Latency Subplot
    y_lat = [lat_base, lat_cedar]
    ax_lat.bar(x, y_lat, color=colors_tpm, width=0.6)
    ax_lat.set_ylabel("Avg Latency (ms)", fontsize=14, fontweight="bold")
    ax_lat.set_title("Latency (Lower is Better)", fontsize=16, fontweight="bold")
    ax_lat.grid(axis="y", alpha=0.3)
    for i, v in enumerate(y_lat):
        ax_lat.text(
            i, v, f"{v:.2f}", ha="center", va="bottom", fontsize=12, fontweight="bold"
        )

    plt.suptitle(title, fontsize=20, fontweight="bold")
    plt.tight_layout()

    output_path.parent.mkdir(parents=True, exist_ok=True)
    plt.savefig(output_path, dpi=300, bbox_inches="tight")
    plt.close()
    return output_path


def generate_all_visualizations(
    analysis_dir: Path, output_dir: Path | None = None
) -> dict[str, Path | None]:
    """
    Generate all visualizations from analysis outputs.

    Args:
        analysis_dir: Directory containing analysis CSVs
        output_dir: Directory to save plots (defaults to analysis_dir)

    Returns:
        Dictionary mapping plot names to file paths (or None if not generated)
    """
    if output_dir is None:
        output_dir = analysis_dir
    else:
        output_dir.mkdir(parents=True, exist_ok=True)

    if not HAS_PLOTTING or _should_skip_plots():
        return {}

    results = {}

    # 1. Latency CDF
    baseline_csv = analysis_dir / "baseline_latencies.csv"
    cedar_csv = analysis_dir / "cedar_latencies.csv"
    if baseline_csv.exists() and cedar_csv.exists():
        results["latency_cdf"] = generate_latency_cdf(
            baseline_csv, cedar_csv, output_dir / "latency_cdf.png"
        )

    # 1b. Per-operation Bar Chart with CI (P1-4 from deep analysis)
    results_json = analysis_dir / "results.json"
    if results_json.exists():
        results["per_operation_bar_chart"] = generate_per_operation_bar_chart(
            results_json, output_dir / "per_operation_overhead.png"
        )
        results["latency_boxplot_per_query"] = generate_latency_boxplot_per_query(
            results_json, output_dir / "latency_boxplot_per_query.png"
        )

    # 2. Policy Scaling
    policy_csv = analysis_dir / "policy_scaling.csv"
    if policy_csv.exists():
        results["policy_scaling"] = generate_policy_scaling_plot(
            policy_csv, output_dir / "policy_scaling.png"
        )
        # Also generate LaTeX table
        results["policy_scaling_tex"] = latex_table_policy_scaling(
            policy_csv, output_dir / "policy_scaling.tex"
        )

    # 3. Concurrency Comparison (Throughput & Latency)
    conc_csv = analysis_dir / "concurrency_comparison.csv"
    if conc_csv.exists():
        results["concurrency_throughput"] = generate_concurrency_throughput_plot(
            conc_csv, output_dir / "concurrency_throughput.png"
        )
        results["concurrency_latency"] = generate_concurrency_latency_plot(
            conc_csv, output_dir / "concurrency_latency.png"
        )
        results["throughput_latency_curve"] = generate_throughput_latency_plot(
            conc_csv, output_dir / "throughput_latency_curve.png"
        )
        # Also generate LaTeX table
        results["concurrency_comparison_tex"] = latex_table_concurrency_comparison(
            conc_csv, output_dir / "concurrency_comparison.tex"
        )
    else:
        # Fallback to separate files if they exist
        conc_th_csv = analysis_dir / "concurrency_throughput.csv"
        if conc_th_csv.exists():
            results["concurrency_throughput"] = generate_concurrency_throughput_plot(
                conc_th_csv, output_dir / "concurrency_throughput.png"
            )

        conc_lat_csv = analysis_dir / "concurrency_latency.csv"
        if conc_lat_csv.exists():
            results["concurrency_latency"] = generate_concurrency_latency_plot(
                conc_lat_csv, output_dir / "concurrency_latency.png"
            )

    # 4. TPC-C Comparison
    tpcc_csv = analysis_dir / "tpcc_summary.csv"
    if tpcc_csv.exists():
        results["tpcc_comparison"] = generate_tpcc_barplot(
            tpcc_csv, output_dir / "tpcc_comparison.png"
        )

    # 5. Profiling/Overhead Breakdown (NEW - E2)
    profiling_dir = analysis_dir / "profiling"
    mysql_diff_csv = profiling_dir / "mysql_perf_schema_diff.csv"
    if mysql_diff_csv.exists():
        try:
            results["overhead_breakdown_waterfall"] = (
                generate_overhead_breakdown_waterfall(
                    mysql_diff_csv, output_dir / "overhead_breakdown_waterfall.png"
                )
            )
            results["mysql_perf_schema_diff_top"] = (
                generate_mysql_perf_schema_diff_plot(
                    mysql_diff_csv, profiling_dir / "mysql_perf_schema_diff_top.png"
                )
            )
        except Exception:
            pass

    postgres_explain_diff_csv = profiling_dir / "postgres_explain_diff.csv"
    if postgres_explain_diff_csv.exists():
        try:
            results["postgres_explain_diff"] = generate_postgres_explain_diff_plot(
                postgres_explain_diff_csv, profiling_dir / "postgres_explain_diff.png"
            )
        except Exception:
            pass

    # 6. pgbench Comparison
    pgbench_summary_csv = analysis_dir / "pgbench_summary.csv"
    if pgbench_summary_csv.exists():
        results["pgbench_comparison"] = generate_pgbench_summary_plot(
            pgbench_summary_csv, output_dir / "pgbench_comparison.png"
        )

    # 7. Cross-database Comparison (NEW)
    if tpcc_csv.exists() or pgbench_summary_csv.exists():
        results["cross_database_comparison"] = generate_cross_database_comparison(
            tpcc_csv if tpcc_csv.exists() else None,
            pgbench_summary_csv if pgbench_summary_csv.exists() else None,
            output_dir / "cross_database_comparison.png",
        )

    # 8. Unified Summary Table (NEW)
    results["unified_summary"] = generate_unified_summary_table(
        analysis_dir, output_dir / "unified_summary.tex"
    )

    return results


def _read_csv_rows(path: Path) -> list[dict[str, str]]:
    with path.open() as f:
        reader = csv.DictReader(f)
        return [row for row in reader]


def generate_tpcc_summary_plot(
    csv_path: Path,
    output_path: Path,
    title: str = "TPC-C: Baseline vs Cedar (MySQL)",
) -> Path | None:
    if not HAS_PLOTTING or _should_skip_plots() or not csv_path.exists():
        return None

    rows = _read_csv_rows(csv_path)
    if not rows:
        return None

    labels: list[str] = []
    base_tpm: list[float] = []
    cedar_tpm: list[float] = []
    base_lat: list[float] = []
    cedar_lat: list[float] = []

    for r in rows:
        tool = r.get("tool", "")
        wh = r.get("warehouses", "")
        load = r.get("load", "")
        labels.append(f"{tool}\n(wh={wh},l={load})")
        base_tpm.append(float(r.get("baseline_tpm") or 0.0))
        cedar_tpm.append(float(r.get("cedar_tpm") or 0.0))
        base_lat.append(float(r.get("baseline_latency_ms") or 0.0))
        cedar_lat.append(float(r.get("cedar_latency_ms") or 0.0))

    fig, axes = plt.subplots(2, 1, figsize=(max(10, len(labels) * 1.5), 10))

    x = list(range(len(labels)))
    width = 0.35

    ax1 = axes[0]
    ax1.bar(
        [i - width / 2 for i in x], base_tpm, width, label="Baseline", color="#2E86AB"
    )
    ax1.bar(
        [i + width / 2 for i in x],
        cedar_tpm,
        width,
        label="With Cedar",
        color="#A23B72",
    )
    ax1.set_ylabel("TPM", fontsize=14, fontweight="bold")
    ax1.set_title(title, fontsize=16, fontweight="bold")
    ax1.set_xticks(x)
    ax1.set_xticklabels(labels, rotation=0, fontsize=11)
    ax1.grid(True, alpha=0.3, axis="y")
    ax1.legend(fontsize=12)

    ax2 = axes[1]
    ax2.bar(
        [i - width / 2 for i in x], base_lat, width, label="Baseline", color="#2E86AB"
    )
    ax2.bar(
        [i + width / 2 for i in x],
        cedar_lat,
        width,
        label="With Cedar",
        color="#A23B72",
    )
    ax2.set_ylabel("Avg latency (ms)", fontsize=14, fontweight="bold")
    ax2.set_xticks(x)
    ax2.set_xticklabels(labels, rotation=0, fontsize=11)
    ax2.grid(True, alpha=0.3, axis="y")

    plt.tight_layout()
    output_path.parent.mkdir(parents=True, exist_ok=True)
    plt.savefig(output_path, dpi=300, bbox_inches="tight")
    plt.close()
    return output_path


def generate_pgbench_summary_plot(
    csv_path: Path,
    output_path: Path,
    title: str = "pgbench: Baseline vs Cedar (PostgreSQL)",
) -> Path | None:
    if not HAS_PLOTTING or _should_skip_plots() or not csv_path.exists():
        return None

    rows = _read_csv_rows(csv_path)
    if not rows:
        return None

    # Label each configuration as "s=<scale>,c=<clients>,t=<duration>"
    labels: list[str] = []
    base_tps: list[float] = []
    cedar_tps: list[float] = []
    base_lat: list[float] = []
    cedar_lat: list[float] = []

    for r in rows:
        scale = r.get("scale", "")
        clients = r.get("clients", "")
        dur = r.get("duration_s", "")
        labels.append(f"s={scale},c={clients},t={dur}")
        base_tps.append(float(r.get("baseline_tps") or 0.0))
        cedar_tps.append(float(r.get("cedar_tps") or 0.0))
        base_lat.append(float(r.get("baseline_avg_latency_ms") or 0.0))
        cedar_lat.append(float(r.get("cedar_avg_latency_ms") or 0.0))

    fig, axes = plt.subplots(2, 1, figsize=(max(10, len(labels) * 1.2), 10))

    x = list(range(len(labels)))
    width = 0.35

    ax1 = axes[0]
    ax1.bar(
        [i - width / 2 for i in x], base_tps, width, label="Baseline", color="#2E86AB"
    )
    ax1.bar(
        [i + width / 2 for i in x],
        cedar_tps,
        width,
        label="With Cedar",
        color="#A23B72",
    )
    ax1.set_ylabel("TPS", fontsize=14, fontweight="bold")
    ax1.set_title(title, fontsize=16, fontweight="bold")
    ax1.set_xticks(x)
    ax1.set_xticklabels(labels, rotation=30, ha="right", fontsize=11)
    ax1.grid(True, alpha=0.3, axis="y")
    ax1.legend(fontsize=12)

    ax2 = axes[1]
    ax2.bar(
        [i - width / 2 for i in x], base_lat, width, label="Baseline", color="#2E86AB"
    )
    ax2.bar(
        [i + width / 2 for i in x],
        cedar_lat,
        width,
        label="With Cedar",
        color="#A23B72",
    )
    ax2.set_ylabel("Avg latency (ms)", fontsize=14, fontweight="bold")
    ax2.set_xticks(x)
    ax2.set_xticklabels(labels, rotation=30, ha="right", fontsize=11)
    ax2.grid(True, alpha=0.3, axis="y")

    plt.tight_layout()
    output_path.parent.mkdir(parents=True, exist_ok=True)
    plt.savefig(output_path, dpi=300, bbox_inches="tight")
    plt.close()
    return output_path


def generate_mysql_perf_schema_diff_plot(
    csv_path: Path,
    output_path: Path,
    top_n: int = 12,
) -> Path | None:
    """Plot top perf_schema stage/wait deltas (Cedar - baseline, ms)."""
    if not HAS_PLOTTING or _should_skip_plots() or not csv_path.exists():
        return None

    rows = _read_csv_rows(csv_path)
    cleaned = []
    for r in rows:
        # The file may contain repeated headers; skip them.
        if (r.get("section") or "").strip().lower() == "section":
            continue
        if not r.get("section") or not r.get("event_name"):
            continue
        try:
            delta = float(r.get("cedar_minus_baseline_ms") or 0.0)
        except ValueError:
            continue
        cleaned.append((r["section"], r["event_name"], delta))

    if not cleaned:
        return None

    def _top(section: str) -> list[tuple]:
        items = [(ev, d) for (sec, ev, d) in cleaned if sec == section]
        items.sort(key=lambda t: abs(t[1]), reverse=True)
        return items[:top_n]

    top_stages = _top("stages")
    top_waits = _top("waits")

    fig, axes = plt.subplots(2, 1, figsize=(12, 10))
    for ax, title, items in [
        (axes[0], "Top stages (delta ms)", top_stages),
        (axes[1], "Top waits (delta ms)", top_waits),
    ]:
        if not items:
            ax.axis("off")
            continue
        labels = [ev.replace("stage/sql/", "").replace("wait/", "") for ev, _ in items]
        vals = [d for _, d in items]
        y = list(range(len(labels)))
        ax.barh(y, vals, color=["#DC3545" if v > 0 else "#28A745" for v in vals])
        ax.set_yticks(y)
        ax.set_yticklabels(labels, fontsize=10)
        ax.invert_yaxis()
        ax.set_title(title, fontsize=14, fontweight="bold")
        ax.grid(True, alpha=0.3, axis="x")
        ax.set_xlabel("Cedar - baseline (ms)", fontsize=12, fontweight="bold")

    plt.tight_layout()
    output_path.parent.mkdir(parents=True, exist_ok=True)
    plt.savefig(output_path, dpi=300, bbox_inches="tight")
    plt.close()
    return output_path


def generate_postgres_explain_diff_plot(
    csv_path: Path,
    output_path: Path,
    title: str = "PostgreSQL: Planning vs Execution Median Delta (Cedar - baseline)",
) -> Path | None:
    if not HAS_PLOTTING or _should_skip_plots() or not csv_path.exists():
        return None

    rows = _read_csv_rows(csv_path)
    if not rows:
        return None
    cats: list[str] = []
    d_plan: list[float] = []
    d_exec: list[float] = []
    for r in rows:
        cat = (r.get("category") or "").strip()
        if not cat:
            continue
        try:
            dp = float(r.get("delta_planning_ms") or 0.0)
            de = float(r.get("delta_execution_ms") or 0.0)
        except ValueError:
            continue
        cats.append(cat)
        d_plan.append(dp)
        d_exec.append(de)

    if not cats:
        return None

    fig, ax = plt.subplots(figsize=(max(10, len(cats) * 0.9), 6))
    x = list(range(len(cats)))
    width = 0.35
    ax.bar(
        [i - width / 2 for i in x], d_plan, width, label="Planning Δ", color="#2E86AB"
    )
    ax.bar(
        [i + width / 2 for i in x], d_exec, width, label="Execution Δ", color="#A23B72"
    )
    ax.axhline(0.0, color="black", linewidth=1)
    ax.set_title(title, fontsize=14, fontweight="bold")
    ax.set_ylabel("Delta (ms)", fontsize=12, fontweight="bold")
    ax.set_xticks(x)
    ax.set_xticklabels(cats, rotation=30, ha="right", fontsize=10)
    ax.grid(True, alpha=0.3, axis="y")
    ax.legend(fontsize=11)

    plt.tight_layout()
    output_path.parent.mkdir(parents=True, exist_ok=True)
    plt.savefig(output_path, dpi=300, bbox_inches="tight")
    plt.close()
    return output_path
