#!/usr/bin/env python3
"""
Enhanced visualization functions with confidence interval support.

Provides USENIX-grade plots with:
- Error bars (confidence intervals)
- Statistical annotations
- Publication-ready formatting

Reference: TRD Section 8.3 - Plot updates (error bars)
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

try:
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    import numpy as np

    HAS_PLOTTING = True
except ImportError:
    HAS_PLOTTING = False

from .stats import (
    bootstrap_ci_median,
)

# Color schemes for consistent publication-quality plots
COLORS = {
    "baseline": "#2E86AB",  # Blue
    "cedar": "#A23B72",  # Purple/Magenta
    "median": "#2E86AB",
    "p95": "#F18F01",  # Orange
    "p99": "#C73E1D",  # Red
    "success": "#28A745",  # Green
    "error": "#DC3545",  # Red
}

MARKERS = {
    "baseline": "o",
    "cedar": "s",
    "median": "o",
    "p95": "s",
    "p99": "^",
}


def generate_overhead_comparison_with_ci(
    baseline_values: list[float],
    cedar_values: list[float],
    output_path: Path,
    metric_name: str = "Latency",
    title: str = "Baseline vs Cedar Comparison",
    n_bootstrap: int = 10000,
) -> Path | None:
    """
    Generate bar plot with CI error bars comparing baseline vs Cedar.

    Args:
        baseline_values: Run-level metric values for baseline
        cedar_values: Run-level metric values for Cedar
        output_path: Path to save plot
        metric_name: Name of the metric (for axis label)
        title: Plot title
        n_bootstrap: Number of bootstrap samples for CI

    Returns:
        Path to generated plot
    """
    if not HAS_PLOTTING:
        return None

    baseline_ci = bootstrap_ci_median(baseline_values, n_bootstrap=n_bootstrap)
    cedar_ci = bootstrap_ci_median(cedar_values, n_bootstrap=n_bootstrap)

    fig, ax = plt.subplots(figsize=(8, 6))

    # Bar positions
    x = [0, 1]
    labels = ["Baseline", "With Cedar"]
    values = [baseline_ci.point_estimate, cedar_ci.point_estimate]

    # Error bars (asymmetric)
    yerr_lower = [
        baseline_ci.point_estimate - baseline_ci.lower,
        cedar_ci.point_estimate - cedar_ci.lower,
    ]
    yerr_upper = [
        baseline_ci.upper - baseline_ci.point_estimate,
        cedar_ci.upper - cedar_ci.point_estimate,
    ]
    yerr = [yerr_lower, yerr_upper]

    bars = ax.bar(
        x,
        values,
        yerr=yerr,
        capsize=8,
        color=[COLORS["baseline"], COLORS["cedar"]],
        edgecolor="black",
        linewidth=1.5,
        error_kw={"elinewidth": 2, "capthick": 2},
    )

    # Add value annotations
    for i, (bar, val, ci_lower, ci_upper) in enumerate(
        zip(
            bars,
            values,
            [baseline_ci.lower, cedar_ci.lower],
            [baseline_ci.upper, cedar_ci.upper],
        )
    ):
        height = bar.get_height()
        ax.text(
            bar.get_x() + bar.get_width() / 2,
            height + yerr_upper[i] + max(values) * 0.02,
            f"{val:.2f}\n[{ci_lower:.2f}, {ci_upper:.2f}]",
            ha="center",
            va="bottom",
            fontsize=11,
        )

    ax.set_xticks(x)
    ax.set_xticklabels(labels, fontsize=14)
    ax.set_ylabel(f"{metric_name} (ms)", fontsize=14, fontweight="bold")
    ax.set_title(title, fontsize=16, fontweight="bold")

    # Add n annotations
    ax.text(
        0.02,
        0.98,
        f"n={len(baseline_values)} runs (baseline), n={len(cedar_values)} runs (cedar)",
        transform=ax.transAxes,
        fontsize=10,
        va="top",
    )
    ax.text(
        0.02,
        0.93,
        "Error bars: 95% CI (bootstrap)",
        transform=ax.transAxes,
        fontsize=10,
        va="top",
    )

    ax.grid(True, alpha=0.3, axis="y")
    plt.tight_layout()

    output_path.parent.mkdir(parents=True, exist_ok=True)
    plt.savefig(output_path, dpi=300, bbox_inches="tight")
    plt.close()

    return output_path


def generate_category_overhead_with_ci(
    category_data: dict[str, dict[str, Any]],
    output_path: Path,
    title: str = "Query-by-Query Overhead with Confidence Intervals",
) -> Path | None:
    """
    Generate grouped bar plot with CI for each query category.

    Args:
        category_data: Dict mapping category -> {
            "baseline_ci": ConfidenceInterval dict,
            "cedar_ci": ConfidenceInterval dict,
            "overhead_ms": float,
            "overhead_pct": float,
        }
        output_path: Path to save plot
        title: Plot title

    Returns:
        Path to generated plot
    """
    if not HAS_PLOTTING:
        return None

    categories = list(category_data.keys())
    n = len(categories)
    if n == 0:
        return None

    fig, ax = plt.subplots(figsize=(max(10, n * 1.5), 7))

    x = np.arange(n)
    width = 0.35

    # Extract values and CIs
    baseline_vals = []
    baseline_errs = [[], []]  # lower, upper
    cedar_vals = []
    cedar_errs = [[], []]

    for cat in categories:
        data = category_data[cat]
        b_ci = data.get("baseline_ci", {})
        c_ci = data.get("cedar_ci", {})

        b_point = b_ci.get("point_estimate", 0)
        c_point = c_ci.get("point_estimate", 0)

        baseline_vals.append(b_point)
        cedar_vals.append(c_point)

        baseline_errs[0].append(b_point - b_ci.get("lower", b_point))
        baseline_errs[1].append(b_ci.get("upper", b_point) - b_point)

        cedar_errs[0].append(c_point - c_ci.get("lower", c_point))
        cedar_errs[1].append(c_ci.get("upper", c_point) - c_point)

    ax.bar(
        x - width / 2,
        baseline_vals,
        width,
        yerr=baseline_errs,
        capsize=5,
        label="Baseline",
        color=COLORS["baseline"],
        edgecolor="black",
        linewidth=1,
        error_kw={"elinewidth": 1.5, "capthick": 1.5},
    )

    ax.bar(
        x + width / 2,
        cedar_vals,
        width,
        yerr=cedar_errs,
        capsize=5,
        label="With Cedar",
        color=COLORS["cedar"],
        edgecolor="black",
        linewidth=1,
        error_kw={"elinewidth": 1.5, "capthick": 1.5},
    )

    # Add overhead annotations
    for i, cat in enumerate(categories):
        data = category_data[cat]
        overhead_pct = data.get("overhead_pct", 0)
        sig = data.get("significant_after_correction", False)

        max_y = max(
            baseline_vals[i] + baseline_errs[1][i], cedar_vals[i] + cedar_errs[1][i]
        )

        sign = "+" if overhead_pct >= 0 else ""
        sig_marker = "*" if sig else ""
        overhead_factor = data.get("overhead_factor")
        label = f"{sign}{overhead_pct:.1f}%{sig_marker}"
        if overhead_factor and abs(overhead_pct) > 0.1:
            label += f"\n({overhead_factor:.2f}x)"

        ax.text(
            x[i],
            max_y + max_y * 0.05,
            label,
            ha="center",
            fontsize=10,
            fontweight="bold" if sig else "normal",
            color="green" if overhead_pct < 0 else "red",
        )

    ax.set_xlabel("Query Type", fontsize=14, fontweight="bold")
    ax.set_ylabel("Latency (ms)", fontsize=14, fontweight="bold")
    ax.set_title(title, fontsize=16, fontweight="bold")
    ax.set_xticks(x)
    ax.set_xticklabels(categories, rotation=45, ha="right", fontsize=12)
    ax.legend(fontsize=12)
    ax.grid(True, alpha=0.3, axis="y")

    # Add note about significance
    ax.text(
        0.02,
        0.98,
        "* = significant after Holm-Bonferroni correction",
        transform=ax.transAxes,
        fontsize=9,
        va="top",
    )

    plt.tight_layout()

    output_path.parent.mkdir(parents=True, exist_ok=True)
    plt.savefig(output_path, dpi=300, bbox_inches="tight")
    plt.close()

    return output_path


def generate_policy_scaling_with_ci(
    data: list[dict[str, Any]],
    output_path: Path,
    title: str = "Policy Count vs. Authorization Time",
) -> Path | None:
    """
    Generate policy scaling plot with CI error bars.

    Args:
        data: List of dicts with keys:
            - policy_count: int
            - median_ms: float
            - median_ci_lower: float
            - median_ci_upper: float
            - p95_ms: float (optional)
            - p99_ms: float (optional)
        output_path: Path to save plot
        title: Plot title

    Returns:
        Path to generated plot
    """
    if not HAS_PLOTTING or not data:
        return None

    fig, ax = plt.subplots(figsize=(10, 6))

    policy_counts = [d["policy_count"] for d in data]
    medians = [d["median_ms"] for d in data]

    # Check for CI data
    has_ci = "median_ci_lower" in data[0] and "median_ci_upper" in data[0]

    if has_ci:
        ci_lower = [d["median_ms"] - d["median_ci_lower"] for d in data]
        ci_upper = [d["median_ci_upper"] - d["median_ms"] for d in data]
        yerr = [ci_lower, ci_upper]

        ax.errorbar(
            policy_counts,
            medians,
            yerr=yerr,
            fmt="o-",
            label="Median",
            linewidth=2,
            markersize=8,
            capsize=5,
            capthick=2,
            color=COLORS["median"],
        )
    else:
        ax.plot(
            policy_counts,
            medians,
            "o-",
            label="Median",
            linewidth=2,
            markersize=8,
            color=COLORS["median"],
        )

    # Add p95/p99 if available
    if "p95_ms" in data[0]:
        p95 = [d["p95_ms"] for d in data]
        ax.plot(
            policy_counts,
            p95,
            "s--",
            label="p95",
            linewidth=2,
            markersize=6,
            color=COLORS["p95"],
            alpha=0.7,
        )

    if "p99_ms" in data[0]:
        p99 = [d["p99_ms"] for d in data]
        ax.plot(
            policy_counts,
            p99,
            "^:",
            label="p99",
            linewidth=2,
            markersize=6,
            color=COLORS["p99"],
            alpha=0.7,
        )

    ax.set_xlabel("Policy Count", fontsize=14, fontweight="bold")
    ax.set_ylabel("Authorization Time (ms)", fontsize=14, fontweight="bold")
    ax.set_title(title, fontsize=16, fontweight="bold")
    ax.set_xscale("log")
    ax.legend(fontsize=12)
    ax.grid(True, alpha=0.3)
    ax.tick_params(axis="both", which="major", labelsize=12)

    if has_ci:
        ax.text(
            0.02,
            0.98,
            "Error bars: 95% CI",
            transform=ax.transAxes,
            fontsize=10,
            va="top",
        )

    plt.tight_layout()

    output_path.parent.mkdir(parents=True, exist_ok=True)
    plt.savefig(output_path, dpi=300, bbox_inches="tight")
    plt.close()

    return output_path


def generate_concurrency_throughput_with_ci(
    data: list[dict[str, Any]],
    output_path: Path,
    title: str = "Concurrency vs. Throughput",
) -> Path | None:
    """
    Generate concurrency throughput plot with CI error bars.

    Args:
        data: List of dicts with keys:
            - threads: int
            - baseline_qps: float
            - baseline_ci_lower: float
            - baseline_ci_upper: float
            - cedar_qps: float
            - cedar_ci_lower: float
            - cedar_ci_upper: float
        output_path: Path to save plot
        title: Plot title

    Returns:
        Path to generated plot
    """
    if not HAS_PLOTTING or not data:
        return None

    fig, ax = plt.subplots(figsize=(10, 6))

    threads = [d["threads"] for d in data]
    baseline_qps = [d["baseline_qps"] for d in data]
    cedar_qps = [d["cedar_qps"] for d in data]

    has_ci = "baseline_ci_lower" in data[0]

    if has_ci:
        baseline_yerr = [
            [d["baseline_qps"] - d["baseline_ci_lower"] for d in data],
            [d["baseline_ci_upper"] - d["baseline_qps"] for d in data],
        ]
        cedar_yerr = [
            [d["cedar_qps"] - d["cedar_ci_lower"] for d in data],
            [d["cedar_ci_upper"] - d["cedar_qps"] for d in data],
        ]

        ax.errorbar(
            threads,
            baseline_qps,
            yerr=baseline_yerr,
            fmt="o-",
            label="Baseline",
            linewidth=2,
            markersize=8,
            capsize=5,
            color=COLORS["baseline"],
        )
        ax.errorbar(
            threads,
            cedar_qps,
            yerr=cedar_yerr,
            fmt="s-",
            label="With Cedar",
            linewidth=2,
            markersize=8,
            capsize=5,
            color=COLORS["cedar"],
        )
    else:
        ax.plot(
            threads,
            baseline_qps,
            "o-",
            label="Baseline",
            linewidth=2,
            markersize=8,
            color=COLORS["baseline"],
        )
        ax.plot(
            threads,
            cedar_qps,
            "s-",
            label="With Cedar",
            linewidth=2,
            markersize=8,
            color=COLORS["cedar"],
        )

    ax.set_xlabel("Concurrent Threads", fontsize=14, fontweight="bold")
    ax.set_ylabel("Queries per Second (QPS)", fontsize=14, fontweight="bold")
    ax.set_title(title, fontsize=16, fontweight="bold")
    ax.legend(fontsize=12)
    ax.grid(True, alpha=0.3)
    ax.tick_params(axis="both", which="major", labelsize=12)

    if has_ci:
        ax.text(
            0.02,
            0.98,
            "Error bars: 95% CI",
            transform=ax.transAxes,
            fontsize=10,
            va="top",
        )

    plt.tight_layout()

    output_path.parent.mkdir(parents=True, exist_ok=True)
    plt.savefig(output_path, dpi=300, bbox_inches="tight")
    plt.close()

    return output_path


def generate_multi_run_summary_plot(
    multi_run_result_path: Path,
    output_path: Path,
    title: str = "Multi-Run Experiment Summary",
) -> Path | None:
    """
    Generate summary visualization from multi-run results.

    Args:
        multi_run_result_path: Path to multi_run_results.json
        output_path: Path to save plot
        title: Plot title

    Returns:
        Path to generated plot
    """
    if not HAS_PLOTTING or not multi_run_result_path.exists():
        return None

    data = json.loads(multi_run_result_path.read_text())

    # Create multi-panel figure
    fig, axes = plt.subplots(2, 2, figsize=(14, 10))

    # Panel 1: Median latency comparison
    ax1 = axes[0, 0]
    _plot_metric_comparison(ax1, data, "median_latency", "Median Latency (ms)")

    # Panel 2: p95 latency comparison
    ax2 = axes[0, 1]
    _plot_metric_comparison(ax2, data, "p95_latency", "p95 Latency (ms)")

    # Panel 3: QPS comparison
    ax3 = axes[1, 0]
    _plot_metric_comparison(ax3, data, "qps", "Queries per Second")

    # Panel 4: Run-by-run view
    ax4 = axes[1, 1]
    _plot_run_by_run(ax4, data)

    fig.suptitle(title, fontsize=18, fontweight="bold", y=1.02)
    plt.tight_layout()

    output_path.parent.mkdir(parents=True, exist_ok=True)
    plt.savefig(output_path, dpi=300, bbox_inches="tight")
    plt.close()

    return output_path


def _plot_metric_comparison(
    ax: Any,
    data: dict[str, Any],
    metric: str,
    ylabel: str,
) -> None:
    """Helper to plot a single metric comparison."""
    baseline_summary = data.get("aggregate", {}).get("baseline_summary", {})
    cedar_summary = data.get("aggregate", {}).get("cedar_summary", {})

    b_data = baseline_summary.get(metric, {})
    c_data = cedar_summary.get(metric, {})

    x = [0, 1]
    values = [b_data.get("point_estimate", 0), c_data.get("point_estimate", 0)]

    yerr = [
        [
            values[0] - b_data.get("lower", values[0]),
            values[1] - c_data.get("lower", values[1]),
        ],
        [
            b_data.get("upper", values[0]) - values[0],
            c_data.get("upper", values[1]) - values[1],
        ],
    ]

    ax.bar(
        x,
        values,
        yerr=yerr,
        capsize=6,
        color=[COLORS["baseline"], COLORS["cedar"]],
        edgecolor="black",
    )

    ax.set_xticks(x)
    ax.set_xticklabels(["Baseline", "Cedar"], fontsize=12)
    ax.set_ylabel(ylabel, fontsize=12, fontweight="bold")
    ax.grid(True, alpha=0.3, axis="y")


def _plot_run_by_run(ax: Any, data: dict[str, Any]) -> None:
    """Helper to plot run-by-run comparison."""
    paired_runs = data.get("paired_runs", [])
    if not paired_runs:
        ax.text(0.5, 0.5, "No run data", ha="center", va="center")
        return

    baseline_medians = []
    cedar_medians = []

    for pr in paired_runs:
        baseline_medians.append(pr["baseline"]["metrics"]["median_latency"])
        cedar_medians.append(pr["cedar"]["metrics"]["median_latency"])

    x = range(len(paired_runs))
    ax.plot(
        x,
        baseline_medians,
        "o-",
        label="Baseline",
        color=COLORS["baseline"],
        linewidth=2,
        markersize=6,
    )
    ax.plot(
        x,
        cedar_medians,
        "s-",
        label="Cedar",
        color=COLORS["cedar"],
        linewidth=2,
        markersize=6,
    )

    ax.set_xlabel("Run Index", fontsize=12, fontweight="bold")
    ax.set_ylabel("Median Latency (ms)", fontsize=12, fontweight="bold")
    ax.set_title("Run-by-Run Comparison", fontsize=14, fontweight="bold")
    ax.legend(fontsize=10)
    ax.grid(True, alpha=0.3)


def generate_overhead_breakdown_plot(
    breakdown_data: list[dict[str, Any]],
    output_path: Path,
    title: str = "Authorization Overhead Breakdown",
) -> Path | None:
    """
    Generate stacked bar or waterfall chart for overhead breakdown.

    Args:
        breakdown_data: List of phase breakdown dicts
        output_path: Path to save plot
        title: Plot title

    Returns:
        Path to generated plot
    """
    if not HAS_PLOTTING or not breakdown_data:
        return None

    # Filter out total row if present
    phases = [d for d in breakdown_data if d.get("phase_name") != "TOTAL"]

    fig, ax = plt.subplots(figsize=(12, 7))

    names = [p["phase_name"] for p in phases]
    medians = [p["median_ms"] for p in phases]
    shares = [p["share_of_total_pct"] for p in phases]

    # Colors for phases
    colors = plt.cm.viridis(np.linspace(0.2, 0.8, len(phases)))

    # Horizontal bar chart
    y_pos = np.arange(len(names))

    bars = ax.barh(
        y_pos,
        medians,
        color=colors,
        edgecolor="black",
        linewidth=1,
    )

    # Add CI error bars if available
    if "ci_lower" in phases[0]:
        xerr = [
            [p["median_ms"] - p["ci_lower"] for p in phases],
            [p["ci_upper"] - p["median_ms"] for p in phases],
        ]
        ax.errorbar(
            medians,
            y_pos,
            xerr=xerr,
            fmt="none",
            ecolor="black",
            capsize=4,
        )

    # Add share annotations
    for i, (bar, share) in enumerate(zip(bars, shares)):
        width = bar.get_width()
        ax.text(
            width + max(medians) * 0.02,
            bar.get_y() + bar.get_height() / 2,
            f"{share:.1f}%",
            va="center",
            fontsize=11,
        )

    ax.set_yticks(y_pos)
    ax.set_yticklabels(names, fontsize=11)
    ax.set_xlabel("Time (ms)", fontsize=14, fontweight="bold")
    ax.set_title(title, fontsize=16, fontweight="bold")
    ax.grid(True, alpha=0.3, axis="x")

    plt.tight_layout()

    output_path.parent.mkdir(parents=True, exist_ok=True)
    plt.savefig(output_path, dpi=300, bbox_inches="tight")
    plt.close()

    return output_path


def generate_latex_table_with_ci(
    data: list[dict[str, Any]],
    output_path: Path,
    caption: str = "Query-by-Query Overhead",
    label: str = "tab:overhead",
) -> None:
    """
    Generate LaTeX table with CI in standard USENIX format.

    Args:
        data: List of dicts with operation, baseline_ci, cedar_ci, overhead
        output_path: Path to save LaTeX file
        caption: Table caption
        label: Table label
    """
    lines = [
        "\\begin{table}[t]",
        f"\\caption{{{caption}}}",
        f"\\label{{{label}}}",
        "\\centering",
        "\\small",
        "\\begin{tabular}{lrrr}",
        "\\toprule",
        "Operation & Baseline (ms) & Cedar (ms) & Overhead \\\\",
        "\\midrule",
    ]

    for row in data:
        op = row.get("operation", "")
        b_ci = row.get("baseline_ci", {})
        c_ci = row.get("cedar_ci", {})
        overhead_pct = row.get("overhead_pct", 0)
        sig = row.get("significant", False)

        b_str = f"{b_ci.get('point_estimate', 0):.2f}"
        if b_ci.get("lower") is not None:
            b_str += f" [{b_ci['lower']:.2f}, {b_ci['upper']:.2f}]"

        c_str = f"{c_ci.get('point_estimate', 0):.2f}"
        if c_ci.get("lower") is not None:
            c_str += f" [{c_ci['lower']:.2f}, {c_ci['upper']:.2f}]"

        sign = "+" if overhead_pct >= 0 else ""
        sig_marker = "$^*$" if sig else ""
        overhead_str = f"{sign}{overhead_pct:.1f}\\%{sig_marker}"

        lines.append(f"{op} & {b_str} & {c_str} & {overhead_str} \\\\")

    lines.extend(
        [
            "\\bottomrule",
            "\\end{tabular}",
            "\\vspace{1mm}",
            "\\footnotesize{$^*$ Significant after Holm-Bonferroni correction ($p < 0.05$).}",
            "\\end{table}",
        ]
    )

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text("\n".join(lines))
