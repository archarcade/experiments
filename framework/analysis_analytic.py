#!/usr/bin/env python3
"""
Analytic Query Analysis Module (E5).

Parses analytic query results with differentiation by query complexity:
- Single table queries
- JOIN queries (2-table, 3-table, etc.)
- Aggregation queries (GROUP BY, HAVING)
- Window function queries
- Subqueries

USENIX-style artifacts:
- analytic_summary.csv: Overhead by query complexity
- analytic_summary.tex: LaTeX table for paper
"""

from __future__ import annotations

import csv
import json
import re
import statistics
from pathlib import Path
from typing import Any


def categorize_query_complexity(sql: str) -> str:
    """
    Categorize SQL query by complexity.

    Args:
        sql: SQL query string

    Returns:
        Category string (e.g., "SIMPLE_SELECT", "JOIN_2TABLE", "AGGREGATION", etc.)
    """
    sql_upper = sql.upper().strip()

    # Count JOINs
    join_count = len(re.findall(r"\bJOIN\b", sql_upper))

    # Check for window functions
    has_window = bool(re.search(r"\bOVER\s*\(", sql_upper))

    # Check for subqueries
    has_subquery = sql_upper.count("SELECT") > 1

    # Check for aggregations
    has_aggregation = bool(
        re.search(r"\b(GROUP\s+BY|HAVING|COUNT|SUM|AVG|MIN|MAX)\b", sql_upper)
    )

    # Categorize
    if has_window:
        return "WINDOW_FUNCTION"
    elif has_subquery:
        return "SUBQUERY"
    elif join_count >= 3:
        return "JOIN_3PLUS"
    elif join_count == 2:
        return "JOIN_2TABLE"
    elif join_count == 1:
        return "JOIN_1TABLE"
    elif has_aggregation:
        return "AGGREGATION"
    else:
        return "SIMPLE_SELECT"


def extract_analytic_results(results_json: Path) -> list[dict[str, Any]]:
    """
    Extract analytic query results from a results JSON file.

    Args:
        results_json: Path to results.json or analytic_results.json

    Returns:
        List of result dictionaries with query details
    """
    if not results_json.exists():
        return []

    try:
        data = json.loads(results_json.read_text())
    except Exception:
        return []

    results = []

    # Get baseline and cedar results
    baseline_results = data.get("baseline", [])
    cedar_results = data.get("cedar", [])

    if not baseline_results or not cedar_results:
        return []

    # Group by original category or SQL-based complexity
    baseline_by_cat: dict[str, list[tuple[float, float]]] = {}  # (latency, exec_time)
    cedar_by_cat: dict[str, list[tuple[float, float]]] = {}

    for r in baseline_results:
        # Try to categorize by SQL if available
        sql = r.get("sql", r.get("query", ""))
        original_cat = r.get("category", "UNKNOWN")

        if sql and original_cat == "SELECT_ANALYTIC":
            cat = categorize_query_complexity(sql)
        else:
            cat = original_cat

        latency = float(r.get("latency_ms", 0))
        exec_time = float(
            r.get("execution_time_ms", latency)
        )  # Some results have execution time

        if latency > 0:
            baseline_by_cat.setdefault(cat, []).append((latency, exec_time))

    for r in cedar_results:
        sql = r.get("sql", r.get("query", ""))
        original_cat = r.get("category", "UNKNOWN")

        if sql and original_cat == "SELECT_ANALYTIC":
            cat = categorize_query_complexity(sql)
        else:
            cat = original_cat

        latency = float(r.get("latency_ms", 0))
        exec_time = float(r.get("execution_time_ms", latency))

        if latency > 0:
            cedar_by_cat.setdefault(cat, []).append((latency, exec_time))

    all_cats = sorted(set(baseline_by_cat.keys()) | set(cedar_by_cat.keys()))

    for cat in all_cats:
        base_entries = baseline_by_cat.get(cat, [])
        cedar_entries = cedar_by_cat.get(cat, [])

        if not base_entries or not cedar_entries:
            continue

        base_latencies = [e[0] for e in base_entries]
        cedar_latencies = [e[0] for e in cedar_entries]
        base_exec_times = [e[1] for e in base_entries]
        [e[1] for e in cedar_entries]

        base_median = statistics.median(base_latencies)
        cedar_median = statistics.median(cedar_latencies)
        base_exec_median = statistics.median(base_exec_times)

        overhead_ms = cedar_median - base_median
        overhead_pct = (overhead_ms / base_median * 100) if base_median > 0 else 0

        # Calculate overhead ratio (overhead relative to execution time)
        overhead_ratio = (
            (overhead_ms / base_exec_median * 100) if base_exec_median > 0 else 0
        )

        results.append(
            {
                "category": cat,
                "query_type": _friendly_category_name(cat),
                "baseline_median_ms": round(base_median, 3),
                "cedar_median_ms": round(cedar_median, 3),
                "overhead_ms": round(overhead_ms, 3),
                "overhead_pct": round(overhead_pct, 2),
                "exec_time_ms": round(base_exec_median, 3),
                "overhead_ratio_pct": round(overhead_ratio, 2),
                "count": min(len(base_entries), len(cedar_entries)),
            }
        )

    return results


def _friendly_category_name(cat: str) -> str:
    """Convert category code to friendly name."""
    mapping = {
        "SIMPLE_SELECT": "Simple SELECT",
        "JOIN_1TABLE": "1-Table JOIN",
        "JOIN_2TABLE": "2-Table JOIN",
        "JOIN_3PLUS": "3+ Table JOIN",
        "AGGREGATION": "Aggregation",
        "WINDOW_FUNCTION": "Window Function",
        "SUBQUERY": "Subquery",
        "SELECT_ANALYTIC": "Analytic",
    }
    return mapping.get(cat, cat)


def collect_analytic_results(analytic_dir: Path) -> list[dict[str, Any]]:
    """
    Collect all analytic query results from a directory.

    Args:
        analytic_dir: Directory containing analytic result files

    Returns:
        Combined list of result dictionaries
    """
    if not analytic_dir.exists():
        return []

    all_results = []

    # Look for results files
    for pattern in ["results.json", "analytic_results.json", "*_results.json"]:
        for p in analytic_dir.glob(pattern):
            all_results.extend(extract_analytic_results(p))

    return all_results


def write_analytic_summary_csv(rows: list[dict[str, Any]], out_path: Path) -> None:
    """
    Write analytic query summary to CSV file.

    Args:
        rows: List of result dictionaries
        out_path: Path to output CSV file
    """
    if not rows:
        return

    out_path.parent.mkdir(parents=True, exist_ok=True)

    header = [
        "category",
        "query_type",
        "baseline_median_ms",
        "cedar_median_ms",
        "overhead_ms",
        "overhead_pct",
        "exec_time_ms",
        "overhead_ratio_pct",
        "count",
    ]

    with out_path.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=header)
        writer.writeheader()
        for row in rows:
            writer.writerow({k: row.get(k, "") for k in header})


def write_analytic_summary_table_tex(
    rows: list[dict[str, Any]], out_path: Path
) -> None:
    """
    Write analytic query summary to LaTeX table.

    The table highlights how overhead becomes less significant for complex queries.

    Args:
        rows: List of result dictionaries
        out_path: Path to output LaTeX file
    """
    if not rows:
        return

    out_path.parent.mkdir(parents=True, exist_ok=True)

    lines = [
        "\\begin{table}[h]",
        "\\centering",
        "\\caption{Authorization Overhead by Query Complexity}",
        "\\label{tab:analytic_overhead}",
        "\\begin{tabular}{lrrrrr}",
        "\\toprule",
        "Query Type & Exec Time (ms) & Overhead (ms) & Overhead (\\%) & Ratio (\\%) & n \\\\",
        "\\midrule",
    ]

    # Sort by execution time (simple to complex)
    for row in sorted(rows, key=lambda r: r.get("exec_time_ms", 0)):
        query_type = row.get("query_type", "")
        exec_time = row.get("exec_time_ms", 0)
        overhead_ms = row.get("overhead_ms", 0)
        overhead_pct = row.get("overhead_pct", 0)
        overhead_ratio = row.get("overhead_ratio_pct", 0)
        count = row.get("count", 0)

        lines.append(
            f"{query_type} & {exec_time:.2f} & {overhead_ms:+.2f} & "
            f"{overhead_pct:+.1f}\\% & {overhead_ratio:.1f}\\% & {count} \\\\"
        )

    lines.extend(
        [
            "\\bottomrule",
            "\\end{tabular}",
            "\\footnotesize{Ratio = Overhead / Execution Time. For complex queries, ratio approaches 0\\%.}",
            "\\end{table}",
        ]
    )

    out_path.write_text("\n".join(lines))


def generate_analytic_visualizations(
    analytic_dir: Path, output_dir: Path
) -> dict[str, Path | None]:
    """
    Generate all analytic query visualizations and tables.

    Args:
        analytic_dir: Directory containing analytic result files
        output_dir: Directory to write output files

    Returns:
        Dictionary mapping artifact names to file paths
    """
    results = {}

    analytic_rows = collect_analytic_results(analytic_dir)

    if analytic_rows:
        csv_path = output_dir / "analytic_summary.csv"
        write_analytic_summary_csv(analytic_rows, csv_path)
        results["analytic_summary_csv"] = csv_path

        tex_path = output_dir / "analytic_summary.tex"
        write_analytic_summary_table_tex(analytic_rows, tex_path)
        results["analytic_summary_tex"] = tex_path

    return results


def generate_overhead_ratio_plot(
    analytic_rows: list[dict[str, Any]], output_path: Path
) -> Path | None:
    """
    Generate bar chart showing overhead ratio (overhead/exec_time) by query complexity.

    This highlights that for complex queries, the authorization overhead
    becomes a negligible fraction of total execution time.

    Args:
        analytic_rows: List of analytic result dictionaries
        output_path: Path to save the plot

    Returns:
        Path to generated plot, or None if plotting unavailable
    """
    try:
        import matplotlib

        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
    except ImportError:
        return None

    if not analytic_rows:
        return None

    # Sort by execution time
    sorted_rows = sorted(analytic_rows, key=lambda r: r.get("exec_time_ms", 0))

    query_types = [r.get("query_type", "") for r in sorted_rows]
    overhead_ratios = [r.get("overhead_ratio_pct", 0) for r in sorted_rows]
    exec_times = [r.get("exec_time_ms", 0) for r in sorted_rows]

    fig, ax1 = plt.subplots(figsize=(12, 7))

    x = range(len(query_types))
    ax1.bar(x, overhead_ratios, color="#3498db", alpha=0.8, label="Overhead Ratio")

    ax1.set_ylabel(
        "Overhead Ratio (%)", fontsize=14, fontweight="bold", color="#3498db"
    )
    ax1.tick_params(axis="y", labelcolor="#3498db")
    ax1.set_ylim(0, max(overhead_ratios) * 1.2 if overhead_ratios else 100)

    # Add execution time as secondary axis
    ax2 = ax1.twinx()
    ax2.plot(x, exec_times, "ro-", label="Execution Time", linewidth=2, markersize=8)
    ax2.set_ylabel(
        "Execution Time (ms)", fontsize=14, fontweight="bold", color="#e74c3c"
    )
    ax2.tick_params(axis="y", labelcolor="#e74c3c")

    ax1.set_xticks(x)
    ax1.set_xticklabels(query_types, rotation=45, ha="right", fontsize=11)
    ax1.set_xlabel("Query Complexity", fontsize=14, fontweight="bold")

    ax1.set_title(
        "Authorization Overhead Ratio by Query Complexity",
        fontsize=16,
        fontweight="bold",
    )

    # Combine legends
    lines1, labels1 = ax1.get_legend_handles_labels()
    lines2, labels2 = ax2.get_legend_handles_labels()
    ax1.legend(lines1 + lines2, labels1 + labels2, loc="upper right", fontsize=12)

    ax1.grid(True, alpha=0.3, axis="y")

    plt.tight_layout()
    output_path.parent.mkdir(parents=True, exist_ok=True)
    plt.savefig(output_path, dpi=300, bbox_inches="tight")
    plt.close()

    return output_path
