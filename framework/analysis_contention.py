#!/usr/bin/env python3
"""
Multi-User Contention Analysis Module (E6).

Analyzes concurrent user access patterns with different roles contending
for the same resources. Different from E3 (concurrency scaling) which
focuses on raw connection count.

USENIX-style artifacts:
- contention_summary.csv: Contention analysis results
- contention_summary.tex: LaTeX table for paper
"""

from __future__ import annotations

import csv
import json
import statistics
from pathlib import Path
from typing import Any


def extract_contention_results(results_json: Path) -> list[dict[str, Any]]:
    """
    Extract multi-user contention results from a results JSON file.

    Expects results with user/role diversity information.

    Args:
        results_json: Path to contention results JSON

    Returns:
        List of result dictionaries with contention metrics
    """
    if not results_json.exists():
        return []

    try:
        data = json.loads(results_json.read_text())
    except Exception:
        return []

    results = []

    # Handle different formats
    # Format 1: {"config": {...}, "baseline": {...}, "cedar": {...}}
    if "baseline" in data and "cedar" in data:
        config = data.get("config", {})

        baseline_data = data.get("baseline", {})
        cedar_data = data.get("cedar", {})

        # Extract contention-specific metrics
        user_count = config.get("user_count", config.get("users", 1))
        role_count = config.get("role_count", config.get("roles", 1))
        thread_count = config.get("threads", config.get("concurrency", 1))

        base_qps = baseline_data.get("qps", baseline_data.get("throughput", 0))
        cedar_qps = cedar_data.get("qps", cedar_data.get("throughput", 0))

        base_latency = baseline_data.get(
            "avg_latency_ms", baseline_data.get("latency_ms", 0)
        )
        cedar_latency = cedar_data.get(
            "avg_latency_ms", cedar_data.get("latency_ms", 0)
        )

        # Contention rate (how many authorization decisions conflict)
        contention_rate = cedar_data.get("contention_rate", 0)
        cache_hit_rate = cedar_data.get("cache_hit_rate", 0)

        if base_qps > 0:
            degradation = ((base_qps - cedar_qps) / base_qps) * 100
        else:
            degradation = 0

        results.append(
            {
                "user_count": user_count,
                "role_count": role_count,
                "thread_count": thread_count,
                "baseline_qps": round(base_qps, 2),
                "cedar_qps": round(cedar_qps, 2),
                "degradation_pct": round(degradation, 2),
                "baseline_latency_ms": round(base_latency, 3),
                "cedar_latency_ms": round(cedar_latency, 3),
                "contention_rate": round(contention_rate, 4),
                "cache_hit_rate": round(cache_hit_rate, 4),
            }
        )

    # Format 2: List of test runs
    elif isinstance(data, list):
        for run in data:
            user_count = run.get("user_count", run.get("users", 1))
            role_count = run.get("role_count", run.get("roles", 1))
            thread_count = run.get("threads", run.get("concurrency", 1))

            base_qps = run.get("baseline_qps", run.get("baseline_throughput", 0))
            cedar_qps = run.get("cedar_qps", run.get("cedar_throughput", 0))

            base_latency = run.get("baseline_latency_ms", 0)
            cedar_latency = run.get("cedar_latency_ms", 0)

            contention_rate = run.get("contention_rate", 0)
            cache_hit_rate = run.get("cache_hit_rate", 0)

            if base_qps > 0:
                degradation = ((base_qps - cedar_qps) / base_qps) * 100
            else:
                degradation = 0

            results.append(
                {
                    "user_count": user_count,
                    "role_count": role_count,
                    "thread_count": thread_count,
                    "baseline_qps": round(float(base_qps), 2),
                    "cedar_qps": round(float(cedar_qps), 2),
                    "degradation_pct": round(degradation, 2),
                    "baseline_latency_ms": round(float(base_latency), 3),
                    "cedar_latency_ms": round(float(cedar_latency), 3),
                    "contention_rate": round(float(contention_rate), 4),
                    "cache_hit_rate": round(float(cache_hit_rate), 4),
                }
            )

    # Format 3: Grouped by test configuration
    elif "runs" in data or "tests" in data:
        runs = data.get("runs", data.get("tests", []))
        for run in runs:
            config = run.get("config", {})
            baseline = run.get("baseline", {})
            cedar = run.get("cedar", {})

            user_count = config.get("user_count", 1)
            role_count = config.get("role_count", 1)
            thread_count = config.get("threads", 1)

            base_qps = baseline.get("qps", 0)
            cedar_qps = cedar.get("qps", 0)

            base_latency = baseline.get("avg_latency_ms", 0)
            cedar_latency = cedar.get("avg_latency_ms", 0)

            contention_rate = cedar.get("contention_rate", 0)
            cache_hit_rate = cedar.get("cache_hit_rate", 0)

            if base_qps > 0:
                degradation = ((base_qps - cedar_qps) / base_qps) * 100
            else:
                degradation = 0

            results.append(
                {
                    "user_count": user_count,
                    "role_count": role_count,
                    "thread_count": thread_count,
                    "baseline_qps": round(base_qps, 2),
                    "cedar_qps": round(cedar_qps, 2),
                    "degradation_pct": round(degradation, 2),
                    "baseline_latency_ms": round(base_latency, 3),
                    "cedar_latency_ms": round(cedar_latency, 3),
                    "contention_rate": round(contention_rate, 4),
                    "cache_hit_rate": round(cache_hit_rate, 4),
                }
            )

    return results


def collect_contention_results(contention_dir: Path) -> list[dict[str, Any]]:
    """
    Collect all contention results from a directory.

    Args:
        contention_dir: Directory containing contention result files

    Returns:
        Combined list of result dictionaries
    """
    if not contention_dir.exists():
        return []

    all_results = []

    # Look for results files
    for pattern in ["contention_results.json", "*_contention.json", "results.json"]:
        for p in contention_dir.glob(pattern):
            all_results.extend(extract_contention_results(p))

    return all_results


def write_contention_summary_csv(rows: list[dict[str, Any]], out_path: Path) -> None:
    """
    Write contention summary to CSV file.

    Args:
        rows: List of result dictionaries
        out_path: Path to output CSV file
    """
    if not rows:
        return

    out_path.parent.mkdir(parents=True, exist_ok=True)

    header = [
        "user_count",
        "role_count",
        "thread_count",
        "baseline_qps",
        "cedar_qps",
        "degradation_pct",
        "baseline_latency_ms",
        "cedar_latency_ms",
        "contention_rate",
        "cache_hit_rate",
    ]

    with out_path.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=header)
        writer.writeheader()
        for row in rows:
            writer.writerow({k: row.get(k, "") for k in header})


def write_contention_summary_table_tex(
    rows: list[dict[str, Any]], out_path: Path
) -> None:
    """
    Write contention summary to LaTeX table.

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
        "\\caption{Multi-User Contention Analysis}",
        "\\label{tab:contention}",
        "\\begin{tabular}{rrrrrr}",
        "\\toprule",
        "Users & Roles & Threads & Baseline QPS & Cedar QPS & Degradation (\\%) \\\\",
        "\\midrule",
    ]

    # Sort by user count, then role count
    for row in sorted(
        rows, key=lambda r: (r.get("user_count", 0), r.get("role_count", 0))
    ):
        users = row.get("user_count", 0)
        roles = row.get("role_count", 0)
        threads = row.get("thread_count", 0)
        base_qps = row.get("baseline_qps", 0)
        cedar_qps = row.get("cedar_qps", 0)
        degradation = row.get("degradation_pct", 0)

        lines.append(
            f"{users} & {roles} & {threads} & {base_qps:.1f} & "
            f"{cedar_qps:.1f} & {degradation:.1f}\\% \\\\"
        )

    lines.extend(["\\bottomrule", "\\end{tabular}", "\\end{table}"])

    out_path.write_text("\n".join(lines))


def generate_contention_visualizations(
    contention_dir: Path, output_dir: Path
) -> dict[str, Path | None]:
    """
    Generate all contention-related visualizations and tables.

    Args:
        contention_dir: Directory containing contention result files
        output_dir: Directory to write output files

    Returns:
        Dictionary mapping artifact names to file paths
    """
    results = {}

    contention_rows = collect_contention_results(contention_dir)

    if contention_rows:
        csv_path = output_dir / "contention_summary.csv"
        write_contention_summary_csv(contention_rows, csv_path)
        results["contention_summary_csv"] = csv_path

        tex_path = output_dir / "contention_summary.tex"
        write_contention_summary_table_tex(contention_rows, tex_path)
        results["contention_summary_tex"] = tex_path

    return results


def analyze_role_diversity_impact(
    contention_results: list[dict[str, Any]],
) -> dict[str, Any]:
    """
    Analyze how role diversity affects contention.

    With more diverse roles, cache hit rate may decrease but
    throughput may remain stable if cache is well-sized.

    Args:
        contention_results: List of contention result dictionaries

    Returns:
        Dictionary with analysis summary
    """
    if not contention_results:
        return {}

    # Group by role count
    by_role_count: dict[int, list[dict[str, Any]]] = {}
    for r in contention_results:
        role_count = r.get("role_count", 1)
        by_role_count.setdefault(role_count, []).append(r)

    analysis = {
        "role_counts": sorted(by_role_count.keys()),
        "degradation_by_roles": {},
        "cache_hit_by_roles": {},
    }

    for role_count, results in by_role_count.items():
        avg_degradation = statistics.mean(
            [r.get("degradation_pct", 0) for r in results]
        )
        avg_cache_hit = statistics.mean([r.get("cache_hit_rate", 0) for r in results])

        analysis["degradation_by_roles"][role_count] = round(avg_degradation, 2)
        analysis["cache_hit_by_roles"][role_count] = round(avg_cache_hit, 4)

    return analysis
