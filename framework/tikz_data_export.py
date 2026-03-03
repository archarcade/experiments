#!/usr/bin/env python3
"""
TikZ-compatible CSV data export for USENIX paper figures.

Generates CSV files that match the expected format for pgfplots/TikZ templates
in usenix/paper/figures/tikz/. This module replaces matplotlib-generated PNG
figures with data files that can be rendered directly in LaTeX.

CSV Formats:
- concurrency_comparison_str.csv: For concurrency_latency.tex, concurrency_throughput.tex
- policy_scaling_boxplot_stats.csv: For policy_scaling_boxplot.tex
- tpcc_summary.csv: For cross_database_comparison.tex (already compatible)
"""

from __future__ import annotations

import csv
import json
import statistics
from pathlib import Path
from typing import Any


def compute_boxplot_stats(values: list[float]) -> dict[str, float]:
    """
    Compute boxplot statistics with 1.5*IQR whiskers.

    Uses standard boxplot definition:
    - Q1: 25th percentile
    - Median: 50th percentile
    - Q3: 75th percentile
    - Whiskers: Extend to min/max values within 1.5*IQR of Q1/Q3

    Args:
        values: List of numeric values to compute statistics for

    Returns:
        Dict with q1_ms, median_ms, q3_ms, whisker_low_ms, whisker_high_ms
        Empty dict if input is empty
    """
    if not values:
        return {}

    sorted_vals = sorted(values)
    n = len(sorted_vals)

    # Use proper quantile calculation
    median = statistics.median(sorted_vals)

    # Calculate quartiles using linear interpolation
    if n >= 4:
        q1_idx = (n - 1) * 0.25
        q3_idx = (n - 1) * 0.75

        q1_low = int(q1_idx)
        q1_high = min(q1_low + 1, n - 1)
        q1_frac = q1_idx - q1_low
        q1 = sorted_vals[q1_low] * (1 - q1_frac) + sorted_vals[q1_high] * q1_frac

        q3_low = int(q3_idx)
        q3_high = min(q3_low + 1, n - 1)
        q3_frac = q3_idx - q3_low
        q3 = sorted_vals[q3_low] * (1 - q3_frac) + sorted_vals[q3_high] * q3_frac
    else:
        q1 = sorted_vals[n // 4] if n > 1 else sorted_vals[0]
        q3 = sorted_vals[(3 * n) // 4] if n > 2 else sorted_vals[-1]

    iqr = q3 - q1

    # Whiskers: furthest points within 1.5*IQR
    whisker_low_limit = q1 - 1.5 * iqr
    whisker_high_limit = q3 + 1.5 * iqr

    # Find actual whisker values (furthest non-outlier points)
    whisker_low = min(v for v in sorted_vals if v >= whisker_low_limit)
    whisker_high = max(v for v in sorted_vals if v <= whisker_high_limit)

    return {
        "q1_ms": q1,
        "median_ms": median,
        "q3_ms": q3,
        "whisker_low_ms": whisker_low,
        "whisker_high_ms": whisker_high,
    }


def write_concurrency_comparison_str_csv(
    by_threads: dict[int, dict[str, Any]],
    out_path: Path,
) -> Path:
    """
    Write TikZ-compatible concurrency comparison CSV with string thread counts.

    Output matches format expected by concurrency_latency.tex and concurrency_throughput.tex:
    - threads_str: String for TikZ symbolic x coords (required for non-linear axis)
    - baseline_qps, cedar_qps: Queries per second
    - baseline_p50_ms, cedar_p50_ms: Median latency
    - baseline_p95_ms, cedar_p95_ms: 95th percentile latency

    Args:
        by_threads: Dict mapping thread count to metrics dict
                   e.g., {1: {"baseline_qps": 873.74, "cedar_qps": 907.83, ...}}
        out_path: Output CSV file path

    Returns:
        Path to written CSV file
    """
    fieldnames = [
        "threads_str",
        "baseline_qps",
        "cedar_qps",
        "baseline_p50_ms",
        "cedar_p50_ms",
        "baseline_p95_ms",
        "cedar_p95_ms",
    ]

    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open("w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for th in sorted(by_threads.keys()):
            row = by_threads[th]
            w.writerow(
                {
                    "threads_str": str(th),
                    "baseline_qps": round(row.get("baseline_qps", 0), 2),
                    "cedar_qps": round(row.get("cedar_qps", 0), 2),
                    "baseline_p50_ms": round(row.get("baseline_p50_ms", 0), 2),
                    "cedar_p50_ms": round(row.get("cedar_p50_ms", 0), 2),
                    "baseline_p95_ms": round(row.get("baseline_p95_ms", 0), 2),
                    "cedar_p95_ms": round(row.get("cedar_p95_ms", 0), 2),
                }
            )
    return out_path


def write_policy_scaling_boxplot_stats_csv(
    policy_dir: Path,
    out_path: Path,
) -> Path | None:
    """
    Generate boxplot statistics CSV for policy scaling experiments.

    Reads policies_*/results.json and computes IQR-based boxplot stats
    for both baseline and cedar systems.

    Output matches format expected by policy_scaling_boxplot.tex:
    - policy_count: Number of policies (1, 10, 100, 1000)
    - system: "baseline" or "cedar"
    - q1_ms, median_ms, q3_ms: Quartile values
    - whisker_low_ms, whisker_high_ms: 1.5*IQR whisker endpoints

    Args:
        policy_dir: Directory containing policies_*/results.json files
        out_path: Output CSV file path

    Returns:
        Path to written CSV file, or None if no data found
    """
    if not policy_dir.exists():
        return None

    rows = []

    for results_file in sorted(policy_dir.glob("policies_*/results.json")):
        try:
            policy_count = int(results_file.parent.name.replace("policies_", ""))
            data = json.loads(results_file.read_text())

            # Extract latencies for each system
            for system in ["baseline", "cedar"]:
                if data.get("multi_run"):
                    latencies = []
                    for run in data.get("runs", []) or []:
                        if not isinstance(run, dict):
                            continue
                        latencies.extend(
                            [
                                float(r.get("latency_ms", 0))
                                for r in (run.get(system, []) or [])
                                if isinstance(r, dict) and r.get("success")
                            ]
                        )
                else:
                    system_data = data.get(system, [])
                    if isinstance(system_data, list):
                        latencies = [
                            float(r.get("latency_ms", 0))
                            for r in system_data
                            if isinstance(r, dict) and r.get("success")
                        ]
                    else:
                        latencies = []

                if latencies:
                    stats = compute_boxplot_stats(latencies)
                    rows.append(
                        {
                            "policy_count": policy_count,
                            "system": system,
                            **stats,
                        }
                    )
        except (ValueError, KeyError, json.JSONDecodeError):
            continue

    if not rows:
        return None

    # Sort by policy_count, then system (baseline before cedar)
    rows.sort(key=lambda r: (r["policy_count"], r["system"]))

    fieldnames = [
        "policy_count",
        "system",
        "q1_ms",
        "median_ms",
        "q3_ms",
        "whisker_low_ms",
        "whisker_high_ms",
    ]

    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open("w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        w.writerows(rows)

    return out_path


def write_tpcc_summary_tikz_csv(
    tpcc_rows: list[dict[str, Any]],
    out_path: Path,
) -> Path | None:
    """
    Write TPC-C summary in TikZ-compatible format.

    The existing tpcc_summary.csv is already compatible, but this function
    ensures consistent formatting and column ordering for pgfplots.

    Output matches format expected by cross_database_comparison.tex:
    - tool: Benchmark identifier (e.g., "sysbench-tpcc-pg", "sysbench-tpcc-mysql")
    - baseline_tpm, cedar_tpm: Transactions per minute
    - tpm_overhead_pct: Percentage overhead
    - baseline_latency_ms, cedar_latency_ms: Latency values
    - lat_overhead_pct: Latency overhead percentage

    Args:
        tpcc_rows: List of TPC-C result dicts from analysis_tpcc
        out_path: Output CSV file path

    Returns:
        Path to written CSV file, or None if no data
    """
    if not tpcc_rows:
        return None

    fieldnames = [
        "tool",
        "warehouses",
        "load",
        "baseline_tpm",
        "cedar_tpm",
        "tpm_overhead_pct",
        "baseline_latency_ms",
        "cedar_latency_ms",
        "lat_overhead_pct",
    ]

    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open("w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for row in tpcc_rows:
            w.writerow(
                {
                    "tool": row.get("tool", "unknown"),
                    "warehouses": row.get("warehouses", 0),
                    "load": row.get("load", 0),
                    "baseline_tpm": round(float(row.get("baseline_tpm", 0)), 1),
                    "cedar_tpm": round(float(row.get("cedar_tpm", 0)), 1),
                    "tpm_overhead_pct": round(float(row.get("tpm_overhead_pct", 0)), 2),
                    "baseline_latency_ms": round(
                        float(row.get("baseline_latency_ms", 0)), 2
                    ),
                    "cedar_latency_ms": round(float(row.get("cedar_latency_ms", 0)), 2),
                    "lat_overhead_pct": round(float(row.get("lat_overhead_pct", 0)), 2),
                }
            )

    return out_path


def collect_boxplot_outliers(
    policy_dir: Path,
    stats_csv: Path,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """
    Collect sample points and outliers for boxplot scatter overlay.

    Reads policies_*/results.json and identifies:
    1. Sample points for jittered scatter (random subset)
    2. Outliers (points beyond 1.5*IQR whiskers)

    These can be written to separate CSVs for TikZ scatter plots.

    Args:
        policy_dir: Directory containing policies_*/results.json files
        stats_csv: Path to stats CSV (to read whisker bounds)

    Returns:
        Tuple of (sample_points, outliers) where each is a list of
        {"x": float, "y": float} dicts for TikZ coordinates
    """
    import random

    if not policy_dir.exists() or not stats_csv.exists():
        return [], []

    # Read stats to get whisker bounds
    whisker_bounds: dict[tuple[int, str], tuple[float, float]] = {}
    with stats_csv.open() as f:
        reader = csv.DictReader(f)
        for row in reader:
            key = (int(row["policy_count"]), row["system"])
            whisker_bounds[key] = (
                float(row["whisker_low_ms"]),
                float(row["whisker_high_ms"]),
            )

    # Map policy_count to x-position (matching TikZ draw positions)
    policy_to_x_base = {1: 1, 10: 2, 100: 3, 1000: 4}

    sample_points = []
    outliers = []

    for results_file in sorted(policy_dir.glob("policies_*/results.json")):
        try:
            policy_count = int(results_file.parent.name.replace("policies_", ""))
            data = json.loads(results_file.read_text())
            x_base = policy_to_x_base.get(policy_count, policy_count)

            for system in ["baseline", "cedar"]:
                # Offset: baseline at -0.15, cedar at +0.15
                x_offset = -0.15 if system == "baseline" else 0.15
                x = x_base + x_offset

                key = (policy_count, system)
                if key not in whisker_bounds:
                    continue

                whisker_low, whisker_high = whisker_bounds[key]

                # Extract latencies
                if data.get("multi_run"):
                    latencies = []
                    for run in data.get("runs", []) or []:
                        if isinstance(run, dict):
                            latencies.extend(
                                [
                                    float(r.get("latency_ms", 0))
                                    for r in (run.get(system, []) or [])
                                    if isinstance(r, dict) and r.get("success")
                                ]
                            )
                else:
                    system_data = data.get(system, [])
                    latencies = (
                        [
                            float(r.get("latency_ms", 0))
                            for r in system_data
                            if isinstance(r, dict) and r.get("success")
                        ]
                        if isinstance(system_data, list)
                        else []
                    )

                # Separate outliers and valid points
                valid = [v for v in latencies if whisker_low <= v <= whisker_high]
                outlier_vals = [
                    v for v in latencies if v < whisker_low or v > whisker_high
                ]

                # Sample up to 50 points for scatter (with jitter)
                sampled = random.sample(valid, min(50, len(valid)))
                for v in sampled:
                    jitter = random.uniform(-0.08, 0.08)
                    sample_points.append({"x": round(x + jitter, 3), "y": round(v, 3)})

                # All outliers
                for v in outlier_vals:
                    jitter = random.uniform(-0.05, 0.05)
                    outliers.append({"x": round(x + jitter, 3), "y": round(v, 3)})

        except (ValueError, KeyError, json.JSONDecodeError):
            continue

    return sample_points, outliers


def write_boxplot_points_csv(
    points: list[dict[str, Any]],
    out_path: Path,
) -> Path | None:
    """Write sample/outlier points to CSV for TikZ scatter overlay."""
    if not points:
        return None

    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open("w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["x", "y"])
        w.writeheader()
        w.writerows(points)

    return out_path
