#!/usr/bin/env python3
"""
Results analysis and visualization helpers.

Generates:
- Query-by-Query Overhead table (LaTeX and CSV)
- CSVs for latency distributions (for CDF plots)
"""

from __future__ import annotations

import csv
import json
import statistics
from pathlib import Path
from typing import Any


def _group_by_category(results: list[dict[str, Any]]) -> dict[str, list[float]]:
    grouped: dict[str, list[float]] = {}
    for r in results:
        category = r.get("category") or r.get("action") or "unknown"
        grouped.setdefault(str(category), []).append(float(r["latency_ms"]))
    return grouped


def _median(values: list[float]) -> float:
    if not values:
        return 0.0
    try:
        return statistics.median(values)
    except statistics.StatisticsError:
        return 0.0


def _percentile(values: list[float], p: float) -> float:
    if not values:
        return 0.0
    try:
        return (
            statistics.quantiles(values, n=100)[int(p * 100) - 1]
            if len(values) > 1
            else values[0]
        )
    except statistics.StatisticsError:
        return 0.0


def compute_query_overhead(
    result_payload: dict[str, Any],
) -> list[tuple[str, float, float, float, float, float, float, float, float, float]]:
    """
    Returns rows: (category,
                   base_med, cedar_med,
                   base_p95, cedar_p95,
                   base_p99, cedar_p99,
                   overhead_ms (median), overhead_pct (median), overhead_factor (median))

    Note: overhead_ms can be negative if Cedar is faster (speedup).
    """
    from .stats import calculate_overhead_metrics

    baseline = result_payload.get("baseline", [])
    cedar = result_payload.get("cedar", [])

    by_cat_base = _group_by_category(baseline)
    by_cat_cedar = _group_by_category(cedar)

    categories = sorted(set(by_cat_base.keys()) | set(by_cat_cedar.keys()))
    rows: list[
        tuple[str, float, float, float, float, float, float, float, float, float]
    ] = []

    for cat in categories:
        base_vals = sorted(by_cat_base.get(cat, []))
        cedar_vals = sorted(by_cat_cedar.get(cat, []))

        base_med = _median(base_vals)
        cedar_med = _median(cedar_vals)

        # Calculate P95 and P99
        # Python 3.8+ statistics.quantiles is cleaner, but let's use a safe fallback if needed or just simple index logic
        # For very small N, quantiles might error.
        # Using a simple helper _percentile
        ver = 0
        try:
            import sys

            ver = sys.version_info.minor
        except Exception:
            pass

        if ver >= 8 and len(base_vals) >= 2:
            base_p95 = statistics.quantiles(base_vals, n=100, method="inclusive")[94]
            base_p99 = statistics.quantiles(base_vals, n=100, method="inclusive")[98]
        else:
            # Fallback for small lists or older python (though environment is likely new)
            # simple nearest rank
            import math

            def get_p(lst, p):
                if not lst:
                    return 0.0
                k = (len(lst) - 1) * p
                f = math.floor(k)
                c = math.ceil(k)
                if f == c:
                    return lst[int(k)]
                return lst[int(f)] * (c - k) + lst[int(c)] * (k - f)

            base_p95 = get_p(base_vals, 0.95)
            base_p99 = get_p(base_vals, 0.99)

        if ver >= 8 and len(cedar_vals) >= 2:
            cedar_p95 = statistics.quantiles(cedar_vals, n=100, method="inclusive")[94]
            cedar_p99 = statistics.quantiles(cedar_vals, n=100, method="inclusive")[98]
        else:

            def get_p(lst, p):
                if not lst:
                    return 0.0
                k = (len(lst) - 1) * p
                f = math.floor(k)
                c = math.ceil(k)
                if f == c:
                    return lst[int(k)]
                return lst[int(f)] * (c - k) + lst[int(c)] * (k - f)

            cedar_p95 = get_p(cedar_vals, 0.95)
            cedar_p99 = get_p(cedar_vals, 0.99)

        # Calculate overhead metrics based on MEDIAN
        oh = calculate_overhead_metrics(base_med, cedar_med, is_throughput=False)
        rows.append(
            (
                cat,
                base_med,
                cedar_med,
                base_p95,
                cedar_p95,
                base_p99,
                cedar_p99,
                cedar_med - base_med,
                oh["overhead_pct"],
                oh["overhead_factor"],
            )
        )
    return rows


def write_overhead_table_latex(
    rows: list[
        tuple[str, float, float, float, float, float, float, float, float, float]
    ],
    out_path: Path,
) -> None:
    """
    Writes LaTeX table for Query-by-Query Overhead.
    Columns: Operation, Base Med, Cedar Med, Base P95, Cedar P95, Base P99, Cedar P99, Overhead (ms), Overhead (%), Factor
    """
    if not rows:
        return

    # Check validity (index 1 is base_med, index 2 is cedar_med)
    has_baseline = any(row[1] > 0 for row in rows)
    has_cedar = any(row[2] > 0 for row in rows)

    if not (has_baseline and has_cedar):
        return

    lines: list[str] = []
    # 10 columns
    lines.append("\\begin{tabular}{lrrrrrrrrr}")
    lines.append("\\toprule")
    lines.append(
        " & \\multicolumn{2}{c}{Median (ms)} & \\multicolumn{2}{c}{P95 (ms)} & \\multicolumn{2}{c}{P99 (ms)} & \\multicolumn{3}{c}{Overhead (Median)} \\\\"
    )
    lines.append(
        "\\cmidrule(lr){2-3} \\cmidrule(lr){4-5} \\cmidrule(lr){6-7} \\cmidrule(lr){8-10}"
    )
    lines.append(
        "Operation & Base & Cedar & Base & Cedar & Base & Cedar & $\\Delta$ms & \\% & Factor \\\\"
    )
    lines.append("\\midrule")

    for cat, b_med, c_med, b_p95, c_p95, b_p99, c_p99, oh_ms, oh_pct, oh_factor in rows:
        lines.append(
            f"{cat} & {b_med:.2f} & {c_med:.2f} & "
            f"{b_p95:.2f} & {c_p95:.2f} & "
            f"{b_p99:.2f} & {c_p99:.2f} & "
            f"{oh_ms:.2f} & {oh_pct:.1f} & {oh_factor:.2f}x\\\\"
        )
    lines.append("\\bottomrule")
    lines.append("\\end{tabular}")
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text("\n".join(lines))


def write_overhead_table_csv(
    rows: list[
        tuple[str, float, float, float, float, float, float, float, float, float]
    ],
    out_path: Path,
) -> None:
    if not rows:
        return

    has_baseline = any(row[1] > 0 for row in rows)
    has_cedar = any(row[2] > 0 for row in rows)
    if not (has_baseline and has_cedar):
        return

    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open("w", newline="") as f:
        w = csv.writer(f)
        w.writerow(
            [
                "operation",
                "baseline_median_ms",
                "cedar_median_ms",
                "baseline_p95_ms",
                "cedar_p95_ms",
                "baseline_p99_ms",
                "cedar_p99_ms",
                "overhead_ms",
                "overhead_pct",
                "overhead_factor",
            ]
        )
        for row in rows:
            w.writerow(row)


def write_latency_distributions_csv(
    result_payload: dict[str, Any], out_dir: Path
) -> None:
    """
    Writes two CSVs: baseline_latencies.csv and cedar_latencies.csv
    Columns: category, latency_ms
    """
    out_dir.mkdir(parents=True, exist_ok=True)
    for key in ("baseline", "cedar"):
        rows = result_payload.get(key, [])
        if not rows:
            continue

        out_path = out_dir / f"{key}_latencies.csv"
        with out_path.open("w", newline="") as f:
            w = csv.writer(f)
            w.writerow(["category", "latency_ms"])
            for r in rows:
                category = r.get("category") or r.get("action")
                w.writerow([category, f"{float(r['latency_ms']):.6f}"])


def _normalize_benchmark_payload(payload: dict[str, Any]) -> dict[str, Any]:
    """Normalize result formats to a baseline/cedar list payload.

    Some experiments write a "multi_run" wrapper (with top-level "runs" and
    "aggregate_stats"). Most analysis helpers expect top-level "baseline" and
    "cedar" arrays.

    This function preserves the original payload keys (like "aggregate_stats")
    but ensures "baseline"/"cedar" are present.
    """

    if not payload.get("multi_run"):
        return payload

    runs = payload.get("runs", []) or []
    baseline_rows: list[dict[str, Any]] = []
    cedar_rows: list[dict[str, Any]] = []

    for r in runs:
        if not isinstance(r, dict):
            continue
        baseline_rows.extend(r.get("baseline", []) or [])
        cedar_rows.extend(r.get("cedar", []) or [])

    normalized = dict(payload)
    normalized["baseline"] = baseline_rows
    normalized["cedar"] = cedar_rows
    return normalized


def analyze_to_outputs(
    results_json_path: Path,
    output_dir: Path,
) -> dict[str, Any]:
    """
    High-level entry: reads results JSON, writes LaTeX table and CSVs.
    Returns summary with written file paths.
    """
    payload = json.loads(results_json_path.read_text())
    payload = _normalize_benchmark_payload(payload)

    rows = compute_query_overhead(payload)
    latex_path = output_dir / "query_by_query_overhead.tex"
    csv_path = output_dir / "query_by_query_overhead.csv"
    write_overhead_table_latex(rows, latex_path)
    write_overhead_table_csv(rows, csv_path)
    write_latency_distributions_csv(payload, output_dir)
    return {
        "latex_table": str(latex_path),
        "csv_table": str(csv_path),
        "baseline_latencies": str(output_dir / "baseline_latencies.csv"),
        "cedar_latencies": str(output_dir / "cedar_latencies.csv"),
    }
