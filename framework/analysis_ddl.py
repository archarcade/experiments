#!/usr/bin/env python3
"""
DDL Operations Analysis Module (E10).

Parses DDL operation results and generates summary tables and CSVs for paper artifacts.

USENIX-style artifacts:
- ddl_summary.csv: DDL operation latency comparison
- ddl_summary.tex: LaTeX table for paper
"""

from __future__ import annotations

import csv
import json
import statistics
from pathlib import Path
from typing import Any


def extract_ddl_results(results_json: Path) -> list[dict[str, Any]]:
    """
    Extract DDL operation results from a comprehensive results JSON file.

    Args:
        results_json: Path to ddl_comprehensive_results.json

    Returns:
        List of result dictionaries with operation details
    """
    if not results_json.exists():
        return []

    try:
        data = json.loads(results_json.read_text())
    except Exception:
        return []

    results = []

    # Handle different JSON formats
    # Format 1: {"baseline": {...}, "cedar": {...}}
    if "baseline" in data and "cedar" in data:
        baseline_ops = data.get("baseline", {}).get("operations", [])
        cedar_ops = data.get("cedar", {}).get("operations", [])

        # Group by operation type
        baseline_by_type: dict[str, list[float]] = {}
        cedar_by_type: dict[str, list[float]] = {}

        for op in baseline_ops:
            op_type = op.get("operation_type", op.get("type", "UNKNOWN"))
            latency = float(op.get("latency_ms", 0))
            success = op.get("success", True)
            if success and latency > 0:
                baseline_by_type.setdefault(op_type, []).append(latency)

        for op in cedar_ops:
            op_type = op.get("operation_type", op.get("type", "UNKNOWN"))
            latency = float(op.get("latency_ms", 0))
            success = op.get("success", True)
            if success and latency > 0:
                cedar_by_type.setdefault(op_type, []).append(latency)

        all_types = sorted(set(baseline_by_type.keys()) | set(cedar_by_type.keys()))

        for op_type in all_types:
            base_lats = baseline_by_type.get(op_type, [])
            cedar_lats = cedar_by_type.get(op_type, [])

            if base_lats and cedar_lats:
                base_median = statistics.median(base_lats)
                cedar_median = statistics.median(cedar_lats)
                overhead_ms = cedar_median - base_median
                overhead_pct = (
                    (overhead_ms / base_median * 100) if base_median > 0 else 0
                )

                results.append(
                    {
                        "operation_type": op_type,
                        "baseline_median_ms": round(base_median, 3),
                        "cedar_median_ms": round(cedar_median, 3),
                        "overhead_ms": round(overhead_ms, 3),
                        "overhead_pct": round(overhead_pct, 2),
                        "baseline_count": len(base_lats),
                        "cedar_count": len(cedar_lats),
                    }
                )

    # Format 2: List of operations directly
    elif isinstance(data, list):
        # Group by system and operation type
        by_system_type: dict[str, dict[str, list[float]]] = {
            "baseline": {},
            "cedar": {},
        }

        for op in data:
            system = op.get("system", "baseline")
            op_type = op.get("operation_type", op.get("type", "UNKNOWN"))
            latency = float(op.get("latency_ms", 0))
            success = op.get("success", True)

            if success and latency > 0:
                by_system_type.setdefault(system, {}).setdefault(op_type, []).append(
                    latency
                )

        all_types = sorted(
            set(by_system_type.get("baseline", {}).keys())
            | set(by_system_type.get("cedar", {}).keys())
        )

        for op_type in all_types:
            base_lats = by_system_type.get("baseline", {}).get(op_type, [])
            cedar_lats = by_system_type.get("cedar", {}).get(op_type, [])

            if base_lats and cedar_lats:
                base_median = statistics.median(base_lats)
                cedar_median = statistics.median(cedar_lats)
                overhead_ms = cedar_median - base_median
                overhead_pct = (
                    (overhead_ms / base_median * 100) if base_median > 0 else 0
                )

                results.append(
                    {
                        "operation_type": op_type,
                        "baseline_median_ms": round(base_median, 3),
                        "cedar_median_ms": round(cedar_median, 3),
                        "overhead_ms": round(overhead_ms, 3),
                        "overhead_pct": round(overhead_pct, 2),
                        "baseline_count": len(base_lats),
                        "cedar_count": len(cedar_lats),
                    }
                )

    # Format 3: Grouped results with test results
    elif "test_results" in data:
        test_results = data.get("test_results", [])

        by_type: dict[str, dict[str, list[float]]] = {}

        for test in test_results:
            op_type = test.get("operation_type", test.get("type", "UNKNOWN"))

            base_lat = test.get("baseline_latency_ms", 0)
            cedar_lat = test.get("cedar_latency_ms", 0)

            if base_lat and cedar_lat:
                by_type.setdefault(op_type, {"baseline": [], "cedar": []})
                by_type[op_type]["baseline"].append(float(base_lat))
                by_type[op_type]["cedar"].append(float(cedar_lat))

        for op_type, lats in by_type.items():
            if lats["baseline"] and lats["cedar"]:
                base_median = statistics.median(lats["baseline"])
                cedar_median = statistics.median(lats["cedar"])
                overhead_ms = cedar_median - base_median
                overhead_pct = (
                    (overhead_ms / base_median * 100) if base_median > 0 else 0
                )

                results.append(
                    {
                        "operation_type": op_type,
                        "baseline_median_ms": round(base_median, 3),
                        "cedar_median_ms": round(cedar_median, 3),
                        "overhead_ms": round(overhead_ms, 3),
                        "overhead_pct": round(overhead_pct, 2),
                        "baseline_count": len(lats["baseline"]),
                        "cedar_count": len(lats["cedar"]),
                    }
                )

    return results


def collect_ddl_results(ddl_dir: Path) -> list[dict[str, Any]]:
    """
    Collect all DDL results from a directory.

    Looks for ddl_comprehensive_results.json and tpcc_schema_*.json files.

    Args:
        ddl_dir: Directory containing DDL result files

    Returns:
        Combined list of DDL result dictionaries
    """
    if not ddl_dir.exists():
        return []

    all_results = []

    # Look for comprehensive results
    comprehensive = ddl_dir / "ddl_comprehensive_results.json"
    if comprehensive.exists():
        all_results.extend(extract_ddl_results(comprehensive))

    # Look for TPC-C schema results
    for p in ddl_dir.glob("tpcc_schema_*.json"):
        all_results.extend(extract_ddl_results(p))

    return all_results


def write_ddl_summary_csv(rows: list[dict[str, Any]], out_path: Path) -> None:
    """
    Write DDL summary to CSV file.

    Args:
        rows: List of DDL result dictionaries
        out_path: Path to output CSV file
    """
    if not rows:
        return

    out_path.parent.mkdir(parents=True, exist_ok=True)

    header = [
        "operation_type",
        "baseline_median_ms",
        "cedar_median_ms",
        "overhead_ms",
        "overhead_pct",
        "baseline_count",
        "cedar_count",
    ]

    with out_path.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=header)
        writer.writeheader()
        for row in rows:
            writer.writerow({k: row.get(k, "") for k in header})


def write_ddl_summary_table_tex(rows: list[dict[str, Any]], out_path: Path) -> None:
    """
    Write DDL summary to LaTeX table.

    Args:
        rows: List of DDL result dictionaries
        out_path: Path to output LaTeX file
    """
    if not rows:
        return

    out_path.parent.mkdir(parents=True, exist_ok=True)

    lines = [
        "\\begin{table}[h]",
        "\\centering",
        "\\caption{DDL Operation Authorization Overhead}",
        "\\label{tab:ddl_overhead}",
        "\\begin{tabular}{lrrrr}",
        "\\toprule",
        "Operation Type & Baseline (ms) & Cedar (ms) & Overhead (ms) & Overhead (\\%) \\\\",
        "\\midrule",
    ]

    for row in sorted(rows, key=lambda r: r.get("operation_type", "")):
        op_type = row.get("operation_type", "")
        base = row.get("baseline_median_ms", 0)
        cedar = row.get("cedar_median_ms", 0)
        overhead_ms = row.get("overhead_ms", 0)
        overhead_pct = row.get("overhead_pct", 0)

        lines.append(
            f"{op_type} & {base:.2f} & {cedar:.2f} & "
            f"{overhead_ms:+.2f} & {overhead_pct:+.1f}\\% \\\\"
        )

    lines.extend(["\\bottomrule", "\\end{tabular}", "\\end{table}"])

    out_path.write_text("\n".join(lines))


def generate_ddl_visualizations(
    ddl_dir: Path, output_dir: Path
) -> dict[str, Path | None]:
    """
    Generate all DDL-related visualizations and tables.

    Args:
        ddl_dir: Directory containing DDL result files
        output_dir: Directory to write output files

    Returns:
        Dictionary mapping artifact names to file paths
    """
    results = {}

    ddl_rows = collect_ddl_results(ddl_dir)

    if ddl_rows:
        csv_path = output_dir / "ddl_summary.csv"
        write_ddl_summary_csv(ddl_rows, csv_path)
        results["ddl_summary_csv"] = csv_path

        tex_path = output_dir / "ddl_summary.tex"
        write_ddl_summary_table_tex(ddl_rows, tex_path)
        results["ddl_summary_tex"] = tex_path

    return results
