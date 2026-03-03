#!/usr/bin/env python3
"""
TPC-C analysis helpers (macrobenchmark for MySQL).

USENIX-style paper artifacts:
- tpcc_summary.csv: TPM (throughput) + latency comparison
- tpcc_summary.tex: Compact LaTeX table for paper
"""

from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Any


def extract_tpcc_result_row(results_json: Path) -> dict[str, Any] | None:
    """
    Extract TPC-C metrics from a results JSON file.
    Supports tpcc-mysql and sysbench-tpcc formats.
    """
    if not results_json.exists():
        return None

    try:
        data = json.loads(results_json.read_text())
    except Exception:
        return None

    # Detect system keys
    prefix = ""
    tool_name = "sysbench-tpcc-mysql"

    if "postgres-baseline" in data:
        prefix = "postgres-"
        tool_name = "sysbench-tpcc-pg"

    rows = []

    # Common extraction logic for both tools
    for system_suffix in ["baseline", "cedar"]:
        system_key = f"{prefix}{system_suffix}"
        sys_data = data.get(system_key, {})
        # Try "aggregate" key first (multi-run), then "benchmark" (single-run)
        agg = sys_data.get("aggregate", {})
        bench = sys_data.get("benchmark", {})

        tpm = 0.0
        tpm_std = 0.0
        avg_lat = 0.0
        lat_std = 0.0

        if agg:
            # Multi-run format
            tpm = float(agg.get("tpm_mean", 0) or agg.get("tpm_median", 0) or 0.0)
            tpm_std = float(agg.get("tpm_std", 0) or 0.0)
            avg_lat = float(
                agg.get("lat_avg_mean", 0) or agg.get("lat_avg_median", 0) or 0.0
            )
            lat_std = float(agg.get("lat_avg_std", 0) or 0.0)
        elif bench:
            # Single-run format
            tpm = float(bench.get("tpm") or bench.get("tps", 0) * 60.0)
            avg_lat = float(
                bench.get("avg_latency_ms") or bench.get("new_order_avg_ms") or 0.0
            )
        else:
            continue

        rows.append(
            {
                "system": system_suffix,
                "tpm": round(tpm, 2),
                "tpm_std": round(tpm_std, 2),
                "avg_latency_ms": round(avg_lat, 2),
                "lat_std": round(lat_std, 2),
                "success": bool(sys_data.get("success", True)),
            }
        )

    if len(rows) < 2:
        return None

    base = next(r for r in rows if r["system"] == "baseline")
    cedar = next(r for r in rows if r["system"] == "cedar")

    from framework.stats import calculate_overhead_metrics

    oh_tpm = calculate_overhead_metrics(base["tpm"], cedar["tpm"], is_throughput=True)
    oh_lat = calculate_overhead_metrics(
        base["avg_latency_ms"], cedar["avg_latency_ms"], is_throughput=False
    )

    cfg = data.get("config", {})
    if not cfg:
        # Try extracting from the baseline key we found
        cfg = data.get(f"{prefix}baseline", {}).get("config", {})
    if not cfg:
        # Try cedar
        cfg = data.get(f"{prefix}cedar", {}).get("config", {})

    return {
        "file": results_json.name,
        "tool": tool_name,
        "warehouses": cfg.get("warehouses"),
        "load": cfg.get("terminals") or cfg.get("connections") or cfg.get("threads"),
        "baseline_tpm": base["tpm"],
        "baseline_tpm_std": base["tpm_std"],
        "cedar_tpm": cedar["tpm"],
        "cedar_tpm_std": cedar["tpm_std"],
        "tpm_overhead_pct": round(oh_tpm["overhead_pct"], 2),
        "tpm_overhead_factor": round(oh_tpm["overhead_factor"], 3),
        "baseline_latency_ms": base["avg_latency_ms"],
        "baseline_lat_std": base["lat_std"],
        "cedar_latency_ms": cedar["avg_latency_ms"],
        "cedar_lat_std": cedar["lat_std"],
        "lat_overhead_pct": round(oh_lat["overhead_pct"], 2),
        "lat_overhead_factor": round(oh_lat["overhead_factor"], 3),
    }


def collect_tpcc_results(tpcc_results_dir: Path) -> list[dict[str, Any]]:
    """Collect all TPC-C results from a directory."""
    if not tpcc_results_dir.exists():
        return []
    rows = []
    for p in sorted(tpcc_results_dir.glob("*_results.json")):
        row = extract_tpcc_result_row(p)
        if row:
            rows.append(row)
    return rows


def write_tpcc_summary_csv(rows: list[dict[str, Any]], out_path: Path) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    header = [
        "tool",
        "warehouses",
        "load",
        "baseline_tpm",
        "baseline_tpm_std",
        "cedar_tpm",
        "cedar_tpm_std",
        "tpm_overhead_pct",
        "baseline_latency_ms",
        "baseline_lat_std",
        "cedar_latency_ms",
        "cedar_lat_std",
        "lat_overhead_pct",
    ]
    with out_path.open("w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=header)
        w.writeheader()
        for r in rows:
            w.writerow({k: r.get(k, "") for k in header})


def write_tpcc_summary_table_tex(rows: list[dict[str, Any]], out_path: Path) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    lines = [
        "\\begin{tabular}{lrrrrrrr}",
        "\\toprule",
        "Tool & WH & Load & Baseline TPM & Cedar TPM & $\\Delta$TPM(\\%) & Base Lat(ms) & Cedar Lat(ms) \\\\",
        "\\midrule",
    ]
    for r in rows:
        lines.append(
            f"{r['tool']} & {r['warehouses']} & {r['load']} & "
            f"{r['baseline_tpm']:.1f} & {r['cedar_tpm']:.1f} & {r['tpm_overhead_pct']:+.1f} & "
            f"{r['baseline_latency_ms']:.2f} & {r['cedar_latency_ms']:.2f} \\\\"
        )
    lines.extend(["\\bottomrule", "\\end{tabular}"])
    out_path.write_text("\n".join(lines))
