#!/usr/bin/env python3
"""
pgbench analysis helpers (PostgreSQL macrobenchmark).

USENIX-style paper artifacts:
- pgbench_summary.csv: per-configuration throughput + latency comparison
  (baseline vs Cedar)
- pgbench_summary.tex: compact LaTeX table for paper appendix / report
"""

from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Any


def _safe_get(d: dict[str, Any], path: list[str], default: Any = None) -> Any:
    cur: Any = d
    for k in path:
        if not isinstance(cur, dict) or k not in cur:
            return default
        cur = cur[k]
    return cur


def _derived_tps(system_payload: dict[str, Any]) -> float:
    """Fallback TPS if parser failed: transactions_processed / duration."""
    txns = (
        _safe_get(
            system_payload,
            ["results", "benchmark", "transactions_processed"],
            0,
        )
        or 0
    )
    dur = (
        _safe_get(
            system_payload,
            ["results", "benchmark", "config", "duration"],
            0,
        )
        or 0
    )
    try:
        txns_i = int(txns)
    except (TypeError, ValueError):
        txns_i = 0
    try:
        dur_i = int(dur)
    except (TypeError, ValueError):
        dur_i = 0
    return (txns_i / dur_i) if dur_i > 0 else 0.0


def extract_pgbench_comparison_row(
    comparison_json: Path,
) -> dict[str, Any] | None:
    """
    Extract a single comparison row from pgbench_comparison_*.json.
    Returns None if parsing fails.
    """
    if not comparison_json.exists():
        return None

    data = json.loads(comparison_json.read_text())
    cfg = data.get("config", {}) or {}
    systems = data.get("systems", {}) or {}
    base = systems.get("postgres-baseline", {}) or {}
    cedar = systems.get("postgres-cedar", {}) or {}

    baseline_tps = float(base.get("tps") or 0.0)
    cedar_tps = float(cedar.get("tps") or 0.0)
    if baseline_tps <= 0:
        baseline_tps = _derived_tps(base)
    if cedar_tps <= 0:
        cedar_tps = _derived_tps(cedar)

    baseline_lat = float(base.get("avg_latency_ms") or 0.0)
    cedar_lat = float(cedar.get("avg_latency_ms") or 0.0)

    from framework.stats import calculate_overhead_metrics

    oh_tps = calculate_overhead_metrics(baseline_tps, cedar_tps, is_throughput=True)
    oh_lat = calculate_overhead_metrics(baseline_lat, cedar_lat, is_throughput=False)

    return {
        "file": comparison_json.name,
        "scale": int(cfg.get("scale") or 0),
        "clients": int(cfg.get("clients") or 0),
        "duration_s": int(cfg.get("duration") or 0),
        "builtin": str(cfg.get("builtin") or ""),
        "baseline_tps": round(baseline_tps, 3),
        "cedar_tps": round(cedar_tps, 3),
        "tps_overhead_pct": round(oh_tps["overhead_pct"], 3),
        "tps_overhead_factor": round(oh_tps["overhead_factor"], 3),
        "baseline_avg_latency_ms": round(baseline_lat, 3),
        "cedar_avg_latency_ms": round(cedar_lat, 3),
        "lat_overhead_pct": round(oh_lat["overhead_pct"], 3),
        "lat_overhead_factor": round(oh_lat["overhead_factor"], 3),
        "baseline_success": bool(base.get("success", False)),
        "cedar_success": bool(cedar.get("success", False)),
    }


def collect_pgbench_comparisons(
    pgbench_results_dir: Path,
) -> list[dict[str, Any]]:
    """
    Collect rows from all pgbench comparison JSON files in a results directory.
    """
    if not pgbench_results_dir.exists():
        return []
    rows: list[dict[str, Any]] = []
    for p in sorted(pgbench_results_dir.glob("pgbench_comparison_*.json")):
        row = extract_pgbench_comparison_row(p)
        if row:
            rows.append(row)
    return rows


def write_pgbench_summary_csv(
    rows: list[dict[str, Any]],
    out_path: Path,
) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    header = [
        "file",
        "scale",
        "clients",
        "duration_s",
        "builtin",
        "baseline_tps",
        "cedar_tps",
        "tps_overhead_pct",
        "baseline_avg_latency_ms",
        "cedar_avg_latency_ms",
        "lat_overhead_pct",
        "baseline_success",
        "cedar_success",
    ]
    with out_path.open("w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=header)
        w.writeheader()
        for r in rows:
            row_out = {k: r.get(k, "") for k in header}
            w.writerow(row_out)


def write_pgbench_summary_table_tex(
    rows: list[dict[str, Any]],
    out_path: Path,
) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    lines: list[str] = [
        "\\begin{tabular}{rrrrrrrr}",
        "\\toprule",
        "Scale & Clients & Dur(s) & Baseline TPS & Cedar TPS & "
        "$\\Delta$TPS(\\%) & Baseline Lat(ms) & Cedar Lat(ms) \\\\",
        "\\midrule",
    ]
    for r in rows:
        scale = int(r.get("scale", 0))
        clients = int(r.get("clients", 0))
        duration_s = int(r.get("duration_s", 0))
        baseline_tps = float(r.get("baseline_tps", 0.0))
        cedar_tps = float(r.get("cedar_tps", 0.0))
        tps_overhead_pct = float(r.get("tps_overhead_pct", 0.0))
        baseline_lat = float(r.get("baseline_avg_latency_ms", 0.0))
        cedar_lat = float(r.get("cedar_avg_latency_ms", 0.0))
        lines.append(
            f"{scale} & {clients} & {duration_s} & "
            f"{baseline_tps:.1f} & {cedar_tps:.1f} & "
            f"{tps_overhead_pct:+.1f} & "
            f"{baseline_lat:.2f} & {cedar_lat:.2f} \\\\"
        )
    lines.extend(["\\bottomrule", "\\end{tabular}"])
    out_path.write_text("\n".join(lines))
