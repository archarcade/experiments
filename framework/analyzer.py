#!/usr/bin/env python3
"""
Results analysis utilities.
"""

from __future__ import annotations

from statistics import mean, median
from typing import Any


def _percentile(sorted_values: list[float], p: float) -> float:
    if not sorted_values:
        return 0.0
    idx = int(len(sorted_values) * p)
    idx = max(0, min(idx, len(sorted_values) - 1))
    return sorted_values[idx]


class ResultsAnalyzer:
    def __init__(self, results: dict[str, Any]):
        self.results = results

    def _stats(self, latencies: list[float]) -> dict[str, float]:
        if not latencies:
            return {"mean": 0.0, "median": 0.0, "p95": 0.0, "p99": 0.0}
        vals = sorted(latencies)
        return {
            "mean": float(mean(vals)),
            "median": float(median(vals)),
            "p95": float(_percentile(vals, 0.95)),
            "p99": float(_percentile(vals, 0.99)),
        }

    def _normalize_rows(self) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        """Return (baseline_rows, cedar_rows) for single-run and multi-run formats."""

        if not self.results.get("multi_run"):
            return (
                self.results.get("baseline", []) or [],
                self.results.get("cedar", []) or [],
            )

        baseline_rows: list[dict[str, Any]] = []
        cedar_rows: list[dict[str, Any]] = []
        for run in self.results.get("runs", []) or []:
            if not isinstance(run, dict):
                continue
            baseline_rows.extend(run.get("baseline", []) or [])
            cedar_rows.extend(run.get("cedar", []) or [])
        return baseline_rows, cedar_rows

    def compute_summary(self) -> dict[str, Any]:
        baseline, cedar = self._normalize_rows()

        baseline_lat = [
            float(r.get("latency_ms", 0.0))
            for r in baseline
            if r.get("latency_ms") is not None
        ]
        cedar_lat = [
            float(r.get("latency_ms", 0.0))
            for r in cedar
            if r.get("latency_ms") is not None
        ]

        base_stats = self._stats(baseline_lat)
        cedar_stats = self._stats(cedar_lat)

        from .stats import calculate_overhead_metrics

        oh = calculate_overhead_metrics(
            base_stats["median"], cedar_stats["median"], is_throughput=False
        )

        summary: dict[str, Any] = {
            "baseline_median_ms": round(base_stats["median"], 3),
            "cedar_median_ms": round(cedar_stats["median"], 3),
            "overhead_ms": round(cedar_stats["median"] - base_stats["median"], 3),
            "overhead_pct": round(oh["overhead_pct"], 2),
            "overhead_factor": round(oh["overhead_factor"], 3),
            "baseline_mean_ms": round(base_stats["mean"], 3),
            "cedar_mean_ms": round(cedar_stats["mean"], 3),
            "baseline_p95_ms": round(base_stats["p95"], 3),
            "cedar_p95_ms": round(cedar_stats["p95"], 3),
            "baseline_p99_ms": round(base_stats["p99"], 3),
            "cedar_p99_ms": round(cedar_stats["p99"], 3),
            "iterations": int(self.results.get("metadata", {}).get("iterations", 0)),
            "warmup_iterations": int(
                self.results.get("metadata", {}).get("warmup_iterations", 0)
            ),
        }

        # If the experiment emitted CI/aggregate stats, include them verbatim so
        # downstream analysis doesn't silently drop them.
        if self.results.get("multi_run"):
            summary["multi_run"] = True
            summary["n_runs"] = int(self.results.get("n_runs", 0) or 0)
            summary["confidence_level"] = float(
                self.results.get("confidence_level", 0) or 0
            )

            agg = self.results.get("aggregate_stats") or {}
            if isinstance(agg, dict):
                summary["aggregate_stats"] = agg

                # Convenience flattened fields for quick inspection / CSV.
                overhead_ci = agg.get("overhead_ci") or {}
                baseline_ci = agg.get("baseline_ci") or {}
                cedar_ci = agg.get("cedar_ci") or {}

                if isinstance(overhead_ci, dict):
                    summary["overhead_pct_ci_lower"] = overhead_ci.get("lower")
                    summary["overhead_pct_ci_upper"] = overhead_ci.get("upper")
                if isinstance(baseline_ci, dict):
                    summary["baseline_median_ci_lower_ms"] = baseline_ci.get("lower")
                    summary["baseline_median_ci_upper_ms"] = baseline_ci.get("upper")
                if isinstance(cedar_ci, dict):
                    summary["cedar_median_ci_lower_ms"] = cedar_ci.get("lower")
                    summary["cedar_median_ci_upper_ms"] = cedar_ci.get("upper")

        return summary
