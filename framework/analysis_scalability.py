from __future__ import annotations

import csv
import json
import statistics
from pathlib import Path
from typing import Any


def compute_policy_scaling_summary(results_dirs: list[Path]) -> list[dict[str, Any]]:
    """
    Computes summary for policy scaling experiments.
    Returns rows: (policy_count, median_ms, p95_ms, p99_ms) for Cedar.
    """
    rows = []
    for r_dir in results_dirs:
        results_path = r_dir / "results.json"
        if not results_path.exists():
            continue

        with results_path.open() as f:
            data = json.load(f)

        policy_count = data.get("metadata", {}).get("policy_count", 0)
        if data.get("multi_run"):
            cedar_latencies = []
            for run in data.get("runs", []):
                cedar_latencies.extend(
                    [
                        r.get("latency_ms", 0)
                        for r in run.get("cedar", [])
                        if r.get("success")
                    ]
                )
        else:
            cedar_latencies = [
                r.get("latency_ms", 0)
                for r in data.get("cedar", [])
                if r.get("success")
            ]

        if not cedar_latencies:
            continue

        cedar_latencies.sort()
        median_ms = statistics.median(cedar_latencies)
        p95_ms = cedar_latencies[int(len(cedar_latencies) * 0.95)]
        p99_ms = cedar_latencies[int(len(cedar_latencies) * 0.99)]

        rows.append(
            {
                "policy_count": policy_count,
                "median_ms": median_ms,
                "p95_ms": p95_ms,
                "p99_ms": p99_ms,
            }
        )

    return sorted(rows, key=lambda x: x["policy_count"])


def write_policy_scaling_csv(summary_data: list[dict[str, Any]], out_path: Path):
    if not summary_data:
        return
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=summary_data[0].keys())
        writer.writeheader()
        writer.writerows(summary_data)


def write_policy_scaling_table_tex(summary_data: list[dict[str, Any]], out_path: Path):
    if not summary_data:
        return

    lines = [
        "\\begin{tabular}{lrrr}",
        "\\toprule",
        "Policy Count & Median (ms) & p95 (ms) & p99 (ms) \\\\",
        "\\midrule",
    ]
    for row in summary_data:
        lines.append(
            f"{row['policy_count']} & {row['median_ms']:.2f} & {row['p95_ms']:.2f} & {row['p99_ms']:.2f} \\\\"
        )
    lines.extend(["\\bottomrule", "\\end{tabular}"])

    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text("\n".join(lines))


def compute_concurrency_summary(
    results_dir: Path,
) -> dict[str, dict[int, dict[str, float]]]:
    """
    Computes summary for concurrency experiments from sysbench results.
    Returns a dict structured as: { 'baseline': {<threads>: {<metrics>}}, 'cedar': ... }
    """
    summary = {"baseline": {}, "cedar": {}}
    for target in ["baseline", "cedar"]:
        target_dir = results_dir / target
        if not target_dir.exists():
            # Try nested path results_dir/concurrency/target (legacy or parent dir passed)
            target_dir = results_dir / "concurrency" / target
            if not target_dir.exists():
                # Try finding concurrency dir in siblings or parents
                # Look for results/tag/concurrency structure
                current = results_dir
                found = False
                for _ in range(3):  # Look up to 3 levels
                    if (current / "concurrency" / target).exists():
                        target_dir = current / "concurrency" / target
                        found = True
                        break
                    current = current.parent
                if not found:
                    continue

        for thread_dir in target_dir.iterdir():
            if thread_dir.is_dir() and thread_dir.name.startswith("threads_"):
                try:
                    threads = int(thread_dir.name.split("_")[1])
                    summary_path = thread_dir / "summary.json"
                    if summary_path.exists():
                        with summary_path.open() as f:
                            metrics = json.load(f)
                            # Backwards/forwards compatibility with evolving summary schemas
                            if "qps" not in metrics and "qps_median" in metrics:
                                metrics["qps"] = metrics.get("qps_median")
                            if (
                                "lat_p95_ms" not in metrics
                                and "lat_p95_median" in metrics
                            ):
                                metrics["lat_p95_ms"] = metrics.get("lat_p95_median")
                            if (
                                "lat_p99_ms" not in metrics
                                and "lat_p99_median" in metrics
                            ):
                                metrics["lat_p99_ms"] = metrics.get("lat_p99_median")
                            if (
                                "lat_avg_ms" not in metrics
                                and "lat_avg_median" in metrics
                            ):
                                metrics["lat_avg_ms"] = metrics.get("lat_avg_median")

                            summary[target][threads] = metrics
                except (ValueError, IndexError):
                    continue
    return summary


def write_concurrency_throughput_csv(
    summary_data: dict[str, dict[int, dict[str, float]]], out_path: Path
):
    baseline_res = summary_data.get("baseline", {})
    cedar_res = summary_data.get("cedar", {})

    header = ["threads", "baseline_qps", "cedar_qps", "overhead_pct", "overhead_factor"]
    rows = []

    all_threads = sorted(set(baseline_res.keys()) | set(cedar_res.keys()))

    for t in all_threads:
        baseline_qps = baseline_res.get(t, {}).get("qps", 0)
        cedar_qps = cedar_res.get(t, {}).get("qps", 0)
        from .stats import calculate_overhead_metrics

        oh = calculate_overhead_metrics(baseline_qps, cedar_qps, is_throughput=True)
        rows.append(
            {
                "threads": t,
                "baseline_qps": f"{baseline_qps:.2f}",
                "cedar_qps": f"{cedar_qps:.2f}",
                "overhead_pct": f"{oh['overhead_pct']:.2f}",
                "overhead_factor": f"{oh['overhead_factor']:.2f}",
            }
        )

    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=header)
        writer.writeheader()
        writer.writerows(rows)


def write_concurrency_latency_csv(
    summary_data: dict[str, dict[int, dict[str, float]]], out_path: Path
):
    header = ["threads", "system", "p50_ms", "p95_ms", "p99_ms"]
    rows = []

    for system, results in summary_data.items():
        for threads, metrics in sorted(results.items()):
            rows.append(
                {
                    "threads": threads,
                    "system": system,
                    "p50_ms": f"{(metrics.get('lat_avg_ms') or 0):.2f}",  # Sysbench avg is close to p50
                    "p95_ms": f"{(metrics.get('lat_p95_ms') or 0):.2f}",
                    "p99_ms": f"{(metrics.get('lat_p99_ms') or 0):.2f}",
                }
            )

    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=header)
        writer.writeheader()
        writer.writerows(rows)


def write_concurrency_throughput_table_tex(
    summary_data: dict[str, dict[int, dict[str, float]]], out_path: Path
):
    lines = [
        "\\begin{tabular}{rrrrr}",
        "\\toprule",
        "Threads & Baseline QPS & Cedar QPS & Overhead (\\%) & Factor \\\\",
        "\\midrule",
    ]

    baseline_res = summary_data.get("baseline", {})
    cedar_res = summary_data.get("cedar", {})
    all_threads = sorted(set(baseline_res.keys()) | set(cedar_res.keys()))

    for t in all_threads:
        baseline_qps = baseline_res.get(t, {}).get("qps", 0)
        cedar_qps = cedar_res.get(t, {}).get("qps", 0)
        from .stats import calculate_overhead_metrics

        oh = calculate_overhead_metrics(baseline_qps, cedar_qps, is_throughput=True)
        lines.append(
            f"{t} & {baseline_qps:.2f} & {cedar_qps:.2f} & {oh['overhead_pct']:.1f} & {oh['overhead_factor']:.2f}x \\\\"
        )

    lines.extend(["\\bottomrule", "\\end{tabular}"])
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text("\n".join(lines))
