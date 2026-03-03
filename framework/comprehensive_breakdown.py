#!/usr/bin/env python3
"""
Comprehensive overhead breakdown: unifies high-level metrics, query-by-category
overhead, and internal DB stage breakdowns.
"""

from __future__ import annotations

import csv
import json
import statistics
from pathlib import Path
from typing import Any


class ComprehensiveOverheadAnalyzer:
    def __init__(self, results_dir: Path, analysis_dir: Path):
        self.results_dir = results_dir
        self.analysis_dir = analysis_dir

    def analyze(self) -> dict[str, Any]:
        """
        Produce a unified report by combining multiple sources of analysis.
        """
        report = {
            "summary": self._get_high_level_summary(),
            "query_breakdown": self._get_query_breakdown(),
            "stage_breakdown": self._get_stage_breakdown(),
        }
        return report

    def _get_high_level_summary(self) -> dict[str, Any]:
        pair_file = self.results_dir / "pair_result.json"
        tpcc_file_mysql = self.results_dir / "tpcc_mysql_results.json"
        tpcc_file_sysbench = self.results_dir / "sysbench_tpcc_results.json"
        pgbench_files = list(self.results_dir.glob("pgbench_comparison_*.json"))

        if pair_file.exists():
            # ... (keep existing pair_file logic)
            pass

        elif tpcc_file_mysql.exists() or tpcc_file_sysbench.exists():
            tpcc_file = (
                tpcc_file_mysql if tpcc_file_mysql.exists() else tpcc_file_sysbench
            )
            data = json.load(tpcc_file.open())
            base = data.get("baseline", {}).get("benchmark", {})
            cedar = data.get("cedar", {}).get("benchmark", {})
            if not base or not cedar:
                return {}

            base_lat = base.get("avg_latency_ms", base.get("new_order_avg_ms", 0))
            cedar_lat = cedar.get("avg_latency_ms", cedar.get("new_order_avg_ms", 0))

            from framework.stats import calculate_overhead_metrics

            oh_lat = calculate_overhead_metrics(
                base_lat, cedar_lat, is_throughput=False
            )

            base_tpm = base.get("tpm", base.get("tps", 0) * 60)
            cedar_tpm = cedar.get("tpm", cedar.get("tps", 0) * 60)
            oh_tpm = calculate_overhead_metrics(base_tpm, cedar_tpm, is_throughput=True)

            return {
                "baseline_median_ms": base_lat,
                "cedar_median_ms": cedar_lat,
                "overhead_ms": cedar_lat - base_lat,
                "overhead_pct": oh_lat["overhead_pct"],
                "overhead_factor": oh_lat["overhead_factor"],
                "baseline_qps": base_tpm / 60.0,
                "cedar_qps": cedar_tpm / 60.0,
                "qps_degradation_pct": ((base_tpm - cedar_tpm) / base_tpm * 100)
                if base_tpm > 0
                else 0,  # Keep for backward compatibility
                "throughput_overhead_pct": oh_tpm["overhead_pct"],
                "throughput_overhead_factor": oh_tpm["overhead_factor"],
            }

        elif pgbench_files:
            data = json.load(pgbench_files[0].open())
            base = data.get("systems", {}).get("postgres-baseline", {})
            cedar = data.get("systems", {}).get("postgres-cedar", {})
            if not base or not cedar:
                return {}

            base_lat = base.get("avg_latency_ms", 0)
            cedar_lat = cedar.get("avg_latency_ms", 0)

            from framework.stats import calculate_overhead_metrics

            oh_lat = calculate_overhead_metrics(
                base_lat, cedar_lat, is_throughput=False
            )

            base_tps = base.get("tps", 0)
            cedar_tps = cedar.get("tps", 0)
            oh_tps = calculate_overhead_metrics(base_tps, cedar_tps, is_throughput=True)

            return {
                "baseline_median_ms": base_lat,
                "cedar_median_ms": cedar_lat,
                "overhead_ms": cedar_lat - base_lat,
                "overhead_pct": oh_lat["overhead_pct"],
                "overhead_factor": oh_lat["overhead_factor"],
                "baseline_qps": base_tps,
                "cedar_qps": cedar_tps,
                "qps_degradation_pct": ((base_tps - cedar_tps) / base_tps * 100)
                if base_tps > 0
                else 0,
                "throughput_overhead_pct": oh_tps["overhead_pct"],
                "throughput_overhead_factor": oh_tps["overhead_factor"],
            }

        return {}

    def _get_query_breakdown(self) -> list[dict[str, Any]]:
        results_file = self.results_dir / "combined_results.json"
        tpcc_file_mysql = self.results_dir / "tpcc_mysql_results.json"
        tpcc_file_sysbench = self.results_dir / "sysbench_tpcc_results.json"
        pgbench_files = list(self.results_dir.glob("pgbench_comparison_*.json"))

        if (
            results_file.exists()
            or (self.results_dir / "baseline" / "results.json").exists()
        ):
            # ... (keep existing results_file logic)
            pass

        elif tpcc_file_mysql.exists() or tpcc_file_sysbench.exists():
            tpcc_file = (
                tpcc_file_mysql if tpcc_file_mysql.exists() else tpcc_file_sysbench
            )
            data = json.load(tpcc_file.open())
            base = data.get("baseline", {}).get("benchmark", {})
            cedar = data.get("cedar", {}).get("benchmark", {})
            if not base or not cedar:
                return []

            base_lat = base.get("avg_latency_ms", base.get("new_order_avg_ms", 0))
            cedar_lat = cedar.get("avg_latency_ms", cedar.get("new_order_avg_ms", 0))
            if base_lat > 0:
                from framework.stats import calculate_overhead_metrics

                oh = calculate_overhead_metrics(
                    base_lat, cedar_lat, is_throughput=False
                )
                return [
                    {
                        "operation": "TPC-C Transaction",
                        "baseline_ms": base_lat,
                        "cedar_ms": cedar_lat,
                        "overhead_ms": cedar_lat - base_lat,
                        "overhead_pct": oh["overhead_pct"],
                        "overhead_factor": oh["overhead_factor"],
                    }
                ]
            return []

        elif pgbench_files:
            data = json.load(pgbench_files[0].open())
            base_full = data.get("systems", {}).get("postgres-baseline", {})
            cedar_full = data.get("systems", {}).get("postgres-cedar", {})
            base = base_full.get("results", {}).get("benchmark", {})
            cedar = cedar_full.get("results", {}).get("benchmark", {})
            if not base or not cedar:
                return []

            out = []

            from framework.stats import calculate_overhead_metrics

            # 1. Overall transaction overhead
            base_lat = base.get("avg_latency_ms", 0)
            cedar_lat = cedar.get("avg_latency_ms", 0)
            builtin = data["config"].get("builtin") or (
                Path(data["config"]["script"]).name
                if data["config"].get("script")
                else "custom"
            )
            if base_lat > 0:
                oh = calculate_overhead_metrics(
                    base_lat, cedar_lat, is_throughput=False
                )
                out.append(
                    {
                        "operation": f"Full Transaction ({builtin})",
                        "baseline_ms": base_lat,
                        "cedar_ms": cedar_lat,
                        "overhead_ms": cedar_lat - base_lat,
                        "overhead_pct": oh["overhead_pct"],
                        "overhead_factor": oh["overhead_factor"],
                    }
                )

            # 2. Per-statement overhead if available
            base_stmt = base.get("statement_latencies", {})
            cedar_stmt = cedar.get("statement_latencies", {})

            if base_stmt and cedar_stmt:
                for stmt, b_ms in base_stmt.items():
                    if stmt in cedar_stmt:
                        c_ms = cedar_stmt[stmt]
                        if b_ms > 0:
                            oh = calculate_overhead_metrics(
                                b_ms, c_ms, is_throughput=False
                            )
                            out.append(
                                {
                                    "operation": f"SQL: {stmt[:50]}...",
                                    "baseline_ms": b_ms,
                                    "cedar_ms": c_ms,
                                    "overhead_ms": c_ms - b_ms,
                                    "overhead_pct": oh["overhead_pct"],
                                    "overhead_factor": oh["overhead_factor"],
                                }
                            )

            return out

        return []

    def _get_stage_breakdown(self) -> dict[str, Any]:
        # Search patterns for profiling files
        base_prof_path = None
        cedar_prof_path = None
        db_type = None

        # Try a few common locations
        search_dirs = [
            self.analysis_dir / "profiling",
            self.results_dir / "profiling",
            self.analysis_dir,
            self.results_dir,
            self.analysis_dir.parent / "profiling",
            self.results_dir.parent / "profiling",
            self.analysis_dir.parent,
            self.results_dir.parent,
        ]

        for d in search_dirs:
            # Check MySQL first
            p_base_mysql = d / "mysql_baseline_perf_schema.json"
            p_cedar_mysql = d / "mysql_cedar_perf_schema.json"
            if p_base_mysql.exists() and p_cedar_mysql.exists():
                base_prof_path, cedar_prof_path = p_base_mysql, p_cedar_mysql
                db_type = "mysql"
                break

            # Then Check Postgres
            # We used postgres-baseline and postgres-cedar in pgbench
            p_base_pg = d / "postgres_postgres-baseline_explain.json"
            p_cedar_pg = d / "postgres_postgres-cedar_explain.json"
            if p_base_pg.exists() and p_cedar_pg.exists():
                base_prof_path, cedar_prof_path = p_base_pg, p_cedar_pg
                db_type = "postgres"
                break

            # Fallback for simple names
            p_base_pg_alt = d / "postgres_baseline_explain.json"
            p_cedar_pg_alt = d / "postgres_cedar_explain.json"
            if p_base_pg_alt.exists() and p_cedar_pg_alt.exists():
                base_prof_path, cedar_prof_path = p_base_pg_alt, p_cedar_pg_alt
                db_type = "postgres"
                break

        if not base_prof_path:
            # Recursive fallback for MySQL
            curr = self.analysis_dir
            for _ in range(4):
                p_dir = curr / "profiling"
                if p_dir.exists():
                    # Check MySQL
                    p_base = p_dir / "mysql_baseline_perf_schema.json"
                    p_cedar = p_dir / "mysql_cedar_perf_schema.json"
                    if p_base.exists() and p_cedar.exists():
                        base_prof_path, cedar_prof_path = p_base, p_cedar
                        db_type = "mysql"
                        break
                    # Check Postgres
                    p_base = p_dir / "postgres_postgres-baseline_explain.json"
                    p_cedar = p_dir / "postgres_postgres-cedar_explain.json"
                    if p_base.exists() and p_cedar.exists():
                        base_prof_path, cedar_prof_path = p_base, p_cedar
                        db_type = "postgres"
                        break
                curr = curr.parent
                if curr == curr.parent:
                    break

        if not base_prof_path or not cedar_prof_path:
            # Check if we have pg_authorization plugin stats in the main results file
            pgbench_files = list(self.results_dir.glob("pgbench_comparison_*.json"))
            if pgbench_files:
                data = json.load(pgbench_files[0].open())
                cedar_res = (
                    data.get("systems", {})
                    .get("postgres-cedar", {})
                    .get("results", {})
                    .get("benchmark", {})
                )
                auth_stats = cedar_res.get("auth_stats", {})
                if auth_stats and auth_stats.get("avg_total_time_ms"):
                    avg_total = auth_stats["avg_total_time_ms"]
                    avg_remote = auth_stats.get("avg_remote_time_ms", 0)
                    network = avg_total - avg_remote

                    return {
                        "stages": [
                            {
                                "stage": "Auth Plugin (Total Hook)",
                                "baseline_total_ms": 0,
                                "cedar_total_ms": avg_total,
                                "delta_ms": avg_total,
                            },
                            {
                                "stage": "  -> Cedar Evaluation (Remote)",
                                "baseline_total_ms": 0,
                                "cedar_total_ms": avg_remote,
                                "delta_ms": avg_remote,
                            },
                            {
                                "stage": "  -> Network Trip + Overhead",
                                "baseline_total_ms": 0,
                                "cedar_total_ms": network,
                                "delta_ms": network,
                            },
                        ]
                    }
            return {}

        base_prof = json.loads(base_prof_path.read_text())
        cedar_prof = json.loads(cedar_prof_path.read_text())

        if db_type == "mysql":

            def _get_stages(prof):
                return {
                    s["event_name"]: s["delta_ms"] for s in prof.get("stages_delta", [])
                }

            base_stages = _get_stages(base_prof)
            cedar_stages = _get_stages(cedar_prof)

            all_stage_names = sorted(set(base_stages.keys()) | set(cedar_stages.keys()))
            stage_diffs = []
            for name in all_stage_names:
                b_ms = base_stages.get(name, 0)
                c_ms = cedar_stages.get(name, 0)
                diff = c_ms - b_ms
                if abs(diff) > 0.01:
                    stage_diffs.append(
                        {
                            "stage": name.replace("stage/sql/", ""),
                            "baseline_total_ms": b_ms,
                            "cedar_total_ms": c_ms,
                            "delta_ms": diff,
                        }
                    )
            stage_diffs.sort(key=lambda x: abs(x["delta_ms"]), reverse=True)
            return {"stages": stage_diffs}

        elif db_type == "postgres":
            # For Postgres, we compare median planning and execution times
            def _get_avg_times(prof):
                results = prof.get("results", [])
                if not results:
                    return {}
                planning = [r["planning_ms"] for r in results if "planning_ms" in r]
                execution = [r["execution_ms"] for r in results if "execution_ms" in r]
                return {
                    "planning_ms": statistics.median(planning) if planning else 0,
                    "execution_ms": statistics.median(execution) if execution else 0,
                }

            base_avg = _get_avg_times(base_prof)
            cedar_avg = _get_avg_times(cedar_prof)

            return {
                "stages": [
                    {
                        "stage": "Postgres Planning",
                        "baseline_total_ms": base_avg["planning_ms"],
                        "cedar_total_ms": cedar_avg["planning_ms"],
                        "delta_ms": cedar_avg["planning_ms"] - base_avg["planning_ms"],
                    },
                    {
                        "stage": "Postgres Execution",
                        "baseline_total_ms": base_avg["execution_ms"],
                        "cedar_total_ms": cedar_avg["execution_ms"],
                        "delta_ms": cedar_avg["execution_ms"]
                        - base_avg["execution_ms"],
                    },
                ]
            }

        return {}

    def generate_report_latex(self, out_path: Path):
        report = self.analyze()
        if not report["summary"]:
            return

        lines = []
        lines.append("\\begin{table}[t]")
        lines.append("\\caption{Comprehensive Overhead Breakdown}")
        lines.append("\\label{tab:comprehensive_overhead}")
        lines.append("\\centering")
        lines.append("\\small")

        # Section 1: Summary
        lines.append("\\begin{tabular}{lrr}")
        lines.append("\\toprule")
        lines.append("Metric & Baseline & With Cedar \\\\")
        lines.append("\\midrule")
        s = report["summary"]
        lines.append(
            f"Median Latency (ms) & {s['baseline_median_ms']:.2f} & {s['cedar_median_ms']:.2f} \\\\"
        )
        lines.append(
            f"Throughput (QPS) & {s['baseline_qps']:.1f} & {s['cedar_qps']:.1f} \\\\"
        )

        oh_pct = s.get("overhead_pct", 0)
        oh_factor = s.get("overhead_factor", 1.0)
        lines.append(
            f"\\textbf{{Total Overhead}} & & \\textbf{{{oh_pct:.1f}\\% ({oh_factor:.2f}x)}} \\\\"
        )
        lines.append("\\bottomrule")
        lines.append("\\end{tabular}")
        lines.append("\\vspace{2mm}")

        # Section 2: Query Breakdown
        if report["query_breakdown"]:
            lines.append("\\begin{tabular}{lrrrr}")
            lines.append("\\toprule")
            lines.append(
                "Operation & Baseline (ms) & Cedar (ms) & Overhead & Factor \\\\"
            )
            lines.append("\\midrule")
            for r in report["query_breakdown"]:
                lines.append(
                    f"{r['operation']} & {r['baseline_ms']:.2f} & {r['cedar_ms']:.2f} & {r['overhead_pct']:+.1f}\\% & {r.get('overhead_factor', 1.0):.2f}x \\\\"
                )
            lines.append("\\bottomrule")
            lines.append("\\end{tabular}")
            lines.append("\\vspace{2mm}")

        # Section 3: Internal Stages
        if report["stage_breakdown"]:
            is_postgres = any(
                "Postgres" in s["stage"] for s in report["stage_breakdown"]["stages"]
            )
            header_unit = "ms" if is_postgres else "Total s"
            lines.append("\\begin{tabular}{lr}")
            lines.append("\\toprule")
            lines.append(f"Internal Stage & Delta ({header_unit}) \\\\")
            lines.append("\\midrule")
            for s in report["stage_breakdown"]["stages"][:5]:  # Top 5
                if is_postgres:
                    val = s["delta_ms"]
                    lines.append(f"{s['stage']} & {val:+.2f}ms \\\\")
                else:
                    val = s["delta_ms"] / 1000.0
                    lines.append(f"{s['stage']} & {val:+.2f}s \\\\")
            lines.append("\\bottomrule")
            lines.append("\\end{tabular}")

        lines.append("\\end{table}")

        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text("\n".join(lines))

    def generate_report_csv(self, out_path: Path):
        report = self.analyze()
        # Flat CSV for spreadsheet analysis
        out_path.parent.mkdir(parents=True, exist_ok=True)
        with out_path.open("w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(
                ["Type", "Name", "Baseline", "Cedar", "Delta", "Delta%", "Factor"]
            )

            s = report["summary"]
            if s:
                writer.writerow(
                    [
                        "Summary",
                        "Median Latency",
                        s["baseline_median_ms"],
                        s["cedar_median_ms"],
                        s["overhead_ms"],
                        s["overhead_pct"],
                        s.get("overhead_factor"),
                    ]
                )
                writer.writerow(
                    [
                        "Summary",
                        "Throughput",
                        s["baseline_qps"],
                        s["cedar_qps"],
                        s["cedar_qps"] - s["baseline_qps"],
                        s.get("throughput_overhead_pct"),
                        s.get("throughput_overhead_factor"),
                    ]
                )

            for r in report["query_breakdown"]:
                writer.writerow(
                    [
                        "Query",
                        r["operation"],
                        r["baseline_ms"],
                        r["cedar_ms"],
                        r["overhead_ms"],
                        r["overhead_pct"],
                        r.get("overhead_factor"),
                    ]
                )

            for st in report["stage_breakdown"].get("stages", []):
                writer.writerow(
                    [
                        "Stage",
                        st["stage"],
                        st["baseline_total_ms"],
                        st["cedar_total_ms"],
                        st["delta_ms"],
                        "",
                    ]
                )
