#!/usr/bin/env python3
"""
Differential profiling utilities: baseline vs modified DB.

Goal:
- Produce profiling data that helps explain *where* latency differences come from
  between baseline and Cedar-modified database versions.

This module intentionally uses **built-in instrumentation** (no kernel perf/eBPF),
because the project often runs in Docker-on-Mac/VM environments where `perf`
is hard to enable reliably.

What we collect:
- MySQL: Performance Schema stage + wait summaries (delta during workload execution)
- PostgreSQL: EXPLAIN (ANALYZE, FORMAT JSON) planning vs execution time (per-query sample)
"""

from __future__ import annotations

import json
import time
from collections.abc import Callable
from pathlib import Path
from typing import Any

import mysql.connector
import psycopg2

from .benchmark_runner import BenchmarkRunner
from .config import Config
from .workload_generator import Workload

PS_TIMER_PS_PER_MS = 1_000_000_000  # performance_schema timers are in picoseconds


def _now_iso() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def _mysql_connect(cfg: Config, system_name: str):
    db = cfg.databases[system_name]
    return mysql.connector.connect(
        host=db.host,
        port=db.port,
        user=db.user,
        password=db.password,
        database=db.database,
        autocommit=True,
    )


def _postgres_connect(cfg: Config, system_name: str):
    db = cfg.databases[system_name]
    conn = psycopg2.connect(
        host=db.host,
        port=db.port,
        user=db.user,
        password=db.password,
        dbname=db.database,
    )
    conn.autocommit = False
    return conn


def _mysql_exec(conn, sql: str) -> None:
    cur = conn.cursor()
    try:
        cur.execute(sql)
    finally:
        cur.close()


def _mysql_query_rows(conn, sql: str) -> list[tuple[Any, ...]]:
    cur = conn.cursor()
    try:
        cur.execute(sql)
        return list(cur.fetchall())
    finally:
        cur.close()


def mysql_try_enable_perf_schema(conn) -> dict[str, Any]:
    """
    Best-effort enable for stage/wait instrumentation.
    Safe to call even if some consumers/instruments don't exist.
    """
    actions: list[dict[str, Any]] = []

    statements = [
        # Enable stage and wait instruments
        "UPDATE performance_schema.setup_instruments SET ENABLED='YES', TIMED='YES' WHERE NAME LIKE 'stage/%'",
        "UPDATE performance_schema.setup_instruments SET ENABLED='YES', TIMED='YES' WHERE NAME LIKE 'wait/%'",
        # Enable statement instruments too (useful for context)
        "UPDATE performance_schema.setup_instruments SET ENABLED='YES', TIMED='YES' WHERE NAME LIKE 'statement/%'",
        # Enable common consumers
        "UPDATE performance_schema.setup_consumers SET ENABLED='YES' WHERE NAME LIKE 'events_stages_%'",
        "UPDATE performance_schema.setup_consumers SET ENABLED='YES' WHERE NAME LIKE 'events_waits_%'",
        "UPDATE performance_schema.setup_consumers SET ENABLED='YES' WHERE NAME LIKE 'events_statements_%'",
        "UPDATE performance_schema.setup_consumers SET ENABLED='YES' WHERE NAME IN ('global_instrumentation','thread_instrumentation','statements_digest')",
    ]

    for sql in statements:
        try:
            _mysql_exec(conn, sql)
            actions.append({"sql": sql, "ok": True})
        except Exception as e:
            actions.append({"sql": sql, "ok": False, "error": str(e)})

    return {"actions": actions}


def mysql_try_reset_summaries(conn) -> dict[str, Any]:
    """
    Best-effort reset of summary tables.

    Many performance_schema tables support TRUNCATE; if not, we proceed without reset.
    """
    actions: list[dict[str, Any]] = []
    for tbl in (
        "performance_schema.events_stages_summary_global_by_event_name",
        "performance_schema.events_waits_summary_global_by_event_name",
        "performance_schema.events_statements_summary_global_by_event_name",
        "performance_schema.events_statements_summary_by_digest",
    ):
        try:
            _mysql_exec(conn, f"TRUNCATE TABLE {tbl}")
            actions.append({"table": tbl, "ok": True})
        except Exception as e:
            actions.append({"table": tbl, "ok": False, "error": str(e)})
    return {"actions": actions}


def _mysql_collect_summary(
    conn,
    table: str,
    top_n: int = 50,
    where: str | None = None,
) -> list[dict[str, Any]]:
    """
    Collect a summary table of (EVENT_NAME, SUM_TIMER_WAIT) and convert to ms.
    """
    where_sql = f"WHERE {where}" if where else ""
    sql = f"""
        SELECT EVENT_NAME, SUM_TIMER_WAIT
        FROM {table}
        {where_sql}
        ORDER BY SUM_TIMER_WAIT DESC
        LIMIT {int(top_n)}
    """
    rows = _mysql_query_rows(conn, sql)
    out = []
    for event_name, sum_timer_wait in rows:
        ps = float(sum_timer_wait or 0)
        out.append(
            {
                "event_name": str(event_name),
                "sum_ms": ps / PS_TIMER_PS_PER_MS,
            }
        )
    return out


def mysql_collect_profile_generic(
    cfg: Config,
    system_name: str,
    action_fn: Callable[[], Any],
    out_path: Path,
    top_n: int = 50,
    reset: bool = True,
    enable: bool = True,
) -> dict[str, Any]:
    """
    Run a generic action on a MySQL system and capture perf-schema stage/wait deltas.
    """
    conn = _mysql_connect(cfg, system_name)
    try:
        enable_info = mysql_try_enable_perf_schema(conn) if enable else {"actions": []}
        reset_info = mysql_try_reset_summaries(conn) if reset else {"actions": []}

        before_stages = _mysql_collect_summary(
            conn,
            "performance_schema.events_stages_summary_global_by_event_name",
            top_n=top_n,
            where="SUM_TIMER_WAIT > 0",
        )
        before_waits = _mysql_collect_summary(
            conn,
            "performance_schema.events_waits_summary_global_by_event_name",
            top_n=top_n,
            where="SUM_TIMER_WAIT > 0",
        )

        t0 = time.time()
        action_result = action_fn()
        t1 = time.time()

        after_stages = _mysql_collect_summary(
            conn,
            "performance_schema.events_stages_summary_global_by_event_name",
            top_n=top_n,
            where="SUM_TIMER_WAIT > 0",
        )
        after_waits = _mysql_collect_summary(
            conn,
            "performance_schema.events_waits_summary_global_by_event_name",
            top_n=top_n,
            where="SUM_TIMER_WAIT > 0",
        )
    finally:
        try:
            conn.close()
        except Exception:
            pass

    def _to_map(items: list[dict[str, Any]]) -> dict[str, float]:
        return {i["event_name"]: float(i["sum_ms"]) for i in items}

    def _diff(a: list[dict[str, Any]], b: list[dict[str, Any]]) -> list[dict[str, Any]]:
        am = _to_map(a)
        bm = _to_map(b)
        keys = set(am.keys()) | set(bm.keys())
        rows = []
        for k in keys:
            before = am.get(k, 0.0)
            after = bm.get(k, 0.0)
            delta = after - before
            if abs(delta) < 1e-9:
                continue
            rows.append(
                {
                    "event_name": k,
                    "before_ms": before,
                    "after_ms": after,
                    "delta_ms": delta,
                }
            )
        rows.sort(key=lambda r: abs(r["delta_ms"]), reverse=True)
        return rows

    payload = {
        "profile_type": "mysql_perf_schema_stage_wait",
        "timestamp": _now_iso(),
        "system": system_name,
        "top_n": top_n,
        "wall_time_s": round(t1 - t0, 3),
        "enable": enable_info,
        "reset": reset_info,
        "stages_delta": _diff(before_stages, after_stages),
        "waits_delta": _diff(before_waits, after_waits),
    }

    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(payload, indent=2))
    return {"path": out_path, "result": action_result}


def mysql_collect_stage_wait_profile(
    cfg: Config,
    system_name: str,
    workload_path: Path,
    out_path: Path,
    top_n: int = 50,
    reset: bool = True,
    enable: bool = True,
) -> Path:
    """
    Run the workload on a single MySQL system and capture perf-schema stage/wait deltas.
    """
    workload = Workload.load(workload_path)
    runner = BenchmarkRunner(workload, cfg)

    def run_workload():
        return runner.run_system(system_name)

    res = mysql_collect_profile_generic(
        cfg, system_name, run_workload, out_path, top_n, reset, enable
    )

    # Add workload specific metadata to the saved JSON
    payload = json.loads(out_path.read_text())
    payload["workload_path"] = str(workload_path)
    payload["benchmark_run"] = res["result"]["metadata"]
    out_path.write_text(json.dumps(payload, indent=2))

    return out_path


def postgres_collect_explain_profile(
    cfg: Config,
    system_name: str,
    out_path: Path,
    workload_path: Path | None = None,
    queries: list[str] | None = None,
    sample_n: int = 200,
) -> Path:
    """
    Run EXPLAIN (ANALYZE, FORMAT JSON) for a sample of queries and extract:
    - Planning Time (ms)
    - Execution Time (ms)

    NOTE: EXPLAIN ANALYZE executes the statement; we wrap each in a transaction and ROLLBACK
    to avoid mutating the database for write queries.
    """
    if workload_path:
        workload = Workload.load(workload_path)
        queries_to_run = [
            {
                "id": q.id,
                "sql": q.sql,
                "category": q.category,
                "action": q.action,
                "table": q.table,
            }
            for q in workload.queries[: max(0, int(sample_n))]
        ]
    elif queries:
        queries_to_run = [
            {
                "id": f"q{i}",
                "sql": sql,
                "category": "manual",
                "action": "manual",
                "table": "manual",
            }
            for i, sql in enumerate(queries[: max(0, int(sample_n))])
        ]
    else:
        raise ValueError("Either workload_path or queries must be provided")

    conn = _postgres_connect(cfg, system_name)
    per_query: list[dict[str, Any]] = []
    try:
        with conn.cursor() as cur:
            for q in queries_to_run:
                try:
                    cur.execute("BEGIN;")
                    cur.execute(
                        f"EXPLAIN (ANALYZE, BUFFERS, VERBOSE, FORMAT JSON) {q['sql']}"
                    )
                    raw = cur.fetchone()
                    cur.execute("ROLLBACK;")

                    plan_json = raw[0]
                    # psycopg2 typically returns python objects for json output; if str, decode it.
                    if isinstance(plan_json, str):
                        plan_json = json.loads(plan_json)

                    # Format JSON returns a list with a single object
                    root = (
                        plan_json[0]
                        if isinstance(plan_json, list) and plan_json
                        else plan_json
                    )
                    planning_ms = float(root.get("Planning Time", 0.0))
                    execution_ms = float(root.get("Execution Time", 0.0))

                    per_query.append(
                        {
                            "query_id": q["id"],
                            "action": q["action"],
                            "category": q["category"],
                            "table": q["table"],
                            "planning_ms": planning_ms,
                            "execution_ms": execution_ms,
                        }
                    )
                except Exception as e:
                    try:
                        conn.rollback()
                    except Exception:
                        pass
                    per_query.append(
                        {
                            "query_id": q["id"],
                            "action": q["action"],
                            "category": q["category"],
                            "table": q["table"],
                            "error": str(e),
                        }
                    )
    finally:
        try:
            conn.close()
        except Exception:
            pass

    out_path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "profile_type": "postgres_explain_json",
        "timestamp": _now_iso(),
        "system": system_name,
        "workload_path": str(workload_path) if workload_path else None,
        "sample_n": sample_n,
        "results": per_query,
    }
    out_path.write_text(json.dumps(payload, indent=2))
    return out_path


def diff_profiles_to_csv(
    baseline_profile_path: Path,
    cedar_profile_path: Path,
    out_csv_path: Path,
) -> Path:
    """
    Create a simple diff CSV (baseline vs cedar) for profiles produced by this module.
    """
    base = json.loads(baseline_profile_path.read_text())
    ced = json.loads(cedar_profile_path.read_text())

    base_type = base.get("profile_type")
    ced_type = ced.get("profile_type")
    if base_type != ced_type:
        raise ValueError(f"Profile types differ: {base_type} vs {ced_type}")

    out_csv_path.parent.mkdir(parents=True, exist_ok=True)

    if base_type == "mysql_perf_schema_stage_wait":
        # Diff by event_name for stages + waits separately
        def _index(rows: list[dict[str, Any]]) -> dict[str, float]:
            return {r["event_name"]: float(r["delta_ms"]) for r in rows}

        def _write_section(
            f,
            section: str,
            base_rows: list[dict[str, Any]],
            ced_rows: list[dict[str, Any]],
        ):
            b = _index(base_rows)
            c = _index(ced_rows)
            keys = set(b.keys()) | set(c.keys())
            merged = []
            for k in keys:
                b_ms = b.get(k, 0.0)
                c_ms = c.get(k, 0.0)
                merged.append((k, b_ms, c_ms, c_ms - b_ms))
            merged.sort(key=lambda t: abs(t[3]), reverse=True)
            f.write(
                "section,event_name,baseline_delta_ms,cedar_delta_ms,cedar_minus_baseline_ms\n"
            )
            for k, b_ms, c_ms, d_ms in merged:
                f.write(f"{section},{k},{b_ms:.6f},{c_ms:.6f},{d_ms:.6f}\n")

        with out_csv_path.open("w") as f:
            _write_section(
                f, "stages", base.get("stages_delta", []), ced.get("stages_delta", [])
            )
            # Append waits as another block (repeat header for easy copy-paste in pandas)
            f.write("\n")
            _write_section(
                f, "waits", base.get("waits_delta", []), ced.get("waits_delta", [])
            )

        return out_csv_path

    if base_type == "postgres_explain_json":
        # Diff medians by category
        def _median(xs: list[float]) -> float:
            xs = sorted(xs)
            if not xs:
                return 0.0
            mid = len(xs) // 2
            return xs[mid] if len(xs) % 2 else (xs[mid - 1] + xs[mid]) / 2

        def _group(profile: dict[str, Any]) -> dict[str, dict[str, float]]:
            groups: dict[str, dict[str, list[float]]] = {}
            for r in profile.get("results", []):
                if r.get("error"):
                    continue
                cat = r.get("category") or r.get("action") or "UNKNOWN"
                groups.setdefault(cat, {"planning_ms": [], "execution_ms": []})
                groups[cat]["planning_ms"].append(float(r.get("planning_ms", 0.0)))
                groups[cat]["execution_ms"].append(float(r.get("execution_ms", 0.0)))
            out: dict[str, dict[str, float]] = {}
            for cat, vals in groups.items():
                out[cat] = {
                    "planning_median_ms": _median(vals["planning_ms"]),
                    "execution_median_ms": _median(vals["execution_ms"]),
                }
            return out

        b = _group(base)
        c = _group(ced)
        cats = set(b.keys()) | set(c.keys())
        rows = []
        for cat in cats:
            bp = b.get(cat, {}).get("planning_median_ms", 0.0)
            be = b.get(cat, {}).get("execution_median_ms", 0.0)
            cp = c.get(cat, {}).get("planning_median_ms", 0.0)
            ce = c.get(cat, {}).get("execution_median_ms", 0.0)
            rows.append((cat, bp, cp, cp - bp, be, ce, ce - be))
        rows.sort(key=lambda t: abs(t[3]) + abs(t[6]), reverse=True)

        with out_csv_path.open("w") as f:
            f.write(
                "category,baseline_planning_median_ms,cedar_planning_median_ms,delta_planning_ms,"
                "baseline_execution_median_ms,cedar_execution_median_ms,delta_execution_ms\n"
            )
            for cat, bp, cp, dp, be, ce, de in rows:
                f.write(
                    f"{cat},{bp:.6f},{cp:.6f},{dp:.6f},{be:.6f},{ce:.6f},{de:.6f}\n"
                )
        return out_csv_path

    raise ValueError(f"Unsupported profile_type: {base_type}")
