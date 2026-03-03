#!/usr/bin/env python3
"""
Benchmark runner:
- Loads pre-generated workload
- Executes against baseline and Cedar using connection pools
- Measures latency per query
"""

from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any

from tqdm import tqdm

from .config import Config
from .connection_pool import ConnectionPool
from .workload_generator import Query, Workload


class BenchmarkRunner:
    def __init__(self, workload: Workload, config: Config):
        self.workload = workload
        self.config = config
        self._baseline_pool = ConnectionPool(self.config.databases["baseline"])
        self._cedar_pool = ConnectionPool(self.config.databases["cedar"])
        self._user_pools: dict[tuple[str, str], ConnectionPool] = {}
        self._user_passwords: dict[str, str] = {}
        if self.config.benchmark.use_query_user:
            self._user_passwords = self._load_user_passwords_from_auth_spec()

    def _load_user_passwords_from_auth_spec(self) -> dict[str, str]:
        """
        Load username->password mapping from the auth spec referenced by workload metadata.
        The workload contains query.user values that should match auth spec user entries.
        """
        auth_spec_path = self.workload.metadata.get("auth_spec_path")
        if not auth_spec_path:
            return {}
        try:
            spec = json.loads(Path(auth_spec_path).read_text())
        except Exception:
            return {}

        users = spec.get("users", []) or []
        mapping: dict[str, str] = {}
        for u in users:
            username = u.get("username")
            if not username:
                continue
            mapping[username] = u.get("password") or ""
        return mapping

    def _get_pool_for_query(self, system_name: str, query_user: str) -> ConnectionPool:
        """
        Returns a ConnectionPool for (system_name, query_user).
        Falls back to the system default pool if per-query user execution is disabled
        or the auth spec did not provide credentials for the query user.
        """
        if not self.config.benchmark.use_query_user:
            return (
                self._baseline_pool if system_name == "baseline" else self._cedar_pool
            )

        password = self._user_passwords.get(query_user)
        if password is None:
            return (
                self._baseline_pool if system_name == "baseline" else self._cedar_pool
            )

        key = (system_name, query_user)
        if key in self._user_pools:
            return self._user_pools[key]

        base_cfg = self.config.databases[system_name]
        # Create a derived config for this user
        from .config import (
            DatabaseConfig,  # local import to avoid cycles in type checkers
        )

        user_cfg = DatabaseConfig(
            name=f"{system_name}_{query_user}",
            host=base_cfg.host,
            port=base_cfg.port,
            user=query_user,
            password=password,
            database=base_cfg.database,
            pool_size=base_cfg.pool_size,
        )
        pool = ConnectionPool(user_cfg)
        self._user_pools[key] = pool
        return pool

    def _measure_query(
        self, pool: ConnectionPool, query: Query
    ) -> tuple[float, bool, str | None]:
        start = time.perf_counter()
        success = True
        error_msg = None
        conn = None

        try:
            with pool.get_connection() as conn:
                cur = conn.cursor(buffered=True)
                try:
                    # Handle multi-statement queries
                    # (e.g., DELETE followed by INSERT)
                    # Split by semicolon and execute each separately
                    statements = [s.strip() for s in query.sql.split(";") if s.strip()]

                    for stmt in statements:
                        cur.execute(stmt)
                        # Consume all results to avoid
                        # "Unread result found" error
                        # MySQL connector requires all results
                        # to be consumed before next execute
                        if cur.with_rows:
                            cur.fetchall()

                    conn.commit()
                finally:
                    cur.close()
        except Exception as e:
            if conn:
                try:
                    conn.rollback()
                except Exception:
                    pass
            success = False
            error_msg = str(e)

        return (time.perf_counter() - start) * 1000.0, success, error_msg

    def _select_queries(self, total: int) -> list[Query]:
        """
        Select queries for execution, cycling through the workload
        if more iterations are requested than available queries.
        """
        if not self.workload.queries:
            return []

        if total <= len(self.workload.queries):
            return self.workload.queries[:total]

        # Cycle through queries to reach the requested number of iterations
        queries = []
        workload_len = len(self.workload.queries)
        for i in range(total):
            queries.append(self.workload.queries[i % workload_len])
        return queries

    def _run_on_pool(
        self,
        system_name: str,
        queries: list[Query],
        show_progress: bool = True,
    ) -> list[dict[str, Any]]:
        results: list[dict[str, Any]] = []

        iterator = queries
        if show_progress:
            iterator = tqdm(
                queries, desc=f"Running on {system_name}", unit="query", leave=False
            )

        for q in iterator:
            pool = self._get_pool_for_query(system_name, q.user)
            latency, success, error = self._measure_query(pool, q)

            result_entry = {
                "system": system_name,
                "query_id": q.id,
                "user": q.user,
                "action": q.action,
                "category": getattr(q, "category", q.action),
                "table": q.table,
                "latency_ms": latency,
                "success": success,
                "sql": q.sql,
            }
            if error:
                result_entry["error"] = error
            results.append(result_entry)
        return results

    def run_system(self, system_name: str) -> dict[str, Any]:
        """
        Run the configured benchmark workload against a single system ("baseline" or "cedar").

        This is useful for profiling workflows that need to isolate one DB at a time.
        """
        if system_name not in ("baseline", "cedar"):
            raise ValueError(f"Unknown system_name: {system_name}")

        # Re-initialize pools for this run to ensure fresh connections
        # and avoid "mysql server has gone away" issues on long runs
        if system_name == "baseline":
            self._baseline_pool = ConnectionPool(self.config.databases["baseline"])
        else:
            self._cedar_pool = ConnectionPool(self.config.databases["cedar"])

        # Clear user pools for this system
        keys_to_remove = [k for k in self._user_pools.keys() if k[0] == system_name]
        for k in keys_to_remove:
            del self._user_pools[k]

        warmup_iters = self.config.benchmark.warmup_iterations
        warmup_seconds = self.config.benchmark.warmup_seconds

        if warmup_seconds > 0 and self.workload.queries:
            import time

            start_time = time.time()
            # Run in batches of 50 to check time frequently without too much overhead
            batch_size = 50
            while time.time() - start_time < warmup_seconds:
                warm_q = self._select_queries(batch_size)
                self._run_on_pool(system_name, warm_q, show_progress=False)
        elif warmup_iters > 0 and self.workload.queries:
            warm_q = self._select_queries(warmup_iters)
            self._run_on_pool(system_name, warm_q, show_progress=False)

        total = self.config.benchmark.iterations
        queries = self._select_queries(total)
        results = self._run_on_pool(system_name, queries, show_progress=True)

        return {
            "metadata": {
                "system": system_name,
                "iterations": len(queries),
                "warmup_iterations": warmup_iters,
                "warmup_seconds": warmup_seconds,
            },
            "results": results,
        }

    def run(self) -> dict[str, Any]:
        baseline_run = self.run_system("baseline")
        cedar_run = self.run_system("cedar")

        return {
            "metadata": {
                "iterations": baseline_run["metadata"]["iterations"],
                "warmup_iterations": baseline_run["metadata"]["warmup_iterations"],
                "warmup_seconds": baseline_run["metadata"].get("warmup_seconds", 0),
            },
            "baseline": baseline_run["results"],
            "cedar": cedar_run["results"],
        }
