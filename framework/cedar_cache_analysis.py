"""
Cedar authorization cache analysis utilities.

This module provides functions to:
1. Query Cedar agent cache statistics
2. Analyze cache effectiveness vs overhead correlation
3. Recommend cache configurations based on workload patterns
"""

from __future__ import annotations

import statistics
from typing import Any


def get_mysql_cache_stats(
    host: str = "127.0.0.1",
    port: int = 3306,
    user: str = "root",
    password: str = "",
) -> dict[str, Any]:
    """
    Get MySQL Cedar authorization cache statistics.

    Queries the MySQL status variables for cache metrics.

    Args:
        host: MySQL host
        port: MySQL port
        user: MySQL user
        password: MySQL password

    Returns:
        Dictionary with cache statistics
    """
    try:
        import mysql.connector

        conn = mysql.connector.connect(
            host=host,
            port=port,
            user=user,
            password=password,
        )
        cursor = conn.cursor()

        # Query Cedar authorization plugin status variables.
        # Historically some builds exposed variables as Cedar_auth_* while others
        # expose cedar_authorization_*.
        rows = []
        cursor.execute("SHOW STATUS LIKE 'Cedar%'")
        rows.extend(cursor.fetchall())
        cursor.execute("SHOW STATUS LIKE 'cedar_authorization%'")
        rows.extend(cursor.fetchall())

        raw: dict[str, Any] = {}
        for name, value in rows:
            key = str(name)
            try:
                raw[key] = int(str(value))
            except Exception:
                raw[key] = str(value)

        cursor.close()
        conn.close()

        def _first_int(keys: list[str]) -> int:
            for k in keys:
                v = raw.get(k)
                if isinstance(v, int):
                    return v
                if isinstance(v, str) and v.isdigit():
                    return int(v)
            return 0

        cache_hits = _first_int(
            [
                "Cedar_auth_cache_hits",
                "cedar_authorization_cache_hits",
            ]
        )
        cache_misses = _first_int(
            [
                "Cedar_auth_cache_misses",
                "cedar_authorization_cache_misses",
            ]
        )
        total_requests = _first_int(
            [
                "Cedar_auth_requests",
                "cedar_authorization_requests",
            ]
        )
        total_time_us = _first_int(
            [
                "Cedar_auth_total_time_us",
                "cedar_authorization_total_time_us",
            ]
        )
        remote_time_us = _first_int(
            [
                "Cedar_auth_remote_time_us",
                "cedar_authorization_remote_time_us",
            ]
        )
        cache_evictions = _first_int(
            [
                "Cedar_auth_cache_evictions",
                "cedar_authorization_cache_evictions",
            ]
        )
        grants = _first_int(
            [
                "Cedar_auth_grants",
                "cedar_authorization_grants",
            ]
        )
        denies = _first_int(
            [
                "Cedar_auth_denies",
                "cedar_authorization_denies",
            ]
        )
        errors = _first_int(
            [
                "Cedar_auth_errors",
                "cedar_authorization_errors",
            ]
        )

        total_cache = cache_hits + cache_misses
        cache_hit_rate = cache_hits / total_cache if total_cache > 0 else 0.0

        out: dict[str, Any] = dict(raw)
        out["cache_hits"] = cache_hits
        out["cache_misses"] = cache_misses
        out["cache_evictions"] = cache_evictions
        out["cache_hit_rate"] = cache_hit_rate
        out["cache_miss_rate"] = 1.0 - cache_hit_rate if total_cache > 0 else 0.0
        out["total_requests"] = total_requests
        out["grants"] = grants
        out["denies"] = denies
        out["errors"] = errors
        out["total_time_us"] = total_time_us
        out["remote_time_us"] = remote_time_us
        out["avg_total_time_us"] = (
            total_time_us / total_requests if total_requests > 0 else 0.0
        )
        out["avg_remote_time_us"] = (
            remote_time_us / total_requests if total_requests > 0 else 0.0
        )

        return out

    except Exception as e:
        return {"error": str(e), "cache_hit_rate": 0.0}


def get_postgres_cache_stats(
    host: str = "127.0.0.1",
    port: int = 5432,
    database: str = "postgres",
    user: str = "postgres",
    password: str = "",
) -> dict[str, Any]:
    """
    Get PostgreSQL Cedar authorization cache statistics.

    Args:
        host: PostgreSQL host
        port: PostgreSQL port
        database: Database name
        user: PostgreSQL user
        password: PostgreSQL password

    Returns:
        Dictionary with cache statistics
    """
    try:
        import psycopg2

        conn = psycopg2.connect(
            host=host,
            port=port,
            database=database,
            user=user,
            password=password,
        )
        cursor = conn.cursor()

        # Extension exposes stats via pg_authorization_* functions.
        cursor.execute("SELECT * FROM pg_authorization_stats();")
        auth_row = cursor.fetchone()

        cursor.execute("SELECT * FROM pg_authorization_cache_stats();")
        cache_row = cursor.fetchone()
        cursor.close()
        conn.close()

        # pg_authorization_stats(): auth_requests is first OUT column.
        total_requests = int(auth_row[0]) if auth_row else 0

        # pg_authorization_cache_stats(): hits, misses, evictions, entries.
        hits = int(cache_row[0]) if cache_row else 0
        misses = int(cache_row[1]) if cache_row else 0
        total = hits + misses

        return {
            "cache_hits": hits,
            "cache_misses": misses,
            "total_requests": total_requests,
            "cache_hit_rate": hits / total if total > 0 else 0.0,
        }

    except Exception as e:
        return {"error": str(e), "cache_hit_rate": 0.0}


def analyze_cache_effectiveness(
    cache_hit_rates: list[float],
    overheads: list[float],
) -> dict[str, Any]:
    """
    Analyze correlation between cache hit rate and overhead.

    Args:
        cache_hit_rates: List of cache hit rates (0.0 to 1.0)
        overheads: List of corresponding overhead percentages

    Returns:
        Dictionary with correlation analysis
    """
    if len(cache_hit_rates) < 2 or len(overheads) < 2:
        return {"error": "Insufficient data points", "correlation": 0.0}

    if len(cache_hit_rates) != len(overheads):
        return {"error": "Mismatched data lengths", "correlation": 0.0}

    n = len(cache_hit_rates)

    # Calculate Pearson correlation coefficient
    mean_hit = statistics.mean(cache_hit_rates)
    mean_oh = statistics.mean(overheads)

    numerator = sum(
        (cache_hit_rates[i] - mean_hit) * (overheads[i] - mean_oh) for i in range(n)
    )

    denom_hit = sum((x - mean_hit) ** 2 for x in cache_hit_rates) ** 0.5
    denom_oh = sum((x - mean_oh) ** 2 for x in overheads) ** 0.5

    if denom_hit == 0 or denom_oh == 0:
        correlation = 0.0
    else:
        correlation = numerator / (denom_hit * denom_oh * n)

    # Expected: negative correlation (higher hit rate = lower overhead)
    interpretation = (
        "strong negative"
        if correlation < -0.7
        else "moderate negative"
        if correlation < -0.4
        else "weak negative"
        if correlation < -0.1
        else "negligible"
        if correlation < 0.1
        else "unexpected positive (higher hit rate correlates with higher overhead)"
    )

    return {
        "correlation": correlation,
        "interpretation": interpretation,
        "n_observations": n,
        "mean_hit_rate": mean_hit,
        "mean_overhead": mean_oh,
    }


def recommend_cache_config(
    avg_request_rate_rps: float,
    unique_auth_combinations: int,
    acceptable_stale_seconds: int = 60,
) -> dict[str, Any]:
    """
    Recommend cache configuration based on workload characteristics.

    Args:
        avg_request_rate_rps: Average authorization requests per second
        unique_auth_combinations: Estimated unique (user, action, resource) tuples
        acceptable_stale_seconds: How long stale cache entries are acceptable

    Returns:
        Dictionary with recommended configuration
    """
    # Size recommendation: cover all unique combinations with 20% buffer
    recommended_size = int(unique_auth_combinations * 1.2)

    # TTL recommendation: balance freshness vs hit rate
    # Higher request rates benefit more from longer TTLs
    if avg_request_rate_rps > 1000:
        recommended_ttl = min(acceptable_stale_seconds, 300)
    elif avg_request_rate_rps > 100:
        recommended_ttl = min(acceptable_stale_seconds, 120)
    else:
        recommended_ttl = min(acceptable_stale_seconds, 60)

    # Estimate hit rate based on assumptions
    # Higher unique combinations = lower hit rate
    estimated_hit_rate = max(
        0.5, 1.0 - (unique_auth_combinations / (recommended_size * 10))
    )

    # Estimate overhead reduction
    # Without cache: ~7ms overhead per request
    # With cache (hit): ~0.1ms overhead
    # Net: hit_rate * 6.9ms reduction
    estimated_overhead_reduction = estimated_hit_rate * 6.9  # ms

    return {
        "recommended_size": recommended_size,
        "recommended_ttl_seconds": recommended_ttl,
        "estimated_hit_rate": estimated_hit_rate,
        "estimated_overhead_reduction_ms": estimated_overhead_reduction,
        "config_yaml": f"""cedar_authorization:
  cache_enabled: true
  cache_size: {recommended_size}
  cache_ttl: {recommended_ttl}""",
    }


def format_cache_report(stats: dict[str, Any]) -> str:
    """Format cache statistics as a human-readable report."""
    lines = [
        "=" * 50,
        "Cedar Authorization Cache Report",
        "=" * 50,
    ]

    if "error" in stats:
        lines.append(f"Error: {stats['error']}")
    else:
        hit_rate = stats.get("cache_hit_rate", 0) * 100
        lines.extend(
            [
                f"Cache Hits:    {stats.get('cache_hits', 'N/A')}",
                f"Cache Misses:  {stats.get('cache_misses', 'N/A')}",
                f"Hit Rate:      {hit_rate:.1f}%",
                "",
                "Interpretation:",
            ]
        )

        if hit_rate >= 90:
            lines.append("  ✓ Excellent cache efficiency - overhead is minimized")
        elif hit_rate >= 70:
            lines.append("  ✓ Good cache efficiency")
        elif hit_rate >= 50:
            lines.append(
                "  ⚠ Moderate cache efficiency - consider increasing cache size"
            )
        else:
            lines.append(
                "  ⚠ Low cache efficiency - workload may have high cardinality"
            )

    lines.append("=" * 50)
    return "\n".join(lines)
