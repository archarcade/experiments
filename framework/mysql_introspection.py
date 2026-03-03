"""MySQL introspection helpers for experiment validity.

We use these to capture evidence that Cedar authorization is exercised and to
surface server configuration/caching confounders when overhead looks suspicious.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class MySQLSnapshot:
    captured_at_unix_s: float
    version: str | None
    variables: dict[str, Any]
    status: dict[str, Any]


def _to_int_if_possible(val: Any) -> Any:
    if val is None:
        return None
    if isinstance(val, (int, float)):
        return val
    s = str(val)
    try:
        return int(s)
    except Exception:
        return s


def diff_counters(before: dict[str, Any], after: dict[str, Any]) -> dict[str, Any]:
    """Compute after-before for numeric-ish counters; fallback to raw after."""
    out: dict[str, Any] = {}
    for k, v_after in after.items():
        v_before = before.get(k)
        a = _to_int_if_possible(v_after)
        b = _to_int_if_possible(v_before)
        if isinstance(a, int) and isinstance(b, int):
            out[k] = a - b
        else:
            out[k] = v_after
    return out


def fetch_show_status_like(conn, pattern: str) -> dict[str, Any]:
    cur = conn.cursor()
    try:
        cur.execute(f"SHOW GLOBAL STATUS LIKE '{pattern}'")
        rows = cur.fetchall() or []
        return {str(k): _to_int_if_possible(v) for (k, v) in rows}
    finally:
        cur.close()


def fetch_show_variables_like(conn, pattern: str) -> dict[str, Any]:
    cur = conn.cursor()
    try:
        cur.execute(f"SHOW GLOBAL VARIABLES LIKE '{pattern}'")
        rows = cur.fetchall() or []
        return {str(k): str(v) for (k, v) in rows}
    finally:
        cur.close()


def fetch_mysql_version(conn) -> str | None:
    cur = conn.cursor()
    try:
        cur.execute("SELECT VERSION()")
        row = cur.fetchone()
        return str(row[0]) if row and row[0] is not None else None
    finally:
        cur.close()


def reset_cedar_plugin_stats(conn) -> bool:
    """Best-effort: resets cedar_authorization status counters if supported."""
    cur = conn.cursor()
    try:
        cur.execute("SET GLOBAL cedar_authorization_reset_stats = 1")
        conn.commit()
        return True
    except Exception:
        try:
            conn.rollback()
        except Exception:
            pass
        return False
    finally:
        cur.close()


def capture_validity_snapshot(conn) -> MySQLSnapshot:
    import time

    captured_at = time.time()
    version = None
    try:
        version = fetch_mysql_version(conn)
    except Exception:
        version = None

    # Minimal set of confounder variables (durability, logging, memory sizing)
    variables: dict[str, Any] = {}
    variables.update(fetch_show_variables_like(conn, "innodb_flush_log_at_trx_commit"))
    variables.update(fetch_show_variables_like(conn, "sync_binlog"))
    variables.update(fetch_show_variables_like(conn, "innodb_doublewrite"))
    variables.update(fetch_show_variables_like(conn, "innodb_flush_method"))
    variables.update(fetch_show_variables_like(conn, "innodb_buffer_pool_size"))
    variables.update(fetch_show_variables_like(conn, "innodb_redo_log_capacity"))
    variables.update(fetch_show_variables_like(conn, "innodb_log_file_size"))
    variables.update(fetch_show_variables_like(conn, "innodb_log_files_in_group"))
    variables.update(fetch_show_variables_like(conn, "log_bin"))
    variables.update(fetch_show_variables_like(conn, "binlog_format"))
    variables.update(fetch_show_variables_like(conn, "transaction_isolation"))
    variables.update(fetch_show_variables_like(conn, "autocommit"))

    status: dict[str, Any] = {}
    # Buffer pool / IO signals (cache warmness)
    status.update(fetch_show_status_like(conn, "Innodb_buffer_pool_%"))
    status.update(fetch_show_status_like(conn, "Handler_read%"))
    status.update(fetch_show_status_like(conn, "Threads_running"))
    status.update(fetch_show_status_like(conn, "Questions"))
    status.update(fetch_show_status_like(conn, "Connections"))

    return MySQLSnapshot(
        captured_at_unix_s=captured_at,
        version=version,
        variables=variables,
        status=status,
    )


def capture_cedar_plugin_status(conn) -> dict[str, Any]:
    """Capture cedar_authorization status vars across naming variants."""
    out: dict[str, Any] = {}
    # Some builds use Cedar_* prefix.
    out.update(fetch_show_status_like(conn, "Cedar%"))
    out.update(fetch_show_status_like(conn, "cedar_authorization%"))
    return out


def ensure_cedar_authorization_config(
    conn, plugin_cfg: dict[str, Any]
) -> dict[str, Any]:
    """Best-effort apply + verify cedar_authorization sysvars.

    This is important because MySQL container restarts reset dynamic sysvars,
    and the plugin will log "URL not configured" and return IGNORE.
    """

    cur = conn.cursor()
    try:
        cur.execute("SHOW VARIABLES LIKE 'cedar_authorization%'")
        rows = cur.fetchall() or []
        available = {str(k): str(v) for (k, v) in rows}

        desired: dict[str, Any] = {
            "cedar_authorization_url": plugin_cfg.get("url"),
            "cedar_authorization_timeout": plugin_cfg.get("timeout_ms"),
            "cedar_authorization_namespace": plugin_cfg.get("namespace"),
            "cedar_authorization_collect_stats": plugin_cfg.get("collect_stats"),
            "cedar_authorization_cache_enabled": plugin_cfg.get("cache_enabled"),
            "cedar_authorization_cache_size": plugin_cfg.get("cache_size"),
            "cedar_authorization_cache_ttl": plugin_cfg.get("cache_ttl"),
            "cedar_authorization_log_info": plugin_cfg.get("log_info"),
            "cedar_authorization_enable_column_access": plugin_cfg.get(
                "enable_column_access"
            ),
        }

        applied: dict[str, Any] = {}
        skipped: dict[str, str] = {}

        def _has(var: str) -> bool:
            return var in available

        def _set_sql(var: str, value: Any) -> None:
            if isinstance(value, bool):
                rhs = "ON" if value else "OFF"
            elif value is None:
                return
            elif isinstance(value, (int, float)):
                rhs = str(int(value))
            else:
                s = str(value)
                rhs = "'" + s.replace("'", "''") + "'"
            cur.execute(f"SET GLOBAL {var} = {rhs}")
            applied[var] = value

        for var, value in desired.items():
            if value is None:
                continue
            if not _has(var):
                skipped[var] = "missing_sysvar"
                continue
            try:
                # Only write if it's different to reduce churn.
                if str(available.get(var, "")) == str(value):
                    continue
                _set_sql(var, value)
            except Exception as e:
                skipped[var] = f"set_failed: {e}"

        try:
            conn.commit()
        except Exception:
            pass

        # Verify key vars
        cur.execute("SHOW VARIABLES LIKE 'cedar_authorization_url'")
        url_row = cur.fetchone()
        cur.execute("SHOW VARIABLES LIKE 'cedar_authorization_collect_stats'")
        cs_row = cur.fetchone()

        return {
            "available_count": len(available),
            "applied": applied,
            "skipped": skipped,
            "url": (str(url_row[1]) if url_row and len(url_row) > 1 else None),
            "collect_stats": (str(cs_row[1]) if cs_row and len(cs_row) > 1 else None),
        }
    finally:
        cur.close()


def ensure_ddl_audit_config(conn, plugin_cfg: dict[str, Any]) -> dict[str, Any]:
    """Best-effort apply + verify ddl_audit sysvars.

    This is important because the plugin can be enabled by default and may ship
    with a default Cedar URL (often localhost) that is wrong inside Docker.
    """

    cur = conn.cursor()
    try:
        cur.execute("SHOW VARIABLES LIKE 'ddl_audit%'")
        rows = cur.fetchall() or []
        available = {str(k): str(v) for (k, v) in rows}

        desired: dict[str, Any] = {
            "ddl_audit_enabled": plugin_cfg.get("enabled"),
            "ddl_audit_cedar_url": plugin_cfg.get("url"),
            "ddl_audit_cedar_timeout": plugin_cfg.get("timeout_ms"),
            "ddl_audit_cedar_namespace": plugin_cfg.get("namespace"),
        }

        applied: dict[str, Any] = {}
        skipped: dict[str, str] = {}

        def _has(var: str) -> bool:
            return var in available

        def _set_sql(var: str, value: Any) -> None:
            if isinstance(value, bool):
                rhs = "ON" if value else "OFF"
            elif value is None:
                return
            elif isinstance(value, (int, float)):
                rhs = str(int(value))
            else:
                s = str(value)
                rhs = "'" + s.replace("'", "''") + "'"
            cur.execute(f"SET GLOBAL {var} = {rhs}")
            applied[var] = value

        for var, value in desired.items():
            if value is None:
                continue
            if not _has(var):
                skipped[var] = "missing_sysvar"
                continue
            try:
                if str(available.get(var, "")) == str(value):
                    continue
                _set_sql(var, value)
            except Exception as e:
                skipped[var] = f"set_failed: {e}"

        try:
            conn.commit()
        except Exception:
            pass

        # Verify key vars
        verified: dict[str, Any] = {}
        for key in (
            "ddl_audit_enabled",
            "ddl_audit_cedar_url",
            "ddl_audit_cedar_timeout",
            "ddl_audit_cedar_namespace",
        ):
            if not _has(key):
                continue
            try:
                cur.execute(f"SHOW VARIABLES LIKE '{key}'")
                row = cur.fetchone()
                if row and len(row) > 1:
                    verified[key] = str(row[1])
            except Exception:
                continue

        return {
            "available_count": len(available),
            "applied": applied,
            "skipped": skipped,
            "verified": verified,
        }
    finally:
        cur.close()


def ensure_mysql_cedar_plugin_sysvars(
    conn, plugins_cfg: dict[str, Any]
) -> dict[str, Any]:
    """Apply+verify MySQL Cedar plugin sysvars (ddl_audit + cedar_authorization)."""
    ddl_cfg = dict((plugins_cfg or {}).get("ddl_audit", {}) or {})
    auth_cfg = dict((plugins_cfg or {}).get("cedar_authorization", {}) or {})
    return {
        "ddl_audit": ensure_ddl_audit_config(conn, ddl_cfg) if ddl_cfg else {},
        "cedar_authorization": (
            ensure_cedar_authorization_config(conn, auth_cfg) if auth_cfg else {}
        ),
    }
