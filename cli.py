#!/usr/bin/env python3
"""
Pure-Python CLI for the Authorization-Aware Benchmark Framework.

Subcommands:
- generate-workload: Pre-compute workload from auth spec and store to disk
- run-benchmark: Run pre-computed workload against baseline and Cedar
- setup-baseline: Apply CREATE USER + GRANTs to baseline MySQL from auth spec
- setup-cedar: Configure Cedar agent (attributes + policies) from auth spec
- analyze-results: Compute statistics and generate summary
- full-experiment: End-to-end (setup -> workload -> run -> analyze)
"""

import json
import os
import shutil
import sys
from pathlib import Path
from typing import Any

import click
from tqdm import tqdm

# Local imports
try:
    from framework.analysis import analyze_to_outputs  # type: ignore
    from framework.analysis_scalability import (
        compute_concurrency_summary,
        compute_policy_scaling_summary,
        write_concurrency_latency_csv,
        write_concurrency_throughput_csv,
        write_concurrency_throughput_table_tex,
        write_policy_scaling_csv,
        write_policy_scaling_table_tex,
    )
    from framework.analyzer import ResultsAnalyzer
    from framework.benchmark_runner import BenchmarkRunner
    from framework.config import Config, load_config_file
    from framework.ddl_operations_test import DDLOperationsTester, run_ddl_audit_test
    from framework.failure_semantics_test import run_semantic_correctness_tests
    from framework.pgbench_runner import compare_pgbench_systems, run_pgbench_experiment
    from framework.policy_scaler import build_policy_set, get_policies, put_policies
    from framework.sql_generator import SQLGenerator  # type: ignore
    from framework.sql_latency_runner import (
        analyze_latency_results,
        run_sql_latency_experiment,
    )
    from framework.sysbench_parser import parse_sysbench_output, run_sysbench_command
    from framework.toxiproxy_client import ToxiproxyClient
    from framework.tpcc_mysql_client import TPCCMySQLClient, run_tpcc_mysql_benchmark

    # Import helpers from Cedar translator
    from framework.translate_to_cedar import (  # type: ignore
        assign_database_attributes,
        assign_resource_attributes,
        assign_user_attributes,
        check_cedar_agent,
        create_cedar_policies,
        setup_cedar_schema,
    )
    from framework.translate_to_grants import translate_to_grants  # type: ignore
    from framework.vegeta_runner import check_vegeta_installed, run_vegeta_stress_test
    from framework.visualizations import (
        generate_agent_delay_comprehensive_plot,
        generate_agent_delay_vs_query_latency_plot,
        generate_agent_rps_vs_latency_plot,
        generate_agent_stress_comprehensive_plot,
        generate_all_visualizations,
        latex_table_agent_delay_impact,
        latex_table_agent_stress_test,
    )
    from framework.workload_generator import Workload, WorkloadGenerator

    import requests
    from framework.cedar_stats import (
        get_cedar_agent_stats,
        reset_cedar_agent_stats,
        verify_auth_invocations,
    )
    from framework.translate_to_cedar import (
        entity_exists,
        create_entity,
    )

    try:
        import pandas as pd

        HAS_PANDAS = True
    except ImportError:
        HAS_PANDAS = False
        pd = None
    import time
except Exception:
    # Allow running via `python experiments/cli.py` directly
    try:
        import pandas as pd

        HAS_PANDAS = True
    except ImportError:
        HAS_PANDAS = False
        pd = None
    # Allow running via `python experiments/cli.py` directly
    sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
    import time

    from framework.analysis import analyze_to_outputs  # type: ignore
    from framework.analysis_scalability import (
        compute_concurrency_summary,
        compute_policy_scaling_summary,
        write_concurrency_latency_csv,
        write_concurrency_throughput_csv,
        write_concurrency_throughput_table_tex,
        write_policy_scaling_csv,
        write_policy_scaling_table_tex,
    )
    from framework.analyzer import ResultsAnalyzer
    from framework.benchmark_runner import BenchmarkRunner
    from framework.config import Config, load_config_file
    from framework.ddl_operations_test import DDLOperationsTester, run_ddl_audit_test
    from framework.failure_semantics_test import run_semantic_correctness_tests
    from framework.pgbench_runner import compare_pgbench_systems, run_pgbench_experiment
    from framework.policy_scaler import build_policy_set, get_policies, put_policies
    from framework.sql_generator import SQLGenerator  # type: ignore
    from framework.sql_latency_runner import (
        analyze_latency_results,
        run_sql_latency_experiment,
    )
    from framework.sysbench_parser import parse_sysbench_output, run_sysbench_command
    from framework.toxiproxy_client import ToxiproxyClient
    from framework.tpcc_mysql_client import TPCCMySQLClient, run_tpcc_mysql_benchmark
    from framework.translate_to_cedar import (  # type: ignore
        assign_database_attributes,
        assign_resource_attributes,
        assign_user_attributes,
        check_cedar_agent,
        create_cedar_policies,
        setup_cedar_schema,
    )
    from framework.translate_to_grants import translate_to_grants  # type: ignore
    from framework.vegeta_runner import check_vegeta_installed, run_vegeta_stress_test
    from framework.visualizations import (
        generate_agent_delay_comprehensive_plot,
        generate_agent_delay_vs_query_latency_plot,
        generate_agent_rps_vs_latency_plot,
        generate_agent_stress_comprehensive_plot,
        generate_all_visualizations,
        latex_table_agent_delay_impact,
        latex_table_agent_stress_test,
    )
    from framework.workload_generator import Workload, WorkloadGenerator

    # Imported for fixes
    import requests
    from framework.cedar_stats import (
        get_cedar_agent_stats,
        reset_cedar_agent_stats,
        verify_auth_invocations,
    )
    from framework.translate_to_cedar import (
        entity_exists,
        create_entity,
    )


def _get_experiment_paths(cfg: Config, experiment_name: str) -> tuple[Path, Path, Path]:
    """
    Get workload, results, and analysis paths for a specific experiment.
    Appends the experiment tag and name to the configured base paths.
    Structure: {base_dir}/{experiment_tag}/{experiment_name}
    """
    tag = cfg.experiment_tag
    workload_dir = Path(cfg.output.workload_dir) / tag / experiment_name
    results_dir = Path(cfg.output.results_dir) / tag / experiment_name
    analysis_dir = Path(cfg.output.analysis_dir) / tag / experiment_name
    return workload_dir, results_dir, analysis_dir


def _detect_primary_db_type(cfg: Config) -> str:
    """Select a primary database type from config for mixed-db setups."""
    if cfg.databases:
        for key in ("baseline", "cedar", "postgres-baseline", "postgres-cedar"):
            db = cfg.databases.get(key)
            if db:
                return db.type
        return next(iter(cfg.databases.values())).type
    return "mysql"


def _ensure_workload_exists(
    cfg: Config, experiment_name: str, config_path: str | None = None
):
    """Check if workload exists for an experiment, generate if missing."""
    workload_dir, _, _ = _get_experiment_paths(cfg, experiment_name)
    workload_path = workload_dir / "workload.json"

    if workload_path.exists():
        return

    click.echo(f"  ⚠ Workload missing for {experiment_name}, generating...")
    workload_dir.mkdir(parents=True, exist_ok=True)

    auth_spec = cfg.auth_spec_path
    if not auth_spec or not Path(auth_spec).exists():
        # Try to find it in the same directory as the config
        if config_path:
            config_dir = Path(config_path).parent
            if auth_spec:
                potential_path = config_dir / auth_spec
                if potential_path.exists():
                    auth_spec = str(potential_path)

    if not auth_spec or not Path(auth_spec).exists():
        # Try default if path relative
        if auth_spec:
            p = Path(auth_spec)
            if not p.is_absolute() and p.exists():
                auth_spec = str(p.absolute())

    if not auth_spec or not Path(auth_spec).exists():
        raise click.ClickException(
            f"Cannot generate workload: auth_spec not found at {auth_spec}"
        )

    db_type = _detect_primary_db_type(cfg)

    # Use config values for workload generation
    queries_per_combo = cfg.workload.queries_per_combination

    generate_workload.callback(
        auth_spec=auth_spec,
        config=config_path,
        queries_per_combo=queries_per_combo,
        seed=cfg.workload.seed,
        db_type=db_type,
        output=str(workload_dir),
        experiment=experiment_name,
    )
    click.echo(f"  ✓ Workload generated at {workload_dir}")


def _ensure_mysql_databases(cfg: Config, purpose: str) -> None:
    """Fail fast if a MySQL-only command targets non-MySQL systems."""
    if not cfg.databases:
        return
    for name in ("baseline", "cedar"):
        db = cfg.databases.get(name)
        if db and db.type != "mysql":
            raise click.ClickException(
                f"{purpose} requires MySQL for '{name}', but config has type '{db.type}'."
            )


@click.group()
def cli():
    """MySQL Authorization Benchmark Framework (Pure Python)"""
    pass


@cli.group("failure")
def failure():
    """Commands for failure resilience experiments."""
    pass


@cli.group("bench-user")
def bench_user():
    """Commands for benchmark user management and authorization verification."""
    pass


@bench_user.command("setup")
@click.option("--config", type=click.Path(exists=True), help="Path to YAML/JSON config")
@click.option(
    "--target", default="both", type=click.Choice(["baseline", "cedar", "both"])
)
@click.option("--db-type", default="mysql", type=click.Choice(["mysql", "postgres"]))
def bench_user_setup(config: str | None, target: str, db_type: str):
    """Create benchmark user (cedar_bench) with appropriate privileges.

    For 'baseline' mode: grants FULL native privileges (for proper comparison).
    For 'cedar' mode: grants NO native privileges (Cedar-only authorization).
    """
    cfg = load_config_file(config)

    from framework.benchmark_user_setup import (
        BENCHMARK_USER,
        create_mysql_benchmark_user,
        create_postgres_benchmark_user,
    )

    targets = ["baseline", "cedar"] if target == "both" else [target]

    for t in targets:
        db_key = t if db_type == "mysql" else f"postgres-{t}"
        db_config = cfg.databases.get(db_key)

        if not db_config:
            click.echo(f"Warning: Database '{db_key}' not found in config, skipping.")
            continue

        grant_native = t == "baseline"

        if db_type == "mysql":
            success = create_mysql_benchmark_user(
                host=db_config.host,
                port=db_config.port,
                admin_user=db_config.user,
                admin_pass=db_config.password,
                db_name=cfg.sysbench.db_name,
                grant_native_privileges=grant_native,
            )
        else:
            success = create_postgres_benchmark_user(
                host=db_config.host,
                port=db_config.port,
                db_name=cfg.pgbench.db_name,
                admin_password=db_config.password,
                grant_native_privileges=grant_native,
                table_owner_role=db_config.user,
            )

        if success:
            mode = (
                "FULL privileges"
                if grant_native
                else "NO native privileges (Cedar-only)"
            )
            click.echo(f"✓ Created '{BENCHMARK_USER}' on {db_key} with {mode}")
        else:
            click.echo(f"✗ Failed to create '{BENCHMARK_USER}' on {db_key}", err=True)


@bench_user.command("verify")
@click.option("--config", type=click.Path(exists=True), help="Path to YAML/JSON config")
@click.option("--target", default="cedar", type=click.Choice(["baseline", "cedar"]))
@click.option("--db-type", default="mysql", type=click.Choice(["mysql", "postgres"]))
def bench_user_verify(config: str | None, target: str, db_type: str):
    """Verify benchmark user access level.

    For 'cedar' target: user should NOT have native access (expect permission denied).
    For 'baseline' target: user should have full native access.
    """
    cfg = load_config_file(config)

    from framework.benchmark_user_setup import verify_benchmark_user_access

    db_key = target if db_type == "mysql" else f"postgres-{target}"
    db_config = cfg.databases.get(db_key)

    if not db_config:
        raise click.ClickException(f"Database '{db_key}' not found in config.")

    db_name = cfg.sysbench.db_name if db_type == "mysql" else cfg.pgbench.db_name

    result = verify_benchmark_user_access(
        host=db_config.host,
        port=db_config.port,
        db_name=db_name,
        db_type=db_type,
        table_name="sbtest1" if db_type == "mysql" else "pgbench_accounts",
    )

    click.echo(f"Connection: {'✓' if result['can_connect'] else '✗'}")
    click.echo(f"Query access: {'✓' if result['can_query'] else '✗'}")

    if target == "cedar":
        if result["can_connect"] and not result["can_query"]:
            click.echo(
                "✓ Correct: User can connect but cannot query (Cedar will handle access)"
            )
        elif result["can_query"]:
            click.echo(
                "⚠ Warning: User has native query access - Cedar authorization may not be tested!"
            )
    else:
        if result["can_connect"] and result["can_query"]:
            click.echo("✓ Correct: User has full native access for baseline comparison")
        else:
            click.echo(
                "⚠ Warning: User lacks native access - baseline comparison may be invalid!"
            )


@bench_user.command("check-cedar")
@click.option("--config", type=click.Path(exists=True), help="Path to YAML/JSON config")
def bench_user_check_cedar(config: str | None):
    """Check Cedar agent health and readiness for benchmarks."""
    cfg = load_config_file(config)

    from framework.cedar_stats import check_cedar_agent_health

    result = check_cedar_agent_health(cfg.cedar_agent.url)

    click.echo(f"Reachable: {'✓' if result['reachable'] else '✗'}")
    click.echo(
        f"Has policies: {'✓' if result['has_policies'] else '✗'} ({result.get('policy_count', 0)} policies)"
    )
    click.echo(
        f"Has entities: {'✓' if result['has_entities'] else '✗'} ({result.get('entity_count', 0)} entities)"
    )
    click.echo(f"Overall: {'✓ Healthy' if result['healthy'] else '✗ Not ready'}")

    if result.get("error"):
        click.echo(f"Error: {result['error']}", err=True)


@bench_user.command("get-stats")
@click.option("--config", type=click.Path(exists=True), help="Path to YAML/JSON config")
def bench_user_get_stats(config: str | None):
    """Get Cedar agent authorization statistics."""
    cfg = load_config_file(config)

    from framework.cedar_stats import (
        get_authorization_decision_breakdown,
        get_cedar_agent_stats,
    )

    stats = get_cedar_agent_stats(cfg.cedar_agent.url)
    decisions = get_authorization_decision_breakdown(cfg.cedar_agent.url)

    if stats:
        click.echo("Cedar Agent Statistics:")
        for key, value in stats.items():
            click.echo(f"  {key}: {value}")
    else:
        click.echo("No statistics available from Cedar agent")

    if decisions.get("total", 0) > 0:
        click.echo("\nAuthorization Decisions:")
        click.echo(f"  Allow: {decisions['allow']}")
        click.echo(f"  Deny: {decisions['deny']}")
        click.echo(f"  Total: {decisions['total']}")


@bench_user.command("cache-stats")
@click.option("--config", type=click.Path(exists=True), help="Path to YAML/JSON config")
@click.option("--db-type", default="mysql", type=click.Choice(["mysql", "postgres"]))
def bench_user_cache_stats(config: str | None, db_type: str):
    """Get MySQL/PostgreSQL Cedar authorization cache statistics."""
    cfg = load_config_file(config)

    from framework.cedar_cache_analysis import (
        format_cache_report,
        get_mysql_cache_stats,
        get_postgres_cache_stats,
    )

    db_key = "cedar" if db_type == "mysql" else "postgres-cedar"
    db_config = cfg.databases.get(db_key)

    if not db_config:
        raise click.ClickException(f"Database '{db_key}' not found in config.")

    if db_type == "mysql":
        stats = get_mysql_cache_stats(
            host=db_config.host,
            port=db_config.port,
            user=db_config.user,
            password=str(db_config.password or ""),
        )
    else:
        stats = get_postgres_cache_stats(
            host=db_config.host,
            port=db_config.port,
            database=db_config.database,
            user=db_config.user,
            password=str(db_config.password or ""),
        )

    click.echo(format_cache_report(stats))


@bench_user.command("cache-recommend")
@click.option("--config", type=click.Path(exists=True), help="Path to YAML/JSON config")
@click.option(
    "--rps", type=float, help="Average request rate (RPS) [default: from config]"
)
@click.option(
    "--unique-combos",
    type=int,
    help="Estimated unique auth combinations [default: from config]",
)
@click.option(
    "--stale-ok",
    type=int,
    help="Acceptable stale time (seconds) [default: from config or 60]",
)
def bench_user_cache_recommend(
    config: str | None,
    rps: float | None,
    unique_combos: int | None,
    stale_ok: int | None,
):
    """Recommend cache configuration based on workload."""
    cfg = load_config_file(config)

    # Resolve values: CLI arg -> Config -> Default
    actual_rps = rps if rps is not None else cfg.cache_analysis.rps
    actual_combos = (
        unique_combos if unique_combos is not None else cfg.cache_analysis.unique_combos
    )
    actual_stale_ok = stale_ok if stale_ok is not None else cfg.cache_analysis.stale_ok

    from framework.cedar_cache_analysis import recommend_cache_config

    recommendation = recommend_cache_config(
        avg_request_rate_rps=actual_rps,
        unique_auth_combinations=actual_combos,
        acceptable_stale_seconds=actual_stale_ok,
    )

    click.echo("=" * 50)
    click.echo("Cache Configuration Recommendation")
    click.echo("=" * 50)
    click.echo(f"Input RPS: {actual_rps:,.0f}")
    click.echo(f"Input Unique Combinations: {actual_combos:,}")
    click.echo("=" * 50)
    click.echo(f"Recommended cache size: {recommendation['recommended_size']}")
    click.echo(f"Recommended TTL: {recommendation['recommended_ttl_seconds']}s")
    click.echo(f"Estimated hit rate: {recommendation['estimated_hit_rate'] * 100:.1f}%")
    click.echo(
        f"Estimated overhead reduction: {recommendation['estimated_overhead_reduction_ms']:.2f}ms/query"
    )
    click.echo("")
    click.echo("Suggested config.yaml:")
    click.echo(recommendation["config_yaml"])


@bench_user.command("cache-verify")
@click.option("--config", type=click.Path(exists=True), help="Path to YAML/JSON config")
@click.option(
    "--mysql-db",
    default="cedar_cache_smoke",
    help="Database used for the cache smoke test",
)
@click.option(
    "--mysql-user",
    default="cedar_cache_smoke_user",
    help="User used for the cache smoke test",
)
@click.option(
    "--mysql-password",
    default="cedar_cache_smoke_pass",
    help="Password used for the cache smoke test user",
)
@click.option(
    "--queries",
    default=2,
    type=int,
    help="Number of repeated queries to run (>=2 recommended)",
)
@click.option(
    "--native-privileges/--no-native-privileges",
    default=False,
    help="Grant MySQL privileges (default: no, force plugin path)",
)
def bench_user_cache_verify(
    config: str | None,
    mysql_db: str,
    mysql_user: str,
    mysql_password: str,
    queries: int,
    native_privileges: bool,
):
    """Run a minimal MySQL workload and verify Cedar cache stats move."""
    if queries < 1:
        raise click.ClickException("--queries must be >= 1")

    cfg = load_config_file(config)
    db_config = cfg.databases.get("cedar")
    if not db_config:
        raise click.ClickException("Database 'cedar' not found in config.")

    try:
        import mysql.connector
    except Exception as e:
        raise click.ClickException(f"mysql-connector-python not available: {e}")

    from framework.cedar_cache_analysis import (
        format_cache_report,
        get_mysql_cache_stats,
    )

    admin_password = db_config.password or ""

    def _admin_conn(database: str | None = None):
        kwargs = {
            "host": db_config.host,
            "port": db_config.port,
            "user": db_config.user,
            "password": admin_password,
        }
        if database:
            kwargs["database"] = database
        return mysql.connector.connect(**kwargs)

    click.echo("Resetting Cedar authorization stats + cache...")
    conn = _admin_conn()
    cur = conn.cursor()

    plugins_cfg = (
        cfg.cedar_agent.plugins if cfg.cedar_agent and cfg.cedar_agent.plugins else {}
    )
    cedar_auth_cfg = plugins_cfg.get("cedar_authorization", {})
    url = cedar_auth_cfg.get("url")
    cache_enabled = cedar_auth_cfg.get("cache_enabled", True)
    cache_size = cedar_auth_cfg.get("cache_size", 1000)
    cache_ttl = cedar_auth_cfg.get("cache_ttl", 300)
    log_info = cedar_auth_cfg.get("log_info", False)

    # Best-effort: not all plugin builds expose all system variables.
    # If a variable isn't present, skip it and rely on status counters.
    cur.execute("SHOW VARIABLES LIKE 'cedar_authorization%'")
    vars_rows = cur.fetchall() or []
    available_vars = {str(name): str(value) for name, value in vars_rows}

    def _safe_set(var_name: str, rhs_sql: str) -> None:
        if var_name not in available_vars:
            return
        try:
            cur.execute(f"SET GLOBAL {var_name} = {rhs_sql}")
        except Exception:
            # Unknown/unsupported variable on this build; ignore.
            return

    _safe_set("cedar_authorization_collect_stats", "ON")
    if url:
        _safe_set("cedar_authorization_url", f"'{url}'")
    _safe_set(
        "cedar_authorization_cache_enabled",
        "ON" if cache_enabled else "OFF",
    )
    _safe_set("cedar_authorization_cache_size", str(int(cache_size)))
    _safe_set("cedar_authorization_cache_ttl", str(int(cache_ttl)))
    _safe_set("cedar_authorization_log_info", "ON" if log_info else "OFF")
    _safe_set("cedar_authorization_cache_flush", "1")
    _safe_set("cedar_authorization_reset_stats", "1")
    try:
        conn.commit()
    except Exception:
        pass

    click.echo("Verifying plugin configuration...")
    cur.execute("SHOW VARIABLES LIKE 'cedar_authorization%'")
    vars_rows = cur.fetchall() or []
    for name, value in vars_rows:
        if name in (
            "cedar_authorization_collect_stats",
            "cedar_authorization_url",
            "cedar_authorization_cache_enabled",
            "cedar_authorization_cache_size",
            "cedar_authorization_cache_ttl",
            "cedar_authorization_log_info",
        ):
            click.echo(f"  {name}: {value}")

    cur.execute(
        "SELECT PLUGIN_STATUS FROM INFORMATION_SCHEMA.PLUGINS "
        "WHERE PLUGIN_NAME = 'cedar_authorization'"
    )
    row = cur.fetchone()
    click.echo(f"  cedar_authorization plugin status: {row[0] if row else 'N/A'}")

    cur.close()
    conn.close()

    click.echo(f"Preparing smoke DB/table: {mysql_db}...")
    conn = _admin_conn()
    cur = conn.cursor()
    cur.execute(f"CREATE DATABASE IF NOT EXISTS `{mysql_db}`")
    cur.execute(f"CREATE TABLE IF NOT EXISTS `{mysql_db}`.`t` (id INT PRIMARY KEY)")
    cur.execute(f"INSERT IGNORE INTO `{mysql_db}`.`t` (id) VALUES (1)")
    cur.execute(f"DROP USER IF EXISTS '{mysql_user}'@'%'")
    cur.execute(f"CREATE USER '{mysql_user}'@'%' IDENTIFIED BY '{mysql_password}'")
    if native_privileges:
        cur.execute(f"GRANT SELECT ON `{mysql_db}`.* TO '{mysql_user}'@'%'")
    cur.execute("FLUSH PRIVILEGES")
    conn.commit()

    cur.execute(f"SHOW GRANTS FOR '{mysql_user}'@'%'")
    for (grant_stmt,) in cur.fetchall():
        click.echo(f"  {grant_stmt}")

    cur.close()
    conn.close()

    click.echo(f"Running {queries} repeated SELECTs as {mysql_user}...")
    conn = mysql.connector.connect(
        host=db_config.host,
        port=db_config.port,
        user=mysql_user,
        password=mysql_password,
        database=mysql_db,
    )
    cur = conn.cursor()
    ok = 0
    for _ in range(queries):
        try:
            cur.execute("SELECT * FROM t LIMIT 1")
            cur.fetchall()
            ok += 1
        except Exception:
            pass
    cur.close()
    conn.close()
    click.echo(f"  Queries executed (may include denied): {ok}/{queries}")

    click.echo("\nMySQL plugin status vars (cedar_authorization%):")
    conn = _admin_conn()
    cur = conn.cursor()
    cur.execute("SHOW STATUS LIKE 'cedar_authorization%'")
    rows = cur.fetchall()
    for name, value in rows:
        click.echo(f"  {name}: {value}")
    cur.close()
    conn.close()

    click.echo("\nParsed cache report:")
    stats = get_mysql_cache_stats(
        host=db_config.host,
        port=db_config.port,
        user=db_config.user,
        password=admin_password,
    )
    click.echo(format_cache_report(stats))


@failure.command("agent-delay-benchmark")
@click.option("--config", type=click.Path(exists=True), help="Path to YAML/JSON config")
def agent_delay_benchmark(config: str | None):
    """Run agent delay benchmark (E7.1)."""
    cfg = load_config_file(config)
    _ensure_mysql_databases(cfg, "Failure resilience experiments")
    failure_cfg = cfg.failure_tests
    if not failure_cfg:
        raise click.ClickException("`failure_tests` section not found in config.")

    proxy_cfg = failure_cfg.proxy
    if not proxy_cfg or not proxy_cfg.enabled:
        raise click.ClickException(
            "Toxiproxy is not enabled in the `failure_tests` config."
        )

    # Determine MySQL container name for URL detection (needed for both proxy URL and upstream)
    mysql_container = None
    if failure_cfg.mysql.port == cfg.databases["cedar"].port:
        mysql_container = "mysql-cedar"
    elif failure_cfg.mysql.port == cfg.databases["baseline"].port:
        mysql_container = "mysql-baseline"

    toxiproxy = ToxiproxyClient(proxy_cfg.control_api)
    # Listen on 0.0.0.0 (all interfaces) so Docker containers can access it
    # Using 127.0.0.1 would only allow localhost access
    listen_addr = f"0.0.0.0:{proxy_cfg.listen_port}"

    # Determine upstream address for Cedar agent
    # If Toxiproxy is in Docker (docker-compose), it should forward to cedar-agent:8180
    # (the container's internal port, not the host-mapped port 8280)
    # If Toxiproxy is on the host, it should forward to 127.0.0.1:8280
    # We assume if MySQL is in Docker, Toxiproxy is also in Docker (docker-compose)
    if mysql_container:
        # Both are in Docker - use container name and internal port
        upstream_addr = "cedar-agent:8180"
    else:
        # Toxiproxy is on host - use host address and mapped port
        upstream_addr = f"{proxy_cfg.upstream_host}:{proxy_cfg.upstream_port}"

    try:
        toxiproxy.ensure_proxy(proxy_cfg.name, listen_addr, upstream_addr)
    except RuntimeError as e:
        raise click.ClickException(str(e))

    # Ensure Cedar agent has policies before starting
    click.echo("Pushing base policies to Cedar agent for failure tests...")
    base_url = cfg.cedar_agent.url.rstrip("/") + "/v1"
    with open(cfg.auth_spec_path) as f:
        spec = json.load(f)

    from framework.translate_to_cedar import create_cedar_policies

    # Default to MySQL for failure tests
    policies = create_cedar_policies(spec, "MySQL")
    if not put_policies(base_url, policies):
        click.echo("Warning: Failed to push policies to Cedar agent", err=True)

    results_dir = (
        Path(cfg.output.results_dir)
        / cfg.experiment_tag
        / "failure"
        / "2_1_agent_delay"
    )
    results_dir.mkdir(parents=True, exist_ok=True)
    raw_csv_path = results_dir / "raw_data.csv"
    summary_csv_path = results_dir / "summary.csv"

    all_results_df = pd.DataFrame()
    summary_data = []

    # Verify database connection and user before starting
    user, password = list(failure_cfg.mysql.users.items())[0]
    db_config_dict = {
        "host": failure_cfg.mysql.host,
        "port": failure_cfg.mysql.port,
        "db": failure_cfg.mysql.db,
    }

    # Get root credentials for setting global variables
    # Determine which database we're using based on port
    root_user = None
    root_password = None
    if failure_cfg.mysql.port == cfg.databases["cedar"].port:
        root_user = cfg.databases["cedar"].user
        root_password = cfg.databases["cedar"].password
    elif failure_cfg.mysql.port == cfg.databases["baseline"].port:
        root_user = cfg.databases["baseline"].user
        root_password = cfg.databases["baseline"].password

    # Test connection first
    try:
        import mysql.connector

        test_conn = mysql.connector.connect(
            host=db_config_dict["host"],
            port=db_config_dict["port"],
            user=user,
            password=password,
            database=db_config_dict["db"],
            connection_timeout=5,
        )
        test_conn.close()
    except mysql.connector.Error as e:
        raise click.ClickException(
            f"Failed to connect to MySQL as user '{user}': {e}\n\n"
            "The failure resilience experiments require the MySQL database and users to be set up first.\n\n"
            "Please run one of the following setup commands:\n"
            "  python cli.py setup-cedar --config config.yaml\n"
            "  python cli.py setup-baseline --config config.yaml\n\n"
            "Or ensure that:\n"
            f"  1. MySQL is running on {db_config_dict['host']}:{db_config_dict['port']}\n"
            f"  2. User '{user}' exists and has access to database '{db_config_dict['db']}'\n"
            f"  3. The password in config.yaml matches the user's password"
        )

    # Verify Cedar permissions by running a test query
    click.echo("Verifying Cedar authorization setup...")
    try:
        import mysql.connector

        test_conn = mysql.connector.connect(
            host=db_config_dict["host"],
            port=db_config_dict["port"],
            user=user,
            password=password,
            database=db_config_dict["db"],
            connection_timeout=5,
        )
        test_cursor = test_conn.cursor()
        try:
            test_cursor.execute(failure_cfg.mysql.query)
            test_cursor.fetchall()
            click.echo("✓ Test query succeeded - Cedar authorization is working")
        except mysql.connector.Error as e:
            if "denied" in str(e).lower() or "1142" in str(e):
                raise click.ClickException(
                    f"Permission denied for test query: {e}\n\n"
                    "This indicates that Cedar authorization is denying access. Possible causes:\n\n"
                    "1. Cedar policies are not set up correctly\n"
                    "2. User entity attributes are missing or incorrect\n"
                    "3. Table entity attributes are missing or incorrect\n"
                    "4. Cedar agent is not accessible from MySQL\n\n"
                    "To fix this:\n"
                    "1. Verify Cedar agent is running: curl http://localhost:8280/v1/\n"
                    "2. Check entities exist: curl http://localhost:8280/v1/data | jq '.[] | select(.uid.id == \"user_bob\")'\n"
                    "3. Check policies exist: curl http://localhost:8280/v1/policies | jq '.[].id'\n"
                    "4. Re-run setup: python cli.py setup-cedar --config config.yaml\n"
                )
            else:
                raise click.ClickException(f"Test query failed: {e}")
        finally:
            test_cursor.close()
            test_conn.close()
    except mysql.connector.Error as e:
        raise click.ClickException(f"Failed to verify permissions: {e}")

    # Detect proxy URL for MySQL container to access
    # If MySQL is in Docker, check if Toxiproxy is also in Docker (docker-compose)
    # If both are in Docker on the same network, use container name
    # Otherwise, use host.docker.internal to reach host-based Toxiproxy
    if mysql_container:
        # MySQL is in Docker - check if Toxiproxy is also in Docker
        # Since Toxiproxy is in docker-compose.yml, use container name
        # This assumes both are on the same Docker network (mysql-experiments)
        proxy_url_for_mysql = f"http://toxiproxy:{proxy_cfg.listen_port}"
    else:
        # MySQL is not in Docker, use localhost
        proxy_url_for_mysql = f"http://{proxy_cfg.host}:{proxy_cfg.listen_port}"
    # Add the /v1/is_authorized path for the authorization endpoint
    proxy_auth_url = f"{proxy_url_for_mysql}/v1/is_authorized"

    click.echo(f"Configuring MySQL plugin to use proxy: {proxy_auth_url}")

    # Get current plugin URL to restore later
    original_auth_url = None
    try:
        import mysql.connector

        conn = mysql.connector.connect(
            host=db_config_dict["host"],
            port=db_config_dict["port"],
            user=root_user or "root",
            password=root_password,
            connection_timeout=5,
        )
        cur = conn.cursor()
        cur.execute("SHOW VARIABLES LIKE 'cedar_authorization_url'")
        result = cur.fetchone()
        if result:
            original_auth_url = result[1]
            click.echo(f"  Current authorization URL: {original_auth_url}")

        # Update plugin to use proxy
        cur.execute(f"SET GLOBAL cedar_authorization_url = '{proxy_auth_url}'")
        click.echo(f"  Updated authorization URL to: {proxy_auth_url}")
        cur.close()
        conn.close()
    except mysql.connector.Error as e:
        raise click.ClickException(
            f"Failed to configure MySQL plugin to use proxy: {e}\n"
            "Ensure you have root credentials configured."
        )

    for delay_ms in tqdm(
        failure_cfg.delays_ms, desc="Testing delays", position=0, leave=True
    ):
        tqdm.write(f"--- Testing with {delay_ms}ms agent delay ---")
        # Toxiproxy applies latency to both request and response directions,
        # effectively doubling the delay. To achieve the desired one-way delay,
        # we halve the configured value.
        actual_latency_ms = delay_ms // 2
        toxiproxy.set_latency(proxy_cfg.name, latency_ms=actual_latency_ms)

        # Get timeout from config if available, otherwise use default
        base_timeout_ms = (
            failure_cfg.timeouts_ms[0] if failure_cfg.timeouts_ms else 5000
        )
        # Adjust timeout to accommodate the delay: round-trip delay + base query time + buffer
        # Round-trip delay = actual_latency_ms * 2 (request + response)
        # Add 20% buffer for safety
        expected_total_latency = (
            actual_latency_ms * 2
        ) + 100  # 100ms for base query + buffer
        timeout_ms = max(base_timeout_ms, int(expected_total_latency * 1.2))

        # For very large delays, reduce repetitions to avoid extremely long test runs
        # Estimate: if expected latency * repetitions > 5 minutes, reduce repetitions
        expected_query_time_ms = expected_total_latency
        max_total_time_ms = 5 * 60 * 1000  # 5 minutes
        repetitions = failure_cfg.sql_repetitions
        if expected_query_time_ms * repetitions > max_total_time_ms:
            original_repetitions = repetitions
            repetitions = max(10, int(max_total_time_ms / expected_query_time_ms))
            tqdm.write(
                f"  Note: Reduced repetitions from {original_repetitions} to {repetitions} "
                f"to keep test duration reasonable (~{repetitions * expected_query_time_ms / 1000:.1f}s)"
            )

        df = run_sql_latency_experiment(
            db_config=db_config_dict,
            user=user,
            password=password,
            query=failure_cfg.mysql.query,
            repetitions=repetitions,
            plugin_timeout_ms=timeout_ms,
            root_user=root_user,
            root_password=root_password,
        )
        df["delay_ms"] = delay_ms
        all_results_df = pd.concat([all_results_df, df], ignore_index=True)

        stats = analyze_latency_results(df)
        stats["delay_ms"] = delay_ms
        summary_data.append(stats)
        tqdm.write(
            f"  Median latency: {stats['median']:.2f}ms, p95: {stats['p95']:.2f}ms, Errors: {stats['errors']}"
        )

    all_results_df.to_csv(raw_csv_path, index=False)
    summary_df = pd.DataFrame(summary_data)
    summary_df.to_csv(summary_csv_path, index=False)

    # Restore original authorization URL if it was changed
    if original_auth_url:
        try:
            import mysql.connector

            conn = mysql.connector.connect(
                host=db_config_dict["host"],
                port=db_config_dict["port"],
                user=root_user or "root",
                password=root_password,
                connection_timeout=5,
            )
            cur = conn.cursor()
            cur.execute(f"SET GLOBAL cedar_authorization_url = '{original_auth_url}'")
            click.echo(f"Restored original authorization URL: {original_auth_url}")
            cur.close()
            conn.close()
        except mysql.connector.Error as e:
            click.echo(f"Warning: Could not restore original authorization URL: {e}")

    click.echo(f"✓ Agent delay benchmark complete. Results saved to {results_dir}")


@failure.command("agent-unavailability-test")
@click.option("--config", type=click.Path(exists=True), help="Path to YAML/JSON config")
@click.option(
    "--timeout-ms",
    type=int,
    help="Cedar authorization plugin timeout in milliseconds "
    "(default: from config or 5000)",
)
def agent_unavailability_test(config: str | None, timeout_ms: int | None):
    """Run agent unavailability test (E7.2)."""
    cfg = load_config_file(config)
    _ensure_mysql_databases(cfg, "Failure resilience experiments")
    failure_cfg = cfg.failure_tests
    if not failure_cfg:
        raise click.ClickException("`failure_tests` section not found in config.")

    proxy_cfg = failure_cfg.proxy
    if not proxy_cfg or not proxy_cfg.enabled:
        raise click.ClickException(
            "Toxiproxy is not enabled in the `failure_tests` config."
        )

    # Determine MySQL container name for upstream detection
    # For this test, we assume MySQL is in Docker if using cedar database
    mysql_container = None
    if failure_cfg.mysql.port == cfg.databases["cedar"].port:
        mysql_container = "mysql-cedar"
    elif failure_cfg.mysql.port == cfg.databases["baseline"].port:
        mysql_container = "mysql-baseline"

    toxiproxy = ToxiproxyClient(proxy_cfg.control_api)
    # Listen on 0.0.0.0 (all interfaces) so Docker containers can access it
    # Using 127.0.0.1 would only allow localhost access
    listen_addr = f"0.0.0.0:{proxy_cfg.listen_port}"

    # Determine upstream address for Cedar agent
    # If Toxiproxy is in Docker (docker-compose), it should forward to cedar-agent:8180
    # (the container's internal port, not the host-mapped port 8280)
    # If Toxiproxy is on the host, it should forward to 127.0.0.1:8280
    # We assume if MySQL is in Docker, Toxiproxy is also in Docker (docker-compose)
    if mysql_container:
        # Both are in Docker - use container name and internal port
        upstream_addr = "cedar-agent:8180"
    else:
        # Toxiproxy is on host - use host address and mapped port
        upstream_addr = f"{proxy_cfg.upstream_host}:{proxy_cfg.upstream_port}"

    try:
        toxiproxy.ensure_proxy(proxy_cfg.name, listen_addr, upstream_addr)
    except RuntimeError as e:
        raise click.ClickException(str(e))

    results_dir = (
        Path(cfg.output.results_dir)
        / cfg.experiment_tag
        / "failure"
        / "2_2_agent_unavailability"
    )
    results_dir.mkdir(parents=True, exist_ok=True)

    click.echo("--- Testing with Cedar agent unavailable ---")
    toxiproxy.set_unavailable(proxy_cfg.name)

    user, password = list(failure_cfg.mysql.users.items())[0]
    db_config_dict = {
        "host": failure_cfg.mysql.host,
        "port": failure_cfg.mysql.port,
        "db": failure_cfg.mysql.db,
    }

    # Get root credentials for setting global variables
    root_user = None
    root_password = None
    if failure_cfg.mysql.port == cfg.databases["cedar"].port:
        root_user = cfg.databases["cedar"].user
        root_password = cfg.databases["cedar"].password
    elif failure_cfg.mysql.port == cfg.databases["baseline"].port:
        root_user = cfg.databases["baseline"].user
        root_password = cfg.databases["baseline"].password

    # Test connection first
    try:
        import mysql.connector

        test_conn = mysql.connector.connect(
            host=db_config_dict["host"],
            port=db_config_dict["port"],
            user=user,
            password=password,
            database=db_config_dict["db"],
            connection_timeout=5,
        )
        test_conn.close()
    except mysql.connector.Error as e:
        toxiproxy.set_available(proxy_cfg.name)  # Restore proxy before failing
        raise click.ClickException(
            f"Failed to connect to MySQL as user '{user}': {e}\n\n"
            "Please run: python cli.py setup-cedar --config config.yaml"
        )

    # Use CLI override if provided, otherwise use config,
    # otherwise default to 5000ms
    if timeout_ms is None:
        timeout_ms = failure_cfg.timeouts_ms[0] if failure_cfg.timeouts_ms else 5000
    click.echo(f"  Using Cedar authorization plugin timeout: {timeout_ms}ms")
    df = run_sql_latency_experiment(
        db_config=db_config_dict,
        user=user,
        password=password,
        query=failure_cfg.mysql.query,
        repetitions=failure_cfg.sql_repetitions,
        plugin_timeout_ms=timeout_ms,
        root_user=root_user,
        root_password=root_password,
    )

    stats = analyze_latency_results(df)
    click.echo(
        f"  Results with agent unavailable: Errors: {stats['errors']}, Successful queries: {stats['count']}"
    )

    df.to_csv(results_dir / "raw_data.csv", index=False)
    pd.DataFrame([stats]).to_csv(results_dir / "summary.csv", index=False)

    toxiproxy.set_available(proxy_cfg.name)
    click.echo(f"✓ Agent unavailability test complete. Results saved to {results_dir}")


@failure.command("agent-stress-test")
@click.option("--config", type=click.Path(exists=True), help="Path to YAML/JSON config")
def agent_stress_test(config: str | None):
    """Run Cedar agent stress test (E7.3)."""
    try:
        check_vegeta_installed()
    except FileNotFoundError as e:
        raise click.ClickException(str(e))

    cfg = load_config_file(config)
    failure_cfg = cfg.failure_tests
    if not failure_cfg:
        raise click.ClickException("`failure_tests` section not found in config.")

    stress_cfg = failure_cfg.agent_stress
    results_dir = (
        Path(cfg.output.results_dir)
        / cfg.experiment_tag
        / "failure"
        / "2_3_agent_stress"
    )
    results_dir.mkdir(parents=True, exist_ok=True)
    summary_data = []

    # Get expected decision for response validation
    expected_decision = getattr(stress_cfg, "expected_decision", None)
    if expected_decision:
        click.echo(
            f"Response validation enabled: expecting '{expected_decision}' decision"
        )
    else:
        click.echo(
            "Warning: No expected_decision configured - responses will not be validated"
        )

    for rps in tqdm(stress_cfg.rps_list, desc="Stress testing", position=0, leave=True):
        tqdm.write(f"--- Stress testing Cedar agent at {rps} RPS ---")

        # Warmup (skip validation during warmup)
        if stress_cfg.warmup_s > 0:
            tqdm.write(f"  Running warmup for {stress_cfg.warmup_s}s...")
            run_vegeta_stress_test(
                target_url=failure_cfg.agent_url,
                rate=rps,
                duration_s=stress_cfg.warmup_s,
                auth_body=stress_cfg.auth_request_body,
            )

        tqdm.write(f"  Running test for {stress_cfg.duration_s}s...")
        results = run_vegeta_stress_test(
            target_url=failure_cfg.agent_url,
            rate=rps,
            duration_s=stress_cfg.duration_s,
            auth_body=stress_cfg.auth_request_body,
            expected_decision=expected_decision,
        )

        if results:
            results["target_rps"] = rps
            summary_data.append(results)
            p50_ms = results["p50_ns"] / 1e6
            p95_ms = results["p95_ns"] / 1e6
            error_rate = results["error_rate"] * 100

            # Show validation status
            validated = results.get("decision_validated")
            if validated is True:
                validation_status = "✓ validated"
            elif validated is False:
                validation_status = (
                    f"✗ {results.get('validation_error', 'validation failed')}"
                )
            else:
                validation_status = "not checked"

            tqdm.write(
                f"  p50: {p50_ms:.2f}ms, p95: {p95_ms:.2f}ms, Failure Rate: {error_rate:.2f}% ({validation_status})"
            )
        else:
            tqdm.write(f"  Failed to get results for {rps} RPS.")

    summary_df = pd.DataFrame(summary_data)
    summary_df.to_csv(results_dir / "summary.csv", index=False)
    click.echo(
        f"✓ Agent stress test complete. Summary saved to {results_dir / 'summary.csv'}"
    )


@failure.command("mysql-under-stress")
@click.option("--config", type=click.Path(exists=True), help="Path to YAML/JSON config")
@click.option("--rps", type=int, help="RPS to stress the Cedar agent with.")
def mysql_under_stress(config: str | None, rps: int):
    """Run MySQL queries while Cedar agent is under stress (E7.4)."""
    cfg = load_config_file(config)
    _ensure_mysql_databases(cfg, "MySQL under stress experiment")
    failure_cfg = cfg.failure_tests
    if not failure_cfg:
        raise click.ClickException("`failure_tests` section not found in config.")

    if not rps:
        raise click.ClickException("Please provide --rps to stress the agent.")

    results_dir = (
        Path(cfg.output.results_dir)
        / cfg.experiment_tag
        / "failure"
        / f"2_4_mysql_under_stress_at_{rps}_rps"
    )
    results_dir.mkdir(parents=True, exist_ok=True)

    stress_cfg = failure_cfg.agent_stress

    import threading

    stress_thread = threading.Thread(
        target=run_vegeta_stress_test,
        args=(
            failure_cfg.agent_url,
            rps,
            stress_cfg.duration_s + 5,  # run for slightly longer
            stress_cfg.auth_request_body,
        ),
    )

    click.echo(f"--- Starting Cedar agent stress at {rps} RPS in background ---")
    stress_thread.start()
    time.sleep(2)  # give vegeta time to start

    click.echo("--- Running MySQL latency test ---")
    user, password = list(failure_cfg.mysql.users.items())[0]
    db_config_dict = {
        "host": failure_cfg.mysql.host,
        "port": failure_cfg.mysql.port,
        "db": failure_cfg.mysql.db,
    }

    # Get root credentials for setting global variables
    root_user = None
    root_password = None
    if failure_cfg.mysql.port == cfg.databases["cedar"].port:
        root_user = cfg.databases["cedar"].user
        root_password = cfg.databases["cedar"].password
    elif failure_cfg.mysql.port == cfg.databases["baseline"].port:
        root_user = cfg.databases["baseline"].user
        root_password = cfg.databases["baseline"].password

    # Test connection first
    try:
        import mysql.connector

        test_conn = mysql.connector.connect(
            host=db_config_dict["host"],
            port=db_config_dict["port"],
            user=user,
            password=password,
            database=db_config_dict["db"],
            connection_timeout=5,
        )
        test_conn.close()
    except mysql.connector.Error as e:
        raise click.ClickException(
            f"Failed to connect to MySQL as user '{user}': {e}\n\n"
            "Please run: python cli.py setup-cedar --config config.yaml"
        )

    summary_data = []

    for timeout in tqdm(
        failure_cfg.timeouts_ms, desc="Testing timeouts", position=0, leave=True
    ):
        tqdm.write(f"  Testing with plugin timeout: {timeout}ms")
        df = run_sql_latency_experiment(
            db_config=db_config_dict,
            user=user,
            password=password,
            query=failure_cfg.mysql.query,
            repetitions=failure_cfg.sql_repetitions,
            plugin_timeout_ms=timeout,
            root_user=root_user,
            root_password=root_password,
        )
        stats = analyze_latency_results(df)
        stats["plugin_timeout_ms"] = timeout
        stats["agent_rps"] = rps
        summary_data.append(stats)
        tqdm.write(
            f"  Median latency: {stats['median']:.2f}ms, p95: {stats['p95']:.2f}ms, Errors: {stats['errors']}"
        )

    summary_df = pd.DataFrame(summary_data)
    summary_df.to_csv(results_dir / "summary.csv", index=False)

    click.echo("Waiting for stress test thread to complete...")
    stress_thread.join()
    click.echo(f"✓ MySQL under stress test complete. Results saved to {results_dir}")


@cli.command("generate-workload")
@click.argument("auth_spec", required=False, type=click.Path(exists=True))
@click.option("--config", type=click.Path(exists=True), help="Path to YAML/JSON config")
@click.option(
    "--queries-per-combo",
    default=None,
    type=int,
    help="Queries per (user,action,table)",
)
@click.option("--seed", default=None, type=int, help="Random seed")
@click.option(
    "--db-type",
    default="mysql",
    type=click.Choice(["mysql", "postgres"]),
    help="Database type for SQL syntax",
)
@click.option(
    "--output",
    default=None,
    type=click.Path(),
    help="Output workload dir (overrides config and experiment)",
)
@click.option(
    "--experiment",
    default="benchmark",
    type=str,
    help="Experiment name for path suffix (default: benchmark)",
)
def generate_workload(
    auth_spec: str | None,
    config: str | None,
    queries_per_combo: int | None,
    seed: int | None,
    db_type: str,
    output: str | None,
    experiment: str,
):
    """Generate workload from authorization spec

    AUTH_SPEC can be provided as argument or via --config (auth_spec_path).
    """
    cfg: Config = load_config_file(config)

    # Determine auth_spec: CLI argument > config > error
    auth_spec_path = auth_spec or cfg.auth_spec_path
    if not auth_spec_path:
        raise click.ClickException(
            "Auth spec not specified. Provide AUTH_SPEC as argument or set 'auth_spec_path' in config file."
        )
    if not Path(auth_spec_path).exists():
        raise click.ClickException(f"Auth spec file not found: {auth_spec_path}")

    if queries_per_combo is not None:
        cfg.workload.queries_per_combination = queries_per_combo
    if seed is not None:
        cfg.workload.seed = seed

    # Determine output directory: CLI option > experiment-specific path > error
    if output is not None:
        output_dir = Path(output)
    elif cfg.output.workload_dir is not None:
        # Use experiment-specific path
        exp_workload_dir, _, _ = _get_experiment_paths(cfg, experiment)
        output_dir = exp_workload_dir
    else:
        raise click.ClickException(
            "No output directory specified. Provide --output or set 'output.workload_dir' in config file."
        )

    output_dir.mkdir(parents=True, exist_ok=True)

    wg = WorkloadGenerator(
        auth_spec_path=str(auth_spec_path),
        config=cfg,
        seed=cfg.workload.seed,
        db_type=db_type,
    )
    workload: Workload = wg.generate()
    workload_path = output_dir / "workload.json"
    workload.save(workload_path)
    click.echo(f"✓ Workload generated for {db_type}: {workload_path}")


@cli.command("run-benchmark")
@click.argument("workload_dir", required=False, type=click.Path(exists=True))
@click.option("--config", type=click.Path(exists=True), help="Path to YAML/JSON config")
@click.option(
    "--iterations", default=None, type=int, help="Total iterations to execute"
)
@click.option(
    "--concurrency",
    default=None,
    type=int,
    help="Number of parallel workers (not required for single-thread)",
)
@click.option(
    "--experiment",
    default="benchmark",
    type=str,
    help="Experiment name for path suffix (default: benchmark)",
)
@click.option(
    "--warmup-iterations",
    default=None,
    type=int,
    help="Number of iterations to discard from start",
)
@click.option(
    "--warmup-seconds",
    default=None,
    type=int,
    help="Seconds to run warmup before measurement (mutually exclusive with iterations)",
)
@click.option(
    "--n-runs",
    default=None,
    type=int,
    help="Number of independent runs for statistical rigor (default: from config)",
)
def run_benchmark(
    workload_dir: str | None,
    config: str | None,
    iterations: int | None,
    concurrency: int | None,
    experiment: str,
    warmup_iterations: int | None,
    warmup_seconds: int | None,
    n_runs: int | None,
):
    """Run benchmark with pre-generated workload

    WORKLOAD_DIR can be provided as argument or via --config (output.workload_dir).
    """
    cfg: Config = load_config_file(config)

    # Use experiment-specific paths
    exp_workload_dir, exp_results_dir, _ = _get_experiment_paths(cfg, experiment)

    # Determine workload directory: CLI argument > config > error
    if workload_dir is not None:
        pass
    else:
        # Just update local variable, logic handled below
        pass

    # ... (existing workload path logic handled below, re-inserting)

    if workload_dir is not None:
        workload_path = Path(workload_dir) / "workload.json"
    else:
        workload_path = exp_workload_dir / "workload.json"

    if not workload_path.exists():
        raise click.ClickException(f"Workload file not found: {workload_path}")

    if iterations is not None:
        cfg.benchmark.iterations = iterations
    if concurrency is not None:
        cfg.benchmark.concurrency = concurrency

    if warmup_iterations is not None:
        cfg.benchmark.warmup_iterations = warmup_iterations
    if warmup_seconds is not None:
        cfg.benchmark.warmup_seconds = warmup_seconds
    if n_runs is not None:
        cfg.benchmark.n_runs = n_runs

    workload = Workload.load(workload_path)
    runner = BenchmarkRunner(workload=workload, config=cfg)

    # Use experiment-specific results directory
    exp_results_dir.mkdir(parents=True, exist_ok=True)

    # Multi-run execution with statistical analysis
    actual_n_runs = cfg.benchmark.n_runs

    if actual_n_runs > 1:
        tqdm.write(f"Running {actual_n_runs} independent runs for statistical rigor...")
        all_runs = []

        for run_idx in range(actual_n_runs):
            tqdm.write(f"\n--- Run {run_idx + 1}/{actual_n_runs} ---")
            run_result = runner.run()
            run_result["run_id"] = run_idx + 1
            all_runs.append(run_result)

        # Compute aggregate statistics with CIs
        from framework.stats import compute_overhead_with_ci, format_overhead_with_ci

        # Extract latencies from each run
        baseline_latencies = []
        cedar_latencies = []
        for run in all_runs:
            baseline_latencies.extend(
                [
                    r.get("latency_ms", 0)
                    for r in run.get("baseline", [])
                    if r.get("success")
                ]
            )
            cedar_latencies.extend(
                [
                    r.get("latency_ms", 0)
                    for r in run.get("cedar", [])
                    if r.get("success")
                ]
            )

        overhead_analysis = compute_overhead_with_ci(
            baseline_latencies,
            cedar_latencies,
            is_throughput=False,
            n_bootstrap=cfg.benchmark.n_bootstrap,
            confidence_level=cfg.benchmark.confidence_level,
        )

        results = {
            "multi_run": True,
            "n_runs": actual_n_runs,
            "confidence_level": cfg.benchmark.confidence_level,
            "runs": all_runs,
            "aggregate_stats": overhead_analysis,
        }

        tqdm.write(f"\n=== Aggregate Results ({actual_n_runs} runs) ===")
        tqdm.write(f"Overhead: {format_overhead_with_ci(overhead_analysis)}")
    else:
        results = runner.run()

    results_path = exp_results_dir / "results.json"
    with results_path.open("w") as f:
        json.dump(results, f, indent=2)
    tqdm.write(f"✓ Results saved to: {results_path}")


def _execute_sql_via_container(
    container_name: str,
    sql: str,
    user: str = "root",
    password: str | None = None,
    show_errors: bool = False,
) -> tuple[bool, str | None]:
    """Execute SQL via docker exec. Returns (success, error_message)."""
    import subprocess

    try:
        # First check if container is running
        check_result = subprocess.run(
            ["docker", "ps", "--format", "{{.Names}}"],
            capture_output=True,
            text=True,
            timeout=5,
        )
        if container_name not in check_result.stdout:
            return False, f"Container {container_name} is not running"

        # Use -p{password} format (no space) to avoid password prompt
        # Use default socket connection for reliability inside container
        if password:
            cmd = [
                "docker",
                "exec",
                "-i",
                container_name,
                "mysql",
                f"-u{user}",
                f"-p{password}",
            ]
        else:
            cmd = ["docker", "exec", "-i", container_name, "mysql", f"-u{user}"]

        result = subprocess.run(
            cmd,
            input=sql.encode(),
            capture_output=True,
            timeout=30,
        )

        # Decode output
        stdout_text = result.stdout.decode("utf-8", errors="ignore")
        stderr_text = result.stderr.decode("utf-8", errors="ignore")
        combined_output = (stderr_text + stdout_text).strip()

        # Check for actual MySQL errors (not warnings)
        # Look for ERROR lines specifically (warnings contain "Warning" or "[Warning]")
        has_error = result.returncode != 0
        error_lines = []

        for line in combined_output.split("\n"):
            line_upper = line.upper()
            # Skip warning lines
            if "[WARNING]" in line_upper or "WARNING:" in line_upper:
                continue
            # Check for actual errors
            if (
                "ERROR" in line_upper
                or "Access denied" in line
                or "access denied" in line.lower()
            ):
                error_lines.append(line)
                has_error = True

        if has_error:
            error_msg = "\n".join(error_lines) if error_lines else combined_output
            if show_errors and error_msg:
                click.echo(f"SQL execution error: {error_msg}", err=True)
            return False, error_msg.strip() if error_msg else "Unknown error"

        return True, None
    except subprocess.TimeoutExpired:
        return False, "Command timed out"
    except FileNotFoundError:
        return False, "docker command not found"
    except Exception as e:
        return False, str(e)


def _find_mysql_container(port: int) -> str | None:
    """Find MySQL container by port mapping."""
    import subprocess

    try:
        # Find container with port mapping matching port:3306
        result = subprocess.run(
            ["docker", "ps", "--format", "{{.Names}}\t{{.Ports}}"],
            capture_output=True,
            text=True,
            timeout=5,
        )
        if result.returncode == 0:
            for line in result.stdout.splitlines():
                parts = line.split("\t")
                if len(parts) == 2:
                    name, ports = parts
                    if f"{port}:3306" in ports or f"0.0.0.0:{port}->3306" in ports:
                        return name
            # Try alternative: find container with mysql-cedar in name
            result = subprocess.run(
                ["docker", "ps", "--format", "{{.Names}}"],
                capture_output=True,
                text=True,
                timeout=5,
            )
            if result.returncode == 0:
                for name in result.stdout.splitlines():
                    if "mysql" in name.lower() and "cedar" in name.lower():
                        return name
    except Exception:
        pass
    return None


def _reload_mysql_privileges(
    container_name: str | None, user: str = "root", password: str | None = None
) -> None:
    """
    Force MySQL to reload privileges after creating root@'%'.

    This addresses the issue where MySQL caches connection information and
    new connections may fail even after FLUSH PRIVILEGES. We:
    1. Kill any non-system connections to force privilege cache refresh
    2. Use mysqladmin reload for more forceful privilege reload
    3. Wait for MySQL to process the changes
    """
    import subprocess
    import time

    if not container_name:
        # If no container, just wait longer
        time.sleep(3.0)
        return

    try:
        # Method 1: Use mysqladmin reload (more forceful than FLUSH PRIVILEGES)
        click.echo("Reloading MySQL privileges to refresh connection cache...")
        if password:
            reload_cmd = [
                "docker",
                "exec",
                container_name,
                "mysqladmin",
                f"-u{user}",
                f"-p{password}",
                "reload",
            ]
        else:
            reload_cmd = [
                "docker",
                "exec",
                container_name,
                "mysqladmin",
                f"-u{user}",
                "reload",
            ]

        reload_result = subprocess.run(reload_cmd, capture_output=True, timeout=10)

        if reload_result.returncode == 0:
            click.echo("✓ Privileges reloaded via mysqladmin")
        else:
            # If mysqladmin fails, try killing connections
            click.echo(
                "Warning: mysqladmin reload failed, trying alternative method..."
            )

            # Method 2: Kill non-system connections to force cache refresh
            # Get list of connection IDs (excluding system connections)
            kill_sql = """
            SELECT CONCAT('KILL ', id, ';') 
            FROM information_schema.processlist 
            WHERE user != 'system user' AND id != CONNECTION_ID();
            """

            if password:
                kill_cmd = [
                    "docker",
                    "exec",
                    "-i",
                    container_name,
                    "mysql",
                    f"-u{user}",
                    f"-p{password}",
                    "-N",
                    "-s",
                ]
            else:
                kill_cmd = [
                    "docker",
                    "exec",
                    "-i",
                    container_name,
                    "mysql",
                    f"-u{user}",
                    "-N",
                    "-s",
                ]

            kill_result = subprocess.run(
                kill_cmd,
                input=kill_sql.encode(),
                capture_output=True,
                timeout=10,
            )

            if kill_result.returncode == 0:
                kill_statements = kill_result.stdout.decode(
                    "utf-8", errors="ignore"
                ).strip()
                if kill_statements:
                    # Execute kill statements
                    for kill_stmt in kill_statements.split("\n"):
                        if kill_stmt.strip():
                            subprocess.run(
                                kill_cmd,
                                input=kill_stmt.encode(),
                                capture_output=True,
                                timeout=5,
                            )
                    click.echo("✓ Killed existing connections to refresh cache")

        # Wait for MySQL to fully process the privilege changes
        time.sleep(2.0)

    except Exception as e:
        # If reload fails, just wait longer
        click.echo(f"Warning: Could not reload privileges: {e}")
        click.echo("Waiting longer for MySQL to process changes...")
        time.sleep(3.0)


def _connect_with_retry(
    host: str,
    port: int,
    user: str,
    password: str | None,
    max_retries: int = 3,
    initial_delay: float = 1.0,
):
    """
    Connect to MySQL with retry logic.

    After creating root@'%', MySQL may need time to refresh its connection cache.
    This function retries the connection with exponential backoff.
    """
    import time

    import mysql.connector

    delay = initial_delay
    last_error = None

    for attempt in range(max_retries):
        try:
            conn = mysql.connector.connect(
                host=host,
                port=port,
                user=user,
                password=password,
                connection_timeout=5,
            )
            if attempt > 0:
                click.echo(f"✓ Connection successful on attempt {attempt + 1}")
            return conn
        except mysql.connector.Error as e:
            last_error = e
            if attempt < max_retries - 1:
                click.echo(f"Connection attempt {attempt + 1} failed: {e}")
                click.echo(f"Retrying in {delay:.1f} seconds...")
                time.sleep(delay)
                delay *= 2  # Exponential backoff
            else:
                # Last attempt failed
                raise

    # Should never reach here, but just in case
    raise (
        last_error
        if last_error
        else mysql.connector.Error("Connection failed after retries")
    )


def _detect_cedar_agent_url_for_container(
    mysql_container: str | None, host_url: str
) -> str:
    """Detect the correct Cedar agent URL for MySQL container to use.

    Args:
        mysql_container: Name of MySQL container (if running in Docker)
        host_url: URL that works from host (e.g., http://localhost:8280)

    Returns:
        URL that MySQL container can use to reach Cedar agent
    """
    if not mysql_container:
        # Not in Docker, use host URL
        return host_url

    # Check if cedar-agent container exists and is on same network
    import re
    import subprocess

    try:
        result = subprocess.run(
            ["docker", "ps", "--format", "{{.Names}}"],
            capture_output=True,
            text=True,
            timeout=5,
        )
        if result.returncode == 0:
            if "cedar-agent" in result.stdout:
                # Check if they're on the same network
                mysql_networks_result = subprocess.run(
                    [
                        "docker",
                        "inspect",
                        mysql_container,
                        "--format",
                        "{{range $key, $value := .NetworkSettings.Networks}}{{$key}} {{end}}",
                    ],
                    capture_output=True,
                    text=True,
                    timeout=5,
                )
                cedar_networks_result = subprocess.run(
                    [
                        "docker",
                        "inspect",
                        "cedar-agent",
                        "--format",
                        "{{range $key, $value := .NetworkSettings.Networks}}{{$key}} {{end}}",
                    ],
                    capture_output=True,
                    text=True,
                    timeout=5,
                )

                if (
                    mysql_networks_result.returncode == 0
                    and cedar_networks_result.returncode == 0
                ):
                    mysql_networks = set(mysql_networks_result.stdout.strip().split())
                    cedar_networks = set(cedar_networks_result.stdout.strip().split())

                    # Check if they share a non-default network
                    shared_networks = mysql_networks & cedar_networks
                    shared_networks.discard("bridge")  # Ignore default bridge

                    if shared_networks:
                        # On same Docker network, use container name + internal port
                        return "http://cedar-agent:8180"
    except Exception:
        pass

    # Fallback: use host.docker.internal (works on Mac/Windows Docker Desktop)
    # For Linux, this might not work, but it's the best guess
    # Extract port from host_url
    if ":8280" in host_url:
        return "http://host.docker.internal:8280"

    # If port is different, try to extract it
    port_match = re.search(r":(\d+)", host_url)
    if port_match:
        port = port_match.group(1)
        return f"http://host.docker.internal:{port}"

    return host_url


def _is_localhost_url(url: str) -> bool:
    s = (url or "").strip().lower()
    return (
        s.startswith("http://localhost")
        or s.startswith("https://localhost")
        or s.startswith("http://127.0.0.1")
        or s.startswith("https://127.0.0.1")
    )


def _resolve_mysql_plugin_cfg_for_runtime(
    cfg: Config, mysql_container: str | None
) -> dict[str, dict[str, Any]]:
    plugins_cfg = dict(cfg.cedar_agent.plugins or {}) if cfg.cedar_agent else {}
    host_url = cfg.cedar_agent.url if cfg.cedar_agent else "http://localhost:8280"
    base_for_mysql = _detect_cedar_agent_url_for_container(mysql_container, host_url)
    base_for_mysql = base_for_mysql.rstrip("/")

    ddl = dict((plugins_cfg.get("ddl_audit") or {}) or {})
    auth = dict((plugins_cfg.get("cedar_authorization") or {}) or {})

    # If user supplied host-only URLs, rewrite to container-reachable base.
    if not ddl.get("url") or _is_localhost_url(str(ddl.get("url"))):
        ddl["url"] = base_for_mysql
    if not auth.get("url") or _is_localhost_url(str(auth.get("url"))):
        auth["url"] = f"{base_for_mysql}/v1/is_authorized"

    return {"ddl_audit": ddl, "cedar_authorization": auth}


def _apply_and_check_mysql_cedar_sysvars(
    *,
    cfg: Config,
    db_config: Any,
    admin_user: str,
    admin_password: str | None,
    label: str,
) -> dict[str, Any]:
    """Best-effort: apply+check Cedar MySQL plugin sysvars.

    Prints mismatches but does not fail the run (experiments already have other
    verification paths).
    """

    try:
        import mysql.connector

        from framework.mysql_introspection import ensure_mysql_cedar_plugin_sysvars
    except Exception as e:
        click.echo(f"  ⚠ {label}: sysvar check skipped (missing deps): {e}")
        return {"skipped": True, "error": str(e)}

    mysql_container = _find_mysql_container(db_config.port)
    plugins_cfg = _resolve_mysql_plugin_cfg_for_runtime(cfg, mysql_container)

    conn = mysql.connector.connect(
        host=db_config.host,
        port=db_config.port,
        user=admin_user,
        password=admin_password or "",
    )
    try:
        apply_res = ensure_mysql_cedar_plugin_sysvars(conn, plugins_cfg)

        cur = conn.cursor()
        try:
            cur.execute("SHOW VARIABLES LIKE 'ddl_audit_%'")
            ddl_rows = cur.fetchall() or []
            ddl_vars = {str(k): str(v) for (k, v) in ddl_rows}

            cur.execute("SHOW VARIABLES LIKE 'cedar_authorization_%'")
            auth_rows = cur.fetchall() or []
            auth_vars = {str(k): str(v) for (k, v) in auth_rows}
        finally:
            cur.close()

        expected_ddl_url = str((plugins_cfg.get("ddl_audit") or {}).get("url") or "")
        expected_auth_url = str(
            (plugins_cfg.get("cedar_authorization") or {}).get("url") or ""
        )
        actual_ddl_url = ddl_vars.get("ddl_audit_cedar_url", "")
        actual_auth_url = auth_vars.get("cedar_authorization_url", "")

        mismatches: dict[str, dict[str, str]] = {}
        if expected_ddl_url and actual_ddl_url and expected_ddl_url != actual_ddl_url:
            mismatches["ddl_audit_cedar_url"] = {
                "expected": expected_ddl_url,
                "actual": actual_ddl_url,
            }
        if (
            expected_auth_url
            and actual_auth_url
            and expected_auth_url != actual_auth_url
        ):
            mismatches["cedar_authorization_url"] = {
                "expected": expected_auth_url,
                "actual": actual_auth_url,
            }

        click.echo(f"  {label}: MySQL Cedar plugin sysvars")
        if "ddl_audit_enabled" in ddl_vars:
            click.echo(f"    - ddl_audit_enabled = {ddl_vars.get('ddl_audit_enabled')}")
        if "ddl_audit_cedar_url" in ddl_vars:
            click.echo(
                f"    - ddl_audit_cedar_url = {ddl_vars.get('ddl_audit_cedar_url')}"
            )
        if "cedar_authorization_url" in auth_vars:
            click.echo(
                f"    - cedar_authorization_url = {auth_vars.get('cedar_authorization_url')}"
            )

        for k, v in mismatches.items():
            click.echo(
                f"  ⚠ {label}: sysvar mismatch {k}: expected={v['expected']} actual={v['actual']}",
                err=True,
            )

        return {
            "mysql_container": mysql_container,
            "apply": apply_res,
            "vars": {
                "ddl_audit_enabled": ddl_vars.get("ddl_audit_enabled"),
                "ddl_audit_cedar_url": actual_ddl_url,
                "cedar_authorization_url": actual_auth_url,
            },
            "mismatches": mismatches,
        }
    finally:
        conn.close()


def _print_cedar_agent_stats(*, cfg: Config, label: str) -> dict[str, Any]:
    try:
        from framework.cedar_stats import (
            get_authorization_decision_breakdown,
            get_cedar_agent_stats,
        )
    except Exception as e:
        click.echo(f"  ⚠ {label}: cedar agent stats skipped (missing deps): {e}")
        return {"skipped": True, "error": str(e)}

    stats = get_cedar_agent_stats(cfg.cedar_agent.url)
    decisions = get_authorization_decision_breakdown(cfg.cedar_agent.url)
    total = stats.get("total_requests", stats.get("requests", "N/A"))
    click.echo(
        f"  {label}: cedar-agent stats total={total} allow={decisions.get('allow', 0)} deny={decisions.get('deny', 0)}"
    )
    return {"stats": stats, "decisions": decisions}


@cli.command("setup-baseline")
@click.argument("auth_spec", required=False, type=click.Path(exists=True))
@click.option("--config", type=click.Path(exists=True))
@click.option(
    "--db-type",
    default="mysql",
    type=click.Choice(["mysql", "postgres"]),
    help="Database type",
)
def setup_baseline(auth_spec: str | None, config: str | None, db_type: str):
    """Setup baseline database from auth spec (CREATE USER + GRANT)

    AUTH_SPEC can be provided as argument or via --config (auth_spec_path).
    """
    cfg: Config = load_config_file(config)

    # Determine auth_spec: CLI argument > config > error
    auth_spec_path = auth_spec or cfg.auth_spec_path
    if not auth_spec_path:
        raise click.ClickException(
            "Auth spec not specified. Provide AUTH_SPEC as argument or set 'auth_spec_path' in config file."
        )
    if not Path(auth_spec_path).exists():
        raise click.ClickException(f"Auth spec file not found: {auth_spec_path}")

    with open(auth_spec_path) as f:
        spec = json.load(f)

    # Use database-specific logic
    if db_type == "postgres":
        click.echo("Configuring Baseline PostgreSQL...")

        # Determine target database config
        baseline = cfg.databases.get("postgres-baseline") or cfg.databases.get(
            "baseline"
        )
        if not baseline or baseline.type != "postgres":
            raise click.ClickException(
                "No PostgreSQL baseline database found in config"
            )

        sql_gen = SQLGenerator(spec, db_type="postgres")
        grant_statements = translate_to_grants(spec, db_type="postgres")

        setup_sql = "\n".join(
            [
                "-- Base Schema and Data\n",
                sql_gen.generate_table_creation_sql(),
                sql_gen.generate_user_creation_sql(),
                sql_gen.generate_sample_data_sql(),
                "\n-- Applied Grants\n",
                "\n".join(grant_statements),
            ]
        )

        # For PostgreSQL, we usually use psycopg2 or psql
        try:
            import psycopg2
            from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

            # Step 1: Create database (requires connection to postgres or template1)
            click.echo(f"Connecting to {baseline.host}:{baseline.port} as postgres...")
            conn = psycopg2.connect(
                host=baseline.host,
                port=baseline.port,
                user="postgres",
                password=baseline.password or "postgres",
                dbname="postgres",
            )
            conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
            cur = conn.cursor()

            # Execute database creation separately because it can't be in a transaction
            for db_resource in sql_gen.databases:
                db_name = db_resource["name"]
                try:
                    cur.execute(f"CREATE DATABASE {db_name};")
                    click.echo(f"✓ Database created: {db_name}")
                except Exception as e:
                    if "already exists" in str(e):
                        click.echo(f"✓ Database already exists: {db_name}")
                    else:
                        click.echo(f"⚠️  Warning creating database {db_name}: {e}")

            # Step 2: Connect to target database and create tables/users/data/grants
            target_db = baseline.database
            click.echo(f"Connecting to {baseline.host}:{baseline.port}/{target_db}...")
            conn.close()

            conn = psycopg2.connect(
                host=baseline.host,
                port=baseline.port,
                user="postgres",
                password=baseline.password or "postgres",
                dbname=target_db,
            )
            conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
            cur = conn.cursor()

            # Execute everything else
            # We need to split the setup_sql into individual statements for execution
            # This is a bit simplified; a proper SQL parser would be better
            for stmt in setup_sql.split(";"):
                stmt = stmt.strip()
                if not stmt or stmt.startswith("--"):
                    continue
                try:
                    cur.execute(stmt)
                except Exception as e:
                    click.echo(
                        f"⚠️  Warning executing statement: {e}\nStatement: {stmt[:50]}..."
                    )

            cur.close()
            conn.close()
            click.echo("✓ Baseline PostgreSQL setup complete")

        except ImportError:
            click.echo("❌ Error: psycopg2 is required for PostgreSQL setup")
            click.echo("   Try: pip install psycopg2-binary")
            sys.exit(1)
        except Exception as e:
            raise click.ClickException(f"Failed to setup PostgreSQL: {e}")

        return

    # MySQL logic (original)
    import mysql.connector

    baseline = cfg.databases.get("baseline")
    if not baseline or baseline.type != "mysql":
        raise click.ClickException("No MySQL baseline database found in config")
    sql_gen = SQLGenerator(spec)
    setup_sql = sql_gen.generate_complete_setup_sql()
    grant_statements = translate_to_grants(spec)
    # ... rest of the original function ...

    # Step 1: Ensure root@'%' exists with privileges (CRITICAL for external access)
    click.echo("Ensuring root@'%' exists with privileges...")
    container_name = _find_mysql_container(baseline.port)

    root_setup_sql = """CREATE USER IF NOT EXISTS 'root'@'%';
GRANT ALL PRIVILEGES ON *.* TO 'root'@'%' WITH GRANT OPTION;
"""
    if baseline.password:
        root_setup_sql += (
            f"ALTER USER 'root'@'%' IDENTIFIED BY '{baseline.password}';\n"
        )
    root_setup_sql += "FLUSH PRIVILEGES;\n"

    if container_name:
        click.echo(f"Using Docker container: {container_name}")
        # First, test authentication with a simple query
        # Docker MySQL containers create root@localhost with MYSQL_ROOT_PASSWORD
        test_sql = "SELECT 1;"
        password_to_use = (
            baseline.password
            if (baseline.password and baseline.password.strip())
            else None
        )

        # Test connection first
        if password_to_use:
            test_success, test_error = _execute_sql_via_container(
                container_name, test_sql, "root", password_to_use, show_errors=False
            )
            if not test_success:
                click.echo(
                    f"Warning: Authentication test failed with password. Error: {test_error}"
                )
                click.echo("Trying without password...")
                password_to_use = None

        # Now execute the actual setup SQL
        success, error = _execute_sql_via_container(
            container_name, root_setup_sql, "root", password_to_use, show_errors=True
        )
        if success:
            click.echo("✓ root@'%' configured via container")
            # Reload privileges to refresh connection cache
            _reload_mysql_privileges(container_name, "root", password_to_use)
        else:
            click.echo(f"Warning: Failed to configure root@'%' via container: {error}")
            click.echo("Will try direct connection...")
    else:
        click.echo("No Docker container found, will configure via direct connection")
        # Still reload privileges even if no container (wait longer)
        _reload_mysql_privileges(None)

    # Step 2: Connect and create database, tables, users
    # Use direct connection (root@'%' now exists)
    click.echo("Creating database, tables, users, and sample data...")
    try:
        # Use retry logic to handle connection cache refresh delay
        conn = _connect_with_retry(
            host=baseline.host,
            port=baseline.port,
            user="root",
            password=baseline.password if baseline.password else None,
        )
    except mysql.connector.Error:
        password_line = (
            f"  ALTER USER 'root'@'%' IDENTIFIED BY '{baseline.password}';\n"
            if baseline.password
            else ""
        )
        raise click.ClickException(
            "Cannot connect to Baseline MySQL. If you see 'Host is not "
            "allowed to connect', ensure root@'%' exists.\n\n"
            "You can run inside the container:\n\n"
            "  CREATE USER IF NOT EXISTS 'root'@'%';\n"
            "  GRANT ALL PRIVILEGES ON *.* TO 'root'@'%' WITH GRANT OPTION;\n"
            + password_line
            + "  FLUSH PRIVILEGES;\n"
        )

    cur = conn.cursor()

    # Parse and execute dynamic setup SQL (handle multi-line statements)
    statements_to_exec = []
    buffer = []
    for line in setup_sql.split("\n"):
        line = line.strip()
        if not line or line.startswith("--"):
            continue
        buffer.append(line)
        if line.endswith(";"):
            stmt = " ".join(buffer)
            if stmt.strip():
                statements_to_exec.append(stmt)
            buffer = []
    for stmt in statements_to_exec:
        display_stmt = stmt[:100] + "..." if len(stmt) > 100 else stmt
        click.echo(f"Executing: {display_stmt}")
        cur.execute(stmt)

    # Execute CREATE USER and GRANT statements
    click.echo("Creating users and granting privileges...")
    for stmt in grant_statements:
        if not stmt.strip() or stmt.strip().startswith("--"):
            continue
        cur.execute(stmt)

    conn.commit()
    cur.close()
    conn.close()
    click.echo("✓ Baseline MySQL setup complete.")


def _setup_cedar_agent_shared(spec: dict, cfg: Config, namespace: str = ""):
    """Shared logic for configuring Cedar agent with attributes and policies."""
    import requests

    base_url = cfg.cedar_agent.url
    if not base_url.endswith("/v1"):
        base_url = base_url.rstrip("/") + "/v1"
    if not check_cedar_agent(base_url):
        raise click.ClickException(f"Cedar agent not accessible at {base_url}")

    click.echo(
        f"Setting up Cedar agent schema and attributes for namespace '{namespace}'..."
    )
    setup_cedar_schema(base_url, spec, namespace)

    # Wait a moment for entities to propagate
    click.echo("Waiting for entities to propagate to Cedar agent...")
    import time

    time.sleep(2)

    assign_user_attributes(base_url, spec, namespace)
    assign_database_attributes(base_url, spec, namespace)
    assign_resource_attributes(base_url, spec, namespace)
    policies = create_cedar_policies(spec, namespace)

    # Create policies via HTTP
    click.echo("Creating Cedar policies...")
    for policy in policies:
        resp = requests.post(f"{base_url}/policies", json=policy, timeout=10)
        if resp.status_code not in (200, 201, 204, 409):
            raise click.ClickException(
                f"Failed to create policy {policy['id']}: {resp.status_code} {resp.text}"
            )
    click.echo("✓ Cedar agent configuration complete.")


@cli.command("setup-cedar")
@click.argument("auth_spec", required=False, type=click.Path(exists=True))
@click.option("--config", type=click.Path(exists=True))
@click.option(
    "--db-type",
    default="mysql",
    type=click.Choice(["mysql", "postgres"]),
    help="Database type",
)
def setup_cedar(auth_spec: str | None, config: str | None, db_type: str):
    """Setup Cedar database (database, tables, users, Cedar agent) from auth spec

    AUTH_SPEC can be provided as argument or via --config (auth_spec_path).
    """
    cfg: Config = load_config_file(config)

    # Determine auth_spec: CLI argument > config > error
    auth_spec_path = auth_spec or cfg.auth_spec_path
    if not auth_spec_path:
        raise click.ClickException(
            "Auth spec not specified. Provide AUTH_SPEC as argument or set 'auth_spec_path' in config file."
        )
    if not Path(auth_spec_path).exists():
        raise click.ClickException(f"Auth spec file not found: {auth_spec_path}")

    with open(auth_spec_path) as f:
        spec = json.load(f)

    # Use database-specific logic
    if db_type == "postgres":
        click.echo("Configuring Cedar PostgreSQL...")

        # Determine target database config
        cedar_db = cfg.databases.get("postgres-cedar") or cfg.databases.get("cedar")
        if not cedar_db or cedar_db.type != "postgres":
            raise click.ClickException("No PostgreSQL Cedar database found in config")

        sql_gen = SQLGenerator(spec, db_type="postgres")

        # Step 1: Basic setup (databases, tables, users, data)
        # Similar to setup-baseline but for the Cedar-enabled DB
        try:
            import psycopg2
            from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

            click.echo(f"Connecting to {cedar_db.host}:{cedar_db.port} as postgres...")
            conn = psycopg2.connect(
                host=cedar_db.host,
                port=cedar_db.port,
                user="postgres",
                password=cedar_db.password or "postgres",
                dbname="postgres",
            )
            conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
            cur = conn.cursor()

            # Create database
            for db_resource in sql_gen.databases:
                db_name = db_resource["name"]
                try:
                    cur.execute(f"CREATE DATABASE {db_name};")
                    click.echo(f"✓ Database created: {db_name}")
                except Exception as e:
                    if "already exists" in str(e):
                        click.echo(f"✓ Database already exists: {db_name}")
                    else:
                        click.echo(f"⚠️  Warning creating database {db_name}: {e}")

            # Connect to target database
            target_db = cedar_db.database
            click.echo(f"Connecting to {cedar_db.host}:{cedar_db.port}/{target_db}...")
            conn.close()

            conn = psycopg2.connect(
                host=cedar_db.host,
                port=cedar_db.port,
                user="postgres",
                password=cedar_db.password or "postgres",
                dbname=target_db,
            )
            conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
            cur = conn.cursor()

            # Execute schema and data
            setup_sql = "\n".join(
                [
                    sql_gen.generate_table_creation_sql(),
                    sql_gen.generate_user_creation_sql(),
                    sql_gen.generate_sample_data_sql(),
                ]
            )
            for stmt in setup_sql.split(";"):
                stmt = stmt.strip()
                if not stmt or stmt.startswith("--"):
                    continue
                try:
                    cur.execute(stmt)
                except Exception as e:
                    click.echo(f"⚠️  Warning executing statement: {e}")

            # Step 2: Initialize Cedar Extension
            click.echo("Initializing Cedar extension...")
            plugin_init_sql = sql_gen.generate_cedar_plugin_init_sql()
            for stmt in plugin_init_sql.split(";"):
                stmt = stmt.strip()
                if not stmt or stmt.startswith("--"):
                    continue
                try:
                    cur.execute(stmt)
                except Exception as e:
                    click.echo(f"⚠️  Warning initializing Cedar extension: {e}")

            cur.close()
            conn.close()
            click.echo("✓ Cedar PostgreSQL database configured")

        except ImportError:
            click.echo("❌ Error: psycopg2 is required for PostgreSQL setup")
            sys.exit(1)
        except Exception as e:
            raise click.ClickException(f"Failed to setup Cedar PostgreSQL: {e}")

        # Step 3: Configure Cedar Agent (Shared logic)
        click.echo("Configuring Cedar Agent...")
        namespace = "PostgreSQL"
        _setup_cedar_agent_shared(spec, cfg, namespace)

        return

    # MySQL logic (original)
    import mysql.connector

    cedar_db = cfg.databases["cedar"]
    # ... rest of the original function ...

    # Step 1: Ensure root@'%' exists with privileges (CRITICAL for external access)
    click.echo("Ensuring root@'%' exists with privileges...")
    container_name = _find_mysql_container(cedar_db.port)

    # Determine password to use (set early so it's available throughout)
    # Always use the password from config if it exists
    password_to_use = (
        cedar_db.password if (cedar_db.password and cedar_db.password.strip()) else None
    )

    # Build comprehensive root@'%' setup SQL
    # First create user (or ensure it exists), then set password, then grant privileges
    # IMPORTANT: Always set password if one is configured, even if we connect without password initially
    root_setup_sql = """CREATE USER IF NOT EXISTS 'root'@'%';
"""
    # Always set password if configured (even if we connected without password to create the user)
    if password_to_use:
        # Escape single quotes in password for SQL safety
        escaped_password = password_to_use.replace("'", "''")
        root_setup_sql += f"ALTER USER 'root'@'%' IDENTIFIED BY '{escaped_password}';\n"
        click.echo(f"Setting password for root@'%' (length: {len(password_to_use)})")
    root_setup_sql += """GRANT ALL PRIVILEGES ON *.* TO 'root'@'%' WITH GRANT OPTION;
FLUSH PRIVILEGES;
"""

    # Verification SQL to check if root@'%' exists
    verify_sql = """SELECT User, Host FROM mysql.user WHERE User='root' AND Host='%';
"""

    if container_name:
        click.echo(f"Using Docker container: {container_name}")
        # Docker MySQL containers create root@localhost with MYSQL_ROOT_PASSWORD
        # We need to connect as root@localhost to create root@'%'
        # Try with password first, fall back to no password if that fails
        container_password = password_to_use

        # Test connection first to determine what password works for root@localhost
        test_sql = "SELECT 1;"
        if container_password:
            test_success, test_error = _execute_sql_via_container(
                container_name, test_sql, "root", container_password, show_errors=False
            )
            if not test_success:
                click.echo("Warning: Authentication test failed with password.")
                click.echo(
                    "Trying without password (some containers don't require password)..."
                )
                container_password = None

        # Execute the setup SQL to create root@'%'
        # This will create root@'%' and set its password (if configured)
        click.echo("Creating root@'%' user via container...")
        success, error = _execute_sql_via_container(
            container_name, root_setup_sql, "root", container_password, show_errors=True
        )
        if success:
            click.echo("✓ root@'%' configured via container")

            # Verify that root@'%' was created successfully
            # Use subprocess directly to capture actual output (not just success/error)
            try:
                import subprocess

                if container_password:
                    verify_cmd = [
                        "docker",
                        "exec",
                        "-i",
                        container_name,
                        "mysql",
                        "-uroot",
                        f"-p{container_password}",
                        "-N",
                        "-s",
                    ]
                else:
                    verify_cmd = [
                        "docker",
                        "exec",
                        "-i",
                        container_name,
                        "mysql",
                        "-uroot",
                        "-N",
                        "-s",
                    ]
                verify_result = subprocess.run(
                    verify_cmd,
                    input=verify_sql.encode(),
                    capture_output=True,
                    timeout=10,
                    check=True,
                )
                if verify_result.returncode == 0:
                    verify_output = verify_result.stdout.decode(
                        "utf-8", errors="ignore"
                    ).strip()
                    if (
                        verify_output
                        and "root" in verify_output
                        and "%" in verify_output
                    ):
                        click.echo("✓ Verified root@'%' exists in mysql.user table")
                    else:
                        click.echo(
                            f"Warning: Could not verify root@'%' creation (output: {verify_output})"
                        )
                else:
                    error_output = verify_result.stderr.decode("utf-8", errors="ignore")
                    click.echo(
                        f"Warning: Could not verify root@'%' creation: {error_output}"
                    )
            except Exception as e:
                click.echo(f"Warning: Could not verify root@'%' creation: {e}")

            # Verify password was set correctly (if password was configured)
            if password_to_use:
                click.echo("Verifying password was set for root@'%'...")
                verify_password_sql = "SELECT authentication_string FROM mysql.user WHERE User='root' AND Host='%';"
                try:
                    import subprocess

                    if container_password:
                        verify_pwd_cmd = [
                            "docker",
                            "exec",
                            "-i",
                            container_name,
                            "mysql",
                            "-uroot",
                            f"-p{container_password}",
                            "-N",
                            "-s",
                        ]
                    else:
                        verify_pwd_cmd = [
                            "docker",
                            "exec",
                            "-i",
                            container_name,
                            "mysql",
                            "-uroot",
                            "-N",
                            "-s",
                        ]
                    verify_pwd_result = subprocess.run(
                        verify_pwd_cmd,
                        input=verify_password_sql.encode(),
                        capture_output=True,
                        timeout=10,
                        check=False,
                    )
                    if verify_pwd_result.returncode == 0:
                        auth_string = verify_pwd_result.stdout.decode(
                            "utf-8", errors="ignore"
                        ).strip()
                        if auth_string and auth_string != "*":
                            click.echo("✓ Password authentication string is set")
                        else:
                            click.echo(
                                "Warning: Password authentication string appears empty"
                            )
                    else:
                        click.echo("Warning: Could not verify password setting")
                except Exception as e:
                    click.echo(f"Warning: Could not verify password: {e}")

            # Reload privileges to refresh connection cache
            # This addresses MySQL's connection caching issue after creating root@'%'
            _reload_mysql_privileges(container_name, "root", container_password)
        else:
            click.echo(f"Warning: Failed to configure root@'%' via container: {error}")
            click.echo("Will try direct connection...")
    else:
        click.echo("No Docker container found, will configure via direct connection")
        # Still reload privileges even if no container (wait longer)
        _reload_mysql_privileges(None)

    # Step 2: Configure Cedar plugins dynamically from auth spec
    # Now that root@'%' exists, we can use direct connection
    click.echo("Configuring Cedar plugins in Cedar MySQL...")

    # Check and install required runtime dependencies if using Docker container
    if container_name:
        click.echo("Checking for required runtime dependencies...")
        import subprocess

        try:
            # Check if libcurl4 is installed
            check_result = subprocess.run(
                [
                    "docker",
                    "exec",
                    container_name,
                    "sh",
                    "-c",
                    "ldconfig -p | grep -q libcurl.so.4",
                ],
                capture_output=True,
                timeout=10,
            )
            if check_result.returncode != 0:
                click.echo("Installing libcurl4 and libjsoncpp25...")
                install_result = subprocess.run(
                    [
                        "docker",
                        "exec",
                        container_name,
                        "sh",
                        "-c",
                        "apt-get update -qq && apt-get install -y -qq libcurl4 libjsoncpp25 > /dev/null 2>&1",
                    ],
                    capture_output=True,
                    timeout=60,
                )
                if install_result.returncode != 0:
                    click.echo(
                        "Warning: Failed to install dependencies via apt-get. Trying alternative method..."
                    )
                    # Try alternative: install with more verbose output
                    install_result2 = subprocess.run(
                        [
                            "docker",
                            "exec",
                            container_name,
                            "sh",
                            "-c",
                            "apt-get update -qq && apt-get install -y -qq curl libcurl4 libjsoncpp25 2>&1",
                        ],
                        capture_output=True,
                        timeout=60,
                    )
                    if install_result2.returncode != 0:
                        click.echo(
                            "Warning: Could not install required dependencies "
                            "(libcurl4, libjsoncpp25)"
                        )
                        click.echo("Plugins may fail to load. Error output:")
                        click.echo(
                            install_result2.stderr.decode("utf-8", errors="ignore")
                        )
                    else:
                        click.echo("✓ Dependencies installed successfully")
                else:
                    click.echo("✓ Dependencies installed successfully")
            else:
                click.echo("✓ Runtime dependencies already installed")
        except subprocess.TimeoutExpired:
            click.echo("Warning: Dependency check timed out, continuing anyway...")
        except Exception as e:
            click.echo(f"Warning: Could not check dependencies: {e}")
            click.echo("Continuing anyway, but plugins may fail to load...")

    # Generate Cedar plugin init SQL dynamically from auth spec
    sql_gen = SQLGenerator(spec)

    # Generate Cedar plugin initialization SQL
    # URLs are now configured directly in each plugin's config
    plugin_config = cfg.cedar_agent.plugins if cfg.cedar_agent.plugins else None

    # If plugin URLs aren't configured, auto-detect and update plugin config
    if plugin_config:
        # Auto-detect URL for DDL audit if not set
        if "ddl_audit" in plugin_config and "url" not in plugin_config["ddl_audit"]:
            detected_url = _detect_cedar_agent_url_for_container(
                container_name, cfg.cedar_agent.url
            )
            if not detected_url.startswith("http"):
                detected_url = f"http://{detected_url}"
            plugin_config["ddl_audit"]["url"] = detected_url
            click.echo(f"Auto-detected DDL audit URL: {detected_url}")

        # Auto-detect URL for authorization if not set
        if (
            "cedar_authorization" in plugin_config
            and "url" not in plugin_config["cedar_authorization"]
        ):
            detected_url = _detect_cedar_agent_url_for_container(
                container_name, cfg.cedar_agent.url
            )
            if not detected_url.startswith("http"):
                detected_url = f"http://{detected_url}"
            plugin_config["cedar_authorization"]["url"] = (
                f"{detected_url}/v1/is_authorized"
            )
            click.echo(
                f"Auto-detected authorization URL: {plugin_config['cedar_authorization']['url']}"
            )

    # Generate cedar user creation SQL and plugin init SQL.
    # IMPORTANT: For Cedar experiments, workload principals must NOT have native
    # privileges, otherwise MySQL will satisfy access checks via GRANTs and the
    # cedar_authorization plugin + Cedar agent will never be exercised.
    #
    # We drop/recreate users to ensure we clear both static and dynamic privileges.
    user_sql_parts = ["-- Create workload users (Cedar mode: no native privileges)"]
    for user in spec.get("users", []):
        username = user["username"]
        password = user.get("password", "")
        host = user.get("host", "%")
        user_sql_parts.append(f"DROP USER IF EXISTS '{username}'@'{host}';")
        user_sql_parts.append(
            f"CREATE USER '{username}'@'{host}' IDENTIFIED BY '{password}';"
        )
        user_sql_parts.append(f"GRANT USAGE ON *.* TO '{username}'@'{host}';")
    cedar_user_sql = "\n".join(user_sql_parts)

    cedar_init_sql = (
        cedar_user_sql
        + "\n"
        + sql_gen.generate_cedar_plugin_init_sql(plugin_config=plugin_config)
    )

    # password_to_use is already defined above during root@'%' setup
    # Ensure we have the password value (re-fetch from config if somehow lost)
    if "password_to_use" not in locals() or password_to_use is None:
        password_to_use = (
            cedar_db.password
            if (cedar_db.password and cedar_db.password.strip())
            else None
        )

    # Use direct connection (root@'%' now exists)
    # Try connecting with password, but if container setup worked, we can also try via container
    try:
        # Connect using password if configured (root@'%' should have this password set)
        # mysql.connector requires password to be a string or None, not empty string
        # If password_to_use is None or empty, pass None explicitly
        connect_password = password_to_use if password_to_use else None

        # Debug: Show what we're trying to connect with
        click.echo(f"Attempting direct connection to {cedar_db.host}:{cedar_db.port}")
        if connect_password:
            click.echo(
                f"  Using password: {'*' * len(connect_password)} (length: {len(connect_password)})"
            )
        else:
            click.echo("  No password configured - connecting without password")

        # Try connection with retry logic to handle connection cache refresh delay
        conn = _connect_with_retry(
            host=cedar_db.host,
            port=cedar_db.port,
            user="root",
            password=connect_password,
        )

        # Connection successful, proceed with plugin configuration
        cur = conn.cursor()

        # Track plugin installation status
        plugins_installed = {"ddl_audit": False, "cedar_authorization": False}

        for stmt in cedar_init_sql.split("\n"):
            stmt = stmt.strip()
            if not stmt or stmt.startswith("--"):
                continue

            try:
                cur.execute(stmt)

                # Check if this was a plugin installation
                if "INSTALL PLUGIN ddl_audit" in stmt.upper():
                    plugins_installed["ddl_audit"] = True
                    click.echo("  ✓ Installed ddl_audit plugin")
                elif "INSTALL PLUGIN cedar_authorization" in stmt.upper():
                    plugins_installed["cedar_authorization"] = True
                    click.echo("  ✓ Installed cedar_authorization plugin")

                    # Apply cache configuration if present
                    if "cedar_authorization" in plugin_config:
                        auth_config = plugin_config["cedar_authorization"]
                        # Check for cache settings (with defaults if missing)
                        cache_enabled = auth_config.get("cache_enabled", True)
                        cache_size = auth_config.get("cache_size", 1000)
                        cache_ttl = auth_config.get("cache_ttl", 300)

                        click.echo(
                            f"  Configuring authorization cache: enabled={cache_enabled}, size={cache_size}, ttl={cache_ttl}"
                        )

                        # Apply settings
                        try:
                            # Enable/disable
                            enabled_val = "ON" if cache_enabled else "OFF"
                            cur.execute(
                                f"SET GLOBAL cedar_authorization_cache_enabled = {enabled_val}"
                            )

                            # Size
                            cur.execute(
                                f"SET GLOBAL cedar_authorization_cache_size = {cache_size}"
                            )

                            # TTL
                            cur.execute(
                                f"SET GLOBAL cedar_authorization_cache_ttl = {cache_ttl}"
                            )

                            click.echo("  ✓ Applied cache configuration")
                        except mysql.connector.Error as e:
                            click.echo(f"  ⚠ Warning: Failed to configure cache: {e}")
                            click.echo(
                                "    This might happen if the plugin version doesn't support caching yet."
                            )

                # Fetch results for SELECT/SHOW statements to verify
                if stmt.upper().startswith(("SELECT", "SHOW")):
                    results = cur.fetchall()
                    if "PLUGIN_NAME" in stmt.upper() and "ddl_audit" in stmt.upper():
                        if results:
                            click.echo(
                                f"  ✓ Verified ddl_audit plugin status: {results}"
                            )
                    elif (
                        "ddl_audit_cedar_url" in stmt.upper()
                        or "ddl_audit_enabled" in stmt.upper()
                    ):
                        if results:
                            for row in results:
                                click.echo(f"  ✓ {row[0]} = {row[1]}")

            except mysql.connector.Error as e:
                # Only ignore "already exists" errors for INSTALL PLUGIN
                if "INSTALL PLUGIN" in stmt.upper() and (
                    "already exists" in str(e).lower() or "1125" in str(e)
                ):
                    click.echo(f"  ℹ Plugin already installed: {stmt.split()[2]}")
                else:
                    click.echo(f"  ⚠ SQL warning: {e}")
                    click.echo(f"    Statement: {stmt[:100]}...")

        conn.commit()
        cur.close()
        conn.close()

        # Verify plugin is enabled
        click.echo("Verifying DDL audit plugin configuration...")
        # Use retry logic to handle connection cache refresh delay
        conn = _connect_with_retry(
            host=cedar_db.host,
            port=cedar_db.port,
            user="root",
            password=connect_password,
        )
        cur = conn.cursor()
        cur.execute("SHOW VARIABLES LIKE 'ddl_audit_enabled'")
        result = cur.fetchone()
        if result and result[1] == "ON":
            click.echo("  ✓ DDL audit plugin is enabled")
        else:
            click.echo(f"  ⚠ DDL audit plugin may not be enabled: {result}")

        cur.execute("SHOW VARIABLES LIKE 'ddl_audit_cedar_url'")
        result = cur.fetchone()
        if result:
            click.echo(f"  ✓ DDL audit Cedar URL: {result[1]}")

        cur.close()
        conn.close()

        # Verify Cedar authorization plugin configuration
        click.echo("Verifying Cedar authorization plugin configuration...")
        conn = _connect_with_retry(
            host=cedar_db.host,
            port=cedar_db.port,
            user="root",
            password=connect_password,
        )
        cur = conn.cursor()

        # Check if plugin is installed
        cur.execute(
            "SELECT PLUGIN_STATUS FROM INFORMATION_SCHEMA.PLUGINS "
            "WHERE PLUGIN_NAME = 'cedar_authorization'"
        )
        plugin_status = cur.fetchone()
        if plugin_status:
            click.echo(
                f"  ✓ Cedar authorization plugin is installed: {plugin_status[0]}"
            )
        else:
            click.echo("  ⚠ Cedar authorization plugin may not be installed")

        # Check authorization URL
        cur.execute("SHOW VARIABLES LIKE 'cedar_authorization_url'")
        result = cur.fetchone()
        if result:
            click.echo(f"  ✓ Cedar authorization URL: {result[1]}")
        else:
            click.echo("  ⚠ Cedar authorization URL not found")

        # Check authorization timeout
        cur.execute("SHOW VARIABLES LIKE 'cedar_authorization_timeout'")
        result = cur.fetchone()
        if result:
            click.echo(f"  ✓ Cedar authorization timeout: {result[1]}ms")
        else:
            click.echo("  ⚠ Cedar authorization timeout not found")

        # Check authorization cache settings
        click.echo("  Checking authorization cache settings:")

        cur.execute("SHOW VARIABLES LIKE 'cedar_authorization_cache_enabled'")
        result = cur.fetchone()
        if result:
            click.echo(f"    - cedar_authorization_cache_enabled: {result[1]}")

        cur.execute("SHOW VARIABLES LIKE 'cedar_authorization_cache_size'")
        result = cur.fetchone()
        if result:
            click.echo(f"    - cedar_authorization_cache_size: {result[1]}")

        cur.execute("SHOW VARIABLES LIKE 'cedar_authorization_cache_ttl'")
        result = cur.fetchone()
        if result:
            click.echo(f"    - cedar_authorization_cache_ttl: {result[1]}s")

        cur.close()
        conn.close()

    except mysql.connector.Error as e:
        # If direct connection failed, try via container if available
        error_msg = str(e)
        click.echo(f"Direct connection failed: {error_msg}")
        if "Access denied" in error_msg:
            # connect_password is defined in the try block, check if it exists
            if "connect_password" in locals() and connect_password:
                click.echo(
                    "  Password was provided but rejected. Check if root@'%' password matches."
                )
            else:
                click.echo("  No password provided. root@'%' may require a password.")

        if container_name:
            click.echo("Trying to configure plugins via container...")
            # Execute plugin SQL via container
            success, error = _execute_sql_via_container(
                container_name,
                cedar_init_sql,
                "root",
                password_to_use,
                show_errors=True,
            )
            if success:
                click.echo("✓ Cedar plugins configured via container")
            else:
                click.echo(
                    f"Warning: Failed to configure Cedar plugins via container: {error}"
                )
                click.echo(
                    "Continuing anyway, but DDL plugin may not be configured correctly..."
                )
        else:
            click.echo(f"Warning: Failed to configure Cedar plugins: {e}")
            click.echo(
                "Continuing anyway, but DDL plugin may not be configured correctly..."
            )

    # Step 3: Create database, tables, and users dynamically from auth spec
    # Use direct connection (root@'%' now exists)
    click.echo("Creating database, tables, and users in Cedar MySQL...")

    # Generate SQL dynamically from auth spec using SQLGenerator
    setup_sql = sql_gen.generate_complete_setup_sql()

    # Use direct connection (root@'%' now exists)
    # password_to_use already defined above
    # Ensure we have the password value
    if "password_to_use" not in locals() or password_to_use is None:
        password_to_use = (
            cedar_db.password
            if (cedar_db.password and cedar_db.password.strip())
            else None
        )

    try:
        connect_password = password_to_use if password_to_use else None
        click.echo(
            f"Attempting direct connection to {cedar_db.host}:{cedar_db.port} for database/tables/users"
        )
        if connect_password:
            click.echo(
                f"  Using password: {'*' * len(connect_password)} (length: {len(connect_password)})"
            )
        else:
            click.echo("  No password configured - connecting without password")

        # Use retry logic to handle connection cache refresh delay
        conn = _connect_with_retry(
            host=cedar_db.host,
            port=cedar_db.port,
            user="root",
            password=connect_password,
        )
        cur = conn.cursor()

        # Split SQL by semicolons to handle multi-line statements properly
        # Remove comments and empty lines, then split by semicolon
        statements = []
        current_stmt = []
        for line in setup_sql.split("\n"):
            line = line.strip()
            # Skip empty lines and comments
            if not line or line.startswith("--"):
                continue
            current_stmt.append(line)
            # If line ends with semicolon, we have a complete statement
            if line.endswith(";"):
                stmt = " ".join(current_stmt)
                if stmt.strip():
                    statements.append(stmt)
                current_stmt = []

        # Execute each statement
        for stmt in statements:
            # Truncate long statements for display
            display_stmt = stmt[:100] + "..." if len(stmt) > 100 else stmt
            click.echo(f"Executing: {display_stmt}")
            cur.execute(stmt)

        conn.commit()
        cur.close()
        conn.close()
        click.echo("✓ Database, tables, and users created")
    except mysql.connector.Error as e:
        # If direct connection failed, try via container if available
        if container_name:
            click.echo(f"Direct connection failed: {e}")
            click.echo("Trying to create database/tables/users via container...")
            success, error = _execute_sql_via_container(
                container_name, setup_sql, "root", password_to_use, show_errors=True
            )
            if success:
                click.echo("✓ Database, tables, and users created via container")
            else:
                password_line = (
                    f"  ALTER USER 'root'@'%' IDENTIFIED BY '{cedar_db.password}';\n"
                    if cedar_db.password
                    else ""
                )
                raise click.ClickException(
                    f"Cannot connect to Cedar MySQL or execute SQL via container.\n"
                    f"Direct connection error: {e}\n"
                    f"Container execution error: {error}\n\n"
                    "If you see 'Host is not allowed to connect', ensure root@'%' exists.\n\n"
                    "You can run inside the container:\n\n"
                    "  CREATE USER IF NOT EXISTS 'root'@'%';\n"
                    "  GRANT ALL PRIVILEGES ON *.* TO 'root'@'%' WITH GRANT OPTION;\n"
                    + password_line
                    + "  FLUSH PRIVILEGES;\n"
                )
        else:
            password_line = (
                f"  ALTER USER 'root'@'%' IDENTIFIED BY '{cedar_db.password}';\n"
                if cedar_db.password
                else ""
            )
            raise click.ClickException(
                f"Cannot connect to Cedar MySQL: {e}\n\n"
                "If you see 'Host is not allowed to connect', ensure root@'%' exists.\n\n"
                "You can run inside the container:\n\n"
                "  CREATE USER IF NOT EXISTS 'root'@'%';\n"
                "  GRANT ALL PRIVILEGES ON *.* TO 'root'@'%' WITH GRANT OPTION;\n"
                + password_line
                + "  FLUSH PRIVILEGES;\n"
            )

    # Step 4: Setup Cedar agent
    namespace = "MySQL"
    _setup_cedar_agent_shared(spec, cfg, namespace)

    # Final sanity: ensure plugin sysvars match expected container URLs.
    try:
        _apply_and_check_mysql_cedar_sysvars(
            cfg=cfg,
            db_config=cedar_db,
            admin_user="root",
            admin_password=cedar_db.password,
            label="setup-cedar",
        )
    except Exception as e:
        click.echo(f"  ⚠ setup-cedar: sysvar check failed: {e}", err=True)

    click.echo("✓ Cedar MySQL setup complete.")


@cli.command("analyze-results")
@click.argument("results_dir", required=False, type=click.Path(exists=True))
@click.option("--config", type=click.Path(exists=True), help="Path to YAML/JSON config")
@click.option("--format", "fmt", type=click.Choice(["json", "csv"]), default="json")
@click.option(
    "--outputs",
    "outputs_dir",
    type=click.Path(),
    default=None,
    help="Directory to write LaTeX/CSV analysis outputs (overrides config)",
)
@click.option(
    "--visualizations/--no-visualizations",
    default=True,
    help="Generate visualization plots after analysis (default: enabled)",
)
@click.option(
    "--include-extra",
    is_flag=True,
    default=False,
    help="Include extra analyses (policy scaling, concurrency, failures)",
)
def analyze_results(
    results_dir: str | None,
    config: str | None,
    fmt: str,
    outputs_dir: str | None,
    visualizations: bool,
    include_extra: bool = False,
):
    """Analyze benchmark results and print summary; also write overhead tables/CSVs.

    RESULTS_DIR can be provided as argument or via --config (output.results_dir).
    """
    cfg: Config = load_config_file(config)

    # Determine which experiment to analyze based on directory structure
    if results_dir:
        results_path = Path(results_dir)
    else:
        # Default to the "benchmark" experiment if no dir is specified
        _, results_path, _ = _get_experiment_paths(cfg, "benchmark")

    if not results_path.exists():
        raise click.ClickException(f"Results path not found: {results_path}")

    processed_nested = False
    # Handle both file and directory cases.
    # Note: for convenience (and Makefile integration), we also support passing a
    # *root* results directory like "results/" that contains nested
    # "{tag}/{experiment}/results.json" files.
    if results_path.is_file():
        # Direct file path provided
        results_file = results_path
        results_dir_path = results_path.parent
    elif results_path.is_dir():
        # Directory provided, look for results.json or pair_result.json inside
        pair_file = results_path / "pair_result.json"
        results_file = results_path / "results.json"

        if pair_file.exists():
            # Analyze as a pair
            with pair_file.open() as f:
                pair_data = json.load(f)

            # Load raw results for both
            payload = {}
            for sys_key in ["baseline", "cedar"]:
                sys_info = pair_data.get(sys_key)
                if sys_info and "results_path" in sys_info:
                    raw_path = Path(sys_info["results_path"])
                    if not raw_path.exists():
                        # Try relative to results_path (which is the pair dir)
                        raw_path = results_path / raw_path.name

                    if raw_path.exists():
                        with raw_path.open() as f:
                            raw_data = json.load(f)
                            payload.update(raw_data)

            # Save temporary combined results for analyzer
            results_file = results_path / "combined_results.json"
            results_file.write_text(json.dumps(payload))
            results_dir_path = results_path
        elif not results_file.exists():
            # Treat this as a root results directory and analyze all nested results.json and pair_result.json
            results_dir_path = results_path
            discovered_pairs = sorted(results_path.glob("**/pair_result.json"))

            # Determine base directories for mirrored analysis structure
            # We want to preserve the tag/experiment structure in the analysis output
            results_root = (
                Path(cfg.output.results_dir) if cfg.output.results_dir else results_path
            )
            analysis_root = (
                Path(cfg.output.analysis_dir)
                if cfg.output.analysis_dir
                else results_path.parent / "analysis"
            )

            if discovered_pairs:
                click.echo(f"Analyzing {len(discovered_pairs)} nested pair results...")
                for pf in discovered_pairs:
                    pf_dir = pf.parent
                    try:
                        # Try relative to results_root to keep tag/experiment structure if possible
                        rel_dir = pf_dir.relative_to(results_root)
                    except Exception:
                        rel_dir = pf_dir.name

                    per_out_dir = analysis_root / rel_dir

                    # Instead of calling callback recursively, we'll just run the analysis logic here or
                    # use a separate function. But for now, let's stick to the callback but be careful.
                    # We can use ctx.invoke if we had ctx, but we don't easily here without more changes.
                    analyze_results.callback(
                        results_dir=str(pf_dir),
                        config=config,
                        fmt=fmt,
                        outputs_dir=str(per_out_dir),
                        visualizations=visualizations,
                        include_extra=False,
                    )
                processed_nested = True
            else:
                discovered = sorted(results_path.glob("**/results.json"))
                # Filter out results.json that are inside a pair directory already handled
                # (though with the 'return' above for pairs, we might not need this, but good for safety)
                discovered = [
                    d
                    for d in discovered
                    if not any(
                        pf_dir in d.parents
                        for pf_dir in [p.parent for p in discovered_pairs]
                    )
                ]
                if not discovered:
                    raise click.ClickException(
                        f"Results file not found: {results_file}. "
                        f"Expected results.json in directory {results_path}"
                    )

                click.echo(
                    f"No results.json at root; analyzing {len(discovered)} nested results files..."
                )

                for rf in discovered:
                    try:
                        rf_dir = rf.parent
                        rel_dir = rf_dir.relative_to(results_root)
                    except Exception:
                        rf_dir = rf.parent
                        rel_dir = rf_dir.name

                    per_out_dir = analysis_root / rel_dir
                    per_out_dir.mkdir(parents=True, exist_ok=True)

                    # Reuse this command's logic by invoking the callback on each file.
                    analyze_results.callback(
                        results_dir=str(rf),
                        config=config,
                        fmt=fmt,
                        outputs_dir=str(per_out_dir),
                        visualizations=visualizations,
                        include_extra=False,
                    )
                processed_nested = True
        else:
            results_dir_path = results_path
    else:
        raise click.ClickException(f"Invalid results path: {results_path}")

    if not processed_nested:
        with results_file.open() as f:
            results = json.load(f)
        analyzer = ResultsAnalyzer(results)
        summary = analyzer.compute_summary()
        if fmt == "json":
            click.echo(json.dumps(summary, indent=2))
        else:
            # Minimal CSV summary (operation-level not tracked; show overall only)
            import csv

            csv_path = results_dir_path.parent / "summary.csv"
            with csv_path.open("w", newline="") as f:
                writer = csv.DictWriter(f, fieldnames=list(summary.keys()))
                writer.writeheader()
                writer.writerow(summary)
            click.echo(f"✓ Summary CSV written: {csv_path}")

        # Additionally, generate query-by-query overhead LaTeX and CSVs
        try:
            # Determine analysis output directory: CLI option > config > default (results_dir/analysis)
            if outputs_dir is not None:
                out_dir = Path(outputs_dir)
            elif cfg.output.analysis_dir is not None:
                # Use experiment-specific analysis directory
                experiment_name = (
                    results_dir_path.name
                    if results_dir_path.name != "results"
                    else "benchmark"
                )
                _, _, out_dir = _get_experiment_paths(cfg, experiment_name)
            else:
                out_dir = results_dir_path.parent / "analysis"

            out_dir.mkdir(parents=True, exist_ok=True)
            outputs = analyze_to_outputs(results_file, out_dir)
            click.echo("✓ Query-by-Query overhead written:")
            for k, v in outputs.items():
                click.echo(f"  - {k}: {v}")

            # NEW: Automatically attempt comprehensive breakdown if it's a pair or has profiling data
            try:
                from framework.comprehensive_breakdown import (
                    ComprehensiveOverheadAnalyzer,
                )

                comp_analyzer = ComprehensiveOverheadAnalyzer(results_dir_path, out_dir)
                comp_analyzer.generate_report_latex(
                    out_dir / "comprehensive_breakdown.tex"
                )
                comp_analyzer.generate_report_csv(
                    out_dir / "comprehensive_breakdown.csv"
                )
                click.echo(
                    f"  - comprehensive_breakdown: {out_dir}/comprehensive_breakdown.tex"
                )
            except Exception as e:
                click.echo(f"  (Skipping comprehensive breakdown: {e})")
        except Exception as e:  # noqa: BLE001
            click.echo(f"Warning: Failed to generate overhead tables/CSVs: {e}")
    else:
        # For nested analysis, we still need an out_dir for visualizations if include_extra is False but visualizations is True
        # but visualizations is handled at the end.
        # Let's set a default out_dir for the top-level visualizations
        out_dir = (
            Path(cfg.output.analysis_dir) / cfg.experiment_tag / "benchmark"
            if cfg.output.analysis_dir
            else results_path.parent / "analysis" / "benchmark"
        )

    # Only include extra analyses if explicitly requested (or if we are analyzing a top-level dir)
    if not include_extra:
        return

    # Analyze policy scaling results if they exist
    try:
        policy_scaling_dir = (
            Path(cfg.output.results_dir) / cfg.experiment_tag / "policy_scaling"
        )
        if policy_scaling_dir.exists():
            click.echo(f"\nAnalyzing policy scaling results in: {policy_scaling_dir}")
            scaling_subdirs = [d for d in policy_scaling_dir.iterdir() if d.is_dir()]
            if scaling_subdirs:
                summary_data = compute_policy_scaling_summary(scaling_subdirs)

                _, _, analysis_path = _get_experiment_paths(cfg, "policy_scaling")
                analysis_path.mkdir(parents=True, exist_ok=True)

                csv_path = analysis_path / "policy_scaling.csv"
                tex_path = analysis_path / "policy_scaling_table.tex"

                write_policy_scaling_csv(summary_data, csv_path)
                write_policy_scaling_table_tex(summary_data, tex_path)
                click.echo(f"✓ Policy scaling analysis saved to {analysis_path}")
            else:
                click.echo(
                    f"  (No policy scaling subdirectories found in {policy_scaling_dir})"
                )
        else:
            click.echo(f"\n(Policy scaling results not found: {policy_scaling_dir})")
            click.echo("  Run 'policy-scaling' experiment to generate results")
    except Exception as e:
        click.echo(f"Warning: Failed to analyze policy scaling results: {e}")

    # Analyze concurrency results if they exist
    try:
        concurrency_dir = (
            Path(cfg.output.results_dir) / cfg.experiment_tag / "concurrency"
        )
        if concurrency_dir.exists():
            click.echo(f"\nAnalyzing concurrency results in: {concurrency_dir}")
            summary_data = compute_concurrency_summary(concurrency_dir)

            _, _, analysis_path = _get_experiment_paths(cfg, "concurrency")
            analysis_path.mkdir(parents=True, exist_ok=True)

            # Throughput CSV and LaTeX
            throughput_csv_path = analysis_path / "concurrency_throughput.csv"
            throughput_tex_path = analysis_path / "concurrency_throughput_table.tex"
            write_concurrency_throughput_csv(summary_data, throughput_csv_path)
            write_concurrency_throughput_table_tex(summary_data, throughput_tex_path)

            # Latency CSV
            latency_csv_path = analysis_path / "concurrency_latency.csv"
            write_concurrency_latency_csv(summary_data, latency_csv_path)
            click.echo(f"✓ Concurrency analysis saved to {analysis_path}")
        else:
            click.echo(f"\n(Concurrency results not found: {concurrency_dir})")
            click.echo("  Run 'concurrency-benchmark' experiment to generate results")
    except Exception as e:
        click.echo(f"Warning: Failed to analyze concurrency results: {e}")

    # Analyze pgbench results (PostgreSQL macrobenchmark) if they exist
    try:
        pgbench_dir = Path(cfg.output.results_dir) / cfg.experiment_tag / "pgbench"
        if pgbench_dir.exists():
            click.echo(f"\nAnalyzing pgbench results in: {pgbench_dir}")
            from framework.analysis_pgbench import (
                collect_pgbench_comparisons,
                write_pgbench_summary_csv,
                write_pgbench_summary_table_tex,
            )

            rows = collect_pgbench_comparisons(pgbench_dir)
            if rows:
                _, _, pgbench_analysis_dir = _get_experiment_paths(cfg, "pgbench")
                pgbench_analysis_dir.mkdir(parents=True, exist_ok=True)
                csv_out = pgbench_analysis_dir / "pgbench_summary.csv"
                tex_out = pgbench_analysis_dir / "pgbench_summary.tex"
                write_pgbench_summary_csv(rows, csv_out)
                write_pgbench_summary_table_tex(rows, tex_out)
                click.echo(f"✓ pgbench analysis saved to {pgbench_analysis_dir}")
            else:
                click.echo("  (No pgbench comparison files found)")
        else:
            click.echo(f"\n(pgbench results not found: {pgbench_dir})")
    except Exception as e:
        click.echo(f"Warning: Failed to analyze pgbench results: {e}")

    # Analyze TPC-C results if they exist
    try:
        tpcc_dir = Path(cfg.output.results_dir) / cfg.experiment_tag / "tpcc"

        if tpcc_dir.exists():
            click.echo(f"\nAnalyzing TPC-C results in: {tpcc_dir}")
            from framework.analysis_tpcc import (
                collect_tpcc_results,
                write_tpcc_summary_csv,
                write_tpcc_summary_table_tex,
            )

            rows = collect_tpcc_results(tpcc_dir)
            if rows:
                _, _, tpcc_analysis_dir = _get_experiment_paths(cfg, "tpcc")
                tpcc_analysis_dir.mkdir(parents=True, exist_ok=True)
                write_tpcc_summary_csv(rows, tpcc_analysis_dir / "tpcc_summary.csv")
                write_tpcc_summary_table_tex(
                    rows, tpcc_analysis_dir / "tpcc_summary.tex"
                )
                click.echo(f"✓ TPC-C analysis saved to {tpcc_analysis_dir}")
            else:
                click.echo("  (No TPC-C results files found)")
    except Exception as e:
        click.echo(f"Warning: Failed to analyze TPC-C results: {e}")

    # Analyze semantic correctness results if they exist
    try:
        semantics_file = (
            Path(cfg.output.results_dir)
            / cfg.experiment_tag
            / "semantics"
            / "semantic_correctness_results.json"
        )
        if semantics_file.exists():
            click.echo(f"\nAnalyzing robustness results in: {semantics_file.parent}")
            from framework.analysis_semantics import (
                extract_semantics_summary,
                write_robustness_summary_csv,
                write_robustness_summary_table_tex,
            )

            summary = extract_semantics_summary(semantics_file)
            if summary:
                _, _, sem_analysis_dir = _get_experiment_paths(cfg, "semantics")
                sem_analysis_dir.mkdir(parents=True, exist_ok=True)
                write_robustness_summary_csv(
                    summary, sem_analysis_dir / "robustness_summary.csv"
                )
                write_robustness_summary_table_tex(
                    summary, sem_analysis_dir / "robustness_summary.tex"
                )
                click.echo(f"✓ Robustness analysis saved to {sem_analysis_dir}")
    except Exception as e:
        click.echo(f"Warning: Failed to analyze semantics results: {e}")

    # Generate visualizations if requested
    if visualizations:
        try:
            click.echo("\nGenerating visualizations...")

            # 1. Generate visualizations for the current experiment's results
            if out_dir.exists():
                viz_results = generate_all_visualizations(out_dir, out_dir)
                generated = [name for name, path in viz_results.items() if path]
                if generated:
                    click.echo(
                        f"  ✓ Generated {len(generated)} visualizations for current experiment"
                    )

            # 2. Only include extra visualizations if include_extra is true
            if include_extra:
                base_analysis_dir = (
                    Path(cfg.output.analysis_dir) / cfg.experiment_tag
                    if cfg.output.analysis_dir
                    else results_dir_path.parent / "analysis"
                )

                # Check all experiment analysis directories
                experiments_to_check = [
                    "benchmark",
                    "policy_scaling",
                    "concurrency",
                    "pgbench",
                    "tpcc",
                    "semantics",
                ]

                for exp_name in experiments_to_check:
                    # Skip the current experiment if we already did it
                    if out_dir.name == exp_name:
                        continue

                    exp_analysis_dir = base_analysis_dir / exp_name
                    if exp_analysis_dir.exists():
                        generate_all_visualizations(exp_analysis_dir, exp_analysis_dir)

                # Failure experiments (2.x) visualizations and tables
                if cfg.failure_tests:
                    try:
                        failure_results_root = (
                            Path(cfg.output.results_dir)
                            / cfg.experiment_tag
                            / "failure"
                        )
                        failure_output_dir = base_analysis_dir / "failure"
                        failure_output_dir.mkdir(parents=True, exist_ok=True)

                        # 2.1 Agent Delay -> Query Latency
                        agent_delay_csv = (
                            failure_results_root / "2_1_agent_delay" / "summary.csv"
                        )
                        if agent_delay_csv.exists():
                            click.echo(
                                "  Generating failure experiment visualizations..."
                            )
                            generate_agent_delay_vs_query_latency_plot(
                                agent_delay_csv,
                                failure_output_dir / "agent_delay_vs_query_latency.png",
                            )
                            generate_agent_delay_comprehensive_plot(
                                agent_delay_csv,
                                failure_output_dir / "agent_delay_comprehensive.png",
                            )
                            latex_table_agent_delay_impact(
                                agent_delay_csv,
                                failure_output_dir / "agent_delay_impact.tex",
                            )

                        # 2.3 Agent Stress (RPS -> latency percentiles)
                        agent_stress_csv = (
                            failure_results_root / "2_3_agent_stress" / "summary.csv"
                        )
                        if agent_stress_csv.exists():
                            generate_agent_rps_vs_latency_plot(
                                agent_stress_csv,
                                failure_output_dir / "agent_rps_vs_latency.png",
                            )
                            generate_agent_stress_comprehensive_plot(
                                agent_stress_csv,
                                failure_output_dir / "agent_stress_comprehensive.png",
                            )
                            latex_table_agent_stress_test(
                                agent_stress_csv,
                                failure_output_dir / "agent_stress_test.tex",
                            )
                    except Exception as e:  # noqa: BLE001
                        click.echo(
                            f"  Warning: Failed to generate failure visualizations: {e}"
                        )
        except Exception as e:
            click.echo(f"  Warning: Visualization generation failed: {e}")


@cli.command("generate-visualizations")
@click.argument("analysis_dir", required=False, type=click.Path(exists=True))
@click.option("--config", type=click.Path(exists=True), help="Path to YAML/JSON config")
@click.option(
    "--output",
    "output_dir",
    type=click.Path(),
    default=None,
    help="Output directory for plots (defaults to analysis_dir)",
)
def generate_visualizations(
    analysis_dir: str | None, config: str | None, output_dir: str | None
):
    """Generate visualization plots from analysis CSV outputs.

    ANALYSIS_DIR can be provided as argument or will be determined from config.
    Generates:
    - Latency CDF plot (from benchmark analysis)
    - Policy scaling plot (from policy_scaling.csv)
    - Concurrency throughput plot (from concurrency_throughput.csv)
    - Concurrency latency plot (from concurrency_latency.csv)
    """
    cfg: Config = load_config_file(config)

    # Determine analysis directory
    if analysis_dir:
        analysis_path = Path(analysis_dir)
    elif cfg.output.analysis_dir is not None:
        # Try to find analysis directories for different experiments
        base_analysis_dir = Path(cfg.output.analysis_dir)
        # Check benchmark first (most common)
        benchmark_analysis = base_analysis_dir / "benchmark"
        if benchmark_analysis.exists():
            analysis_path = benchmark_analysis
        else:
            # Use base directory
            analysis_path = base_analysis_dir
    else:
        raise click.ClickException(
            "No analysis directory specified. Provide ANALYSIS_DIR as argument "
            "or set 'output.analysis_dir' in config file."
        )

    if not analysis_path.exists():
        raise click.ClickException(f"Analysis directory not found: {analysis_path}")

    # Determine output directory
    if output_dir:
        output_path = Path(output_dir)
    else:
        output_path = analysis_path

    click.echo(f"Generating visualizations from: {analysis_path}")
    click.echo(f"Output directory: {output_path}")

    results = generate_all_visualizations(analysis_path, output_path)

    if not any(results.values()):
        # Check if plotting libraries are available
        try:
            import matplotlib  # noqa: F401
            import seaborn  # noqa: F401
        except ImportError:
            click.echo(
                "Warning: matplotlib and/or seaborn not installed. "
                "Install with: pip install matplotlib seaborn"
            )
            return

        click.echo("No CSV files found to generate visualizations from.")
        click.echo("Expected files:")
        click.echo("  - baseline_latencies.csv, cedar_latencies.csv (for CDF)")
        click.echo("  - policy_scaling.csv (for policy scaling plot)")
        click.echo("  - concurrency_throughput.csv (for throughput plot)")
        click.echo("  - concurrency_latency.csv (for latency plot)")
        return

    click.echo("✓ Generated visualizations:")
    for name, path in results.items():
        if path:
            click.echo(f"  - {name}: {path}")
        else:
            click.echo(f"  - {name}: (not generated - CSV not found)")


@cli.command("collect-system-info")
@click.option(
    "--config", type=click.Path(exists=True), help="Path to YAML/JSON config (optional)"
)
@click.option(
    "--output-dir",
    type=click.Path(),
    default=None,
    help="Output directory (default: from config or experiments/results)",
)
def collect_system_info(config: str | None, output_dir: str | None):
    """Collect system information for experimental reproducibility.

    Collects comprehensive system information including:
    - Hardware (CPU, RAM, storage)
    - Software versions (OS, Docker, Python, MySQL)
    - Docker container status
    - Network port availability
    - System configuration

    Outputs both JSON and text formats to <output_dir>/system_info/
    """
    # Load config if provided to get results_dir
    if config:
        cfg: Config = load_config_file(config)
        if output_dir is None:
            output_dir = cfg.output.results_dir if cfg.output.results_dir else None

    script_dir = Path(__file__).parent
    collect_script = script_dir / "scripts" / "collect_system_info.sh"

    if not collect_script.exists():
        raise click.ClickException(
            f"System info script not found: {collect_script}\n"
            "Please ensure scripts/collect_system_info.sh exists."
        )

    if not collect_script.is_file():
        raise click.ClickException(
            f"System info script is not a file: {collect_script}"
        )

    click.echo("Collecting system information...")
    try:
        import subprocess

        # Build command with optional output directory
        cmd = ["bash", str(collect_script)]
        if output_dir:
            # Convert to absolute path
            output_path = Path(output_dir).resolve()
            cmd.append(str(output_path))
            click.echo(f"  Output directory: {output_path}/system_info/")

        result = subprocess.run(
            cmd, capture_output=True, text=True, cwd=str(script_dir.parent)
        )

        if result.returncode == 0:
            click.echo("✓ System information collected successfully!")
            # Show output paths
            for line in result.stdout.split("\n"):
                if "system_info_latest" in line or "JSON:" in line or "Text:" in line:
                    click.echo(f"  {line.strip()}")
        else:
            click.echo("✗ System info collection failed:", err=True)
            click.echo(result.stderr, err=True)
            raise click.ClickException("System info collection failed")
    except subprocess.CalledProcessError as e:
        raise click.ClickException(f"Failed to run system info script: {e}")
    except Exception as e:
        raise click.ClickException(f"Unexpected error: {e}")


@cli.command("full-experiment")
@click.argument("auth_spec", required=False, type=click.Path(exists=True))
@click.option("--config", type=click.Path(exists=True))
@click.option(
    "--output",
    default=None,
    type=click.Path(),
    help="Output workload dir (overrides config)",
)
@click.option(
    "--skip-policy-scaling", is_flag=True, help="Skip policy scaling experiment"
)
@click.option("--skip-concurrency", is_flag=True, help="Skip concurrency benchmark")
@click.option(
    "--skip-failure", is_flag=True, help="Skip all failure resilience experiments"
)
@click.option(
    "--failure-rps",
    default=None,
    type=int,
    help="RPS for mysql-under-stress experiment (default: first RPS from config)",
)
@click.option(
    "--skip-system-info", is_flag=True, help="Skip system information collection"
)
def full_experiment(
    auth_spec: str | None,
    config: str | None,
    output: str | None,
    skip_policy_scaling: bool,
    skip_concurrency: bool,
    skip_failure: bool,
    failure_rps: int | None,
    skip_system_info: bool,
):
    """Run all experiments: setup -> workload -> benchmark -> policy scaling -> concurrency -> failure tests -> analysis

    AUTH_SPEC can be provided as argument or via --config (auth_spec_path).

    This runs all available experiments:
    - System information collection (for reproducibility)
    - Basic benchmark (E1)
    - Overhead breakdown (E2)
    - Concurrency benchmark (E3)
    - Policy scaling (E4)
    - Analytic benchmark (E5)
    - Concurrency contention (E6)
    - Failure resilience (E7)
    - Semantic correctness (E8)
    - TPC-C macrobenchmarks (E9)
    - DDL operations (E10)
    - PostgreSQL parity (E11)

    Use --skip-* flags to exclude specific experiment types.
    """
    cfg: Config = load_config_file(config)

    # Determine auth_spec: CLI argument > config > error
    auth_spec_path = auth_spec or cfg.auth_spec_path
    if not auth_spec_path:
        raise click.ClickException(
            "Auth spec not specified. Provide AUTH_SPEC as argument or set 'auth_spec_path' in config file."
        )

    click.echo("=" * 80)
    click.echo("Running Full Experiment Suite")
    click.echo("=" * 80)

    # 0) Collect system information for reproducibility
    if not skip_system_info:
        click.echo("\n[0/8] Collecting system information...")
        try:
            script_dir = Path(__file__).parent
            collect_script = script_dir / "scripts" / "collect_system_info.sh"
            if collect_script.exists():
                import subprocess

                # Use results_dir from config
                results_base_dir = (
                    Path(cfg.output.results_dir) if cfg.output.results_dir else None
                )
                cmd = ["bash", str(collect_script)]
                if results_base_dir:
                    cmd.append(str(results_base_dir.resolve()))

                result = subprocess.run(
                    cmd, capture_output=True, text=True, cwd=str(script_dir.parent)
                )
                if result.returncode == 0:
                    click.echo("✓ System information collected")
                    # Extract output paths from script output
                    for line in result.stdout.split("\n"):
                        if "system_info_latest" in line:
                            click.echo(f"  {line.strip()}")
                else:
                    click.echo(
                        f"⚠ System info collection failed: {result.stderr}", err=True
                    )
            else:
                click.echo(
                    f"⚠ System info script not found: {collect_script}", err=True
                )
        except Exception as e:
            click.echo(f"⚠ Failed to collect system info (continuing): {e}", err=True)
    else:
        click.echo("\n[0/8] Skipping system information collection...")

    # 1) Setup both systems
    click.echo("\n[1/11] Setting up baseline database...")
    db_type = _detect_primary_db_type(cfg)
    try:
        setup_baseline.callback(
            auth_spec=auth_spec_path, config=config, db_type=db_type
        )
    except Exception as e:
        click.echo(f"✗ Setup baseline failed: {e}", err=True)
        raise

    click.echo("\n[2/11] Setting up Cedar agent...")
    try:
        setup_cedar.callback(auth_spec=auth_spec_path, config=config, db_type=db_type)
    except Exception as e:
        click.echo(f"✗ Setup Cedar failed: {e}", err=True)
        raise

    # 2) Generate workload (use "benchmark" as experiment name)
    click.echo("\n[3/11] Generating workload...")
    try:
        db_type = _detect_primary_db_type(cfg)

        generate_workload.callback(
            auth_spec=auth_spec_path,
            config=config,
            queries_per_combo=None,
            seed=None,
            db_type=db_type,
            output=output,
            experiment="benchmark",
        )
    except Exception as e:
        click.echo(f"✗ Workload generation failed: {e}", err=True)
        raise

    # 3) E1: Run basic benchmark
    click.echo("\n[4/11] Running E1: Query-by-query overhead...")
    try:
        run_benchmark.callback(
            workload_dir=output,
            config=config,
            iterations=None,
            concurrency=None,
            experiment="benchmark",
            warmup_iterations=None,
            warmup_seconds=None,
            n_runs=None,
        )
    except Exception as e:
        click.echo(f"✗ E1 benchmark failed: {e}", err=True)
        raise

    # E2: Overhead breakdown
    click.echo("\n[5/11] Running E2: Overhead breakdown...")
    try:
        overhead_breakdown.callback(config, True)  # simulate=True by default for suite
    except Exception as e:
        click.echo(f"⚠ E2 breakdown failed (continuing): {e}", err=True)

    # 4) E4: Policy scaling experiment
    if not skip_policy_scaling:
        click.echo("\n[6/11] Running E4: Policy scaling experiment...")
        try:
            policy_scaling.callback(
                config,
                None,
                None,
                None,
                None,
                None,
                None,  # type: ignore
            )
        except Exception as e:
            click.echo(f"⚠ E4 policy scaling failed (continuing): {e}", err=True)
    else:
        click.echo("\n[6/11] Skipping E4 policy scaling...")

    # 5) E3: Concurrency benchmark
    if not skip_concurrency:
        click.echo("\n[7/11] Running E3: Concurrency scaling (sysbench)...")
        try:
            concurrency_benchmark.callback(
                config,
                None,
                None,
                "both",
                None,
                None,
                None,
                None,
                None,
                False,  # type: ignore
            )
        except Exception as e:
            click.echo(f"⚠ E3 concurrency benchmark failed (continuing): {e}", err=True)
    else:
        click.echo("\n[7/11] Skipping E3 concurrency benchmark...")

    # E5: Analytic benchmark
    click.echo("\n[8/11] Running E5: Analytic / Join-heavy workload...")
    try:
        analytic_benchmark.callback(config, None, None, "analytic")
    except Exception as e:
        click.echo(f"⚠ E5 analytic benchmark failed (continuing): {e}", err=True)

    # E6: Concurrency contention
    click.echo("\n[9/11] Running E6: Multi-user concurrency contention...")
    try:
        concurrency_benchmark.callback(
            config, "1,4,8,16,32", None, "both", None, None, None, None, None, False
        )
    except Exception as e:
        click.echo(f"⚠ E6 contention benchmark failed (continuing): {e}", err=True)

    # 6) E7: Failure resilience experiments
    if not skip_failure:
        click.echo("\n[10/11] Running E7: Failure resilience experiments...")

        click.echo("  - E7.1: Agent delay benchmark...")
        try:
            agent_delay_benchmark.callback(config)  # type: ignore
        except Exception as e:
            click.echo(f"    ⚠ E7.1 delay benchmark failed (continuing): {e}", err=True)

        click.echo("  - E7.2: Agent unavailability test...")
        try:
            agent_unavailability_test.callback(config, None)  # type: ignore
        except Exception as e:
            click.echo(
                f"    ⚠ E7.2 unavailability test failed (continuing): {e}", err=True
            )

        click.echo("  - E7.3: Agent stress test...")
        try:
            agent_stress_test.callback(config)  # type: ignore
        except Exception as e:
            click.echo(f"    ⚠ E7.3 stress test failed (continuing): {e}", err=True)

        click.echo("  - E7.4: MySQL under stress...")
        try:
            # Use first RPS from config if not provided
            rps_to_use = failure_rps
            if rps_to_use is None:
                if cfg.failure_tests and cfg.failure_tests.agent_stress:
                    rps_list = cfg.failure_tests.agent_stress.rps_list
                    rps_to_use = rps_list[0] if rps_list else 100
                else:
                    rps_to_use = 100
            mysql_under_stress.callback(config, rps_to_use)  # type: ignore
        except Exception as e:
            click.echo(
                f"    ⚠ E7.4 MySQL under stress failed (continuing): {e}", err=True
            )
    else:
        click.echo("\n[10/11] Skipping E7 failure resilience...")

    # E8: Semantics
    click.echo("\n[11/11] Running E8: Semantic correctness testing...")
    try:
        semantics_test.callback(
            config, output, "agent_unavailable,network_timeout,malformed_response", None
        )
    except Exception as e:
        click.echo(f"⚠ E8 semantics failed (continuing): {e}", err=True)

    # E9: TPC-C
    click.echo("\n[Extra] Running E9: TPC-C macrobenchmarks...")
    try:
        tpcc_sysbench.callback(config, None, None, None, None, None, None)
    except Exception as e:
        click.echo(f"⚠ E9 TPC-C failed (continuing): {e}", err=True)

    # E10: DDL
    click.echo("\n[Extra] Running E10: DDL operations testing...")
    try:
        ddl_test.callback(config, "comprehensive", "both", None)
    except Exception as e:
        click.echo(f"⚠ E10 DDL failed (continuing): {e}", err=True)

    # E11: PostgreSQL
    click.echo("\n[Extra] Running E11: PostgreSQL parity comparison...")
    try:
        pgbench_compare.callback(config, None, None, None, None, None)
    except Exception as e:
        click.echo(f"⚠ E11 pgbench failed (continuing): {e}", err=True)

    # 7) Analyze all results
    click.echo("\n[Analysis] Analyzing all results and generating visualizations...")
    try:
        analyze_results.callback(
            results_dir=None,
            config=config,
            fmt="json",
            outputs_dir=None,
            visualizations=True,
        )
    except Exception as e:
        click.echo(f"⚠ Analysis failed: {e}", err=True)

    click.echo("\n" + "=" * 80)
    click.echo("✓ Full experiment suite complete!")
    click.echo("=" * 80)


@cli.command("policy-scaling")
@click.option(
    "--config", type=click.Path(exists=True), help="Path to YAML/JSON config file."
)
@click.option(
    "--counts", default=None, help="Comma-separated list of policy counts to test."
)
@click.option(
    "--iterations",
    default=None,
    type=int,
    help="Total iterations to execute per count.",
)
@click.option(
    "--seed", default=None, type=int, help="Random seed for policy generation."
)
@click.option(
    "--reset/--no-reset",
    default=None,
    help="Reset Cedar agent policies before starting.",
)
@click.option(
    "--match-ratio",
    default=None,
    type=float,
    help="Ratio of matching policies (0.0-1.0).",
)
@click.option(
    "--workload-dir",
    default=None,
    type=click.Path(exists=True),
    help="Path to pre-generated workload directory.",
)
@click.option(
    "--warmup-iterations",
    default=None,
    type=int,
    help="Number of iterations to discard from start.",
)
@click.option(
    "--warmup-seconds",
    default=None,
    type=int,
    help="Seconds to run warmup before measurement.",
)
@click.option(
    "--n-runs",
    default=None,
    type=int,
    help="Number of independent runs per policy count.",
)
def policy_scaling(
    config: str | None,
    counts: str | None,
    iterations: int | None,
    seed: int | None,
    reset: bool | None,
    match_ratio: float | None,
    workload_dir: str | None,
    warmup_iterations: int | None,
    warmup_seconds: int | None,
    n_runs: int | None,
):
    """Run benchmark with different policy counts to measure scaling."""
    cfg: Config = load_config_file(config)

    exp_workload_dir, exp_results_dir, _ = _get_experiment_paths(cfg, "policy_scaling")

    # Use values from CLI > config > default
    policy_counts_str = counts or ",".join(map(str, cfg.scaling.policy_counts))
    policy_counts = [int(c.strip()) for c in policy_counts_str.split(",")]

    iterations_to_use = iterations if iterations is not None else cfg.scaling.iterations
    seed_to_use = seed if seed is not None else (cfg.scaling.seed or cfg.workload.seed)
    reset_to_use = reset if reset is not None else cfg.scaling.reset
    match_ratio_to_use = (
        match_ratio if match_ratio is not None else cfg.scaling.match_ratio
    )

    warmup_iterations_to_use = (
        warmup_iterations
        if warmup_iterations is not None
        else cfg.scaling.warmup_iterations
    )
    warmup_seconds_to_use = (
        warmup_seconds if warmup_seconds is not None else cfg.scaling.warmup_seconds
    )
    n_runs_to_use = n_runs if n_runs is not None else cfg.scaling.n_runs

    auth_spec_path = cfg.auth_spec_path
    if not auth_spec_path or not Path(auth_spec_path).exists():
        raise click.ClickException(f"Auth spec file not found: {auth_spec_path}")

    with open(auth_spec_path) as f:
        spec = json.load(f)

    base_url = cfg.cedar_agent.url.rstrip("/") + "/v1"

    # Determine workload directory: CLI > generated experiment path
    workload_path = (
        Path(workload_dir) / "workload.json"
        if workload_dir
        else exp_workload_dir / "workload.json"
    )

    # Check if workload exists and is relevant to this experiment
    needs_regeneration = True
    if workload_path and workload_path.exists():
        try:
            existing_workload = Workload.load(workload_path)
            existing_meta = existing_workload.metadata

            # Check if workload matches current experiment parameters
            auth_spec_resolved = str(Path(auth_spec_path).resolve())
            existing_auth_spec = existing_meta.get("auth_spec_path")
            existing_seed = existing_meta.get("seed")
            existing_qpc = existing_meta.get("queries_per_combination")
            existing_action_dist = existing_meta.get("action_distribution")

            # If any critical metadata is missing, regenerate
            if (
                existing_auth_spec is None
                or existing_seed is None
                or existing_qpc is None
                or existing_action_dist is None
            ):
                click.echo(
                    "Existing workload missing metadata fields. "
                    "Regenerating workload..."
                )
            elif (
                existing_auth_spec == auth_spec_resolved
                and existing_seed == seed_to_use
                and existing_qpc == cfg.workload.queries_per_combination
                and existing_action_dist == cfg.workload.action_distribution
            ):
                needs_regeneration = False
                click.echo(f"✓ Using existing workload: {workload_path}")
            else:
                click.echo(
                    "Existing workload parameters don't match. Regenerating workload..."
                )
                if existing_auth_spec != auth_spec_resolved:
                    click.echo(
                        f"  - Auth spec path mismatch: "
                        f"{existing_auth_spec} != {auth_spec_resolved}"
                    )
                if existing_seed != seed_to_use:
                    click.echo(f"  - Seed mismatch: {existing_seed} != {seed_to_use}")
                if existing_qpc != cfg.workload.queries_per_combination:
                    click.echo(
                        f"  - Queries per combination mismatch: "
                        f"{existing_qpc} != "
                        f"{cfg.workload.queries_per_combination}"
                    )
                if existing_action_dist != cfg.workload.action_distribution:
                    click.echo(
                        f"  - Action distribution mismatch: "
                        f"{existing_action_dist} != "
                        f"{cfg.workload.action_distribution}"
                    )
        except Exception as e:
            click.echo(
                f"Warning: Could not validate existing workload ({e}). Regenerating..."
            )

    if needs_regeneration:
        # Generate workload if it doesn't exist or doesn't match
        if not workload_path:
            raise click.ClickException(
                "No workload directory specified. Provide --workload-dir "
                "or set 'scaling.workload_dir' or 'output.workload_dir' "
                "in config."
            )
        click.echo("Generating workload...")
        db_type = _detect_primary_db_type(cfg)

        generate_workload.callback(
            auth_spec=auth_spec_path,
            config=config,
            queries_per_combo=None,
            seed=seed_to_use,
            db_type=db_type,
            output=str(workload_path.parent),
            experiment="policy_scaling",
        )
        workload_path = workload_path.parent / "workload.json"

    if reset_to_use:
        click.echo("Resetting Cedar agent with empty policy set...")
        put_policies(base_url, [])

    # Determine namespace for policies
    db_type = _detect_primary_db_type(cfg)
    namespace = "MySQL" if db_type.lower() == "mysql" else "PostgreSQL"

    # Ensure Cedar schema and attributes are set up correctly
    # detailed setup is needed because policies rely on attributes
    click.echo("Ensuring Cedar schema and attributes are configured...")
    setup_cedar_schema(base_url, spec, namespace)
    assign_user_attributes(base_url, spec, namespace)
    assign_database_attributes(base_url, spec, namespace)
    assign_resource_attributes(base_url, spec, namespace)

    for count in tqdm(
        policy_counts, desc="Policy Scaling", unit="counts", position=0, leave=True
    ):
        tqdm.write(f"--- Running benchmark for {count} policies ---")

        # 1. Generate and set policies
        policies = build_policy_set(
            spec,
            count,
            match_ratio=match_ratio_to_use,
            seed=seed_to_use,
            namespace=namespace,
        )
        tqdm.write(
            f"Generated {len(policies)} policies with seed {seed_to_use} "
            f"(match_ratio={match_ratio_to_use}). Setting policies..."
        )
        if not put_policies(base_url, policies):
            raise click.ClickException(f"Failed to set policies for count {count}")

        # 2. Run the benchmark
        tqdm.write("\tRunning benchmark...")
        # Create a specific output directory for this run
        results_dir = exp_results_dir / f"policies_{count}"
        results_dir.mkdir(parents=True, exist_ok=True)

        # Use the workload directory that contains the workload.json
        workload_dir_to_use = workload_path.parent
        # Pass "policy_scaling" as experiment name so results go to correct base directory
        run_benchmark.callback(
            workload_dir=str(workload_dir_to_use),
            config=config,
            iterations=iterations_to_use,
            concurrency=None,
            experiment="policy_scaling",
            warmup_iterations=warmup_iterations_to_use,
            warmup_seconds=warmup_seconds_to_use,
            n_runs=n_runs_to_use,
        )

        # Move results from base policy_scaling directory to the specific count subdirectory
        base_results_path = exp_results_dir / "results.json"
        results_path = results_dir / "results.json"
        if base_results_path.exists():
            shutil.move(str(base_results_path), str(results_path))

        # Add metadata to results
        if results_path.exists():
            with results_path.open("r+") as f:
                results_data = json.load(f)
                # Initialize metadata if it doesn't exist
                if "metadata" not in results_data:
                    results_data["metadata"] = {}
                results_data["metadata"]["policy_count"] = count
                results_data["metadata"]["match_ratio"] = match_ratio_to_use
                f.seek(0)
                json.dump(results_data, f, indent=2)
                f.truncate()
        tqdm.write(f"✓ Completed benchmark for {count} policies.")


@cli.command("concurrency-benchmark")
@click.option("--config", type=click.Path(exists=True))
@click.option("--threads", default=None, help="Comma-separated list of thread counts.")
@click.option("--duration", default=None, type=int)
@click.option(
    "--target", default="both", type=click.Choice(["baseline", "cedar", "both"])
)
@click.option("--sysbench-bin", default=None)
@click.option("--oltp", default=None)
@click.option("--db", default=None)
@click.option("--tables", default=None, type=int)
@click.option("--table-size", default=None, type=int)
@click.option("--docker", is_flag=True, default=False)
@click.option(
    "--n-runs",
    default=None,
    type=int,
    help="Number of independent runs per thread count (default: from config)",
)
def concurrency_benchmark(
    config,
    threads,
    duration,
    target,
    sysbench_bin,
    oltp,
    db,
    tables,
    table_size,
    docker,
    n_runs,
):
    """Run concurrency benchmark using sysbench.

    Runs each thread configuration n_runs times for statistical rigor.
    Results are aggregated with confidence intervals.
    """
    cfg: Config = load_config_file(config)

    # Get sysbench params from CLI > config
    threads_list_str = threads or ",".join(map(str, cfg.sysbench.threads))
    threads_list = [int(t.strip()) for t in threads_list_str.split(",")]
    sb_duration = duration or cfg.sysbench.duration
    sb_bin = sysbench_bin or cfg.sysbench.binary
    sb_oltp = oltp or cfg.sysbench.oltp
    sb_db = db or cfg.sysbench.db_name
    sb_tables = tables or cfg.sysbench.tables
    sb_table_size = table_size or cfg.sysbench.table_size
    sb_docker = docker or cfg.sysbench.docker
    sb_n_runs = n_runs or cfg.sysbench.n_runs

    click.echo(f"Statistical rigor: {sb_n_runs} runs per configuration")

    targets = []
    if target == "both":
        targets.extend(["baseline", "cedar"])
    else:
        targets.append(target)

    for current_target in targets:
        click.echo(f"--- Running sysbench on {current_target} ---")
        db_config = cfg.databases[current_target]
        base_url = cfg.cedar_agent.url
        # Normalize URL to ensure it ends with /v1
        base_url = base_url.rstrip("/")
        if not base_url.endswith("/v1"):
            base_url = f"{base_url}/v1"

        # Initialize stats_before to avoid UnboundLocalError
        stats_before = {}

        if db_config.type != "mysql":
            raise click.ClickException(
                f"Sysbench concurrency benchmark requires MySQL for '{current_target}', "
                f"but config has type '{db_config.type}'."
            )

        # Common sysbench command parts
        base_cmd = [
            sb_bin,
            sb_oltp,
            f"--mysql-host={db_config.host}",
            f"--mysql-port={db_config.port}",
            f"--mysql-user={db_config.user}",
            f"--mysql-password={db_config.password}",
            f"--mysql-db={sb_db}",
            f"--tables={sb_tables}",
            f"--table-size={sb_table_size}",
        ]

        _, exp_results_dir, _ = _get_experiment_paths(
            cfg, f"concurrency/{current_target}"
        )
        results_root_dir = exp_results_dir
        results_root_dir.mkdir(parents=True, exist_ok=True)

        # Create DB if it doesn't exist
        try:
            import mysql.connector

            conn = mysql.connector.connect(
                host=db_config.host,
                port=db_config.port,
                user=db_config.user,
                password=db_config.password,
            )
            cursor = conn.cursor()
            cursor.execute(f"CREATE DATABASE IF NOT EXISTS {sb_db}")
            cursor.close()
            conn.close()
        except ImportError:
            click.echo(
                "Warning: mysql-connector-python not found. Cannot create database automatically.",
                err=True,
            )
        except Exception as e:
            raise click.ClickException(f"Failed to create sysbench database: {e}")

        # Inject Cedar policy for benchmark user if running against Cedar
        if current_target == "cedar":
            _apply_and_check_mysql_cedar_sysvars(
                cfg=cfg,
                db_config=db_config,
                admin_user=db_config.user,
                admin_password=db_config.password,
                label="sysbench/concurrency preflight",
            )

            from framework.benchmark_user_setup import BENCHMARK_USER

            # Ensure entity exists
            namespace = "MySQL"  # Enforces MySQL anyway
            if not entity_exists(base_url, "User", BENCHMARK_USER, namespace):
                click.echo(f"Creating User entity for {BENCHMARK_USER}...")
                create_entity(base_url, "User", BENCHMARK_USER, namespace)

            # Inject permit-all policy for sysbench
            # Policy ID: mysql_sysbench_allow_all
            policy_id = "mysql_sysbench_allow_all"
            policy_content = f"""permit(
                principal == {namespace}::User::"{BENCHMARK_USER}",
                action,
                resource
            );"""

            click.echo("Injecting Cedar policy for sysbench user...")
            try:
                # Create/Update policy
                resp = requests.put(
                    f"{base_url}/policies/{policy_id}",
                    json={"id": policy_id, "content": policy_content},
                    timeout=5,
                )
                if resp.status_code not in (200, 201, 204):
                    click.echo(
                        f"Warning: Failed to inject policy: {resp.text}", err=True
                    )
            except Exception as e:
                click.echo(f"Warning: Failed to inject policy: {e}", err=True)

            # Capture stats before run
            stats_before = get_cedar_agent_stats(base_url)

        click.echo("Preparing database and tables...")

        prepare_cmd = base_cmd + ["prepare"]
        try:
            run_sysbench_command(
                prepare_cmd,
                sb_docker,
                log_dir=results_root_dir / "raw" / "prepare",
                label=f"concurrency/{current_target}/prepare",
                timeout_s=600,
            )
        except RuntimeError as e:
            if "already exists" in str(e):
                click.echo("Tables already exist, skipping prepare.")
            else:
                raise click.ClickException(f"Sysbench prepare failed: {e}")

        for t_count in tqdm(
            threads_list,
            desc=f"Concurrency: {current_target}",
            unit="threads",
            position=0,
            leave=True,
        ):
            tqdm.write(
                f"Running with {t_count} threads for {sb_duration}s ({sb_n_runs} runs)..."
            )

            # Use benchmark user for Cedar runs to ensure authorization overhead is measured
            # Root user (in base_cmd) might bypass authorization plugins
            base_cmd[2:]  # validation: skip binary and command

            # Filter out user/password/host/port from base options if we are overriding them
            # Actually simpler to just build a new command for the run phase

            if current_target == "cedar":
                # Import credentials
                from framework.benchmark_user_setup import (
                    BENCHMARK_PASSWORD,
                    BENCHMARK_USER,
                )

                # Reconstruct command with benchmark user
                run_user_cmd = [
                    sb_bin,
                    sb_oltp,
                    f"--mysql-host={db_config.host}",
                    f"--mysql-port={db_config.port}",
                    f"--mysql-user={BENCHMARK_USER}",
                    f"--mysql-password={BENCHMARK_PASSWORD}",
                    f"--mysql-db={sb_db}",
                    f"--tables={sb_tables}",
                    f"--table-size={sb_table_size}",
                    f"--threads={t_count}",
                    f"--time={sb_duration}",
                    "run",
                ]
                run_cmd = run_user_cmd
            else:
                run_cmd = base_cmd + [
                    f"--threads={t_count}",
                    f"--time={sb_duration}",
                    "run",
                ]

            # Multi-run: collect results from all runs
            run_results = []

            for run_idx in range(sb_n_runs):
                tqdm.write(f"  Run {run_idx + 1}/{sb_n_runs}...")
                try:
                    run_log_dir = (
                        results_root_dir
                        / "raw"
                        / f"threads_{t_count}"
                        / f"run_{run_idx + 1}"
                    )
                    output = run_sysbench_command(
                        run_cmd,
                        sb_docker,
                        log_dir=run_log_dir,
                        label=f"concurrency/{current_target}/threads={t_count}/run={run_idx + 1}",
                        timeout_s=sb_duration + 60,
                    )
                    parsed_metrics = parse_sysbench_output(output)

                    if parsed_metrics:
                        run_results.append(
                            {
                                "run": run_idx + 1,
                                "qps": parsed_metrics.qps,
                                "tps": parsed_metrics.tps,
                                "lat_avg_ms": parsed_metrics.lat_avg_ms,
                                "lat_p95_ms": parsed_metrics.lat_p95_ms,
                                "lat_p99_ms": parsed_metrics.lat_p99_ms,
                                "raw": {
                                    "dir": str(run_log_dir),
                                    "stdout": str(
                                        run_log_dir / "attempt_1" / "stdout.log"
                                    ),
                                    "stderr": str(
                                        run_log_dir / "attempt_1" / "stderr.log"
                                    ),
                                    "meta": str(
                                        run_log_dir / "attempt_1" / "meta.json"
                                    ),
                                },
                            }
                        )
                        tqdm.write(
                            f"    QPS: {parsed_metrics.qps:.2f}, p95: {parsed_metrics.lat_p95_ms:.2f}ms"
                        )
                    else:
                        tqdm.write(
                            f"    Warning: Could not parse run {run_idx + 1} output"
                        )

                except RuntimeError as e:
                    tqdm.write(f"    Error on run {run_idx + 1}: {e}")

            # Save all runs and compute aggregate statistics
            thread_dir = results_root_dir / f"threads_{t_count}"
            thread_dir.mkdir(parents=True, exist_ok=True)

            if run_results:
                # Compute aggregate stats with CIs
                import statistics

                qps_values = [r["qps"] for r in run_results]
                lat_p95_values = [r["lat_p95_ms"] for r in run_results]

                from framework.stats import bootstrap_ci_median

                qps_median = statistics.median(qps_values) if qps_values else 0
                lat_p95_median = (
                    statistics.median(lat_p95_values) if lat_p95_values else 0
                )
                lat_p99_values = [
                    r["lat_p99_ms"]
                    for r in run_results
                    if r.get("lat_p99_ms") is not None
                ]
                lat_p99_median = (
                    statistics.median(lat_p99_values) if lat_p99_values else 0
                )

                qps_ci = bootstrap_ci_median(
                    qps_values,
                    n_bootstrap=cfg.benchmark.n_bootstrap,
                    confidence_level=cfg.benchmark.confidence_level,
                )
                lat_p95_ci = bootstrap_ci_median(
                    lat_p95_values,
                    n_bootstrap=cfg.benchmark.n_bootstrap,
                    confidence_level=cfg.benchmark.confidence_level,
                )
                lat_p99_ci = bootstrap_ci_median(
                    lat_p99_values,
                    n_bootstrap=cfg.benchmark.n_bootstrap,
                    confidence_level=cfg.benchmark.confidence_level,
                )

                aggregate = {
                    "n_runs": len(run_results),
                    # Backwards-compatible "canonical" keys used by analysis scripts
                    "qps": qps_median,
                    "lat_p95_ms": lat_p95_median,
                    "lat_p99_ms": lat_p99_median,
                    "lat_avg_ms": statistics.median(
                        [r["lat_avg_ms"] for r in run_results]
                    )
                    if run_results
                    else 0,
                    # More explicit aggregate fields
                    "qps_median": qps_median,
                    "qps_mean": statistics.mean(qps_values) if qps_values else 0,
                    "qps_std": statistics.stdev(qps_values)
                    if len(qps_values) > 1
                    else 0,
                    "qps_ci_lower": qps_ci.lower,
                    "qps_ci_upper": qps_ci.upper,
                    "lat_p95_median": lat_p95_median,
                    "lat_p95_mean": statistics.mean(lat_p95_values)
                    if lat_p95_values
                    else 0,
                    "lat_p95_ci_lower": lat_p95_ci.lower,
                    "lat_p95_ci_upper": lat_p95_ci.upper,
                    "lat_p99_median": lat_p99_median,
                    "lat_p99_ci_lower": lat_p99_ci.lower,
                    "lat_p99_ci_upper": lat_p99_ci.upper,
                    "confidence_level": cfg.benchmark.confidence_level,
                    "n_bootstrap": cfg.benchmark.n_bootstrap,
                    "runs": run_results,
                }

                # Save results
                (thread_dir / "summary.json").write_text(
                    json.dumps(aggregate, indent=2)
                )
                tqdm.write(
                    f"  Aggregate: QPS={aggregate['qps_median']:.2f}±{aggregate['qps_std']:.2f}, p95={aggregate['lat_p95_median']:.2f}ms"
                )
            else:
                tqdm.write("Warning: No successful runs for this thread count.")

        # Verify authorization was actually invoked for Cedar
        if current_target == "cedar":
            stats_after = get_cedar_agent_stats(base_url)
            _print_cedar_agent_stats(cfg=cfg, label="sysbench/concurrency end")
            auth_verification_result = verify_auth_invocations(
                stats_before,
                stats_after,
                expected_min=100,  # Expect at least some auth requests
                verbose=True,
            )

            if auth_verification_result.get("auth_requests", 0) > 0:
                click.echo(
                    f"✓ Authorization verification: {auth_verification_result['auth_requests']} auth requests during benchmark"
                )
            else:
                click.echo(
                    "⚠ WARNING: No authorization requests detected! Overhead measurement may be invalid.",
                    err=True,
                )

            # Save verification result
            verification_file = results_root_dir / "auth_verification.json"
            with open(verification_file, "w") as f:
                json.dump(auth_verification_result, f, indent=2)

        # Cleanup
        click.echo("Cleaning up tables...")
        cleanup_cmd = base_cmd + ["cleanup"]
        try:
            run_sysbench_command(
                cleanup_cmd,
                sb_docker,
                log_dir=results_root_dir / "raw" / "cleanup",
                label=f"concurrency/{current_target}/cleanup",
                timeout_s=600,
            )
        except RuntimeError as e:
            click.echo(f"Sysbench cleanup failed: {e}", err=True)

    click.echo(f"✓ Completed concurrency benchmark. Results saved to {exp_results_dir}")


@cli.command("analytic-benchmark")
@click.option("--config", type=click.Path(exists=True), help="Path to YAML/JSON config")
@click.option("--iterations", type=int, help="Number of iterations")
@click.option("--concurrency", type=int, help="Number of concurrent clients")
@click.option("--experiment", default="analytic", help="Experiment name")
@click.option(
    "--warmup-iterations",
    default=None,
    type=int,
    help="Number of iterations to discard from start",
)
@click.option(
    "--warmup-seconds",
    default=None,
    type=int,
    help="Seconds to run warmup before measurement",
)
@click.option("--n-runs", default=None, type=int, help="Number of independent runs")
def analytic_benchmark(
    config,
    iterations,
    concurrency,
    experiment,
    warmup_iterations,
    warmup_seconds,
    n_runs,
):
    """Run analytic macrobenchmark (E5) with complex queries."""
    cfg = load_config_file(config)
    import random

    # 1. Setup paths
    tag = cfg.experiment_tag
    _, results_dir, analysis_dir = _get_experiment_paths(cfg, experiment)
    results_dir.mkdir(parents=True, exist_ok=True)

    # 2. Generate analytic workload
    click.echo(f"Generating analytic workload for experiment: {experiment}...")
    db_type = _detect_primary_db_type(cfg)
    auth_spec_path = cfg.auth_spec_path

    from framework.query_generator import get_query_generator
    from framework.workload_generator import Query

    # Custom analytic workload generation
    with open(auth_spec_path) as f:
        auth_spec = json.load(f)

    qgen = get_query_generator(cfg.workload.seed, auth_spec=auth_spec, db_type=db_type)

    # Find tables for joins
    tables = [
        r["name"] for r in auth_spec.get("resources", []) if r.get("type") == "Table"
    ]
    users = [u["username"] for u in auth_spec.get("users", [])]

    queries = []
    iters = iterations if iterations is not None else cfg.benchmark.iterations

    # 2. Generate analytic workload
    click.echo(f"Generating analytic workload for experiment: {experiment}...")
    db_type = _detect_primary_db_type(cfg)
    auth_spec_path = cfg.auth_spec_path

    from framework.query_generator import get_query_generator

    # Custom analytic workload generation
    with open(auth_spec_path) as f:
        auth_spec = json.load(f)

    qgen = get_query_generator(cfg.workload.seed, auth_spec=auth_spec, db_type=db_type)

    # Find tables for joins
    tables = [
        r["name"] for r in auth_spec.get("resources", []) if r.get("type") == "Table"
    ]
    users = [u["username"] for u in auth_spec.get("users", [])]

    for i in range(iters):
        user = random.choice(users)
        table = random.choice(tables)

        # Pick 1-2 other tables for joins
        other_tables = [t for t in tables if t != table]
        joins = random.sample(
            other_tables, min(len(other_tables), random.randint(1, 2))
        )

        # Pick a better group by column if available
        group_by_col = "id"
        table_schema = qgen._get_table_schema(table)
        if table_schema:
            potential_cols = [
                c["name"]
                for c in table_schema.get("columns", [])
                if c["name"].lower()
                in ["department", "classification", "type", "status", "category"]
            ]
            if potential_cols:
                group_by_col = potential_cols[0]

        sql = qgen.generate_analytic_query(
            table,
            with_joins=joins,
            group_by=group_by_col,
            aggregate="COUNT(*)",
            limit=10,
        )

        queries.append(
            Query(
                id=i,
                user=user,
                action="SELECT",
                table=table,
                sql=sql,
                category="SELECT_ANALYTIC",
            )
        )

    from framework.workload_generator import Workload

    workload = Workload(
        queries=queries,
        metadata={
            "type": "analytic",
            "seed": cfg.workload.seed,
            "total_queries": len(queries),
        },
    )

    workload_path = Path(cfg.output.workload_dir) / tag / experiment / "workload.json"
    workload_path.parent.mkdir(parents=True, exist_ok=True)
    workload.save(workload_path)

    # 3. Run benchmark
    click.echo(f"Running analytic benchmark with {concurrency or 1} concurrency...")
    run_benchmark.callback(
        workload_dir=str(workload_path.parent),
        config=config,
        iterations=iters,
        concurrency=concurrency,
        experiment=experiment,
        warmup_iterations=warmup_iterations,
        warmup_seconds=warmup_seconds,
        n_runs=n_runs,
    )

    # 4. Analyze results
    click.echo("Analyzing analytic results...")
    analyze_results.callback(
        results_dir=str(results_dir),
        config=config,
        fmt="json",
        outputs_dir=str(analysis_dir),
        visualizations=True,
        include_extra=True,
    )

    click.echo(f"✓ Analytic benchmark complete. Results in {analysis_dir}")


# =============================================================================
# Multi-Run Experiments (USENIX-grade evaluation)
# =============================================================================


@cli.group("multi-run")
def multi_run():
    """Commands for multi-run statistical experiments (USENIX-grade)."""
    pass


@multi_run.command("benchmark")
@click.option("--config", type=click.Path(exists=True), help="Path to YAML/JSON config")
@click.option(
    "--ordering",
    type=click.Choice(["abba", "randomized", "baseline_first", "cedar_first"]),
    default="abba",
    help="Run ordering strategy",
)
@click.option(
    "--rest-seconds", default=5.0, type=float, help="Rest period between runs (seconds)"
)
@click.option(
    "--cedar-cache",
    type=click.Choice(["config", "on", "off"]),
    default="config",
    show_default=True,
    help="Override cedar_authorization_cache_enabled for Cedar runs only",
)
@click.option("--seed", default=None, type=int, help="Random seed for reproducibility")
@click.option("--workload-dir", default=None, help="Workload directory")
@click.option("--output-dir", default=None, help="Output directory for results")
def multi_run_benchmark(
    config, ordering, rest_seconds, cedar_cache, seed, workload_dir, output_dir
):
    """Run multi-run benchmark experiment for statistical rigor."""
    from framework.config import load_config_file
    from framework.multi_run import MultiRunOrchestrator

    cfg = load_config_file(config)

    # Determine paths
    exp_workload_dir, exp_results_dir, _ = _get_experiment_paths(cfg, "benchmark")

    wl_dir = Path(workload_dir) if workload_dir else exp_workload_dir
    out_dir = Path(output_dir) if output_dir else exp_results_dir

    # Load workload
    workload_path = wl_dir / "workload.json"
    if not workload_path.exists():
        raise click.ClickException(
            f"Workload not found at {workload_path}. Run 'generate-workload' first."
        )

    workload = Workload.load(workload_path)

    click.echo("Starting multi-run benchmark experiment:")
    click.echo(f"  - ordering: {ordering}")
    click.echo(f"  - rest between runs: {rest_seconds}s")
    click.echo(f"  - workload: {workload_path}")
    click.echo(f"  - output: {out_dir}")

    # Define single run function
    def run_single(system: str, run_index: int, output_path: Path) -> dict[str, Any]:
        """Run a single benchmark for one system."""
        import time

        output_path.mkdir(parents=True, exist_ok=True)

        # Capture validity evidence (auth invocation + config/caching signals)
        evidence: dict[str, Any] = {
            "system": system,
            "run_index": run_index,
            "captured_at_unix_s": time.time(),
            "cedar_cache_override": cedar_cache,
        }

        # MySQL snapshots (baseline + cedar)
        try:
            import mysql.connector

            from framework.mysql_introspection import (
                capture_cedar_plugin_status,
                capture_validity_snapshot,
                diff_counters,
                ensure_mysql_cedar_plugin_sysvars,
                reset_cedar_plugin_stats,
            )

            db_cfg = cfg.databases.get(system)
            if db_cfg and db_cfg.type == "mysql":
                # Use retry here too: containers can briefly flap between pairs.
                admin_conn = _connect_with_retry(
                    host=db_cfg.host,
                    port=db_cfg.port,
                    user=db_cfg.user,
                    password=db_cfg.password,
                    max_retries=6,
                    initial_delay=0.5,
                )
                try:
                    if system == "cedar":
                        plugins_cfg = _resolve_mysql_plugin_cfg_for_runtime(
                            cfg, _find_mysql_container(db_cfg.port)
                        )
                        auth_cfg = dict(plugins_cfg.get("cedar_authorization") or {})
                        if cedar_cache == "off":
                            auth_cfg["cache_enabled"] = False
                        elif cedar_cache == "on":
                            auth_cfg["cache_enabled"] = True
                        plugins_cfg["cedar_authorization"] = auth_cfg

                        # Apply Cedar plugin sysvars FIRST. If snapshots fail (e.g. MySQL
                        # drops the connection with error 2013), we still want the
                        # benchmark run to exercise Cedar.
                        try:
                            evidence["cedar_plugin_config_apply"] = (
                                ensure_mysql_cedar_plugin_sysvars(
                                    admin_conn, plugins_cfg
                                )
                            )
                            evidence["cedar_plugin_reset_ok"] = (
                                reset_cedar_plugin_stats(admin_conn)
                            )
                            plugin_before = capture_cedar_plugin_status(admin_conn)
                            evidence["cedar_plugin_status_before"] = plugin_before
                        except Exception as e:
                            evidence["cedar_plugin_config_error"] = str(e)
                            plugin_before = {}
                    else:
                        plugin_before = {}

                    # Snapshots are best-effort; failures should be recorded but
                    # should not prevent Cedar plugin config from being applied.
                    try:
                        evidence["mysql_before"] = capture_validity_snapshot(
                            admin_conn
                        ).__dict__
                    except Exception as e:
                        evidence["mysql_before_error"] = str(e)
                finally:
                    admin_conn.close()
            else:
                plugin_before = {}
        except Exception as e:
            evidence["mysql_snapshot_error"] = str(e)
            plugin_before = {}

        # Cedar agent stats (cedar system only)
        agent_before: dict[str, Any] = {}
        if system == "cedar":
            try:
                from framework.cedar_stats import (
                    get_authorization_decision_breakdown,
                    get_cedar_agent_stats,
                    reset_cedar_agent_stats,
                    verify_auth_invocations,
                )

                reset_ok = reset_cedar_agent_stats(cfg.cedar_agent.url)
                agent_before = get_cedar_agent_stats(cfg.cedar_agent.url)
                evidence["cedar_agent_reset_ok"] = reset_ok
                evidence["cedar_agent_stats_before"] = agent_before
                evidence["cedar_agent_decisions_before"] = (
                    get_authorization_decision_breakdown(cfg.cedar_agent.url)
                )
            except Exception as e:
                evidence["cedar_agent_stats_error"] = str(e)

        runner = BenchmarkRunner(workload, cfg)

        # Run benchmark for ONLY the specified system
        res = runner.run_system(system)

        # Capture after snapshots
        try:
            import mysql.connector

            from framework.mysql_introspection import (
                capture_cedar_plugin_status,
                capture_validity_snapshot,
                diff_counters,
            )

            db_cfg = cfg.databases.get(system)
            if db_cfg and db_cfg.type == "mysql":
                admin_conn = _connect_with_retry(
                    host=db_cfg.host,
                    port=db_cfg.port,
                    user=db_cfg.user,
                    password=db_cfg.password,
                    max_retries=6,
                    initial_delay=0.5,
                )
                try:
                    try:
                        evidence["mysql_after"] = capture_validity_snapshot(
                            admin_conn
                        ).__dict__
                    except Exception as e:
                        evidence["mysql_after_error"] = str(e)
                    if system == "cedar":
                        plugin_after = capture_cedar_plugin_status(admin_conn)
                        evidence["cedar_plugin_status_after"] = plugin_after
                        evidence["cedar_plugin_status_delta"] = diff_counters(
                            plugin_before, plugin_after
                        )
                finally:
                    admin_conn.close()
        except Exception as e:
            evidence["mysql_snapshot_after_error"] = str(e)

        if system == "cedar":
            try:
                from framework.cedar_stats import (
                    get_authorization_decision_breakdown,
                    get_cedar_agent_stats,
                    verify_auth_invocations,
                )

                agent_after = get_cedar_agent_stats(cfg.cedar_agent.url)
                evidence["cedar_agent_stats_after"] = agent_after
                evidence["cedar_agent_decisions_after"] = (
                    get_authorization_decision_breakdown(cfg.cedar_agent.url)
                )
                expected_min = int(cfg.benchmark_user.min_expected_auth_requests)
                evidence["cedar_agent_auth_verification"] = verify_auth_invocations(
                    agent_before, agent_after, expected_min=expected_min, verbose=False
                )
            except Exception as e:
                evidence["cedar_agent_stats_after_error"] = str(e)

        # Persist evidence next to results.json for easy auditing
        evidence_path = output_path / "auth_evidence.json"
        try:
            evidence_path.write_text(json.dumps(evidence, indent=2))
        except Exception:
            # Best-effort: don't fail the experiment if evidence can't be written
            pass

        # Return in the format expected by orchestrator
        meta = dict(res["metadata"])
        meta["auth_evidence_path"] = str(evidence_path)
        return {
            system: res["results"],
            "metadata": meta,
        }

    # Create and run orchestrator
    orchestrator = MultiRunOrchestrator(
        experiment_name="benchmark",
        ordering=ordering,
        n_pairs=int(cfg.benchmark.n_runs),
        rest_between_runs=rest_seconds,
        seed=seed,
        output_base_dir=out_dir,
    )

    # Convert config to dict for metadata
    config_dict = {
        "benchmark": {
            "iterations": cfg.benchmark.iterations,
            "warmup_iterations": cfg.benchmark.warmup_iterations,
            "use_query_user": cfg.benchmark.use_query_user,
        },
        "databases": {
            k: {"host": v.host, "port": v.port, "database": v.database}
            for k, v in cfg.databases.items()
        },
    }

    result = orchestrator.run(
        run_single_experiment=run_single,
        config=config_dict,
        workload_path=workload_path,
        warmup_iterations=cfg.benchmark.warmup_iterations,
        measurement_iterations=cfg.benchmark.iterations,
    )

    click.echo("\n✓ Multi-run experiment completed!")
    click.echo("  - Paired runs executed")
    click.echo(f"  - Results saved to: {out_dir}")

    # Post-run: print Cedar agent stats (helps validate Cedar was exercised)
    try:
        _print_cedar_agent_stats(cfg=cfg, label="multi-run/benchmark end")
    except Exception:
        pass

    # Print summary
    agg = result.aggregate
    if "comparisons" in agg:
        median_comp = agg["comparisons"].get("median_latency", {})
        overhead_pct = median_comp.get("overhead_pct", 0)
        p_value = median_comp.get("test_result", {}).get("p_value", 1.0)
        significant = median_comp.get("test_result", {}).get("significant", False)

        click.echo("\nSummary (median latency):")
        click.echo(f"  - Overhead: {overhead_pct:+.2f}%")
        click.echo(
            f"  - p-value: {p_value:.4f} ({'significant' if significant else 'not significant'})"
        )


@multi_run.command("analyze")
@click.option("--config", type=click.Path(exists=True), help="Path to YAML/JSON config")
@click.option(
    "--results-dir",
    type=click.Path(exists=True),
    help="Directory containing multi-run results",
)
@click.option("--output-dir", default=None, help="Output directory for analysis")
def multi_run_analyze(config, results_dir, output_dir):
    """Analyze multi-run experiment results and generate reports."""
    from framework.visualizations_ci import (
        generate_category_overhead_with_ci,
        generate_latex_table_with_ci,
        generate_multi_run_summary_plot,
    )

    cfg = load_config_file(config)

    # Resolve results_dir
    if not results_dir:
        _, results_dir, _ = _get_experiment_paths(cfg, "benchmark")

    results_path = Path(results_dir)
    out_path = Path(output_dir) if output_dir else results_path / "analysis"
    out_path.mkdir(parents=True, exist_ok=True)

    # Load multi-run results
    multi_run_json = results_path / "multi_run_results.json"
    if not multi_run_json.exists():
        raise click.ClickException(f"Multi-run results not found at {multi_run_json}")

    data = json.loads(multi_run_json.read_text())

    click.echo(f"Analyzing results from {results_path}...")

    # Generate summary plot
    summary_plot = generate_multi_run_summary_plot(
        multi_run_json,
        out_path / "multi_run_summary.png",
    )
    if summary_plot:
        click.echo(f"  ✓ Generated: {summary_plot}")

    # Generate category comparison plot
    cat_data = data.get("category_comparisons", {})
    if cat_data:
        cat_plot = generate_category_overhead_with_ci(
            cat_data,
            out_path / "category_overhead_with_ci.png",
        )
        if cat_plot:
            click.echo(f"  ✓ Generated: {cat_plot}")

    # Generate LaTeX table
    latex_data = []
    for cat, cat_info in cat_data.items():
        latex_data.append(
            {
                "operation": cat,
                "baseline_ci": cat_info.get("baseline_ci", {}),
                "cedar_ci": cat_info.get("cedar_ci", {}),
                "overhead_pct": cat_info.get("overhead_pct", 0),
                "significant": cat_info.get("significant_after_correction", False),
            }
        )

    if latex_data:
        generate_latex_table_with_ci(
            latex_data,
            out_path / "overhead_table.tex",
            caption="Query-by-Query Overhead with 95\\% Confidence Intervals",
            label="tab:overhead-ci",
        )
        click.echo(f"  ✓ Generated: {out_path / 'overhead_table.tex'}")

    click.echo(f"\n✓ Analysis complete! Results in: {out_path}")


# =============================================================================
# Overhead Breakdown Analysis
# =============================================================================


@cli.command("comprehensive-breakdown")
@click.option(
    "--results-dir",
    type=click.Path(exists=True),
    help="Directory containing experiment results",
)
@click.option(
    "--analysis-dir", type=click.Path(), help="Directory containing analysis outputs"
)
@click.option("--config", type=click.Path(exists=True), help="Path to YAML/JSON config")
def comprehensive_breakdown_cmd(results_dir, analysis_dir, config):
    """Generate a complete overhead breakdown by unifying multiple data sources."""
    from framework.comprehensive_breakdown import ComprehensiveOverheadAnalyzer
    from framework.config import load_config_file

    cfg = load_config_file(config)

    # Resolve paths
    res_path = (
        Path(results_dir)
        if results_dir
        else Path(cfg.output.results_dir or "./results")
    )
    ana_path = (
        Path(analysis_dir)
        if analysis_dir
        else Path(cfg.output.analysis_dir or "./analysis")
    )

    click.echo(f"Generating comprehensive breakdown from {res_path}...")

    # We might be looking at a root results dir or a specific experiment dir
    # If it's a root dir, we should find sub-experiments
    experiments = []

    def is_experiment_dir(d):
        # Check for any of our known result files
        if (
            (d / "pair_result.json").exists()
            or (d / "results.json").exists()
            or (d / "tpcc_mysql_results.json").exists()
            or (d / "sysbench_tpcc_results.json").exists()
        ):
            return True

        # Check for pgbench comparison files (robustly)
        if any(d.glob("pgbench_comparison_*.json")):
            return True

        # Also check for profiling-only directories
        if (d / "profiling").exists():
            return True

        return False

    if is_experiment_dir(res_path):
        experiments.append((res_path, ana_path))
    else:
        # Look for subdirectories that look like experiments
        for subdir in res_path.iterdir():
            if not subdir.is_dir():
                continue
            # Try to find matching analysis dir
            rel_path = subdir.relative_to(res_path)
            sub_ana = ana_path / rel_path
            if is_experiment_dir(subdir):
                experiments.append((subdir, sub_ana))
            else:
                # One more level deep (e.g. results/tag/experiment)
                for subsubdir in subdir.iterdir():
                    if not subsubdir.is_dir():
                        continue
                    rel_sub = subsubdir.relative_to(res_path)
                    sub_sub_ana = ana_path / rel_sub
                    if is_experiment_dir(subsubdir):
                        experiments.append((subsubdir, sub_sub_ana))

    if not experiments:
        click.echo("No valid experiment results found.")
        return

    click.echo(f"Found {len(experiments)} experiments to analyze")

    for r_dir, a_dir in experiments:
        click.echo(f"\nAnalyzing: {r_dir.name}")
        analyzer = ComprehensiveOverheadAnalyzer(r_dir, a_dir)

        # Generate LaTeX and CSV
        latex_out = a_dir / "comprehensive_breakdown.tex"
        csv_out = a_dir / "comprehensive_breakdown.csv"

        analyzer.generate_report_latex(latex_out)
        analyzer.generate_report_csv(csv_out)

        click.echo(f"  ✓ Saved breakdown to {a_dir}")


@cli.command("overhead-breakdown")
@click.option("--config", type=click.Path(exists=True), help="Path to YAML/JSON config")
@click.option(
    "--plugin-logs",
    type=click.Path(exists=True),
    help="Path to plugin timing logs (CSV/JSON)",
)
@click.option(
    "--agent-logs",
    type=click.Path(exists=True),
    help="Path to agent timing logs (CSV/JSON)",
)
@click.option("--output-dir", default=None, help="Output directory for analysis")
@click.option(
    "--simulate", is_flag=True, help="Generate simulated data if logs are missing"
)
def overhead_breakdown(config, plugin_logs, agent_logs, output_dir, simulate):
    """Analyze granular overhead breakdown from instrumented logs."""
    from framework.config import load_config_file
    from framework.overhead_breakdown import (
        OverheadBreakdownAnalyzer,
        create_simulated_breakdown_data,
    )
    from framework.visualizations_ci import generate_overhead_breakdown_plot

    cfg = load_config_file(config)
    out_path = (
        Path(output_dir)
        if output_dir
        else Path(cfg.output.analysis_dir or "./analysis") / "breakdown"
    )
    out_path.mkdir(parents=True, exist_ok=True)

    analyzer = OverheadBreakdownAnalyzer()

    if simulate:
        click.echo("Generating simulated breakdown data for demonstration...")
        analyzer = create_simulated_breakdown_data(
            n_requests=1000, mean_total_ms=cfg.scaling.max_latency_ms or 5.0
        )
    else:
        if not plugin_logs:
            raise click.ClickException("Must provide --plugin-logs or use --simulate")

        click.echo(f"Loading plugin timings from {plugin_logs}...")
        n_plugin = analyzer.load_plugin_timings(Path(plugin_logs))
        click.echo(f"  ✓ Loaded {n_plugin} records")

        if agent_logs:
            click.echo(f"Correlating with agent timings from {agent_logs}...")
            n_agent = analyzer.load_agent_timings(Path(agent_logs))
            click.echo(f"  ✓ Correlated {n_agent} records")

    # Perform analysis
    click.echo("Analyzing overhead breakdown...")
    result = analyzer.analyze()

    if result.n_requests == 0:
        click.echo("No records to analyze.")
        return

    # Print summary to console
    click.echo("\nPhase Breakdown (Medians):")
    for phase in result.phases:
        ci_str = f" [{phase.ci.lower:.2f}, {phase.ci.upper:.2f}]" if phase.ci else ""
        click.echo(
            f"  - {phase.phase_name:25}: {phase.median_ms:6.2f} ms{ci_str} ({phase.share_of_total_pct:5.1f}%)"
        )

    click.echo(f"  {'-' * 60}")
    total = result.total_authorization_overhead
    click.echo(
        f"  - {'TOTAL':25}: {total.median_ms:6.2f} ms ({total.share_of_total_pct:5.1f}%)"
    )

    # Validation
    val = result.validation_result
    click.echo(f"\nValidation: {val['message']} (diff: {val['difference_pct']:.1f}%)")

    # Generate outputs
    analyzer.generate_breakdown_csv(out_path / "overhead_breakdown.csv")
    analyzer.generate_breakdown_latex(out_path / "overhead_breakdown.tex")
    analyzer.generate_raw_csv(out_path / "raw_breakdown.csv")

    # Plot
    plot_path = out_path / "overhead_breakdown.png"
    # Convert phase breakdowns to dict for plotter
    plot_data = []
    for p in result.phases:
        d = p.model_dump()
        if p.ci:
            d["ci_lower"] = p.ci.lower
            d["ci_upper"] = p.ci.upper
        plot_data.append(d)

    generate_overhead_breakdown_plot(plot_data, plot_path)

    click.echo(f"\n✓ Analysis complete! Results saved to: {out_path}")


# =============================================================================
# Differential Profiling (Baseline vs Cedar)
# =============================================================================


@cli.group("profile")
def profile():
    """Differential profiling helpers (baseline vs Cedar) to localize latency shifts."""
    pass


@profile.command("mysql")
@click.option("--config", type=click.Path(exists=True), help="Path to YAML/JSON config")
@click.option(
    "--target",
    type=click.Choice(["baseline", "cedar"]),
    default="baseline",
    show_default=True,
)
@click.option(
    "--experiment",
    default="benchmark",
    show_default=True,
    help="Experiment name used to locate workload.json",
)
@click.option(
    "--workload-dir",
    default=None,
    help="Override workload directory (must contain workload.json)",
)
@click.option(
    "--top-n", default=50, show_default=True, help="Top-N perf schema events to include"
)
@click.option(
    "--no-reset",
    is_flag=True,
    help="Do not reset performance_schema summary tables before run",
)
@click.option(
    "--no-enable",
    is_flag=True,
    help="Do not attempt to enable performance_schema instruments/consumers",
)
@click.option(
    "--output", default=None, help="Output JSON path (defaults under analysis dir)"
)
def profile_mysql(
    config, target, experiment, workload_dir, top_n, no_reset, no_enable, output
):
    """Profile MySQL internal stage/wait time deltas while executing the workload."""
    from framework.config import load_config_file
    from framework.differential_profiling import mysql_collect_stage_wait_profile
    from framework.workload_generator import Workload

    cfg = load_config_file(config)
    _ensure_mysql_databases(cfg, "MySQL profiling")

    exp_workload_dir, _, exp_analysis_dir = _get_experiment_paths(cfg, experiment)
    wl_dir = Path(workload_dir) if workload_dir else exp_workload_dir
    wl_path = wl_dir / "workload.json"
    if not wl_path.exists():
        raise click.ClickException(f"workload.json not found: {wl_path}")

    # Verify workload loads (fail fast)
    Workload.load(wl_path)

    out_path = (
        Path(output)
        if output
        else (exp_analysis_dir / "profiling" / f"mysql_{target}_perf_schema.json")
    )
    out_path.parent.mkdir(parents=True, exist_ok=True)

    click.echo(f"Profiling MySQL ({target}) using workload: {wl_path}")
    click.echo(f"Output: {out_path}")

    mysql_collect_stage_wait_profile(
        cfg,
        system_name=target,
        workload_path=wl_path,
        out_path=out_path,
        top_n=int(top_n),
        reset=not no_reset,
        enable=not no_enable,
    )

    click.echo("✓ MySQL profiling complete")


@profile.command("postgres")
@click.option("--config", type=click.Path(exists=True), help="Path to YAML/JSON config")
@click.option(
    "--target",
    type=click.Choice(["postgres-baseline", "postgres-cedar"]),
    default="postgres-baseline",
    show_default=True,
)
@click.option(
    "--experiment",
    default="benchmark",
    show_default=True,
    help="Experiment name used to locate workload.json",
)
@click.option(
    "--workload-dir",
    default=None,
    help="Override workload directory (must contain workload.json)",
)
@click.option(
    "--sample-n",
    default=200,
    show_default=True,
    help="Number of workload queries to sample for EXPLAIN ANALYZE",
)
@click.option(
    "--output", default=None, help="Output JSON path (defaults under analysis dir)"
)
def profile_postgres(config, target, experiment, workload_dir, sample_n, output):
    """Profile PostgreSQL planning vs execution time via EXPLAIN (ANALYZE, FORMAT JSON)."""
    from framework.config import load_config_file
    from framework.differential_profiling import postgres_collect_explain_profile
    from framework.workload_generator import Workload

    cfg = load_config_file(config)

    exp_workload_dir, _, exp_analysis_dir = _get_experiment_paths(cfg, experiment)
    wl_dir = Path(workload_dir) if workload_dir else exp_workload_dir
    wl_path = wl_dir / "workload.json"
    if not wl_path.exists():
        raise click.ClickException(f"workload.json not found: {wl_path}")

    Workload.load(wl_path)

    out_path = (
        Path(output)
        if output
        else (exp_analysis_dir / "profiling" / f"postgres_{target}_explain.json")
    )
    out_path.parent.mkdir(parents=True, exist_ok=True)

    click.echo(f"Profiling PostgreSQL ({target}) using workload: {wl_path}")
    click.echo(f"Output: {out_path}")

    postgres_collect_explain_profile(
        cfg,
        system_name=target,
        workload_path=wl_path,
        out_path=out_path,
        sample_n=int(sample_n),
    )

    click.echo("✓ PostgreSQL profiling complete")


@profile.command("diff")
@click.option(
    "--baseline-profile",
    required=True,
    type=click.Path(exists=True),
    help="Baseline profile JSON",
)
@click.option(
    "--cedar-profile",
    required=True,
    type=click.Path(exists=True),
    help="Cedar profile JSON",
)
@click.option("--output", required=True, help="Output CSV path")
def profile_diff(baseline_profile, cedar_profile, output):
    """Generate a baseline vs Cedar diff CSV from two profile JSON files."""
    from framework.differential_profiling import diff_profiles_to_csv

    out = diff_profiles_to_csv(
        Path(baseline_profile),
        Path(cedar_profile),
        Path(output),
    )
    click.echo(f"✓ Wrote diff CSV: {out}")


@cli.group("suite")
def suite():
    """Run complete experiment suites."""
    pass


@suite.command("smoke")
@click.option("--config", type=click.Path(exists=True), help="Path to YAML/JSON config")
def suite_smoke(config):
    """Run smoke test suite (quick verification, ~15 minutes)."""
    cfg = load_config_file(config)

    click.echo("=" * 60)
    click.echo("SMOKE TEST SUITE")
    click.echo("Quick verification of experiment framework")
    click.echo("=" * 60)

    # Reduced parameters for smoke test
    smoke_iterations = 100
    smoke_warmup = 10

    click.echo("\nParameters:")
    click.echo(f"  - iterations: {smoke_iterations}")
    click.echo(f"  - warmup: {smoke_warmup}")

    # Step 1: Setup systems (create databases, tables, users)
    click.echo("\n[1/4] Setting up databases and authorization...")
    auth_spec_path = cfg.auth_spec_path
    if auth_spec_path and Path(auth_spec_path).exists():
        try:
            db_type = _detect_primary_db_type(cfg)

            click.echo("  Setting up baseline...")
            setup_baseline.callback(
                auth_spec=auth_spec_path, config=config, db_type=db_type
            )
            click.echo("  Setting up Cedar...")
            setup_cedar.callback(
                auth_spec=auth_spec_path, config=config, db_type=db_type
            )
            click.echo("  ✓ Setup completed")
        except Exception as e:
            click.echo(f"  ⚠ Setup failed (might already be set up): {e}")
    else:
        click.echo("  ⚠ No auth spec found, skipping setup")

    # Step 2: Generate workload
    click.echo("\n[2/4] Generating workload...")
    if auth_spec_path and Path(auth_spec_path).exists():
        workload_dir = (
            Path(cfg.output.workload_dir or "./workload") / cfg.experiment_tag / "smoke"
        )
        workload_dir.mkdir(parents=True, exist_ok=True)
        db_type = _detect_primary_db_type(cfg)

        generate_workload.callback(
            auth_spec=auth_spec_path,
            config=config,
            queries_per_combo=smoke_iterations,
            seed=cfg.workload.seed,
            db_type=db_type,
            output=str(workload_dir),
            experiment="smoke",
        )
        click.echo("  ✓ Workload generated")
    else:
        click.echo("  ⚠ No auth spec found, skipping workload generation")

    # Step 3: Run quick benchmark
    click.echo("\n[3/4] Running quick benchmark...")
    try:
        results_dir = (
            Path(cfg.output.results_dir or "./results") / cfg.experiment_tag / "smoke"
        )
        results_dir.mkdir(parents=True, exist_ok=True)
        run_benchmark.callback(
            workload_dir=str(workload_dir),
            config=config,
            iterations=smoke_iterations,
            concurrency=1,
            experiment="smoke",
            warmup_iterations=None,
            warmup_seconds=None,
            n_runs=None,
        )
        click.echo("  ✓ Benchmark completed")
    except Exception as e:
        click.echo(f"  ⚠ Benchmark failed: {e}")

    # Step 4: Analyze
    click.echo("\n[4/4] Analyzing results...")
    try:
        analysis_dir = (
            Path(cfg.output.analysis_dir or "./analysis") / cfg.experiment_tag / "smoke"
        )
        analysis_dir.mkdir(parents=True, exist_ok=True)
        analyze_results.callback(
            results_dir=str(results_dir),
            config=config,
            fmt="json",
            outputs_dir=str(analysis_dir),
            visualizations=True,
            include_extra=False,
        )
        click.echo("  ✓ Analysis completed")
    except Exception as e:
        click.echo(f"  ⚠ Analysis failed: {e}")

    click.echo("\n" + "=" * 60)
    click.echo("SMOKE TEST COMPLETE")
    click.echo("=" * 60)


@suite.command("paper")
@click.option("--config", type=click.Path(exists=True), help="Path to YAML/JSON config")
@click.option(
    "--skip-existing", is_flag=True, help="Skip experiments with existing results"
)
def suite_paper(config, skip_existing):
    """Run full paper experiment suite (USENIX-grade, several hours)."""
    cfg = load_config_file(config)

    click.echo("=" * 60)
    click.echo("FULL PAPER EXPERIMENT SUITE")
    click.echo("USENIX-grade evaluation with statistical rigor")
    click.echo("=" * 60)

    click.echo("\nParameters:")
    click.echo(f"  - iterations: {cfg.benchmark.iterations}")
    click.echo(f"  - warmup: {cfg.benchmark.warmup_iterations}")

    experiments = [
        ("E1: Query-by-Query Overhead", "benchmark"),
        ("E2: Overhead Breakdown", "breakdown"),
        ("E3: Concurrency Scaling", "concurrency"),
        ("E4: Policy Scaling", "policy_scaling"),
        ("E5: Analytic / Join-heavy", "analytic"),
        ("E6: Concurrency Contention", "contention"),
        ("E7: Failure Resilience", "failure"),
        ("E8: PostgreSQL Parity", "postgres"),
        ("E9: TPC-C (sysbench)", "tpcc_sysbench"),
        ("E10: DDL Operations", "ddl"),
    ]

    for exp_name, exp_type in experiments:
        click.echo(f"\n{'=' * 60}")
        click.echo(f"Running {exp_name}...")
        click.echo("=" * 60)

        try:
            if exp_type == "benchmark":
                _ensure_workload_exists(cfg, "benchmark", config)
                # Use multi-run benchmark
                multi_run_benchmark.callback(
                    config=config,
                    ordering="abba",
                    rest_seconds=5.0,
                    seed=cfg.workload.seed,
                    workload_dir=None,
                    output_dir=None,
                )
            elif exp_type == "breakdown":
                overhead_breakdown.callback(
                    config=config,
                    plugin_logs=None,
                    agent_logs=None,
                    output_dir=None,
                    simulate=True,
                )
            elif exp_type == "concurrency":
                concurrency_benchmark.callback(
                    config=config,
                    threads=None,
                    duration=None,
                    target="both",
                    sysbench_bin=None,
                    oltp=None,
                    db=None,
                    tables=None,
                    table_size=None,
                    docker=False,
                )
            elif exp_type == "policy_scaling":
                _ensure_workload_exists(cfg, "policy_scaling", config)
                policy_scaling.callback(
                    config=config,
                    counts=None,
                    iterations=None,
                    seed=None,
                    workload_dir=None,
                    reset=True,
                    match_ratio=None,
                )
            elif exp_type == "analytic":
                _ensure_workload_exists(cfg, "analytic", config)
                analytic_benchmark.callback(
                    config=config,
                    iterations=None,
                    concurrency=None,
                    experiment="analytic",
                )
            elif exp_type == "contention":
                concurrency_benchmark.callback(
                    config=config,
                    threads="1,4,8,16,32",
                    duration=None,
                    target="both",
                    sysbench_bin=None,
                    oltp=None,
                    db=None,
                    tables=None,
                    table_size=None,
                    docker=False,
                )
            elif exp_type == "failure":
                # Run key failure tests
                agent_delay_benchmark.callback(config=config)
                agent_stress_test.callback(config=config)
            elif exp_type == "postgres":
                # Run PostgreSQL parity tests
                run_pgbench_experiment(
                    config=config,
                    db_system="postgres-baseline",
                    scale=10,
                    clients=4,
                    duration=60,
                    experiment="postgres_parity",
                )
                run_pgbench_experiment(
                    config=config,
                    db_system="postgres-cedar",
                    scale=10,
                    clients=4,
                    duration=60,
                    experiment="postgres_parity",
                )
                compare_pgbench_systems(
                    config=config,
                    scale=10,
                    clients=4,
                    duration=60,
                    experiment="postgres_parity",
                )
            elif exp_type == "tpcc_sysbench":
                tpcc_sysbench.callback(
                    config=config,
                    tpcc_lua=None,
                    warehouses=None,
                    scale=None,
                    threads=None,
                    duration=None,
                    output_dir=None,
                )
            elif exp_type == "ddl":
                ddl_test.callback(config=config, suite="comprehensive", output_dir=None)
            click.echo(f"  ✓ {exp_name} completed")
        except Exception as e:
            click.echo(f"  ✗ {exp_name} failed: {e}")

    click.echo("\n" + "=" * 60)
    click.echo("PAPER SUITE COMPLETE")
    click.echo("=" * 60)
    click.echo("\nNext steps:")
    click.echo("  1. Run 'multi-run analyze' on results")
    click.echo("  2. Run 'analyze-results' for each experiment")
    click.echo("  3. Copy outputs to paper/figures/")


@cli.group("tpcc")
def tpcc():
    """TPC-C benchmark commands for industry-standard OLTP evaluation."""
    pass


@tpcc.command("sysbench-tpcc")
@click.option("--config", type=click.Path(exists=True), help="Path to YAML/JSON config")
@click.option(
    "--tpcc-lua", type=click.Path(exists=True), help="Path to tpcc.lua script"
)
@click.option(
    "--warehouses", default=None, type=int, help="Number of warehouses per table set"
)
@click.option("--tables", default=None, type=int, help="Number of table sets")
@click.option(
    "--threads", default=None, type=int, help="Number of threads for benchmark"
)
@click.option(
    "--duration", default=None, type=int, help="Benchmark duration in seconds"
)
@click.option("--prepare/--no-prepare", default=None, help="Run the prepare step")
@click.option(
    "--cleanup/--no-cleanup", default=None, help="Run the cleanup step before prepare"
)
@click.option("--run/--no-run", default=True, help="Run the benchmark step")
@click.option("--output-dir", default=None, help="Output directory for results")
@click.option(
    "--n-runs",
    default=None,
    type=int,
    help="Number of independent runs for statistical rigor (default: from config)",
)
def tpcc_sysbench(
    config,
    tpcc_lua,
    warehouses,
    tables,
    threads,
    duration,
    prepare,
    cleanup,
    run,
    output_dir,
    n_runs,
):
    """Run TPC-C benchmark using sysbench-tpcc.

    Runs benchmark n_runs times for statistical rigor.
    """
    from framework.sysbench_tpcc_client import SysbenchTPCCClient, SysbenchTPCCConfig

    cfg = load_config_file(config)

    # Use config values if not provided via CLI
    tpcc_lua = tpcc_lua or cfg.tpcc.tpcc_lua_path
    if not tpcc_lua or not Path(tpcc_lua).exists():
        raise click.ClickException(
            f"sysbench-tpcc lua script not found at {tpcc_lua}. Please provide via --tpcc-lua or config."
        )

    # Determine scale and tables from CLI or config
    # For sysbench-tpcc:
    #   --scale is warehouses per table set
    #   --tables is the number of sets of tables
    warehouses = warehouses if warehouses is not None else cfg.tpcc.warehouses
    # If 'tables' is not provided, try to use 'scale' from config as 'tables' (legacy mapping)
    if tables is None:
        tables = cfg.tpcc.scale if hasattr(cfg.tpcc, "scale") and cfg.tpcc.scale else 1

    threads = threads if threads is not None else cfg.tpcc.threads
    duration = duration if duration is not None else cfg.tpcc.duration_s
    tpcc_n_runs = n_runs if n_runs is not None else cfg.tpcc.n_runs

    # Use config for prepare/cleanup if not specified on CLI
    if prepare is None:
        prepare = cfg.tpcc.prepare if hasattr(cfg.tpcc, "prepare") else True
    if cleanup is None:
        cleanup = cfg.tpcc.cleanup if hasattr(cfg.tpcc, "cleanup") else False

    click.echo("=" * 60)
    click.echo("TPC-C BENCHMARK (sysbench-tpcc)")
    click.echo("=" * 60)
    click.echo(f"Warehouses per set (--scale): {warehouses}")
    click.echo(f"Table Sets (--tables): {tables}")
    click.echo(f"Total Warehouses: {warehouses * tables}")
    click.echo(f"Threads: {threads}")
    click.echo(f"Duration: {duration} seconds")
    click.echo(f"Runs per system: {tpcc_n_runs}")
    click.echo(f"Lua script: {tpcc_lua}")

    # Run benchmark for both systems
    results = {}

    for system in ["baseline", "cedar"]:
        click.echo(f"\n--- Running on {system.upper()} ({tpcc_n_runs} runs) ---")

        db_config = cfg.databases[system]

        # Authorization verification for Cedar
        stats_before = None
        if system == "cedar":
            from framework.cedar_stats import (
                get_cedar_agent_stats,
                reset_cedar_agent_stats,
                verify_auth_invocations,
            )

            base_url = cfg.cedar_agent.url.rstrip("/") + "/v1"

            # Ensure MySQL plugin sysvars are correct before running anything.
            _apply_and_check_mysql_cedar_sysvars(
                cfg=cfg,
                db_config=db_config,
                admin_user=db_config.user,
                admin_password=db_config.password,
                label="tpcc/sysbench-tpcc preflight",
            )

            reset_cedar_agent_stats(base_url)
            stats_before = get_cedar_agent_stats(base_url)
            click.echo(
                f"  Cedar agent stats before: {stats_before.get('total_requests', 'N/A')} total requests"
            )

        try:
            output_path = (
                Path(output_dir)
                if output_dir
                else Path(cfg.output.results_dir) / cfg.experiment_tag / "tpcc"
            )

            # Create client
            admin_user = db_config.user
            admin_password = db_config.password
            run_user = admin_user
            run_password = admin_password

            if system == "cedar" and cfg.benchmark_user.enabled:
                from framework.benchmark_user_setup import (
                    BENCHMARK_PASSWORD,
                    BENCHMARK_USER,
                    create_mysql_benchmark_user,
                )

                create_mysql_benchmark_user(
                    host=db_config.host,
                    port=db_config.port,
                    admin_user=admin_user,
                    admin_pass=admin_password or "",
                    db_name="sbtest",
                    grant_native_privileges=False,
                )

                run_user = BENCHMARK_USER
                run_password = BENCHMARK_PASSWORD

            admin_config = SysbenchTPCCConfig(
                tpcc_lua_path=Path(tpcc_lua),
                db_host=db_config.host,
                db_port=db_config.port,
                db_user=admin_user,
                db_password=admin_password,
                db_name="sbtest",
                warehouses=warehouses,
                tables=tables,
                threads=threads,
                duration=duration,
                output_dir=output_path,
            )
            admin_client = SysbenchTPCCClient(admin_config)

            admin_config = SysbenchTPCCConfig(
                tpcc_lua_path=Path(tpcc_lua),
                db_host=db_config.host,
                db_port=db_config.port,
                db_user=admin_user,
                db_password=admin_password,
                db_name="sbtest",
                warehouses=warehouses,
                tables=tables,
                threads=threads,
                duration=duration,
                output_dir=output_path,
            )
            admin_client = SysbenchTPCCClient(admin_config)
            if system == "cedar" and cfg.benchmark_user.enabled:
                admin_client.register_cedar_entities(cfg)

            # Force-apply authorization cache settings if running on Cedar
            # This ensures cache is configured even if plugin was already installed
            if system == "cedar":
                try:
                    import mysql.connector

                    # Get cache config from cfg.cedar_agent.plugins
                    plugins_cfg = (
                        cfg.cedar_agent.plugins
                        if cfg.cedar_agent and cfg.cedar_agent.plugins
                        else {}
                    )
                    cedar_auth_cfg = plugins_cfg.get("cedar_authorization", {})

                    cache_enabled = cedar_auth_cfg.get("cache_enabled", True)
                    cache_size = cedar_auth_cfg.get("cache_size", 1000)
                    cache_ttl = cedar_auth_cfg.get("cache_ttl", 300)
                    log_info = cedar_auth_cfg.get("log_info", False)
                    enable_column_access = cedar_auth_cfg.get(
                        "enable_column_access", False
                    )

                    collect_stats = bool(cedar_auth_cfg.get("collect_stats", False))

                    click.echo(
                        f"  Configuring Cedar cache: enabled={cache_enabled}, size={cache_size}, ttl={cache_ttl}"
                    )

                    # Connect to admin/root to set globals
                    conn = mysql.connector.connect(
                        host=db_config.host,
                        port=db_config.port,
                        user=admin_user,
                        password=admin_password,
                        database="sbtest",
                    )
                    cursor = conn.cursor()

                    enabled_val = "ON" if cache_enabled else "OFF"
                    log_info_val = "ON" if log_info else "OFF"
                    enable_column_access_val = "ON" if enable_column_access else "OFF"
                    collect_stats_val = "ON" if collect_stats else "OFF"
                    cursor.execute(
                        f"SET GLOBAL cedar_authorization_cache_enabled = {enabled_val}"
                    )
                    cursor.execute(
                        f"SET GLOBAL cedar_authorization_cache_size = {cache_size}"
                    )
                    cursor.execute(
                        f"SET GLOBAL cedar_authorization_cache_ttl = {cache_ttl}"
                    )
                    cursor.execute(
                        f"SET GLOBAL cedar_authorization_log_info = {log_info_val}"
                    )
                    cursor.execute(
                        f"SET GLOBAL cedar_authorization_enable_column_access = {enable_column_access_val}"
                    )
                    cursor.execute("SET GLOBAL cedar_authorization_cache_flush = 1")
                    cursor.execute(
                        f"SET GLOBAL cedar_authorization_collect_stats = {collect_stats_val}"
                    )
                    cursor.execute("SET GLOBAL cedar_authorization_reset_stats = 1")

                    conn.commit()
                    cursor.close()
                    conn.close()
                    click.echo(
                        "  ✓ Applied cache configuration + stats reset via SET GLOBAL"
                    )

                except Exception as e:
                    click.echo(f"  ⚠ Warning: Failed to apply cache settings: {e}")

            run_config = SysbenchTPCCConfig(
                tpcc_lua_path=Path(tpcc_lua),
                db_host=db_config.host,
                db_port=db_config.port,
                db_user=run_user,
                db_password=run_password,
                db_name="sbtest",
                warehouses=warehouses,
                tables=tables,
                threads=threads,
                duration=duration,
                output_dir=output_path,
            )
            run_client = SysbenchTPCCClient(run_config)

            if cleanup:
                click.echo("  Cleaning up existing tables...")
                try:
                    admin_client.cleanup()
                    click.echo("  ✓ Cleanup complete")
                except Exception as clean_e:
                    click.echo(f"  (Cleanup info: {clean_e})")

            if prepare:
                click.echo(
                    "  Preparing database (this might take a while if data is missing)..."
                )
                try:
                    # Use 1 thread for prepare to be safe with data loading
                    admin_client.prepare(threads=1)
                    click.echo("  ✓ Database prepared")
                except Exception as prep_e:
                    click.echo(f"  ✗ Prepare failed: {prep_e}")
                    if "already exists" in str(prep_e).lower():
                        click.echo(
                            "  Tables already exist, attempting to run anyway..."
                        )
                    else:
                        raise prep_e

            # Sanity check: verify some data exists
            click.echo("  Verifying data exists...")
            try:
                import mysql.connector

                conn = mysql.connector.connect(
                    host=db_config.host,
                    port=db_config.port,
                    user=db_config.user,
                    password=db_config.password,
                    database="sbtest",
                )
                cursor = conn.cursor()
                # Check first table set's warehouse table
                cursor.execute("SELECT count(*) FROM warehouse1")
                count = cursor.fetchone()[0]
                conn.close()
                if count == 0:
                    click.echo("  ⚠ Warning: warehouse1 table is empty!")
                else:
                    click.echo(f"  ✓ Data verified ({count} warehouses in set 1)")
            except Exception as v_e:
                click.echo(f"  ⚠ Could not verify data: {v_e}")

            if run:
                # Multi-run: execute benchmark n_runs times
                run_results = []

                for run_idx in range(tpcc_n_runs):
                    click.echo(f"  Run {run_idx + 1}/{tpcc_n_runs}...")
                    result = run_client.run()
                    run_results.append(
                        {
                            "run": run_idx + 1,
                            "tpm": result.get("tpm", 0),
                            "avg_latency_ms": result.get("avg_latency_ms", 0),
                            "p95_latency_ms": result.get("p95_latency_ms", 0),
                        }
                    )
                    click.echo(
                        f"    TPM: {result.get('tpm', 0):.1f}, Lat: {result.get('avg_latency_ms', 0):.2f}ms"
                    )

                # Aggregate results
                import statistics

                tpm_values = [r["tpm"] for r in run_results if r["tpm"] > 0]
                lat_values = [
                    r["avg_latency_ms"] for r in run_results if r["avg_latency_ms"] > 0
                ]

                aggregate = {
                    "n_runs": len(run_results),
                    "tpm_median": statistics.median(tpm_values) if tpm_values else 0,
                    "tpm_mean": statistics.mean(tpm_values) if tpm_values else 0,
                    "tpm_std": statistics.stdev(tpm_values)
                    if len(tpm_values) > 1
                    else 0,
                    "avg_latency_median": statistics.median(lat_values)
                    if lat_values
                    else 0,
                    "runs": run_results,
                }

                results[system] = {
                    "config": {
                        "warehouses": warehouses,
                        "tables": tables,
                        "threads": threads,
                        "duration": duration,
                        "n_runs": tpcc_n_runs,
                    },
                    "aggregate": aggregate,
                    "benchmark": run_results[-1]
                    if run_results
                    else {},  # Last run for compatibility
                }

                click.echo(
                    f"  Aggregate: TPM={aggregate['tpm_median']:.1f}±{aggregate['tpm_std']:.1f}"
                )

                if system == "cedar":
                    try:
                        from framework.cedar_cache_analysis import get_mysql_cache_stats

                        plugin_stats = get_mysql_cache_stats(
                            host=db_config.host,
                            port=db_config.port,
                            user=admin_user,
                            password=admin_password or "",
                        )
                        results[system]["mysql_plugin_stats"] = plugin_stats

                        total_requests = int(plugin_stats.get("total_requests", 0) or 0)
                        cache_hits = int(plugin_stats.get("cache_hits", 0) or 0)
                        cache_misses = int(plugin_stats.get("cache_misses", 0) or 0)
                        cache_evictions = int(
                            plugin_stats.get("cache_evictions", 0) or 0
                        )
                        grants = int(plugin_stats.get("grants", 0) or 0)
                        denies = int(plugin_stats.get("denies", 0) or 0)
                        errors = int(plugin_stats.get("errors", 0) or 0)
                        avg_total_us = float(
                            plugin_stats.get("avg_total_time_us", 0.0) or 0.0
                        )
                        avg_remote_us = float(
                            plugin_stats.get("avg_remote_time_us", 0.0) or 0.0
                        )

                        total_cache = cache_hits + cache_misses
                        hit_rate = (
                            float(cache_hits) / float(total_cache)
                            if total_cache > 0
                            else 0.0
                        )

                        click.echo(
                            "  MySQL plugin stats: "
                            f"requests={total_requests}, grants={grants}, denies={denies}, errors={errors}, "
                            f"cache_hits={cache_hits}, cache_misses={cache_misses}, evictions={cache_evictions}, "
                            f"hit_rate={hit_rate:.3f}, avg_total_us={avg_total_us:.1f}, avg_remote_us={avg_remote_us:.1f}"
                        )
                    except Exception as _ps_e:
                        click.echo(
                            f"  ⚠ Warning: Failed to fetch MySQL plugin stats: {_ps_e}"
                        )

                # Verify authorization was invoked for Cedar
                if system == "cedar" and stats_before is not None:
                    stats_after = get_cedar_agent_stats(base_url)
                    from framework.cedar_stats import verify_auth_invocations

                    auth_result = verify_auth_invocations(
                        stats_before, stats_after, expected_min=100, verbose=True
                    )

                    if auth_result.get("auth_requests", 0) > 0:
                        click.echo(
                            f"  ✓ Authorization verification: {auth_result['auth_requests']} auth requests"
                        )
                        results[system]["auth_verification"] = auth_result
                    else:
                        click.echo(
                            "  ⚠ WARNING: No authorization requests detected!", err=True
                        )
                        click.echo(
                            "  This may indicate Cedar is NOT being invoked. Check user privileges.",
                            err=True,
                        )
                        results[system]["auth_verification"] = {
                            "warning": "No auth requests detected",
                            **auth_result,
                        }
            else:
                click.echo("  Skipping benchmark run (--no-run)")

        except Exception as e:
            click.echo(f"  ✗ Failed on {system}: {e}")
            failure_auth_stats: dict[str, Any] = {}
            failure_cache_stats: dict[str, Any] = {}
            if system == "postgres-cedar":
                try:
                    failure_auth_stats = client.get_authorization_stats() or {}
                    failure_cache_stats = client.get_authorization_cache_stats() or {}
                    click.echo(
                        f"  Authorization stats at failure: requests={failure_auth_stats.get('auth_requests', 0)}, grants={failure_auth_stats.get('auth_grants', 0)}, denies={failure_auth_stats.get('auth_denies', 0)}, ignores={failure_auth_stats.get('auth_ignores', 0)}, errors={failure_auth_stats.get('auth_errors', 0)}"
                    )
                    click.echo(
                        f"  Authorization cache at failure: hits={failure_cache_stats.get('hits', 0)}, misses={failure_cache_stats.get('misses', 0)}, evictions={failure_cache_stats.get('evictions', 0)}, entries={failure_cache_stats.get('entries', 0)}"
                    )
                except Exception:
                    pass

            results[system] = {
                "error": str(e),
                "auth_stats": failure_auth_stats,
                "cache_stats": failure_cache_stats,
            }

        # Always print Cedar agent stats at end of Cedar run
        if system == "cedar":
            try:
                _print_cedar_agent_stats(cfg=cfg, label="tpcc/sysbench-tpcc end")
            except Exception:
                pass

    # Save results
    output_path = (
        Path(output_dir)
        if output_dir
        else Path(cfg.output.results_dir) / cfg.experiment_tag / "tpcc"
    )
    output_path.mkdir(parents=True, exist_ok=True)
    results_file = output_path / "sysbench_tpcc_results.json"
    results_file.write_text(json.dumps(results, indent=2))

    click.echo(f"\n✓ Results saved to {results_file}")

    # Compare results if both succeeded
    if (
        "baseline" in results
        and "cedar" in results
        and "error" not in results["baseline"]
    ):
        base_bench = results["baseline"].get("benchmark", {})
        cedar_bench = results["cedar"].get("benchmark", {})

        baseline_tpm = base_bench.get("tpm", 0)
        cedar_tpm = cedar_bench.get("tpm", 0)

        if baseline_tpm > 0:
            from framework.stats import calculate_overhead_metrics

            oh = calculate_overhead_metrics(baseline_tpm, cedar_tpm, is_throughput=True)
            click.echo(
                f"\nThroughput overhead: {oh['overhead_pct']:+.2f}% ({oh['overhead_factor']:.2f}x slowdown)"
            )

            # Warn if overhead is negative (suspicious)
            if oh["overhead_pct"] < 0:
                click.echo(
                    "\n⚠ WARNING: Negative overhead detected (Cedar faster than baseline)!",
                    err=True,
                )
                click.echo("  This is suspicious and may indicate:", err=True)
                click.echo(
                    "  1. Cedar authorization is not being invoked (check auth_verification)",
                    err=True,
                )
                click.echo("  2. Configuration differences between databases", err=True)
                click.echo("  3. Run order bias (consider re-running)", err=True)


@tpcc.command("sysbench-tpcc-postgres")
@click.option("--config", type=click.Path(exists=True), help="Path to YAML/JSON config")
@click.option(
    "--tpcc-lua", type=click.Path(exists=True), help="Path to tpcc.lua script"
)
@click.option(
    "--warehouses", default=None, type=int, help="Number of warehouses per table set"
)
@click.option("--tables", default=None, type=int, help="Number of table sets")
@click.option(
    "--threads", default=None, type=int, help="Number of threads for benchmark"
)
@click.option(
    "--duration", default=None, type=int, help="Benchmark duration in seconds"
)
@click.option("--prepare/--no-prepare", default=None, help="Run the prepare step")
@click.option(
    "--cleanup/--no-cleanup", default=None, help="Run the cleanup step before prepare"
)
@click.option("--run/--no-run", default=True, help="Run the benchmark step")
@click.option("--output-dir", default=None, help="Output directory for results")
@click.option(
    "--cache/--no-cache", default=True, help="Enable/disable pg_authorization cache"
)
@click.option(
    "--n-runs",
    default=None,
    type=int,
    help="Number of independent runs for statistical rigor (default: from config)",
)
def tpcc_sysbench_postgres(
    config,
    tpcc_lua,
    warehouses,
    tables,
    threads,
    duration,
    prepare,
    cleanup,
    run,
    output_dir,
    cache,
    n_runs,
):
    """Run TPC-C benchmark using sysbench-tpcc on PostgreSQL.

    Runs benchmark n_runs times for statistical rigor.
    """
    from framework.sysbench_tpcc_client import SysbenchTPCCClient, SysbenchTPCCConfig

    cfg = load_config_file(config)

    # Use config values if not provided via CLI
    tpcc_lua = tpcc_lua or cfg.tpcc.tpcc_lua_path
    if not tpcc_lua or not Path(tpcc_lua).exists():
        raise click.ClickException(
            f"sysbench-tpcc lua script not found at {tpcc_lua}. Please provide via --tpcc-lua or config."
        )

    warehouses = warehouses if warehouses is not None else cfg.tpcc.warehouses
    if tables is None:
        tables = cfg.tpcc.scale if hasattr(cfg.tpcc, "scale") and cfg.tpcc.scale else 1

    threads = threads if threads is not None else cfg.tpcc.threads
    duration = duration if duration is not None else cfg.tpcc.duration_s
    tpcc_n_runs = n_runs if n_runs is not None else cfg.tpcc.n_runs

    if prepare is None:
        prepare = cfg.tpcc.prepare if hasattr(cfg.tpcc, "prepare") else False
    if cleanup is None:
        cleanup = cfg.tpcc.cleanup if hasattr(cfg.tpcc, "cleanup") else False

    click.echo("=" * 60)
    click.echo("POSTGRESQL TPC-C BENCHMARK (sysbench-tpcc)")
    click.echo("=" * 60)
    click.echo(f"Warehouses per set (--scale): {warehouses}")
    click.echo(f"Table Sets (--tables): {tables}")
    click.echo(f"Total Warehouses: {warehouses * tables}")
    click.echo(f"Threads: {threads}")
    click.echo(f"Duration: {duration} seconds")
    click.echo(f"Runs per system: {tpcc_n_runs}")
    click.echo(f"Lua script: {tpcc_lua}")

    results = {}
    systems = ["postgres-baseline", "postgres-cedar"]

    # docker-compose defaults; used for psql/ALTER SYSTEM/stats.
    pg_admin_user = "postgres"
    pg_admin_password = "postgres"

    for system in systems:
        click.echo(f"\n--- Running on {system.upper()} ---")
        if system not in cfg.databases:
            click.echo(f"  ✗ Skipping {system}: not found in config")
            continue

        db_config = cfg.databases[system]
        db_name = (
            cfg.sysbench.db_name
            if hasattr(cfg.sysbench, "db_name")
            else "abac_sysbench"
        )

        try:
            output_path = (
                Path(output_dir)
                if output_dir
                else Path(cfg.output.results_dir) / cfg.experiment_tag / "tpcc"
            )

            client_config = SysbenchTPCCConfig(
                tpcc_lua_path=Path(tpcc_lua),
                db_host=db_config.host,
                db_port=db_config.port,
                db_user=db_config.user,
                db_password=db_config.password,
                db_name=db_name,
                db_driver="pgsql",
                admin_user=pg_admin_user,
                admin_password=pg_admin_password,
                warehouses=warehouses,
                tables=tables,
                threads=threads,
                duration=duration,
                output_dir=output_path,
            )
            client = SysbenchTPCCClient(client_config)

            # For benchmarking: run the sysbench workload as a dedicated benchmark user.
            # Baseline: benchmark user has full native privileges.
            # Cedar: benchmark user has NO native data privileges (forces pg_authorization hook).
            from framework.benchmark_user_setup import (
                BENCHMARK_PASSWORD,
                BENCHMARK_USER,
                create_postgres_benchmark_user,
            )

            if system == "postgres-cedar":
                click.echo(
                    f"  Setting Cedar GUCs (cache={'enabled' if cache else 'disabled'})..."
                )
                plugins_cfg = (
                    cfg.cedar_agent.plugins
                    if cfg.cedar_agent and cfg.cedar_agent.plugins
                    else {}
                )
                pg_auth_cfg = plugins_cfg.get("pg_authorization", {})
                collect_stats = bool(pg_auth_cfg.get("collect_stats", False))
                client.set_guc("pg_authorization.namespace", "PostgreSQL")
                client.set_guc(
                    "pg_authorization.cedar_agent_url", "http://cedar-agent:8180"
                )
                client.set_guc("pg_authorization.enabled", "on")
                client.set_guc(
                    "pg_authorization.cache_enabled", "on" if cache else "off"
                )
                client.set_guc(
                    "pg_authorization.collect_stats", "on" if collect_stats else "off"
                )

            if cleanup:
                click.echo("  Cleaning up existing tables...")
                try:
                    client.cleanup()
                    click.echo("  ✓ Cleanup complete")
                except Exception as clean_e:
                    click.echo(f"  (Cleanup info: {clean_e})")

            if prepare:
                click.echo(
                    "  Preparing database (this might take a while if data is missing)..."
                )
                try:
                    client.prepare(threads=1)
                    click.echo("  ✓ Database prepared")
                except Exception as prep_e:
                    click.echo(f"  ✗ Prepare failed: {prep_e}")
                    if (
                        "already exists" in str(prep_e).lower()
                        or "bulk_insert" in str(prep_e).lower()
                    ):
                        click.echo(
                            "  Wait: Tables might already exist or bulk insert failed. This common on re-runs."
                        )
                    else:
                        raise prep_e

            # Verification for PostgreSQL
            click.echo("  Verifying data exists...")
            try:
                import psycopg2

                conn = psycopg2.connect(
                    host=db_config.host,
                    port=db_config.port,
                    user=db_config.user,
                    password=db_config.password,
                    database=db_name,
                )
                cursor = conn.cursor()
                cursor.execute("SELECT count(*) FROM warehouse1")
                count = cursor.fetchone()[0]
                conn.close()
                if count == 0:
                    click.echo("  ⚠ Warning: warehouse1 table is empty!")
                else:
                    click.echo(f"  ✓ Data verified ({count} warehouses in set 1)")
            except Exception as v_e:
                click.echo(f"  ⚠ Could not verify data: {v_e}")

            if run:
                # Ensure benchmark user exists with correct privilege mode for this system.
                # Do this AFTER prepare/cleanup so privileges reflect current tables.
                grant_native = system == "postgres-baseline"
                ok = create_postgres_benchmark_user(
                    host=db_config.host,
                    port=db_config.port,
                    db_name=db_name,
                    admin_password=pg_admin_password,
                    grant_native_privileges=grant_native,
                    table_owner_role=db_config.user,
                )
                if not ok:
                    raise click.ClickException(
                        f"Failed to create benchmark user '{BENCHMARK_USER}' on {system}"
                    )

                if system == "postgres-cedar":
                    click.echo("  Registering Cedar entities and policies...")
                    client.register_cedar_entities(cfg)
                    client.reset_authorization_stats()
                    client.reset_authorization_cache()

                    cedar_agent_url = cfg.cedar_agent.url
                    if reset_cedar_agent_stats(cedar_agent_url):
                        click.echo("  Cedar agent stats reset")
                    else:
                        click.echo("  Warning: Could not reset Cedar agent stats")

                    try:
                        import psycopg2

                        conn = psycopg2.connect(
                            host=db_config.host,
                            port=db_config.port,
                            user=pg_admin_user,
                            password=pg_admin_password,
                            dbname=db_name,
                        )
                        cur = conn.cursor()
                        cur.execute(
                            "SELECT has_table_privilege(%s, %s, 'SELECT'), has_table_privilege(%s, %s, 'UPDATE')",
                            (
                                BENCHMARK_USER,
                                "public.warehouse1",
                                BENCHMARK_USER,
                                "public.warehouse1",
                            ),
                        )
                        wh_sel, wh_upd = cur.fetchone()
                        cur.execute(
                            "SELECT has_schema_privilege(%s, %s, 'USAGE')",
                            (BENCHMARK_USER, "public"),
                        )
                        (schema_usage,) = cur.fetchone()
                        cur.execute(
                            "SELECT has_table_privilege(%s, %s, 'SELECT'), has_table_privilege(%s, %s, 'UPDATE')",
                            (
                                BENCHMARK_USER,
                                "public.customer1",
                                BENCHMARK_USER,
                                "public.customer1",
                            ),
                        )
                        cu_sel, cu_upd = cur.fetchone()
                        conn.close()
                        click.echo(
                            f"  Native privilege check: schema(public usage={schema_usage}) warehouse1(select={wh_sel}, update={wh_upd}) customer1(select={cu_sel}, update={cu_upd})"
                        )

                        if (wh_sel or wh_upd or cu_sel or cu_upd) and schema_usage:
                            try:
                                conn = psycopg2.connect(
                                    host=db_config.host,
                                    port=db_config.port,
                                    user=pg_admin_user,
                                    password=pg_admin_password,
                                    dbname=db_name,
                                )
                                cur = conn.cursor()

                                cur.execute(
                                    "SELECT c.relowner::regrole::text, c.relacl FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace WHERE n.nspname = 'public' AND c.relname = %s",
                                    ("warehouse1",),
                                )
                                wh_owner, wh_acl = cur.fetchone()

                                cur.execute(
                                    "SELECT c.relowner::regrole::text, c.relacl FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace WHERE n.nspname = 'public' AND c.relname = %s",
                                    ("customer1",),
                                )
                                cu_owner, cu_acl = cur.fetchone()

                                cur.execute(
                                    "SELECT r.rolname FROM pg_roles r JOIN pg_auth_members m ON m.roleid = r.oid JOIN pg_roles u ON u.oid = m.member WHERE u.rolname = %s ORDER BY r.rolname",
                                    (BENCHMARK_USER,),
                                )
                                inherited_roles = [row[0] for row in cur.fetchall()]

                                conn.close()
                                click.echo(
                                    f"  Native privilege debug: warehouse1(owner={wh_owner}, acl={wh_acl}) customer1(owner={cu_owner}, acl={cu_acl}) cedar_bench.member_of={inherited_roles}"
                                )
                            except Exception as _dbg_e:
                                click.echo(f"  Native privilege debug failed: {_dbg_e}")
                    except Exception as _e:
                        click.echo(f"  Native privilege check failed: {_e}")

                run_client_config = SysbenchTPCCConfig(
                    tpcc_lua_path=Path(tpcc_lua),
                    db_host=db_config.host,
                    db_port=db_config.port,
                    db_user=BENCHMARK_USER,
                    db_password=BENCHMARK_PASSWORD,
                    db_name=db_name,
                    db_driver="pgsql",
                    admin_user=pg_admin_user,
                    admin_password=pg_admin_password,
                    warehouses=warehouses,
                    tables=tables,
                    threads=threads,
                    duration=duration,
                    output_dir=output_path,
                )
                run_client = SysbenchTPCCClient(run_client_config)

                try:
                    click.echo(
                        f"  Running benchmark ({duration}s, {threads} threads, {tpcc_n_runs} runs)..."
                    )

                    # Multi-run: execute benchmark n_runs times
                    run_results = []

                    for run_idx in range(tpcc_n_runs):
                        click.echo(f"    Run {run_idx + 1}/{tpcc_n_runs}...")
                        result = run_client.run()
                        run_results.append(
                            {
                                "run": run_idx + 1,
                                "tpm": result.get("tpm", 0),
                                "avg_latency_ms": result.get("avg_latency_ms", 0),
                            }
                        )
                        click.echo(
                            f"      TPM: {result.get('tpm', 0):.1f}, Lat: {result.get('avg_latency_ms', 0):.2f}ms"
                        )

                    click.echo("  ✓ Benchmark complete")

                    # Aggregate results
                    import statistics

                    tpm_values = [r["tpm"] for r in run_results if r["tpm"] > 0]
                    lat_values = [
                        r["avg_latency_ms"]
                        for r in run_results
                        if r["avg_latency_ms"] > 0
                    ]

                    aggregate = {
                        "n_runs": len(run_results),
                        "tpm_median": statistics.median(tpm_values)
                        if tpm_values
                        else 0,
                        "tpm_mean": statistics.mean(tpm_values) if tpm_values else 0,
                        "tpm_std": statistics.stdev(tpm_values)
                        if len(tpm_values) > 1
                        else 0,
                        "avg_latency_median": statistics.median(lat_values)
                        if lat_values
                        else 0,
                        "runs": run_results,
                    }

                    click.echo(
                        f"  Aggregate: TPM={aggregate['tpm_median']:.1f}±{aggregate['tpm_std']:.1f}"
                    )

                    auth_stats = {}
                    cache_stats = {}
                    cedar_agent_stats = {}
                    if system == "postgres-cedar":
                        auth_stats = client.get_authorization_stats() or {}
                        cache_stats = client.get_authorization_cache_stats() or {}

                        cedar_agent_url = cfg.cedar_agent.url
                        cedar_agent_stats = get_cedar_agent_stats(cedar_agent_url) or {}

                        click.echo(
                            "  PostgreSQL extension stats: "
                            f"auth_requests={auth_stats.get('auth_requests', 0)}, "
                            f"grants={auth_stats.get('auth_grants', 0)}, "
                            f"denies={auth_stats.get('auth_denies', 0)}, "
                            f"ignores={auth_stats.get('auth_ignores', 0)}, "
                            f"errors={auth_stats.get('auth_errors', 0)}, "
                            f"sync_requests={auth_stats.get('sync_requests', 0)}, "
                            f"sync_successes={auth_stats.get('sync_successes', 0)}, "
                            f"sync_failures={auth_stats.get('sync_failures', 0)}, "
                            f"avg_total_time_ms={auth_stats.get('avg_total_time_ms', 0)}, "
                            f"avg_remote_time_ms={auth_stats.get('avg_remote_time_ms', 0)}"
                        )
                        click.echo(
                            f"  PostgreSQL cache stats: hits={cache_stats.get('hits', 0)}, misses={cache_stats.get('misses', 0)}, evictions={cache_stats.get('evictions', 0)}, entries={cache_stats.get('entries', 0)}"
                        )

                        cedar_requests = 0
                        for _k in (
                            "authorization_requests",
                            "auth_requests",
                            "is_authorized_requests",
                            "total_requests",
                            "total",
                            "requests",
                        ):
                            if _k in cedar_agent_stats:
                                cedar_requests = cedar_agent_stats.get(_k, 0)
                                break

                        cedar_allows = cedar_agent_stats.get(
                            "allow_count", cedar_agent_stats.get("allows", 0)
                        )
                        cedar_denies = cedar_agent_stats.get(
                            "deny_count", cedar_agent_stats.get("denies", 0)
                        )
                        cedar_errors = cedar_agent_stats.get(
                            "error_count", cedar_agent_stats.get("errors", 0)
                        )

                        click.echo(
                            f"  Cedar agent stats: requests={cedar_requests}, allows={cedar_allows}, denies={cedar_denies}, errors={cedar_errors}"
                        )

                        client.set_guc("pg_authorization.collect_stats", "off")

                    results[system] = {
                        "config": {
                            "system": system,
                            "warehouses": warehouses,
                            "tables": tables,
                            "threads": threads,
                            "duration": duration,
                            "n_runs": tpcc_n_runs,
                            "cache": cache,
                            "isolation": "READ COMMITTED",
                        },
                        "aggregate": aggregate,
                        "benchmark": run_results[-1] if run_results else {},
                        "auth_stats": auth_stats,
                        "cache_stats": cache_stats,
                        "cedar_agent_stats": cedar_agent_stats,
                        "success": True,
                    }

                finally:
                    # Cleanup Cedar entities/policies if explicitly requested OR always if we want temporary benchmark policies gone
                    # Per plan, we want to cleanup valid policies
                    if system == "postgres-cedar":
                        try:
                            client.cleanup_cedar_entities(cfg)
                        except Exception as e:
                            click.echo(
                                f"  Warning: Cleanup of Cedar entities failed: {e}"
                            )
            else:
                click.echo("  Skipping benchmark run (--no-run)")

        except Exception as e:
            click.echo(f"  ✗ Failed on {system}: {e}")
            failure_auth_stats = {}
            failure_cache_stats = {}
            failure_cedar_agent_stats = {}
            if system == "postgres-cedar":
                try:
                    failure_auth_stats = client.get_authorization_stats() or {}
                    failure_cache_stats = client.get_authorization_cache_stats() or {}

                    cedar_agent_url = cfg.cedar_agent.url
                    failure_cedar_agent_stats = (
                        get_cedar_agent_stats(cedar_agent_url) or {}
                    )

                    click.echo(
                        "  PostgreSQL extension stats at failure: "
                        f"auth_requests={failure_auth_stats.get('auth_requests', 0)}, "
                        f"grants={failure_auth_stats.get('auth_grants', 0)}, "
                        f"denies={failure_auth_stats.get('auth_denies', 0)}, "
                        f"ignores={failure_auth_stats.get('auth_ignores', 0)}, "
                        f"errors={failure_auth_stats.get('auth_errors', 0)}, "
                        f"sync_requests={failure_auth_stats.get('sync_requests', 0)}, "
                        f"sync_successes={failure_auth_stats.get('sync_successes', 0)}, "
                        f"sync_failures={failure_auth_stats.get('sync_failures', 0)}, "
                        f"avg_total_time_ms={failure_auth_stats.get('avg_total_time_ms', 0)}, "
                        f"avg_remote_time_ms={failure_auth_stats.get('avg_remote_time_ms', 0)}"
                    )
                    click.echo(
                        f"  PostgreSQL cache at failure: hits={failure_cache_stats.get('hits', 0)}, misses={failure_cache_stats.get('misses', 0)}, evictions={failure_cache_stats.get('evictions', 0)}, entries={failure_cache_stats.get('entries', 0)}"
                    )

                    cedar_requests = 0
                    for _k in (
                        "authorization_requests",
                        "auth_requests",
                        "is_authorized_requests",
                        "total_requests",
                        "total",
                        "requests",
                    ):
                        if _k in failure_cedar_agent_stats:
                            cedar_requests = failure_cedar_agent_stats.get(_k, 0)
                            break

                    cedar_allows = failure_cedar_agent_stats.get(
                        "allow_count", failure_cedar_agent_stats.get("allows", 0)
                    )
                    cedar_denies = failure_cedar_agent_stats.get(
                        "deny_count", failure_cedar_agent_stats.get("denies", 0)
                    )
                    cedar_errors = failure_cedar_agent_stats.get(
                        "error_count", failure_cedar_agent_stats.get("errors", 0)
                    )

                    click.echo(
                        f"  Cedar agent stats at failure: requests={cedar_requests}, allows={cedar_allows}, denies={cedar_denies}, errors={cedar_errors}"
                    )
                except Exception:
                    pass

            results[system] = {
                "error": str(e),
                "auth_stats": failure_auth_stats,
                "cache_stats": failure_cache_stats,
                "cedar_agent_stats": failure_cedar_agent_stats,
            }

    output_path = (
        Path(output_dir)
        if output_dir
        else Path(cfg.output.results_dir) / cfg.experiment_tag / "tpcc"
    )
    output_path.mkdir(parents=True, exist_ok=True)
    results_file = output_path / "sysbench_tpcc_postgres_results.json"
    results_file.write_text(json.dumps(results, indent=2))

    click.echo(f"\n✓ Results saved to {results_file}")

    # Compare results
    if (
        "postgres-baseline" in results
        and "postgres-cedar" in results
        and "error" not in results["postgres-baseline"]
    ):
        base_bench = results["postgres-baseline"].get("benchmark", {})
        cedar_bench = results["postgres-cedar"].get("benchmark", {})

        baseline_tpm = base_bench.get("tpm", 0)
        cedar_tpm = cedar_bench.get("tpm", 0)

        if baseline_tpm > 0:
            from framework.stats import calculate_overhead_metrics

            oh = calculate_overhead_metrics(baseline_tpm, cedar_tpm, is_throughput=True)
            click.echo(
                f"\nThroughput overhead: {oh['overhead_pct']:+.2f}% ({oh['overhead_factor']:.2f}x slowdown)"
            )

            # Warn if overhead is negative (suspicious)
            if oh["overhead_pct"] < 0:
                click.echo(
                    "\n⚠ WARNING: Negative overhead detected (Cedar faster than baseline)!",
                    err=True,
                )
                click.echo("  This is suspicious and may indicate:", err=True)
                click.echo(
                    "  1. Cedar authorization is not being invoked (check auth_stats)",
                    err=True,
                )
                click.echo("  2. Configuration differences between databases", err=True)
                click.echo("  3. Run order bias (consider re-running)", err=True)


@tpcc.command("tpcc-mysql")
@click.option("--config", type=click.Path(exists=True), help="Path to YAML/JSON config")
@click.option(
    "--tpcc-home", type=click.Path(exists=True), help="Path to tpcc-mysql installation"
)
@click.option(
    "--warehouses", default=None, type=int, help="Number of warehouses (scale factor)"
)
@click.option(
    "--connections", default=None, type=int, help="Number of concurrent connections"
)
@click.option(
    "--duration", default=None, type=int, help="Benchmark duration in seconds"
)
@click.option("--output-dir", default=None, help="Output directory for results")
@click.option(
    "--profile/--no-profile",
    default=False,
    help="Collect Performance Schema profiling data",
)
@click.option(
    "--n-runs",
    default=None,
    type=int,
    help="Number of independent runs for statistical rigor (default: from config)",
)
def tpcc_mysql(
    config, tpcc_home, warehouses, connections, duration, output_dir, profile, n_runs
):
    """Run TPC-C benchmark using tpcc-mysql (native C implementation).

    Runs benchmark n_runs times for statistical rigor.
    """
    cfg = load_config_file(config)

    # Use config values if not provided via CLI
    tpcc_home = tpcc_home or cfg.tpcc.tpcc_mysql_home
    if not tpcc_home:
        raise click.ClickException(
            "tpcc-mysql home must be provided via --tpcc-home or tpcc.tpcc_mysql_home in config"
        )

    warehouses = warehouses if warehouses is not None else cfg.tpcc.warehouses
    connections = connections if connections is not None else cfg.tpcc.connections
    duration = duration if duration is not None else cfg.tpcc.duration_s
    tpcc_n_runs = n_runs if n_runs is not None else cfg.tpcc.n_runs

    click.echo("=" * 60)
    click.echo("TPC-C BENCHMARK (tpcc-mysql)")
    click.echo("=" * 60)
    click.echo(f"Scale factor: {warehouses} warehouses")
    click.echo(f"Connections: {connections}")
    click.echo(f"Duration: {duration} seconds")
    click.echo(f"Runs per system: {tpcc_n_runs}")
    click.echo(f"tpcc-mysql: {tpcc_home}")

    # Check tpcc-mysql installation
    status = TPCCMySQLClient.check_installation(Path(tpcc_home))
    if not status["installed"]:
        raise click.ClickException(
            f"tpcc-mysql not properly installed: {status['issues']}"
        )

    # Run benchmark for both systems
    results = {}
    for system in ["baseline", "cedar"]:
        click.echo(f"\n--- Running on {system.upper()} ({tpcc_n_runs} runs) ---")

        db_config = cfg.databases[system]
        if db_config.type != "mysql":
            raise click.ClickException(
                f"TPC-C tpcc-mysql requires MySQL for '{system}', but config has type '{db_config.type}'."
            )

        # Force-apply authorization cache settings if running on Cedar (Fix for missing cache)
        if system == "cedar":
            _apply_and_check_mysql_cedar_sysvars(
                cfg=cfg,
                db_config=db_config,
                admin_user=db_config.user,
                admin_password=db_config.password,
                label="tpcc/tpcc-mysql preflight",
            )

            try:
                import mysql.connector

                # Get cache config from cfg.cedar_agent.plugins
                plugins_cfg = (
                    cfg.cedar_agent.plugins
                    if cfg.cedar_agent and cfg.cedar_agent.plugins
                    else {}
                )
                cedar_auth_cfg = plugins_cfg.get("cedar_authorization", {})

                cache_enabled = cedar_auth_cfg.get("cache_enabled", True)
                cache_size = cedar_auth_cfg.get("cache_size", 1000)
                cache_ttl = cedar_auth_cfg.get("cache_ttl", 300)
                log_info = cedar_auth_cfg.get("log_info", False)
                enable_column_access = cedar_auth_cfg.get("enable_column_access", False)

                collect_stats = bool(cedar_auth_cfg.get("collect_stats", False))

                click.echo(
                    f"  Configuring Cedar cache: enabled={cache_enabled}, size={cache_size}, ttl={cache_ttl}"
                )

                conn = mysql.connector.connect(
                    host=db_config.host,
                    port=db_config.port,
                    user=db_config.user,
                    password=db_config.password,
                    database="tpcc",
                )
                cursor = conn.cursor()

                enabled_val = "ON" if cache_enabled else "OFF"
                log_info_val = "ON" if log_info else "OFF"
                enable_column_access_val = "ON" if enable_column_access else "OFF"
                collect_stats_val = "ON" if collect_stats else "OFF"
                cursor.execute(
                    f"SET GLOBAL cedar_authorization_cache_enabled = {enabled_val}"
                )
                cursor.execute(
                    f"SET GLOBAL cedar_authorization_cache_size = {cache_size}"
                )
                cursor.execute(
                    f"SET GLOBAL cedar_authorization_cache_ttl = {cache_ttl}"
                )
                cursor.execute(
                    f"SET GLOBAL cedar_authorization_log_info = {log_info_val}"
                )
                cursor.execute(
                    f"SET GLOBAL cedar_authorization_enable_column_access = {enable_column_access_val}"
                )
                cursor.execute(
                    f"SET GLOBAL cedar_authorization_collect_stats = {collect_stats_val}"
                )
                cursor.execute("SET GLOBAL cedar_authorization_cache_flush = 1")

                conn.commit()
                cursor.close()
                conn.close()
                click.echo("  ✓ Applied cache configuration via SET GLOBAL")

            except Exception as e:
                click.echo(f"  ⚠ Warning: Failed to apply cache settings: {e}")

        try:
            output_path = (
                Path(output_dir)
                if output_dir
                else Path(cfg.output.results_dir) / cfg.experiment_tag / "tpcc"
            )

            def run_tpcc():
                return run_tpcc_mysql_benchmark(
                    tpcc_home=Path(tpcc_home),
                    db_config={
                        "host": db_config.host,
                        "port": db_config.port,
                        "user": db_config.user,
                        "password": db_config.password,
                        "database": "tpcc",
                    },
                    warehouses=warehouses,
                    connections=connections,
                    duration=duration,
                    output_dir=output_path,
                )

            # Multi-run: execute benchmark n_runs times
            run_results = []

            for run_idx in range(tpcc_n_runs):
                click.echo(f"  Run {run_idx + 1}/{tpcc_n_runs}...")
                if profile and run_idx == 0:
                    # Only profile the first run to avoid overhead on all runs
                    from framework.differential_profiling import (
                        mysql_collect_profile_generic,
                    )

                    prof_out = (
                        output_path / "profiling" / f"mysql_{system}_perf_schema.json"
                    )
                    res = mysql_collect_profile_generic(cfg, system, run_tpcc, prof_out)
                    result = res["result"]
                    click.echo(f"    ✓ Performance Schema profile saved to {prof_out}")
                else:
                    result = run_tpcc()

                bench = result["benchmark"]
                run_data = {
                    "run": run_idx + 1,
                    "tpm": bench.get("tpm", 0),
                    "new_order_avg_ms": bench.get("new_order_avg_ms", 0),
                    # Keep raw metrics for debugging but don't duplicate full output to save space
                    "raw_metrics": bench,
                }
                run_results.append(run_data)

                if "tpm" in bench:
                    click.echo(f"    TPM (Transactions/min): {bench['tpm']:.1f}")
                if "new_order_avg_ms" in bench:
                    click.echo(
                        f"    New Order avg latency: {bench['new_order_avg_ms']:.2f} ms"
                    )

            # Aggregate results
            import statistics

            tpm_values = [r["tpm"] for r in run_results if r["tpm"] > 0]
            lat_values = [
                r["new_order_avg_ms"] for r in run_results if r["new_order_avg_ms"] > 0
            ]

            aggregate = {
                "n_runs": len(run_results),
                "tpm_median": statistics.median(tpm_values) if tpm_values else 0,
                "tpm_mean": statistics.mean(tpm_values) if tpm_values else 0,
                "tpm_std": statistics.stdev(tpm_values) if len(tpm_values) > 1 else 0,
                "new_order_avg_ms_median": statistics.median(lat_values)
                if lat_values
                else 0,
                "runs": run_results,
            }

            results[system] = {
                "config": {
                    "warehouses": warehouses,
                    "connections": connections,
                    "duration": duration,
                    "n_runs": tpcc_n_runs,
                },
                "aggregate": aggregate,
                # Backward compatibility for analysis scripts expecting direct benchmark dict
                "benchmark": {
                    "tpm": aggregate["tpm_median"],
                    "new_order_avg_ms": aggregate["new_order_avg_ms_median"],
                },
            }

            click.echo(
                f"  Aggregate: TPM={aggregate['tpm_median']:.1f}±{aggregate['tpm_std']:.1f}"
            )

        except Exception as e:
            click.echo(f"  ✗ Failed on {system}: {e}")
            results[system] = {"error": str(e)}

        if system == "cedar":
            try:
                _print_cedar_agent_stats(cfg=cfg, label="tpcc/tpcc-mysql end")
            except Exception:
                pass

    # Save results
    output_path = (
        Path(output_dir)
        if output_dir
        else Path(cfg.output.results_dir) / cfg.experiment_tag / "tpcc"
    )
    output_path.mkdir(parents=True, exist_ok=True)
    results_file = output_path / "tpcc_mysql_results.json"
    results_file.write_text(json.dumps(results, indent=2))

    click.echo(f"\n✓ Results saved to {results_file}")

    # Compare results if both succeeded
    if (
        "baseline" in results
        and "cedar" in results
        and "error" not in results["baseline"]
    ):
        baseline_tpm = results["baseline"]["benchmark"].get("tpm", 0)
        cedar_tpm = results["cedar"]["benchmark"].get("tpm", 0)

        if baseline_tpm > 0:
            from framework.stats import calculate_overhead_metrics

            oh = calculate_overhead_metrics(baseline_tpm, cedar_tpm, is_throughput=True)
            click.echo(
                f"\nThroughput overhead: {oh['overhead_pct']:+.2f}% ({oh['overhead_factor']:.2f}x slowdown)"
            )


@cli.group("ddl")
def ddl():
    """DDL operations testing for authorization plugins."""
    pass


@ddl.command("test")
@click.option("--config", type=click.Path(exists=True), help="Path to YAML/JSON config")
@click.option(
    "--suite",
    type=click.Choice(["comprehensive", "tpcc"]),
    default="comprehensive",
    help="DDL test suite to run",
)
@click.option(
    "--system",
    type=click.Choice(["baseline", "cedar", "both"]),
    default="both",
    help="Which system to test",
)
@click.option("--output-dir", default=None, help="Output directory for results")
def ddl_test(config, suite, system, output_dir):
    """Test DDL operations against authorization plugins."""
    cfg = load_config_file(config)

    click.echo("=" * 60)
    click.echo(f"DDL OPERATIONS TEST ({suite.upper()})")
    click.echo("=" * 60)
    click.echo(f"Testing: {system}")
    click.echo(f"Suite: {suite}")

    try:
        results = run_ddl_audit_test(cfg, suite, system)

        # Save results
        output_path = (
            Path(output_dir)
            if output_dir
            else Path(cfg.output.results_dir) / cfg.experiment_tag / "ddl"
        )
        output_path.mkdir(parents=True, exist_ok=True)
        results_file = output_path / f"ddl_{suite}_results.json"
        results_file.write_text(json.dumps(results, indent=2))

        # Report summary
        summary = results["summary"]
        for sys_name in ["baseline", "cedar"]:
            if sys_name in summary:
                stats = summary[sys_name]
                click.echo(f"\n{sys_name.upper()} Results:")
                click.echo(f"  Total operations: {stats['total']}")
                click.echo(f"  Authorized: {stats['authorized']}")
                click.echo(f"  Denied: {stats['denied']}")
                click.echo(f"  Errors: {stats['errors']}")
                click.echo(f"  Avg time: {stats['avg_time_ms']:.2f} ms")

        # Report verification
        if "cedar_verification" in results:
            verif = results["cedar_verification"]
            click.echo("\nCEDAR ENTITY VERIFICATION:")
            if verif["success"]:
                click.echo(
                    f"  ✓ Cedar agent contains {verif['entity_count']} total entities"
                )
                for etype, count in verif["entity_types"].items():
                    click.echo(f"    - {etype}: {count}")
            else:
                click.echo(f"  ✗ Verification failed: {verif['error']}")

        click.echo(f"\n✓ Results saved to {results_file}")

    except Exception as e:
        raise click.ClickException(f"DDL test failed: {e}")


@ddl.command("tpcc-schema")
@click.option("--config", type=click.Path(exists=True), help="Path to YAML/JSON config")
@click.option(
    "--tpcc-tool",
    type=click.Choice(["tpcc-mysql", "sysbench-tpcc"]),
    default="sysbench-tpcc",
    help="TPC-C tool to extract DDL from",
)
@click.option(
    "--tool-home",
    type=click.Path(exists=True),
    help="Path to TPC-C tool installation (binary or lua path)",
)
@click.option(
    "--system",
    type=click.Choice(["baseline", "cedar", "both"]),
    default="both",
    help="Which system to test",
)
@click.option("--output-dir", default=None, help="Output directory for results")
def ddl_tpcc_schema(config, tpcc_tool, tool_home, system, output_dir):
    """Test DDL operations from TPC-C schema creation."""
    cfg = load_config_file(config)

    # Resolve tool_home from config if not provided
    if not tool_home:
        if tpcc_tool == "tpcc-mysql":
            tool_home = cfg.tpcc.tpcc_mysql_home
        elif tpcc_tool == "sysbench-tpcc":
            tool_home = cfg.tpcc.tpcc_lua_path

    if not tool_home:
        raise click.ClickException(
            f"Tool home/path for {tpcc_tool} must be provided via --tool-home or config"
        )

    click.echo("=" * 60)
    click.echo("TPC-C SCHEMA DDL TEST")
    click.echo("=" * 60)
    click.echo(f"Tool: {tpcc_tool}")
    click.echo(f"Testing: {system}")

    # Get DDL statements from the TPC-C tool
    if tpcc_tool == "tpcc-mysql":
        from framework.tpcc_mysql_client import TPCCMySQLClient, TPCCMySQLConfig

        config_obj = TPCCMySQLConfig(tpcc_home=Path(tool_home))
        client = TPCCMySQLClient(config_obj)
    else:  # sysbench-tpcc
        from framework.sysbench_tpcc_client import (
            SysbenchTPCCClient,
            SysbenchTPCCConfig,
        )

        config_obj = SysbenchTPCCConfig(
            tpcc_lua_path=Path(tool_home),
            tables=cfg.tpcc.scale
            if hasattr(cfg.tpcc, "scale") and cfg.tpcc.scale
            else 1,
        )
        client = SysbenchTPCCClient(config_obj)

    ddl_statements = client.get_ddl_operations()

    click.echo(f"Testing {len(ddl_statements)} DDL operations...")

    tester = DDLOperationsTester(cfg)
    results = tester.test_tpcc_schema_creation(ddl_statements, system)

    # Save results
    output_path = (
        Path(output_dir)
        if output_dir
        else Path(cfg.output.results_dir) / cfg.experiment_tag / "ddl"
    )
    output_path.mkdir(parents=True, exist_ok=True)
    results_file = output_path / f"tpcc_schema_{tpcc_tool}_results.json"
    results_file.write_text(json.dumps(results, indent=2))

    # Report summary
    summary = results["summary"]
    for sys_name in ["baseline", "cedar"]:
        if sys_name in summary:
            stats = summary[sys_name]
            click.echo(f"\n{sys_name.upper()} Results:")
            click.echo(f"  Total DDL operations: {stats['total']}")
            click.echo(f"  Authorized: {stats['authorized']}")
            click.echo(f"  Denied: {stats['denied']}")
            click.echo(f"  Errors: {stats['errors']}")

    # Report verification
    if "cedar_verification" in results:
        verif = results["cedar_verification"]
        click.echo("\nCEDAR ENTITY VERIFICATION:")
        if verif["success"]:
            click.echo(
                f"  ✓ Cedar agent contains {verif['entity_count']} total entities"
            )
            for etype, count in verif["entity_types"].items():
                click.echo(f"    - {etype}: {count}")
        else:
            click.echo(f"  ✗ Verification failed: {verif['error']}")

    click.echo(f"\n✓ Results saved to {results_file}")


@cli.group("pgbench")
def pgbench():
    """PostgreSQL pgbench benchmarking commands."""
    pass


@pgbench.command("run")
@click.option("--config", type=click.Path(exists=True), help="Path to YAML/JSON config")
@click.option(
    "--db-system",
    type=click.Choice(["postgres-baseline", "postgres-cedar"]),
    default="postgres-baseline",
    help="PostgreSQL system to benchmark",
)
@click.option("--scale", default=None, type=int, help="pgbench scale factor")
@click.option("--clients", default=None, type=int, help="Number of concurrent clients")
@click.option(
    "--jobs",
    default=None,
    type=int,
    help="Number of pgbench worker threads (defaults to jobs=clients)",
)
@click.option(
    "--duration", default=None, type=int, help="Benchmark duration in seconds"
)
@click.option("--warmup", default=None, type=int, help="Warmup duration in seconds")
@click.option(
    "--builtin",
    default=None,
    type=click.Choice(["tpcb-like", "simple-update", "select-only"]),
    help="Built-in pgbench test",
)
@click.option(
    "--query-mode",
    default="simple",
    type=click.Choice(["simple", "extended", "prepared"]),
    help="PostgreSQL query protocol to use",
)
@click.option("--output-dir", default=None, help="Output directory for results")
@click.option(
    "--profile/--no-profile",
    default=False,
    help="Collect EXPLAIN ANALYZE profiling data",
)
@click.option(
    "--strace/--no-strace",
    default=False,
    help="Collect strace profiling data (requires docker & privileged)",
)
@click.option(
    "--strace-duration",
    default=5,
    type=int,
    help="Duration of strace collection in seconds",
)
@click.option(
    "--perf/--no-perf",
    default=False,
    help="Collect perf profiling data (requires docker & privileged)",
)
@click.option(
    "--perf-duration",
    default=5,
    type=int,
    help="Duration of perf collection in seconds",
)
@click.option(
    "--perf-record/--no-perf-record",
    default=False,
    help="Collect detailed perf record with call graphs",
)
@click.option(
    "--cache/--no-cache", default=True, help="Enable/disable pg_authorization cache"
)
def pgbench_run(
    config,
    db_system,
    scale,
    clients,
    jobs,
    duration,
    warmup,
    builtin,
    query_mode,
    output_dir,
    profile,
    strace,
    strace_duration,
    perf,
    perf_duration,
    perf_record,
    cache,
):
    """Run pgbench benchmark on PostgreSQL system."""
    cfg = load_config_file(config)

    # Use config values if not provided via CLI
    scale = scale if scale is not None else cfg.pgbench.scale
    clients = clients if clients is not None else cfg.pgbench.clients
    jobs = jobs if jobs is not None else (cfg.pgbench.jobs or clients)
    duration = duration if duration is not None else cfg.pgbench.duration
    warmup = warmup if warmup is not None else cfg.pgbench.warmup
    builtin = builtin if builtin is not None else cfg.pgbench.builtin

    click.echo("=" * 60)
    click.echo(f"POSTGRESQL PGBENCH BENCHMARK ({db_system.upper()})")
    click.echo("=" * 60)
    click.echo(f"Scale factor: {scale}")
    click.echo(f"Clients: {clients}")
    click.echo(f"Jobs: {jobs}")
    click.echo(f"Duration: {duration}s")
    click.echo(f"Warmup: {warmup}s")
    click.echo(f"Test: {builtin}")
    click.echo(f"Query Mode: {query_mode}")
    click.echo(f"Cache: {'enabled' if cache else 'disabled'}")

    # Determine a suffix for the filename to avoid overwriting different experiment types
    suffix = ""
    if perf_record:
        suffix = "_perf-record"
    elif perf:
        suffix = "_perf"
    elif strace:
        suffix = "_strace"
    elif profile:
        suffix = "_explain"

    if not cache:
        suffix += "_no-cache"

    try:
        # Save results path setup
        output_path = (
            Path(output_dir)
            if output_dir
            else Path("results") / cfg.experiment_tag / "pgbench"
        )
        output_path.mkdir(parents=True, exist_ok=True)
        results_file = (
            output_path
            / f"pgbench_{db_system}_{scale}_{clients}_{duration}{suffix}.json"
        )

        perf_record_path = None
        if perf_record:
            perf_record_path = results_file.with_suffix(".perf.txt")

        cedar_gucs = {
            "pg_authorization.cedar_agent_url": "'http://cedar-agent:8180'",
            "pg_authorization.namespace": "'PostgreSQL'",
            "pg_authorization.cache_enabled": "on" if cache else "off",
            "pg_authorization.collect_stats": "off",
        }

        results = run_pgbench_experiment(
            cfg,
            db_system,
            scale,
            clients,
            jobs,
            duration,
            builtin,
            warmup,
            query_mode=query_mode,
            profile=profile,
            strace=strace,
            strace_duration=strace_duration,
            perf=perf,
            perf_duration=perf_duration,
            perf_record=perf_record,
            perf_record_path=str(perf_record_path) if perf_record_path else None,
            cedar_gucs=cedar_gucs,
            results_suffix=suffix.lstrip("_"),
        )

        if profile:
            from framework.differential_profiling import (
                postgres_collect_explain_profile,
            )

            # Sample pgbench queries for EXPLAIN ANALYZE
            pgbench_queries = _get_pgbench_profiling_queries(
                builtin, None
            )  # builtin tests

            prof_out = output_path / "profiling" / f"postgres_{db_system}_explain.json"
            postgres_collect_explain_profile(
                cfg, db_system, prof_out, queries=pgbench_queries
            )
            click.echo(f"  ✓ PostgreSQL EXPLAIN profile saved to {prof_out}")

        results_file.write_text(json.dumps(results, indent=2))

        # Report results
        if results.get("success"):
            click.echo("\nResults:")
            click.echo(f"  TPS: {results['tps']:.1f}")
            click.echo(f"  Avg Latency: {results['avg_latency_ms']:.2f} ms")
            click.echo(
                f"  Transactions: {results['results']['benchmark']['transactions_processed']}"
            )
        else:
            click.echo(f"✗ Benchmark failed: {results.get('error', 'Unknown error')}")

        click.echo(f"\n✓ Results saved to {results_file}")

    except Exception as e:
        raise click.ClickException(f"pgbench failed: {e}")


@pgbench.command("compare")
@click.option("--config", type=click.Path(exists=True), help="Path to YAML/JSON config")
@click.option("--scale", default=None, type=int, help="pgbench scale factor")
@click.option("--clients", default=None, type=int, help="Number of concurrent clients")
@click.option(
    "--jobs",
    default=None,
    type=int,
    help="Number of pgbench worker threads (defaults to jobs=clients)",
)
@click.option(
    "--duration", default=None, type=int, help="Benchmark duration in seconds"
)
@click.option("--warmup", default=None, type=int, help="Warmup duration in seconds")
@click.option(
    "--builtin",
    default=None,
    type=click.Choice(["tpcb-like", "simple-update", "select-only"]),
    help="Built-in pgbench test",
)
@click.option(
    "--query-mode",
    default="simple",
    type=click.Choice(["simple", "extended", "prepared"]),
    help="PostgreSQL query protocol to use",
)
@click.option("--output-dir", default=None, help="Output directory for results")
@click.option(
    "--profile/--no-profile",
    default=False,
    help="Collect EXPLAIN ANALYZE profiling data",
)
@click.option(
    "--strace/--no-strace", default=False, help="Collect strace profiling data"
)
@click.option("--perf/--no-perf", default=False, help="Collect perf profiling data")
@click.option(
    "--perf-duration", default=5, type=int, help="Duration of perf collection"
)
@click.option(
    "--perf-record/--no-perf-record", default=False, help="Collect detailed perf record"
)
@click.option(
    "--strace-duration", default=5, type=int, help="Duration of strace collection"
)
@click.option(
    "--cache/--no-cache", default=True, help="Enable/disable pg_authorization cache"
)
@click.option(
    "--n-runs",
    default=None,
    type=int,
    help="Number of independent runs for statistical rigor (default: from config)",
)
def pgbench_compare(
    config,
    scale,
    clients,
    jobs,
    duration,
    warmup,
    builtin,
    query_mode,
    output_dir,
    profile,
    strace,
    perf,
    perf_duration,
    perf_record,
    strace_duration,
    cache,
    n_runs,
):
    """Compare pgbench performance between baseline and Cedar PostgreSQL.

    Runs benchmark n_runs times for statistical rigor.
    """
    _run_pgbench_compare(
        config,
        scale,
        clients,
        jobs,
        duration,
        warmup,
        builtin,
        output_dir,
        profile,
        query_mode=query_mode,
        strace=strace,
        strace_duration=strace_duration,
        perf=perf,
        perf_duration=perf_duration,
        perf_record=perf_record,
        cache=cache,
        n_runs=n_runs,
    )


def _get_pgbench_profiling_queries(builtin, script):
    """Get a list of sample queries for EXPLAIN ANALYZE profiling."""
    if script:
        try:
            with open(script) as f:
                content = f.read()
            # Simple extraction: find lines starting with SQL keywords
            queries = []
            for line in content.split("\n"):
                line = line.strip()
                if not line or line.startswith("\\") or line.startswith("--"):
                    continue
                # Take first word and check if it's a SQL keyword
                parts = line.split()
                if not parts:
                    continue
                first_word = parts[0].upper().rstrip(";")
                if first_word in (
                    "SELECT",
                    "INSERT",
                    "UPDATE",
                    "DELETE",
                    "BEGIN",
                    "COMMIT",
                ):
                    # Basic cleanup of variables like :aid
                    clean_query = (
                        line.replace(":aid", "1")
                        .replace(":tid", "1")
                        .replace(":bid", "1")
                        .replace(":delta", "100")
                        .replace(":scale", "1")
                        .replace(":user", "'bench_user'")
                    )
                    if not clean_query.endswith(";"):
                        clean_query += ";"
                    queries.append(clean_query)
            return queries
        except Exception:
            pass

    if builtin == "tpcb-like":
        return [
            "UPDATE pgbench_accounts SET abalance = abalance + 100 WHERE aid = 1;",
            "SELECT abalance FROM pgbench_accounts WHERE aid = 1;",
            "UPDATE pgbench_tellers SET tbalance = tbalance + 100 WHERE tid = 1;",
            "UPDATE pgbench_branches SET bbalance = bbalance + 100 WHERE bid = 1;",
            "INSERT INTO pgbench_history (tid, bid, aid, delta, mtime) VALUES (1, 1, 1, 100, CURRENT_TIMESTAMP);",
        ]
    elif builtin == "select-only":
        return ["SELECT abalance FROM pgbench_accounts WHERE aid = 1;"]
    elif builtin == "simple-update":
        return [
            "UPDATE pgbench_accounts SET abalance = abalance + 100 WHERE aid = 1;",
            "SELECT abalance FROM pgbench_accounts WHERE aid = 1;",
            "INSERT INTO pgbench_history (tid, bid, aid, delta, mtime) VALUES (1, 1, 1, 100, CURRENT_TIMESTAMP);",
        ]
    return []


def _run_pgbench_compare(
    config,
    scale,
    clients,
    jobs,
    duration,
    warmup,
    builtin,
    output_dir,
    profile,
    script=None,
    cedar_gucs=None,
    query_mode="simple",
    strace=False,
    strace_duration=5,
    perf=False,
    perf_duration=5,
    perf_record=False,
    cache=True,
    n_runs=None,
):
    """Internal helper to run pgbench comparison with multi-run support."""
    cfg = load_config_file(config)

    # Use config values if not provided via CLI
    scale = scale if scale is not None else cfg.pgbench.scale
    clients = clients if clients is not None else cfg.pgbench.clients
    jobs = jobs if jobs is not None else (cfg.pgbench.jobs or clients)
    duration = duration if duration is not None else cfg.pgbench.duration
    warmup = warmup if warmup is not None else cfg.pgbench.warmup
    builtin = builtin if builtin is not None else cfg.pgbench.builtin
    pgbench_n_runs = n_runs if n_runs is not None else cfg.pgbench.n_runs

    click.echo("=" * 60)
    click.echo("POSTGRESQL PGBENCH COMPARISON")
    if script:
        click.echo(f"MODE: NO-CACHE (Script: {script})")
    click.echo("=" * 60)
    click.echo(f"Scale factor: {scale}")
    click.echo(f"Clients: {clients}")
    click.echo(f"Jobs: {jobs}")
    click.echo(f"Duration: {duration}s")
    click.echo(f"Warmup: {warmup}s")
    click.echo(f"Runs per system: {pgbench_n_runs}")
    if not script:
        click.echo(f"Test: {builtin}")
    click.echo(f"Cache: {'enabled' if cache else 'disabled'}")

    # Determine a suffix for the filename
    suffix = ""
    if perf_record:
        suffix = "_perf-record"
    elif perf:
        suffix = "_perf"
    elif strace:
        suffix = "_strace"
    elif profile:
        suffix = "_explain"

    if not cache:
        suffix += "_no-cache"

    try:
        from framework.pgbench_runner import (
            run_pgbench_experiment,
        )

        # Determine output path early
        tag = "pgbench_no_cache" if script else "pgbench"
        output_path = (
            Path(output_dir)
            if output_dir
            else Path("results") / cfg.experiment_tag / tag
        )
        output_path.mkdir(parents=True, exist_ok=True)
        results_file = (
            output_path
            / f"pgbench_comparison_{scale}_{clients}_{duration}{suffix}.json"
        )

        results = {
            "comparison": "postgres-baseline vs postgres-cedar",
            "results_suffix": suffix.lstrip("_"),
            "config": {
                "scale": scale,
                "clients": clients,
                "jobs": jobs,
                "duration": duration,
                "builtin": builtin if not script else None,
                "script": script,
                "warmup": warmup,
                "n_runs": pgbench_n_runs,
            },
            "systems": {},
        }

        for system in ["postgres-baseline", "postgres-cedar"]:
            click.echo(f"\nTesting {system} ({pgbench_n_runs} runs)...")

            # Prepare GUCs for Cedar
            current_gucs = cedar_gucs.copy() if cedar_gucs else {}
            if system == "postgres-cedar":
                current_gucs["pg_authorization.cedar_agent_url"] = (
                    "'http://cedar-agent:8180'"
                )
                current_gucs["pg_authorization.namespace"] = "'PostgreSQL'"
                current_gucs["pg_authorization.cache_enabled"] = (
                    "on" if cache else "off"
                )
                current_gucs["pg_authorization.collect_stats"] = "off"

            perf_record_path = None
            if perf_record:
                perf_record_path = (
                    output_path
                    / f"pgbench_comparison_{scale}_{clients}_{duration}{suffix}_{system}.perf.txt"
                )

            # Prepare dataset + user + Cedar registration once per system
            click.echo("  Preparing dataset...")
            _ = run_pgbench_experiment(
                cfg,
                system,
                scale,
                clients,
                jobs,
                duration,
                builtin,
                0,
                script=script,
                profile=False,
                query_mode=query_mode,
                strace=False,
                perf=False,
                perf_record=False,
                cedar_gucs=current_gucs if current_gucs else None,
                results_suffix=suffix.lstrip("_"),
                prepare=True,
                benchmark=False,
            )

            # Optional warm cache pass (discarded)
            if warmup and warmup > 0:
                click.echo(f"  Warmup: {warmup}s (discarded)")
                _ = run_pgbench_experiment(
                    cfg,
                    system,
                    scale,
                    clients,
                    jobs,
                    warmup,
                    builtin,
                    0,
                    script=script,
                    profile=False,
                    query_mode=query_mode,
                    strace=False,
                    perf=False,
                    perf_record=False,
                    perf_record_path=None,
                    cedar_gucs=current_gucs if current_gucs else None,
                    results_suffix=suffix.lstrip("_"),
                    prepare=False,
                    benchmark=True,
                )

            # Multi-run: execute benchmark n_runs times (no re-init)
            run_results = []

            for run_idx in range(pgbench_n_runs):
                click.echo(f"  Run {run_idx + 1}/{pgbench_n_runs}...")

                # If profiling is enabled, do it only on the first run
                enable_strace = bool(strace and run_idx == 0)
                enable_perf = bool(perf and run_idx == 0)
                enable_perf_record = bool(perf_record and run_idx == 0)

                system_result = run_pgbench_experiment(
                    cfg,
                    system,
                    scale,
                    clients,
                    jobs,
                    duration,
                    builtin,
                    0,
                    script=script,
                    profile=profile,
                    query_mode=query_mode,
                    strace=enable_strace,
                    strace_duration=strace_duration,
                    perf=enable_perf,
                    perf_duration=perf_duration,
                    perf_record=enable_perf_record,
                    perf_record_path=str(perf_record_path)
                    if perf_record_path
                    else None,
                    cedar_gucs=current_gucs if current_gucs else None,
                    results_suffix=suffix.lstrip("_"),
                    prepare=False,
                    benchmark=True,
                )
                run_results.append(
                    {
                        "run": run_idx + 1,
                        "tps": system_result.get("tps", 0),
                        "avg_latency_ms": system_result.get("avg_latency_ms", 0),
                    }
                )
                click.echo(
                    f"    TPS: {system_result.get('tps', 0):.1f}, Lat: {system_result.get('avg_latency_ms', 0):.2f}ms"
                )

            # Aggregate results
            import statistics

            tps_values = [r["tps"] for r in run_results if r["tps"] > 0]
            lat_values = [
                r["avg_latency_ms"] for r in run_results if r["avg_latency_ms"] > 0
            ]

            aggregate = {
                "n_runs": len(run_results),
                "tps_median": statistics.median(tps_values) if tps_values else 0,
                "tps_mean": statistics.mean(tps_values) if tps_values else 0,
                "tps_std": statistics.stdev(tps_values) if len(tps_values) > 1 else 0,
                "avg_latency_median": statistics.median(lat_values)
                if lat_values
                else 0,
                "runs": run_results,
            }

            results["systems"][system] = {
                "aggregate": aggregate,
                "tps": aggregate["tps_median"],  # Use median for comparison
                "avg_latency_ms": aggregate["avg_latency_median"],
            }

            click.echo(
                f"  Aggregate: TPS={aggregate['tps_median']:.1f}±{aggregate['tps_std']:.1f}"
            )

        # Calculate comparison metrics using median values
        baseline_tps = results["systems"]["postgres-baseline"].get("tps", 0)
        cedar_tps = results["systems"]["postgres-cedar"].get("tps", 0)

        if baseline_tps > 0:
            from framework.stats import calculate_overhead_metrics

            oh = calculate_overhead_metrics(baseline_tps, cedar_tps, is_throughput=True)
            results["overhead_percent"] = oh["overhead_pct"]
            results["overhead_factor"] = oh["overhead_factor"]
            results["baseline_tps"] = baseline_tps
            results["cedar_tps"] = cedar_tps

        # Save results
        results_file.write_text(json.dumps(results, indent=2))

        # Perform profiling if requested
        if profile:
            from framework.differential_profiling import (
                postgres_collect_explain_profile,
            )

            click.echo("\nCollecting EXPLAIN ANALYZE profiles for comparison...")

            queries = _get_pgbench_profiling_queries(builtin, script)
            if queries:
                for system in ["postgres-baseline", "postgres-cedar"]:
                    prof_out = (
                        output_path / "profiling" / f"postgres_{system}_explain.json"
                    )
                    try:
                        postgres_collect_explain_profile(
                            cfg, system, prof_out, queries=queries
                        )
                        click.echo(f"  ✓ {system} profile saved to {prof_out}")
                    except Exception as e:
                        click.echo(f"  ✗ {system} profiling failed: {e}")
            else:
                click.echo("  ! No queries found to profile.")

        click.echo("\nResults:")
        click.echo(f"  Baseline TPS: {baseline_tps:.1f}")
        click.echo(f"  Cedar TPS: {cedar_tps:.1f}")

        if "overhead_percent" in results:
            overhead = results["overhead_percent"]
            factor = results.get("overhead_factor", 1.0)
            click.echo(f"  Overhead: {overhead:+.2f}% ({factor:.2f}x slowdown)")

        click.echo(f"\n✓ Results saved to {results_file}")

    except Exception as e:
        raise click.ClickException(f"pgbench comparison failed: {e}")


@pgbench.command("no-cache")
@click.option("--config", type=click.Path(exists=True), help="Path to YAML/JSON config")
@click.option("--scale", default=None, type=int, help="pgbench scale factor")
@click.option("--clients", default=None, type=int, help="Number of concurrent clients")
@click.option(
    "--jobs",
    default=None,
    type=int,
    help="Number of pgbench worker threads (defaults to jobs=clients)",
)
@click.option(
    "--duration", default=None, type=int, help="Benchmark duration in seconds"
)
@click.option("--warmup", default=None, type=int, help="Warmup duration in seconds")
@click.option(
    "--query-mode",
    default="simple",
    type=click.Choice(["simple", "extended", "prepared"]),
    help="PostgreSQL query protocol to use",
)
@click.option("--output-dir", default=None, help="Output directory for results")
@click.option(
    "--profile/--no-profile",
    default=False,
    help="Collect EXPLAIN ANALYZE profiling data",
)
@click.option(
    "--strace/--no-strace", default=False, help="Collect strace profiling data"
)
@click.option("--perf/--no-perf", default=False, help="Collect perf profiling data")
@click.option(
    "--perf-duration", default=5, type=int, help="Duration of perf collection"
)
@click.option(
    "--perf-record/--no-perf-record", default=False, help="Collect detailed perf record"
)
@click.option(
    "--strace-duration", default=5, type=int, help="Duration of strace collection"
)
def pgbench_no_cache(
    config,
    scale,
    clients,
    jobs,
    duration,
    warmup,
    query_mode,
    output_dir,
    profile,
    strace,
    perf,
    perf_duration,
    perf_record,
    strace_duration,
):
    """Compare pgbench performance with caches disabled (using flush script)."""
    script_path = "workloads/pgbench_no_cache.sql"
    _run_pgbench_compare(
        config,
        scale,
        clients,
        jobs,
        duration,
        warmup,
        None,
        output_dir,
        profile,
        script=script_path,
        query_mode=query_mode,
        strace=strace,
        strace_duration=strace_duration,
        perf=perf,
        perf_duration=perf_duration,
        perf_record=perf_record,
        cache=False,
    )


@cli.group("semantics")
def semantics():
    """Semantic correctness testing for authorization failures."""
    pass


@semantics.command("test")
@click.option("--config", type=click.Path(exists=True), help="Path to YAML/JSON config")
@click.option(
    "--workload-dir",
    type=click.Path(exists=True),
    help="Directory containing workload with test queries",
)
@click.option(
    "--failure-scenarios",
    default="agent_unavailable,network_timeout,malformed_response",
    help="Comma-separated list of failure scenarios",
)
@click.option("--output-dir", default=None, help="Output directory for results")
def semantics_test(config, workload_dir, failure_scenarios, output_dir):
    """Test semantic correctness under failure scenarios."""
    cfg = load_config_file(config)

    # Resolve workload_dir
    if not workload_dir:
        if cfg.output.workload_dir:
            workload_dir = (
                Path(cfg.output.workload_dir) / cfg.experiment_tag / "benchmark"
            )
            if not workload_dir.exists():
                workload_dir = Path(cfg.output.workload_dir)
        else:
            raise click.ClickException(
                "Workload directory must be provided via --workload-dir or output.workload_dir in config"
            )

    click.echo("=" * 60)
    click.echo("SEMANTIC CORRECTNESS TEST")
    click.echo("=" * 60)
    click.echo(f"Workload: {workload_dir}")
    click.echo(f"Failure scenarios: {failure_scenarios}")

    # Load workload to get test queries
    from framework.workload_generator import Workload

    workload_path = Path(workload_dir) / "workload.json"
    if not workload_path.exists():
        raise click.ClickException(f"Workload not found: {workload_path}")

    workload = Workload.load(workload_path)

    # For this test, we'll classify queries as should_deny or should_allow
    # We'll use a mix of standard test cases and some from the workload
    from framework.failure_semantics_test import get_standard_security_test_cases

    standard_cases = get_standard_security_test_cases()

    should_deny_queries = []
    should_allow_queries = []

    for case in standard_cases:
        if case.expected_denied:
            should_deny_queries.extend(case.queries)
        else:
            should_allow_queries.extend(case.queries)

    # Add a sample from the workload (all of which are allowed by policy)
    # This provides a larger set for monotonicity and consistency tests
    should_allow_queries.extend(workload.queries[:47])

    scenarios = failure_scenarios.split(",")

    click.echo(f"Testing {len(should_deny_queries)} should-deny queries")
    click.echo(f"Testing {len(should_allow_queries)} should-allow queries")
    click.echo(f"Across {len(scenarios)} failure scenarios")

    try:
        results = run_semantic_correctness_tests(
            cfg, should_deny_queries, should_allow_queries, scenarios
        )

        # Save results
        output_path = (
            Path(output_dir)
            if output_dir
            else Path(cfg.output.results_dir) / cfg.experiment_tag / "semantics"
        )
        output_path.mkdir(parents=True, exist_ok=True)
        results_file = output_path / "semantic_correctness_results.json"
        results_file.write_text(json.dumps(results, indent=2))

        # Report summary
        summary = results["overall_summary"]
        click.echo("\nResults:")
        click.echo(
            f"  Fail-closed: {'PASS' if summary['fail_closed_pass'] else 'FAIL'}"
        )
        click.echo(
            f"  Monotonicity: {'PASS' if summary['monotonicity_pass'] else 'FAIL'}"
        )
        click.echo(
            f"  Consistency: {'PASS' if summary['consistency_pass'] else 'FAIL'}"
        )
        click.echo(f"  Total violations: {summary['total_violations']}")
        click.echo(f"  Overall: {'PASS' if summary['all_tests_pass'] else 'FAIL'}")

        click.echo(f"\n✓ Results saved to {results_file}")

    except Exception as e:
        raise click.ClickException(f"Semantic test failed: {e}")


@semantics.command("monotonicity")
@click.option("--config", type=click.Path(exists=True), help="Path to YAML/JSON config")
@click.option(
    "--workload-dir", type=click.Path(exists=True), help="Directory containing workload"
)
@click.option("--output-dir", default=None, help="Output directory for results")
def semantics_monotonicity(config, workload_dir, output_dir):
    """Test monotonicity property: Cedar ≤ Baseline authorization."""
    cfg = load_config_file(config)

    # Resolve workload_dir
    if not workload_dir:
        if cfg.output.workload_dir:
            workload_dir = (
                Path(cfg.output.workload_dir) / cfg.experiment_tag / "benchmark"
            )
            if not workload_dir.exists():
                workload_dir = Path(cfg.output.workload_dir)
        else:
            raise click.ClickException(
                "Workload directory must be provided via --workload-dir or output.workload_dir in config"
            )

    click.echo("=" * 60)
    click.echo("MONOTONICITY TEST")
    click.echo("=" * 60)

    # Load workload
    from framework.workload_generator import Workload

    workload_path = Path(workload_dir) / "workload.json"
    if not workload_path.exists():
        raise click.ClickException(f"Workload not found: {workload_path}")

    workload = Workload.load(workload_path)
    test_queries = workload.queries[:100]  # Test subset

    click.echo(f"Testing monotonicity on {len(test_queries)} queries...")

    from framework.failure_semantics_test import FailureSemanticsTester

    tester = FailureSemanticsTester(cfg)
    results = tester.test_monotonicity(test_queries)

    # Save results
    output_path = (
        Path(output_dir)
        if output_dir
        else Path(cfg.output.results_dir) / cfg.experiment_tag / "semantics"
    )
    output_path.mkdir(parents=True, exist_ok=True)
    results_file = output_path / "monotonicity_results.json"
    results_file.write_text(json.dumps(results, indent=2))

    # Report summary
    summary = results["summary"]
    click.echo("\nResults:")
    click.echo(f"  Total tests: {summary['total_tests']}")
    click.echo(f"  Violations: {summary['violations']}")
    click.echo(f"  Monotonic: {'YES' if summary['monotonic'] else 'NO'}")

    if summary["violations"] > 0:
        click.echo("\nViolations:")
        for violation in summary["violation_details"][:5]:  # Show first 5
            click.echo(f"  - Query {violation['query']} violated monotonicity")

    click.echo(f"\n✓ Results saved to {results_file}")


@cli.command("report")
@click.option("--config", type=click.Path(exists=True), help="Path to YAML/JSON config")
@click.option(
    "--results-dir", type=click.Path(exists=True), help="Root results directory"
)
@click.option("--output-dir", default=None, help="Output directory for report")
@click.option(
    "--format",
    "output_format",
    type=click.Choice(["latex", "markdown", "json"]),
    default="latex",
    help="Output format",
)
def generate_report(config, results_dir, output_dir, output_format):
    """Generate comprehensive experiment report with all figures and tables."""
    from datetime import datetime

    cfg = load_config_file(config)

    # Resolve results_dir from config if not provided
    results_dir = results_dir or cfg.output.results_dir
    if not results_dir:
        raise click.ClickException(
            "Results directory must be provided via --results-dir or output.results_dir in config"
        )

    results_path = Path(results_dir)
    out_path = Path(output_dir) if output_dir else results_path / "report"
    out_path.mkdir(parents=True, exist_ok=True)

    click.echo(f"Generating report from {results_path}...")

    # Collect all experiment results
    experiments = []

    # Look for multi-run results
    for subdir in results_path.iterdir():
        if subdir.is_dir():
            multi_run_json = subdir / "multi_run_results.json"
            if multi_run_json.exists():
                experiments.append(
                    {
                        "name": subdir.name,
                        "type": "multi_run",
                        "path": multi_run_json,
                    }
                )

    # Look for analysis outputs
    analysis_dirs = list(results_path.glob("**/analysis*"))
    for analysis_dir in analysis_dirs:
        if analysis_dir.is_dir():
            experiments.append(
                {
                    "name": analysis_dir.name,
                    "type": "analysis",
                    "path": analysis_dir,
                }
            )

    click.echo(f"Found {len(experiments)} experiment results")

    # Generate report
    if output_format == "json":
        now_iso = datetime.now().isoformat()
        report = {
            "generated_at": now_iso,
            "source": str(results_path),
            "experiments": [],
        }
        for exp in experiments:
            if exp["type"] == "multi_run":
                data = json.loads(exp["path"].read_text())
                report["experiments"].append(
                    {
                        "name": exp["name"],
                        "experiment_name": data.get("experiment_name"),
                        "aggregate": data.get("aggregate"),
                    }
                )

        report_path = out_path / "report.json"
        report_path.write_text(json.dumps(report, indent=2))
        click.echo(f"✓ Report saved to {report_path}")

    elif output_format == "markdown":
        now_iso = datetime.now().isoformat()
        lines = [
            "# Experiment Report",
            "",
            f"Generated: {now_iso}",
            f"Source: {results_path}",
            "",
            "## Experiments",
            "",
        ]

        for exp in experiments:
            lines.append(f"### {exp['name']}")
            if exp["type"] == "multi_run":
                data = json.loads(exp["path"].read_text())
                lines.append(f"- Ordering: {data.get('ordering_strategy')}")
            lines.append("")

        report_path = out_path / "report.md"
        report_path.write_text("\n".join(lines))
        click.echo(f"✓ Report saved to {report_path}")

    else:  # LaTeX
        lines = [
            "\\section{Summary of Results}",
            "This report summarizes the experimental evaluation of external ABAC authorization in Cedar-modified databases.",
            "",
        ]

        # 1. Microbenchmarks (E1)
        lines.append("\\subsection{Microbenchmark Results (E1)}")
        lines.append("Query-by-query overhead and latency distributions.")
        lines.append("")
        benchmark_tex = list(
            results_path.glob("**/benchmark/query_by_query_overhead.tex")
        )
        if benchmark_tex:
            lines.append(
                "\\input{"
                + str(benchmark_tex[0].relative_to(results_path.parent))
                + "}"
            )

        # 2. Scalability (E3, E4)
        lines.append("\\subsection{Scalability Results (E3, E4)}")
        conc_tex = list(
            results_path.glob("**/concurrency/concurrency_throughput_table.tex")
        )
        if conc_tex:
            lines.append("\\subsubsection{Concurrency Scaling}")
            lines.append(
                "\\input{" + str(conc_tex[0].relative_to(results_path.parent)) + "}"
            )

        policy_tex = list(
            results_path.glob("**/policy_scaling/policy_scaling_table.tex")
        )
        if policy_tex:
            lines.append("\\subsubsection{Policy Scaling}")
            lines.append(
                "\\input{" + str(policy_tex[0].relative_to(results_path.parent)) + "}"
            )

        # 3. Macrobenchmarks (E8, TPC-C)
        lines.append("\\subsection{Macrobenchmark Results (E8, TPC-C)}")
        pg_tex = list(results_path.glob("**/pgbench/pgbench_summary.tex"))
        if pg_tex:
            lines.append("\\subsubsection{PostgreSQL pgbench Summary}")
            lines.append(
                "\\input{" + str(pg_tex[0].relative_to(results_path.parent)) + "}"
            )

        tpcc_tex = list(results_path.glob("**/tpcc/tpcc_summary.tex"))
        if tpcc_tex:
            lines.append("\\subsubsection{MySQL TPC-C Summary}")
            lines.append(
                "\\input{" + str(tpcc_tex[0].relative_to(results_path.parent)) + "}"
            )

        # 4. Failure Resilience (E7)
        lines.append("\\subsection{Failure Resilience and Robustness (E7)}")
        delay_tex = list(results_path.glob("**/failure/agent_delay_impact.tex"))
        if delay_tex:
            lines.append("\\subsubsection{Agent Delay Impact}")
            lines.append(
                "\\input{" + str(delay_tex[0].relative_to(results_path.parent)) + "}"
            )

        robust_tex = list(results_path.glob("**/semantics/robustness_summary.tex"))
        if robust_tex:
            lines.append("\\subsubsection{Security Robustness Summary}")
            lines.append(
                "\\input{" + str(robust_tex[0].relative_to(results_path.parent)) + "}"
            )

        report_path = out_path / "report.tex"
        report_path.write_text("\n".join(lines))
        click.echo(f"✓ Comprehensive LaTeX report saved to {report_path}")


@cli.command("generate-artifacts")
@click.option("--config", type=click.Path(exists=True), help="Path to YAML/JSON config")
@click.option(
    "--experiment",
    default=None,
    type=str,
    help="Override experiment name (default: uses experiment_tag from config)",
)
@click.option(
    "--skip-plots",
    is_flag=True,
    help="Skip PNG figure generation (generate only CSV and LaTeX tables)",
)
def generate_artifacts(config: str | None, experiment: str | None, skip_plots: bool):
    """
    Generate paper artifacts (tables, plots) from experiment results.

    Uses experiment_tag from config to determine which results directory to process.
    Outputs are saved to paper_artifacts/<experiment_tag>/
    """
    import generate_paper_artifacts as gpa

    if skip_plots:
        os.environ["CEDAR_SKIP_PLOTS"] = "1"

    cfg = load_config_file(config)

    # Use experiment from CLI, or fall back to config's experiment_tag
    experiment_tag = experiment or cfg.experiment_tag

    # Determine paths from config
    results_root = Path(cfg.output.results_dir)
    artifacts_root = Path("paper_artifacts")

    experiment_dir = results_root / experiment_tag
    output_dir = artifacts_root / experiment_tag

    if not experiment_dir.exists():
        raise click.ClickException(f"Results directory not found: {experiment_dir}")

    output_dir.mkdir(parents=True, exist_ok=True)

    click.echo(f"Using config: {config or 'default'}")
    click.echo(f"Experiment tag: {experiment_tag}")
    click.echo(f"Results directory: {experiment_dir}")
    click.echo(f"Output directory: {output_dir}")
    click.echo()

    # Use functions from generate_paper_artifacts module
    gpa.process_benchmark_results(experiment_dir, output_dir)
    gpa.process_profiling_results(experiment_dir, output_dir)
    gpa.process_concurrency_results(experiment_dir, output_dir)
    gpa.process_policy_scaling_results(experiment_dir, output_dir)
    gpa.process_analytic_results(experiment_dir, output_dir)
    gpa.process_failure_results(experiment_dir, output_dir)
    gpa.process_semantics_results(experiment_dir, output_dir)
    gpa.process_tpcc_results(experiment_dir, output_dir)
    gpa.process_ddl_results(experiment_dir, output_dir)
    gpa.process_pgbench_results(experiment_dir, output_dir)

    # Generate cross-cutting artifacts
    gpa.process_cross_database_comparison(output_dir)
    gpa.generate_unified_summary(output_dir)

    # Generate all visualizations
    click.echo("Generating visualizations...")
    from framework.visualizations import generate_all_visualizations

    outputs = generate_all_visualizations(output_dir, output_dir)
    for name, path in outputs.items():
        if path:
            click.echo(f"  ✓ Generated {name}: {path.name}")

    click.echo()
    click.echo(f"✓ Paper artifacts generated in {output_dir}")


if __name__ == "__main__":
    cli()
