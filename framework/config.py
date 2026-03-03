#!/usr/bin/env python3
"""
Configuration management for the benchmark framework.
Supports YAML and JSON with basic environment variable interpolation.
"""

from __future__ import annotations

import json
import os
from datetime import datetime
from pathlib import Path
from typing import Any

from pydantic import BaseModel, Field

try:
    import yaml  # type: ignore
except Exception:
    yaml = None  # Optional; JSON works without YAML


def _env_interpolate(value: Any) -> Any:
    if isinstance(value, str):
        # Replace ${VAR} or ${VAR:-default} with env var
        def repl(match):
            var_expr = match.group(1)
            # Check for default value syntax: VAR:-default
            if ":-" in var_expr:
                var, default = var_expr.split(":-", 1)
                # Strip surrounding quotes if present in default value
                if (default.startswith('"') and default.endswith('"')) or (
                    default.startswith("'") and default.endswith("'")
                ):
                    default = default[1:-1]
                # Use default if var is not set OR if it's set but empty
                env_value = os.environ.get(var)
                return env_value if env_value else default
            else:
                return os.environ.get(var_expr, "")

        import re

        return re.sub(r"\$\{([^}]+)\}", repl, value)
    if isinstance(value, dict):
        return {k: _env_interpolate(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_env_interpolate(v) for v in value]
    return value


class DatabaseConfig(BaseModel):
    name: str
    host: str
    port: int
    user: str
    password: str | None = None
    database: str
    type: str = "mysql"  # "mysql" or "postgres"
    pool_size: int = 5

    @property
    def connection_params(self) -> dict[str, Any]:
        return {
            "host": self.host,
            "port": self.port,
            "user": self.user,
            "password": self.password or "",
            "database": self.database,
        }


class DDLAuditPluginConfig(BaseModel):
    url: str = "http://localhost:8280"
    enabled: bool = True
    timeout_ms: int = 5000
    namespace: str = "MySQL"


class CedarAuthorizationPluginConfig(BaseModel):
    url: str = "http://localhost:8280/v1/is_authorized"
    enabled: bool = True
    timeout_ms: int = 5000
    namespace: str = "MySQL"
    cache_enabled: bool = True
    cache_size: int = 1000
    cache_ttl: int = 300
    log_info: bool = False
    enable_column_access: bool = False
    collect_stats: bool = False


class PgAuthorizationPluginConfig(BaseModel):
    collect_stats: bool = False


class CedarAgentConfig(BaseModel):
    # URL for host access (CLI running on host)
    url: str = "http://localhost:8280"
    timeout: int = 5
    retry_attempts: int = 3
    # MySQL plugin configuration
    plugins: dict[str, Any] | None = None


class WorkloadConfig(BaseModel):
    seed: int = 42
    queries_per_combination: int = 100
    action_distribution: dict[str, float] = Field(
        default_factory=lambda: {
            "SELECT": 0.60,
            "INSERT": 0.15,
            "UPDATE": 0.15,
            "DELETE": 0.10,
        }
    )


class BenchmarkConfig(BaseModel):
    iterations: int = 1000
    warmup_iterations: int = 100
    warmup_seconds: int = 0
    concurrency: int = 1
    timeout: int = 30
    # If true, execute each query using the user specified in workload.json
    use_query_user: bool = False

    # Multi-run configuration for statistical rigor
    n_runs: int = 1  # Number of independent runs (for confidence intervals)
    confidence_level: float = 0.95  # Confidence level for bootstrap CIs
    n_bootstrap: int = 10000  # Number of bootstrap samples for CIs


class OutputConfig(BaseModel):
    results_dir: str = "./results"
    workload_dir: str | None = None
    analysis_dir: str | None = None
    format: str = "json"
    save_raw_data: bool = True


class ScalingConfig(BaseModel):
    policy_counts: list[int] = Field(default_factory=lambda: [1, 10, 50, 100, 500])
    iterations: int = 1000
    warmup_iterations: int = 100
    warmup_seconds: int = 0
    n_runs: int = 3
    seed: int | None = None
    reset: bool = True
    match_ratio: float = 0.2
    max_latency_ms: float | None = None


class SysbenchConfig(BaseModel):
    binary: str = "sysbench"
    docker: bool = False
    db_name: str = "abac_sysbench"
    oltp: str = "oltp_read_only"
    tables: int = 8
    table_size: int = 100000
    duration: int = 60
    threads: list[int] = Field(default_factory=lambda: [1, 2, 4, 8, 16])
    n_runs: int = 3  # Number of independent runs for statistical rigor


class PgBenchConfig(BaseModel):
    binary: str = "pgbench"
    docker: bool = False
    container_name: str | None = None
    db_name: str = "abac_test"
    scale: int = 10
    clients: int = 4
    jobs: int | None = None
    duration: int = 60
    warmup: int = 0
    builtin: str = "tpcb-like"
    n_runs: int = 3  # Number of independent runs for statistical rigor


class TPCConfig(BaseModel):
    tpcc_mysql_home: str | None = None
    tpcc_lua_path: str | None = "/usr/share/sysbench/tpcc.lua"
    warehouses: int = 10
    scale: int = 1
    terminals: int = 10
    connections: int = 10
    threads: int = 48
    duration_s: int = 300
    run_mins: int = 5
    prepare: bool = True
    cleanup: bool = False
    n_runs: int = 3  # Number of independent runs for statistical rigor


class BenchmarkUserConfig(BaseModel):
    """Configuration for benchmark user without native privileges."""

    username: str = "cedar_bench"
    password: str = "benchpass123"
    enabled: bool = True
    verify_auth_invocations: bool = True
    min_expected_auth_requests: int = 100


class ProxyConfig(BaseModel):
    enabled: bool = True
    name: str = "cedar_agent_proxy"
    host: str = "127.0.0.1"
    listen_port: int = 8474
    control_api: str = "http://127.0.0.1:8474"
    upstream_host: str = "127.0.0.1"
    upstream_port: int = 8181


class MySQLFailureConfig(BaseModel):
    host: str = "127.0.0.1"
    port: int = 3306
    db: str = "abac_test"
    users: dict[str, str] = Field(default_factory=dict)
    query: str = "SELECT * FROM abac_test.projects LIMIT 1;"


class AgentStressConfig(BaseModel):
    rps_list: list[int] = Field(default_factory=lambda: [50, 100, 200, 400, 800])
    duration_s: int = 60
    warmup_s: int = 10
    auth_request_body: str = ""
    expected_decision: str = "Allow"


class FailureTestsConfig(BaseModel):
    agent_url: str = "http://localhost:8181/v1/is_authorized"
    proxy: ProxyConfig | None = None
    delays_ms: list[int] = Field(
        default_factory=lambda: [0, 10, 50, 100, 200, 500, 5000]
    )
    sql_repetitions: int = 100
    mysql: MySQLFailureConfig | None = None
    agent_stress: AgentStressConfig | None = None
    timeouts_ms: list[int] = Field(default_factory=lambda: [5000])
    results_dir: str = "experiments/results/failure"


class CacheAnalysisConfig(BaseModel):
    rps: float = 1000.0
    unique_combos: int = 1000
    stale_ok: int = 60


class Config(BaseModel):
    databases: dict[str, DatabaseConfig]
    auth_spec_path: str | None = None
    experiment_tag: str = Field(default_factory=lambda: "default")
    cedar_agent: CedarAgentConfig = Field(default_factory=CedarAgentConfig)
    benchmark_user: BenchmarkUserConfig = Field(default_factory=BenchmarkUserConfig)
    workload: WorkloadConfig = Field(default_factory=WorkloadConfig)
    benchmark: BenchmarkConfig = Field(default_factory=BenchmarkConfig)
    output: OutputConfig = Field(default_factory=OutputConfig)
    scaling: ScalingConfig = Field(default_factory=ScalingConfig)
    sysbench: SysbenchConfig = Field(default_factory=SysbenchConfig)
    pgbench: PgBenchConfig = Field(default_factory=PgBenchConfig)
    tpcc: TPCConfig = Field(default_factory=TPCConfig)
    failure_tests: FailureTestsConfig | None = None
    cache_analysis: CacheAnalysisConfig = Field(default_factory=CacheAnalysisConfig)


def _default_config() -> Config:
    # Default password matches docker-compose.yml
    # (MYSQL_ROOT_PASSWORD: rootpass)
    return Config(
        databases={
            "baseline": DatabaseConfig(
                name="baseline",
                host="127.0.0.1",
                port=13306,
                user="root",
                password=os.environ.get("BASELINE_MYSQL_PASSWORD", "rootpass"),
                database="abac_test",
                pool_size=5,
            ),
            "cedar": DatabaseConfig(
                name="cedar",
                host="127.0.0.1",
                port=13307,
                user="root",
                password=os.environ.get("CEDAR_MYSQL_PASSWORD", ""),
                database="abac_test",
                pool_size=5,
            ),
        }
    )


def load_config_file(path: str | None) -> Config:
    if not path:
        return _default_config()
    p = Path(path)
    if not p.exists():
        return _default_config()
    text = p.read_text(encoding="utf-8")

    data: dict[str, Any] = {}
    if p.suffix.lower() in (".yaml", ".yml"):
        if yaml is None:
            raise RuntimeError("pyyaml is required to load YAML configs")
        data = yaml.safe_load(text) or {}
    else:
        data = json.loads(text or "{}")

    # Interpolate environment variables
    data = _env_interpolate(data)

    # Merge with default config to ensure critical sections (like databases) exist if not in file
    # But for Pydantic we want to pass the data and let it validate/fill defaults.
    # However, 'databases' field is required and has no default in Config model (unlike others).
    # So if it's missing in data, we should probably provide the default one.

    if "databases" not in data:
        # This is a bit tricky because _default_config returns a Config object
        default_dbs = _default_config().databases
        # We need to serialize them back to dict or just pass the objects if Pydantic allows (it does)
        data["databases"] = default_dbs

    # Handle databases configuration - need to ensure name field is set from the key
    if "databases" in data:
        dbs_data = data["databases"]
        processed_dbs: dict[str, Any] = {}
        for db_name, db_config in dbs_data.items():
            if isinstance(db_config, dict):
                # Ensure the name is set from the key
                processed_dbs[db_name] = {**db_config, "name": db_name}
            else:
                # If it's already a DatabaseConfig object (from default_config), keep it
                processed_dbs[db_name] = db_config
        data["databases"] = processed_dbs

    return Config(**data)
