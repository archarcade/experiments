#!/usr/bin/env python3

from __future__ import annotations

import os
import re
import subprocess
import sys
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

from .command_runner import run_logged_command

from .sysbench_parser import parse_sysbench_output


@dataclass
class SysbenchTPCCConfig:
    tpcc_lua_path: Path
    warehouses: int = 10
    tables: int = 1
    threads: int = 48
    duration: int = 300  # seconds

    # Database connection
    db_driver: str = "mysql"
    db_host: str = "127.0.0.1"
    db_port: int = 3306
    db_name: str = "sbtest"
    db_user: str = "root"
    db_password: str = ""

    # Admin connection for management commands (psql/ALTER SYSTEM/stats).
    # This must be a superuser (or at least able to CREATE USER/DB and ALTER SYSTEM).
    admin_user: str = "postgres"
    admin_password: str = ""

    # Output
    output_dir: Path | None = None

    @property
    def common_args(self) -> list[str]:
        args = [
            str(self.tpcc_lua_path),
            f"--db-driver={self.db_driver}",
            f"--scale={self.warehouses}",
            f"--tables={self.tables}",
        ]

        if self.db_driver == "mysql":
            args.extend(
                [
                    f"--mysql-host={self.db_host}",
                    f"--mysql-port={self.db_port}",
                    f"--mysql-user={self.db_user}",
                    f"--mysql-db={self.db_name}",
                ]
            )
            if self.db_password:
                args.append(f"--mysql-password={self.db_password}")
        elif self.db_driver == "pgsql":
            args.extend(
                [
                    f"--pgsql-host={self.db_host}",
                    f"--pgsql-port={self.db_port}",
                    f"--pgsql-user={self.db_user}",
                    f"--pgsql-db={self.db_name}",
                ]
            )
            if self.db_password:
                args.append(f"--pgsql-password={self.db_password}")

        return args


class SysbenchTPCCClient:
    def __init__(self, config: SysbenchTPCCConfig):
        self.config = config
        self._validate_installation()

    def _validate_installation(self) -> None:
        """Validate that sysbench and the TPC-C lua script are available."""
        # On remote systems, Path.exists() might not work if this script runs locally
        # but the files are remote. However, this script is intended to run WHERE sysbench is.
        if not self.config.tpcc_lua_path.exists():
            # Try relative to CWD as a fallback
            if not (Path.cwd() / self.config.tpcc_lua_path).exists():
                raise RuntimeError(
                    f"sysbench-tpcc lua script not found: {self.config.tpcc_lua_path}"
                )

        try:
            subprocess.run(["sysbench", "--version"], capture_output=True, check=True)
        except (subprocess.CalledProcessError, FileNotFoundError):
            raise RuntimeError("sysbench not found in PATH")

        # Check if database client is available for prepare's DB creation
        db_client = "mysql" if self.config.db_driver == "mysql" else "psql"
        try:
            subprocess.run([db_client, "--version"], capture_output=True, check=True)
        except (subprocess.CalledProcessError, FileNotFoundError):
            # We don't raise error here, just a warning in prepare
            pass

    def _run_command(
        self,
        command: list[str],
        timeout: int | None = None,
        stream_output: bool = False,
        log_dir: Path | None = None,
        label: str | None = None,
    ) -> subprocess.CompletedProcess:
        # Get the directory of the Lua script to set as CWD
        # This ensures that 'require' statements for common modules work
        cwd = self.config.tpcc_lua_path.parent

        try:
            effective_log_dir = log_dir
            if effective_log_dir is None and self.config.output_dir is not None:
                # Stable output dirs: one complete set per experiment.
                # Use label subdirs to avoid overwriting prepare/run/cleanup logs.
                effective_log_dir = (
                    self.config.output_dir / "raw" / (label or "sysbench-tpcc")
                )

            if effective_log_dir is not None:
                res = run_logged_command(
                    command,
                    effective_log_dir,
                    cwd=cwd,
                    env=None,
                    timeout_s=timeout,
                    stream_to_console=stream_output,
                    combine_stderr=True,
                    label=label or "sysbench-tpcc",
                )
                stdout = res.stdout_path.read_text(encoding="utf-8", errors="replace")
                stderr = res.stderr_path.read_text(encoding="utf-8", errors="replace")
                result = subprocess.CompletedProcess(
                    command, res.returncode, stdout, stderr
                )
            else:
                result = subprocess.run(
                    command,
                    capture_output=True,
                    text=True,
                    encoding="utf-8",
                    errors="replace",
                    timeout=timeout,
                    cwd=str(cwd),
                )

            if result.returncode != 0:
                error_msg = f"Sysbench command failed: {' '.join(command)}\n"
                error_msg += f"STDOUT: {result.stdout}\n"
                error_msg += f"STDERR: {result.stderr}"
                raise RuntimeError(error_msg)

            return result

        except subprocess.TimeoutExpired as e:
            raise RuntimeError(
                f"Sysbench command timed out after {timeout} seconds: {' '.join(command)}"
            ) from e

    def _ensure_database_exists(self) -> None:
        """Ensure that the benchmark database and user exist with correct permissions."""
        try:
            if self.config.db_driver == "mysql":
                db_cmd = [
                    "mysql",
                    f"--host={self.config.db_host}",
                    f"--port={self.config.db_port}",
                    f"--user={self.config.db_user}",
                ]
                if self.config.db_password:
                    db_cmd.append(f"--password={self.config.db_password}")
                db_cmd.extend(
                    ["-e", f"CREATE DATABASE IF NOT EXISTS {self.config.db_name}"]
                )
                subprocess.run(db_cmd, capture_output=True, check=True)
            elif self.config.db_driver == "pgsql":
                env = os.environ.copy()
                if self.config.admin_password:
                    env["PGPASSWORD"] = self.config.admin_password

                # 1. Ensure User exists (connect to postgres)
                # We use a DO block to make it idempotent
                user_sql = f"""
DO $$
BEGIN
IF NOT EXISTS (SELECT FROM pg_catalog.pg_user WHERE usename = '{self.config.db_user}') THEN
    CREATE USER {self.config.db_user} WITH PASSWORD '{self.config.db_password or "postgres"}';
ELSE
    ALTER USER {self.config.db_user} WITH PASSWORD '{self.config.db_password or "postgres"}';
END IF;
END $$;
"""
                subprocess.run(
                    [
                        "psql",
                        "-h",
                        self.config.db_host,
                        "-p",
                        str(self.config.db_port),
                        "-U",
                        self.config.admin_user,
                        "-d",
                        "postgres",
                        "-c",
                        user_sql,
                    ],
                    capture_output=True,
                    check=True,
                    env=env,
                )

                # 2. Check if DB exists
                check_sql = (
                    f"SELECT 1 FROM pg_database WHERE datname = '{self.config.db_name}'"
                )
                args_base = [
                    "psql",
                    "-h",
                    self.config.db_host,
                    "-p",
                    str(self.config.db_port),
                    "-U",
                    self.config.admin_user,
                    "-d",
                    "postgres",
                ]

                result = subprocess.run(
                    args_base + ["-tc", check_sql],
                    capture_output=True,
                    text=True,
                    env=env,
                )
                if "1" not in result.stdout:
                    print(f"  Creating database {self.config.db_name}...")
                    create_sql = f"CREATE DATABASE {self.config.db_name} OWNER {self.config.db_user}"
                    subprocess.run(
                        args_base + ["-c", create_sql],
                        capture_output=True,
                        check=True,
                        env=env,
                    )

                # 3. Grant schema permissions (connect to target DB)
                # In PG 15+, public schema permissions are restricted.
                # We grant all on DB, all on schema, and make user the owner of the schema for good measure.
                perm_sql = f"""
GRANT ALL PRIVILEGES ON DATABASE {self.config.db_name} TO {self.config.db_user};
GRANT ALL ON SCHEMA public TO {self.config.db_user};
ALTER SCHEMA public OWNER TO {self.config.db_user};
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON TABLES TO {self.config.db_user};
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON SEQUENCES TO {self.config.db_user};
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON FUNCTIONS TO {self.config.db_user};
"""
                subprocess.run(
                    [
                        "psql",
                        "-h",
                        self.config.db_host,
                        "-p",
                        str(self.config.db_port),
                        "-U",
                        self.config.admin_user,
                        "-d",
                        self.config.db_name,
                        "-c",
                        perm_sql,
                    ],
                    capture_output=True,
                    check=True,
                    env=env,
                )
        except Exception as e:
            print(
                f"Warning: Could not ensure database {self.config.db_name} exists or set permissions: {e}"
            )

    def prepare(self, threads: int | None = None) -> dict[str, Any]:
        """Prepare the database for benchmarking (create tables and load data)."""
        # Create database if it doesn't exist
        self._ensure_database_exists()

        start_time = time.time()

        # Many sysbench-tpcc versions are not thread-safe for prepare,
        # or require threads to match tables. Using 1 thread is safest.
        prep_threads = threads if threads is not None else 1

        command = [
            "sysbench",
            *self.config.common_args,
            f"--threads={prep_threads}",
            "prepare",
        ]

        # Use stream_output=True to show progress for long-running prepare
        result = self._run_command(
            command, timeout=3600, stream_output=True, label="prepare"
        )  # 1 hour timeout for data load

        return {
            "prepare_time_seconds": time.time() - start_time,
            "output": result.stdout,
        }

    def run(self) -> dict[str, Any]:
        """Run the benchmark."""
        start_time = time.time()

        command = [
            "sysbench",
            *self.config.common_args,
            f"--threads={self.config.threads}",
            f"--time={self.config.duration}",
            "run",
        ]

        result = self._run_command(
            command, timeout=self.config.duration + 300, label="run"
        )

        # Parse output
        metrics = parse_sysbench_output(result.stdout)

        results = {
            "total_time_seconds": time.time() - start_time,
            "duration_seconds": self.config.duration,
            "threads": self.config.threads,
            "output": result.stdout,
        }

        if metrics:
            results.update(
                {
                    "tps": metrics.qps
                    / 10.0,  # Approximate TPS from QPS if needed, but sysbench also gives transactions
                    "qps": metrics.qps,
                    "avg_latency_ms": metrics.lat_avg_ms,
                    "p95_latency_ms": metrics.lat_p95_ms,
                    "min_latency_ms": metrics.lat_min_ms,
                    "max_latency_ms": metrics.lat_max_ms,
                }
            )
            # TPC-C specifically uses TPM (Transactions Per Minute)
            # Sysbench output for TPC-C usually includes transactions.
            # Let's try to find "transactions:" in output
            tx_match = re.search(
                r"transactions:\s+\d+\s+\(([0-9.]+)\s+per sec\.\)", result.stdout
            )
            if tx_match:
                tps = float(tx_match.group(1))
                results["tps"] = tps
                results["tpm"] = tps * 60.0

        return results

    def cleanup(self) -> None:
        """Clean up the benchmark data."""
        # Ensure database exists so sysbench can connect to it
        self._ensure_database_exists()

        command = ["sysbench", *self.config.common_args, "cleanup"]
        try:
            self._run_command(command, label="cleanup")
        except Exception as e:
            # If cleanup fails, it might be because tables don't exist, which is fine for fresh run
            print(f"  (Cleanup info: {e})")

    def set_guc(self, name: str, value: str) -> bool:
        """Set a PostgreSQL GUC variable."""
        if self.config.db_driver != "pgsql":
            return False

        sql = f"ALTER SYSTEM SET {name} = '{value}';"
        # We need to reload too for some GUCs, but ALTER SYSTEM + reload is safer
        # For session-level, we'd use SET, but ALTER SYSTEM is more persistent for benchmarks

        args = [
            "psql",
            "-h",
            self.config.db_host,
            "-p",
            str(self.config.db_port),
            "-U",
            self.config.admin_user,
            "-d",
            "postgres",
            "-c",
            sql,
        ]

        # Also reload config
        reload_sql = "SELECT pg_reload_conf();"
        reload_args = [
            "psql",
            "-h",
            self.config.db_host,
            "-p",
            str(self.config.db_port),
            "-U",
            self.config.admin_user,
            "-d",
            "postgres",
            "-c",
            reload_sql,
        ]

        try:
            # Use subprocess directly as self._run_command is for sysbench
            env = os.environ.copy()
            if self.config.admin_password:
                env["PGPASSWORD"] = self.config.admin_password

            subprocess.run(args, capture_output=True, check=True, env=env)
            subprocess.run(reload_args, capture_output=True, check=True, env=env)
            return True
        except Exception as e:
            print(f"Warning: Failed to set GUC {name}={value}: {e}")
            return False

    def get_authorization_stats(self) -> dict[str, Any]:
        """Fetch authorization statistics from the database (PostgreSQL only)."""
        if self.config.db_driver != "pgsql":
            return {}

        sql = "SELECT * FROM pg_authorization_stats();"
        args = [
            "psql",
            "-h",
            self.config.db_host,
            "-p",
            str(self.config.db_port),
            "-U",
            self.config.admin_user,
            "-d",
            self.config.db_name,
            "-X",
            "-A",
            "-c",
            sql,
        ]

        try:
            env = os.environ.copy()
            if self.config.admin_password:
                env["PGPASSWORD"] = self.config.admin_password

            result = subprocess.run(args, capture_output=True, text=True, env=env)
            if result.returncode != 0:
                return {}

            lines = result.stdout.strip().split("\n")
            if len(lines) < 2:
                return {}

            headers = lines[0].split("|")
            values = lines[1].split("|")

            stats = {}
            for h, v in zip(headers, values):
                try:
                    if "." in v:
                        stats[h] = float(v)
                    else:
                        stats[h] = int(v)
                except ValueError:
                    stats[h] = v
            return stats
        except Exception:
            return {}

    def reset_authorization_stats(self) -> bool:
        """Reset authorization statistics in the database (PostgreSQL only)."""
        if self.config.db_driver != "pgsql":
            return False

        sql = "SELECT pg_authorization_reset_stats();"
        args = [
            "psql",
            "-h",
            self.config.db_host,
            "-p",
            str(self.config.db_port),
            "-U",
            self.config.admin_user,
            "-d",
            self.config.db_name,
            "-c",
            sql,
        ]

        try:
            env = os.environ.copy()
            if self.config.admin_password:
                env["PGPASSWORD"] = self.config.admin_password
            subprocess.run(args, capture_output=True, check=True, env=env)
            return True
        except Exception:
            return False

    def get_authorization_cache_stats(self) -> dict[str, Any]:
        """Fetch authorization cache statistics from the database (PostgreSQL only)."""
        if self.config.db_driver != "pgsql":
            return {}

        sql = "SELECT * FROM pg_authorization_cache_stats();"
        args = [
            "psql",
            "-h",
            self.config.db_host,
            "-p",
            str(self.config.db_port),
            "-U",
            self.config.admin_user,
            "-d",
            self.config.db_name,
            "-X",
            "-A",
            "-c",
            sql,
        ]

        try:
            env = os.environ.copy()
            if self.config.admin_password:
                env["PGPASSWORD"] = self.config.admin_password

            result = subprocess.run(args, capture_output=True, text=True, env=env)
            if result.returncode != 0:
                return {}

            lines = result.stdout.strip().split("\n")
            if len(lines) < 2:
                return {}

            headers = lines[0].split("|")
            values = lines[1].split("|")

            stats: dict[str, Any] = {}
            for h, v in zip(headers, values):
                try:
                    stats[h] = int(v)
                except ValueError:
                    stats[h] = v
            return stats
        except Exception:
            return {}

    def reset_authorization_cache(self) -> bool:
        """Reset authorization cache in the database (PostgreSQL only)."""
        if self.config.db_driver != "pgsql":
            return False

        sql = "SELECT pg_authorization_cache_reset();"
        args = [
            "psql",
            "-h",
            self.config.db_host,
            "-p",
            str(self.config.db_port),
            "-U",
            self.config.admin_user,
            "-d",
            self.config.db_name,
            "-c",
            sql,
        ]

        try:
            env = os.environ.copy()
            if self.config.admin_password:
                env["PGPASSWORD"] = self.config.admin_password
            subprocess.run(args, capture_output=True, check=True, env=env)
            return True
        except Exception:
            return False

    def register_cedar_entities(self, config_obj: Any) -> None:
        """
        Register necessary entities, schema, and policies in Cedar agent for TPC-C.

        Args:
            config_obj: The global Config object from cli.py
        """
        import json

        import requests

        base_url = config_obj.cedar_agent.url
        if not base_url.endswith("/v1"):
            base_url = base_url.rstrip("/") + "/v1"

        namespace = "PostgreSQL" if self.config.db_driver == "pgsql" else "MySQL"

        bench_username: str | None = None

        # 1. Register Schema and Policies
        try:
            from .translate_to_cedar import create_cedar_policies, setup_cedar_schema

            with open(config_obj.auth_spec_path) as f:
                spec = json.load(f)

            bench_username = None
            if getattr(config_obj, "benchmark_user", None) and getattr(
                config_obj.benchmark_user, "enabled", False
            ):
                bench_username = config_obj.benchmark_user.username

            if bench_username:
                has_bench_user = any(
                    user.get("username") == bench_username
                    for user in spec.get("users", [])
                )
                if not has_bench_user:
                    spec.setdefault("users", []).append(
                        {
                            "username": bench_username,
                            "password": "",
                            "host": "%",
                            "attributes": {
                                "user_role": "benchmarking",
                                "clearance_level": "top_secret",
                                "department": "benchmark",
                            },
                        }
                    )

                has_bench_policy = any(
                    policy.get("id") == "bench_user_table_access"
                    for policy in spec.get("policies", [])
                )
                if not has_bench_policy:
                    spec.setdefault("policies", []).append(
                        {
                            "id": "bench_user_table_access",
                            "privileges": ["SELECT", "INSERT", "UPDATE", "DELETE"],
                            "condition": "principal.user_role == 'benchmarking'",
                            "description": "Benchmark user can access all tables",
                        }
                    )

            print(
                f"Ensuring Cedar agent schema attributes are registered for namespace '{namespace}'..."
            )
            setup_cedar_schema(base_url, spec, namespace)

            for policy_spec in spec.get("policies", []):
                if policy_spec.get("id") == "bench_user_table_access":
                    privileges = policy_spec.get("privileges", [])
                    if "TRUNCATE" in privileges:
                        policy_spec["privileges"] = [
                            p for p in privileges if p != "TRUNCATE"
                        ]

            print(f"Registering policies in Cedar agent for namespace '{namespace}'...")
            policies = create_cedar_policies(spec, namespace)

            existing_resp = requests.get(f"{base_url}/policies", timeout=5)
            existing_ids = []
            if existing_resp.status_code == 200:
                existing_ids = [p["id"] for p in existing_resp.json()]

            for policy in policies:
                resp = requests.put(
                    f"{base_url}/policies/{policy['id']}", json=policy, timeout=5
                )
                if resp.status_code not in (200, 201, 204, 409):
                    raise RuntimeError(
                        f"Failed to upsert policy {policy['id']}: {resp.status_code} {resp.text}"
                    )
        except Exception as e:
            print(f"Warning: Error registering schema/policies: {e}")

        # 2. Wait for Entities to Propagate (DDL Plugin should create them)
        # Define expected entities list

        # Users
        expected_entities = [("User", self.config.db_user)]
        if bench_username:
            expected_entities.append(("User", bench_username))
        if self.config.db_driver == "pgsql":
            expected_entities.append(("User", "postgres"))

        # Database
        expected_entities.append(("Database", self.config.db_name))

        if self.config.db_driver == "pgsql":
            # For PostgreSQL, we expect public schema
            # (Note: schemas might be pre-created or auto-created, usually 'public' is there)
            # But DDL plugin only syncs what it sees.
            # Let's rely on create_entity for schema if missing, or just check it.
            pass

        # TPC-C Tables
        tpcc_tables = []
        for i in range(1, self.config.tables + 1):
            suffix = str(i)
            tables = [
                "warehouse",
                "district",
                "customer",
                "history",
                "orders",
                "new_orders",
                "order_line",
                "stock",
                "item",
            ]
            for t in tables:
                table_id = f"{t}{suffix}"
                if self.config.db_driver == "pgsql":
                    table_id = f"public.{table_id}"
                else:
                    table_id = f"{self.config.db_name}.{table_id}"
                tpcc_tables.append(table_id)
                expected_entities.append(("Table", table_id))

        print(
            f"Waiting for {len(expected_entities)} TPC-C entities to propagate from DDL plugin..."
        )
        # Give it a reasonable timeout (e.g. 30s) as prepare might have just finished
        # or if existing, they should be there.
        try:
            from .translate_to_cedar import (
                create_entity,
                entity_exists,
                wait_for_entities,
            )

            found_entities = wait_for_entities(
                base_url, expected_entities, namespace, max_wait=10, check_interval=2
            )

            # Create any missing entities explicitly with their attributes
            # (DDL plugin may not have propagated them for PostgreSQL)
            missing_entities = [e for e in expected_entities if e not in found_entities]
            if missing_entities:
                print(
                    f"Creating {len(missing_entities)} missing entities explicitly..."
                )
                for entity_type, entity_id in missing_entities:
                    if not entity_exists(base_url, entity_type, entity_id, namespace):
                        # Determine initial attributes based on entity type
                        attrs = {}
                        if entity_type == "User":
                            if entity_id in (self.config.db_user, bench_username):
                                attrs = {"user_role": "benchmarking"}
                            elif entity_id == "postgres":
                                attrs = {"user_role": "admin"}
                        elif entity_type == "Database":
                            attrs = {
                                "security_level": "high",
                                "compliance_tier": "tier1",
                            }
                        elif entity_type == "Table":
                            attrs = {
                                "data_classification": "public",
                                "table_type": "tpcc_data",
                            }

                        created = create_entity(
                            base_url, entity_type, entity_id, namespace, attrs
                        )
                        if created:
                            print(
                                f'  Created {namespace}::{entity_type}::"{entity_id}" with attrs={attrs}'
                            )
                        else:
                            print(
                                f'  Warning: Failed to create {namespace}::{entity_type}::"{entity_id}"'
                            )
        except Exception as e:
            print(f"Warning: Failed to wait for/create entities: {e}")

        # 3. Assign Attributes
        print(f"Assigning attributes to TPC-C entities in Cedar agent at {base_url}...")

        def set_attribute(
            entity_type: str, entity_id: str, attr_name: str, attr_val: str
        ) -> bool:
            """Set attribute on entity, return True if successful."""
            try:
                resp = requests.put(
                    f"{base_url}/data/attribute",
                    json={
                        "entity_type": entity_type,
                        "namespace": namespace,
                        "entity_id": entity_id,
                        "attribute_name": attr_name,
                        "attribute_value": attr_val,
                    },
                    timeout=5,
                )
                if resp.status_code == 404:
                    print(
                        f'  Warning: Entity {namespace}::{entity_type}::"{entity_id}" not found'
                    )
                    return False
                elif resp.status_code not in (200, 201, 204):
                    print(
                        f"  Warning: Failed to set {attr_name}={attr_val} on {entity_id}: {resp.status_code}"
                    )
                    return False
                return True
            except Exception as e:
                print(f"  Warning: Error setting attribute for {entity_id}: {e}")
                return False

        users_to_assign: list[tuple[str, str, str]] = [
            (self.config.db_user, "user_role", "benchmarking"),
        ]
        if bench_username:
            users_to_assign.append((bench_username, "user_role", "benchmarking"))
        if self.config.db_driver == "pgsql":
            users_to_assign.append(("postgres", "user_role", "admin"))

        for user_id, attr_name, attr_val in users_to_assign:
            set_attribute("User", user_id, attr_name, attr_val)

        set_attribute("Database", self.config.db_name, "security_level", "high")
        set_attribute("Database", self.config.db_name, "compliance_tier", "tier1")

        # TPC-C Tables
        for table_id in tpcc_tables:
            set_attribute("Table", table_id, "data_classification", "public")
            set_attribute("Table", table_id, "table_type", "tpcc_data")

    def cleanup_cedar_entities(self, config_obj: Any) -> None:
        """Cleanup TPC-C specific policies from Cedar agent."""
        import requests

        base_url = config_obj.cedar_agent.url
        if not base_url.endswith("/v1"):
            base_url = base_url.rstrip("/") + "/v1"

        namespace = "PostgreSQL" if self.config.db_driver == "pgsql" else "MySQL"

        # We only clean up policies, as deleting entities that might be re-used/synced
        # is complex and attributes are harmless.
        # But we should clean up the policies we ensured existed.
        # Specifically those from auth_spec.json for benchmarking

        # Note: We rely on policy IDs matching what create_cedar_policies generates
        # which is typically {namespace}_{id}

        policies_to_cleanup = [
            "bench_user_db_access",
            "bench_user_schema_access",
            "bench_user_table_access",
        ]

        print("Cleaning up TPC-C policies in Cedar agent...")
        for pid in policies_to_cleanup:
            # Try plain ID and namespace-prefixed ID
            ids_to_try = [pid, f"{namespace.lower()}_{pid}"]
            for policy_id in ids_to_try:
                try:
                    requests.delete(f"{base_url}/policies/{policy_id}", timeout=5)
                except Exception:
                    pass

    def get_ddl_operations(self) -> list[str]:
        """
        Generate DDL operations that would be performed by sysbench-tpcc.

        Returns:
            List of DDL SQL statements that would be executed
        """
        ddl_statements = []

        # TPC-C creates a set of tables for each "table set"
        for i in range(1, self.config.tables + 1):
            suffix = str(i)

            # Warehouse table
            ddl_statements.append(f"""
                CREATE TABLE IF NOT EXISTS warehouse{suffix} (
                    w_id smallint not null,
                    w_name varchar(10),
                    w_street_1 varchar(20),
                    w_street_2 varchar(20),
                    w_city varchar(20),
                    w_state char(2),
                    w_zip char(9),
                    w_tax decimal(4,2),
                    w_ytd decimal(12,2),
                    primary key (w_id)
                ) ENGINE = InnoDB
            """)

            # District table
            ddl_statements.append(f"""
                CREATE TABLE IF NOT EXISTS district{suffix} (
                    d_id tinyint not null,
                    d_w_id smallint not null,
                    d_name varchar(10),
                    d_street_1 varchar(20),
                    d_street_2 varchar(20),
                    d_city varchar(20),
                    d_state char(2),
                    d_zip char(9),
                    d_tax decimal(4,2),
                    d_ytd decimal(12,2),
                    d_next_o_id int,
                    primary key (d_w_id, d_id)
                ) ENGINE = InnoDB
            """)

            # Customer table
            ddl_statements.append(f"""
                CREATE TABLE IF NOT EXISTS customer{suffix} (
                    c_id int not null,
                    c_d_id tinyint not null,
                    c_w_id smallint not null,
                    c_first varchar(16),
                    c_middle char(2),
                    c_last varchar(16),
                    c_street_1 varchar(20),
                    c_street_2 varchar(20),
                    c_city varchar(20),
                    c_state char(2),
                    c_zip char(9),
                    c_phone char(16),
                    c_since datetime,
                    c_credit char(2),
                    c_credit_lim bigint,
                    c_discount decimal(4,2),
                    c_balance decimal(12,2),
                    c_ytd_payment decimal(12,2),
                    c_payment_cnt smallint,
                    c_delivery_cnt smallint,
                    c_data text,
                    PRIMARY KEY(c_w_id, c_d_id, c_id)
                ) ENGINE = InnoDB
            """)

            # History table
            ddl_statements.append(f"""
                CREATE TABLE IF NOT EXISTS history{suffix} (
                    h_c_id int,
                    h_c_d_id tinyint,
                    h_c_w_id smallint,
                    h_d_id tinyint,
                    h_w_id smallint,
                    h_date datetime,
                    h_amount decimal(6,2),
                    h_data varchar(24)
                ) ENGINE = InnoDB
            """)

            # Orders table
            ddl_statements.append(f"""
                CREATE TABLE IF NOT EXISTS orders{suffix} (
                    o_id int not null,
                    o_d_id tinyint not null,
                    o_w_id smallint not null,
                    o_c_id int,
                    o_entry_d datetime,
                    o_carrier_id tinyint,
                    o_ol_cnt tinyint,
                    o_all_local tinyint,
                    PRIMARY KEY(o_w_id, o_d_id, o_id)
                ) ENGINE = InnoDB
            """)

            # New Order table
            ddl_statements.append(f"""
                CREATE TABLE IF NOT EXISTS new_orders{suffix} (
                    no_o_id int not null,
                    no_d_id tinyint not null,
                    no_w_id smallint not null,
                    PRIMARY KEY(no_w_id, no_d_id, no_o_id)
                ) ENGINE = InnoDB
            """)

            # Order Line table
            ddl_statements.append(f"""
                CREATE TABLE IF NOT EXISTS order_line{suffix} (
                    ol_o_id int not null,
                    ol_d_id tinyint not null,
                    ol_w_id smallint not null,
                    ol_number tinyint not null,
                    ol_i_id int,
                    ol_supply_w_id smallint,
                    ol_delivery_d datetime,
                    ol_quantity tinyint,
                    ol_amount decimal(6,2),
                    ol_dist_info char(24),
                    PRIMARY KEY(ol_w_id, ol_d_id, ol_o_id, ol_number)
                ) ENGINE = InnoDB
            """)

            # Stock table
            ddl_statements.append(f"""
                CREATE TABLE IF NOT EXISTS stock{suffix} (
                    s_i_id int not null,
                    s_w_id smallint not null,
                    s_quantity smallint,
                    s_dist_01 char(24),
                    s_dist_02 char(24),
                    s_dist_03 char(24),
                    s_dist_04 char(24),
                    s_dist_05 char(24),
                    s_dist_06 char(24),
                    s_dist_07 char(24),
                    s_dist_08 char(24),
                    s_dist_09 char(24),
                    s_dist_10 char(24),
                    s_ytd decimal(8,0),
                    s_order_cnt smallint,
                    s_remote_cnt smallint,
                    s_data varchar(50),
                    PRIMARY KEY(s_w_id, s_i_id)
                ) ENGINE = InnoDB
            """)

            # Item table
            ddl_statements.append(f"""
                CREATE TABLE IF NOT EXISTS item{suffix} (
                    i_id int not null,
                    i_im_id int,
                    i_name varchar(24),
                    i_price decimal(5,2),
                    i_data varchar(50),
                    PRIMARY KEY(i_id)
                ) ENGINE = InnoDB
            """)

        return ddl_statements


def run_sysbench_tpcc_benchmark(
    tpcc_lua_path: Path,
    db_config: dict[str, Any],
    warehouses: int = 10,
    tables: int = 1,
    threads: int = 48,
    duration: int = 300,
    output_dir: Path | None = None,
) -> dict[str, Any]:
    """Convenience function to run a complete sysbench-tpcc benchmark."""
    db_driver = db_config.get("type", "mysql")
    if db_driver == "postgres":
        db_driver = "pgsql"

    default_port = 3306 if db_driver == "mysql" else 5432

    config = SysbenchTPCCConfig(
        tpcc_lua_path=tpcc_lua_path,
        warehouses=warehouses,
        tables=tables,
        threads=threads,
        duration=duration,
        db_driver=db_driver,
        db_host=db_config.get("host", "127.0.0.1"),
        db_port=db_config.get("port", default_port),
        db_name=db_config.get("database", "sbtest"),
        db_user=db_config.get("user", "root"),
        db_password=db_config.get("password", ""),
        output_dir=output_dir,
    )

    client = SysbenchTPCCClient(config)

    # Note: prepare might fail if database doesn't exist.
    # Usually sysbench expect the DB to exist.

    # Prepare
    prepare_results = client.prepare()

    # Run
    benchmark_results = client.run()

    # Cleanup
    client.cleanup()

    return {
        "config": {
            "warehouses": warehouses,
            "tables": tables,
            "threads": threads,
            "duration": duration,
        },
        "prepare": prepare_results,
        "benchmark": benchmark_results,
    }
