#!/usr/bin/env python3
"""
PostgreSQL pgbench runner for OLTP benchmarking.

Provides pgbench integration for PostgreSQL authorization benchmarking,
similar to the sysbench runner for MySQL.
"""

from __future__ import annotations

import os
import re
import subprocess
import time
from dataclasses import dataclass, field
from typing import Any

import requests

from .config import Config


@dataclass
class PgBenchConfig:
    """Configuration for pgbench execution."""

    binary: str = "pgbench"
    docker: bool = False
    container_name: str | None = None
    system_name: str = "postgres-baseline"  # Name of the system being tested
    db_name: str = "abac_test"
    host: str = "127.0.0.1"
    port: int = 5432
    user: str = "postgres"
    password: str | None = None
    scale: int = 10  # Scale factor (rows = scale * 100000)
    clients: int = 1
    threads: int = 1
    no_vacuum_run: bool = True
    duration: int = 60  # seconds
    warmup_time: int = 10  # seconds
    builtin: str | None = "tpcb-like"  # tpcb-like, simple-update, select-only
    query_mode: str = "simple"  # simple, extended, prepared
    script: str | None = None  # Custom script file
    report_latencies: bool = False  # pgbench -r flag
    fillfactor: int = 100
    no_vacuum: bool = False
    foreign_keys: bool = False
    partitioning: bool = False
    unlogged_tables: bool = False

    @property
    def connection_string(self) -> str:
        """PostgreSQL connection string for pgbench."""
        conn_parts = [
            f"host={self.host}",
            f"port={self.port}",
            f"user={self.user}",
            f"dbname={self.db_name}",
        ]
        if self.password:
            conn_parts.append(f"password={self.password}")

        return " ".join(conn_parts)

    def get_command_args(self, action: str) -> list[str]:
        """Get pgbench command arguments for the specified action."""
        args = []

        if self.docker and self.container_name:
            args.extend(["docker", "exec", "-i"])
            if self.password:
                args.extend(["-e", f"PGPASSWORD={self.password}"])
            args.append(self.container_name)

        args.append(self.binary)

        # Connection options
        if self.docker:
            # When running inside docker, host should often be 127.0.0.1
            # if we are exec'ing into the DB container itself.
            args.extend(["-h", self.host, "-p", str(self.port), "-U", self.user])
        else:
            # For local execution, use the connection string property
            # which will be appended at the end as a positional argument
            pass

        # Action-specific options
        if action == "initialize":
            args.append("--initialize")
            args.extend(["--scale", str(self.scale)])
            if self.fillfactor != 100:
                args.extend(["--fillfactor", str(self.fillfactor)])
            if self.no_vacuum:
                args.append("--no-vacuum")
            if self.foreign_keys:
                args.append("--foreign-keys")
            if self.partitioning:
                args.append("--partitioning")
            if self.unlogged_tables:
                args.append("--unlogged-tables")

        elif action == "run":
            args.extend(["--client", str(self.clients)])
            args.extend(["--jobs", str(self.threads)])
            args.extend(["--time", str(self.duration)])
            args.extend(["--protocol", self.query_mode])

            # Avoid pgbench's pre-run VACUUM/TRUNCATE when the benchmark user
            # intentionally lacks native privileges (e.g., Cedar path). We do an
            # explicit superuser vacuum step separately.
            if self.no_vacuum_run:
                args.append("--no-vacuum")

            if self.report_latencies:
                args.append("-r")

            # Add scale variable for custom scripts
            args.extend(["--define", f"scale={self.scale}"])
            # Add user variable for custom scripts
            args.extend(["--define", f"user={self.user}"])
            # Add flush flag for custom scripts
            do_flush = 1 if (self.script and "no_cache" in self.script) else 0
            args.extend(["--define", f"do_flush={do_flush}"])

            if self.builtin:
                args.extend(["--builtin", self.builtin])
            elif self.script:
                # For custom scripts, pgbench will otherwise report scale factor as 1
                # unless explicitly provided.
                args.extend(["--scale", str(self.scale)])
                args.extend(["--file", self.script])

        elif action == "vacuum":
            args.append("--vacuum-all")

        # Database name / connection string (positional argument at the end)
        if self.docker:
            args.append(self.db_name)
        else:
            args.append(self.connection_string)

        return args


@dataclass
class PgBenchResult:
    """Results from a pgbench run."""

    timestamp: float
    config: PgBenchConfig
    action: str

    # Raw pgbench output
    stdout: str
    stderr: str
    returncode: int

    # Parsed metrics (for run action)
    transactions_processed: int = 0
    transactions_per_second: float = 0.0
    avg_latency_ms: float = 0.0
    latency_stddev: float = 0.0
    initial_connection_time_ms: float = 0.0

    # Per-statement latencies (if available)
    statement_latencies: dict[str, float] = field(default_factory=dict)

    # Authorization plugin stats (if available)
    auth_stats: dict[str, Any] = field(default_factory=dict)

    # Strace summary (if available)
    strace_summary: list[dict[str, Any]] = field(default_factory=list)

    # Perf summary (if available)
    perf_summary: dict[str, Any] = field(default_factory=dict)

    # Perf record output (if available)
    perf_record: str = ""

    # Errors
    errors: int = 0

    def parse_output(self) -> None:
        """Parse pgbench output to extract metrics."""
        if self.action != "run":
            return

        # Combine stdout and stderr as pgbench sometimes outputs to stderr
        output = self.stdout + "\n" + self.stderr
        lines = output.split("\n")

        # Regex-based parsing is more robust across pgbench versions/locales
        # Examples seen in the wild:
        # - "tps = 2571.216667 (including connections establishing)"
        # - "tps =2571.216667 (including connections establishing)"
        # - "latency average = 1.555 ms"
        tps_re = re.compile(r"\btps\s*=\s*([0-9]+(?:\.[0-9]+)?)")
        txn_re = re.compile(r"number of transactions actually processed:\s*([0-9]+)")
        lat_avg_re = re.compile(r"latency average\s*=\s*([0-9]+(?:\.[0-9]+)?)\s*ms")
        lat_std_re = re.compile(r"latency stddev\s*=\s*([0-9]+(?:\.[0-9]+)?)\s*ms")
        conn_re = re.compile(
            r"initial connection time\s*=\s*([0-9]+(?:\.[0-9]+)?)\s*ms"
        )

        for line in lines:
            raw = line.strip()
            line = raw.lower()
            if not line:
                continue

            # Prefer regex parsing first (robust)
            m = tps_re.search(line)
            if m:
                try:
                    val = float(m.group(1))
                    # If pgbench prints both "including" and "excluding", prefer excluding.
                    if "excluding" in line or self.transactions_per_second == 0.0:
                        self.transactions_per_second = val
                except ValueError:
                    pass
                continue

            m = txn_re.search(line)
            if m:
                try:
                    self.transactions_processed = int(m.group(1))
                except ValueError:
                    pass
                continue

            m = lat_avg_re.search(line)
            if m:
                try:
                    self.avg_latency_ms = float(m.group(1))
                except ValueError:
                    pass
                continue

            m = lat_std_re.search(line)
            if m:
                try:
                    self.latency_stddev = float(m.group(1))
                except ValueError:
                    pass
                continue

            m = conn_re.search(line)
            if m:
                try:
                    self.initial_connection_time_ms = float(m.group(1))
                except ValueError:
                    pass
                continue

            # Parse transaction rate
            # Example: tps = 2571.216667 (including connections establishing)
            if "tps =" in line:
                parts = line.split()
                for i, part in enumerate(parts):
                    if part == "=":
                        try:
                            # Use parts[i+1] for the value after '='
                            val = float(parts[i + 1])
                            # Prefer 'excluding' connections if both are present,
                            # or just take the first one found.
                            if (
                                "excluding" in line
                                or self.transactions_per_second == 0.0
                            ):
                                self.transactions_per_second = val
                        except (ValueError, IndexError):
                            pass
                        break

            # Parse transaction count
            # Example: number of transactions actually processed: 154273
            elif "number of transactions actually processed:" in line:
                parts = line.split(":")
                if len(parts) >= 2:
                    try:
                        self.transactions_processed = int(parts[1].strip())
                    except ValueError:
                        pass

            # Parse latency
            # Example: latency average = 1.555 ms
            elif "latency average =" in line:
                parts = line.split()
                for i, part in enumerate(parts):
                    if part == "=":
                        try:
                            self.avg_latency_ms = float(parts[i + 1])
                        except (ValueError, IndexError):
                            pass
                        break

            # Parse latency stddev
            # Example: latency stddev = 0.123 ms
            elif "latency stddev =" in line:
                parts = line.split()
                for i, part in enumerate(parts):
                    if part == "=":
                        try:
                            self.latency_stddev = float(parts[i + 1])
                        except (ValueError, IndexError):
                            pass
                        break

            # Parse initial connection time
            # Example: initial connection time = 9.308 ms
            elif "initial connection time =" in line:
                parts = line.split()
                for i, part in enumerate(parts):
                    if part == "=":
                        try:
                            self.initial_connection_time_ms = float(parts[i + 1])
                        except (ValueError, IndexError):
                            pass
                        break

        # Parse per-statement latencies if present
        # Format is usually:
        # statement latencies in milliseconds:
        #         0.002  \set aid ...
        #         1.456  UPDATE ...
        if "statement latencies in milliseconds:" in output:
            lat_section = output.split("statement latencies in milliseconds:")[1]
            for line in lat_section.split("\n"):
                line = line.strip()
                if not line or line.startswith("tps =") or line.startswith("number of"):
                    continue
                # Match "0.123  SQL_COMMAND"
                match = re.match(r"([0-9]+\.[0-9]+)\s+(.+)", line)
                if match:
                    try:
                        ms = float(match.group(1))
                        cmd = match.group(2).strip()
                        self.statement_latencies[cmd] = ms
                    except ValueError:
                        pass

    def to_dict(self) -> dict[str, Any]:
        """Convert result to dictionary."""
        return {
            "timestamp": self.timestamp,
            "config": {
                "scale": self.config.scale,
                "clients": self.config.clients,
                "threads": self.config.threads,
                "duration": self.config.duration,
                "builtin": self.config.builtin,
            },
            "action": self.action,
            "transactions_processed": self.transactions_processed,
            "transactions_per_second": self.transactions_per_second,
            "avg_latency_ms": self.avg_latency_ms,
            "latency_stddev": self.latency_stddev,
            "initial_connection_time_ms": self.initial_connection_time_ms,
            "statement_latencies": self.statement_latencies,
            "auth_stats": self.auth_stats,
            "strace_summary": self.strace_summary,
            "perf_summary": self.perf_summary,
            "perf_record": self.perf_record,
            "errors": self.errors,
            "returncode": self.returncode,
            "stdout": self.stdout,
            "stderr": self.stderr,
        }


class PgBenchRunner:
    """
    Runner for PostgreSQL pgbench benchmarks.

    Provides:
    - Database initialization
    - Benchmark execution
    - Result parsing and analysis
    - Integration with the experiment framework
    """

    def __init__(self, config: PgBenchConfig):
        self.config = config

    def _run_command(
        self,
        args: list[str],
        env: dict[str, str] | None = None,
        input_str: str | None = None,
    ) -> tuple[int, str, str]:
        """Run a pgbench command and return results."""
        try:
            # Set up environment
            cmd_env = os.environ.copy()
            if env:
                cmd_env.update(env)

            # Add password to environment if NOT using docker
            # (If using docker, we use -e PGPASSWORD=... in the docker exec command itself)
            if self.config.password and not self.config.docker:
                cmd_env["PGPASSWORD"] = self.config.password

            result = subprocess.run(
                args,
                input=input_str,
                capture_output=True,
                text=True,
                encoding="utf-8",
                errors="replace",
                env=cmd_env,
                timeout=3600,  # 1 hour timeout
            )

            return result.returncode, result.stdout, result.stderr

        except subprocess.TimeoutExpired:
            return -1, "", "Command timed out"
        except Exception as e:
            return -1, "", f"Command failed: {e}"

    def initialize_database(self) -> PgBenchResult:
        """Initialize the pgbench database."""
        args = self.config.get_command_args("initialize")

        returncode, stdout, stderr = self._run_command(args)

        # Ensure the pg_authorization extension is created
        # (This is needed for the flush functions in no-cache experiments)
        ext_sql = "CREATE EXTENSION IF NOT EXISTS pg_authorization;"
        if self.config.docker:
            ext_args = [
                "docker",
                "exec",
                "-i",
                self.config.container_name,
                "psql",
                "-h",
                "127.0.0.1",
                "-U",
                "postgres",
                "-d",
                self.config.db_name,
                "-c",
                ext_sql,
            ]
        else:
            ext_args = [
                "psql",
                "-h",
                self.config.host,
                "-p",
                str(self.config.port),
                "-U",
                "postgres",
                "-d",
                self.config.db_name,
                "-c",
                ext_sql,
            ]

        ext_rc, ext_out, ext_err = self._run_command(ext_args)
        if ext_rc != 0:
            print(f"Warning: Failed to create pg_authorization extension: {ext_err}")

        result = PgBenchResult(
            timestamp=time.time(),
            config=self.config,
            action="initialize",
            stdout=stdout,
            stderr=stderr,
            returncode=returncode,
        )

        return result

    def vacuum_database(self) -> PgBenchResult:
        """Vacuum the pgbench database."""
        args = self.config.get_command_args("vacuum")

        returncode, stdout, stderr = self._run_command(args)

        result = PgBenchResult(
            timestamp=time.time(),
            config=self.config,
            action="vacuum",
            stdout=stdout,
            stderr=stderr,
            returncode=returncode,
        )

        return result

    def run_benchmark(self) -> PgBenchResult:
        """Run the pgbench benchmark."""
        # Manual warmup if requested
        if self.config.warmup_time > 0:
            print(f"Running manual warmup for {self.config.warmup_time}s...")
            # Create temporary duration change for warmup
            original_duration = self.config.duration
            self.config.duration = self.config.warmup_time

            input_str = None
            if self.config.script and self.config.docker:
                with open(self.config.script) as f:
                    input_str = f.read()
                # If we are piping the script, we must use -f -
                args = self.config.get_command_args("run")
                # Replace the script path with '-'
                for i, arg in enumerate(args):
                    if arg == self.config.script:
                        args[i] = "-"
                returncode, stdout, stderr = self._run_command(
                    args, input_str=input_str
                )
            else:
                args = self.config.get_command_args("run")
                returncode, stdout, stderr = self._run_command(args)

            # Restore original duration
            self.config.duration = original_duration
            print("Warmup complete. Starting actual benchmark...")

        args = self.config.get_command_args("run")
        input_str = None

        # If running in docker with a custom script, we need to pipe the script to stdin
        if self.config.script and self.config.docker:
            with open(self.config.script) as f:
                input_str = f.read()
            # Replace script path with '-' in args
            for i, arg in enumerate(args):
                if arg == self.config.script:
                    args[i] = "-"
            returncode, stdout, stderr = self._run_command(args, input_str=input_str)
        else:
            returncode, stdout, stderr = self._run_command(args)

        result = PgBenchResult(
            timestamp=time.time(),
            config=self.config,
            action="run",
            stdout=stdout,
            stderr=stderr,
            returncode=returncode,
        )

        # Parse the output
        result.parse_output()

        return result

    def set_guc(self, name: str, value: str) -> bool:
        """Set a GUC variable in the database."""
        # Split into two separate commands to avoid "ALTER SYSTEM cannot run inside a transaction block"
        # which can happen if psql sends multiple commands in a single query string.
        commands = [f"ALTER SYSTEM SET {name} = {value};", "SELECT pg_reload_conf();"]

        for sql in commands:
            if self.config.docker:
                if not self.config.container_name:
                    print("Failed to set GUC: missing docker container name")
                    return False
                args = ["docker", "exec", "-i"]
                if self.config.password:
                    args.extend(["-e", f"PGPASSWORD={self.config.password}"])
                args.extend(
                    [
                        self.config.container_name,
                        "psql",
                        "-h",
                        "127.0.0.1",
                        "-U",
                        "postgres",
                        "-d",
                        "postgres",
                        "-c",
                        sql,
                    ]
                )
            else:
                args = [
                    "psql",
                    "-h",
                    self.config.host,
                    "-p",
                    str(self.config.port),
                    "-U",
                    "postgres",
                    "-d",
                    "postgres",
                    "-c",
                    sql,
                ]

            returncode, stdout, stderr = self._run_command(args)
            if returncode != 0:
                print(f"Failed to execute GUC command ({sql}): {stderr}")
                return False

        # Wait a bit for reload
        time.sleep(1)
        return True

    def get_authorization_stats(self) -> dict[str, Any]:
        """Fetch authorization statistics from the database."""
        sql = "SELECT * FROM pg_authorization_stats();"
        if self.config.docker:
            if not self.config.container_name:
                print("Warning: missing docker container name; cannot fetch stats")
                return {}
            args = ["docker", "exec", "-i"]
            if self.config.password:
                args.extend(["-e", f"PGPASSWORD={self.config.password}"])
            args.extend(
                [
                    self.config.container_name,
                    "psql",
                    "-h",
                    "127.0.0.1",
                    "-U",
                    "postgres",
                    "-d",
                    self.config.db_name,
                    "-X",
                    "-A",
                    "-c",
                    sql,
                ]
            )
        else:
            args = [
                "psql",
                "-h",
                self.config.host,
                "-p",
                str(self.config.port),
                "-U",
                "postgres",
                "-d",
                self.config.db_name,
                "-X",
                "-A",
                "-c",
                sql,
            ]

        returncode, stdout, stderr = self._run_command(args)
        if returncode != 0:
            print(f"Warning: Failed to fetch authorization stats: {stderr}")
            return {}

        # Parse the output (format: col1|col2|...\nval1|val2|...)
        lines = stdout.strip().split("\n")
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

    def reset_authorization_stats(self) -> bool:
        """Reset authorization statistics in the database."""
        sql = "SELECT pg_authorization_reset_stats();"
        if self.config.docker:
            if not self.config.container_name:
                print("Warning: missing docker container name; cannot reset stats")
                return False
            args = ["docker", "exec", "-i"]
            if self.config.password:
                args.extend(["-e", f"PGPASSWORD={self.config.password}"])
            args.extend(
                [
                    self.config.container_name,
                    "psql",
                    "-h",
                    "127.0.0.1",
                    "-U",
                    "postgres",
                    "-d",
                    self.config.db_name,
                    "-c",
                    sql,
                ]
            )
        else:
            args = [
                "psql",
                "-h",
                self.config.host,
                "-p",
                str(self.config.port),
                "-U",
                "postgres",
                "-d",
                self.config.db_name,
                "-c",
                sql,
            ]

        returncode, stdout, stderr = self._run_command(args)
        return returncode == 0

    def _ensure_debug_symbols(self) -> bool:
        """Ensure PostgreSQL debug symbols are installed in the container/host."""
        if not self.config.docker:
            # For native, we just warn the user as installing requires root and varies by OS
            return True

        print(f"Checking for debug symbols in {self.config.container_name}...")

        # 1. Get PostgreSQL version
        ver_args = ["docker", "exec", self.config.container_name, "psql", "--version"]
        rc, stdout, stderr = self._run_command(ver_args)
        if rc != 0:
            return False

        # Example: psql (PostgreSQL) 17.0 (Debian 17.0-1.pgdg120+1)
        version_match = re.search(r"PostgreSQL\) ([0-9]+)", stdout)
        if not version_match:
            return False
        pg_version = version_match.group(1)

        # 2. Check if debug symbols are already installed (check for a known file)
        # Usually debug symbols for postgres binary are in /usr/lib/debug/usr/lib/postgresql/XX/bin/postgres
        check_args = [
            "docker",
            "exec",
            self.config.container_name,
            "ls",
            f"/usr/lib/debug/usr/lib/postgresql/{pg_version}/bin/postgres",
        ]
        rc, _, _ = self._run_command(check_args)
        if rc == 0:
            print("  ✓ Debug symbols already installed.")
            return True

        # 3. Install debug symbols
        print(f"  Installing postgresql-{pg_version}-dbgsym...")
        # We must use sh -c to ensure the whole chain runs inside the container as root
        install_cmd = (
            f"apt-get update && apt-get install -y postgresql-{pg_version}-dbgsym"
        )
        install_args = [
            "docker",
            "exec",
            "-u",
            "root",
            self.config.container_name,
            "sh",
            "-c",
            install_cmd,
        ]

        rc, stdout, stderr = self._run_command(install_args)
        if rc == 0:
            print("  ✓ Debug symbols installed successfully.")
            return True
        else:
            print(f"  Warning: Failed to install debug symbols: {stderr}")
            return False

    def collect_strace_profile(self, duration: int = 5) -> list[dict[str, Any]]:
        """
        Collect strace summary for the active pgbench backends.
        Note: Requires --privileged docker container and strace installed in container,
        or strace installed on host if running natively.
        """
        # 1. Find the PIDs of the backends for the benchmark user
        pids = self._get_active_pids()

        if not pids:
            print(
                f"Warning: Could not find active backend PIDs for user {self.config.user}."
            )
            return []

        print(
            f"Profiling {len(pids)} backend(s) (PIDs: {', '.join(pids)}) for {duration}s..."
        )

        # 2. Run strace summary
        # Note: strace -p only takes one PID, but we can use -P or multiple -p
        # For simplicity and to match the summary format, we'll profile the first one
        # or use a loop if needed. strace -c is best on a single process.
        target_pid = pids[0]

        if self.config.docker:
            strace_args = [
                "docker",
                "exec",
                "--privileged",
                self.config.container_name,
                "strace",
                "-p",
                target_pid,
                "-c",
                "sleep",
                str(duration),
            ]
        else:
            strace_args = ["strace", "-p", target_pid, "-c", "sleep", str(duration)]
            if os.getuid() != 0:
                strace_args = ["sudo"] + strace_args

        # strace -c outputs to stderr
        rc, stdout, stderr = self._run_command(strace_args)

        if rc != 0 and "Process " not in stderr:
            print(f"Warning: strace failed: {stderr}")
            return []

        return self._parse_strace_summary(stderr)

    def _parse_strace_summary(self, stderr: str) -> list[dict[str, Any]]:
        """Parse strace -c output."""
        # Example output:
        # % time     seconds  usecs/call     calls    errors syscall
        # ------ ----------- ----------- --------- --------- ----------------
        #  22.77    0.408996           5     74524           sendto
        # ...

        lines = stderr.split("\n")
        results = []

        # Regex to match strace -c output lines
        # Group 1: % time, Group 2: seconds, Group 3: usecs/call, Group 4: calls, Group 5: errors (optional), Group 6: syscall
        # Note: errors column can be empty, and syscall name can contain numbers
        line_re = re.compile(
            r"^\s*([0-9]+\.[0-9]+)\s+([0-9]+\.[0-9]+)\s+([0-9]+)\s+([0-9]+)\s+([0-9]*)\s*(\w+)"
        )

        for line in lines:
            line = line.strip()
            if not line or "------" in line or "total" in line or "seconds" in line:
                continue

            match = line_re.match(line)
            if match:
                try:
                    results.append(
                        {
                            "percent_time": float(match.group(1)),
                            "seconds": float(match.group(2)),
                            "usecs_per_call": int(match.group(3)),
                            "calls": int(match.group(4)),
                            "errors": int(match.group(5)) if match.group(5) else 0,
                            "syscall": match.group(6),
                        }
                    )
                except (ValueError, IndexError):
                    continue
            else:
                # Fallback for lines where regex might fail but split works
                parts = line.split()
                if len(parts) >= 5:
                    try:
                        # If len is 5, errors is empty
                        if len(parts) == 5:
                            results.append(
                                {
                                    "percent_time": float(parts[0]),
                                    "seconds": float(parts[1]),
                                    "usecs_per_call": int(parts[2]),
                                    "calls": int(parts[3]),
                                    "errors": 0,
                                    "syscall": parts[4],
                                }
                            )
                        else:
                            results.append(
                                {
                                    "percent_time": float(parts[0]),
                                    "seconds": float(parts[1]),
                                    "usecs_per_call": int(parts[2]),
                                    "calls": int(parts[3]),
                                    "errors": int(parts[4]),
                                    "syscall": parts[5],
                                }
                            )
                    except (ValueError, IndexError):
                        continue

        return results

    def collect_perf_profile(self, duration: int = 5) -> dict[str, Any]:
        """
        Collect perf stat summary for the active pgbench backends.
        Note: Requires --privileged docker container and perf installed in container,
        or perf installed on host if running natively.
        Usually fails on macOS Docker due to virtualization limits.
        """
        # 1. Find the PIDs
        pids = self._get_active_pids()

        if not pids:
            print(
                f"Warning: Could not find active backend PIDs for user {self.config.user}"
            )
            return {}

        pid_str = ",".join(pids)
        print(f"Running perf stat on {len(pids)} PID(s) ({pid_str}) for {duration}s...")

        # 2. Run perf stat
        # If running in docker, we prefer host-side profiling if we have permissions,
        # as it avoids kernel/perf version mismatches.
        if self.config.docker:
            # Try host-side profiling first if we are on Linux
            import platform

            if platform.system() == "Linux":
                host_pids = self._get_host_pids_for_container()
                if host_pids:
                    host_pid_str = ",".join(host_pids)
                    print(
                        f"Attempting host-side profiling of container PIDs: {host_pid_str}"
                    )
                    perf_args = [
                        "perf",
                        "stat",
                        "-p",
                        host_pid_str,
                        "-e",
                        "cpu-clock,task-clock,context-switches,cpu-migrations,page-faults",
                        "sleep",
                        str(duration),
                    ]
                    if os.getuid() != 0:
                        perf_args = ["sudo"] + perf_args

                    rc, stdout, stderr = self._run_command(perf_args)
                    if rc == 0:
                        return self._parse_perf_stat(stderr)
                    else:
                        print(
                            f"Host-side profiling failed: {stderr.splitlines()[0] if stderr else 'unknown error'}"
                        )
                        if "permission" in stderr.lower():
                            self._print_perf_permission_warning()

            # Fallback to container-side profiling
            perf_args = [
                "docker",
                "exec",
                "--privileged",
                self.config.container_name,
                "perf",
                "stat",
                "-p",
                pid_str,
                "-e",
                "cpu-clock,task-clock,context-switches,cpu-migrations,page-faults",
                "sleep",
                str(duration),
            ]
        else:
            perf_args = [
                "perf",
                "stat",
                "-p",
                pid_str,
                "-e",
                "cpu-clock,task-clock,context-switches,cpu-migrations,page-faults",
                "sleep",
                str(duration),
            ]
            if os.getuid() != 0:
                perf_args = ["sudo"] + perf_args

        rc, stdout, stderr = self._run_command(perf_args)

        if rc == 0:
            stats = self._parse_perf_stat(stderr)
            if stats:
                print("\nPerf Stat Summary:")
                for k, v in stats.items():
                    print(f"  {k}: {v}")
            return stats

        if rc != 0:
            if (
                "permission" in stderr.lower()
                or "EPERM" in stderr
                or "No permission" in stderr
            ):
                self._print_perf_permission_warning()

            print(f"Warning: perf failed: {stderr}")
            # Try a simpler version if the above fails
            print("Retrying with minimal perf events...")
            if self.config.docker:
                perf_args = [
                    "docker",
                    "exec",
                    "--privileged",
                    self.config.container_name,
                    "perf",
                    "stat",
                    "-p",
                    pid_str,
                    "sleep",
                    str(duration),
                ]
            else:
                perf_args = ["perf", "stat", "-p", pid_str, "sleep", str(duration)]
                if os.getuid() != 0:
                    perf_args = ["sudo"] + perf_args
            rc, stdout, stderr = self._run_command(perf_args)
            if rc != 0:
                return {"error": stderr}

        return self._parse_perf_stat(stderr)

    def _print_perf_permission_warning(self):
        """Print a helpful warning about perf permissions."""
        print("\n" + "=" * 60)
        print("PERFORMANCE PROFILING PERMISSION ERROR")
        print("=" * 60)
        print(
            "The Linux kernel is blocking perf. Please run the following on the HOST:"
        )
        print("  sudo sysctl -w kernel.perf_event_paranoid=1")
        print("  sudo sysctl -w kernel.kptr_restrict=0")
        print(
            "\nIf using Docker, also ensure the container has CAP_PERFMON and CAP_SYS_PTRACE:"
        )
        print("  services:")
        print("    postgres-cedar:")
        print("      cap_add:")
        print("        - PERFMON")
        print("        - SYS_PTRACE")
        print("=" * 60 + "\n")

    def _get_host_pids_for_container(self) -> list[str]:
        """Get the host PIDs for the processes running inside the container."""
        if not self.config.docker or not self.config.container_name:
            return []

        # docker top <container> -o pid,ppid,comm
        args = ["docker", "top", self.config.container_name, "-o", "pid,ppid,comm"]
        rc, stdout, stderr = self._run_command(args)
        if rc != 0:
            return []

        all_host_pids = []
        lines = stdout.strip().split("\n")
        # Skip header
        for line in lines[1:]:
            parts = line.split()
            if len(parts) >= 3:
                hpid, hppid, comm = parts[0], parts[1], parts[2]
                if "postgres" in comm:
                    all_host_pids.append((hpid, hppid))

        if not all_host_pids:
            return []

        # Identify the postmaster (the one with the lowest PID)
        all_host_pids.sort(key=lambda x: int(x[0]))
        postmaster_pid = all_host_pids[0][0]

        # Backends are usually direct children of the postmaster
        backend_pids = [p[0] for p in all_host_pids if p[1] == postmaster_pid]

        # If we couldn't find children by PPID, just take all but the postmaster
        if not backend_pids:
            backend_pids = [p[0] for p in all_host_pids[1:]]

        return backend_pids

    def _parse_perf_stat(self, stderr: str) -> dict[str, Any]:
        """Parse perf stat output."""
        stats = {}
        lines = stderr.split("\n")

        for line in lines:
            line = line.strip()
            if not line:
                continue

            # Example: 5,001.23 msec task-clock
            if "task-clock" in line:
                m = re.search(r"([0-9,.]+)\s+msec\s+task-clock", line)
                if m:
                    stats["task_clock_ms"] = float(m.group(1).replace(",", ""))

            # Example: 1,234 context-switches
            if "context-switches" in line:
                m = re.search(r"([0-9,.]+)\s+context-switches", line)
                if m:
                    stats["context_switches"] = int(m.group(1).replace(",", ""))

            # Example: 123 cpu-migrations
            if "cpu-migrations" in line:
                m = re.search(r"([0-9,.]+)\s+cpu-migrations", line)
                if m:
                    stats["cpu_migrations"] = int(m.group(1).replace(",", ""))

            # Example: 45 page-faults
            if "page-faults" in line:
                m = re.search(r"([0-9,.]+)\s+page-faults", line)
                if m:
                    stats["page_faults"] = int(m.group(1).replace(",", ""))

        return stats

    def collect_perf_record(self, duration: int = 5) -> str:
        """
        Collect 'perf record' with call graphs for the active backends.
        Note: Requires Linux host and --privileged container (if using Docker).
        """
        pids = self._get_active_pids()
        if not pids:
            return "Error: Could not find active backend PIDs"

        pid_str = ",".join(pids)
        print(
            f"Recording perf call-graph for {len(pids)} PID(s) ({pid_str}) for {duration}s..."
        )

        # 1. Record samples with call-graph
        # We prefer host-side profiling for better reliability and symbol resolution
        import platform

        is_linux = platform.system() == "Linux"
        perf_data_path = "/tmp/perf.data"

        # Ensure we start with a clean slate
        if os.path.exists(perf_data_path):
            try:
                if os.getuid() == 0:
                    os.remove(perf_data_path)
                else:
                    subprocess.run(["sudo", "rm", "-f", perf_data_path], check=False)
            except Exception:
                pass

        if self.config.docker and is_linux:
            host_pids = self._get_host_pids_for_container()
            if host_pids:
                host_pid_str = ",".join(host_pids)
                print(
                    f"Attempting host-side perf record of container PIDs: {host_pid_str}"
                )
                # Use -o to specify explicit path
                record_args = [
                    "perf",
                    "record",
                    "-o",
                    perf_data_path,
                    "-p",
                    host_pid_str,
                    "-e",
                    "task-clock",
                    "-g",
                    "--call-graph",
                    "fp",
                    "sleep",
                    str(duration),
                ]
                if os.getuid() != 0:
                    record_args = ["sudo"] + record_args

                rc, _, stderr = self._run_command(record_args)
                if rc == 0:
                    report_args = [
                        "perf",
                        "report",
                        "-i",
                        perf_data_path,
                        "--stdio",
                        "--header",
                        "--children",
                    ]
                    if os.getuid() != 0:
                        report_args = ["sudo"] + report_args
                    rc, stdout, stderr = self._run_command(report_args)
                    if rc == 0:
                        return self._process_perf_report(stdout)

                print(
                    f"Host-side perf record failed: {stderr.splitlines()[0] if stderr else 'unknown error'}"
                )
                if "permission" in stderr.lower():
                    self._print_perf_permission_warning()

        # Fallback to container-side profiling or native host profiling
        if self.config.docker:
            # First, check if perf is available in container
            check_args = ["docker", "exec", self.config.container_name, "which", "perf"]
            rc, _, _ = self._run_command(check_args)
            if rc != 0:
                return "Error: perf not found in container. Please install linux-tools-generic or equivalent."

            # Use an explicit path in the container
            container_perf_data = "/tmp/perf.data"
            record_args = [
                "docker",
                "exec",
                "--privileged",
                self.config.container_name,
                "perf",
                "record",
                "-o",
                container_perf_data,
                "-p",
                pid_str,
                "-e",
                "task-clock",
                "-g",
                "--call-graph",
                "fp",
                "sleep",
                str(duration),
            ]
            self._run_command(record_args)

            # 2. Generate report
            report_args = [
                "docker",
                "exec",
                "--privileged",
                self.config.container_name,
                "perf",
                "report",
                "-i",
                container_perf_data,
                "--stdio",
                "--header",
                "--children",
            ]
        else:
            # Native execution
            record_args = [
                "perf",
                "record",
                "-o",
                perf_data_path,
                "-p",
                pid_str,
                "-e",
                "task-clock",
                "-g",
                "--call-graph",
                "fp",
                "sleep",
                str(duration),
            ]
            if os.getuid() != 0:
                record_args = ["sudo"] + record_args

            self._run_command(record_args)

            report_args = [
                "perf",
                "report",
                "-i",
                perf_data_path,
                "--stdio",
                "--header",
                "--children",
            ]
            if os.getuid() != 0:
                report_args = ["sudo"] + report_args

        rc, stdout, stderr = self._run_command(report_args)

        if rc != 0:
            if (
                "permission" in stderr.lower()
                or "EPERM" in stderr
                or "No permission" in stderr
            ):
                self._print_perf_permission_warning()
            return f"Error: perf report failed: {stderr}"

        return self._process_perf_report(stdout)

    def _process_perf_report(self, stdout: str) -> str:
        """Process and print the perf report output."""
        if not stdout or stdout.strip() == "":
            print(
                "\nWarning: perf report produced no output. This can happen if no samples were collected."
            )
            return "Error: Empty perf report"

        # Print top functions for immediate feedback
        print("\nTop functions by latency (from perf report):")
        lines = stdout.split("\n")
        found_header = False
        count = 0
        for line in lines:
            if "Children" in line and "Self" in line and "Symbol" in line:
                found_header = True
                print(line)
                continue
            if found_header and line.strip() and not line.startswith("#"):
                print(line)
                count += 1
                if count > 20:
                    break

        if (
            "[unknown]" in stdout
            and stdout.count("[unknown]") > stdout.count("\n") * 0.3
        ):
            print(
                "\nWarning: Many symbols are '[unknown]'. Consider installing debug symbols for PostgreSQL."
            )
            if self.config.docker:
                print(
                    f"Try: docker exec -u root {self.config.container_name} apt-get update && apt-get install -y postgresql-17-dbgsym (version may vary)"
                )
            else:
                print(
                    "Try: sudo apt-get install postgresql-17-dbgsym (or equivalent for your OS/version)"
                )

        return stdout

    def _get_active_pids(self) -> list[str]:
        """Helper to get all active backend PIDs for the benchmark user."""
        pid_sql = f"SELECT pid FROM pg_stat_activity WHERE usename = '{self.config.user}' AND query NOT LIKE '%pg_stat_activity%';"

        if self.config.docker:
            pid_args = [
                "docker",
                "exec",
                "-i",
                self.config.container_name,
                "psql",
                "-U",
                "postgres",
                "-d",
                self.config.db_name,
                "-t",
                "-A",
                "-c",
                pid_sql,
            ]
        else:
            pid_args = [
                "psql",
                "-h",
                self.config.host,
                "-p",
                str(self.config.port),
                "-U",
                "postgres",
                "-d",
                self.config.db_name,
                "-t",
                "-A",
                "-c",
                pid_sql,
            ]

        rc, stdout, stderr = self._run_command(pid_args)
        # Filter and split by newline to get all PIDs
        pids = [p.strip() for p in stdout.strip().split("\n") if p.strip().isdigit()]
        return pids

    def setup_benchmark_user(self, target_user: str) -> bool:
        """Create the target user and optionally grant native permissions."""
        if target_user == "postgres":
            return True

        # Build the complete SQL setup
        setup_parts = []

        # Base user creation (idempotent)
        setup_parts.append(f"""
DO $$
BEGIN
    IF NOT EXISTS (SELECT FROM pg_catalog.pg_user WHERE usename = '{target_user}') THEN
        CREATE USER {target_user} WITH PASSWORD 'postgres';
    ELSE
        ALTER USER {target_user} WITH PASSWORD 'postgres';
    END IF;
END $$;
""")

        # Only grant native permissions for baseline
        # For cedar, we want native checks to fail so the plugin is triggered
        if "baseline" in self.config.system_name:
            print(f"Applying native GRANTs for {self.config.system_name}...")
            setup_parts.append(f"""
GRANT CONNECT ON DATABASE {self.config.db_name} TO {target_user};
GRANT USAGE ON SCHEMA public TO {target_user};
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO {target_user};
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO {target_user};
ALTER TABLE pgbench_accounts OWNER TO {target_user};
ALTER TABLE pgbench_branches OWNER TO {target_user};
ALTER TABLE pgbench_tellers OWNER TO {target_user};
ALTER TABLE pgbench_history OWNER TO {target_user};
""")
        else:
            print(
                f"Skipping native GRANTs for {self.config.system_name} (testing plugin hooks)..."
            )
            # We still need to make sure the user can at least be seen
            # but we don't grant them any table-level permissions.
            pass

        setup_sql = "\n".join(setup_parts)

        # Construct psql command
        if self.config.docker:
            if not self.config.container_name:
                print("Failed to setup benchmark user: missing docker container name")
                return False
            args = ["docker", "exec", "-i"]
            if self.config.password:
                args.extend(["-e", f"PGPASSWORD={self.config.password}"])
            args.extend(
                [
                    self.config.container_name,
                    "psql",
                    "-h",
                    "127.0.0.1",
                    "-U",
                    "postgres",
                    "-d",
                    self.config.db_name,
                    "-c",
                    setup_sql,
                ]
            )
        else:
            args = [
                "psql",
                "-h",
                self.config.host,
                "-p",
                str(self.config.port),
                "-U",
                "postgres",
                "-d",
                self.config.db_name,
                "-c",
                setup_sql,
            ]

        returncode, stdout, stderr = self._run_command(args)
        if returncode != 0:
            print(f"Failed to setup benchmark user: {stderr}")
            return False
        return True

    def run_full_benchmark(
        self,
    ) -> tuple[PgBenchResult, PgBenchResult | None, PgBenchResult | None]:
        """
        Run a complete pgbench workflow: initialize, vacuum, benchmark.

        Returns:
            Tuple of (init_result, vacuum_result, benchmark_result)
        """
        original_user = self.config.user

        # Step 1: Initialize (always as superuser)
        print(f"Initializing pgbench database (scale={self.config.scale})...")
        self.config.user = "postgres"
        init_result = self.initialize_database()

        if init_result.returncode != 0:
            print(f"Initialization failed: {init_result.stderr}")
            self.config.user = original_user
            return init_result, None, None

        # Step 2: Vacuum (always as superuser)
        print("Vacuuming database...")
        self.config.user = "postgres"
        vacuum_result = self.vacuum_database()

        # Step 3: Setup benchmark user if needed
        if original_user != "postgres":
            print(f"Setting up benchmark user '{original_user}'...")
            if not self.setup_benchmark_user(original_user):
                print("Warning: User setup failed, proceeding as original user anyway.")

        # Step 4: Run actual benchmark as the target user
        print(
            f"Running pgbench benchmark as '{original_user}' (clients={self.config.clients}, duration={self.config.duration}s)..."
        )
        self.config.user = original_user
        benchmark_result = self.run_benchmark()

        return init_result, vacuum_result, benchmark_result


def run_pgbench_experiment(
    config: Config,
    db_system: str = "postgres-baseline",
    scale: int = 10,
    clients: int = 1,
    jobs: int | None = None,
    duration: int = 60,
    builtin: str | None = "tpcb-like",
    warmup: int = 0,
    script: str | None = None,
    prepare: bool = True,
    benchmark: bool = True,
    profile: bool = False,
    query_mode: str = "simple",
    strace: bool = False,
    strace_duration: int = 5,
    perf: bool = False,
    perf_duration: int = 5,
    perf_record: bool = False,
    perf_record_path: str | None = None,
    cedar_gucs: dict[str, str] | None = None,
    results_suffix: str | None = None,
) -> dict[str, Any]:
    """
    Run a complete pgbench experiment.

    Args:
        config: Experiment configuration
        db_system: Database system to use ("postgres-baseline" or "postgres-cedar")
        scale: pgbench scale factor
        clients: Number of concurrent clients
        jobs: Number of pgbench worker threads (defaults to jobs=clients)
        duration: Benchmark duration in seconds
        builtin: Built-in test to run
        warmup: Warmup duration in seconds
        script: Path to custom pgbench script
        prepare: If true, initialize/vacuum/setup before running
        benchmark: If true, run the timed benchmark
        profile: If true, report per-statement latencies
        query_mode: pgbench query mode
        strace: If true, collect strace profiling
        strace_duration: Duration of strace collection in seconds
        perf: If true, collect perf profiling
        perf_duration: Duration of perf collection in seconds
        perf_record: If true, collect detailed perf record with call graphs
        perf_record_path: Optional path to save the detailed perf record text file
        cedar_gucs: Optional GUC variables to set for Cedar
        results_suffix: Optional suffix for result files

    Returns:
        Dictionary with experiment results
    """
    db_config = config.databases[db_system]
    pg_cfg = config.pgbench

    if clients < 1:
        clients = 1
    threads = jobs if jobs is not None else getattr(pg_cfg, "jobs", None)
    if not threads or threads < 1:
        threads = clients

    pgbench_config = PgBenchConfig(
        binary=pg_cfg.binary,
        docker=pg_cfg.docker,
        container_name=pg_cfg.container_name or db_system,
        system_name=db_system,
        host=db_config.host if not pg_cfg.docker else "127.0.0.1",
        port=db_config.port if not pg_cfg.docker else 5432,
        user=db_config.user,
        password=db_config.password,
        db_name=db_config.database,
        scale=scale,
        clients=clients,
        threads=int(threads),
        duration=duration,
        builtin=builtin if not script else None,
        query_mode=query_mode,
        script=script,
        warmup_time=warmup,
        report_latencies=profile,
    )

    runner = PgBenchRunner(pgbench_config)

    # Split the workflow to allow entity registration after initialization
    original_user = pgbench_config.user

    init_result = None
    vacuum_result = None
    if prepare:
        # 1. Initialize and vacuum (always as superuser)
        print(f"Initializing pgbench database (scale={pgbench_config.scale})...")
        pgbench_config.user = "postgres"
        init_result = runner.initialize_database()
        if init_result.returncode != 0:
            print(f"Initialization failed: {init_result.stderr}")
            pgbench_config.user = original_user
            return _build_results_dict(
                db_system,
                scale,
                clients,
                duration,
                builtin,
                init_result,
                None,
                None,
                results_suffix=results_suffix,
            )

        print("Vacuuming database...")
        vacuum_result = runner.vacuum_database()

        # 2. Setup benchmark user if needed
        if original_user != "postgres":
            print(f"Setting up benchmark user '{original_user}'...")
            if not runner.setup_benchmark_user(original_user):
                print("Warning: User setup failed, proceeding as original user anyway.")

        # 3. Apply GUCs if needed
        if db_system == "postgres-cedar" and cedar_gucs:
            # Use superuser to set GUC
            pgbench_config.user = "postgres"
            for name, val in cedar_gucs.items():
                print(f"Setting GUC {name} = {val}...")
                runner.set_guc(name, val)
            pgbench_config.user = original_user

        # 4. If testing cedar, ensure entities/policies/attributes are registered
        # This MUST happen after initialization/user creation because the plugin
        # might have synced them with empty attributes during creation.
        if db_system == "postgres-cedar":
            _register_postgres_cedar_entities(config, pgbench_config, original_user)

    bench_result = None
    if benchmark:
        # Run actual benchmark as the target user
        print(
            f"Running pgbench benchmark as '{original_user}' (clients={pgbench_config.clients}, duration={pgbench_config.duration}s)..."
        )
        pgbench_config.user = original_user

        # Reset stats before the actual run to get clean numbers for this benchmark
        if db_system == "postgres-cedar":
            runner.reset_authorization_stats()

        if strace or perf or perf_record:
            # Ensure debug symbols are present for better profiling
            if perf_record or strace:
                runner._ensure_debug_symbols()

            # Run pgbench in a separate thread so we can profile it
            import threading

            bench_result_container = []

            def run_bench():
                res = runner.run_benchmark()
                bench_result_container.append(res)

            thread = threading.Thread(target=run_bench)
            thread.start()

            # Wait for pgbench to start and establish connections
            time.sleep(min(5, duration / 2))

            # Collect strace if requested
            strace_summary = []
            if strace:
                strace_summary = runner.collect_strace_profile(
                    duration=min(strace_duration, duration - 5)
                )

            # Collect perf if requested
            perf_summary = {}
            if perf:
                perf_summary = runner.collect_perf_profile(
                    duration=min(perf_duration, duration - 5)
                )

            # Collect perf record if requested
            perf_record_out = ""
            if perf_record:
                perf_record_out = runner.collect_perf_record(
                    duration=min(perf_duration, duration - 5)
                )
                if (
                    perf_record_path
                    and perf_record_out
                    and not perf_record_out.startswith("Error:")
                ):
                    try:
                        # Ensure directory exists
                        os.makedirs(
                            os.path.dirname(os.path.abspath(perf_record_path)),
                            exist_ok=True,
                        )
                        with open(perf_record_path, "w") as f:
                            f.write(perf_record_out)
                        print(f"  ✓ Detailed perf report saved to {perf_record_path}")
                        # Replace with path in the results dict to save space
                        perf_record_out = f"Saved to {perf_record_path}"
                    except Exception as e:
                        print(
                            f"  Warning: Failed to save perf record to {perf_record_path}: {e}"
                        )

            # Wait for benchmark to finish
            thread.join()
            bench_result = bench_result_container[0]
            bench_result.strace_summary = strace_summary
            bench_result.perf_summary = perf_summary
            bench_result.perf_record = perf_record_out
        else:
            bench_result = runner.run_benchmark()

        # Fetch stats after the run
        if db_system == "postgres-cedar" and bench_result.returncode == 0:
            bench_result.auth_stats = runner.get_authorization_stats()

    results = _build_results_dict(
        db_system,
        scale,
        clients,
        duration,
        builtin,
        init_result,
        vacuum_result,
        bench_result,
        results_suffix=results_suffix,
    )
    return results


def _build_results_dict(
    db_system,
    scale,
    clients,
    duration,
    builtin,
    init_result,
    vacuum_result,
    bench_result,
    results_suffix=None,
):
    """Helper to build the results dictionary."""
    results = {
        "experiment": "pgbench",
        "db_system": db_system,
        "results_suffix": results_suffix,
        "config": {
            "scale": scale,
            "clients": clients,
            "duration": duration,
            "builtin": builtin,
        },
        "results": {
            "initialization": init_result.to_dict() if init_result else None,
            "vacuum": vacuum_result.to_dict() if vacuum_result else None,
            "benchmark": bench_result.to_dict() if bench_result else None,
        },
    }

    # Add success indicators
    if bench_result and bench_result.returncode == 0:
        results["success"] = True
        results["tps"] = bench_result.transactions_per_second
        results["avg_latency_ms"] = bench_result.avg_latency_ms
        if bench_result.statement_latencies:
            results["statement_latencies"] = bench_result.statement_latencies
    elif bench_result is None and init_result and init_result.returncode == 0:
        # Prepare-only path (initialize/vacuum succeeded, benchmark intentionally skipped)
        results["success"] = True
    else:
        results["success"] = False
        if bench_result:
            results["error"] = bench_result.stderr
        elif init_result:
            results["error"] = init_result.stderr or "Initialization failed"
        else:
            results["error"] = "Initialization failed"

    return results


def _register_postgres_cedar_entities(
    config: Config, pg_config: PgBenchConfig, target_user: str
):
    """Register necessary entities, schema, and policies in Cedar agent for PostgreSQL pgbench."""
    base_url = config.cedar_agent.url
    if not base_url.endswith("/v1"):
        base_url = base_url.rstrip("/") + "/v1"

    namespace = "PostgreSQL"

    try:
        import json

        from .translate_to_cedar import (
            assign_database_attributes,
            assign_user_attributes,
            create_cedar_policies,
            create_entity,
            setup_cedar_schema,
            wait_for_entities,
        )

        if not config.auth_spec_path:
            raise RuntimeError(
                "auth_spec_path is required for pgbench Cedar registration"
            )

        # Load auth spec
        with open(config.auth_spec_path) as f:
            spec = json.load(f)

        print(f"[{namespace}] Ensuring Cedar agent schema attributes are registered...")
        setup_cedar_schema(base_url, spec, namespace)

        # 1. Register Policies
        # Merge general policies from auth_spec with our dynamic bench_user policies

        # Dynamic policies for bench_user (since we removed them from auth_spec)
        bench_policies = [
            {
                "id": "bench_user_db_access",
                "privileges": ["CONNECT"],
                "condition": f"principal is {namespace}::User AND principal.user_role == 'benchmarking' AND resource is {namespace}::Database",
                "description": "Allow bench_user to connect to databases",
            },
            {
                "id": "bench_user_schema_access",
                "privileges": ["USAGE"],
                "condition": f"principal is {namespace}::User AND principal.user_role == 'benchmarking' AND resource is {namespace}::Schema",
                "description": "Allow bench_user to use schemas (PostgreSQL)",
            },
            {
                "id": "bench_user_table_access",
                "privileges": ["SELECT", "INSERT", "UPDATE", "DELETE"],
                "condition": f"principal is {namespace}::User AND principal.user_role == 'benchmarking' AND resource is {namespace}::Table",
                "description": "Allow bench_user to perform table operations",
            },
            {
                "id": "superuser_permit",
                "privileges": ["*"],
                "condition": f'principal == {namespace}::User::"postgres"',
                "description": "Superuser permit",
            },
        ]

        # We need to temporarily add these to the spec structure so create_cedar_policies can process them
        # (it handles condition conversion to Cedar syntax)
        temp_spec = spec.copy()
        if "policies" not in temp_spec:
            temp_spec["policies"] = []
        temp_spec["policies"].extend(bench_policies)

        print(f"[{namespace}] Registering policies...")
        policies = create_cedar_policies(temp_spec, namespace)

        # Get existing policies
        existing_resp = requests.get(f"{base_url}/policies", timeout=5)
        existing_ids = []
        if existing_resp.status_code == 200:
            existing_ids = [p["id"] for p in existing_resp.json()]

        for policy in policies:
            if policy["id"] in existing_ids:
                continue
            resp = requests.post(f"{base_url}/policies", json=policy, timeout=5)
            if resp.status_code not in (200, 201, 204, 409):
                print(
                    f"  Warning: Failed to register policy {policy['id']}: {resp.status_code}"
                )

        # 2. Wait for Entities to Propagate (DDL Plugin should create them)
        # We need to wait for the tables to appear before we can assign attributes

        # Define the tables we expect (based on pgbench standard tables)
        # Note: DDL plugin uses "schema.table" format for IDs in PostgreSQL namespace
        pgbench_tables = [
            "public.pgbench_accounts",
            "public.pgbench_branches",
            "public.pgbench_history",
            "public.pgbench_tellers",
        ]

        expected_entities = [("Table", t) for t in pgbench_tables]
        expected_entities.append(("User", target_user))
        expected_entities.append(("User", "postgres"))

        print(
            f"[{namespace}] Waiting for pgbench entities to propagate from DDL plugin..."
        )
        # Give it some time - pgbench initialization just finished
        wait_for_entities(
            base_url, expected_entities, namespace, max_wait=10, check_interval=1
        )

        # 3. Assign Attributes
        # We can't use assign_resource_attributes directly as these tables aren't in spec
        # So we do it manually for our dynamic tables

        print(f"[{namespace}] Assigning attributes to pgbench entities...")

        # Assign attributes to tables
        for table_id in pgbench_tables:
            # We want data_classification = "public" to match simple policies if we had them,
            # but more importantly we need them to correspond to what policies expect?
            # Actually, the policies we added check 'resource is Table', they don't check attributes.
            # But let's add attributes anyway for completeness/future-proofing.
            try:
                requests.put(
                    f"{base_url}/data/attribute",
                    json={
                        "entity_type": "Table",
                        "namespace": namespace,
                        "entity_id": table_id,
                        "attribute_name": "data_classification",
                        "attribute_value": "public",
                    },
                )
            except Exception as e:
                print(f"  Warning: Failed to set attributes for table {table_id}: {e}")

        # Assign attributes to users
        # bench_user needs user_role = 'benchmarking'
        try:
            requests.put(
                f"{base_url}/data/attribute",
                json={
                    "entity_type": "User",
                    "namespace": namespace,
                    "entity_id": target_user,
                    "attribute_name": "user_role",
                    "attribute_value": "benchmarking",
                },
            )
        except Exception as e:
            print(f"  Warning: Failed to set attributes for user {target_user}: {e}")

        # postgres user needs user_role = 'admin'
        try:
            requests.put(
                f"{base_url}/data/attribute",
                json={
                    "entity_type": "User",
                    "namespace": namespace,
                    "entity_id": "postgres",
                    "attribute_name": "user_role",
                    "attribute_value": "admin",
                },
            )
        except Exception as e:
            print(f"  Warning: Failed to set attributes for user postgres: {e}")

        # Also run the standard assignment for other entities in spec (if any overlap/persist)
        assign_user_attributes(base_url, spec, namespace)
        assign_database_attributes(base_url, spec, namespace)
        # assign_resource_attributes(base_url, spec, namespace) # Skip resources from spec since we removed pgbench tables

        # 4. Ensure public schema is registered
        try:
            create_entity(base_url, "Schema", "public", namespace)
        except Exception:
            pass

    except Exception as e:
        print(f"  Warning: Error during Cedar registration: {e}")
        import traceback

        traceback.print_exc()


def _cleanup_postgres_cedar_entities(config: Config):
    """Cleanup policies and attributes created for pgbench."""
    base_url = config.cedar_agent.url
    if not base_url.endswith("/v1"):
        base_url = base_url.rstrip("/") + "/v1"

    namespace = "PostgreSQL"
    print(f"[{namespace}] Cleaning up pgbench policies and attributes...")

    # 1. Delete dynamic policies
    policies_to_delete = [
        "bench_user_db_access",
        "bench_user_schema_access",
        "bench_user_table_access",
        "superuser_permit",
    ]

    try:
        for policy_id in policies_to_delete:
            # We used namespace prefixed IDs for some, check if we need to adjust
            # The registration code used: "id": policy['id']
            # And bench_policies defined IDs simply like "bench_user_db_access"
            # BUT create_cedar_policies prefixes them with namespace.lower() + "_" if namespace is provided

            # Let's try both forms just in case
            ids_to_try = [policy_id, f"{namespace.lower()}_{policy_id}"]

            for pid in ids_to_try:
                try:
                    requests.delete(f"{base_url}/policies/{pid}", timeout=5)
                except Exception:
                    pass
    except Exception as e:
        print(f"  Warning: Error deleting policies: {e}")

    # 2. Reset attributes (optional)
    # Ideally we would remove the attributes we added, but Cedar Agent/Data Store
    # might not support fine-grained attribute removal easily via simple API calls
    # without retrieving and updating the whole entity.
    # Given we might be re-running benchmarks, leaving attributes is mostly harmless
    # as long as policies are gone or we re-register them.
    # However, strictly speaking, we added user_role=benchmarking to bench_user.
    pass


def compare_pgbench_systems(
    config: Config,
    scale: int = 10,
    clients: int = 1,
    duration: int = 60,
    builtin: str = "tpcb-like",
    warmup: int = 0,
    query_mode: str = "simple",
    strace: bool = False,
    perf: bool = False,
    perf_record: bool = False,
) -> dict[str, Any]:
    """
    Compare pgbench performance between baseline and Cedar PostgreSQL.

    Args:
        config: Experiment configuration
        scale: pgbench scale factor
        clients: Number of concurrent clients
        duration: Benchmark duration in seconds
        builtin: Built-in pgbench test
        warmup: Warmup duration in seconds
        query_mode: pgbench query mode
        strace: If true, collect strace profiling
        perf: If true, collect perf profiling
        perf_record: If true, collect perf record

    Returns:
        Dictionary with comparison results
    """
    results = {
        "scale": scale,
        "clients": clients,
        "duration": duration,
        "builtin": builtin,
        "timestamp": time.time(),
        "systems": {},
    }

    # Run baseline
    print(f"\n=== Running Baseline PostgreSQL (Scale {scale}, Clients {clients}) ===")
    baseline_res = run_pgbench_experiment(
        config,
        db_system="postgres-baseline",
        scale=scale,
        clients=clients,
        duration=duration,
        builtin=builtin,
        warmup=warmup,
        query_mode=query_mode,
        strace=strace,
        perf=perf,
        perf_record=perf_record,
        results_suffix="baseline",
    )
    results["systems"]["postgres-baseline"] = baseline_res

    # Run Cedar
    print(f"\n=== Running Cedar PostgreSQL (Scale {scale}, Clients {clients}) ===")

    # Enable caching for Cedar
    cedar_gucs = {
        "pg_authorization.cedar_agent_url": "'http://cedar-agent:8180'",
        "pg_authorization.namespace": "'PostgreSQL'",
        "pg_authorization.cache_enabled": "on",
        "pg_authorization.collect_stats": "off",
    }

    cedar_res: dict[str, Any] = {"error": "Cedar experiment not run"}
    try:
        cedar_res = run_pgbench_experiment(
            config,
            db_system="postgres-cedar",
            scale=scale,
            clients=clients,
            duration=duration,
            builtin=builtin,
            warmup=warmup,
            query_mode=query_mode,
            strace=strace,
            perf=perf,
            perf_record=perf_record,
            cedar_gucs=cedar_gucs,
            results_suffix="cedar",
        )
        results["systems"]["postgres-cedar"] = cedar_res
    except Exception as e:
        print(f"Error running Cedar experiment: {e}")
        import traceback

        traceback.print_exc()
        cedar_res = {"error": str(e)}
        results["systems"]["postgres-cedar"] = cedar_res
    finally:
        # Cleanup Cedar entities/policies
        _cleanup_postgres_cedar_entities(config)

    # Calculate overhead
    baseline_tps = 0
    cedar_tps = 0

    if baseline_res["results"]["benchmark"] and baseline_res["results"][
        "benchmark"
    ].get("tps"):
        baseline_tps = baseline_res["results"]["benchmark"]["tps"]

    cedar_bench = cedar_res.get("results", {}).get("benchmark")
    if "error" not in cedar_res and cedar_bench and cedar_bench.get("tps"):
        cedar_tps = cedar_bench["tps"]

    overhead = float("inf")
    if cedar_tps > 0:
        overhead = (baseline_tps - cedar_tps) / cedar_tps * 100

    results["comparison"] = {
        "baseline_tps": baseline_tps,
        "cedar_tps": cedar_tps,
        "overhead_percent": overhead,
    }

    print("\n=== Results Summary ===")
    print(f"Baseline TPS: {baseline_tps:.2f}")
    print(f"Cedar TPS:    {cedar_tps:.2f}")
    print(f"Overhead:     {overhead:.2f}%")

    return results
