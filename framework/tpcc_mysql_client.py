#!/usr/bin/env python3
"""
tpcc-mysql client for TPC-C benchmarking.

tpcc-mysql is a high-performance, native MySQL implementation of TPC-C
that provides accurate OLTP workload simulation.

Reference: https://github.com/Percona-Lab/tpcc-mysql
"""

from __future__ import annotations

import os
import re
import subprocess
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any


@dataclass
class TPCCMySQLConfig:
    """Configuration for tpcc-mysql execution."""

    tpcc_home: Path
    warehouses: int = 10
    connections: int = 10
    duration: int = 300  # seconds
    warmup_time: int = 60  # seconds

    # Database connection
    db_host: str = "127.0.0.1"
    db_port: int = 3306
    db_name: str = "tpcc"
    db_user: str = "root"
    db_password: str = ""

    # Output
    output_dir: Path | None = None

    @property
    def mysql_args(self) -> list[str]:
        """MySQL connection arguments for tpcc-mysql."""
        args = [
            f"-h{self.db_host}",
            f"-P{self.db_port}",
            f"-u{self.db_user}",
        ]
        if self.db_password:
            args.append(f"-p{self.db_password}")
        return args

    @property
    def mysql_conn_str(self) -> str:
        """MySQL connection string for tpcc-mysql."""
        return " ".join(self.mysql_args)


class TPCCMySQLClient:
    """
    Client for running tpcc-mysql TPC-C benchmarks.

    Advantages over BenchmarkSQL:
    - Native C implementation (higher performance)
    - MySQL-optimized
    - Lower overhead
    - More accurate timing measurements
    """

    def __init__(self, config: TPCCMySQLConfig):
        """
        Initialize tpcc-mysql client.

        Args:
            config: tpcc-mysql configuration
        """
        self.config = config
        self._validate_installation()

    def _validate_installation(self) -> None:
        """Validate that tpcc-mysql is properly installed."""
        if not self.config.tpcc_home.exists():
            raise RuntimeError(
                f"tpcc-mysql home directory not found: {self.config.tpcc_home}"
            )

        required_executables = ["tpcc_load", "tpcc_start"]
        for exe in required_executables:
            exe_path = self.config.tpcc_home / exe
            if not exe_path.exists():
                raise RuntimeError(f"Required tpcc-mysql executable not found: {exe}")

            # Check if executable
            if not os.access(exe_path, os.X_OK):
                raise RuntimeError(f"tpcc-mysql executable not executable: {exe}")

    def _run_command(
        self,
        command: list[str],
        cwd: Path,
        timeout: int | None = None,
        log_dir: Path | None = None,
        label: str | None = None,
    ) -> subprocess.CompletedProcess:
        try:
            env = os.environ.copy()

            if log_dir is not None:
                from .command_runner import run_logged_command

                res = run_logged_command(
                    command,
                    log_dir,
                    cwd=cwd,
                    env=env,
                    timeout_s=timeout,
                    combine_stderr=False,
                    label=label or "tpcc-mysql",
                )
                stdout = res.stdout_path.read_text(encoding="utf-8", errors="replace")
                stderr = res.stderr_path.read_text(encoding="utf-8", errors="replace")
                cp = subprocess.CompletedProcess(
                    command, res.returncode, stdout, stderr
                )
                if cp.returncode != 0:
                    raise RuntimeError(
                        f"tpcc-mysql command failed with exit code {cp.returncode}: {' '.join(command)}\n"
                        f"Logs: {log_dir}\n"
                        f"Stdout: {res.stdout_path}\n"
                        f"Stderr: {res.stderr_path}\n"
                    )
                return cp

            result = subprocess.run(
                command,
                cwd=cwd,
                env=env,
                capture_output=True,
                text=True,
                encoding="utf-8",
                errors="replace",
                timeout=timeout,
            )

            if result.returncode != 0:
                error_msg = f"tpcc-mysql command failed: {' '.join(command)}\n"
                error_msg += f"STDOUT: {result.stdout}\n"
                error_msg += f"STDERR: {result.stderr}"
                raise RuntimeError(error_msg)

            return result

        except subprocess.TimeoutExpired as e:
            raise RuntimeError(
                f"tpcc-mysql command timed out after {timeout} seconds: {' '.join(command)}"
            ) from e

    def create_database(self) -> None:
        """Create the TPC-C database and its schema."""
        try:
            import mysql.connector

            conn = mysql.connector.connect(
                host=self.config.db_host,
                port=self.config.db_port,
                user=self.config.db_user,
                password=self.config.db_password,
            )
            cursor = conn.cursor()
            cursor.execute(f"DROP DATABASE IF EXISTS {self.config.db_name}")
            cursor.execute(f"CREATE DATABASE {self.config.db_name}")
            cursor.execute(f"USE {self.config.db_name}")

            # Execute DDL operations to create tables and indexes
            for statement in self.get_ddl_operations():
                cursor.execute(statement)

            cursor.close()
            conn.close()
        except ImportError as e:
            raise RuntimeError(
                "mysql-connector-python required for database creation"
            ) from e
        except Exception as e:
            raise RuntimeError(f"MySQL error during database creation: {e}") from e

    def load_data(self, progress_callback: Any | None = None) -> dict[str, Any]:
        start_time = time.time()

        # Run tpcc_load
        command = [
            "./tpcc_load",
            *self.config.mysql_args,
            "-d",
            self.config.db_name,
            "-w",
            str(self.config.warehouses),
        ]

        result = self._run_command(
            command, self.config.tpcc_home, timeout=1800
        )  # 30 minute timeout

        end_time = time.time()
        load_time = end_time - start_time

        # Parse output for statistics
        stats = self._parse_load_output(result.stdout)

        return {
            "load_time_seconds": load_time,
            "warehouses": self.config.warehouses,
            "output": result.stdout,
            **stats,
        }

    def _parse_load_output(self, output: str) -> dict[str, Any]:
        """Parse tpcc_load output for statistics."""
        stats = {}

        lines = output.split("\n")
        for line in lines:
            line = line.strip()

            # Look for completion messages
            if "LOADED WAREHOUSE" in line:
                try:
                    parts = line.split()
                    if len(parts) >= 3:
                        stats["warehouses_loaded"] = int(parts[2])
                except (ValueError, IndexError):
                    pass

            elif "DATA LOADING COMPLETED" in line:
                stats["load_completed"] = True

        return stats

    def run_benchmark(self) -> dict[str, Any]:
        """
        Run the TPC-C benchmark using tpcc_start.

        Returns:
            Dictionary with benchmark results
        """
        start_time = time.time()

        # Run tpcc_start
        command = [
            "./tpcc_start",
            *self.config.mysql_args,
            "-d",
            self.config.db_name,
            "-w",
            str(self.config.warehouses),
            "-c",
            str(self.config.connections),
            "-r",
            str(self.config.warmup_time),
            "-l",
            str(self.config.duration),
        ]

        result = self._run_command(
            command,
            self.config.tpcc_home,
            timeout=self.config.duration + self.config.warmup_time + 60,  # Extra minute
        )

        end_time = time.time()
        total_time = end_time - start_time

        # Parse results
        results = self._parse_benchmark_output(result.stdout)

        return {
            "total_time_seconds": total_time,
            "duration_seconds": self.config.duration,
            "warmup_seconds": self.config.warmup_time,
            "connections": self.config.connections,
            "warehouses": self.config.warehouses,
            "output": result.stdout,
            **results,
        }

    def _parse_benchmark_output(self, output: str) -> dict[str, Any]:
        """Parse tpcc_start output for key metrics."""
        results = {}

        # The output often contains [0] for total and [1...N] for each thread
        # We want to find the final summary or the [0] aggregate
        lines = output.split("\n")
        for line in lines:
            line = line.strip()

            # Look for TPM (transactions per minute) - common format: "[0] TPM: 123.4" or "<TpmC>\n 123.4 TpmC"
            if "TPM:" in line or "TpmC" in line:
                match = re.search(r"(?:TPM:|<TpmC>)\s*([0-9.]+)", line)
                if not match and "TpmC" in line:
                    # Try next line if it was <TpmC> on its own line
                    try:
                        next_line = lines[lines.index(line) + 1].strip()
                        match = re.search(r"([0-9.]+)", next_line)
                    except (IndexError, ValueError):
                        pass
                if match:
                    results["tpm"] = float(match.group(1))

            # Look for transaction latencies - common format: "NEWORDER AVG: 12.3" or "avg_rt: 12.3"
            if "AVG:" in line or "avg_rt:" in line:
                val_match = re.search(r"(?:AVG:|avg_rt:)\s*([0-9.]+)", line)
                if val_match:
                    val = float(val_match.group(1))
                    if "NEWORDER" in line or "[0]" in line:
                        results["new_order_avg_ms"] = val
                        results["avg_latency_ms"] = val
                    elif "PAYMENT" in line or "[1]" in line:
                        results["payment_avg_ms"] = val
                    elif "DELIVERY" in line or "[3]" in line:
                        results["delivery_avg_ms"] = val
                    elif "SLEV" in line or "[4]" in line:
                        results["stock_level_avg_ms"] = val
                    elif "OSTAT" in line or "[2]" in line:
                        results["order_status_avg_ms"] = val

        return results

    def get_ddl_operations(self) -> list[str]:
        """
        Extract DDL operations that would be performed by TPC-C workload.

        Since tpcc-mysql creates tables internally, we need to infer the schema.
        This is based on the standard TPC-C schema.

        Returns:
            List of DDL SQL statements that would be executed
        """
        # tpcc-mysql creates these standard TPC-C tables
        ddl_statements = [
            # Warehouse table
            """CREATE TABLE warehouse (
                w_id INT NOT NULL,
                w_name VARCHAR(10),
                w_street_1 VARCHAR(20),
                w_street_2 VARCHAR(20),
                w_city VARCHAR(20),
                w_state CHAR(2),
                w_zip CHAR(9),
                w_tax DECIMAL(4,4),
                w_ytd DECIMAL(12,2),
                PRIMARY KEY (w_id)
            );""",
            # District table
            """CREATE TABLE district (
                d_id INT NOT NULL,
                d_w_id INT NOT NULL,
                d_name VARCHAR(10),
                d_street_1 VARCHAR(20),
                d_street_2 VARCHAR(20),
                d_city VARCHAR(20),
                d_state CHAR(2),
                d_zip CHAR(9),
                d_tax DECIMAL(4,4),
                d_ytd DECIMAL(12,2),
                d_next_o_id INT,
                PRIMARY KEY (d_w_id, d_id),
                FOREIGN KEY (d_w_id) REFERENCES warehouse(w_id)
            );""",
            # Customer table
            """CREATE TABLE customer (
                c_id INT NOT NULL,
                c_d_id INT NOT NULL,
                c_w_id INT NOT NULL,
                c_first VARCHAR(16),
                c_middle CHAR(2),
                c_last VARCHAR(16),
                c_street_1 VARCHAR(20),
                c_street_2 VARCHAR(20),
                c_city VARCHAR(20),
                c_state CHAR(2),
                c_zip CHAR(9),
                c_phone CHAR(16),
                c_since DATETIME,
                c_credit CHAR(2),
                c_credit_lim DECIMAL(12,2),
                c_discount DECIMAL(4,4),
                c_balance DECIMAL(12,2),
                c_ytd_payment DECIMAL(12,2),
                c_payment_cnt INT,
                c_delivery_cnt INT,
                c_data TEXT,
                PRIMARY KEY (c_w_id, c_d_id, c_id),
                FOREIGN KEY (c_w_id, c_d_id) REFERENCES district(d_w_id, d_id)
            );""",
            # History table (no primary key in TPC-C)
            """CREATE TABLE history (
                h_c_id INT,
                h_c_d_id INT,
                h_c_w_id INT,
                h_d_id INT,
                h_w_id INT,
                h_date DATETIME,
                h_amount DECIMAL(6,2),
                h_data VARCHAR(24)
            );""",
            # Item table
            """CREATE TABLE item (
                i_id INT NOT NULL,
                i_im_id INT,
                i_name VARCHAR(24),
                i_price DECIMAL(5,2),
                i_data VARCHAR(50),
                PRIMARY KEY (i_id)
            );""",
            # Stock table
            """CREATE TABLE stock (
                s_i_id INT NOT NULL,
                s_w_id INT NOT NULL,
                s_quantity INT,
                s_dist_01 CHAR(24),
                s_dist_02 CHAR(24),
                s_dist_03 CHAR(24),
                s_dist_04 CHAR(24),
                s_dist_05 CHAR(24),
                s_dist_06 CHAR(24),
                s_dist_07 CHAR(24),
                s_dist_08 CHAR(24),
                s_dist_09 CHAR(24),
                s_dist_10 CHAR(24),
                s_ytd DECIMAL(8,0),
                s_order_cnt INT,
                s_remote_cnt INT,
                s_data VARCHAR(50),
                PRIMARY KEY (s_w_id, s_i_id),
                FOREIGN KEY (s_w_id) REFERENCES warehouse(w_id),
                FOREIGN KEY (s_i_id) REFERENCES item(i_id)
            );""",
            # Orders table
            """CREATE TABLE orders (
                o_id INT NOT NULL,
                o_d_id INT NOT NULL,
                o_w_id INT NOT NULL,
                o_c_id INT,
                o_entry_d DATETIME,
                o_carrier_id INT,
                o_ol_cnt INT,
                o_all_local INT,
                PRIMARY KEY (o_w_id, o_d_id, o_id),
                FOREIGN KEY (o_w_id, o_d_id) REFERENCES district(d_w_id, d_id),
                FOREIGN KEY (o_w_id, o_d_id, o_c_id) REFERENCES customer(c_w_id, c_d_id, c_id)
            );""",
            # New Order table
            """CREATE TABLE new_orders (
                no_o_id INT NOT NULL,
                no_d_id INT NOT NULL,
                no_w_id INT NOT NULL,
                PRIMARY KEY (no_w_id, no_d_id, no_o_id),
                FOREIGN KEY (no_w_id, no_d_id, no_o_id) REFERENCES orders(o_w_id, o_d_id, o_id)
            );""",
            # Order Line table
            """CREATE TABLE order_line (
                ol_o_id INT NOT NULL,
                ol_d_id INT NOT NULL,
                ol_w_id INT NOT NULL,
                ol_number INT NOT NULL,
                ol_i_id INT,
                ol_supply_w_id INT,
                ol_delivery_d DATETIME,
                ol_quantity INT,
                ol_amount DECIMAL(6,2),
                ol_dist_info CHAR(24),
                PRIMARY KEY (ol_w_id, ol_d_id, ol_o_id, ol_number),
                FOREIGN KEY (ol_w_id, ol_d_id, ol_o_id) REFERENCES orders(o_w_id, o_d_id, o_id),
                FOREIGN KEY (ol_supply_w_id, ol_i_id) REFERENCES stock(s_w_id, s_i_id)
            );""",
        ]

        # Add indexes that tpcc-mysql would create
        index_statements = [
            "CREATE INDEX idx_customer_name ON customer (c_w_id, c_d_id, c_last);",
            "CREATE INDEX idx_orders_customer ON orders (o_w_id, o_d_id, o_c_id);",
            "CREATE INDEX idx_orders_date ON orders (o_w_id, o_d_id, o_entry_d);",
            "CREATE INDEX idx_stock_item ON stock (s_w_id, s_i_id);",
        ]

        ddl_statements.extend(index_statements)
        return ddl_statements

    def cleanup(self) -> None:
        """Clean up the TPC-C database."""
        try:
            import mysql.connector

            conn = mysql.connector.connect(
                host=self.config.db_host,
                port=self.config.db_port,
                user=self.config.db_user,
                password=self.config.db_password,
            )
            cursor = conn.cursor()
            cursor.execute(f"DROP DATABASE IF EXISTS {self.config.db_name}")
            cursor.close()
            conn.close()
        except ImportError as e:
            raise RuntimeError("mysql-connector-python required for cleanup") from e

    @classmethod
    def check_installation(cls, tpcc_home: Path) -> dict[str, Any]:
        """
        Check if tpcc-mysql is properly installed.

        Returns:
            Dictionary with installation status
        """
        status = {
            "installed": False,
            "version": None,
            "executables": [],
            "issues": [],
        }

        if tpcc_home.exists():
            required_executables = ["tpcc_load", "tpcc_start"]
            for exe in required_executables:
                exe_path = tpcc_home / exe
                if exe_path.exists() and os.access(exe_path, os.X_OK):
                    status["executables"].append(exe)
                else:
                    status["issues"].append(f"Missing or non-executable: {exe}")

            if len(status["executables"]) == len(required_executables):
                status["installed"] = True

                # Try to get version info
                try:
                    result = subprocess.run(
                        ["./tpcc_load", "-V"],
                        cwd=tpcc_home,
                        capture_output=True,
                        text=True,
                        encoding="utf-8",
                        errors="replace",
                        timeout=10,
                    )
                    if result.returncode == 0:
                        status["version"] = result.stdout.strip()
                except Exception:
                    pass
        else:
            status["issues"].append(f"tpcc-mysql home directory not found: {tpcc_home}")

        return status


def run_tpcc_mysql_benchmark(
    tpcc_home: Path,
    db_config: dict[str, Any],
    warehouses: int = 10,
    connections: int = 10,
    duration: int = 300,
    output_dir: Path | None = None,
) -> dict[str, Any]:
    """
    Convenience function to run a complete tpcc-mysql benchmark.

    Args:
        tpcc_home: Path to tpcc-mysql installation
        db_config: Database connection config
        warehouses: Number of warehouses (scale factor)
        connections: Number of concurrent connections
        duration: Benchmark duration in seconds
        output_dir: Output directory for results

    Returns:
        Dictionary with benchmark results
    """
    config = TPCCMySQLConfig(
        tpcc_home=tpcc_home,
        warehouses=warehouses,
        connections=connections,
        duration=duration,
        db_host=db_config.get("host", "127.0.0.1"),
        db_port=db_config.get("port", 3306),
        db_name=db_config.get("database", "tpcc"),
        db_user=db_config.get("user", "root"),
        db_password=db_config.get("password", ""),
        output_dir=output_dir,
    )

    client = TPCCMySQLClient(config)

    # Create database
    client.create_database()

    # Load data
    load_results = client.load_data()

    # Run benchmark
    benchmark_results = client.run_benchmark()

    # Cleanup
    client.cleanup()

    return {
        "config": {
            "warehouses": warehouses,
            "connections": connections,
            "duration": duration,
        },
        "load": load_results,
        "benchmark": benchmark_results,
        "ddl_operations": client.get_ddl_operations(),
    }
