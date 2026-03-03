#!/usr/bin/env python3
"""
BenchmarkSQL client for TPC-C-like OLTP macrobenchmarking.

BenchmarkSQL is a Java-based TPC-C implementation that provides industry-standard
OLTP workload evaluation for database systems research.

Reference: https://github.com/petergeoghegan/benchmarksql
"""

from __future__ import annotations

import os
import subprocess
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any


@dataclass
class BenchmarkSQLConfig:
    """Configuration for BenchmarkSQL execution."""

    benchmarksql_home: Path
    warehouses: int = 10
    load_workers: int = 4
    terminals: int = 10
    run_mins: int = 5
    warmup_mins: int = 1
    db_type: str = "mariadb"  # Default to mariadb as it's MySQL-compatible and widely supported by BenchmarkSQL

    # Database connection
    db_host: str = "127.0.0.1"
    db_port: int = 3306
    db_name: str = "tpcc"
    db_user: str = "root"
    db_password: str = ""

    # Output
    output_dir: Path | None = None

    @property
    def props_file_path(self) -> Path:
        """Path to the generated properties file."""
        return self.benchmarksql_home / "props.mysql"


@dataclass
class TPCCTable:
    """TPC-C table specification."""

    name: str
    columns: list[str]
    primary_key: list[str]
    indexes: list[list[str]] = field(default_factory=list)


class BenchmarkSQLClient:
    """
    Client for running BenchmarkSQL TPC-C benchmarks.

    Provides:
    - Automated setup and configuration
    - Data loading with progress monitoring
    - Benchmark execution with timing
    - Result parsing and analysis
    - DDL operations extraction
    """

    # TPC-C schema definition (simplified for reference)
    TPC_C_TABLES = {
        "warehouse": TPCCTable(
            name="warehouse",
            columns=[
                "w_id",
                "w_name",
                "w_street_1",
                "w_street_2",
                "w_city",
                "w_state",
                "w_zip",
                "w_tax",
                "w_ytd",
            ],
            primary_key=["w_id"],
        ),
        "district": TPCCTable(
            name="district",
            columns=[
                "d_id",
                "d_w_id",
                "d_name",
                "d_street_1",
                "d_street_2",
                "d_city",
                "d_state",
                "d_zip",
                "d_tax",
                "d_ytd",
                "d_next_o_id",
            ],
            primary_key=["d_w_id", "d_id"],
        ),
        "customer": TPCCTable(
            name="customer",
            columns=[
                "c_id",
                "c_d_id",
                "c_w_id",
                "c_first",
                "c_middle",
                "c_last",
                "c_street_1",
                "c_street_2",
                "c_city",
                "c_state",
                "c_zip",
                "c_phone",
                "c_since",
                "c_credit",
                "c_credit_lim",
                "c_discount",
                "c_balance",
                "c_ytd_payment",
                "c_payment_cnt",
                "c_delivery_cnt",
                "c_data",
            ],
            primary_key=["c_w_id", "c_d_id", "c_id"],
            indexes=[["c_w_id", "c_d_id", "c_last"], ["c_w_id", "c_d_id", "c_since"]],
        ),
        "history": TPCCTable(
            name="history",
            columns=[
                "h_c_id",
                "h_c_d_id",
                "h_c_w_id",
                "h_d_id",
                "h_w_id",
                "h_date",
                "h_amount",
                "h_data",
            ],
            primary_key=[],  # No primary key
        ),
        "orders": TPCCTable(
            name="orders",
            columns=[
                "o_id",
                "o_d_id",
                "o_w_id",
                "o_c_id",
                "o_entry_d",
                "o_carrier_id",
                "o_ol_cnt",
                "o_all_local",
            ],
            primary_key=["o_w_id", "o_d_id", "o_id"],
            indexes=[["o_w_id", "o_d_id", "o_c_id"], ["o_w_id", "o_d_id", "o_entry_d"]],
        ),
        "new_order": TPCCTable(
            name="new_order",
            columns=["no_o_id", "no_d_id", "no_w_id"],
            primary_key=["no_w_id", "no_d_id", "no_o_id"],
        ),
        "order_line": TPCCTable(
            name="order_line",
            columns=[
                "ol_o_id",
                "ol_d_id",
                "ol_w_id",
                "ol_number",
                "ol_i_id",
                "ol_supply_w_id",
                "ol_delivery_d",
                "ol_quantity",
                "ol_amount",
                "ol_dist_info",
            ],
            primary_key=["ol_w_id", "ol_d_id", "ol_o_id", "ol_number"],
        ),
        "stock": TPCCTable(
            name="stock",
            columns=[
                "s_i_id",
                "s_w_id",
                "s_quantity",
                "s_dist_01",
                "s_dist_02",
                "s_dist_03",
                "s_dist_04",
                "s_dist_05",
                "s_dist_06",
                "s_dist_07",
                "s_dist_08",
                "s_dist_09",
                "s_dist_10",
                "s_ytd",
                "s_order_cnt",
                "s_remote_cnt",
                "s_data",
            ],
            primary_key=["s_w_id", "s_i_id"],
            indexes=[["s_w_id", "s_i_id"]],
        ),
        "item": TPCCTable(
            name="item",
            columns=["i_id", "i_im_id", "i_name", "i_price", "i_data"],
            primary_key=["i_id"],
        ),
    }

    def __init__(self, config: BenchmarkSQLConfig):
        """
        Initialize BenchmarkSQL client.

        Args:
            config: BenchmarkSQL configuration
        """
        self.config = config
        self._validate_installation()

    def _validate_installation(self) -> None:
        """Validate that BenchmarkSQL is properly installed."""
        if not self.config.benchmarksql_home.exists():
            raise RuntimeError(
                f"BenchmarkSQL home directory not found: {self.config.benchmarksql_home}"
            )

        required_files = [
            "runBenchmark.sh",
            "runLoader.sh",
            "runSQL.sh",
            "props.mysql.template",
        ]

        for file in required_files:
            if not (self.config.benchmarksql_home / file).exists():
                raise RuntimeError(f"Required BenchmarkSQL file not found: {file}")

        # Check if Java is available
        try:
            result = subprocess.run(
                ["java", "-version"], capture_output=True, text=True
            )
            if result.returncode != 0:
                raise RuntimeError("Java is not available")
        except FileNotFoundError:
            raise RuntimeError("Java is not installed")

    def _generate_props_file(self) -> Path:
        """Generate the properties file for BenchmarkSQL."""
        template_path = self.config.benchmarksql_home / "props.mysql.template"
        props_path = self.config.props_file_path

        if not template_path.exists():
            raise RuntimeError(f"Template file not found: {template_path}")

        # Read template
        with template_path.open() as f:
            template = f.read()

        # Replace configuration values
        replacements = {
            "db": self.config.db_type,
            "driver": "com.mysql.jdbc.Driver",  # Default to the standard MySQL driver
            "DBUrl": f"jdbc:mysql://{self.config.db_host}:{self.config.db_port}/{self.config.db_name}?useSSL=false&allowPublicKeyRetrieval=true",
            "conn": f"jdbc:mysql://{self.config.db_host}:{self.config.db_port}/{self.config.db_name}?useSSL=false&allowPublicKeyRetrieval=true",
            "DBUser": self.config.db_user,
            "user": self.config.db_user,
            "DBPassword": self.config.db_password,
            "password": self.config.db_password,
            "warehouses": str(self.config.warehouses),
            "loadWorkers": str(self.config.load_workers),
            "terminals": str(self.config.terminals),
            "runMins": str(self.config.run_mins),
            "warmupMins": str(self.config.warmup_mins),
        }

        for key, value in replacements.items():
            template = template.replace(f"{{{key}}}", value)

        # Write modified props file
        with props_path.open("w") as f:
            f.write(template)

        return props_path

    def _run_command(
        self, command: list[str], cwd: Path, timeout: int | None = None
    ) -> subprocess.CompletedProcess:
        """Run a BenchmarkSQL command with proper error handling."""
        try:
            env = os.environ.copy()
            env["JAVA_HOME"] = os.environ.get("JAVA_HOME", "")

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
                error_msg = f"BenchmarkSQL command failed: {' '.join(command)}\n"
                error_msg += f"STDOUT: {result.stdout}\n"
                error_msg += f"STDERR: {result.stderr}"
                raise RuntimeError(error_msg)

            return result

        except subprocess.TimeoutExpired:
            raise RuntimeError(
                f"BenchmarkSQL command timed out after {timeout} seconds: {' '.join(command)}"
            )

    def create_database(self) -> None:
        """Create the TPC-C database."""
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
            cursor.close()
            conn.close()
        except ImportError:
            raise RuntimeError("mysql-connector-python required for database creation")

    def load_data(self, progress_callback: callable | None = None) -> dict[str, Any]:
        """
        Load TPC-C data using BenchmarkSQL loader.

        Args:
            progress_callback: Optional callback for progress updates

        Returns:
            Dictionary with loading statistics
        """
        start_time = time.time()

        # Generate props file
        self._generate_props_file()

        # Run loader
        command = ["./runLoader.sh", "props.mysql"]
        result = self._run_command(
            command, self.config.benchmarksql_home, timeout=3600
        )  # 1 hour timeout

        end_time = time.time()
        load_time = end_time - start_time

        # Parse output for statistics
        stats = self._parse_loader_output(result.stdout)

        return {
            "load_time_seconds": load_time,
            "warehouses": self.config.warehouses,
            "output": result.stdout,
            **stats,
        }

    def _parse_loader_output(self, output: str) -> dict[str, Any]:
        """Parse BenchmarkSQL loader output for statistics."""
        stats = {}

        # Look for key metrics in output
        lines = output.split("\n")
        for line in lines:
            line = line.strip()
            if "Loading Item table" in line:
                stats["items_loaded"] = True
            elif "Loading Warehouse table" in line:
                stats["warehouses_loaded"] = True
            elif "Loading Stock table" in line:
                stats["stock_loaded"] = True
            elif "Loading District table" in line:
                stats["districts_loaded"] = True
            elif "Loading Customer table" in line:
                stats["customers_loaded"] = True
            elif "Loading Orders table" in line:
                stats["orders_loaded"] = True

        return stats

    def run_benchmark(self) -> dict[str, Any]:
        """
        Run the TPC-C benchmark.

        Returns:
            Dictionary with benchmark results
        """
        start_time = time.time()

        # Generate props file
        self._generate_props_file()

        # Run benchmark
        command = ["./runBenchmark.sh", "props.mysql"]
        result = self._run_command(
            command,
            self.config.benchmarksql_home,
            timeout=(self.config.run_mins + self.config.warmup_mins + 10)
            * 60,  # Extra 10 minutes
        )

        end_time = time.time()
        total_time = end_time - start_time

        # Parse results
        results = self._parse_benchmark_output(result.stdout)

        return {
            "total_time_seconds": total_time,
            "run_mins": self.config.run_mins,
            "warmup_mins": self.config.warmup_mins,
            "terminals": self.config.terminals,
            "warehouses": self.config.warehouses,
            "output": result.stdout,
            **results,
        }

    def _parse_benchmark_output(self, output: str) -> dict[str, Any]:
        """Parse BenchmarkSQL benchmark output for key metrics."""
        results = {}

        lines = output.split("\n")
        for line in lines:
            line = line.strip()

            # Look for performance metrics
            if "Measured tpmC" in line:
                # Extract TPM (transactions per minute)
                parts = line.split()
                for i, part in enumerate(parts):
                    if part.startswith("Measured"):
                        try:
                            results["tpm"] = float(parts[i + 2])
                        except (ValueError, IndexError):
                            pass
                        break

            elif "New Order" in line and "Average" in line:
                # Extract New Order transaction latency
                parts = line.split()
                for i, part in enumerate(parts):
                    if part == "Average":
                        try:
                            results["new_order_avg_ms"] = float(parts[i + 1])
                        except (ValueError, IndexError):
                            pass
                        break

        return results

    def get_ddl_operations(self) -> list[str]:
        """
        Extract DDL operations that would be performed by TPC-C workload.

        This is important for testing DDL audit plugins.

        Returns:
            List of DDL SQL statements
        """
        ddl_statements = []

        # Table creation DDL
        for table_name, table_spec in self.TPC_C_TABLES.items():
            columns_def = []
            for col in table_spec.columns:
                if col in table_spec.primary_key:
                    columns_def.append(f"{col} INT NOT NULL")
                else:
                    columns_def.append(f"{col} VARCHAR(255)")

            ddl = f"CREATE TABLE {table_name} ({', '.join(columns_def)}"
            if table_spec.primary_key:
                ddl += f", PRIMARY KEY ({', '.join(table_spec.primary_key)})"
            ddl += ");"
            ddl_statements.append(ddl)

        # Index creation DDL
        for table_name, table_spec in self.TPC_C_TABLES.items():
            for idx_cols in table_spec.indexes:
                idx_name = f"idx_{table_name}_{'_'.join(idx_cols)}"
                ddl = (
                    f"CREATE INDEX {idx_name} ON {table_name} ({', '.join(idx_cols)});"
                )
                ddl_statements.append(ddl)

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
        except ImportError:
            raise RuntimeError("mysql-connector-python required for cleanup")

    @classmethod
    def check_installation(cls, benchmarksql_home: Path) -> dict[str, Any]:
        """
        Check if BenchmarkSQL is properly installed.

        Returns:
            Dictionary with installation status and version info
        """
        status = {
            "installed": False,
            "version": None,
            "java_available": False,
            "required_files": [],
            "issues": [],
        }

        # Check Java
        try:
            result = subprocess.run(
                ["java", "-version"], capture_output=True, text=True
            )
            status["java_available"] = result.returncode == 0
        except FileNotFoundError:
            status["issues"].append("Java is not installed")

        # Check BenchmarkSQL files
        if benchmarksql_home.exists():
            required_files = ["runBenchmark.sh", "runLoader.sh", "props.mysql.template"]
            for file in required_files:
                file_path = benchmarksql_home / file
                if file_path.exists():
                    status["required_files"].append(file)
                else:
                    status["issues"].append(f"Missing file: {file}")

            if len(status["required_files"]) == len(required_files):
                status["installed"] = True

                # Try to get version from README or build files
                readme = benchmarksql_home / "README.md"
                if readme.exists():
                    content = readme.read_text()
                    if "version" in content.lower():
                        status["version"] = "present"
        else:
            status["issues"].append(
                f"BenchmarkSQL home directory not found: {benchmarksql_home}"
            )

        return status


def run_tpcc_benchmark(
    benchmarksql_home: Path,
    db_config: dict[str, Any],
    warehouses: int = 10,
    terminals: int = 10,
    run_mins: int = 5,
    output_dir: Path | None = None,
    db_type: str = "mariadb",
) -> dict[str, Any]:
    """
    Convenience function to run a complete TPC-C benchmark.

    Args:
        benchmarksql_home: Path to BenchmarkSQL installation
        db_config: Database connection config
        warehouses: Number of warehouses (scale factor)
        terminals: Number of client terminals
        run_mins: Benchmark runtime in minutes
        output_dir: Output directory for results
        db_type: BenchmarkSQL database type (e.g., 'mysql', 'postgres', 'mariadb')

    Returns:
        Dictionary with benchmark results
    """
    config = BenchmarkSQLConfig(
        benchmarksql_home=benchmarksql_home,
        warehouses=warehouses,
        terminals=terminals,
        run_mins=run_mins,
        db_host=db_config.get("host", "127.0.0.1"),
        db_port=db_config.get("port", 3306),
        db_name=db_config.get("database", "tpcc"),
        db_user=db_config.get("user", "root"),
        db_password=db_config.get("password", ""),
        db_type=db_type,
        output_dir=output_dir,
    )

    client = BenchmarkSQLClient(config)

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
            "terminals": terminals,
            "run_mins": run_mins,
        },
        "load": load_results,
        "benchmark": benchmark_results,
        "ddl_operations": client.get_ddl_operations(),
    }
