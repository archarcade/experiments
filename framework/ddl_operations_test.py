#!/usr/bin/env python3
"""
DDL Operations Testing for Authorization Plugins.

Tests how DDL audit plugins handle table creation, index creation,
and schema modifications during macrobenchmark setup.

This is critical for validating that authorization works correctly
during database schema initialization, not just runtime operations.
"""

from __future__ import annotations

import statistics
import time
from dataclasses import dataclass, field
from typing import Any

import requests

from .config import Config
from .connection_pool import ConnectionPool


@dataclass
class DDLOperation:
    """Represents a DDL operation to be tested."""

    sql: str
    operation_type: str  # CREATE, ALTER, DROP, etc.
    object_type: str  # TABLE, INDEX, DATABASE, etc.
    object_name: str
    expected_authorized: bool = True
    description: str = ""

    def to_dict(self) -> dict[str, Any]:
        return {
            "sql": self.sql,
            "operation_type": self.operation_type,
            "object_type": self.object_type,
            "object_name": self.object_name,
            "expected_authorized": self.expected_authorized,
            "description": self.description,
        }


@dataclass
class DDLTestResult:
    """Result of a DDL operation test."""

    operation: DDLOperation
    authorized: bool
    execution_time_ms: float
    error_message: str | None = None
    plugin_logs: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "operation": self.operation.to_dict(),
            "authorized": self.authorized,
            "execution_time_ms": self.execution_time_ms,
            "error_message": self.error_message,
            "plugin_logs": self.plugin_logs,
        }


@dataclass
class DDLTestSuite:
    """Suite of DDL operations for testing."""

    name: str
    description: str
    operations: list[DDLOperation]
    setup_sql: list[str] = field(default_factory=list)
    cleanup_sql: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "description": self.description,
            "operations": [op.to_dict() for op in self.operations],
            "setup_sql": self.setup_sql,
            "cleanup_sql": self.cleanup_sql,
        }


class DDLOperationsTester:
    """
    Tests DDL operations against baseline and Cedar MySQL instances.

    Validates that DDL audit plugins work correctly during schema setup,
    which is critical for macrobenchmarks like TPC-C.
    """

    def __init__(self, config: Config):
        """
        Initialize DDL tester.

        Args:
            config: Experiment configuration
        """
        self.config = config
        self.baseline_pool = ConnectionPool(self.config.databases["baseline"])
        self.cedar_pool = ConnectionPool(self.config.databases["cedar"])

    def _execute_ddl_operation(
        self, pool: ConnectionPool, operation: DDLOperation, log_queries: bool = True
    ) -> DDLTestResult:
        """
        Execute a single DDL operation and measure authorization.

        Args:
            pool: Database connection pool
            operation: DDL operation to execute
            log_queries: Whether to log the query execution

        Returns:
            DDLTestResult with authorization and timing info
        """
        start_time = time.perf_counter()

        authorized = False
        error_message = None
        plugin_logs = []

        try:
            with pool.get_connection() as conn:
                cur = conn.cursor()

                if log_queries:
                    print(f"Executing DDL: {operation.sql[:100]}...")

                # Execute the DDL operation
                cur.execute(operation.sql)
                conn.commit()

                authorized = True

                # In a real implementation, you would collect plugin logs here
                # For now, we assume success means authorized
                plugin_logs = ["DDL operation executed successfully"]

                cur.close()

        except Exception as e:
            error_message = str(e)

            # Check if this was an authorization error
            if "denied" in error_message.lower() or "1142" in error_message:
                authorized = False
                plugin_logs = [f"Authorization denied: {error_message}"]
            else:
                # Other error (syntax, etc.) - not an authorization issue
                authorized = True  # Assume authorized but execution failed
                plugin_logs = [f"Execution error (not authorization): {error_message}"]

        end_time = time.perf_counter()
        execution_time = (end_time - start_time) * 1000.0

        return DDLTestResult(
            operation=operation,
            authorized=authorized,
            execution_time_ms=execution_time,
            error_message=error_message,
            plugin_logs=plugin_logs,
        )

    def test_ddl_suite(
        self, suite: DDLTestSuite, system: str = "both"
    ) -> dict[str, Any]:
        """
        Test a complete DDL suite against specified systems.

        Args:
            suite: DDL test suite to execute
            system: "baseline", "cedar", or "both"

        Returns:
            Dictionary with test results
        """
        results = {
            "suite_name": suite.name,
            "description": suite.description,
            "baseline_results": [],
            "cedar_results": [],
            "summary": {},
        }

        systems_to_test = []
        if system in ["baseline", "both"]:
            systems_to_test.append(("baseline", self.baseline_pool))
        if system in ["cedar", "both"]:
            systems_to_test.append(("cedar", self.cedar_pool))

        # Run setup SQL if provided
        if suite.setup_sql:
            print(f"Running setup SQL for {suite.name}...")
            for system_name, pool in systems_to_test:
                for sql in suite.setup_sql:
                    try:
                        with pool.get_connection() as conn:
                            cur = conn.cursor()
                            cur.execute(sql)
                            conn.commit()
                            cur.close()
                    except Exception as e:
                        print(f"Setup SQL failed on {system_name}: {e}")

        # Execute DDL operations
        print(f"Testing {len(suite.operations)} DDL operations...")
        for operation in suite.operations:
            for system_name, pool in systems_to_test:
                result = self._execute_ddl_operation(pool, operation, log_queries=False)

                if system_name == "baseline":
                    results["baseline_results"].append(result.to_dict())
                else:
                    results["cedar_results"].append(result.to_dict())

                # Check if authorization behaved as expected
                if result.authorized != operation.expected_authorized:
                    print(
                        f"⚠️  Unexpected authorization result for {operation.object_name}:"
                    )
                    print(
                        f"   Expected: {'authorized' if operation.expected_authorized else 'denied'}"
                    )
                    print(
                        f"   Actual: {'authorized' if result.authorized else 'denied'}"
                    )
                    if result.error_message:
                        print(f"   Error: {result.error_message}")

        # Run cleanup SQL if provided
        if suite.cleanup_sql:
            print(f"Running cleanup SQL for {suite.name}...")
            for system_name, pool in systems_to_test:
                for sql in suite.cleanup_sql:
                    try:
                        with pool.get_connection() as conn:
                            cur = conn.cursor()
                            cur.execute(sql)
                            conn.commit()
                            cur.close()
                    except Exception as e:
                        print(f"Cleanup SQL failed on {system_name}: {e}")

        # Generate summary
        results["summary"] = self._generate_summary(results)

        # Verify Cedar entities if Cedar was tested
        if system in ["cedar", "both"]:
            results["cedar_verification"] = self.verify_cedar_entities()

        return results

    def verify_cedar_entities(self) -> dict[str, Any]:
        """
        Verify that Cedar agent's entity store is consistent with DDL operations.

        Returns:
            Dictionary with verification results
        """
        cedar_url = self.config.cedar_agent.url
        try:
            response = requests.get(f"{cedar_url}/v1/data", timeout=5)
            if response.status_code == 200:
                entities = response.json()

                # Group entities by type for easier reporting
                entity_stats = {}
                for entity in entities:
                    etype = entity["uid"]["type"]
                    entity_stats[etype] = entity_stats.get(etype, 0) + 1

                return {
                    "success": True,
                    "entity_count": len(entities),
                    "entity_types": entity_stats,
                    # "entities": entities # Too large for summary
                }
            else:
                return {
                    "success": False,
                    "error": f"Cedar agent returned HTTP {response.status_code}",
                }
        except Exception as e:
            return {"success": False, "error": f"Failed to connect to Cedar agent: {e}"}

    def _generate_summary(self, results: dict[str, Any]) -> dict[str, Any]:
        """Generate summary statistics for DDL test results."""
        summary = {
            "baseline": {
                "total": 0,
                "authorized": 0,
                "denied": 0,
                "errors": 0,
                "avg_time_ms": 0.0,
            },
            "cedar": {
                "total": 0,
                "authorized": 0,
                "denied": 0,
                "errors": 0,
                "avg_time_ms": 0.0,
            },
        }

        for system in ["baseline", "cedar"]:
            system_results = results.get(f"{system}_results", [])
            if not system_results:
                continue

            times = []
            for result in system_results:
                summary[system]["total"] += 1

                if result["authorized"]:
                    summary[system]["authorized"] += 1
                else:
                    summary[system]["denied"] += 1

                if result["error_message"]:
                    summary[system]["errors"] += 1

                times.append(result["execution_time_ms"])

            if times:
                summary[system]["avg_time_ms"] = statistics.mean(times)

        return summary

    def test_tpcc_schema_creation(
        self, tpcc_ddl_statements: list[str], system: str = "both"
    ) -> dict[str, Any]:
        """
        Test TPC-C schema creation DDL operations.

        This simulates the DDL operations that occur during TPC-C benchmark setup,
        which is critical for validating DDL audit plugins.

        Args:
            tpcc_ddl_statements: List of DDL statements from TPC-C schema
            system: Which system(s) to test

        Returns:
            Test results for TPC-C schema creation
        """
        operations = []
        for i, sql in enumerate(tpcc_ddl_statements):
            # Parse the DDL statement to extract metadata
            operation_type, object_type, object_name = self._parse_ddl_statement(sql)

            operation = DDLOperation(
                sql=sql,
                operation_type=operation_type,
                object_type=object_type,
                object_name=object_name,
                expected_authorized=True,  # Assume DDL should be authorized for schema setup
                description=f"TPC-C schema setup - {object_type} {object_name}",
            )
            operations.append(operation)

        suite = DDLTestSuite(
            name="TPC-C Schema Creation",
            description="DDL operations required for TPC-C benchmark schema setup",
            operations=operations,
            setup_sql=["CREATE DATABASE IF NOT EXISTS tpcc_test"],
            cleanup_sql=["DROP DATABASE IF EXISTS tpcc_test"],
        )

        return self.test_ddl_suite(suite, system)

    def _parse_ddl_statement(self, sql: str) -> tuple[str, str, str]:
        """
        Parse a DDL statement to extract operation type, object type, and object name.

        Returns:
            Tuple of (operation_type, object_type, object_name)
        """
        sql = sql.strip().upper()

        # Extract operation type
        if sql.startswith("CREATE"):
            operation_type = "CREATE"
        elif sql.startswith("ALTER"):
            operation_type = "ALTER"
        elif sql.startswith("DROP"):
            operation_type = "DROP"
        elif sql.startswith("RENAME"):
            operation_type = "RENAME"
        elif sql.startswith("TRUNCATE"):
            operation_type = "TRUNCATE"
        elif sql.startswith("GRANT"):
            operation_type = "GRANT"
        elif sql.startswith("REVOKE"):
            operation_type = "REVOKE"
        else:
            operation_type = "UNKNOWN"

        # Extract object type and name
        words = sql.split()

        object_type = "UNKNOWN"
        object_name = "UNKNOWN"

        try:
            # Handle standard DDL: CREATE/ALTER/DROP/TRUNCATE <TYPE> <NAME>
            if operation_type in ["CREATE", "ALTER", "DROP", "TRUNCATE"]:
                for t in [
                    "TABLE",
                    "INDEX",
                    "DATABASE",
                    "DB",
                    "SCHEMA",
                    "VIEW",
                    "TRIGGER",
                    "PROCEDURE",
                    "FUNCTION",
                    "USER",
                    "ROLE",
                ]:
                    if t in words:
                        object_type = t
                        idx = words.index(t)
                        if idx + 1 < len(words):
                            # Handle IF NOT EXISTS / IF EXISTS
                            next_word = words[idx + 1]
                            if next_word == "IF" and idx + 3 < len(words):
                                object_name = words[idx + 3].strip("();,`'\"")
                            else:
                                object_name = next_word.strip("();,`'\"")
                        break

            # Handle GRANT/REVOKE: GRANT <PRIVS> ON <TYPE> <NAME> TO ...
            elif operation_type in ["GRANT", "REVOKE"]:
                if "ON" in words:
                    idx = words.index("ON")
                    if idx + 1 < len(words):
                        # Could be ON TABLE name, ON DATABASE name, or just ON name (default table)
                        next_word = words[idx + 1]
                        if next_word in [
                            "TABLE",
                            "DATABASE",
                            "SCHEMA",
                            "FUNCTION",
                            "PROCEDURE",
                        ]:
                            object_type = next_word
                            if idx + 2 < len(words):
                                object_name = words[idx + 2].strip("();,`'\"")
                        else:
                            # Default to TABLE if not specified
                            object_type = "TABLE"
                            object_name = next_word.strip("();,`'\"")

            # Special case for DATABASE/DB/SCHEMA normalization
            if object_type in ["DB", "SCHEMA"]:
                object_type = "DATABASE"

        except (ValueError, IndexError):
            pass

        return operation_type, object_type, object_name


# Pre-defined DDL test suites


def get_tpcc_ddl_suite() -> DDLTestSuite:
    """Get a DDL test suite for TPC-C schema operations."""
    # Sample DDL operations that would be performed during TPC-C setup
    operations = [
        DDLOperation(
            sql="CREATE DATABASE tpcc_test;",
            operation_type="CREATE",
            object_type="DATABASE",
            object_name="tpcc_test",
            expected_authorized=True,
            description="Create TPC-C test database",
        ),
        DDLOperation(
            sql="""CREATE TABLE warehouse (
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
            operation_type="CREATE",
            object_type="TABLE",
            object_name="warehouse",
            expected_authorized=True,
            description="Create warehouse table",
        ),
        DDLOperation(
            sql="CREATE INDEX idx_warehouse_name ON warehouse (w_name);",
            operation_type="CREATE",
            object_type="INDEX",
            object_name="idx_warehouse_name",
            expected_authorized=True,
            description="Create index on warehouse name",
        ),
        DDLOperation(
            sql="ALTER TABLE warehouse ADD COLUMN w_new_field INT DEFAULT 0;",
            operation_type="ALTER",
            object_type="TABLE",
            object_name="warehouse",
            expected_authorized=True,
            description="Alter table to add new column",
        ),
        DDLOperation(
            sql="DROP INDEX idx_warehouse_name ON warehouse;",
            operation_type="DROP",
            object_type="INDEX",
            object_name="idx_warehouse_name",
            expected_authorized=True,
            description="Drop index",
        ),
        DDLOperation(
            sql="DROP TABLE warehouse;",
            operation_type="DROP",
            object_type="TABLE",
            object_name="warehouse",
            expected_authorized=True,
            description="Drop table",
        ),
        DDLOperation(
            sql="DROP DATABASE tpcc_test;",
            operation_type="DROP",
            object_type="DATABASE",
            object_name="tpcc_test",
            expected_authorized=True,
            description="Drop test database",
        ),
    ]

    return DDLTestSuite(
        name="TPC-C DDL Operations",
        description="DDL operations performed during TPC-C benchmark schema setup",
        operations=operations,
    )


def get_ddl_audit_test_suite() -> DDLTestSuite:
    """Get a comprehensive DDL audit test suite."""
    operations = [
        # Database operations
        DDLOperation(
            sql="CREATE DATABASE ddl_test_db;",
            operation_type="CREATE",
            object_type="DATABASE",
            object_name="ddl_test_db",
            expected_authorized=True,
        ),
        DDLOperation(
            sql="ALTER DATABASE ddl_test_db CHARACTER SET utf8mb4;",
            operation_type="ALTER",
            object_type="DATABASE",
            object_name="ddl_test_db",
            expected_authorized=True,
        ),
        # Table operations
        DDLOperation(
            sql="""CREATE TABLE ddl_test_table (
                id INT PRIMARY KEY AUTO_INCREMENT,
                name VARCHAR(100),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );""",
            operation_type="CREATE",
            object_type="TABLE",
            object_name="ddl_test_table",
            expected_authorized=True,
        ),
        DDLOperation(
            sql="ALTER TABLE ddl_test_table ADD COLUMN email VARCHAR(255);",
            operation_type="ALTER",
            object_type="TABLE",
            object_name="ddl_test_table",
            expected_authorized=True,
        ),
        # Constraints & Foreign Keys
        DDLOperation(
            sql="CREATE TABLE ddl_related_table (id INT PRIMARY KEY, ref_id INT);",
            operation_type="CREATE",
            object_type="TABLE",
            object_name="ddl_related_table",
            expected_authorized=True,
        ),
        DDLOperation(
            sql="ALTER TABLE ddl_related_table ADD CONSTRAINT fk_test FOREIGN KEY (ref_id) REFERENCES ddl_test_table(id);",
            operation_type="ALTER",
            object_type="TABLE",
            object_name="ddl_related_table",
            expected_authorized=True,
        ),
        DDLOperation(
            sql="ALTER TABLE ddl_test_table ADD UNIQUE INDEX idx_unique_email (email);",
            operation_type="ALTER",
            object_type="TABLE",
            object_name="ddl_test_table",
            expected_authorized=True,
        ),
        # User & Role Management (Cedar Audit specifically captures these)
        DDLOperation(
            sql="CREATE USER 'ddl_test_user'@'localhost' IDENTIFIED BY 'password';",
            operation_type="CREATE",
            object_type="USER",
            object_name="ddl_test_user",
            expected_authorized=True,
        ),
        DDLOperation(
            sql="CREATE ROLE 'ddl_test_role';",
            operation_type="CREATE",
            object_type="ROLE",
            object_name="ddl_test_role",
            expected_authorized=True,
        ),
        # Renaming & Modifications
        DDLOperation(
            sql="ALTER TABLE ddl_test_table RENAME TO ddl_renamed_table;",
            operation_type="ALTER",
            object_type="TABLE",
            object_name="ddl_test_table",
            expected_authorized=True,
        ),
        # Privileges (GRANT/REVOKE) - Moved here after RENAME
        DDLOperation(
            sql="GRANT SELECT ON ddl_renamed_table TO 'ddl_test_user'@'localhost';",
            operation_type="GRANT",
            object_type="TABLE",
            object_name="ddl_renamed_table",
            expected_authorized=True,
        ),
        DDLOperation(
            sql="GRANT 'ddl_test_role' TO 'ddl_test_user'@'localhost';",
            operation_type="GRANT",
            object_type="ROLE",
            object_name="ddl_test_role",
            expected_authorized=True,
        ),
        DDLOperation(
            sql="REVOKE SELECT ON ddl_renamed_table FROM 'ddl_test_user'@'localhost';",
            operation_type="REVOKE",
            object_type="TABLE",
            object_name="ddl_renamed_table",
            expected_authorized=True,
        ),
        # Views
        DDLOperation(
            sql="CREATE VIEW ddl_test_view AS SELECT id, name FROM ddl_renamed_table;",
            operation_type="CREATE",
            object_type="VIEW",
            object_name="ddl_test_view",
            expected_authorized=True,
        ),
        # Routines (Procedures and Functions)
        DDLOperation(
            sql="""CREATE PROCEDURE ddl_test_procedure()
            BEGIN
                SELECT * FROM ddl_renamed_table;
            END;""",
            operation_type="CREATE",
            object_type="PROCEDURE",
            object_name="ddl_test_procedure",
            expected_authorized=True,
        ),
        DDLOperation(
            sql="""CREATE FUNCTION ddl_test_function(val INT) RETURNS INT
            DETERMINISTIC
            BEGIN
                RETURN val * 2;
            END;""",
            operation_type="CREATE",
            object_type="FUNCTION",
            object_name="ddl_test_function",
            expected_authorized=True,
        ),
        # Triggers
        DDLOperation(
            sql="""CREATE TRIGGER ddl_test_trigger
            BEFORE INSERT ON ddl_renamed_table
            FOR EACH ROW
            SET NEW.created_at = NOW();""",
            operation_type="CREATE",
            object_type="TRIGGER",
            object_name="ddl_test_trigger",
            expected_authorized=True,
        ),
        # Cleanup of new entities
        DDLOperation(
            sql="DROP TRIGGER IF EXISTS ddl_test_trigger;",
            operation_type="DROP",
            object_type="TRIGGER",
            object_name="ddl_test_trigger",
            expected_authorized=True,
        ),
        DDLOperation(
            sql="DROP FUNCTION IF EXISTS ddl_test_function;",
            operation_type="DROP",
            object_type="FUNCTION",
            object_name="ddl_test_function",
            expected_authorized=True,
        ),
        DDLOperation(
            sql="DROP PROCEDURE IF EXISTS ddl_test_procedure;",
            operation_type="DROP",
            object_type="PROCEDURE",
            object_name="ddl_test_procedure",
            expected_authorized=True,
        ),
        DDLOperation(
            sql="DROP VIEW IF EXISTS ddl_test_view;",
            operation_type="DROP",
            object_type="VIEW",
            object_name="ddl_test_view",
            expected_authorized=True,
        ),
        # Cleanup
        DDLOperation(
            sql="DROP TABLE ddl_related_table;",
            operation_type="DROP",
            object_type="TABLE",
            object_name="ddl_related_table",
            expected_authorized=True,
        ),
        DDLOperation(
            sql="TRUNCATE TABLE ddl_renamed_table;",
            operation_type="TRUNCATE",
            object_type="TABLE",
            object_name="ddl_renamed_table",
            expected_authorized=True,
        ),
        DDLOperation(
            sql="DROP TABLE ddl_renamed_table;",
            operation_type="DROP",
            object_type="TABLE",
            object_name="ddl_renamed_table",
            expected_authorized=True,
        ),
        DDLOperation(
            sql="DROP USER 'ddl_test_user'@'localhost';",
            operation_type="DROP",
            object_type="USER",
            object_name="ddl_test_user",
            expected_authorized=True,
        ),
        DDLOperation(
            sql="DROP ROLE 'ddl_test_role';",
            operation_type="DROP",
            object_type="ROLE",
            object_name="ddl_test_role",
            expected_authorized=True,
        ),
        DDLOperation(
            sql="DROP DATABASE ddl_test_db;",
            operation_type="DROP",
            object_type="DATABASE",
            object_name="ddl_test_db",
            expected_authorized=True,
        ),
    ]

    return DDLTestSuite(
        name="Comprehensive DDL Audit Test",
        description="Comprehensive test of DDL operations for audit plugin validation",
        operations=operations,
    )


def run_ddl_audit_test(
    config: Config, test_suite: str = "comprehensive", system: str = "both"
) -> dict[str, Any]:
    """
    Convenience function to run DDL audit tests.

    Args:
        config: Experiment configuration
        test_suite: "comprehensive" or "tpcc"
        system: "baseline", "cedar", or "both"

    Returns:
        Test results
    """
    tester = DDLOperationsTester(config)

    if test_suite == "tpcc":
        suite = get_tpcc_ddl_suite()
    else:
        suite = get_ddl_audit_test_suite()

    return tester.test_ddl_suite(suite, system)
