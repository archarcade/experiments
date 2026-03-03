#!/usr/bin/env python3
"""
Test Data Manager Module for Benchmarking Framework

Manages test data lifecycle: setup, cleanup, and tracking for repeatable experiments.
"""

import json
import os
import sys

import mysql.connector

# Support both relative and absolute imports
try:
    from .data_generator import get_generator
except ImportError:
    # Fallback for direct script execution
    sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
    from data_generator import get_generator


class TestDataManager:
    """Manages test data for benchmarking experiments."""

    def __init__(self, mysql_config: dict[str, any], seed: int | None = None):
        """
        Initialize the test data manager.

        Args:
            mysql_config: MySQL connection configuration dict
            seed: Random seed for reproducibility
        """
        self.mysql_config = mysql_config
        self.data_gen = get_generator(seed)
        self.seed = seed if seed is not None else self.data_gen.get_seed()
        self._connection = None
        self._test_ids_tracked: dict[str, list[int]] = {}

    def connect(self):
        """Establish database connection."""
        if self._connection is None or not self._connection.is_connected():
            self._connection = mysql.connector.connect(**self.mysql_config)
        return self._connection

    def disconnect(self):
        """Close database connection."""
        if self._connection and self._connection.is_connected():
            self._connection.close()
            self._connection = None

    def setup_test_data(
        self, table: str, count: int, start_id: int = 10000
    ) -> list[int]:
        """
        Setup test data in the database.

        Args:
            table: Table name (e.g., 'abac_test.employees')
            count: Number of records to insert
            start_id: Starting ID value

        Returns:
            List of inserted IDs
        """
        conn = self.connect()
        cursor = conn.cursor()

        table_name = table.split(".")[-1]
        records = self.data_gen.generate_bulk_data(table_name, count, start_id)
        inserted_ids = [r["id"] for r in records]

        # Track inserted IDs for cleanup
        if table not in self._test_ids_tracked:
            self._test_ids_tracked[table] = []
        self._test_ids_tracked[table].extend(inserted_ids)

        # Generate and execute INSERT statement
        insert_sql = self.data_gen.to_sql_insert(table, records)
        if insert_sql:
            cursor.execute(insert_sql)
            conn.commit()

        cursor.close()
        return inserted_ids

    def cleanup_test_data(self, table: str, ids: list[int] | None = None):
        """
        Cleanup test data from the database.

        Args:
            table: Table name (e.g., 'abac_test.employees')
            ids: Optional list of IDs to delete. If None, deletes all tracked IDs.
        """
        conn = self.connect()
        cursor = conn.cursor()

        if ids is None:
            ids = self._test_ids_tracked.get(table, [])

        if ids:
            ids_str = ", ".join(map(str, ids))
            delete_sql = f"DELETE FROM {table} WHERE id IN ({ids_str});"
            cursor.execute(delete_sql)
            conn.commit()

            # Remove from tracking
            if table in self._test_ids_tracked:
                self._test_ids_tracked[table] = [
                    tid for tid in self._test_ids_tracked[table] if tid not in ids
                ]

        cursor.close()

    def cleanup_all_test_data(self):
        """Cleanup all tracked test data."""
        for table in list(self._test_ids_tracked.keys()):
            self.cleanup_test_data(table)
        self._test_ids_tracked.clear()

    def ensure_test_record_exists(self, table: str, test_id: int) -> bool:
        """
        Ensure a test record exists in the database.

        Args:
            table: Table name
            test_id: Test record ID

        Returns:
            True if record exists or was created, False otherwise
        """
        conn = self.connect()
        cursor = conn.cursor()

        table_name = table.split(".")[-1]

        # Check if record exists
        cursor.execute(f"SELECT COUNT(*) FROM {table} WHERE id = %s", (test_id,))
        exists = cursor.fetchone()[0] > 0

        if not exists:
            # Create the record
            if table_name == "employees":
                record = self.data_gen.generate_employee(test_id)
                cursor.execute(
                    f"INSERT INTO {table} (id, name, department) VALUES (%s, %s, %s)",
                    (record["id"], record["name"], record["department"]),
                )
            elif table_name == "projects":
                record = self.data_gen.generate_project(test_id)
                cursor.execute(
                    f"INSERT INTO {table} (id, name, classification) VALUES (%s, %s, %s)",
                    (record["id"], record["name"], record["classification"]),
                )
            elif table_name == "sensitive_data":
                record = self.data_gen.generate_sensitive_data(test_id)
                cursor.execute(
                    f"INSERT INTO {table} (id, info) VALUES (%s, %s)",
                    (record["id"], record["info"]),
                )
            else:
                cursor.close()
                return False

            conn.commit()
            exists = True

        cursor.close()
        return exists

    def get_table_row_count(self, table: str) -> int:
        """
        Get the number of rows in a table.

        Args:
            table: Table name

        Returns:
            Row count
        """
        conn = self.connect()
        cursor = conn.cursor()
        cursor.execute(f"SELECT COUNT(*) FROM {table}")
        count = cursor.fetchone()[0]
        cursor.close()
        return count

    def reset(self, seed: int | None = None):
        """Reset the manager with a new seed."""
        if seed is not None:
            self.seed = seed
        self.data_gen.reset(self.seed)
        self._test_ids_tracked.clear()

    def save_state(self, filepath: str):
        """Save current state to a file."""
        state = {"seed": self.seed, "test_ids": self._test_ids_tracked}
        with open(filepath, "w") as f:
            json.dump(state, f, indent=2)

    def load_state(self, filepath: str):
        """Load state from a file."""
        if os.path.exists(filepath):
            with open(filepath) as f:
                state = json.load(f)
            self.seed = state.get("seed", self.seed)
            self.data_gen.reset(self.seed)
            self._test_ids_tracked = state.get("test_ids", {})

    def __enter__(self):
        """Context manager entry."""
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.disconnect()


if __name__ == "__main__":
    # Example usage
    config = {
        "host": "localhost",
        "port": 3306,
        "user": "root",
        "password": "",
        "database": "abac_test",
    }

    with TestDataManager(config, seed=42) as manager:
        print("=== Test Data Manager Example ===\n")

        # Setup test data
        print("Setting up test data...")
        ids = manager.setup_test_data("abac_test.employees", 10)
        print(f"Inserted {len(ids)} records with IDs: {ids[:5]}...")

        # Get row count
        count = manager.get_table_row_count("abac_test.employees")
        print(f"Total rows in employees table: {count}")

        # Cleanup
        print("\nCleaning up test data...")
        manager.cleanup_test_data("abac_test.employees", ids[:5])
        print("Cleaned up first 5 records")
