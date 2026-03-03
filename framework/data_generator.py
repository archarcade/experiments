#!/usr/bin/env python3
"""
Data Generator Module for Benchmarking Framework

Generates fake test data using Faker library for repeatable, conflict-free experiments.
Supports dynamic ID generation, bulk data generation, and various test scenarios.

Now schema-aware: generates data based on table schemas from auth spec.
"""

import random
import re
import time
from typing import Any

from faker import Faker


class DataGenerator:
    """Generates test data for database tables using Faker.

    Now supports schema-aware generation based on column definitions.
    """

    def __init__(self, seed: int | None = None):
        """
        Initialize the data generator.

        Args:
            seed: Random seed for reproducibility. If None, uses current time.
        """
        self.seed = seed if seed is not None else int(time.time())
        self.fake = Faker()
        Faker.seed(self.seed)
        random.seed(self.seed)

        # ID tracking for unique ID generation
        self._id_counters: dict[str, int] = {}
        self._used_ids: dict[str, set] = {}

    def reset(self, seed: int | None = None):
        """Reset the generator with a new seed."""
        if seed is not None:
            self.seed = seed
        else:
            self.seed = int(time.time())
        Faker.seed(self.seed)
        random.seed(self.seed)
        self._id_counters.clear()
        self._used_ids.clear()

    def generate_unique_id(
        self, table: str, start_from: int = 10000, max_id: int = 999999999
    ) -> int:
        """
        Generate a unique ID for a table.

        Args:
            table: Table name (e.g., 'employees')
            start_from: Starting ID value
            max_id: Maximum ID value

        Returns:
            A unique integer ID
        """
        if table not in self._id_counters:
            self._id_counters[table] = start_from
            self._used_ids[table] = set()

        # Find next available ID
        while self._id_counters[table] in self._used_ids[table]:
            self._id_counters[table] += 1

        if self._id_counters[table] > max_id:
            # Reset if we've exhausted the range
            self._id_counters[table] = start_from
            self._used_ids[table].clear()

        id_value = self._id_counters[table]
        self._used_ids[table].add(id_value)
        self._id_counters[table] += 1

        return id_value

    def _infer_data_type_from_column(self, col_name: str, col_type: str) -> str:
        """
        Infer what kind of data to generate based on column name and type.

        Returns a hint like 'id', 'name', 'department', 'text', 'number', etc.
        """
        col_name_lower = col_name.lower()

        # ID columns
        if col_name_lower == "id" or col_name_lower.endswith("_id"):
            return "id"

        # Name columns
        if "name" in col_name_lower:
            if "project" in col_name_lower or "product" in col_name_lower:
                return "project_name"
            return "name"

        # Department/Organization columns
        if "department" in col_name_lower or "dept" in col_name_lower:
            return "department"

        # Classification/Security columns
        if "classification" in col_name_lower or "security" in col_name_lower:
            return "classification"

        # Text/Info columns
        if (
            "info" in col_name_lower
            or "description" in col_name_lower
            or "text" in col_name_lower
        ):
            return "text"

        # Email columns
        if "email" in col_name_lower or "mail" in col_name_lower:
            return "email"

        # Date columns
        if "date" in col_name_lower or "time" in col_name_lower:
            return "date"

        # URL columns
        if "url" in col_name_lower or "link" in col_name_lower:
            return "url"

        # Default based on SQL type
        col_type_upper = col_type.upper()
        if "INT" in col_type_upper:
            return "number"
        elif "VARCHAR" in col_type_upper or "CHAR" in col_type_upper:
            return "text"
        elif "TEXT" in col_type_upper:
            return "text"
        elif "DATE" in col_type_upper or "TIME" in col_type_upper:
            return "date"
        elif "BOOL" in col_type_upper:
            return "boolean"

        return "text"  # Default fallback

    def _generate_value_for_column(
        self,
        col_name: str,
        col_type: str,
        col_constraints: str,
        table_name: str,
        id_value: int | None = None,
    ) -> Any:  # noqa: E501
        """
        Generate a value for a specific column based on its definition.

        Args:
            col_name: Column name
            col_type: SQL column type (e.g., 'INT', 'VARCHAR(100)')
            col_constraints: Column constraints (e.g., 'PRIMARY KEY')
            table_name: Table name (for ID generation)
            id_value: Optional ID value (for PRIMARY KEY columns)

        Returns:
            Generated value appropriate for the column
        """
        data_hint = self._infer_data_type_from_column(col_name, col_type)
        is_primary_key = "PRIMARY KEY" in col_constraints.upper()

        # Handle ID columns
        if is_primary_key or data_hint == "id":
            if id_value is not None:
                return id_value
            return self.generate_unique_id(table_name, start_from=1)

        # Generate based on data hint
        if data_hint == "name":
            return self.fake.name()
        elif data_hint == "project_name":
            return f"{self.fake.company()} {self.fake.word().title()} Project"
        elif data_hint == "department":
            return self.fake.bs().title()
        elif data_hint == "classification":
            return self.fake.word().title()
        elif data_hint == "text":
            # Extract max length from VARCHAR(n) or use default
            max_length = 200
            match = re.search(r"VARCHAR\((\d+)\)", col_type.upper())
            if match:
                max_length = min(int(match.group(1)), 500)
            elif "TEXT" in col_type.upper():
                max_length = 500
            return self.fake.text(max_nb_chars=max_length)
        elif data_hint == "email":
            return self.fake.email()
        elif data_hint == "date":
            return self.fake.date()
        elif data_hint == "url":
            return self.fake.url()
        elif data_hint == "boolean":
            return random.choice([True, False])
        elif data_hint == "number":
            # Generate integer based on type constraints
            if "TINYINT" in col_type.upper():
                return random.randint(0, 255)
            elif "SMALLINT" in col_type.upper():
                return random.randint(-32768, 32767)
            elif "BIGINT" in col_type.upper():
                return random.randint(1000000, 9999999)
            else:
                return random.randint(1, 10000)
        else:
            # Default: generate text
            return self.fake.word()

    def generate_record_from_schema(
        self, table_name: str, schema: dict[str, Any], id_value: int | None = None
    ) -> dict[str, Any]:
        """
        Generate a record based on table schema definition.

        Args:
            table_name: Full table name (e.g., 'abac_test.employees')
            schema: Schema definition with 'columns' list
            id_value: Optional ID value for PRIMARY KEY column

        Returns:
            Dictionary with column names as keys and generated values
        """
        table_name_short = (
            table_name.split(".")[-1] if "." in table_name else table_name
        )
        record = {}

        # Find PRIMARY KEY column first
        pk_column = None
        for col in schema.get("columns", []):
            constraints = col.get("constraints", "")
            if "PRIMARY KEY" in constraints.upper():
                pk_column = col["name"]
                break

        # Generate values for all columns
        for col in schema.get("columns", []):
            col_name = col["name"]
            col_type = col.get("type", "VARCHAR(100)")
            col_constraints = col.get("constraints", "")

            # Use provided ID for PK, or generate one
            if col_name == pk_column and id_value is not None:
                record[col_name] = id_value
            else:
                record[col_name] = self._generate_value_for_column(
                    col_name, col_type, col_constraints, table_name_short, id_value
                )

        return record

    def to_sql_insert(self, table: str, records: list[dict[str, Any]]) -> str:
        """
        Convert records to SQL INSERT statement.

        Args:
            table: Table name (e.g., 'abac_test.employees')
            records: List of record dictionaries

        Returns:
            SQL INSERT statement string
        """
        if not records:
            return ""

        # Get column names from first record
        columns = list(records[0].keys())
        columns_str = ", ".join(columns)

        # Build VALUES clause
        values_list = []
        for record in records:
            values = []
            for col in columns:
                value = record[col]
                if isinstance(value, str):
                    # Escape single quotes
                    value = value.replace("'", "''")
                    values.append(f"'{value}'")
                elif value is None:
                    values.append("NULL")
                elif isinstance(value, bool):
                    values.append("1" if value else "0")
                else:
                    values.append(str(value))
            values_list.append(f"({', '.join(values)})")

        values_str = ",\n    ".join(values_list)

        return f"INSERT INTO {table} ({columns_str}) VALUES\n    {values_str};"

    def get_seed(self) -> int:
        """Get the current seed value."""
        return self.seed


# Singleton instance for easy access
_generator_instance: DataGenerator | None = None


def get_generator(seed: int | None = None) -> DataGenerator:
    """
    Get or create a singleton DataGenerator instance.

    Args:
        seed: Optional seed for reproducibility

    Returns:
        DataGenerator instance
    """
    global _generator_instance
    if _generator_instance is None or seed is not None:
        _generator_instance = DataGenerator(seed)
    return _generator_instance


if __name__ == "__main__":
    # Example usage
    gen = DataGenerator(seed=42)

    print("=== Generating Test Data ===\n")

    # Schema-based generation
    schema = {
        "columns": [
            {"name": "id", "type": "INT", "constraints": "PRIMARY KEY"},
            {"name": "name", "type": "VARCHAR(100)", "constraints": ""},
            {"name": "department", "type": "VARCHAR(50)", "constraints": ""},
        ]
    }
    print("Schema-based Employee:")
    schema_emp = gen.generate_record_from_schema("test.employees", schema)
    print(f"  {schema_emp}\n")
