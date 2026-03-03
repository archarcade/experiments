#!/usr/bin/env python3
"""
Query Generator Module for Benchmarking Framework

Generates dynamic SQL queries for performance testing without hardcoded values.
Supports INSERT, UPDATE, DELETE, SELECT operations with unique IDs.

Now schema-aware: generates queries based on table schemas from auth spec.
"""

import os
import sys
from typing import Any

# Support both relative and absolute imports
try:
    from .data_generator import get_generator
except ImportError:
    # Fallback for direct script execution
    sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
    from data_generator import get_generator


class QueryGenerator:
    """Generates dynamic SQL queries for testing.

    Now supports schema-aware generation based on table definitions
    and database-specific syntax (MySQL vs PostgreSQL).
    """

    def __init__(
        self,
        seed: int | None = None,
        auth_spec: dict[str, Any] | None = None,
        db_type: str = "mysql",
    ):
        """
        Initialize the query generator.

        Args:
            seed: Random seed for reproducibility
            auth_spec: Optional authorization spec for schema-aware generation
            db_type: Database type ("mysql" or "postgres")
        """
        self.data_gen = get_generator(seed)
        self._test_ids: dict[str, int] = {}
        self._auth_spec = auth_spec or {}
        self._db_type = db_type.lower()
        self._schema_cache: dict[str, dict[str, Any]] = {}

        # Build schema cache from auth spec
        if auth_spec:
            self._build_schema_cache()

    def _build_schema_cache(self) -> None:
        """Build a cache of table schemas from auth spec."""
        resources = self._auth_spec.get("resources", [])
        for resource in resources:
            if resource.get("type") == "Table" and "schema" in resource:
                table_name = resource["name"]
                self._schema_cache[table_name] = resource["schema"]

    def _get_table_schema(self, table: str) -> dict[str, Any] | None:
        """
        Get schema for a table.

        Args:
            table: Full table name (e.g., 'abac_test.employees')

        Returns:
            Schema dict with 'columns' list, or None if not found
        """
        # Try exact match first
        if table in self._schema_cache:
            return self._schema_cache[table]

        # Try table name only (without database prefix)
        table_name_short = table.split(".")[-1] if "." in table else table
        for cached_table, schema in self._schema_cache.items():
            if cached_table.split(".")[-1] == table_name_short:
                return schema

        return None

    def _get_primary_key_column(self, schema: dict[str, Any]) -> str | None:
        """Get the PRIMARY KEY column name from schema."""
        for col in schema.get("columns", []):
            constraints = col.get("constraints", "")
            if "PRIMARY KEY" in constraints.upper():
                return col["name"]
        return None

    def _get_non_pk_columns(self, schema: dict[str, Any]) -> list[dict[str, Any]]:
        """Get all non-PRIMARY KEY columns from schema."""
        pk_col = self._get_primary_key_column(schema)
        return [col for col in schema.get("columns", []) if col["name"] != pk_col]

    def reset(self, seed: int | None = None):
        """Reset the generator."""
        self.data_gen.reset(seed)
        self._test_ids.clear()

    def _get_test_id(self, table: str, base_id: int = 1) -> int:
        """
        Get or generate a test ID for a table.
        Uses a high base_id range to avoid conflicts with real data.

        Args:
            table: Table name
            base_id: Base ID for test records

        Returns:
            Test ID for the table
        """
        if table not in self._test_ids:
            # Generate unique test ID in high range
            self._test_ids[table] = self.data_gen.generate_unique_id(
                f"test_{table}", start_from=base_id
            )
        return self._test_ids[table]

    def generate_insert_query(
        self,
        table: str,
        schema: dict[str, Any] | None = None,
        record: dict[str, Any] | None = None,
        id_value: int | None = None,
        ignore_duplicate: bool = False,
    ) -> str:
        """
        Generate an INSERT query with dynamic data.

        Args:
            table: Table name (e.g., 'abac_test.employees')
            schema: Optional schema definition (if None, tries to get from cache)
            record: Optional pre-generated record (if None, generates one)
            id_value: Optional ID value to use (if None, generates one)
            ignore_duplicate: If True, uses INSERT IGNORE to handle duplicate keys

        Returns:
            SQL query string
        """
        # Get schema
        if schema is None:
            schema = self._get_table_schema(table)

        table_name = table.split(".")[-1]  # Extract table name without schema
        test_id = id_value if id_value is not None else self._get_test_id(table_name)

        # Generate record - schema is required
        if schema is None:
            raise ValueError(
                f"Schema required for table {table}. "
                "Provide schema or ensure table is in auth_spec."
            )

        if record is None:
            record = self.data_gen.generate_record_from_schema(
                table, schema, id_value=test_id
            )

        # Build INSERT statement
        columns = list(record.keys())
        columns_str = ", ".join(columns)

        values = []
        for col in columns:
            value = record[col]
            if isinstance(value, str):
                value_escaped = value.replace("'", "''")
                values.append(f"'{value_escaped}'")
            elif value is None:
                values.append("NULL")
            elif isinstance(value, bool):
                if self._db_type == "postgres":
                    values.append("true" if value else "false")
                else:
                    values.append("1" if value else "0")
            else:
                values.append(str(value))

        values_str = ", ".join(values)

        if self._db_type == "postgres":
            if ignore_duplicate:
                pk_col = self._get_primary_key_column(schema) or "id"
                query = f"INSERT INTO {table} ({columns_str}) VALUES ({values_str}) ON CONFLICT ({pk_col}) DO NOTHING;"
            else:
                query = f"INSERT INTO {table} ({columns_str}) VALUES ({values_str});"
        else:
            insert_keyword = "INSERT IGNORE" if ignore_duplicate else "INSERT"
            query = (
                f"{insert_keyword} INTO {table} ({columns_str}) VALUES ({values_str});"
            )

        return query

    def generate_update_query(
        self,
        table: str,
        schema: dict[str, Any] | None = None,
        id_value: int | None = None,
    ) -> str:
        """
        Generate an UPDATE query with dynamic data.

        Args:
            table: Table name (e.g., 'abac_test.employees')
            ensure_exists: If True, ensures the record exists before updating
            schema: Optional schema definition (if None, tries to get from cache)

        Returns:
            SQL query string
        """
        # Get schema
        if schema is None:
            schema = self._get_table_schema(table)

        table_name = table.split(".")[-1]
        test_id = id_value if id_value is not None else self._get_test_id(table_name)

        # Generate record for INSERT (if needed) and UPDATE
        # Schema is required
        if schema is None:
            raise ValueError(
                f"Schema required for table {table}. "
                "Provide schema or ensure table is in auth_spec."
            )

        record = self.data_gen.generate_record_from_schema(
            table, schema, id_value=test_id
        )
        pk_col = self._get_primary_key_column(schema) or "id"
        non_pk_cols = self._get_non_pk_columns(schema)

        # Build UPDATE statement (update first non-PK column)
        if non_pk_cols:
            update_col = non_pk_cols[0]["name"]
            update_value = record[update_col]

            if isinstance(update_value, str):
                update_value_escaped = update_value.replace("'", "''")
                update_value_str = f"'{update_value_escaped}'"
            elif update_value is None:
                update_value_str = "NULL"
            elif isinstance(update_value, bool):
                if self._db_type == "postgres":
                    update_value_str = "true" if update_value else "false"
                else:
                    update_value_str = "1" if update_value else "0"
            else:
                update_value_str = str(update_value)

            update_query = (
                f"UPDATE {table} SET {update_col} = {update_value_str} "
                f"WHERE {pk_col} = {test_id};"
            )
        else:
            # No non-PK columns, just update a dummy value
            update_query = (
                f"UPDATE {table} SET {pk_col} = {test_id} WHERE {pk_col} = {test_id};"
            )

        return update_query

    def generate_delete_query(
        self,
        table: str,
        schema: dict[str, Any] | None = None,
        id_value: int | None = None,
    ) -> str:
        """
        Generate a DELETE query.

        Args:
            table: Table name (e.g., 'abac_test.employees')
            ensure_exists: If True, ensures the record exists before deleting
            schema: Optional schema definition (if None, tries to get from cache)

        Returns:
            SQL query string
        """
        # Get schema
        if schema is None:
            schema = self._get_table_schema(table)

        table_name = table.split(".")[-1]
        test_id = id_value if id_value is not None else self._get_test_id(table_name)

        # Find PK column
        pk_col = None
        if schema:
            pk_col = self._get_primary_key_column(schema) or "id"
        else:
            pk_col = "id"  # Default assumption

        delete_query = f"DELETE FROM {table} WHERE {pk_col} = {test_id};"
        return delete_query

    def _get_column_names(self, schema: dict[str, Any]) -> list[str]:
        """Get all column names from schema."""
        return [col["name"] for col in schema.get("columns", [])]

    def _find_valid_join_condition(
        self,
        table_schema: dict[str, Any] | None,
        join_schema: dict[str, Any] | None,
    ) -> tuple[str, str] | None:
        """
        Find a valid join condition between two tables.

        Returns:
            Tuple of (left_col, right_col) if valid join found, None otherwise.
        """
        if not table_schema or not join_schema:
            return None

        table_cols = set(self._get_column_names(table_schema))
        join_cols = set(self._get_column_names(join_schema))

        # Strategy 1: Look for foreign key relationships (e.g., user_id -> id)
        join_pk = self._get_primary_key_column(join_schema)
        if join_pk:
            # Look for FK patterns like "user_id", "project_id", etc.
            for col in table_cols:
                if col.endswith("_id") and col != join_pk:
                    # This might be a FK to the join table
                    if join_pk in join_cols:
                        return (col, join_pk)

        table_pk = self._get_primary_key_column(table_schema)
        if table_pk and join_pk and table_pk in table_cols and join_pk in join_cols:
            return (table_pk, join_pk)

        return None

    def generate_select_query(
        self,
        table: str,
        limit: int = 1,
        with_join: str | None = None,
        schema: dict[str, Any] | None = None,
    ) -> str:
        """
        Generate a SELECT query.

        Args:
            table: Table name (e.g., 'abac_test.employees')
            limit: LIMIT clause value
            with_join: Optional table to JOIN with (e.g., 'abac_test.projects')
            schema: Optional schema definition (if None, tries to get from cache)

        Returns:
            SQL query string
        """
        if with_join:
            # Get schemas for both tables
            table_schema = schema or self._get_table_schema(table)
            join_schema = self._get_table_schema(with_join)

            # Find a valid join condition
            join_condition = self._find_valid_join_condition(table_schema, join_schema)

            if join_condition is None:
                # No valid join possible - fall back to simple SELECT
                return f"SELECT * FROM {table} LIMIT {limit};"

            left_col, right_col = join_condition

            # Build JOIN query with validated columns
            return (
                f"SELECT * FROM {table} e "
                f"JOIN {with_join} p ON e.{left_col} = p.{right_col} "
                f"LIMIT {limit};"
            )
        else:
            return f"SELECT * FROM {table} LIMIT {limit};"

    def generate_analytic_query(
        self,
        table: str,
        with_joins: list[str] = [],
        group_by: str | None = None,
        aggregate: str = "COUNT(*)",
        limit: int = 10,
        schema: dict[str, Any] | None = None,
    ) -> str:
        """
        Generate an analytic query (multiple joins + aggregations).

        Args:
            table: Primary table name
            with_joins: List of tables to join with
            group_by: Column name to group by
            aggregate: Aggregate function (e.g., "COUNT(*)", "SUM(col)")
            limit: LIMIT clause value
            schema: Optional schema definition

        Returns:
            SQL query string
        """
        table_name = table.split(".")[-1]
        table_alias = table_name[0]

        select_cols = []
        if group_by:
            select_cols.append(f"{table_alias}.{group_by}")
        select_cols.append(f"{aggregate} as result")

        query_parts = [f"SELECT {', '.join(select_cols)} FROM {table} {table_alias}"]

        # Add joins
        for i, join_table in enumerate(with_joins):
            join_table.split(".")[-1]
            j_alias = f"j{i}"

            # Determine join condition
            # 1. Try to find common column names (e.g. user_id -> user_id, or id -> id)
            # 2. Try to find FK relationships (e.g. user_id -> id)

            join_cond = None

            table_schema = self._get_table_schema(table)
            join_schema = self._get_table_schema(join_table)

            t_pk = self._get_primary_key_column(table_schema) if table_schema else "id"
            j_pk = self._get_primary_key_column(join_schema) if join_schema else "id"

            table_cols = (
                [c["name"] for c in table_schema.get("columns", [])]
                if table_schema
                else []
            )
            join_cols = (
                [c["name"] for c in join_schema.get("columns", [])]
                if join_schema
                else []
            )

            if "audit_logs" in table and "user_id" in table_cols:
                if j_pk == "id":
                    join_cond = f"{table_alias}.user_id = {j_alias}.id"

            elif "audit_logs" in join_table and "user_id" in join_cols:
                if t_pk == "id":
                    join_cond = f"{table_alias}.id = {j_alias}.user_id"

            if not join_cond:
                # Case 2: Join on Primary Keys (if they match types, arguably valid for synthetic bench)
                # Or if they have same name
                if t_pk == j_pk:
                    join_cond = f"{table_alias}.{t_pk} = {j_alias}.{j_pk}"
                else:
                    # Fallback: assume ID
                    join_cond = f"{table_alias}.id = {j_alias}.id"

            query_parts.append(f"JOIN {join_table} {j_alias} ON {join_cond}")

        if group_by:
            query_parts.append(f"GROUP BY {table_alias}.{group_by}")

        query_parts.append("ORDER BY result DESC")
        query_parts.append(f"LIMIT {limit};")

        return " ".join(query_parts)

    def generate_ddl_query(self, table: str, if_not_exists: bool = True) -> str:
        """
        Generate a DDL query (CREATE TABLE).

        Args:
            table: Table name (e.g., 'abac_test.test_table')
            if_not_exists: Use IF NOT EXISTS clause

        Returns:
            SQL query string
        """
        if_not_exists_clause = "IF NOT EXISTS" if if_not_exists else ""
        return f"CREATE TABLE {if_not_exists_clause} {table} (id INT PRIMARY KEY);"

    def get_test_id(self, table: str) -> int:
        """
        Get the test ID being used for a table.

        Args:
            table: Table name

        Returns:
            Test ID
        """
        table_name = table.split(".")[-1]
        return self._get_test_id(table_name)


# Singleton instance
_query_gen_instance: QueryGenerator | None = None


def get_query_generator(
    seed: int | None = None,
    auth_spec: dict[str, Any] | None = None,
    db_type: str = "mysql",
) -> QueryGenerator:
    """
    Get or create a QueryGenerator instance.

    Note: Creates a new instance if seed, auth_spec, or db_type is provided,
    to ensure schema awareness, proper syntax, and reproducibility.

    Args:
        seed: Optional seed for reproducibility
        auth_spec: Optional authorization spec for schema-aware generation
        db_type: Database type ("mysql" or "postgres")

    Returns:
        QueryGenerator instance
    """
    global _query_gen_instance
    # Always create new instance if seed, auth_spec, or db_type provided
    # to ensure proper schema awareness and syntax
    if seed is not None or auth_spec is not None or db_type != "mysql":
        return QueryGenerator(seed=seed, auth_spec=auth_spec, db_type=db_type)
    # Otherwise use singleton
    if _query_gen_instance is None:
        _query_gen_instance = QueryGenerator(
            seed=seed, auth_spec=auth_spec, db_type=db_type
        )
    return _query_gen_instance


if __name__ == "__main__":
    # Example usage with schemas
    qgen = QueryGenerator(seed=42)

    # Define schemas for testing
    employees_schema = {
        "columns": [
            {"name": "id", "type": "INT", "constraints": "PRIMARY KEY"},
            {"name": "name", "type": "VARCHAR(100)", "constraints": ""},
            {"name": "department", "type": "VARCHAR(50)", "constraints": ""},
        ]
    }

    projects_schema = {
        "columns": [
            {"name": "id", "type": "INT", "constraints": "PRIMARY KEY"},
            {"name": "name", "type": "VARCHAR(100)", "constraints": ""},
            {"name": "classification", "type": "VARCHAR(50)", "constraints": ""},
        ]
    }

    print("=== Generated Queries ===\n")

    print("INSERT Query:")
    print(qgen.generate_insert_query("abac_test.employees", schema=employees_schema))
    print()

    print("UPDATE Query:")
    print(qgen.generate_update_query("abac_test.employees", schema=employees_schema))
    print()

    print("DELETE Query:")
    print(qgen.generate_delete_query("abac_test.employees", schema=employees_schema))
    print()

    print("SELECT Query:")
    print(qgen.generate_select_query("abac_test.employees", schema=employees_schema))
    print()

    print("SELECT with JOIN:")
    print(
        qgen.generate_select_query(
            "abac_test.employees",
            with_join="abac_test.projects",
            schema=employees_schema,
        )
    )
    print()
