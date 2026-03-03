#!/usr/bin/env python3
"""
Dynamic SQL generator for authorization specifications.

This module generates all SQL statements dynamically from the auth spec,
eliminating the need for hardcoded SQL files.

Features:
- Generates CREATE DATABASE statements
- Generates CREATE TABLE statements with full schema
- Generates CREATE USER statements
- Generates INSERT statements for sample data
- Generates Cedar plugin initialization SQL
- Generates GRANT statements for baseline MySQL
"""

import json
from typing import Any


class SQLGenerator:
    """Generates SQL statements dynamically from authorization specification."""

    def __init__(self, auth_spec: dict[str, Any], db_type: str = "mysql"):
        """Initialize with authorization specification.

        Args:
            auth_spec: Authorization specification dictionary
            db_type: Database type ("mysql" or "postgres")
        """
        self.auth_spec = auth_spec
        self.db_type = db_type.lower()
        self.resources = auth_spec.get("resources", [])
        self.users = auth_spec.get("users", [])
        self.policies = auth_spec.get("policies", [])
        self.cedar_plugins = auth_spec.get("cedar_plugins", {})

        # Group resources by type for easier access
        # Legacy format: resources without type default to Table
        self.databases = [r for r in self.resources if r.get("type") == "Database"]
        self.tables = [
            r
            for r in self.resources
            if r.get("type") == "Table" or r.get("type") is None
        ]

    def generate_cedar_plugin_init_sql(
        self, plugin_config: dict[str, Any] | None = None
    ) -> str:
        """Generate Cedar plugin initialization SQL dynamically.

        Args:
            plugin_config: Plugin configuration dict from config.yaml (takes precedence over auth_spec)

        Returns:
            SQL string for Cedar plugin initialization
        """
        # Get Cedar configuration from config.yaml (preferred) or fall back to auth_spec
        if plugin_config:
            ddl_config = plugin_config.get("ddl_audit", {})
            auth_config = plugin_config.get("cedar_authorization", {})
        else:
            # Fallback to auth_spec for backward compatibility
            ddl_config = self.cedar_plugins.get("ddl_audit", {})
            auth_config = self.cedar_plugins.get("cedar_authorization", {})

        # Read URLs directly from plugin configs
        ddl_url = ddl_config.get("url", "http://localhost:8280")
        ddl_timeout = ddl_config.get("timeout_ms", 5000)
        ddl_enabled = ddl_config.get("enabled", True)

        # Authorization URL is read directly from config (full path)
        auth_url = auth_config.get("url", "http://localhost:8280/v1/is_authorized")
        auth_timeout = auth_config.get("timeout_ms", 5000)

        sql_parts = []

        if self.db_type == "postgres":
            sql_parts.append("-- Initialize Cedar extension for PostgreSQL")
            sql_parts.append("CREATE EXTENSION IF NOT EXISTS cedar_auth;")
            sql_parts.append("")
            # Set GUCs (using the functions provided by the extension)
            # Assuming the extension schema is 'cedar_auth'
            sql_parts.append(f"SELECT cedar_auth.set_agent_url('{auth_url}');")
            sql_parts.append(f"SELECT cedar_auth.set_timeout({auth_timeout});")
            sql_parts.append("")
            sql_parts.append("-- Verify extension status")
            sql_parts.append("SELECT * FROM cedar_auth.status;")
        else:
            # Install DDL audit plugin
            sql_parts.append("-- Install and configure ddl_audit plugin")
            sql_parts.append("INSTALL PLUGIN ddl_audit SONAME 'ddl_audit.so';")
            sql_parts.append("")
            sql_parts.append(f"SET GLOBAL ddl_audit_cedar_url = '{ddl_url}';")
            sql_parts.append(f"SET GLOBAL ddl_audit_cedar_timeout = {ddl_timeout};")
            sql_parts.append(
                f"SET GLOBAL ddl_audit_enabled = {'ON' if ddl_enabled else 'OFF'};"
            )
            sql_parts.append("")
            sql_parts.append("-- Verify DDL audit plugin")
            sql_parts.append(
                "SELECT PLUGIN_NAME, PLUGIN_STATUS FROM INFORMATION_SCHEMA.PLUGINS WHERE PLUGIN_NAME='ddl_audit';"
            )
            sql_parts.append("SHOW VARIABLES LIKE 'ddl_audit_%';")
            sql_parts.append("")

            # Install Cedar authorization plugin
            sql_parts.append("-- Install and configure cedar_authorization plugin")
            sql_parts.append(
                "INSTALL PLUGIN cedar_authorization SONAME 'cedar_authorization.so';"
            )
            sql_parts.append("")
            sql_parts.append(f"SET GLOBAL cedar_authorization_url = '{auth_url}';")
            sql_parts.append(
                f"SET GLOBAL cedar_authorization_timeout = {auth_timeout};"
            )
            sql_parts.append("")
            sql_parts.append("-- Verify Cedar authorization plugin")
            sql_parts.append(
                "SELECT PLUGIN_NAME, PLUGIN_STATUS, PLUGIN_TYPE FROM INFORMATION_SCHEMA.PLUGINS WHERE PLUGIN_NAME = 'cedar_authorization';"
            )
            sql_parts.append("SHOW VARIABLES LIKE 'cedar_authorization%';")

        return "\n".join(sql_parts)

    def generate_database_creation_sql(self) -> str:
        """Generate CREATE DATABASE statements for all databases in spec.

        Returns:
            SQL string for database creation
        """
        sql_parts = []

        # Get unique database names from Database-type resources
        db_names = set()
        for db in self.databases:
            db_names.add(db["name"])

        # Also extract database names from Table-type resources (e.g., "abac_test.employees" -> "abac_test")
        for table in self.tables:
            table_name = table["name"]
            if "." in table_name:
                db_name = table_name.split(".")[0]
                db_names.add(db_name)

        # Generate CREATE DATABASE statements
        for db_name in sorted(db_names):
            sql_parts.append(f"-- Create database: {db_name}")
            if self.db_type == "postgres":
                # Postgres doesn't support IF NOT EXISTS for CREATE DATABASE
                # We'll use a shell-compatible comment or a DO block (too complex for here)
                # Usually handled by the setup script or by checking existence
                sql_parts.append(f"CREATE DATABASE {db_name};")
            else:
                sql_parts.append(f"CREATE DATABASE IF NOT EXISTS {db_name};")
            sql_parts.append("")

        return "\n".join(sql_parts)

    def generate_table_creation_sql(self, database_name: str | None = None) -> str:
        """Generate CREATE TABLE statements for all tables in spec.

        Args:
            database_name: If specified, only generate tables for this database

        Returns:
            SQL string for table creation
        """
        sql_parts = []

        for table in self.tables:
            full_table_name = table["name"]

            # Skip if database filter is specified and table doesn't match
            if database_name:
                if "." not in full_table_name:
                    continue
                table_db_name = full_table_name.split(".")[0]
                if table_db_name != database_name:
                    continue

            # Skip if table doesn't have a schema definition
            if "schema" not in table:
                continue

            sql_parts.append(f"-- Create table: {full_table_name}")

            # Build column definitions
            columns = []
            for col in table["schema"]["columns"]:
                col_name = col["name"]
                col_type = col["type"]
                col_constraints = col.get("constraints", "")

                col_def = f"    {col_name} {col_type}"
                if col_constraints:
                    col_def += f" {col_constraints}"
                columns.append(col_def)

            columns_sql = ",\n".join(columns)
            sql_parts.append(f"CREATE TABLE IF NOT EXISTS {full_table_name} (")
            sql_parts.append(columns_sql)
            sql_parts.append(");")
            sql_parts.append("")

        return "\n".join(sql_parts)

    def generate_sample_data_sql(self, database_name: str | None = None) -> str:
        """Generate INSERT statements for sample data.

        Args:
            database_name: If specified, only generate data for this database

        Returns:
            SQL string for sample data insertion
        """
        sql_parts = []

        for table in self.tables:
            full_table_name = table["name"]

            # Skip if database filter is specified and table doesn't match
            if database_name:
                if "." not in full_table_name:
                    continue
                table_db_name = full_table_name.split(".")[0]
                if table_db_name != database_name:
                    continue

            sample_data = table.get("sample_data", [])

            if not sample_data:
                continue

            sql_parts.append(f"-- Insert sample data: {full_table_name}")

            # Get column names from first row
            columns = list(sample_data[0].keys())
            columns_str = ", ".join(columns)

            # Build values list
            values_list = []
            for row in sample_data:
                values = []
                for col in columns:
                    value = row[col]
                    if isinstance(value, str):
                        # Escape single quotes in strings
                        value = value.replace("'", "''")
                        values.append(f"'{value}'")
                    elif value is None:
                        values.append("NULL")
                    elif isinstance(value, bool):
                        if self.db_type == "postgres":
                            values.append("true" if value else "false")
                        else:
                            values.append("1" if value else "0")
                    else:
                        values.append(str(value))
                values_list.append(f"    ({', '.join(values)})")

            values_str = ",\n".join(values_list)

            if self.db_type == "postgres":
                # Generate INSERT with ON CONFLICT DO UPDATE for idempotency
                sql_parts.append(
                    f"INSERT INTO {full_table_name} ({columns_str}) VALUES"
                )
                sql_parts.append(values_str)

                # Assume 'id' is the primary key for conflict target
                update_parts = [
                    f"{col}=EXCLUDED.{col}" for col in columns if col != "id"
                ]
                if update_parts:
                    sql_parts.append(
                        f"ON CONFLICT (id) DO UPDATE SET {', '.join(update_parts)};"
                    )
                else:
                    sql_parts.append("ON CONFLICT (id) DO NOTHING;")
            else:
                # Generate INSERT with ON DUPLICATE KEY UPDATE for idempotency
                sql_parts.append(
                    f"INSERT INTO {full_table_name} ({columns_str}) VALUES"
                )
                sql_parts.append(values_str)

                # Build ON DUPLICATE KEY UPDATE clause
                update_parts = [
                    f"{col}=VALUES({col})" for col in columns if col != "id"
                ]
                if update_parts:
                    sql_parts.append(
                        f"ON DUPLICATE KEY UPDATE {', '.join(update_parts)};"
                    )
                else:
                    sql_parts[-1] += ";"

            sql_parts.append("")

        return "\n".join(sql_parts)

    def generate_user_creation_sql(self) -> str:
        """Generate CREATE USER statements for all users in spec.

        Returns:
            SQL string for user creation
        """
        sql_parts = []
        sql_parts.append("-- Create users")

        for user in self.users:
            username = user["username"]
            password = user.get("password", "")
            host = user.get("host", "%")

            if self.db_type == "postgres":
                # Postgres user creation (IF NOT EXISTS is not standard, but we'll use a simple CREATE ROLE)
                # In research environments, we usually don't care about host-based restriction at the SQL level as much
                sql_parts.append(
                    f"CREATE USER {username} WITH PASSWORD '{password if password else 'postgres'}';"
                )
            else:
                sql_parts.append(
                    f"CREATE USER IF NOT EXISTS '{username}'@'{host}' IDENTIFIED BY '{password}';"
                )

        return "\n".join(sql_parts)

    def generate_complete_setup_sql(self, cedar_url: str | None = None) -> str:
        """Generate complete SQL setup (databases, tables, users, data).

        Args:
            cedar_url: Override Cedar agent URL

        Returns:
            Complete SQL string for setup
        """
        sql_parts = []

        sql_parts.append("-- Complete Setup SQL")
        sql_parts.append("-- Generated dynamically from authorization specification")
        sql_parts.append("")
        sql_parts.append(self.generate_database_creation_sql())
        sql_parts.append(self.generate_table_creation_sql())
        sql_parts.append(self.generate_user_creation_sql())
        sql_parts.append("")
        sql_parts.append(self.generate_sample_data_sql())

        return "\n".join(sql_parts)

    def get_all_resources(self) -> list[dict[str, Any]]:
        """Get all resources (databases and tables) with their attributes.

        Returns:
            List of resource dictionaries with type, name, and attributes
        """
        # Simply return all resources from the auth spec
        return self.resources

    def get_tables_for_database(self, database_name: str) -> list[dict[str, Any]]:
        """Get all tables for a specific database.

        Args:
            database_name: Database name

        Returns:
            List of table dictionaries
        """
        tables = []
        for table in self.tables:
            full_table_name = table["name"]
            if "." in full_table_name:
                table_db_name = full_table_name.split(".")[0]
                if table_db_name == database_name:
                    tables.append(table)
        return tables

    def get_resource_by_name(
        self, name: str, resource_type: str | None = None
    ) -> dict[str, Any] | None:
        """Get a resource by name and optionally by type.

        Args:
            name: Resource name
            resource_type: Optional resource type filter (e.g., "Database", "Table")

        Returns:
            Resource dictionary if found, None otherwise
        """
        for resource in self.resources:
            if resource["name"] == name:
                if resource_type is None or resource.get("type") == resource_type:
                    return resource
        return None


def load_auth_spec(path: str) -> dict[str, Any]:
    """Load authorization specification from JSON file.

    Args:
        path: Path to auth spec JSON file

    Returns:
        Authorization specification dictionary
    """
    with open(path) as f:
        return json.load(f)


if __name__ == "__main__":
    import sys

    if len(sys.argv) < 2:
        print("Usage: sql_generator.py <auth_spec.json> [command]")
        print("")
        print("Commands:")
        print("  plugin-init     Generate Cedar plugin initialization SQL")
        print("  databases       Generate database creation SQL")
        print("  tables          Generate table creation SQL")
        print("  users           Generate user creation SQL")
        print("  sample-data     Generate sample data insertion SQL")
        print("  complete        Generate complete setup SQL (default)")
        sys.exit(1)

    auth_spec_path = sys.argv[1]
    command = sys.argv[2] if len(sys.argv) > 2 else "complete"

    spec = load_auth_spec(auth_spec_path)
    generator = SQLGenerator(spec)

    if command == "plugin-init":
        print(generator.generate_cedar_plugin_init_sql())
    elif command == "databases":
        print(generator.generate_database_creation_sql())
    elif command == "tables":
        print(generator.generate_table_creation_sql())
    elif command == "users":
        print(generator.generate_user_creation_sql())
    elif command == "sample-data":
        print(generator.generate_sample_data_sql())
    elif command == "complete":
        print(generator.generate_complete_setup_sql())
    else:
        print(f"Unknown command: {command}", file=sys.stderr)
        sys.exit(1)
