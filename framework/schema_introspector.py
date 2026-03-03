#!/usr/bin/env python3
"""
Schema introspection helpers for MySQL INFORMATION_SCHEMA.
Currently minimal and optional.
"""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass
class ColumnSchema:
    name: str
    data_type: str
    is_primary: bool = False


@dataclass
class TableSchema:
    name: str
    columns: list[ColumnSchema] = field(default_factory=list)


class SchemaIntrospector:
    def __init__(self, database: str):
        self.database = database

    def introspect(self, connection) -> dict[str, TableSchema]:
        """
        Query INFORMATION_SCHEMA to get basic table/column metadata.
        """
        tables: dict[str, TableSchema] = {}
        cur = connection.cursor()
        try:
            cur.execute(
                """
                SELECT TABLE_NAME, COLUMN_NAME, DATA_TYPE, COLUMN_KEY
                FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_SCHEMA = %s
                """,
                (self.database,),
            )
            for table_name, col_name, data_type, column_key in cur:
                if table_name not in tables:
                    tables[table_name] = TableSchema(name=table_name, columns=[])
                tables[table_name].columns.append(
                    ColumnSchema(
                        name=col_name,
                        data_type=str(data_type),
                        is_primary=(str(column_key).upper() == "PRI"),
                    )
                )
        finally:
            cur.close()
        return tables
