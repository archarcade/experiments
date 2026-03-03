#!/usr/bin/env python3
"""
Template-based helpers for SQL statements.
"""

from __future__ import annotations


class QueryTemplates:
    """Pre-defined query templates per action."""

    SELECT_SIMPLE = "SELECT * FROM {table} LIMIT {limit};"
    SELECT_WHERE = "SELECT * FROM {table} WHERE {column} = {value} LIMIT {limit};"
    SELECT_JOIN = "SELECT {cols} FROM {table1} t1 JOIN {table2} t2 ON t1.id = t2.id LIMIT {limit};"

    INSERT_SINGLE = "INSERT INTO {table} ({columns}) VALUES ({values});"
    UPDATE_SINGLE = "UPDATE {table} SET {assignments} WHERE {condition};"
    DELETE_SINGLE = "DELETE FROM {table} WHERE {condition};"

    @staticmethod
    def render(template: str, **kwargs: dict[str, object]) -> str:
        return template.format(**kwargs)
