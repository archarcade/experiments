#!/usr/bin/env python3
"""
Database connection pooling utilities for MySQL and PostgreSQL.
"""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any
import time

try:
    import mysql.connector
    from mysql.connector import pooling as mysql_pooling  # type: ignore

    HAS_MYSQL = True
except ImportError:
    mysql = None
    mysql_pooling = None
    HAS_MYSQL = False

try:
    import psycopg2
    from psycopg2 import pool as pg_pool  # type: ignore

    HAS_POSTGRES = True
except ImportError:
    psycopg2 = None
    pg_pool = None
    HAS_POSTGRES = False

from .config import DatabaseConfig


class ConnectionPool:
    def __init__(self, config: DatabaseConfig):
        self.config = config
        self.db_type = config.type

        if self.db_type == "mysql":
            if not HAS_MYSQL:
                raise ImportError("mysql-connector-python required for MySQL databases")

            # MySQL may briefly refuse connections if the container restarts or
            # is performing crash recovery between multi-run pairs. Pool
            # initialization opens a connection immediately, so add a small
            # retry/backoff here to avoid failing the entire experiment.
            params = dict(config.connection_params)
            params.setdefault("connection_timeout", 5)

            last_err: Exception | None = None
            delay_s = 0.5
            for attempt in range(8):
                try:
                    self._pool = mysql_pooling.MySQLConnectionPool(
                        pool_name=f"{config.name}_pool",
                        pool_size=config.pool_size,
                        **params,
                    )
                    last_err = None
                    break
                except (
                    Exception
                ) as e:  # mysql.connector.Error hierarchy not always imported
                    last_err = e
                    if attempt == 7:
                        break
                    time.sleep(delay_s)
                    delay_s = min(delay_s * 2, 8.0)

            if last_err is not None:
                raise last_err
        elif self.db_type == "postgres":
            if not HAS_POSTGRES:
                raise ImportError("psycopg2 required for PostgreSQL databases")
            self._pool = pg_pool.SimpleConnectionPool(
                minconn=1,
                maxconn=config.pool_size,
                **config.connection_params,
            )
        else:
            raise ValueError(f"Unsupported database type: {self.db_type}")

    @contextmanager
    def get_connection(self) -> Iterator[Any]:
        if self.db_type == "mysql":
            conn = self._pool.get_connection()
            try:
                yield conn
            finally:
                conn.close()
        elif self.db_type == "postgres":
            conn = self._pool.getconn()
            try:
                yield conn
            finally:
                self._pool.putconn(conn)

    def execute_batch(self, queries: list[str], commit: bool = True) -> None:
        with self.get_connection() as conn:
            cur = conn.cursor()
            try:
                for q in queries:
                    cur.execute(q)
                if commit:
                    conn.commit()
            finally:
                cur.close()

    def get_db_type(self) -> str:
        """Return the database type (mysql or postgres)."""
        return self.db_type
