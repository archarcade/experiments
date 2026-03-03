#!/usr/bin/env python3
"""
Workload generation from authorization spec:
- Computes grants mapping from policies and attributes
- Generates queries per valid (user, action, table) combination
- Stores workload with metadata for reproducibility
"""

from __future__ import annotations

import json
import random
from collections.abc import Iterator
from pathlib import Path
from typing import Any

from pydantic import BaseModel
from tqdm import tqdm

from .config import Config
from .query_generator import get_query_generator

# Reuse existing grants mapping computation
from .translate_to_grants import compute_grants_mapping  # type: ignore

SUPPORTED_ACTIONS = ("SELECT", "INSERT", "UPDATE", "DELETE")


class Query(BaseModel):
    id: int
    user: str
    action: str
    table: str
    sql: str
    category: str  # e.g., SELECT, SELECT_JOIN, INSERT, UPDATE, DELETE


class Workload(BaseModel):
    queries: list[Query]
    metadata: dict[str, Any]

    def save(self, path: Path) -> None:
        payload = {
            "metadata": self.metadata,
            "queries": [q.model_dump() for q in self.queries],
        }
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(payload, indent=2))

    @staticmethod
    def load(path: Path) -> Workload:
        data = json.loads(path.read_text())
        queries = [Query(**q) for q in data.get("queries", [])]
        metadata = data.get("metadata", {})
        return Workload(queries=queries, metadata=metadata)


class WorkloadGenerator:
    def __init__(
        self,
        auth_spec_path: str,
        config: Config,
        seed: int | None = None,
        db_type: str = "mysql",
    ):
        self.auth_spec_path = auth_spec_path
        self.config = config
        self.db_type = db_type
        self.seed = self.config.workload.seed if seed is None else seed
        self._rand = random.Random(self.seed)
        self._auth_spec: dict[str, Any] = {}
        self._grants_map: list[dict[str, Any]] = []
        # Load auth spec early to pass to query generator for schema awareness
        self._load_auth_spec()
        self._qgen = get_query_generator(
            self.seed, auth_spec=self._auth_spec, db_type=self.db_type
        )
        # Track per-table existing IDs to avoid overhead queries
        self._table_state: dict[str, dict[str, Any]] = {}
        self._init_table_state()

    def _load_auth_spec(self) -> dict[str, Any]:
        if self._auth_spec:
            return self._auth_spec
        with open(self.auth_spec_path) as f:
            self._auth_spec = json.load(f)
        return self._auth_spec

    def _compute_grants_mapping(self) -> list[dict[str, Any]]:
        if self._grants_map:
            return self._grants_map
        spec = self._load_auth_spec()
        self._grants_map = compute_grants_mapping(spec)
        return self._grants_map

    def _valid_combinations(self) -> Iterator[tuple[str, str, str]]:
        """
        Yields (user, action, resource_table) for which the policy grants apply.
        """
        for entry in self._compute_grants_mapping():
            username = entry["username"]
            for grant in entry.get("grants", []):
                resource = grant[
                    "resource"
                ]  # e.g., "abac_test.employees" or "abac_test"
                resource_type = grant.get("resource_type", "Table")
                # Only generate queries for Table resources
                if resource_type != "Table":
                    continue
                # Defensive: skip if resource is not fully-qualified (db.table)
                if "." not in resource:
                    continue
                for priv in grant.get("privileges", []):
                    action = priv.upper()
                    if action in SUPPORTED_ACTIONS:
                        yield (username, action, resource)

    def _get_table_schema(self, table: str) -> dict[str, Any] | None:
        """Get schema for a table from auth spec."""
        spec = self._load_auth_spec()
        resources = spec.get("resources", [])
        for resource in resources:
            if resource.get("type") == "Table" and resource.get("name") == table:
                return resource.get("schema")
        return None

    def _get_primary_key_column(self, schema: dict[str, Any]) -> str | None:
        for col in schema.get("columns", []):
            constraints = col.get("constraints", "")
            if "PRIMARY KEY" in constraints.upper():
                return col["name"]
        return None

    def _init_table_state(self) -> None:
        """Initialize per-table state from auth_spec (sample_data and schema)."""
        spec = self._load_auth_spec()
        resources = spec.get("resources", [])
        for resource in resources:
            if resource.get("type") != "Table":
                continue
            table = resource.get("name")
            schema = resource.get("schema") or {}
            pk = self._get_primary_key_column(schema) or "id"
            existing_ids = set()
            for row in resource.get("sample_data", []):
                if pk in row:
                    existing_ids.add(int(row[pk]))
            # Use a high ID range (100000+) to avoid conflicts with sample_data
            # and ensure uniqueness across multiple workload runs
            max_existing = max(existing_ids) if existing_ids else 0
            next_id = max(100000, max_existing + 1)
            self._table_state[table] = {
                "pk": pk,
                "existing_ids": existing_ids,
                "next_id": next_id,
            }

    def _choose_existing_id(self, table: str) -> int | None:
        state = self._table_state.get(table)
        if not state:
            return None
        ids = list(state["existing_ids"])
        if not ids:
            return None
        return self._rand.choice(ids)

    def _has_privilege(self, user: str, action: str, table: str) -> bool:
        """Check if a user has a specific privilege on a table.

        Note: We only consider direct table-level grants here to ensure compatibility
        with both Baseline MySQL and the Cedar ABAC implementation, which currently
        evaluates policies against the specific resource being accessed.
        """
        grants = self._compute_grants_mapping()
        user_entry = next((e for e in grants if e["username"] == user), None)
        if not user_entry:
            return False

        for grant in user_entry.get("grants", []):
            resource = grant["resource"]
            privileges = [p.upper() for p in grant.get("privileges", [])]

            if action.upper() not in privileges:
                continue

            # Direct table match
            if resource == table:
                return True

        return False

    def _find_joinable_table(self, table: str, user: str) -> str | None:
        """Find a table that can be joined with the given table, which the user has SELECT access to."""
        spec = self._load_auth_spec()
        resources = spec.get("resources", [])
        tables = [
            r.get("name")
            for r in resources
            if r.get("type") == "Table" and r.get("name") != table
        ]

        # Filter tables that the user has SELECT access to
        authorized_tables = [
            t for t in tables if self._has_privilege(user, "SELECT", t)
        ]

        if authorized_tables:
            return self._rand.choice(authorized_tables)
        return None

    def _generate_sql(self, action: str, table: str, user: str) -> str | None:
        schema = self._get_table_schema(table)

        if action == "SELECT":
            # 80% simple select, 20% join (if another table exists and user has access)
            if self._rand.random() < 0.2:
                join_table = self._find_joinable_table(table, user)
                if join_table:
                    return self._qgen.generate_select_query(
                        table, limit=1, with_join=join_table, schema=schema
                    )
            return self._qgen.generate_select_query(table, limit=1, schema=schema)
        if action == "INSERT":
            # Use and advance next_id for this table
            state = self._table_state.get(table)
            if state is None:
                return None
            new_id = int(state["next_id"])
            # Generate INSERT IGNORE to handle duplicate key errors gracefully
            # This allows the workload to be run multiple times without errors
            sql = self._qgen.generate_insert_query(
                table, schema=schema, id_value=new_id, ignore_duplicate=True
            )
            # Update state
            state["existing_ids"].add(new_id)
            state["next_id"] = new_id + 1
            return sql
        if action == "UPDATE":
            # Pick an existing ID
            existing_id = self._choose_existing_id(table)
            if existing_id is None:
                return None
            return self._qgen.generate_update_query(
                table, schema=schema, id_value=existing_id
            )
        if action == "DELETE":
            # Pick an existing ID
            existing_id = self._choose_existing_id(table)
            if existing_id is None:
                return None
            sql = self._qgen.generate_delete_query(
                table, schema=schema, id_value=existing_id
            )
            # Update state: remove the id (best-effort)
            state = self._table_state.get(table)
            if state:
                state["existing_ids"].discard(existing_id)
            return sql
        raise ValueError(f"Unsupported action: {action}")

    def _generate_queries_for_combo(
        self, user: str, action: str, table: str, count: int
    ) -> list[Query]:
        queries: list[Query] = []
        attempts = 0
        # Try until we produce 'count' queries (guarded by a max attempts to avoid infinite loops)
        while len(queries) < count and attempts < count * 5:
            attempts += 1
            sql = self._generate_sql(action, table, user)
            if not sql:
                continue
            # Derive category (SELECT vs SELECT_JOIN) from SQL generation choice
            if action == "SELECT" and " JOIN " in sql:
                category = "SELECT_JOIN"
            else:
                category = action
            queries.append(
                Query(
                    id=self._rand.randrange(1_000_000_000),
                    user=user,
                    action=action,
                    table=table,
                    sql=sql,
                    category=category,
                )
            )
        return queries

    def generate(self) -> Workload:
        """
        Generates complete workload based on grants mapping and config.
        """
        spec = self._load_auth_spec()
        queries: list[Query] = []
        per_combo = int(self.config.workload.queries_per_combination)

        combinations = list(self._valid_combinations())

        for user, action, table in tqdm(
            combinations, desc="Generating workload", unit="combo"
        ):
            queries.extend(
                self._generate_queries_for_combo(user, action, table, per_combo)
            )

        self._rand.shuffle(queries)
        metadata = {
            "auth_spec": spec.get("metadata", {"name": "workload"}),
            "auth_spec_path": str(Path(self.auth_spec_path).resolve()),
            "seed": self.seed,
            "queries_per_combination": int(
                self.config.workload.queries_per_combination
            ),
            "action_distribution": self.config.workload.action_distribution,
            "total_queries": len(queries),
        }
        return Workload(queries=queries, metadata=metadata)
