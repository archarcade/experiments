#!/usr/bin/env python3
"""
Translates authorization requirements to MySQL GRANT statements.
For original MySQL baseline comparison.

Automatically computes grants by evaluating policies against user/resource attributes.
Generates both CREATE USER and GRANT statements.
"""

import argparse
import json
import re
import sys
from collections import defaultdict


def evaluate_condition(condition, user_attrs, resource_attrs, resource_type="Table"):
    """Evaluate a policy condition against user and resource attributes.

    Args:
        condition: String like "principal.user_role == 'manager' AND resource.data_classification == 'sensitive'"
        user_attrs: Dict of user attributes, e.g., {"user_role": "manager", "clearance_level": "top_secret"}
        resource_attrs: Dict of resource attributes, e.g., {"data_classification": "sensitive"}
        resource_type: Type of the resource (e.g., "Table", "Column", "Database")

    Returns:
        True if condition evaluates to true, False otherwise
    """
    # Simple condition evaluator - handles basic comparisons with AND
    # For production, use a proper expression parser

    # Split by AND
    parts = [p.strip() for p in condition.split(" AND ")]

    for part in parts:
        part = part.strip()
        if not part:
            continue

        # Match patterns like: principal.attr == 'value' or resource.attr == 'value'
        principal_match = re.match(r"principal\.(\w+)\s*==\s*'([^']+)'", part)
        resource_match = re.match(r"resource\.(\w+)\s*==\s*'([^']+)'", part)
        # Match entity type checks: (principal|resource) is (Namespace::)?Type
        # Also handle optional curly braces if they somehow end up here
        type_match = re.search(
            r"(principal|resource)\s+is\s+([\w:]+)", part, re.IGNORECASE
        )

        if principal_match:
            attr_name, expected_value = principal_match.groups()
            actual_value = user_attrs.get(attr_name)
            if actual_value != expected_value:
                return False
        elif resource_match:
            attr_name, expected_value = resource_match.groups()
            actual_value = resource_attrs.get(attr_name)
            if actual_value != expected_value:
                return False
        elif type_match:
            entity_ref, expected_type_raw = type_match.groups()
            # Handle namespaced types by taking the last part (e.g., MySQL::User -> User)
            expected_type = expected_type_raw.split("::")[-1]

            if entity_ref.lower() == "principal":
                # For now we assume principal is always a User in this mapping logic
                if expected_type.lower() != "user":
                    return False
            else:
                # Case-insensitive comparison for resource type
                if resource_type.lower() != expected_type.lower():
                    # Handle some common mappings
                    if (
                        resource_type.lower() == "database"
                        and expected_type.lower() == "schema"
                    ):
                        # MySQL treats Schema and Database as synonymous
                        pass
                    elif (
                        resource_type.lower() == "table"
                        and expected_type.lower() == "column"
                    ):
                        # If we're checking Column but we have a Table, it's a mismatch
                        return False
                    else:
                        return False
        elif "resource is" in part.lower() or "principal is" in part.lower():
            # Fallback for "resource is Column" etc if regex fails
            # This handles cases where there might be curly braces or other noise
            if "principal is" in part.lower():
                if "user" not in part.lower():
                    return False
            else:
                # Basic check for resource type in the string
                if resource_type.lower() not in part.lower():
                    # Check for Column specifically
                    if "column" in part.lower() and resource_type.lower() != "column":
                        return False
                    # Check for Database (MySQL Schema is Database)
                    if (
                        "database" in part.lower() or "schema" in part.lower()
                    ) and resource_type.lower() != "database":
                        return False
                    return False
        elif part.lower() == "true":
            continue
        else:
            # Unknown condition format - be conservative and return False
            print(f"Warning: Could not parse condition part: {part}", file=sys.stderr)
            return False

    return True


def compute_grants_mapping(auth_spec):
    """Compute grants mapping by evaluating policies against user/resource attributes.

    Supports both legacy format (resources list) and new format (databases with nested tables).

    Returns:
        List of dicts containing username and grants. Each grant includes:
        {"privileges": [...], "resource": "...", "resource_type": "..."}
    """
    grants_map = defaultdict(
        lambda: defaultdict(set)
    )  # username -> resource -> set of privileges

    users = {u["username"]: u.get("attributes", {}) for u in auth_spec["users"]}

    # Build resources dict (name -> attrs) and resource types (name -> type)
    resources = {}
    resource_types = {}

    if "resources" in auth_spec:
        for resource in auth_spec["resources"]:
            resource_name = resource["name"]
            resource_attrs = resource.get("attributes", {})
            resources[resource_name] = resource_attrs
            resource_types[resource_name] = resource.get("type", "Table")

    # Evaluate each policy against all user/resource combinations
    for policy in auth_spec.get("policies", []):
        condition = policy.get("condition", "True")
        privileges = policy.get("privileges", [])
        # Support both 'action' (Cedar style) and 'privileges' (Grants style)
        if not privileges and "action" in policy:
            action = policy["action"]
            if isinstance(action, dict):
                privileges = [action.get("id", "")]
            else:
                privileges = [action]

        policy.get("id", "unknown")

        if not privileges:
            continue

        for username, user_attrs in users.items():
            for resource_name, resource_attrs in resources.items():
                r_type = resource_types.get(resource_name, "Table")
                if condition == "True" or evaluate_condition(
                    condition, user_attrs, resource_attrs, r_type
                ):
                    # This user/resource combination matches the policy
                    for priv in privileges:
                        if priv:
                            grants_map[username][resource_name].add(priv.upper())

    # Convert to list format
    result = []
    for username, resource_grants in grants_map.items():
        user_grants = []
        for resource_name, privileges_set in resource_grants.items():
            user_grants.append(
                {
                    "privileges": sorted(list(privileges_set)),
                    "resource": resource_name,
                    "resource_type": resource_types.get(resource_name, "Table"),
                }
            )
        if user_grants:
            result.append({"username": username, "grants": user_grants})

    return result


def translate_to_create_users(auth_spec):
    """Generate CREATE USER statements from authorization specification."""
    create_user_statements = []

    for user in auth_spec.get("users", []):
        username = user["username"]
        create_stmt = f"CREATE USER IF NOT EXISTS '{username}'@'%' IDENTIFIED BY '';"
        create_user_statements.append(create_stmt)

    return create_user_statements


def translate_to_grants(auth_spec, db_type="mysql"):
    """Generate GRANT statements from authorization specification."""
    grants = []

    # Compute grants mapping by evaluating policies
    grants_mapping = compute_grants_mapping(auth_spec)

    for mapping in grants_mapping:
        username = mapping["username"]
        for grant in mapping["grants"]:
            privileges = ", ".join(grant["privileges"])
            resource = grant["resource"]
            resource_type = grant.get("resource_type", "Table")

            # Determine the correct GRANT target
            # - Database resources must use db.* format (MySQL) or SCHEMA (Postgres)
            # - Table resources should be fully-qualified (db.table) in spec
            if db_type == "postgres":
                if resource_type == "Database":
                    # Postgres uses SCHEMA instead of database-level grants for tables
                    grant_stmt = f"GRANT ALL PRIVILEGES ON SCHEMA public TO {username};"
                else:
                    # Postgres doesn't use @'%' for users
                    grant_stmt = (
                        f"GRANT {privileges} ON TABLE {resource} TO {username};"
                    )
            else:
                # MySQL mapping
                mysql_privs = []
                for p in grant["privileges"]:
                    p_upper = p.upper()
                    if p_upper == "CONNECT":
                        # MySQL doesn't have CONNECT privilege, it's inherent in USAGE or just having an account
                        mysql_privs.append("USAGE")
                    elif p_upper == "*":
                        # Handle wildcard as ALL PRIVILEGES for baseline
                        mysql_privs.append("ALL PRIVILEGES")
                    else:
                        mysql_privs.append(p_upper)

                # Filter duplicates and join
                mysql_privs = sorted(list(set(mysql_privs)))
                priv_str = ", ".join(mysql_privs)

                if resource_type == "Database":
                    target = f"{resource}.*"
                elif resource_type == "Schema":
                    # MySQL treats Schema and Database as same
                    target = f"{resource}.*"
                else:
                    target = resource

                grant_stmt = f"GRANT {priv_str} ON {target} TO '{username}'@'%';"

            grants.append(grant_stmt)

            grants.append(grant_stmt)

    return grants


def translate_to_setup_sql(auth_spec, db_type="mysql"):
    """Generate complete setup SQL (CREATE USER + GRANT statements)."""
    statements = []

    # Generate CREATE USER statements
    if db_type == "postgres":
        for user in auth_spec.get("users", []):
            username = user["username"]
            statements.append(f"CREATE USER {username} WITH PASSWORD 'postgres';")
    else:
        create_users = translate_to_create_users(auth_spec)
        if create_users:
            statements.append("-- Create users (if they don't exist)")
            statements.extend(create_users)
            statements.append("")

    # Generate GRANT statements
    grants = translate_to_grants(auth_spec, db_type=db_type)
    if grants:
        statements.append("-- Grant privileges")
        statements.extend(grants)

    return statements


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Translate authorization spec to MySQL SQL statements"
    )
    parser.add_argument(
        "auth_spec", help="Path to authorization specification JSON file"
    )
    parser.add_argument(
        "--mode",
        choices=["users", "grants", "setup"],
        default="grants",
        help="Output mode: users (CREATE USER only), grants (GRANT only), setup (both)",
    )

    args = parser.parse_args()

    with open(args.auth_spec) as f:
        spec = json.load(f)

    if args.mode == "users":
        for stmt in translate_to_create_users(spec):
            print(stmt)
    elif args.mode == "grants":
        for stmt in translate_to_grants(spec):
            print(stmt)
    elif args.mode == "setup":
        for stmt in translate_to_setup_sql(spec):
            print(stmt)
