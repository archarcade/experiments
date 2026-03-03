#!/usr/bin/env python3
"""
Translates authorization requirements to Cedar policies and attributes.
For modified MySQL with Cedar authorization.

This script assumes the Cedar agent has been started with the base schema
from mysql_schemas/schema.json, which defines entity types (User, Table, etc.)
and actions (SELECT, INSERT, UPDATE, DELETE).

This script then:
1. Adds custom attributes to the schema (user_role, clearance_level, data_classification)
2. Assigns attribute values to entities (entities must already exist in Cedar agent,
   having been propagated from MySQL via the DDL audit plugin)
3. Creates policies based on the auth spec

Note: Entities are NOT created by this script. They are created in MySQL (via CREATE USER,
CREATE TABLE, etc.) and automatically propagated to Cedar agent by the DDL audit plugin.
"""

import json
import sys
import time

import requests

BASE_URL = "http://localhost:8280/v1"


def check_cedar_agent(base_url):
    """Check if Cedar agent is accessible."""
    try:
        # Health endpoint is /v1/, and base_url should already be .../v1
        # So we check base_url/ (which is /v1/)
        health_url = base_url if base_url.endswith("/") else base_url + "/"
        response = requests.get(health_url, timeout=5)
        response.raise_for_status()
        return True
    except requests.exceptions.RequestException as e:
        print(f"Error: Cedar agent not accessible at {base_url}: {e}", file=sys.stderr)
        return False


def get_all_entities(base_url):
    """Get all entities from Cedar agent."""
    try:
        response = requests.get(f"{base_url}/data", timeout=5)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Error: Failed to get all entities at {base_url}: {e}", file=sys.stderr)
        return []
    except Exception as e:
        print(f"Error: Failed to get all entities at {base_url}: {e}", file=sys.stderr)
        return []


def entity_exists(base_url, entity_type, entity_id, namespace=""):
    """Check if an entity exists in Cedar agent."""
    try:
        entities = get_all_entities(base_url)
        # Entities format: [{"uid": {"type": "User", "id": "user_alice"}, ...}, ...]
        full_type = namespace + "::" + entity_type if namespace else entity_type
        for entity in entities:
            uid = entity.get("uid", {})
            if uid.get("type") == full_type and uid.get("id") == entity_id:
                return True
        return False
    except requests.exceptions.RequestException as e:
        print(
            f"Error: Failed to check if entity exists at {base_url}: {e}",
            file=sys.stderr,
        )
        return False
    except Exception as e:
        print(
            f"Error: Failed to check if entity exists at {base_url}: {e}",
            file=sys.stderr,
        )
        return False


def create_entity(base_url, entity_type, entity_id, namespace="", attrs=None):
    """Create an entity in Cedar agent if it doesn't exist."""
    full_type = namespace + "::" + entity_type if namespace else entity_type
    uid = {"id": entity_id, "type": full_type}
    entity = {"uid": uid, "attrs": attrs or {}, "parents": []}

    try:
        # Try to add single data entry
        # The endpoint expects a list of entities
        response = requests.put(
            f"{base_url}/data/single/{entity_id}", json=[entity], timeout=5
        )
        if response.status_code in (200, 201):
            return True
        elif response.status_code == 409:
            return True  # Already exists
        else:
            print(
                f"Warning: Failed to create entity {full_type}::{entity_id}: {response.status_code} {response.text}",
                file=sys.stderr,
            )
            return False
    except Exception as e:
        print(
            f"Warning: Error creating entity {full_type}::{entity_id}: {e}",
            file=sys.stderr,
        )
        return False


def wait_for_entities(
    base_url, entity_list, namespace="", max_wait=30, check_interval=2
):
    """Wait for entities to appear in Cedar agent.

    Args:
        base_url: Cedar agent base URL
        entity_list: List of (entity_type, entity_id) tuples
        namespace: Cedar namespace
        max_wait: Maximum time to wait in seconds
        check_interval: Time between checks in seconds

    Returns:
        List of (entity_type, entity_id) tuples that were found
    """
    waited = 0
    found = []
    remaining = entity_list.copy()

    while remaining and waited < max_wait:
        for entity_type, entity_id in remaining[:]:  # Copy list to iterate safely
            if entity_exists(base_url, entity_type, entity_id, namespace):
                found.append((entity_type, entity_id))
                remaining.remove((entity_type, entity_id))

        if not remaining:
            break

        time.sleep(check_interval)
        waited += check_interval

    return found


def setup_cedar_schema(base_url, auth_spec, namespace=""):
    """Set up Cedar schema attributes.

    Note: The base schema (entity types and actions) should already be loaded
    from mysql_schemas/schema.json when the Cedar agent starts.
    This function only adds custom attributes.

    Supports both legacy format (resources list) and new format (databases list).
    """
    # Extract unique attributes from users
    user_attrs = set()
    for user in auth_spec["users"]:
        user_attrs.update(user.get("attributes", {}).keys())

    # Extract attributes for databases and tables
    database_attrs = set()
    table_attrs = set()

    # Process resources (both legacy and new format)
    if "resources" in auth_spec:
        for resource in auth_spec["resources"]:
            resource_type = resource.get(
                "type", "Table"
            )  # Default to Table for legacy format

            if resource_type == "Database":
                database_attrs.update(resource.get("attributes", {}).keys())
            elif resource_type == "Table":
                table_attrs.update(resource.get("attributes", {}).keys())

    # Add User attributes to schema
    for attr in user_attrs:
        try:
            response = requests.post(
                f"{base_url}/schema/attribute",
                json={
                    "entity_type": "User",
                    "namespace": namespace,
                    "name": attr,
                    "attr_type": "String",
                    "required": False,
                },
            )
            # 200, 201 = success, 409 = already exists, 400 might also mean already exists
            if response.status_code not in [200, 201, 400, 409]:
                # Only print warning if it's a real error (not "already exists")
                error_msg = ""
                try:
                    error_msg = response.text
                except Exception:
                    pass
                if (
                    "already exists" not in error_msg.lower()
                    and "duplicate" not in error_msg.lower()
                ):
                    print(
                        f"Warning: Failed to add User attribute {attr}: {response.status_code}",
                        file=sys.stderr,
                    )
        except requests.exceptions.RequestException as e:
            print(f"Warning: Error adding User attribute {attr}: {e}", file=sys.stderr)

    # Add Database attributes to schema
    for attr in database_attrs:
        try:
            response = requests.post(
                f"{base_url}/schema/attribute",
                json={
                    "entity_type": "Database",
                    "namespace": namespace,
                    "name": attr,
                    "attr_type": "String",
                    "required": False,
                },
            )
            if response.status_code not in [200, 201, 400, 409]:
                error_msg = ""
                try:
                    error_msg = response.text
                except Exception as e:
                    print(f"[translate_to_cedar.py] Error: {e}", file=sys.stderr)
                    pass
                if (
                    "already exists" not in error_msg.lower()
                    and "duplicate" not in error_msg.lower()
                ):
                    print(
                        f"Warning: Failed to add Database attribute {attr}: {response.status_code}",
                        file=sys.stderr,
                    )
        except requests.exceptions.RequestException as e:
            print(
                f"Warning: Error adding Database attribute {attr}: {e}", file=sys.stderr
            )

    # Add Table attributes to schema
    for attr in table_attrs:
        try:
            response = requests.post(
                f"{base_url}/schema/attribute",
                json={
                    "entity_type": "Table",
                    "namespace": namespace,
                    "name": attr,
                    "attr_type": "String",
                    "required": False,
                },
            )
            # 200, 201 = success, 409 = already exists, 400 might also mean already exists
            if response.status_code not in [200, 201, 400, 409]:
                # Only print warning if it's a real error (not "already exists")
                error_msg = ""
                try:
                    error_msg = response.text
                except Exception:
                    pass
                if (
                    "already exists" not in error_msg.lower()
                    and "duplicate" not in error_msg.lower()
                ):
                    print(
                        f"Warning: Failed to add Table attribute {attr}: {response.status_code}",
                        file=sys.stderr,
                    )
        except requests.exceptions.RequestException as e:
            print(f"Warning: Error adding Table attribute {attr}: {e}", file=sys.stderr)


# Note: Entities are NOT created here. They are created in MySQL (via CREATE USER, CREATE TABLE, etc.)
# and automatically propagated to Cedar agent by the DDL audit plugin.


def assign_user_attributes(base_url, auth_spec, namespace=""):
    """Assign attribute values to user entities.

    Note: Entities must already exist in Cedar agent (propagated from MySQL via DDL audit plugin).
    This function waits for entities to appear before setting attributes.
    """

    for user in auth_spec["users"]:
        username = user["username"]

        # Ensure entity exists (create if not present)
        # This is more robust than waiting for DDL plugin propagation
        if not entity_exists(base_url, "User", username, namespace):
            create_entity(base_url, "User", username, namespace)

        # Now set attributes
        for attr_name, attr_value in user["attributes"].items():
            max_retries = 3
            retry_delay = 1  # seconds

            for attempt in range(max_retries):
                try:
                    response = requests.put(
                        f"{base_url}/data/attribute",
                        json={
                            "entity_type": "User",
                            "namespace": namespace,
                            "entity_id": username,
                            "attribute_name": attr_name,
                            "attribute_value": attr_value,
                        },
                    )
                    # 200, 201 = success
                    if response.status_code in [200, 201]:
                        break

                    if response.status_code == 404:
                        if attempt < max_retries - 1:
                            time.sleep(retry_delay)
                            continue
                        else:
                            print(
                                f"Warning: Failed to set User {username}.{attr_name}: 404 (entity may have disappeared)",
                                file=sys.stderr,
                            )
                    else:
                        print(
                            f"Warning: Failed to set User {username}.{attr_name}: {response.status_code} - {response.text}",
                            file=sys.stderr,
                        )
                        break
                except requests.exceptions.RequestException as e:
                    if attempt < max_retries - 1:
                        time.sleep(retry_delay)
                        continue
                    else:
                        print(
                            f"Warning: Error setting User {username}.{attr_name}: {e}",
                            file=sys.stderr,
                        )


def assign_database_attributes(base_url, auth_spec, namespace=""):
    """Assign attribute values to database entities.

    Note: Entities must already exist in Cedar agent (propagated from MySQL via DDL audit plugin).
    This function waits for entities to appear before setting attributes.
    """
    if "resources" not in auth_spec:
        return

    # Filter to only Database-type resources
    databases = [r for r in auth_spec["resources"] if r.get("type") == "Database"]

    for database in databases:
        database_name = database["name"]
        database_attributes = database.get("attributes", {})

        if not database_attributes:
            continue

        # Ensure entity exists (create if not present)
        if not entity_exists(base_url, "Database", database_name, namespace):
            create_entity(base_url, "Database", database_name, namespace)

        # Now set attributes
        for attr_name, attr_value in database_attributes.items():
            max_retries = 3
            retry_delay = 1  # seconds

            for attempt in range(max_retries):
                try:
                    response = requests.put(
                        f"{base_url}/data/attribute",
                        json={
                            "entity_type": "Database",
                            "namespace": namespace,
                            "entity_id": database_name,
                            "attribute_name": attr_name,
                            "attribute_value": attr_value,
                        },
                    )
                    # 200, 201 = success
                    if response.status_code in [200, 201]:
                        break

                    if response.status_code == 404:
                        if attempt < max_retries - 1:
                            time.sleep(retry_delay)
                            continue
                        else:
                            print(
                                f"Warning: Failed to set Database {database_name}.{attr_name}: 404 (entity may have disappeared)",
                                file=sys.stderr,
                            )
                    else:
                        print(
                            f"Warning: Failed to set Database {database_name}.{attr_name}: {response.status_code} - {response.text}",
                            file=sys.stderr,
                        )
                        break
                except requests.exceptions.RequestException as e:
                    if attempt < max_retries - 1:
                        time.sleep(retry_delay)
                        continue
                    else:
                        print(
                            f"Warning: Error setting Database {database_name}.{attr_name}: {e}",
                            file=sys.stderr,
                        )


def assign_resource_attributes(base_url, auth_spec, namespace=""):
    """Assign attribute values to resource entities (tables).

    Note: Entities must already exist in Cedar agent (propagated from MySQL via DDL audit plugin).
    This function waits for entities to appear before setting attributes.

    Supports both legacy format (resources without type) and new format (resources with type).
    """
    if "resources" not in auth_spec:
        return

    # Filter to all resources with attributes
    resources_to_process = []
    for resource in auth_spec["resources"]:
        if resource.get("attributes"):
            resources_to_process.append(resource)

    # Process all resources
    for resource in resources_to_process:
        resource_name = resource["name"]
        resource_type = resource.get("type", "Table")

        # PostgreSQL specific handling: add public. prefix for tables if not already present
        if (
            namespace == "PostgreSQL"
            and resource_type == "Table"
            and not resource_name.startswith("public.")
        ):
            resource_name = f"public.{resource_name}"

        # Ensure entity exists (create if not present)
        if not entity_exists(base_url, resource_type, resource_name, namespace):
            create_entity(base_url, resource_type, resource_name, namespace)

        # Now set attributes
        for attr_name, attr_value in resource["attributes"].items():
            max_retries = 3
            retry_delay = 1  # seconds

            for attempt in range(max_retries):
                try:
                    response = requests.put(
                        f"{base_url}/data/attribute",
                        json={
                            "entity_type": resource_type,
                            "namespace": namespace,
                            "entity_id": resource_name,
                            "attribute_name": attr_name,
                            "attribute_value": attr_value,
                        },
                    )
                    # 200, 201 = success
                    if response.status_code in [200, 201]:
                        break

                    if response.status_code == 404:
                        if attempt < max_retries - 1:
                            time.sleep(retry_delay)
                            continue
                        else:
                            print(
                                f"Warning: Failed to set Table {resource_name}.{attr_name}: 404 (entity may have disappeared)",
                                file=sys.stderr,
                            )
                    else:
                        print(
                            f"Warning: Failed to set Table {resource_name}.{attr_name}: {response.status_code} - {response.text}",
                            file=sys.stderr,
                        )
                        break
                except requests.exceptions.RequestException as e:
                    if attempt < max_retries - 1:
                        time.sleep(retry_delay)
                        continue
                    else:
                        print(
                            f"Warning: Error setting Table {resource_name}.{attr_name}: {e}",
                            file=sys.stderr,
                        )


def create_cedar_policies(auth_spec, namespace=""):
    """Create Cedar policies from authorization requirements.

    Policies are attribute-based and defined separately, not per-user.
    This matches the ABAC paradigm where policies evaluate attributes.
    """
    policies = []

    if "policies" not in auth_spec:
        return policies

    prefix = namespace + "::" if namespace else ""

    for policy_spec in auth_spec["policies"]:
        policy_id = policy_spec["id"]
        privileges = policy_spec["privileges"]
        condition = policy_spec["condition"]

        # Build Cedar policy string
        # Convert condition from simplified format to Cedar syntax
        # Example: "principal.user_role == 'manager' AND resource.data_classification == 'sensitive'"
        # becomes: "principal has user_role && principal.user_role == \"manager\" && resource has data_classification && resource.data_classification == \"sensitive\""

        # Parse condition and convert to Cedar format
        cedar_condition = convert_condition_to_cedar(condition)

        # Build action expression
        # Action name case sensitivity should match what the plugins send
        # If no namespace, use legacy Action::"SELECT"
        # If namespace, use MySQL::Action::"Select" or PostgreSQL::Action::"Select"

        def format_action(p):
            # All actions are now standardized to ALL CAPS (e.g., SELECT)
            return f'{prefix}Action::"{p.upper()}"'

        actions = ", ".join([format_action(p) for p in privileges])
        if len(privileges) == 1:
            if privileges[0] == "*":
                action_expr = "action"
            else:
                action_expr = f"action == {actions}"
        else:
            action_expr = f"action in [{actions}]"

        # Build Cedar policy (policies don't reference specific users, only attributes)
        # Handle "resource is <EntityType>" or "principal is <EntityType>" special cases
        for entity_type in [
            "User",
            "Table",
            "Column",
            "Database",
            "Schema",
            "Routine",
            "Type",
        ]:
            if f"is {entity_type}" in cedar_condition:
                # For MySQL, Schema and Database are analogous
                if namespace == "MySQL" and entity_type == "Schema":
                    cedar_condition = cedar_condition.replace(
                        "is Schema", f"is {prefix}Database"
                    )

                cedar_condition = cedar_condition.replace(
                    f"is {entity_type}", f"is {prefix}{entity_type}"
                )

        policy_content = f"""permit(
  principal,
  {action_expr},
  resource
)
when {{
  {cedar_condition}
}};"""

        policies.append(
            {
                "id": f"{namespace.lower()}_{policy_id}" if namespace else policy_id,
                "content": policy_content,
            }
        )

    return policies


def convert_condition_to_cedar(condition):
    """Convert simplified condition format to Cedar policy syntax.

    Example input: "principal.user_role == 'manager' AND resource.data_classification == 'sensitive'"
    Example output: "principal has user_role && principal.user_role == \"manager\" && resource has data_classification && resource.data_classification == \"sensitive\""
    """
    # Simple conversion - in production, use proper parsing
    # Replace principal.attr with principal has attr && principal.attr
    # Replace resource.attr with resource has attr && resource.attr
    import re

    # Add "has" checks for attributes
    def add_has_check(match):
        attr_name = match.group(1)
        entity = match.group(0).split(".")[0]
        return f"{entity} has {attr_name} && {match.group(0)}"

    # Replace principal.attr with principal has attr && principal.attr
    condition = re.sub(r"principal\.(\w+)", add_has_check, condition)
    # Replace resource.attr with resource has attr && resource.attr
    condition = re.sub(r"resource\.(\w+)", add_has_check, condition)

    # Replace single quotes with double quotes for Cedar
    condition = condition.replace("'", '"')

    # Replace AND with &&
    condition = condition.replace(" AND ", " && ")

    return condition


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print(
            "Usage: translate_to_cedar.py <auth_spec.json> [cedar_agent_url] [namespace]",
            file=sys.stderr,
        )
        sys.exit(1)

    base_url = sys.argv[2] if len(sys.argv) > 2 else BASE_URL
    if not base_url.endswith("/v1"):
        if base_url.endswith("/"):
            base_url = base_url + "v1"
        else:
            base_url = base_url + "/v1"

    namespace = sys.argv[3] if len(sys.argv) > 3 else ""

    # Check if Cedar agent is accessible
    if not check_cedar_agent(base_url):
        print(
            "Error: Cedar agent is not accessible. Please ensure it's running.",
            file=sys.stderr,
        )
        print(f"Expected URL: {base_url}", file=sys.stderr)
        sys.exit(1)

    with open(sys.argv[1]) as f:
        spec = json.load(f)

    print(f"Setting up Cedar schema attributes for namespace '{namespace}'...")
    setup_cedar_schema(base_url, spec, namespace)

    # Note: Entities are NOT created here. They must already exist in Cedar agent,
    # having been propagated from MySQL via the DDL audit plugin when users/tables
    # were created in MySQL.

    print("Assigning attributes to entities...")

    # Build list of expected entities
    expected_entities = []
    for user in spec["users"]:
        expected_entities.append(("User", user["username"]))

    # Add resources (databases and tables)
    if "resources" in spec:
        for resource in spec["resources"]:
            resource_type = resource.get(
                "type", "Table"
            )  # Default to Table for legacy format
            resource_name = resource["name"]
            expected_entities.append((resource_type, resource_name))

    # Wait for entities to propagate from MySQL (up to 30 seconds)
    print(
        f"Waiting for {len(expected_entities)} entities to propagate from MySQL in namespace '{namespace}'..."
    )
    found_entities = wait_for_entities(
        base_url, expected_entities, namespace, max_wait=30, check_interval=2
    )

    if len(found_entities) < len(expected_entities):
        missing = set(expected_entities) - set(found_entities)
        print(
            f"Warning: {len(missing)} entities not found in Cedar agent after waiting:",
            file=sys.stderr,
        )
        for entity_type, entity_id in missing:
            print(f"  - {entity_type}:{entity_id}", file=sys.stderr)
        print(
            "  This may indicate that the DDL audit plugin is not propagating entities correctly.",
            file=sys.stderr,
        )
        print(
            "  Continuing anyway - attributes will be set when entities appear...",
            file=sys.stderr,
        )

    assign_user_attributes(base_url, spec, namespace)
    assign_database_attributes(base_url, spec, namespace)
    assign_resource_attributes(base_url, spec, namespace)

    print(f"Creating policies for namespace '{namespace}'...")
    for policy in create_cedar_policies(spec, namespace):
        try:
            response = requests.post(f"{base_url}/policies", json=policy)
            if response.status_code in [200, 201]:
                print(f"Created policy: {policy['id']}")
            elif response.status_code == 409:
                # Policy already exists - that's fine for idempotency
                print(f"Policy {policy['id']} already exists (skipping)")
            else:
                # Try PUT instead of POST (some APIs use PUT for create/update)
                try:
                    response = requests.put(
                        f"{base_url}/policies/{policy['id']}", json=policy
                    )
                    if response.status_code in [200, 201, 204]:
                        print(f"Created/updated policy: {policy['id']}")
                    elif response.status_code == 409:
                        print(f"Policy {policy['id']} already exists (skipping)")
                    else:
                        print(
                            f"Warning: Failed to create policy {policy['id']}: {response.status_code}",
                            file=sys.stderr,
                        )
                except Exception as e:
                    print(
                        f"Warning: Failed to create policy {policy['id']}: {e}",
                        file=sys.stderr,
                    )
        except requests.exceptions.RequestException as e:
            print(f"Error creating policy {policy['id']}: {e}", file=sys.stderr)

    print("Setup complete!")
