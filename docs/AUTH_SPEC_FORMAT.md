# Authorization Specification Format

## Overview

The authorization specification format supports a **flat resources list** where each resource can be of different types (Database, Table, Procedure, etc.). This design is:

- **Extensible**: Easy to add new resource types in the future
- **Flexible**: Resources are independent and can have different attributes
- **Simple**: Flat structure is easier to understand and process

## Structure

```json
{
  "metadata": {
    "name": "workload_name",
    "description": "Description",
    "version": "2.0"
  },
  "resources": [
    {
      "type": "Database",
      "name": "database_name",
      "attributes": {...}
    },
    {
      "type": "Table",
      "name": "database.table_name",
      "attributes": {...},
      "schema": {...},
      "sample_data": [...]
    }
  ],
  "users": [...],
  "policies": [...],
  "cedar_plugins": {...}
}
```

## Resource Types

### Database

Represents a database entity with attributes.

```json
{
  "type": "Database",
  "name": "abac_test",
  "attributes": {
    "security_level": "high",
    "compliance_tier": "tier1",
    "environment": "production"
  }
}
```

**Fields:**
- `type`: Must be `"Database"`
- `name`: Database name (e.g., `"abac_test"`)
- `attributes`: Dictionary of database-level attributes

### Table

Represents a table entity with attributes, schema, and optional sample data.

```json
{
  "type": "Table",
  "name": "abac_test.employees",
  "attributes": {
    "data_classification": "public",
    "table_type": "hr_data"
  },
  "schema": {
    "columns": [
      {"name": "id", "type": "INT", "constraints": "PRIMARY KEY"},
      {"name": "name", "type": "VARCHAR(100)", "constraints": ""},
      {"name": "department", "type": "VARCHAR(50)", "constraints": ""}
    ]
  },
  "sample_data": [
    {"id": 1, "name": "Alice", "department": "HR"},
    {"id": 2, "name": "Bob", "department": "IT"}
  ]
}
```

**Fields:**
- `type`: Must be `"Table"`
- `name`: Fully qualified table name (e.g., `"database.table"`)
- `attributes`: Dictionary of table-level attributes
- `schema` (optional): Table schema definition
  - `columns`: Array of column definitions
    - `name`: Column name
    - `type`: SQL data type
    - `constraints`: Column constraints (e.g., `"PRIMARY KEY"`, `"NOT NULL"`)
- `sample_data` (optional): Array of sample rows for testing

### Future Resource Types

The format is designed to support additional resource types:

```json
{
  "type": "Procedure",
  "name": "database.procedure_name",
  "attributes": {...}
}
```

```json
{
  "type": "View",
  "name": "database.view_name",
  "attributes": {...}
}
```

## Complete Example

```json
{
  "metadata": {
    "name": "abac_test_workload_extended",
    "description": "Authorization specification with databases and tables",
    "version": "2.0"
  },
  "resources": [
    {
      "type": "Database",
      "name": "abac_test",
      "attributes": {
        "security_level": "high",
        "compliance_tier": "tier1"
      }
    },
    {
      "type": "Table",
      "name": "abac_test.employees",
      "attributes": {
        "data_classification": "public",
        "table_type": "hr_data"
      },
      "schema": {
        "columns": [
          {"name": "id", "type": "INT", "constraints": "PRIMARY KEY"},
          {"name": "name", "type": "VARCHAR(100)", "constraints": ""},
          {"name": "department", "type": "VARCHAR(50)", "constraints": ""}
        ]
      },
      "sample_data": [
        {"id": 1, "name": "Alice", "department": "HR"}
      ]
    },
    {
      "type": "Table",
      "name": "abac_test.projects",
      "attributes": {
        "data_classification": "private",
        "table_type": "project_data"
      },
      "schema": {
        "columns": [
          {"name": "id", "type": "INT", "constraints": "PRIMARY KEY"},
          {"name": "name", "type": "VARCHAR(100)", "constraints": ""}
        ]
      }
    }
  ],
  "users": [
    {
      "username": "user_alice",
      "password": "",
      "host": "%",
      "attributes": {
        "user_role": "manager",
        "clearance_level": "top_secret"
      }
    }
  ],
  "policies": [
    {
      "id": "database_level_access",
      "privileges": ["SELECT"],
      "condition": "principal.clearance_level == 'top_secret' AND resource.security_level == 'high'",
      "description": "Top secret users can access high security databases"
    },
    {
      "id": "manager_sensitive_access",
      "privileges": ["SELECT"],
      "condition": "principal.user_role == 'manager' AND resource.data_classification == 'sensitive'",
      "description": "Managers can read sensitive data"
    }
  ],
  "cedar_plugins": {
    "ddl_audit": {
      "enabled": true,
      "cedar_url": "http://localhost:8280",
      "timeout_ms": 5000
    },
    "cedar_authorization": {
      "enabled": true,
      "authorization_url": "http://localhost:8280/v1/is_authorized",
      "timeout_ms": 5000
    }
  }
}
```

## Legacy Format Support

The framework also supports the legacy format (without `type` field):

```json
{
  "resources": [
    {
      "name": "abac_test.employees",
      "attributes": {
        "data_classification": "public"
      }
    }
  ]
}
```

When `type` is not specified, it defaults to `"Table"`.

## Policy Conditions

Policies can reference attributes from both databases and tables:

### Database-Level Policy

```json
{
  "id": "high_security_db_access",
  "privileges": ["SELECT"],
  "condition": "principal.clearance_level == 'top_secret' AND resource.security_level == 'high'"
}
```

This policy applies to all resources (databases and tables) with `security_level == 'high'`.

### Table-Level Policy

```json
{
  "id": "public_data_access",
  "privileges": ["SELECT"],
  "condition": "principal.user_role == 'intern' AND resource.data_classification == 'public'"
}
```

This policy applies specifically to tables with `data_classification == 'public'`.

### Combined Policy

```json
{
  "id": "production_db_sensitive_tables",
  "privileges": ["SELECT"],
  "condition": "principal.clearance_level == 'secret' AND resource.environment == 'production' AND resource.data_classification == 'sensitive'"
}
```

This policy evaluates both database-level and table-level attributes.

## Schema Definition

### Minimal Schema

```json
{
  "schema": {
    "columns": [
      {"name": "id", "type": "INT", "constraints": "PRIMARY KEY"}
    ]
  }
}
```

### Full Schema

```json
{
  "schema": {
    "columns": [
      {"name": "id", "type": "INT", "constraints": "PRIMARY KEY AUTO_INCREMENT"},
      {"name": "name", "type": "VARCHAR(200)", "constraints": "NOT NULL"},
      {"name": "email", "type": "VARCHAR(100)", "constraints": "UNIQUE"},
      {"name": "created_at", "type": "TIMESTAMP", "constraints": "DEFAULT CURRENT_TIMESTAMP"},
      {"name": "status", "type": "ENUM('active','inactive')", "constraints": "DEFAULT 'active'"}
    ]
  }
}
```

## Sample Data

Sample data is used for:
- Initial database population
- Testing authorization policies
- Reproducible experiments

```json
{
  "sample_data": [
    {"id": 1, "name": "Alice", "department": "HR", "status": "active"},
    {"id": 2, "name": "Bob", "department": "IT", "status": "active"},
    {"id": 3, "name": "Charlie", "department": "Finance", "status": "inactive"}
  ]
}
```

**Important:** Sample data must match the schema columns.

## Benefits of This Design

### 1. Extensibility

Adding new resource types is trivial:

```json
{
  "type": "Procedure",
  "name": "database.proc_name",
  "attributes": {"procedure_type": "admin"}
}
```

### 2. Independence

Resources are independent - no nested structures to navigate:

```python
# Get all databases
databases = [r for r in spec['resources'] if r['type'] == 'Database']

# Get all tables
tables = [r for r in spec['resources'] if r['type'] == 'Table']

# Get specific resource
employee_table = next(r for r in spec['resources'] if r['name'] == 'abac_test.employees')
```

### 3. Flexibility

Resources can have any attributes without affecting others:

```json
{
  "type": "Database",
  "name": "analytics_db",
  "attributes": {
    "custom_field_1": "value1",
    "custom_field_2": "value2",
    "any_attribute": "any_value"
  }
}
```

### 4. Simplicity

No complex nested structures - just a flat list of resources.

## Validation Rules

1. **Resources must have:**
   - `name` field (required)
   - `type` field (optional, defaults to "Table")

2. **Table resources should have:**
   - Fully qualified name (e.g., `"database.table"`)
   - Optional: `schema`, `sample_data`

3. **Database resources should have:**
   - Simple name (e.g., `"abac_test"`)
   - Optional: `attributes`

4. **Attributes:**
   - Must be a dictionary
   - Keys and values should be strings for policy evaluation

## Migration from Nested Format

If you have a nested format (databases containing tables), convert it to flat:

**Before (Nested):**
```json
{
  "databases": [
    {
      "name": "db1",
      "tables": [
        {"name": "table1", "attributes": {...}},
        {"name": "table2", "attributes": {...}}
      ]
    }
  ]
}
```

**After (Flat):**
```json
{
  "resources": [
    {
      "type": "Database",
      "name": "db1",
      "attributes": {...}
    },
    {
      "type": "Table",
      "name": "db1.table1",
      "attributes": {...}
    },
    {
      "type": "Table",
      "name": "db1.table2",
      "attributes": {...}
    }
  ]
}
```

## See Also

- `framework/auth_spec_example.json` — Legacy format example
- `framework/auth_spec_extended_example.json` — Extended format example
- `framework/sql_generator.py` — SQL generation from auth spec
- `auth_spec.json` — Active authorization specification

