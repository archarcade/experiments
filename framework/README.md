# Authorization-Aware Benchmarking Framework

## Overview

This framework provides a pure-Python tool for comparing performance between baseline MySQL (GRANT-based RBAC) and Cedar MySQL (ABAC with Cedar policies). The framework eliminates shell script dependencies and provides a clean, maintainable architecture optimized for performance.

**Key Features:**
- ✅ Pure Python implementation (no bash scripts)
- ✅ Pre-computed workload generation
- ✅ Connection pooling for efficient database access
- ✅ Configuration-driven with YAML/JSON support
- ✅ Reproducible experiments with seed-based generation
- ✅ Authorization-aware query generation

## Documentation

- **[Architecture Guide](../ARCHITECTURE.md)**: Complete architecture documentation
- **[Dynamic Architecture](../DYNAMIC_ARCHITECTURE.md)**: New dynamic SQL generation architecture ⭐
- **[Migration Guide](../MIGRATION_GUIDE.md)**: Migrate from hardcoded SQL to dynamic architecture
- **[API Reference](../API_REFERENCE.md)**: Detailed API documentation
- **[Configuration Guide](../CONFIG_GUIDE.md)**: Configuration file reference
- **[Quick Start Guide](../QUICKSTART.md)**: Getting started tutorial

## The Challenge: Paradigm Shift in Authorization

Comparing original MySQL (GRANT-based RBAC) with modified MySQL (Cedar ABAC) presents a fundamental challenge:

- **Original MySQL**: Uses `GRANT SELECT ON table TO user` statements
- **Modified MySQL**: Uses Cedar policies with attributes (user_role, clearance_level, data_classification)

**The Problem**: We cannot simply run the same SQL queries - the authorization setup is completely different. We need equivalent authorization semantics (same users get same access) but implemented differently.

## Solution: Authorization Requirement Specification Framework

We propose a **declarative authorization requirement specification** that can be translated to both systems:

1. **Define authorization requirements** in a neutral format (JSON/YAML)
2. **Translate to GRANT statements** for original MySQL
3. **Translate to Cedar policies + attributes** for modified MySQL
4. **Run identical workloads** against both systems
5. **Compare performance and correctness**

## Framework Specification

### Authorization Requirement Format (JSON)

**Note**: Policies are defined separately based on attribute conditions, not per-user. This matches the ABAC paradigm where policies evaluate attributes rather than being tied to specific users.

See `auth_spec_example.json` for a complete example.

### Translators

- **`translate_to_grants.py`**: Converts authorization spec to MySQL SQL statements (CREATE USER + GRANT)
  - `--mode users`: Generate CREATE USER statements only
  - `--mode grants`: Generate GRANT statements only (default)
  - `--mode setup`: Generate complete setup SQL (CREATE USER + GRANT)
- **`translate_to_cedar.py`**: Converts authorization spec to Cedar policies and attributes
- **`run_benchmark.py`**: Runs identical workloads against both MySQL versions

### Usage Workflow (Pure Python CLI)

1. Define authorization requirements: `experiments/framework/auth_spec_example.json`
2. Setup baseline MySQL (users + grants):
   ```bash
   uv run python3 experiments/cli.py setup-baseline experiments/framework/auth_spec_example.json
   ```
3. Setup Cedar agent (attributes + policies):
   ```bash
   uv run python3 experiments/cli.py setup-cedar experiments/framework/auth_spec_example.json
   ```
4. Generate workload:
   ```bash
   uv run python3 experiments/cli.py generate-workload experiments/framework/auth_spec_example.json \
     --output ./experiments/workloads/demo --seed 42 --queries-per-combo 10
   ```
5. Run benchmark:
   ```bash
   uv run python3 experiments/cli.py run-benchmark ./experiments/workloads/demo --iterations 100
   ```
6. Analyze results:
   ```bash
   uv run python3 experiments/cli.py analyze-results ./experiments/results
   ```

### Integration with Existing Benchmarking Tools

This framework can be integrated with **sysbench** or other tools:

1. **Use sysbench for workload generation** (concurrency, duration, etc.)
2. **Use this framework for authorization setup** (before running sysbench)
3. **Run sysbench against both MySQL instances** (baseline and Cedar)
4. **Compare sysbench results** (QPS, latency, etc.)

**Example**:
```bash
# Setup authorization for baseline MySQL (includes CREATE USER statements)
python3 translate_to_grants.py auth_spec.json --mode setup | mysql -u root -p baseline_db

# Or if users already exist, just grant privileges:
python3 translate_to_grants.py auth_spec.json --mode grants | mysql -u root -p baseline_db

# Setup authorization for Cedar MySQL
python3 translate_to_cedar.py auth_spec.json

# Run sysbench against baseline
sysbench oltp_read_write --mysql-host=localhost --mysql-port=3306 ...

# Run sysbench against Cedar
sysbench oltp_read_write --mysql-host=localhost --mysql-port=3307 ...
```

### Data Generation and Query Generation

The framework includes **schema-aware** data and query generation capabilities for repeatable, conflict-free experiments:

#### Data Generator (`data_generator.py`)

Generates fake test data using Faker library based on table schemas:

```python
from framework.data_generator import DataGenerator

# Initialize with seed for reproducibility
gen = DataGenerator(seed=42)

# Schema-based generation
schema = {
    "columns": [
        {"name": "id", "type": "INT", "constraints": "PRIMARY KEY"},
        {"name": "name", "type": "VARCHAR(100)", "constraints": ""},
        {"name": "department", "type": "VARCHAR(50)", "constraints": ""}
    ]
}

# Generate record from schema
record = gen.generate_record_from_schema('abac_test.employees', schema, id_value=1)
# Result: {"id": 1, "name": "John Doe", "department": "Engineering"}

# Convert to SQL
sql = gen.to_sql_insert('abac_test.employees', [record])
```

**Features**:
- ✅ **Schema-aware**: Generates data based on column names, types, and constraints
- ✅ **Intelligent inference**: Infers data types from column names (e.g., 'name' → person name, 'department' → business name)
- ✅ **Faker integration**: Uses Faker's built-in methods (no custom providers needed)
- ✅ **Unique ID generation**: Tracks IDs per table to avoid conflicts
- ✅ **Reproducible**: Seed-based generation for consistent results

#### Query Generator (`query_generator.py`)

Generates dynamic SQL queries based on table schemas:

```python
from framework.query_generator import QueryGenerator

# Initialize with auth_spec for schema awareness
qgen = QueryGenerator(seed=42, auth_spec=auth_spec)

# Generate single-statement queries (no overhead)
insert_query = qgen.generate_insert_query('abac_test.employees', id_value=1000)
# Result: "INSERT INTO abac_test.employees (id, name, department) VALUES (1000, 'John Doe', 'Engineering');"

update_query = qgen.generate_update_query('abac_test.employees', id_value=1)
# Result: "UPDATE abac_test.employees SET name = 'Jane Smith' WHERE id = 1;"

delete_query = qgen.generate_delete_query('abac_test.employees', id_value=1)
# Result: "DELETE FROM abac_test.employees WHERE id = 1;"

select_query = qgen.generate_select_query('abac_test.employees', limit=10)
# Result: "SELECT * FROM abac_test.employees LIMIT 10;"

join_query = qgen.generate_select_query('abac_test.employees', 
                                         with_join='abac_test.projects')
# Result: "SELECT * FROM abac_test.employees e JOIN abac_test.projects p ON e.id = p.id LIMIT 1;"
```

**Features**:
- ✅ **Schema-aware**: Uses table schemas to generate appropriate queries
- ✅ **Single-statement**: Each query is a single SQL statement (no cleanup/ensure-exists overhead)
- ✅ **Dynamic data**: Uses DataGenerator to create realistic test data
- ✅ **ID-aware**: Accepts `id_value` parameter for stateful workload generation
- ✅ **Supports all SQL operations**: INSERT, UPDATE, DELETE, SELECT (with JOIN support)

#### Test Data Manager (`test_data_manager.py`)

Manages test data lifecycle for experiments:

```python
from framework.test_data_manager import TestDataManager

config = {
    'host': 'localhost',
    'port': 3306,
    'user': 'root',
    'password': '',
    'database': 'abac_test'
}

with TestDataManager(config, seed=42) as manager:
    # Setup test data
    ids = manager.setup_test_data('abac_test.employees', count=1000)
    
    # Run experiments...
    
    # Cleanup
    manager.cleanup_test_data('abac_test.employees', ids)
```

**Features**:
- ✅ Automatic ID tracking
- ✅ Bulk data setup/cleanup
- ✅ State persistence
- ✅ Context manager support

### Workload Generation Details
The CLI pre-computes queries based on grants mapping using **stateful generation**:
1. Loads authorization spec and initializes per-table state from `sample_data`
2. Evaluates policies -> per-user grants on resources
3. Enumerates valid (user, action, table) combinations
4. Generates N queries per combination using `QueryGenerator`:
   - **INSERT**: Uses and advances `next_id` (adds to existing_ids)
   - **UPDATE/DELETE**: Selects from `existing_ids` (no overhead queries)
   - **SELECT**: Simple queries or JOINs
5. Stores `workload.json` with queries + metadata

**Key Benefits:**
- **No overhead queries**: Each query is a single SQL statement
- **Efficient execution**: No unnecessary INSERT IGNORE or DELETE cleanup
- **Stateful tracking**: Maintains per-table ID state throughout generation

### Benefits

✅ **Fair Comparison**: Same authorization semantics, different implementations  
✅ **Reproducible**: JSON specification ensures consistency  
✅ **Extensible**: Easy to add new authorization requirements  
✅ **Tool-Agnostic**: Works with sysbench, TPC-C, custom workloads  
✅ **Correctness Verification**: Can verify both systems enforce same access rules  
✅ **Schema-Aware Data Generation**: Faker-based data generation based on table schemas  
✅ **Schema-Aware Query Generation**: Dynamic queries based on table schemas, no hardcoded values  
✅ **Stateful Workload Generation**: Efficient single-statement queries with no overhead  
✅ **Repeatable Setup**: Seed-based reproducibility for all data generation  
✅ **Pure Python**: No shell script dependencies, easier to maintain and debug  
✅ **Pre-computed Workloads**: Generate once, reuse many times for faster benchmarks  
✅ **Connection Pooling**: Efficient database connection management  
✅ **Configuration-Driven**: YAML/JSON configs with environment variable support

## Architecture Overview

The framework consists of several key components:

- **CLI (`cli.py`)**: Unified command-line interface
- **Configuration System (`config.py`)**: YAML/JSON config management
- **Workload Generator (`workload_generator.py`)**: Pre-computes queries from auth spec
- **Connection Pool (`connection_pool.py`)**: Efficient MySQL connection management
- **Benchmark Runner (`benchmark_runner.py`)**: Executes workloads and measures latency
- **Results Analyzer (`analyzer.py`)**: Computes statistics and generates reports

See [ARCHITECTURE.md](../ARCHITECTURE.md) for complete architecture documentation.

