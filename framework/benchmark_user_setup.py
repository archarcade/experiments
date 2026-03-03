"""
Benchmark user setup for Cedar authorization testing.

This module creates dedicated benchmark users with NO native database privileges.
All access must be granted via Cedar policies only, ensuring that authorization
overhead is properly measured during benchmarks.
"""

import subprocess

# Benchmark user credentials - shared across MySQL and PostgreSQL
BENCHMARK_USER = "cedar_bench"
BENCHMARK_PASSWORD = "benchpass123"


def create_mysql_benchmark_user(
    host: str,
    port: int,
    admin_user: str,
    admin_pass: str,
    db_name: str,
    grant_native_privileges: bool = False,
) -> bool:
    """
    Create MySQL benchmark user with minimal or no native privileges.

    Args:
        host: MySQL host
        port: MySQL port
        admin_user: Admin user (root) for creating new user
        admin_pass: Admin password
        db_name: Database name for benchmarks
        grant_native_privileges: If True, grant full privileges (for baseline comparison).
                                 If False, grant only USAGE (Cedar-only authorization).

    Returns:
        True if successful, False otherwise
    """
    try:
        import mysql.connector

        conn = mysql.connector.connect(
            host=host, port=port, user=admin_user, password=admin_pass
        )
        cursor = conn.cursor()

        # Drop and recreate to ensure clean state
        cursor.execute(f"DROP USER IF EXISTS '{BENCHMARK_USER}'@'%'")
        cursor.execute(
            f"CREATE USER '{BENCHMARK_USER}'@'%' IDENTIFIED BY '{BENCHMARK_PASSWORD}'"
        )

        if grant_native_privileges:
            # For baseline: grant full access so we measure query execution only
            cursor.execute(
                f"GRANT ALL PRIVILEGES ON {db_name}.* TO '{BENCHMARK_USER}'@'%'"
            )
            print(
                f"Created benchmark user '{BENCHMARK_USER}' with FULL native privileges (baseline mode)"
            )
        else:
            cursor.execute(f"GRANT USAGE ON *.* TO '{BENCHMARK_USER}'@'%'")
            print(
                f"Created benchmark user '{BENCHMARK_USER}' with NO native data privileges (Cedar mode)"
            )

        cursor.execute("FLUSH PRIVILEGES")
        cursor.close()
        conn.close()

        return True

    except ImportError:
        print("Error: mysql-connector-python not installed")
        return False
    except Exception as e:
        print(f"Error creating MySQL benchmark user: {e}")
        return False


def create_postgres_benchmark_user(
    host: str,
    port: int,
    db_name: str,
    admin_password: str | None = None,
    grant_native_privileges: bool = False,
    table_owner_role: str | None = None,
) -> bool:
    """
    Create PostgreSQL benchmark user with minimal or no native privileges.

    Args:
        host: PostgreSQL host
        port: PostgreSQL port
        db_name: Database name for benchmarks
        admin_password: Password for postgres superuser
        grant_native_privileges: If True, grant full privileges (for baseline comparison).
                                 If False, grant only CONNECT (Cedar-only authorization).

    Returns:
        True if successful, False otherwise
    """
    import os

    env = os.environ.copy()
    if admin_password:
        env["PGPASSWORD"] = admin_password

    try:
        if grant_native_privileges:
            # For baseline: grant full access
            sql = f"""
DO $$
BEGIN
    -- Drop user if exists (requires handling owned objects)
    IF EXISTS (SELECT FROM pg_catalog.pg_user WHERE usename = '{BENCHMARK_USER}') THEN
        REASSIGN OWNED BY {BENCHMARK_USER} TO postgres;
        DROP OWNED BY {BENCHMARK_USER};
        DROP USER {BENCHMARK_USER};
    END IF;
    
    -- Create user
    CREATE USER {BENCHMARK_USER} WITH PASSWORD '{BENCHMARK_PASSWORD}';
END $$;

-- Grant full access for baseline comparison
GRANT ALL PRIVILEGES ON DATABASE {db_name} TO {BENCHMARK_USER};
GRANT ALL ON SCHEMA public TO {BENCHMARK_USER};
GRANT ALL ON ALL TABLES IN SCHEMA public TO {BENCHMARK_USER};
GRANT ALL ON ALL SEQUENCES IN SCHEMA public TO {BENCHMARK_USER};
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON TABLES TO {BENCHMARK_USER};
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON SEQUENCES TO {BENCHMARK_USER};
"""
            print(
                f"Created benchmark user '{BENCHMARK_USER}' with FULL native privileges (baseline mode)"
            )
        else:
            # For Cedar: grant only connection, no data access
            owner_clause = f"FOR ROLE {table_owner_role}" if table_owner_role else ""

            # CRITICAL: Transfer ownership from table_owner to postgres
            # This is the ONLY way to revoke owner privileges - you can't revoke from owner directly.
            # Without this, the owner (bench_user) retains all privileges on tables they created,
            # and if cedar_bench is a member of bench_user or inherits via PUBLIC, they get access.
            ownership_transfer = ""
            if table_owner_role:
                ownership_transfer = f"""
 -- Transfer ownership of all tables/sequences from {table_owner_role} to postgres
 -- This breaks the owner privilege chain and allows us to control access via Cedar only
 REASSIGN OWNED BY {table_owner_role} TO postgres;
"""

            sql = f"""
DO $$
BEGIN
    -- Drop user if exists
    IF EXISTS (SELECT FROM pg_catalog.pg_user WHERE usename = '{BENCHMARK_USER}') THEN
        REASSIGN OWNED BY {BENCHMARK_USER} TO postgres;
        DROP OWNED BY {BENCHMARK_USER};
        DROP USER {BENCHMARK_USER};
    END IF;
    
    -- Create user
    CREATE USER {BENCHMARK_USER} WITH PASSWORD '{BENCHMARK_PASSWORD}';
END $$;
{ownership_transfer}
 -- Grant ONLY connect (pg_authorization extension will handle access via Cedar)
 GRANT CONNECT ON DATABASE {db_name} TO {BENCHMARK_USER};

 -- Allow name resolution via search_path (keep schema visible), but no table/sequence access.
 GRANT USAGE ON SCHEMA public TO {BENCHMARK_USER};
  
 -- Explicitly revoke any inherited permissions
 REVOKE ALL ON ALL TABLES IN SCHEMA public FROM {BENCHMARK_USER};
 REVOKE ALL ON ALL SEQUENCES IN SCHEMA public FROM {BENCHMARK_USER};

 -- Also revoke from PUBLIC to avoid accidental native access via broad grants
  REVOKE ALL ON ALL TABLES IN SCHEMA public FROM PUBLIC;
  REVOKE ALL ON ALL SEQUENCES IN SCHEMA public FROM PUBLIC;
 ALTER DEFAULT PRIVILEGES {owner_clause} IN SCHEMA public REVOKE ALL ON TABLES FROM PUBLIC;
 ALTER DEFAULT PRIVILEGES {owner_clause} IN SCHEMA public REVOKE ALL ON SEQUENCES FROM PUBLIC;
"""
            print(
                f"Created benchmark user '{BENCHMARK_USER}' with NO native data privileges (Cedar mode)"
            )

        result = subprocess.run(
            [
                "psql",
                "-h",
                host,
                "-p",
                str(port),
                "-U",
                "postgres",
                "-d",
                db_name,
                "-c",
                sql,
            ],
            capture_output=True,
            text=True,
            env=env,
        )

        if result.returncode != 0:
            print(f"Error: {result.stderr}")
            return False

        return True

    except Exception as e:
        print(f"Error creating PostgreSQL benchmark user: {e}")
        return False


def verify_benchmark_user_access(
    host: str,
    port: int,
    db_name: str,
    db_type: str = "mysql",
    table_name: str = "sbtest1",
) -> dict:
    """
    Verify that benchmark user has correct access level.

    For Cedar mode: user should NOT have native access (expect permission denied)
    For baseline mode: user should have full native access

    Returns:
        Dictionary with 'can_connect' and 'can_query' booleans
    """
    result = {"can_connect": False, "can_query": False, "error": None}

    try:
        if db_type == "mysql":
            import mysql.connector

            try:
                conn = mysql.connector.connect(
                    host=host,
                    port=port,
                    user=BENCHMARK_USER,
                    password=BENCHMARK_PASSWORD,
                    database=db_name,
                )
                result["can_connect"] = True

                cursor = conn.cursor()
                try:
                    cursor.execute(f"SELECT 1 FROM {table_name} LIMIT 1")
                    cursor.fetchall()
                    result["can_query"] = True
                except Exception as e:
                    result["can_query"] = False
                    result["query_error"] = str(e)
                finally:
                    cursor.close()
                    conn.close()

            except Exception as e:
                result["error"] = str(e)

        elif db_type in ("postgres", "pgsql"):
            import psycopg2

            try:
                conn = psycopg2.connect(
                    host=host,
                    port=port,
                    user=BENCHMARK_USER,
                    password=BENCHMARK_PASSWORD,
                    dbname=db_name,
                )
                result["can_connect"] = True

                cursor = conn.cursor()
                try:
                    cursor.execute(f"SELECT 1 FROM {table_name} LIMIT 1")
                    cursor.fetchall()
                    result["can_query"] = True
                except Exception as e:
                    result["can_query"] = False
                    result["query_error"] = str(e)
                finally:
                    cursor.close()
                    conn.close()

            except Exception as e:
                result["error"] = str(e)

    except ImportError as e:
        result["error"] = f"Database driver not installed: {e}"

    return result


def get_benchmark_credentials() -> tuple:
    """Return benchmark user credentials as (username, password) tuple."""
    return (BENCHMARK_USER, BENCHMARK_PASSWORD)
