import logging
import time

import mysql.connector
import numpy as np

try:
    import pandas as pd

    HAS_PANDAS = True
except ImportError:
    HAS_PANDAS = False
    pd = None

from tqdm import tqdm

logger = logging.getLogger(__name__)


def run_sql_latency_experiment(
    db_config: dict,
    user: str,
    password: str,
    query: str,
    repetitions: int,
    plugin_timeout_ms: int = 5000,  # Default plugin timeout
    root_user: str | None = None,
    root_password: str | None = None,
) -> "pd.DataFrame":
    """
    Connects to a MySQL database as a specific user and runs a query
    multiple times to measure latency.

    Args:
        db_config: Database connection configuration (host, port, db)
        user: MySQL username for running queries
        password: MySQL password for the user
        query: SQL query to execute
        repetitions: Number of times to run the query
        plugin_timeout_ms: Cedar authorization plugin timeout in milliseconds
        root_user: Optional root username for setting global variables (if user lacks SUPER privilege)
        root_password: Optional root password for setting global variables
    """
    latencies = []
    errors = 0

    connection = None
    try:
        connection = mysql.connector.connect(
            host=db_config["host"],
            port=db_config["port"],
            user=user,
            password=password,
            database=db_config["db"],
            connection_timeout=10,
        )
        cursor = connection.cursor()

        # Set the Cedar authorization plugin timeout (global variable)
        # Try with current user first, then fall back to root if needed
        try:
            cursor.execute(
                f"SET GLOBAL cedar_authorization_timeout = {plugin_timeout_ms}"
            )
            logger.info(
                f"Set global cedar_authorization_timeout to {plugin_timeout_ms}ms"
            )
        except mysql.connector.Error as err:
            # Error 1227 (42000): Access denied; you need SUPER privilege
            if err.errno == 1227:
                logger.info(
                    f"User '{user}' lacks SUPER privilege. Attempting to set timeout as root..."
                )
                # Try connecting as root to set the global variable
                if root_user and root_password:
                    try:
                        root_conn = mysql.connector.connect(
                            host=db_config["host"],
                            port=db_config["port"],
                            user=root_user,
                            password=root_password,
                            connection_timeout=5,
                        )
                        root_cursor = root_conn.cursor()
                        try:
                            root_cursor.execute(
                                f"SET GLOBAL cedar_authorization_timeout = {plugin_timeout_ms}"
                            )
                            logger.info(
                                f"Set global cedar_authorization_timeout to {plugin_timeout_ms}ms using root user"
                            )
                        finally:
                            root_cursor.close()
                            root_conn.close()
                    except mysql.connector.Error as root_err:
                        logger.warning(
                            f"Could not set 'cedar_authorization_timeout' as root: {root_err}. "
                            "Continuing with default timeout."
                        )
                else:
                    logger.warning(
                        f"Could not set 'cedar_authorization_timeout' global variable: {err}. "
                        "Root credentials not provided. Continuing with default timeout."
                    )
            # Error 1193 (HY000): Unknown system variable (plugin not installed)
            elif err.errno == 1193:
                logger.warning(
                    f"Could not set 'cedar_authorization_timeout' global variable: {err}. "
                    "Plugin may not be installed or variable name may differ. Continuing with default timeout."
                )
            else:
                logger.warning(
                    f"Could not set 'cedar_authorization_timeout' global variable: {err}. "
                    "Continuing with default timeout."
                )

        logger.info(f"Running {repetitions} queries for user '{user}'...")

        # Use tqdm for progress bar with detailed information
        pbar = tqdm(
            total=repetitions,
            desc="Running queries",
            unit="query",
            leave=False,
            bar_format=(
                "{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}, {rate_fmt}]"
            ),
        )

        for i in range(repetitions):
            start_time = time.monotonic()
            try:
                cursor.execute(query)
                # Fetch results to ensure query is complete
                cursor.fetchall()
                end_time = time.monotonic()
                latency_ms = (end_time - start_time) * 1000
                # Store latency in milliseconds
                latencies.append(latency_ms)

                # Update progress bar with current stats
                if latencies:
                    avg_latency = np.mean(latencies)
                    pbar.set_postfix({"avg": f"{avg_latency:.1f}ms", "errors": errors})

                pbar.update(1)
            except mysql.connector.Error as err:
                errors += 1
                pbar.set_postfix({"errors": errors})
                logger.warning(
                    f"Query failed for user '{user}' (repetition {i + 1}): {err}"
                )
                pbar.update(1)

        pbar.close()

    except mysql.connector.Error as err:
        logger.error(f"Database connection failed for user '{user}': {err}")
        # If connection fails, all repetitions are considered errors
        errors = repetitions
    finally:
        if connection and connection.is_connected():
            cursor.close()
            connection.close()

    # If all queries failed, fill with NaN
    if not latencies:
        latencies = [np.nan] * repetitions

    if not HAS_PANDAS:
        raise ImportError("pandas is required for run_sql_latency_experiment")
    df = pd.DataFrame(latencies, columns=["latency_ms"])
    df["user"] = user
    df["query"] = query
    df["errors"] = errors
    df["successful_queries"] = len(latencies)

    return df


def analyze_latency_results(df: "pd.DataFrame") -> dict:
    """Analyzes a DataFrame of latency results to get key statistics."""
    if df.empty or df["latency_ms"].isnull().all():
        return {
            "count": 0,
            "errors": df["errors"].sum() if not df.empty else 0,
            "median": np.nan,
            "p95": np.nan,
            "p99": np.nan,
            "mean": np.nan,
            "std": np.nan,
        }

    latencies = df["latency_ms"].dropna()

    stats = {
        "count": len(latencies),
        "errors": df["errors"].iloc[0],
        "median": np.median(latencies),
        "p95": np.percentile(latencies, 95),
        "p99": np.percentile(latencies, 99),
        "mean": np.mean(latencies),
        "std": np.std(latencies),
    }
    return stats
