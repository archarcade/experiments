#!/usr/bin/env python3
"""
Semantic correctness testing for failure scenarios.

Tests that the authorization system maintains correctness guarantees
even when the agent is unavailable or returns unexpected responses.

This is critical for demonstrating fail-closed behavior and monotonicity.
"""

from __future__ import annotations

import json
import time
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Any

from .config import Config
from .connection_pool import ConnectionPool
from .workload_generator import Query


@dataclass
class SecurityTestCase:
    """A test case for security properties under failures."""

    name: str
    description: str
    queries: list[Query]  # Queries that should be tested
    expected_denied: bool  # Whether these queries should be denied
    failure_scenario: str  # Which failure scenario to test

    def to_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "description": self.description,
            "queries": [
                {"user": q.user, "action": q.action, "table": q.table}
                for q in self.queries
            ],
            "expected_denied": self.expected_denied,
            "failure_scenario": self.failure_scenario,
        }


@dataclass
class SecurityTestResult:
    """Result of a security test under failure."""

    test_case: SecurityTestCase
    results: list[dict[str, Any]]  # Per-query results
    all_correct: bool  # Whether all queries behaved as expected
    violations: list[str]  # Description of any violations

    def to_dict(self) -> dict[str, Any]:
        return {
            "test_case": self.test_case.to_dict(),
            "results": self.results,
            "all_correct": self.all_correct,
            "violations": self.violations,
        }


@dataclass
class MonotonicityTest:
    """Test for monotonicity: if agent allows, then baseline should allow."""

    query: Query
    baseline_authorized: bool
    cedar_authorized: bool
    monotonic: bool  # cedar_authorized <= baseline_authorized
    violation_reason: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "query": {
                "user": self.query.user,
                "action": self.query.action,
                "table": self.query.table,
            },
            "baseline_authorized": self.baseline_authorized,
            "cedar_authorized": self.cedar_authorized,
            "monotonic": self.monotonic,
            "violation_reason": self.violation_reason,
        }


class FailureSemanticsTester:
    """
    Tests semantic correctness of authorization under failure scenarios.

    Validates:
    - Fail-closed behavior: should-deny queries are denied when agent fails
    - Monotonicity: Cedar never allows more than baseline
    - Consistency: Same queries get same results under same conditions
    """

    def __init__(self, config: Config):
        """
        Initialize failure semantics tester.

        Args:
            config: Experiment configuration
        """
        self.config = config
        self.baseline_pool = ConnectionPool(self.config.databases["baseline"])
        self.cedar_pool = ConnectionPool(self.config.databases["cedar"])

        # For per-query user support
        self._user_pools: dict[tuple[str, str], ConnectionPool] = {}
        self._user_passwords: dict[str, str] = self._load_user_passwords()

        # Toxiproxy client for failure simulation
        self.toxiproxy = None
        if (
            hasattr(self.config, "failure_tests")
            and self.config.failure_tests
            and self.config.failure_tests.proxy.enabled
        ):
            from .toxiproxy_client import ToxiproxyClient

            self.toxiproxy = ToxiproxyClient(
                self.config.failure_tests.proxy.control_api
            )

    def _load_user_passwords(self) -> dict[str, str]:
        """Load user passwords from auth spec."""
        try:
            with open(self.config.auth_spec_path) as f:
                spec = json.load(f)
            return {u["username"]: u.get("password", "") for u in spec.get("users", [])}
        except Exception:
            return {}

    def _get_pool_for_query(self, system_name: str, query_user: str) -> ConnectionPool:
        """Get or create a connection pool for a specific user."""
        if (
            not self.config.benchmark.use_query_user
            or query_user not in self._user_passwords
        ):
            return self.baseline_pool if system_name == "baseline" else self.cedar_pool

        key = (system_name, query_user)
        if key in self._user_pools:
            return self._user_pools[key]

        base_cfg = self.config.databases[system_name]
        from .config import DatabaseConfig

        user_cfg = DatabaseConfig(
            name=f"{system_name}_{query_user}_semantics",
            host=base_cfg.host,
            port=base_cfg.port,
            user=query_user,
            password=self._user_passwords[query_user],
            database=base_cfg.database,
            pool_size=1,  # Small pool for semantics tests
        )
        pool = ConnectionPool(user_cfg)
        self._user_pools[key] = pool
        return pool

    def _execute_query_with_authorization_check(
        self, system_name: str, query: Query
    ) -> tuple[bool, float, str | None]:
        """
        Execute a query and check if it was authorized.

        Returns:
            Tuple of (authorized: bool, latency_ms: float, error_msg: Optional[str])
        """
        pool = self._get_pool_for_query(system_name, query.user)
        start_time = time.perf_counter()

        authorized = False
        error_msg = None

        try:
            with pool.get_connection() as conn:
                cur = conn.cursor()

                # Execute the query
                cur.execute(query.sql)

                # Consume results
                if cur.with_rows:
                    cur.fetchall()

                conn.commit()
                authorized = True

                cur.close()

        except Exception as e:
            error_msg = str(e)

            # Check if this was an authorization denial
            if "denied" in error_msg.lower() or "1142" in error_msg:
                authorized = False
            else:
                # Other error - assume authorized but execution failed
                authorized = True

        end_time = time.perf_counter()
        latency_ms = (end_time - start_time) * 1000.0

        return authorized, latency_ms, error_msg

    @contextmanager
    def _simulate_failure(self, scenario: str):
        """Context manager to simulate failure scenarios using Toxiproxy."""
        if not self.toxiproxy or scenario == "normal":
            yield
            return

        proxy_name = self.config.failure_tests.proxy.name

        try:
            if scenario == "agent_unavailable":
                self.toxiproxy.set_unavailable(proxy_name)
            elif scenario == "network_timeout":
                # 10 second latency is effectively a timeout for most clients
                self.toxiproxy.set_latency(proxy_name, latency_ms=10000)
            elif scenario == "malformed_response":
                # "limit_data" closes the connection after a certain number of bytes
                # 50 bytes is enough to get HTTP headers but truncate the JSON body
                self.toxiproxy.add_toxic(
                    proxy_name,
                    "limit_data",
                    "malformed_toxic",
                    attributes={"bytes": 50},
                )

            yield
        finally:
            self.toxiproxy.reset_proxy(proxy_name)

    def _set_mysql_auth_url(self, url: str) -> str | None:
        """Update cedar_authorization_url in MySQL and return original value."""
        try:
            import mysql.connector

            base_cfg = self.config.databases["cedar"]
            conn = mysql.connector.connect(
                host=base_cfg.host,
                port=base_cfg.port,
                user="root",
                password=base_cfg.password,
                connection_timeout=5,
            )
            cur = conn.cursor()

            # Get original
            cur.execute("SHOW VARIABLES LIKE 'cedar_authorization_url'")
            res = cur.fetchone()
            original = res[1] if res else None

            # Set new
            cur.execute(f"SET GLOBAL cedar_authorization_url = '{url}'")

            cur.close()
            conn.close()
            return original
        except Exception as e:
            print(f"Warning: Failed to update MySQL auth URL: {e}")
            return None

    def test_fail_closed_behavior(
        self, should_deny_queries: list[Query], failure_scenarios: list[str] = None
    ) -> dict[str, Any]:
        """
        Test that should-deny queries are properly denied under failures.

        Args:
            should_deny_queries: Queries that should be denied by policy
            failure_scenarios: List of failure scenarios to test

        Returns:
            Test results for fail-closed behavior
        """
        if failure_scenarios is None:
            failure_scenarios = [
                "agent_unavailable",
                "network_timeout",
                "malformed_response",
            ]

        results = {
            "test_type": "fail_closed",
            "scenarios": {},
            "summary": {},
        }

        # If Toxiproxy is enabled, we need to point MySQL to the proxy
        original_url = None
        if self.toxiproxy:
            proxy_cfg = self.config.failure_tests.proxy
            # The URL MySQL uses to reach the proxy (from inside container)
            # Typically this is http://toxiproxy:8182/v1/is_authorized
            proxy_url = f"http://toxiproxy:{proxy_cfg.listen_port}/v1/is_authorized"
            original_url = self._set_mysql_auth_url(proxy_url)

        try:
            for scenario in failure_scenarios:
                click.echo(f"Testing fail-closed under {scenario}...")

                scenario_results = []

                with self._simulate_failure(scenario):
                    for query in should_deny_queries:
                        # Test on cedar (where authorization should kick in)
                        cedar_auth, cedar_time, cedar_error = (
                            self._execute_query_with_authorization_check("cedar", query)
                        )

                        # Test on baseline (should always allow for comparison)
                        # We don't apply failure to baseline
                        baseline_auth, baseline_time, baseline_error = (
                            self._execute_query_with_authorization_check(
                                "baseline", query
                            )
                        )

                        result = {
                            "query": {
                                "user": query.user,
                                "action": query.action,
                                "table": query.table,
                            },
                            "scenario": scenario,
                            "cedar_authorized": cedar_auth,
                            "cedar_latency_ms": cedar_time,
                            "cedar_error": cedar_error,
                            "baseline_authorized": baseline_auth,
                            "baseline_latency_ms": baseline_time,
                            "baseline_error": baseline_error,
                            "fail_closed_violation": cedar_auth,  # Should be False for should-deny queries
                        }
                        scenario_results.append(result)

                results["scenarios"][scenario] = scenario_results
        finally:
            if original_url:
                self._set_mysql_auth_url(original_url)

        # Generate summary
        results["summary"] = self._summarize_fail_closed_results(results)

        return results

    def _summarize_fail_closed_results(self, results: dict[str, Any]) -> dict[str, Any]:
        """Summarize fail-closed test results."""
        summary = {
            "total_queries_tested": 0,
            "scenarios_tested": len(results.get("scenarios", {})),
            "violations": [],
        }

        for scenario, scenario_results in results.get("scenarios", {}).items():
            for result in scenario_results:
                summary["total_queries_tested"] += 1

                if result["fail_closed_violation"]:
                    violation = {
                        "scenario": scenario,
                        "query": result["query"],
                        "message": f"Query was authorized when it should have been denied under {scenario}",
                    }
                    summary["violations"].append(violation)

        summary["total_violations"] = len(summary["violations"])
        summary["pass"] = summary["total_violations"] == 0

        return summary

    def test_monotonicity(self, test_queries: list[Query]) -> dict[str, Any]:
        """
        Test monotonicity: Cedar should never allow more than baseline.

        This is a fundamental security property.

        Args:
            test_queries: Queries to test for monotonicity

        Returns:
            Monotonicity test results
        """
        results = {
            "test_type": "monotonicity",
            "monotonicity_tests": [],
            "summary": {},
        }

        click.echo(f"Testing monotonicity on {len(test_queries)} queries...")

        for query in test_queries:
            # Test on baseline
            baseline_auth, baseline_time, baseline_error = (
                self._execute_query_with_authorization_check("baseline", query)
            )

            # Test on cedar
            cedar_auth, cedar_time, cedar_error = (
                self._execute_query_with_authorization_check("cedar", query)
            )

            # Check monotonicity: cedar_auth <= baseline_auth
            monotonic = not (
                cedar_auth and not baseline_auth
            )  # Not (cedar allows but baseline denies)

            violation_reason = None
            if not monotonic:
                violation_reason = "Cedar authorized but baseline denied"

            test_result = MonotonicityTest(
                query=query,
                baseline_authorized=baseline_auth,
                cedar_authorized=cedar_auth,
                monotonic=monotonic,
                violation_reason=violation_reason,
            )

            results["monotonicity_tests"].append(test_result.to_dict())

        # Generate summary
        results["summary"] = self._summarize_monotonicity_results(results)

        return results

    def _summarize_monotonicity_results(
        self, results: dict[str, Any]
    ) -> dict[str, Any]:
        """Summarize monotonicity test results."""
        tests = results.get("monotonicity_tests", [])
        violations = [t for t in tests if not t["monotonic"]]

        summary = {
            "total_tests": len(tests),
            "violations": len(violations),
            "monotonic": len(violations) == 0,
            "violation_details": violations,
        }

        return summary

    def test_consistency_under_failures(
        self, test_queries: list[Query], failure_scenarios: list[str] = None
    ) -> dict[str, Any]:
        """
        Test that the same queries produce consistent results under failure scenarios.

        Args:
            test_queries: Queries to test
            failure_scenarios: Failure scenarios to test

        Returns:
            Consistency test results
        """
        if failure_scenarios is None:
            failure_scenarios = ["normal", "agent_unavailable", "network_timeout"]

        results = {
            "test_type": "consistency",
            "scenarios": {},
            "summary": {},
        }

        click.echo(f"Testing consistency under {len(failure_scenarios)} scenarios...")

        # If Toxiproxy is enabled, we need to point MySQL to the proxy
        original_url = None
        if self.toxiproxy:
            proxy_cfg = self.config.failure_tests.proxy
            proxy_url = f"http://toxiproxy:{proxy_cfg.listen_port}/v1/is_authorized"
            original_url = self._set_mysql_auth_url(proxy_url)

        try:
            for scenario in failure_scenarios:
                click.echo(f"  Testing scenario: {scenario}")
                scenario_results = []

                with self._simulate_failure(scenario):
                    for query in test_queries:
                        auth, latency, error = (
                            self._execute_query_with_authorization_check("cedar", query)
                        )

                        result = {
                            "query": {
                                "user": query.user,
                                "action": query.action,
                                "table": query.table,
                            },
                            "scenario": scenario,
                            "authorized": auth,
                            "latency_ms": latency,
                            "error": error,
                        }
                        scenario_results.append(result)

                results["scenarios"][scenario] = scenario_results
        finally:
            if original_url:
                self._set_mysql_auth_url(original_url)

        # Check consistency across scenarios
        results["summary"] = self._check_consistency(results)

        return results

    def _check_consistency(self, results: dict[str, Any]) -> dict[str, Any]:
        """Check for consistency violations across scenarios."""
        scenarios = results.get("scenarios", {})
        if len(scenarios) < 2:
            return {"consistent": True, "violations": []}

        # Get queries from first scenario
        next(iter(scenarios.values()))
        query_results = {}

        # Group results by query
        for scenario_name, scenario_results in scenarios.items():
            for result in scenario_results:
                query_key = (
                    result["query"]["user"],
                    result["query"]["action"],
                    result["query"]["table"],
                )
                if query_key not in query_results:
                    query_results[query_key] = {}
                query_results[query_key][scenario_name] = result["authorized"]

        # Check consistency
        violations = []
        for query_key, scenario_auths in query_results.items():
            auth_values = list(scenario_auths.values())
            if len(set(auth_values)) > 1:  # Not all the same
                violation = {
                    "query": {
                        "user": query_key[0],
                        "action": query_key[1],
                        "table": query_key[2],
                    },
                    "scenario_results": scenario_auths,
                    "message": "Inconsistent authorization across failure scenarios",
                }
                violations.append(violation)

        return {
            "consistent": len(violations) == 0,
            "total_queries": len(query_results),
            "violations": violations,
        }

    def run_comprehensive_semantic_tests(
        self,
        should_deny_queries: list[Query],
        should_allow_queries: list[Query],
        failure_scenarios: list[str] = None,
    ) -> dict[str, Any]:
        """
        Run comprehensive semantic correctness tests.

        Args:
            should_deny_queries: Queries that should be denied by policy
            should_allow_queries: Queries that should be allowed by policy
            failure_scenarios: Failure scenarios to test

        Returns:
            Comprehensive test results
        """
        if failure_scenarios is None:
            failure_scenarios = [
                "agent_unavailable",
                "network_timeout",
                "malformed_response",
            ]

        results = {
            "test_suite": "comprehensive_semantic_correctness",
            "timestamp": time.time(),
            "fail_closed_tests": {},
            "monotonicity_tests": {},
            "consistency_tests": {},
            "overall_summary": {},
        }

        click.echo("Running comprehensive semantic correctness tests...")

        # 1. Fail-closed behavior
        click.echo("\n1. Testing fail-closed behavior...")
        results["fail_closed_tests"] = self.test_fail_closed_behavior(
            should_deny_queries, failure_scenarios
        )

        # 2. Monotonicity
        click.echo("\n2. Testing monotonicity...")
        all_test_queries = should_deny_queries + should_allow_queries
        results["monotonicity_tests"] = self.test_monotonicity(all_test_queries)

        # 3. Consistency under failures
        click.echo("\n3. Testing consistency under failures...")
        results["consistency_tests"] = self.test_consistency_under_failures(
            all_test_queries, failure_scenarios
        )

        # Overall summary
        results["overall_summary"] = self._generate_overall_summary(results)

        return results

    def _generate_overall_summary(self, results: dict[str, Any]) -> dict[str, Any]:
        """Generate overall summary of all semantic tests."""
        summary = {
            "fail_closed_pass": results["fail_closed_tests"]["summary"]["pass"],
            "monotonicity_pass": results["monotonicity_tests"]["summary"]["monotonic"],
            "consistency_pass": results["consistency_tests"]["summary"]["consistent"],
            "all_tests_pass": False,
            "total_violations": 0,
        }

        summary["total_violations"] = (
            results["fail_closed_tests"]["summary"]["total_violations"]
            + results["monotonicity_tests"]["summary"]["violations"]
            + len(results["consistency_tests"]["summary"]["violations"])
        )

        summary["all_tests_pass"] = (
            summary["fail_closed_pass"]
            and summary["monotonicity_pass"]
            and summary["consistency_pass"]
        )

        return summary


# Pre-defined test suites


def get_standard_security_test_cases() -> list[SecurityTestCase]:
    """Get standard security test cases for semantic correctness."""
    return [
        SecurityTestCase(
            name="unauthorized_select",
            description="SELECT queries on tables user should not access",
            queries=[
                # Bob is an employee, not allowed to see sensitive_data
                Query(
                    id=101,
                    user="user_bob",
                    action="SELECT",
                    table="abac_test.sensitive_data",
                    sql="SELECT * FROM abac_test.sensitive_data LIMIT 1",
                    category="SELECT",
                ),
                # Charlie is an intern, not allowed to see projects (private)
                Query(
                    id=102,
                    user="user_charlie",
                    action="SELECT",
                    table="abac_test.projects",
                    sql="SELECT * FROM abac_test.projects LIMIT 1",
                    category="SELECT",
                ),
            ],
            expected_denied=True,
            failure_scenario="agent_unavailable",
        ),
        SecurityTestCase(
            name="unauthorized_dml",
            description="DML queries on tables user should not modify",
            queries=[
                # Charlie is an intern, not allowed to INSERT into employees
                Query(
                    id=103,
                    user="user_charlie",
                    action="INSERT",
                    table="abac_test.employees",
                    sql="INSERT INTO abac_test.employees (id, name, department) VALUES (99, 'Malicious', 'HR')",
                    category="INSERT",
                ),
            ],
            expected_denied=True,
            failure_scenario="network_timeout",
        ),
        SecurityTestCase(
            name="authorized_operations",
            description="Operations that should be allowed",
            queries=[
                # Alice is a manager, allowed to see sensitive_data
                Query(
                    id=104,
                    user="user_alice",
                    action="SELECT",
                    table="abac_test.sensitive_data",
                    sql="SELECT * FROM abac_test.sensitive_data LIMIT 1",
                    category="SELECT",
                ),
                # Bob is an employee, allowed to see projects (private)
                Query(
                    id=105,
                    user="user_bob",
                    action="SELECT",
                    table="abac_test.projects",
                    sql="SELECT * FROM abac_test.projects LIMIT 1",
                    category="SELECT",
                ),
            ],
            expected_denied=False,
            failure_scenario="normal_operation",
        ),
    ]


def run_semantic_correctness_tests(
    config: Config,
    should_deny_queries: list[Query] = None,
    should_allow_queries: list[Query] = None,
    failure_scenarios: list[str] = None,
) -> dict[str, Any]:
    """
    Convenience function to run semantic correctness tests.

    Args:
        config: Experiment configuration
        should_deny_queries: Queries that should be denied
        should_allow_queries: Queries that should be allowed
        failure_scenarios: Failure scenarios to test

    Returns:
        Comprehensive test results
    """
    if should_deny_queries is None:
        # Use standard test cases
        test_cases = get_standard_security_test_cases()
        should_deny_queries = []
        should_allow_queries = []
        for case in test_cases:
            if case.expected_denied:
                should_deny_queries.extend(case.queries)
            else:
                should_allow_queries.extend(case.queries)

    if should_allow_queries is None:
        should_allow_queries = []

    if failure_scenarios is None:
        failure_scenarios = [
            "agent_unavailable",
            "network_timeout",
            "malformed_response",
        ]

    tester = FailureSemanticsTester(config)
    return tester.run_comprehensive_semantic_tests(
        should_deny_queries, should_allow_queries, failure_scenarios
    )


# CLI helper (needs to be imported)
try:
    import click
except ImportError:
    click = None
