"""
Cedar agent statistics and verification utilities.

This module provides functions to:
1. Get Cedar agent request statistics
2. Verify that authorization is being invoked during benchmarks
3. Reset statistics between runs
"""

from typing import Any

import requests


def get_cedar_agent_stats(base_url: str, timeout: int = 5) -> dict[str, Any]:
    """
    Get Cedar agent statistics including request count.

    Args:
        base_url: Cedar agent base URL (e.g., http://localhost:8280)
        timeout: Request timeout in seconds

    Returns:
        Dictionary with agent statistics, or empty dict if unavailable
    """
    try:
        # Normalize URL
        url = base_url.rstrip("/")
        if not url.endswith("/v1"):
            url = f"{url}/v1"

        resp = requests.get(f"{url}/stats", timeout=timeout)
        if resp.status_code == 200:
            return resp.json()
    except requests.exceptions.RequestException:
        pass
    except Exception:
        pass

    return {}


def reset_cedar_agent_stats(base_url: str, timeout: int = 5) -> bool:
    """
    Reset Cedar agent statistics.

    Args:
        base_url: Cedar agent base URL
        timeout: Request timeout in seconds

    Returns:
        True if successful, False otherwise
    """
    try:
        url = base_url.rstrip("/")
        if not url.endswith("/v1"):
            url = f"{url}/v1"

        resp = requests.post(f"{url}/stats/reset", timeout=timeout)
        return resp.status_code in (200, 204)
    except Exception:
        return False


def verify_auth_invocations(
    before_stats: dict[str, Any],
    after_stats: dict[str, Any],
    expected_min: int = 1,
    verbose: bool = True,
) -> dict[str, Any]:
    """
    Verify that Cedar authorization was invoked during benchmark.

    Args:
        before_stats: Stats captured before benchmark
        after_stats: Stats captured after benchmark
        expected_min: Minimum expected authorization requests
        verbose: Print verification results

    Returns:
        Dictionary with verification results:
        - verified: True if auth requests >= expected_min
        - auth_requests: Number of auth requests during benchmark
        - warning: Warning message if verification failed
    """
    # Try different possible stat key names
    stat_keys = [
        "authorization_requests",
        "auth_requests",
        "is_authorized_requests",
        "total_requests",
        "requests",
    ]

    before_count = 0
    after_count = 0
    key_found = None

    for key in stat_keys:
        if key in before_stats or key in after_stats:
            before_count = before_stats.get(key, 0)
            after_count = after_stats.get(key, 0)
            key_found = key
            break

    actual = after_count - before_count

    result = {
        "verified": actual >= expected_min,
        "auth_requests": actual,
        "before_count": before_count,
        "after_count": after_count,
        "stat_key": key_found,
    }

    if verbose:
        print(f"  Authorization requests during benchmark: {actual}")
        if not result["verified"]:
            print(
                f"  WARNING: Expected at least {expected_min} auth requests, got {actual}"
            )
            print("  This may indicate authorization is NOT being invoked!")
            result["warning"] = (
                f"Expected >= {expected_min} auth requests, got {actual}"
            )

    return result


def check_cedar_agent_health(base_url: str, timeout: int = 5) -> dict[str, Any]:
    """
    Check Cedar agent health and readiness.

    Args:
        base_url: Cedar agent base URL
        timeout: Request timeout in seconds

    Returns:
        Dictionary with health status
    """
    result = {
        "healthy": False,
        "reachable": False,
        "has_policies": False,
        "has_entities": False,
        "error": None,
    }

    try:
        url = base_url.rstrip("/")
        if not url.endswith("/v1"):
            url = f"{url}/v1"

        # Check if agent is reachable.
        # Some agent builds may not implement /health; treat a non-5xx response
        # from any known endpoint as "reachable".
        endpoints = ["health", "policies", "stats", ""]
        for ep in endpoints:
            try:
                ep_url = f"{url}/{ep}" if ep else f"{url}/"
                resp = requests.get(ep_url, timeout=timeout)
                if resp.status_code < 500:
                    result["reachable"] = True
                    break
            except Exception:
                continue

        if not result["reachable"]:
            result["error"] = "Cedar agent not reachable"
            return result

        # Check if policies are loaded
        try:
            resp = requests.get(f"{url}/policies", timeout=timeout)
            if resp.status_code == 200:
                policies = resp.json()
                result["has_policies"] = len(policies) > 0
                result["policy_count"] = len(policies)
        except Exception:
            pass

        # Check if entities are registered
        try:
            resp = requests.get(f"{url}/entities", timeout=timeout)
            if resp.status_code == 200:
                entities = resp.json()
                result["has_entities"] = len(entities) > 0
                result["entity_count"] = len(entities)
        except Exception:
            pass

        result["healthy"] = result["reachable"] and result["has_policies"]

    except Exception as e:
        result["error"] = str(e)

    return result


def get_authorization_decision_breakdown(
    base_url: str, timeout: int = 5
) -> dict[str, int]:
    """
    Get breakdown of authorization decisions (allow/deny counts).

    Args:
        base_url: Cedar agent base URL
        timeout: Request timeout in seconds

    Returns:
        Dictionary with decision counts: {'allow': N, 'deny': M}
    """
    try:
        url = base_url.rstrip("/")
        if not url.endswith("/v1"):
            url = f"{url}/v1"

        resp = requests.get(f"{url}/stats", timeout=timeout)
        if resp.status_code == 200:
            stats = resp.json()
            return {
                "allow": stats.get("allow_count", stats.get("allows", 0)),
                "deny": stats.get("deny_count", stats.get("denies", 0)),
                "total": stats.get("total_requests", stats.get("requests", 0)),
            }
    except Exception:
        pass

    return {"allow": 0, "deny": 0, "total": 0}


class AuthorizationVerifier:
    """Context manager for verifying authorization invocations during benchmarks."""

    def __init__(self, base_url: str, expected_min: int = 1, verbose: bool = True):
        """
        Initialize verifier.

        Args:
            base_url: Cedar agent base URL
            expected_min: Minimum expected authorization requests
            verbose: Print verification results
        """
        self.base_url = base_url
        self.expected_min = expected_min
        self.verbose = verbose
        self.before_stats = {}
        self.after_stats = {}
        self.result = None

    def __enter__(self):
        """Capture stats before benchmark."""
        self.before_stats = get_cedar_agent_stats(self.base_url)
        if self.verbose:
            print(
                f"  Auth stats before: {self.before_stats.get('total_requests', 'N/A')} total requests"
            )
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Capture stats after benchmark and verify."""
        self.after_stats = get_cedar_agent_stats(self.base_url)
        self.result = verify_auth_invocations(
            self.before_stats, self.after_stats, self.expected_min, self.verbose
        )
        return False  # Don't suppress exceptions

    @property
    def verified(self) -> bool:
        """Return whether authorization was verified."""
        return self.result.get("verified", False) if self.result else False

    @property
    def auth_requests(self) -> int:
        """Return number of authorization requests during benchmark."""
        return self.result.get("auth_requests", 0) if self.result else 0
