#!/usr/bin/env python3
"""
Utilities for scaling Cedar policies for performance testing.

- Generate policy sets of varying sizes.
- Interact with Cedar agent to get/set policies.
"""

from __future__ import annotations

import random
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any

import requests

try:
    from tqdm import tqdm
except ImportError:

    def tqdm(iterable, **kwargs):
        return iterable


def get_policies(base_url: str) -> list[dict[str, Any]]:
    """Get all policies from Cedar agent."""
    try:
        response = requests.get(f"{base_url}/policies", timeout=10)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Error: Failed to get policies: {e}", file=sys.stderr)
        return []


def put_policies(base_url: str, policies: list[dict[str, Any]]) -> bool:
    """Replace all policies in the Cedar agent with a new set."""
    try:
        response = requests.put(f"{base_url}/policies", json=policies, timeout=30)

        # If payload is too large, fallback to batched/individual upload
        if response.status_code == 413:
            print(
                f"Warning: Single batch too large (413). Switching to parallel upload for {len(policies)} policies...",
                file=sys.stderr,
            )
            return _batch_put_policies(base_url, policies)

        response.raise_for_status()
        return True
    except requests.exceptions.RequestException as e:
        # Check for 413 in the exception if it wasn't caught by status_code check above
        # (requests might raise before we check unless raise_for_status is called, but here we call it after)
        # However, if 'response' exists and is 413, we handled it.
        # If raise_for_status() raised it (e.g. if we didn't check), we catch it here.

        is_413 = False
        if hasattr(e, "response") and e.response is not None:
            if e.response.status_code == 413:
                is_413 = True

        if is_413:
            print(
                f"Warning: Payload too large (413). Switching to parallel upload for {len(policies)} policies...",
                file=sys.stderr,
            )
            return _batch_put_policies(base_url, policies)

        error_msg = "Unknown error"
        if hasattr(e, "response") and e.response is not None:
            try:
                error_data = e.response.json()
                error_msg = error_data.get(
                    "error", error_data.get("reason", e.response.text)
                )
            except (ValueError, KeyError):
                error_msg = e.response.text or str(e)
        else:
            error_msg = str(e)
        print(f"Error: Failed to put policies: {e}", file=sys.stderr)
        print(f"Response: {error_msg}", file=sys.stderr)
        print(f"Policies being sent: {len(policies)} policies", file=sys.stderr)
        if policies:
            print(f"First policy example: {policies[0]}", file=sys.stderr)
        return False


def _batch_put_policies(base_url: str, policies: list[dict[str, Any]]) -> bool:
    """Fallback: clear policies and upload individually in parallel with connection pooling."""
    from requests.adapters import HTTPAdapter
    from urllib3.util.retry import Retry

    session = requests.Session()
    retry_strategy = Retry(
        total=3,
        backoff_factor=0.5,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET", "PUT", "POST", "DELETE"],
    )
    adapter = HTTPAdapter(
        pool_connections=20, pool_maxsize=20, max_retries=retry_strategy
    )
    session.mount("http://", adapter)
    session.mount("https://", adapter)

    try:
        session.put(f"{base_url}/policies", json=[], timeout=10).raise_for_status()
    except Exception as e:
        print(f"Error clearing policies for batch upload: {e}", file=sys.stderr)
        session.close()
        return False

    success_count = 0
    failure_samples: list[str] = []
    max_failure_samples = 10

    def upload_single_policy(policy: dict[str, Any]) -> tuple[bool, str]:
        pid = policy.get("id")
        try:
            if not pid:
                resp = session.post(f"{base_url}/policies", json=policy, timeout=30)
            else:
                resp = session.put(
                    f"{base_url}/policies/{pid}", json=policy, timeout=30
                )
            if resp.ok:
                return (True, "")
            return (False, f"HTTP {resp.status_code}: {resp.text[:200]}")
        except Exception as e:
            return (False, f"Exception: {type(e).__name__}: {str(e)[:100]}")

    max_workers = min(16, len(policies))
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(upload_single_policy, p): p for p in policies}

        iterator = as_completed(futures)
        if len(policies) > 100:
            iterator = tqdm(
                iterator,
                total=len(policies),
                desc="Uploading policies",
                unit="pol",
                leave=False,
            )

        for future in iterator:
            try:
                ok, err_msg = future.result()
                if ok:
                    success_count += 1
                elif len(failure_samples) < max_failure_samples:
                    policy = futures[future]
                    failure_samples.append(
                        f"Policy '{policy.get('id', 'unknown')}': {err_msg}"
                    )
            except Exception as e:
                if len(failure_samples) < max_failure_samples:
                    failure_samples.append(
                        f"Future exception: {type(e).__name__}: {str(e)[:100]}"
                    )

    session.close()

    if failure_samples:
        print(
            f"Sample failures ({len(failure_samples)} of {len(policies) - success_count}):",
            file=sys.stderr,
        )
        for sample in failure_samples[:5]:
            print(f"  - {sample}", file=sys.stderr)

    if success_count == len(policies):
        print(
            f"✓ Successfully uploaded {success_count} policies in batches.",
            file=sys.stderr,
        )
        return True
    else:
        print(
            f"Error: Only {success_count}/{len(policies)} policies uploaded successfully.",
            file=sys.stderr,
        )
        return False


def build_policy_set(
    base_spec: dict[str, Any],
    count: int,
    match_ratio: float = 0.2,
    seed: int | None = None,
    namespace: str = "",
) -> list[dict[str, Any]]:
    """
    Builds a set of Cedar policies.
    - A portion of policies are based on the spec and likely to match.
    - The rest are randomly generated non-matching policies to increase count.
    """
    if seed is not None:
        random.seed(seed)

    namespace + "::" if namespace else ""

    base_policies = []
    if "policies" in base_spec:
        from .translate_to_cedar import create_cedar_policies

        # Use the existing translator to get properly namespaced base policies
        base_policies = create_cedar_policies(base_spec, namespace=namespace)

    # If desired count is less than or equal to base policies, return a subset
    if count <= len(base_policies) and len(base_policies) > 0:

        def _policy_allows_any_workload_role(policy: dict[str, Any]) -> bool:
            content = (policy.get("content") or "").lower()
            return any(
                r in content
                for r in (
                    'principal.user_role == "manager"',
                    'principal.user_role == "employee"',
                    'principal.user_role == "intern"',
                    'principal.user_role == "auditor"',
                )
            )

        preferred = [p for p in base_policies if _policy_allows_any_workload_role(p)]
        fallback = [p for p in base_policies if p not in preferred]

        pool = preferred or base_policies
        if count <= len(pool):
            return random.sample(pool, count)

        return pool + random.sample(fallback, count - len(pool))

    num_matching_to_generate = int(count * match_ratio)
    count - num_matching_to_generate

    generated_policies = []

    # 1. Include all base policies first if we have them
    if base_policies:
        generated_policies.extend(base_policies)

    # 2. Generate matching policies by duplicating and slightly modifying base policies
    # Only if we have base policies to work with and need more
    num_to_fill_matching = max(0, num_matching_to_generate - len(generated_policies))
    if base_policies and num_to_fill_matching > 0:
        for i in range(num_to_fill_matching):
            base_policy = random.choice(base_policies)
            new_id = (
                f"{namespace.lower()}_gen_match_{i}" if namespace else f"gen_match_{i}"
            )
            generated_policies.append({"id": new_id, "content": base_policy["content"]})

    # 3. Generate non-matching policies to reach desired count
    num_to_fill_total = max(0, count - len(generated_policies))
    for i in range(num_to_fill_total):
        new_id = (
            f"{namespace.lower()}_gen_non_match_{i}"
            if namespace
            else f"gen_non_match_{i}"
        )
        # This policy is unlikely to match any realistic query
        # Use "has" check before accessing attribute (Cedar requirement)
        rand_val = random.randint(1, 100000)
        content = f"""permit(principal, action, resource)
when {{ resource has data_classification && resource.data_classification == "non_existent_classification_{rand_val}" }};"""
        generated_policies.append({"id": new_id, "content": content})

    random.shuffle(generated_policies)

    # Ensure we return exactly `count` policies
    return generated_policies[:count]
