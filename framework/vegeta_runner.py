import json
import logging
import shlex
import shutil
import subprocess
import tempfile
from pathlib import Path
from typing import Any

import requests

logger = logging.getLogger(__name__)


def check_vegeta_installed():
    """Check if vegeta is installed and executable."""
    if not shutil.which("vegeta"):
        raise FileNotFoundError(
            "Vegeta is not installed or not in the system's PATH. Please install it to run stress tests."
        )
    logger.debug("Vegeta installation confirmed.")


def validate_cedar_response(
    target_url: str,
    auth_body: str,
    expected_decision: str | None = None,
    timeout: int = 5,
) -> dict[str, Any]:
    """
    Send a single authorization request to validate the schema and expected response.

    Args:
        target_url: The Cedar agent /v1/is_authorized endpoint
        auth_body: JSON body for the authorization request
        expected_decision: Expected decision ("Allow" or "Deny"), or None to skip validation
        timeout: Request timeout in seconds

    Returns:
        Dict with keys:
          - success: bool - whether the request succeeded and decision matched
          - status_code: int - HTTP status code
          - decision: str or None - the decision from the response
          - decision_matched: bool - whether decision matched expected
          - error: str or None - error message if failed
          - response_body: str - raw response body for debugging
    """
    result = {
        "success": False,
        "status_code": 0,
        "decision": None,
        "decision_matched": False,
        "error": None,
        "response_body": "",
    }

    try:
        # Parse auth_body to ensure it's valid JSON
        try:
            body_dict = json.loads(auth_body)
        except json.JSONDecodeError as e:
            result["error"] = f"Invalid JSON in auth_request_body: {e}"
            return result

        response = requests.post(
            target_url,
            json=body_dict,
            headers={"Content-Type": "application/json"},
            timeout=timeout,
        )

        result["status_code"] = response.status_code
        result["response_body"] = response.text

        if response.status_code != 200:
            result["error"] = f"HTTP {response.status_code}: {response.text}"
            return result

        # Try to parse response and extract decision
        try:
            response_data = response.json()
        except json.JSONDecodeError:
            result["error"] = f"Invalid JSON response: {response.text}"
            return result

        # Cedar agent responses contain "decision" field with "Allow" or "Deny"
        decision = response_data.get("decision")
        if decision is None:
            # Fallback: check for legacy format
            if "Allow" in response.text:
                decision = "Allow"
            elif "Deny" in response.text:
                decision = "Deny"

        result["decision"] = decision

        if decision is None:
            result["error"] = f"No decision found in response: {response.text}"
            return result

        result["success"] = True

        # Validate against expected decision if provided
        if expected_decision:
            result["decision_matched"] = decision == expected_decision
            if not result["decision_matched"]:
                logger.warning(
                    f"Decision mismatch: expected '{expected_decision}', got '{decision}'. "
                    f"This may indicate a policy configuration issue."
                )
        else:
            result["decision_matched"] = (
                True  # No expectation, so it matches by default
            )

    except requests.RequestException as e:
        result["error"] = f"Request failed: {e}"

    return result


def run_vegeta_stress_test(
    target_url: str,
    rate: int,
    duration_s: int,
    auth_body: str,
    headers: str = "Content-Type: application/json",
    expected_decision: str | None = None,
) -> dict[str, Any] | None:
    """
    Runs a stress test using Vegeta.

    Before running the load test, validates that the authorization request schema
    is correct and that responses contain the expected decision.

    Args:
        target_url: The Cedar agent endpoint URL
        rate: Requests per second
        duration_s: Duration of the stress test in seconds
        auth_body: JSON body for authorization requests
        headers: HTTP headers (default: Content-Type: application/json)
        expected_decision: Expected decision ("Allow" or "Deny") for pre-flight validation

    Returns:
        Dict with stress test results including:
          - rate, duration_ns, requests, success, latencies (p50, p95, p99, mean)
          - error_rate: Percentage of failed HTTP requests
          - decision_validated: Whether pre-flight validation passed
          - validation_decision: The decision returned during validation
    """
    check_vegeta_installed()

    # Pre-flight validation: ensure the request schema is correct
    validation_result = None
    if expected_decision:
        logger.info(f"Running pre-flight validation against {target_url}...")
        validation_result = validate_cedar_response(
            target_url, auth_body, expected_decision
        )

        if not validation_result["success"]:
            logger.error(
                f"Pre-flight validation failed: {validation_result['error']}. "
                f"The authorization request schema may be incorrect. "
                f"Continuing with stress test but results may not reflect true authorization behavior."
            )
        elif not validation_result["decision_matched"]:
            logger.warning(
                f"Pre-flight validation: decision '{validation_result['decision']}' "
                f"does not match expected '{expected_decision}'. "
                f"Check your Cedar policies and entity configuration."
            )
        else:
            logger.info(
                f"Pre-flight validation passed: received expected decision '{expected_decision}'"
            )

    with tempfile.NamedTemporaryFile(
        mode="w", delete=False, suffix=".txt"
    ) as body_file:
        body_path = Path(body_file.name)
        body_file.write(auth_body)

    targets = f"POST {target_url}\n@{body_path}"

    with tempfile.NamedTemporaryFile(
        mode="w", delete=False, suffix=".txt"
    ) as targets_file:
        targets_path = Path(targets_file.name)
        targets_file.write(targets)

    # Output results to a temporary JSON file
    with tempfile.NamedTemporaryFile(
        mode="w", delete=False, suffix=".json"
    ) as json_report_file:
        report_path = Path(json_report_file.name)

    # Build the command properly, quoting the header value for shell safety
    # Since we need to pipe to vegeta report, we'll build a shell command string
    attack_cmd = [
        "vegeta",
        "attack",
        "-targets",
        str(targets_path),
        "-rate",
        str(rate),
        "-duration",
        f"{duration_s}s",
        "-header",
        shlex.quote(headers),
    ]

    report_cmd = ["vegeta", "report", "-type=json", f"-output={report_path}"]

    # Join commands with pipe
    command_str = " ".join(attack_cmd) + " | " + " ".join(report_cmd)

    logger.info(f"Running Vegeta command: {command_str}")

    try:
        # Using shell=True because of the pipe `|`
        process = subprocess.run(
            command_str,
            shell=True,
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            check=True,
        )
        logger.debug(f"Vegeta stdout: {process.stdout}")
        logger.debug(f"Vegeta stderr: {process.stderr}")

        with open(report_path) as f:
            results = json.load(f)

        parsed = parse_vegeta_results(results)

        # Add validation results to output
        if validation_result:
            parsed["decision_validated"] = (
                validation_result["success"] and validation_result["decision_matched"]
            )
            parsed["validation_decision"] = validation_result["decision"]
            parsed["validation_error"] = validation_result["error"]
        else:
            parsed["decision_validated"] = None  # Not checked
            parsed["validation_decision"] = None
            parsed["validation_error"] = None

        return parsed

    except subprocess.CalledProcessError as e:
        logger.error(f"Vegeta command failed with exit code {e.returncode}:")
        logger.error(f"Stdout: {e.stdout}")
        logger.error(f"Stderr: {e.stderr}")
        return None
    except FileNotFoundError:
        logger.error(f"Could not find report file at {report_path}")
        return None
    finally:
        # Clean up temporary files
        body_path.unlink()
        targets_path.unlink()
        report_path.unlink()


def parse_vegeta_results(results: dict[str, Any]) -> dict[str, Any]:
    """Parses the JSON output from Vegeta into a flat dictionary."""

    error_rate = 0.0
    if len(results.get("status_codes", {})) > 0 and results.get("requests", 0) > 0:
        successful_requests = results["status_codes"].get("200", 0)
        total_requests = results["requests"]
        if total_requests > 0:
            error_rate = (total_requests - successful_requests) / total_requests

    return {
        "rate": results.get("rate"),
        "duration_ns": results.get("duration"),
        "requests": results.get("requests"),
        "success": results.get("success"),
        "p50_ns": results["latencies"].get("50th"),
        "p95_ns": results["latencies"].get("95th"),
        "p99_ns": results["latencies"].get("99th"),
        "mean_ns": results["latencies"].get("mean"),
        "errors": results.get("errors"),
        "error_rate": error_rate,
        "status_codes": results.get("status_codes"),
    }
