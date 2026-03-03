#!/usr/bin/env python3
"""
Sysbench output parser using Pydantic for robust data modeling.
"""

from __future__ import annotations

import re
import subprocess
import time
from pathlib import Path

from pydantic import BaseModel


class SysbenchMetrics(BaseModel):
    tps: float = 0.0
    qps: float = 0.0
    lat_min_ms: float = 0.0
    lat_avg_ms: float = 0.0
    lat_max_ms: float = 0.0
    lat_p95_ms: float = 0.0
    lat_p99_ms: float | None = None
    threads: int = 1


def parse_sysbench_output(output: str) -> SysbenchMetrics | None:
    """
    Parses sysbench output to extract key performance metrics.
    """
    data = {}
    try:
        # TPS
        tps_match = re.search(
            r"transactions:\s+\d+\s+\((\d+(?:\.\d+)?)\s+per sec\.\)", output
        )
        if tps_match:
            data["tps"] = float(tps_match.group(1))

        # QPS
        qps_match = re.search(
            r"queries:\s+\d+\s+\((\d+(?:\.\d+)?)\s+per sec\.\)", output
        )
        if qps_match:
            data["qps"] = float(qps_match.group(1))

        # Latency
        lat_min_match = re.search(r"min:\s+(\d+(?:\.\d+)?)", output)
        if lat_min_match:
            data["lat_min_ms"] = float(lat_min_match.group(1))

        lat_avg_match = re.search(r"avg:\s+(\d+(?:\.\d+)?)", output)
        if lat_avg_match:
            data["lat_avg_ms"] = float(lat_avg_match.group(1))

        lat_max_match = re.search(r"max:\s+(\d+(?:\.\d+)?)", output)
        if lat_max_match:
            data["lat_max_ms"] = float(lat_max_match.group(1))

        lat_p95_match = re.search(r"95th percentile:\s+(\d+(?:\.\d+)?)", output)
        if lat_p95_match:
            data["lat_p95_ms"] = float(lat_p95_match.group(1))

        lat_p99_match = re.search(r"99th percentile:\s+(\d+(?:\.\d+)?)", output)
        if lat_p99_match:
            data["lat_p99_ms"] = float(lat_p99_match.group(1))

        # Threads
        threads_match = re.search(r"Number of threads:\s+(\d+)", output)
        if threads_match:
            data["threads"] = int(threads_match.group(1))

        if not data:
            return None

        return SysbenchMetrics(**data)
    except (ValueError, IndexError):
        return None


def run_sysbench_command(
    command: list[str],
    docker: bool,
    *,
    log_dir: Path | None = None,
    label: str | None = None,
    timeout_s: int | None = None,
) -> str:
    from .command_runner import run_logged_command

    max_retries = 3
    is_prepare = "prepare" in command

    for attempt in range(max_retries):
        if log_dir is None:
            if docker:
                docker_cmd = [
                    "docker",
                    "run",
                    "--rm",
                    "--network=host",
                    "severalnines/sysbench",
                ] + command[1:]
                result = subprocess.run(
                    docker_cmd,
                    capture_output=True,
                    text=True,
                    encoding="utf-8",
                    errors="replace",
                    check=False,
                    timeout=timeout_s,
                )
            else:
                result = subprocess.run(
                    command,
                    capture_output=True,
                    text=True,
                    encoding="utf-8",
                    errors="replace",
                    check=False,
                    timeout=timeout_s,
                )

            if result.returncode == 0:
                return (result.stdout or "") + (
                    "\n" + result.stderr if result.stderr else ""
                )

            output_combined = (result.stderr or "") + (result.stdout or "")
            if is_prepare and "already exists" in output_combined:
                return (result.stdout or "") + (
                    "\n" + result.stderr if result.stderr else ""
                )

            if is_prepare and attempt < max_retries - 1:
                time.sleep(2)
                continue

            error_msg = (
                f"Sysbench command failed with exit code {result.returncode}:\n"
                f"Command: {' '.join(command)}\n"
            )
            if result.stderr:
                error_msg += f"Stderr: {result.stderr}\n"
            if result.stdout:
                error_msg += f"Stdout: {result.stdout}\n"
            raise RuntimeError(error_msg)

        attempt_dir = Path(log_dir) / f"attempt_{attempt + 1}"

        if docker:
            docker_cmd = [
                "docker",
                "run",
                "--rm",
                "--network=host",
                "severalnines/sysbench",
            ] + command[1:]
            res = run_logged_command(
                docker_cmd,
                attempt_dir,
                cwd=None,
                timeout_s=timeout_s,
                combine_stderr=False,
                label=f"{label or 'sysbench'} (docker)"
                if label
                else "sysbench (docker)",
            )
        else:
            res = run_logged_command(
                command,
                attempt_dir,
                cwd=None,
                timeout_s=timeout_s,
                combine_stderr=False,
                label=label or "sysbench",
            )

        stdout = res.stdout_path.read_text(encoding="utf-8", errors="replace")
        stderr = res.stderr_path.read_text(encoding="utf-8", errors="replace")
        combined = (stdout or "") + ("\n" + stderr if stderr else "")

        if res.returncode == 0:
            return combined

        # If it's a prepare command and it failed because table already exists
        if is_prepare and "already exists" in (stderr + stdout):
            return combined

        # If it failed for other reasons during prepare, retry after a delay
        if is_prepare and attempt < max_retries - 1:
            time.sleep(2)
            continue

        error_msg = (
            f"Sysbench command failed with exit code {res.returncode}:\n"
            f"Command: {' '.join(command)}\n"
            f"Logs: {attempt_dir}\n"
            f"Stdout: {res.stdout_path}\n"
            f"Stderr: {res.stderr_path}\n"
        )
        raise RuntimeError(error_msg)

    return ""
