from __future__ import annotations

import json
import os
import subprocess
import time
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any


@dataclass(frozen=True)
class LoggedCommandResult:
    returncode: int
    stdout_path: Path
    stderr_path: Path
    meta_path: Path
    duration_seconds: float


def _redact_arg(arg: str) -> str:
    # Best-effort redaction for common password flags.
    # Keep this conservative; don't try to parse every possible DSN.
    for prefix in (
        "--mysql-password=",
        "--password=",
        "--pgsql-password=",
        "--db-password=",
    ):
        if arg.startswith(prefix):
            return f"{prefix}REDACTED"
    return arg


def _redact_command(cmd: list[str]) -> list[str]:
    return [_redact_arg(a) for a in cmd]


def run_logged_command(
    command: list[str],
    log_dir: Path,
    *,
    cwd: Path | None = None,
    env: dict[str, str] | None = None,
    timeout_s: int | None = None,
    stream_to_console: bool = False,
    combine_stderr: bool = False,
    label: str | None = None,
) -> LoggedCommandResult:
    """Run a command and persist raw stdout/stderr + metadata.

    This is intentionally file-first: stdout/stderr are always written to disk.
    For long-running commands, this avoids holding large outputs in memory.

    Args:
        command: argv list.
        log_dir: directory to write stdout.log, stderr.log, meta.json.
        cwd: working directory.
        env: environment overrides.
        timeout_s: timeout in seconds.
        stream_to_console: if True, stream combined output to console.
        combine_stderr: if True, redirect stderr to stdout.
        label: optional label to include in meta.json.
    """
    log_dir.mkdir(parents=True, exist_ok=True)

    stdout_path = log_dir / "stdout.log"
    stderr_path = log_dir / "stderr.log"
    meta_path = log_dir / "meta.json"

    start = time.time()
    start_dt = datetime.now(UTC)

    cmd_env = os.environ.copy()
    if env:
        cmd_env.update(env)

    redacted_cmd = _redact_command(command)

    meta: dict[str, Any] = {
        "label": label,
        "command": redacted_cmd,
        "cwd": str(cwd) if cwd else None,
        "start_time_utc": start_dt.isoformat(),
    }

    try:
        if stream_to_console:
            # Stream combined output to console to preserve existing UX for long runs.
            # We also persist the stream to stdout.log.
            with stdout_path.open("w", encoding="utf-8", errors="replace") as out_f:
                # When streaming, we merge stderr for a single ordered timeline.
                proc = subprocess.Popen(
                    command,
                    cwd=str(cwd) if cwd else None,
                    env=cmd_env,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.STDOUT,
                    text=True,
                    encoding="utf-8",
                    errors="replace",
                )
                assert proc.stdout is not None

                while True:
                    line = proc.stdout.readline()
                    if not line and proc.poll() is not None:
                        break
                    if line:
                        out_f.write(line)
                        out_f.flush()
                        # Mirror to console
                        print(line, end="")

                rc = proc.wait(timeout=timeout_s)

            # No separate stderr stream when merged.
            stderr_path.write_text("", encoding="utf-8")

        else:
            with (
                stdout_path.open("w", encoding="utf-8", errors="replace") as out_f,
                stderr_path.open("w", encoding="utf-8", errors="replace") as err_f,
            ):
                proc = subprocess.Popen(
                    command,
                    cwd=str(cwd) if cwd else None,
                    env=cmd_env,
                    stdout=out_f,
                    stderr=(subprocess.STDOUT if combine_stderr else err_f),
                    text=False,
                )
                rc = proc.wait(timeout=timeout_s)

        duration = time.time() - start
        meta.update(
            {
                "end_time_utc": datetime.now(UTC).isoformat(),
                "duration_seconds": duration,
                "returncode": rc,
            }
        )
        meta_path.write_text(json.dumps(meta, indent=2), encoding="utf-8")

        return LoggedCommandResult(
            returncode=rc,
            stdout_path=stdout_path,
            stderr_path=stderr_path,
            meta_path=meta_path,
            duration_seconds=duration,
        )

    except subprocess.TimeoutExpired:
        duration = time.time() - start
        meta.update(
            {
                "end_time_utc": datetime.now(UTC).isoformat(),
                "duration_seconds": duration,
                "returncode": -1,
                "error": "timeout",
                "timeout_s": timeout_s,
            }
        )
        meta_path.write_text(json.dumps(meta, indent=2), encoding="utf-8")
        return LoggedCommandResult(
            returncode=-1,
            stdout_path=stdout_path,
            stderr_path=stderr_path,
            meta_path=meta_path,
            duration_seconds=duration,
        )
