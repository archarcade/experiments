#!/usr/bin/env python3
"""
Metadata collection for experiment reproducibility.

Captures:
- Git commit hashes
- Container image tags and digests
- Host hardware information
- OS and kernel version
- Docker engine version
- Configuration snapshots
- Experiment parameters

This is critical for USENIX artifact evaluation reproducibility.
"""

from __future__ import annotations

import hashlib
import json
import os
import platform
import subprocess
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from pydantic import BaseModel, Field


class GitInfo(BaseModel):
    """Git repository information."""

    commit_hash: str
    branch: str
    is_dirty: bool
    remote_url: str | None = None
    commit_date: str | None = None
    commit_message: str | None = None


class ContainerInfo(BaseModel):
    """Docker container/image information."""

    image: str
    tag: str
    digest: str | None = None
    container_id: str | None = None
    container_name: str | None = None
    status: str | None = None


class HardwareInfo(BaseModel):
    """Host hardware information."""

    cpu_model: str
    cpu_cores: int
    cpu_threads: int
    memory_total_gb: float
    architecture: str
    machine: str


class SoftwareInfo(BaseModel):
    """Software versions."""

    os_name: str
    os_version: str
    kernel_version: str
    python_version: str
    docker_version: str | None = None
    docker_compose_version: str | None = None


class ExperimentMetadata(BaseModel):
    """Complete experiment metadata for reproducibility."""

    experiment_id: str
    experiment_name: str
    timestamp: str
    timestamp_utc: str

    # Version information
    git: GitInfo | None = None
    containers: list[ContainerInfo] = Field(default_factory=list)
    hardware: HardwareInfo | None = None
    software: SoftwareInfo | None = None

    # Configuration
    config_snapshot: dict[str, Any] = Field(default_factory=dict)
    config_hash: str | None = None

    # Experiment parameters
    parameters: dict[str, Any] = Field(default_factory=dict)
    workload_hash: str | None = None

    # Run information
    run_order: str | None = (
        None  # "baseline_first", "cedar_first", "ABBA", "randomized"
    )
    warmup_iterations: int = 0
    measurement_iterations: int = 0
    seed: int | None = None

    # Notes
    notes: str | None = None

    def save(self, path: Path) -> None:
        """Save metadata to JSON file."""
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(self.model_dump_json(indent=2))


class MetadataCollector:
    """Collects experiment metadata from various sources."""

    def __init__(self, repo_path: Path | None = None):
        """
        Initialize metadata collector.

        Args:
            repo_path: Path to git repository (defaults to current directory)
        """
        self.repo_path = repo_path or Path.cwd()

    def collect_git_info(self, path: Path | None = None) -> GitInfo | None:
        """Collect git repository information."""
        repo = path or self.repo_path

        try:
            # Get commit hash
            result = subprocess.run(
                ["git", "rev-parse", "HEAD"],
                cwd=repo,
                capture_output=True,
                text=True,
                timeout=5,
            )
            if result.returncode != 0:
                return None
            commit_hash = result.stdout.strip()

            # Get branch name
            result = subprocess.run(
                ["git", "rev-parse", "--abbrev-ref", "HEAD"],
                cwd=repo,
                capture_output=True,
                text=True,
                timeout=5,
            )
            branch = result.stdout.strip() if result.returncode == 0 else "unknown"

            # Check if dirty
            result = subprocess.run(
                ["git", "status", "--porcelain"],
                cwd=repo,
                capture_output=True,
                text=True,
                timeout=5,
            )
            is_dirty = bool(result.stdout.strip()) if result.returncode == 0 else False

            # Get remote URL
            result = subprocess.run(
                ["git", "remote", "get-url", "origin"],
                cwd=repo,
                capture_output=True,
                text=True,
                timeout=5,
            )
            remote_url = result.stdout.strip() if result.returncode == 0 else None

            # Get commit date
            result = subprocess.run(
                ["git", "log", "-1", "--format=%ci"],
                cwd=repo,
                capture_output=True,
                text=True,
                timeout=5,
            )
            commit_date = result.stdout.strip() if result.returncode == 0 else None

            # Get commit message (first line)
            result = subprocess.run(
                ["git", "log", "-1", "--format=%s"],
                cwd=repo,
                capture_output=True,
                text=True,
                timeout=5,
            )
            commit_message = result.stdout.strip() if result.returncode == 0 else None

            return GitInfo(
                commit_hash=commit_hash,
                branch=branch,
                is_dirty=is_dirty,
                remote_url=remote_url,
                commit_date=commit_date,
                commit_message=commit_message,
            )
        except Exception:
            return None

    def collect_container_info(
        self, container_names: list[str] | None = None
    ) -> list[ContainerInfo]:
        """Collect Docker container information."""
        containers = []

        if container_names is None:
            # Default containers for this experiment
            container_names = [
                "mysql-baseline",
                "mysql-cedar",
                "cedar-agent",
            ]

        for name in container_names:
            info = self._get_container_info(name)
            if info:
                containers.append(info)

        return containers

    def _get_container_info(self, container_name: str) -> ContainerInfo | None:
        """Get info for a specific container."""
        try:
            # Get container details
            result = subprocess.run(
                [
                    "docker",
                    "inspect",
                    container_name,
                    "--format",
                    '{"id":"{{.Id}}","image":"{{.Config.Image}}","status":"{{.State.Status}}"}',
                ],
                capture_output=True,
                text=True,
                timeout=10,
            )
            if result.returncode != 0:
                return None

            data = json.loads(result.stdout.strip())
            image = data.get("image", "")

            # Parse image:tag
            if ":" in image:
                image_name, tag = image.rsplit(":", 1)
            else:
                image_name, tag = image, "latest"

            # Try to get image digest
            digest = None
            try:
                result = subprocess.run(
                    [
                        "docker",
                        "image",
                        "inspect",
                        image,
                        "--format",
                        "{{.RepoDigests}}",
                    ],
                    capture_output=True,
                    text=True,
                    timeout=10,
                )
                if result.returncode == 0:
                    digests = result.stdout.strip()
                    if digests and digests != "[]":
                        # Extract first digest
                        digest = digests.strip("[]").split()[0] if digests else None
            except Exception:
                pass

            return ContainerInfo(
                image=image_name,
                tag=tag,
                digest=digest,
                container_id=data.get("id", "")[:12],
                container_name=container_name,
                status=data.get("status"),
            )
        except Exception:
            return None

    def collect_hardware_info(self) -> HardwareInfo:
        """Collect host hardware information."""
        cpu_model = "unknown"
        cpu_cores = os.cpu_count() or 1
        cpu_threads = cpu_cores
        memory_gb = 0.0

        # Get CPU info
        try:
            if platform.system() == "Darwin":
                # macOS
                result = subprocess.run(
                    ["sysctl", "-n", "machdep.cpu.brand_string"],
                    capture_output=True,
                    text=True,
                    timeout=5,
                )
                if result.returncode == 0:
                    cpu_model = result.stdout.strip()

                result = subprocess.run(
                    ["sysctl", "-n", "hw.physicalcpu"],
                    capture_output=True,
                    text=True,
                    timeout=5,
                )
                if result.returncode == 0:
                    cpu_cores = int(result.stdout.strip())

                result = subprocess.run(
                    ["sysctl", "-n", "hw.logicalcpu"],
                    capture_output=True,
                    text=True,
                    timeout=5,
                )
                if result.returncode == 0:
                    cpu_threads = int(result.stdout.strip())

                result = subprocess.run(
                    ["sysctl", "-n", "hw.memsize"],
                    capture_output=True,
                    text=True,
                    timeout=5,
                )
                if result.returncode == 0:
                    memory_gb = int(result.stdout.strip()) / (1024**3)

            elif platform.system() == "Linux":
                # Linux
                if Path("/proc/cpuinfo").exists():
                    cpuinfo = Path("/proc/cpuinfo").read_text()
                    for line in cpuinfo.split("\n"):
                        if line.startswith("model name"):
                            cpu_model = line.split(":")[1].strip()
                            break

                if Path("/proc/meminfo").exists():
                    meminfo = Path("/proc/meminfo").read_text()
                    for line in meminfo.split("\n"):
                        if line.startswith("MemTotal"):
                            mem_kb = int(line.split()[1])
                            memory_gb = mem_kb / (1024**2)
                            break

                # Physical cores
                try:
                    result = subprocess.run(
                        ["nproc", "--all"],
                        capture_output=True,
                        text=True,
                        timeout=5,
                    )
                    if result.returncode == 0:
                        cpu_threads = int(result.stdout.strip())
                except Exception:
                    pass
        except Exception:
            pass

        return HardwareInfo(
            cpu_model=cpu_model,
            cpu_cores=cpu_cores,
            cpu_threads=cpu_threads,
            memory_total_gb=round(memory_gb, 2),
            architecture=platform.machine(),
            machine=platform.node(),
        )

    def collect_software_info(self) -> SoftwareInfo:
        """Collect software version information."""
        docker_version = None
        docker_compose_version = None

        # Docker version
        try:
            result = subprocess.run(
                ["docker", "--version"],
                capture_output=True,
                text=True,
                timeout=5,
            )
            if result.returncode == 0:
                docker_version = result.stdout.strip()
        except Exception:
            pass

        # Docker Compose version
        try:
            result = subprocess.run(
                ["docker", "compose", "version"],
                capture_output=True,
                text=True,
                timeout=5,
            )
            if result.returncode == 0:
                docker_compose_version = result.stdout.strip()
        except Exception:
            pass

        return SoftwareInfo(
            os_name=platform.system(),
            os_version=platform.version(),
            kernel_version=platform.release(),
            python_version=platform.python_version(),
            docker_version=docker_version,
            docker_compose_version=docker_compose_version,
        )

    def compute_config_hash(self, config: dict[str, Any]) -> str:
        """Compute SHA256 hash of configuration."""
        config_str = json.dumps(config, sort_keys=True)
        return hashlib.sha256(config_str.encode()).hexdigest()[:16]

    def compute_workload_hash(self, workload_path: Path) -> str | None:
        """Compute SHA256 hash of workload file."""
        if not workload_path.exists():
            return None
        content = workload_path.read_bytes()
        return hashlib.sha256(content).hexdigest()[:16]

    def collect_all(
        self,
        experiment_name: str,
        config: dict[str, Any] | None = None,
        parameters: dict[str, Any] | None = None,
        workload_path: Path | None = None,
        container_names: list[str] | None = None,
        run_order: str = "baseline_first",
        warmup_iterations: int = 0,
        measurement_iterations: int = 0,
        seed: int | None = None,
        notes: str | None = None,
    ) -> ExperimentMetadata:
        """
        Collect all metadata for an experiment.

        Args:
            experiment_name: Name of the experiment
            config: Configuration dictionary
            parameters: Experiment parameters
            workload_path: Path to workload file
            container_names: List of container names to inspect
            run_order: Order of conditions ("baseline_first", "cedar_first", "ABBA", "randomized")
            warmup_iterations: Number of warmup iterations
            measurement_iterations: Number of measurement iterations
            seed: Random seed
            notes: Additional notes

        Returns:
            ExperimentMetadata with all collected information
        """
        now = datetime.now()
        now_utc = datetime.now(UTC)

        # Stable experiment ID: one complete set per experiment.
        experiment_id = str(experiment_name)

        # Collect all info
        git_info = self.collect_git_info()
        containers = self.collect_container_info(container_names)
        hardware = self.collect_hardware_info()
        software = self.collect_software_info()

        # Config hash
        config_hash = self.compute_config_hash(config) if config else None

        # Workload hash
        workload_hash = (
            self.compute_workload_hash(workload_path) if workload_path else None
        )

        return ExperimentMetadata(
            experiment_id=experiment_id,
            experiment_name=experiment_name,
            timestamp=now.isoformat(),
            timestamp_utc=now_utc.isoformat(),
            git=git_info,
            containers=containers,
            hardware=hardware,
            software=software,
            config_snapshot=config or {},
            config_hash=config_hash,
            parameters=parameters or {},
            workload_hash=workload_hash,
            run_order=run_order,
            warmup_iterations=warmup_iterations,
            measurement_iterations=measurement_iterations,
            seed=seed,
            notes=notes,
        )


def load_metadata(path: Path) -> ExperimentMetadata:
    """Load metadata from JSON file."""
    data = json.loads(path.read_text())
    return ExperimentMetadata(**data)
