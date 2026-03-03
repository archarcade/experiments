#!/usr/bin/env python3
"""
Multi-run experiment orchestrator for USENIX-grade evaluation.

Implements:
- Multiple independent runs with controlled ordering (ABBA, randomized)
- Structured output directory layout
- Automatic metadata collection
- Aggregate statistics with confidence intervals
- Run-level summaries for statistical analysis

Reference: TRD Section 8.1 - Multi-run orchestration
"""

from __future__ import annotations

import json
import random
import statistics
import time
from collections.abc import Callable
from datetime import datetime
from pathlib import Path
from typing import Any

from pydantic import BaseModel, Field
from tqdm import tqdm

from .metadata import ExperimentMetadata, MetadataCollector
from .stats import (
    RunLevelMetrics,
    bootstrap_ci_median,
    cliffs_delta,
    compare_systems,
    compute_run_level_metrics,
    holm_bonferroni_correction,
    summary_stats,
    wilcoxon_signed_rank_test,
)


class RunResult(BaseModel):
    """Result of a single experiment run."""

    run_id: str
    run_index: int
    system: str  # "baseline" or "cedar"
    order_in_pair: int  # 0 or 1 (first or second in pair)

    # Timing
    start_time: str
    end_time: str
    duration_seconds: float

    # Raw results
    results_path: str  # Path stored as string in Pydantic model

    # Per-run metrics
    metrics: RunLevelMetrics

    # Per-category metrics (for breakdown)
    category_metrics: dict[str, dict[str, float]] = Field(default_factory=dict)


class PairedRunResult(BaseModel):
    """Result of a paired run (baseline + cedar)."""

    pair_index: int
    order: str  # "baseline_first" or "cedar_first"
    baseline: RunResult
    cedar: RunResult

    # Rest period between runs
    rest_seconds: float = 0.0


class MultiRunResult(BaseModel):
    """Complete result of multi-run experiment."""

    experiment_name: str
    ordering_strategy: str

    # All paired runs
    paired_runs: list[PairedRunResult]

    # Aggregate metrics with CIs
    aggregate: dict[str, Any] = Field(default_factory=dict)

    # Per-category comparisons
    category_comparisons: dict[str, dict[str, Any]] = Field(default_factory=dict)

    # Metadata
    metadata: ExperimentMetadata | None = None

    def save(self, output_dir: Path) -> None:
        """Save complete results to directory."""
        output_dir.mkdir(parents=True, exist_ok=True)

        # Save full results
        (output_dir / "multi_run_results.json").write_text(
            self.model_dump_json(indent=2)
        )

        # Save aggregate summary
        (output_dir / "aggregate_summary.json").write_text(
            json.dumps(self.aggregate, indent=2)
        )

        # Save category comparisons
        (output_dir / "category_comparisons.json").write_text(
            json.dumps(self.category_comparisons, indent=2)
        )

        # Save metadata
        if self.metadata:
            self.metadata.save(output_dir / "metadata.json")


class OrderingStrategy:
    """Strategies for ordering baseline/cedar runs."""

    @staticmethod
    def baseline_first(n_pairs: int) -> list[tuple[str, str]]:
        """Always run baseline first."""
        return [("baseline", "cedar")] * max(1, int(n_pairs))

    @staticmethod
    def cedar_first(n_pairs: int) -> list[tuple[str, str]]:
        """Always run cedar first."""
        return [("cedar", "baseline")] * max(1, int(n_pairs))

    @staticmethod
    def abba(n_pairs: int) -> list[tuple[str, str]]:
        """
        ABBA ordering to balance order effects.
        Pattern: AB, BA, AB, BA, ...
        """
        pairs: list[tuple[str, str]] = []
        n = max(1, int(n_pairs))
        for i in range(n):
            if i % 2 == 0:
                pairs.append(("baseline", "cedar"))
            else:
                pairs.append(("cedar", "baseline"))
        return pairs

    @staticmethod
    def randomized(n_pairs: int, seed: int | None = None) -> list[tuple[str, str]]:
        """Randomized ordering with optional seed."""
        if seed is not None:
            random.seed(seed)

        pairs: list[tuple[str, str]] = []
        n = max(1, int(n_pairs))
        for _ in range(n):
            if random.random() < 0.5:
                pairs.append(("baseline", "cedar"))
            else:
                pairs.append(("cedar", "baseline"))
        return pairs


class MultiRunOrchestrator:
    """
    Orchestrates multiple independent experiment runs.

    Features:
    - Configurable ordering (ABBA, randomized, fixed)
    - Rest periods between runs
    - Automatic metadata collection
    - Aggregate statistics with CIs
    """

    def __init__(
        self,
        experiment_name: str,
        ordering: str = "abba",
        n_pairs: int = 1,
        rest_between_runs: float = 5.0,
        seed: int | None = None,
        output_base_dir: Path | None = None,
    ):
        """
        Initialize orchestrator.

        Args:
            experiment_name: Name of the experiment
            ordering: "abba", "randomized", "baseline_first", "cedar_first"
            rest_between_runs: Seconds to wait between runs (thermal stabilization)
            seed: Random seed for reproducibility
            output_base_dir: Base directory for outputs
        """
        self.experiment_name = experiment_name
        self.ordering = ordering
        self.n_pairs = max(1, int(n_pairs))
        self.rest_between_runs = rest_between_runs
        self.seed = seed
        self.output_base_dir = output_base_dir or Path("results")

        # Generate run orders
        self._run_orders = self._generate_orders()

        # Metadata collector
        self._metadata_collector = MetadataCollector()

        # Results storage
        self._paired_runs: list[PairedRunResult] = []

    def _generate_orders(self) -> list[tuple[str, str]]:
        """Generate run orders based on strategy."""
        if self.ordering == "abba":
            return OrderingStrategy.abba(self.n_pairs)
        elif self.ordering == "randomized":
            return OrderingStrategy.randomized(self.n_pairs, self.seed)
        elif self.ordering == "cedar_first":
            return OrderingStrategy.cedar_first(self.n_pairs)
        else:  # baseline_first
            return OrderingStrategy.baseline_first(self.n_pairs)

    def run(
        self,
        run_single_experiment: Callable[[str, int, Path], dict[str, Any]],
        config: dict[str, Any] | None = None,
        workload_path: Path | None = None,
        warmup_iterations: int = 0,
        measurement_iterations: int = 0,
    ) -> MultiRunResult:
        """
        Execute all runs.

        Args:
            run_single_experiment: Function that runs a single experiment.
                Signature: (system: str, run_index: int, output_path: Path) -> results_dict
            config: Configuration dictionary for metadata
            workload_path: Path to workload file
            warmup_iterations: Warmup iteration count
            measurement_iterations: Measurement iteration count

        Returns:
            MultiRunResult with all runs and aggregate statistics
        """
        # Stable output directory: one complete set per experiment.
        # If caller already pointed output_base_dir at the experiment directory,
        # don't nest it again.
        output_dir = (
            self.output_base_dir
            if self.output_base_dir.name == self.experiment_name
            else self.output_base_dir / self.experiment_name
        )
        output_dir.mkdir(parents=True, exist_ok=True)

        # Clean prior run artifacts to avoid stale pairs when re-running.
        # Also remove legacy timestamped run dirs (e.g., benchmark_YYYYMMDD_HHMMSS).
        for p in output_dir.glob(f"{self.experiment_name}_*"):
            if p.is_dir():
                import shutil

                shutil.rmtree(p, ignore_errors=True)
        for p in output_dir.glob("pair_*"):
            if p.is_dir():
                import shutil

                shutil.rmtree(p, ignore_errors=True)
        for f in (
            "multi_run_results.json",
            "aggregate_summary.json",
            "category_comparisons.json",
            "metadata.json",
        ):
            try:
                (output_dir / f).unlink()
            except FileNotFoundError:
                pass

        self._paired_runs = []

        for pair_idx, (first_system, second_system) in enumerate(
            tqdm(
                self._run_orders, desc=f"Multi-run {self.experiment_name}", unit="pair"
            )
        ):
            pair_dir = output_dir / f"pair_{pair_idx:03d}"
            pair_dir.mkdir(parents=True, exist_ok=True)

            # Run first system
            first_result = self._run_single(
                run_single_experiment,
                first_system,
                pair_idx * 2,
                0,
                pair_dir / first_system,
            )

            # Rest period
            if self.rest_between_runs > 0:
                time.sleep(self.rest_between_runs)

            # Run second system
            second_result = self._run_single(
                run_single_experiment,
                second_system,
                pair_idx * 2 + 1,
                1,
                pair_dir / second_system,
            )

            # Create paired result
            if first_system == "baseline":
                baseline_result = first_result
                cedar_result = second_result
                order = "baseline_first"
            else:
                baseline_result = second_result
                cedar_result = first_result
                order = "cedar_first"

            paired = PairedRunResult(
                pair_index=pair_idx,
                order=order,
                baseline=baseline_result,
                cedar=cedar_result,
                rest_seconds=self.rest_between_runs,
            )
            self._paired_runs.append(paired)

            # Save intermediate results
            (pair_dir / "pair_result.json").write_text(paired.model_dump_json(indent=2))

        # Collect metadata
        metadata = self._metadata_collector.collect_all(
            experiment_name=self.experiment_name,
            config=config,
            workload_path=workload_path,
            run_order=self.ordering,
            warmup_iterations=warmup_iterations,
            measurement_iterations=measurement_iterations,
            seed=self.seed,
        )

        # Compute aggregates
        aggregate = self._compute_aggregate()
        category_comparisons = self._compute_category_comparisons()

        result = MultiRunResult(
            experiment_name=self.experiment_name,
            ordering_strategy=self.ordering,
            paired_runs=self._paired_runs,
            aggregate=aggregate,
            category_comparisons=category_comparisons,
            metadata=metadata,
        )

        # Save results
        result.save(output_dir)

        return result

    def _run_single(
        self,
        run_func: Callable[[str, int, Path], dict[str, Any]],
        system: str,
        run_index: int,
        order_in_pair: int,
        output_path: Path,
    ) -> RunResult:
        """Execute a single run and collect results."""
        output_path.mkdir(parents=True, exist_ok=True)

        start_time = datetime.now()

        # Run the experiment
        results = run_func(system, run_index, output_path)

        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()

        # Extract latencies and compute metrics
        latencies = self._extract_latencies(results, system)
        metrics = compute_run_level_metrics(
            latencies,
            run_id=f"{system}_{run_index:03d}",
            system=system,
            duration_seconds=duration,
        )

        # Extract category metrics
        category_metrics = self._extract_category_metrics(results, system)

        # Save results
        results_path = output_path / "results.json"
        results_path.write_text(json.dumps(results, indent=2))

        return RunResult(
            run_id=f"{system}_{run_index:03d}",
            run_index=run_index,
            system=system,
            order_in_pair=order_in_pair,
            start_time=start_time.isoformat(),
            end_time=end_time.isoformat(),
            duration_seconds=duration,
            results_path=str(results_path),
            metrics=metrics,
            category_metrics=category_metrics,
        )

    def _extract_latencies(self, results: dict[str, Any], system: str) -> list[float]:
        """Extract latency values from results."""
        if system in results:
            return [r["latency_ms"] for r in results[system] if "latency_ms" in r]
        return []

    def _extract_category_metrics(
        self,
        results: dict[str, Any],
        system: str,
    ) -> dict[str, dict[str, float]]:
        """Extract per-category metrics."""
        if system not in results:
            return {}

        by_category: dict[str, list[float]] = {}
        for r in results[system]:
            cat = r.get("category") or r.get("action", "unknown")
            by_category.setdefault(cat, []).append(r.get("latency_ms", 0))

        category_metrics = {}
        for cat, latencies in by_category.items():
            if latencies:
                stats = summary_stats(latencies)
                category_metrics[cat] = stats

        return category_metrics

    def _compute_aggregate(self) -> dict[str, Any]:
        """Compute aggregate statistics with CIs."""
        baseline_runs = [pr.baseline.metrics for pr in self._paired_runs]
        cedar_runs = [pr.cedar.metrics for pr in self._paired_runs]

        # Compare for each metric
        metrics = [
            "median_latency",
            "mean_latency",
            "p95_latency",
            "p99_latency",
            "qps",
        ]
        comparisons = {}

        for metric in metrics:
            comparisons[metric] = compare_systems(baseline_runs, cedar_runs, metric)

        # Also compute CIs for each system's metrics
        baseline_medians = [r.median_latency for r in baseline_runs]
        cedar_medians = [r.median_latency for r in cedar_runs]

        return {
            "ordering": self.ordering,
            "comparisons": comparisons,
            "baseline_summary": {
                "median_latency": bootstrap_ci_median(baseline_medians).model_dump(),
                "mean_latency": bootstrap_ci_median(
                    [r.mean_latency for r in baseline_runs]
                ).model_dump(),
                "p95_latency": bootstrap_ci_median(
                    [r.p95_latency for r in baseline_runs]
                ).model_dump(),
                "p99_latency": bootstrap_ci_median(
                    [r.p99_latency for r in baseline_runs]
                ).model_dump(),
                "qps": bootstrap_ci_median([r.qps for r in baseline_runs]).model_dump(),
            },
            "cedar_summary": {
                "median_latency": bootstrap_ci_median(cedar_medians).model_dump(),
                "mean_latency": bootstrap_ci_median(
                    [r.mean_latency for r in cedar_runs]
                ).model_dump(),
                "p95_latency": bootstrap_ci_median(
                    [r.p95_latency for r in cedar_runs]
                ).model_dump(),
                "p99_latency": bootstrap_ci_median(
                    [r.p99_latency for r in cedar_runs]
                ).model_dump(),
                "qps": bootstrap_ci_median([r.qps for r in cedar_runs]).model_dump(),
            },
        }

    def _compute_category_comparisons(self) -> dict[str, dict[str, Any]]:
        """Compute per-category comparisons with multiple comparison correction."""
        # Collect all categories
        all_categories = set()
        for pr in self._paired_runs:
            all_categories.update(pr.baseline.category_metrics.keys())
            all_categories.update(pr.cedar.category_metrics.keys())

        category_results = {}
        p_values = []
        category_order = []

        for cat in sorted(all_categories):
            baseline_medians = []
            cedar_medians = []

            for pr in self._paired_runs:
                b_stats = pr.baseline.category_metrics.get(cat, {})
                c_stats = pr.cedar.category_metrics.get(cat, {})

                if b_stats and c_stats:
                    baseline_medians.append(b_stats.get("median", 0))
                    cedar_medians.append(c_stats.get("median", 0))

            if len(baseline_medians) >= 5:  # Need enough samples
                # Paired test
                test = wilcoxon_signed_rank_test(baseline_medians, cedar_medians)
                p_values.append(test.p_value)
                category_order.append(cat)

                # CIs
                baseline_ci = bootstrap_ci_median(baseline_medians)
                cedar_ci = bootstrap_ci_median(cedar_medians)

                # Effect sizes
                cliff = cliffs_delta(baseline_medians, cedar_medians)

                # Overhead
                b_med = statistics.median(baseline_medians)
                c_med = statistics.median(cedar_medians)
                from framework.stats import calculate_overhead_metrics

                oh = calculate_overhead_metrics(b_med, c_med, is_throughput=False)

                category_results[cat] = {
                    "n_pairs": len(baseline_medians),
                    "baseline_ci": baseline_ci.model_dump(),
                    "cedar_ci": cedar_ci.model_dump(),
                    "test": test.model_dump(),
                    "cliffs_delta": cliff.model_dump(),
                    "overhead_ms": c_med - b_med,
                    "overhead_pct": oh["overhead_pct"],
                    "overhead_factor": oh["overhead_factor"],
                }

        # Apply multiple comparison correction
        if p_values:
            correction = holm_bonferroni_correction(p_values)
            for i, cat in enumerate(category_order):
                if cat in category_results:
                    category_results[cat]["adjusted_p_value"] = (
                        correction.adjusted_p_values[i]
                    )
                    category_results[cat]["significant_after_correction"] = (
                        correction.significant[i]
                    )

        return category_results


def run_experiment_batch(
    experiment_func: Callable[[str, int, Path], dict[str, Any]],
    experiment_name: str,
    ordering: str = "abba",
    output_dir: Path | None = None,
    config: dict[str, Any] | None = None,
    workload_path: Path | None = None,
    warmup_iterations: int = 0,
    measurement_iterations: int = 0,
    rest_between_runs: float = 5.0,
    seed: int | None = None,
) -> MultiRunResult:
    """
    Convenience function to run a batch of experiments.

    Args:
        experiment_func: Function that runs a single experiment
        experiment_name: Name of the experiment
        ordering: Ordering strategy
        output_dir: Output directory
        config: Configuration for metadata
        workload_path: Workload file path
        warmup_iterations: Warmup count
        measurement_iterations: Measurement count
        rest_between_runs: Rest period in seconds
        seed: Random seed

    Returns:
        MultiRunResult with all results and statistics
    """
    orchestrator = MultiRunOrchestrator(
        experiment_name=experiment_name,
        ordering=ordering,
        rest_between_runs=rest_between_runs,
        seed=seed,
        output_base_dir=output_dir or Path("results"),
    )

    return orchestrator.run(
        run_single_experiment=experiment_func,
        config=config,
        workload_path=workload_path,
        warmup_iterations=warmup_iterations,
        measurement_iterations=measurement_iterations,
    )
