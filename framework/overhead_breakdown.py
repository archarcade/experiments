#!/usr/bin/env python3
"""
Overhead breakdown analysis for phase-level timing decomposition.

Implements:
- Request ID correlation between plugin and agent logs
- Phase timing aggregation (plugin, network, agent evaluation)
- Breakdown table generation with CIs

Reference: TRD Section 7.2 (E2) - Overhead breakdown (instrumentation-backed)
"""

from __future__ import annotations

import csv
import json
import statistics
import uuid
from pathlib import Path
from typing import Any

from pydantic import BaseModel, Field

from .stats import (
    ConfidenceInterval,
    bootstrap_ci_median,
    summary_stats,
)


class PhaseTimingRecord(BaseModel):
    """Timing record for a single request phase."""

    request_id: str
    timestamp_ms: float
    phase: str  # e.g., "context_extraction", "network_request", "agent_evaluation"
    duration_ms: float
    metadata: dict[str, Any] = Field(default_factory=dict)


class RequestTimingRecord(BaseModel):
    """Complete timing record for a single authorization request."""

    request_id: str
    total_latency_ms: float

    # Plugin-side timings
    context_extraction_ms: float = 0.0
    request_serialization_ms: float = 0.0
    network_request_ms: float = 0.0  # Round-trip time
    response_parse_ms: float = 0.0
    decision_enforcement_ms: float = 0.0

    # Agent-side timings
    agent_request_parse_ms: float = 0.0
    agent_policy_evaluation_ms: float = 0.0
    agent_response_serialize_ms: float = 0.0

    # Metadata
    query_type: str = ""
    user: str = ""
    table: str = ""
    decision: str = ""  # "allow" or "deny"
    timestamp: str = ""

    def plugin_overhead_ms(self) -> float:
        """Total plugin-side overhead (excluding network)."""
        return (
            self.context_extraction_ms
            + self.request_serialization_ms
            + self.response_parse_ms
            + self.decision_enforcement_ms
        )

    def agent_overhead_ms(self) -> float:
        """Total agent-side overhead."""
        return (
            self.agent_request_parse_ms
            + self.agent_policy_evaluation_ms
            + self.agent_response_serialize_ms
        )

    def total_authorization_ms(self) -> float:
        """Total authorization overhead (plugin + network + agent)."""
        return self.plugin_overhead_ms() + self.network_request_ms


class PhaseBreakdown(BaseModel):
    """Breakdown of timing for a single phase across multiple requests."""

    phase_name: str
    n_samples: int
    mean_ms: float
    median_ms: float
    std_ms: float
    p95_ms: float
    p99_ms: float
    ci: ConfidenceInterval | None = None
    share_of_total_pct: float = 0.0
    share_ci: ConfidenceInterval | None = None


class OverheadBreakdownResult(BaseModel):
    """Complete overhead breakdown analysis result."""

    n_requests: int
    phases: list[PhaseBreakdown]
    total_authorization_overhead: PhaseBreakdown
    validation_result: dict[str, Any] = Field(default_factory=dict)


def generate_request_id() -> str:
    """Generate a unique request ID for correlation."""
    return str(uuid.uuid4())[:8]


class OverheadBreakdownAnalyzer:
    """
    Analyzes overhead breakdown from plugin and agent timing logs.

    The analyzer correlates timing data from:
    1. Plugin logs (context extraction, serialization, network, parsing, enforcement)
    2. Agent logs (request parsing, policy evaluation, response serialization)

    Using request IDs to join events across systems.
    """

    def __init__(self, tolerance_pct: float = 10.0):
        """
        Initialize analyzer.

        Args:
            tolerance_pct: Acceptable difference between sum of phases and total (%)
        """
        self.tolerance_pct = tolerance_pct
        self._timing_records: list[RequestTimingRecord] = []

    def load_plugin_timings(self, path: Path) -> int:
        """
        Load plugin-side timing data from CSV or JSON.

        Expected CSV columns:
        - request_id, total_latency_ms, context_extraction_ms, request_serialization_ms,
          network_request_ms, response_parse_ms, decision_enforcement_ms, query_type, user, table

        Returns:
            Number of records loaded
        """
        if not path.exists():
            return 0

        if path.suffix == ".csv":
            return self._load_plugin_csv(path)
        else:
            return self._load_plugin_json(path)

    def _load_plugin_csv(self, path: Path) -> int:
        """Load plugin timings from CSV."""
        count = 0
        with path.open() as f:
            reader = csv.DictReader(f)
            for row in reader:
                record = RequestTimingRecord(
                    request_id=row.get("request_id", generate_request_id()),
                    total_latency_ms=float(row.get("total_latency_ms", 0)),
                    context_extraction_ms=float(row.get("context_extraction_ms", 0)),
                    request_serialization_ms=float(
                        row.get("request_serialization_ms", 0)
                    ),
                    network_request_ms=float(row.get("network_request_ms", 0)),
                    response_parse_ms=float(row.get("response_parse_ms", 0)),
                    decision_enforcement_ms=float(
                        row.get("decision_enforcement_ms", 0)
                    ),
                    query_type=row.get("query_type", ""),
                    user=row.get("user", ""),
                    table=row.get("table", ""),
                    decision=row.get("decision", ""),
                    timestamp=row.get("timestamp", ""),
                )
                self._timing_records.append(record)
                count += 1
        return count

    def _load_plugin_json(self, path: Path) -> int:
        """Load plugin timings from JSON."""
        data = json.loads(path.read_text())
        records = data if isinstance(data, list) else data.get("records", [])

        count = 0
        for item in records:
            record = RequestTimingRecord(
                request_id=item.get("request_id", generate_request_id()),
                total_latency_ms=float(item.get("total_latency_ms", 0)),
                context_extraction_ms=float(item.get("context_extraction_ms", 0)),
                request_serialization_ms=float(item.get("request_serialization_ms", 0)),
                network_request_ms=float(item.get("network_request_ms", 0)),
                response_parse_ms=float(item.get("response_parse_ms", 0)),
                decision_enforcement_ms=float(item.get("decision_enforcement_ms", 0)),
                query_type=item.get("query_type", ""),
                user=item.get("user", ""),
                table=item.get("table", ""),
                decision=item.get("decision", ""),
                timestamp=item.get("timestamp", ""),
            )
            self._timing_records.append(record)
            count += 1
        return count

    def load_agent_timings(self, path: Path) -> int:
        """
        Load agent-side timing data and correlate with plugin timings.

        Expected CSV columns:
        - request_id, agent_request_parse_ms, agent_policy_evaluation_ms, agent_response_serialize_ms

        Returns:
            Number of records updated
        """
        if not path.exists():
            return 0

        # Create lookup by request_id
        records_by_id = {r.request_id: r for r in self._timing_records}

        count = 0
        if path.suffix == ".csv":
            with path.open() as f:
                reader = csv.DictReader(f)
                for row in reader:
                    req_id = row.get("request_id")
                    if req_id and req_id in records_by_id:
                        rec = records_by_id[req_id]
                        rec.agent_request_parse_ms = float(
                            row.get("agent_request_parse_ms", 0)
                        )
                        rec.agent_policy_evaluation_ms = float(
                            row.get("agent_policy_evaluation_ms", 0)
                        )
                        rec.agent_response_serialize_ms = float(
                            row.get("agent_response_serialize_ms", 0)
                        )
                        count += 1
        else:
            data = json.loads(path.read_text())
            records = data if isinstance(data, list) else data.get("records", [])
            for item in records:
                req_id = item.get("request_id")
                if req_id and req_id in records_by_id:
                    rec = records_by_id[req_id]
                    rec.agent_request_parse_ms = float(
                        item.get("agent_request_parse_ms", 0)
                    )
                    rec.agent_policy_evaluation_ms = float(
                        item.get("agent_policy_evaluation_ms", 0)
                    )
                    rec.agent_response_serialize_ms = float(
                        item.get("agent_response_serialize_ms", 0)
                    )
                    count += 1

        return count

    def add_timing_record(self, record: RequestTimingRecord) -> None:
        """Add a timing record directly."""
        self._timing_records.append(record)

    def analyze(self) -> OverheadBreakdownResult:
        """
        Analyze overhead breakdown across all loaded timing records.

        Returns:
            OverheadBreakdownResult with phase-level breakdowns and validation
        """
        if not self._timing_records:
            return OverheadBreakdownResult(
                n_requests=0,
                phases=[],
                total_authorization_overhead=PhaseBreakdown(
                    phase_name="total_authorization",
                    n_samples=0,
                    mean_ms=0,
                    median_ms=0,
                    std_ms=0,
                    p95_ms=0,
                    p99_ms=0,
                ),
            )

        # Collect phase timings
        phase_names = [
            ("context_extraction_ms", "Context Extraction"),
            ("request_serialization_ms", "Request Serialization"),
            ("network_request_ms", "Network Round-Trip"),
            ("response_parse_ms", "Response Parsing"),
            ("decision_enforcement_ms", "Decision Enforcement"),
            ("agent_request_parse_ms", "Agent: Request Parse"),
            ("agent_policy_evaluation_ms", "Agent: Policy Evaluation"),
            ("agent_response_serialize_ms", "Agent: Response Serialize"),
        ]

        phase_breakdowns = []
        total_auth_times = [r.total_authorization_ms() for r in self._timing_records]
        total_median = statistics.median(total_auth_times) if total_auth_times else 0

        for attr_name, display_name in phase_names:
            values = [getattr(r, attr_name) for r in self._timing_records]
            values = [v for v in values if v > 0]  # Filter zeros

            if values:
                stats = summary_stats(values)
                ci = bootstrap_ci_median(values)

                # Share of total
                median_val = stats["median"]
                share_pct = (median_val / total_median * 100) if total_median > 0 else 0

                # CI for share (bootstrap)
                share_values = [
                    v / t * 100 if t > 0 else 0
                    for v, t in zip(values, total_auth_times[: len(values)])
                ]
                share_ci = bootstrap_ci_median(share_values) if share_values else None

                breakdown = PhaseBreakdown(
                    phase_name=display_name,
                    n_samples=len(values),
                    mean_ms=stats["mean"],
                    median_ms=stats["median"],
                    std_ms=stats["std"],
                    p95_ms=stats["p95"],
                    p99_ms=stats["p99"],
                    ci=ci,
                    share_of_total_pct=share_pct,
                    share_ci=share_ci,
                )
                phase_breakdowns.append(breakdown)

        # Total authorization overhead
        total_stats = summary_stats(total_auth_times)
        total_ci = bootstrap_ci_median(total_auth_times)
        total_breakdown = PhaseBreakdown(
            phase_name="Total Authorization Overhead",
            n_samples=len(total_auth_times),
            mean_ms=total_stats["mean"],
            median_ms=total_stats["median"],
            std_ms=total_stats["std"],
            p95_ms=total_stats["p95"],
            p99_ms=total_stats["p99"],
            ci=total_ci,
            share_of_total_pct=100.0,
        )

        # Validation: check that sum of phases ≈ total
        validation = self._validate_totals(phase_breakdowns, total_median)

        return OverheadBreakdownResult(
            n_requests=len(self._timing_records),
            phases=phase_breakdowns,
            total_authorization_overhead=total_breakdown,
            validation_result=validation,
        )

    def _validate_totals(
        self,
        phases: list[PhaseBreakdown],
        total_median: float,
    ) -> dict[str, Any]:
        """Validate that phase sum matches total within tolerance."""
        # Sum plugin phases (exclude agent as they're inside network time)
        plugin_phases = [
            "Context Extraction",
            "Request Serialization",
            "Network Round-Trip",
            "Response Parsing",
            "Decision Enforcement",
        ]
        sum_phases = sum(p.median_ms for p in phases if p.phase_name in plugin_phases)

        difference_ms = abs(sum_phases - total_median)
        difference_pct = (difference_ms / total_median * 100) if total_median > 0 else 0

        valid = difference_pct <= self.tolerance_pct

        return {
            "sum_of_phases_ms": sum_phases,
            "total_median_ms": total_median,
            "difference_ms": difference_ms,
            "difference_pct": difference_pct,
            "tolerance_pct": self.tolerance_pct,
            "valid": valid,
            "message": "Sum matches total within tolerance"
            if valid
            else f"Warning: sum differs from total by {difference_pct:.1f}%",
        }

    def generate_breakdown_csv(self, output_path: Path) -> None:
        """Generate CSV with breakdown statistics."""
        result = self.analyze()

        output_path.parent.mkdir(parents=True, exist_ok=True)
        with output_path.open("w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(
                [
                    "phase",
                    "n_samples",
                    "mean_ms",
                    "median_ms",
                    "std_ms",
                    "p95_ms",
                    "p99_ms",
                    "ci_lower",
                    "ci_upper",
                    "share_pct",
                ]
            )

            for phase in result.phases:
                ci_lower = phase.ci.lower if phase.ci else 0
                ci_upper = phase.ci.upper if phase.ci else 0
                writer.writerow(
                    [
                        phase.phase_name,
                        phase.n_samples,
                        f"{phase.mean_ms:.4f}",
                        f"{phase.median_ms:.4f}",
                        f"{phase.std_ms:.4f}",
                        f"{phase.p95_ms:.4f}",
                        f"{phase.p99_ms:.4f}",
                        f"{ci_lower:.4f}",
                        f"{ci_upper:.4f}",
                        f"{phase.share_of_total_pct:.2f}",
                    ]
                )

            # Add total row
            total = result.total_authorization_overhead
            ci_lower = total.ci.lower if total.ci else 0
            ci_upper = total.ci.upper if total.ci else 0
            writer.writerow(
                [
                    "TOTAL",
                    total.n_samples,
                    f"{total.mean_ms:.4f}",
                    f"{total.median_ms:.4f}",
                    f"{total.std_ms:.4f}",
                    f"{total.p95_ms:.4f}",
                    f"{total.p99_ms:.4f}",
                    f"{ci_lower:.4f}",
                    f"{ci_upper:.4f}",
                    "100.00",
                ]
            )

    def generate_breakdown_latex(self, output_path: Path) -> None:
        """Generate LaTeX table with breakdown statistics."""
        result = self.analyze()

        lines = [
            "\\begin{tabular}{lrrrrr}",
            "\\toprule",
            "Phase & Median (ms) & 95\\% CI & p95 (ms) & Share (\\%) \\\\",
            "\\midrule",
        ]

        for phase in result.phases:
            ci_str = (
                f"[{phase.ci.lower:.2f}, {phase.ci.upper:.2f}]" if phase.ci else "—"
            )
            lines.append(
                f"{phase.phase_name} & {phase.median_ms:.2f} & {ci_str} & "
                f"{phase.p95_ms:.2f} & {phase.share_of_total_pct:.1f}\\% \\\\"
            )

        lines.append("\\midrule")
        total = result.total_authorization_overhead
        ci_str = f"[{total.ci.lower:.2f}, {total.ci.upper:.2f}]" if total.ci else "—"
        lines.append(
            f"\\textbf{{Total}} & \\textbf{{{total.median_ms:.2f}}} & {ci_str} & "
            f"\\textbf{{{total.p95_ms:.2f}}} & \\textbf{{100\\%}} \\\\"
        )

        lines.extend(
            [
                "\\bottomrule",
                "\\end{tabular}",
            ]
        )

        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text("\n".join(lines))

    def generate_raw_csv(self, output_path: Path) -> None:
        """Export raw timing records to CSV."""
        output_path.parent.mkdir(parents=True, exist_ok=True)

        with output_path.open("w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(
                [
                    "request_id",
                    "query_type",
                    "user",
                    "table",
                    "decision",
                    "total_latency_ms",
                    "context_extraction_ms",
                    "request_serialization_ms",
                    "network_request_ms",
                    "response_parse_ms",
                    "decision_enforcement_ms",
                    "agent_request_parse_ms",
                    "agent_policy_evaluation_ms",
                    "agent_response_serialize_ms",
                    "plugin_overhead_ms",
                    "agent_overhead_ms",
                    "total_authorization_ms",
                ]
            )

            for rec in self._timing_records:
                writer.writerow(
                    [
                        rec.request_id,
                        rec.query_type,
                        rec.user,
                        rec.table,
                        rec.decision,
                        f"{rec.total_latency_ms:.4f}",
                        f"{rec.context_extraction_ms:.4f}",
                        f"{rec.request_serialization_ms:.4f}",
                        f"{rec.network_request_ms:.4f}",
                        f"{rec.response_parse_ms:.4f}",
                        f"{rec.decision_enforcement_ms:.4f}",
                        f"{rec.agent_request_parse_ms:.4f}",
                        f"{rec.agent_policy_evaluation_ms:.4f}",
                        f"{rec.agent_response_serialize_ms:.4f}",
                        f"{rec.plugin_overhead_ms():.4f}",
                        f"{rec.agent_overhead_ms():.4f}",
                        f"{rec.total_authorization_ms():.4f}",
                    ]
                )


def create_simulated_breakdown_data(
    n_requests: int = 1000,
    mean_total_ms: float = 5.0,
    seed: int | None = None,
) -> OverheadBreakdownAnalyzer:
    """
    Create simulated breakdown data for testing/demonstration.

    Generates realistic timing distributions based on typical overhead patterns.
    """
    import random

    if seed is not None:
        random.seed(seed)

    analyzer = OverheadBreakdownAnalyzer()

    for i in range(n_requests):
        # Simulate realistic phase timings
        # Context extraction: ~10% of total
        context = random.gauss(mean_total_ms * 0.10, mean_total_ms * 0.02)
        context = max(0.01, context)

        # Request serialization: ~5% of total
        serialization = random.gauss(mean_total_ms * 0.05, mean_total_ms * 0.01)
        serialization = max(0.01, serialization)

        # Network round-trip: ~60% of total (includes agent time)
        network = random.gauss(mean_total_ms * 0.60, mean_total_ms * 0.10)
        network = max(0.1, network)

        # Response parse: ~5% of total
        response_parse = random.gauss(mean_total_ms * 0.05, mean_total_ms * 0.01)
        response_parse = max(0.01, response_parse)

        # Decision enforcement: ~20% of total
        enforcement = random.gauss(mean_total_ms * 0.20, mean_total_ms * 0.03)
        enforcement = max(0.01, enforcement)

        # Agent-side (inside network time)
        # Request parse: ~10% of network
        agent_parse = network * 0.10 * random.uniform(0.8, 1.2)

        # Policy evaluation: ~70% of network
        agent_eval = network * 0.70 * random.uniform(0.8, 1.2)

        # Response serialize: ~10% of network
        agent_serialize = network * 0.10 * random.uniform(0.8, 1.2)

        total = context + serialization + network + response_parse + enforcement

        record = RequestTimingRecord(
            request_id=f"req_{i:06d}",
            total_latency_ms=total * random.uniform(1.0, 1.2),  # Add some variance
            context_extraction_ms=context,
            request_serialization_ms=serialization,
            network_request_ms=network,
            response_parse_ms=response_parse,
            decision_enforcement_ms=enforcement,
            agent_request_parse_ms=agent_parse,
            agent_policy_evaluation_ms=agent_eval,
            agent_response_serialize_ms=agent_serialize,
            query_type=random.choice(["SELECT", "INSERT", "UPDATE", "DELETE"]),
            user=f"user_{random.randint(1, 10)}",
            table=f"table_{random.randint(1, 5)}",
            decision=random.choice(["allow", "allow", "allow", "deny"]),  # 75% allow
        )
        analyzer.add_timing_record(record)

    return analyzer
