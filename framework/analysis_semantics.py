#!/usr/bin/env python3
"""
Semantic correctness analysis helpers.

USENIX-style paper artifacts:
- robustness_summary.csv: Summary of fail-closed, monotonicity, and consistency tests
- robustness_summary.tex: LaTeX table showing robustness guarantees
"""

from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Any


def extract_semantics_summary(results_json: Path) -> dict[str, Any] | None:
    """Extract semantic test summary."""
    if not results_json.exists():
        return None

    try:
        data = json.loads(results_json.read_text())
    except Exception:
        return None

    summary = data.get("overall_summary", {})
    if not summary:
        return None

    # Get property-specific results
    fail_closed = data.get("fail_closed_tests", {}).get("summary", {})
    monotonicity = data.get("monotonicity_tests", {}).get("summary", {})
    consistency = data.get("consistency_tests", {}).get("summary", {})

    return {
        "file": results_json.name,
        "test_type": data.get("test_type", "comprehensive"),
        "fail_closed_pass": bool(summary.get("fail_closed_pass", False)),
        "fail_closed_violations": int(fail_closed.get("total_violations", 0) or 0),
        "monotonicity_pass": bool(summary.get("monotonicity_pass", False)),
        "monotonicity_violations": int(monotonicity.get("violations", 0) or 0),
        "consistency_pass": bool(summary.get("consistency_pass", False)),
        "consistency_violations": len(consistency.get("violations", []) or []),
        "all_pass": bool(summary.get("all_tests_pass", False)),
        "total_violations": int(summary.get("total_violations", 0) or 0),
    }


def write_robustness_summary_csv(results: dict[str, Any], out_path: Path) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    header = ["property", "status", "violations"]
    rows = [
        {
            "property": "Fail-Closed",
            "status": "PASS" if results["fail_closed_pass"] else "FAIL",
            "violations": results["fail_closed_violations"],
        },
        {
            "property": "Monotonicity",
            "status": "PASS" if results["monotonicity_pass"] else "FAIL",
            "violations": results["monotonicity_violations"],
        },
        {
            "property": "Consistency",
            "status": "PASS" if results["consistency_pass"] else "FAIL",
            "violations": results["consistency_violations"],
        },
    ]
    with out_path.open("w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=header)
        w.writeheader()
        w.writerows(rows)


def write_robustness_summary_table_tex(results: dict[str, Any], out_path: Path) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)

    def fmt_status(p):
        return "\\checkmark" if p else "\\texttimes"

    lines = [
        "\\begin{tabular}{lcc}",
        "\\toprule",
        "Security Property & Status & Violations \\\\",
        "\\midrule",
        f"Fail-Closed (Secure Fallback) & {fmt_status(results['fail_closed_pass'])} & {results['fail_closed_violations']} \\\\",
        f"Monotonicity (No Privilege Escalation) & {fmt_status(results['monotonicity_pass'])} & {results['monotonicity_violations']} \\\\",
        f"Consistency (Deterministic Under Failure) & {fmt_status(results['consistency_pass'])} & {results['consistency_violations']} \\\\",
        "\\bottomrule",
        "\\end{tabular}",
    ]
    out_path.write_text("\n".join(lines))
