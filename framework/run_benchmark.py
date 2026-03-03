#!/usr/bin/env python3
"""
Runs identical workloads against both MySQL versions.
"""

import json
import subprocess
import sys
import time

import mysql.connector


def run_workload(mysql_config, workload_file):
    """Run workload against MySQL instance."""
    conn = mysql.connector.connect(**mysql_config)
    cursor = conn.cursor()

    results = []
    with open(workload_file) as f:
        for line in f:
            if line.strip() and not line.strip().startswith("--"):
                start = time.time()
                try:
                    cursor.execute(line)
                    if cursor.description:  # SELECT query
                        cursor.fetchall()
                    conn.commit()
                    elapsed = (time.time() - start) * 1000  # ms
                    results.append(
                        {"query": line.strip(), "latency_ms": elapsed, "success": True}
                    )
                except Exception as e:
                    elapsed = (time.time() - start) * 1000
                    results.append(
                        {
                            "query": line.strip(),
                            "latency_ms": elapsed,
                            "success": False,
                            "error": str(e),
                        }
                    )

    cursor.close()
    conn.close()
    return results


def compare_results(baseline_results, cedar_results):
    """Compare results from both systems."""
    # Calculate statistics
    baseline_latencies = [r["latency_ms"] for r in baseline_results if r["success"]]
    cedar_latencies = [r["latency_ms"] for r in cedar_results if r["success"]]

    def stats(latencies):
        if not latencies:
            return {}
        sorted_lat = sorted(latencies)
        return {
            "median": sorted_lat[len(sorted_lat) // 2],
            "p95": sorted_lat[int(len(sorted_lat) * 0.95)],
            "p99": sorted_lat[int(len(sorted_lat) * 0.99)],
            "mean": sum(latencies) / len(latencies),
        }

    baseline_stats = stats(baseline_latencies)
    cedar_stats = stats(cedar_latencies)

    overhead = {}
    for key in baseline_stats:
        overhead[key] = cedar_stats[key] - baseline_stats[key]
        overhead[f"{key}_percent"] = (overhead[key] / baseline_stats[key]) * 100

    return {"baseline": baseline_stats, "cedar": cedar_stats, "overhead": overhead}


if __name__ == "__main__":
    # Configuration
    baseline_config = {
        "host": "localhost",
        "user": "root",
        "password": "password",
        "database": "abac_test",
    }

    cedar_config = {
        "host": "localhost",
        "user": "root",
        "password": "password",
        "database": "abac_test",
        "port": 3307,  # Different port for modified MySQL
    }

    workload_file = sys.argv[1]

    # Setup: Translate authorization requirements
    print("Setting up baseline MySQL...")
    subprocess.run(
        ["python3", "translate_to_grants.py", "auth_spec.json"],
        stdout=subprocess.PIPE,
        text=True,
    )

    print("Setting up Cedar MySQL...")
    subprocess.run(["python3", "translate_to_cedar.py", "auth_spec.json"])

    # Run workloads
    print("Running baseline workload...")
    baseline_results = run_workload(baseline_config, workload_file)

    print("Running Cedar workload...")
    cedar_results = run_workload(cedar_config, workload_file)

    # Compare
    comparison = compare_results(baseline_results, cedar_results)
    print(json.dumps(comparison, indent=2))
