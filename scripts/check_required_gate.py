#!/usr/bin/env python3

"""Validate that every required CI dependency exists and completed successfully."""

from __future__ import annotations

import argparse
import json
import os
from typing import Any


EXPECTED_JOBS = {
    "changes-and-policy",
    "content-safety",
    "language-and-docs",
    "license-and-notice",
    "policy-tests",
    "jvm-quality",
    "assembly-contract",
    "clickhouse-it",
    "doris-it",
    "iceberg-it",
    "support-it",
    "submitter-it",
    "vulnerability-scan",
}


def gate_failures(needs: dict[str, Any]) -> dict[str, str]:
    failures = {name: "missing" for name in sorted(EXPECTED_JOBS) if name not in needs}
    classify = needs.get("changes-and-policy", {})
    outputs = classify.get("outputs", {}) if classify.get("result") == "success" else {}
    selected_jobs = {
        f"{name}-it": name
        for name in ("clickhouse", "doris", "iceberg", "support", "submitter")
    }
    for name in ("jvm", "policy", "assembly", *selected_jobs.values()):
        if outputs.get(name) not in ("true", "false"):
            failures[f"classification.{name}"] = "missing or invalid"
    if outputs.get("submitter") == "true" and outputs.get("assembly") != "true":
        failures["classification.assembly"] = "required by submitter"
    for name, data in needs.items():
        if name not in EXPECTED_JOBS:
            continue
        result = data.get("result", "missing-result")
        allowed_skip = (
            name in selected_jobs and outputs.get(selected_jobs[name]) == "false"
        )
        if result != "success" and not (allowed_skip and result == "skipped"):
            failures[name] = str(result)
    return failures


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--needs-json")
    args = parser.parse_args()
    raw = args.needs_json or os.environ.get("NEEDS_JSON")
    if not raw:
        print("Required CI job data is missing.")
        return 1
    needs = json.loads(raw)
    failures = gate_failures(needs)
    if failures:
        print("Required CI jobs did not succeed:", failures)
        return 1
    print("All required CI jobs succeeded.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
