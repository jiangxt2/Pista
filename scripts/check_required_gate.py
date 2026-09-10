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
    "domain-it",
    "vulnerability-scan",
}


def gate_failures(needs: dict[str, Any]) -> dict[str, str]:
    failures = {name: "missing" for name in sorted(EXPECTED_JOBS) if name not in needs}
    failures.update(
        {
            name: str(data.get("result", "missing-result"))
            for name, data in needs.items()
            if name in EXPECTED_JOBS and data.get("result") != "success"
        }
    )
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
