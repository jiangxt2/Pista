#!/usr/bin/env python3

"""Classify changed paths for CI without relying on path-filtered required jobs."""

from __future__ import annotations

import argparse
import os
import subprocess
from pathlib import Path


DOMAINS = {
    "jvm": (
        "pom.xml",
        "pista-",
        "examples/",
        "shell/",
        "sqls/",
    ),
    "docs": (
        "README.md",
        "CONTRIBUTING.md",
        "LICENSE",
        "NOTICE",
        "THIRD_PARTY_NOTICES",
        "docs/",
        ".github/",
    ),
    "assembly": (
        "pom.xml",
        "LICENSE",
        "NOTICE",
        "THIRD_PARTY_NOTICES",
        "pista-assembly/",
        "pista-",
        "scripts/check_assembly.py",
    ),
    "clickhouse": (
        "pista-clickhouse-meta/",
        "pista-connector/src/main/scala/com/pista/spark/sql/connector/clickhouse/",
        "pista-connector/src/test/scala/com/pista/spark/sql/connector/clickhouse/",
    ),
    "doris": (
        "pista-doris-meta/",
        "pista-connector/src/main/scala/com/pista/spark/sql/connector/doris/",
        "pista-connector/src/test/scala/com/pista/spark/sql/connector/doris/",
    ),
    "iceberg": (
        "pista-connector/src/main/scala/com/pista/spark/sql/connector/iceberg/",
        "pista-connector/src/test/scala/com/pista/spark/sql/connector/iceberg/",
        "pista-batch/src/test/scala/com/pista/spark/sql/batch/iceberg/",
    ),
    "streaming": ("pista-streaming/",),
    "submitter": (
        "pista-batch/",
        "pista-assembly/",
        "shell/",
        "scripts/prepare_hosted_runner.sh",
    ),
}

POLICY_PREFIXES = (".github/workflows/", "scripts/")
TARGET_DOMAINS = ("clickhouse", "doris", "iceberg")
CONNECTOR_PREFIX = "pista-connector/"
DOMAIN_ALL_PREFIXES = (
    "pista-test-common/",
    "scripts/prepare_hosted_runner.sh",
    "scripts/run_domain_it.sh",
)


def git_lines(root: Path, *args: str) -> list[str]:
    result = subprocess.run(
        ["git", *args], cwd=root, check=True, capture_output=True, text=True
    )
    return [line for line in result.stdout.splitlines() if line]


def changed_files(root: Path, base: str | None, head: str) -> list[str]:
    if not base or set(base) == {"0"}:
        return git_lines(root, "ls-files")
    subprocess.run(
        ["git", "cat-file", "-e", f"{base}^{{commit}}"], cwd=root, check=True
    )
    return git_lines(root, "diff", "--name-only", f"{base}...{head}")


def matches(path: str, prefixes: tuple[str, ...]) -> bool:
    return any(path == prefix or path.startswith(prefix) for prefix in prefixes)


def classify_paths(paths: list[str]) -> dict[str, bool]:
    values = {
        domain: any(matches(path, prefixes) for path in paths)
        for domain, prefixes in DOMAINS.items()
    }
    values["policy"] = any(matches(path, POLICY_PREFIXES) for path in paths)
    values["domain_all"] = any(matches(path, DOMAIN_ALL_PREFIXES) for path in paths)
    values["connector_all"] = any(
        path.startswith(CONNECTOR_PREFIX)
        and not any(matches(path, DOMAINS[domain]) for domain in TARGET_DOMAINS)
        for path in paths
    )
    values["domain_it"] = (
        values["domain_all"]
        or values["connector_all"]
        or any(values[name] for name in TARGET_DOMAINS)
    )
    values["assembly"] = (
        values["assembly"] or values["domain_it"] or values["submitter"]
    )
    values["any"] = bool(paths)
    return values


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--base")
    parser.add_argument("--head", default="HEAD")
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    root = Path.cwd()
    paths = changed_files(root, args.base, args.head)
    values = classify_paths(paths)

    lines = [f"{name}={str(value).lower()}" for name, value in sorted(values.items())]
    lines.append(f"changed_count={len(paths)}")
    output = args.output or (
        Path(os.environ["GITHUB_OUTPUT"]) if "GITHUB_OUTPUT" in os.environ else None
    )
    if output:
        with output.open("a", encoding="utf-8") as handle:
            handle.write("\n".join(lines) + "\n")
    else:
        print("\n".join(lines))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
