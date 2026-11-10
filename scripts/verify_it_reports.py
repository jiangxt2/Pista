#!/usr/bin/env python3
"""Check the ScalaTest discovery inventory against the selected shard's XML reports."""

from __future__ import annotations

import argparse
import base64
import collections
import sys
import xml.etree.ElementTree as ET
from pathlib import Path


SHARDS = {"clickhouse", "doris", "iceberg", "support", "submitter"}


def decoded(value: str) -> str:
    return base64.b64decode(value, validate=True).decode("utf-8")


def verify(root: Path, shard_name: str, directory: str, before: bool) -> None:
    expected: collections.Counter[tuple[str, str, str]] = collections.Counter()
    external: collections.Counter[str] = collections.Counter()
    inventories = sorted(root.glob(f"*/target/{directory}/discovered.tsv"))
    if not inventories:
        raise ValueError("No ScalaTest discovery inventories found")
    for inventory in inventories:
        for line in inventory.read_text(encoding="utf-8").splitlines():
            fields = line.split("\t")
            if fields[0] == "external" and len(fields) == 2:
                external[decoded(fields[1])] += 1
                continue
            if len(fields) != 3 or fields[0] not in SHARDS:
                raise ValueError(f"Invalid discovery record in {inventory}")
            expected[(fields[0], decoded(fields[1]), decoded(fields[2]))] += 1
    duplicates = [identity for identity, count in expected.items() if count != 1]
    cross_shard = collections.Counter((suite, test) for _, suite, test in expected)
    duplicates.extend(identity for identity, count in cross_shard.items() if count != 1)
    if duplicates:
        raise ValueError(f"Duplicate discovered integration tests: {duplicates}")
    if external != {
        "com.pista.spark.sql.batch.iceberg.SparkSQLSubmitterIcebergRestIT": 1
    }:
        raise ValueError(f"Unexpected external test inventory: {external}")
    selected = {(suite, test) for shard, suite, test in expected if shard == shard_name}
    if not selected:
        raise ValueError(f"No tests discovered for {shard_name}")
    if before:
        print(f"Discovered {len(selected)} tests for {shard_name}")
        return
    reports = sorted(root.glob(f"*/target/{directory}/TEST-*.xml"))
    actual: collections.Counter[tuple[str, str]] = collections.Counter()
    for report in reports:
        suite = ET.parse(report).getroot()
        if int(suite.get("errors", "0")) or int(suite.get("failures", "0")):
            raise ValueError(f"Failed or aborted suite in {report}")
        for test in suite.findall("testcase"):
            if any(
                test.find(status) is not None
                for status in ("skipped", "failure", "error")
            ):
                raise ValueError(f"Non-successful test in {report}: {test.get('name')}")
            actual[(test.get("classname", ""), test.get("name", ""))] += 1
    missing = selected - actual.keys()
    extra = actual.keys() - selected
    repeated = [identity for identity, count in actual.items() if count != 1]
    if missing or extra or repeated:
        raise ValueError(
            f"Test report mismatch: missing={missing}, extra={extra}, repeated={repeated}"
        )
    print(f"Verified {len(selected)} executed tests for {shard_name}")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("shard", choices=sorted(SHARDS))
    parser.add_argument("directory")
    parser.add_argument("--before", action="store_true")
    args = parser.parse_args()
    root = Path(__file__).resolve().parent.parent
    verify(root, args.shard, args.directory, args.before)
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except (ValueError, OSError, ET.ParseError) as exc:
        print(f"Integration test coverage verification failed: {exc}", file=sys.stderr)
        sys.exit(1)
