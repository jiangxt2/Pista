#!/usr/bin/env python3

"""Inspect the Pista assembly JAR for public-release contract violations."""

from __future__ import annotations

import argparse
import re
import zipfile
from collections import Counter
from pathlib import Path

from check_public_content import RULES


CJK = re.compile(r"[\u3400-\u4dbf\u4e00-\u9fff\uf900-\ufaff]")
SIGNATURE_SUFFIXES = (".SF", ".RSA", ".DSA")
TEXT_SUFFIXES = (
    ".class",
    ".conf",
    ".csv",
    ".html",
    ".json",
    ".md",
    ".properties",
    ".sql",
    ".txt",
    ".xml",
    ".yml",
    ".yaml",
)
PSL_CJK_ENTRIES = {
    "mozilla/public-suffix-list.txt",
    "public_suffix_list.dat",
}
ASSEMBLY_RULE_IDS = {
    "identity-zh",
    "identity-en",
    "identity-acronym",
    "identity-service-number",
    "brand-01",
    "brand-cloud",
    "subsidiary-zh",
    "business-province-id",
    "business-province-option",
    "business-statistics-date",
    "business-province-code",
    "identity-domain",
    "internal-workdir",
    "private-key",
    "aws-access-key",
    "github-token",
    "internal-domain",
}
PROHIBITED_CONTENT = tuple(
    (
        rule.rule_id,
        re.compile(
            rule.pattern.pattern.encode("utf-8"), rule.pattern.flags & ~re.UNICODE
        ),
    )
    for rule in RULES
    if rule.rule_id in ASSEMBLY_RULE_IDS
)
PROJECT_ONLY_RULES = {
    "identity-service-number",
    "business-province-id",
    "business-statistics-date",
    "business-province-code",
}
REQUIRED_SERVICES = {
    "META-INF/services/com.pista.spark.sql.execution.datasources.reader.DataReader": {
        "com.pista.spark.sql.connector.doris.DorisReader",
        "com.pista.spark.sql.connector.clickhouse.ClickHouseReader",
    },
    "META-INF/services/com.pista.spark.sql.execution.datasources.writer.DataWriter": {
        "com.pista.spark.sql.connector.doris.DorisWriter",
        "com.pista.spark.sql.connector.clickhouse.ClickHouseWriter",
    },
}
PROJECT_COMPLIANCE = {
    "META-INF/pista/LICENSE": "LICENSE",
    "META-INF/pista/NOTICE": "NOTICE",
    "META-INF/pista/THIRD_PARTY_NOTICES": "THIRD_PARTY_NOTICES",
}
DEPENDENCY_COMPLIANCE = {
    "META-INF/LICENSE",
    "META-INF/NOTICE",
    "META-INF/DEPENDENCIES",
}


def find_jar(root: Path) -> Path:
    candidates = sorted(
        path
        for path in (root / "pista-assembly" / "target").glob("pista-*.jar")
        if not any(
            marker in path.name
            for marker in ("original-", "-sources", "-javadoc", "-tests")
        )
    )
    if len(candidates) != 1:
        raise RuntimeError(f"Expected one assembly JAR, found {len(candidates)}")
    return candidates[0]


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", type=Path, default=Path.cwd())
    args = parser.parse_args()
    jar = find_jar(args.root.resolve())
    findings: list[str] = []
    with zipfile.ZipFile(jar) as archive:
        names = archive.namelist()
        duplicates = sorted(name for name, count in Counter(names).items() if count > 1)
        findings.extend(f"duplicate-entry: {name}" for name in duplicates)
        findings.extend(
            f"signature-entry: {name}"
            for name in names
            if name.upper().startswith("META-INF/")
            and name.upper().endswith(SIGNATURE_SUFFIXES)
        )
        findings.extend(
            f"test-entry: {name}"
            for name in names
            if "/src/test/" in name or name.startswith("src/test/")
        )
        for service, expected_providers in REQUIRED_SERVICES.items():
            if service not in names:
                findings.append(f"missing-service: {service}")
                continue
            providers = [
                line.strip()
                for line in archive.read(service).decode("utf-8").splitlines()
                if line.strip() and not line.lstrip().startswith("#")
            ]
            if len(providers) != len(set(providers)):
                findings.append(f"duplicate-service-provider: {service}")
            for provider in sorted(expected_providers - set(providers)):
                findings.append(f"missing-service-provider: {service}: {provider}")
        for compliance in sorted(DEPENDENCY_COMPLIANCE - set(names)):
            findings.append(f"missing-compliance-entry: {compliance}")
        for entry, source in PROJECT_COMPLIANCE.items():
            if entry not in names:
                findings.append(f"missing-project-compliance-entry: {entry}")
            elif archive.read(entry) != (args.root.resolve() / source).read_bytes():
                findings.append(f"project-compliance-content-mismatch: {entry}")
        for name in names:
            if name.endswith("/"):
                continue
            data = archive.read(name)
            for rule_id, pattern in PROHIBITED_CONTENT:
                if rule_id == "private-key" and name.lower().endswith(".class"):
                    continue
                if rule_id in PROJECT_ONLY_RULES and not name.startswith("com/pista/"):
                    continue
                if pattern.search(data):
                    findings.append(f"{rule_id}-entry: {name}")
            if not name.lower().endswith(TEXT_SUFFIXES):
                continue
            if name.lower().endswith(".class"):
                continue
            try:
                text = data.decode("utf-8")
            except UnicodeDecodeError:
                findings.append(f"non-utf8-text-entry: {name}")
                continue
            if CJK.search(text) and name not in PSL_CJK_ENTRIES:
                findings.append(f"unapproved-cjk-entry: {name}")
    for finding in findings:
        print(finding)
    if findings:
        return 1
    print(f"Assembly contract passed: {jar}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
