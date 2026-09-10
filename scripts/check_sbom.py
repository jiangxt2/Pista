#!/usr/bin/env python3

"""Validate generated CycloneDX candidate SBOM files."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

from check_language import CJK
from check_public_content import RULES


EXPECTED_FILES = {
    "pista-jvm.cdx.json",
}
NON_CONTENT_FIELDS = {"bom-ref", "dependsOn", "purl", "ref", "version"}
PROVIDED_JVM_PURL_PREFIXES = (
    "pkg:maven/org.apache.hadoop/",
    "pkg:maven/org.apache.spark/",
    "pkg:maven/org.scala-lang/",
)


def content_strings(value: Any, field: str | None = None) -> list[str]:
    if isinstance(value, dict):
        return [
            text for key, child in value.items() for text in content_strings(child, key)
        ]
    if isinstance(value, list):
        return [text for child in value for text in content_strings(child, field)]
    if isinstance(value, str) and field not in NON_CONTENT_FIELDS:
        return [value]
    return []


def validate_sbom(path: Path) -> list[str]:
    try:
        source = path.read_text(encoding="utf-8")
        document: dict[str, Any] = json.loads(source)
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exception:
        return [f"invalid-json: {path}: {exception.__class__.__name__}"]

    findings: list[str] = []
    scan_source = "\n".join(content_strings(document))
    if CJK.search(scan_source):
        findings.append(f"unapproved-cjk: {path}")
    for rule in RULES:
        if rule.pattern.search(scan_source):
            findings.append(f"{rule.rule_id}: {path}")
    if document.get("bomFormat") != "CycloneDX":
        findings.append(f"invalid-format: {path}")
    if not document.get("specVersion"):
        findings.append(f"missing-spec-version: {path}")
    components = document.get("components")
    if not isinstance(components, list) or not components:
        findings.append(f"missing-components: {path}")
        return findings
    for index, component in enumerate(components):
        if not isinstance(component, dict):
            findings.append(f"invalid-component: {path}:{index}")
            continue
        if not component.get("name") or not component.get("version"):
            findings.append(f"incomplete-component: {path}:{index}")
        if path.name == "pista-jvm.cdx.json":
            purl = component.get("purl")
            if isinstance(purl, str) and purl.startswith(PROVIDED_JVM_PURL_PREFIXES):
                findings.append(f"provided-jvm-component: {path}:{purl}")
    return findings


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("directory", type=Path, nargs="?", default=Path("target/sbom"))
    args = parser.parse_args()
    missing = sorted(
        name for name in EXPECTED_FILES if not (args.directory / name).is_file()
    )
    findings = [f"missing-sbom: {args.directory / name}" for name in missing]
    for name in sorted(EXPECTED_FILES - set(missing)):
        findings.extend(validate_sbom(args.directory / name))
    for finding in findings:
        print(finding)
    if findings:
        return 1
    print(f"SBOM validation passed: {args.directory}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
