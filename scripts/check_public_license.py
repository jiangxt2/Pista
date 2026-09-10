#!/usr/bin/env python3

"""Validate Apache-2.0 licensing, provenance notices, and source identifiers."""

from __future__ import annotations

import hashlib
import subprocess
from pathlib import Path


REQUIRED_FILES = (
    "LICENSE",
    "NOTICE",
    "THIRD_PARTY_NOTICES",
    "README.md",
    "CONTRIBUTING.md",
    "SECURITY.md",
    "CODE_OF_CONDUCT.md",
    "GOVERNANCE.md",
    "MAINTAINERS.md",
    "CHANGELOG.md",
    "docs/STABILITY.md",
    "docs/SUPPORT_MATRIX.md",
)
APACHE_LICENSE_SHA256 = (
    "cfc7749b96f63bd31c3c42b5c471bf756814053e847c10f3eb003417bc523d30"
)
PROJECT_NOTICE = (
    "Pista\n"
    "Copyright 2026 The Pista Authors\n\n"
    "This product includes software developed by The Pista Authors.\n"
)
ASF_HEADER = "Licensed to the Apache Software Foundation"
PSL_PATH = Path(
    "pista-catalyst/src/main/resources/com/pista/spark/sql/catalyst/functions/public_suffix_list.dat"
)
PSL_NOTICE_PATH = PSL_PATH.with_name("PUBLIC_SUFFIX_LIST.md")
PSL_SHA256 = "6f17568c0cf95b1fd5347e759bf250e442cd472b7ff7052f601fa5217fb75094"


def candidate_files(root: Path) -> list[str]:
    result = subprocess.run(
        ["git", "ls-files", "--cached", "--others", "--exclude-standard", "-z"],
        cwd=root,
        check=True,
        capture_output=True,
    )
    return sorted(item.decode("utf-8") for item in result.stdout.split(b"\0") if item)


def project_license_findings(root: Path) -> list[str]:
    findings: list[str] = []
    license_bytes = (root / "LICENSE").read_bytes()
    if hashlib.sha256(license_bytes).hexdigest() != APACHE_LICENSE_SHA256:
        findings.append("apache-license-text-mismatch: LICENSE")
    if (root / "NOTICE").read_text(encoding="utf-8") != PROJECT_NOTICE:
        findings.append("project-notice-mismatch: NOTICE")
    third_party = (root / "THIRD_PARTY_NOTICES").read_text(encoding="utf-8")
    if (
        "Mozilla Public License 2.0" not in third_party
        or "e1b8015c3b2f0f4f8c18659c2480fc1a22c07b20" not in third_party
    ):
        findings.append("third-party-notice-incomplete: THIRD_PARTY_NOTICES")
    pom = (root / "pom.xml").read_text(encoding="utf-8")
    if "<name>Apache License, Version 2.0</name>" not in pom:
        findings.append("missing-maven-license-metadata: pom.xml")
    return findings


def source_header_findings(root: Path) -> list[str]:
    """Reject ASF ownership claims without requiring per-file license markers."""
    findings: list[str] = []
    for relative_path in candidate_files(root):
        path = root / relative_path
        if not path.is_file():
            continue
        try:
            header = "\n".join(path.read_text(encoding="utf-8").splitlines()[:20])
        except UnicodeDecodeError:
            continue
        if ASF_HEADER in header:
            findings.append(f"unexpected-asf-header: {relative_path}")
    return findings


def public_data_findings(root: Path) -> list[str]:
    findings: list[str] = []
    data_path = root / PSL_PATH
    notice_path = root / PSL_NOTICE_PATH
    if not data_path.is_file():
        return [f"missing-public-data: {PSL_PATH}"]
    digest = hashlib.sha256(data_path.read_bytes()).hexdigest()
    if digest != PSL_SHA256:
        findings.append(f"public-data-checksum-mismatch: {PSL_PATH}")
    data_header = "\n".join(data_path.read_text(encoding="utf-8").splitlines()[:4])
    if "Mozilla Public" not in data_header or "MPL/2.0" not in data_header:
        findings.append(f"public-data-license-notice-missing: {PSL_PATH}")
    if not notice_path.is_file():
        findings.append(f"missing-public-data-notice: {PSL_NOTICE_PATH}")
    else:
        notice = notice_path.read_text(encoding="utf-8")
        if PSL_SHA256 not in notice or "Mozilla Public License 2.0" not in notice:
            findings.append(f"public-data-notice-drift: {PSL_NOTICE_PATH}")
    return findings


def main() -> int:
    root = Path.cwd()
    missing = [
        relative_path
        for relative_path in REQUIRED_FILES
        if not (root / relative_path).is_file()
    ]
    for relative_path in missing:
        print(f"missing-public-file: {relative_path}")
    findings = (
        project_license_findings(root)
        + public_data_findings(root)
        + source_header_findings(root)
        if not missing
        else []
    )
    for finding in findings:
        print(finding)
    if missing or findings:
        print("Public licensing/governance check failed.")
        return 1
    print("Public licensing/governance file check passed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
