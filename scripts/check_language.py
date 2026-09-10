#!/usr/bin/env python3

"""Enforce English canonical content with narrow, documented CJK exceptions."""

from __future__ import annotations

import argparse
import fnmatch
import re
import subprocess
from pathlib import Path


CJK = re.compile(
    r"[\u3400-\u4dbf\u4e00-\u9fff\uf900-\ufaff\uff0c\u3002\uff1b\uff1a\u3001"
    r"\uff08\uff09\u201c\u201d\u2018\u2019\uff01\uff1f\u300a\u300b\u3010\u3011]"
)


def candidate_files(root: Path) -> list[str]:
    result = subprocess.run(
        ["git", "ls-files", "--cached", "--others", "--exclude-standard", "-z"],
        cwd=root,
        check=True,
        capture_output=True,
    )
    return sorted({item.decode("utf-8") for item in result.stdout.split(b"\0") if item})


def load_allowlist(path: Path) -> list[tuple[str, str]]:
    entries: list[tuple[str, str]] = []
    if not path.exists():
        return entries
    for number, raw in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        parts = raw.split("\t", 1)
        if len(parts) != 2 or not all(part.strip() for part in parts):
            raise ValueError(f"Invalid CJK allowlist entry at {path}:{number}")
        entries.append((parts[0].strip(), parts[1].strip()))
    return entries


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", type=Path, default=Path.cwd())
    parser.add_argument(
        "--allowlist", type=Path, default=Path("config/cjk-allowlist.tsv")
    )
    args = parser.parse_args()
    root = args.root.resolve()
    allowlist_path = args.allowlist
    if not allowlist_path.is_absolute():
        allowlist_path = root / allowlist_path
    allowlist = load_allowlist(allowlist_path)

    findings: list[tuple[str, int]] = []
    for relative_path in candidate_files(root):
        path = root / relative_path
        if not path.exists() and not path.is_symlink():
            continue
        if CJK.search(relative_path):
            findings.append((relative_path, 0))
        if not path.is_file():
            continue
        try:
            text = path.read_text(encoding="utf-8")
        except (UnicodeDecodeError, IsADirectoryError):
            continue
        if not CJK.search(text):
            continue
        if any(fnmatch.fnmatch(relative_path, path_glob) for path_glob, _ in allowlist):
            continue
        for line_number, line in enumerate(text.splitlines(), 1):
            if CJK.search(line):
                findings.append((relative_path, line_number))

    for relative_path, line_number in findings:
        print(f"unapproved-cjk: {relative_path}:{line_number}")
    if findings:
        print(f"Language scan failed with {len(findings)} CJK-containing line(s).")
        return 1
    print("Language scan passed: only approved CJK content remains.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
