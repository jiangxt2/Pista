#!/usr/bin/env python3

"""Fail when a public-source candidate contains prohibited organization or secret data."""

from __future__ import annotations

import argparse
import fnmatch
import re
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class Rule:
    rule_id: str
    pattern: re.Pattern[str]


def _joined(*parts: str) -> str:
    return "".join(parts)


RULES = (
    Rule("identity-zh", re.compile("\u4e2d\u56fd\u79fb\u52a8")),
    Rule(
        "identity-en", re.compile(_joined("china", r"[ _-]?", "mobile"), re.IGNORECASE)
    ),
    Rule(
        "identity-acronym",
        re.compile(
            r"\b(?:" + "CM" + r"CC|" + "CM" + r"IT|" + "CM" + r"IOT)\b", re.IGNORECASE
        ),
    ),
    Rule(
        "identity-service-number",
        re.compile(r"(?<!\d)" + _joined("100", "86") + r"(?!\d)"),
    ),
    Rule("brand-01", re.compile(r"\b" + _joined("mi", "gu") + r"\b", re.IGNORECASE)),
    Rule("brand-cloud", re.compile("\u79fb\u52a8\u4e91")),
    Rule(
        "subsidiary-zh",
        re.compile(
            "\u4e2d\u79fb(?:\u4e92\u8054\u7f51|\u7269\u8054\u7f51|\u7cfb\u7edf\u96c6\u6210|\u91d1\u79d1|\u6570\u667a|\u5728\u7ebf|\u94c1\u901a|\u8d44\u672c)"
        ),
    ),
    Rule(
        "business-province-id",
        re.compile(r"\b" + _joined("prov", "_id") + r"\b", re.IGNORECASE),
    ),
    Rule(
        "business-province-option",
        re.compile(_joined("--sql-", "prov-id"), re.IGNORECASE),
    ),
    Rule(
        "business-statistics-date",
        re.compile(r"\b" + _joined("statis", "_ym") + r"d?\b", re.IGNORECASE),
    ),
    Rule(
        "business-province-code",
        re.compile(r"(?<!\d)" + _joined("109", "00") + r"(?!\d)"),
    ),
    Rule(
        "identity-domain",
        re.compile(
            r"(?:"
            + _joined("china", "mobile")
            + r"|"
            + "cm"
            + r"cc|"
            + "cm"
            + r"it|"
            + "cm"
            + r"iot)\.(?:com|cn)\b",
            re.IGNORECASE,
        ),
    ),
    Rule(
        "internal-workdir",
        re.compile(r"/mnt/" + _joined("nfs", "-workdir") + r"/", re.IGNORECASE),
    ),
    Rule(
        "private-ip",
        re.compile(
            r"(?<!\d)(?:10(?:\.\d{1,3}){3}|192\.168(?:\.\d{1,3}){2}|172\.(?:1[6-9]|2\d|3[01])(?:\.\d{1,3}){2})(?!\d)"
        ),
    ),
    Rule(
        "private-key",
        re.compile(
            _joined("-----BEGIN ", r"(?:RSA |EC |DSA |OPENSSH )?", "PRIVATE KEY-----")
        ),
    ),
    Rule(
        "aws-access-key",
        re.compile(_joined("AK", "IA") + r"[0-9A-Z]{16}"),
    ),
    Rule(
        "github-token",
        re.compile(_joined(r"\bgh", r"[pousr]_", r"[A-Za-z0-9]{30,}\b")),
    ),
    Rule(
        "internal-domain",
        re.compile(r"(?i)\b(?:https?://|[A-Za-z0-9.-]+@)[^\s/]+\.(?:corp|internal)\b"),
    ),
)

BINARY_EXTENSIONS = {
    ".7z",
    ".avro",
    ".bin",
    ".class",
    ".dll",
    ".dylib",
    ".egg",
    ".gz",
    ".jar",
    ".jpeg",
    ".jpg",
    ".orc",
    ".parquet",
    ".pdf",
    ".png",
    ".so",
    ".tar",
    ".tgz",
    ".ttf",
    ".webp",
    ".woff",
    ".woff2",
    ".zip",
}

SENSITIVE_ENV_ASSIGNMENT = re.compile(
    r"(?i)^\s*(?:export\s+)?[A-Za-z_][A-Za-z0-9_.-]*(?:password|passwd|secret|token|access[_-]?key)"
    r"[A-Za-z0-9_.-]*\s*=\s*(?P<value>.*?)\s*$"
)

SENSITIVE_LITERAL_ASSIGNMENT = re.compile(
    r"(?i)\b(?:password|passwd|secret|token|access[_-]?key)\b\s*(?:=|->)\s*"
    r"(?P<value>['\"][^'\"]*['\"])"
)

SAFE_SECRET_VALUES = re.compile(
    r"^(?:|['\"]{2}|['\"]?(?:xxx|test|test-password|password|changeme|default|minioadmin)['\"]?[,;)]?)$",
    re.IGNORECASE,
)
LOG_CALL = re.compile(
    r"\b(?:log(?:Info|Debug|Warning|Error)\s*\(|logger\.(?:info|debug|warning|error)\s*\()"
)
UNSAFE_LOG_VALUE = re.compile(
    "|".join(
        (
            _joined(r"\$", "sqlContent", r"\b"),
            _joined(r"\$", "jdbcUrl", r"\b"),
            _joined(r"\$", "filterQuery", r"\b"),
            _joined(r"\$\{", "options"),
            _joined(r"\$\{", "e", r"\.getMessage"),
            _joined(r"\$\{", "result", r"\.errorMessage"),
            _joined(r"\{", "sql_content", r"\}"),
            _joined(r"\{", "options", r"\}"),
        )
    )
)


def candidate_files(root: Path) -> list[str]:
    result = subprocess.run(
        ["git", "ls-files", "--cached", "--others", "--exclude-standard", "-z"],
        cwd=root,
        check=True,
        capture_output=True,
    )
    return sorted({item.decode("utf-8") for item in result.stdout.split(b"\0") if item})


def load_allowlist(path: Path) -> list[tuple[str, str, str]]:
    entries: list[tuple[str, str, str]] = []
    if not path.exists():
        return entries
    for number, raw in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        parts = raw.split("\t", 2)
        if len(parts) != 3 or not all(part.strip() for part in parts):
            raise ValueError(f"Invalid allowlist entry at {path}:{number}")
        entries.append((parts[0].strip(), parts[1].strip(), parts[2].strip()))
    return entries


def is_allowed(
    rule_id: str, relative_path: str, allowlist: list[tuple[str, str, str]]
) -> bool:
    return any(
        allowed_rule == rule_id and fnmatch.fnmatch(relative_path, path_glob)
        for allowed_rule, path_glob, _ in allowlist
    )


def decode_text(data: bytes) -> str | None:
    if b"\0" in data:
        return None
    try:
        return data.decode("utf-8")
    except UnicodeDecodeError:
        return None


def check_secret_assignment(line: str) -> bool:
    match = SENSITIVE_ENV_ASSIGNMENT.match(line) or SENSITIVE_LITERAL_ASSIGNMENT.search(
        line
    )
    if not match:
        return False
    value = match.group("value").strip()
    if SAFE_SECRET_VALUES.match(value):
        return False
    if (
        "$" in value
        or "<" in value
        and ">" in value
        or ".get(" in value
        or "os.environ" in value
        or "getenv(" in value
        or "metaPassword" in value
        or "<REDACTED>" in value
    ):
        return False
    return True


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", type=Path, default=Path.cwd())
    parser.add_argument(
        "--allowlist",
        type=Path,
        default=Path("config/public-content-allowlist.tsv"),
    )
    args = parser.parse_args()
    root = args.root.resolve()
    allowlist_path = args.allowlist
    if not allowlist_path.is_absolute():
        allowlist_path = root / allowlist_path
    allowlist = load_allowlist(allowlist_path)

    findings: list[tuple[str, str, int]] = []
    files = candidate_files(root)
    for relative_path in files:
        path = root / relative_path
        if not path.exists() and not path.is_symlink():
            continue
        for rule in RULES:
            if rule.pattern.search(relative_path) and not is_allowed(
                rule.rule_id, relative_path, allowlist
            ):
                findings.append((rule.rule_id, relative_path, 0))
        if path.is_symlink():
            findings.append(("symlink-entry", relative_path, 0))
            continue
        if not path.is_file():
            continue
        if path.suffix.lower() in BINARY_EXTENSIONS and not is_allowed(
            "tracked-binary", relative_path, allowlist
        ):
            findings.append(("tracked-binary", relative_path, 0))
            continue
        data = path.read_bytes()
        text = decode_text(data)
        if text is None:
            if not is_allowed("binary-content", relative_path, allowlist):
                findings.append(("binary-content", relative_path, 0))
            continue
        for line_number, line in enumerate(text.splitlines(), 1):
            for rule in RULES:
                if rule.pattern.search(line) and not is_allowed(
                    rule.rule_id, relative_path, allowlist
                ):
                    findings.append((rule.rule_id, relative_path, line_number))
            if check_secret_assignment(line) and not is_allowed(
                "non-empty-secret", relative_path, allowlist
            ):
                findings.append(("non-empty-secret", relative_path, line_number))
            if LOG_CALL.search(line) and UNSAFE_LOG_VALUE.search(line):
                findings.append(("unsafe-log-context", relative_path, line_number))

    for rule_id, relative_path, line_number in sorted(set(findings)):
        location = f"{relative_path}:{line_number}" if line_number else relative_path
        print(f"{rule_id}: {location}")
    if findings:
        print(
            f"Public-content scan failed with {len(set(findings))} finding(s).",
            file=sys.stderr,
        )
        return 1
    existing_count = sum((root / relative_path).is_file() for relative_path in files)
    print(f"Public-content scan passed for {existing_count} candidate file(s).")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
