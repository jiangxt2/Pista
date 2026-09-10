#!/usr/bin/env python3

"""Validate public Markdown, links, and non-normative translation pointers."""

from __future__ import annotations

import argparse
import re
from pathlib import Path
from urllib.parse import unquote


LINK = re.compile(r"(?<!!)\[[^\]]*\]\((?P<target><[^>]+>|[^)\s]+)(?:\s+[^)]*)?\)")
CANONICAL_LINK = re.compile(
    r"^> Canonical English: \[[^\]]+\]\((?P<target>[^)]+)\)$",
    re.MULTILINE,
)
REQUIRED_CANONICAL = {
    Path("README.md"),
    Path("CONTRIBUTING.md"),
    Path("SECURITY.md"),
    Path("docs/index.md"),
    Path("docs/architecture.md"),
    Path("docs/reference/configuration.md"),
    Path("docs/reference/error-codes.md"),
    Path("docs/SUPPORT_MATRIX.md"),
}


def translation_source(relative: Path) -> Path | None:
    parts = relative.parts
    if len(parts) >= 3 and parts[:2] == ("docs", "zh-CN"):
        return Path("docs", *parts[2:])
    return None


def markdown_findings(root: Path) -> list[str]:
    findings = [
        f"missing-canonical: {path}"
        for path in sorted(REQUIRED_CANONICAL)
        if not (root / path).is_file()
    ]
    for path in sorted(root.rglob("*.md")):
        if any(part in {"target", ".venv"} for part in path.parts):
            continue
        relative = path.relative_to(root)
        text = path.read_text(encoding="utf-8")
        first_heading = next(
            (line for line in text.splitlines() if line.startswith("# ")), None
        )
        if first_heading is None:
            findings.append(f"missing-title: {relative}")
        elif re.match(r"#\s+\d", first_heading):
            findings.append(f"numbered-title: {relative}")

        fence: str | None = None
        for number, line in enumerate(text.splitlines(), 1):
            stripped = line.lstrip()
            marker = (
                "```"
                if stripped.startswith("```")
                else "~~~"
                if stripped.startswith("~~~")
                else None
            )
            if marker:
                if fence is None:
                    fence = marker
                elif fence == marker:
                    fence = None
                continue
            if fence is not None:
                continue
            for match in LINK.finditer(line):
                raw_target = match.group("target").strip("<>")
                if raw_target.startswith(("#", "http://", "https://", "mailto:")):
                    continue
                target = unquote(raw_target.split("#", 1)[0])
                if target and not (path.parent / target).resolve().exists():
                    findings.append(f"missing-link: {relative}:{number}: {target}")
        if fence is not None:
            findings.append(f"unclosed-fence: {relative}")

        canonical = translation_source(relative)
        if canonical is not None:
            if not (root / canonical).is_file():
                findings.append(f"missing-translation-source: {relative}: {canonical}")
            marker = CANONICAL_LINK.search(text)
            if marker is None:
                findings.append(f"missing-canonical-pointer: {relative}")
            else:
                target = unquote(marker.group("target").split("#", 1)[0])
                resolved = (path.parent / target).resolve()
                if resolved != (root / canonical).resolve():
                    findings.append(f"canonical-pointer-mismatch: {relative}: {target}")
    return findings


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", type=Path, default=Path.cwd())
    args = parser.parse_args()
    findings = markdown_findings(args.root.resolve())
    for finding in findings:
        print(finding)
    if findings:
        return 1
    print("Markdown validation passed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
