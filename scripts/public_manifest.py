#!/usr/bin/env python3

"""Generate or verify a reproducible manifest of the public candidate tree."""

from __future__ import annotations

import argparse
import hashlib
import os
import stat
import subprocess
import tempfile
from pathlib import Path


HEADER = (
    "source_commit\tsource_path\tpublic_path\tmode\tsize\tsha256"
    "\tprovenance\tdisposition"
)
DEFAULT_MANIFEST = "config/public-snapshot-manifest.tsv"


def load_existing_origins(path: Path) -> dict[str, tuple[str, str, str]]:
    """Load stable source origin metadata from an existing manifest."""
    if not path.is_file():
        return {}
    lines = path.read_text(encoding="utf-8").splitlines()
    if not lines or lines[0] != HEADER:
        raise ValueError(f"Unsupported manifest header: {path}")
    origins: dict[str, tuple[str, str, str]] = {}
    for number, line in enumerate(lines[1:], 2):
        columns = line.split("\t")
        if len(columns) != 8:
            raise ValueError(f"Invalid manifest row at {path}:{number}")
        source_commit, source_path, public_path, _, _, _, provenance, _ = columns
        origins[public_path] = (source_commit, source_path, provenance)
    return origins


def file_data(path: Path) -> bytes:
    if path.is_symlink():
        return os.readlink(path).encode("utf-8")
    return path.read_bytes()


def entries(
    root: Path,
    excluded: set[str],
    existing_origins: dict[str, tuple[str, str, str]],
) -> list[str]:
    base_commit = subprocess.run(
        ["git", "rev-parse", "HEAD"],
        cwd=root,
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    head_tree = subprocess.run(
        ["git", "ls-tree", "-r", "--name-only", "-z", "HEAD"],
        cwd=root,
        check=True,
        capture_output=True,
    )
    paths_at_head = {
        item.decode("utf-8") for item in head_tree.stdout.split(b"\0") if item
    }
    tracked = subprocess.run(
        ["git", "ls-files", "-s", "-z"], cwd=root, check=True, capture_output=True
    )
    metadata_by_path: dict[str, tuple[str, str, str, str]] = {}
    for raw in tracked.stdout.split(b"\0"):
        if not raw:
            continue
        metadata, relative = raw.decode("utf-8").split("\t", 1)
        path = root / relative
        if relative in excluded or not os.path.lexists(path):
            continue
        existing_origin = existing_origins.get(relative)
        existed_at_head = relative in paths_at_head
        if existing_origin is not None:
            source_commit, source_path, provenance = existing_origin
        else:
            source_commit = base_commit if existed_at_head else "WORKTREE"
            source_path = relative
            provenance = (
                "incubator-baseline" if existed_at_head else "pista-public-preparation"
            )
        metadata_by_path[relative] = (
            metadata.split(" ", 1)[0],
            source_commit,
            source_path,
            provenance,
        )

    untracked = subprocess.run(
        ["git", "ls-files", "--others", "--exclude-standard", "-z"],
        cwd=root,
        check=True,
        capture_output=True,
    )
    for raw in untracked.stdout.split(b"\0"):
        if not raw:
            continue
        relative = raw.decode("utf-8")
        path = root / relative
        if relative in excluded or not os.path.lexists(path):
            continue
        path_mode = path.lstat().st_mode
        if stat.S_ISLNK(path_mode):
            mode = "120000"
        else:
            mode = "100755" if path_mode & 0o111 else "100644"
        metadata_by_path[relative] = (
            mode,
            "WORKTREE",
            relative,
            "pista-public-preparation",
        )

    rows: list[str] = []
    for relative, (
        mode,
        source_commit,
        source_path,
        provenance,
    ) in metadata_by_path.items():
        data = file_data(root / relative)
        rows.append(
            f"{source_commit}\t{source_path}\t{relative}\t{mode}\t{len(data)}"
            f"\t{hashlib.sha256(data).hexdigest()}\t{provenance}\tinclude"
        )
    return [HEADER, *sorted(rows)]


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", type=Path, default=Path.cwd())
    parser.add_argument("--output", type=Path, default=Path(DEFAULT_MANIFEST))
    parser.add_argument("--check", action="store_true")
    args = parser.parse_args()
    root = args.root.resolve()
    output = args.output if args.output.is_absolute() else root / args.output
    excluded = {DEFAULT_MANIFEST}
    try:
        excluded.add(output.relative_to(root).as_posix())
    except ValueError:
        pass
    existing_origins = load_existing_origins(root / DEFAULT_MANIFEST)
    content = "\n".join(entries(root, excluded, existing_origins)) + "\n"
    if args.check:
        if not output.is_file():
            print(f"manifest-missing: {output}")
            return 1
        if output.read_text(encoding="utf-8") != content:
            with tempfile.NamedTemporaryFile(
                "w", encoding="utf-8", delete=False
            ) as handle:
                handle.write(content)
                print(f"manifest-drift: expected content written to {handle.name}")
            return 1
        print(f"Public manifest is current: {output}")
        return 0
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(content, encoding="utf-8")
    print(f"Wrote {len(content.splitlines()) - 1} manifest entries to {output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
