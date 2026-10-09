#!/usr/bin/env python3

"""Bind and verify a stable GitHub candidate without publishing or rebuilding it."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import tarfile
import tempfile
import xml.etree.ElementTree as ET
import zipfile

from check_sbom import validate_sbom


STABLE_VERSION = re.compile(r"(?:0|[1-9]\d*)\.(?:0|[1-9]\d*)\.(?:0|[1-9]\d*)")
COMMIT = re.compile(r"[0-9a-f]{40}")
POM_NAMESPACE = {"m": "http://maven.apache.org/POM/4.0.0"}


def validate_identity(version: str, commit: str) -> None:
    if not STABLE_VERSION.fullmatch(version):
        raise ValueError(
            "A stable major.minor.patch version without SNAPSHOT is required"
        )
    if not COMMIT.fullmatch(commit):
        raise ValueError("The approved full Git commit SHA is required")


def git(root: Path, *arguments: str) -> str:
    return subprocess.check_output(
        ["git", "-C", str(root), *arguments], text=True
    ).strip()


def check_source(root: Path, version: str, commit: str) -> dict[str, str]:
    validate_identity(version, commit)
    if Path(git(root, "rev-parse", "--show-toplevel")).resolve() != root.resolve():
        raise ValueError("Run from the Pista repository root")
    if git(root, "rev-parse", "HEAD") != commit:
        raise ValueError("The checked-out source does not match the approved commit")
    if git(root, "status", "--porcelain"):
        raise ValueError("Release source must be clean, including untracked files")
    return check_project(ET.parse(root / "pom.xml").getroot(), version)


def check_project(project: ET.Element, version: str) -> dict[str, str]:
    if project.findtext("m:properties/m:revision", namespaces=POM_NAMESPACE) != version:
        raise ValueError("The reactor revision does not match the approved version")
    baseline = {}
    for name in ("java.version", "scala.version", "spark.version"):
        value = project.findtext(f"m:properties/m:{name}", namespaces=POM_NAMESPACE)
        if not value:
            raise ValueError(f"Missing runtime baseline property: {name}")
        baseline[name] = value
    return baseline


def check_archive(path: Path, version: str, commit: str) -> dict[str, str]:
    with tarfile.open(path, "r:gz") as archive:
        if archive.pax_headers.get("comment") != commit:
            raise ValueError("The source archive does not match the approved commit")
        member = archive.getmember(f"pista-{version}/pom.xml")
        if not member.isfile():
            raise ValueError("The archived reactor POM must be a regular file")
        with archive.extractfile(member) as source:
            return check_project(ET.fromstring(source.read()), version)


def sha256(path: Path) -> str:
    with path.open("rb") as source:
        digest = hashlib.sha256()
        for block in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def check_jar(path: Path, version: str, commit: str) -> None:
    with zipfile.ZipFile(path) as jar:
        manifest = jar.read("META-INF/MANIFEST.MF").decode("utf-8")
    # JAR manifests fold long values onto continuation lines.
    manifest = manifest.replace("\r\n", "\n").replace("\n ", "")
    headers = dict(
        line.split(": ", 1)
        for line in manifest.split("\n\n", 1)[0].splitlines()
        if ": " in line
    )
    if headers.get("Implementation-Version") != version:
        raise ValueError(
            "The submission JAR version does not match the approved version"
        )
    if headers.get("Pista-Source-Revision") != commit:
        raise ValueError("The submission JAR source does not match the approved commit")


def check_sbom(path: Path, version: str) -> None:
    findings = validate_sbom(path)
    if findings:
        raise ValueError("Invalid release SBOM: " + "; ".join(findings))
    document = json.loads(path.read_text(encoding="utf-8"))
    if document.get("metadata", {}).get("component", {}).get("version") != version:
        raise ValueError("The SBOM project version does not match the approved version")
    if any(
        str(component["version"]).endswith("-SNAPSHOT")
        for component in document["components"]
    ):
        raise ValueError("Release dependencies must not use SNAPSHOT versions")


def asset_names(version: str) -> tuple[str, str, str]:
    return f"pista-{version}.jar", f"pista-{version}-src.tar.gz", "pista-jvm.cdx.json"


def package(root: Path, output: Path, version: str, commit: str) -> None:
    baseline = check_source(root, version, commit)
    jar_name, source_name, sbom_name = asset_names(version)
    jar = root / "pista-assembly" / "target" / jar_name
    sbom = root / "target" / "sbom" / sbom_name
    check_jar(jar, version, commit)
    check_sbom(sbom, version)
    if output.exists():
        raise ValueError(
            "Candidate output already exists; preserve it and use a new directory"
        )
    output.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(
        prefix=".pista-release-", dir=output.parent
    ) as temp:
        staging = Path(temp) / "bundle"
        staging.mkdir()
        shutil.copyfile(jar, staging / jar_name)
        shutil.copyfile(sbom, staging / sbom_name)
        subprocess.run(
            [
                "git",
                "-C",
                str(root),
                "archive",
                "--format=tar.gz",
                f"--prefix=pista-{version}/",
                "--output",
                str(staging / source_name),
                commit,
            ],
            check=True,
        )
        assets = {name: sha256(staging / name) for name in asset_names(version)}
        provenance = {
            "version": version,
            "tag": f"v{version}",
            "commit": commit,
            "baseline": baseline,
            "assets": assets,
        }
        manifest = staging / "release-manifest.json"
        manifest.write_text(json.dumps(provenance, indent=2) + "\n", encoding="utf-8")
        checksums = {**assets, manifest.name: sha256(manifest)}
        (staging / "SHA256SUMS").write_text(
            "".join(
                f"{digest}  {name}\n" for name, digest in sorted(checksums.items())
            ),
            encoding="utf-8",
        )
        verify(staging, version, commit)
        os.rename(staging, output)


def verify(directory: Path, version: str, commit: str) -> None:
    validate_identity(version, commit)
    expected_assets = set(asset_names(version))
    expected_files = expected_assets | {"release-manifest.json", "SHA256SUMS"}
    if {path.name for path in directory.iterdir()} != expected_files:
        raise ValueError(
            "The candidate does not contain exactly the expected release files"
        )
    if any(not path.is_file() or path.is_symlink() for path in directory.iterdir()):
        raise ValueError("Release files must be regular files, not symbolic links")
    manifest = json.loads(
        (directory / "release-manifest.json").read_text(encoding="utf-8")
    )
    if (manifest.get("version"), manifest.get("tag"), manifest.get("commit")) != (
        version,
        f"v{version}",
        commit,
    ):
        raise ValueError("Candidate provenance does not match the approved identity")
    assets = manifest.get("assets", {})
    if set(assets) != expected_assets:
        raise ValueError("Candidate provenance has unexpected assets")
    for name, digest in assets.items():
        if digest != sha256(directory / name):
            raise ValueError(f"Candidate checksum mismatch: {name}")
    expected_checksums = {
        **assets,
        "release-manifest.json": sha256(directory / "release-manifest.json"),
    }
    expected_text = "".join(
        f"{digest}  {name}\n" for name, digest in sorted(expected_checksums.items())
    )
    if (directory / "SHA256SUMS").read_text(encoding="utf-8") != expected_text:
        raise ValueError("SHA256SUMS must match the assets and use basename-only paths")
    check_jar(directory / f"pista-{version}.jar", version, commit)
    check_sbom(directory / "pista-jvm.cdx.json", version)
    baseline = check_archive(directory / f"pista-{version}-src.tar.gz", version, commit)
    if manifest.get("baseline") != baseline:
        raise ValueError("Candidate baseline does not match the archived source")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("operation", choices=("check", "package", "verify"))
    parser.add_argument("--version", required=True)
    parser.add_argument("--commit", required=True)
    parser.add_argument("--root", type=Path, default=Path.cwd())
    parser.add_argument("--directory", type=Path, default=Path("target/release"))
    args = parser.parse_args()
    try:
        if args.operation == "check":
            check_source(args.root.resolve(), args.version, args.commit)
        elif args.operation == "package":
            package(
                args.root.resolve(), args.directory.resolve(), args.version, args.commit
            )
        else:
            verify(args.directory.resolve(), args.version, args.commit)
    except (
        ValueError,
        OSError,
        KeyError,
        tarfile.TarError,
        zipfile.BadZipFile,
        ET.ParseError,
        subprocess.CalledProcessError,
    ) as error:
        parser.error(str(error))
    print(f"Release candidate {args.operation} passed: v{args.version} {args.commit}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
