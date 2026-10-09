"""Direct tests for stable candidate identity, archives and rejection boundaries."""

from __future__ import annotations

import importlib
import json
import os
from pathlib import Path
import subprocess
import sys
import tarfile
import zipfile

import pytest


SCRIPTS_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(SCRIPTS_DIR))
release = importlib.import_module("prepare_release")
VERSION = "2.5.0"


@pytest.fixture
def repository(tmp_path: Path) -> tuple[Path, str]:
    root = tmp_path / "source"
    root.mkdir()
    (root / "pom.xml").write_text(
        '<project xmlns="http://maven.apache.org/POM/4.0.0"><properties>'
        "<revision>2.5.0</revision><java.version>17</java.version>"
        "<scala.version>2.12.18</scala.version><spark.version>3.5.8</spark.version>"
        "</properties></project>",
        encoding="utf-8",
    )
    (root / ".gitignore").write_text("target/\n**/target/\n", encoding="utf-8")
    (root / "LICENSE").write_text(
        "Synthetic source license fixture\n", encoding="utf-8"
    )
    # Isolate synthetic fixture commits from the user's signing, hooks and identity.
    env = {**os.environ, "GIT_CONFIG_GLOBAL": os.devnull, "GIT_CONFIG_NOSYSTEM": "1"}
    for arguments in (
        ("init", "--quiet"),
        ("config", "user.name", "Pista Test"),
        ("config", "user.email", "pista@example.invalid"),
        ("add", "."),
        ("commit", "--quiet", "-m", "Create synthetic release fixture"),
    ):
        subprocess.run(["git", "-C", str(root), *arguments], env=env, check=True)
    return root, release.git(root, "rev-parse", "HEAD")


def write_artifacts(root: Path, commit: str, version: str = VERSION) -> None:
    jar = root / "pista-assembly" / "target" / f"pista-{VERSION}.jar"
    jar.parent.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(jar, "w") as archive:
        archive.writestr(
            "META-INF/MANIFEST.MF",
            f"Manifest-Version: 1.0\r\n"
            f"Implementation-Version: {version}\r\nPista-Source-Revision: {commit}\r\n\r\n",
        )
        archive.writestr("fixture.txt", "Synthetic submission artifact")
    sbom = root / "target" / "sbom" / "pista-jvm.cdx.json"
    sbom.parent.mkdir(parents=True, exist_ok=True)
    sbom.write_text(
        json.dumps(
            {
                "bomFormat": "CycloneDX",
                "specVersion": "1.6",
                "metadata": {"component": {"name": "pista-parent", "version": VERSION}},
                "components": [{"name": "synthetic-library", "version": "1.0.0"}],
            }
        ),
        encoding="utf-8",
    )


def test_candidate_contains_frozen_source_and_portable_checksums(repository, tmp_path):
    root, commit = repository
    write_artifacts(root, commit)
    bundle = tmp_path / "candidate"
    release.package(root, bundle, VERSION, commit)
    release.verify(bundle, VERSION, commit)
    manifest = json.loads((bundle / "release-manifest.json").read_text())
    assert manifest["commit"] == commit
    assert manifest["tag"] == "v2.5.0"
    assert manifest["baseline"] == {
        "java.version": "17",
        "scala.version": "2.12.18",
        "spark.version": "3.5.8",
    }
    for line in (bundle / "SHA256SUMS").read_text().splitlines():
        digest, name = line.split("  ")
        assert "/" not in name
        assert digest == release.sha256(bundle / name)
    with tarfile.open(bundle / "pista-2.5.0-src.tar.gz") as source:
        assert "pista-2.5.0/LICENSE" in source.getnames()
        assert (
            source.extractfile("pista-2.5.0/pom.xml").read()
            == (root / "pom.xml").read_bytes()
        )
        assert not any(
            "/target/" in name or "/.git/" in name for name in source.getnames()
        )


@pytest.mark.parametrize(
    "version", ["2.6.0-SNAPSHOT", "2.5.0-rc.1", "02.5.0", "../2.5.0"]
)
def test_only_stable_versions_can_enter_candidate_packaging(version):
    with pytest.raises(ValueError, match="stable major.minor.patch"):
        release.validate_identity(version, "a" * 40)


def test_source_guard_rejects_wrong_commit_version_and_dirty_state(repository):
    root, commit = repository
    with pytest.raises(ValueError, match="approved commit"):
        release.check_source(root, VERSION, "a" * 40)
    with pytest.raises(ValueError, match="reactor revision"):
        release.check_source(root, "2.5.1", commit)
    (root / "untracked.txt").write_text("untracked", encoding="utf-8")
    with pytest.raises(ValueError, match="source must be clean"):
        release.check_source(root, VERSION, commit)


@pytest.mark.parametrize("jar_version,wrong_commit", [("2.4", False), (VERSION, True)])
def test_wrong_jar_metadata_cannot_be_packaged(
    repository, tmp_path, jar_version, wrong_commit
):
    root, commit = repository
    write_artifacts(root, "b" * 40 if wrong_commit else commit, jar_version)
    bundle = tmp_path / "candidate"
    with pytest.raises(ValueError, match="submission JAR"):
        release.package(root, bundle, VERSION, commit)
    assert not bundle.exists()


def test_verification_rejects_changed_assets_and_existing_bundles(repository, tmp_path):
    root, commit = repository
    write_artifacts(root, commit)
    bundle = tmp_path / "candidate"
    release.package(root, bundle, VERSION, commit)
    original = (bundle / "release-manifest.json").read_bytes()
    with pytest.raises(ValueError, match="output already exists"):
        release.package(root, bundle, VERSION, commit)
    assert (bundle / "release-manifest.json").read_bytes() == original
    (bundle / "pista-2.5.0.jar").write_bytes(b"corrupted")
    with pytest.raises(ValueError, match="checksum mismatch"):
        release.verify(bundle, VERSION, commit)


def test_verification_rejects_wrong_identity_extra_files_and_checksum_paths(
    repository, tmp_path
):
    root, commit = repository
    write_artifacts(root, commit)
    bundle = tmp_path / "candidate"
    release.package(root, bundle, VERSION, commit)
    with pytest.raises(ValueError, match="approved identity"):
        release.verify(bundle, VERSION, "b" * 40)
    checksums = bundle / "SHA256SUMS"
    checksums.write_text(checksums.read_text().replace("  pista-", "  target/pista-"))
    with pytest.raises(ValueError, match="basename-only"):
        release.verify(bundle, VERSION, commit)
    (bundle / "unexpected.txt").write_text("extra", encoding="utf-8")
    with pytest.raises(ValueError, match="exactly the expected"):
        release.verify(bundle, VERSION, commit)


def test_sbom_identity_and_snapshot_dependencies_are_rejected(repository, tmp_path):
    root, commit = repository
    write_artifacts(root, commit)
    sbom = root / "target" / "sbom" / "pista-jvm.cdx.json"
    value = json.loads(sbom.read_text())
    value["metadata"]["component"]["version"] = "2.4"
    sbom.write_text(json.dumps(value))
    with pytest.raises(ValueError, match="SBOM project version"):
        release.package(root, tmp_path / "candidate", VERSION, commit)
    value["metadata"]["component"]["version"] = VERSION
    value["components"][0]["version"] = "1.0.0-SNAPSHOT"
    sbom.write_text(json.dumps(value))
    with pytest.raises(ValueError, match="dependencies must not use SNAPSHOT"):
        release.package(root, tmp_path / "candidate", VERSION, commit)


def test_named_manifest_sections_cannot_override_submission_identity(tmp_path):
    path = tmp_path / "pista.jar"
    with zipfile.ZipFile(path, "w") as archive:
        archive.writestr(
            "META-INF/MANIFEST.MF",
            "Implementation-Version: wrong\n\n"
            f"Name: fixture.txt\nImplementation-Version: {VERSION}\nPista-Source-Revision: {'a' * 40}\n",
        )
    with pytest.raises(ValueError, match="JAR version"):
        release.check_jar(path, VERSION, "a" * 40)


def test_archived_source_identity_is_checked_independently(repository, tmp_path):
    root, commit = repository
    write_artifacts(root, commit)
    bundle = tmp_path / "candidate"
    release.package(root, bundle, VERSION, commit)
    archive = bundle / "pista-2.5.0-src.tar.gz"
    with pytest.raises(ValueError, match="archive does not match"):
        release.check_archive(archive, VERSION, "b" * 40)
    wrong_version_archive = tmp_path / "wrong-version.tar.gz"
    subprocess.run(
        [
            "git",
            "-C",
            str(root),
            "archive",
            "--format=tar.gz",
            "--prefix=pista-2.5.1/",
            "--output",
            str(wrong_version_archive),
            commit,
        ],
        check=True,
    )
    with pytest.raises(ValueError, match="reactor revision"):
        release.check_archive(wrong_version_archive, "2.5.1", commit)


def test_provenance_baseline_must_match_the_archived_source(repository, tmp_path):
    root, commit = repository
    write_artifacts(root, commit)
    bundle = tmp_path / "candidate"
    release.package(root, bundle, VERSION, commit)
    manifest = bundle / "release-manifest.json"
    value = json.loads(manifest.read_text())
    value["baseline"]["spark.version"] = "wrong"
    manifest.write_text(json.dumps(value))
    checksums = {**value["assets"], manifest.name: release.sha256(manifest)}
    (bundle / "SHA256SUMS").write_text(
        "".join(f"{digest}  {name}\n" for name, digest in sorted(checksums.items()))
    )
    with pytest.raises(ValueError, match="baseline does not match"):
        release.verify(bundle, VERSION, commit)
