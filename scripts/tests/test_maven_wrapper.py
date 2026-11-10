"""Exercise the wrapper's portable checksum verification without network access."""

from __future__ import annotations

import hashlib
import os
import shutil
import subprocess
import sys
import zipfile
from pathlib import Path


def test_checksum_falls_back_and_rejects_modified_archive(tmp_path: Path) -> None:
    project = tmp_path / "project"
    project.mkdir()
    wrapper = project / "mvnw"
    shutil.copy2(Path(__file__).resolve().parents[2] / "mvnw", wrapper)
    wrapper.chmod(0o755)
    repo = tmp_path / "repo"
    archive = repo / "org/apache/maven/apache-maven/3.9.5/apache-maven-3.9.5-bin.zip"
    archive.parent.mkdir(parents=True)
    info = zipfile.ZipInfo("apache-maven-3.9.5/bin/mvn")
    info.create_system = 3
    info.external_attr = 0o100755 << 16
    with zipfile.ZipFile(archive, "w") as output:
        output.writestr(info, "#!/bin/sh\nprintf '%s\\n' verified-wrapper\n")
    digest = hashlib.sha256(archive.read_bytes()).hexdigest()
    properties = project / ".mvn/wrapper/maven-wrapper.properties"
    properties.parent.mkdir(parents=True)
    properties.write_text(
        "distributionUrl=https://repo.maven.apache.org/maven2/org/apache/maven/"
        "apache-maven/3.9.5/apache-maven-3.9.5-bin.zip\n"
        f"distributionSha256Sum={digest}\n",
        encoding="utf-8",
    )
    binaries = tmp_path / "bin"
    binaries.mkdir()

    def command(name: str, body: str) -> None:
        path = binaries / name
        path.write_text(f"#!{sys.executable}\n{body}\n", encoding="utf-8")
        path.chmod(0o755)

    command("sha256sum", "raise SystemExit(2)")
    command(
        "shasum",
        "import hashlib, os, pathlib, sys\n"
        "expected, filename = sys.stdin.read().rstrip('\\n').split('  ', 1)\n"
        "pathlib.Path(os.environ['PISTA_CHECKSUM_MARKER']).touch()\n"
        "raise SystemExit(hashlib.sha256(pathlib.Path(filename).read_bytes()).hexdigest() != expected)",
    )
    command(
        "wget",
        "import shutil, sys, urllib.parse\n"
        "url = next(arg for arg in sys.argv if arg.startswith('file:'))\n"
        "shutil.copyfile(urllib.parse.unquote(urllib.parse.urlparse(url).path), "
        "sys.argv[sys.argv.index('-O') + 1])",
    )
    command("trash", "raise SystemExit(1)")
    temp = tmp_path / "temp"
    temp.mkdir()
    marker = tmp_path / "used-fallback"
    env = os.environ.copy()
    env.update(
        PATH=f"{binaries}:/usr/bin:/bin",
        TMPDIR=str(temp),
        MVNW_REPOURL=repo.as_uri(),
        MAVEN_USER_HOME=str(tmp_path / "good-cache"),
        PISTA_CHECKSUM_MARKER=str(marker),
    )
    env.pop("MVNW_USERNAME", None)
    env.pop("MVNW_PASSWORD", None)
    success = subprocess.run(
        [str(wrapper), "-version"], env=env, text=True, capture_output=True
    )
    assert success.returncode == 0, success.stderr
    assert "verified-wrapper" in success.stdout and marker.exists()

    with archive.open("ab") as modified:
        modified.write(b"changed")
    env["MAVEN_USER_HOME"] = str(tmp_path / "bad-cache")
    failure = subprocess.run(
        [str(wrapper), "-version"], env=env, text=True, capture_output=True
    )
    assert failure.returncode != 0
    assert "Failed to validate Maven distribution SHA-256" in failure.stderr
    assert not list((tmp_path / "bad-cache").rglob("bin/mvn"))
