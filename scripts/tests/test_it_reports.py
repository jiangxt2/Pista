"""Coverage checks reject reports that cannot prove the selected tests ran."""

from __future__ import annotations

import base64
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from verify_it_reports import verify


def test_discovery_and_execution_must_match(tmp_path: Path) -> None:
    directory = tmp_path / "module/target/pista-it-example"
    directory.mkdir(parents=True)

    def encode(value: str) -> str:
        return base64.b64encode(value.encode()).decode()

    row = f"doris\t{encode('example.Suite')}\t{encode('writes data')}\n"
    external = f"external\t{encode('com.pista.spark.sql.batch.iceberg.SparkSQLSubmitterIcebergRestIT')}\n"
    (directory / "discovered.tsv").write_text(row + external, encoding="utf-8")
    verify(tmp_path, "doris", directory.name, True)
    with pytest.raises(ValueError, match="missing"):
        verify(tmp_path, "doris", directory.name, False)
    report = directory / "TEST-example.xml"
    report.write_text(
        '<testsuite errors="0" failures="0"><testcase classname="example.Suite" name="writes data"/></testsuite>',
        encoding="utf-8",
    )
    verify(tmp_path, "doris", directory.name, False)
    report.write_text('<testsuite errors="1" failures="0"/>', encoding="utf-8")
    with pytest.raises(ValueError, match="aborted"):
        verify(tmp_path, "doris", directory.name, False)
    report.write_text(
        '<testsuite errors="0" failures="0"><testcase classname="example.Suite" name="writes data"/><testcase classname="example.Suite" name="writes data"/></testsuite>',
        encoding="utf-8",
    )
    with pytest.raises(ValueError, match="repeated"):
        verify(tmp_path, "doris", directory.name, False)
    report.write_text(
        '<testsuite errors="0" failures="0"><testcase classname="example.Suite" name="writes data"><skipped/></testcase></testsuite>',
        encoding="utf-8",
    )
    with pytest.raises(ValueError, match="Non-successful"):
        verify(tmp_path, "doris", directory.name, False)
    (directory / "discovered.tsv").write_text(row + row + external, encoding="utf-8")
    with pytest.raises(ValueError, match="Duplicate discovered"):
        verify(tmp_path, "doris", directory.name, True)
