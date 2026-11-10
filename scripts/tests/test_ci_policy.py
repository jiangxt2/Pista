"""Fixtures for CI path classification and required CI gate behavior."""

from __future__ import annotations

import importlib
import json
import re
import sys
from pathlib import Path


SCRIPTS_DIR = Path(__file__).resolve().parents[1]
WORKFLOW_DIR = SCRIPTS_DIR.parent / ".github/workflows"
OFFICIAL_ACTION_PINS = {
    "actions/checkout": "3d3c42e5aac5ba805825da76410c181273ba90b1",
    "actions/download-artifact": "3e5f45b2cfb9172054b4087a40e8e0b5a5461e7c",
    "actions/setup-java": "77dd5b2ce225ea11469928ac317850245a74ae78",
    "actions/setup-python": "5fda3b95a4ea91299a34e894583c3862153e4b97",
    "actions/upload-artifact": "043fb46d1a93c77aae656e7c1c64a875d1fc6a0a",
}
sys.path.insert(0, str(SCRIPTS_DIR))

classify_changes = importlib.import_module("classify_changes")
required_gate = importlib.import_module("check_required_gate")
public_content = importlib.import_module("check_public_content")
check_language = importlib.import_module("check_language")
check_markdown = importlib.import_module("check_markdown")
public_license = importlib.import_module("check_public_license")
sbom_check = importlib.import_module("check_sbom")
RUNTIME_VALIDATION_DOMAINS = tuple(
    domain for domain in classify_changes.DOMAINS if domain != "docs"
)


def test_docs_only_classification() -> None:
    result = classify_changes.classify_paths(["docs/architecture.md"])
    assert result["docs"]
    assert not result["jvm"]
    assert not result["assembly"]
    assert not result["domain_it"]


def test_legal_file_classification_routes_assembly() -> None:
    result = classify_changes.classify_paths(["NOTICE"])
    assert result["docs"]
    assert result["assembly"]
    assert not result["domain_it"]


def test_connector_classification_routes_only_its_domain_and_assembly() -> None:
    paths = {
        "clickhouse": (
            "pista-connector/src/main/scala/com/pista/spark/sql/connector/"
            "clickhouse/ClickHouseWriter.scala"
        ),
        "doris": (
            "pista-connector/src/main/scala/com/pista/spark/sql/connector/"
            "doris/DorisWriter.scala"
        ),
        "iceberg": (
            "pista-batch/src/test/scala/com/pista/spark/sql/batch/"
            "iceberg/SparkSQLSubmitterIcebergSuite.scala"
        ),
    }
    for expected_domain, path in paths.items():
        result = classify_changes.classify_paths([path])
        assert result["jvm"]
        for domain in paths:
            assert result[domain] is (domain == expected_domain)
        assert not result["connector_all"]
        assert not result["domain_all"]
        assert result["domain_it"]
        assert result["assembly"]


def test_shared_test_infrastructure_routes_domain_and_assembly() -> None:
    paths = (
        "pista-test-common/src/test/scala/com/pista/spark/sql/test/container/PistaClickHouseContainer.scala",
        "pista-test-common/src/test/scala/com/pista/spark/sql/test/container/PistaPostgresContainer.scala",
        "pista-test-common/src/test/resources/doris/fe-entrypoint.sh",
    )
    for path in paths:
        result = classify_changes.classify_paths([path])
        assert result["jvm"]
        assert not result["connector_all"]
        assert result["domain_all"]
        assert result["domain_it"]
        assert result["assembly"]


def test_content_metadata_does_not_route_runtime_validation() -> None:
    paths = (
        "config/cjk-allowlist.tsv",
        "config/public-content-allowlist.tsv",
    )
    for path in paths:
        result = classify_changes.classify_paths([path])
        assert result["any"]
        assert not result["policy"]
        for domain in classify_changes.DOMAINS:
            assert not result[domain]
        assert not result["connector_all"]
        assert not result["domain_all"]
        assert not result["domain_it"]


def test_non_workflow_github_metadata_does_not_route_policy_validation() -> None:
    result = classify_changes.classify_paths([".github/CODEOWNERS"])
    assert result["any"]
    assert result["docs"]
    assert not result["policy"]
    for domain in RUNTIME_VALIDATION_DOMAINS:
        assert not result[domain]
    assert not result["connector_all"]
    assert not result["domain_all"]
    assert not result["domain_it"]


def test_policy_change_routes_only_policy_validation() -> None:
    paths = (
        "scripts/classify_changes.py",
        "scripts/check_public_content.py",
        "scripts/check_public_license.py",
    )
    for path in paths:
        result = classify_changes.classify_paths([path])
        assert result["policy"]
        for domain in RUNTIME_VALIDATION_DOMAINS:
            assert not result[domain]
        assert not result["connector_all"]
        assert not result["domain_all"]
        assert not result["domain_it"]


def test_policy_output_does_not_route_runtime_validation() -> None:
    workflow = (WORKFLOW_DIR / "ci.yml").read_text(encoding="utf-8")
    changes = workflow.split("  changes-and-policy:", 1)[1].split(
        "\n  content-safety:", 1
    )[0]
    jvm = workflow.split("  jvm-quality:", 1)[1].split("\n  assembly-contract:", 1)[0]
    assembly = workflow.split("  assembly-contract:", 1)[1].split(
        "\n  clickhouse-it:", 1
    )[0]
    domain_it = workflow.split("  clickhouse-it:", 1)[1].split("\n  core-gate:", 1)[0]

    assert "policy: ${{ steps.classify.outputs.policy }}" in changes
    assert "python-quality:" not in workflow
    assert "python_bridge" not in workflow
    for long_running_job in (jvm, assembly, domain_it):
        assert "outputs.policy" not in long_running_job


def test_policy_tests_job_runs_generic_governance_checks() -> None:
    workflow = (WORKFLOW_DIR / "ci.yml").read_text(encoding="utf-8")
    policy = workflow.split("  policy-tests:", 1)[1].split(
        "\n  vulnerability-scan:", 1
    )[0]
    gate = workflow.split("  core-gate:", 1)[1]

    assert "outputs.policy == 'true'" in policy
    assert "pytest scripts/tests" in policy
    assert "ruff format --check scripts" in policy
    assert "ruff check scripts" in policy
    assert "- policy-tests" in gate


def test_policy_scripts_route_only_their_affected_runtime_validation() -> None:
    assembly = classify_changes.classify_paths(["scripts/check_assembly.py"])
    assert assembly["policy"]
    assert assembly["assembly"]
    assert not assembly["jvm"]
    assert not assembly["domain_it"]

    domain_it = classify_changes.classify_paths(["scripts/run_domain_it.sh"])
    assert domain_it["policy"]
    assert domain_it["domain_all"]
    assert domain_it["domain_it"]
    assert domain_it["assembly"]
    assert domain_it["submitter"]


def test_hosted_runner_preparation_routes_all_dependent_integration_suites() -> None:
    result = classify_changes.classify_paths(["scripts/prepare_hosted_runner.sh"])
    assert result["policy"]
    assert result["domain_all"]
    assert result["domain_it"]
    assert result["submitter"]
    assert result["assembly"]
    assert not result["jvm"]


def test_shared_connector_change_routes_all_connector_shards_only() -> None:
    paths = (
        "pista-connector/pom.xml",
        "pista-connector/src/main/scala/com/pista/spark/sql/connector/BatchWriteConfig.scala",
    )
    for path in paths:
        result = classify_changes.classify_paths([path])
        assert result["jvm"]
        assert result["connector_all"]
        assert not result["domain_all"]
        assert result["domain_it"]
        assert result["assembly"]


def test_streaming_change_does_not_run_unrelated_database_shards() -> None:
    result = classify_changes.classify_paths(
        [
            "pista-streaming/src/main/scala/com/pista/spark/sql/streaming/"
            "StreamingSQLSubmitter.scala"
        ]
    )
    assert result["jvm"]
    assert result["streaming"]
    assert result["assembly"]
    assert not result["connector_all"]
    assert not result["domain_all"]
    assert not result["domain_it"]


def successful_needs() -> dict[str, dict[str, object]]:
    needs = {name: {"result": "success"} for name in required_gate.EXPECTED_JOBS}
    needs["changes-and-policy"]["outputs"] = {
        name: "true"
        for name in (
            "jvm",
            "policy",
            "assembly",
            "clickhouse",
            "doris",
            "iceberg",
            "support",
            "submitter",
        )
    }
    return needs


def test_required_gate_accepts_success_and_not_required_outputs() -> None:
    assert required_gate.gate_failures(successful_needs()) == {}


def test_required_gate_rejects_failure_cancel_skip_and_missing() -> None:
    for result in ("failure", "cancelled", "skipped"):
        needs = successful_needs()
        needs["jvm-quality"]["result"] = result
        assert required_gate.gate_failures(needs)["jvm-quality"] == result

    needs = successful_needs()
    del needs["clickhouse-it"]
    assert required_gate.gate_failures(needs)["clickhouse-it"] == "missing"


def test_selected_jobs_fail_closed_and_only_explicit_unselected_may_skip() -> None:
    for shard in ("clickhouse", "doris", "iceberg", "support", "submitter"):
        job = f"{shard}-it"
        for result in ("failure", "cancelled", "skipped"):
            needs = successful_needs()
            needs[job]["result"] = result
            assert required_gate.gate_failures(needs)[job] == result
        needs = successful_needs()
        needs["changes-and-policy"]["outputs"][shard] = "false"
        needs[job]["result"] = "skipped"
        assert required_gate.gate_failures(needs) == {}
        del needs["changes-and-policy"]["outputs"][shard]
        assert required_gate.gate_failures(needs)[job] == "skipped"

    needs = successful_needs()
    needs["changes-and-policy"]["outputs"]["assembly"] = "false"
    assert required_gate.gate_failures(needs)["classification.assembly"]
    needs["changes-and-policy"]["result"] = "failure"
    assert required_gate.gate_failures(needs)["changes-and-policy"] == "failure"


def test_shared_and_unknown_changes_route_all_integration_tests() -> None:
    for path in (
        "pom.xml",
        "pista-common/src/main/scala/Example.scala",
        "pista-sql/src/main/scala/Example.scala",
        "pista-test-common/src/test/resources/test.sql",
        "sqls/postgre/00_meta.sql",
        "scripts/run_domain_it.sh",
        ".github/workflows/ci.yml",
        "new-area/config.yaml",
        "README.md.sh",
        "docs/build.py",
        ".mvn/wrapper/maven-wrapper.properties",
        "mvnw",
    ):
        result = classify_changes.classify_paths([path])
        assert all(
            result[name]
            for name in ("clickhouse", "doris", "iceberg", "support", "submitter")
        ), path
        assert result["assembly"]


def test_target_production_and_test_changes_route_submitter_appropriately() -> None:
    root = "pista-connector/src/{}/scala/com/pista/spark/sql/connector/clickhouse/Reader.scala"
    production = classify_changes.classify_paths([root.format("main")])
    test = classify_changes.classify_paths([root.format("test")])
    assert production["clickhouse"] and production["submitter"]
    assert test["clickhouse"] and not test["submitter"]
    assert not test["doris"] and not test["support"]


def test_changed_paths_preserve_both_sides_of_renames(monkeypatch) -> None:
    calls = []

    def fake_git_lines(root, *args):
        calls.append(args)
        return [
            "pista-connector/src/main/scala/com/pista/spark/sql/connector/clickhouse/Reader.scala",
            "pista-connector/src/main/scala/com/pista/spark/sql/connector/doris/Reader.scala",
        ]

    monkeypatch.setattr(classify_changes, "git_lines", fake_git_lines)
    monkeypatch.setattr(
        classify_changes.subprocess, "run", lambda *args, **kwargs: None
    )
    paths = classify_changes.changed_files(Path("."), "base", "head")
    assert "--no-renames" in calls[0] and "-z" in calls[0]
    result = classify_changes.classify_paths(paths)
    assert result["clickhouse"] and result["doris"] and result["submitter"]


def test_manual_shard_selection_is_explicit(monkeypatch, capsys) -> None:
    monkeypatch.delenv("GITHUB_OUTPUT", raising=False)
    monkeypatch.setattr(classify_changes, "changed_files", lambda *_: [])
    monkeypatch.setattr(sys, "argv", ["classify_changes.py", "--selection", "doris"])
    assert classify_changes.main() == 0
    outputs = dict(line.split("=", 1) for line in capsys.readouterr().out.splitlines())
    assert outputs["doris"] == "true"
    assert outputs["clickhouse"] == outputs["submitter"] == "false"


def test_manual_dispatch_does_not_cancel_automatic_runs() -> None:
    workflow = (WORKFLOW_DIR / "ci.yml").read_text(encoding="utf-8")
    concurrency = workflow.split("concurrency:", 1)[1].split("\njobs:", 1)[0]
    assert "github.event_name == 'workflow_dispatch'" in concurrency
    assert "format('manual-{0}', github.run_id)" in concurrency
    assert "github.event.pull_request.number || github.ref" in concurrency


def test_required_gate_checks_out_repository_script() -> None:
    workflow = (SCRIPTS_DIR.parent / ".github/workflows/ci.yml").read_text(
        encoding="utf-8"
    )
    gate = workflow.split("  core-gate:", 1)[1]

    assert "actions/checkout@" in gate
    assert gate.index("actions/checkout@") < gate.index(
        "python3 -B scripts/check_required_gate.py"
    )


def test_vulnerability_scan_callers_use_local_workflow() -> None:
    for name in ("ci.yml", "nightly.yml", "release-candidate.yml"):
        workflow = (WORKFLOW_DIR / name).read_text(encoding="utf-8")
        assert "uses: ./.github/workflows/osv-scan.yml" in workflow
        assert "osv-scanner-reusable.yml@" not in workflow


def test_workflows_pin_current_official_actions() -> None:
    workflows = "\n".join(
        path.read_text(encoding="utf-8") for path in WORKFLOW_DIR.glob("*.yml")
    )
    for action, commit in OFFICIAL_ACTION_PINS.items():
        marker = f"uses: {action}@"
        references = [line for line in workflows.splitlines() if marker in line]
        assert references
        assert all(f"{action}@{commit}" in line for line in references)


def test_local_osv_workflow_uses_least_privilege() -> None:
    workflow = (WORKFLOW_DIR / "osv-scan.yml").read_text(encoding="utf-8")
    assert "workflow_call:" in workflow
    assert "security-events:" not in workflow
    assert "permissions:\n  contents: read" in workflow
    assert (
        "google/osv-scanner-action/osv-scanner-action@"
        "6e4298ebc4db23e847df9b2e2de2939d6f066c67"
    ) in workflow
    assert (
        "google/osv-scanner-action/osv-reporter-action@"
        "6e4298ebc4db23e847df9b2e2de2939d6f066c67"
    ) in workflow
    assert "--fail-on-vuln=true" in workflow
    assert "scripts/generate_sbom.sh" in workflow
    assert "--recursive" not in workflow
    for name in sbom_check.EXPECTED_FILES:
        assert f"--lockfile=target/sbom/{name}" in workflow


def test_doris_test_runtime_handles_hosted_runner_limits() -> None:
    project_root = SCRIPTS_DIR.parent
    fe = (
        project_root / "pista-test-common/src/test/resources/doris/fe-entrypoint.sh"
    ).read_text(encoding="utf-8")
    be = (
        project_root / "pista-test-common/src/test/resources/doris/be-entrypoint.sh"
    ).read_text(encoding="utf-8")
    container = (
        project_root
        / "pista-test-common/src/test/scala/com/pista/spark/sql/test/container/PistaDorisContainer.scala"
    ).read_text(encoding="utf-8")

    assert "-XX:-UseContainerSupport" in fe
    assert "-XX:-UseContainerSupport" in be
    assert "set_config storage_high_watermark_usage_percent 99" in fe
    assert "set_config storage_min_left_capacity_bytes 536870912" in fe
    for script in (fe, be):
        assert "set_config storage_flood_stage_usage_percent 99" in script
        assert "set_config storage_flood_stage_left_capacity_bytes 536870912" in script
    assert "bes.foreach(_.start())" in container
    assert "Startables.deepStart" not in container
    assert "exec bash /usr/local/bin/init_be.sh" in be
    assert "exec bash /usr/local/bin/entry_point.sh" not in be


def test_clickhouse_test_runtime_limits_hosted_runner_startup_pressure() -> None:
    project_root = SCRIPTS_DIR.parent
    container = (
        project_root
        / "pista-test-common/src/test/scala/com/pista/spark/sql/test/container/PistaClickHouseContainer.scala"
    ).read_text(encoding="utf-8")

    assert "Thread.sleep(index * 5000L)" in container
    assert "withLogConsumer(ContainerLogUtils.streamToReport" in container
    assert "withFileSystemBind(configDir.toString" not in container
    assert '"/etc/clickhouse-server/config.d/$fileName"' in container
    assert 'PosixFilePermissions.fromString("rw-r--r--")' in container
    for setting in (
        "<logger><console>true</console></logger>",
        "<max_thread_pool_size>256</max_thread_pool_size>",
        "<background_pool_size>16</background_pool_size>",
        "<background_schedule_pool_size>16</background_schedule_pool_size>",
        "<uncompressed_cache_size>67108864</uncompressed_cache_size>",
        "<mark_cache_size>67108864</mark_cache_size>",
    ):
        assert setting in container


def test_container_it_prepares_hosted_runner_disk() -> None:
    project_root = SCRIPTS_DIR.parent
    script = (SCRIPTS_DIR / "prepare_hosted_runner.sh").read_text(encoding="utf-8")

    assert '"${GITHUB_ACTIONS:-}" != "true"' in script
    assert '"${RUNNER_ENVIRONMENT:-}" != "github-hosted"' in script
    assert "20 * 1024 * 1024 * 1024" in script
    assert 'sudo find "$target" -xdev -mindepth 1 -delete' in script
    for target in (
        "/usr/local/lib/android",
        "/usr/share/dotnet",
        "/opt/ghc",
        "/usr/local/.ghcup",
    ):
        assert target in script

    for name in ("ci.yml", "nightly.yml", "release-candidate.yml"):
        workflow = (project_root / ".github/workflows" / name).read_text(
            encoding="utf-8"
        )
        preparation = workflow.index("scripts/prepare_hosted_runner.sh")
        domain_it = workflow.index("scripts/run_domain_it.sh")
        assert preparation < domain_it

    ci = (project_root / ".github/workflows/ci.yml").read_text(encoding="utf-8")
    for shard in ("clickhouse", "doris", "iceberg", "support", "submitter"):
        job = re.split(
            r"\n  [a-z][\w-]*:", ci.split(f"  {shard}-it:", 1)[1], maxsplit=1
        )[0]
        assert f"outputs.{shard} == 'true'" in job
        assert job.index("scripts/prepare_hosted_runner.sh") < job.index(
            "scripts/run_domain_it.sh"
        )


def test_domain_it_isolated_shards_cover_every_infrastructure_family() -> None:
    project_root = SCRIPTS_DIR.parent
    script = (SCRIPTS_DIR / "run_domain_it.sh").read_text(encoding="utf-8")

    expected_shards = {
        "clickhouse": (
            "com.pista.spark.sql.clickhouse.meta,"
            "com.pista.spark.sql.connector.clickhouse",
            11,
        ),
        "doris": (
            "com.pista.spark.sql.doris.meta,com.pista.spark.sql.connector.doris",
            31,
        ),
        "iceberg": ("com.pista.spark.sql.batch.iceberg", 5),
        "support": (
            "com.pista.spark.sql.test.util,com.pista.spark.sql.batch,"
            "com.pista.spark.sql.test.container",
            4,
        ),
    }
    for name, (packages, count) in expected_shards.items():
        assert f"    {name})" in script
        assert packages in script
        assert f"expected_tests={count}" in script

    assert "-DmembersOnlySuites=" in script
    assert "actual_tests" in script
    assert "expected_tests" in script
    assert "set -- clickhouse doris iceberg support" in script
    assert 'run_requested_shard "$shard"' in script
    assert "Unknown Domain IT shard" in script
    assert sum(count for _, count in expected_shards.values()) == 51

    for name in ("ci.yml", "nightly.yml", "release-candidate.yml"):
        workflow = (project_root / ".github/workflows" / name).read_text(
            encoding="utf-8"
        )
        assert "scripts/run_domain_it.sh" in workflow
        assert "run: mvn -B -Pit test" not in workflow

    ci = (project_root / ".github/workflows/ci.yml").read_text(encoding="utf-8")
    for shard in ("clickhouse", "doris", "iceberg", "support", "submitter"):
        job = re.split(
            r"\n  [a-z][\w-]*:", ci.split(f"  {shard}-it:", 1)[1], maxsplit=1
        )[0]
        assert f"scripts/run_domain_it.sh {shard}" in job
        assert f"name: {shard}-it-reports" in job
        assert "**/target/surefire-reports/pista-it/**" in job
        assert ("actions/download-artifact@" in job) is (shard == "submitter")
        assert f"- {shard}-it" in ci.split("  core-gate:", 1)[1]


def test_sbom_generation_excludes_provided_components() -> None:
    script = (SCRIPTS_DIR / "generate_sbom.sh").read_text(encoding="utf-8")
    assert "-DincludeProvidedScope=false" in script


def test_ci_toolchains_use_pinned_java_and_maven_wrapper() -> None:
    root = SCRIPTS_DIR.parent
    properties = (root / ".mvn/wrapper/maven-wrapper.properties").read_text()
    assert "apache-maven/3.9.5/apache-maven-3.9.5-bin.zip" in properties
    assert (
        "distributionSha256Sum=7822eb593d29558d8edf87845a2c47e36e2a89d17a84cd2390824633214ed423"
        in properties
    )
    windows_wrapper = (root / "mvnw.cmd").read_text()
    assert (
        'if ($USE_MVND) { "/maven/mvnd/" } else { "/org/apache/maven/" }'
        in windows_wrapper
    )
    assert "Remove-Item" not in windows_wrapper
    assert "rm -rf" not in (root / "mvnw").read_text()
    for workflow_name in (
        "ci.yml",
        "nightly.yml",
        "release-candidate.yml",
        "osv-scan.yml",
    ):
        workflow = (WORKFLOW_DIR / workflow_name).read_text()
        assert 'java-version: "17.0.18.9.1"' in workflow
        assert not re.search(r"(?<![\w./])mvn -B", workflow)
    for script_name in ("run_domain_it.sh", "generate_sbom.sh"):
        script = (SCRIPTS_DIR / script_name).read_text()
        assert "./mvnw" in script
        assert not re.search(r"(?<![\w./])mvn (?:-B|\")", script)


def test_only_exact_apache_wrapper_scripts_may_have_asf_header(
    tmp_path: Path, monkeypatch
) -> None:
    root = SCRIPTS_DIR.parent
    for name in public_license.MAVEN_WRAPPER_SHA256:
        (tmp_path / name).write_bytes((root / name).read_bytes())
    other = tmp_path / "other.sh"
    other.write_text(public_license.ASF_HEADER, encoding="utf-8")
    monkeypatch.setattr(
        public_license, "candidate_files", lambda _: ["mvnw", "mvnw.cmd", "other.sh"]
    )
    assert public_license.source_header_findings(tmp_path) == [
        "unexpected-asf-header: other.sh"
    ]
    (tmp_path / "mvnw").write_text("altered", encoding="utf-8")
    assert (
        "invalid-apache-maven-wrapper: mvnw"
        in public_license.source_header_findings(tmp_path)
    )


def test_release_candidate_binds_identity_and_scans_the_delivered_sbom() -> None:
    workflow = (WORKFLOW_DIR / "release-candidate.yml").read_text(encoding="utf-8")
    scanner = (WORKFLOW_DIR / "osv-scan.yml").read_text(encoding="utf-8")
    check = workflow.index("prepare_release.py check")
    package = workflow.index("prepare_release.py package")
    assert check < workflow.index("./mvnw -B test")
    assert workflow.index("scripts/run_domain_it.sh submitter") < package
    assert "expected_commit:" in workflow and "version:" in workflow
    assert '-Dpista.source.revision="$SOURCE_COMMIT"' in workflow
    assert "path: target/release/*" in workflow
    assert "if: always()" in workflow and "**/target/surefire-reports/**" in workflow
    assert "needs: build-candidate" in workflow
    assert (
        "candidate_artifact: ${{ needs.build-candidate.outputs.artifact_name }}"
        in workflow
    )
    assert "inputs.candidate_artifact == ''" in scanner
    assert "inputs.candidate_artifact != ''" in scanner
    assert "prepare_release.py verify" in scanner
    assert (
        "cp target/release/pista-jvm.cdx.json target/sbom/pista-jvm.cdx.json" in scanner
    )
    assert "contents: write" not in workflow


def test_jvm_sbom_rejects_provided_components(tmp_path: Path) -> None:
    sbom = tmp_path / "pista-jvm.cdx.json"
    sbom.write_text(
        json.dumps(
            {
                "bomFormat": "CycloneDX",
                "specVersion": "1.6",
                "components": [
                    {
                        "name": "spark-core_2.12",
                        "version": "3.5.8",
                        "purl": "pkg:maven/org.apache.spark/spark-core_2.12@3.5.8",
                    }
                ],
            }
        ),
        encoding="utf-8",
    )

    assert any(
        finding.startswith("provided-jvm-component:")
        for finding in sbom_check.validate_sbom(sbom)
    )


def test_content_rules_detect_constructed_identity_and_business_markers() -> None:
    samples = (
        "".join(("China", " Mobile")),
        "".join(("prov", "_id")),
        "".join(("statis", "_ymd")),
        "".join(("10", "900")),
    )
    matched = {
        rule.rule_id
        for sample in samples
        for rule in public_content.RULES
        if rule.pattern.search(sample)
    }
    assert {
        "identity-en",
        "business-province-id",
        "business-statistics-date",
        "business-province-code",
    } <= matched


def test_secret_assignment_distinguishes_fixture_and_runtime_values() -> None:
    unsafe = "".join(("password", ' = "non-public-value"'))
    safe = "".join(("password", ' = "test-password"'))
    assert public_content.check_secret_assignment(unsafe)
    assert not public_content.check_secret_assignment(safe)


def test_content_rules_detect_constructed_key_and_token_markers() -> None:
    samples = (
        "".join(("-----BEGIN ", "PRIVATE KEY-----")),
        "".join(("AK", "IA", "1234567890ABCDEF")),
        "".join(("gh", "p_", "a" * 36)),
    )
    matched = {
        rule.rule_id
        for sample in samples
        for rule in public_content.RULES
        if rule.pattern.search(sample)
    }
    assert {"private-key", "aws-access-key", "github-token"} <= matched


def test_sensitive_log_context_detection() -> None:
    unsafe = "".join(('logInfo(s"URL: $', 'jdbcUrl")'))
    safe = 'logInfo("JDBC destination configured")'
    assert public_content.LOG_CALL.search(unsafe)
    assert public_content.UNSAFE_LOG_VALUE.search(unsafe)
    assert not public_content.UNSAFE_LOG_VALUE.search(safe)


def test_language_pattern_detects_cjk_and_allows_ascii() -> None:
    assert check_language.CJK.search(chr(0x4E2D))
    assert not check_language.CJK.search("English canonical documentation")


def test_public_suffix_snapshot_checksum_and_notice() -> None:
    project_root = SCRIPTS_DIR.parent
    assert public_license.public_data_findings(project_root) == []


def test_markdown_checker_reports_missing_local_link(tmp_path: Path) -> None:
    for relative in check_markdown.REQUIRED_CANONICAL:
        path = tmp_path / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("# Canonical\n", encoding="utf-8")
    page = tmp_path / "docs/page.md"
    page.write_text("# Page\n\n[missing](absent.md)\n", encoding="utf-8")
    findings = check_markdown.markdown_findings(tmp_path)
    assert any(finding.startswith("missing-link: docs/page.md") for finding in findings)


def test_markdown_checker_requires_exact_translation_pointer(tmp_path: Path) -> None:
    for relative in check_markdown.REQUIRED_CANONICAL:
        path = tmp_path / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("# Canonical\n", encoding="utf-8")
    translation = tmp_path / "docs/zh-CN/architecture.md"
    translation.parent.mkdir(parents=True, exist_ok=True)
    translation.write_text("# Translation\n", encoding="utf-8")

    findings = check_markdown.markdown_findings(tmp_path)

    assert "missing-canonical-pointer: docs/zh-CN/architecture.md" in findings
