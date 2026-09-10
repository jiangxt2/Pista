# Pista Project Collaboration Guide

This file is the canonical project-level collaboration guide for Pista. Workspace-wide rules remain in the parent workspace guide. `CLAUDE.md` is an entry-point guide and must remain consistent with this file.

## Repository identity and source boundary

Pista is a public JVM and Spark SQL runtime for recoverable data delivery to OLAP engines. This repository is the canonical source, collaboration, and release target.

The repository must preserve an auditable relationship between source files, public paths, manifests, provenance records, documentation, tests, and release artifacts. A file being present, a unit test passing, or an implementation being reachable does not by itself establish a public support guarantee.

Public support requires an approved scope, documented configuration and entry points, compatible behavior, direct contract tests, relevant real-infrastructure evidence, and recovery evidence when the capability has external side effects.

## Product boundary

Pista connects Spark SQL computation with target delivery protocols. It owns:

- parameterized SQL, templates, statement splitting, and single-file submission;
- Catalyst expressions and the JVM function catalog;
- Processor, Reader, Writer, output routing, and materialization extensions;
- batch execution and Structured Streaming integration;
- ClickHouse, Doris, Iceberg, and Spark-native output integrations;
- receipts, verification, publication, reconciliation, metrics, reports, and data-quality hooks where implemented and verified.

Pista does not provide a multi-tenant SQL gateway, a general workflow scheduler, a resource queue, an approval system, a complete CDC platform, a Kubernetes control plane, or universal cross-target exactly-once delivery. Cross-file dependencies, retries, timeouts, repair runs, and human intervention belong to an external scheduler.

Machine-learning training, inference, model storage, and MLflow lifecycle management belong to Tributo. Pista does not provide a Python client or a PyPI runtime artifact.

## Source layout and module boundaries

The current Maven reactor contains:

```text
pista-common
pista-catalyst
pista-sql
pista-test-common
pista-batch
pista-streaming
pista-connector
pista-metrics
pista-clickhouse-meta
pista-doris-meta
pista-assembly
```

The high-level dependency direction is:

```text
pista-common -> pista-catalyst -> pista-sql
pista-sql -> pista-batch / pista-streaming / pista-connector / pista-metrics
pista-assembly -> approved runtime modules
```

Module responsibilities are:

| Module | Responsibility |
| --- | --- |
| `pista-common` | Shared errors and low-level utilities |
| `pista-catalyst` | Catalyst expressions, function metadata, and function implementations |
| `pista-sql` | Configuration, function installation, Reader/Writer SPI, Processor support, and shared SQL execution utilities |
| `pista-test-common` | Shared Spark, database, and Testcontainers fixtures |
| `pista-batch` | SQL submission, templating, statement execution, and output routing |
| `pista-streaming` | Structured Streaming submission, source, parser, trigger, sink, and listener integration |
| `pista-connector` | ClickHouse, Doris, Iceberg, and Spark-native Reader/Writer integrations |
| `pista-metrics` | SQL, resource, data-quality, and streaming metrics |
| `pista-clickhouse-meta` | ClickHouse delivery and continuation metadata |
| `pista-doris-meta` | Doris task and transaction metadata |
| `pista-assembly` | Runtime assembly, ServiceLoader merging, and submission JAR generation |

Spark dependencies use `provided` scope where appropriate. Database drivers, cloud SDKs, and target-specific dependencies must not leak into lower-level runtime artifacts without an explicit dependency and packaging decision.

## Execution model and consistency boundaries

The current batch execution path is centered on `SparkSQLSubmitter`, `SQLExecutionContext`, `SQLStatementExecutor`, and `OutputRouter`. New behavior must use these established extension points or an explicitly reviewed replacement; do not add target-name branches to the submitter or router.

The target runtime model is:

```text
JobSpec
  -> plan and fingerprint
  -> one JVM runtime
       -> Spark computation
       -> delivery prepare/write/receipt
       -> verify/publish/reconcile
       -> operation state
       -> structured outcome and observability
```

Delivery behavior must distinguish preparation, writing, receipt, verification, publication, and reconciliation. `UNKNOWN`, partial publication, cleanup pending, and manual intervention are explicit outcomes and must not be converted into success.

Guarantees are local to a verified target, transport, table model, version, scope, and set of preconditions. Pista must not claim cross-target atomicity or universal exactly-once behavior. A target adapter must declare the guarantees it can actually provide.

The driver owns planning, stable identities, and state coordination. Executors receive versioned data-transfer objects rather than plugin-internal objects. Public outcomes must be structured and must not rely only on logs, booleans, or the absence of an exception.

## Capability status

Documentation, code, and tests must distinguish these states:

- **Current**: implemented, configured, documented, and covered by the evidence required for its stated guarantee;
- **Experimental**: usable for evaluation but missing part of the compatibility, recovery, or operational evidence;
- **Planned**: design or implementation direction without a complete supported path;
- **Blocked**: intentionally unavailable until a stated dependency, governance decision, or safety condition is resolved.

Structured Streaming remains Experimental until batch identity, checkpoint behavior, sink failure, replay, and target reconciliation are verified together. Target recovery guarantees are target-scoped and must cover unknown submission, duplicate submission, lost receipts, timeouts, process failure, and reconciliation where applicable.

## Configuration, logging, and security

Pista-owned runtime configuration uses the `spark.pista.*` namespace. Deprecated namespaces must not be silently read or reintroduced. Properties files use English comments.

Credentials must come from environment variables or controlled credential providers. Do not commit passwords, tokens, access keys, private keys, internal endpoints, private topology, business data, or local machine configuration.

Validate template parameters, SQL inputs, paths, URIs, and connector options. Logs, exceptions, metrics, reports, fingerprints, and lineage must not expose secrets, full connection strings, or business SQL literals.

Use the project logging facade or Spark logging facilities. Do not use `System.out.println` in production code. Include stable run, job, attempt, and target identities where useful without disclosing sensitive values.

Business failures use the project error catalog. Preserve the root cause and actionable context. Do not swallow exceptions or silently fall back to a weaker consistency mode. `fail-fast` and `continue-on-error` control scheduling policy only; a required output failure remains a failure.

## Development and GitHub workflow

Develop each change in a dedicated worktree under `workspace/pista-<branch-name>/`. Keep the canonical `master` worktree clean and use it only for read-only inspection and synchronization. Do not develop, stage, or commit feature changes directly on `master`.

Before changing files or running project Git operations, verify the actual repository with `git rev-parse --show-toplevel`. A worktree must belong to the Pista repository and must use a branch name that describes the feature without temporary PR or phase numbers.

Configure the worktree-specific Git identity immediately after creating a worktree:

```bash
git config user.name "<name>"
git config user.email "<email>"
```

Changes normally enter `master` through a pull request and the repository's required checks. Commit, push, pull request, issue, comment, release, repository-setting, and artifact-publication actions require explicit authorization. Never use `--no-verify` to bypass required checks.

Every commit must have an English subject and a valid `Signed-off-by` trailer. Do not add AI co-author trailers unless the repository policy explicitly requires them.

## Design and implementation rules

### Reuse and dependency direction

Search the existing Expression, UDF, Processor, Reader, Writer, configuration, error, and utility implementations before adding new code. Prefer existing Pista behavior, then Spark public APIs, and only then a new dependency or implementation. Do not add wrappers for equivalent Spark SQL behavior without independent semantic value.

### Spark session lifecycle

Use the controlled execution context and shared Spark session lifecycle. Do not create or close a Spark session from a Processor, Writer, or UDF. Timeouts and cancellation must use verifiable Spark job-group or equivalent cancellation mechanisms rather than only stopping a waiting thread.

### Processor and Catalyst functions

Processors are discovered through the existing Manager/SPI mechanism; do not add name-based branches to submitters. `pista-catalyst` is the single function-definition artifact and must not depend on `SparkSession`, `SessionState`, or session extensions.

Use Catalyst expressions rather than Scala UDFs or `CodegenFallback` for performance paths. Interpreted and code-generated execution must agree on type, nullability, foldability, determinism, canonicalization, null handling, invalid input, and error behavior. Function tests must cover SQL registration, optimization, code generation, and boundary values.

### Readers, Writers, and Delivery

Reuse `AbstractDataReader`, `AbstractDataWriter`, and ServiceLoader registration where they provide the correct boundary. Target writes must go through delivery adapters and coordination ports rather than target branches in `OutputRouter` or submitters.

ClickHouse and Doris changes must be reviewed across batching, overwrite behavior, sharding or partitioning, metadata, transactions, failover, continuation, receipt, and reconciliation. Missing optional metadata may be handled only where the documented target guarantee permits it; strong-consistency paths must fail closed when required coordination is unavailable.

### Checkpoints and external orchestration

DataFrame checkpoints manage intermediate-result lifetime. Structured Streaming checkpoints remain under Spark's control. Pista does not provide a cross-file DAG or parse scheduler annotations for dependencies, retries, timeouts, or error policy. Recovery must reconcile target receipts before retrying; an indeterminate submission enters `UNKNOWN` or manual intervention rather than being blindly rewritten.

## Build and validation

The baseline is JDK 17, Scala 2.12.18, Apache Spark 3.5.8, Maven 3.8.8-compatible, and ScalaTest 3.2.16.

Before Maven or Scala validation, confirm the active JDK and build-tool versions. Typical local checks are:

```bash
mvn -B test
mvn -B package -DskipTests
python3 -B scripts/check_assembly.py
python3 -B scripts/check_public_content.py
python3 -B scripts/check_public_license.py
python3 -B scripts/check_markdown.py
python3 -B scripts/public_manifest.py --check
```

The submission artifact is `pista-assembly/target/pista-*.jar`. Do not use a module JAR as the Spark-submit runtime or hand-assemble multiple module JARs.

Select tests according to the changed behavior:

- pure algorithms and configuration: focused unit tests;
- SPI, serialization, compatibility, and state transitions: contract or differential tests;
- SQL, Catalog, Connector, metadata, transactions, object storage, and streaming: the corresponding real-infrastructure tests;
- assembly or ServiceLoader changes: assembly contract checks and final JAR inspection.

Required integration tests must not be reported as passed when they were skipped or unavailable. Long-running tests require an explicit matrix, traceable logs, recorded environment details, and no duplicate rerun under the same code and environment state.

## Public documentation and release policy

English documentation under `docs/` is canonical. Keep README files, examples, configuration, logs, fixtures, manifests, license notices, dependency notices, and generated reports consistent with the public repository boundary.

When a capability is removed, renamed, or moved, update its configuration, documentation, ServiceLoader resources, tests, manifest, and compatibility conclusion as applicable. Do not remove attribution or provenance records to make a change appear simpler.

Before a public release or artifact publication, complete the applicable authorization, license, copyright, NOTICE, dependency-license, secret, PII, private-endpoint, binary, SBOM, clean-install, assembly-content, and real-infrastructure checks. A prototype, skeleton, enum, design document, or generated report is not a support guarantee.

## Completion checklist

Before reporting a change complete:

- confirm the actual Pista worktree, repository, branch, and remote;
- review the complete accumulated diff and the relevant unchanged context;
- verify configuration, documentation, ServiceLoader, CLI, error, manifest, and packaging changes;
- verify that new behavior has direct tests and that required real-infrastructure evidence is present;
- record test commands, environments, logs, known omissions, and recovery limitations;
- check for secrets, private endpoints, internal topology, business data, and unintended generated files;
- distinguish a local prototype, an Experimental capability, a supported Current capability, release readiness, and a merged change;
- do not claim a commit, push, pull request, release, or remote setting until it has actually been performed and read back.
