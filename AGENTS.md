# Pista - Project Collaboration Guide

This file is the canonical project-level collaboration guide for Pista. Workspace-wide rules remain in the parent workspace guide. Claude-specific entry guidance is in `CLAUDE.md`; project facts must remain consistent with this file.

## Repository identity and source boundary

Pista is a public Spark SQL task-submission and execution-enhancement framework. The public repository is the canonical collaboration and release target.

`Pista-incubator` is a private source and candidate repository. It may be used for provenance, capability ledgers, and controlled snapshot preparation, but its Git history, remotes, tags, reflog, and unreviewed files must not be copied into Pista.

Public code must be introduced from an approved, no-history snapshot with a reproducible manifest and per-file provenance. Current source existence or passing tests do not by themselves establish a public support guarantee.

## Product boundary

Pista wraps Spark SQL submission and execution. It provides:

- parameterized SQL, templates, multi-statement execution, and single-file submission;
- Catalyst functions and a JVM function catalog;
- Processor, Reader, Writer, output routing, materialization, and metrics extensions;
- target-specific ClickHouse, Doris, Iceberg, and Spark-native output integrations;
- Structured Streaming submission, currently experimental.

Pista does not provide a multi-tenant SQL gateway, general workflow scheduler, resource queue, approval system, CDC platform, Kubernetes control plane, or universal exactly-once delivery. Cross-file dependencies, retries, timeouts, repair runs, and human intervention belong to an external scheduler.

Machine-learning training, inference, model storage, and MLflow lifecycle management belong to Tributo. Pista does not provide a Python client or PyPI runtime artifact.

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

The dependency direction is:

```text
pista-common -> pista-catalyst -> pista-sql
pista-sql -> pista-batch / pista-streaming / pista-connector / pista-metrics
pista-assembly -> approved runtime modules
```

English documentation under `docs/` is canonical. The initial public snapshot intentionally does not include `examples/` or `docs/zh-CN/`; future additions require their own review and manifest update.

## Runtime and consistency boundaries

All entry points must use the shared Spark execution path. Required output failure, missing receipt, failed verification, unknown publication, or partial publication must not be reported as success.

Target guarantees are local to a verified target, transport, table model, version, scope, and preconditions. Pista must not claim cross-target atomicity or universal exactly-once delivery.

Streaming remains Experimental until batch identity, checkpoint, sink failure, replay, and target reconciliation semantics are verified.

## Configuration and security

Pista-owned runtime configuration uses the `spark.pista.*` namespace. Deprecated namespaces must not be silently read or reintroduced.

Credentials must come from environment variables or controlled credential providers. Do not commit passwords, tokens, access keys, private keys, internal endpoints, business data, or real operational topology. Logs, exceptions, metrics, reports, fingerprints, and lineage must not expose secrets, full connection strings, or business SQL literals.

New or changed error identifiers, configuration keys, ServiceLoader entries, public functions, and user-facing capabilities require an explicit compatibility and provenance conclusion.

## Development and GitHub management

Develop changes in a dedicated worktree under `workspace/pista-<branch-name>/`. Do not develop, stage, or commit directly on a canonical `master` worktree.

The Pista `master` branch is protected by the repository Ruleset named `Protect master`:

- changes enter through pull requests;
- at least one approval is required and stale approvals are dismissed after a push;
- `core-gate` and `DCO Check` are required status checks;
- required checks are strict;
- direct pushes are not the normal workflow;
- merge, squash, and rebase are the allowed merge methods;
- `CODEOWNERS` names `@jiangxt2` as the repository owner.

Every commit must use an English subject and include:

```text
Signed-off-by: <name> <email>
```

Do not add AI co-author trailers. Commit, push, pull request, issue, comment, release, and repository-setting changes require explicit authorization.

## Build and validation

The baseline is JDK 17, Scala 2.12.18, Apache Spark 3.5.8, Maven 3.8.8-compatible, and ScalaTest 3.2.16.

Typical local validation:

```bash
mvn -B test
mvn -B package -DskipTests
python3 -B scripts/check_assembly.py
python3 -B scripts/check_public_content.py
python3 -B scripts/check_public_license.py
python3 -B scripts/check_markdown.py
python3 -B scripts/public_manifest.py --check
```

Changes to SQL execution, configuration, assembly, ServiceLoader resources, connectors, metadata, transactions, object storage, or streaming require the relevant real infrastructure tests. Do not report a skipped or unavailable required integration test as passed.

Long-running tests must have a stated matrix, a traceable log, and no duplicate rerun under the same code and environment state.

## Public snapshot and documentation policy

The public snapshot is an English, no-history candidate tree. Its manifest, provenance, license notices, dependency notices, and public-content checks must remain synchronized.

The repository must distinguish Current, Experimental, Planned, and Blocked capabilities. Source enums, skeletons, prototypes, and design documents are not support guarantees.

When a capability is removed, renamed, or moved, update its manifest, configuration, documentation, ServiceLoader resources, tests, and compatibility conclusion as applicable. Do not hide provenance or delete source attribution to make a snapshot appear cleaner.

## Agent completion checklist

Before reporting a change complete:

- confirm the actual worktree and repository;
- review the complete accumulated diff;
- verify the manifest and public-content checks;
- verify configuration, documentation, ServiceLoader, CLI, and error changes;
- record tests, environments, logs, and known omissions;
- distinguish a local prototype, a public snapshot, release readiness, and a merged upstream change;
- do not claim a push, release, or protected-branch configuration until it has been read back from the remote.
