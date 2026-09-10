# Pista - Claude Code Guide

Read `AGENTS.md` before working on this repository. `AGENTS.md` is the canonical source for Pista project facts, architecture, public-snapshot boundaries, testing requirements, and GitHub management rules. This file only provides the Claude Code entry point and must not contradict it.

## Working directory

Use a dedicated Pista worktree under `workspace/pista-<branch-name>/`. Keep canonical `master` worktrees for read-only inspection and synchronization. Do not create feature commits directly on `master`.

Pista is the public repository. `Pista-incubator` is a private source/provenance repository; do not copy its history, remotes, tags, reflog, ignored files, or unreviewed content into Pista.

## Repository management

Pista `master` is protected by the `Protect master` Ruleset. Changes normally arrive through a pull request with one approval, stale-review dismissal, strict `core-gate` and `DCO Check` status checks, and the repository CODEOWNERS review path.

Use English commit subjects and include a Signed-off-by trailer. Do not add AI co-author trailers. Never commit, push, create a pull request, change repository settings, or publish an artifact without explicit user authorization.

## Current project boundary

Pista is a JVM/Spark SQL task-submission and execution-enhancement framework. It owns single-file SQL submission, templates, Catalyst functions, processors, output routing, materialization, metrics, and target-specific integrations.

Structured Streaming and target recovery guarantees are Experimental or target-scoped until their required evidence exists. Tributo owns machine-learning training, inference, model lifecycle, and MLflow concerns. Pista has no Python client or PyPI runtime artifact.

English documentation under `docs/` is canonical. The current public snapshot does not include `examples/` or `docs/zh-CN/`.

## Local commands

Use JDK 17 and the project Maven/Scala/Spark versions documented in `AGENTS.md`.

```bash
mvn -B test
mvn -B package -DskipTests
python3 -B scripts/check_assembly.py
python3 -B scripts/check_public_content.py
python3 -B scripts/check_public_license.py
python3 -B scripts/check_markdown.py
python3 -B scripts/public_manifest.py --check
```

Select real ClickHouse, Doris, Iceberg, object-storage, metadata, or streaming infrastructure tests according to the changed domain. Preserve long-test logs and do not replace missing infrastructure with a false green result.

## Safety

Do not commit credentials, internal endpoints, private topology, business data, or local machine configuration. Do not broaden a change beyond the approved file and behavior scope. When the public snapshot, manifest, provenance, CI policy, or branch protection changes, re-check the full accumulated state before reporting completion.
