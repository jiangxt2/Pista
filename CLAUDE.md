# Pista Claude Code Guide

Read `AGENTS.md` before working on this repository. `AGENTS.md` is the canonical source for Pista's project facts, architecture, support states, validation requirements, and GitHub workflow. This file is only an entry point and must not contradict it.

## Working directory

Use a dedicated Pista worktree under `workspace/pista-<branch-name>/`. Keep the canonical `master` worktree clean for read-only inspection and synchronization. Do not develop, stage, or commit feature changes directly on `master`.

Before editing files or running project Git operations, verify the repository with `git rev-parse --show-toplevel`. Keep each change within the approved worktree and file scope.

## Repository management

Pista `master` is protected by repository policy. Changes normally arrive through a pull request and the required CI checks. Use English commit subjects and a valid `Signed-off-by` trailer. Do not add AI co-author trailers unless required by repository policy.

Never commit, push, create a pull request, create an issue, post a comment, change repository settings, or publish an artifact without explicit user authorization. Never use `--no-verify` to bypass checks.

## Project boundary

Pista is a JVM and Spark SQL runtime for single-file SQL submission, templates, Catalyst functions, Processors, Readers, Writers, output routing, materialization, metrics, and target-specific delivery integrations.

Structured Streaming and target recovery guarantees remain Experimental or target-scoped until their required contract, real-infrastructure, and recovery evidence exists. Pista does not provide a general workflow scheduler, multi-tenant SQL gateway, universal exactly-once delivery, or a Python/PyPI runtime. Machine-learning lifecycle concerns belong to Tributo.

## Local validation

Use JDK 17, Scala 2.12.18, Apache Spark 3.5.8, and the Maven/Scala versions documented in `AGENTS.md`.

```bash
mvn -B test
mvn -B package -DskipTests
python3 -B scripts/check_assembly.py
python3 -B scripts/check_public_content.py
python3 -B scripts/check_public_license.py
python3 -B scripts/check_markdown.py
python3 -B scripts/public_manifest.py --check
```

Choose focused unit, contract, assembly, or real-infrastructure tests according to the changed domain. Preserve long-test logs and do not report skipped or unavailable required tests as passed.

## Safety and completion

Do not commit credentials, private endpoints, business data, local machine configuration, or unintended generated files. Do not weaken failure handling, silently change a target guarantee, or broaden a change beyond its approved scope.

Before reporting completion, confirm the worktree, review the full accumulated diff, verify the relevant tests and public-content checks, record known omissions, and distinguish implementation status from release or remote-merge status.
