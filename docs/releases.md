# Release process

Pista 2.5.0 is the first stable GitHub distribution. This process applies to future
stable releases; commands below use 2.5.0 as an example. The
submission JAR is built for JDK 17, Scala 2.12.18 and Spark 3.5.8. The
[support matrix](SUPPORT_MATRIX.md) and [stability policy](STABILITY.md) define
the promise; Experimental features retain their status when a stable release is
published. GitHub Release notes are the versioned change history: summarize
user-visible changes and upgrade notes, and link the full tag comparison.
Maven Central and PyPI publication are outside this process.

## Authorization and preparation

Develop release changes in a dedicated worktree. Obtain explicit authorization
before commit, push, PR creation, repository settings, candidate workflow execution,
tag creation, Release creation and publication. Do not infer publication approval
from a successful build or a request to prepare the release.

Merge the reviewed preparation PR, then select a clean full commit SHA whose
reactor revision matches the approved stable version. Keep that commit and its
candidate artifacts fixed;
master may continue development. Preserve existing introduced-version metadata.

The candidate workflow takes the approved `version` and `expected_commit` as
inputs. It rejects snapshots, a different checkout, a different reactor version,
dirty source and JAR metadata that does not match the selected source. Ordinary
local builds use a `development` source stamp and are not release candidates.

## Candidate validation

Authorize one final run using a branch or tag that points to the selected commit:

```bash
gh workflow run release-candidate.yml --ref APPROVED_SOURCE_REF \
  -f version=2.5.0 -f expected_commit=APPROVED_FULL_COMMIT_SHA
```

The workflow runs public-content, language, Markdown, license, policy and JVM
checks, builds the complete Assembly, generates the runtime SBOM, and executes
the isolated domain and Submitter IT suites. Reports are archived even on failure.
The existing vulnerability workflow then verifies the packaged candidate and
scans its exact SBOM. Require both build and vulnerability jobs to succeed.

Required tests must execute; skipped or unavailable infrastructure checks are
not passing evidence. Reuse dependency caches and keep an execution ledger.
Do not run a complete Nightly and repeat the same candidate suites on unchanged
source merely for additional confidence. Diagnose failed or interrupted runs
before requesting a necessary rerun.

## Candidate files and download verification

The bundle contains exactly:

- `pista-2.5.0.jar`: the complete Spark-submit runtime, with version and source-commit metadata.
- `pista-2.5.0-src.tar.gz`: tracked source from the same commit, including licenses, documentation, configuration examples and metadata migration SQL.
- `pista-jvm.cdx.json`: the runtime CycloneDX SBOM.
- `release-manifest.json`: version, tag, source commit, runtime baseline and asset hashes.
- `SHA256SUMS`: hashes with basename-only paths that work after download.

Download `pista-release-candidate-APPROVED_FULL_COMMIT_SHA` from the successful
approved run. Verify it without rebuilding:

```bash
python3 -B scripts/prepare_release.py verify --version 2.5.0 \
  --commit APPROVED_FULL_COMMIT_SHA --directory target/downloaded-candidate
```

This verifies hashes and candidate identity. For optional checksum-only checking,
run `sha256sum --check SHA256SUMS` inside the downloaded directory.

Reuse successful RC Submitter IT evidence only for behavior its assertions cover,
when it exercised the same Assembly JAR and the downloaded hashes match. Artifact
identity does not establish behavioral coverage. Add a focused check to the
relevant candidate suite for each required release behavior without direct test
evidence. A separate consumer smoke is needed only when the RC suites leave a
supported distribution or submission path unexercised. Use the exact candidate
JAR and archive the command, result and digest.

Review license/NOTICE content, dependency attribution, source ownership and
provenance, secret/PII/private-endpoint checks, vulnerability findings and the
declared compatibility/migration scope before requesting publication approval.

## Publish the verified candidate

Present the complete English release notes, exact version tag, source commit and
five attachments for approval. Obtain repository-setting approval to enable
[immutable Releases](https://docs.github.com/en/code-security/concepts/supply-chain-security/immutable-releases),
then read back that immutability is enabled. Create `v2.5.0` at the approved source
commit; an existing tag must resolve to that same commit. Never move an existing
published version tag.

Create a draft Release, attach all five verified files, download them again and
compare their hashes. Publish the draft only after explicit publication approval.
Promote the tested bytes; do not rebuild or replace them during publication.
Mark it as a stable release only within its approved support scope.

Read back the published tag, source commit, stable/immutable status, notes and
complete asset list. Verify the release attestation and each attached file with
GitHub's documented release-integrity tools. GitHub's automatically generated
source downloads are not a substitute for the explicitly attached and hashed
source archive. Retain the publication receipt and candidate validation logs.

If a released version has a defect, publish a new patch version after validation;
leave the old tag and assets unchanged. For Doris, metadata failure after
publication requires target reconciliation before a retry, as documented in the
writer guide. Release publication does not apply production schema migrations.

## Prepare master for the next version

After publication verification, create a separate worktree and PR to:

- Set the root `revision` to `2.6.0-SNAPSHOT`; child modules and the Assembly follow the reactor value.
- Keep versioned change history in the GitHub Release notes, including upgrade notes and a full comparison with the previous release tag.
- Keep released download examples at 2.5.0 while development build paths follow the reactor revision.
- Preserve existing `.version("2.5")` annotations, which record introduction rather than the current build version.
- Retain the runtime baseline unless a separately reviewed upgrade provides its required compatibility evidence.

Use a 2.5.x maintenance branch only when a patch is needed, starting from the
appropriate release tag and forwarding the fix to master. Candidate checks keep
rejecting SNAPSHOT versions after master advances.

The release is ready for administrative cleanup only after published-artifact
verification, the next-development-version PR and documentation updates are
complete. Keep the independent plan while those follow-ups remain open.
