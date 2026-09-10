# Governance

## Project model

Pista is a maintainer-led independent open-source project. Maintainers are responsible for technical direction, release readiness, security response, repository administration, and enforcement of project policies.

The current maintainers are listed in `MAINTAINERS.md`.

## Decisions

Routine changes are decided through pull-request review. Decisions should favor correctness, recoverable delivery semantics, compatibility with the documented support matrix, and a maintainable public API.

Changes that affect public APIs, persistent state, recovery guarantees, supported engines, release coordinates, licensing, or governance require an explicit design record and maintainer approval. Architecture changes are not implied by ordinary cleanup or maintenance work.

When consensus is not reached, the lead maintainer makes the final decision and records the rationale in the relevant issue, pull request, or design document.

## Contributions

Contributors retain credit for accepted work. A contribution must pass the applicable review and CI gates, include tests for behavior changes, and comply with the repository's security, content, and licensing rules.

Repeated, substantial, and constructive participation may lead to maintainer nomination. Existing maintainers decide additions and removals. Inactive maintainers may move to emeritus status while retaining attribution.

## Releases

Only maintainers may create releases. A release requires passing the published release-candidate workflow, completing the support and licensing records, and documenting known limitations. Code present in the repository is not automatically a supported or released capability.

## Amendments

Governance changes use the same public review process as other policy changes and require lead-maintainer approval.
