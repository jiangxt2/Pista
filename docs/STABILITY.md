# Stability Policy

Published stable [GitHub Releases](https://github.com/jiangxt2/Pista/releases) define
the versions covered by this policy. A source revision or candidate build is not
itself a published release. The first stable release is
[2.5.0](https://github.com/jiangxt2/Pista/releases/tag/v2.5.0).
Master uses 2.6.0-SNAPSHOT; development snapshots carry no stable commitment.

## Stability levels

| Level | Meaning |
|---|---|
| Current | Present in the source tree and covered by the stated validation. It is not automatically a public compatibility commitment. |
| Experimental | Available for evaluation, with incomplete compatibility or recovery guarantees. Breaking changes may occur without a deprecation period. |
| Planned | Design intent only; not an implemented capability. |
| Blocked | Not releasable until the documented blocker is resolved. |

## Compatibility policy

For the documented stable scope of a published version:

- Patch versions preserve public CLI, SQL function, configuration and error contracts while fixing defects.
- Minor versions add compatible behavior. Existing defaults and supported entry points remain compatible unless an explicitly documented experimental feature changes.
- Incompatible removal or replacement requires a major version and advance deprecation in a supported minor release, with migration guidance and direct compatibility tests.

Configuration renames provide aliases and conflict detection during deprecation.
Target versions and deployment shapes are supported only within the support
matrix; release numbers do not imply compatibility with additional Spark or
Scala baselines. Experimental capabilities remain outside the stable promise.

Persistent operation-state formats and delivery identities require an explicit
migration path regardless of release maturity. Preserve historical introduced-version
annotations when advancing the project revision. Follow the [release process](releases.md)
for publication verification and master preparation.

## Guarantees

Delivery claims must identify the target, scope, and preconditions. A successful process exit alone is not evidence of exactly-once delivery or recoverability. Experimental streaming and connector paths must not be described as generally supported without their real-infrastructure and failure-recovery evidence.
