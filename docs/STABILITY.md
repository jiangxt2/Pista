# Stability Policy

Pista has not published a stable release. Unless a document explicitly says otherwise, current APIs and configuration are pre-release and may change with release notes and migration guidance.

## Stability levels

| Level | Meaning |
|---|---|
| Current | Present in the source tree and covered by the stated validation. It is not automatically a public compatibility commitment. |
| Experimental | Available for evaluation, with incomplete compatibility or recovery guarantees. Breaking changes may occur without a deprecation period. |
| Planned | Design intent only; not an implemented capability. |
| Blocked | Not releasable until the documented blocker is resolved. |

## Compatibility policy

Before the first stable release, compatibility changes must be called out in the changelog. Configuration renames should provide aliases, conflict detection, and migration tests when users could reasonably depend on the old key.

After a stable compatibility policy is adopted, public API removals will require advance deprecation notice. Persistent operation-state formats and delivery identities require an explicit migration path regardless of release maturity.

## Guarantees

Delivery claims must identify the target, scope, and preconditions. A successful process exit alone is not evidence of exactly-once delivery or recoverability. Experimental streaming and connector paths must not be described as generally supported without their real-infrastructure and failure-recovery evidence.
