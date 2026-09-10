# Security Policy

## Supported versions

Pista has not published a stable release. Security fixes currently target the latest commit on the default branch only.

## Reporting a vulnerability

Do not open a public issue for a suspected vulnerability.

Use GitHub private vulnerability reporting from the repository's **Security** tab. Include the affected revision, the impact, reproduction steps, and any suggested mitigation. If private reporting is unavailable, open a minimal issue asking the maintainer to enable a private contact channel without disclosing technical details.

The maintainer will acknowledge a complete report, assess severity, coordinate a fix, and publish an advisory when users need to take action. No response-time guarantee is made before the first stable release.

## Scope

Reports are especially useful for:

- credential or sensitive-data exposure;
- SQL, command, path, or template injection;
- unsafe deserialization or class loading;
- authorization bypass in supported integrations;
- dependency vulnerabilities with a demonstrated Pista impact;
- incorrect recovery behavior that can silently corrupt or expose data.

General hardening suggestions and unsupported deployment questions belong in normal issues after the public repository is available.

## Dependency scan boundary

The required vulnerability gate scans the dependencies embedded in the Pista JVM assembly. Apache Spark, Hadoop, Scala, and their transitive dependencies are supplied by the target Spark cluster and are deliberately excluded from the Pista assembly and its release SBOM. Operators must scan and patch that external runtime independently while preserving the documented Spark compatibility baseline.

Any accepted advisory exception must be narrowly scoped in `osv-scanner.toml`, explain why the affected code path is unreachable, and include an expiry date. An exception does not change the support matrix or turn an affected external runtime into a Pista-supported component.
