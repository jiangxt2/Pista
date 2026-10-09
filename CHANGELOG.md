# Changelog

This project follows a human-readable changelog. No stable public release has been published.

## Unreleased

### Added

- English public documentation and project governance files.
- Public-content, language, licensing, manifest, and assembly validation tools.
- Pull-request, nightly, and release-candidate CI workflows.

### Changed

- Examples, configuration templates, source comments, messages, and tests use public-safe English terminology.
- CI uses current Node-runtime action releases, guarded hosted-runner disk preparation, engine-isolated domain IT shards, and resource-bounded ClickHouse and Doris test settings.
- Doris overwrite uses the atomic connector path for generic and Doris-specific overwrite requests. Partially configured metadata stores and missing state updates fail explicitly, with redacted stage and SQLSTATE diagnostics.
- Doris metadata stores complete partition selectors and predicates as TEXT. Existing stores require `sqls/postgre/migrations/doris_task_metadata_text.sql`; the migration preserves task state and coordination objects. Task identities that already fit the 256-character key retain their representation, while longer identities use a deterministic digest.

### Removed

- Removed the experimental PyPista Python client, its JVM bridge, packaging metadata, and Python-specific release gates. JVM Spark SQL remains the Pista entry path.

### Security

- Removed organization-specific identifiers, internal environment defaults, business-shaped fixtures, and credential-like example values from the public candidate tree.
- Updated bundled JDBC drivers and changed vulnerability gating to scan release-scoped SBOMs while documenting externally provided Spark dependencies.

Release entries will be added only when a release is actually published.
