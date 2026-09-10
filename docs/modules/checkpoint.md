# DataFrame Materialization

Pista manages DataFrame materialization through `MaterializationManager`. This capability is distinct from Spark Structured Streaming checkpoints and target-specific connector recovery state.

## Lifecycle

```text
initialize
  -> materialize DataFrame
  -> obtain handle
  -> consume stable materialized data
  -> release handle
  -> cleanup remaining resources
```

A handle records identity, path, format, creation time, and the materialized DataFrame. Callers must keep the materialization alive until all dependent writes and validation complete.

## Storage

Supported storage depends on Spark and Hadoop configuration. Local, HDFS, and S3-compatible paths can be used when the required filesystem implementation and credentials are available.

## Rules

- Initialize once for the controlled execution lifecycle.
- Materialize before destructive target changes when source and target may overlap.
- Do not confuse a lazy `persist` call with completed materialization.
- Release only handles owned by the current execution.
- Do not remove a shared base path without exact ownership evidence.
- Keep credentials out of paths, metadata, logs, and exceptions.

## Non-goals

Materialization does not implement cross-file workflow recovery, Structured Streaming offsets, or a universal connector transaction store.
