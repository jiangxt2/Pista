# Readers and Writers

Pista provides ServiceLoader-based reader and writer contracts in `pista-sql`. Target implementations live in `pista-connector`.

## Reader contract

A reader receives a SparkSession and `InputConfig`. Depending on the target, it may support explicit schema, column projection, predicates, partition filters, and statistics. Unsupported pushdown must remain visible; it must not change results silently.

The current JVM batch CLI does not expose every reader path as a first-class submission mode. Treat source presence and public entry-point support as separate facts.

## Writer contract

A writer receives a DataFrame and `OutputConfig` with path, format, save mode, partition columns, and options. Spark-native formats use DataFrameWriter. Custom formats use the writer registry.

The compatibility writer contract returns `Unit`; callers must not infer receipt, publication, or recovery guarantees from a successful method return. Target-specific verification belongs to the target integration.

## Target notes

- ClickHouse includes JDBC and multi-shard orchestration paths. Cluster-wide atomicity is not implied.
- Doris includes JDBC and Spark connector paths. Transaction and label behavior depends on the selected transport and target version.
- Iceberg uses Spark/Iceberg commit semantics and snapshot metadata where available.

## Partition configuration

Partition filtering uses a configured column and value. The column, selector, and verification predicate must describe the same target scope.

```text
spark.pista.doris.partition.columnName=partition_date
spark.pista.doris.partition.dateValue=20260407
```

Expression partitions and shared target scopes require explicit validation; do not generalize a direct date-column example to arbitrary partition expressions.

## Doris metadata and diagnostics

Doris task keys remain bounded to 256 Unicode characters. Existing identities that fit the schema, including legacy filter hashes, keep their representation. Otherwise the writer stores `pista_sha256_` followed by the SHA-256 digest of the complete legacy identity. This covers both whole-table and partition writes while preserving legacy keys and avoiding truncation. Partition dates and predicates are stored completely in `TEXT` columns; see the [configuration and migration instructions](../reference/configuration.md#doris-overwrite-and-metadata).

The connector and atomic-overwrite paths register metadata before temporary-table or temporary-partition preparation. JDBC append does not participate in this metadata flow. Re-registering a task preserves its key and refreshes the selector, predicate, source, and write mode while clearing stale completion state. When metadata is configured on these paths, registration and query failures stop the write, and a success update must affect exactly one registered task. A connector failure cleans up temporary objects and records FAILURE; a secondary metadata failure retains the original connector error. A metadata failure after publication still reports failure: reconcile the target before deciding whether to retry.

INFO logs identify the task and target by stable fingerprints and summarize write mode, metadata availability, partition count, field lengths, and the final outcome. DEBUG logs show preparation, writing, publication, verification, and metadata-stage durations. ERROR logs include the failing stage, exception class, SQLSTATE when available, and actionable schema diagnostics. `taskRef` is the first 12 hexadecimal characters of SHA-256 over the stored task ID; it correlates writer and metadata events. Connection strings, credentials, predicates, partition values, and raw database exception messages are excluded from these diagnostics. Successful low-level metadata operations use DEBUG to avoid repeating the final write summary.

## Extension rules

- Fail before side effects when configuration or capability requirements are missing.
- Do not log credentials or complete connection strings.
- Preserve task retry and speculative-execution semantics.
- Document idempotency, verification, and recovery preconditions.
- Test real DDL, transactions, publication, and recovery against real infrastructure.
