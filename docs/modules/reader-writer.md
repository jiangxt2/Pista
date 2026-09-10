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

## Extension rules

- Fail before side effects when configuration or capability requirements are missing.
- Do not log credentials or complete connection strings.
- Preserve task retry and speculative-execution semantics.
- Document idempotency, verification, and recovery preconditions.
- Test real DDL, transactions, publication, and recovery against real infrastructure.
