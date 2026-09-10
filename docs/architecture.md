# Architecture

Pista wraps Spark SQL with a repeatable submission and execution layer. It does not replace Spark SQL parsing, Catalyst optimization, DAGScheduler, TaskScheduler, Structured Streaming checkpoints, or external ML runtimes.

## Batch execution

```text
SparkSQLSubmitter
  -> SQLExecutionContext
       -> configuration validation
       -> typed parameters and FreeMarker parameters
       -> Pista function installation
  -> SQLStatementExecutor
       -> SparkSession.sql
       -> optional Processor chain
       -> OutputRouter
       -> execution report and metrics
```

Pista executes one SQL file per submission. External schedulers own dependencies, retries, timeouts, and recovery across files.

## Module boundaries

```text
pista-common -> pista-catalyst -> pista-sql
                                  |-> pista-batch
                                  |-> pista-streaming
                                  |-> pista-connector
                                  |-> pista-metrics

pista-clickhouse-meta and pista-doris-meta are optional target-specific state modules.
pista-assembly packages the spark-submit application.
```

Dependencies should remain one-way. Target adapters must not depend on one another, and target-specific state must not become a universal transaction claim.

## State boundaries

- Spark owns application, job, stage, task, and Structured Streaming checkpoint state.
- Pista owns single-file execution reports and managed DataFrame materialization.
- ClickHouse and Doris metadata are optional and target-specific.
- A successful Spark action is not automatically proof that a remote target is visible or recoverable.

## Guarantees

Pista does not promise cross-target atomicity or universal exactly-once delivery. A connector guarantee is valid only for the documented target version, transport, table model, scope, and preconditions proven by real infrastructure tests.

## Public entry points

- `com.pista.spark.sql.batch.SparkSQLSubmitter`
- `com.pista.spark.sql.functions.PistaSparkSessionExtensions`
- Reader, writer, and processor ServiceLoader contracts
- The public Catalyst function catalog

Structured Streaming and target-specific recovery behavior remain experimental until their contracts and integration tests are complete.
