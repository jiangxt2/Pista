# Metrics

`pista-metrics` collects Spark execution, SQL, resource, shuffle, skew, error, query-plan, and data-quality metrics.

## Collection

The module uses SparkListener, QueryExecutionListener, Catalyst rules, and processor callbacks. Metrics should preserve Spark application, job, stage, task, SQL, and Pista execution identity where available.

## Storage

The current lightweight output is JSON Lines through an asynchronous queue. Queue overflow follows the configured drop policy and must increment an observable dropped-record count.

## Data quality

Data-quality metrics can be attached to a query through the processor and observation path. A quality failure must follow the configured processor policy and remain visible in the execution report.

## Privacy

SQL text, literals, connection options, catalog/table identifiers, column names, and lineage identifiers may contain sensitive data. Export only allowlisted fields or redacted/hash representations. Exporters must not receive an unredacted payload.

## Stability

Prometheus, OpenTelemetry, and OpenLineage are integration directions, not automatically available public guarantees. The current implementation and tests are authoritative.
