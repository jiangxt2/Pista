# SQL Authoring Guide

Pista accepts one SQL file per Spark submission. A file may contain multiple SQL statements. Cross-file ordering and retries belong to the external scheduler.

## SQL files

- Save SQL as UTF-8 text.
- Separate statements with semicolons.
- Semicolons inside quoted strings or comments are not statement boundaries.
- Keep target credentials out of SQL files and template parameters.
- Use Spark-supported SQL syntax for the configured Spark version and catalog.

## Parameters

Template parameters use the `spark.pista.params.` prefix. Type declarations add `.type` to the parameter name.

```text
spark.pista.params.partition_date=20260407
spark.pista.params.partition_date.type=date
```

Use Spark parameter markers for values whenever possible. FreeMarker is intended for controlled structural templating, not for unvalidated SQL fragments.

## Multiple statements

Statements execute in source order. Error policies can stop immediately or continue to later statements. Continuing does not turn a failed required output into success; review the execution report and process exit status.

## Output

SELECT results are routed through the configured output. Spark-native formats include console, Parquet, ORC, JSON, CSV, and text. Custom writers are discovered through the writer registry.

Each target has its own capabilities and limitations. Do not infer transaction, recovery, or exactly-once guarantees from the output format name.

## Submission

Use the assembly JAR produced by `pista-assembly`:

```bash
spark-submit \
  --class com.pista.spark.sql.batch.SparkSQLSubmitter \
  --conf spark.pista.sqlFile_=/path/to/query.sql \
  --files /path/to/query.sql \
  pista-assembly/target/pista-<version>.jar
```

The exact SQL-file localization behavior is deployment-sensitive. Validate the configured path in the target cluster manager before claiming cluster-mode support.
