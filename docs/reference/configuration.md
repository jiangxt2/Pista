# Configuration Reference

Pista configuration is defined by typed entries in the JVM source. Source definitions and generated inventories are authoritative; this document explains namespaces and safety rules.

## Namespaces

| Namespace | Purpose |
|---|---|
| `spark.pista.*` | Batch execution, SQL files, parameters, processors, output, errors, and metrics |
| `spark.pista.params.*` | Template and typed SQL parameters |
| `spark.pista.output.*` | Output format, path, mode, partition columns, and options |
| `spark.pista.clickhouse.*` | ClickHouse reader/writer and cluster options |
| `spark.pista.doris.*` | Doris reader/writer, connector, transaction, and partition options |
| `spark.pista.meta.*` | Optional connector metadata database |
| `spark.sql.catalog.*` | Spark catalog configuration |
| `spark.sql.catalog.*` | Spark Catalog configuration, including native Iceberg Catalogs |

## SQL input

`spark.pista.sqlFile_` selects the SQL file. Template parameters use `spark.pista.params.<name>`. A corresponding `.type` key enables typed Spark parameters.

## Output

```text
spark.pista.output.format=parquet
spark.pista.output.path=/data/output
spark.pista.output.mode=overwrite
spark.pista.output.partitionBy=partition_date
```

Custom target settings use `spark.pista.output.options.<key>`. Never store real credentials in tracked configuration.

## Error policies

The global policy and SQL, processor, output, and materialization overrides accept `fail-fast` or `continue-on-error`. Continue policies control scheduling only; they do not convert required failures into success.

## Secrets

Passwords, tokens, access keys, complete connection strings, and business SQL literals must come from deployment-managed secrets or environment references. Logs, exceptions, reports, metrics, and manifests must redact them.

## Validation

Configuration changes require tests for defaults, explicit values, invalid input, missing required dependencies, and redaction. Connector configuration also requires real target validation.
