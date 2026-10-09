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

## Doris overwrite and metadata

`spark.pista.output.mode=overwrite` and `spark.pista.doris.overwrite=true` both select Doris atomic replacement through the Spark connector, including small datasets and `force.jdbc=true` requests. Configure the Doris connector destination with `spark.pista.doris.fenodes`, `spark.pista.doris.database`, and `spark.pista.doris.table`. For overwrite, `spark.pista.output.path` must name that same table; an unqualified table name is resolved within the configured Doris database. Conflicting targets are rejected before metadata registration or Doris preparation. A non-empty `spark.pista.doris.partition.dateValue` selects partition replacement; an empty value selects whole-table replacement. Set these options on the DataFrame's SparkSession before writing, or supply them at submission.

On the connector and atomic-overwrite paths, PostgreSQL metadata is optional only when no `spark.pista.meta.*` settings are supplied. Once requested on these paths, all of the following keys are required:

- `spark.pista.meta.host`
- `spark.pista.meta.database`
- `spark.pista.meta.username`
- `spark.pista.meta.password` (the key must exist; an empty value is allowed)
- `spark.pista.doris.clusterName`

`spark.pista.meta.port` defaults to 5432. On these paths, partial configuration, an unavailable configured store, and failed metadata state transitions are errors. Cluster identity alone does not enable PostgreSQL metadata. The small-data or forced JDBC append path does not use this metadata flow. The Doris metadata relation is `public.doris_task_info`.

Existing stores must widen `rdate` and `partition_filter` to `TEXT` before storing selectors or predicates that exceed their old limits. Configure standard PostgreSQL connection environment variables through the deployment's credential provider, then apply the non-destructive migration from the repository root:

```bash
psql --set ON_ERROR_STOP=1 --file sqls/postgre/migrations/doris_task_metadata_text.sql
```

The migration preserves task rows, keys, indexes, triggers, and partition-record foreign keys. Schedule it during a maintenance window because [ALTER TABLE acquires a table lock](https://www.postgresql.org/docs/16/sql-altertable.html). Do not rerun the table-initialization scripts against an existing store; those scripts recreate tables. Applications do not migrate the store automatically. If a selector exceeds a legacy column limit, the error identifies the column, requested character count, and migration path before Doris preparation begins.

## Error policies

The global policy and SQL, processor, output, and materialization overrides accept `fail-fast` or `continue-on-error`. Continue policies control scheduling only; they do not convert required failures into success.

## Secrets

Passwords, tokens, access keys, complete connection strings, and business SQL literals must come from deployment-managed secrets or environment references. Logs, exceptions, reports, metrics, and manifests must redact them.

## Validation

Configuration changes require tests for defaults, explicit values, invalid input, missing required dependencies, and redaction. Connector configuration also requires real target validation.
