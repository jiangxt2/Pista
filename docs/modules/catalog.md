# Catalog Integration

Pista configures Spark catalogs; it does not implement a separate metadata service.

## Supported catalog integrations

The repository contains configuration and examples for Spark-native Iceberg, Lance, ClickHouse, and Doris catalogs. Pista does not provide a custom Iceberg writer; Iceberg writes use Spark SQL/DataFrameWriterV2 and the configured Iceberg Catalog.

## Configuration principles

- Configure catalogs before creating or reusing the SparkSession that needs them.
- Keep the selected Catalog and connector dependencies available on both driver and executors.
- Do not place credentials in tracked files, SQL text, logs, or exception messages.
- Use environment variables or deployment-managed secret references.
- Pin Spark, Scala, connector, and target versions in the support matrix.

## Cross-catalog SQL

Spark can analyze SQL that references multiple configured catalogs. Pista does not add a cross-catalog transaction. Each target operation keeps its native commit and recovery behavior.

## Validation

Catalog changes require tests for namespace resolution, table discovery, schema conversion, pushdown, missing dependencies, and authentication failure. DDL and connector behavior require real target infrastructure.

The explicit REST Catalog submitter test uses `PISTA_ICEBERG_REST_URI`, `PISTA_ICEBERG_CATALOG_NAME`, and `PISTA_ICEBERG_NAMESPACE`; provide storage credentials through the Spark deployment environment before running `SparkSQLSubmitterIcebergRestIT`.
