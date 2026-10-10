# Pista

A lightweight Spark SQL task-submission and execution-enhancement framework with parameterized SQL, a typed Catalyst function catalog, processors, and extensible outputs.

[![Spark](https://img.shields.io/badge/Spark-3.5.8-orange.svg)](https://spark.apache.org/)
[![Scala](https://img.shields.io/badge/Scala-2.12.18-red.svg)](https://www.scala-lang.org/)
[![Java](https://img.shields.io/badge/Java-17-blue.svg)](https://www.oracle.com/java/)
[![Maven](https://img.shields.io/badge/Maven-3.8+-green.svg)](https://maven.apache.org/)

## Features

- **SQL execution** — Multi-statement SQL, FreeMarker templates, typed parameters, and quote-aware statement splitting.
- **Catalyst catalog** — 30 public scalar, aggregate, and table functions for network, date/time, Roaring64, JSON, URL/domain, and vector workloads.
- **Processor pipeline** — Data masking, time dimensions, data quality, and custom processors. ML training, inference, and model lifecycle are provided by Tributo.
- **Extensible output** — Spark-native formats plus ServiceLoader-discovered ClickHouse and Doris integrations.
- **Catalog integration** — Spark catalog configuration for Iceberg, Lance, ClickHouse, and Doris; Iceberg writes use native Spark/Iceberg SQL.
- **Materialization** — Managed DataFrame materialization with explicit lifecycle and cleanup.
- **Observability** — Execution reports and Spark SQL, resource, and data-quality metrics.

Structured Streaming is experimental. Connector guarantees depend on the target, transport, table model, and verified deployment conditions; Pista does not claim cross-target atomicity or universal exactly-once delivery.

## Quick start

The first stable distribution is [Pista 2.5.0](https://github.com/jiangxt2/Pista/releases/tag/v2.5.0).
Download its complete [Spark-submit JAR](https://github.com/jiangxt2/Pista/releases/download/v2.5.0/pista-2.5.0.jar).
Builds from master use `2.6.0-SNAPSHOT` and are development artifacts. Maven Central coordinates are not published.
See the [support scope](docs/SUPPORT_MATRIX.md), [stability policy](docs/STABILITY.md), and
[release process](docs/releases.md) for candidate validation, migration and development-version rules.

### Requirements

- JDK 17
- Scala 2.12.18
- Maven 3.8.8 or later
- Apache Spark 3.5.8

### Build

```bash
mvn clean compile
mvn test
mvn clean package -DskipTests
```

## Project structure

```
pista/
├── pista-common/          # Errors and shared utilities
├── pista-catalyst/        # Catalyst functions and generated manifest
├── pista-sql/             # Spark SQL configuration, SPI, and materialization
├── pista-test-common/     # Shared test infrastructure
├── pista-batch/           # SparkSQLSubmitter
├── pista-streaming/       # Experimental Structured Streaming entry point
├── pista-connector/       # ClickHouse and Doris integrations
├── pista-metrics/         # Spark and data-quality metrics
├── pista-clickhouse-meta/ # Optional ClickHouse metadata
├── pista-doris-meta/      # Optional Doris metadata
├── pista-assembly/        # spark-submit assembly JAR
├── shell/                 # Submission helpers
└── docs/                  # Project documentation
```

## Documentation

| Document | Description |
|------|------|
| [Documentation index](docs/index.md) | Module and reference documentation |
| [SQL templates](docs/reference/sql-template.md) | SQL execution, templates, and parameters |
| [Kyuubi batch submission](docs/kyuubi-submission.md) | Experimental submission of the complete Pista runtime through Kyuubi Batch REST API |
| [SQL functions](docs/modules/udf.md) | Public function catalog and contracts |
| [Processors](docs/modules/processor.md) | Processor usage and extension points |
| [Readers and writers](docs/modules/reader-writer.md) | Data-source SPI and target integrations |
| [Catalogs](docs/modules/catalog.md) | Spark catalog integration |
| [Materialization](docs/modules/checkpoint.md) | DataFrame materialization lifecycle |
| [Metrics](docs/modules/metrics.md) | Metrics collection |
| [Streaming](docs/modules/streaming.md) | Experimental streaming guide |
| [Configuration](docs/reference/configuration.md) | Configuration reference |
| [Errors](docs/reference/error-codes.md) | Error reference |

## License

Pista is licensed under the [Apache License 2.0](LICENSE). See
[NOTICE](NOTICE) and [THIRD_PARTY_NOTICES](THIRD_PARTY_NOTICES) for attribution
and bundled third-party information.
