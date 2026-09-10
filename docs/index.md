# Pista Documentation

Pista is a Spark SQL task-submission and execution-enhancement framework. Spark remains the execution engine; Pista adds templates, typed parameters, Catalyst functions, processors, output routing, metrics, and optional target integrations.

## Guides

- [Architecture](architecture.md)
- [SQL authoring](sql-authoring-guide.md)
- [SQL templates and parameters](reference/sql-template.md)
- [Kyuubi batch submission (experimental)](kyuubi-submission.md)
- [Configuration](reference/configuration.md)
- [Errors](reference/error-codes.md)

## Modules

- [Catalyst functions](modules/udf.md)
- [Processors](modules/processor.md)
- [Built-in processors](modules/basic-processor.md)
- [Readers and writers](modules/reader-writer.md)
- [Catalog integration](modules/catalog.md)
- [DataFrame materialization](modules/checkpoint.md)
- [Metrics](modules/metrics.md)
- [Structured Streaming](modules/streaming.md)

## Stability

The batch submitter and Catalyst catalog are the primary implementation paths. Structured Streaming and target-specific recovery behavior remain experimental unless a support matrix explicitly states otherwise. Source code and tests are authoritative when documentation and implementation differ.
