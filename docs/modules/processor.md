# Processor Pipeline

Processors transform a SELECT result before output. They do not run for DDL or write commands that do not produce a query DataFrame.

## Discovery

Processor classes are configured through the submitter configuration and loaded by `ProcessorManager`. Implementations extend `DataProcessor` and receive a `ProcessContext` containing the SparkSession, processor configuration, SQL name, and post-action callbacks.

## Lifecycle

```text
query DataFrame
  -> validate processor configuration
  -> process in configured order
  -> write output
  -> run post-action callbacks only after the output action succeeds
```

Processor failure behavior is controlled independently from SQL and output policies. A continue policy must remain visible in logs and reports; it must not silently claim that a required transform ran.

## Configuration

Processor configuration uses the `spark.pista.processor.` prefix. Public processors should use stable, documented keys. Pista does not provide Spark ML processors or MLflow integration; use Tributo for those capabilities.

## Development rules

- Reuse Spark SQL functionality instead of creating equivalent custom engines.
- Keep processors stateless unless state is part of a documented processor contract.
- Never stop a shared SparkSession.
- Avoid extra `count`, cache, or collect actions unless their cost is explicit.
- Preserve caller-owned columns unless the processor contract says otherwise.
- Add tests for configuration, schema, nulls, empty data, errors, and post-action behavior.

See [Built-in processors](basic-processor.md).
