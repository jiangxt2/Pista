# SQL Templates and Parameters

Pista reads a SQL file, applies FreeMarker template parameters, splits statements, and executes them through SparkSession.

## Template parameters

Parameters use the `spark.pista.params.` prefix:

```text
spark.pista.params.partition_date=20260407
```

FreeMarker can reference the configured name in the SQL template. Treat structural fragments as trusted configuration; do not accept arbitrary user SQL fragments without validation.

## Typed parameters

Add a type declaration for Spark parameter markers:

```text
spark.pista.params.minimum_amount=10.50
spark.pista.params.minimum_amount.type=decimal(10,2)
```

Typed values are parsed by `ParamTypeParser`. Unsupported types and invalid values fail before query execution.

## Statement splitting

The splitter recognizes semicolons while respecting quoted strings, quoted identifiers, line comments, block comments, and escapes. Tests cover empty input, nested-looking text, and unterminated constructs.

## File distribution

Spark `--files` localizes files for the driver and executors. Pista preserves a readable driver-local path. If that path is unavailable and `spark.pista.sqlFile_` contains a plain filename, Pista resolves it with `SparkFiles.get`. Values containing path separators are not treated as distributed filenames. Use a unique filename and validate the behavior with the selected cluster manager.
