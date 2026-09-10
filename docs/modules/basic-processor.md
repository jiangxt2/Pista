# Built-in Processors

Built-in processors implement `DataProcessor` and are loaded through the processor manager.

## Data masking

`DataMaskingProcessor` masks configured columns. Supported masking modes include phone numbers, email addresses, names, and identity-like strings. It is a presentation transform, not a substitute for access control or encryption.

Validate the input column, masking type, null behavior, and output ownership before enabling it in a public workload.

## Time dimensions

`TimeDimensionProcessor` derives configured calendar and time fields from a source timestamp. Time zone and input type must be explicit because Spark session settings can change results.

## User tags

`UserTagProcessor` derives generic, snake_case labels from configurable spending, income, and activity columns. Its fixed thresholds are examples, not domain recommendations; review them before use and prefer a custom processor when different semantics are required.

## Data quality

Data-quality processing is integrated with the metrics module. Rules should fail or report according to the configured processor policy, and required failures must remain visible in the execution report.

## Extension contract

A custom processor must:

- validate all required configuration before triggering Spark actions;
- preserve or document its output schema;
- avoid creating or stopping a shared SparkSession;
- keep credentials and business literals out of logs;
- propagate failures according to the processor error policy;
- include unit tests for nulls, empty input, invalid configuration, and schema changes.
