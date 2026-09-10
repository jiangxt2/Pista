# Structured Streaming

The `pista-streaming` module is experimental. Spark owns query planning, offsets, state stores, micro-batch identity, checkpointing, and restart behavior.

## Flow

```text
readStream
  -> optional parser
  -> optional SQL transform
  -> DataStreamWriter or foreachBatch
  -> target output
  -> StreamingQueryListener metrics
```

Source and sink options use Spark configuration. Pista can add parsing, processor, output-routing, and metrics behavior but must not advance a successful callback after a required sink failure.

## Delivery semantics

Spark `foreachBatch` is at-least-once by default. A stronger target guarantee requires a stable `batchId`, target-side idempotency or a queryable receipt, and real restart tests. Multiple sinks are not an atomic transaction.

## Checkpoints

Use a durable checkpoint path unique to the query identity. Do not reuse a checkpoint after changing incompatible source, schema, query, or sink semantics. Pista does not create a second streaming checkpoint system.

## Validation

Streaming changes require real Kafka and checkpoint tests covering parser failure, sink failure, restart, replay, duplicate batches, and listener behavior. Batch tests do not prove streaming commit semantics.
