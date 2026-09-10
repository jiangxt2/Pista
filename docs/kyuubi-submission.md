# Kyuubi Batch Submission

Kyuubi integration is an external submission path for the complete Pista Spark application. Kyuubi starts the same Pista assembly and main class used by direct `spark-submit`; it does not replace the Pista SQL execution, processor, or writer pipeline.

Status: Experimental. The verified baseline is Kyuubi 1.12.0 submitting Spark 3.5.8 applications to YARN, with Doris 3.0.6.2 for the Doris writer scenario. Other version combinations require their own validation.

## Scope

The initial integration covers batch SQL only. It does not add a Pista server, submission SDK, persistent state store, Python submission path, or streaming submission path.

Use the Kyuubi Batch REST API rather than a Kyuubi JDBC or ODBC session. A JDBC or ODBC session executes SQL in a Kyuubi SQL engine and does not invoke the complete Pista application entry point.

## Prerequisites

- A running Kyuubi 1.12.0 server with a Spark 3.5.8 distribution for the verified baseline.
- An existing Spark cluster supported by the selected Kyuubi deployment.
- A Pista assembly JAR accessible to the Kyuubi submission process and Spark cluster.
- A SQL file accessible through Spark's distributed-file mechanism.
- Network access from the Spark driver and executors to every configured data source and output target.

Use only a reviewed Pista assembly. Do not submit `pista-batch` by itself or construct a runtime from separate module JARs.

## Batch Request

The following request shows the required shape. Replace every placeholder with an environment-specific value. The example intentionally omits passwords and other secrets.

```json
{
  "batchType": "SPARK",
  "resource": "hdfs:///apps/pista/pista-<version>.jar",
  "className": "com.pista.spark.sql.batch.SparkSQLSubmitter",
  "name": "pista-batch-query",
  "conf": {
    "spark.master": "yarn",
    "spark.submit.deployMode": "cluster",
    "spark.files": "hdfs:///jobs/pista/pista-query-<unique-id>.sql",
    "spark.pista.sqlFile_": "pista-query-<unique-id>.sql",
    "spark.pista.output.format": "console"
  },
  "args": []
}
```

Submit the request to `POST /api/v1/batches`. Preserve the returned `id`; it is the Kyuubi batch ID used for later operations.

The value of `spark.pista.sqlFile_` must be the unique distributed filename, not the path on the submitting host. Pista first preserves an existing readable driver-local path and otherwise resolves this filename through `SparkFiles.get`.

## Lifecycle Operations

Use the Kyuubi batch ID with the upstream Batch REST endpoints:

- `GET /api/v1/batches/{batchId}` returns the batch state, Spark application ID, application state, tracking URL, and diagnostics.
- `GET /api/v1/batches/{batchId}/localLog` returns the Kyuubi-side submission log.
- `DELETE /api/v1/batches/{batchId}` requests cancellation.

A successful cancellation request is not proof that an output system rolled back data already written by Spark tasks.

## Doris Output

Kyuubi passes the existing Pista configuration to the Spark application. Pista continues to route a SELECT result through `OutputRouter`, `WriterRegistry`, and `DorisWriter`.

A representative Doris batch adds the following non-secret settings to the request's `conf` map:

```text
spark.pista.output.format=doris
spark.pista.output.path=<database>.<table>
spark.pista.output.mode=append
spark.pista.output.options.doris.fenodes=<fe-host>:<http-port>
spark.pista.output.options.doris.user=<user>
spark.pista.doris.fenodes=<fe-host>:<http-port>
spark.pista.doris.database=<database>
spark.pista.doris.table=<table>
spark.pista.doris.user=<user>
```

The duplicate option domains are part of the current compatibility surface. Kyuubi integration does not change them. Pista chooses the underlying Doris transport according to the existing writer configuration and row-count threshold; Kyuubi integration validation proves the `DorisWriter` path, not a specific transport.

Do not put a non-empty Doris password directly in the Batch request. Use the credential mechanism approved for the target YARN or Kubernetes deployment. A production deployment using non-empty credentials remains blocked until that path has been verified not to expose values through Kyuubi metadata, process arguments, Spark configuration, logs, or exceptions.

The verified Kyuubi scope covers successful Doris batch writes and explicit failure propagation. It does not add or strengthen Pista guarantees for Doris 2PC, overwrite recovery, unknown commits, metadata availability, or exactly-once delivery.

## Server-Owned Configuration

Cluster administrators should own settings such as the Spark master, deploy mode, queue, runtime image, and approved assembly location. Kyuubi can predefine Spark batch configuration and ignore client overrides. Pista callers should not assume that a silently ignored configuration was applied.

## Validation Contract

Each deployment must preserve the following verified contract:

- The final Pista assembly contains `SparkSQLSubmitter`, the Doris writer service entry, and the Doris Spark Connector.
- A real Kyuubi server submits a Spark 3.5.8 application using the assembly.
- A SQL file distributed with `spark.files` is resolved by filename in the driver.
- A Pista Catalyst function executes, proving that Pista initialization ran.
- Kyuubi exposes the batch ID, Spark application ID, state, log, tracking URL, and cancellation behavior.
- A real Doris write succeeds through the Pista writer path and the expected rows are present.
- An explicit SQL or Doris write failure produces a non-successful application result.
- Logs and Kyuubi metadata do not expose test credentials.

Direct `spark-submit` remains the compatibility and rollback path. An unknown Kyuubi submission result must not automatically fall back to direct submission because that can create a duplicate Spark application and duplicate target writes.

## References

- [Kyuubi REST API v1](https://kyuubi.readthedocs.io/en/v1.12.0/client/rest/rest_api.html)
- [Kyuubi configuration](https://kyuubi.readthedocs.io/en/v1.12.0/configuration/settings.html)
- [Spark file distribution](https://spark.apache.org/docs/3.5.8/api/java/org/apache/spark/api/java/JavaSparkContext.html)
- [Apache Doris Spark Connector](https://doris.apache.org/docs/3.x/ecosystem/spark-doris-connector/)
