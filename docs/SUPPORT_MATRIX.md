# Support Matrix

This matrix describes the source candidate, not a published release. A component is supported only within the scope explicitly listed here and after the corresponding release gate passes.

## Runtime baseline

| Component | Candidate baseline | Status |
|---|---:|---|
| JDK | 17 | Current build baseline |
| Scala | 2.12.18 | Current build baseline |
| Apache Spark | 3.5.8 | Current compute baseline |
| Maven | 3.8.8 or newer compatible version | Current build baseline |

## Capability status

| Area | Status | Validation boundary |
|---|---|---|
| Parameterized single-file batch SQL | Current | JVM unit and submitter integration tests |
| Catalyst function catalog | Current | Catalog, registration, interpreted, and code-generation tests |
| ClickHouse batch adapter | Current candidate | Unit tests plus Docker-backed write, overwrite, routing, and resume tests |
| Doris batch adapter | Current candidate | Unit tests plus Docker-backed write, partition, transaction, and overwrite tests |
| Native Iceberg Catalog integration | Current candidate | Spark SQL/DataFrameWriterV2 tests plus locked REST Catalog, S3/HDFS, and source-to-table validation |
| Metrics and data-quality collection | Current candidate | JVM unit and Spark-local tests |
| Structured Streaming | Experimental | Spark-local tests; public recovery matrix is incomplete |
| Spark ML processors / MLflow integration | Removed from Pista | Use Tributo for model training, inference, experiment tracking, artifacts, and registry |

## Not promised

The candidate does not promise a multi-tenant SQL gateway, a workflow scheduler, a Kubernetes control plane, universal exactly-once delivery, or compatibility outside the baselines above. Public Maven coordinates and a stable compatibility window remain blocked until release approval; Pista does not publish a PyPI client artifact.
