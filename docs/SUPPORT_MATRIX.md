# Support Matrix

This matrix defines the scope of the 2.5.0 GitHub candidate. A stable commitment
applies only after that version is published on [GitHub Releases](https://github.com/jiangxt2/Pista/releases)
and its release gates pass. Source presence and a successful build alone do not
extend the supported deployment scope.

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
| Native Iceberg SQL integration | Current candidate | Local Hadoop Catalog CTAS, INSERT, replacement, properties and readable snapshot tests |
| Iceberg REST Catalog and additional S3/HDFS deployment combinations | Experimental | Require explicit external-service evidence; the REST suite is excluded from default discovery |
| Metrics and data-quality collection | Current candidate | JVM unit and Spark-local tests |
| Structured Streaming | Experimental | Spark-local tests; public recovery matrix is incomplete |
| Kyuubi batch submission | Experimental | The documented external submission baseline does not establish a general recovery or compatibility promise |
| Spark ML processors / MLflow integration | Removed from Pista | Use Tributo for model training, inference, experiment tracking, artifacts, and registry |

## Verified adapter baselines

- ClickHouse: 25.3.2.39, limited to the table models, write/overwrite paths and continuation scenarios exercised by the connector IT.
- Doris: 3.0.6.2 with the Spark connector, including the tested append, whole-table/partition replacement and transaction visibility paths. PostgreSQL metadata uses 16.14. Existing stores apply the [documented TEXT migration](reference/configuration.md#doris-overwrite-and-metadata) before writing long selectors.

These adapter baselines do not establish untested transports, storage systems,
table models, failover scenarios or universal recovery guarantees.

## Not promised

The release does not promise a multi-tenant SQL gateway, a workflow scheduler,
a Kubernetes control plane, universal exactly-once delivery, or compatibility
outside the baselines above. Distribution is the GitHub submission JAR and source
archive. Public Maven Central coordinates remain outside this release scope;
Pista does not publish a PyPI client artifact.
