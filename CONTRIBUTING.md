# Contributing

Thank you for contributing to Pista. This guide describes the supported development environment and validation workflow.

## Development environment

### Requirements

| Tool | Version | Notes |
|------|------|------|
| JDK | 17 | `export JAVA_HOME=~/Library/Java/JavaVirtualMachines/corretto-17.0.18/Contents/Home` |
| Maven | 3.8.8+ | Use a compatible Maven distribution |
| Spark | 3.5.8 | Install Spark or set `SPARK_HOME` |
| Docker | Supported Docker Engine | Required only for real infrastructure tests |

### Clone and build

```bash
git clone <repo-url> pista
cd pista
mvn clean compile -DskipTests
```

## Project structure

```
pista/
├── pista-common/          # Shared errors and utilities
├── pista-catalyst/        # Catalyst functions and metadata
├── pista-sql/             # Configuration, SPI, and materialization
├── pista-batch/           # SparkSQLSubmitter
├── pista-streaming/       # Experimental streaming entry point
├── pista-connector/       # ClickHouse and Doris integrations
├── pista-metrics/         # Metrics collection
├── pista-clickhouse-meta/ # ClickHouse metadata
├── pista-doris-meta/      # Doris metadata
├── pista-assembly/        # Submission assembly JAR
├── shell/                 # Submission helpers
└── docs/                  # Documentation
```

Keep module dependencies one-way: `common -> catalyst -> sql -> {batch, connector, metrics, streaming}`. Do not introduce reverse dependencies.

## Code style

- Use two-space indentation, PascalCase class names, and camelCase method names.
- Prefer immutable `val` declarations over `var`.
- Avoid unnecessary `return`, mutable global state, and broad exception handling.
- Use `org.apache.spark.internal.Logging`; do not use standard output in production code.
- Create business exceptions through `PistaErrors`.
- Use English for code, comments, logs, exceptions, tests, configuration help, and canonical documentation.

## Validation

```bash
# Non-Docker unit and contract tests
mvn test

# Docker integration tests, excluding Submitter IT
mvn test -Pit

# Submitter integration tests require the final assembly JAR
mvn -pl pista-assembly -am package -DskipTests
mvn -pl pista-batch -am test -Psubmitter-it \
  -Dpista.it.submitter.jar="$PWD/pista-assembly/target/pista-2.4-SNAPSHOT.jar"
```

Run each approved long-duration suite once for an unchanged code and environment state. Preserve failure logs under the suite report directory. Do not skip failing tests to produce a green result.

## Commits

- Use an English imperative subject.
- Include `Signed-off-by: <name> <email>` in every commit.
- Keep one logical change per commit and include tests for behavior changes.
- Do not commit credentials, generated build directories, local environments, or private configuration.

## Pull requests

1. Create a descriptive feature branch from `master`.
2. Use a dedicated Git worktree for the change.
3. Add or update tests and run the affected validation matrix.
4. Review the complete diff, public-content checks, documentation, and artifact impact.
5. Open a pull request and address review feedback against the cumulative diff.
6. Use squash merge unless repository policy requires another strategy.
