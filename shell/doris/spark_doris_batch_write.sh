#!/bin/bash

source "${PISTA_HOME}/shell/load-pista-env.sh"
source "${PISTA_HOME}/shell/spark-utils.sh"

# --- Default Values (Preferred from pista-env.sh Configuration) ---
SQL_FILE="${SQL_FILE:-}"
DORIS_FENODES="${DORIS_FENODES:-${PISTA_DORIS_FENODES:-}}"
DORIS_DATABASE="${DORIS_DATABASE:-${PISTA_DORIS_DATABASE:-}}"
DORIS_TABLE="${DORIS_TABLE:-}"
DORIS_USER="${DORIS_USER:-${PISTA_DORIS_USER:-root}}"
DORIS_PASSWORD="${DORIS_PASSWORD:-${PISTA_DORIS_PASSWORD:-}}"
DORIS_QUERY_PORT="${DORIS_QUERY_PORT:-${PISTA_DORIS_QUERY_PORT:-9030}}"

OUTPUT_MODE="${OUTPUT_MODE:-append}"
FORCE_JDBC="${FORCE_JDBC:-false}"

# Spark Doris Connector Configuration
DORIS_AUTO_REDIRECT="${DORIS_AUTO_REDIRECT:-true}"
DORIS_BENODES="${DORIS_BENODES:-${PISTA_DORIS_BENODES:-}}"
DORIS_BATCH_SIZE="${DORIS_BATCH_SIZE:-500000}"
DORIS_FORMAT="${DORIS_FORMAT:-csv}"

# Doris Advanced Feature Configuration
DORIS_CLUSTER_NAME="${DORIS_CLUSTER_NAME:-${PISTA_DORIS_CLUSTER_NAME:-}}"
DORIS_LABEL_PREFIX="${DORIS_LABEL_PREFIX:-pista}"
DORIS_ENABLE_2PC="${DORIS_ENABLE_2PC:-false}"
DORIS_OVERWRITE="${DORIS_OVERWRITE:-false}"
DORIS_PARTITION_DATE="${DORIS_PARTITION_DATE:-}"
DORIS_PARTITION_COLUMN="${DORIS_PARTITION_COLUMN:-}"
DORIS_PARTITION_DATE_FORMAT="${DORIS_PARTITION_DATE_FORMAT:-yyyyMMdd}"
DORIS_OUTPUT_PARTITIONS="${DORIS_OUTPUT_PARTITIONS:-50}"

# Meta Database Configuration
META_HOST="${META_HOST:-${PISTA_META_HOST:-}}"
META_PORT="${META_PORT:-${PISTA_META_PORT:-}}"
META_DATABASE="${META_DATABASE:-${PISTA_META_DATABASE:-}}"
META_USER="${META_USER:-${PISTA_META_USER:-}}"
META_PASSWORD="${META_PASSWORD:-${PISTA_META_PASSWORD:-}}"

# Generic SQL template parameters.
SQL_PARAMS=()
SQL_PARAM_TYPES=()

PISTA_ASSEMBLY_JAR="${PISTA_ASSEMBLY_JAR:-${PISTA_JAR:-}}"

DRIVER_MEMORY="${DRIVER_MEMORY:-${PISTA_DRIVER_MEMORY:-4g}}"
NUM_EXECUTORS="${NUM_EXECUTORS:-${PISTA_NUM_EXECUTORS:-25}}"
EXECUTOR_MEMORY="${EXECUTOR_MEMORY:-${PISTA_EXECUTOR_MEMORY:-16g}}"

## --- HELP INFORMATION ---
usage() {
  cat <<EOF
Usage: $(basename "$0") [OPTIONS]

Through SparkSQLSubmitter, the query results are written to the Doris cluster in Spark. SparkSQLSubmitter the data is written out as lightweight output. SQL results are written to Doris cluster (lightweight output)

Options:
  --sql-file         <path>   SQL File Path (required)
  --nodes          <nodes>  Doris FE nodes addresses, comma-separated (required, e.g. fe1:8030,fe2:8030)
  --database         <name>   Doris Target Database Name (Required)
  --table            <name>   Doris <table name> <name> Doris target table name (required)
  --doris-user       <user>   Doris Authentication Username (Default: root)
  --doris-password   <pass>   Doris Authentication Password (default: empty)
  --query-port       <port>   Doris FE MySQL Query Port (default: 9030)
  --output-mode      <mode>   Output mode: append/overwrite (default: append)
  --force-jdbc       <boolean>   Force JDBC mode (default: false, auto-select)
  --auto-redirect    <boolean>    true=FE redirect, false=BE direct (default: true)
  --benodes          <nodes>  Node lists for BE HTTP nodes, comma-separated (required when auto-redirect=false)
  --batch-size       <n>      Connector batch size (default: 500000)
  --format           <fmt>    Data format: csv/json (default: csv)
  --doris-cluster-name <name> Doris Cluster Name (required for Meta tracking)
  --label-prefix     <prefix> Stream Load Label Prefix (Default: pista)
  --enable-2pc       <boolean>   Enable 2PC transaction (default: false)
  --overwrite        <boolean>   Overwrite Write: With partition-date, overwrite specified partition; without, overwrite entire table (default: false)
  --partition-date   <value>  Date value for partitioning, comma-separated (e.g., 20260407)
  --partition-column <name>   Partition column (for example, partition_date)
  --partition-date-format <fmt> column date format for partitioning (default: yyyyMMdd)
  --output-partitions <n>     Number of partitions (default: 50)
  --param            <k=v>    SQL template parameter; may be repeated
  --param-type       <k=t>    SQL parameter type; may be repeated
  --driver-memory    <size>   Driver memory, such as 4g (default: 4g)
  --num-executors    <n>      Executor Count (Default: 25)
  --executor-memory <size> Executor memory, such as 16g (default: 16g)
  --pista-jar        <path>   Pista assembly JAR path
  --help                      SHOW THIS HELP INFORMATION

Explanation::
  This script uses SparkSQLSubmitter as the entry class to import the results of SQL queries into Doris using a SparkSQLSubmitter.

  Base Mode: Suitable for Outputting Temporary Query Result and Lightweight Data Import Scenarios.
  Advanced Mode: Supports 2PC Transaction, Partition Overwrite Write, and Meta Tracking for enterprise-level features.

Example:
  $(basename "$0") \\
    --sql-file /path/to/query.sql \\
    --fenodes fe1:8030,fe2:8030 \\
    --database mydb \\
    --table mytable \\
    --doris-user root \\
    --doris-password mypassword
EOF
  exit 0
}

[[ $# -eq 0 ]] && usage

# --- Parse command line parameters (higher priority than environment variables) ---
while [[ $# -gt 0 ]]; do
  case "$1" in
    --help)            usage ;;
    --sql-file)        SQL_FILE="$2";            shift 2 ;;
    --fenodes)         DORIS_FENODES="$2";        shift 2 ;;
    --database)        DORIS_DATABASE="$2";       shift 2 ;;
    --table)           DORIS_TABLE="$2";          shift 2 ;;
    --doris-user)      DORIS_USER="$2";           shift 2 ;;
    --doris-password)  DORIS_PASSWORD="$2";       shift 2 ;;
    --query-port)      DORIS_QUERY_PORT="$2";     shift 2 ;;
    --output-mode)     OUTPUT_MODE="$2";          shift 2 ;;
    --force-jdbc)      FORCE_JDBC="$2";           shift 2 ;;
    --auto-redirect)   DORIS_AUTO_REDIRECT="$2";  shift 2 ;;
    --benodes)         DORIS_BENODES="$2";        shift 2 ;;
    --batch-size)      DORIS_BATCH_SIZE="$2";     shift 2 ;;
    --format)          DORIS_FORMAT="$2";         shift 2 ;;
    --doris-cluster-name) DORIS_CLUSTER_NAME="$2"; shift 2 ;;
    --label-prefix)    DORIS_LABEL_PREFIX="$2";   shift 2 ;;
    --enable-2pc)      DORIS_ENABLE_2PC="$2";     shift 2 ;;
    --overwrite) DORIS_OVERWRITE="$2"; shift 2 ;;
    --partition-date)  DORIS_PARTITION_DATE="$2"; shift 2 ;;
    --partition-column) DORIS_PARTITION_COLUMN="$2"; shift 2 ;;
    --partition-date-format) DORIS_PARTITION_DATE_FORMAT="$2"; shift 2 ;;
    --output-partitions) DORIS_OUTPUT_PARTITIONS="$2"; shift 2 ;;
    --param)            SQL_PARAMS+=("$2");       shift 2 ;;
    --param-type)       SQL_PARAM_TYPES+=("$2");  shift 2 ;;
    --driver-memory)   DRIVER_MEMORY="$2";        shift 2 ;;
    --num-executors)   NUM_EXECUTORS="$2";        shift 2 ;;
    --executor-memory) EXECUTOR_MEMORY="$2";      shift 2 ;;
    --pista-jar)       PISTA_ASSEMBLY_JAR="$2";   shift 2 ;;
    *) echo "[ERROR] Unknown parameter: $1" >&2; exit 1 ;;
  esac
done

# --- Required Parameters Validation ---
missing=()
[[ -z "$SQL_FILE" ]]       && missing+=("--sql-file")
[[ -z "$DORIS_FENODES" ]]  && missing+=("--fenodes")
[[ -z "$DORIS_DATABASE" ]] && missing+=("--database")
[[ -z "$DORIS_TABLE" ]]    && missing+=("--table")
if [[ ${#missing[@]} -gt 0 ]]; then
  echo "[ERROR] Missing required parameters: ${missing[*]}" >&2
  exit 1
fi

check_spark_home
require_files "${SQL_FILE}" "${PISTA_ASSEMBLY_JAR}"

echo "========================================"
echo "SparkSQL → Doris Write (via SparkSQLSubmitter)"
echo "  SQL:           ${SQL_FILE}"
echo "  Target:        ${DORIS_DATABASE}.${DORIS_TABLE}"
echo "  FE nodes:      ${DORIS_FENODES}"
echo "  User:          ${DORIS_USER}"
echo "  Mode:          ${OUTPUT_MODE}"
echo "  Force JDBC:    ${FORCE_JDBC}"
echo "  Auto redirect: ${DORIS_AUTO_REDIRECT}"
[[ -n "$DORIS_BENODES" ]] && echo "  BE nodes:      ${DORIS_BENODES}"
echo "  Batch size:    ${DORIS_BATCH_SIZE}"
echo "  Format:        ${DORIS_FORMAT}"
[[ -n "$DORIS_CLUSTER_NAME" ]] && echo "  Cluster:       ${DORIS_CLUSTER_NAME}"
[[ "$DORIS_ENABLE_2PC" == "true" ]] && echo "  2PC:           enabled"
[[ "$DORIS_OVERWRITE" == "true" ]] && echo "  Overwrite: ${DORIS_PARTITION_DATE:-full table}"
[[ -n "$META_HOST" ]] && echo "  Meta tracking: enabled (${META_HOST}:${META_PORT})"
echo "========================================"

# Generic SQL template parameters. Values are passed to Spark without being logged.
SQL_PARAM_CONFS=()
for entry in "${SQL_PARAMS[@]}"; do
  if [[ "$entry" != *=* ]]; then
    echo "[ERROR] Invalid --param value; expected name=value" >&2
    exit 1
  fi
  name="${entry%%=*}"
  value="${entry#*=}"
  if [[ ! "$name" =~ ^[A-Za-z_][A-Za-z0-9_]*$ ]]; then
    echo "[ERROR] Invalid SQL parameter name" >&2
    exit 1
  fi
  SQL_PARAM_CONFS+=(--conf "spark.pista.params.${name}=${value}")
done
for entry in "${SQL_PARAM_TYPES[@]}"; do
  if [[ "$entry" != *=* ]]; then
    echo "[ERROR] Invalid --param-type value; expected name=type" >&2
    exit 1
  fi
  name="${entry%%=*}"
  value="${entry#*=}"
  if [[ ! "$name" =~ ^[A-Za-z_][A-Za-z0-9_]*$ ]]; then
    echo "[ERROR] Invalid SQL parameter type name" >&2
    exit 1
  fi
  SQL_PARAM_CONFS+=(--conf "spark.pista.params.${name}.type=${value}")
done

# Doris Advanced Features Configuration (All via spark.pista.doris.* complete key, read by DorisWriter.buildBatchConfig)
DORIS_ADVANCED_CONFS=()

# clusterName (Meta Usage)
[[ -n "$DORIS_CLUSTER_NAME" ]] && DORIS_ADVANCED_CONFS+=(
  --conf "spark.pista.doris.clusterName=${DORIS_CLUSTER_NAME}"
)

# labelPrefix
[[ -n "$DORIS_LABEL_PREFIX" ]] && DORIS_ADVANCED_CONFS+=(
  --conf "spark.pista.doris.labelPrefix=${DORIS_LABEL_PREFIX}"
)

# enable2PC
[[ "$DORIS_ENABLE_2PC" == "true" ]] && DORIS_ADVANCED_CONFS+=(
  --conf "spark.pista.doris.connector.enable2PC=true"
)

# overwrite
[[ "$DORIS_OVERWRITE" == "true" ]] && DORIS_ADVANCED_CONFS+=(
  --conf "spark.pista.doris.overwrite=true"
)

# partitionDate
[[ -n "$DORIS_PARTITION_DATE" ]] && DORIS_ADVANCED_CONFS+=(
  --conf "spark.pista.doris.partition.dateValue=${DORIS_PARTITION_DATE}"
)

# partitionColumn
[[ -n "$DORIS_PARTITION_COLUMN" ]] && DORIS_ADVANCED_CONFS+=(
  --conf "spark.pista.doris.partition.columnName=${DORIS_PARTITION_COLUMN}"
)

# partitionDateFormat
DORIS_ADVANCED_CONFS+=(
  --conf "spark.pista.doris.partition.dateFormat=${DORIS_PARTITION_DATE_FORMAT}"
)

# outputPartitions(50, overwrite = true)
DORIS_ADVANCED_CONFS+=(
  --conf "spark.pista.doris.outputPartitions=${DORIS_OUTPUT_PARTITIONS}"
)

# batchSize
DORIS_ADVANCED_CONFS+=(
  --conf "spark.pista.doris.connector.batchSize=${DORIS_BATCH_SIZE}"
)

# format
DORIS_ADVANCED_CONFS+=(
  --conf "spark.pista.doris.connector.format=${DORIS_FORMAT}"
)

# autoRedirect
DORIS_ADVANCED_CONFS+=(
  --conf "spark.pista.doris.connector.autoRedirect=${DORIS_AUTO_REDIRECT}"
)

# benodes(auto-redirect=false when used
[[ -n "$DORIS_BENODES" ]] && DORIS_ADVANCED_CONFS+=(
  --conf "spark.pista.doris.connector.benodes=${DORIS_BENODES}"
)

# Meta Database Configuration (Optional, enables transactional equality tracking; runs in degraded mode otherwise)
META_CONFS=()
if [[ -n "$META_HOST" ]]; then
  META_CONFS+=(
    --conf "spark.pista.meta.host=${META_HOST}"
    --conf "spark.pista.meta.port=${META_PORT}"
  )
  # Database, Username, Password: Always Pass Through (Even Empty Strings), Ensure Included in SparkConf as Configuration Key
  # DorisMetaConf.fromSparkConf requires key must exist (value can be null)
  META_CONFS+=(--conf "spark.pista.meta.database=${META_DATABASE}")
  META_CONFS+=(--conf "spark.pista.meta.username=${META_USER}")
  META_CONFS+=(--conf "spark.pista.meta.password=${META_PASSWORD}")
fi

# According to the operating system, get the driver host
if [[ "$(uname -s)" == "Darwin" ]]; then
  SPARK_DRIVER_HOST=$(hostname -f 2>/dev/null || hostname)
else
  SPARK_DRIVER_HOST=$(hostname -i)
fi

SUBMIT_ARGS=(
  --conf "spark.driver.host=${SPARK_DRIVER_HOST}"
  --driver-memory "${DRIVER_MEMORY}"
  --num-executors "${NUM_EXECUTORS}"
  --executor-cores 4
  --executor-memory "${EXECUTOR_MEMORY}"
  --conf spark.yarn.maxAppAttempts=1
  --class com.pista.spark.sql.batch.SparkSQLSubmitter
  --conf "spark.pista.sqlFile_=${SQL_FILE}"
  --conf "spark.pista.output.format=doris"
  --conf "spark.pista.output.path=${DORIS_DATABASE}.${DORIS_TABLE}"
  --conf "spark.pista.output.mode=${OUTPUT_MODE}"
  # JDBC path authentication (local)output.options short keyby Spark configuration parameters DorisSupport.buildJdbcUrl / getJdbcAuth read)
  --conf "spark.pista.output.options.doris.fenodes=${DORIS_FENODES}"
  --conf "spark.pista.output.options.doris.user=${DORIS_USER}"
  --conf "spark.pista.output.options.doris.password=${DORIS_PASSWORD}"
  --conf "spark.pista.output.options.doris.query.port=${DORIS_QUERY_PORT}"
  --conf "spark.pista.output.options.force.jdbc=${FORCE_JDBC}"
  # Connector Path Configuration (complete key for spark.pista.doris.*, read by DorisWriter.buildBatchConfig)
  --conf "spark.pista.doris.fenodes=${DORIS_FENODES}"
  --conf "spark.pista.doris.database=${DORIS_DATABASE}"
  --conf "spark.pista.doris.table=${DORIS_TABLE}"
  --conf "spark.pista.doris.user=${DORIS_USER}"
  --conf "spark.pista.doris.password=${DORIS_PASSWORD}"
  --conf "spark.pista.doris.feQueryPort=${DORIS_QUERY_PORT}"
  "${DORIS_ADVANCED_CONFS[@]+"${DORIS_ADVANCED_CONFS[@]}"}"
  "${META_CONFS[@]+"${META_CONFS[@]}"}"
  "${SQL_PARAM_CONFS[@]+"${SQL_PARAM_CONFS[@]}"}"
  --conf spark.sql.optimizer.runtime.bloomFilter.enabled=true
  --conf spark.sql.optimizer.runtime.bloomFilter.creationSideThreshold=10240MB
  --conf spark.sql.optimizer.runtime.bloomFilter.applicationSideScanSizeThreshold=10GB
  --conf spark.sql.files.maxPartitionBytes=2048MB
  --conf spark.sql.files.minPartitionNum=10
  --conf spark.sql.adaptive.enabled=true
  --conf spark.sql.adaptive.coalescePartitions.enabled=true
  --conf spark.sql.adaptive.coalescePartitions.minPartitionNum=20
  --conf spark.sql.adaptive.coalescePartitions.initialPartitionNum=2000
  --conf spark.sql.adaptive.coalescePartitions.parallelismFirst=false
  --conf spark.sql.adaptive.advisoryPartitionSizeInBytes=256MB
  --conf spark.sql.adaptive.autoBroadcastJoinThreshold=128MB
  --conf spark.executor.memoryOverhead=4096
  --conf spark.sql.shuffle.partitions=1000
  --files "${SQL_FILE}"
  "${PISTA_ASSEMBLY_JAR}"
)

"${SPARK_HOME}/bin/spark-submit" "${SUBMIT_ARGS[@]}"
