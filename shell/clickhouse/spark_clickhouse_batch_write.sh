#!/bin/bash

source "${PISTA_HOME}/shell/load-pista-env.sh"
source "${PISTA_HOME}/shell/spark-utils.sh"

# --- Default Values (Preferred from pista-env.sh Configuration) ---
CLICKHOUSE_DATABASE="${CLICKHOUSE_DATABASE:-}"
CLICKHOUSE_TABLE="${CLICKHOUSE_TABLE:-}"
CLICKHOUSE_USER="${CLICKHOUSE_USER:-${PISTA_CLICKHOUSE_WRITER_USER:-}}"
CLICKHOUSE_PASSWORD="${CLICKHOUSE_PASSWORD:-${PISTA_CLICKHOUSE_WRITER_PASSWORD:-}}"
CLICKHOUSE_HOST="${CLICKHOUSE_HOST:-}"
CLICKHOUSE_PORT="${CLICKHOUSE_PORT:-${PISTA_CLICKHOUSE_PORT:-8123}}"
CLICKHOUSE_CLUSTER="${CLICKHOUSE_CLUSTER:-${PISTA_CLICKHOUSE_CLUSTER:-ck_cluster}}"

CLICKHOUSE_MACHINE_COUNT="${CLICKHOUSE_MACHINE_COUNT:-${PISTA_CLICKHOUSE_MACHINE_COUNT:-6}}"
CLICKHOUSE_PRIMARY_KEY="${CLICKHOUSE_PRIMARY_KEY:-id}"
CLICKHOUSE_MAX_THREADS="${CLICKHOUSE_MAX_THREADS:-12}"
CLICKHOUSE_BATCH_SIZE="${CLICKHOUSE_BATCH_SIZE:-200000}"
CLICKHOUSE_OUTPUT_PARTITIONS="${CLICKHOUSE_OUTPUT_PARTITIONS:-10}"
CLICKHOUSE_OVERWRITE="${CLICKHOUSE_OVERWRITE:-false}"
CLICKHOUSE_OVERWRITE_MODE="${CLICKHOUSE_OVERWRITE_MODE:-on_cluster}"

CLICKHOUSE_PARTITION_COLUMN="${CLICKHOUSE_PARTITION_COLUMN:-}"
CLICKHOUSE_PARTITION_DATE="${CLICKHOUSE_PARTITION_DATE:-}"

CLICKHOUSE_PREPARTITION_DIR="${CLICKHOUSE_PREPARTITION_DIR:-}"
CLICKHOUSE_PREPARTITION_FORMAT="${CLICKHOUSE_PREPARTITION_FORMAT:-parquet}"
CLICKHOUSE_PREPARTITION_COMPRESSION="${CLICKHOUSE_PREPARTITION_COMPRESSION:-zstd}"

# Generic SQL template parameters.
SQL_PARAMS=()
SQL_PARAM_TYPES=()

NO_META=false
META_HOST="${META_HOST:-${PISTA_META_HOST:-}}"
META_PORT="${META_PORT:-${PISTA_META_PORT:-}}"
META_DATABASE="${META_DATABASE:-${PISTA_META_DATABASE:-}}"
META_USER="${META_USER:-${PISTA_META_USER:-}}"
META_PASSWORD="${META_PASSWORD:-${PISTA_META_PASSWORD:-}}"

SQL_FILE="${SQL_FILE:-}"
PISTA_ASSEMBLY_JAR="${PISTA_ASSEMBLY_JAR:-${PISTA_JAR:-}}"

DRIVER_MEMORY="${DRIVER_MEMORY:-${PISTA_DRIVER_MEMORY:-4g}}"
NUM_EXECUTORS="${NUM_EXECUTORS:-${PISTA_NUM_EXECUTORS:-25}}"
EXECUTOR_MEMORY="${EXECUTOR_MEMORY:-${PISTA_EXECUTOR_MEMORY:-16g}}"

## --- HELP INFORMATION ---
usage() {
  cat <<EOF
Usage: $(basename "$0") [OPTIONS]

Options:
  --sql-file            <path>   SQL File Path (Required)
  --database            <name>   Target database name in ClickHouse (required)
  --table               <name>   ClickHouse <table name> <name> ClickHouse target table name (required)
  --ck-user             <user>   ClickHouse User Name (Required)
  --ck-password         <pass>   Password for ClickHouse (required)
  --ck-host             <host>   ClickHouse Direct Connect Node, used in single-machine mode (machine-count=1)
  --ck-port             <port>   ClickHouse HTTP Port (default: 8123)
  --cluster             <name>   ClickHouse Cluster Name (default: ck_cluster)
  --machine-count       <n>      number of partitions (default): 6)
  --primary-key         <col>    Primary key column (default: id)
  --max-threads <n>      Maximum concurrent threads per shard (default: 12)
  --batch-size          <n>      Batch write size for JDBC (default: 200000)
  --output-partitions <n>      Write partitions before coalesce (default: 10)
  --overwrite           <boolean>   Overwrite write, true/false (default: false)
  --overwrite-mode      <mode>   Overwrite mode: on_cluster/per_host (default: on_cluster)
  --partition-column    <col>    Partition Column Name, specify with --partition-date.
  --partition-date      <value>  Date value for partitioning, such as 202603, must be paired with --partition-column
  --prepartition-dir    <path>   AQE partition directory, e.g. file:///tmp/pista-prepartition/ (skip if not provided)
  --param               <k=v>    SQL template parameter; may be repeated
  --param-type          <k=t>    SQL parameter type; may be repeated
  --driver-memory       <size>   Driver memory, such as 4g (default: 4g)
  --num-executors       <n>      Executor number (default): 25)
  --executor-memory     <size>   Executor memory, such as 16g (default: 16g)
  --pista-jar           <path>   Pista assembly JAR path
  --no-meta                      skip Meta Use meta configuration for batch write-on-single-node.
  --help                         DISPLAY THIS HELP INFORMATION
EOF
  exit 0
}

[[ $# -eq 0 ]] && usage

# --- Parse command line parameters (higher priority than environment variables) ---
while [[ $# -gt 0 ]]; do
  case "$1" in
    --help)               usage ;;
    --sql-file)           SQL_FILE="$2";                      shift 2 ;;
    --database)           CLICKHOUSE_DATABASE="$2";           shift 2 ;;
    --table)              CLICKHOUSE_TABLE="$2";              shift 2 ;;
    --ck-user)            CLICKHOUSE_USER="$2";               shift 2 ;;
    --ck-password)        CLICKHOUSE_PASSWORD="$2";           shift 2 ;;
    --ck-host)            CLICKHOUSE_HOST="$2";               shift 2 ;;
    --ck-port)            CLICKHOUSE_PORT="$2";               shift 2 ;;
    --cluster)            CLICKHOUSE_CLUSTER="$2";            shift 2 ;;
    --machine-count)      CLICKHOUSE_MACHINE_COUNT="$2";      shift 2 ;;
    --primary-key)        CLICKHOUSE_PRIMARY_KEY="$2";        shift 2 ;;
    --max-threads)        CLICKHOUSE_MAX_THREADS="$2";        shift 2 ;;
    --batch-size)         CLICKHOUSE_BATCH_SIZE="$2";         shift 2 ;;
    --output-partitions)  CLICKHOUSE_OUTPUT_PARTITIONS="$2";  shift 2 ;;
    --overwrite)          CLICKHOUSE_OVERWRITE="$2";          shift 2 ;;
    --overwrite-mode)     CLICKHOUSE_OVERWRITE_MODE="$2";     shift 2 ;;
    --partition-column)   CLICKHOUSE_PARTITION_COLUMN="$2";   shift 2 ;;
    --partition-date)     CLICKHOUSE_PARTITION_DATE="$2";     shift 2 ;;
    --prepartition-dir)   CLICKHOUSE_PREPARTITION_DIR="$2";   shift 2 ;;
    --param)               SQL_PARAMS+=("$2");                shift 2 ;;
    --param-type)          SQL_PARAM_TYPES+=("$2");           shift 2 ;;
    --driver-memory)      DRIVER_MEMORY="$2";                 shift 2 ;;
    --num-executors)      NUM_EXECUTORS="$2";                 shift 2 ;;
    --executor-memory)    EXECUTOR_MEMORY="$2";               shift 2 ;;
    --pista-jar)          PISTA_ASSEMBLY_JAR="$2";            shift 2 ;;
    --no-meta)            NO_META=true;                       shift ;;
    *) echo "[ERROR] Unknown parameter: $1" >&2; exit 1 ;;
  esac
done

# --- Required Parameters Validation ---
missing=()
[[ -z "$SQL_FILE" ]]             && missing+=("--sql-file")
[[ -z "$CLICKHOUSE_DATABASE" ]]  && missing+=("--database")
[[ -z "$CLICKHOUSE_TABLE" ]]     && missing+=("--table")
[[ -z "$CLICKHOUSE_USER" ]]      && missing+=("--ck-user")
[[ -z "$CLICKHOUSE_PASSWORD" ]]  && missing+=("--ck-password")
if [[ ${#missing[@]} -gt 0 ]]; then
  echo "[ERROR] Missing required parameters: ${missing[*]}" >&2
  exit 1
fi

if [[ -n "$CLICKHOUSE_PARTITION_COLUMN" && -z "$CLICKHOUSE_PARTITION_DATE" ]]; then
  echo "[ERROR] Specified --partition-column but missing --partition-date" >&2; exit 1
fi

check_spark_home
require_files "${SQL_FILE}" "${PISTA_ASSEMBLY_JAR}"

echo "========================================"
echo "SparkSQL → Write to ClickHouse"
echo "  SQL:            ${SQL_FILE}"
echo "  Target:         ${CLICKHOUSE_DATABASE}.${CLICKHOUSE_TABLE}"
echo "  Cluster:        ${CLICKHOUSE_CLUSTER}  Shards: ${CLICKHOUSE_MACHINE_COUNT}"
echo "  PrimaryKey:     ${CLICKHOUSE_PRIMARY_KEY}"
echo "  maxThreads:     ${CLICKHOUSE_MAX_THREADS}  batchSize: ${CLICKHOUSE_BATCH_SIZE}"
echo "  overwrite:      ${CLICKHOUSE_OVERWRITE}  overwriteMode: ${CLICKHOUSE_OVERWRITE_MODE}"
echo "  meta:           $( [[ "$NO_META" == true ]] && echo disabled || echo enabled )"
[[ -n "$CLICKHOUSE_PARTITION_COLUMN" ]] && echo "  partition:      ${CLICKHOUSE_PARTITION_COLUMN}=${CLICKHOUSE_PARTITION_DATE}"
[[ -n "$CLICKHOUSE_PREPARTITION_DIR" ]] && echo "  prepartition:   ${CLICKHOUSE_PREPARTITION_DIR} (${CLICKHOUSE_PREPARTITION_FORMAT}/${CLICKHOUSE_PREPARTITION_COMPRESSION})"
echo "========================================"

# meta JAR is included in pista-assembly and does not need to be passed via --jars directly.

# meta conf(no-meta Mode is not transmitted.
META_CONFS=()
if [[ "$NO_META" == false ]]; then
  META_CONFS=(
    --conf "spark.pista.meta.host=${META_HOST}"
    --conf "spark.pista.meta.port=${META_PORT}"
    --conf "spark.pista.meta.database=${META_DATABASE}"
    --conf "spark.pista.meta.username=${META_USER}"
    --conf "spark.pista.meta.password=${META_PASSWORD}"
  )
fi

# partition parameters (empty array for non-partitioned tables)
PARTITION_CONFS=()
if [[ -n "$CLICKHOUSE_PARTITION_COLUMN" ]]; then
  PARTITION_CONFS=(
    --conf "spark.pista.clickhouse.partition.columnName=${CLICKHOUSE_PARTITION_COLUMN}"
    --conf "spark.pista.clickhouse.partition.dateValue=${CLICKHOUSE_PARTITION_DATE}"
  )
fi

# AQE Pre-Partition Parameters (Skipped When Not Passing --prepartition-dir)
PREPARTITION_CONFS=()
if [[ -n "$CLICKHOUSE_PREPARTITION_DIR" ]]; then
  PREPARTITION_CONFS=(
    --conf "spark.pista.clickhouse.prepartition.dir=${CLICKHOUSE_PREPARTITION_DIR}"
    --conf "spark.pista.clickhouse.prepartition.format=${CLICKHOUSE_PREPARTITION_FORMAT}"
    --conf "spark.pista.clickhouse.prepartition.compression=${CLICKHOUSE_PREPARTITION_COMPRESSION}"
  )
fi

# Single-Node Direct Parameters (Effective with --ck-host specified)
CLICKHOUSE_HOST_CONFS=()
if [[ -n "$CLICKHOUSE_HOST" ]]; then
  CLICKHOUSE_HOST_CONFS=(
    --conf "spark.pista.clickhouse.host=${CLICKHOUSE_HOST}"
    --conf "spark.pista.clickhouse.port=${CLICKHOUSE_PORT}"
    --conf "spark.pista.output.path=jdbc:clickhouse://${CLICKHOUSE_HOST}:${CLICKHOUSE_PORT}/${CLICKHOUSE_DATABASE}.${CLICKHOUSE_TABLE}"
  )
fi

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
  --conf "spark.pista.output.format=clickhouse"
  --conf "spark.pista.output.options.clickhouse.username=${CLICKHOUSE_USER}"
  --conf "spark.pista.output.options.clickhouse.password=${CLICKHOUSE_PASSWORD}"
  --conf "spark.pista.clickhouse.database=${CLICKHOUSE_DATABASE}"
  --conf "spark.pista.clickhouse.table=${CLICKHOUSE_TABLE}"
  --conf "spark.pista.clickhouse.clusterName=${CLICKHOUSE_CLUSTER}"
  --conf "spark.pista.clickhouse.machine.count=${CLICKHOUSE_MACHINE_COUNT}"
  --conf "spark.pista.clickhouse.primaryKey=${CLICKHOUSE_PRIMARY_KEY}"
  --conf "spark.pista.clickhouse.concurrent.maxThreads=${CLICKHOUSE_MAX_THREADS}"
  --conf "spark.pista.clickhouse.batch.size=${CLICKHOUSE_BATCH_SIZE}"
  --conf "spark.pista.clickhouse.output.partitions=${CLICKHOUSE_OUTPUT_PARTITIONS}"
  --conf "spark.pista.clickhouse.overwrite=${CLICKHOUSE_OVERWRITE}"
  --conf "spark.pista.clickhouse.overwrite.mode=${CLICKHOUSE_OVERWRITE_MODE}"
  "${CLICKHOUSE_HOST_CONFS[@]+"${CLICKHOUSE_HOST_CONFS[@]}"}"
  "${PARTITION_CONFS[@]+"${PARTITION_CONFS[@]}"}"
  "${META_CONFS[@]+"${META_CONFS[@]}"}"
  "${PREPARTITION_CONFS[@]+"${PREPARTITION_CONFS[@]}"}"
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
