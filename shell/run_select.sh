#!/bin/bash

source "${PISTA_HOME}/shell/load-pista-env.sh"
source "${PISTA_HOME}/shell/spark-utils.sh"

# --- Default Values (Preferred from pista-env.sh Configuration) ---
SQL_FILE="${SQL_FILE:-}"
PISTA_ASSEMBLY_JAR="${PISTA_ASSEMBLY_JAR:-${PISTA_JAR:-}}"

DRIVER_MEMORY="${DRIVER_MEMORY:-${PISTA_DRIVER_MEMORY:-4g}}"
NUM_EXECUTORS="${NUM_EXECUTORS:-${PISTA_NUM_EXECUTORS:-25}}"
EXECUTOR_MEMORY="${EXECUTOR_MEMORY:-${PISTA_EXECUTOR_MEMORY:-16g}}"

# Generic SQL template parameters.
SQL_PARAMS=()
SQL_PARAM_TYPES=()

## --- HELP INFORMATION ---
usage() {
  cat <<EOF
Usage: $(basename "$0") [OPTIONS]

Options:
  --sql-file         <path>   SQL file path (required)
  --param            <k=v>    SQL template parameter; may be repeated
  --param-type       <k=t>    SQL parameter type; may be repeated
  --driver-memory    <size>   Driver memory (default: 4g)
  --num-executors    <n>      Number of executors (default: 25)
  --executor-memory  <size>   Executor memory (default: 16g)
  --pista-jar        <path>   Pista assembly JAR path
  --help                      Show this help message
EOF
  exit 0
}

[[ $# -eq 0 ]] && usage

# --- Parse command line parameters (higher priority than environment variables) ---
while [[ $# -gt 0 ]]; do
  case "$1" in
    --help)            usage ;;
    --sql-file)        SQL_FILE="$2";           shift 2 ;;
    --param)            SQL_PARAMS+=("$2");      shift 2 ;;
    --param-type)       SQL_PARAM_TYPES+=("$2"); shift 2 ;;
    --driver-memory)   DRIVER_MEMORY="$2";      shift 2 ;;
    --num-executors)   NUM_EXECUTORS="$2";      shift 2 ;;
    --executor-memory) EXECUTOR_MEMORY="$2";    shift 2 ;;
    --pista-jar)       PISTA_ASSEMBLY_JAR="$2"; shift 2 ;;
    *) echo "[ERROR] Unknown option: $1" >&2; exit 1 ;;
  esac
done

# --- Required Parameters Validation ---
[[ -z "$SQL_FILE" ]] && { echo "[ERROR] Missing required option: --sql-file" >&2; exit 1; }

check_spark_home
require_files "${SQL_FILE}" "${PISTA_ASSEMBLY_JAR}"

echo "========================================"
echo "SparkSQL SELECT query"
echo "  SQL: ${SQL_FILE}"
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
  "${SQL_PARAM_CONFS[@]+"${SQL_PARAM_CONFS[@]}"}"
  --conf spark.sql.files.maxPartitionBytes=2048MB
  --conf spark.sql.files.minPartitionNum=10
  --conf spark.sql.adaptive.enabled=true
  --conf spark.sql.adaptive.coalescePartitions.enabled=true
  --conf spark.sql.adaptive.coalescePartitions.minPartitionNum=20
  --conf spark.sql.adaptive.coalescePartitions.initialPartitionNum=1000
  --conf spark.sql.adaptive.coalescePartitions.parallelismFirst=false
  --conf spark.sql.adaptive.advisoryPartitionSizeInBytes=256MB
  --conf spark.sql.adaptive.autoBroadcastJoinThreshold=128MB
  --conf spark.executor.memoryOverhead=4096
  --conf spark.sql.shuffle.partitions=1000
  --files "${SQL_FILE}"
  "${PISTA_ASSEMBLY_JAR}"
)

"${SPARK_HOME}/bin/spark-submit" "${SUBMIT_ARGS[@]}"
