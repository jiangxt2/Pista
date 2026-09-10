#!/usr/bin/env bash

# General Spark Utility Functions Library, No Native Path Dependency, Adaptable for Development and Production Environments

# Check if SPARK_HOME is set and available.
check_spark_home() {
  if [ -z "${SPARK_HOME:-}" ]; then
    echo "Error: SPARK_HOME is not set" >&2
    exit 1
  fi
  if [ ! -x "${SPARK_HOME}/bin/spark-submit" ]; then
    echo "Error: spark-submit not found at ${SPARK_HOME}/bin/spark-submit" >&2
    exit 1
  fi
}

# check if the inspection file exists, report an error and exit if missing.
# Usage: require_files file1 file2 ...
require_files() {
  local missing=()
  local file
  for file in "$@"; do
    [ -f "${file}" ] || missing+=("${file}")
  done
  if [ "${#missing[@]}" -gt 0 ]; then
    echo "Error: required files are missing:" >&2
    printf '  %s\n' "${missing[@]}" >&2
    exit 1
  fi
}
