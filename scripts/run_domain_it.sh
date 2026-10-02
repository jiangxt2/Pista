#!/usr/bin/env bash

set -euo pipefail

run_shard() {
  local name="$1"
  local packages="$2"
  local expected_tests="$3"
  local log_file="${TMPDIR:-/tmp}/pista-domain-it-${name}.log"

  mvn -B -Pit test -DmembersOnlySuites="$packages" 2>&1 | tee "$log_file"

  local actual_tests
  actual_tests="$(awk '/Total number of tests run:/ { total += $NF } END { print total + 0 }' "$log_file")"
  if [[ "$actual_tests" != "$expected_tests" ]]; then
    echo "Domain IT shard ${name} ran ${actual_tests} tests; expected ${expected_tests}." >&2
    exit 1
  fi
}

run_native_shard() {
  local name="$1"
  local packages="$2"
  local expected_tests="$3"
  local log_file="${TMPDIR:-/tmp}/pista-domain-it-${name}.log"

  mvn -B test -DmembersOnlySuites="$packages" 2>&1 | tee "$log_file"

  local actual_tests
  actual_tests="$(awk '/Total number of tests run:/ { total += $NF } END { print total + 0 }' "$log_file")"
  if [[ "$actual_tests" != "$expected_tests" ]]; then
    echo "Native domain shard ${name} ran ${actual_tests} tests; expected ${expected_tests}." >&2
    exit 1
  fi
}

# Each shard runs in a separate Maven process so its Testcontainers graph is
# closed before the next engine starts on a memory-constrained hosted runner.
run_requested_shard() {
  case "$1" in
    clickhouse)
      run_shard clickhouse \
        "com.pista.spark.sql.clickhouse.meta,com.pista.spark.sql.connector.clickhouse" 8
      ;;
    doris)
      run_shard doris \
        "com.pista.spark.sql.doris.meta,com.pista.spark.sql.connector.doris" 10
      ;;
    iceberg)
      run_native_shard iceberg "com.pista.spark.sql.batch.iceberg" 5
      ;;
    support)
      run_shard support \
        "com.pista.spark.sql.test.util,com.pista.spark.sql.batch,com.pista.spark.sql.test.container" 4
      ;;
    *)
      echo "Unknown Domain IT shard: $1" >&2
      exit 1
      ;;
  esac
}

if (( $# == 0 )); then
  set -- clickhouse doris iceberg support
fi

for shard in "$@"; do
  run_requested_shard "$shard"
done
