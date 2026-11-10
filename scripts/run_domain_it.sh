#!/usr/bin/env bash

set -euo pipefail

clickhouse_packages="com.pista.spark.sql.clickhouse.meta,com.pista.spark.sql.connector.clickhouse"
doris_packages="com.pista.spark.sql.doris.meta,com.pista.spark.sql.connector.doris"
iceberg_packages="com.pista.spark.sql.batch.iceberg"
support_packages="com.pista.spark.sql.test.util,com.pista.spark.sql.batch,com.pista.spark.sql.test.container"

run_requested_shard() {
  local name="$1"
  local packages=""
  local expected_tests=""
  local profile="it"
  case "$name" in
    clickhouse)
      packages="$clickhouse_packages"
      expected_tests=11
      ;;
    doris)
      packages="$doris_packages"
      expected_tests=31
      ;;
    iceberg)
      packages="$iceberg_packages"
      expected_tests=5
      profile=""
      ;;
    support)
      packages="$support_packages"
      expected_tests=4
      ;;
    submitter)
      profile="submitter-it"
      ;;
    *)
      echo "Unknown Domain IT shard: $name" >&2
      return 1
      ;;
  esac

  local report_dir="pista-it-${name}-$(date +%s)-$$"
  local log_file="target/${report_dir}/run.log"
  mkdir -p "target/${report_dir}"
  local report_option="-Dpista.test.report.directory=${report_dir}"
  ./mvnw -B -Pci-discovery process-test-classes -DskipTests "$report_option" \
    "-Dpista.it.clickhouse.packages=$clickhouse_packages" \
    "-Dpista.it.doris.packages=$doris_packages" \
    "-Dpista.it.iceberg.packages=$iceberg_packages" \
    "-Dpista.it.support.packages=$support_packages" 2>&1 | tee "$log_file"
  python3 -B scripts/verify_it_reports.py "$name" "$report_dir" --before | tee -a "$log_file"

  local args=(-B test "$report_option")
  if [[ -n "$profile" ]]; then
    args+=("-P${profile}")
  fi
  if [[ -n "$packages" ]]; then
    args+=("-DmembersOnlySuites=${packages}")
  fi
  ./mvnw "${args[@]}" 2>&1 | tee -a "$log_file"
  python3 -B scripts/verify_it_reports.py "$name" "$report_dir" | tee -a "$log_file"
  if [[ -n "$expected_tests" ]]; then
    local actual_tests
    actual_tests="$(awk '/Total number of tests run:/ { total += $NF } END { print total + 0 }' "$log_file")"
    if [[ "$actual_tests" != "$expected_tests" ]]; then
      echo "Domain IT shard ${name} ran ${actual_tests} tests; expected ${expected_tests}." >&2
      return 1
    fi
  fi
}

if (( $# == 0 )); then
  set -- clickhouse doris iceberg support
fi
for shard in "$@"; do
  run_requested_shard "$shard"
done
