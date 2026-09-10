#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
OUTPUT_DIR="${1:-$PROJECT_ROOT/target/sbom}"

mkdir -p "$OUTPUT_DIR"

cd "$PROJECT_ROOT"
mvn -B org.cyclonedx:cyclonedx-maven-plugin:2.9.3:makeAggregateBom \
  -DoutputFormat=json \
  -DoutputName=pista-jvm.cdx \
  -DoutputDirectory="$OUTPUT_DIR" \
  -DoutputReactorProjects=false \
  -Dcyclonedx.skipAttach=true \
  -DincludeProvidedScope=false \
  -DexcludeTestProject=true

python3 -B "$PROJECT_ROOT/scripts/check_sbom.py" "$OUTPUT_DIR"
