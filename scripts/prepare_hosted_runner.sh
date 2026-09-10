#!/usr/bin/env bash

set -euo pipefail

if [[ "${GITHUB_ACTIONS:-}" != "true" || "${RUNNER_ENVIRONMENT:-}" != "github-hosted" ]]; then
  echo "Refusing to modify disk outside a GitHub-hosted Actions runner." >&2
  exit 1
fi

readonly minimum_free_bytes=$((20 * 1024 * 1024 * 1024))
readonly reclaim_targets=(
  /usr/local/lib/android
  /usr/share/dotnet
  /opt/ghc
  /usr/local/.ghcup
)

free_bytes() {
  df --output=avail --block-size=1 / | tail -n 1 | tr -d '[:space:]'
}

before_bytes="$(free_bytes)"
echo "Root filesystem free space before cleanup: ${before_bytes} bytes"

for target in "${reclaim_targets[@]}"; do
  if [[ -d "$target" ]]; then
    echo "Removing unused hosted-runner tool content from $target"
    sudo find "$target" -xdev -mindepth 1 -delete
  fi
done

after_bytes="$(free_bytes)"
echo "Root filesystem free space after cleanup: ${after_bytes} bytes"

if (( after_bytes < minimum_free_bytes )); then
  echo "Hosted runner has less than 20 GiB free after cleanup." >&2
  exit 1
fi
