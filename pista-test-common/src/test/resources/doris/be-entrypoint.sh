#!/usr/bin/env bash

set -euo pipefail

be_conf=/opt/apache-doris/be/conf/be.conf

set_config() {
  local key="$1"
  local value="$2"
  if grep -qE "^${key}[[:space:]]*=" "$be_conf"; then
    sed -i -E "s|^${key}[[:space:]]*=.*|${key} = ${value}|" "$be_conf"
  else
    echo "${key} = ${value}" >> "$be_conf"
  fi
}

test -w "$be_conf"
grep -q -- '-Xmx2048m' "$be_conf"
sed -i 's/-Xmx2048m/-Xmx1024m -XX:-UseContainerSupport/g' "$be_conf"
grep -q -- '-XX:-UseContainerSupport' "$be_conf"

# Hosted runners have limited disk after the Doris images are unpacked. Keep
# an explicit safety reserve while allowing this small, ephemeral test cluster.
set_config mem_limit 1G
set_config storage_flood_stage_usage_percent 99
set_config storage_flood_stage_left_capacity_bytes 536870912

self_ip="$(awk '$1 ~ /^[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+$/ && $1 !~ /^127\./ {print $1; exit}' /etc/hosts)"
if [ -z "$self_ip" ]; then
  self_ip="$(hostname -I 2>/dev/null | tr ' ' '\n' | awk '$0 ~ /^[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+$/ && $0 !~ /^127\./ {print; exit}')"
fi
test -n "$self_ip" && test -n "${PISTA_FE_IP:-}"
test -r /usr/local/bin/init_be.sh
export MASTER_FE_IP="$PISTA_FE_IP"
export CURRENT_BE_IP="$self_ip"
export CURRENT_BE_PORT=9050
export PRIORITY_NETWORKS="${self_ip%.*}.0/24"

# Keep the image's registration logic, but make its foreground supervisor the
# container process so a BE exit is reported directly and with complete logs.
exec bash /usr/local/bin/init_be.sh
