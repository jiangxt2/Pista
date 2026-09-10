#!/usr/bin/env bash

set -euo pipefail

fe_conf=/opt/apache-doris/fe/conf/fe.conf

set_config() {
  local key="$1"
  local value="$2"
  if grep -qE "^${key}[[:space:]]*=" "$fe_conf"; then
    sed -i -E "s|^${key}[[:space:]]*=.*|${key} = ${value}|" "$fe_conf"
  else
    echo "${key} = ${value}" >> "$fe_conf"
  fi
}

test -w "$fe_conf"
grep -q -- '-Xmx8192m' "$fe_conf"
grep -q -- '-Xms8192m' "$fe_conf"
sed -i -e 's/-Xmx8192m/-Xmx1536m -XX:-UseContainerSupport/' \
  -e 's/-Xms8192m/-Xms512m/' "$fe_conf"
grep -q -- '-XX:-UseContainerSupport' "$fe_conf"

# Match the BE test reserve so FE scheduling does not reject the ephemeral
# hosted-runner disk after the Doris images are unpacked.
set_config storage_high_watermark_usage_percent 99
set_config storage_min_left_capacity_bytes 536870912
set_config storage_flood_stage_usage_percent 99
set_config storage_flood_stage_left_capacity_bytes 536870912

self_ip="$(awk '$1 ~ /^[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+$/ && $1 !~ /^127\./ {print $1; exit}' /etc/hosts)"
if [ -z "$self_ip" ]; then
  self_ip="$(hostname -I 2>/dev/null | tr ' ' '\n' | awk '$0 ~ /^[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+$/ && $0 !~ /^127\./ {print; exit}')"
fi
test -n "$self_ip"
export FE_SERVERS="fe1:${self_ip}:9010"
export FE_ID="${PISTA_FE_ID:-1}"
exec bash /usr/local/bin/init_fe.sh
