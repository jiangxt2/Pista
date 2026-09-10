#!/usr/bin/env bash

#
# Pista Environment Variable Loader
# Load pista-env.sh and elevate all variables to environment variables using set -a.
#
# REQUIRE: PISTA_HOME must be set beforehand (e.g., in ~/.bashrc or ~/.zshrc).
#
# Usage: Add at the top of the business script:
#   source "${PISTA_HOME}/shell/load-pista-env.sh"
#

# prevent duplicate load
if [ -n "${PISTA_ENV_LOADED:-}" ]; then
  return 0
fi
export PISTA_ENV_LOADED=1

if [ -z "${PISTA_HOME:-}" ]; then
  echo "[ERROR] PISTA_HOME is not set. Export it in ~/.bashrc or ~/.zshrc before running Pista scripts." >&2
  exit 1
fi

# Determine the configuration file search directory (supports override by PISTA_CONF_DIR)
PISTA_ENV_DIR="${PISTA_CONF_DIR:-${PISTA_HOME}/shell}"

# Priority Load: pista-env.local.sh (local testing) > pista-env.sh (production environment)
if [ -f "${PISTA_ENV_DIR}/pista-env.local.sh" ]; then
  PISTA_ENV_SH="${PISTA_ENV_DIR}/pista-env.local.sh"
elif [ -f "${PISTA_ENV_DIR}/pista-env.sh" ]; then
  PISTA_ENV_SH="${PISTA_ENV_DIR}/pista-env.sh"
else
  echo "[ERROR] Neither pista-env.local.sh nor pista-env.sh found in ${PISTA_ENV_DIR}" >&2
  exit 1
fi

set -a
# shellcheck source=pista-env.sh
. "${PISTA_ENV_SH}"
set +a
