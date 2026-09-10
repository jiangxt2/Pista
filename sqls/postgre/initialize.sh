#!/usr/bin/env bash

# Usage: ./initialize.sh [host] [port] [user] [database]
# Configuration Explanation: Modify the following environment variables or directly replace the default values
#   META_HOST: PostgreSQL host address
#   META_PORT: PostgreSQL port
#   META_USER: PostgreSQL username
#   META_DATABASE: PostgreSQL database name
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

# ==================== CONFIGURATION METRICS ====================
# Modify the following configuration based on actual circumstances.
HOST="${META_HOST:-localhost}"            # PostgreSQL Host Address
PORT="${META_PORT:-5432}"                 # PostgreSQL PORT
USER="${META_USER:-}"                     # PostgreSQL Username (Please set manually)
DATABASE = "${META_DATABASE:-pista_meta}"   # PostgreSQL Database Name

# Check if username is configured
if [ -z "$USER" ]; then
  echo "Error: PostgreSQL username is not configured"
  echo "Set the environment variable META_USER or modify the USER variable in the script"
  echo "Example: export META_USER=your_username"
  exit 1
fi

# A password can be empty, using the PGPASSWORD environment variable if set.
# If not configured, psql will prompt for a password based on the .pgpass file or input a password.

PSQL="${PG_HOME}/bin/psql -h${HOST} -p${PORT} -U${USER} -v ON_ERROR_STOP=1"

echo "PostgreSQL connection information:"
echo "  Host: ${HOST}"
echo "  Port: ${PORT}"
echo "  user: ${USER}"
echo "  DATABASE: ${DATABASE}"
echo ""

# Check PostgreSQL connection
echo "Check PostgreSQL connection..."
if ! $PSQL -tc "SELECT 1" > /dev/null 2>&1; then
  echo "Error: Unable to connect to PostgreSQL"
  echo "Please check:"
  echo "  1. Is the PostgreSQL service running"
  echo "  2. Host address and port are correct"
  echo "  3. Username and password are correct"
  echo "  4. Is the PGPASSWORD environment variable set?"
  exit 1
fi
echo "Connection successful!"
echo ""

# Create database if not exists
echo "Create database (if it does not exist): $DATABASE"
$PSQL -tc "SELECT 1 FROM pg_database WHERE datname = '$DATABASE'" | grep -q 1 || \
  $PSQL -c "CREATE DATABASE $DATABASE"
echo "Database Ready: $DATABASE"
echo ""

# Run all SQL files in lexicographic order
echo "Executing SQL script begins..."
for f in "$SCRIPT_DIR"/0*.sql; do
  echo "  -> Execute: $(basename "$f")"
  $PSQL -d "$DATABASE" -f "$f"
done

echo ""
echo "Initialization complete!"
