#!/usr/bin/env bash
set -euo pipefail

# Try default connection first, then fallback to -U postgres
run_psql_file() {
  local db="$1"; shift
  local file="$1"; shift
  if ! psql -v ON_ERROR_STOP=1 -d "$db" -f "$file" "$@" >/dev/null 2>&1; then
    psql -v ON_ERROR_STOP=1 -U postgres -d "$db" -f "$file" "$@"
  fi
}

run_psql_cmd() {
  local db="$1"; shift
  local cmd="$1"; shift
  if ! psql -v ON_ERROR_STOP=1 -d "$db" -c "$cmd" "$@" >/dev/null 2>&1; then
    psql -v ON_ERROR_STOP=1 -U postgres -d "$db" -c "$cmd" "$@"
  fi
}

# Create role 'test' if needed
run_psql_file postgres tools/setup_postgres_role.sql

# Create database 'test' if needed (must be outside DO block)
if ! psql -v ON_ERROR_STOP=1 -d postgres -tAc "SELECT 1 FROM pg_database WHERE datname = 'test_ormin'" | grep -q 1; then
  run_psql_cmd postgres "CREATE DATABASE test_ormin OWNER test"
fi

# Grant privileges on public schema
run_psql_cmd test_ormin "GRANT ALL PRIVILEGES ON SCHEMA public TO test"

echo "Postgres test DB/user ensured (role 'test', db 'test_ormin')."

