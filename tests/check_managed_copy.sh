#!/usr/bin/env bash
set -euo pipefail

check_copy() {
  local owned_type="$1"
  shift
  local output
  if output=$(nim c "$@" tests/should_not_compile_managed_copy.nim 2>&1); then
    echo "$owned_type unexpectedly allowed a copy"
    exit 1
  fi
  if [[ "$output" != *"'=copy' is not available for type <$owned_type>"* ]]; then
    echo "$output"
    exit 1
  fi
}

check_copy ManagedDbConn "$@"
if [[ " $* " == *" -d:sqlite "* ]]; then
  check_copy ManagedStmt "$@" -d:copyManagedStatement
fi
