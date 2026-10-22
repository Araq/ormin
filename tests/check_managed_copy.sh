#!/usr/bin/env bash
set -euo pipefail

output=$(nim c tests/should_not_compile_managed_copy.nim 2>&1) && {
  echo "ManagedDbConn unexpectedly allowed a copy"
  exit 1
}

if [[ "$output" != *"'=copy' is not available for type <ManagedDbConn>"* ]]; then
  echo "$output"
  exit 1
fi
