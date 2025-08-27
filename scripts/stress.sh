#!/bin/bash

set -euo pipefail

# Stress all packages, one at a time. This allows for a more useful output.
for p in $(go list ./... | sed 's#github.com/cockroachdb/pebble/v2#.#'); do
echo
  echo ""
  echo ""
  echo "Stressing $p"
  echo ""
  make stress STRESSFLAGS='-maxtime 5m -maxruns 100' "PKG=$p"
done
