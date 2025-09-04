#!/bin/bash

set -euo pipefail

# The GitHub Action runner has 4 cores. Use 3 processes to get good utilization
# but not slow down things too much.
# TODO(radu): replace with a percentage when stress supports it.
PARALLEL="3"

# Stress all packages, one at a time. This allows for a more useful output.
for p in $(go list ./... | sed 's#github.com/cockroachdb/pebble#.#'); do
echo
  echo ""
  echo ""
  echo "Stressing $p"
  echo ""
  case "$p" in
    .|./internal/manifest|./sstable|./wal)
      # These packages have a lot of state space to cover, so we give them more
      # time.
      MAX_TIME=30m
      MAX_RUNS=100
      ;;
    *)
      MAX_TIME=5m
      MAX_RUNS=100
      ;;
  esac
  echo "make stress STRESSFLAGS=\"-maxtime $MAX_TIME -maxruns $MAX_RUNS -p $PARALLEL\" PKG=\"$p\""
  make stress STRESSFLAGS="-maxtime $MAX_TIME -maxruns $MAX_RUNS -p $PARALLEL" PKG="$p"
done
