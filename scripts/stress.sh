#!/bin/bash

set -euo pipefail

# Stress all packages, one at a time. This allows for a more useful output.
for p in $(go list ./... | sed 's#github.com/cockroachdb/pebble#.#'); do
echo
  echo ""
  echo ""
  echo "Stressing $p"
  echo ""
  case "$p" in
    .|./internal/manifest|./internal/metamorphic|./sstable|./wal)
      # These packages have a lot of state space to cover, so we give them more
      # time. We also reduce the number of processes to avoid slowing down each
      # individual run too much.
      MAX_TIME=30m
      MAX_RUNS=1000
      PARALLEL="75%"
      ;;
    *)
      MAX_TIME=5m
      MAX_RUNS=1000
      PARALLEL="100%"
      ;;
  esac
  echo "make stress STRESSFLAGS=\"-maxtime $MAX_TIME -maxruns $MAX_RUNS -p $PARALLEL\" PKG=\"$p\""
  make stress STRESSFLAGS="-maxtime $MAX_TIME -maxruns $MAX_RUNS -p $PARALLEL" PKG="$p"
done
