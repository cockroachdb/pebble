#!/usr/bin/env bash

set -xeuo pipefail

output_json_file="$1"
packages="$2"

if [ -z "${packages}" ]; then
  echo "Skipping"
  touch "${output_json_file}"
  exit 0
fi

# Find the targets. We need to convert from, e.g.
#   . objstorage objstorage/objstorageprovider
# to
#   . ./objstorage ./objstorage/objstorageprovider

paths=""
sep=""

for p in ${packages}; do
  if [[ $p != "." ]]; then
    p="./$p"
  fi
  paths="${paths}${sep}${p}"
  sep=" "
done

tmpfile=$(mktemp --suffix -coverprofile)
trap 'rm -f "${tmpfile}"' EXIT

make testcoverage COVER_PROFILE="${tmpfile}" PKG="$paths"
go run github.com/cockroachdb/code-cov-utils/gocover2json@v1.0.0 \
  --trim-prefix github.com/cockroachdb/pebble/ \
  "${tmpfile}" "${output_json_file}"
