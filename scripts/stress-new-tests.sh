#!/bin/bash
set -euo pipefail

# Base branch to diff against
BASE_BRANCH="${BASE_BRANCH:-origin/master}"
MODULE_PREFIX="github.com/cockroachdb/pebble"

# Loop through each Go package
echo "Checking packages for new tests..."
for pkg in $(go list ./...); do
  # Strip module prefix to get relative path
  pkg=${pkg#${MODULE_PREFIX}}
  pkg="./${pkg#/}"  # Ensure ./ and remove any leading /

  # Get added test functions in this package
  added_tests=$(git diff --no-ext-diff "$BASE_BRANCH" --unified=0 -- "$pkg"/*.go 2>/dev/null \
    | grep '^+func Test' \
    | awk '{print $2}' \
    | cut -d'(' -f1 \
    | sort -u || true)

  if [[ -z "$added_tests" ]]; then
    continue
  fi

  # Build regex for go test -run
  regex=$(echo "$added_tests" | paste -sd '|' -)
  full_regex="^($regex)$"

  echo ""
  echo "Found new tests in $pkg"

  echo "go test --tags invariants --exec 'stress -p 2 --maxruns 1000 --maxtime 10m --timeout 2m' -v -run \"$full_regex\" \"$pkg\""
  go test --tags invariants --exec 'stress -p 2 --maxruns 1000 --maxtime 10m --timeout 2m' -v -run "$full_regex" "$pkg" || {
    echo ""
    echo "❌ Failure in $pkg with new tests: $regex"
    exit 1
  }
done

echo "✅ All stress tests passed."
