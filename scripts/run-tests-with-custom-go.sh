#!/usr/bin/env bash
# run-tests-with-custom-go.sh
#
# Downloads and builds a custom Go toolchain from cockcroachdb/go (specific
# branch), caches it in ~/.cache/cockcroachdb-go/<branch>/<sha>, and runs
# `go test`.
#
# Works in GitHub Actions or when run manually.
# In CI: pass GO_SHA to pin an exact commit.
# Locally: if GO_SHA is unset, script fetches the latest branch tip.

set -euo pipefail

GO_REPO="https://github.com/cockroachdb/go.git"
GO_SHA="${GO_SHA:-}"

if [ -z "$GO_SHA" ]; then
  GO_BRANCH="${GO_BRANCH:-cockroach-go1.23.12}"
  echo "==> Resolving latest SHA for branch $GO_BRANCH..."
  GO_SHA=$(git ls-remote "$GO_REPO" "refs/heads/$GO_BRANCH" | cut -f1)
fi

# Use GITHUB_WORKSPACE if present (GitHub Actions), else current dir
REPO_ROOT="${GITHUB_WORKSPACE:-$(pwd)}"

# Cache location (works locally and in CI)
CACHE_BASE="${XDG_CACHE_HOME:-$HOME/.cache}/cockroachdb-go"
CACHE_DIR="$CACHE_BASE/$GO_SHA"
SRC_DIR="$CACHE_DIR/src"

echo "==> Commit SHA: $GO_SHA"
echo "==> Repository root: $REPO_ROOT"
echo "==> Cache directory: $CACHE_DIR"

if [ ! -x "$SRC_DIR/go/bin/go" ]; then
  echo "==> Building new Go toolchain..."
  mkdir -p "$SRC_DIR"
  rm -rf "$SRC_DIR/go" # in case of partial/incomplete build

  git clone "$GO_REPO" "$SRC_DIR/go"
  cd "$SRC_DIR/go"
  git checkout "$GO_SHA"

  cd "$SRC_DIR/go/src"
  ./make.bash
else
  echo "==> Reusing cached Go toolchain"
fi

# Point environment to new Go
export GOROOT="$SRC_DIR/go"
export PATH="$GOROOT/bin:$PATH"

echo "==> Custom Go version:"
go version

echo "==> Running tests in $REPO_ROOT"
cd "$REPO_ROOT"
echo go test -tags cockroach_go "$@"
go test -tags cockroach_go "$@"
