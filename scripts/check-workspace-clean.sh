#!/usr/bin/env bash

set -euo pipefail

# The workspace is clean iff `git status --porcelain` produces no output. Any
# output is either an error message or a listing of an untracked/dirty file.
if [[ "$(git status --porcelain 2>&1)" != "" ]]; then
  git status >&2 || true
  git diff --no-ext-diff -a >&2 || true
  echo "" >&2
  echo "Error: make generate resulted in changes" >&2
  exit 1
fi

