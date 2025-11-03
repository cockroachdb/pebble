# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Pebble Development Environment

Pebble is a LevelDB/RocksDB inspired key-value store focused on
performance and internal usage by CockroachDB.

## Testing
```bash
go test -tags invariants .
go test -tags invariants ./...
```

## Regenerating test files
```bash
go test -tags invariants ./<package> --rewrite
```

## Linting
```bash
go test -tags invariants ./internal/lint"
```

## When generating PRs and commit records

- Follow the format:
  - Separate the subject from the body with a blank line.
  - Use the body of the commit record to explain what existed before your change, what you changed, and why.
  - Prefix the subject line with the package in which the bulk of the changes occur.
  - For multi-commit PRs, summarize each commit in the PR record.
  - Do not include a test plan unless explicitly asked by the user.

## Interaction Style

* Be direct and honest.
* Skip unnecessary acknowledgments.
* Correct me when I'm wrong and explain why.
* Suggest better alternatives if my ideas can be improved.
* Focus on accuracy and efficiency.
* Challenge my assumptions when needed.
* Prioritize quality information and directness.
