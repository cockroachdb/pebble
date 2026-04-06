---
name: bug-catcher
description: Audit a Go package in this repository for subtle correctness bugs, including both bugs inside the package and bugs in how the rest of the repository uses it. Optimized for Pebble- and CockroachDB-style code where invariants, iterator semantics, ownership, and API contracts matter more than style issues.
---

You are running a multi-stage audit of a Go package.

The user will provide exactly one argument: a repo-relative path to a Go package directory.

Your goal is to produce a final report containing **confirmed bugs only**, ordered by severity, with no fix suggestions.

This skill is optimized for large Go storage/database codebases like Pebble and CockroachDB. Favor semantic correctness, invariants, and contract violations over stylistic critique.

## Primary objective

Find bugs that are likely to pass unnoticed by a human reviewer, especially:

- typos that still compile but alter behavior
- wrong field / constant / comparer / option / bound / level / sequence number chosen
- subtle API misuse by callers
- semantic mismatches between comments, names, and implementation
- edge cases around iterator validity, bounds, exhaustion, and positioning
- ownership / aliasing / lifetime mistakes
- incorrect assumptions around zero values or sentinel values
- ordering mistakes
- concurrency / locking / callback / reentrancy mistakes
- stale invariants after refactors
- test coverage that appears convincing but fails to pin down the actual contract

## Non-goals

- no style review
- no formatting nits
- no speculative “maybe” issues in the final report
- no patch suggestions
- no performance-only observations unless they imply a correctness bug

## Required workflow

### Stage 1: build package understanding

Start the `package-understander` subagent on the target package path.

Its output must be written to a durable artifact, not only held in conversational state.

Write to:

`$TMPDIR/bug-catcher/<sanitized-package-path>/understanding.md`

The understanding must capture:

- package purpose
- exported API
- key internal helpers and state transitions
- invariants and hidden assumptions
- important data representations
- ownership / lifetime expectations
- concurrency / synchronization expectations
- important tests and what they appear to specify
- key dependencies
- major callers in the repo
- what those callers appear to assume about this package
- bug-prone surfaces

### Stage 2: generate candidate bugs

Using the understanding artifact plus direct source inspection, generate a candidate list from two angles:

1. bugs in the package implementation
2. bugs in how other code in the repository uses the package

For each candidate, record:

- title
- category: `internal-package-bug` or `caller-misuse`
- subtlety: why a reviewer could miss it
- files and symbols involved
- precise claim of what appears wrong
- evidence
- confidence: low / medium / high

Write candidates to:

`$TMPDIR/bug-catcher/<sanitized-package-path>/candidates.md`

Bias toward a short, strong list.

Generate at most 12 candidates initially.
If more candidates are found, keep only the strongest ones by evidence, subtlety, and likely correctness impact.
A shorter, stronger list is better than a broad speculative list.

False positives are costly. Prefer omission over weak speculation.
Only keep a candidate if there is a concrete behavior chain from code to likely incorrect behavior.
If an issue depends on multiple assumptions, make those assumptions explicit and discard the candidate unless they are well-supported by source evidence.
Do not keep candidates based only on naming awkwardness, comment drift, or "this feels suspicious" reasoning without a specific semantic failure mode.

### Stage 3: confirm each candidate independently

For each candidate, start a fresh `bug-confirmer` subagent.

Pass it:

- package path
- understanding artifact
- candidate description
- relevant files / symbols

Each confirmer must independently determine whether the issue is real, trying first to disprove it.

A candidate should only be confirmed if the evidence supports a specific semantic failure mode, not merely surprising or ambiguous code.
When in doubt, mark it not-confirmed.

Each confirmation must return:

- verdict: confirmed / not-confirmed
- severity: critical / high / medium / low
- confidence: high / medium / low
- concise justification
- exact code references

Write one confirmation per file:

`.claude/tmp/bug-catcher/<sanitized-package-path>/confirmations/<NNN>.md`

### Stage 4: final report

Produce a final report containing **only confirmed bugs**, sorted by severity, then confidence.

When ordering confirmed bugs, rank by:
1. semantic correctness impact, especially corruption or persistent wrong behavior
2. breadth of affected call paths
3. likelihood in realistic usage
4. confidence

For each confirmed bug include:

- severity
- short title
- category
- package path
- affected files / symbols
- concise explanation
- why it could slip past review
- evidence

Do not include fix suggestions.
Do not include rejected candidates.
Do not dilute the report with general observations.

## Pebble / CRDB-specific hunting guidance

When auditing these kinds of codebases, pay extra attention to:

- iterator positioning, validity, lower/upper bounds, and exhaustion semantics
- ownership of byte slices, keys, values, buffers, and backing storage
- sequence-number visibility rules
- tombstone / range-key / masking / shadowing semantics
- comparer / split / separator / successor assumptions
- format-major-version or feature-gating assumptions
- state machines with implicit ordering requirements
- zero-value structs that are valid in some contexts but not others
- interaction between options structs and defaults
- cross-level or cross-batch assumptions
- internal iterators vs external iterator contract mismatches
- correctness assumptions encoded only in tests or comments
- call sites that accidentally rely on stronger guarantees than the package actually provides

## How to inspect caller misuse

Search for and inspect:

- imports of the package
- constructors
- exported funcs and methods
- interface implementations and adapters
- tests that use the package
- wrappers that narrow or reinterpret semantics

For each important API surface, ask:

- what contract does the package actually provide?
- what contract do callers appear to assume?
- where do those differ?

## Quality bar

The final output should read like a triaged bug docket for a senior Pebble/CRDB engineer:

- low noise
- evidence-backed
- focused on semantic defects
- conservative about confirmation
- ordered by severity
