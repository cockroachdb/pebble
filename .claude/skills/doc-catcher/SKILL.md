---
name: doc-catcher
description: Audit a Go package in this repository for incorrect or misleading code comments — places where the documentation contradicts what the code actually does. Optimized for Pebble- and CockroachDB-style code where comments encode contracts, invariants, and ownership rules that callers depend on.
disable-model-invocation: true
---

You are running a two-stage audit of a Go package, hunting for **doc defects**, not code
defects.

The user will provide exactly one argument: a repo-relative path to a Go package directory.

Your goal is to produce a final report containing only **clear, confirmed comment/code
mismatches**, ordered by severity, with no fix suggestions.

## Foundational stance: trust the code

This codebase has thorough testing. When code and comment disagree, **assume the code is
correct and the comment is the defect.** Do not pivot into "is this actually a code bug?"
analysis — that's `bug-catcher`'s job.

A doc defect is a defect in *what is documented*, not in what the code does.

## Primary objective

Find comments (godoc on exported symbols, function/method/type/field doc, inline comments
on non-trivial logic) where:

- the comment makes a specific factual claim about behavior, contract, invariant, return
  value, ordering, ownership, parameter meaning, or call requirement, AND
- the code clearly does something different, OR a careful reader would naturally
  misinterpret the comment as contradicting the code (inverted condition, wrong field
  name, wrong direction, stale parameter list, off-by-one in described range, wrong
  default, wrong nil/zero-value semantics, wrong error case, etc.).

## Non-goals (must not appear in the report)

- typos, grammar, formatting in comments
- missing documentation (this skill finds wrong docs, not absent docs)
- comments that are merely terse or could be more helpful
- stylistic disagreements with naming
- comments that are technically imprecise but where the natural reading still matches the
  code
- TODOs / FIXMEs (their staleness is a separate concern)
- comments clearly labeled as historical / deprecated context
- speculative "this comment might be confusing" without a concrete misreading
- code-level bugs (defer to `bug-catcher`)
- patch suggestions

## Required workflow

### Stage 1: build package understanding

Start the `package-understander` subagent on the target package path.

Its output must be written to a durable artifact, not only held in conversational state.

Write to:

`$TMPDIR/doc-catcher/<sanitized-package-path>/understanding.md`

This is the same agent used by `bug-catcher`; it extracts the actual contract from the
code, which is exactly what we need to compare comments against.

### Stage 2: audit comments and produce the final report

Start the `doc-auditor` subagent on the target package.

Pass it:

- package path
- understanding artifact path (`$TMPDIR/doc-catcher/<sanitized-package-path>/understanding.md`)
- output path: `$TMPDIR/doc-catcher/<sanitized-package-path>/report.md`

The auditor finds candidate comment/code mismatches, verifies each one against the source,
and writes the final ranked report directly. There is no separate confirmation stage —
the auditor is responsible for being conservative.

After the agent finishes, read the report file and present it to the user.

## Filter the auditor must apply

A comment is reported only if **all** of the following hold:

1. The comment makes a **specific, falsifiable claim** about code behavior or contract.
2. The code clearly does something different from what the comment claims, OR the comment
   is phrased in a way a careful reader would naturally misinterpret as contradicting the
   code.
3. The discrepancy has a plausible **behavioral consequence for a reader** — they would
   write incorrect calling code, debug in the wrong direction, or fail to preserve an
   invariant.
4. The auditor has read the cited code and confirmed the contradiction directly (not
   inferred from the understanding artifact alone).

When in doubt, drop it. A report of three high-quality items beats a report of twenty
mixed-quality items.

## Severity rubric

- **high**: comment documents a contract callers rely on, and following the comment leads
  to wrong calling code or violated invariants
- **medium**: comment describes internal behavior incorrectly in a way that misleads
  maintainers
- **low**: localized comment that is wrong but unlikely to mislead anyone reading nearby
  code

Anything that would only rate `low` with low confidence should be dropped, not reported.

## Pebble / CRDB-specific places to look hardest

Comments in these areas are especially load-bearing and especially prone to drift:

- iterator positioning, validity, bounds, exhaustion, and `SeekGE` / `SeekPrefixGE`
  semantics
- ownership / lifetime of returned slices, keys, values, buffers
- sequence-number visibility and snapshot semantics
- tombstone / range-key / masking / shadowing rules
- comparer / split / separator / successor contracts
- format-major-version gating
- zero-value validity of structs and options
- defaults applied by `Options.EnsureDefaults`-style helpers
- internal vs external iterator contract differences
- locking / reentrancy / callback rules
- "must be called with X held" or "caller must not Y" preconditions
- comments documenting the meaning of returned errors or sentinel values

## Quality bar

The final output should read like a triaged doc-fix list for a senior Pebble/CRDB
engineer:

- low noise
- evidence-backed (each entry cites file:line and quotes the comment)
- focused on contradiction, not stylistic disagreement
- conservative
- ordered by severity
