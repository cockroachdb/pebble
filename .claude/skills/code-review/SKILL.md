---
name: code-review
description: Review code, PRs, diffs, and changes in the Pebble codebase for correctness issues including resource leaks, concurrency bugs, iterator misuse, and lint violations. Use when asked to review code, a pull request, diff, or changes.
---

# Pebble Code Review

Use this skill when asked to review code, a PR, diff, or changes in the Pebble codebase.

## Priority: Critical Correctness Issues

These issues cause bugs, crashes, or data corruption. Flag immediately.

### 1. Resource Leaks

**`Get()` returns a closer that MUST be closed:**
```go
// WRONG - memory leak
val, _, err := db.Get(key)

// CORRECT
val, closer, err := db.Get(key)
if err != nil {
    return err
}
defer closer.Close()
```

**Iterators must be closed:**
This refers to database iterators or internal key-value iterators.
```go
iter, _ := db.NewIter(nil)
defer iter.Close()  // Required
```

### 2. Concurrency Bugs

**One iterator per goroutine:**
```go
// WRONG - race condition
iter := db.NewIter(nil)
go func() { iter.Next() }()  // Goroutine 1
iter.First()                  // Goroutine 2

// CORRECT - separate iterators
go func() {
    iter := db.NewIter(nil)
    defer iter.Close()
    // use exclusively
}()
```

**Lock release on panic:**
```go
// WRONG - deadlock if panic
mu.Lock()
doWork()
mu.Unlock()

// CORRECT
mu.Lock()
defer mu.Unlock()
doWork()
```

### 3. Iterator Misuse

This section refers to key-value iterators (including base.InternalIterator),
not to Go loop iterators.

**Must position before accessing Key/Value:**
```go
// WRONG - undefined behavior
iter := db.NewIter(nil)
key := iter.Key()  // Not positioned!

// CORRECT
iter := db.NewIter(nil)
if iter.First() {
    key := iter.Key()
}
```

**Must check Error() after iteration:**
```go
// WRONG - silent failures
for iter.First(); iter.Valid(); iter.Next() {
    process(iter.Key())
}
// What if I/O error occurred?

// CORRECT
for iter.First(); iter.Valid(); iter.Next() {
    process(iter.Key())
}
if err := iter.Error(); err != nil {
    return err
}
```

### 4. Double-Close Panics

Many types panic on double-close. Use `invariants.CloseChecker` for new types:
```go
type MyResource struct {
    closeCheck invariants.CloseChecker
}

func (r *MyResource) Close() error {
    r.closeCheck.Close()  // Panics on double-close in invariant builds
    return r.underlying.Close()
}
```

### 5. Bounds Checking

Use invariants package for safety:
```go
// WRONG in hot paths
if i >= len(slice) {
    panic("out of bounds")
}

// CORRECT - only panics in invariant builds
invariants.CheckBounds(i, len(slice))
```

## Priority: Required Patterns (Lint-Enforced)

These are caught by `go test -tags invariants ./internal/lint` but flag them in review.

### Error Handling
```go
// WRONG
return fmt.Errorf("failed: %v", err)

// CORRECT - preserves stack traces
return errors.Errorf("failed: %v", err)
return errors.Wrap(err, "context")
```

### Atomic Operations
```go
// WRONG - alignment issues, not type-safe
var count uint64
atomic.StoreUint64(&count, 10)

// CORRECT
var count atomic.Uint64
count.Store(10)
```

### Finalizers
```go
// WRONG
runtime.SetFinalizer(obj, cleanup)

// CORRECT - controlled, testable
invariants.SetFinalizer(obj, cleanup)
```

### OS Error Checks
```go
// WRONG
if os.IsNotExist(err) { ... }

// CORRECT
if oserror.IsNotExist(err) { ... }
```

### Forbidden Imports
- `errors` → use `github.com/cockroachdb/errors`
- `pkg/errors` → use `github.com/cockroachdb/errors`
- `golang.org/x/exp/slices` → use `slices` (builtin)
- `golang.org/x/exp/rand` → use `math/rand/v2`

## Priority: Corruption Prevention

### Corruption Errors
Mark errors appropriately so they can be detected:
```go
// For data corruption
return base.CorruptionErrorf("invalid key: %s", key)

// To check
if base.IsCorruptionError(err) {
    // Handle corruption
}
```

### Invariant Checks
Use invariants for assertions that should hold:
```go
if invariants.Enabled {
    if unexpectedCondition {
        panic("invariant violated: ...")
    }
}
```

## CockroachDB Conventions

### Error Wrapping
Always add context when wrapping:
```go
// WRONG
return err

// CORRECT
return errors.Wrap(err, "loading manifest")
return errors.Wrapf(err, "reading block %d", blockNum)
```

### Redact Safety
Use `redact.SafeFormat` for types that may contain user data:
```go
func (k Key) SafeFormat(w redact.SafePrinter, _ rune) {
    w.Print(redact.SafeString(k.String()))
}
```

### TODO Attribution
Always attribute TODOs:
```go
// TODO(username): description of future work
```

## Review Checklist

When reviewing, verify in order:

1. **Resource Management**
   - [ ] All `Get()` callers close the returned closer
   - [ ] All iterators are closed
   - [ ] No double-close potential

2. **Concurrency**
   - [ ] Iterators not shared across goroutines
   - [ ] Locks released via defer
   - [ ] Atomic types used (not raw atomic ops)

3. **Error Handling**
   - [ ] Errors not silently ignored
   - [ ] `iter.Error()` checked after loops
   - [ ] Using `errors.Errorf` not `fmt.Errorf`

4. **Safety**
   - [ ] No potential nil dereferences
   - [ ] Bounds checked where needed
   - [ ] Corruption marked appropriately

5. **Lint Compliance**
   - [ ] No forbidden imports
   - [ ] `oserror` not `os.Is*`
   - [ ] `invariants.SetFinalizer` not `runtime.SetFinalizer`

## Commit Message Format

When reviewing commit messages:
- Subject: `package: short description` (lowercase after colon)
- Body: Explain "what existed before" and "why"
- No test plan unless explicitly requested

Good examples:
```
bloom: improve simulation output
db: add progressive bloom filter policy
internal/manifest: rename LevelFile to LevelTable
```
