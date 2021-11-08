- Feature Name: Range Keys
- Status: draft
- Start Date: 2021-10-18
- Authors: Sumeer Bhola, Jackson Owens
- RFC PR: #1341
- Pebble Issues:
  https://github.com/cockroachdb/pebble/issues/1339
- Cockroach Issues:
  https://github.com/cockroachdb/cockroach/issues/70429
  https://github.com/cockroachdb/cockroach/issues/70412

** Design Draft**

TODO:
- Expand on masking implementation.
- Expand the sstable boundaries discussion.

# Summary

An ongoing effort within CockroachDB to preserve MVCC history across all SQL
operations (see cockroachdb/cockroach#69380) requires a more efficient method of
deleting ranges of MVCC history.

This document describes an extension to Pebble introducing first-class support
for range keys. Range keys map a range of keyspace to a value.  Optionally, the
key range may include an suffix encoding a version (eg, MVCC timestamp). Pebble
iterators may be configured to surface range keys during iteration, or to mask
point keys at lower MVCC timestamps covered by range keys.

CockroachDB will make use of these range keys to enable history-preserving
removal of contiguous ranges of MVCC keys with constant writes, and efficient
iteration past deleted versions.

# Background

Pebble currently has only one kind of key that is associated with a range:
`RANGEDEL [k1, k2)#seq`, where [k1, k2) is supplied by the caller, and is used
to efficiently remove a set of point keys.

A previous CockroachDB RFC cockroach/cockroachdb#69380 describes the motivation
for the larger project of migrating MVCC-noncompliant operations into MVCC
compliance. Implemented with the existing MVCC primitives, some operations like
removal of an index or table would require performing writes linearly
proportional to the size of the table. Dropping a large table using existing
MVCC point-delete primitives would be prohibitively expensive. The desire for a
sublinear delete of an MVCC range motivates this work.

The detailed design for MVCC compliant bulk operations ([high-level
description](https://github.com/cockroachdb/cockroach/blob/master/docs/RFCS/20210825_mvcc_bulk_ops.md);
detailed design draft for DeleteRange in internal
[doc](https://docs.google.com/document/d/1ItxpitNwuaEnwv95RJORLCGuOczuS2y_GoM2ckJCnFs/edit#heading=h.x6oktstoeb9t)),
ran into complexity by placing range operations above the Pebble layer, such
that Pebble sees these as points. The complexity causes are various: (a) which
key (start or end) to anchor this range on, when represented as a point (there
are performance consequences), (b) rewriting on CockroachDB range splits (and
concerns about rewrite volume), (c) fragmentation on writes and complexity
thereof (and performance concerns for reads when not fragmenting), (d) inability
to efficiently skip older MVCC versions that are masked by a `[k1,k2)@ts` (where
ts is the MVCC timestamp).

First-class support for range keys in Pebble would eliminate all these issues.
Additionally, it could allow for future extensions like efficient transactional
range operations. This issue describes how this feature would work from the
perspective of a user of Pebble (like CockroachDB), and sketches some
implementation details.

# Design

## Interface

### New `Comparer` requirements

User-provided `Comparer`s must adhere to stricter requirements
surrounding the `Split` function and the ordering of keys. The details
of why these new requirements are necessary are explained in the
implementation section.

The Split function is already documented as producing a suffix
representing an MVCC version, without specifying what constitutes an
MVCC version and how it is encoded.

1. The user key consisting of just a key prefix `k` must sort before all
   other user keys containing that prefix. Specifically
   `Compare(k[:Split(k)], k) < 0` where `Split(k) < len(k)`.
2. The empty key prefix must be a valid key and comparable. The ordering
   of the empty key prefix with any suffixes must be consistent with the
   ordering of those same suffixes applied to any other key prefix.
   Specifically `Compare(k[Split(k):], k2[Split(k2):]) == Compare(k,
   k2)` where `Compare(k[:Split(k)], k2[:Split(k2)]) == 0`.

### Writes

This design splits introduces a new separate Range API with three write
operations:

- `RangeSet([k1, k2), [optional suffix], <value>)`: This represents the
  mapping `[k1, k2)@suffix => value`. Keys `k1` and `k2` must not
  contain a suffix (i.e., `Split(k1)==len(k1)` and
  `Split(k2)==len(k2))`.

- `RangeUnset([k1, k2), [optional suffix])`: This removes a mapping
  previously applied by `RangeSet`. The unset may use a smaller key
  range than the original `RangeSet`, in which case part of the range
  is deleted. The unset only applies to range keys with a matching
  optional suffix. If the optional suffix is absent in both the RangeSet
  and RangeUnset, they are considered matching.

- `RangeDelete([k1, k2))`: This removes all range keys within the
  provided key span. It behaves like an `Unset` unencumbered by suffix
  restrictions.

For example, consider `RangeSet([a,d), foo)` (i.e., no suffix). If
there is a later call `RangeUnset([b,c))`, the resulting state seen by
a reader is `[a,b) => foo`, `[c,d) => foo`. Note that the value is not
modified when the key is fragmented.

Partially overlapping `RangeSet`s with the same suffix overwrite one
another.  For example, consider `RangeSet([a,d), foo)`, followed by
`RangeSet([c,e), bar)`.  The resulting state is `[a,c) => foo`, `[c,e)
=> bar`.

Point keys and range keys do not overwrite one another. They have a
parallel existence. Point deletes only apply to points. Range unsets
only apply to range keys. However, users may configure iterators to mask
point keys covered by newer range keys. This masking behavior is
explicitly requested by the user in the context of the iteration and
does not apply to internal iterators used for compaction writes. Masking
is described in more detail below.

There exist separate range delete operations for point keys and range
keys. A `RangeDelete` issued through the range API can remove part of a
range key, just like the new `RangeUnset` operation introduced earlier.
Range deletes differ from `RangeUnset`s, because the latter requires
that the suffix matches and applies only to range keys.

The optional suffix is related to the pebble `Comparer.Split` operation
which is explicitly documented as being for [MVCC
keys](https://github.com/cockroachdb/pebble/blob/e95e73745ce8a85d605ef311d29a6574db8ed3bf/internal/base/comparer.go#L69-L88),
without mandating exactly how the versions are represented. `RangeSet`
and `RangeUnset` keys with different suffixes do not interact logically,
although Pebble may observably fragment ranges at any user key,
including at range keys intersection points.

The introduction of a second `RangeDelete` that applies to range keys
only is confusing. To clarify the interface, writing interfaces may
be refactored:

```
Delete(key []byte, _ *WriteOptions) error
DeleteDeferred(keyLen int) *DeferredBatchOp
Merge(key, value []byte, _ *WriteOptions) error
MergeDeferred(keyLen, valueLen int) *DeferredBatchOp
Set(key, value []byte, _ *WriteOptions) error
SetDeferred(keyLen, valueLen int) *DeferredBatchOp
SingleDelete(key []byte, _ *WriteOptions) error
SingleDeleteDeferred(keyLen int) *DeferredBatchOp

Range(start, end []byte) RangeOpWriter

type RangeOpWriter interface {
    Set(suffix, value []byte, _ *WriteOptions) error
    Unset(suffix []byte, _ *WriteOptions) error
    DeletePoints(_ *WriteOptions) error
    DeleteRanges(_ *WriteOptions) error
}
```

### Iteration

A user iterating over a key interval [k1,k2) can request:

- **[I1]** An iterator over only point keys.

- **[I2]** A combined iterator over point and range keys. This is what
  we mainly discuss below in the implementation discussion.

- **[I3]** An iterator over only range keys. In the CockroachDB use
    case, range keys will need to be subject to MVCC GC just like
    point keys — this iterator may be useful for that purpose.

The `pebble.Iterator` type will be extended to provide accessors for
range keys for use in the combined and exclusively range iteration
modes.

```
HasPointAndRange() (hasPoint, hasRange bool)
Key() []byte

Value() []byte

RangeBounds() (start, end []byte)
RangeKeys() []RangeKey

type RangeKey struct {
    Suffix []byte
    Value  []byte
}
```

When a combined iterator exposes range keys, it exposes all the range
keys covering `Key`.  During iteration with a combined iterator, an
iteration position may surface just a point key, just a range key or
both at the currently-positioned `Key`.

Described another way, a Pebble combined iterator guarantees that it
will stop at all positions within the keyspace where:
1. There exists a point key at that position.
2. There exists a range key that logically begins at that postition.

In addition to the above positions, a Pebble iterator may also stop at
keys in-between the above positions due to internal fragmentation. The
range keys surfaced through iteration may be fragmented.

Range keys are physically fragmented as an artifact of the
log-structured merge tree structure and internal sstable boundaries.
Range key fragments surfaced through a user-facing iterator may be split
at any user key boundary within the original range's user key range.
This allows for range keys with boundary keys that have suffixes that
sort above or below the range key's suffix. For example a
`RangeSet([a,c), @50, <value>)` may be surfaced as the range key
fragments `RangeSet([a, b@30), @50, <value>)` and `RangeSet([b@30,c),
@50, <value>)`. The exposed fragments may also be finer than the
physical fragments. This is considered acceptable since:

1. the `RangeUnset` semantics are over the logical range and not a
   physical key,
2. our current use cases don't need to know when all the fragments for
   an original `RangeSet` have been seen,
3. users that want to know when all the fragments have been seen can
   store the original _k2_ in the `<value>` and iterate until they are
   past that _k2_.

This finer fragmentation is necessary during iteration to ensure that
range keys are truncated appropriately by unsets. Consider this example:

```
                   iterator pos          [ ] - sstable bounds
                         |
L1:         [a----v1@t2--|-h]     [l-----unset@t1----u]
L2:                 [e---|------v1@t1----------r]
             a b c d e f g h i j k l m n o p q r s t u v w x y z
```

If the iterator is at point `g`, there are two overlapping range keys:
`[a,h)@t2→v1` and `[e,r)@t1→v1`. The range key `[e,r)@t1→v1` isn't valid
for its entire key span: It's unset within `[l,r)`. The iterator can't
tell without looking ahead in the next sstable. So instead, the iterator
fragments to the nearest fragment bound (in this case `h`), Because
range key fragments are truncated to sstable bounds, this also
guarantees fragments won't extend beyond the sstables open under the
current iterator position.

#### Iteration order

Recall that the user-provided `Comparer.Split(k)` function divides all
user keys into a prefix and a suffix, such that the prefix is
`k[:Split(k)]`, and the suffix is `k[Split(k):]`. If a key does not
contain a suffix, the user key equals the prefix.

An iterator that is configured to surface range keys alongside point
keys will surface all range keys covering the current `Key()` position.
```
  Point keys: a, b, c, d, e, f
  Range key: [a,e)
```

A pebble.Iterator, which shows user keys, will output (during forward
iteration), the following keys:

```
  (a,[a,e)), (b,[a,e)), (c,[a,e)), (d,[a,e)), e
```

The notation `(b,[a,e))` indicates both these keys and their
corresponding values are visible at the same time when the iterator is
at that position.

- There can be multiple range keys covering a `Key()`, each with a
  different suffix.

- There cannot be multiple range keys covering a `Key()` with the same
  suffix, since the one with the highest sequence number will win, just
  like for point keys.

- If the iterator has a configured upper bound, it will truncate the
  range key to that upper bound. e.g. if the upper bound was c, the
  sequence seen would be `(a,[a,c))`, `(b,[a,c))`. The same applies to
  lower bound and backward iteration.

In the above example, the range key `[a,e)` had no suffix. Remember
these range keys may also specify a suffix: `RangeSet([k1,k2), @suffix,
<value>)` and point keys optionally have suffixes.

Consider the following state in terms of user keys (the `@number` part
is the version or "suffix"):

```
point keys: a@100, a@30, b@100, b@40, b@30, c@40, d@40, e@30, f@20
range key: [a,e)@50
```

If we consider the user key ordering across the union of these keys,
where we have used ↦ and ↤ to mark the start and end keys for the
range key, we get:

```
[a                                         e#inf)
↦                                               ↤
 ·      ·     ·     ·      ·     ·     ·     ·     ·     ·     ·
a@100, a@50, a@30, b@100, b@40, b@30, c@40, d@40, e@50, e@30, f@20
```

A `pebble.Iterator`, which shows user keys, will output (during forward
iteration), the following keys:

```
(point,    range)
(       [a,e)@50)
(a@100, [a,e)@50)
(a@50,  [a,e)@50)
(a@30,  [a,e)@50)
(b@100, [a,e)@50)
(b@40,  [a,e)@50)
       ⋮
(d@40,  [a,e)@50)
(e@50,          )
(e@30,          )
(f@20,          )
```

- Like the non-MVCC case, the end key for the range is not being
  fragmented using the succeeding point key.

#### Masking

When constructing an iterator, a user may request that range keys mask
point keys.  Masking takes a suffix that configures which range keys may
mask point keys. Only range keys with suffixes that sort after the
mask's suffix mask point keys. A range key that meets this condition
only masks points with suffixes that sort after the range key's suffix.

```
type IterOptions struct {
    // ...
    Mask IteratorMask
}

// RangeKeyMask may be used as an iterator mask to mask all point
// keys covered by range keys with suffixes less than suffix.
//
// Specifically, if the iterator encounters a range key for which
//
//   Compare(RangeKeySuffix(), maskSuffix) <= 0
//
// the iterator will skip over all point keys k contained within the
// range key's span such that
//
//   Compare(k[Split(k):], RangeKeySuffix()) < 0
//
func RangeKeyMask(
    maskSuffix   []byte,
    newFilter    func(lessThanSuffix []byte) BlockPropertyFilter,
    adjustFilter func(lessThanSuffix, filter BlockPropertyFilter),
) IteratorMask {
    // ...
}
```

Example: A user may construct an iterator with a mask `RangeKeyMask(@t50, ...)`.
A range key `[a, c)@t60` masks nothing. A range key `[a,c)@30` masks `a@20` and
`apple@10` but not `apple@40`.

A Pebble iterator with a mask may still be opened in combined point-and-range
key iteration mode. In this case, any range keys with suffixes ≤ `maskSuffix`
will be hidden form the iterator. Any point keys with suffixes contained within
the bounds of range keys with suffixes ≤ `maskSuffix` and with suffixes less
than one of these range keys' suffixes will also be hidden from the iterator.

Range keys with suffixes greater than the `maskSuffix` will be surfaced through
the iterator.

## Implementation

### Write operations

This design introduces two new Pebble write operations: `RangeSet` and
`RangeUnset`. Internally, these operations are represented as keys with
`RANGESET` and `RANGEUNSET` key kinds. These keys are stored within 'range key'
blocks separate from point keys. The 'range key' blocks hold both
`RANGESET` and `RANGEUNSET` keys, but are separate from the blocks holding
ordinary point keys. Within the memtables, these keys are stored in a separate
skip list.

- `RangeSet([k1,k2), @suffix, value)` is encoded as a `k1.RANGESET` key
  with a value encoding the tuple `(k2,@suffix,value)`.
- `RangeUnset([k1,k2), @suffix)` is encoded as a `k1.RANGEUNSET` key
  with a value encoding the tuple `(k2,@suffix)`.

Although the public interface `RangeSet` and `RangeUnset` operations
require both boundary keys `[k1,k2)` to not have a suffix, internally
these keys may be fragmented to bounds containing suffixes.

Example: If a user attempts to write `RangeSet([a@v1, c@v2), @v3,
value)`, Pebble will return an error to the user. If a user writes
`RangeSet([a, c), @v3, value)`, Pebble will allow the write and may
later fragment the `RangeSet` into:
 - `RangeSet([a, a@v1), @v3, value)`
 - `RangeSet([a@v1, c@v2), @v3, value)`
 - `RangeSet([c@v2, c), @v3, value)`

`RangeSet` and `RangeUnset` keys are assigned sequence numbers, like
other existing internal keys. These keys are written to the same sstable
as point keys and stored in separate block(s) like range deletion
tombstones. The LSM level invariants are valid across range and point
keys (just like they are for RANGEDEL + point keys). That is,
`RangeSet([k1,k2))#s2` cannot be at a lower level than `RangeSet(k)#s1`
where `k \in [k1,k2)` and `s1 < s2`.

Like point and range deletion tombstones, the `RangeUnset` key can be
elided when it falls to L6 and there is no snapshot preventing its
elision. So there is no additional garbage collection problem introduced
by these keys.

Pebble internally is free to fragment range keys even if the user did
not unset any portion of the key. This fragmentation is essential for
preserving the performance characteristics of a log-structured merge
tree. Fragmentation may occur at user key contained within the `[k1,k2)`
range, including at intermediary keys that contain suffixes higher or
lower than the suffix of the range key.

There is no Merge operation that affects range keys.

These range keys may be encoded within the same sstables as points in
separate blocks, or in separate sstables forming a parallel range-key
LSM:

- Storing range keys in separate sstables is possible because the only
  iteractions between range keys and point keys happens at a global
  level. Masking is defined over suffixes. It may be extended to be
  defiend over sequence numbers too (see 'Sequence numbers' section
  below), but that is optional. Unlike `RANGEDELs`, range keys have no
  effect on point keys during compactions.

- With shared sstables, reads must iterate through all the range key
  blocks of every overlapping sstable that contains range keys. Range
  keys are expected to be rare, so this is expected to be okay. If many
  range keys are accumulated within L6, a read may need to iterate
  through them all because there is no index. With separate sstables,
  reads may need to open additional sstable(s) and read additional
  blocks. The number of additional sstables is the number of nonempty
  levels in the range-key LSM, so it grows logarithmically with the
  number of range keys. For each sstable, a read must read the index
  block and a data block.

- With our expectation of few range keys, the range-key LSM is expected
  to be small, with one or two levels. Heuristics around sstable
  boundaries may prevent unnecessary range-key reads when there is no
  covering range key. Range key sstables and blocks are expected to have
  much higher hit rates, since they are orders of magnitude less dense.
  Reads in any overlapping point sstables all access the same range key
  sstables.

- With shared sstables, `SeekPrefixGE` cannot use bloom filters to
  eliminate sstables that contain range keys. Pebble does not use bloom
  filters in L6, so once a range key is compacted into L6 its impact to
  `SeekPrefixGE` is lessened. With separate sstables, `SeekPrefixGE` can
  always use bloom filters for point-key sstables.  If there are any
  overlapping range-key sstables, the read must read them.

- With shared sstables, range keys create dense sstable boundaries. A
  range key spanning an sstable boundary leaves no gap between the
  sstables' bounds. This can force ingested sstables into higher levels
  of the LSM, even if the sstables' point key spans don't overlap. This
  problem was previously observed with wide `RANGEDEL` tombstones and
  was mitigated by prioritizing compaction of sstables that contain
  `RANGEDEL` keys. We could do the same with range keys, but the write
  amplification is expected to be much worse. The `RANGEDEL` tombstones
  drop keys and eventually are dropped themselves as long as there is
  not an open snapshot. Range keys do not drop data and are expected to
  persist in L6 for long durations, always requiring ingests to target
  at least L5.

- With separate sstables, compaction logic is separate, which helps
  avoid complexity of tricky sstable boundary conditions. Because there
  are expected to be order of magnitude fewer range keys, we could
  impose the constraint that a prefix cannot be split across multiple
  range key sstables. The simplified compaction logic comes at the cost
  of higher levels, iterators, etc all needing to deal with the concept
  of two parallel LSMs.

#### Physical representation

`RANGESET` and `RANGEUNSET` keys are keyed by their start key. This
poses an obstacle. Consider the two range keys:
* `RangeSet([k1,k2), @t1, v1)#s2`
* `RangeSet([k1,k2), @t2, v2)#s1`

Since the keys are physically keyed by just the start key, these keys
are represented by the key→value pairs:
* `k1.RANGESET#s2` → `(k2,@t1,v1)`
* `k1.RANGESET#s1` → `(k2,@t2,v2)`

Since these two keys are equal in user key, they're ordered by sequence
number: `k1.RANGESET#s2` followed by `k1.RANGESET#s1`. The keys are not
ordered by their suffixes `@t1` and `@t2` (the order they're surfaced
during iteration). An iterator would need to buffer all of overlapping
fragments in-memory and sort them. Additionally, we must be able to
support multiple range keys at the same sequence number, because all
keys within an ingested sstable adopt the same sequence number. To
resolve this issue, fragments with the same bounds are merged within
snapshot stripes into a single physical key-value, representing multiple
logical key-value pairs:

```
k1.RANGESET#s2 → (k2,[(@t2,v2),(@t1,v1)])
```

Within a physical key-value pair, suffix-value pairs are stored sorted
by suffix, descending. This has a minor additional advantage of reducing
iteration-time user-key comparisons when there exist multiple range keys
in a table.

Unlike other Pebble keys, the `RANGESET` and `RANGEUNSET` keys have
values that encode fields of data known to Pebble. The value that the
user sets in a call to `RangeSet` is opaque to Pebble, but the physical
representation of the `RANGESET`'s value is known. This encoding is a
sequence of fields:

* End key, `varstring`, encodes the end user key of the fragment.
* A varint count of the number of logical range keys encoded.
* A series of (suffix, value-length) tuples representing the logical
  range keys that were merged into this one physical `RANGESET` key:
  * Suffix, `varstring`
  * Value length, `varint`, the length of the logical range key's value.
    The value itself is encoded at the end of the `RANGESET`'s value.
* The values in opposite order as the above suffix tuples and without any
  delimiters. A reader calculates the value offset by summing value lengths,
  and indexes from the end of the `RANGESET` value. Storing the values in
  opposite order allows readers to incrementally decode suffix/value pairs
  while iterating forward. Storing values separate from suffixes improves
  cpu cache locality while searching among suffixes for the appropriate
  range key.

Similarly, `RANGEUNSET` keys are merged within snapshot stripes and
have a physical representation like:

```
k1.RANGEUNSET#s2 → (k2,[(@t2),(@t1)])
```

A `RANGEUNSET` key's value is encoded as:
* End key, `varstring`, encodes the end user key of the fragment.
* A series of suffix `varstring`s.

When `RANGESET` and `RANGEUNSET` fragments with identical bounds meet
within the same snapshot stripe within a compaction, any of the
`RANGEUNSET`'s suffixes that exist within the `RANGESET` key are
removed.

NB: `RANGESET` and `RANGEUNSET` keys are not merged within batches or
the memtable. That's okay, because batches are append-only and indexed
batches will refragment and merge the range keys on-demand. In the
memtable, every key is guaranteed to have a unique sequence number.

### Sequence numbers

Like all Pebble keys, `RANGESET` and `RANGEUNSET` are assigned sequence
numbers when committed. As described above, overlapping `RANGESET`s and
`RANGEUNSET`s within the same snapshot stripe and sstable are merged
into a single internal key-value pair. The original unmerged internal
keys each have their own sequence number, indicating the moment they
were committed within the history of all write operations.

Remember, sequence numbers are used within Pebble to determine which
keys appear live to which iterators. When an iterator is constructed, it
takes note of the current _visible sequence number_, and for the
lifetime of the iterator, only surfaces keys less than that sequence
number. Similarly, snapshots read the current _visible sequence number_,
remember it, but also leave a note asking compactions to preserve
history at that sequence number. The space between snapshotted sequence
numbers is referred to as a _snapshot stripe_, and operations cannot
drop or otherwise mutate keys unless they fall within the same _snapshot
stripe_. For example a `k.MERGE#5` key may not be merged with a
`k.MERGE#1` operation if there's an open snapshot at `#3`.

The new `RANGESET` and `RANGEUNSET` keys behave similarly. Overlapping
range keys won't be merged if there's an open snapshot separating them.
Consider a range key `a-z` written at sequence number `#1` and a point
key `d.SET#2`. A combined point-and-range iterator using a sequence
number `#3` and positioned at `d` will surface both the range key `a-z`
and the point key `d`.

In the context of masking, the suffix-based masking of range keys can cause
potentially unexpected behavior. A range key `[a,z)@t10` may be
committed as sequence number `#1`. Afterwards, a point key `d@t5#2` may
be committed. An iterator that is configured with range-key masking with
suffix `@t20` would mask the point key `d@t5#2` because although
`d@t5#2`'s sequence number is higher, range-key masking uses suffixes to
impose order, not sequence numbers.

In the CockroachDB MVCCDeleteRange use case, a point key should never be
written below an existing range key with a higher timestamp. The
MVCCDeleteRange use case would allow us to _also_ dictate that an
overlapping range key with a higher sequence number always masks range
keys with lower sequence numbers. Adding this additional masking scope
would avoid the comparatively costly suffix comparison when a point key
_is_ masked by a range key. We need to consider how sequence number
masking might be affected by the merging of range keys within snapshot
stripes.

Consider the committing of range key `[a,z)@{t1}#10`, followed by point
keys `d@t2#11` and `m@t2#11`, followed by range key `[j,z)@{t3}#12`.
This sequencing respects the expected timestamp, sequence number
relationship in CockroachDB's use case. If all keys are flushed within
the same sstable, fragmentation and merging overlapping fragments yields
range keys `[a,j)@{t1}#10`, `[j,z)@{t3,t1}#12`. The key `d@t2#11` must
not be masked because it's not covered by the new range key, and indeed
that's the case because the covering range key's fragment is unchanged
`[a,j)@{t1}#10`.

For now we defer the judgment on adding sequence number masking until
we're absolutely sure CockroachDB has no requirement of 'backfilling'
history beneath a MVCCDeleteRange tombstone. Note that this may always
be layered in after the fact as a performance optimization to avoid
suffix comparisons.

TODO: Make a decision.

### Boundaries for sstables

Range keys will follow the same relationship to sstable bounadries as
`RANGEDEL` tombstones. The bounds of an internal range key are user
keys. Every range key is limited by its containing sstable's bounds.

Consider these keys, annotated with sequence numbers:

```
Point keys: a#50, b#70, b#49, b#48, c#47, d#46, e#45, f#44
Range key: [a,e)#60
```

We have created three versions of `b` in this example. Pebble currently
can split output sstables during a compaction such that the different
`b` versons span more than one sstable. This creates problems for
`RANGEDEL`s which span these two sstables which are discussed in the
section on [improperly truncated
RANGEDELS](https://github.com/cockroachdb/pebble/blob/master/docs/range_deletions.md#improperly-truncated-range-deletes).
We manage to tolerate this for RANGEDELs since their semantics are
defined by the system, which is not true for these range keys where
the actual semantics are up to the user.

We will disallow such sstable split points if they can span a range
key. In this example, by postponing the sstable split point to the
user key c, we can cleanly split the range key into `[a,c)#60` and
`[c,e)#60`. The sstable end bound for the first sstable (sstable
bounds are inclusive) will be c#inf (where inf is the largest possible
seqnum, which is unused except for these cases), and sstable start
bound for the second sstable will be c#60.

It is possible we will choose to not let the same key span sstables
broadly, regardless of whether or not a range key spans the sstable
boundary, since it simplifies some existing code in Pebble too.

The above example deals exclusively with point and range keys without
suffixes. Consider this example with suffixed keys, and compaction
outputs split in the middle of the `b` prefix:

```
first sstable: points: a@100, a@30, b@100, b@40 ranges: [a,c)@50
second sstable: points: b@30, c@40, d@40, e@30, ranges: [c,e)@50
```

When the compaction code decides to defer `b@30` to the next sstable and
finish the first sstable, the range key `[a,c)@50` is sitting in the
fragmenter. The compaction must split the range key at the bounds
determined by the user key. The compaction uses the first point key of
the next sstable, in this case `b@30`, to truncate the range key. The
compaction flushes the fragment `[a,b@30)@50` to the first sstable and
updates the existing fragment to begin at `b@30`.

Note that the above truncate-and-flush behavior is the same as Pebble's
current treatment of Pebble range tombstones during flushes to L0. It is
a divergence from Pebble's existing _compaction_ range tombstone
behavior that flushes without truncation and allows a Pebble user key to
span multiple sstables. We choose to flush-and-truncate both range keys
and range tombstones from now on and prohibit user keys spanning
mulitple sstables. CockroachDB rarely uses snapshots and rarely writes
the same user key multiple times, so the requirement that the same user
key be output to the same sstable should not pose a problem for
CockroachDB. The flush-and-truncation behavior is conceptually simpler
and allows us to simplify compaction and eventually compaction-picking
logic.

If a range key extends into the next file, the range key's truncated end
is is used for the purposes of determining the sstable end boundary. The
first sstable's end boundary becomes `b@30#inf`, signifying the range
key does not cover `b@30`. The second sstable's start boundary is
`b@30`.

[TODO(jackson): This explanation needs work.]

### Block property collectors

Block property collectors will be fed range keys, just like point keys.
This is necessary for CockroachDB's MVCC block property collectors to
ensure the sstable-level properties are correct.

### Iteration

This design extends the `*pebble.Iterator` with the ability to iterate
over exclusively range keys, range keys and point keys together or
exclusively point keys (the current behavior).

- Pebble already requires that the prefix `k` follows the same key
  validity rules as `k@suffix`.

- Previously, Pebble did not require that a user key consisting of just
  a prefix `k` sort before the same prefix with a non-empty suffix.
  CockroachDB has adopted this behavior since it results in the
  following clean behavior: `RANGEDEL` over [k1, k2) deletes all
  versioned keys which have prefixes in the interval [k1, k2). Pebble
  will now require this behavior for anyone using MVCC keys.
  Specifically, it must hold that `Compare(k[:Split(k)], k) < 0` if
  `Split(k) < len(k)`.

When an iterator is configured to expose range keys, Pebble's internal
level iterators will each maintain a pointer into the current open
sstable's range key block. Because logical `RangeSet`s are merged, in
the absence of snapshots, at most one physical `RANGESET` key is
relevant at any given position. Each level iterator parses the
`RANGESET` suffix entries, maintaining a slice of (suffix, offset)
tuples and a current index among them.

The details of masking during iteration are discussed below.

#### Determinism

Range keys will be split based on boundaries of sstables in an LSM. We
typically expect that two different LSMs with different sstable
settings that receive the same writes should output the same key-value
pairs when iterating. To provide this behavior, the iterator
implementation could defragment range keys during iteration time. The
defragmentation behavior would be:

- Two visible ranges `[k1,k2)@suffix1=>val1`, `[k2,k3)@suffix2=>val2`
  are defragmented if suffix1==suffix2 and val1==val2, and become
  [k1,k3).

- Defragmentation does not consider the sequence number. This is
  necessary since LSM state can be exported to another LSM via the use
  of sstable ingestion, which can collapse different seqnums to the
  same seqnum. We would like both LSMs to look identical to the user
  when iterating.

The above defragmentation is conceptually simple, but hard to
implement efficiently, since it requires stepping ahead from the
current position to defragment range keys. This stepping ahead could
swich sstables while there are still points to be consumed in a
previous sstable. We note the application correctness cannot be
relying on determinism of fragments, and that this determinism is
useful only for testing and verification purposes:

- Randomized tests that encounter non-determinism in fragmentation can
  buffer all the fragments in-memory, defragment them, and then
  compare these defragmented range keys for equality testing. This
  behavior would need to be applied to Pebble's metamorphic test, for
  which a single Next/Prev call would turn into as many calls are
  needed until the next point is encountered. All the buffered
  fragments from these many calls would be defragmented and compared
  for equality.

- CockroachDB's replica divergence detector can iterate over only the
  range keys using iterator I3, defragment in a streaming manner,
  since it only requires keeping a start/end key pair in-memory, and
  compute the fingerprint after defragmenting. When we consider MVCC
  keys, we will see that the defragmentation may actually need to keep
  multiple start/end key pairs in-memory since there may be different
  versions over overlapping key spans -- we do not expect that the
  number of versions of range keys that overlap will be significant
  enough for this defragmentation to suffer from high memory usage.

In short, we will not guarantee determinism of output.

TODO: Think more about whether there is an efficient way to offer
determinism.

#### Efficient masking

Recollect that in the example from the iteration interface, during
forward iteration the `pebble.Iterator` would output the following keys:

```
a@100, [a,e)@50, a@30, b@100, [b,e)@50, b@40, b@30, [c,e)@50, c@40, [d,e)@50, d@40, e@30
```

It would be desirable if the caller could indicate a desire to skip over
a@30, b@40, b@30, c@40, d@40, and this could be done efficiently. We
will support iteration in this MVCC-masked mode, when specified as an
iterator option. This iterator option requires a `suffix` such that the
caller wants to allow all range keys with suffixes `< suffix` to mask.

To efficiently implement this we cannot rely on the LSM invariant since
`b@100` can be at a lower level than `[a,e)@50`. However, we do have (a)
per-block key bounds, and (b) for time-bound iteration purposes we will
be maintaining block properties for the timestamp range (in
CockroachDB). We will require that for efficient masking, the user
provide the name of the block property to utilize. In this example, when
the iterator will encounter `[a,e)@100` it can skip blocks whose keys
are guaranteed to be in [a,e) and whose timestamp interval is < 100.

TODO(jackson): Expand and rephrase this.

### CockroachDB implications

CockroachDB will use range keys to represent MVCC delete ranges. An
MVCC Delete Range specifies a range of user keys and a MVCC timestamp.

```
MVCCDeleteRange(start, end []roachpb.Key, timestamp hlc.Timestamp, /* ... */)
```

This operation will translate into a Pebble-level `RangeSet` operation,
with the same key bounds, the encoded timestamp as the suffix and a
marshalled `enginepb.MVCCRangeTombstone` protocol buffer as the value.
The value will re-encode the start and end key bounds. This may appear
to be redundant, but it's useful for defragmenting keys to construct the
current bounds of a MVCCRangeTombstone. They may change overtime, for
example if a subregion of the tombstone is garbage collected.

```
RangeSet(start, end, encode(timestamp), encode(enginepb.MVCCRangeTombstone{
    Start:     start,
    End:       end,
}))
```

TODO: Is there anything else to encode within the value?

The CockroachDB `pkg/storage` package will provide higher-level
utilities like the existing MVCC utlities, and implement heuristics for
deciding when to write point tombstones and when to write a
`MVCCRangeTombstone`.

#### Long-term MVCC Iterator API

Within CockroachDB's `pkg/storage` package and within the rest of
CockroachDB, we use a `MVCCIterator` interface for iterating over
keyspaces containing MVCC-versioned data. This iterator interface is an
iterator over key-value pairs where a key-value pair may be a point key,
a MVCC point tombstone or an intent. Clients use knowledge of the
represenation of each and context to determine what type of key-value
pair they're reading. This interface at one time reflected the physical
reality where point keys, MVCC point tombstones and intents were all
interleaved within the same keyspace and sstables.

With the separated intents project, intents are no longer physically
interleaved. The `intentInterleavingIter` in the CockroachDB storage
package handles artifically interleaving intents to satisfy the existing
interface. The range keys project introduces a new key that must be
exposed to clients, and like intents, they're not interleaved.

We should consider alternative iterator APIs, both for internal use
within the storage package and externally for the rest of CockroachDB,
that don't interleave. There may be options that leak less of
intricacies of MVCC to clients or offer more performance. The design
space is large, and we don't hope to offer an API now. We hope to offer
enough flexibility within the Pebble API that the storage package is not
committed to interleaving MVCC range deletions forever.

#### MVCC Range Deletion interleaving

Initially, we may follow the pattern set by the `intentInterleavingIter`
and offer an iteration mode that interleaves range deletion tombstones
as synthetic point tombstones. Existing code paths that don't require
specific knowledge of MVCC range tombstones may use this iterator
without explicitly needing to learn about the existence of range
tombstones. Eventually, there would likely be both performance and
clarity benefits to migrating to a richer MVCC API so that clients don't
need to step over synthetic tombstones if they don't care about them.

#### Replica divergence detection

CockroachDB detects replica divergence through periodically computing a
checksum over a replica. The consistency checker queue orchestrates
these computations. This checksum is calculated by iterating over all
the keys within the replica, hashing keys and values. While hashing, it
also recomputes MVCCStats.

The `storage.ComputeStatsForRange` function is responsible for driving
this iteration over the replica's data and recomputing the MVCC stats.
This requires knowledge of MVCC Delete Range tombstones in order to
know which keys have been deleted and when. Today, this function relies
on Pebble surfacing overwriting key-value pairs or tombstones before
the overwritten key-value pairs they shadow.

`ComputeStatsForRange` will use a **[I2]** pebble combined iterator over
both point and range keys. For every key, it will iterate through the
range keys covering the point key to find the first one whose suffix
(MVCC timestamp) is greater than the point key's timestamp. If there is
such a range key covering the point key, `storage.ComputeStatsForRange`
will set `implictMeta = false`, indicating that the key is not a live
key. If the discovered range key's timestamp is less than the existing
`accrueGCAgeNanos` populated from a previous (point or range) key, it
updates it to the range key timestamp.

The above paragraph describes how the MVCCStats recomputation for point
keys is updated to account for MVCC range deletions. However, it ignores
the state of MVCC range tombstones themselves. If range keys are never
surfaced as materialized point keys, they're never hashed as part of the
consistency checker's checksum. To account for that, we additionally use
a defragmenting range-key-only iterator to iterate through all of a
replica's range keys, normalized into deterministic, defragmented
bounds, hashing the start, end and value. This separate, second
iteration is reasonable because:

1. We expect range keys to be rare. The Pebble range-key iterator only
   needs to load sstables with sstable properties indicating that they
   contain range keys.
2. When a sstable does contain range keys, the Pebble range-key iterator
   only needs to read 'range key' blocks.

Since we intend to use MVCC Delete Range only to delete many point keys,
this second iteration is expected to be cheaper than the alternative of
hashing a synthesized point key for every point key deleted by a MVCC
delete range tombstone.

#### MVCCStats

Range keys have a contribution to MVCCStats, recording the count of all
range keys within a range. For a discussion of the possibility of
omitting range keys from MVCCStats, see section A1 below.

During a CockroachDB KV range split, CockroachDB reads the the left-hand
side of the split, computing stats for the LHS and subtracting the
computed stats from the original range's stats to retrieve stats for the
RHS. The arbitrary physical fragmentation of range keys makes it
difficult for this one-sided iteration to determine what range keys, if
any, exist on the RHS of the split.

For the purposes of calculating range keys' MVCC stats, CockroachDB
could count all of the range keys that straddle the range boundary.
This could produce an accurate count of the logical range keys in both
left and right ranges, adding double count of range keys that span the
boundary.

During a merge, we need to determine how many ranges cross the merge
boundary to ensure we don't continue to double count the range keys
after the merge. Merges are performed at times of relative low load, so
we decide to read the range keys within both ranges on either side of
the boundary. CockroachDB can deduplicate the ranges that span the
boundary by reading their original start and end bounds from their
value.  CockroachDB may use this count to avoid double-counting ranges
in the merged range.

For a discussion of another, more complicated scheme that avoids this
merge-time iteration, see section A2 below.

#### Garbage collection

Currently, to identify garbage eligible for collection, CockroachDB
performs a scan in backwards direction through a range's replicated
data. It maintains a ring buffer of three versions of the same key for
the purpose of determining whether a key is garbage.

In addition to the ring buffer of three point keys, this garbage
collection scan may be extended with an additional ring buffer to hold
three MVCC timestamps corresponding to 1 range key per point key. When a
new timestamped key is extracted from the underlying iterator, the
`gcIterator` will also examine the iterator's `RangeKeys()` and extract
the smallest suffix of an overlapping that is also greater than the
point key's suffix.

When making the `isGarbage` determination, the scan can consult a key's
`earliestRangeKeyTimestamp` and determine the key as garbage if that
covering range key's timestamp is less than or equal to the GC
threshold.

The above describes how point keys may be detected as garbage if they're
removed by a MVCC DeleteRange but not how the MVCC DeleteRange
tombstones themselves may be detected as garbage when all the keys they
delete have been covered.

After iterating through all the point keys of a range and issuing GC
requests for all the garbage points, we may separately use a range-key
only iterator to iterate through all range keys written below the
threshold and reclaim them. We would need to adjust the `GCRequest` to
separately support specifying range keys `GCRequest_GCRangeKey`.

TODO(jackson): Discuss options for efficiently GC-ing point keys using
Pebble delete ranges. We never do this today with MVCC-ful deletes, but
since we anticipate using MVCC Delete Range for `TRUNCATE`, `DROP
TABLE`, we should consider it. Would it be enough to just have a fast
path for GC-ing a whole range? Eg, If we continue to _not_ reuse table
keyspaces are a `DROP TABLE` or `TRUNCATE`.

#### CheckForKeyCollisions

```
// If the timestamp of the SST key is greater than or equal to the
// timestamp of the tombstone, then it is not considered a collision ...
```

The process of checking for key collisions must be updated to check
whether there exist any MVCC Delete Range tombstones covering any of a
SST's keys at a timestamp higher than _x_.

TODO:

#### MVCC reads

All MVCC Reads will need to use the combined range and point key
iterator. They may configure masking of point keys, configuring the
masking with their read timestamp so that only range keys that exist at
the read timestamp mask. There may be range keys that exist at
timestamps higher than the read's timestamp. These range keys are
surfaced by the Pebble iterator, which allow readers to handle
conditions where they need to consider versions in the future up to the
`GlobalUncertaintyLimit`.

Since range keys are surfaced by the Pebble iterator even when there are
no point keys beneath them, the MVCC read is capable of detecting MVCC
Delete Range writes at higher timestamps. The exact mechanism for
checking these keys via internal iterator interfaces is to-be-decided.
We may choose to use the synthetic point tombstone interleaving
iterator.

#### MVCC writes

TODO

### Alternatives

#### A1. Automatic elision of range keys that don't cover keys

We could decide that range keys:

- Don't contribute to `MVCCStats` themselves.
- May be elided by Pebble when they cover zero point keys.

This means that garbage collection does not need to explicitly remove
the range keys, only the point keys they deleted. This option is clean
when paired with `RANGEDEL`s dropping both point and range keys.
CockroachDB can issue `RANGEDEL`s whenever it wants to drop a contiguous
swath of points, and not worry about the fact that it might also need to
update the MVCC stats for overlapping range keys.

However, this option makes deterministic iteration over defragmented
range keys for replica divergence detection challenging, because
internal fragmentation may elide regions of a range key at any point.
Producing a normalized form would require storing state in the value
(ie, the original start key) and recalculating the smallest and largest
extant covered point keys within the range key and replica bounds. This
would require maintaining _O_(range-keys) state during the
`storage.ComputeStatsForRange` pass over a replica's combined point and
range iterator.

This likely forces replica divergence detection to use other means (eg,
altering the checksum of covered points) to incorporate MVCC range
tombstone state.

This option is also highly tailored to the MVCC Delete Range use case.
Other range key usages, like ranged intents, would not want this
behavior, so we don't consider it further.

#### A2. MVCCStats describes left and right boundary statistics

In addition to stats about the range keys that lie within a range's
bounds, a split calculates statistics about the range keys that span the
range boundary.  The statistics about these boundary-spanning range keys
are set in both ranges `MVCCStats`: the LHS's `DeleteRangeRHS` field and
the RHS's `DeleteRangeLHS` field. When a MVCC delete range is GC'd,
CockroachDB must read its value to observe its original start and end
boundaries. If either or both extend beyond the range's boundaries, it
must subtract the range's statistics from the appropriate sides' fields,
in addition to the range-level fields.

During a merge, the two ranges' delete range stats are summed, and the
`min(lhs.DeleteRangeRHS, rhs.DeleteRangeLHS)` is subtracted to avoid
double-counting MVCC delete range tombstones that still exist on both
sides of the split point.

Example: Imagine an initial range `a-z` containing 4 range keys:
```
                                                               original
                              fragments                          bounds

                                     m---------r                   m-r
                       f---------------n                           f-n
                                 k---------p                       k-p
                                                 s-------w         s-w
             a b c d e f g h i j k l m n o p q r s t u v w x y z
                               r1 [a,z)
lhs,tot,rhs                    0, 4, 0
```

This range has 4 range keys. The range is split at key `m`:
```
                                     |
                                     m---------r                   m-r
                       f-------------|-n                           f-n
                                 k---|-----p                       k-p
                                     |           s-------w         s-w
                    r1 [a,m)         |           r2 [m,z)
lhs,tot,rhs         0, 2, 2          |           2, 4, 0
```

Range keys `f-n` and `k-p` overlap the range boundary, _r1_'s RHS count
and _r2_'s LGHS count are both set to 2. _r2_ inherits _r1_'s old
RHS count of 0.

_r1_ splits again at _g_:
```
                         |           |
                         |           m---------r                   m-r
                       f-|-----------|-n                           f-n
                         |       k---|-----p                       k-p
                         |           |           s-------w         s-w
               r1 [a,g)  | r3 [g,m)  |      r2 [m,z)
lhs,tot,rhs    0, 1, 1   | 1, 2, 2   |      2, 4, 0
```

Adjacent ranges' RHS,LHS counts are always equal immediately after a
split but may diverge.

The range `f-n` is now eligible for GC. _r1_ is the first to perform GC
and removes its `[f,g)` fragment. GC observes that the range boundaries
contained within the value indicate that it used to span beyond `g`, so
in addition to decrementing the total, it decrements its RHS count.
```
                         |           |
                         |           m---------r                   m-r
                         g-----------|-n                           f-n
                         |       k---|-----p                       k-p
                         |           |           s-------w         s-w
               r1 [a,g)  | r3 [g,m)  |      r2 [m,z)
lhs,tot,rhs    0, 0, 0   | 1, 2, 2   |      2, 4, 0
```

Now imagine user deletes all data `a-z`. This would manifest as three
spearate range tombstones, one-per range:

```
                         |           |
             a-----------g           |                             a-g
                         g-----------m                             g-m
                         |           m-------------------------z   m-z
                         |           m---------r                   m-r
                         g-----------|-n                           f-n
                         |       k---|-----p                       k-p
                         |           |           s-------w         s-w
               r1 [a,g)  | r3 [g,m)  |      r2 [m,z)
lhs,tot,rhs    0, 1, 0   | 1, 3, 2   |      2, 5, 0
```

Although these del-ranges abut at range boundaries, each encodes their
original bounds and are not considered range-boundary spanning and do
not increment LHS/RHS counts. Now imagine r1 and r3 are merged. _r1_ has
1 range, _r3_ has 4 ranges. They disagree on how many overlap the range
boundary (0 vs 1). The number of range keys spanning the boundary can
only have reduced, so the correct is answer is the minimum (0). The new
range key count ignoring double-counting is `tot(r1)+tot(r3)`. In this
case there is no double counting, because _r1_ already GC'd the relevant
range. When double-counting is calculating, it's calculated as
`min(rhs(r1), lhs(r3)) = 0` and the total is left unchanged.

The merged range inherits _r3_'s RHS:
```
                                     |
             a-----------g           |                             a-g
                         g-----------m                             g-m
                                     m-------------------------z   m-z
                                     m---------r                   m-r
                         g-----------|-n                           f-n
                                 k---|-----p                       k-p
                                     |           s-------w         s-w
                    r1 [a,m)         |      r2 [m,z)
lhs,tot,rhs         0, 4, 2          |      2, 5, 0
```

Note that `a-g` and `g-m` are still separate range keys, because they
were written with different original bounds. Say now, the `m-n` fragment
of range key `f-n` is GC'd from _r2_. Garbage collection would notice
that the fragment `m-n` fragment had an original start bound of `f`, and
decrement its LHS, in addition to its total:

```
                                     |
             a-----------g           |                             a-g
                         g-----------m                             g-m
                                     m-------------------------z   m-z
                                     m---------r                   m-r
                         g-----------m                             f-n
                                 k---|-----p                       k-p
                                     |           s-------w         s-w
                    r1 [a,m)         |      r2 [m,z)
lhs,tot,rhs         0, 4, 2          |      1, 4, 0
```

If these two ranges were to merge again, the resulting range would
calculate its new range key count as:
```
tot(r1)+tot(r2)-min(rhs(r1),lhs(r2)) = 4 + 4 - min(2, 1) = 7


             a-----------g                                         a-g
                         g-----------m                             g-m
                                     m-------------------------z   m-z
                                     m---------r                   m-r
                         g-----------m                             f-n
                                 k---------p                       k-p
                                                 s-------w         s-w
                                r1 [a,z)
lhs,tot,rhs                      0, 7, 0
```

