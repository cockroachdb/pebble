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
- Fully flesh out the design.
  - Name the operations: tentatively RangeSet and RangeUnset:
    thoughts?
- Consider following CockroachDB RFC format.
- Document relationship to CockroachDB's MVCC and new key ordering requirements
  (eg, encoding-agnostic, prefix < prefix@t1)
- Expand on masking implementation.
- Expand the sstable boundaries discussion.
- Decide if blanket prohibiting splitting user keys across sstables.
- Fix formatting.

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

1. The user key consisting of just a key prefix `k` must sort before all
   other user keys containing that prefix. Specifically
   `Compare(k[:Split(k)], k) < 0` where `Split(k) < len(k)`.
2. The empty key prefix must be a valid key and comparable. The ordering
   of the empty key prefix with any suffixes must be consistent with the
   ordering of those same suffixes applied to any other key prefix.
   Specifically `Compare(k[Split(k):], k2[Split(k2):]) == Compare(k,
   k2)` where `Compare(k[:Split(k)], k2[:Split(k2)]) == 0`.

### Writes

This design introduces two new write operations:

- `RangeSet([k1, k2), [optional suffix], <value>)`: This represents
  the mapping `[k1, k2)@suffix => value`. Keys `k1` and `k2` must not
  contain a suffix.

- `RangeUnset([k1, k2), [optional suffix])`: This removes a mapping
  previously applied by `RangeSet`. The unset may use a smaller key
  range than the original `RangeSet`, in which case part of the range
  is deleted. The unset only applies to range keys with a matching
  optional suffix.

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

Range deletes apply to both point keys and range keys. A range delete
can remove part of a range key, just like the new `RangeUnset` operation
introduced earlier. Range deletes differ from `RangeUnset`s, because the
latter requires that the suffix matches and applies only to range keys.

[TODO(jackson): The new internal range key bounds scheme makes it
possible to support deleting point keys with RANGEDEL (eg, does not
_require_ RANGEDEL_PREFIX). Think about whether we might want a
point-only range deletion operation regardless, for example, maybe when
GC-ing swaths of point keys.]

The optional suffix is related to the pebble `Comparer.Split` operation
which is explicitly documented as being for [MVCC
keys](https://github.com/cockroachdb/pebble/blob/e95e73745ce8a85d605ef311d29a6574db8ed3bf/internal/base/comparer.go#L69-L88),
without mandating exactly how the versions are represented. `RangeSet`
and `RangeUnset` keys with different suffixes do not interact logically,
although Pebble may observably fragment ranges at any user key,
including at range keys intersection points.

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
HasPointAndRange() (hasPoint bool, hasRange bool)
Key() []byte

PointValue() []byte

RangeEndKey() []byte
RangeSuffix() []byte
RangeValue() []byte
```

During iteration with a combined iterator, an iteration position may
surface both a point key and a range key at the currently-positioned
`Key`.

Range keys surfaced through iteration may be fragmented. Range keys are
physically fragmented as an artifact of the log-structured merge tree
structure and internal sstable boundaries. Range key fragments surfaced
through a user-facing iterator may be split at any user key boundary
within the original range's user key range. This allows for range keys
with boundary keys that have suffixes that sort above or below the range
key's suffix. For example a `RangeSet([a,c), @50, <value>)` may be
surfaced as the range key fragments `RangeSet([a, b@30), @50, <value>)`
and `RangeSet([b@30,c), @50, <value>)`. The exposed fragments may also
be finer than the physical fragments. This is considered acceptable
since:

1. the `RangeUnset` semantics are over the logical range and not a
   physical key,
2. our current use cases don't need to know when all the fragments for
   an original `RangeSet` have been seen,
3. users that want to know when all the fragments have been seen can
   store the original _k2_ in the `<value>` and iterate until they are
   past that _k2_.

#### Iteration order

Recall that the user-provided `Comparer.Split(k)` function divides all
user keys into a prefix and a suffix, such that the prefix is
`k[:Split(k)]`, and the suffix is `k[Split(k):]`. If a key does not
contain a suffix, the user key equals the prefix.

An iterator that is configured to surface range keys alongside point
keys will surface them at unique encountered key _prefixes_. If the
range key was created with a suffix, the range key appears at that
suffix. For example, first consider these user keys without suffixes:

```
  Point keys: a, b, c, d, e, f
  Range key: [a,e)
```

A pebble.Iterator, which shows user keys, will output (during forward
iteration), the following keys:

```
  (a,[a,e)), (b,[b,e)), (c,[c,e)), (d,[d,e)),  e
```

The notation `(b,[b,e))` indicates both these keys and their
corresponding values are visible at the same time when the iterator is
at that position.

- There cannot be multiple ranges with start key at `Key()` since the
  one with the highest seqnum will win, just like for point keys.

- The reason the `[b,e)` key in the pair `(b,[b,e))` cannot be truncated
  to c is that the iterator at that point does not know what point key
  it will find next, and we do not want the cost and complexity of
  looking ahead (which may need to switch sstables).

- If the iterator has a configured upper bound, it will truncate the
  range key to that upper bound. e.g. if the upper bound was c, the
  sequence seen would be `(a,[a,c))`, `(b,[b,c))`. The same applies to
  lower bound and backward iteration.

In the above example, the range key `[a,e)` had no suffix, so the range
key appeared at key prefixes without any suffix. Remember these range
keys may also specify a suffix: `RangeSet([k1,k2), @suffix, <value>)`
and point keys optionally have suffixes.

Consider the following state in terms of user keys (the `@number` part
is the version or "suffix"):

```
point keys: a@100, a@30, b@100, b@40, b@30, c@40, d@40, e@30
range key: [a,e)@50
```

If we consider the user key ordering across the union of these keys,
where we have used ' and '' to mark the start and end keys for the
range key, we get:

```
a@100, a@50', a@30, b@100, b@40, b@30, c@40, d@40, e@50'', e@30
```

A `pebble.Iterator`, which shows user keys, will output (during forward
iteration), the following keys:

```
a@100, [a,e)@50, a@30, b@100, [b,e)@50, b@40, b@30, [c,e)@50, c@40, [d,e)@50, d@40, e@30
```

- In this example each Next will be positioned such that there is only
  either a point key or a range key, but not both. If there existed a
  point key `b@50`, Next-ing from `b@100` would position the iterator
  over both the range key `[b,e)@50` and the point key `b@50`.

- Like the non-MVCC case, the end key for the range is not being
  fragmented using the succeeding point key.

- As a generalization of what we described earlier, the start key of
  the range is being adjusted such that the prefix matches the prefix
  of the succeeding point key, which this "masks" from an MVCC
  perspective. This repetition of parts of the original `[a,e)@502, in
  the context of the latest point key, is useful to a caller that is
  making decisions on how to interpret the latest prefix and its various
  versions. The same repetition will happen during reverse iteration.

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
//   Compare(RangeKeySuffix(), suffix) < 0
//
// the iterator will skip over all point keys k contained within the
// range key's span such that
//
//   Compare(k[Split(k):], RangeKeySuffix()) < 0
//
func RangeKeyMask(suffixBlockPropertyName string, suffix []byte) IteratorMask {
    // ...
}
```

Example: A user may construct an iterator with a mask
`RangeKeyMask(@t50)`. A range key `[a, c)@t60` masks nothing. A range
key `[a,c)@30` masks `a@20` and `apple@10` but not `apple@40`.

## Implementation

### Write operations

This design introduces two new Pebble write operations: `RangeSet` and
`RangeUnset`. Internally, these operations are represented as keys with
`RANGESET` and `RANGEUNSET` key kinds stored within the same sstables as
point keys, but in different blocks. Within the memtables, these keys
are stored in a separate skip list.

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

Unlike other Pebble keys, the `RANGESET` and `RANGEUNSET` keys have
values encoding multiple fields of data known to Pebble. This encoded
representation is used in batches, sstables and within the memtable's
skiplist:
```
RangeSet: varint(len(k2)) <k2> varint(len(suffix)) <suffix> [varint(len(suffix)) <suffix>...] <value>
RangeUnset: varint(len(k2)) <k2> varint(len(suffix)) <suffix> [varint(len(suffix)) <suffix>...]
```

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

Range keys are expected to be rare compared to point keys. This rarity
is important since bloom filters used for `SeekPrefixGE` cannot
efficiently eliminate an sstable that contains such range keys -- we
also need to read the range key block(s). Note that if these range keys
were interleaved with the point keys in the sstable we would need to
potentially read all the blocks to find these range keys, which is why
we do not make this design choice.  Pebble does not use bloom filters in
L6, so once a range key is compacted into L6 its impact to
`SeekPrefixGE` is lessened.

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
determined by the user key. Since the first point key of the next
sstable is `b@30`, the compaction flushes the fragment `[a,e)@50` to the
first sstable and updates the existing fragment to begin at `b@30`.

If a range key extends into the next file, and the range key's end is
truncated for the purposes of determining the sstable end boundary. The
first sstable's end boundary becomes `b@30#inf`, signifying the range
key does not cover `b@30`. The second sstable's start boundary is
`b@30`.

[TODO(jackson): This explanation needs work.]

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
sstable's range key block.

[TODO(jackson): This explanation needs work.]

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

TODO: think more about whether there is an efficient way to offer
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

TODO(jackson): Expand and rephrase this.

To efficiently implement this we cannot rely on the LSM invariant since
`b@100` can be at a lower level than `[a,e)@50`. However, we do have (a)
per-block key bounds, and (b) for time-bound iteration purposes we will
be maintaining block properties for the timestamp range (in
CockroachDB). We will require that for efficient masking, the user
provide the name of the block property to utilize. In this example, when
the iterator will encounter `[a,e)@100` it can skip blocks whose keys
are guaranteed to be in [a,e) and whose timestamp interval is < 100.
