
** Design Draft**
TODO:
- Fully flesh out the design.
- consider following CockroachDB RFC format.
- Fix formatting.

### Context

Pebble currently has only one kind of key that is associated with a
range: `RANGEDEL [k1, k2)#seq`, where [k1, k2) is supplied by the
caller, and is used to efficiently remove a set of point keys.

The detailed design for MVCC compliant bulk operations ([high-level
description](https://github.com/cockroachdb/cockroach/blob/master/docs/RFCS/20210825_mvcc_bulk_ops.md);
detailed design draft for DeleteRange in internal
[doc](https://docs.google.com/document/d/1ItxpitNwuaEnwv95RJORLCGuOczuS2y_GoM2ckJCnFs/edit#heading=h.x6oktstoeb9t)),
is running into increasing complexity by placing range operations
above the Pebble layer, such that Pebble sees these as points. The
complexity causes are various: (a) which key (start or end) to anchor
this range on, when represented as a point (there are performance
consequences), (b) rewriting on CockroachDB range splits (and concerns
about rewrite volume), (c) fragmentation on writes and complexity
thereof (and performance concerns for reads when not fragmenting), (d)
inability to efficiently skip older MVCC versions that are masked by a
`[k1,k2)@ts` (where ts is the MVCC timestamp).

First-class support for range keys in Pebble would eliminate all these
issues. Additionally, it could allow for future extensions like
efficient transactional range operations. This issue describes how
this feature would work from the perspective of a user of Pebble (like
CockroachDB), and sketches some implementation details.

### 1. Writes

There are two new operations: 

- `Set([k1, k2), [optional suffix], <value>)`: This represents the
  mapping `[k1, k2)@suffix => value`.

- `Del([k1, k2), [optional suffix])`: The delete can use a smaller key
  range than the original Set, in which case part of the range is
  deleted. The optional suffix must match the original Set.

For example, consider `Set([a,d), foo)` (i.e., no suffix). If there is
a later call `Del([b,c))`, the resulting state seen by a reader is
`[a,b) => foo`, `[c,d) => foo`. Note that the value is not modified
when the key is fragmented.

- Partially overlapping Sets overlay as expected based on
  fragments. For example, consider `Set([a,d), foo)`, followed by
  `Set([c,e), bar)`. The resulting state is `[a,c) => foo`, `[c,e) =>
  bar`.

- The optional suffix is related to the pebble Split operation which
  is explicitly documented as being for [MVCC
  keys](https://github.com/cockroachdb/pebble/blob/e95e73745ce8a85d605ef311d29a6574db8ed3bf/internal/base/comparer.go#L69-L88),
  without mandating exactly how the versions are represented.

- Pebble internally is free to fragment the range even if there was no
  Del. This fragmentation is essential for preserving the performance
  characteristics of a log-structured merge tree.

- Iteration will see fragmented state, i.e., there is no attempt to
  hide the physical fragmentation during iteration. Additionally,
  iteration may expose finer fragments than the physical fragments
  (discussed later). This is considered acceptable since (a) the Del
  semantics are over the logical range, and not a physical key, (b)
  our current use cases don't need to know when all the fragments for
  an original Set have been seen, (c) users that want to know when all
  the fragments have been seen can store the original k2 in the
  `<value>` and iterate until they are past that k2.

- Like point deletion, the Del can be elided when it falls to L6 and
  there is no snapshot preventing its elision. So there is no
  additional garbage collection problem introduced by these keys.

- Point keys and range keys do not interact in terms of overwriting
  one another. They have a parallel existence. The one interaction we
  will discuss happens at user-facing iteration time, where there can
  be masking behavior applied on points due to later range
  writes. This masking behavior is explicitly requested by the user in
  the context of the iteration, and does not apply to internal
  iterators used for compaction writes. Point deletes only apply to
  points.

- Range deletes apply to both point keys and range keys. A RANGEDEL
  can remove part of a range key, just like the new Del we introduced
  earlier. Note that these two delete operations are different since
  the Del requires that the suffix match. A RANGEDEL on the other hand
  is trying to remove a set of keys (historically only point keys),
  which can be point keys or range keys. We will discuss this in more
  detail below after we elaborate on the semantics of the range
  keys. Our current notion of RANGEDEL fails to fully consider the
  behavior of range keys and will need to be strengthened.

- There is no Merge operaton.

- Pebble will assign seqnums to the Set and Del, like it does for the
  existing internal keys.

  - The LSM level invariants are valid across range and point keys
    (just like they are for RANGEDEL + point keys). That is,
    `Set([k1,k2))#s2` cannot be at a lower level than `Set(k)#s1`
    where `k \in [k1,k2)` and `s1 < s2`.

  - These range keys will be stored in the same sstable as point
    keys. They will be in separate block(s).

  - Range keys are expected to be rare compared to point keys. This
    rarity is important since bloom filters used for SeekPrefixGE
    cannot efficiently eliminate an sstable that contains such range
    keys -- we also need to read the range key block(s). Note that if
    these range keys are interleaved with the point keys in the
    sstable we would need to potentially read all the blocks to find
    these range keys, which is why we do not make this design choice.
    Pebble does not use bloom filters in L6, so once a range key is
    compacted into L6 its impact to SeekPrefixGE is lessened.

A user iterating over a key interval [k1,k2) can request:

- **[I1]** An iterator over only point keys.

- **[I2]** A combined iterator over point + range keys. This is what
    we mainly discuss below.

- **[I3]** An iterator over only range keys. In the CockroachDB use
    case, range keys will need to be subject to MVCC GC just like
    point keys -- this iterator may be useful for that purpose.


### 2. Key Ordering, Iteration etc.

#### 2.1 Non-MVCC keys, i.e., no suffix

Consider the following state in terms of user keys:
point keys: a, b, c, d, e, f
range key: [a,e)

A pebble.Iterator, which shows user keys, will output (during forward
iteration), the following keys:

(a,[a,e)), (b,[b,e)), (c,[c,e)), (d,[d,e)),  e

The notation (b,[b,e)) indicates both these keys and their
corresponding values are visible at the same time when the iterator is
at that position. This can be handled by having an iterator interface
like

```
HasPointAndRange() (hasPoint bool, hasRange bool)
Key() []byte
PointValue() []byte
RangeEndKey() []byte
RangeValue() []byte
```

- There cannot be multiple ranges with start key at `Key()` since the
  one with the highest seqnum will win, just like for point keys.

- The reason the [b,e) key in the pair (b,[b,e)) cannot be truncated
  to c is that the iterator at that point does not know what point key
  it will find next, and we do not want the cost and complexity of
  looking ahead (which may need to switch sstables).

- If the iterator has a configured upper bound, it will truncate the
  range key to that upper bound. e.g. if the upper bound was c, the
  sequence seen would be (a,[a,c)), (b,[b,c)). The same applies to
  lower bound and backward iteration.

#### 2.2 Split points for sstables

Consider the actual seqnums assigned to these keys and say we have:
point keys: a#50, b#70, b#49, b#48, c#47, d#46, e#45, f#44
range key: `[a,e)#60`

We have created three versions of b in this example. Pebble currently
can split output sstables during a compaction such that the different
b versons span more than one sstable. This creates problems for
RANGEDELs which span these two sstables which are discussed in the
section on [improperly truncated
RANGEDELS](https://github.com/cockroachdb/pebble/blob/master/docs/range_deletions.md#improperly-truncated-range-deletes). We
manage to tolerate this for RANGEDELs since their semantics are
defined by the system, which is not true for these range keys where
the actual semantics are up to the user.

We will disallow such sstable split points if they can span a range
key. In this example, by postponing the sstable split point to the
user key c, we can cleanly split the range key into `[a,c)#60` and
`[c,e)#60`. The sstable end bound for the first sstable (sstable
bounds are inclusive) will be c#inf (where inf is the largest possible
seqnum, which is unused except for these cases), and sstable start
bound for the second sstable will be c#60.

Addendum: When we discuss keys with a suffix, i.e., MVCC keys, we
discover additional difficulties (described below). The solution there
is to require an `ImmediateSuccessor` function on the key prefix in
order to enable range keys (here the prefix is the same as the user
key). The example above is modified in the following way, when the
sstables are split after b#49:

- sstable 1: a#50, `[a,b)#60`, b#70, `[b,ImmediateSuccessor(b))#60`,
  b#49

- sstable 2: b#48, `[ImmediateSuccessor(b),e)#60`, c#47, d#46, e#45,
  f#44

  The key [b,ImmediateSuccessor(b)) behaves like a point key at b in
  terms of bounds, which means the end bound for the first sstable is
  b#49, which does not overlap with the start of the second sstable at
  b#48.

So we have two workable alternative solutions here. It is possible we
will choose to not let the same key span sstables since it simplifies
some existing code in Pebble too.

#### 2.3 Determinism of output

Range keys will be split based on boundaries of sstables in an LSM. We
typically expect that two different LSMs with different sstable
settings that receive the same writes should output the same key-value
pairs when iterating. To provide this behavior, the iterator
implementation could defragment range keys during iteration time. The
defragmentation behavior would be:

- Two visible ranges [k1,k2)=>val1, [k2,k3)=>val2 are defragmented if
  val1==val2, and become [k1,k3).

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

#### 2.4 MVCC keys, i.e., may have suffix

As a reminder, we will have range writes of the form `Set([k1, k2),
<ver>, <value>)`.

- The requirement is that running split on `k1+<ver>` and `k2+<ver>`
  will give us prefix k1 and k2 respectively (+ is simply
  concatenation).

- Existing point writes would have been of the form `k+<ver> =>
  <value>` (in some cases `<ver>` is empty for these point writes).

- Pebble already requires that the prefix k follows the same key
  validity rules as `k+<ver>`.

- Even though Pebble does not require that `k < k+<ver>` (when `<ver>`
  is non-empty), CockroachDB has adopted this behavior since it
  results in the following clean behavior: RANGEDEL over [k1, k2)
  deletes all versioned keys which have prefixes in the interval [k1,
  k2). Pebble will now require this behavior for anyone using MVCC
  keys.

Consider the following state in terms of user keys (the @number part
is the version)

point keys: a@100, a@30, b@100, b@40, b@30, c@40, d@40, e@30
range key: [a,e)@50

If we consider the user key ordering across the union of these keys,
where we have used ' and '' to mark the start and end keys for the
range key, we get:

a@100, a@50', a@30, b@100, b@40, b@30, c@40, d@40, e@50'', e@30 

A pebble.Iterator, which shows user keys, will output (during forward
iteration), the following keys:

a@100, [a,e)@50, a@30, b@100, [b,e)@50, b@40, b@30, [c,e)@50, c@40, [d,e)@50, d@40, e@30

- In this example each Next will be positioned such that there is only
  either a point key or a range key, but not both. The same interface
  outlined earlier can of course handle this more restrictive example.

- Like the non-MVCC case, the end key for the range is not being
  fragmented using the succeeding point key.

- As a generalization of what we described earlier, the start key of
  the range is being adjusted such that the prefix matches the prefix
  of the succeeding point key, which this "masks" from an MVCC
  perspective. This repetition of parts of the original [a,e)@50, in
  the context of the latest point key, is useful to a caller that is
  making decisions on how to interpret the latest prefix and its
  various versions. The same repetition will happen during reverse
  iteration.

#### 2.5 Splitting MVCC keys across sstables

Splitting of an MVCC range key is always done at the key prefix
boundary.

- [a,e)@50#s can be split into [a,c)@50#s, [c,e)@50#s, and so on.

- The obvious thing would be for the first key to contribute c@50#inf
  to the first sstable's end bound, and the second key to contribute
  c@50#s to the second sstable's start bound.

Consider the earlier example and let us allow the same prefix b to
span multiple sstables. The two sstables would have the following user
keys:

- first sstable: points: a@100, a@30, b@100, b@40 ranges: [a,c)@50

- second sstable: points: b@30, c@40, d@40, e@30, ranges: [c,e)@50.

In practice the compaction code would need to know that the point key
after b@30 has key prefix c, in order to fragment the range into
[a,c)@50, for inclusion in the first sstable. This is messy wrt code,
so a prerequisite for using range keys is to define an
`ImmediateSuccesor` function on the key prefix. The compaction code
can then include [a,ImmediateSuccessor(b))@50 in the first sstable and
[ImmediateSuccessor(b),e)@50 in the second sstable. But this raises
another problem: the end bound of the first sstable is
ImmediateSuccessor(b)@50#inf and the start bound of the second sstable
is b@30#s, where s is some seqnum. Such overlapping bounds are not
permitted.

We consider two high-level options to address this problem.

- Not have a key prefix span multiple files: This would disallow
  splitting after b@40, since b@30 remains. Requiring all point
  versions for an MVCC key to be in one sstable is dangerous given
  that histories can be maintained for a long time. This would also
  potentially increase write amplification. So we do not consider this
  further.

- Use the ImmediateSuccessor to make range keys behave as points, when
  necessary: Since the prefix b is spanning multiple sstables in this
  example, the range key would get split into [a,b)@50,
  [b,ImmediateSuccessor(b))@50, [ImmediateSuccessor(b),e)@50.

  - first sstable: points: a@100, a@30, b@100, b@40 ranges: [a,b)@50,
    [b,ImmediateSuccessor(b))@50

  - second sstable: points: b@30, c@40, d@40, e@30, ranges:
    [ImmediateSuccessor(b),e)@50

  - The sstable end bounds contribution for the two range keys in the
    first sstable are b#inf, b@50#s. This is justified as follows:

    - The b prefix is not included in [a,b)@50 and we know that b
      sorts before b@ver (for non-empty ver).

    - [b,ImmediateSuccessor(b))@50 is effectively representing a point
    b@50.  Hence the end bound for the first sstable is b@40#s1, which
    does not overlap with the b@30#s2 start of the second sstable.

  If the second sstable starts on a new point key prefix we would use
  that prefix for splitting the range keys in the first sstable.

We adopt the second option. However, now it is possible for b@100#s2
to be at a lower LSM level than [a,e)@50#s1 where s1<s2. This is
because the LSM invariant only works over user keys, and not over user
key prefixes, and we have deliberately chosen this behavior.

Truncation of the range keys using the configured upper bound of the
iterator can only be done if the configured upper bound is its own
prefix. That is, we will not permit such iterators to have an upper
bound like d@200. It has to be a prefix like d. This is to ensure the
truncation is sound -- we are not permitting these range keys to have
a different suffix for the start and end keys. If the user desires a
tighter upper bound than what this rule permits, they can do the
bounds checking themselves.

#### 2.6 Strengthening RANGEDEL semantics

The current RANGEDEL behavior is defined over points and is oblivious
to the prefix/suffix behavior of MVCC keys. The common case is a
RANGEDEL of the form [b,d), i.e., no suffix. Such a RANGEDEL can be
cleanly applied to range keys since it is not limited to a subset of
versions. However, currently one can also construct RANGEDELs like
[b@30,d@90), which are acceptable for bulk deletion of points, but
semantically hard to handle when applied against a range key
[a,e)@50. To deal with this we will keep the current RANGEDEL to apply
to only point keys, and introduce RANGEDEL_PREFIX, which must have
start and end keys that are equal to their prefix.

#### 2.7 Efficient masking of old MVCC versions

Recollect that in the earlier example (before we discussed splitting)
the pebble.Iterator would output (during forward iteration), the
following keys:

a@100, [a,e)@50, a@30, b@100, [b,e)@50, b@40, b@30, [c,e)@50, c@40, [d,e)@50, d@40, e@30

It would be desirable if the caller could indicate a desire to skip
over a@30, b@40, b@30, c@40, d@40, and this could be done
efficiently. We will support iteration in this MVCC-masked mode, when
specified as an iterator option. This iterator option would also need
to specify the version such that the caller wants all values >=
version.

To efficiently implement this we cannot rely on the LSM invariant
since b@100 can be at a lower level than [a,e)@50. However, we do have
(a) per-block key bounds, and (b) for time-bound iteration purposes we
will be maintaining block properties for the timestamp range (in
CockroachDB). We will require that for efficient masking, the user
provide the name of the block property to utilize. In this example,
when the iterator will encounter [a,e)@100 it can skip blocks whose
keys are guaranteed to be in [a,e) and whose timestamp interval is <
100.

------

# Color Range

In this design Pebble exposes an API to color, or label, ranges of its
1-dimensional keyspace with a value. A coloring may be applied to a range of the
keyspace multiple times, and compactions handle merging colors through a
user-defined color mixing operation.

CockroachDB uses this key-coloring API to implement MVCC range deletions,
coloring point keys with MVCC range tombstone(s) encoding the timestamps at
which data is logically deleted.

## Pebble

Pebble requires the user provide an implementation of a `ColorMixer` interface in
order to apply colorings to point keys. During compactions, when two overlapping
colorings are encountered, the spans are fragmented at the span bounadries. The
span contained within both colorings is merged (or 'mixed') through invoking the
user-provided `ColorMixer`. This operation yields a single value, which is then
associated with the span. If mixing colors yields an empty value and the
coloring span containing the empty value reaches the last snapshot stripe of the
bottomost level of the LSM, the coloring span may be elided.

Snapshots prevent colors from mixing, and colors may only mix within a snapshot
stripe. At a given sequence number, there is at most one fragmented coloring
within a level covering a point. This ensures the number of fragments within a
sstable are O(N × S) where N is the number of overlapping colorings provided and
S is the number of open snapshots. This merging avoids the quadratic blowup of
fragments when range keys cannot be merged. Since fragments encode the range
within long user keys, this is important in reducing the number of key
comparisons required during iteraiton. This also ensures that an iterator
reading at a particular sequence number is only concerned with at most one
coloring span per level.

During iteration over a LSM containing N levels, there may exist up to N
coloring spans that overlap a given point. If the iterator had requested access
to point key coverings when it was constructed, the iterator may surface a data
structure encompassing the O(N) overlapping coloring spans, in level-descending
order. This data structure holds pointers into the O(N) coloring span blocks.
For convenience, this data structure exposes a method that mixes the values of
all the coloring values, yielding the point key's discrete color. For
efficiency, a user may alternatively iterate over the coloring spans directly
to avoid unnecessary work.

An individual point key's mixed color is deterministic as long as the
user-defined color mixing operation is associative. The coloring spans directly
surfaced by the iterator are not deterministic and depend on physical sstable
boundaries.

DeleteRange operations delete all overlapping coloring spans as well.

Removing colorings from ranges without removing the described point keys may be
implemented through the semantics of the user-defined color mixer. Coloring
fragments with zero-length values are elided when they reach the last snapshot
stripe of the bottommmost level of the LSM.  User-defined color mixers can
ensure that they can write new color values that yield an empty value when
merged, to implement removal.

Note that Pebble's range tombstones and these coloring spans are very closely
related. They're both defined over the same keyspace. For both of these kinds of
range keys, at most 1 of the range keys within a level overlap a given point at
a given sequence number. This design aspires to closely mirror range deletions
in semantics and mechanics (eg, fragmenting) so that learnings, code, etc may be
shared between the two implementations.

With respect to sstable boundaries and truncation, coloring fragments behave
identically to range deletions. A coloring fragment is only "effective within"
the span of the sstable boundaries.

```
type ColorMixer interface {
  // Name returns the name of the color mixer. It must be stable and is
  // serialized within a database’s OPTIONS file.
  Name() string
  // ColorMix mixes color values, producing a single color value
  // representing the combination. Operands are provided in-order
  // left-to-right representing newest to oldest.
  ColorMix(dst []byte, includesBase bool, operands ...[]byte) []byte
}

// ColorRange writes a new coloring to the key range specified by [startKey,
// endKey).
(d *DB) ColorRange(startKey, endKey, value []byte, *WriteOptions) error

// A Coloring represents a span of color applied to point keys.
type Coloring struct {
  StartKey []byte
  EndKey   []byte
  Color    []byte
}

// Color provides facility to access the color of a point key under the
// iterator. Its Mixed method returns the color of the point key, fully merged.
// Color also exposes the set of unmerged, physical coloring spans encountered
// so that some use cases may avoid unneccessarily mixing colors.
type Color struct {
  buf       []byte
  colorMix  ColorMixer

  // Colorings contains the physical coloring spans that cover the current point
  // key, that when mixed, yield the point's color.
  Colorings []Coloring
}

// Mixed applies the color mixer on the set of colorings, yielding a single
// color.
func (c Color) Mixed() []byte {
  switch len(c.Colorings) {
  case 0:
      return nil
  case 1:
      return c.Colorings[0].Color
  default:
      // ...
  }
}
```

## CockroachDB

CockroachDB may use the new Pebble coloring API to implement MVCC range deletes.
Point keys are 'colored' with the timestamp at which they are deleted. The
Pebble keyspace is one-dimensional, and Pebble has limited knowledge of MVCC
timestamp suffixes. When a coloring is applied to a range, it's applied to this
one-dimensional keyrange, which includes point keys both above and below the
range delete timestamp.

To accommodate this, CockroachDB's color values encode a sequence of MVCC
timestamps, sorted. These timestamps encode whether a 'MVCC DeleteRange' is
being applied or removed at the provided timestamp:

```
1-byte version header
[-1|1] [timestamp len] timestamp1
[-1|1] [timestamp len] timestamp2
[-1|1] [timestamp len] timestamp3
0
```

[Is there other metadata we would want to encode?]

CockroachDB's implementation of the `ColorMix` operator adds all the entries
from the left operand and removes any overwritten entries from the right. If
`includesBase` is set, the operator may drop any timestamp removals because
there exist no operands in lower levels. If there exist no entries, because
they've all been dropped, the operator may return an empty value. If the
coloring with an empty value is in the bottommost layer of the LSM, the
corresponding coloring span will be dropped altogether.

When requested during iteration, a Pebble iterator directly exposes the unmerged
coloring spans. To evaluate whether the current point is key deleted, the MVCC
iterator must find the first coloring span containing a timestamp beneath the
iterator's read timestamp. For reads at recent timestamps, this should be the
first coloring span's first timestamp entry.

The first entry less than the read timestamp but greater than the point key's
timestamp, if such an entry exists, indicates whether or not the point key is
deleted by a MVCC range tombstone.

The MVCC iterator may use the corresponding coloring span's range bounds in
order to mask entries beneath the deleted timestamp.

