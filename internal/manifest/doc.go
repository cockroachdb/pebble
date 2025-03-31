// Copyright 2025 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package manifest

/*

Proof Sketch of Correctness of LSM with Sub-Levels (Incomplete):

NB: Ordering of L0 files (from oldest to youngest) is done by (LargestSeqNum,
SmallestSeqNum, FileNum). This is used by sub-level initialization to do a
more sophisticated time and key-span based ordering.

1. History

Consider a scenario where there are no compactions out of L0. Consider the
history of files getting added to L0 and two ways of accumulating that history
into a stack of sstables:

- Simple-stack: Files are simply stacked into levels based on their add
  ordering (files added atomically can randomly be ordered in any way).

- Seqnum-stack: Files are stacked based on (LargestSeqNum, SmallestSeqNum,
  FileNum).

These two stacks will not be identical after every file add, even though they
contain the same set of files. The trivial divergence is because of the atomic
adds of multiple files. But the real divergence is because we cannot claim
that files are added to L0 in increasing order of LargestSeqNum: ingests that
don't have the same keys as memtables can be added to L0 before the memtables
are flushed.

We make some claims about the invariants at any point of the history, in these
stacks.

Claim 1: If sstable S1 and sstable S2 contain the same user key k and the k
seqnum in S1 is < the k seqnum in S2, then S1 is added to L0 before S2.

Proof by contradiction:

- Flushes only: Consider that S2 is added to L0 before S1.

  - S2 and S1 are part of the same flush operation: All instances of k will be
    in the same sst (flush splits are always on userkey boundaries).
    Contradiction.

  - S2 and S1 are in different flushes: Since flushes are sequential, the
    flush of S2 must end before the flush of S1 starts. Since we flush
    memtables in order of their filling up and all seqnums inside older
    memtables are less than all seqnums inside newer memtables, the seqnum of
    k in S2 < the seqnum in S1. Contradiction.

- Ingests and flushes: Consider that S2 is added to L0 before S1.

  - S2 and S1 are part of the same atomic ingest: Atomic ingests have ssts
    with non-overlapping user key bounds, so they cannot contain k in
    different ssts. Contradiction.

  - S2 and S1 are part of different ingests. They must be assigned different
    sst seqnums. Ingests must be added to the LSM in order of their seqnum
    (see the discussion here
    https://github.com/cockroachdb/pebble/issues/2196#issuecomment-1523461535).
    So seqnum of S2 < seqnum of S1. Contradiction.

  - S2 is an ingest and S1 is from a flush, and S2 is added first. Cases:

    - The seqnum of k in S1 was present in an unflushed memtable when S2
      ingestion was requested: So the memtable seqnum for k < S2 seqnum. And k
      in the memtable will be flushed first (both with normal ingests and
      flushable ingests). Contradiction.

    - The seqnum of k in S1 was not present in any memtable when S2 ingestion
      was requested: The k in S1 will be assigned a higher seqnum than S2.
      Contradiction.

  - S2 is a flush and S1 is an ingest, and S2 is added first to L0.

    - S2 is from memtable(s) that flushed before the ingest was requested.
      Seqnum of k in S2 < Seqnum of k in S1. Contradiction.

    - S2 is from memtable(s) that flushed because the ingest was requested.
      Seqnum of k in S2 < Seqnum of k in S1. Contradiction.

    - The k in S2 was added to the memtable after S1 was assigned a seqnum,
      but S2 got added first. This is disallowed by the fix in
      https://github.com/cockroachdb/pebble/issues/2196. Contradiction.

Claim 1 is sufficient to prove the level invariant for key k for the
simple-stack, since when S1 has k#t1 and S2 has k#t2, and t1 < t2, S1 is added
first. However it is insufficient to prove the level invariant for
seqnum-stack: say S1 LargestSeqNum LSN1 > S2's LargestSeqNum LSN2. Then the
ordering of levels will be inverted after S2 is added. We address this with
claim 2.

Claim 2: Consider sstable S1 and sstable S2, such that S1 is added to L0
before S2. Then LSN(S1) < LSN(S2) or S1 and S2 have no userkeys in common.

Proof sketch by contradiction:

Consider LSN(S1) >= LSN(S2) and S1 has k#t1 and S2 has k#t2, where t1 < t2. We
will consider the case of LSN(S1) >= t2. If we can contradict this and show
LSN(S1) < t2, then by t2 <= LSN(S2), we have LSN(S1) < LSN(S2).

Cases:

- S1 and S2 are from different flushes: The seqnums are totally ordered across
  memtables, so all seqnums in S1 < all seqnums in S2. So LSN(S1) < t2.
  Contradiction.

- S1 is a flush and S2 is an ingest:

  - S1 was the result of flushing memtables that were immutable when S2
    arrived, which has LSN(S2)=t2. Then all seqnums in those immutable
    memtables < t2, i.e., LSN(S1) < t2, Contradiction.

  - S1 was the result of flushing a mutable memtable when S2 arrived for
    ingestion. k#t1 was in this mutable memtable or one of the immutable
    memtables that will be flushed together. Consider the sequence of such
    memtables M1, M2, Mn, where Mn is the mutable memtable:

    - k#t1 is in Mn: S2 waits for the flush of Mn. Can Mn concurrently get a
      higher seqnum write (of some other key) > t2 added to it before the
      flush. No, because we are holding commitPipeline.mu when assigning t2
      and while holding it, we mark Mn as immutable. So the flush of M1 … Mn
      has LSN(S1) < t2. Contradiction.

    - k#t1 is in M1: Mn becomes full and flushes together with M1. Can happen
      but will be fixed in https://github.com/cockroachdb/pebble/issues/2196
      by preventing any memtable with seqnum > t2 from flushing.

- S1 is an ingest and S2 is a flush: By definition LSN(S1)=t1. t1 >= t2 is not
  possible since by definition t1 < t2.

Claim 1 and claim 2 can together be used to prove the level invariant for the
seqnum-stack: We are given S1 is added before S2 and both have user key k,
with seqnum t1 and t2 respectively. From claim 1, t1 < t2. From claim 2,
LSN(S1) < LSN(s2). So the ordering based on the LSN will not violate the
seqnum invariant.

Since we have the level-invariant for both simple-stack and seqnum-stack,
reads are consistent across both.

A real LSM behaves as a hybrid of these two stacks, since there are
compactions out from L0 at arbitrary points in time. So any reordering of the
stack that is possible in the seqnum-stack may not actually happen, since
those files may no longer be in L0. This hybrid can be shown to be correct
since both the simple-stack and seqnum-stack are correct. This correctness
argument predates the introduction of sub-levels.

TODO(sumeer): proof of level-invariant for the hybrid.

Because of the seqnum-stack being equivalent to the simple-stack, we no longer
worry about future file additions to L0 and only consider what is currently in
L0. We focus on the seqnum-stack and the current L0, and how it is organized
into sub-levels. Sub-levels is a conceptually simple reorganization of the
seqnum-stack in that files that don't overlap in the keyspans, so
pessimistically cannot have conflicting keys, no longer need to stack up on
top of each other. This cannot violate the level invariant since the key span
(in terms of user keys) of S1 which is at a higher level than S2 despite LSN(s1)
< LSN(s2), must be non-overlapping.

2. L0=>Lbase compactions with sub-levels

They cut out a triangle from the bottom of the sub-levels, so will never
compact out a higher seqnum of k while leaving behind a lower seqnum. Once in
Lbase, the seqnums of the files play no part and only the keyspans are used
for future maintenance of the already established level invariant.
TODO(needed): more details.

3. Intra-L0 compactions with sub-levels

They cut out an inverted triangle from the top of the sub-levels. Correctness
here is more complicated because (a) the use of earliest-unflushed-seq-num,
(b) the ordering of output files and untouched existing files is based on
(LargestSeqNum, SmallestSeqNum, FileNum). We consider these in turn.

3.1 Use of earliest-unflushed-seq-num to exclude files

Consider a key span at sub-level i for which all files in the key-span have
LSN >= earliest-unflushed-seq-num (so are excluded). Extend this key span to
include any adjacent files on that sub-level that also have the same property,
then extend it until the end-bounds of the adjacent files that do not satisfy
this property. Consider the rectangle defined by this key-span going all the
way down to sub-level 0. And then start with that key-span in sub-level i-1.
In the following picture -- represents the key span in i and | bars represent
that rectangle defined.

i      +++++|----------------|++f2++
i-1       --|--------------++|++f1+++


We claim that the files in this key-span in sub-level i-1 that satisfy this
property cannot extend out of the key-span. This can be proved by
contradiction: if a file f1 at sub-level i-1 extends beyond, there must be a
file at sub-level i, say f2, that did not satisfy this property (otherwise the
maximal keyspan in i would have been wider). Now we know that
earliest-unflushed-seq-num > LSN(f2) and LSN(f1) >=
earliest-unflushed-seq-num. So LSN(f1) > LSN(f2) and they have overlapping
keyspans, which is not possible since f1 is in sub-level i-1 and f2 in
sub-level i. This argument can be continued to claim that the
earliest-unflushed-seq-num property cuts out an inverted triangle from the
sub-levels. Pretending these files are not in the history is ok, since the
final sub-levels will look the same if these were not yet known and we then
added them in the future in LSN order.

3.2 Ordering of output files

The actual files chosen for the intra-L0 compaction also follow the same
inverted triangle pattern. This means we have a contiguous history of the
seqnums for a key participating in the compaction, and anything not
participating has either lower or higher seqnums. The shape can look like the
following picture where . represents the spans chosen for the compaction and -
the spans ignored because of earliest-unflushed-seq-num and x that are older
and not participating.

6    ------------------------
5      --------------------
4       .....----.......
3	        ...........
2           ........
1     xxxxxxx....xxxxxxxxxxxxxxxx
0     xxxxxxxxxxxxxxxxxxxxxxxxx

We know the compaction input choice is sound, but the question is whether an
output . produced by the compaction can fall either too low, i.e., lower than
a conflicting x, or end up too high, above a conflicting -. This is because
the choice of sub-level depends on the LSN of the output and not the actual
conflicting key seqnums in the file (the LSN is just a summary). Claim 1 and 2
are insufficient to prove this. Those claims allow for the following sequence
of files (from higher to lower sub-level):

-  a#20       f3
.  a#10 b#15  f2
x  a#5  c#12  f1

If the compaction separates a#10 from b#15 in the output, a#10 can fall below
f1. To prove this cannot happen we need another claim.

Claim 3: if key k is in files S1 and S2, with k#t1, k#t2 with t1 < t2, then
LSN(S1) < t2.

Proof: We have proved this stronger claim when proving claim 2.

Based on claim 3, the above example is impossible, since it would require
LSN(f1) < 10.

Using claim 3 we can prove that even if the intra-L0 compaction writes one
userkey per output sst, the LSN of that output sst will be > the LSN of ssts
with the same userkey that are categorized as x.

Next we need to show that the output won't land higher than - with a
conflicting key, say when we produce a single output file. NB: the narrowest
possible outputs (considered in the previous paragraph, with one key per file)
were the risk in the output sinking too low, and the widest possible output
(considered now) is the risk in staying too high.

The file that was excluded (- file) with the conflicting key has LSN >=
earliest-unflushed-seq-num. By definition there is no point in any of the
participating files that is >= earliest-unflushed-seq-num. So the LSN of this
single output file is < earliest-unflushed-seq-num. Hence the output can't
land higher than the excluded file with the conflicting key.

*/
