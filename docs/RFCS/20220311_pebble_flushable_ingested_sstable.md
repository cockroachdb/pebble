- Feature Name: Flushable Ingested SSTable
- Status: in-progress
- Start Date: 2022-03-11
- Authors: Mufeez Amjad
- RFC PR: [#1586](https://github.com/cockroachdb/pebble/pull/1586)
- Pebble Issues: [#25](https://github.com/cockroachdb/pebble/issues/25)
- Cockroach Issues:

## Summary

To avoid a forced flush when ingesting SSTables that have an overlap with a
memtable, we lazily add the SSTs to the LSM as a `*flushableEntry` to
`d.mu.mem.queue`. This state is only persisted in memory until a flush occurs,
thus we require a WAL entry to replay the ingestion in the event of a crash.

## Motivation

Currently, if any of the SSTs that need to be ingested have an overlap with a
memtable, we
[wait](https://github.com/cockroachdb/pebble/blob/56c5aebe151977964db7e464bb6c87ebd3451bd5/ingest.go#L671)
for the memtable to be flushed before the ingestion can proceed. This is to
satisfy the invariant that newer entries (those in the ingested SSTs) in the LSM
have a higher sequence number than old entries (those in the memtables).

## Technical Design

The proposed design is mostly taken from Peter's suggestion in #25. The core
requirements are:
1. Replayable WAL entry for the ingest.
2. Implementation of the `flushable` interface for a new `ingestedSSTables` struct.
3. Lazily adding the ingested SSTs to the LSM.
4. Flushing logic to move SSTs into L0.

<br>

### 1. WAL Entry

We require a WAL entry to make the ingestion into the flushable queue
replayable, and there is a need for a new type of WAL entry that does not get
applied to the memtable. 2 approaches were considered:
1. Using `seqnum=0` to differentiate this new WAL entry.
2. Introduce a new `InternalKeyKind` for the new WAL entry,
   `InternalKeyKindIngestSST`.

I believe the second approach is better because it avoids modifying batch
headers, and also gives way for a cleaner implementation that is similar to the
treatment of `InternalKeyKindLogData`. It also follows the correct seqnum
semantics for SSTable ingestion in the event of a WAL replay — an ingestion
batch already gets one sequence number for all SSTs in it.

This change will need to be gated on a `FormatMajorVersion` because if the store
is opened with an older version of Pebble, Pebble will not understand any WAL
entry that contains the new `InternalKeyKind`.

<br>

When performing an ingest (with overlap), we create a batch with a record for
each ingested SST:

```
+-----------+-----------------+-------------------+
| Kind (1B) | Key (varstring) | Value (varstring) |
+-----------+-----------------+-------------------+
```

where the kind is `InternalKeyKindIngestSST`, and the key is a path to the ingested SST on disk.

`batch.Count` is the number of files ingested so that the record that follows in
the WAL has sequence number at `(batch.SeqNum + Batch.Count)`. The batch is
assigned a sequence number in the call to `d.commit.Commit(batch, sync)`, which
gets persisted to the WAL. When replaying, the sequence number from the entry
can be used to assign sequence numbers to the SSTs.

Since the batch is not applied to the memtable, and because
`InternalKeyKindIngestSSTs` cannot appear within batches created outside of
ingestion, we skip the call to `memtable.Apply()` in `d.commitApply`. Note that
`InternalKeyKindLogData` can appear amidst other records that are applied to the
memtable, making it different than `InternalKeyKindIngestSSTs`.

When replaying the WAL, we check every batch's first record and replay the
ingestion steps - we construct a `flushableEntry` and add it to the flushable
queue:

```go
b = Batch{db: d}
b.SetRepr(buf.Bytes())
seqNum := b.SeqNum()
maxSeqNum = seqNum + uint64(b.Count())
br := b.Reader()
if kind, _, _, _ := br.Next(); kind == InternalKeyKindIngestSST {
  // construct flushable of sstables with correct seqnum and add to queue
  buf.Reset()
  continue
}
```

<br>

An alternative to having one record is to have each ingested SST path as its own
record in the batch. `batch.Count` would still be the number of SSTs ingested
but the `mem.apply` code block above would *just work* without modifying
`seqNum`. However, this would require multiple calls to `BatchReader.Read()`
when replaying the WAL.

### 2. `flushable` Implementation

Introduce a new flushable type: `ingestedSSTables`.

```go
type ingestedSSTables struct {
    files []*fileMetadata
    size  uint64

    cmp      Compare
    newIters tableNewIters
}
```
which implements the following functions from the `flushable` interface:

#### 1. `newIter(o *IterOptions) internalIterator`

We return a `levelIter` since we can treat the ingested SSTs to be on the
same L0 sublevel.

```go
levelSlice := manifest.NewLevelSliceKeySorted(s.cmp, s.files)
return newLevelIter(*o, s.cmp, nil, s.newIters, levelSlice.Iter(), 0, nil)
```

<br>

On the client-side, this iterator would have to be used like this:
```go
var iter internalIteratorWithStats
var rangeDelIter keyspan.FragmentIterator
iter = base.WrapIterWithStats(mem.newIter(&dbi.opts))
switch mem.flushable.(type) {
case *ingestedSSTables:
    iter.(*levelIter).initRangeDel(&rangeDelIter)
default:
    rangeDelIter = mem.newRangeDelIter(&dbi.opts)
}

mlevels = append(mlevels, mergingIterLevel{
    iter:         iter,
    rangeDelIter: rangeDelIter,
})
```

#### 2. `newFlushIter(o *IterOptions, bytesFlushed *uint64) internalIterator`

#### 3. `newRangeDelIter(o *IterOptions) keyspan.FragmentIterator`

The above two methods would return `nil`. By doing so, in `c.newInputIter()`:
```go
iters = append(iters, f.newFlushIter(nil, &c.bytesIterated))
rangeDelIter := f.newRangeDelIter(nil)
if rangeDelIter != nil {
	iters = append(iters, rangeDelIter)
}
```
we ensure that no iterators on `ingestedSSTables` will be used while flushing in
`c.runCompaction`.

The special-cased flush process for this flushable is described in [Section
4](#4-flushing-logic-to-move-ssts-into-l0).

#### 4. `newRangeKeyIter(o *IterOptions) keyspan.FragmentIterator`

Will wait on range key support in `levelIter` to land before implementing.

#### 5. `inuseBytes() uint64` and `totalBytes() uint64`

For `inuseBytes()` return 0 so that the calculation of `c.maxOverlapBytes` is
not affected by the SSTs (the ingested SSTs don't participate in the
compaction). 

For `totalBytes()` return the sum of file sizes, can be cached when the struct
is initialized. `totalBytes()` is used for checking if the size of the flushable
queue is greater than the flush threshold.

We don't want the ingested SSTs to contribute to the size of the memtable when
determining whether or not to stall writes; they should contribute to the L0
read-amp instead. Thus, we'll have to special case for ingested SSTs in
`d.makeRoomForWrite()` to address this detail.

#### 6. `readyForFlush() bool`

The flushable of ingested SSTs can always be flushed, so we return true.

### 3. Lazily adding the ingested SSTs to the LSM

The steps to add the ingested SSTs to the flushable queue are:
1. Detect an overlap exists (existing logic).

Add a check that falls back to the old ingestion logic of blocking the ingest on
the flush when `len(d.mu.mem.queue) >= MemtablesStopWritesThreshold - 1`. This
reduces the chance that many short, overlapping, and successive ingestions cause
a memtable write stall.

2. Create a batch with the list of ingested SSTs.
```go
b := newBatch()
b.IngestSSTs([]byte(strings.Join(paths, ",")), nil)
```
3. Apply the batch.
The `prepare` step of the ingest is called with `d.commit.mu` held. Applying the
batch through `b.db.Apply(b)` reacquires `d.commit.mu`, so the lock must be
released before calling and reacquired after:

```go
d.commit.mu.Unlock()
err := b.Commit(nil)
d.commit.mu.Lock()
if err != nil {
  return
}
```

5. Create `ingestedSSTables` flushable and `flushableEntry`.
6. Add to flushable queue.

Pebble requires that the last entry in `d.mu.mem.queue` is the mutable memtable
with value `d.mu.mem.mutable`. When adding a `flushableEntry` to the queue, we
want to maintain this invariant. To do this we pass `nil` as the batch to
`d.makeRoomForWrite()`. The result is

```
| immutable old memtable | mutable new memtable |
```

We then append our new `flushableEntry`, and swap the last two elements in
`d.mu.mem.queue`:

```
| immutable old memtable | ingestedSSTables | mutable new memtable |
```

Because we add the ingested SSTs to the flushable queue when there is overlap,
we want to avoid applying the version edit through the regular execution flow in
the [apply
step](https://github.com/cockroachdb/pebble/blob/02418522e6467d4b235755f27440f35b366e3891/ingest.go#L676)
(`d.commit.AllocateSeqNum()`). Doing so would add the ingested SSTs twice to the
LSM (the other time being after `c.runCompaction()` finishes). We could skip the
`apply` step entirely, however, we still want to update the sequence numbers for
the ingested SSTs, but that needs to happen *before* they are added to the
memtable. This is to respect the sequence number ordering invariant while the
SSTs reside in the flushable queue.

7. Call `d.maybeScheduleFlush()`.

Because we've added an immutable memtable to the flushable queue and the
ingested SSTs would surpass the flush threshold, this will almost certainly
result in a flush.

### 4. Flushing logic to move SSTs into L0

By returning `nil` for both `flushable.newFlushIter()` and
`flushable.newRangeDelIter()`, the `ingestedSSTables` flushable will not be
flushed normally.

Instead, in `c.runCompaction()`, the ingested SSTs can be added to
`versionEdit.NewFiles` for the compaction:
```go
for _, f := range c.flushing {
  switch f.flushable.(type) {
  case *ingestedSSTables:
    files := f.flushable.(*ingestedSSTables).files
    for _, file := range files {
      ve.NewFiles = append(ve.NewFiles, newFileEntry{
        Level: 0,
        Meta:  file,
      })
    }
  }
}
```

The changes to this `versionEdit` will then be applied to the current version
through `d.mu.versions.logAndApply()`, added to a `BulkVersionEdit`, and applied
through `BulkVersionEdit.apply()` - the `L0Sublevels` data structure is thereafter
populated with the ingested SSTs.
