# Implementation Details of Shared SSTables

This document describes the implementation details of shared SSTable functionality in Pebble. The current
implementation is more like a PoC and it may not work with all corner cases. However, the major body of this set of
functionalities has already been implemented and can be used for future reference, if needed.

Shared SSTable is part of the disaggregated storage rfc. For more details of the disaggregated storage concept and
details, you can refer to the following records:

- [RFC: disaggregated storage](https://github.com/cockroachdb/cockroach/pull/70419)
- [Virtual sstable proposal](https://github.com/cockroachdb/pebble/issues/1683)
- Disaggregated storage prototype (by Bilal Akhtar)
  - [CockroachDB](https://github.com/itsbilal/cockroach/tree/disagg-storage-prototype)
  - [Pebble](https://github.com/itsbilal/pebble/tree/disagg-storage-wip)

## Problem Setup

The disaggregated storage prototype has already added essential functions for storing L5/L6 SSTables in a Pebble
instance. When the `SharedFS` and `SharedDir` options are specified in a Pebble instance, the output SSTable of a
compaction will be moved to the shared file system, which is a `vfs`-like interface that represents a remote blob
storage (e.g., AWS S3). The shared storage is essentially shared amongst multiple Pebble instances, and the path a
SSTable is stored in an individual directory in the shared storage system with the creator Pebble's `UniqueID`, which
is a new property of a Pebble instance. Meanwhile, necessary functions in the CockroachDB codebase, like the cloud file
system wrapper, are implemented in the prototype.

With the prototype, Pebble supports a secondary file system that it can read and write. Also, with the updated logic in
the compaction routines, the majority of all the data (L5/L6 data, which can stand for up to 99% of all the data) are
stored in a remote file system.

Starting from here, there is still a significant part of the story that is not completed, the sharing of SSTables
between different Pebble instances that share the same secondary (shared) file system. By supporting SSTable sharing,
when one replica sends KV data to another replica, it does not need to scan all the KV pairs and send them over the
wire. Instead, it just need to send data for L0 to L4, which is only 1% of the total data set. For the data in L5 and
L6, it only sends some sort of metadata that represents a remote SSTable reference in the shared file system. The
receiver replica can just insert the reference into its MANIFEST and correctly retrieve the data in the future, without
writing real data into the disk. The benefits here are obvious - significantly reduing the mass of data tansfer when
sending data from one replica to another. This is especially useful for range rebalancing, or adding new nodes into a
range replication.

To this end, we need to implement the following functionalities:

- Part 1: A new virtual representation of a SSTable, that only appears in the MANIFEST but does not correspond to a
  local file. It corresponds to an SSTable created by another Pebble instance and it is stored in the foreign Pebble's
  directory in the shared file system. In the meantime, the local Pebble instance may only read a portion of the data
  in the physical SSTable, so the virtual SSTable may have a different key boundary. Note that the virtual key boundary
  must be inside the physical key boundary.

- Part 2: A new internal iterator that handles local SSTables and shared SSTables differently. It follows the old
  routines for local SSTables. For imported shared SSTables, it needs to maintain compatibility of the key visibility
  and sequence numbers, because multiple LSM-Trees that share the same SSTable usually have different data and shapes.

- Part 3: A new export and ingestion function that can let Pebble actually share data in action without sending real KV
  data for shared SSTables if those SSTables are shared by the two instances. A new struct of shared SSTable metadata is
  needed to represent a slice of virtual SSTables that correspond to multiple shared SSTables. In addition, the user
  needs a special type of iterator that can expose the underlying SSTable information to a limited extent so that the
  exported KV data/metadata can be constructed by the user correctly.

## Major Updates

The major modifications are in these files/directories:

```
─── pebble/
    ├── internal/
    │   └── manifest/
    │       ├── version.go
    │       └── version_edit.go
    ├── sstable/
    │   ├── reader.go
    │   ├── shared.go
    │   └── shared_test.go
    ├── compaction.go
    ├── disagg_test.go
    └── ingest.go
```

The remaining contents in the commit history are mainly test case updates or minor, non-structural changes. Next, I
will document the major changes by different parts of the functionalities, as said in the previous section.

### Part 1: Virtual SSTable Representation

A virtual SSTable has a smaller boundary than the file's key boundary. To simplify the implementation, the `Smallest`
and `Largest` property of `FileMetadata` struct is reused to encode the virtual boundary. This guarantees the
compatibility with existing codebase, like the tree structure of a level.

In `version.go`, the `FileMetadata` struct has more updated fields to record necessary metadata that defines:

```golang
type FileMetadata struct {
    ...

    // IsShared indicates whether the file is local-only or shared
    // among Pebble instances
    IsShared bool

    // CreatorUniqueID is the sst creator's UniqueID.
    // This is used in MakeSharedSSTPath
    CreatorUniqueID uint32

    // PhysicalFileNum is the file's file num when it is created
    // This is used in MakeSharedSSTPath
    PhysicalFileNum base.FileNum

    // FileSmallest and FileLargest record the key boundaries of the file
    // instead of the virtual boundaries that the current Pebble instance
    // can read
    FileSmallest InternalKey
    FileLargest  InternalKey
}
```

When `IsShared` is set to `true`, this file may be a externally created, virtual SSTable, or a locally created SSTable
but can be future shared with other Pebble instances. The `CreatorUniqueID` and `PhysicalFileNum` are the creator's
`UniqueID` and `FileNum` (of that SSTable). Specifically, the `FileNum` of a shared SSTable may be different from
the `PhysicalFileNum` on a virtual SSTable, as it is imported from another Pebble instance. Finally, the `FileSmallest`
and `FileLargest` cache the file key boundary of the SSTable file in the file system, and a virtual SSTable must
satisfy the invariant where the virtual boundaries are within the physical boundaries.

The `version_edit.go` modifications implement the storing/loading of the corresponding `FileMetadata` updates in a
`VersionEdit`.

### Part 2: Interal Iterators for Shared SSTable

When iterating over a SSTable internally, in `sstable/reader.go` there are already two kinds of iterators to serve this
purpose, the `singleLevelIterator` and the `twoLevelIterator`. On top of them, the `compactionIter` and
`twoLevelCompactionIter` wrapped an internal single/two-level iterator and also record the bytes that have been
iterated over for compaction metrics.

```golang
type tableIterator struct {
    Iterator
    rangeDelIter keyspan.FragmentIterator
}
```

Inside `sstable/shared.go`, a new wrapper iterator, namely `tableIterator`, is implemented. The `tableIterator` wraps
around a single/two-level iterator, and implements the `InternalIterator`/`Iterator` interface. The main job of this
struct is twofold. First, it applies virtual SSTable boundaries on the fly. Take the `SeekGE` function as an example:

```golang
func (i *tableIterator) seekGEShared(
    prefix, key []byte, flags base.SeekGEFlags,
) (*InternalKey, []byte) {
    r := i.getReader()
    ib := i.cmpSharedBound(key)
    if ib > 0 {
        // The search key overflows
        i.setExhaustedBounds(+1)
        return nil, nil
    } else if ib < 0 {
        // The search key underflows, substitute it with the lower shared bound
        key = r.meta.Smallest.UserKey
    }

    ...
}
```

At the beginning, the function checks whether the search key is inside or outside the virtual SSTable boundary. If it
overflows, `nil` is returned. If it underflows, the search key will be substituted by the lower bound and passed into
the underlying wrapped iterator.

The second important job is to maintain sequence number compatibility. The details can be found in the disaggregated
storage rfc (`Reads -> Manufacturing sequence numbers for reads -> Solution`). In general, L5 point keys use sequence
number of 2, L5 range deletes use 1 and all L6 keys use 0. To this end, a `tableIterator` maintains an internal
`FragmentIterator` to iterate over L5 range deletes and pre-apply them. For each key, according to the rfc, only the
latest version of a key is exposed. As a result, a reposition of a `tableIterator` will automatically point to the
latest version of a key.

The `Iterator` struct in the `sstable` package also requires an extra pair of `SetLevel` and `GetLevel` function.
Therefore, from the level iterator layer, the level of the SSTable that an iterator is reading can be passed in. See
the `newIters` function in `table_cache.go`.

In `sstable/shared_test.go`, a unit test is implemented.

### Part 3: Export and Ingestion

The implementation also includes a set of updates to the `Ingest` function of a Pebble instance. When two Pebble
instances share the same secondary storage and one instance (the sender) wants to send data to another instance (the
receiver), it only needs to send L0-L4 data. For L5-L6, only metadata (a serialized representation of a set of virtual
SSTables) needs to be sent.

To this end, the `Ingest` function now supports a secondary parameter:

```golang
func (d *DB) Ingest(paths []string, smeta []SharedSSTMeta) error
```

where the `SharedSSTMeta` is a new struct that contains essential information to define a virtual (shared) SSTable.
The struct is defined as:

```golang
type SharedSSTMeta struct {
    CreatorUniqueID uint32
    PhysicalFileNum base.FileNum
    Smallest        InternalKey
    Largest         InternalKey
    FileSmallest    InternalKey
    FileLargest     InternalKey
}
```

It is also worth noting that the `ingestTargetLevel` function is updated accordingly. In general, L0-L4 SSTables can
only be ingested into L0-L4, and L5-L6 shared SSTables can only be ingested into L5 or L6. In addition, since now the
exported SSTables are not completely sequential (i.e., constructed by an iterator), when ingesting local SSTables and
shared SSTables at the same time, there might be overlaps in the ingested keys. As a result, currently, it is needed to
ingest local SSTables and shared SSTables separately (i.e., in two `Ingest` calls).

To let the user construct exported SSTable data easily, a new set of options are added to user iterators:

```golang
type IterOptions struct {
    ...

    // SkipSharedFile determines whether the iterator will read shared sstables during
    // its iteration. If the callback function is not nil, it will pass in the
    // FileMetadata of the shared table for internal usage (e.g., constructing msg)
    SkipSharedFile     bool
    SharedFileCallback func(*manifest.FileMetadata)

    ...
}
```

When the `SkipSharedFile` flag is set, the user iterator will skip shared SSTables when iterating over a range of user
keys. In the meantime, if `SharedFileCallback` is not `nil`, it will be called and the corresponding `FileMetadata` of
that shared SSTable will be passed into the function so that the user can construct the slice of `SharedSSTMeta`. If a
shared SSTable is not accessed by the level iterator, its (meta)data will not be exposed to the callback function. A
typical usage of these extra options is constructing an iterator and let a writer write in-range data into some
to-be-exported SSTables, then construct the slice of `SharedSSTMeta` when shared SSTables are accessed.

In `disagg_test.go`, two unit tests are implemented. The `TestDBWithSharedSST` tests the overall shared SSTable
correctness, and the `TestIngestSharedSST` uses two Pebble instances and a contiguous set of integer keys to test the
ingestion of shared SSTables.

## Future Work

1. **CockroachDB Integration**. This includes two parts. The first part is to let CRDB export states/snapshots using shared
   SSTable functionalities inside Pebble if two replicas share the same shared storage. Another import part is
   shared reference count and obsolete data removal (GC). Currently, the removal of shared SSTables are disabled on
   both its creator instance or any instance that shares it. The reason is that one node may exit the replication group
   later but another one which is sharing some SSTables with it may not. Currently, there is no ownership concept in
   shared SSTables, so no instance will initiate a deletion of shared SSTables (it will only be removed from a new
   `Version`, if needed, but the files will always be there). A shared SSTable can be deleted only when no Pebble
   instance is referencing it any more. This can be done by using a standalone thread (service) that maintains
   reference information and performs background GC. Another potential solution is introducing a new Raft log type that
   synchronizes the reference information amongst replicas, and the last dereferencer can safely remove the file on the
   shared storage. This is feasible as virtual SSTables, in the context of CRDB, are usually organized in the unit of
   key ranges. As a result, the reference information is part of a range's distributed state.

2. **Range Key Support**. Currently, the implementation of shared SSTables assume that only point keys and range
   deletes can appear in the system. As range keys are introduced, the implementation needs to be extended to further
   adopt them.

3. **Optimal Caching**. To let disaggregated storage enabled CRDB has throughput on-par with vanilla CRDB, we need a
   viable and performant caching solution. It can be a per-LSM cache for shared SSTables, or a new distributed cache
   service. Currently, if we enable a large enough persistent cache on each node that can store all the shared states,
   the shared SSTable functionality can greatly accelerate range state replication. The shared storage overhead is
   amortized by later read requests (aka. lazy loading). This can be further optimized.


