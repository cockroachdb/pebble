# arenaskl

Fast, lock-free, arena-based Skiplist implementation in Go that supports iteration
in both directions.

## Advantages

Arenaskl offers several advantages over other skiplist implementations:

* High performance that linearly scales with the number of cores. This is
  achieved by allocating from a fixed-size arena and by avoiding locks.
* Iterators that can be allocated on the stack and easily cloned by value.
* Simple-to-use and low overhead model for detecting and handling race conditions
  with other threads.
* Support for iterating in reverse (i.e. previous links). 

## Limitations

The advantages come at a cost that prevents arenaskl from being a general-purpose
skiplist implementation:

* The size of the arena sets a hard upper bound on the combined size of skiplist
  nodes, keys, and values. This limit includes even the size of deleted nodes,
  keys, and values.
* Deletion is not supported. Instead, higher-level code is expected to
  add deletion tombstones and needs to process those tombstones
  appropriately.

## Pedigree

This code is based on Andy Kimball's arenaskl code:

https://github.com/andy-kimball/arenaskl

The arenaskl code is based on the skiplist found in Badger, a Go-based
KV store:

https://github.com/dgraph-io/badger/tree/master/skl

The skiplist in Badger is itself based on a C++ skiplist built for
Facebook's RocksDB:

https://github.com/facebook/rocksdb/tree/master/memtable

## Benchmarks

The benchmarks consist of a mix of reads and writes executed in parallel. The
fraction of reads is indicated in the run name: "frac_X" indicates a run where
X percent of the operations are reads.

The results are much better than `skiplist` and `slist`.

```
name                  time/op
ReadWrite/frac_0-8     470ns ±11%
ReadWrite/frac_10-8    462ns ± 3%
ReadWrite/frac_20-8    436ns ± 2%
ReadWrite/frac_30-8    410ns ± 2%
ReadWrite/frac_40-8    385ns ± 2%
ReadWrite/frac_50-8    360ns ± 4%
ReadWrite/frac_60-8    386ns ± 1%
ReadWrite/frac_70-8    352ns ± 2%
ReadWrite/frac_80-8    306ns ± 3%
ReadWrite/frac_90-8    253ns ± 4%
ReadWrite/frac_100-8  28.1ns ± 2%
IterNext-8            3.97ns ± 3%
IterPrev-8            3.93ns ± 2%
```
