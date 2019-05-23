# DO NOT USE: Pebble is an incomplete work-in-progress.

# Pebble [![Build Status](https://travis-ci.org/petermattis/pebble.svg?branch=master)](https://travis-ci.org/petermattis/pebble)

Pebble is a LevelDB/RocksDB inspired key-value store focused on
performance and internal usage by CockroachDB. Pebble inherits the
RocksDB file formats and a few extensions such as range deletion
tombstones, table-level bloom filters, and updates to the MANIFEST
format.

Pebble intentionally does not aspire to include every feature in
RocksDB and is specifically targetting the use case and feature set
needed by CockroachDB:

* Block-based tables
* Indexed batches
* Iterator options (lower/upper bound, table filter)
* Level-based compaction
* Manual compaction
* Merge operator
* Prefix bloom filters
* Range deletion tombstones
* Prefix iteration
* Reverse iteration
* Snapshots
* SSTable ingestion
* Table-level bloom filters

RocksDB has a large number of features that are not implemented in
Pebble:

* Backups and checkpoints
* Column families
* Delete files in range
* FIFO compaction style
* Forward iterator / tailing iterator
* Hash table format
* Memtable bloom filter
* Persistent cache
* Pin iterator key / value
* Plain table format
* Single delete
* SSTable ingest-behind
* Sub-compactions
* Transactions
* Universal compaction style

Pebble may silently corrupt data or behave incorrectly if used with a
RocksDB database that uses a feature Pebble doesn't support. Caveat
emptor!

## Advantages

Pebble offers several improvements over RocksDB:

* Faster reverse iteration via backwards links in the memtable's
  skiplist.
* Faster commit pipeline that achieves better concurrency.
* Seamless merged iteration of indexed batches. The mutations in the
  batch conceptually occupy another memtable level.
* Smaller, more approachable code base.
* [[TODO]](https://github.com/petermattis/pebble/issues/7) Pacing of
  user writes vs flushes vs compactions.

## Pedigree

Pebble is based on the incomplete Go version of LevelDB:

https://github.com/golang/leveldb

The Go version of LevelDB is based on the C++ original:

https://github.com/google/leveldb

Optimizations and inspiration were drawn from RocksDB:

https://github.com/facebook/rocksdb
