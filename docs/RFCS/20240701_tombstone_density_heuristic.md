- Feature Name: Compaction heuristics for point tombstone density
- Status: done as of 2024-08-15
- Start Date: 2024-06-28
- Authors: Anish Shanbhag
- RFC PR: cockroachdb#3719
- Pebble Issues: https://github.com/cockroachdb/pebble/v2/issues/918

# Summary

This design document outlines a new compaction type in Pebble which specifically targets the reduction of point tombstone density to enhance read performance. The heuristic is designed to eliminate high densities of point tombstones which slow down iteration due to unnecessary I/O and CPU usage.

The new heuristic was introduced in cockroachdb#3790.

# Motivation

Pebble currently contains a variety of compaction heuristics which are mostly based on SSTable file size. One weak point in the compaction heuristics involves the case where we have a large buildup of close-together point tombstones, which causes reads for keys after the tombstone cluster to become extremely slow.

As identified [here](https://github.com/cockroachdb/pebble/v2/issues/918#issuecomment-1564714073), uncompacted point tombstones reduce read performance in two ways: the extra CPU cycles needed to iterate over them during a seek, and the extra I/O needed to load more blocks which contain live keys.

It's important that we reduce tombstone density before the tombstones are read because there are many cases where tombstones are written for very long periods before any reads are triggered. 

Even though our current heuristics take the potential amount of reclaimed space from compacting point tombstones into account, there are specific situations where this is not sufficient to prevent point tombstone buildup. Specifically, these are some of the factors that lead to this issue:

1. Tombstone buildup usually only happens in large stores (> 10 GB) once multiple levels of the LSM start to be populated.
2. The LSM is "too well shaped" in that the size ratio between levels is at the proper value, so [compaction scores](https://github.com/cockroachdb/pebble/v2/blob/3ef2e5b1f693dfbf78785e14f603a443af3c674b/compaction_picker.go#L919) for each level are all calculated to be <1 and thus no compactions are scheduled.
	- Observed in [this escalation](https://github.com/cockroachlabs/support/issues/2628) (internal only)
3. Read performance becomes especially bad when we have a high density of point tombstones in higher levels (L0-3) which span many SSTables in the bottommost levels (L4-6).
4. The problem is especially apparent when there's one key range which we write to/delete from frequently and an adjacent key range which we read frequently.
	- RocksDB describes how [queues commonly contain this type of workload](https://github.com/facebook/rocksdb/wiki/Implement-Queue-Service-Using-RocksDB#reclaiming-space-of-deleted-items-faster).
	- We've seen this behavior multiple times with CockroachDB's implementation of the KV liveness range.
		- Observed in multiple escalations including [here](https://github.com/cockroachlabs/support/issues/2107) and [here](https://github.com/cockroachlabs/support/issues/2640) (internal only)
		- We had the raft log sandwiched between the frequently-read keys used for expiration-based leases. Nodes used to gossip their liveness every few seconds, which writes and deletes messages from the raft log over and over. This makes the raft log span numerous SSTables that fill up the cache with tombstones, removing the frequently-read lease expiration keys from the cache. Liveness has since been changed to use a different implementation but the root cause still exists here.
	- Outbox/queue-based workloads also run into this issue because we continuously delete keys from the start of a range and add keys to the end of the range.
5. Tombstones build up more often when KV pairs are small in size because more KVs fit into a single SSTable. In this case, heuristics that measure the possibility of disk space reclamation don't work because the tombstones take up little space despite filling up the key space.
6. The problem is specific to operations including `SeekGE`/`SeekLT` and `Next`/`Prev` because we sequentially iterate over multiple keys during these. For example, if we `Next` into a swath of tombstones, we need to step through all of the tombstones before reaching a live key.

# Design

The new compaction heuristic introduces the concept of a "tombstone-dense" data block. A data block is considered tombstone-dense if it fulfills either of the following criteria:
1. The block contains at least `N` point tombstones. The default value for `N` is 100, and is controlled through a new option `options.Experimental.NumDeletionsThreshold`.
2. The ratio of the uncompressed size of point tombstones to the uncompressed size of the block is at least `Y`.  For example, with the default value of `0.5`, a data block of size 4KB would be considered tombstone-dense if it contains at least 2KB of point tombstones. The threshold is configured through a new option `options.Experimental.DeletionSizeRatioThreshold`.

The two criteria above are meant to eliminate wasteful data blocks that would consume unnecessary resources during reads. The count-based threshold prevents CPU waste, and the size-based threshold prevents I/O waste.

A table is considered eligible for the new tombstone compaction type if the percent of tombstone-dense data blocks (compared to the total number of data blocks in the table) is at least X%. The default is 5% and is configured via the new `options.Experimental.TombstoneDenseCompactionThreshold`. This option may also be set to zero or a negative value to disable tombstone density compactions.

Tombstone-dense data blocks are identified during sstable write time, and their count is recorded in a new table property called `NumTombstoneDenseBlocks`. During table stats collection, this property is used in conjunction with the `NumDataBlocks` property to calculate the new `TombstoneDenseBlocksRatio` table stat. If this calculated ratio is above `TombstoneDenseCompactionThreshold`, the table becomes eligible for a tombstone density compaction.

We use a `manifest.Annotator` in a similar way to elision-only compactions in order to prioritize compacting the table with the highest `TombstoneDenseBlocksRatio` if there are multiple eligible tables.

Tombstone density compactions are executed the same as a default compaction once the candidate file has been chosen. They are prioritized strictly below default compactions and strictly above read-triggered compactions.

## Design Exploration

Below are some ideas which were previously explored when designing this heuristic. Although the heuristic introduced in cockroachdb#3790 differs from these, the ideas may be useful if we still see this problem in the future.

<details>
<summary>(Expand to view)</summary>

### 1. Tombstone Ratio

**Note: this heuristic was implemented in cockroachdb#3793 and was found to be somewhat ineffective based the benchmarks tested there**

The simplest way to detect a buildup of point tombstones is to define some threshold percentage (`TOMBSTONE_THRESHOLD`) which indicates that any SSTable where `NumDeletions/NumEntries > TOMBSTONE_THRESHOLD` should be compacted. For example, if `TOMBSTONE_THRESHOLD = 0.6` an SSTable with 10,000 internal keys would be scheduled for compaction if it has at least 6000 tombstones.

- Only considers tombstone density for one SSTable - overlaps with other tables aren't considered
- Runs into issues if we have a swath of point tombstones in a very large SSTable - they still slow down reads but aren't big compared to the overall number of keys in the table
- Probably insufficient to use on its own, but we could try combining this with other methods
	- For example, both [RocksDB](https://github.com/facebook/rocksdb/blob/main/include/rocksdb/utilities/table_properties_collectors.h#L53) and [ScyllaDB](https://enterprise.docs.scylladb.com/stable/kb/garbage-collection-ics.html) use this in combination with more complex strategies

### 2. More Granularity

For more granularity on a per-SSTable basis, i.e. it's important to know where tombstones are clustered within an SSTable, there are two possible options:
- Divide the key range of the SSTable into buckets of `X` keys and calculate how many have `>Y%` tombstones in them
	- If at least `Z` buckets are tombstone dense, compact this table
- Adapt the [sliding window approach from RocksDB](https://github.com/facebook/rocksdb/blob/22fe23edc89e9842ed72b613de172cd80d3b00da/utilities/table_properties_collectors/compact_on_deletion_collector.cc#L33)
	- RocksDB uses an approach where they "slide a window" across the SSTable keys and schedule compaction if the window has a high enough ratio of tombstones. In other words, while writing if there are ever at least `X` tombstones in the last `Y` keys, compact this table
	- We could adapt this 1:1 or try some modifications:
		- Allow the window to expand while tombstones are still being written in order to check the exact key ranges which are tombstone dense. Prioritize compaction of SSTables based on the length of its tombstone swaths

Like the `TOMBSTONE_THRESHOLD` strategy, this only considers single SSTables, so we can just calculate these metrics on the fly while writing the SSTable and immediately schedule them for compaction if they meet a density criteria.

**Note: the heuristic we implemented in cockroachdb#3790 is an example of this category, as we measure tombstone density at block-level granularity with that approach.**

### 3. Key Range Statistics

Both methods above only consider tombstone density on a per SSTable basis. We could have a situation where a single continuous key range actually spans tombstone swaths across many levels of the LSM, in which case looking at a single SSTable may not indicate that the key range is tombstone-dense.

In this case, we want the ability to query if a certain key range `a->b` is "tombstone dense", and if so then compact table(s) overlapping that range. Range annotations which were introduced in cockroachdb#3759 would be a good fit for the implementation.

Given this method to query tombstone stats for arbitrary key ranges, here's a sketch of how the overall compaction process could look:
- After writing an SSTable, add this SSTable and all SSTables which overlap with its key range (using `version.Overlaps`) to a global set `needsTombstoneCheck` which marks them as possibly eligible for a tombstone density compaction
	- If the logic below ends up being fast enough, we could avoid having `needsTombstoneCheck` entirely and check whether compaction is needed during a write itself. But if not, we should defer the check in order to keep writes fast
- Inside [`pickAuto`](https://github.com/cockroachdb/pebble/v2/blob/4981bd0e5e9538a032a4caf3a12d4571abb8c206/compaction_picker.go#L1324), we'll check whether any SSTable in `needsTombstoneCheck` should be compacted
	- For each SSTable `T` in `needsTombstoneCheck`, we can get the following info using the `Annotator` and the table statistics we already have (assuming this SSTable spans the key range `a->b`:
		- This SSTable
			- number of tombstones in `T`
			- number of internal keys in `T`
		- Whole LSM
			- number of tombstones across the whole LSM between `a->b`
			- total number of internal keys across the whole LSM between `a->b`
			- Note: if we use the technique from \# 2 above to find the tombstone-dense range `m->n` within this SSTable, we could also get more granular stats:
				- number of tombstones across the whole LSM between `m->n`
				- number of internal keys across the whole LSM between `m->n`
	- We now have the 6 stats above about the SSTable's key range, not including any other possible queries we could also make. I'm still unsure about which of these are actually important, and what's the best way to reconcile these into a single signal how how strongly we want to compact this SSTable. My current intuition is that we want to avoid situations where we have a large number of tombstones overlapping a large number of keys in lower levels, so maybe we output a signal proportional to `this sstable's tombstone count / total key count of overlapping sstables`, or `this sstable's tombstone count / total key count of sstables overlapping with this sstable's tombstone cluster(s)` in the more granular case? Open to suggestions here. I'm also wondering if we should explicitly prioritize compaction of lower levels vs. higher levels (e.g. L0 vs. L5)
	- Similar to the check in read-based compaction, we'll want to make sure the SSTable still actually exists before compacting it, since it could have been compacted away between being added to `needsTombstoneCheck` and right now.

### 4. Maximum Granularity

If we find that the key range statistics method above works well but we want even more granularity for key ranges, i.e. because the overestimate of whole-LSM stats above becomes an issue, then we could include per-block tombstone/key counts in the index block of each SSTable, which would allow us to get a more precise count of tombstones for a given key range. This would look pretty similar to the logic separating partial vs. full overlaps in [`estimateReclaimedSizesBeneath`](https://github.com/cockroachdb/pebble/v2/blob/master/table_stats.go#L606), except we'd be checking tombstone/key count instead of disk usage.
- If we store a running total of the tombstones for each block in the index entry, making this query would be O(log n) or faster, not including the I/O overhead of reading the index block
</details>

# Testing

Testing methodology and results can be found [here](https://github.com/cockroachdb/pebble/v2/pull/3790#issuecomment-2251439492). Tombstone buildup has been frequently observed in queue-based workloads and the benchmark introduced in cockroachdb#3744 is meant to capture this case. Below are some further ideas for testing in case they're needed:
- Idea from [here](https://github.com/cockroachdb/pebble/v2/issues/918#issuecomment-1599478862) - seed a new node with a replication snapshot, which has the tendency for tomstone buildup
- cockroachdb/cockroach#113069 measures performance of liveness range scans since those were often slowed down by tombstones. Even though it looks like liveness logic has changed since then to avoid this, we could adapt the test to induce slow reads.
- Create reproduction roachtests for past slowdown scenarios we've observed
