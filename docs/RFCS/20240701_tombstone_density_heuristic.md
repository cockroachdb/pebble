- Feature Name: Compaction heuristics for point tombstone density
- Status: in-progress
- Start Date: 2024-06-28
- Authors: Anish Shanbhag
- RFC PR: TODO
- Pebble Issues: https://github.com/cockroachdb/pebble/issues/918

**Design Draft **

# Summary

This design document outlines a proposal for improving compaction heuristics in Pebble, specifically targeting the reduction of point tombstone density to enhance read performance. It identifies the problem of high densities of point tombstones slowing down iteration due to unnecessary I/O and CPU usage. We propose various methods to identify regions of the LSM with high tombstone density and schedule the corresponding SSTables for compaction.

# Motivation

Pebble currently contains a variety of compaction heuristics which are mostly based on SSTable file size. One weak point in the compaction heuristics involves the case where we have a large buildup of close-together point tombstones, which causes reads for keys after the tombstone cluster to become extremely slow.

As identified [here](https://github.com/cockroachdb/pebble/issues/918#issuecomment-1564714073), uncompacted point tombstones reduce read performance in two ways: the extra CPU cycles needed to iterate over them during a seek, and the extra I/O needed to load more blocks which contain live keys.

It's important that we reduce tombstone density before the tombstones are read because there are many cases where tombstones are written for very long periods before any reads are triggered. 

Even though our current heuristics take the potential amount of reclaimed space from compacting point tombstones into account, there are specific situations where this is not sufficient to prevent point tombstone buildup. Specifically, these are some of the factors that lead to this issue:

1. Tombstone buildup usually only happens in large stores (> few hundred MB) once multiple levels of the LSM start to be populated.
2. The LSM is "too well shaped" in that the size ratio between levels is at the proper value, so [compaction scores](https://github.com/cockroachdb/pebble/blob/3ef2e5b1f693dfbf78785e14f603a443af3c674b/compaction_picker.go#L919) for each level are all calculated to be <0 and thus no compactions are scheduled.
	- Observed in [this escalation](https://github.com/cockroachlabs/support/issues/2628)
3. Read performance becomes especially bad when we have a high density of point tombstones in lower levels (L0-3) which span many SSTables in higher levels (L4-6).
4. The problem is especially apparent when there's one key range which we write to/delete from frequently and an adjacent key range which we read frequently.
	- RocksDB describes how [queues commonly contain this type of workload](https://github.com/facebook/rocksdb/wiki/Implement-Queue-Service-Using-RocksDB#reclaiming-space-of-deleted-items-faster).
	- We've seen this behavior multiple times with CockroachDB's implementation of the KV liveness range.
		- Observed in multiple escalations including [here](https://github.com/cockroachlabs/support/issues/2107) and [here](https://github.com/cockroachlabs/support/issues/2640) 
		- We had the raft log sandwiched between the frequently-read keys used for expiration-based leases. Nodes used to gossip their liveness every few seconds, which writes and deletes messages from the raft log over and over. This makes the raft log span numerous SSTables that fill up the cache with tombstones, removing the frequently-read lease expiration keys from the cache. Liveness has since been changed to use a different implementation but the root cause still exists here.
5. Tombstones build up more often when KV pairs are small in size because more KVs fit into a single SSTable. In this case, heuristics that measure the possibility of disk space reclamation don't work because the tombstones take up little space despite filling up the key space.
6. The problem is specific to `SeekGE` and `SeekLT` because we have to iterate over all keys for these operations.

# Design

Our ideal goal is to maintain an invariant in the LSM that during a seek, for every `N` live keys read, we have to read no more than some `T` tombstones. Here are some ways we can approximate this, in order of increasing complexity:

### 1. Tombstone Ratio

The simplest way to detect a buildup of point tombstones is to define some threshold percentage (`TOMBSTONE_THRESHOLD`) which indicates that any SSTable where `NumDeletions/NumEntries > TOMBSTONE_THRESHOLD` should be compacted. For example, if `TOMBSTONE_THRESHOLD = 0.6` an SSTable with 10,000 internal keys would be scheduled for compaction if it has at least 6000 tombstones.

- Pretty easy to implement
- Only considers tombstone density for one SSTable - overlaps with other tables aren't considered
- Could run into issues if we have a swath of point tombstones in a very large SSTable - they still slow down reads but aren't big compared to the overall number of keys in the table
- Probably insufficient to use on its own, but we could try combining this with other methods
	- For example, both [RocksDB](https://github.com/facebook/rocksdb/blob/main/include/rocksdb/utilities/table_properties_collectors.h#L53) and [ScyllaDB](https://enterprise.docs.scylladb.com/stable/kb/garbage-collection-ics.html) use this in combination with more complex strategies

### 2. More Granularity

If we find that more granularity is needed on a per-SSTable basis, i.e. it's important to know where tombstones are clustered within an SSTable, there are two possible options:
- Divide the key range of the SSTable into buckets of `X` keys and calculate how many have `>Y%` tombstones in them
	- If at least `Z` buckets are tombstone dense, compact this table
- Adapt the [sliding window approach from RocksDB](https://github.com/facebook/rocksdb/blob/22fe23edc89e9842ed72b613de172cd80d3b00da/utilities/table_properties_collectors/compact_on_deletion_collector.cc#L33)
	- RocksDB uses an approach where they "slide a window" across the SSTable keys and schedule compaction if the window has a high enough ratio of tombstones. In other words, while writing if there are ever at least `X` tombstones in the last `Y` keys, compact this table
	- We could adapt this 1:1 or try some modifications:
		- Allow the window to expand while tombstones are still being written in order to check the exact key ranges which are tombstone dense. Prioritize compaction of SSTables based on the length of its tombstone swaths

Like the `TOMBSTONE_THRESHOLD` strategy, this only considers single SSTables, so we can just calculate these metrics on the fly while writing the SSTable and immediately schedule them for compaction if they meet a density criteria.

### 3. Key Range Statistics

Both methods above only consider tombstone density on a per SSTable basis. We could have a situation where a single continuous key range actually spans tombstone swaths across many levels of the LSM, in which case looking at a single SSTable may not indicate that the key range is tombstone-dense.

In this case, we want the ability to query if a certain key range `a->b` is "tombstone dense", and if so then compact table(s) overlapping that range. The `manifest.Annotator` interface seems like a solid option to accumulate stats about tombstone density while keeping queries fast, since arbitrary key ranges can be contained in thousands of SSTables for large stores. We already have the `tombstoneAnnotator` introduced in cockroachdb#2327 which tracks the number of tombstones in each B-tree node's subtree; this could either be used as-is or modified to also track the total number of keys, etc. if needed.
- It looks like the current implementation of `Annotator` is set up mainly to allow computation of an annotation for an entire level, but I think it should be possible to write a helper that computes the annotation for a given range inside the level.

Side note: using an `Annotator` to aggregate key statistics above has the possible additional benefit of giving us a fast way to approximate [db.ScanStatistics](https://github.com/cockroachdb/pebble/blob/4981bd0e5e9538a032a4caf3a12d4571abb8c206/db.go#L2823). If we also keep track of other key counts, this could let us make arbitrary queries about the distribution of keys across the full key range of the LSM. Another possibility - we could generate a graph of the full key range on the x-axis, and histogram about various stats on the y-axis (tombstone count, count of other key types, #keys in L0 sstable, etc.) that would make it easy to see if part of the key range is anomalous compared to the rest of the range.

Here's a sketch of how the overall compaction process could look:
- After writing an SSTable, add this SSTable and all SSTables which overlap with its key range (using `version.Overlaps`) to a global set `needsTombstoneCheck` which marks them as possibly eligible for a tombstone density compaction
	- If the logic below ends up being fast enough, we could avoid having `needsTombstoneCheck` entirely and check whether compaction is needed during a write itself. But if not, we should defer the check in order to keep writes fast
- Inside [`pickAuto`](https://github.com/cockroachdb/pebble/blob/4981bd0e5e9538a032a4caf3a12d4571abb8c206/compaction_picker.go#L1324), we'll check whether any SSTable in `needsTombstoneCheck` should be compacted
	- Like elision-only and read-based compaction, we likely only want to do this check if there are no regular, size-based compactions that should be scheduled. Open question: how should these compactions be prioritized against elision-only and read-based compaction?
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
	- Note that stats about the whole LSM would be overestimates since we'd be including tombstones/keys from SSTables which only partially overlap the range. I think this should be fine?
	- We now have the 6 stats above about the SSTable's key range, not including any other possible queries we could also make. I'm still unsure about which of these are actually important, and what's the best way to reconcile these into a single signal how how strongly we want to compact this SSTable. My current intuition is that we want to avoid situations where we have a large number of tombstones overlapping a large number of keys in lower levels, so maybe we output a signal proportional to `this sstable's tombstone count / total key count of overlapping sstables`, or `this sstable's tombstone count / total key count of sstables overlapping with this sstable's tombstone cluster(s)` in the more granular case? Open to suggestions here. I'm also wondering if we should explicitly prioritize compaction of lower levels vs. higher levels (e.g. L0 vs. L5)
	- Similar to the check in read-based compaction, we'll want to make sure the SSTable still actually exists before compacting it, since it could have been compacted away between being added to `needsTombstoneCheck` and right now.

### 4. Maximum Granularity

If we find that the key range statistics method above works well but we want even more granularity for key ranges, i.e. because the overestimate of whole-LSM stats above becomes an issue, then we could include per-block tombstone/key counts in the index block of each SSTable, which would allow us to get a more precise count of tombstones for a given key range. This would look pretty similar to the logic separating partial vs. full overlaps in [`estimateReclaimedSizesBeneath`](https://github.com/cockroachdb/pebble/blob/master/table_stats.go#L606), except we'd be checking tombstone/key count instead of disk usage.
- If we store a running total of the tombstones for each block in the index entry, making this query would be O(log n) or faster, not including the I/O overhead of reading the index block

# Testing

Since we only see tombstone buildup in relatively large stores, testing should be representative of real workloads run in large clusters. Here are some potential ideas:
- We already have one test implemented thanks to cockroachdb#2657 which constructs a fully-populated key range and then deletes a swath of tombstones from the middle. Even though it's a bit artificial, this is a great way to check whether we can identify tombstone-dense key ranges
	- I've tested this locally as a sanity check, and on master branch there is a noticeable slowdown for reads as the tombstone cluster becomes larger, even after waiting a while for compactions to finish.
- Idea from [here](https://github.com/cockroachdb/pebble/issues/918#issuecomment-1599478862) - seed a new node with a replication snapshot, which has the tendency for tomstone buildup
- Simulate a scenario similar to the raft log, where we have a key range frequently written to/deleted from surrounded by key ranges that are frequently read
- cockroachdb/cockroach#113069 measures performance of liveness range scans since those were often slowed down by tombstones. Even though it looks like liveness logic has changed since then to avoid this, we could adapt the test to induce slow reads.
- Create reproduction roachtests for past slowdown scenarios we've observed
- RocksDB highlights that a [queue service built on top of a KV store](https://github.com/facebook/rocksdb/wiki/Implement-Queue-Service-Using-RocksDB) is a common situation where tombstones build up. We should write a test to capture performance for a queue
