// Copyright 2018 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package pebble

import (
	"encoding/binary"
	"fmt"
	"io"
	"slices"
	"strconv"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/bytealloc"
	"github.com/cockroachdb/pebble/internal/compact"
	"github.com/cockroachdb/pebble/internal/invariants"
	"github.com/cockroachdb/pebble/internal/keyspan"
	"github.com/cockroachdb/pebble/internal/rangekey"
	"github.com/cockroachdb/redact"
)

// compactionIter provides a forward-only iterator that encapsulates the logic
// for collapsing entries during compaction. It wraps an internal iterator and
// collapses entries that are no longer necessary because they are shadowed by
// newer entries. The simplest example of this is when the internal iterator
// contains two keys: a.PUT.2 and a.PUT.1. Instead of returning both entries,
// compactionIter collapses the second entry because it is no longer
// necessary. The high-level structure for compactionIter is to iterate over
// its internal iterator and output 1 entry for every user-key. There are four
// complications to this story.
//
// 1. Eliding Deletion Tombstones
//
// Consider the entries a.DEL.2 and a.PUT.1. These entries collapse to
// a.DEL.2. Do we have to output the entry a.DEL.2? Only if a.DEL.2 possibly
// shadows an entry at a lower level. If we're compacting to the base-level in
// the LSM tree then a.DEL.2 is definitely not shadowing an entry at a lower
// level and can be elided.
//
// We can do slightly better than only eliding deletion tombstones at the base
// level by observing that we can elide a deletion tombstone if there are no
// sstables that contain the entry's key. This check is performed by
// elideTombstone.
//
// 2. Merges
//
// The MERGE operation merges the value for an entry with the existing value
// for an entry. The logical value of an entry can be composed of a series of
// merge operations. When compactionIter sees a MERGE, it scans forward in its
// internal iterator collapsing MERGE operations for the same key until it
// encounters a SET or DELETE operation. For example, the keys a.MERGE.4,
// a.MERGE.3, a.MERGE.2 will be collapsed to a.MERGE.4 and the values will be
// merged using the specified Merger.
//
// An interesting case here occurs when MERGE is combined with SET. Consider
// the entries a.MERGE.3 and a.SET.2. The collapsed key will be a.SET.3. The
// reason that the kind is changed to SET is because the SET operation acts as
// a barrier preventing further merging. This can be seen better in the
// scenario a.MERGE.3, a.SET.2, a.MERGE.1. The entry a.MERGE.1 may be at lower
// (older) level and not involved in the compaction. If the compaction of
// a.MERGE.3 and a.SET.2 produced a.MERGE.3, a subsequent compaction with
// a.MERGE.1 would merge the values together incorrectly.
//
// 3. Snapshots
//
// Snapshots are lightweight point-in-time views of the DB state. At its core,
// a snapshot is a sequence number along with a guarantee from Pebble that it
// will maintain the view of the database at that sequence number. Part of this
// guarantee is relatively straightforward to achieve. When reading from the
// database Pebble will ignore sequence numbers that are larger than the
// snapshot sequence number. The primary complexity with snapshots occurs
// during compaction: the collapsing of entries that are shadowed by newer
// entries is at odds with the guarantee that Pebble will maintain the view of
// the database at the snapshot sequence number. Rather than collapsing entries
// up to the next user key, compactionIter can only collapse entries up to the
// next snapshot boundary. That is, every snapshot boundary potentially causes
// another entry for the same user-key to be emitted. Another way to view this
// is that snapshots define stripes and entries are collapsed within stripes,
// but not across stripes. Consider the following scenario:
//
//	a.PUT.9
//	a.DEL.8
//	a.PUT.7
//	a.DEL.6
//	a.PUT.5
//
// In the absence of snapshots these entries would be collapsed to
// a.PUT.9. What if there is a snapshot at sequence number 7? The entries can
// be divided into two stripes and collapsed within the stripes:
//
//	a.PUT.9        a.PUT.9
//	a.DEL.8  --->
//	a.PUT.7
//	--             --
//	a.DEL.6  --->  a.DEL.6
//	a.PUT.5
//
// All of the rules described earlier still apply, but they are confined to
// operate within a snapshot stripe. Snapshots only affect compaction when the
// snapshot sequence number lies within the range of sequence numbers being
// compacted. In the above example, a snapshot at sequence number 10 or at
// sequence number 5 would not have any effect.
//
// 4. Range Deletions
//
// Range deletions provide the ability to delete all of the keys (and values)
// in a contiguous range. Range deletions are stored indexed by their start
// key. The end key of the range is stored in the value. In order to support
// lookup of the range deletions which overlap with a particular key, the range
// deletion tombstones need to be fragmented whenever they overlap. This
// fragmentation is performed by keyspan.Fragmenter. The fragments are then
// subject to the rules for snapshots. For example, consider the two range
// tombstones [a,e)#1 and [c,g)#2:
//
//	2:     c-------g
//	1: a-------e
//
// These tombstones will be fragmented into:
//
//	2:     c---e---g
//	1: a---c---e
//
// Do we output the fragment [c,e)#1? Since it is covered by [c-e]#2 the answer
// depends on whether it is in a new snapshot stripe.
//
// In addition to the fragmentation of range tombstones, compaction also needs
// to take the range tombstones into consideration when outputting normal
// keys. Just as with point deletions, a range deletion covering an entry can
// cause the entry to be elided.
//
// A note on the stability of keys and values.
//
// The stability guarantees of keys and values returned by the iterator tree
// that backs a compactionIter is nuanced and care must be taken when
// referencing any returned items.
//
// Keys and values returned by exported functions (i.e. First, Next, etc.) have
// lifetimes that fall into two categories:
//
// Lifetime valid for duration of compaction. Range deletion keys and values are
// stable for the duration of the compaction, due to way in which a
// compactionIter is typically constructed (i.e. via (*compaction).newInputIter,
// which wraps the iterator over the range deletion block in a noCloseIter,
// preventing the release of the backing memory until the compaction is
// finished).
//
// Lifetime limited to duration of sstable block liveness. Point keys (SET, DEL,
// etc.) and values must be cloned / copied following the return from the
// exported function, and before a subsequent call to Next advances the iterator
// and mutates the contents of the returned key and value.
type compactionIter struct {
	cmp   Compare
	equal Equal
	merge Merge
	iter  internalIterator
	err   error
	// `key.UserKey` is set to `keyBuf` caused by saving `i.iterKV.UserKey`
	// and `key.Trailer` is set to `i.iterKV.Trailer`. This is the
	// case on return from all public methods -- these methods return `key`.
	// Additionally, it is the internal state when the code is moving to the
	// next key so it can determine whether the user key has changed from
	// the previous key.
	key InternalKey
	// keyTrailer is updated when `i.key` is updated and holds the key's
	// original trailer (eg, before any sequence-number zeroing or changes to
	// key kind).
	keyTrailer  uint64
	value       []byte
	valueCloser io.Closer
	// Temporary buffer used for storing the previous user key in order to
	// determine when iteration has advanced to a new user key and thus a new
	// snapshot stripe.
	keyBuf []byte
	// Temporary buffer used for storing the previous value, which may be an
	// unsafe, i.iter-owned slice that could be altered when the iterator is
	// advanced.
	valueBuf []byte
	// Is the current entry valid?
	valid            bool
	iterKV           *base.InternalKV
	iterValue        []byte
	iterStripeChange stripeChangeType
	// `skip` indicates whether the remaining entries in the current snapshot
	// stripe should be skipped or processed. `skip` has no effect when `pos ==
	// iterPosNext`.
	skip bool
	// `pos` indicates the iterator position at the top of `Next()`. Its type's
	// (`iterPos`) values take on the following meanings in the context of
	// `compactionIter`.
	//
	// - `iterPosCur`: the iterator is at the last key returned.
	// - `iterPosNext`: the iterator has already been advanced to the next
	//   candidate key. For example, this happens when processing merge operands,
	//   where we advance the iterator all the way into the next stripe or next
	//   user key to ensure we've seen all mergeable operands.
	// - `iterPosPrev`: this is invalid as compactionIter is forward-only.
	pos iterPos
	// `snapshotPinned` indicates whether the last point key returned by the
	// compaction iterator was only returned because an open snapshot prevents
	// its elision. This field only applies to point keys, and not to range
	// deletions or range keys.
	snapshotPinned bool
	// forceObsoleteDueToRangeDel is set to true in a subset of the cases that
	// snapshotPinned is true. This value is true when the point is obsolete due
	// to a RANGEDEL but could not be deleted due to a snapshot.
	//
	// NB: it may seem that the additional cases that snapshotPinned captures
	// are harmless in that they can also be used to mark a point as obsolete
	// (it is merely a duplication of some logic that happens in
	// Writer.AddWithForceObsolete), but that is not quite accurate as of this
	// writing -- snapshotPinned originated in stats collection and for a
	// sequence MERGE, SET, where the MERGE cannot merge with the (older) SET
	// due to a snapshot, the snapshotPinned value for the SET is true.
	//
	// TODO(sumeer,jackson): improve the logic of snapshotPinned and reconsider
	// whether we need forceObsoleteDueToRangeDel.
	forceObsoleteDueToRangeDel bool
	// The index of the snapshot for the current key within the snapshots slice.
	curSnapshotIdx    int
	curSnapshotSeqNum uint64
	// The snapshot sequence numbers that need to be maintained. These sequence
	// numbers define the snapshot stripes (see the Snapshots description
	// above). The sequence numbers are in ascending order.
	snapshots compact.Snapshots
	// frontiers holds a heap of user keys that affect compaction behavior when
	// they're exceeded. Before a new key is returned, the compaction iterator
	// advances the frontier, notifying any code that subscribed to be notified
	// when a key was reached. The primary use today is within the
	// implementation of compactionOutputSplitters in compaction.go. Many of
	// these splitters wait for the compaction iterator to call Advance(k) when
	// it's returning a new key. If the key that they're waiting for is
	// surpassed, these splitters update internal state recording that they
	// should request a compaction split next time they're asked in
	// [shouldSplitBefore].
	frontiers compact.Frontiers
	// The fragmented tombstones.
	tombstones []keyspan.Span
	// The fragmented range keys.
	rangeKeys []keyspan.Span
	// Byte allocator for the tombstone keys.
	alloc                                  bytealloc.A
	allowZeroSeqNum                        bool
	elideTombstone                         func(key []byte) bool
	elideRangeTombstone                    func(start, end []byte) bool
	ineffectualSingleDeleteCallback        func(userKey []byte)
	singleDeleteInvariantViolationCallback func(userKey []byte)
	// The on-disk format major version. This informs the types of keys that
	// may be written to disk during a compaction.
	formatVersion FormatMajorVersion
	stats         struct {
		// count of DELSIZED keys that were missized.
		countMissizedDels uint64
	}
}

func newCompactionIter(
	cmp Compare,
	equal Equal,
	merge Merge,
	iter internalIterator,
	snapshots compact.Snapshots,
	allowZeroSeqNum bool,
	elideTombstone func(key []byte) bool,
	elideRangeTombstone func(start, end []byte) bool,
	ineffectualSingleDeleteCallback func(userKey []byte),
	singleDeleteInvariantViolationCallback func(userKey []byte),
	formatVersion FormatMajorVersion,
) *compactionIter {
	i := &compactionIter{
		cmp:                                    cmp,
		equal:                                  equal,
		merge:                                  merge,
		iter:                                   iter,
		snapshots:                              snapshots,
		allowZeroSeqNum:                        allowZeroSeqNum,
		elideTombstone:                         elideTombstone,
		elideRangeTombstone:                    elideRangeTombstone,
		ineffectualSingleDeleteCallback:        ineffectualSingleDeleteCallback,
		singleDeleteInvariantViolationCallback: singleDeleteInvariantViolationCallback,
		formatVersion:                          formatVersion,
	}
	i.frontiers.Init(cmp)
	return i
}

func (i *compactionIter) First() (*InternalKey, []byte) {
	if i.err != nil {
		return nil, nil
	}
	i.iterKV = i.iter.First()
	if i.iterKV != nil {
		i.iterValue, _, i.err = i.iterKV.Value(nil)
		if i.err != nil {
			return nil, nil
		}
		i.curSnapshotIdx, i.curSnapshotSeqNum = i.snapshots.IndexAndSeqNum(i.iterKV.SeqNum())
	}
	i.pos = iterPosNext
	i.iterStripeChange = newStripeNewKey
	return i.Next()
}

func (i *compactionIter) Next() (*InternalKey, []byte) {
	if i.err != nil {
		return nil, nil
	}

	// Close the closer for the current value if one was open.
	if i.closeValueCloser() != nil {
		return nil, nil
	}

	// Prior to this call to `Next()` we are in one of three situations with
	// respect to `iterKey` and related state:
	//
	// - `!skip && pos == iterPosNext`: `iterKey` is already at the next key.
	// - `!skip && pos == iterPosCurForward`: We are at the key that has been returned.
	//   To move forward we advance by one key, even if that lands us in the same
	//   snapshot stripe.
	// - `skip && pos == iterPosCurForward`: We are at the key that has been returned.
	//   To move forward we skip skippable entries in the stripe.
	if i.pos == iterPosCurForward {
		if i.skip {
			i.skipInStripe()
		} else {
			i.nextInStripe()
		}
	} else if i.skip {
		panic(errors.AssertionFailedf("compaction iterator has skip=true, but iterator is at iterPosNext"))
	}

	i.pos = iterPosCurForward
	i.valid = false

	for i.iterKV != nil {
		// If we entered a new snapshot stripe with the same key, any key we
		// return on this iteration is only returned because the open snapshot
		// prevented it from being elided or merged with the key returned for
		// the previous stripe. Mark it as pinned so that the compaction loop
		// can correctly populate output tables' pinned statistics. We might
		// also set snapshotPinned=true down below if we observe that the key is
		// deleted by a range deletion in a higher stripe or that this key is a
		// tombstone that could be elided if only it were in the last snapshot
		// stripe.
		i.snapshotPinned = i.iterStripeChange == newStripeSameKey

		if i.iterKV.Kind() == InternalKeyKindRangeDelete || rangekey.IsRangeKey(i.iterKV.Kind()) {
			// Return the span so the compaction can use it for file truncation and add
			// it to the relevant fragmenter. In the case of range deletions, we do not
			// set `skip` to true before returning as there may be any number of point
			// keys with the same user key and sequence numbers ≥ the range deletion's
			// sequence number. Such point keys must be visible (i.e., not skipped
			// over) since we promise point keys are not deleted by range tombstones at
			// the same sequence number (or higher).
			//
			// Note that `skip` must already be false here, because range keys and range
			// deletions are interleaved at the maximal sequence numbers and neither will
			// set `skip`=true.
			if i.skip {
				panic(errors.AssertionFailedf("pebble: compaction iterator: skip unexpectedly true"))
			}

			// NOTE: there is a subtle invariant violation here in that calling
			// saveKey and returning a reference to the temporary slice violates
			// the stability guarantee for range deletion keys. A potential
			// mediation could return the original iterKey and iterValue
			// directly, as the backing memory is guaranteed to be stable until
			// the compaction completes. The violation here is only minor in
			// that the caller immediately clones the range deletion InternalKey
			// when passing the key to the deletion fragmenter (see the
			// call-site in compaction.go).
			// TODO(travers): address this violation by removing the call to
			// saveKey and instead return the original iterKey and iterValue.
			// This goes against the comment on i.key in the struct, and
			// therefore warrants some investigation.
			i.saveKey()
			// TODO(jackson): Handle tracking pinned statistics for range keys
			// and range deletions. This would require updating
			// emitRangeDelChunk and rangeKeyCompactionTransform to update
			// statistics when they apply their own snapshot striping logic.
			i.snapshotPinned = false
			i.value = i.iterValue
			i.valid = true
			return &i.key, i.value
		}

		// Check if the last tombstone covers the key.
		// TODO(sumeer): we could avoid calling tombstoneCovers if
		// i.iterStripeChange == sameStripeSameKey since that check has already been
		// done in nextInStripeHelper. However, we also need to handle the case of
		// CoversInvisibly below.
		switch i.tombstoneCovers(i.iterKV.K, i.curSnapshotSeqNum) {
		case coversVisibly:
			// A pending range deletion deletes this key. Skip it.
			i.saveKey()
			i.skipInStripe()
			continue

		case coversInvisibly:
			// i.iterKV would be deleted by a range deletion if there weren't any open
			// snapshots. Mark it as pinned.
			//
			// NB: there are multiple places in this file where we check for a
			// covering tombstone and this is the only one where we are writing to
			// i.snapshotPinned. Those other cases occur in mergeNext where the caller
			// is deciding whether the value should be merged or not, and the key is
			// in the same snapshot stripe. Hence, snapshotPinned is by definition
			// false in those cases.
			i.snapshotPinned = true
			i.forceObsoleteDueToRangeDel = true

		default:
			i.forceObsoleteDueToRangeDel = false
		}

		switch i.iterKV.Kind() {
		case InternalKeyKindDelete, InternalKeyKindSingleDelete, InternalKeyKindDeleteSized:
			if i.elideTombstone(i.iterKV.K.UserKey) {
				if i.curSnapshotIdx == 0 {
					// If we're at the last snapshot stripe and the tombstone
					// can be elided skip skippable keys in the same stripe.
					i.saveKey()
					if i.key.Kind() == InternalKeyKindSingleDelete {
						i.skipDueToSingleDeleteElision()
					} else {
						i.skipInStripe()
						if !i.skip && i.iterStripeChange != newStripeNewKey {
							panic(errors.AssertionFailedf("pebble: skipInStripe in last stripe disabled skip without advancing to new key"))
						}
					}
					if i.iterStripeChange == newStripeSameKey {
						panic(errors.AssertionFailedf("pebble: skipInStripe in last stripe found a new stripe within the same key"))
					}
					continue
				} else {
					// We're not at the last snapshot stripe, so the tombstone
					// can NOT yet be elided. Mark it as pinned, so that it's
					// included in table statistics appropriately.
					i.snapshotPinned = true
				}
			}

			switch i.iterKV.Kind() {
			case InternalKeyKindDelete:
				i.saveKey()
				i.value = i.iterValue
				i.valid = true
				i.skip = true
				return &i.key, i.value

			case InternalKeyKindDeleteSized:
				// We may skip subsequent keys because of this tombstone. Scan
				// ahead to see just how much data this tombstone drops and if
				// the tombstone's value should be updated accordingly.
				return i.deleteSizedNext()

			case InternalKeyKindSingleDelete:
				if i.singleDeleteNext() {
					return &i.key, i.value
				} else if i.err != nil {
					return nil, nil
				}
				continue

			default:
				panic(errors.AssertionFailedf(
					"unexpected kind %s", redact.SafeString(i.iterKV.Kind().String())))
			}

		case InternalKeyKindSet, InternalKeyKindSetWithDelete:
			// The key we emit for this entry is a function of the current key
			// kind, and whether this entry is followed by a DEL/SINGLEDEL
			// entry. setNext() does the work to move the iterator forward,
			// preserving the original value, and potentially mutating the key
			// kind.
			i.setNext()
			if i.err != nil {
				return nil, nil
			}
			return &i.key, i.value

		case InternalKeyKindMerge:
			// Record the snapshot index before mergeNext as merging
			// advances the iterator, adjusting curSnapshotIdx.
			origSnapshotIdx := i.curSnapshotIdx
			var valueMerger ValueMerger
			valueMerger, i.err = i.merge(i.iterKV.K.UserKey, i.iterValue)
			if i.err == nil {
				i.mergeNext(valueMerger)
			}
			var needDelete bool
			if i.err == nil {
				// includesBase is true whenever we've transformed the MERGE record
				// into a SET.
				var includesBase bool
				switch i.key.Kind() {
				case InternalKeyKindSet, InternalKeyKindSetWithDelete:
					includesBase = true
				case InternalKeyKindMerge:
				default:
					panic(errors.AssertionFailedf(
						"unexpected kind %s", redact.SafeString(i.key.Kind().String())))
				}
				i.value, needDelete, i.valueCloser, i.err = finishValueMerger(valueMerger, includesBase)
			}
			if i.err == nil {
				if needDelete {
					i.valid = false
					if i.closeValueCloser() != nil {
						return nil, nil
					}
					continue
				}

				i.maybeZeroSeqnum(origSnapshotIdx)
				return &i.key, i.value
			}
			if i.err != nil {
				i.valid = false
				// TODO(sumeer): why is MarkCorruptionError only being called for
				// MERGE?
				i.err = base.MarkCorruptionError(i.err)
			}
			return nil, nil

		default:
			i.err = base.CorruptionErrorf("invalid internal key kind: %d", errors.Safe(i.iterKV.Kind()))
			i.valid = false
			return nil, nil
		}
	}

	return nil, nil
}

func (i *compactionIter) closeValueCloser() error {
	if i.valueCloser == nil {
		return nil
	}

	i.err = i.valueCloser.Close()
	i.valueCloser = nil
	if i.err != nil {
		i.valid = false
	}
	return i.err
}

// skipInStripe skips over skippable keys in the same stripe and user key. It
// may set i.err, in which case i.iterKV will be nil.
func (i *compactionIter) skipInStripe() {
	i.skip = true
	// TODO(sumeer): we can avoid the overhead of calling i.rangeDelFrag.Covers,
	// in this case of nextInStripe, since we are skipping all of them anyway.
	for i.nextInStripe() == sameStripe {
		if i.err != nil {
			panic(i.err)
		}
	}
	// We landed outside the original stripe, so reset skip.
	i.skip = false
}

func (i *compactionIter) iterNext() bool {
	i.iterKV = i.iter.Next()
	if i.iterKV != nil {
		i.iterValue, _, i.err = i.iterKV.Value(nil)
		if i.err != nil {
			i.iterKV = nil
		}
	}
	return i.iterKV != nil
}

// stripeChangeType indicates how the snapshot stripe changed relative to the
// previous key. If the snapshot stripe changed, it also indicates whether the
// new stripe was entered because the iterator progressed onto an entirely new
// key or entered a new stripe within the same key.
type stripeChangeType int

const (
	newStripeNewKey stripeChangeType = iota
	newStripeSameKey
	sameStripe
)

// nextInStripe advances the iterator and returns one of the above const ints
// indicating how its state changed.
//
// All sameStripe keys that are covered by a RANGEDEL will be skipped and not
// returned.
//
// Calls to nextInStripe must be preceded by a call to saveKey to retain a
// temporary reference to the original key, so that forward iteration can
// proceed with a reference to the original key. Care should be taken to avoid
// overwriting or mutating the saved key or value before they have been returned
// to the caller of the exported function (i.e. the caller of Next, First, etc.)
//
// nextInStripe may set i.err, in which case the return value will be
// newStripeNewKey, and i.iterKV will be nil.
func (i *compactionIter) nextInStripe() stripeChangeType {
	i.iterStripeChange = i.nextInStripeHelper()
	return i.iterStripeChange
}

// nextInStripeHelper is an internal helper for nextInStripe; callers should use
// nextInStripe and not call nextInStripeHelper.
func (i *compactionIter) nextInStripeHelper() stripeChangeType {
	origSnapshotIdx := i.curSnapshotIdx
	for {
		if !i.iterNext() {
			return newStripeNewKey
		}
		kv := i.iterKV

		// Is this a new key? There are two cases:
		//
		// 1. The new key has a different user key.
		// 2. The previous key was an interleaved range deletion or range key
		//    boundary. These keys are interleaved in the same input iterator
		//    stream as point keys, but they do not obey the ordinary sequence
		//    number ordering within a user key. If the previous key was one
		//    of these keys, we consider the new key a `newStripeNewKey` to
		//    reflect that it's the beginning of a new stream of point keys.
		if i.key.IsExclusiveSentinel() || !i.equal(i.key.UserKey, kv.K.UserKey) {
			i.curSnapshotIdx, i.curSnapshotSeqNum = i.snapshots.IndexAndSeqNum(kv.SeqNum())
			return newStripeNewKey
		}

		// If i.key and key have the same user key, then
		//   1. i.key must not have had a zero sequence number (or it would've be the last
		//      key with its user key).
		//   2. i.key must have a strictly larger sequence number
		// There's an exception in that either key may be a range delete. Range
		// deletes may share a sequence number with a point key if the keys were
		// ingested together. Range keys may also share the sequence number if they
		// were ingested, but range keys are interleaved into the compaction
		// iterator's input iterator at the maximal sequence number so their
		// original sequence number will not be observed here.
		if prevSeqNum := base.SeqNumFromTrailer(i.keyTrailer); (prevSeqNum == 0 || prevSeqNum <= kv.SeqNum()) &&
			i.key.Kind() != InternalKeyKindRangeDelete && kv.Kind() != InternalKeyKindRangeDelete {
			prevKey := i.key
			prevKey.Trailer = i.keyTrailer
			panic(errors.AssertionFailedf("pebble: invariant violation: %s and %s out of order", prevKey, kv.K))
		}

		i.curSnapshotIdx, i.curSnapshotSeqNum = i.snapshots.IndexAndSeqNum(kv.SeqNum())
		switch kv.Kind() {
		case InternalKeyKindRangeKeySet, InternalKeyKindRangeKeyUnset, InternalKeyKindRangeKeyDelete,
			InternalKeyKindRangeDelete:
			// Range tombstones and range keys are interleaved at the max
			// sequence number for a given user key, and the first key after one
			// is always considered a newStripeNewKey, so we should never reach
			// this.
			panic("unreachable")
		case InternalKeyKindDelete, InternalKeyKindSet, InternalKeyKindMerge, InternalKeyKindSingleDelete,
			InternalKeyKindSetWithDelete, InternalKeyKindDeleteSized:
			// Fall through
		default:
			kind := i.iterKV.Kind()
			i.iterKV = nil
			i.err = base.CorruptionErrorf("invalid internal key kind: %d", errors.Safe(kind))
			i.valid = false
			return newStripeNewKey
		}
		if i.curSnapshotIdx == origSnapshotIdx {
			// Same snapshot.
			if i.tombstoneCovers(i.iterKV.K, i.curSnapshotSeqNum) == coversVisibly {
				continue
			}
			return sameStripe
		}
		return newStripeSameKey
	}
}

func (i *compactionIter) setNext() {
	// Save the current key.
	i.saveKey()
	i.value = i.iterValue
	i.valid = true
	i.maybeZeroSeqnum(i.curSnapshotIdx)

	// If this key is already a SETWITHDEL we can early return and skip the remaining
	// records in the stripe:
	if i.iterKV.Kind() == InternalKeyKindSetWithDelete {
		i.skip = true
		return
	}

	// We are iterating forward. Save the current value.
	i.valueBuf = append(i.valueBuf[:0], i.iterValue...)
	i.value = i.valueBuf

	// Else, we continue to loop through entries in the stripe looking for a
	// DEL. Note that we may stop *before* encountering a DEL, if one exists.
	//
	// NB: nextInStripe will skip sameStripe keys that are visibly covered by a
	// RANGEDEL. This can include DELs -- this is fine since such DELs don't
	// need to be combined with SET to make SETWITHDEL.
	for {
		switch i.nextInStripe() {
		case newStripeNewKey, newStripeSameKey:
			i.pos = iterPosNext
			return
		case sameStripe:
			// We're still in the same stripe. If this is a
			// DEL/SINGLEDEL/DELSIZED, we stop looking and emit a SETWITHDEL.
			// Subsequent keys are eligible for skipping.
			switch i.iterKV.Kind() {
			case InternalKeyKindDelete, InternalKeyKindSingleDelete, InternalKeyKindDeleteSized:
				i.key.SetKind(InternalKeyKindSetWithDelete)
				i.skip = true
				return
			case InternalKeyKindSet, InternalKeyKindMerge, InternalKeyKindSetWithDelete:
				// Do nothing
			default:
				i.err = base.CorruptionErrorf("invalid internal key kind: %d", errors.Safe(i.iterKV.Kind()))
				i.valid = false
			}
		default:
			panic("pebble: unexpected stripeChangeType: " + strconv.Itoa(int(i.iterStripeChange)))
		}
	}
}

func (i *compactionIter) mergeNext(valueMerger ValueMerger) {
	// Save the current key.
	i.saveKey()
	i.valid = true

	// Loop looking for older values in the current snapshot stripe and merge
	// them.
	for {
		if i.nextInStripe() != sameStripe {
			i.pos = iterPosNext
			return
		}
		if i.err != nil {
			panic(i.err)
		}
		// NB: MERGE#10+RANGEDEL#9 stays a MERGE, since nextInStripe skips
		// sameStripe keys that are visibly covered by a RANGEDEL. There may be
		// MERGE#7 that is invisibly covered and will be preserved, but there is
		// no risk that MERGE#10 and MERGE#7 will get merged in the future as
		// the RANGEDEL still exists and will be used in user-facing reads that
		// see MERGE#10, and will also eventually cause MERGE#7 to be deleted in
		// a compaction.
		key := i.iterKV
		switch key.Kind() {
		case InternalKeyKindDelete, InternalKeyKindSingleDelete, InternalKeyKindDeleteSized:
			// We've hit a deletion tombstone. Return everything up to this point and
			// then skip entries until the next snapshot stripe. We change the kind
			// of the result key to a Set so that it shadows keys in lower
			// levels. That is, MERGE+DEL -> SETWITHDEL.
			//
			// We do the same for SingleDelete since SingleDelete is only
			// permitted (with deterministic behavior) for keys that have been
			// set once since the last SingleDelete/Delete, so everything
			// older is acceptable to shadow. Note that this is slightly
			// different from singleDeleteNext() which implements stricter
			// semantics in terms of applying the SingleDelete to the single
			// next Set. But those stricter semantics are not observable to
			// the end-user since Iterator interprets SingleDelete as Delete.
			// We could do something more complicated here and consume only a
			// single Set, and then merge in any following Sets, but that is
			// complicated wrt code and unnecessary given the narrow permitted
			// use of SingleDelete.
			i.key.SetKind(InternalKeyKindSetWithDelete)
			i.skip = true
			return

		case InternalKeyKindSet, InternalKeyKindSetWithDelete:
			// We've hit a Set or SetWithDel value. Merge with the existing
			// value and return. We change the kind of the resulting key to a
			// Set so that it shadows keys in lower levels. That is:
			// MERGE + (SET*) -> SET.
			i.err = valueMerger.MergeOlder(i.iterValue)
			if i.err != nil {
				i.valid = false
				return
			}
			i.key.SetKind(InternalKeyKindSet)
			i.skip = true
			return

		case InternalKeyKindMerge:
			// We've hit another Merge value. Merge with the existing value and
			// continue looping.
			i.err = valueMerger.MergeOlder(i.iterValue)
			if i.err != nil {
				i.valid = false
				return
			}

		default:
			i.err = base.CorruptionErrorf("invalid internal key kind: %d", errors.Safe(i.iterKV.Kind()))
			i.valid = false
			return
		}
	}
}

// singleDeleteNext processes a SingleDelete point tombstone. A SingleDelete, or
// SINGLEDEL, is unique in that it deletes exactly 1 internal key. It's a
// performance optimization when the client knows a user key has not been
// overwritten, allowing the elision of the tombstone earlier, avoiding write
// amplification.
//
// singleDeleteNext returns a boolean indicating whether or not the caller
// should yield the SingleDelete key to the consumer of the compactionIter. If
// singleDeleteNext returns false, the caller may consume/elide the
// SingleDelete.
func (i *compactionIter) singleDeleteNext() bool {
	// Save the current key.
	i.saveKey()
	i.value = i.iterValue
	i.valid = true

	// Loop until finds a key to be passed to the next level.
	for {
		// If we find a key that can't be skipped, return true so that the
		// caller yields the SingleDelete to the caller.
		if i.nextInStripe() != sameStripe {
			// This defers additional error checking regarding single delete
			// invariants to the compaction where the keys with the same user key as
			// the single delete are in the same stripe.
			i.pos = iterPosNext
			return i.err == nil
		}
		if i.err != nil {
			panic(i.err)
		}
		// INVARIANT: sameStripe.
		key := i.iterKV
		kind := key.Kind()
		switch kind {
		case InternalKeyKindDelete, InternalKeyKindSetWithDelete, InternalKeyKindDeleteSized:
			if (kind == InternalKeyKindDelete || kind == InternalKeyKindDeleteSized) &&
				i.ineffectualSingleDeleteCallback != nil {
				i.ineffectualSingleDeleteCallback(i.key.UserKey)
			}
			// We've hit a Delete, DeleteSized, SetWithDelete, transform
			// the SingleDelete into a full Delete.
			i.key.SetKind(InternalKeyKindDelete)
			i.skip = true
			return true

		case InternalKeyKindSet, InternalKeyKindMerge:
			// This SingleDelete deletes the Set/Merge, and we can now elide the
			// SingleDel as well. We advance past the Set and return false to
			// indicate to the main compaction loop that we should NOT yield the
			// current SingleDel key to the compaction loop.
			//
			// NB: singleDeleteNext was called with i.pos == iterPosCurForward, and
			// after the call to nextInStripe, we are still at iterPosCurForward,
			// since we are at the key after the Set/Merge that was single deleted.
			change := i.nextInStripe()
			switch change {
			case sameStripe, newStripeSameKey:
				// On the same user key.
				nextKind := i.iterKV.Kind()
				switch nextKind {
				case InternalKeyKindSet, InternalKeyKindSetWithDelete, InternalKeyKindMerge:
					if i.singleDeleteInvariantViolationCallback != nil {
						// sameStripe keys returned by nextInStripe() are already
						// known to not be covered by a RANGEDEL, so it is an invariant
						// violation. The rare case is newStripeSameKey, where it is a
						// violation if not covered by a RANGEDEL.
						if change == sameStripe ||
							i.tombstoneCovers(i.iterKV.K, i.curSnapshotSeqNum) == noCover {
							i.singleDeleteInvariantViolationCallback(i.key.UserKey)
						}
					}
				case InternalKeyKindDelete, InternalKeyKindDeleteSized, InternalKeyKindSingleDelete:
				default:
					panic(errors.AssertionFailedf(
						"unexpected internal key kind: %d", errors.Safe(i.iterKV.Kind())))
				}
			case newStripeNewKey:
			default:
				panic("unreachable")
			}
			i.valid = false
			return false

		case InternalKeyKindSingleDelete:
			// Two single deletes met in a compaction. The first single delete is
			// ineffectual.
			if i.ineffectualSingleDeleteCallback != nil {
				i.ineffectualSingleDeleteCallback(i.key.UserKey)
			}
			// Continue to apply the second single delete.
			continue

		default:
			i.err = base.CorruptionErrorf("invalid internal key kind: %d", errors.Safe(i.iterKV.Kind()))
			i.valid = false
			return false
		}
	}
}

// skipDueToSingleDeleteElision is called when the SingleDelete is being
// elided because it is in the final snapshot stripe and there are no keys
// with the same user key in lower levels in the LSM (below the files in this
// compaction).
//
// TODO(sumeer): the only difference between singleDeleteNext and
// skipDueToSingleDeleteElision is the fact that the caller knows it will be
// eliding the single delete in the latter case. There are some similar things
// happening in both implementations. My first attempt at combining them into
// a single method was hard to comprehend. Try again.
func (i *compactionIter) skipDueToSingleDeleteElision() {
	for {
		stripeChange := i.nextInStripe()
		if i.err != nil {
			panic(i.err)
		}
		switch stripeChange {
		case newStripeNewKey:
			// The single delete is only now being elided, meaning it did not elide
			// any keys earlier in its descent down the LSM. We stepped onto a new
			// user key, meaning that even now at its moment of elision, it still
			// hasn't elided any other keys. The single delete was ineffectual (a
			// no-op).
			if i.ineffectualSingleDeleteCallback != nil {
				i.ineffectualSingleDeleteCallback(i.key.UserKey)
			}
			i.skip = false
			return
		case newStripeSameKey:
			// This should be impossible. If we're eliding a single delete, we
			// determined that the tombstone is in the final snapshot stripe, but we
			// stepped into a new stripe of the same key.
			panic(errors.AssertionFailedf("eliding single delete followed by same key in new stripe"))
		case sameStripe:
			kind := i.iterKV.Kind()
			switch kind {
			case InternalKeyKindDelete, InternalKeyKindDeleteSized, InternalKeyKindSingleDelete:
				if i.ineffectualSingleDeleteCallback != nil {
					i.ineffectualSingleDeleteCallback(i.key.UserKey)
				}
				switch kind {
				case InternalKeyKindDelete, InternalKeyKindDeleteSized:
					i.skipInStripe()
					return
				case InternalKeyKindSingleDelete:
					// Repeat the same with this SingleDelete. We don't want to simply
					// call skipInStripe(), since it increases the strength of the
					// SingleDel, which hides bugs in the use of single delete.
					continue
				default:
					panic(errors.AssertionFailedf(
						"unexpected internal key kind: %d", errors.Safe(i.iterKV.Kind())))
				}
			case InternalKeyKindSetWithDelete:
				// The SingleDelete should behave like a Delete.
				i.skipInStripe()
				return
			case InternalKeyKindSet, InternalKeyKindMerge:
				// This SingleDelete deletes the Set/Merge, and we are eliding the
				// SingleDel as well. Step to the next key (this is not deleted by the
				// SingleDelete).
				//
				// NB: skipDueToSingleDeleteElision was called with i.pos ==
				// iterPosCurForward, and after the call to nextInStripe, we are still
				// at iterPosCurForward, since we are at the key after the Set/Merge
				// that was single deleted.
				change := i.nextInStripe()
				if i.err != nil {
					panic(i.err)
				}
				switch change {
				case newStripeSameKey:
					panic(errors.AssertionFailedf("eliding single delete followed by same key in new stripe"))
				case newStripeNewKey:
				case sameStripe:
					// On the same key.
					nextKind := i.iterKV.Kind()
					switch nextKind {
					case InternalKeyKindSet, InternalKeyKindSetWithDelete, InternalKeyKindMerge:
						if i.singleDeleteInvariantViolationCallback != nil {
							i.singleDeleteInvariantViolationCallback(i.key.UserKey)
						}
					case InternalKeyKindDelete, InternalKeyKindDeleteSized, InternalKeyKindSingleDelete:
					default:
						panic(errors.AssertionFailedf(
							"unexpected internal key kind: %d", errors.Safe(i.iterKV.Kind())))
					}
				default:
					panic("unreachable")
				}
				// Whether in same stripe or new stripe, this key is not consumed by
				// the SingleDelete.
				i.skip = false
				return
			default:
				panic(errors.AssertionFailedf(
					"unexpected internal key kind: %d", errors.Safe(i.iterKV.Kind())))
			}
		default:
			panic("unreachable")
		}
	}
}

// deleteSizedNext processes a DELSIZED point tombstone. Unlike ordinary DELs,
// these tombstones carry a value that's a varint indicating the size of the
// entry (len(key)+len(value)) that the tombstone is expected to delete.
//
// When a deleteSizedNext is encountered, we skip ahead to see which keys, if
// any, are elided as a result of the tombstone.
func (i *compactionIter) deleteSizedNext() (*base.InternalKey, []byte) {
	i.saveKey()
	i.valid = true
	i.skip = true

	// The DELSIZED tombstone may have no value at all. This happens when the
	// tombstone has already deleted the key that the user originally predicted.
	// In this case, we still peek forward in case there's another DELSIZED key
	// with a lower sequence number, in which case we'll adopt its value.
	if len(i.iterValue) == 0 {
		i.value = i.valueBuf[:0]
	} else {
		i.valueBuf = append(i.valueBuf[:0], i.iterValue...)
		i.value = i.valueBuf
	}

	// Loop through all the keys within this stripe that are skippable.
	i.pos = iterPosNext
	for i.nextInStripe() == sameStripe {
		if i.err != nil {
			panic(i.err)
		}
		switch i.iterKV.Kind() {
		case InternalKeyKindDelete, InternalKeyKindDeleteSized, InternalKeyKindSingleDelete:
			// We encountered a tombstone (DEL, or DELSIZED) that's deleted by
			// the original DELSIZED tombstone. This can happen in two cases:
			//
			// (1) These tombstones were intended to delete two distinct values,
			//     and this DELSIZED has already dropped the relevant key. For
			//     example:
			//
			//     a.DELSIZED.9   a.SET.7   a.DELSIZED.5   a.SET.4
			//
			//     If a.DELSIZED.9 has already deleted a.SET.7, its size has
			//     already been zeroed out. In this case, we want to adopt the
			//     value of the DELSIZED with the lower sequence number, in
			//     case the a.SET.4 key has not yet been elided.
			//
			// (2) This DELSIZED was missized. The user thought they were
			//     deleting a key with this user key, but this user key had
			//     already been deleted.
			//
			// We can differentiate these two cases by examining the length of
			// the DELSIZED's value. A DELSIZED's value holds the size of both
			// the user key and value that it intends to delete. For any user
			// key with a length > 0, a DELSIZED that has not deleted a key must
			// have a value with a length > 0.
			//
			// We treat both cases the same functionally, adopting the identity
			// of the lower-sequence numbered tombstone. However in the second
			// case, we also increment the stat counting missized tombstones.
			if len(i.value) > 0 {
				// The original DELSIZED key was missized. The key that the user
				// thought they were deleting does not exist.
				i.stats.countMissizedDels++
			}
			i.valueBuf = append(i.valueBuf[:0], i.iterValue...)
			i.value = i.valueBuf
			if i.iterKV.Kind() != InternalKeyKindDeleteSized {
				// Convert the DELSIZED to a DEL—The DEL/SINGLEDEL we're eliding
				// may not have deleted the key(s) it was intended to yet. The
				// ordinary DEL compaction heuristics are better suited at that,
				// plus we don't want to count it as a missized DEL. We early
				// exit in this case, after skipping the remainder of the
				// snapshot stripe.
				i.key.SetKind(InternalKeyKindDelete)
				// NB: We skipInStripe now, rather than returning leaving
				// i.skip=true and returning early, because Next() requires
				// that i.skip=true only if i.iterPos = iterPosCurForward.
				//
				// Ignore any error caused by skipInStripe since it does not affect
				// the key/value being returned here, and the next call to Next() will
				// expose it.
				i.skipInStripe()
				return &i.key, i.value
			}
			// Continue, in case we uncover another DELSIZED or a key this
			// DELSIZED deletes.

		case InternalKeyKindSet, InternalKeyKindMerge, InternalKeyKindSetWithDelete:
			// If the DELSIZED is value-less, it already deleted the key that it
			// was intended to delete. This is possible with a sequence like:
			//
			//      DELSIZED.8     SET.7     SET.3
			//
			// The DELSIZED only describes the size of the SET.7, which in this
			// case has already been elided. We don't count it as a missizing,
			// instead converting the DELSIZED to a DEL. Skip the remainder of
			// the snapshot stripe and return.
			if len(i.value) == 0 {
				i.key.SetKind(InternalKeyKindDelete)
				// NB: We skipInStripe now, rather than returning leaving
				// i.skip=true and returning early, because Next() requires
				// that i.skip=true only if i.iterPos = iterPosCurForward.
				//
				// Ignore any error caused by skipInStripe since it does not affect
				// the key/value being returned here, and the next call to Next() will
				// expose it.
				i.skipInStripe()
				return &i.key, i.value
			}
			// The deleted key is not a DEL, DELSIZED, and the DELSIZED in i.key
			// has a positive size.
			expectedSize, n := binary.Uvarint(i.value)
			if n != len(i.value) {
				i.err = base.CorruptionErrorf("DELSIZED holds invalid value: %x", errors.Safe(i.value))
				i.valid = false
				return nil, nil
			}
			elidedSize := uint64(len(i.iterKV.K.UserKey)) + uint64(len(i.iterValue))
			if elidedSize != expectedSize {
				// The original DELSIZED key was missized. It's unclear what to
				// do. The user-provided size was wrong, so it's unlikely to be
				// accurate or meaningful. We could:
				//
				//   1. return the DELSIZED with the original user-provided size unmodified
				//   2. return the DELZIZED with a zeroed size to reflect that a key was
				//   elided, even if it wasn't the anticipated size.
				//   3. subtract the elided size from the estimate and re-encode.
				//   4. convert the DELSIZED into a value-less DEL, so that
				//      ordinary DEL heuristics apply.
				//
				// We opt for (4) under the rationale that we can't rely on the
				// user-provided size for accuracy, so ordinary DEL heuristics
				// are safer.
				i.stats.countMissizedDels++
				i.key.SetKind(InternalKeyKindDelete)
				i.value = i.valueBuf[:0]
				// NB: We skipInStripe now, rather than returning leaving
				// i.skip=true and returning early, because Next() requires
				// that i.skip=true only if i.iterPos = iterPosCurForward.
				//
				// Ignore any error caused by skipInStripe since it does not affect
				// the key/value being returned here, and the next call to Next() will
				// expose it.
				i.skipInStripe()
				return &i.key, i.value
			}
			// NB: We remove the value regardless of whether the key was sized
			// appropriately. The size encoded is 'consumed' the first time it
			// meets a key that it deletes.
			i.value = i.valueBuf[:0]

		default:
			i.err = base.CorruptionErrorf("invalid internal key kind: %d", errors.Safe(i.iterKV.Kind()))
			i.valid = false
			return nil, nil
		}
	}

	if i.iterStripeChange == sameStripe {
		panic(errors.AssertionFailedf("unexpectedly found iter stripe change = %d", i.iterStripeChange))
	}
	// We landed outside the original stripe. Reset skip.
	i.skip = false
	if i.err != nil {
		return nil, nil
	}
	return &i.key, i.value
}

func (i *compactionIter) saveKey() {
	i.keyBuf = append(i.keyBuf[:0], i.iterKV.K.UserKey...)
	i.key = base.InternalKey{
		UserKey: i.keyBuf,
		Trailer: i.iterKV.K.Trailer,
	}
	i.keyTrailer = i.key.Trailer
	i.frontiers.Advance(i.key.UserKey)
}

func (i *compactionIter) cloneKey(key []byte) []byte {
	i.alloc, key = i.alloc.Copy(key)
	return key
}

func (i *compactionIter) Key() InternalKey {
	return i.key
}

func (i *compactionIter) Value() []byte {
	return i.value
}

func (i *compactionIter) Valid() bool {
	return i.valid
}

func (i *compactionIter) Error() error {
	return i.err
}

func (i *compactionIter) Close() error {
	err := i.iter.Close()
	if i.err == nil {
		i.err = err
	}

	// Close the closer for the current value if one was open.
	if i.valueCloser != nil {
		i.err = firstError(i.err, i.valueCloser.Close())
		i.valueCloser = nil
	}

	return i.err
}

// AddTombstoneSpan adds a copy of a span of range dels, to be later returned by
// TombstonesUpTo.
//
// The spans must be non-overlapping and ordered. Empty spans are ignored.
func (i *compactionIter) AddTombstoneSpan(span *keyspan.Span) {
	i.tombstones = i.appendSpan(i.tombstones, span)
}

// cover is returned by tombstoneCovers and describes a span's relationship to
// a key at a particular snapshot.
type cover int8

const (
	// noCover indicates the tested key does not fall within the span's bounds,
	// or the span contains no keys with sequence numbers higher than the key's.
	noCover cover = iota

	// coversInvisibly indicates the tested key does fall within the span's
	// bounds and the span contains at least one key with a higher sequence
	// number, but none visible at the provided snapshot.
	coversInvisibly

	// coversVisibly indicates the tested key does fall within the span's
	// bounds, and the span constains at least one key with a sequence number
	// higher than the key's sequence number that is visible at the provided
	// snapshot.
	coversVisibly
)

// tombstoneCovers returns whether the key is covered by a tombstone and whether
// it is covered by a tombstone visible in the given snapshot.
//
// The key's UserKey must be greater or equal to the last span Start key passed
// to AddTombstoneSpan.
func (i *compactionIter) tombstoneCovers(key InternalKey, snapshot uint64) cover {
	if len(i.tombstones) == 0 {
		return noCover
	}
	last := &i.tombstones[len(i.tombstones)-1]
	if invariants.Enabled && i.cmp(key.UserKey, last.Start) < 0 {
		panic(errors.AssertionFailedf("invalid key %q, last span %s", key, last))
	}
	if i.cmp(key.UserKey, last.End) < 0 && last.Covers(key.SeqNum()) {
		if last.CoversAt(snapshot, key.SeqNum()) {
			return coversVisibly
		}
		return coversInvisibly
	}
	return noCover
}

// TombstonesUpTo returns a list of pending range tombstones up to the
// specified key, or all pending range tombstones if key = nil.
//
// If a key is specified, it must be greater than the last span Start key passed
// to AddTombstoneSpan.
func (i *compactionIter) TombstonesUpTo(key []byte) []keyspan.Span {
	var toReturn []keyspan.Span
	toReturn, i.tombstones = i.splitSpans(i.tombstones, key)

	result := toReturn[:0]
	for _, span := range toReturn {
		// Apply the snapshot stripe rules, keeping only the latest tombstone for
		// each snapshot stripe.
		currentIdx := -1
		keys := make([]keyspan.Key, 0, min(len(span.Keys), len(i.snapshots)+1))
		for _, k := range span.Keys {
			idx := i.snapshots.Index(k.SeqNum())
			if currentIdx == idx {
				continue
			}
			if idx == 0 && i.elideRangeTombstone(span.Start, span.End) {
				// This is the last snapshot stripe and the range tombstone
				// can be elided.
				break
			}

			keys = append(keys, k)
			if idx == 0 {
				// This is the last snapshot stripe.
				break
			}
			currentIdx = idx
		}
		if len(keys) > 0 {
			result = append(result, keyspan.Span{
				Start: i.cloneKey(span.Start),
				End:   i.cloneKey(span.End),
				Keys:  keys,
			})
		}
	}

	return result
}

// FirstTombstoneStart returns the start key of the first pending tombstone (the
// first span that would be returned by TombstonesUpTo). Returns nil if
// there are no pending range keys.
func (i *compactionIter) FirstTombstoneStart() []byte {
	if len(i.tombstones) == 0 {
		return nil
	}
	return i.tombstones[0].Start
}

// AddRangeKeySpan adds a copy of a span of range keys, to be later returned by
// RangeKeysUpTo. The span is shallow cloned.
//
// The spans must be non-overlapping and ordered. Empty spans are ignored.
func (i *compactionIter) AddRangeKeySpan(span *keyspan.Span) {
	i.rangeKeys = i.appendSpan(i.rangeKeys, span)
}

// RangeKeysUpTo returns a list of pending range keys up to the specified key,
// or all pending range keys if key = nil.
//
// If a key is specified, it must be greater than the last span Start key passed
// to AddRangeKeySpan.
func (i *compactionIter) RangeKeysUpTo(key []byte) []keyspan.Span {
	var result []keyspan.Span
	result, i.rangeKeys = i.splitSpans(i.rangeKeys, key)
	// Elision of snapshot stripes happens in rangeKeyCompactionTransform, so no need to
	// do that here.
	return result
}

// FirstRangeKeyStart returns the start key of the first pending range key span
// (the first span that would be returned by RangeKeysUpTo). Returns nil
// if there are no pending range keys.
func (i *compactionIter) FirstRangeKeyStart() []byte {
	if len(i.rangeKeys) == 0 {
		return nil
	}
	return i.rangeKeys[0].Start
}

// maybeZeroSeqnum attempts to set the seqnum for the current key to 0. Doing
// so improves compression and enables an optimization during forward iteration
// to skip some key comparisons. The seqnum for an entry can be zeroed if the
// entry is on the bottom snapshot stripe and on the bottom level of the LSM.
func (i *compactionIter) maybeZeroSeqnum(snapshotIdx int) {
	if !i.allowZeroSeqNum {
		// TODO(peter): allowZeroSeqNum applies to the entire compaction. We could
		// make the determination on a key by key basis, similar to what is done
		// for elideTombstone. Need to add a benchmark for compactionIter to verify
		// that isn't too expensive.
		return
	}
	if snapshotIdx > 0 {
		// This is not the last snapshot
		return
	}
	i.key.SetSeqNum(base.SeqNumZero)
}

// appendSpan appends a span to a list of ordered, non-overlapping spans.
// The span is shallow cloned.
func (i *compactionIter) appendSpan(spans []keyspan.Span, span *keyspan.Span) []keyspan.Span {
	if span.Empty() {
		return spans
	}
	if invariants.Enabled && len(spans) > 0 {
		if last := spans[len(spans)-1]; i.cmp(last.End, span.Start) > 0 {
			panic(errors.AssertionFailedf("overlapping range key spans: %s and %s", last, span))
		}
	}
	return append(spans, keyspan.Span{
		Start: i.cloneKey(span.Start),
		End:   i.cloneKey(span.End),
		Keys:  slices.Clone(span.Keys),
	})
}

// splitSpans takes a list of ordered, non-overlapping spans and a key and
// returns two lists of spans: one that ends at or before the key, and one which
// starts at or after key.
//
// If the key is nil, returns all spans. If not nil, the key must not be smaller
// than the start of the last span in the list.
func (i *compactionIter) splitSpans(
	spans []keyspan.Span, key []byte,
) (before, after []keyspan.Span) {
	n := len(spans)
	if n == 0 || key == nil || i.cmp(spans[n-1].End, key) <= 0 {
		// Common case: return all spans (retaining any extra allocated space).
		return spans[:n:n], spans[n:]
	}
	if c := i.cmp(key, spans[n-1].Start); c <= 0 {
		if c < 0 {
			panic(fmt.Sprintf("invalid key %q, last span %s", key, spans[n-1]))
		}
		// The last span starts exactly at key.
		return spans[: n-1 : n-1], spans[n-1:]
	}
	// We have to split the last span.
	key = i.cloneKey(key)
	before = spans[:n:n]
	after = append(spans[n:], keyspan.Span{
		Start: key,
		End:   spans[n-1].End,
		Keys:  slices.Clone(spans[n-1].Keys),
	})
	before[n-1].End = key
	return before, after
}
