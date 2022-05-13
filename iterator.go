// Copyright 2011 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package pebble

import (
	"bytes"
	"io"
	"sort"
	"sync"
	"sync/atomic"
	"unsafe"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/fastrand"
	"github.com/cockroachdb/pebble/internal/humanize"
	"github.com/cockroachdb/pebble/internal/invariants"
	"github.com/cockroachdb/pebble/internal/keyspan"
	"github.com/cockroachdb/pebble/internal/manifest"
	"github.com/cockroachdb/redact"
)

// iterPos describes the state of the internal iterator, in terms of whether it
// is at the position returned to the user (cur), one ahead of the position
// returned (next for forward iteration and prev for reverse iteration). The cur
// position is split into two states, for forward and reverse iteration, since
// we need to differentiate for switching directions.
//
// There is subtlety in what is considered the current position of the Iterator.
// The internal iterator exposes a sequence of internal keys. There is not
// always a single internalIterator position corresponding to the position
// returned to the user. Consider the example:
//
//    a.MERGE.9 a.MERGE.8 a.MERGE.7 a.SET.6 b.DELETE.9 b.DELETE.5 b.SET.4
//    \                                   /
//      \       Iterator.Key() = 'a'    /
//
// The Iterator exposes one valid position at user key 'a' and the two exhausted
// positions at the beginning and end of iteration. The underlying
// internalIterator contains 7 valid positions and 2 exhausted positions.
//
// Iterator positioning methods must set iterPos to iterPosCur{Foward,Backward}
// iff the user key at the current internalIterator position equals the
// Iterator.Key returned to the user. This guarantees that a call to nextUserKey
// or prevUserKey will advance to the next or previous iterator position.
// iterPosCur{Forward,Backward} does not make any guarantee about the internal
// iterator position among internal keys with matching user keys, and it will
// vary subtly depending on the particular key kinds encountered. In the above
// example, the iterator returning 'a' may set iterPosCurForward if the internal
// iterator is positioned at any of a.MERGE.9, a.MERGE.8, a.MERGE.7 or a.SET.6.
//
// When setting iterPos to iterPosNext or iterPosPrev, the internal iterator
// must be advanced to the first internalIterator position at a user key greater
// (iterPosNext) or less (iterPosPrev) than the key returned to the user. An
// internalIterator position that's !Valid() must also be considered greater or
// less—depending on the direction of iteration—than the last valid Iterator
// position.
type iterPos int8

const (
	iterPosCurForward iterPos = 0
	iterPosNext       iterPos = 1
	iterPosPrev       iterPos = -1
	iterPosCurReverse iterPos = -2

	// For limited iteration. When the iterator is at iterPosCurForwardPaused
	// - Next*() call should behave as if the internal iterator is already
	//   at next (akin to iterPosNext).
	// - Prev*() call should behave as if the internal iterator is at the
	//   current key (akin to iterPosCurForward).
	//
	// Similar semantics apply to CurReversePaused.
	iterPosCurForwardPaused iterPos = 2
	iterPosCurReversePaused iterPos = -3
)

// Approximate gap in bytes between samples of data read during iteration.
// This is multiplied with a default ReadSamplingMultiplier of 1 << 4 to yield
// 1 << 20 (1MB). The 1MB factor comes from:
// https://github.com/cockroachdb/pebble/issues/29#issuecomment-494477985
const readBytesPeriod uint64 = 1 << 16

var errReversePrefixIteration = errors.New("pebble: unsupported reverse prefix iteration")

// IteratorMetrics holds per-iterator metrics. These do not change over the
// lifetime of the iterator.
type IteratorMetrics struct {
	// The read amplification experienced by this iterator. This is the sum of
	// the memtables, the L0 sublevels and the non-empty Ln levels. Higher read
	// amplification generally results in slower reads, though allowing higher
	// read amplification can also result in faster writes.
	ReadAmp int
}

// IteratorStatsKind describes the two kind of iterator stats.
type IteratorStatsKind int8

const (
	// InterfaceCall represents calls to Iterator.
	InterfaceCall IteratorStatsKind = iota
	// InternalIterCall represents calls by Iterator to its internalIterator.
	InternalIterCall
	// NumStatsKind is the number of kinds, and is used for array sizing.
	NumStatsKind
)

// IteratorStats contains iteration stats.
type IteratorStats struct {
	// ForwardSeekCount includes SeekGE, SeekPrefixGE, First.
	ForwardSeekCount [NumStatsKind]int
	// ReverseSeek includes SeekLT, Last.
	ReverseSeekCount [NumStatsKind]int
	// ForwardStepCount includes Next.
	ForwardStepCount [NumStatsKind]int
	// ReverseStepCount includes Prev.
	ReverseStepCount [NumStatsKind]int
	InternalStats    InternalIteratorStats
}

var _ redact.SafeFormatter = &IteratorStats{}

// InternalIteratorStats contains miscellaneous stats produced by internal
// iterators.
type InternalIteratorStats = base.InternalIteratorStats

// Iterator iterates over a DB's key/value pairs in key order.
//
// An iterator must be closed after use, but it is not necessary to read an
// iterator until exhaustion.
//
// An iterator is not goroutine-safe, but it is safe to use multiple iterators
// concurrently, with each in a dedicated goroutine.
//
// It is also safe to use an iterator concurrently with modifying its
// underlying DB, if that DB permits modification. However, the resultant
// key/value pairs are not guaranteed to be a consistent snapshot of that DB
// at a particular point in time.
//
// If an iterator encounters an error during any operation, it is stored by
// the Iterator and surfaced through the Error method. All absolute
// positioning methods (eg, SeekLT, SeekGT, First, Last, etc) reset any
// accumulated error before positioning. All relative positioning methods (eg,
// Next, Prev) return without advancing if the iterator has an accumulated
// error.
type Iterator struct {
	opts      IterOptions
	cmp       Compare
	equal     Equal
	merge     Merge
	split     Split
	iter      internalIteratorWithStats
	pointIter internalIteratorWithStats
	readState *readState
	// rangeKey holds iteration state specific to iteration over range keys.
	// The range key field may be nil if the Iterator has never been configured
	// to iterate over range keys. Its non-nilness cannot be used to determine
	// if the Iterator is currently iterating over range keys: For that, consult
	// the IterOptions using opts.rangeKeys(). If non-nil, its rangeKeyIter
	// field is guaranteed to be non-nil too.
	rangeKey *iteratorRangeKeyState
	err      error
	// When iterValidityState=IterValid, key represents the current key, which
	// is backed by keyBuf.
	key         []byte
	keyBuf      []byte
	value       []byte
	valueBuf    []byte
	valueCloser io.Closer
	// boundsBuf holds two buffers used to store the lower and upper bounds.
	// Whenever the Iterator's bounds change, the new bounds are copied into
	// boundsBuf[boundsBufIdx]. The two bounds share a slice to reduce
	// allocations. opts.LowerBound and opts.UpperBound point into this slice.
	boundsBuf    [2][]byte
	boundsBufIdx int
	// iterKey, iterValue reflect the latest position of iter, except when
	// SetBounds is called. In that case, these are explicitly set to nil.
	iterKey             *InternalKey
	iterValue           []byte
	alloc               *iterAlloc
	getIterAlloc        *getIterAlloc
	prefixOrFullSeekKey []byte
	readSampling        readSampling
	stats               IteratorStats
	closeHook           func()

	// Following fields are only used in Clone and SetOptions.
	// Non-nil if this Iterator includes a Batch.
	batch    *Batch
	newIters tableNewIters
	seqNum   uint64
	// TODO(jackson): Remove db when we no longer require the global arena for
	// range keys. This reference is only temporary.
	db *DB

	// Keeping the bools here after all the 8 byte aligned fields shrinks the
	// sizeof this struct by 24 bytes.

	// INVARIANT:
	// iterValidityState==IterAtLimit <=>
	//  pos==iterPosCurForwardPaused || pos==iterPosCurReversePaused
	iterValidityState IterValidityState
	// Set to true by SetBounds, SetOptions. Causes the Iterator to appear
	// exhausted externally, while preserving the correct iterValidityState for
	// the iterator's internal state. Preserving the correct internal validity
	// is used for SeekPrefixGE(..., trySeekUsingNext), and SeekGE/SeekLT
	// optimizations after "no-op" calls to SetBounds and SetOptions.
	requiresReposition bool
	// The position of iter. When this is iterPos{Prev,Next} the iter has been
	// moved past the current key-value, which can only happen if
	// iterValidityState=IterValid, i.e., there is something to return to the
	// client for the current position.
	pos iterPos
	// Relates to the prefixOrFullSeekKey field above.
	hasPrefix bool
	// Used for deriving the value of SeekPrefixGE(..., trySeekUsingNext),
	// and SeekGE/SeekLT optimizations
	lastPositioningOp lastPositioningOpKind
	// Used in some tests to disable the random disabling of seek optimizations.
	forceEnableSeekOpt bool
}

// iteratorRangeKeyState holds an iterator's range key iteration state.
type iteratorRangeKeyState struct {
	// rangeKeyIter is temporarily an iterator into a single global in-memory
	// range keys arena. This will need to be reworked when we have a merging
	// range key iterator.
	rangeKeyIter keyspan.FragmentIterator
	iter         keyspan.InterleavingIter
	// activeMaskSuffix holds the suffix of a range key currently acting as a
	// mask, hiding point keys with suffixes greater than it. activeMaskSuffix
	// is only ever non-nil if IterOptions.RangeKeyMasking.Suffix is non-nil.
	// activeMaskSuffix is updated whenever the iterator passes over a new range
	// key. See Iterator.rangeKeySpanChanged.
	activeMaskSuffix []byte
	// rangeKeyOnly is set to true if at the current iterator position there is
	// no point key, only a range key start boundary.
	rangeKeyOnly bool
	hasRangeKey  bool
	keys         bySuffix
	// start and end are the [start, end) boundaries of the current range keys.
	start []byte
	end   []byte
	// buf is used to save range-key data before moving the range-key iterator.
	// Start and end boundaries, suffixes and values are all copied into buf.
	buf []byte

	// alloc holds fields that are used for the construction of the
	// iterator stack, but do not need to be directly accessed during
	// iteration. These fields are bundled within the
	// iteratorRangeKeyState struct to reduce allocations.
	alloc struct {
		merging   keyspan.MergingIter
		defraging keyspan.DefragmentingIter
	}
}

var iterRangeKeyStateAllocPool = sync.Pool{
	New: func() interface{} {
		return &iteratorRangeKeyState{}
	},
}

type bySuffix struct {
	cmp  base.Compare
	data []RangeKeyData
}

func (s bySuffix) Len() int           { return len(s.data) }
func (s bySuffix) Less(i, j int) bool { return s.cmp(s.data[i].Suffix, s.data[j].Suffix) < 0 }
func (s bySuffix) Swap(i, j int)      { s.data[i], s.data[j] = s.data[j], s.data[i] }

// isEphemeralPosition returns true iff the current iterator position is
// ephemeral, and won't be visited during subsequent relative positioning
// operations.
//
// The iterator position resulting from a SeekGE or SeekPrefixGE that lands on a
// straddling range key without a coincident point key is such a position.
func (i *Iterator) isEphemeralPosition() bool {
	return i.opts.rangeKeys() && i.rangeKey.rangeKeyOnly && !i.equal(i.rangeKey.start, i.key)
}

type lastPositioningOpKind int8

const (
	unknownLastPositionOp lastPositioningOpKind = iota
	seekPrefixGELastPositioningOp
	seekGELastPositioningOp
	seekLTLastPositioningOp
)

// Limited iteration mode. Not for use with prefix iteration.
//
// SeekGE, SeekLT, Prev, Next have WithLimit variants, that pause the iterator
// at the limit in a best-effort manner. The client should behave correctly
// even if the limits are ignored. These limits are not "deep", in that they
// are not passed down to the underlying collection of internalIterators. This
// is because the limits are transient, and apply only until the next
// iteration call. They serve mainly as a way to bound the amount of work when
// two (or more) Iterators are being coordinated at a higher level.
//
// In limited iteration mode:
// - Avoid using Iterator.Valid if the last call was to a *WithLimit() method.
//   The return value from the *WithLimit() method provides a more precise
//   disposition.
// - The limit is exclusive for forward and inclusive for reverse.
//
//
// Limited iteration mode & range keys
//
// Limited iteration interacts with range-key iteration. When range key
// iteration is enabled, range keys are interleaved at their start boundaries.
// Limited iteration must ensure that if a range key exists within the limit,
// the iterator visits the range key.
//
// During forward limited iteration, this is trivial: An overlapping range key
// must have a start boundary less than the limit, and the range key's start
// boundary will be interleaved and found to be within the limit.
//
// During reverse limited iteration, the tail of the range key may fall within
// the limit. The range key must be surfaced even if the range key's start
// boundary is less than the limit, and if there are no point keys between the
// current iterator position and the limit. To provide this guarantee, reverse
// limited iteration ignores the limit as long as there is a range key
// overlapping the iteration position.

// IterValidityState captures the state of the Iterator.
type IterValidityState int8

const (
	// IterExhausted represents an Iterator that is exhausted.
	IterExhausted IterValidityState = iota
	// IterValid represents an Iterator that is valid.
	IterValid
	// IterAtLimit represents an Iterator that has a non-exhausted
	// internalIterator, but has reached a limit without any key for the
	// caller.
	IterAtLimit
)

// readSampling stores variables used to sample a read to trigger a read
// compaction
type readSampling struct {
	bytesUntilReadSampling uint64
	initialSamplePassed    bool
	pendingCompactions     readCompactionQueue
	// forceReadSampling is used for testing purposes to force a read sample on every
	// call to Iterator.maybeSampleRead()
	forceReadSampling bool
}

func (i *Iterator) findNextEntry(limit []byte) {
	i.iterValidityState = IterExhausted
	i.pos = iterPosCurForward
	if i.opts.rangeKeys() {
		i.rangeKey.rangeKeyOnly = false
	}

	// Close the closer for the current value if one was open.
	if i.closeValueCloser() != nil {
		return
	}

	for i.iterKey != nil {
		key := *i.iterKey

		if i.hasPrefix {
			if n := i.split(key.UserKey); !bytes.Equal(i.prefixOrFullSeekKey, key.UserKey[:n]) {
				return
			}
		}
		// Compare with limit every time we start at a different user key.
		// Note that given the best-effort contract of limit, we could avoid a
		// comparison in the common case by doing this only after
		// i.nextUserKey is called for the deletes below. However that makes
		// the behavior non-deterministic (since the behavior will vary based
		// on what has been compacted), which makes it hard to test with the
		// metamorphic test. So we forego that performance optimization.
		if limit != nil && i.cmp(limit, i.iterKey.UserKey) <= 0 {
			i.iterValidityState = IterAtLimit
			i.pos = iterPosCurForwardPaused
			return
		}

		switch key.Kind() {
		case InternalKeyKindRangeKeySet:
			// Save the current key.
			i.keyBuf = append(i.keyBuf[:0], key.UserKey...)
			i.key = i.keyBuf
			i.value = nil
			// There may also be a live point key at this userkey that we have
			// not yet read. We need to find the next entry with this user key
			// to find it. Save the range key so we don't lose it when we Next
			// the underlying iterator.
			i.saveRangeKey()
			pointKeyExists := i.nextPointCurrentUserKey()
			if i.err != nil {
				i.iterValidityState = IterExhausted
				return
			}
			i.rangeKey.rangeKeyOnly = !pointKeyExists
			i.iterValidityState = IterValid
			return

		case InternalKeyKindDelete, InternalKeyKindSingleDelete:
			i.nextUserKey()
			continue

		case InternalKeyKindSet, InternalKeyKindSetWithDelete:
			i.keyBuf = append(i.keyBuf[:0], key.UserKey...)
			i.key = i.keyBuf
			i.value = i.iterValue
			i.iterValidityState = IterValid
			i.setRangeKey()
			return

		case InternalKeyKindMerge:
			// Resolving the merge may advance us to the next point key, which
			// may be covered by a different set of range keys. Save the range
			// key state so we don't lose it.
			i.saveRangeKey()
			if i.mergeForward(key) {
				i.iterValidityState = IterValid
				return
			}

			// The merge didn't yield a valid key, either because the value
			// merger indicated it should be deleted, or because an error was
			// encountered.
			i.iterValidityState = IterExhausted
			if i.err != nil {
				return
			}
			if i.pos != iterPosNext {
				i.nextUserKey()
			}
			if i.closeValueCloser() != nil {
				return
			}
			i.pos = iterPosCurForward

		default:
			i.err = base.CorruptionErrorf("pebble: invalid internal key kind: %d", errors.Safe(key.Kind()))
			i.iterValidityState = IterExhausted
			return
		}
	}
}

func (i *Iterator) nextPointCurrentUserKey() bool {
	i.pos = iterPosCurForward

	i.iterKey, i.iterValue = i.iter.Next()
	i.stats.ForwardStepCount[InternalIterCall]++
	if i.iterKey == nil || !i.equal(i.key, i.iterKey.UserKey) {
		i.pos = iterPosNext
		return false
	}

	key := *i.iterKey
	switch key.Kind() {
	case InternalKeyKindRangeKeySet:
		// RangeKeySets must always be interleaved as the first internal key
		// for a user key.
		i.err = base.CorruptionErrorf("pebble: unexpected range key set mid-user key")
		return false

	case InternalKeyKindDelete, InternalKeyKindSingleDelete:
		return false

	case InternalKeyKindSet, InternalKeyKindSetWithDelete:
		i.value = i.iterValue
		return true

	case InternalKeyKindMerge:
		return i.mergeForward(key)

	default:
		i.err = base.CorruptionErrorf("pebble: invalid internal key kind: %d", errors.Safe(key.Kind()))
		return false
	}
}

// mergeForward resolves a MERGE key, advancing the underlying iterator forward
// to merge with subsequent keys with the same userkey. mergeForward returns a
// boolean indicating whether or not the merge yielded a valid key. A merge may
// not yield a valid key if an error occurred, in which case i.err is non-nil,
// or the user's value merger specified the key to be deleted.
//
// mergeForward does not update iterValidityState.
func (i *Iterator) mergeForward(key base.InternalKey) (valid bool) {
	var valueMerger ValueMerger
	valueMerger, i.err = i.merge(key.UserKey, i.iterValue)
	if i.err != nil {
		return false
	}

	i.mergeNext(key, valueMerger)
	if i.err != nil {
		return false
	}

	var needDelete bool
	i.value, needDelete, i.valueCloser, i.err = finishValueMerger(
		valueMerger, true /* includesBase */)
	if i.err != nil {
		return false
	}
	if needDelete {
		_ = i.closeValueCloser()
		return false
	}
	return true
}

func (i *Iterator) closeValueCloser() error {
	if i.valueCloser != nil {
		i.err = i.valueCloser.Close()
		i.valueCloser = nil
	}
	return i.err
}

func (i *Iterator) nextUserKey() {
	if i.iterKey == nil {
		return
	}
	done := i.iterKey.SeqNum() == 0
	if i.iterValidityState != IterValid {
		i.keyBuf = append(i.keyBuf[:0], i.iterKey.UserKey...)
		i.key = i.keyBuf
	}
	for {
		i.iterKey, i.iterValue = i.iter.Next()
		i.stats.ForwardStepCount[InternalIterCall]++
		if done || i.iterKey == nil {
			break
		}
		if !i.equal(i.key, i.iterKey.UserKey) {
			break
		}
		done = i.iterKey.SeqNum() == 0
	}
}

func (i *Iterator) maybeSampleRead() {
	// This method is only called when a public method of Iterator is
	// returning, and below we exclude the case were the iterator is paused at
	// a limit. The effect of these choices is that keys that are deleted, but
	// are encountered during iteration, are not accounted for in the read
	// sampling and will not cause read driven compactions, even though we are
	// incurring cost in iterating over them. And this issue is not limited to
	// Iterator, which does not see the effect of range deletes, which may be
	// causing iteration work in mergingIter. It is not clear at this time
	// whether this is a deficiency worth addressing.
	if i.iterValidityState != IterValid {
		return
	}
	if i.readState == nil {
		return
	}
	if i.readSampling.forceReadSampling {
		i.sampleRead()
		return
	}
	samplingPeriod := int32(int64(readBytesPeriod) * i.readState.db.opts.Experimental.ReadSamplingMultiplier)
	if samplingPeriod <= 0 {
		return
	}
	bytesRead := uint64(len(i.key) + len(i.value))
	for i.readSampling.bytesUntilReadSampling < bytesRead {
		i.readSampling.bytesUntilReadSampling += uint64(fastrand.Uint32n(2 * uint32(samplingPeriod)))
		// The block below tries to adjust for the case where this is the
		// first read in a newly-opened iterator. As bytesUntilReadSampling
		// starts off at zero, we don't want to sample the first read of
		// every newly-opened iterator, but we do want to sample some of them.
		if !i.readSampling.initialSamplePassed {
			i.readSampling.initialSamplePassed = true
			if fastrand.Uint32n(uint32(i.readSampling.bytesUntilReadSampling)) > uint32(bytesRead) {
				continue
			}
		}
		i.sampleRead()
	}
	i.readSampling.bytesUntilReadSampling -= bytesRead
}

func (i *Iterator) sampleRead() {
	var topFile *manifest.FileMetadata
	topLevel, numOverlappingLevels := numLevels, 0
	if mi, ok := i.iter.(*mergingIter); ok {
		if len(mi.levels) > 1 {
			mi.ForEachLevelIter(func(li *levelIter) bool {
				l := manifest.LevelToInt(li.level)
				if file := li.files.Current(); file != nil {
					var containsKey bool
					if i.pos == iterPosNext || i.pos == iterPosCurForward ||
						i.pos == iterPosCurForwardPaused {
						containsKey = i.cmp(file.Smallest.UserKey, i.key) <= 0
					} else if i.pos == iterPosPrev || i.pos == iterPosCurReverse ||
						i.pos == iterPosCurReversePaused {
						containsKey = i.cmp(file.Largest.UserKey, i.key) >= 0
					}
					// Do nothing if the current key is not contained in file's
					// bounds. We could seek the LevelIterator at this level
					// to find the right file, but the performance impacts of
					// doing that are significant enough to negate the benefits
					// of read sampling in the first place. See the discussion
					// at:
					// https://github.com/cockroachdb/pebble/pull/1041#issuecomment-763226492
					if containsKey {
						numOverlappingLevels++
						if numOverlappingLevels >= 2 {
							// Terminate the loop early if at least 2 overlapping levels are found.
							return true
						}
						topLevel = l
						topFile = file
					}
				}
				return false
			})
		}
	}
	if topFile == nil || topLevel >= numLevels {
		return
	}
	if numOverlappingLevels >= 2 {
		allowedSeeks := atomic.AddInt64(&topFile.Atomic.AllowedSeeks, -1)
		if allowedSeeks == 0 {

			// Since the compaction queue can handle duplicates, we can keep
			// adding to the queue even once allowedSeeks hits 0.
			// In fact, we NEED to keep adding to the queue, because the queue
			// is small and evicts older and possibly useful compactions.
			atomic.AddInt64(&topFile.Atomic.AllowedSeeks, topFile.InitAllowedSeeks)

			read := readCompaction{
				start:   topFile.Smallest.UserKey,
				end:     topFile.Largest.UserKey,
				level:   topLevel,
				fileNum: topFile.FileNum,
			}
			i.readSampling.pendingCompactions.add(&read, i.cmp)
		}
	}
}

func (i *Iterator) findPrevEntry(limit []byte) {
	i.iterValidityState = IterExhausted
	i.pos = iterPosCurReverse
	if i.opts.rangeKeys() {
		i.rangeKey.rangeKeyOnly = false
	}

	// Close the closer for the current value if one was open.
	if i.valueCloser != nil {
		i.err = i.valueCloser.Close()
		i.valueCloser = nil
		if i.err != nil {
			i.iterValidityState = IterExhausted
			return
		}
	}

	var valueMerger ValueMerger
	firstLoopIter := true
	rangeKeyBoundary := false
	// The code below compares with limit in multiple places. As documented in
	// findNextEntry, this is being done to make the behavior of limit
	// deterministic to allow for metamorphic testing. It is not required by
	// the best-effort contract of limit.
	for i.iterKey != nil {
		key := *i.iterKey

		// NB: We cannot pause if the current key is covered by a range key.
		// Otherwise, the user might not ever learn of a range key that covers
		// the key space being iterated over in which there are no point keys.
		// Since limits are best effort, ignoring the limit in this case is
		// allowed by the contract of limit.
		if firstLoopIter && limit != nil && i.cmp(limit, i.iterKey.UserKey) > 0 && !i.rangeKeyWithinLimit(limit) {
			i.iterValidityState = IterAtLimit
			i.pos = iterPosCurReversePaused
			return
		}
		firstLoopIter = false

		if i.iterValidityState == IterValid {
			if !i.equal(key.UserKey, i.key) {
				// We've iterated to the previous user key.
				i.pos = iterPosPrev
				if valueMerger != nil {
					var needDelete bool
					i.value, needDelete, i.valueCloser, i.err = finishValueMerger(valueMerger, true /* includesBase */)
					if i.err == nil && needDelete {
						// The point key at this key is deleted. If we also have
						// a range key boundary at this key, we still want to
						// return. Otherwise, we need to continue looking for
						// a live key.
						i.value = nil
						if rangeKeyBoundary {
							i.rangeKey.rangeKeyOnly = true
						} else {
							i.iterValidityState = IterExhausted
							if i.closeValueCloser() == nil {
								continue
							}
						}
					}
				}
				if i.err != nil {
					i.iterValidityState = IterExhausted
				}
				return
			}
		}

		switch key.Kind() {
		case InternalKeyKindRangeKeySet:
			// Range key start boundary markers are interleaved with the maximum
			// sequence number, so if there's a point key also at this key, we
			// must've already iterated over it.
			// This is the final entry at this user key, so we may return
			i.rangeKey.rangeKeyOnly = i.iterValidityState != IterValid
			i.keyBuf = append(i.keyBuf[:0], key.UserKey...)
			i.key = i.keyBuf
			i.iterValidityState = IterValid
			i.saveRangeKey()
			// In all other cases, previous iteration requires advancing to
			// iterPosPrev in order to determine if the key is live and
			// unshadowed by another key at the same user key. In this case,
			// because range key start boundary markers are always interleaved
			// at the maximum sequence number, we know that there aren't any
			// additional keys with the same user key in the backward direction.
			//
			// We Prev the underlying iterator once anyways for consistency, so
			// that we can maintain the invariant during backward iteration that
			// i.iterPos = iterPosPrev.
			i.stats.ReverseStepCount[InternalIterCall]++
			i.iterKey, i.iterValue = i.iter.Prev()

			// Set rangeKeyBoundary so that on the next iteration, we know to
			// return the key even if the MERGE point key is deleted.
			rangeKeyBoundary = true

		case InternalKeyKindDelete, InternalKeyKindSingleDelete:
			i.value = nil
			i.iterValidityState = IterExhausted
			valueMerger = nil
			i.iterKey, i.iterValue = i.iter.Prev()
			i.stats.ReverseStepCount[InternalIterCall]++
			// Compare with the limit. We could optimize by only checking when
			// we step to the previous user key, but detecting that requires a
			// comparison too. Note that this position may already passed a
			// number of versions of this user key, but they are all deleted,
			// so the fact that a subsequent Prev*() call will not see them is
			// harmless. Also note that this is the only place in the loop,
			// other than the firstLoopIter case above, where we could step
			// to a different user key and start processing it for returning
			// to the caller.
			if limit != nil && i.iterKey != nil && i.cmp(limit, i.iterKey.UserKey) > 0 && !i.rangeKeyWithinLimit(limit) {
				i.iterValidityState = IterAtLimit
				i.pos = iterPosCurReversePaused
				return
			}
			continue

		case InternalKeyKindSet, InternalKeyKindSetWithDelete:
			i.keyBuf = append(i.keyBuf[:0], key.UserKey...)
			i.key = i.keyBuf
			// iterValue is owned by i.iter and could change after the Prev()
			// call, so use valueBuf instead. Note that valueBuf is only used
			// in this one instance; everywhere else (eg. in findNextEntry),
			// we just point i.value to the unsafe i.iter-owned value buffer.
			i.valueBuf = append(i.valueBuf[:0], i.iterValue...)
			i.value = i.valueBuf
			// TODO(jackson): We may save the same range key many times. We can
			// avoid that with some help from the InterleavingIter. See also the
			// TODO in saveRangeKey.
			i.saveRangeKey()
			i.iterValidityState = IterValid
			i.iterKey, i.iterValue = i.iter.Prev()
			i.stats.ReverseStepCount[InternalIterCall]++
			valueMerger = nil
			continue

		case InternalKeyKindMerge:
			if i.iterValidityState == IterExhausted {
				i.keyBuf = append(i.keyBuf[:0], key.UserKey...)
				i.key = i.keyBuf
				i.saveRangeKey()
				valueMerger, i.err = i.merge(i.key, i.iterValue)
				if i.err != nil {
					return
				}
				i.iterValidityState = IterValid
			} else if valueMerger == nil {
				valueMerger, i.err = i.merge(i.key, i.value)
				if i.err == nil {
					i.err = valueMerger.MergeNewer(i.iterValue)
				}
				if i.err != nil {
					i.iterValidityState = IterExhausted
					return
				}
			} else {
				i.err = valueMerger.MergeNewer(i.iterValue)
				if i.err != nil {
					i.iterValidityState = IterExhausted
					return
				}
			}
			i.iterKey, i.iterValue = i.iter.Prev()
			i.stats.ReverseStepCount[InternalIterCall]++
			continue

		default:
			i.err = base.CorruptionErrorf("pebble: invalid internal key kind: %d", errors.Safe(key.Kind()))
			i.iterValidityState = IterExhausted
			return
		}
	}

	// i.iterKey == nil, so broke out of the preceding loop.
	if i.iterValidityState == IterValid {
		i.pos = iterPosPrev
		if valueMerger != nil {
			var needDelete bool
			i.value, needDelete, i.valueCloser, i.err = finishValueMerger(valueMerger, true /* includesBase */)
			if i.err == nil && needDelete {
				i.key = nil
				i.value = nil
				i.iterValidityState = IterExhausted
			}
		}
		if i.err != nil {
			i.iterValidityState = IterExhausted
		}
	}
}

func (i *Iterator) prevUserKey() {
	if i.iterKey == nil {
		return
	}
	if i.iterValidityState != IterValid {
		// If we're going to compare against the prev key, we need to save the
		// current key.
		i.keyBuf = append(i.keyBuf[:0], i.iterKey.UserKey...)
		i.key = i.keyBuf
	}
	for {
		i.iterKey, i.iterValue = i.iter.Prev()
		i.stats.ReverseStepCount[InternalIterCall]++
		if i.iterKey == nil {
			break
		}
		if !i.equal(i.key, i.iterKey.UserKey) {
			break
		}
	}
}

func (i *Iterator) mergeNext(key InternalKey, valueMerger ValueMerger) {
	// Save the current key.
	i.keyBuf = append(i.keyBuf[:0], key.UserKey...)
	i.key = i.keyBuf

	// Loop looking for older values for this key and merging them.
	for {
		i.iterKey, i.iterValue = i.iter.Next()
		i.stats.ForwardStepCount[InternalIterCall]++
		if i.iterKey == nil {
			i.pos = iterPosNext
			return
		}
		key = *i.iterKey
		if !i.equal(i.key, key.UserKey) {
			// We've advanced to the next key.
			i.pos = iterPosNext
			return
		}
		switch key.Kind() {
		case InternalKeyKindDelete, InternalKeyKindSingleDelete:
			// We've hit a deletion tombstone. Return everything up to this
			// point.
			return

		case InternalKeyKindSet, InternalKeyKindSetWithDelete:
			// We've hit a Set value. Merge with the existing value and return.
			i.err = valueMerger.MergeOlder(i.iterValue)
			return

		case InternalKeyKindMerge:
			// We've hit another Merge value. Merge with the existing value and
			// continue looping.
			i.err = valueMerger.MergeOlder(i.iterValue)
			if i.err != nil {
				return
			}
			continue

		case InternalKeyKindRangeKeySet:
			// The RANGEKEYSET marker must sort before a MERGE at the same user key.
			i.err = base.CorruptionErrorf("pebble: out of order range key marker")
			return

		default:
			i.err = base.CorruptionErrorf("pebble: invalid internal key kind: %d", errors.Safe(key.Kind()))
			return
		}
	}
}

// SeekGE moves the iterator to the first key/value pair whose key is greater
// than or equal to the given key. Returns true if the iterator is pointing at
// a valid entry and false otherwise.
func (i *Iterator) SeekGE(key []byte) bool {
	return i.SeekGEWithLimit(key, nil) == IterValid
}

// SeekGEWithLimit moves the iterator to the first key/value pair whose key is
// greater than or equal to the given key.
//
// If limit is provided, it serves as a best-effort exclusive limit. If the
// first key greater than or equal to the given search key is also greater than
// or equal to limit, the Iterator may pause and return IterAtLimit. Because
// limits are best-effort, SeekGEWithLimit may return a key beyond limit.
//
// If the Iterator is configured to iterate over range keys, SeekGEWithLimit
// guarantees it will surface any range keys with bounds overlapping the
// keyspace [key, limit).
func (i *Iterator) SeekGEWithLimit(key []byte, limit []byte) IterValidityState {
	lastPositioningOp := i.lastPositioningOp
	// Set it to unknown, since this operation may not succeed, in which case
	// the SeekGE following this should not make any assumption about iterator
	// position.
	i.lastPositioningOp = unknownLastPositionOp
	i.requiresReposition = false
	i.err = nil // clear cached iteration error
	i.hasPrefix = false
	i.stats.ForwardSeekCount[InterfaceCall]++
	if lowerBound := i.opts.GetLowerBound(); lowerBound != nil && i.cmp(key, lowerBound) < 0 {
		key = lowerBound
	} else if upperBound := i.opts.GetUpperBound(); upperBound != nil && i.cmp(key, upperBound) > 0 {
		key = upperBound
	}
	seekInternalIter := true
	trySeekUsingNext := false
	// The following noop optimization only applies when i.batch == nil, since
	// an iterator over a batch is iterating over mutable data, that may have
	// changed since the last seek.
	if lastPositioningOp == seekGELastPositioningOp && i.batch == nil {
		cmp := i.cmp(i.prefixOrFullSeekKey, key)
		// If this seek is to the same or later key, and the iterator is
		// already positioned there, this is a noop. This can be helpful for
		// sparse key spaces that have many deleted keys, where one can avoid
		// the overhead of iterating past them again and again.
		if cmp <= 0 {
			if i.iterValidityState == IterExhausted ||
				(i.iterValidityState == IterValid && i.cmp(key, i.key) <= 0 &&
					(limit == nil || i.cmp(i.key, limit) < 0)) {
				// Noop
				if !invariants.Enabled || !disableSeekOpt(key, uintptr(unsafe.Pointer(i))) || i.forceEnableSeekOpt {
					i.lastPositioningOp = seekGELastPositioningOp
					return i.iterValidityState
				}
			}
			// cmp == 0 is not safe to optimize since
			// - i.pos could be at iterPosNext, due to a merge.
			// - Even if i.pos were at iterPosCurForward, we could have a DELETE,
			//   SET pair for a key, and the iterator would have moved past DELETE
			//   but stayed at iterPosCurForward. A similar situation occurs for a
			//   MERGE, SET pair where the MERGE is consumed and the iterator is
			//   at the SET.
			// We also leverage the IterAtLimit <=> i.pos invariant defined in the
			// comment on iterValidityState, to exclude any cases where i.pos
			// is iterPosCur{Forward,Reverse}Paused. This avoids the need to
			// special-case those iterator positions and their interactions with
			// trySeekUsingNext, as the main uses for trySeekUsingNext in CockroachDB
			// do not use limited Seeks in the first place.
			trySeekUsingNext = cmp < 0 && i.iterValidityState != IterAtLimit && limit == nil
			if invariants.Enabled && trySeekUsingNext && !i.forceEnableSeekOpt && disableSeekOpt(key, uintptr(unsafe.Pointer(i))) {
				trySeekUsingNext = false
			}
			if i.pos == iterPosCurForwardPaused && i.cmp(key, i.iterKey.UserKey) <= 0 {
				// Have some work to do, but don't need to seek, and we can
				// start doing findNextEntry from i.iterKey.
				seekInternalIter = false
			}
		}
	}
	if seekInternalIter {
		i.iterKey, i.iterValue = i.iter.SeekGE(key, trySeekUsingNext)
		i.stats.ForwardSeekCount[InternalIterCall]++
	}
	i.findNextEntry(limit)
	i.maybeSampleRead()
	if i.Error() == nil && i.batch == nil {
		// Prepare state for a future noop optimization.
		i.prefixOrFullSeekKey = append(i.prefixOrFullSeekKey[:0], key...)
		i.lastPositioningOp = seekGELastPositioningOp
	}
	return i.iterValidityState
}

// SeekPrefixGE moves the iterator to the first key/value pair whose key is
// greater than or equal to the given key and which has the same "prefix" as
// the given key. The prefix for a key is determined by the user-defined
// Comparer.Split function. The iterator will not observe keys not matching the
// "prefix" of the search key. Calling SeekPrefixGE puts the iterator in prefix
// iteration mode. The iterator remains in prefix iteration until a subsequent
// call to another absolute positioning method (SeekGE, SeekLT, First,
// Last). Reverse iteration (Prev) is not supported when an iterator is in
// prefix iteration mode. Returns true if the iterator is pointing at a valid
// entry and false otherwise.
//
// The semantics of SeekPrefixGE are slightly unusual and designed for
// iteration to be able to take advantage of bloom filters that have been
// created on the "prefix". If you're not using bloom filters, there is no
// reason to use SeekPrefixGE.
//
// An example Split function may separate a timestamp suffix from the prefix of
// the key.
//
//   Split(<key>@<timestamp>) -> <key>
//
// Consider the keys "a@1", "a@2", "aa@3", "aa@4". The prefixes for these keys
// are "a", and "aa". Note that despite "a" and "aa" sharing a prefix by the
// usual definition, those prefixes differ by the definition of the Split
// function. To see how this works, consider the following set of calls on this
// data set:
//
//   SeekPrefixGE("a@0") -> "a@1"
//   Next()              -> "a@2"
//   Next()              -> EOF
//
// If you're just looking to iterate over keys with a shared prefix, as
// defined by the configured comparer, set iterator bounds instead:
//
//  iter := db.NewIter(&pebble.IterOptions{
//    LowerBound: []byte("prefix"),
//    UpperBound: []byte("prefiy"),
//  })
//  for iter.First(); iter.Valid(); iter.Next() {
//    // Only keys beginning with "prefix" will be visited.
//  }
//
// See ExampleIterator_SeekPrefixGE for a working example.
func (i *Iterator) SeekPrefixGE(key []byte) bool {
	lastPositioningOp := i.lastPositioningOp
	// Set it to unknown, since this operation may not succeed, in which case
	// the SeekPrefixGE following this should not make any assumption about
	// iterator position.
	i.lastPositioningOp = unknownLastPositionOp
	i.requiresReposition = false
	i.err = nil // clear cached iteration error
	i.stats.ForwardSeekCount[InterfaceCall]++

	if i.split == nil {
		panic("pebble: split must be provided for SeekPrefixGE")
	}

	prefixLen := i.split(key)
	keyPrefix := key[:prefixLen]
	trySeekUsingNext := false
	if lastPositioningOp == seekPrefixGELastPositioningOp {
		if !i.hasPrefix {
			panic("lastPositioningOpsIsSeekPrefixGE is true, but hasPrefix is false")
		}
		// The iterator has not been repositioned after the last SeekPrefixGE.
		// See if we are seeking to a larger key, since then we can optimize
		// the seek by using next. Note that we could also optimize if Next
		// has been called, if the iterator is not exhausted and the current
		// position is <= the seek key. We are keeping this limited for now
		// since such optimizations require care for correctness, and to not
		// become de-optimizations (if one usually has to do all the next
		// calls and then the seek). This SeekPrefixGE optimization
		// specifically benefits CockroachDB.
		cmp := i.cmp(i.prefixOrFullSeekKey, keyPrefix)
		// cmp == 0 is not safe to optimize since
		// - i.pos could be at iterPosNext, due to a merge.
		// - Even if i.pos were at iterPosCurForward, we could have a DELETE,
		//   SET pair for a key, and the iterator would have moved past DELETE
		//   but stayed at iterPosCurForward. A similar situation occurs for a
		//   MERGE, SET pair where the MERGE is consumed and the iterator is
		//   at the SET.
		// In general some versions of i.prefix could have been consumed by
		// the iterator, so we only optimize for cmp < 0.
		trySeekUsingNext = cmp < 0
		if invariants.Enabled && trySeekUsingNext && !i.forceEnableSeekOpt && disableSeekOpt(key, uintptr(unsafe.Pointer(i))) {
			trySeekUsingNext = false
		}
	}
	// Make a copy of the prefix so that modifications to the key after
	// SeekPrefixGE returns does not affect the stored prefix.
	if cap(i.prefixOrFullSeekKey) < prefixLen {
		i.prefixOrFullSeekKey = make([]byte, prefixLen)
	} else {
		i.prefixOrFullSeekKey = i.prefixOrFullSeekKey[:prefixLen]
	}
	i.hasPrefix = true
	copy(i.prefixOrFullSeekKey, keyPrefix)

	if lowerBound := i.opts.GetLowerBound(); lowerBound != nil && i.cmp(key, lowerBound) < 0 {
		if n := i.split(lowerBound); !bytes.Equal(i.prefixOrFullSeekKey, lowerBound[:n]) {
			i.err = errors.New("pebble: SeekPrefixGE supplied with key outside of lower bound")
			i.iterValidityState = IterExhausted
			return false
		}
		key = lowerBound
	} else if upperBound := i.opts.GetUpperBound(); upperBound != nil && i.cmp(key, upperBound) > 0 {
		if n := i.split(upperBound); !bytes.Equal(i.prefixOrFullSeekKey, upperBound[:n]) {
			i.err = errors.New("pebble: SeekPrefixGE supplied with key outside of upper bound")
			i.iterValidityState = IterExhausted
			return false
		}
		key = upperBound
	}

	i.iterKey, i.iterValue = i.iter.SeekPrefixGE(i.prefixOrFullSeekKey, key, trySeekUsingNext)
	i.stats.ForwardSeekCount[InternalIterCall]++
	i.findNextEntry(nil)
	i.maybeSampleRead()
	if i.Error() == nil {
		i.lastPositioningOp = seekPrefixGELastPositioningOp
	}
	return i.iterValidityState == IterValid
}

// Deterministic disabling of the seek optimization. It uses the iterator
// pointer, since we want diversity in iterator behavior for the same key.
// Used for tests.
func disableSeekOpt(key []byte, ptr uintptr) bool {
	// Fibonacci hash https://probablydance.com/2018/06/16/fibonacci-hashing-the-optimization-that-the-world-forgot-or-a-better-alternative-to-integer-modulo/
	simpleHash := (11400714819323198485 * uint64(ptr)) >> 63
	return key != nil && key[0]&byte(1) == 0 && simpleHash == 0
}

// SeekLT moves the iterator to the last key/value pair whose key is less than
// the given key. Returns true if the iterator is pointing at a valid entry and
// false otherwise.
func (i *Iterator) SeekLT(key []byte) bool {
	return i.SeekLTWithLimit(key, nil) == IterValid
}

// SeekLTWithLimit moves the iterator to the last key/value pair whose key is
// less than the given key.
//
// If limit is provided, it serves as a best-effort inclusive limit. If the last
// key less than the given search key is also less than limit, the Iterator may
// pause and return IterAtLimit. Because limits are best-effort, SeekLTWithLimit
// may return a key beyond limit.
//
// If the Iterator is configured to iterate over range keys, SeekLTWithLimit
// guarantees it will surface any range keys with bounds overlapping the
// keyspace up to limit.
func (i *Iterator) SeekLTWithLimit(key []byte, limit []byte) IterValidityState {
	lastPositioningOp := i.lastPositioningOp
	// Set it to unknown, since this operation may not succeed, in which case
	// the SeekLT following this should not make any assumption about iterator
	// position.
	i.lastPositioningOp = unknownLastPositionOp
	i.requiresReposition = false
	i.err = nil // clear cached iteration error
	i.hasPrefix = false
	i.stats.ReverseSeekCount[InterfaceCall]++
	if upperBound := i.opts.GetUpperBound(); upperBound != nil && i.cmp(key, upperBound) > 0 {
		key = upperBound
	} else if lowerBound := i.opts.GetLowerBound(); lowerBound != nil && i.cmp(key, lowerBound) < 0 {
		key = lowerBound
	}
	seekInternalIter := true
	// The following noop optimization only applies when i.batch == nil, since
	// an iterator over a batch is iterating over mutable data, that may have
	// changed since the last seek.
	if lastPositioningOp == seekLTLastPositioningOp && i.batch == nil {
		cmp := i.cmp(key, i.prefixOrFullSeekKey)
		// If this seek is to the same or earlier key, and the iterator is
		// already positioned there, this is a noop. This can be helpful for
		// sparse key spaces that have many deleted keys, where one can avoid
		// the overhead of iterating past them again and again.
		if cmp <= 0 {
			// NB: when pos != iterPosCurReversePaused, the invariant
			// documented earlier implies that iterValidityState !=
			// IterAtLimit.
			if i.iterValidityState == IterExhausted ||
				(i.iterValidityState == IterValid && i.cmp(i.key, key) < 0 &&
					(limit == nil || i.cmp(limit, i.key) <= 0)) {
				if !invariants.Enabled || !disableSeekOpt(key, uintptr(unsafe.Pointer(i))) {
					i.lastPositioningOp = seekLTLastPositioningOp
					return i.iterValidityState
				}
			}
			if i.pos == iterPosCurReversePaused && i.cmp(i.iterKey.UserKey, key) < 0 {
				// Have some work to do, but don't need to seek, and we can
				// start doing findPrevEntry from i.iterKey.
				seekInternalIter = false
			}
		}
	}
	if seekInternalIter {
		i.iterKey, i.iterValue = i.iter.SeekLT(key)
		i.stats.ReverseSeekCount[InternalIterCall]++
	}
	i.findPrevEntry(limit)
	i.maybeSampleRead()
	if i.Error() == nil && i.batch == nil {
		// Prepare state for a future noop optimization.
		i.prefixOrFullSeekKey = append(i.prefixOrFullSeekKey[:0], key...)
		i.lastPositioningOp = seekLTLastPositioningOp
	}
	return i.iterValidityState
}

// First moves the iterator the the first key/value pair. Returns true if the
// iterator is pointing at a valid entry and false otherwise.
func (i *Iterator) First() bool {
	i.err = nil // clear cached iteration error
	i.hasPrefix = false
	i.lastPositioningOp = unknownLastPositionOp
	i.requiresReposition = false
	i.stats.ForwardSeekCount[InterfaceCall]++
	if lowerBound := i.opts.GetLowerBound(); lowerBound != nil {
		i.iterKey, i.iterValue = i.iter.SeekGE(lowerBound, false /* trySeekUsingNext */)
		i.stats.ForwardSeekCount[InternalIterCall]++
	} else {
		i.iterKey, i.iterValue = i.iter.First()
		i.stats.ForwardSeekCount[InternalIterCall]++
	}
	i.findNextEntry(nil)
	i.maybeSampleRead()
	return i.iterValidityState == IterValid
}

// Last moves the iterator the the last key/value pair. Returns true if the
// iterator is pointing at a valid entry and false otherwise.
func (i *Iterator) Last() bool {
	i.err = nil // clear cached iteration error
	i.hasPrefix = false
	i.lastPositioningOp = unknownLastPositionOp
	i.requiresReposition = false
	i.stats.ReverseSeekCount[InterfaceCall]++
	if upperBound := i.opts.GetUpperBound(); upperBound != nil {
		i.iterKey, i.iterValue = i.iter.SeekLT(upperBound)
		i.stats.ReverseSeekCount[InternalIterCall]++
	} else {
		i.iterKey, i.iterValue = i.iter.Last()
		i.stats.ReverseSeekCount[InternalIterCall]++
	}
	i.findPrevEntry(nil)
	i.maybeSampleRead()
	return i.iterValidityState == IterValid
}

// Next moves the iterator to the next key/value pair. Returns true if the
// iterator is pointing at a valid entry and false otherwise.
func (i *Iterator) Next() bool {
	return i.NextWithLimit(nil) == IterValid
}

// NextWithLimit moves the iterator to the next key/value pair.
//
// If limit is provided, it serves as a best-effort exclusive limit. If the next
// key  is greater than or equal to limit, the Iterator may pause and return
// IterAtLimit. Because limits are best-effort, NextWithLimit may return a key
// beyond limit.
//
// If the Iterator is configured to iterate over range keys, NextWithLimit
// guarantees it will surface any range keys with bounds overlapping the
// keyspace up to limit.
func (i *Iterator) NextWithLimit(limit []byte) IterValidityState {
	i.stats.ForwardStepCount[InterfaceCall]++
	if limit != nil && i.hasPrefix {
		i.err = errors.New("cannot use limit with prefix iteration")
		i.iterValidityState = IterExhausted
		return i.iterValidityState
	}
	if i.err != nil {
		return i.iterValidityState
	}
	i.lastPositioningOp = unknownLastPositionOp
	i.requiresReposition = false
	switch i.pos {
	case iterPosCurForward:
		i.nextUserKey()
	case iterPosCurForwardPaused:
		// Already at the right place.
	case iterPosCurReverse:
		// Switching directions.
		// Unless the iterator was exhausted, reverse iteration needs to
		// position the iterator at iterPosPrev.
		if i.iterKey != nil {
			i.err = errors.New("switching from reverse to forward but iter is not at prev")
			i.iterValidityState = IterExhausted
			return i.iterValidityState
		}
		// We're positioned before the first key. Need to reposition to point to
		// the first key.
		if lowerBound := i.opts.GetLowerBound(); lowerBound != nil {
			i.iterKey, i.iterValue = i.iter.SeekGE(lowerBound, false /* trySeekUsingNext */)
			i.stats.ForwardSeekCount[InternalIterCall]++
		} else {
			i.iterKey, i.iterValue = i.iter.First()
			i.stats.ForwardSeekCount[InternalIterCall]++
		}
	case iterPosCurReversePaused:
		// Switching directions.
		// The iterator must not be exhausted since it paused.
		if i.iterKey == nil {
			i.err = errors.New("switching paused from reverse to forward but iter is exhausted")
			i.iterValidityState = IterExhausted
			return i.iterValidityState
		}
		i.nextUserKey()
	case iterPosPrev:
		// The underlying iterator is pointed to the previous key (this can
		// only happen when switching iteration directions). We set
		// i.iterValidityState to IterExhausted here to force the calls to
		// nextUserKey to save the current key i.iter is pointing at in order
		// to determine when the next user-key is reached.
		i.iterValidityState = IterExhausted
		if i.iterKey == nil {
			// We're positioned before the first key. Need to reposition to point to
			// the first key.
			if lowerBound := i.opts.GetLowerBound(); lowerBound != nil {
				i.iterKey, i.iterValue = i.iter.SeekGE(lowerBound, false /* trySeekUsingNext */)
				i.stats.ForwardSeekCount[InternalIterCall]++
			} else {
				i.iterKey, i.iterValue = i.iter.First()
				i.stats.ForwardSeekCount[InternalIterCall]++
			}
		} else {
			i.nextUserKey()
		}
		i.nextUserKey()
	case iterPosNext:
		// Already at the right place.
	}
	i.findNextEntry(limit)
	i.maybeSampleRead()
	return i.iterValidityState
}

// Prev moves the iterator to the previous key/value pair. Returns true if the
// iterator is pointing at a valid entry and false otherwise.
func (i *Iterator) Prev() bool {
	return i.PrevWithLimit(nil) == IterValid
}

// PrevWithLimit moves the iterator to the previous key/value pair.
//
// If limit is provided, it serves as a best-effort inclusive limit. If the
// previous key is less than limit, the Iterator may pause and return
// IterAtLimit. Because limits are best-effort, PrevWithLimit may return a key
// beyond limit.
//
// If the Iterator is configured to iterate over range keys, PrevWithLimit
// guarantees it will surface any range keys with bounds overlapping the
// keyspace up to limit.
func (i *Iterator) PrevWithLimit(limit []byte) IterValidityState {
	i.stats.ReverseStepCount[InterfaceCall]++
	if i.err != nil {
		return i.iterValidityState
	}
	i.lastPositioningOp = unknownLastPositionOp
	i.requiresReposition = false
	if i.hasPrefix {
		i.err = errReversePrefixIteration
		i.iterValidityState = IterExhausted
		return i.iterValidityState
	}
	switch i.pos {
	case iterPosCurForward:
		// Switching directions, and will handle this below.
	case iterPosCurForwardPaused:
		// Switching directions, and will handle this below.
	case iterPosCurReverse:
		i.prevUserKey()
	case iterPosCurReversePaused:
		// Already at the right place.
	case iterPosNext:
		// The underlying iterator is pointed to the next key (this can only happen
		// when switching iteration directions). We will handle this below.
	case iterPosPrev:
		// Already at the right place.
	}
	if i.pos == iterPosCurForward || i.pos == iterPosNext || i.pos == iterPosCurForwardPaused {
		// Switching direction.
		stepAgain := i.pos == iterPosNext

		// Synthetic range key markers are a special case. Consider SeekGE(b)
		// which finds a range key [a, c). To ensure the user observes the range
		// key, the Iterator pauses at Key() = b. The iterator must advance the
		// internal iterator to see if there's also a coincident point key at
		// 'b', leaving the iterator at iterPosNext if there's not.
		//
		// This is a problem: Synthetic range key markers are only interleaved
		// during the original seek. A subsequent Prev() of i.iter will not move
		// back onto the synthetic range key marker. In this case where the
		// previous iterator position was a synthetic range key start boundary,
		// we must not step a second time.
		if i.isEphemeralPosition() {
			stepAgain = false
		}

		// We set i.iterValidityState to IterExhausted here to force the calls
		// to prevUserKey to save the current key i.iter is pointing at in
		// order to determine when the prev user-key is reached.
		i.iterValidityState = IterExhausted
		if i.iterKey == nil {
			// We're positioned after the last key. Need to reposition to point to
			// the last key.
			if upperBound := i.opts.GetUpperBound(); upperBound != nil {
				i.iterKey, i.iterValue = i.iter.SeekLT(upperBound)
				i.stats.ReverseSeekCount[InternalIterCall]++
			} else {
				i.iterKey, i.iterValue = i.iter.Last()
				i.stats.ReverseSeekCount[InternalIterCall]++
			}
		} else {
			i.prevUserKey()
		}
		if stepAgain {
			i.prevUserKey()
		}
	}
	i.findPrevEntry(limit)
	i.maybeSampleRead()
	return i.iterValidityState
}

// RangeKeyData describes a range key's data, set through RangeKeySet. The key
// boundaries of the range key is provided by Iterator.RangeBounds.
type RangeKeyData struct {
	Suffix []byte
	Value  []byte
}

// rangeKeyWithinLimit is called during limited reverse iteration when
// positioned over a key beyond the limit. If there exists a range key that lies
// within the limit, the iterator must not pause in order to ensure the user has
// an opportunity to observe the range key within limit.
//
// It would be valid to ignore the limit whenever there's a range key covering
// the key, but that would introduce nondeterminism. To preserve determinism for
// testing, the iterator ignores the limit only if the covering range key does
// cover the keyspace within the limit.
//
// This awkwardness exists because range keys are interleaved at their inclusive
// start positions. Note that limit is inclusive.
func (i *Iterator) rangeKeyWithinLimit(limit []byte) bool {
	if !i.opts.rangeKeys() {
		return false
	}
	s := i.rangeKey.iter.Span()
	if s.Empty() {
		// If there are no covering range keys, it is safe to to pause
		// immediately.
		return false
	}
	// If the range key ends beyond the limit, then the range key does not cover
	// any portion of the keyspace within the limit and it is safe to pause.
	if i.cmp(s.End, limit) <= 0 {
		return false
	}
	return true
}

func (i *Iterator) rangeKeySpanChanged(s keyspan.Span) {
	i.rangeKey.activeMaskSuffix = i.rangeKey.activeMaskSuffix[:0]

	// Find the smallest suffix of a range key contained within the Span,
	// excluding suffixes less than i.opts.RangeKeyMasking.Suffix.
	if i.opts.RangeKeyMasking.Suffix == nil || s.Empty() {
		return
	}
	for j := range s.Keys {
		if s.Keys[j].Suffix == nil {
			continue
		}
		if i.cmp(s.Keys[j].Suffix, i.opts.RangeKeyMasking.Suffix) < 0 {
			continue
		}
		if len(i.rangeKey.activeMaskSuffix) == 0 || i.cmp(i.rangeKey.activeMaskSuffix, s.Keys[j].Suffix) > 0 {
			i.rangeKey.activeMaskSuffix = append(i.rangeKey.activeMaskSuffix[:0], s.Keys[j].Suffix...)
		}
	}
}

// rangeKeySkipPoint is installed as a keyspan.InterleavingIter's SkipPoint
// hook during range key iteration. Whenever a point key is covered by a
// non-empty Span, the interleaving iterator invokes the SkipPoint hook. This
// function is responsible for performing range key masking.
//
// If a non-nil IterOptions.RangeKeyMasking.Suffix is set, range key masking is
// enabled. Masking hides point keys, transparently skipping over the keys.
// Whether or not a point key is masked is determined by comparing the point
// key's suffix, the overlapping span's keys' suffixes, and the user-configured
// IterOption's RangeKeyMasking.Suffix. When configured with a masking threshold
// _t_, and there exists a span with suffix _r_ covering a point key with suffix
// _p_, and
//
//     _t_ ≤ _r_ < _p_
//
// then the point key is elided. Consider the following rendering, where
// suffixes with higher integers sort before suffixes with lower integers:
//
//          ^
//       @9 |        •―――――――――――――――○ [e,m)@9
//     s  8 |                      • l@8
//     u  7 |------------------------------------ @7 RangeKeyMasking.Suffix
//     f  6 |      [h,q)@6 •―――――――――――――――――○            (threshold)
//     f  5 |              • h@5
//     f  4 |                          • n@4
//     i  3 |          •―――――――――――○ [f,l)@3
//     x  2 |  • b@2
//        1 |
//        0 |___________________________________
//           a b c d e f g h i j k l m n o p q
//
// An iterator scanning the entire keyspace with the masking threshold set to @7
// will observe point keys b@2 and l@8. The span keys [h,q)@6 and [f,l)@3 serve
// as masks, because cmp(@6,@7) ≥ 0 and cmp(@3,@7) ≥ 0. The span key [e,m)@9
// does not serve as a mask, because cmp(@9,@7) < 0.
//
// Although point l@8 falls within the user key bounds of [e,m)@9, [e,m)@9 is
// non-masking due to its suffix. The point key l@8 also falls within the user
// key bounds of [h,q)@6, but since cmp(@6,@8) ≥ 0, l@8 is unmasked.
//
// Invariant: userKey is within the user key bounds of i.rangeKey.iter.Span().
func (i *Iterator) rangeKeySkipPoint(userKey []byte) bool {
	if len(i.rangeKey.activeMaskSuffix) == 0 {
		// No range key is currently acting as a mask, so don't skip.
		return false
	}
	// Range key masking is enabled and the current span includes a range key
	// that is being used as a mask. (NB: rangeKeySpanChanged already verified
	// that the range key's suffix is ≥ RangeKeyMasking.Suffix).
	//
	// This point key falls within the bounds of the range key (guaranteed by
	// the InterleavingIter). Skip the point key if the range key's suffix is
	// greater than the point key's suffix.
	pointSuffix := userKey[i.split(userKey):]
	return len(pointSuffix) > 0 && i.cmp(i.rangeKey.activeMaskSuffix, pointSuffix) < 0
}

// setRangeKey sets the current range key to the underlying iterator's current
// range key state. It does not make copies of any of the key, value or suffix
// buffers, so it must only be used if the underlying iterator's position
// matches the top-level iterator (eg, i.pos = iterPosCur*).
func (i *Iterator) setRangeKey() {
	if !i.opts.rangeKeys() {
		return
	}
	s := i.rangeKey.iter.Span()
	i.rangeKey.hasRangeKey = !s.Empty()
	if !i.rangeKey.hasRangeKey {
		// Clear out existing pointers, so that we don't unintentionally retain
		// any old range key blocks.
		i.rangeKey.start = nil
		i.rangeKey.end = nil
		for j := 0; j < i.rangeKey.keys.Len(); j++ {
			i.rangeKey.keys.data[j].Suffix = nil
			i.rangeKey.keys.data[j].Value = nil
		}
		i.rangeKey.keys.data = i.rangeKey.keys.data[:0]
		return
	}
	i.rangeKey.start, i.rangeKey.end = s.Start, s.End
	i.rangeKey.keys.data = i.rangeKey.keys.data[:0]
	for j := 0; j < len(s.Keys); j++ {
		if s.Keys[j].Kind() == base.InternalKeyKindRangeKeySet {
			i.rangeKey.keys.data = append(i.rangeKey.keys.data, RangeKeyData{
				Suffix: s.Keys[j].Suffix,
				Value:  s.Keys[j].Value,
			})
		}
	}
	sort.Sort(i.rangeKey.keys)
}

// saveRangeKey sets the current range key to the underlying iterator's current
// range key state, copying all of the key, value and suffixes into
// Iterator-managed buffers. Callers should prefer setRangeKey if under no
// circumstances the underlying iterator will be advanced to the next user key
// before returning to the user.
func (i *Iterator) saveRangeKey() {
	if !i.opts.rangeKeys() {
		return
	}
	s := i.rangeKey.iter.Span()
	i.rangeKey.hasRangeKey = !s.Empty()
	if !i.rangeKey.hasRangeKey {
		return
	}
	// TODO(jackson): Rather than naively copying all the range key state every
	// time, we could copy only if it actually changed from the currently saved
	// state, with some help from the InterleavingIter.

	i.rangeKey.buf = append(i.rangeKey.buf[:0], s.Start...)
	i.rangeKey.start = i.rangeKey.buf
	i.rangeKey.buf = append(i.rangeKey.buf, s.End...)
	i.rangeKey.end = i.rangeKey.buf[len(i.rangeKey.buf)-len(s.End):]

	i.rangeKey.keys.data = i.rangeKey.keys.data[:0]
	for j := 0; j < len(s.Keys); j++ {
		if s.Keys[j].Kind() == base.InternalKeyKindRangeKeySet {
			i.rangeKey.buf = append(i.rangeKey.buf, s.Keys[j].Suffix...)
			suffix := i.rangeKey.buf[len(i.rangeKey.buf)-len(s.Keys[j].Suffix):]
			i.rangeKey.buf = append(i.rangeKey.buf, s.Keys[j].Value...)
			value := i.rangeKey.buf[len(i.rangeKey.buf)-len(s.Keys[j].Value):]
			i.rangeKey.keys.data = append(i.rangeKey.keys.data, RangeKeyData{
				Suffix: suffix,
				Value:  value,
			})
		}
	}
	sort.Sort(i.rangeKey.keys)
}

// HasPointAndRange indicates whether there exists a point key, a range key or
// both at the current iterator position.
func (i *Iterator) HasPointAndRange() (hasPoint, hasRange bool) {
	if i.iterValidityState != IterValid {
		return false, false
	}
	if !i.opts.rangeKeys() {
		return true, false
	}
	return !i.rangeKey.rangeKeyOnly, i.rangeKey.hasRangeKey
}

// RangeBounds returns the start (inclusive) and end (exclusive) bounds of the
// range key covering the current iterator position. RangeBounds returns nil
// bounds if there is no range key covering the current iterator position, or
// the iterator is not configured to surface range keys.
func (i *Iterator) RangeBounds() (start, end []byte) {
	if !i.opts.rangeKeys() || !i.rangeKey.hasRangeKey {
		return nil, nil
	}
	return i.rangeKey.start, i.rangeKey.end
}

// Key returns the key of the current key/value pair, or nil if done. The
// caller should not modify the contents of the returned slice, and its
// contents may change on the next call to Next.
func (i *Iterator) Key() []byte {
	return i.key
}

// Value returns the value of the current key/value pair, or nil if done. The
// caller should not modify the contents of the returned slice, and its
// contents may change on the next call to Next.
//
// Only valid if HasPointAndRange() returns true for hasPoint.
func (i *Iterator) Value() []byte {
	return i.value
}

// RangeKeys returns the range key values and their suffixes covering the
// current iterator position. The range bounds may be retrieved separately
// through Iterator.RangeBounds().
func (i *Iterator) RangeKeys() []RangeKeyData {
	if !i.opts.rangeKeys() || !i.rangeKey.hasRangeKey {
		return nil
	}
	return i.rangeKey.keys.data
}

// Valid returns true if the iterator is positioned at a valid key/value pair
// and false otherwise.
func (i *Iterator) Valid() bool {
	return i.iterValidityState == IterValid && !i.requiresReposition
}

// Error returns any accumulated error.
func (i *Iterator) Error() error {
	err := i.err
	if i.iter != nil {
		err = firstError(i.err, i.iter.Error())
	}
	return err
}

// Close closes the iterator and returns any accumulated error. Exhausting
// all the key/value pairs in a table is not considered to be an error.
// It is not valid to call any method, including Close, after the iterator
// has been closed.
func (i *Iterator) Close() error {
	// Close the child iterator before releasing the readState because when the
	// readState is released sstables referenced by the readState may be deleted
	// which will fail on Windows if the sstables are still open by the child
	// iterator.
	if i.iter != nil {
		i.err = firstError(i.err, i.iter.Close())

		// Closing i.iter did not necessarily close the point and range key
		// iterators. Calls to SetOptions may have 'disconnected' either one
		// from i.iter if iteration key types were changed. Both point and range
		// key iterators are preserved in case the iterator needs to switch key
		// types again. We explicitly close both of these iterators here.
		//
		// NB: If the iterators were still connected to i.iter, they may be
		// closed, but calling Close on a closed internal iterator or fragment
		// iterator is allowed.
		if i.pointIter != nil {
			i.err = firstError(i.err, i.pointIter.Close())
		}
		if i.rangeKey != nil {
			i.err = firstError(i.err, i.rangeKey.rangeKeyIter.Close())
		}
	}
	err := i.err

	if i.readState != nil {
		if i.readSampling.pendingCompactions.size > 0 {
			// Copy pending read compactions using db.mu.Lock()
			i.readState.db.mu.Lock()
			i.readState.db.mu.compact.readCompactions.combine(&i.readSampling.pendingCompactions, i.cmp)
			reschedule := i.readState.db.mu.compact.rescheduleReadCompaction
			i.readState.db.mu.compact.rescheduleReadCompaction = false
			concurrentCompactions := i.readState.db.mu.compact.compactingCount
			i.readState.db.mu.Unlock()

			if reschedule && concurrentCompactions == 0 {
				// In a read heavy workload, flushes may not happen frequently enough to
				// schedule compactions.
				i.readState.db.compactionSchedulers.Add(1)
				go i.readState.db.maybeScheduleCompactionAsync()
			}
		}

		i.readState.unref()
		i.readState = nil
	}

	if i.closeHook != nil {
		i.closeHook()
	}

	// Close the closer for the current value if one was open.
	if i.valueCloser != nil {
		err = firstError(err, i.valueCloser.Close())
		i.valueCloser = nil
	}

	const maxKeyBufCacheSize = 4 << 10 // 4 KB

	if i.rangeKey != nil {
		// Avoid caching the key buf if it is overly large. The constant is
		// fairly arbitrary.
		if cap(i.rangeKey.buf) >= maxKeyBufCacheSize {
			i.rangeKey.buf = nil
		}
		*i.rangeKey = iteratorRangeKeyState{buf: i.rangeKey.buf}
		iterRangeKeyStateAllocPool.Put(i.rangeKey)
		i.rangeKey = nil
	}
	if alloc := i.alloc; alloc != nil {
		// Avoid caching the key buf if it is overly large. The constant is fairly
		// arbitrary.
		if cap(i.keyBuf) >= maxKeyBufCacheSize {
			alloc.keyBuf = nil
		} else {
			alloc.keyBuf = i.keyBuf
		}
		if cap(i.prefixOrFullSeekKey) >= maxKeyBufCacheSize {
			alloc.prefixOrFullSeekKey = nil
		} else {
			alloc.prefixOrFullSeekKey = i.prefixOrFullSeekKey
		}
		for j := range i.boundsBuf {
			if cap(i.boundsBuf[j]) >= maxKeyBufCacheSize {
				alloc.boundsBuf[j] = nil
			} else {
				alloc.boundsBuf[j] = i.boundsBuf[j]
			}
		}
		*i = Iterator{}
		iterAllocPool.Put(alloc)
	} else if alloc := i.getIterAlloc; alloc != nil {
		if cap(i.keyBuf) >= maxKeyBufCacheSize {
			alloc.keyBuf = nil
		} else {
			alloc.keyBuf = i.keyBuf
		}
		*i = Iterator{}
		getIterAllocPool.Put(alloc)
	}
	return err
}

// SetBounds sets the lower and upper bounds for the iterator. Once SetBounds
// returns, the caller is free to mutate the provided slices.
//
// The iterator will always be invalidated and must be repositioned with a call
// to SeekGE, SeekPrefixGE, SeekLT, First, or Last.
func (i *Iterator) SetBounds(lower, upper []byte) {
	// Ensure that the Iterator appears exhausted, regardless of whether we
	// actually have to invalidate the internal iterator. Optimizations that
	// avoid exhaustion are an internal implementation detail that shouldn't
	// leak through the interface. The caller should still call an absolute
	// positioning method to reposition the iterator.
	i.requiresReposition = true

	if ((i.opts.LowerBound == nil) == (lower == nil)) &&
		((i.opts.UpperBound == nil) == (upper == nil)) &&
		i.equal(i.opts.LowerBound, lower) &&
		i.equal(i.opts.UpperBound, upper) {
		// Unchanged, noop.
		return
	}

	// Copy the user-provided bounds into an Iterator-owned buffer, and set them
	// on i.opts.{Lower,Upper}Bound.
	i.saveBounds(lower, upper)

	i.iter.SetBounds(i.opts.LowerBound, i.opts.UpperBound)
	// If the iterator has an open point iterator that's not currently being
	// used, propagate the new bounds to it.
	if i.pointIter != nil && !i.opts.pointKeys() {
		i.pointIter.SetBounds(i.opts.LowerBound, i.opts.UpperBound)
	}

	// Even though this is not a positioning operation, the alteration of the
	// bounds means we cannot optimize Seeks by using Next.
	i.lastPositioningOp = unknownLastPositionOp
	i.hasPrefix = false
	i.iterKey = nil
	i.iterValue = nil
	// This switch statement isn't necessary for correctness since callers
	// should call a repositioning method. We could have arbitrarily set i.pos
	// to one of the values. But it results in more intuitive behavior in
	// tests, which do not always reposition.
	switch i.pos {
	case iterPosCurForward, iterPosNext, iterPosCurForwardPaused:
		i.pos = iterPosCurForward
	case iterPosCurReverse, iterPosPrev, iterPosCurReversePaused:
		i.pos = iterPosCurReverse
	}
	i.iterValidityState = IterExhausted
}

func (i *Iterator) saveBounds(lower, upper []byte) {
	// Copy the user-provided bounds into an Iterator-owned buffer. We can't
	// overwrite the current bounds, because some internal iterators compare old
	// and new bounds for optimizations.

	buf := i.boundsBuf[i.boundsBufIdx][:0]
	if lower != nil {
		buf = append(buf, lower...)
		i.opts.LowerBound = buf
	} else {
		i.opts.LowerBound = nil
	}
	if upper != nil {
		buf = append(buf, upper...)
		i.opts.UpperBound = buf[len(buf)-len(upper):]
	} else {
		i.opts.UpperBound = nil
	}
	i.boundsBuf[i.boundsBufIdx] = buf
	i.boundsBufIdx = 1 - i.boundsBufIdx
}

// SetOptions sets new iterator options for the iterator. Note that the lower
// and upper bounds applied here will supersede any bounds set by previous calls
// to SetBounds.
//
// Note that the slices provided in this SetOptions must not be changed by the
// caller until the iterator is closed, or a subsequent SetBounds or SetOptions
// has returned. This is because comparisons between the existing and new bounds
// are sometimes used to optimize seeking. See the extended commentary on
// SetBounds.
//
// The iterator will always be invalidated and must be repositioned with a call
// to SeekGE, SeekPrefixGE, SeekLT, First, or Last.
//
// If only lower and upper bounds need to be modified, prefer SetBounds.
func (i *Iterator) SetOptions(o *IterOptions) {
	// Ensure that the Iterator appears exhausted, regardless of whether we
	// actually have to invalidate the internal iterator. Optimizations that
	// avoid exhaustion are an internal implementation detail that shouldn't
	// leak through the interface. The caller should still call an absolute
	// positioning method to reposition the iterator.
	i.requiresReposition = true

	// Check if global state requires we close all internal iterators.
	//
	// If the Iterator is in an error state, invalidate the existing iterators
	// so that we reconstruct an iterator state from scratch.
	//
	// If OnlyReadGuaranteedDurable changed, the iterator stacks are incorrect,
	// improperly including or excluding memtables. Invalidate them so that
	// finishInitializingIter will reconstruct them.
	//
	// If either the original options or the new options specify a table filter,
	// we need to reconstruct the iterator stacks. If they both supply a table
	// filter, we can't be certain that it's the same filter since we have no
	// mechanism to compare the filter closures.
	closeBoth := i.err != nil ||
		o.OnlyReadGuaranteedDurable != i.opts.OnlyReadGuaranteedDurable ||
		o.TableFilter != nil || i.opts.TableFilter != nil

	// If either options specify block property filters for an iterator stack,
	// reconstruct it.
	if i.pointIter != nil && (closeBoth || len(o.PointKeyFilters) > 0 || len(i.opts.PointKeyFilters) > 0) {
		i.err = firstError(i.err, i.pointIter.Close())
		i.pointIter = nil
	}
	if i.rangeKey != nil && (closeBoth || len(o.RangeKeyFilters) > 0 || len(i.opts.RangeKeyFilters) > 0) {
		i.err = firstError(i.err, i.rangeKey.rangeKeyIter.Close())
		i.rangeKey = nil
	}

	boundsEqual := ((i.opts.LowerBound == nil) == (o.LowerBound == nil)) &&
		((i.opts.UpperBound == nil) == (o.UpperBound == nil)) &&
		i.equal(i.opts.LowerBound, o.LowerBound) &&
		i.equal(i.opts.UpperBound, o.UpperBound)

	if (i.pointIter != nil || !i.opts.pointKeys()) &&
		(i.rangeKey != nil || !i.opts.rangeKeys()) &&
		boundsEqual && o.KeyTypes == i.opts.KeyTypes &&
		i.equal(o.RangeKeyMasking.Suffix, i.opts.RangeKeyMasking.Suffix) &&
		o.UseL6Filters == i.opts.UseL6Filters {
		// Fast path. The options are identical. This preserves the
		// Seek-using-Next optimizations.
		return
	}

	// The options changed. Save the new ones to i.opts.
	if boundsEqual {
		// Copying the options into i.opts will overwrite LowerBound and
		// UpperBound fields with the user-provided slices. We need to hold on
		// to the Pebble-owned slices, so save them and re-set them after the
		// copy.
		lower, upper := i.opts.LowerBound, i.opts.UpperBound
		i.opts = *o
		i.opts.LowerBound, i.opts.UpperBound = lower, upper
	} else {
		i.opts = *o
		i.saveBounds(o.LowerBound, o.UpperBound)
		// Propagate the changed bounds to the existing point iterator.
		// NB: We propagate i.opts.{Lower,Upper}Bound, not o.{Lower,Upper}Bound
		// because i.opts now point to buffers owned by Pebble.
		if i.pointIter != nil {
			i.pointIter.SetBounds(i.opts.LowerBound, i.opts.UpperBound)
		}
	}

	// Even though this is not a positioning operation, the invalidation of the
	// iterator stack means we cannot optimize Seeks by using Next.
	i.lastPositioningOp = unknownLastPositionOp
	i.hasPrefix = false
	i.iterKey = nil
	i.iterValue = nil
	i.err = nil
	// This switch statement isn't necessary for correctness since callers
	// should call a repositioning method. We could have arbitrarily set i.pos
	// to one of the values. But it results in more intuitive behavior in
	// tests, which do not always reposition.
	switch i.pos {
	case iterPosCurForward, iterPosNext, iterPosCurForwardPaused:
		i.pos = iterPosCurForward
	case iterPosCurReverse, iterPosPrev, iterPosCurReversePaused:
		i.pos = iterPosCurReverse
	}
	i.iterValidityState = IterExhausted
	finishInitializingIter(i.alloc)
}

// Metrics returns per-iterator metrics.
func (i *Iterator) Metrics() IteratorMetrics {
	m := IteratorMetrics{
		ReadAmp: 1,
	}
	if mi, ok := i.iter.(*mergingIter); ok {
		m.ReadAmp = len(mi.levels)
	}
	return m
}

// ResetStats resets the stats to 0.
func (i *Iterator) ResetStats() {
	i.stats = IteratorStats{}
	i.iter.ResetStats()
}

// Stats returns the current stats.
func (i *Iterator) Stats() IteratorStats {
	stats := i.stats
	stats.InternalStats = i.iter.Stats()
	return stats
}

// Clone creates a new Iterator over the same underlying data, i.e., over the
// same {batch, memtables, sstables}). It starts with the same IterOptions but
// is not positioned.
//
// Callers can use Clone if they need multiple iterators that need to see
// exactly the same underlying state of the DB. This should not be used to
// extend the lifetime of the data backing the original Iterator since that
// will cause an increase in memory and disk usage (use NewSnapshot for that
// purpose).
func (i *Iterator) Clone() (*Iterator, error) {
	readState := i.readState
	if readState == nil {
		return nil, errors.Errorf("cannot Clone a closed Iterator")
	}
	// i is already holding a ref, so there is no race with unref here.
	readState.ref()
	// Bundle various structures under a single umbrella in order to allocate
	// them together.
	buf := iterAllocPool.Get().(*iterAlloc)
	dbi := &buf.dbi
	*dbi = Iterator{
		opts:                i.opts,
		alloc:               buf,
		cmp:                 i.cmp,
		equal:               i.equal,
		merge:               i.merge,
		split:               i.split,
		readState:           readState,
		keyBuf:              buf.keyBuf,
		prefixOrFullSeekKey: buf.prefixOrFullSeekKey,
		boundsBuf:           buf.boundsBuf,
		batch:               i.batch,
		newIters:            i.newIters,
		seqNum:              i.seqNum,
		db:                  i.db,
	}
	dbi.saveBounds(i.opts.LowerBound, i.opts.UpperBound)
	return finishInitializingIter(buf), nil
}

func (stats *IteratorStats) String() string {
	return redact.StringWithoutMarkers(stats)
}

// SafeFormat implements the redact.SafeFormatter interface.
func (stats *IteratorStats) SafeFormat(s redact.SafePrinter, verb rune) {
	for i := range stats.ForwardStepCount {
		switch IteratorStatsKind(i) {
		case InterfaceCall:
			s.SafeString("(interface (dir, seek, step): ")
		case InternalIterCall:
			s.SafeString(", (internal (dir, seek, step): ")
		}
		s.Printf("(fwd, %d, %d), (rev, %d, %d))",
			redact.Safe(stats.ForwardSeekCount[i]), redact.Safe(stats.ForwardStepCount[i]),
			redact.Safe(stats.ReverseSeekCount[i]), redact.Safe(stats.ReverseStepCount[i]))
	}
	if stats.InternalStats != (InternalIteratorStats{}) {
		s.SafeString(",\n(internal-stats: ")
		s.Printf("(block-bytes: (total %s, cached %s)), "+
			"(points: (count %s, key-bytes %s, value-bytes %s, tombstoned: %s))",
			humanize.IEC.Uint64(stats.InternalStats.BlockBytes),
			humanize.IEC.Uint64(stats.InternalStats.BlockBytesInCache),
			humanize.SI.Uint64(stats.InternalStats.PointCount),
			humanize.SI.Uint64(stats.InternalStats.KeyBytes),
			humanize.SI.Uint64(stats.InternalStats.ValueBytes),
			humanize.SI.Uint64(stats.InternalStats.PointsCoveredByRangeTombstones),
		)
	}
}
