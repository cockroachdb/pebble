// Copyright 2011 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package pebble

import (
	"bytes"
	"io"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/manifest"
	"github.com/cockroachdb/pebble/internal/randvar"
	"golang.org/x/exp/rand"
)

// iterPos describes the state of the internal iterator, in terms of whether it is
// at the position returned to the user (cur), one ahead of the position returned
// (next for forward iteration and prev for reverse iteration). The cur position
// is split into two states, for forward and reverse iteration, since we need to
// differentiate for switching directions.
type iterPos int8

const (
	iterPosCurForward iterPos = 0
	iterPosNext       iterPos = 1
	iterPosPrev       iterPos = -1
	iterPosCurReverse iterPos = -2
)

var errReversePrefixIteration = errors.New("pebble: unsupported reverse prefix iteration")

// IteratorMetrics holds per-iterator metrics.
type IteratorMetrics struct {
	// The read amplification experienced by this iterator. This is the sum of
	// the memtables, the L0 sublevels and the non-empty Ln levels. Higher read
	// amplification generally results in slower reads, though allowing higher
	// read amplification can also result in faster writes.
	ReadAmp int
}

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
	opts         IterOptions
	cmp          Compare
	equal        Equal
	merge        Merge
	split        Split
	iter         internalIterator
	readState    *readState
	err          error
	key          []byte
	keyBuf       []byte
	value        []byte
	valueBuf     []byte
	valueCloser  io.Closer
	valid        bool
	iterKey      *InternalKey
	iterValue    []byte
	pos          iterPos
	alloc        *iterAlloc
	prefix       []byte
	readSampling readSampling
}

// readSampling stores variables used to randomly select a read
// sample to trigger a read compaction
type readSampling struct {
	bytesUntilReadSampling uint64
	rand                   *rand.Rand
	randvar                *randvar.Uniform
	pendingCompactions     []readCompaction
}

func (i *Iterator) findNextEntry() bool {
	i.valid = false
	i.pos = iterPosCurForward

	// Close the closer for the current value if one was open.
	if i.valueCloser != nil {
		i.err = i.valueCloser.Close()
		i.valueCloser = nil
		if i.err != nil {
			return false
		}
	}

	for i.iterKey != nil {
		key := *i.iterKey

		if i.prefix != nil {
			if n := i.split(key.UserKey); !bytes.Equal(i.prefix, key.UserKey[:n]) {
				return false
			}
		}

		switch key.Kind() {
		case InternalKeyKindDelete, InternalKeyKindSingleDelete:
			i.nextUserKey()
			continue

		case InternalKeyKindSet:
			i.keyBuf = append(i.keyBuf[:0], key.UserKey...)
			i.key = i.keyBuf
			i.value = i.iterValue
			i.valid = true
			return true

		case InternalKeyKindMerge:
			var valueMerger ValueMerger
			valueMerger, i.err = i.merge(i.key, i.iterValue)
			if i.err == nil {
				i.mergeNext(key, valueMerger)
			}
			if i.err == nil {
				i.value, i.valueCloser, i.err = valueMerger.Finish(true /* includesBase */)
			}
			return i.err == nil

		default:
			i.err = base.CorruptionErrorf("pebble: invalid internal key kind: %d", errors.Safe(key.Kind()))
			return false
		}
	}

	return false
}

func (i *Iterator) nextUserKey() {
	if i.iterKey == nil {
		return
	}
	done := i.iterKey.SeqNum() == 0
	if !i.valid {
		i.keyBuf = append(i.keyBuf[:0], i.iterKey.UserKey...)
		i.key = i.keyBuf
	}
	for {
		i.iterKey, i.iterValue = i.iter.Next()
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
	// Approximate gap in bytes between samples of data read during iteration.
	const readBytesPeriod uint64 = 1048576
	if i.readSampling.rand == nil {
		i.readSampling.rand = rand.New(rand.NewSource(uint64(time.Now().UnixNano())))
	}
	if i.readSampling.randvar == nil {
		i.readSampling.randvar = randvar.NewUniform(0, 2*readBytesPeriod)
	}
	bytesRead := uint64(len(i.key) + len(i.value))
	for i.readSampling.bytesUntilReadSampling < bytesRead {
		i.readSampling.bytesUntilReadSampling += i.readSampling.randvar.Uint64(i.readSampling.rand)
		i.sampleRead()
	}
	i.readSampling.bytesUntilReadSampling -= bytesRead
}

func (i *Iterator) sampleRead() {
	if i.readState == nil {
		return
	}
	topOverlapping := manifest.LevelSlice{}
	topLevel, numOverlap := 0, 0
	for l := 0; l < numLevels; l++ {
		overlaps := i.readState.current.Overlaps(l, i.cmp, i.key, i.key)
		if !overlaps.Empty() {
			numOverlap++
			if numOverlap >= 2 {
				break
			}
			topOverlapping = overlaps
			topLevel = l
		}
	}
	if numOverlap >= 2 {
		topOverlapping.Each(func(file *manifest.FileMetadata) {
			allowedSeeks := atomic.AddInt64(&file.Atomic.AllowedSeeks, -1)
			if allowedSeeks == 0 {
				read := readCompaction{
					start: file.Smallest.UserKey,
					end:   file.Largest.UserKey,
					level: topLevel,
				}
				i.readSampling.pendingCompactions = append(i.readSampling.pendingCompactions, read)
			}
		})
	}
}

func (i *Iterator) findPrevEntry() bool {
	i.valid = false
	i.pos = iterPosCurReverse

	// Close the closer for the current value if one was open.
	if i.valueCloser != nil {
		i.err = i.valueCloser.Close()
		i.valueCloser = nil
		if i.err != nil {
			return false
		}
	}

	var valueMerger ValueMerger
	for i.iterKey != nil {
		key := *i.iterKey

		if i.valid {
			if !i.equal(key.UserKey, i.key) {
				// We've iterated to the previous user key.
				i.pos = iterPosPrev
				if valueMerger != nil {
					i.value, i.valueCloser, i.err = valueMerger.Finish(true /* includesBase */)
				}
				return i.err == nil
			}
		}

		switch key.Kind() {
		case InternalKeyKindDelete, InternalKeyKindSingleDelete:
			i.value = nil
			i.valid = false
			valueMerger = nil
			i.iterKey, i.iterValue = i.iter.Prev()
			continue

		case InternalKeyKindSet:
			i.keyBuf = append(i.keyBuf[:0], key.UserKey...)
			i.key = i.keyBuf
			// iterValue is owned by i.iter and could change after the Prev()
			// call, so use valueBuf instead. Note that valueBuf is only used
			// in this one instance; everywhere else (eg. in findNextEntry),
			// we just point i.value to the unsafe i.iter-owned value buffer.
			i.valueBuf = append(i.valueBuf[:0], i.iterValue...)
			i.value = i.valueBuf
			i.valid = true
			i.iterKey, i.iterValue = i.iter.Prev()
			valueMerger = nil
			continue

		case InternalKeyKindMerge:
			if !i.valid {
				i.keyBuf = append(i.keyBuf[:0], key.UserKey...)
				i.key = i.keyBuf
				valueMerger, i.err = i.merge(i.key, i.iterValue)
				if i.err != nil {
					return false
				}
				i.valid = true
			} else if valueMerger == nil {
				valueMerger, i.err = i.merge(i.key, i.value)
				if i.err == nil {
					i.err = valueMerger.MergeNewer(i.iterValue)
				}
				if i.err != nil {
					return false
				}
			} else {
				i.err = valueMerger.MergeNewer(i.iterValue)
				if i.err != nil {
					return false
				}
			}
			i.iterKey, i.iterValue = i.iter.Prev()
			continue

		default:
			i.err = base.CorruptionErrorf("pebble: invalid internal key kind: %d", errors.Safe(key.Kind()))
			return false
		}
	}

	// i.iterKey == nil, so broke out of the preceding loop.
	if i.valid {
		i.pos = iterPosPrev
		if valueMerger != nil {
			i.value, i.valueCloser, i.err = valueMerger.Finish(true /* includesBase */)
		}
		return i.err == nil
	}

	return false
}

func (i *Iterator) prevUserKey() {
	if i.iterKey == nil {
		return
	}
	if !i.valid {
		// If we're going to compare against the prev key, we need to save the
		// current key.
		i.keyBuf = append(i.keyBuf[:0], i.iterKey.UserKey...)
		i.key = i.keyBuf
	}
	for {
		i.iterKey, i.iterValue = i.iter.Prev()
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
	i.valid = true

	// Loop looking for older values for this key and merging them.
	for {
		i.iterKey, i.iterValue = i.iter.Next()
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

		case InternalKeyKindSet:
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
	i.err = nil // clear cached iteration error
	i.prefix = nil
	if lowerBound := i.opts.GetLowerBound(); lowerBound != nil && i.cmp(key, lowerBound) < 0 {
		key = lowerBound
	} else if upperBound := i.opts.GetUpperBound(); upperBound != nil && i.cmp(key, upperBound) > 0 {
		key = upperBound
	}

	i.iterKey, i.iterValue = i.iter.SeekGE(key)
	valid := i.findNextEntry()
	i.maybeSampleRead()
	return valid
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
// See Example_prefixiteration for a working example.
func (i *Iterator) SeekPrefixGE(key []byte) bool {
	i.err = nil // clear cached iteration error

	if i.split == nil {
		panic("pebble: split must be provided for SeekPrefixGE")
	}

	// Make a copy of the prefix so that modifications to the key after
	// SeekPrefixGE returns does not affect the stored prefix.
	prefixLen := i.split(key)
	i.prefix = make([]byte, prefixLen)
	copy(i.prefix, key[:prefixLen])

	if lowerBound := i.opts.GetLowerBound(); lowerBound != nil && i.cmp(key, lowerBound) < 0 {
		if n := i.split(lowerBound); !bytes.Equal(i.prefix, lowerBound[:n]) {
			i.err = errors.New("pebble: SeekPrefixGE supplied with key outside of lower bound")
			return false
		}
		key = lowerBound
	} else if upperBound := i.opts.GetUpperBound(); upperBound != nil && i.cmp(key, upperBound) > 0 {
		if n := i.split(upperBound); !bytes.Equal(i.prefix, upperBound[:n]) {
			i.err = errors.New("pebble: SeekPrefixGE supplied with key outside of upper bound")
			return false
		}
		key = upperBound
	}

	i.iterKey, i.iterValue = i.iter.SeekPrefixGE(i.prefix, key)
	valid := i.findNextEntry()
	i.maybeSampleRead()
	return valid
}

// SeekLT moves the iterator to the last key/value pair whose key is less than
// the given key. Returns true if the iterator is pointing at a valid entry and
// false otherwise.
func (i *Iterator) SeekLT(key []byte) bool {
	i.err = nil // clear cached iteration error
	i.prefix = nil
	if upperBound := i.opts.GetUpperBound(); upperBound != nil && i.cmp(key, upperBound) > 0 {
		key = upperBound
	} else if lowerBound := i.opts.GetLowerBound(); lowerBound != nil && i.cmp(key, lowerBound) < 0 {
		key = lowerBound
	}

	i.iterKey, i.iterValue = i.iter.SeekLT(key)
	valid := i.findPrevEntry()
	i.maybeSampleRead()
	return valid
}

// First moves the iterator the the first key/value pair. Returns true if the
// iterator is pointing at a valid entry and false otherwise.
func (i *Iterator) First() bool {
	i.err = nil // clear cached iteration error
	i.prefix = nil
	if lowerBound := i.opts.GetLowerBound(); lowerBound != nil {
		i.iterKey, i.iterValue = i.iter.SeekGE(lowerBound)
	} else {
		i.iterKey, i.iterValue = i.iter.First()
	}
	valid := i.findNextEntry()
	i.maybeSampleRead()
	return valid
}

// Last moves the iterator the the last key/value pair. Returns true if the
// iterator is pointing at a valid entry and false otherwise.
func (i *Iterator) Last() bool {
	i.err = nil // clear cached iteration error
	i.prefix = nil
	if upperBound := i.opts.GetUpperBound(); upperBound != nil {
		i.iterKey, i.iterValue = i.iter.SeekLT(upperBound)
	} else {
		i.iterKey, i.iterValue = i.iter.Last()
	}
	valid := i.findPrevEntry()
	i.maybeSampleRead()
	return valid
}

// Next moves the iterator to the next key/value pair. Returns true if the
// iterator is pointing at a valid entry and false otherwise.
func (i *Iterator) Next() bool {
	if i.err != nil {
		return false
	}
	switch i.pos {
	case iterPosCurForward:
		i.nextUserKey()
	case iterPosCurReverse:
		// Switching directions.
		// Unless the iterator was exhausted, reverse iteration needs to
		// position the iterator at iterPosPrev.
		if i.iterKey != nil {
			i.err = errors.New("switching from reverse to forward but iter is not at prev")
			i.valid = false
			return false
		}
		// We're positioned before the first key. Need to reposition to point to
		// the first key.
		if lowerBound := i.opts.GetLowerBound(); lowerBound != nil {
			i.iterKey, i.iterValue = i.iter.SeekGE(lowerBound)
		} else {
			i.iterKey, i.iterValue = i.iter.First()
		}
	case iterPosPrev:
		// The underlying iterator is pointed to the previous key (this can only
		// happen when switching iteration directions). We set i.valid to false
		// here to force the calls to nextUserKey to save the current key i.iter is
		// pointing at in order to determine when the next user-key is reached.
		i.valid = false
		if i.iterKey == nil {
			// We're positioned before the first key. Need to reposition to point to
			// the first key.
			if lowerBound := i.opts.GetLowerBound(); lowerBound != nil {
				i.iterKey, i.iterValue = i.iter.SeekGE(lowerBound)
			} else {
				i.iterKey, i.iterValue = i.iter.First()
			}
		} else {
			i.nextUserKey()
		}
		i.nextUserKey()
	case iterPosNext:
	}
	valid := i.findNextEntry()
	i.maybeSampleRead()
	return valid
}

// Prev moves the iterator to the previous key/value pair. Returns true if the
// iterator is pointing at a valid entry and false otherwise.
func (i *Iterator) Prev() bool {
	if i.err != nil {
		return false
	}
	if i.prefix != nil {
		i.err = errReversePrefixIteration
		return false
	}
	switch i.pos {
	case iterPosCurForward:
		// Switching directions, and will handle this below.
	case iterPosCurReverse:
		i.prevUserKey()
	case iterPosNext:
		// The underlying iterator is pointed to the next key (this can only happen
		// when switching iteration directions). We will handle this below.
	case iterPosPrev:
	}
	if i.pos == iterPosCurForward || i.pos == iterPosNext {
		stepAgain := i.pos == iterPosNext
		// Switching direction.
		// We set i.valid to false here to force the calls to prevUserKey to
		// save the current key i.iter is pointing at in order to determine
		// when the prev user-key is reached.
		i.valid = false
		if i.iterKey == nil {
			// We're positioned after the last key. Need to reposition to point to
			// the last key.
			if upperBound := i.opts.GetUpperBound(); upperBound != nil {
				i.iterKey, i.iterValue = i.iter.SeekLT(upperBound)
			} else {
				i.iterKey, i.iterValue = i.iter.Last()
			}
		} else {
			i.prevUserKey()
		}
		if stepAgain {
			i.prevUserKey()
		}
	}
	valid := i.findPrevEntry()
	i.maybeSampleRead()
	return valid
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
func (i *Iterator) Value() []byte {
	return i.value
}

// Valid returns true if the iterator is positioned at a valid key/value pair
// and false otherwise.
func (i *Iterator) Valid() bool {
	return i.valid
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
	}
	err := i.err

	if i.readState != nil {
		// Copy pending read compactions using db.mu.Lock()
		i.readState.db.mu.Lock()
		i.readState.db.mu.compact.readCompactions = append(i.readState.db.mu.compact.readCompactions, i.readSampling.pendingCompactions...)
		i.readState.db.mu.Unlock()

		i.readState.unref()
		i.readState = nil
	}

	// Close the closer for the current value if one was open.
	if i.valueCloser != nil {
		err = firstError(err, i.valueCloser.Close())
		i.valueCloser = nil
	}

	if alloc := i.alloc; alloc != nil {
		// Avoid caching the key buf if it is overly large. The constant is fairly
		// arbitrary.
		const maxKeyBufCacheSize = 4 << 10 // 4 KB
		if cap(i.keyBuf) >= maxKeyBufCacheSize {
			alloc.keyBuf = nil
		} else {
			alloc.keyBuf = i.keyBuf
		}
		*i = Iterator{}
		iterAllocPool.Put(alloc)
	}
	return err
}

// SetBounds sets the lower and upper bounds for the iterator. Note that the
// iterator will always be invalidated and must be repositioned with a call to
// SeekGE, SeekPrefixGE, SeekLT, First, or Last.
func (i *Iterator) SetBounds(lower, upper []byte) {
	i.prefix = nil
	i.iterKey = nil
	i.iterValue = nil
	// This switch statement isn't necessary for correctness since callers
	// should call a repositioning method. We could have arbitrarily set i.pos
	// to one of the values. But it results in more intuitive behavior in
	// tests, which do not always reposition.
	switch i.pos {
	case iterPosCurForward, iterPosNext:
		i.pos = iterPosCurForward
	case iterPosCurReverse, iterPosPrev:
		i.pos = iterPosCurReverse
	}
	i.valid = false

	i.opts.LowerBound = lower
	i.opts.UpperBound = upper
	i.iter.SetBounds(lower, upper)
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
