// Copyright 2023 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package pebble

import (
	"context"
	"fmt"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/keyspan"
	"github.com/cockroachdb/pebble/internal/manifest"
	"github.com/cockroachdb/pebble/objstorage"
)

const (
	// In skip-shared iteration mode, keys in levels sharedLevelsStart and greater
	// (i.e. lower in the LSM) are skipped.
	sharedLevelsStart = 5
)

// ErrInvalidSkipSharedIteration is returned by ScanInternal if it was called
// with a shared file visitor function, and a file in a shareable level (i.e.
// level >= sharedLevelsStart) was found to not be in shared storage according
// to objstorage.Provider.
var ErrInvalidSkipSharedIteration = errors.New("pebble: cannot use skip-shared iteration due to non-shared files in lower levels")

// SharedSSTMeta represents an sstable on shared storage that can be ingested
// by another pebble instance. This struct must contain all fields that are
// required for a Pebble instance to ingest a foreign sstable on shared storage,
// including constructing any relevant objstorage.Provider / sharedobjcat.Catalog
// data structures, as well as creating virtual FileMetadatas.
//
// Note that the Pebble instance creating and returning a SharedSSTMeta might
// not be the one that created the underlying sstable on shared storage to begin
// with; it's possible for a Pebble instance to reshare an sstable that was
// shared to it.
type SharedSSTMeta struct {
	// Backing is the shared object underlying this SST. Can be attached to an
	// objstorage.Provider.
	Backing objstorage.SharedObjectBackingHandle

	// Smallest and Largest internal keys for the overall bounds. The kind and
	// SeqNum of these will reflect what is physically present on the source Pebble
	// instance's view of the sstable; it's up to the ingesting instance to set the
	// sequence number in the trailer to match the read-time sequence numbers
	// reserved for the level this SST is being ingested into. The Kind is expected
	// to remain unchanged by the ingesting instance.
	//
	// Note that these bounds could be narrower than the bounds of the underlying
	// sstable; ScanInternal is expected to truncate sstable bounds to the user key
	// bounds passed into that method.
	Smallest, Largest InternalKey

	// SmallestRangeKey and LargestRangeKey are internal keys that denote the
	// range key bounds of this sstable. Must lie within [Smallest, Largest].
	SmallestRangeKey, LargestRangeKey InternalKey

	// SmallestPointKey and LargestPointKey are internal keys that denote the
	// point key bounds of this sstable. Must lie within [Smallest, Largest].
	SmallestPointKey, LargestPointKey InternalKey

	// Level denotes the level at which this file was present at read time.
	// For files visited by ScanInternal, this value will only be 5 or 6.
	Level uint8

	// Size contains an estimate of the size of this sstable.
	Size uint64

	// fileNum at time of creation in the creator instance. Only used for
	// debugging/tests.
	fileNum base.FileNum
}

func (s *SharedSSTMeta) cloneFromFileMeta(f *fileMetadata) {
	*s = SharedSSTMeta{
		Smallest:         f.Smallest.Clone(),
		Largest:          f.Largest.Clone(),
		SmallestRangeKey: f.SmallestRangeKey.Clone(),
		LargestRangeKey:  f.LargestRangeKey.Clone(),
		SmallestPointKey: f.SmallestPointKey.Clone(),
		LargestPointKey:  f.LargestPointKey.Clone(),
		Size:             f.Size,
		fileNum:          f.FileNum,
	}
}

type pcIterPos int

const (
	pcIterPosCur pcIterPos = iota
	pcIterPosNext
)

// pointCollapsingIterator is an internalIterator that collapses point keys and
// returns at most one point internal key for each user key. Merges are merged,
// sets are emitted as-is, and SingleDeletes are collapsed with the next point
// key, however we don't guarantee that we generate and return a SetWithDel if
// a set shadows a del. Point keys deleted by rangedels are also considered shadowed and not
// exposed.
//
// TODO(bilal): Implement the unimplemented internalIterator methods below.
// Currently this iterator only supports the forward iteration case necessary
// for scanInternal, however foreign sstable iterators will also need to use this
// or a simplified version of this.
type pointCollapsingIterator struct {
	iter     keyspan.InterleavingIter
	pos      pcIterPos
	comparer *base.Comparer
	merge    base.Merge
	err      error
	seqNum   uint64
	// The current position of `iter`. Could be backed by savedKey (i.e. iterKey ==
	// &savedKey) or could be owned by `iter`. findNextEntry and similar methods
	// are expected to save the current value of this to savedKey if they're
	// iterating away from the current key but still need to retain it. See
	// comments in findNextEntry on how this field is used.
	//
	// At the end of a positioning call:
	//  - if pos == pcIterPosNext, iterKey is pointing to the next user key owned
	//    by `iter` while savedKey is holding a copy to our current key.
	//  - If pos == pcIterPosCur, iterKey is pointing to either a savedKey-backed
	//    copy of the current key, or an `iter`-owned current key in which case
	//    the value of savedKey is undefined.
	iterKey  *InternalKey
	savedKey InternalKey
	// Value at the current iterator position, at iterKey.
	value base.LazyValue
	// Used for Merge keys only.
	valueMerger ValueMerger
	valueBuf    []byte
}

// SeekPrefixGE implements the InternalIterator interface.
func (p *pointCollapsingIterator) SeekPrefixGE(
	prefix, key []byte, flags base.SeekGEFlags,
) (*base.InternalKey, base.LazyValue) {
	panic("unimplemented")
}

// SeekGE implements the InternalIterator interface.
func (p *pointCollapsingIterator) SeekGE(
	key []byte, flags base.SeekGEFlags,
) (*base.InternalKey, base.LazyValue) {
	p.resetKey()
	p.iterKey, p.value = p.iter.SeekGE(key, flags)
	p.pos = pcIterPosCur
	if p.iterKey == nil {
		return nil, base.LazyValue{}
	}
	return p.findNextEntry()
}

// SeekLT implements the InternalIterator interface.
func (p *pointCollapsingIterator) SeekLT(
	key []byte, flags base.SeekLTFlags,
) (*base.InternalKey, base.LazyValue) {
	panic("unimplemented")
}

func (p *pointCollapsingIterator) resetKey() {
	p.savedKey.UserKey = p.savedKey.UserKey[:0]
	p.savedKey.Trailer = 0
	p.valueMerger = nil
	p.valueBuf = p.valueBuf[:0]
	p.iterKey = nil
	p.pos = pcIterPosCur
}

// findNextEntry is called to return the next key. p.iter must be positioned at
// the start of the first user key we are interested in.
func (p *pointCollapsingIterator) findNextEntry() (*base.InternalKey, base.LazyValue) {
	finishAndReturnMerge := func() (*base.InternalKey, base.LazyValue) {
		value, closer, err := p.valueMerger.Finish(true /* includesBase */)
		if err != nil {
			p.err = err
			return nil, base.LazyValue{}
		}
		p.valueBuf = append(p.valueBuf[:0], value...)
		if closer != nil {
			_ = closer.Close()
		}
		p.valueMerger = nil
		newValue := base.MakeInPlaceValue(value)
		return &p.savedKey, newValue
	}

	// saveKey sets p.iterKey (if not-nil) to &p.savedKey. We can use this equality
	// as a proxy to determine if we're at the first internal key for a user key.
	p.saveKey()
	for p.iterKey != nil {
		// NB: p.savedKey is either the current key (iff p.iterKey == &p.savedKey),
		// or the previous key.
		if p.iterKey != &p.savedKey && !p.comparer.Equal(p.iterKey.UserKey, p.savedKey.UserKey) {
			if p.valueMerger != nil {
				if p.savedKey.Kind() != InternalKeyKindMerge {
					panic(fmt.Sprintf("expected key %s to have MERGE kind", p.iterKey))
				}
				p.pos = pcIterPosNext
				return finishAndReturnMerge()
			}
			p.saveKey()
			continue
		}
		if s := p.iter.Span(); s != nil && s.CoversAt(p.seqNum, p.iterKey.SeqNum()) {
			// All future keys for this user key must be deleted.
			if p.valueMerger != nil {
				return finishAndReturnMerge()
			} else if p.savedKey.Kind() == InternalKeyKindSingleDelete {
				panic("cannot process singledel key in point collapsing iterator")
			}
			// Fast forward to the next user key.
			p.saveKey()
			p.iterKey, p.value = p.iter.Next()
			for p.iterKey != nil && p.savedKey.SeqNum() >= p.iterKey.SeqNum() && p.comparer.Equal(p.iterKey.UserKey, p.savedKey.UserKey) {
				p.iterKey, p.value = p.iter.Next()
			}
			continue
		}
		switch p.savedKey.Kind() {
		case InternalKeyKindSet, InternalKeyKindDelete, InternalKeyKindSetWithDelete:
			p.saveKey()
			// Note that we return SETs directly, even if they would otherwise get
			// compacted into a Del to turn into a SetWithDelete. This is a fast
			// path optimization that can break SINGLEDEL determinism. To lead to
			// consistent SINGLEDEL behaviour, this iterator should *not* be used for
			// a keyspace where SINGLEDELs could be in use. If this iterator observes
			// a SINGLEDEL as the first internal key for a user key, it will panic.
			//
			// As p.value is a lazy value owned by the child iterator, we can thread
			// it through without loading it into p.valueBuf.
			//
			// TODO(bilal): We can even avoid saving the key in this fast path if
			// we are in a block where setHasSamePrefix = false in a v3 sstable,
			// guaranteeing that there's only one internal key for each user key.
			// Thread this logic through the sstable iterators and/or consider
			// collapsing (ha) this logic into the sstable iterators that are aware
			// of blocks and can determine user key changes without doing key saves
			// or comparisons.
			p.pos = pcIterPosCur
			return p.iterKey, p.value
		case InternalKeyKindSingleDelete:
			// Panic, as this iterator is not expected to observe single deletes.
			panic("cannot process singledel key in point collapsing iterator")
		case InternalKeyKindMerge:
			if p.valueMerger == nil {
				// Set up merger. This is the first Merge key encountered.
				value, callerOwned, err := p.value.Value(p.valueBuf[:0])
				if err != nil {
					p.err = err
					return nil, base.LazyValue{}
				}
				if !callerOwned {
					p.valueBuf = append(p.valueBuf[:0], value...)
				} else {
					p.valueBuf = value
				}
				p.valueMerger, err = p.merge(p.iterKey.UserKey, p.valueBuf)
				if err != nil {
					p.err = err
					return nil, base.LazyValue{}
				}
				p.saveKey()
				p.iterKey, p.value = p.iter.Next()
				continue
			}
			switch p.iterKey.Kind() {
			case InternalKeyKindSet, InternalKeyKindMerge, InternalKeyKindSetWithDelete:
				// Merge into key.
				value, callerOwned, err := p.value.Value(p.valueBuf[:0])
				if err != nil {
					p.err = err
					return nil, base.LazyValue{}
				}
				if !callerOwned {
					p.valueBuf = append(p.valueBuf[:0], value...)
				} else {
					p.valueBuf = value
				}
				if err := p.valueMerger.MergeOlder(value); err != nil {
					p.err = err
					return nil, base.LazyValue{}
				}
			}
			if p.iterKey.Kind() != InternalKeyKindMerge {
				p.savedKey.SetKind(p.iterKey.Kind())
				p.pos = pcIterPosCur
				return finishAndReturnMerge()
			}
			p.iterKey, p.value = p.iter.Next()
		case InternalKeyKindRangeDelete:
			// These are interleaved by the interleaving iterator ahead of all points.
			// We should pass them as-is, but also account for any points ahead of
			// them.
			p.pos = pcIterPosCur
			return p.iterKey, p.value
		default:
			panic(fmt.Sprintf("unexpected kind: %d", p.iterKey.Kind()))
		}
	}
	if p.valueMerger != nil {
		p.pos = pcIterPosNext
		return finishAndReturnMerge()
	}
	p.resetKey()
	return nil, base.LazyValue{}
}

// First implements the InternalIterator interface.
func (p *pointCollapsingIterator) First() (*base.InternalKey, base.LazyValue) {
	p.resetKey()
	p.iterKey, p.value = p.iter.First()
	p.pos = pcIterPosCur
	if p.iterKey == nil {
		return nil, base.LazyValue{}
	}
	return p.findNextEntry()
}

// Last implements the InternalIterator interface.
func (p *pointCollapsingIterator) Last() (*base.InternalKey, base.LazyValue) {
	panic("unimplemented")
}

func (p *pointCollapsingIterator) saveKey() {
	if p.iterKey == nil {
		p.savedKey = InternalKey{UserKey: p.savedKey.UserKey[:0]}
		return
	} else if p.iterKey == &p.savedKey {
		// Do nothing.
		return
	}
	p.savedKey.CopyFrom(*p.iterKey)
	p.iterKey = &p.savedKey
}

// Next implements the InternalIterator interface.
func (p *pointCollapsingIterator) Next() (*base.InternalKey, base.LazyValue) {
	switch p.pos {
	case pcIterPosCur:
		p.saveKey()
		if p.iterKey != nil && p.iterKey.Kind() == InternalKeyKindRangeDelete {
			// Step over the interleaved range delete and process the very next
			// internal key, even if it's at the same user key. This is because a
			// point for that user key has not been returned yet.
			p.iterKey, p.value = p.iter.Next()
			break
		}
		// Fast forward to the next user key. p.iterKey stayed at p.savedKey for
		// this loop.
		key, val := p.iter.Next()
		// p.iterKey.SeqNum() >= key.SeqNum() is an optimization that allows us to
		// use p.iterKey.SeqNum() < key.SeqNum() as a sign that the user key has
		// changed, without needing to do the full key comparison.
		for p.iterKey != nil && key != nil && p.iterKey.SeqNum() >= key.SeqNum() &&
			p.comparer.Equal(p.iterKey.UserKey, key.UserKey) {
			key, val = p.iter.Next()
		}
		if key == nil {
			// There are no keys to return.
			p.resetKey()
			return nil, base.LazyValue{}
		}
		p.iterKey, p.value = key, val
	case pcIterPosNext:
		p.pos = pcIterPosCur
	}
	if p.iterKey == nil {
		p.resetKey()
		return nil, base.LazyValue{}
	}
	return p.findNextEntry()
}

// NextPrefix implements the InternalIterator interface.
func (p *pointCollapsingIterator) NextPrefix(succKey []byte) (*base.InternalKey, base.LazyValue) {
	panic("unimplemented")
}

// Prev implements the InternalIterator interface.
func (p *pointCollapsingIterator) Prev() (*base.InternalKey, base.LazyValue) {
	panic("unimplemented")
}

// Error implements the InternalIterator interface.
func (p *pointCollapsingIterator) Error() error {
	if p.err != nil {
		return p.err
	}
	return p.iter.Error()
}

// Close implements the InternalIterator interface.
func (p *pointCollapsingIterator) Close() error {
	return p.iter.Close()
}

// SetBounds implements the InternalIterator interface.
func (p *pointCollapsingIterator) SetBounds(lower, upper []byte) {
	p.resetKey()
	p.iter.SetBounds(lower, upper)
}

// String implements the InternalIterator interface.
func (p *pointCollapsingIterator) String() string {
	return p.iter.String()
}

var _ internalIterator = &pointCollapsingIterator{}

// scanInternalIterator is an iterator that returns all internal keys, including
// tombstones. For instance, an InternalKeyKindDelete would be returned as an
// InternalKeyKindDelete instead of the iterator skipping over to the next key.
// Internal keys within a user key are collapsed, eg. if there are two SETs, the
// one with the higher sequence is returned. Useful if an external user of Pebble
// needs to observe and rebuild Pebble's history of internal keys, such as in
// node-to-node replication. For use with {db,snapshot}.ScanInternal().
//
// scanInternalIterator is expected to ignore point keys deleted by range
// deletions, and range keys shadowed by a range key unset or delete, however it
// *must* return the range delete as well as the range key unset/delete that did
// the shadowing.
type scanInternalIterator struct {
	opts            scanInternalOptions
	comparer        *base.Comparer
	merge           Merge
	iter            internalIterator
	readState       *readState
	rangeKey        *iteratorRangeKeyState
	pointKeyIter    pointCollapsingIterator
	iterKey         *InternalKey
	iterValue       LazyValue
	alloc           *iterAlloc
	newIters        tableNewIters
	newIterRangeKey keyspan.TableNewSpanIter
	seqNum          uint64

	// boundsBuf holds two buffers used to store the lower and upper bounds.
	// Whenever the InternalIterator's bounds change, the new bounds are copied
	// into boundsBuf[boundsBufIdx]. The two bounds share a slice to reduce
	// allocations. opts.LowerBound and opts.UpperBound point into this slice.
	boundsBuf    [2][]byte
	boundsBufIdx int
}

// truncateSharedFile truncates a shared file's [Smallest, Largest] fields to
// [lower, upper), potentially opening iterators on the file to find keys within
// the requested bounds. A SharedSSTMeta is produced that is suitable for
// external consumption by other Pebble instances. If shouldSkip is true, this
// file does not contain any keys in [lower, upper) and can be skipped.
//
// TODO(bilal): If opening iterators and doing reads in this method is too
// inefficient, consider producing non-tight file bounds instead.
func (d *DB) truncateSharedFile(
	ctx context.Context,
	lower, upper []byte,
	level int,
	file *fileMetadata,
	objMeta objstorage.ObjectMetadata,
) (sst *SharedSSTMeta, shouldSkip bool, err error) {
	cmp := d.cmp
	sst = &SharedSSTMeta{}
	sst.cloneFromFileMeta(file)
	sst.Level = uint8(level)
	sst.Backing, err = d.objProvider.SharedObjectBacking(&objMeta)
	if err != nil {
		return nil, false, err
	}
	needsLowerTruncate := cmp(lower, file.Smallest.UserKey) > 0
	needsUpperTruncate := cmp(upper, file.Largest.UserKey) < 0 || (cmp(upper, file.Largest.UserKey) == 0 && !file.Largest.IsExclusiveSentinel())
	// Fast path: file is entirely within [lower, upper).
	if !needsLowerTruncate && !needsUpperTruncate {
		return sst, false, nil
	}

	// We will need to truncate file bounds in at least one direction. Open all
	// relevant iterators.
	//
	// TODO(bilal): Once virtual sstables go in, verify that the constraining of
	// bounds to virtual sstable bounds happens below this method, so we aren't
	// unintentionally exposing keys we shouldn't be exposing.
	iter, rangeDelIter, err := d.newIters(ctx, file, &IterOptions{
		LowerBound: lower,
		UpperBound: upper,
		level:      manifest.Level(level),
	}, internalIterOpts{})
	if err != nil {
		return nil, false, err
	}
	defer iter.Close()
	if rangeDelIter != nil {
		rangeDelIter = keyspan.Truncate(cmp, rangeDelIter, lower, upper, nil, nil)
		defer rangeDelIter.Close()
	}
	rangeKeyIter, err := d.tableNewRangeKeyIter(file, nil /* spanIterOptions */)
	if err != nil {
		return nil, false, err
	}
	if rangeKeyIter != nil {
		rangeKeyIter = keyspan.Truncate(cmp, rangeKeyIter, lower, upper, nil, nil)
		defer rangeKeyIter.Close()
	}
	// Check if we need to truncate on the left side. This means finding a new
	// LargestPointKey and LargestRangeKey that is >= lower.
	if needsLowerTruncate {
		sst.SmallestPointKey.UserKey = sst.SmallestPointKey.UserKey[:0]
		sst.SmallestPointKey.Trailer = 0
		key, _ := iter.SeekGE(lower, base.SeekGEFlagsNone)
		if key != nil {
			sst.SmallestPointKey.CopyFrom(*key)
		}
		if rangeDelIter != nil {
			span := rangeDelIter.SeekGE(lower)
			if span != nil && (len(sst.SmallestPointKey.UserKey) == 0 || base.InternalCompare(cmp, span.SmallestKey(), sst.SmallestPointKey) < 0) {
				sst.SmallestPointKey.CopyFrom(span.SmallestKey())
			}
		}
		sst.SmallestRangeKey.UserKey = sst.SmallestRangeKey.UserKey[:0]
		sst.SmallestRangeKey.Trailer = 0
		if rangeKeyIter != nil {
			span := rangeKeyIter.SeekGE(lower)
			if span != nil {
				sst.SmallestRangeKey.CopyFrom(span.SmallestKey())
			}
		}
	}
	// Check if we need to truncate on the right side. This means finding a new
	// LargestPointKey and LargestRangeKey that is < upper.
	if needsUpperTruncate {
		sst.LargestPointKey.UserKey = sst.LargestPointKey.UserKey[:0]
		sst.LargestPointKey.Trailer = 0
		key, _ := iter.SeekLT(upper, base.SeekLTFlagsNone)
		if key != nil {
			sst.LargestPointKey.CopyFrom(*key)
		}
		if rangeDelIter != nil {
			span := rangeDelIter.SeekLT(upper)
			if span != nil && (len(sst.LargestPointKey.UserKey) == 0 || base.InternalCompare(cmp, span.LargestKey(), sst.LargestPointKey) > 0) {
				sst.LargestPointKey.CopyFrom(span.LargestKey())
			}
		}
		sst.LargestRangeKey.UserKey = sst.LargestRangeKey.UserKey[:0]
		sst.LargestRangeKey.Trailer = 0
		if rangeKeyIter != nil {
			span := rangeKeyIter.SeekLT(upper)
			if span != nil {
				sst.LargestRangeKey.CopyFrom(span.LargestKey())
			}
		}
	}
	// Set overall bounds based on {Smallest,Largest}{Point,Range}Key.
	switch {
	case len(sst.SmallestRangeKey.UserKey) == 0:
		sst.Smallest = sst.SmallestPointKey
	case len(sst.SmallestPointKey.UserKey) == 0:
		sst.Smallest = sst.SmallestRangeKey
	default:
		sst.Smallest = sst.SmallestPointKey
		if base.InternalCompare(cmp, sst.SmallestRangeKey, sst.SmallestPointKey) < 0 {
			sst.Smallest = sst.SmallestRangeKey
		}
	}
	switch {
	case len(sst.LargestRangeKey.UserKey) == 0:
		sst.Largest = sst.LargestPointKey
	case len(sst.LargestPointKey.UserKey) == 0:
		sst.Largest = sst.LargestRangeKey
	default:
		sst.Largest = sst.LargestPointKey
		if base.InternalCompare(cmp, sst.LargestRangeKey, sst.LargestPointKey) > 0 {
			sst.Largest = sst.LargestRangeKey
		}
	}
	// On rare occasion, a file might overlap with [lower, upper) but not actually
	// have any keys within those bounds. Skip such files.
	if len(sst.Smallest.UserKey) == 0 {
		return nil, true, nil
	}
	return sst, false, nil
}

func scanInternalImpl(
	ctx context.Context,
	lower, upper []byte,
	iter *scanInternalIterator,
	visitPointKey func(key *InternalKey, value LazyValue) error,
	visitRangeDel func(start, end []byte, seqNum uint64) error,
	visitRangeKey func(start, end []byte, keys []keyspan.Key) error,
	visitSharedFile func(sst *SharedSSTMeta) error,
) error {
	if visitSharedFile != nil && (lower == nil || upper == nil) {
		panic("lower and upper bounds must be specified in skip-shared iteration mode")
	}
	// Before starting iteration, check if any files in levels sharedLevelsStart
	// and below are *not* shared. Error out if that is the case, as skip-shared
	// iteration will not produce a consistent point-in-time view of this range
	// of keys. For files that are shared, call visitSharedFile with a truncated
	// version of that file.
	cmp := iter.comparer.Compare
	db := iter.readState.db
	provider := db.objProvider
	if visitSharedFile != nil {
		if provider == nil {
			panic("expected non-nil Provider in skip-shared iteration mode")
		}
		for level := sharedLevelsStart; level < numLevels; level++ {
			files := iter.readState.current.Levels[level].Iter()
			for f := files.SeekGE(cmp, lower); f != nil && cmp(f.Smallest.UserKey, upper) < 0; f = files.Next() {
				var objMeta objstorage.ObjectMetadata
				var err error
				objMeta, err = provider.Lookup(fileTypeTable, f.FileNum.DiskFileNum())
				if err != nil {
					return err
				}
				if !objMeta.IsShared() {
					return errors.Wrapf(ErrInvalidSkipSharedIteration, "when processing file %s", objMeta.DiskFileNum)
				}
				var sst *SharedSSTMeta
				var skip bool
				sst, skip, err = iter.readState.db.truncateSharedFile(ctx, lower, upper, level, f, objMeta)
				if err != nil {
					return err
				}
				if skip {
					continue
				}
				if err = visitSharedFile(sst); err != nil {
					return err
				}
			}
		}
	}

	for valid := iter.seekGE(lower); valid && iter.error() == nil; valid = iter.next() {
		key := iter.unsafeKey()

		switch key.Kind() {
		case InternalKeyKindRangeKeyDelete, InternalKeyKindRangeKeyUnset, InternalKeyKindRangeKeySet:
			span := iter.unsafeSpan()
			if err := visitRangeKey(span.Start, span.End, span.Keys); err != nil {
				return err
			}
		case InternalKeyKindRangeDelete:
			rangeDel := iter.unsafeRangeDel()
			if err := visitRangeDel(rangeDel.Start, rangeDel.End, rangeDel.LargestSeqNum()); err != nil {
				return err
			}
		default:
			val := iter.lazyValue()
			if err := visitPointKey(key, val); err != nil {
				return err
			}
		}
	}

	return nil
}

// constructPointIter constructs a merging iterator and sets i.iter to it.
func (i *scanInternalIterator) constructPointIter(memtables flushableList, buf *iterAlloc) {
	// Merging levels and levels from iterAlloc.
	mlevels := buf.mlevels[:0]
	levels := buf.levels[:0]

	// We compute the number of levels needed ahead of time and reallocate a slice if
	// the array from the iterAlloc isn't large enough. Doing this allocation once
	// should improve the performance.
	numMergingLevels := len(memtables)
	numLevelIters := 0

	current := i.readState.current
	numMergingLevels += len(current.L0SublevelFiles)
	numLevelIters += len(current.L0SublevelFiles)
	for level := 1; level < len(current.Levels); level++ {
		if current.Levels[level].Empty() {
			continue
		}
		if i.opts.skipSharedLevels && level >= sharedLevelsStart {
			continue
		}
		numMergingLevels++
		numLevelIters++
	}

	if numMergingLevels > cap(mlevels) {
		mlevels = make([]mergingIterLevel, 0, numMergingLevels)
	}
	if numLevelIters > cap(levels) {
		levels = make([]levelIter, 0, numLevelIters)
	}
	// TODO(bilal): Push these into the iterAlloc buf.
	var rangeDelMiter keyspan.MergingIter
	rangeDelIters := make([]keyspan.FragmentIterator, 0, numMergingLevels)
	rangeDelLevels := make([]keyspan.LevelIter, 0, numLevelIters)

	// Next are the memtables.
	for j := len(memtables) - 1; j >= 0; j-- {
		mem := memtables[j]
		mlevels = append(mlevels, mergingIterLevel{
			iter: mem.newIter(&i.opts.IterOptions),
		})
		if rdi := mem.newRangeDelIter(&i.opts.IterOptions); rdi != nil {
			rangeDelIters = append(rangeDelIters, rdi)
		}
	}

	// Next are the file levels: L0 sub-levels followed by lower levels.
	mlevelsIndex := len(mlevels)
	levelsIndex := len(levels)
	mlevels = mlevels[:numMergingLevels]
	levels = levels[:numLevelIters]
	rangeDelLevels = rangeDelLevels[:numLevelIters]
	addLevelIterForFiles := func(files manifest.LevelIterator, level manifest.Level) {
		li := &levels[levelsIndex]
		rli := &rangeDelLevels[levelsIndex]

		li.init(
			context.Background(), i.opts.IterOptions, i.comparer.Compare, i.comparer.Split, i.newIters, files, level,
			internalIterOpts{})
		li.initBoundaryContext(&mlevels[mlevelsIndex].levelIterBoundaryContext)
		mlevels[mlevelsIndex].iter = li
		rli.Init(keyspan.SpanIterOptions{RangeKeyFilters: i.opts.RangeKeyFilters},
			i.comparer.Compare, tableNewRangeDelIter(context.Background(), i.newIters), files, level,
			manifest.KeyTypePoint)
		rangeDelIters = append(rangeDelIters, rli)

		levelsIndex++
		mlevelsIndex++
	}

	// Add level iterators for the L0 sublevels, iterating from newest to
	// oldest.
	for i := len(current.L0SublevelFiles) - 1; i >= 0; i-- {
		addLevelIterForFiles(current.L0SublevelFiles[i].Iter(), manifest.L0Sublevel(i))
	}

	// Add level iterators for the non-empty non-L0 levels.
	for level := 1; level < numLevels; level++ {
		if current.Levels[level].Empty() {
			continue
		}
		if i.opts.skipSharedLevels && level >= sharedLevelsStart {
			continue
		}
		addLevelIterForFiles(current.Levels[level].Iter(), manifest.Level(level))
	}
	buf.merging.init(&i.opts.IterOptions, &InternalIteratorStats{}, i.comparer.Compare, i.comparer.Split, mlevels...)
	buf.merging.snapshot = i.seqNum
	rangeDelMiter.Init(i.comparer.Compare, keyspan.VisibleTransform(i.seqNum), new(keyspan.MergingBuffers), rangeDelIters...)
	i.pointKeyIter = pointCollapsingIterator{
		comparer: i.comparer,
		merge:    i.merge,
		seqNum:   i.seqNum,
	}
	i.pointKeyIter.iter.Init(i.comparer, &buf.merging, &rangeDelMiter, nil /* mask */, i.opts.LowerBound, i.opts.UpperBound)
	i.iter = &i.pointKeyIter
}

// constructRangeKeyIter constructs the range-key iterator stack, populating
// i.rangeKey.rangeKeyIter with the resulting iterator. This is similar to
// Iterator.constructRangeKeyIter, except it doesn't handle batches and ensures
// iterConfig does *not* elide unsets/deletes.
func (i *scanInternalIterator) constructRangeKeyIter() {
	// We want the bounded iter from iterConfig, but not the collapsing of
	// RangeKeyUnsets and RangeKeyDels.
	i.rangeKey.rangeKeyIter = i.rangeKey.iterConfig.Init(
		i.comparer, i.seqNum, i.opts.LowerBound, i.opts.UpperBound,
		nil /* hasPrefix */, nil /* prefix */, false, /* onlySets */
		&i.rangeKey.rangeKeyBuffers.internal)

	// Next are the flushables: memtables and large batches.
	for j := len(i.readState.memtables) - 1; j >= 0; j-- {
		mem := i.readState.memtables[j]
		// We only need to read from memtables which contain sequence numbers older
		// than seqNum.
		if logSeqNum := mem.logSeqNum; logSeqNum >= i.seqNum {
			continue
		}
		if rki := mem.newRangeKeyIter(&i.opts.IterOptions); rki != nil {
			i.rangeKey.iterConfig.AddLevel(rki)
		}
	}

	current := i.readState.current
	// Next are the file levels: L0 sub-levels followed by lower levels.
	//
	// Add file-specific iterators for L0 files containing range keys. This is less
	// efficient than using levelIters for sublevels of L0 files containing
	// range keys, but range keys are expected to be sparse anyway, reducing the
	// cost benefit of maintaining a separate L0Sublevels instance for range key
	// files and then using it here.
	//
	// NB: We iterate L0's files in reverse order. They're sorted by
	// LargestSeqNum ascending, and we need to add them to the merging iterator
	// in LargestSeqNum descending to preserve the merging iterator's invariants
	// around Key Trailer order.
	iter := current.RangeKeyLevels[0].Iter()
	for f := iter.Last(); f != nil; f = iter.Prev() {
		spanIterOpts := &keyspan.SpanIterOptions{RangeKeyFilters: i.opts.RangeKeyFilters}
		spanIter, err := i.newIterRangeKey(f, spanIterOpts)
		if err != nil {
			i.rangeKey.iterConfig.AddLevel(&errorKeyspanIter{err: err})
			continue
		}
		i.rangeKey.iterConfig.AddLevel(spanIter)
	}

	// Add level iterators for the non-empty non-L0 levels.
	for level := 1; level < len(current.RangeKeyLevels); level++ {
		if current.RangeKeyLevels[level].Empty() {
			continue
		}
		if i.opts.skipSharedLevels && level >= sharedLevelsStart {
			continue
		}
		li := i.rangeKey.iterConfig.NewLevelIter()
		spanIterOpts := keyspan.SpanIterOptions{RangeKeyFilters: i.opts.RangeKeyFilters}
		li.Init(spanIterOpts, i.comparer.Compare, i.newIterRangeKey, current.RangeKeyLevels[level].Iter(),
			manifest.Level(level), manifest.KeyTypeRange)
		i.rangeKey.iterConfig.AddLevel(li)
	}
}

// seekGE seeks this iterator to the first key that's greater than or equal
// to the specified user key.
func (i *scanInternalIterator) seekGE(key []byte) bool {
	i.iterKey, i.iterValue = i.iter.SeekGE(key, base.SeekGEFlagsNone)
	return i.iterKey != nil
}

// unsafeKey returns the unsafe InternalKey at the current position. The value
// is nil if the iterator is invalid or exhausted.
func (i *scanInternalIterator) unsafeKey() *InternalKey {
	return i.iterKey
}

// lazyValue returns a value pointer to the value at the current iterator
// position. Behaviour undefined if unsafeKey() returns a Range key or Rangedel
// kind key.
func (i *scanInternalIterator) lazyValue() LazyValue {
	return i.iterValue
}

// unsafeRangeDel returns a range key span. Behaviour undefined if UnsafeKey returns
// a non-rangedel kind.
func (i *scanInternalIterator) unsafeRangeDel() *keyspan.Span {
	return i.pointKeyIter.iter.Span()
}

// unsafeSpan returns a range key span. Behaviour undefined if UnsafeKey returns
// a non-rangekey type.
func (i *scanInternalIterator) unsafeSpan() *keyspan.Span {
	return i.rangeKey.iiter.Span()
}

// next advances the iterator in the forward direction, and returns the
// iterator's new validity state.
func (i *scanInternalIterator) next() bool {
	i.iterKey, i.iterValue = i.iter.Next()
	return i.iterKey != nil
}

// error returns an error from the internal iterator, if there's any.
func (i *scanInternalIterator) error() error {
	return i.iter.Error()
}

// close closes this iterator, and releases any pooled objects.
func (i *scanInternalIterator) close() error {
	if err := i.iter.Close(); err != nil {
		return err
	}
	i.readState.unref()
	if i.rangeKey != nil {
		i.rangeKey.PrepareForReuse()
		*i.rangeKey = iteratorRangeKeyState{
			rangeKeyBuffers: i.rangeKey.rangeKeyBuffers,
		}
		iterRangeKeyStateAllocPool.Put(i.rangeKey)
		i.rangeKey = nil
	}
	if alloc := i.alloc; alloc != nil {
		for j := range i.boundsBuf {
			if cap(i.boundsBuf[j]) >= maxKeyBufCacheSize {
				alloc.boundsBuf[j] = nil
			} else {
				alloc.boundsBuf[j] = i.boundsBuf[j]
			}
		}
		*alloc = iterAlloc{
			keyBuf:              alloc.keyBuf[:0],
			boundsBuf:           alloc.boundsBuf,
			prefixOrFullSeekKey: alloc.prefixOrFullSeekKey[:0],
		}
		iterAllocPool.Put(alloc)
		i.alloc = nil
	}
	return nil
}

func (i *scanInternalIterator) initializeBoundBufs(lower, upper []byte) {
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
