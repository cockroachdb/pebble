// Copyright 2023 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package pebble

import (
	"context"
	"fmt"
	"sync"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/invariants"
	"github.com/cockroachdb/pebble/internal/keyspan"
	"github.com/cockroachdb/pebble/internal/manifest"
	"github.com/cockroachdb/pebble/objstorage"
	"github.com/cockroachdb/pebble/sstable"
)

const (
	// In skip-shared iteration mode, keys in levels sharedLevelsStart and greater
	// (i.e. lower in the LSM) are skipped.
	sharedLevelsStart = 5
)

// ErrInvalidSkipSharedIteration is returned by ScanInternal if it was called
// with a shared file visitor function, and a file in a shareable level (i.e.
// level >= sharedLevelsStart) was found to not be in shared storage according
// to objstorage.Provider, or not shareable for another reason such as for
// containing keys newer than the snapshot sequence number.
var ErrInvalidSkipSharedIteration = errors.New("pebble: cannot use skip-shared iteration due to non-shareable files in lower levels")

// SharedSSTMeta represents an sstable on shared storage that can be ingested
// by another pebble instance. This struct must contain all fields that are
// required for a Pebble instance to ingest a foreign sstable on shared storage,
// including constructing any relevant objstorage.Provider / remoteobjcat.Catalog
// data structures, as well as creating virtual FileMetadatas.
//
// Note that the Pebble instance creating and returning a SharedSSTMeta might
// not be the one that created the underlying sstable on shared storage to begin
// with; it's possible for a Pebble instance to reshare an sstable that was
// shared to it.
type SharedSSTMeta struct {
	// Backing is the shared object underlying this SST. Can be attached to an
	// objstorage.Provider.
	Backing objstorage.RemoteObjectBackingHandle

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
	pcIterPosPrev
)

// pointCollapsingSSTIterator implements sstable.Iterator while composing
// pointCollapsingIterator.
type pointCollapsingSSTIterator struct {
	pointCollapsingIterator
	childIter sstable.Iterator
}

var pcSSTIterPool = sync.Pool{
	New: func() interface{} {
		return &pointCollapsingSSTIterator{}
	},
}

// MaybeFilteredKeys implements the sstable.Iterator interface.
func (p *pointCollapsingSSTIterator) MaybeFilteredKeys() bool {
	return p.childIter.MaybeFilteredKeys()
}

// SetCloseHook implements the sstable.Iterator interface.
func (p *pointCollapsingSSTIterator) SetCloseHook(fn func(i sstable.Iterator) error) {
	p.childIter.SetCloseHook(fn)
}

// Close implements the sstable.Iterator interface.
func (p *pointCollapsingSSTIterator) Close() error {
	err := p.pointCollapsingIterator.Close()
	p.pointCollapsingIterator = pointCollapsingIterator{}
	p.childIter = nil
	pcSSTIterPool.Put(p)
	return err
}

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
	// The current position of `iter`. Always owned by the underlying iter.
	iterKey *InternalKey
	// The last saved key. findNextEntry and similar methods are expected to save
	// the current value of iterKey to savedKey if they're iterating away from the
	// current key but still need to retain it. See comments in findNextEntry on
	// how this field is used.
	//
	// At the end of a positioning call:
	//  - if pos == pcIterPosNext, iterKey is pointing to the next user key owned
	//    by `iter` while savedKey is holding a copy to our current key.
	//  - If pos == pcIterPosCur, iterKey is pointing to an `iter`-owned current
	//    key, and savedKey is either undefined or pointing to a version of the
	//    current key owned by this iterator (i.e. backed by savedKeyBuf).
	//  - if pos == pcIterPosPrev, iterKey is pointing to the key before
	//    p.savedKey. p.savedKey is treated as the current key and is owned by
	//    this iterator, while p.iterKey is the previous key that is the current
	//    position of the child iterator.
	savedKey    InternalKey
	savedKeyBuf []byte
	// elideRangeDeletes ignores range deletes returned by the interleaving
	// iterator if true.
	elideRangeDeletes bool
	// Value at the current iterator position, at iterKey.
	iterValue base.LazyValue
	// Saved value backed by valueBuf, if set. Used in reverse iteration, and
	// for merges.
	savedValue base.LazyValue
	// Used for Merge keys only.
	valueMerger ValueMerger
	valueBuf    []byte
	// If fixedSeqNum is non-zero, all emitted points are verified to have this
	// fixed sequence number.
	fixedSeqNum uint64
}

func (p *pointCollapsingIterator) Span() *keyspan.Span {
	return p.iter.Span()
}

// SeekPrefixGE implements the InternalIterator interface.
func (p *pointCollapsingIterator) SeekPrefixGE(
	prefix, key []byte, flags base.SeekGEFlags,
) (*base.InternalKey, base.LazyValue) {
	p.resetKey()
	p.iterKey, p.iterValue = p.iter.SeekPrefixGE(prefix, key, flags)
	p.pos = pcIterPosCur
	if p.iterKey == nil {
		return nil, base.LazyValue{}
	}
	return p.findNextEntry()
}

// SeekGE implements the InternalIterator interface.
func (p *pointCollapsingIterator) SeekGE(
	key []byte, flags base.SeekGEFlags,
) (*base.InternalKey, base.LazyValue) {
	p.resetKey()
	p.iterKey, p.iterValue = p.iter.SeekGE(key, flags)
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
	p.resetKey()
	p.iterKey, p.iterValue = p.iter.SeekLT(key, flags)
	p.pos = pcIterPosCur
	if p.iterKey == nil {
		return nil, base.LazyValue{}
	}
	return p.findPrevEntry()
}

func (p *pointCollapsingIterator) resetKey() {
	p.savedKey.UserKey = p.savedKeyBuf[:0]
	p.savedKey.Trailer = 0
	p.valueMerger = nil
	p.valueBuf = p.valueBuf[:0]
	p.iterKey = nil
	p.pos = pcIterPosCur
}

func (p *pointCollapsingIterator) verifySeqNum(key *base.InternalKey) *base.InternalKey {
	if !invariants.Enabled {
		return key
	}
	if p.fixedSeqNum == 0 || key == nil || key.Kind() == InternalKeyKindRangeDelete {
		return key
	}
	if key.SeqNum() != p.fixedSeqNum {
		panic(fmt.Sprintf("expected foreign point key to have seqnum %d, got %d", p.fixedSeqNum, key.SeqNum()))
	}
	return key
}

// finishAndReturnMerge finishes off the valueMerger and returns the saved key.
func (p *pointCollapsingIterator) finishAndReturnMerge() (*base.InternalKey, base.LazyValue) {
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
	val := base.MakeInPlaceValue(p.valueBuf)
	return p.verifySeqNum(&p.savedKey), val
}

// findNextEntry is called to return the next key. p.iter must be positioned at the
// start of the first user key we are interested in.
func (p *pointCollapsingIterator) findNextEntry() (*base.InternalKey, base.LazyValue) {
	p.saveKey()
	// Saves a comparison in the fast path
	firstIteration := true
	for p.iterKey != nil {
		// NB: p.savedKey is either the current key (iff p.iterKey == firstKey),
		// or the previous key.
		if !firstIteration && !p.comparer.Equal(p.iterKey.UserKey, p.savedKey.UserKey) {
			if p.valueMerger != nil {
				if p.savedKey.Kind() != InternalKeyKindMerge {
					panic(fmt.Sprintf("expected key %s to have MERGE kind", p.iterKey))
				}
				p.pos = pcIterPosNext
				return p.finishAndReturnMerge()
			}
			p.saveKey()
			continue
		}
		firstIteration = false
		if s := p.iter.Span(); s != nil && s.CoversAt(p.seqNum, p.iterKey.SeqNum()) {
			// All future keys for this user key must be deleted.
			if p.valueMerger != nil {
				return p.finishAndReturnMerge()
			} else if p.savedKey.Kind() == InternalKeyKindSingleDelete {
				panic("cannot process singledel key in point collapsing iterator")
			}
			// Fast forward to the next user key.
			p.saveKey()
			p.iterKey, p.iterValue = p.iter.Next()
			for p.iterKey != nil && p.savedKey.SeqNum() >= p.iterKey.SeqNum() && p.comparer.Equal(p.iterKey.UserKey, p.savedKey.UserKey) {
				p.iterKey, p.iterValue = p.iter.Next()
			}
			continue
		}
		switch p.savedKey.Kind() {
		case InternalKeyKindSet, InternalKeyKindDelete, InternalKeyKindSetWithDelete, InternalKeyKindDeleteSized:
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
			return p.verifySeqNum(p.iterKey), p.iterValue
		case InternalKeyKindSingleDelete:
			// Panic, as this iterator is not expected to observe single deletes.
			panic("cannot process singledel key in point collapsing iterator")
		case InternalKeyKindMerge:
			if p.valueMerger == nil {
				// Set up merger. This is the first Merge key encountered.
				value, callerOwned, err := p.iterValue.Value(p.valueBuf[:0])
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
				p.iterKey, p.iterValue = p.iter.Next()
				continue
			}
			switch p.iterKey.Kind() {
			case InternalKeyKindSet, InternalKeyKindMerge, InternalKeyKindSetWithDelete:
				// Merge into key.
				value, callerOwned, err := p.iterValue.Value(p.valueBuf[:0])
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
				return p.finishAndReturnMerge()
			}
			p.iterKey, p.iterValue = p.iter.Next()
		case InternalKeyKindRangeDelete:
			if p.elideRangeDeletes {
				// Skip this range delete, and process any point after it.
				p.iterKey, p.iterValue = p.iter.Next()
				p.saveKey()
				continue
			}
			// These are interleaved by the interleaving iterator ahead of all points.
			// We should pass them as-is, but also account for any points ahead of
			// them.
			p.pos = pcIterPosCur
			return p.verifySeqNum(p.iterKey), p.iterValue
		default:
			panic(fmt.Sprintf("unexpected kind: %d", p.iterKey.Kind()))
		}
	}
	if p.valueMerger != nil {
		p.pos = pcIterPosNext
		return p.finishAndReturnMerge()
	}
	p.resetKey()
	return nil, base.LazyValue{}
}

// findPrevEntry finds the relevant point key to return for the previous user key
// (i.e. in reverse iteration). Requires that the iterator is already positioned
// at the first-in-reverse (i.e. rightmost / largest) internal key encountered
// for that user key.
func (p *pointCollapsingIterator) findPrevEntry() (*base.InternalKey, base.LazyValue) {
	if p.iterKey == nil {
		p.pos = pcIterPosCur
		return nil, base.LazyValue{}
	}

	p.saveKey()
	firstIteration := true
	for p.iterKey != nil {
		if !firstIteration && !p.comparer.Equal(p.iterKey.UserKey, p.savedKey.UserKey) {
			p.pos = pcIterPosPrev
			return p.verifySeqNum(&p.savedKey), p.savedValue
		}
		firstIteration = false
		if s := p.iter.Span(); s != nil && s.CoversAt(p.seqNum, p.iterKey.SeqNum()) {
			// Skip this key.
			p.iterKey, p.iterValue = p.iter.Prev()
			p.saveKey()
			continue
		}
		switch p.iterKey.Kind() {
		case InternalKeyKindSet, InternalKeyKindDelete, InternalKeyKindSetWithDelete:
			// Instead of calling saveKey(), we take advantage of the invariant that
			// p.savedKey.UserKey == p.iterKey.UserKey (otherwise we'd have gone into
			// the !p.comparer.Equal(p.iterKey.UserKey, p.savedKey.UserKey) case at
			// the top of this for loop). That allows us to just save the trailer
			// and move on.
			if invariants.Enabled && !p.comparer.Equal(p.iterKey.UserKey, p.savedKey.UserKey) {
				panic("unexpected inequality between p.iterKey and p.savedKey")
			}
			p.savedKey.Trailer = p.iterKey.Trailer
			// Copy value into p.savedValue.
			value, callerOwned, err := p.iterValue.Value(p.valueBuf[:0])
			if err != nil {
				p.err = err
				return nil, base.LazyValue{}
			}
			if !callerOwned {
				p.valueBuf = append(p.valueBuf[:0], value...)
			} else {
				p.valueBuf = value
			}
			p.valueMerger = nil
			p.savedValue = base.MakeInPlaceValue(p.valueBuf)
			p.iterKey, p.iterValue = p.iter.Prev()
			continue
		case InternalKeyKindSingleDelete:
			// Panic, as this iterator is not expected to observe single deletes.
			panic("cannot process singledel key in point collapsing iterator")
		case InternalKeyKindMerge:
			panic("cannot process merge key in point collapsing iterator in reverse iteration")
		case InternalKeyKindRangeDelete:
			if p.elideRangeDeletes {
				// Skip this range delete, and process any point before it.
				p.iterKey, p.iterValue = p.iter.Prev()
				continue
			}
			// These are interleaved by the interleaving iterator behind all points.
			if p.savedKey.Kind() != InternalKeyKindRangeDelete {
				// If the previous key was not a rangedel, we need to return it. Pretend that we're at the
				// previous user key (i.e. with p.pos = pcIterPosPrev) even if we're not, so on the next
				// Prev() we encounter and return this rangedel. For now return the point ahead of
				// this range del (if any).
				p.pos = pcIterPosPrev
				return p.verifySeqNum(&p.savedKey), p.savedValue
			}
			// We take advantage of the fact that a Prev() *on* a RangeDel iterKey
			// always takes us to a different user key, so on the next iteration
			// we will fall into the !p.comparer.Equal(p.iterKey.UserKey, p.savedKey.UserKey)
			// case.
			//
			// Instead of calling saveKey(), we take advantage of the invariant that
			// p.savedKey.UserKey == p.iterKey.UserKey (otherwise we'd have gone into
			// the !p.comparer.Equal(p.iterKey.UserKey, p.savedKey.UserKey) case at
			// the top of this for loop). That allows us to just save the trailer
			// and move on.
			if invariants.Enabled && !p.comparer.Equal(p.iterKey.UserKey, p.savedKey.UserKey) {
				panic("unexpected inequality between p.iterKey and p.savedKey")
			}
			p.savedKey.Trailer = p.iterKey.Trailer
			// Copy value into p.savedValue.
			value, callerOwned, err := p.iterValue.Value(p.valueBuf[:0])
			if err != nil {
				p.err = err
				return nil, base.LazyValue{}
			}
			if !callerOwned {
				p.valueBuf = append(p.valueBuf[:0], value...)
			} else {
				p.valueBuf = value
			}
			p.valueMerger = nil
			p.savedValue = base.MakeInPlaceValue(p.valueBuf)
			p.iterKey, p.iterValue = p.iter.Prev()
			continue
		default:
			panic(fmt.Sprintf("unexpected kind: %d", p.iterKey.Kind()))
		}
	}
	p.pos = pcIterPosPrev
	return p.verifySeqNum(&p.savedKey), p.savedValue
}

// First implements the InternalIterator interface.
func (p *pointCollapsingIterator) First() (*base.InternalKey, base.LazyValue) {
	p.resetKey()
	p.iterKey, p.iterValue = p.iter.First()
	p.pos = pcIterPosCur
	if p.iterKey == nil {
		return nil, base.LazyValue{}
	}
	return p.findNextEntry()
}

// Last implements the InternalIterator interface.
func (p *pointCollapsingIterator) Last() (*base.InternalKey, base.LazyValue) {
	p.resetKey()
	p.iterKey, p.iterValue = p.iter.Last()
	p.pos = pcIterPosCur
	if p.iterKey == nil {
		return nil, base.LazyValue{}
	}
	return p.findPrevEntry()
}

func (p *pointCollapsingIterator) saveKey() {
	if p.iterKey == nil {
		p.savedKey = InternalKey{UserKey: p.savedKeyBuf[:0]}
		return
	}
	p.savedKeyBuf = append(p.savedKeyBuf[:0], p.iterKey.UserKey...)
	p.savedKey = InternalKey{UserKey: p.savedKeyBuf, Trailer: p.iterKey.Trailer}
}

// Next implements the InternalIterator interface.
func (p *pointCollapsingIterator) Next() (*base.InternalKey, base.LazyValue) {
	switch p.pos {
	case pcIterPosPrev:
		p.saveKey()
		if p.iterKey != nil && p.iterKey.Kind() == InternalKeyKindRangeDelete && !p.elideRangeDeletes {
			p.iterKey, p.iterValue = p.iter.Next()
			p.pos = pcIterPosCur
		} else {
			// Fast forward to the next user key.
			key, val := p.iter.Next()
			// p.iterKey.SeqNum() >= key.SeqNum() is an optimization that allows us to
			// use p.iterKey.SeqNum() < key.SeqNum() as a sign that the user key has
			// changed, without needing to do the full key comparison.
			for key != nil && p.savedKey.SeqNum() >= key.SeqNum() &&
				p.comparer.Equal(p.savedKey.UserKey, key.UserKey) {
				key, val = p.iter.Next()
			}
			if key == nil {
				// There are no keys to return.
				p.resetKey()
				return nil, base.LazyValue{}
			}
			p.iterKey, p.iterValue = key, val
			p.pos = pcIterPosCur
		}
		fallthrough
	case pcIterPosCur:
		p.saveKey()
		if p.iterKey != nil && p.iterKey.Kind() == InternalKeyKindRangeDelete && !p.elideRangeDeletes {
			// Step over the interleaved range delete and process the very next
			// internal key, even if it's at the same user key. This is because a
			// point for that user key has not been returned yet.
			p.iterKey, p.iterValue = p.iter.Next()
			break
		}
		// Fast forward to the next user key.
		key, val := p.iter.Next()
		// p.iterKey.SeqNum() >= key.SeqNum() is an optimization that allows us to
		// use p.iterKey.SeqNum() < key.SeqNum() as a sign that the user key has
		// changed, without needing to do the full key comparison.
		for key != nil && p.savedKey.SeqNum() >= key.SeqNum() &&
			p.comparer.Equal(p.savedKey.UserKey, key.UserKey) {
			key, val = p.iter.Next()
		}
		if key == nil {
			// There are no keys to return.
			p.resetKey()
			return nil, base.LazyValue{}
		}
		p.iterKey, p.iterValue = key, val
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
	// TODO(bilal): Implement this optimally. It'll be similar to SeekGE, except we'll call
	// the child iterator's NextPrefix, and have some special logic in case pos
	// is pcIterPosNext.
	return p.SeekGE(succKey, base.SeekGEFlagsNone)
}

// Prev implements the InternalIterator interface.
func (p *pointCollapsingIterator) Prev() (*base.InternalKey, base.LazyValue) {
	switch p.pos {
	case pcIterPosNext:
		// Rewind backwards to the previous iter key.
		p.saveKey()
		key, val := p.iter.Prev()
		for key != nil && p.savedKey.SeqNum() <= key.SeqNum() &&
			p.comparer.Equal(p.savedKey.UserKey, key.UserKey) {
			if key.Kind() == InternalKeyKindRangeDelete && !p.elideRangeDeletes {
				// We need to pause at this range delete and return it as-is, as "cur"
				// is referencing the point key after it, not the range delete.
				break
			}
			key, val = p.iter.Prev()
		}
		p.iterKey = key
		p.iterValue = val
		p.pos = pcIterPosCur
		fallthrough
	case pcIterPosCur:
		p.saveKey()
		key, val := p.iter.Prev()
		for key != nil && p.savedKey.SeqNum() <= key.SeqNum() &&
			p.comparer.Equal(p.savedKey.UserKey, key.UserKey) {
			if key.Kind() == InternalKeyKindRangeDelete && !p.elideRangeDeletes {
				// We need to pause at this range delete and return it as-is, as "cur"
				// is referencing the point key after it, not the range delete.
				break
			}
			key, val = p.iter.Prev()
		}
		p.iterKey = key
		p.iterValue = val
		p.pos = pcIterPosCur
	case pcIterPosPrev:
		// Do nothing.
		p.pos = pcIterPosCur
	}
	if p.iterKey == nil {
		p.resetKey()
		return nil, base.LazyValue{}
	}
	return p.findPrevEntry()
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

// This is used with scanInternalIterator to surface additional iterator-specific info where possible.
// Note this is currently only used for point keys.
type iterInfo struct {
	level int
}

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
	pointKeyIter    internalIterator
	iterKey         *InternalKey
	iterValue       LazyValue
	alloc           *iterAlloc
	newIters        tableNewIters
	newIterRangeKey keyspan.TableNewSpanIter
	seqNum          uint64
	iterInfo        []iterInfo
	mergingIter     *mergingIter

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
	sst.Backing, err = d.objProvider.RemoteObjectBacking(&objMeta)
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
		rangeDelIter = keyspan.Truncate(
			cmp, rangeDelIter, lower, upper, nil, nil,
			false, /* panicOnUpperTruncate */
		)
		defer rangeDelIter.Close()
	}
	rangeKeyIter, err := d.tableNewRangeKeyIter(file, keyspan.SpanIterOptions{Level: manifest.Level(level)})
	if err != nil {
		return nil, false, err
	}
	if rangeKeyIter != nil {
		rangeKeyIter = keyspan.Truncate(
			cmp, rangeKeyIter, lower, upper, nil, nil,
			false, /* panicOnUpperTruncate */
		)
		defer rangeKeyIter.Close()
	}
	// Check if we need to truncate on the left side. This means finding a new
	// LargestPointKey and LargestRangeKey that is >= lower.
	if needsLowerTruncate {
		sst.SmallestPointKey.UserKey = sst.SmallestPointKey.UserKey[:0]
		sst.SmallestPointKey.Trailer = 0
		key, _ := iter.SeekGE(lower, base.SeekGEFlagsNone)
		foundPointKey := key != nil
		if key != nil {
			sst.SmallestPointKey.CopyFrom(*key)
		}
		if rangeDelIter != nil {
			span := rangeDelIter.SeekGE(lower)
			if span != nil && (len(sst.SmallestPointKey.UserKey) == 0 || base.InternalCompare(cmp, span.SmallestKey(), sst.SmallestPointKey) < 0) {
				sst.SmallestPointKey.CopyFrom(span.SmallestKey())
				foundPointKey = true
			}
		}
		if !foundPointKey {
			// There are no point keys in the span we're interested in.
			sst.SmallestPointKey = InternalKey{}
			sst.LargestPointKey = InternalKey{}
		}
		sst.SmallestRangeKey.UserKey = sst.SmallestRangeKey.UserKey[:0]
		sst.SmallestRangeKey.Trailer = 0
		if rangeKeyIter != nil {
			span := rangeKeyIter.SeekGE(lower)
			if span != nil {
				sst.SmallestRangeKey.CopyFrom(span.SmallestKey())
			} else {
				// There are no range keys in the span we're interested in.
				sst.SmallestRangeKey = InternalKey{}
				sst.LargestRangeKey = InternalKey{}
			}
		}
	}
	// Check if we need to truncate on the right side. This means finding a new
	// LargestPointKey and LargestRangeKey that is < upper.
	if needsUpperTruncate {
		sst.LargestPointKey.UserKey = sst.LargestPointKey.UserKey[:0]
		sst.LargestPointKey.Trailer = 0
		key, _ := iter.SeekLT(upper, base.SeekLTFlagsNone)
		foundPointKey := key != nil
		if key != nil {
			sst.LargestPointKey.CopyFrom(*key)
		}
		if rangeDelIter != nil {
			span := rangeDelIter.SeekLT(upper)
			if span != nil && (len(sst.LargestPointKey.UserKey) == 0 || base.InternalCompare(cmp, span.LargestKey(), sst.LargestPointKey) > 0) {
				sst.LargestPointKey.CopyFrom(span.LargestKey())
				foundPointKey = true
			}
		}
		if !foundPointKey {
			// There are no point keys in the span we're interested in.
			sst.SmallestPointKey = InternalKey{}
			sst.LargestPointKey = InternalKey{}
		}
		sst.LargestRangeKey.UserKey = sst.LargestRangeKey.UserKey[:0]
		sst.LargestRangeKey.Trailer = 0
		if rangeKeyIter != nil {
			span := rangeKeyIter.SeekLT(upper)
			if span != nil {
				sst.LargestRangeKey.CopyFrom(span.LargestKey())
			} else {
				// There are no range keys in the span we're interested in.
				sst.SmallestRangeKey = InternalKey{}
				sst.LargestRangeKey = InternalKey{}
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
	sst.Size, err = d.tableCache.estimateSize(file, sst.Smallest.UserKey, sst.Largest.UserKey)
	if err != nil {
		return nil, false, err
	}
	// On occasion, estimateSize gives us a low estimate, i.e. a 0 file size. This
	// can cause panics in places where we divide by file sizes. Correct for it
	// here.
	if sst.Size == 0 {
		sst.Size = 1
	}
	return sst, false, nil
}

func scanInternalImpl(
	ctx context.Context, lower, upper []byte, iter *scanInternalIterator, opts *scanInternalOptions,
) error {
	if opts.visitSharedFile != nil && (lower == nil || upper == nil) {
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
	seqNum := iter.seqNum
	if opts.visitSharedFile != nil {
		if provider == nil {
			panic("expected non-nil Provider in skip-shared iteration mode")
		}
		for level := sharedLevelsStart; level < numLevels; level++ {
			files := iter.readState.current.Levels[level].Iter()
			for f := files.SeekGE(cmp, lower); f != nil && cmp(f.Smallest.UserKey, upper) < 0; f = files.Next() {
				var objMeta objstorage.ObjectMetadata
				var err error
				objMeta, err = provider.Lookup(fileTypeTable, f.FileBacking.DiskFileNum)
				if err != nil {
					return err
				}
				if !objMeta.IsShared() {
					return errors.Wrapf(ErrInvalidSkipSharedIteration, "file %s is not shared", objMeta.DiskFileNum)
				}
				if !base.Visible(f.LargestSeqNum, seqNum, base.InternalKeySeqNumMax) {
					return errors.Wrapf(ErrInvalidSkipSharedIteration, "file %s contains keys newer than snapshot", objMeta.DiskFileNum)
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
				if err = opts.visitSharedFile(sst); err != nil {
					return err
				}
			}
		}
	}

	for valid := iter.seekGE(lower); valid && iter.error() == nil; valid = iter.next() {
		key := iter.unsafeKey()
		switch key.Kind() {
		case InternalKeyKindRangeKeyDelete, InternalKeyKindRangeKeyUnset, InternalKeyKindRangeKeySet:
			if opts.visitRangeKey != nil {
				span := iter.unsafeSpan()
				if err := opts.visitRangeKey(span.Start, span.End, span.Keys); err != nil {
					return err
				}
			}
		case InternalKeyKindRangeDelete:
			if opts.visitRangeDel != nil {
				rangeDel := iter.unsafeRangeDel()
				if err := opts.visitRangeDel(rangeDel.Start, rangeDel.End, rangeDel.LargestSeqNum()); err != nil {
					return err
				}
			}
		default:
			if opts.visitPointKey != nil {
				mergingIterIdx := iter.mergingIter.heap.items[0].index
				iterInfo := iter.iterInfo[mergingIterIdx]
				val := iter.lazyValue()
				if err := opts.visitPointKey(key, val, iterInfo); err != nil {
					return err
				}
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

	i.iterInfo = make([]iterInfo, numMergingLevels)
	mlevelsIndex := 0

	// Next are the memtables.
	for j := len(memtables) - 1; j >= 0; j-- {
		mem := memtables[j]
		mlevels = append(mlevels, mergingIterLevel{
			iter: mem.newIter(&i.opts.IterOptions),
		})
		// Level -1 represents a memtable.
		i.iterInfo[mlevelsIndex] = iterInfo{level: -1}
		mlevelsIndex++
		if rdi := mem.newRangeDelIter(&i.opts.IterOptions); rdi != nil {
			rangeDelIters = append(rangeDelIters, rdi)
		}
	}

	// Next are the file levels: L0 sub-levels followed by lower levels.
	levelsIndex := len(levels)
	mlevels = mlevels[:numMergingLevels]
	levels = levels[:numLevelIters]
	rangeDelLevels = rangeDelLevels[:numLevelIters]
	i.opts.IterOptions.snapshotForHideObsoletePoints = i.seqNum
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

	for j := len(current.L0SublevelFiles) - 1; j >= 0; j-- {
		i.iterInfo[mlevelsIndex] = iterInfo{level: 0}
		addLevelIterForFiles(current.L0SublevelFiles[j].Iter(), manifest.L0Sublevel(j))
	}
	// Add level iterators for the non-empty non-L0 levels.
	for level := 1; level < numLevels; level++ {
		if current.Levels[level].Empty() {
			continue
		}
		if i.opts.skipSharedLevels && level >= sharedLevelsStart {
			continue
		}
		i.iterInfo[mlevelsIndex] = iterInfo{level: level}
		addLevelIterForFiles(current.Levels[level].Iter(), manifest.Level(level))
	}

	buf.merging.init(&i.opts.IterOptions, &InternalIteratorStats{}, i.comparer.Compare, i.comparer.Split, mlevels...)
	buf.merging.snapshot = i.seqNum
	rangeDelMiter.Init(i.comparer.Compare, keyspan.VisibleTransform(i.seqNum), new(keyspan.MergingBuffers), rangeDelIters...)

	if i.opts.includeObsoleteKeys {
		iiter := &keyspan.InterleavingIter{}
		iiter.Init(i.comparer, &buf.merging, &rangeDelMiter, nil /* mask */, i.opts.LowerBound, i.opts.UpperBound)
		i.pointKeyIter = iiter
	} else {
		pcIter := &pointCollapsingIterator{
			comparer: i.comparer,
			merge:    i.merge,
			seqNum:   i.seqNum,
		}
		pcIter.iter.Init(i.comparer, &buf.merging, &rangeDelMiter, nil /* mask */, i.opts.LowerBound, i.opts.UpperBound)
		i.pointKeyIter = pcIter
	}
	i.iter = i.pointKeyIter
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
		spanIter, err := i.newIterRangeKey(f, i.opts.SpanIterOptions(manifest.Level(0)))
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
		spanIterOpts := i.opts.SpanIterOptions(manifest.Level(level))
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

type spanInternalIterator interface {
	Span() *keyspan.Span
}

// unsafeRangeDel returns a range key span. Behaviour undefined if UnsafeKey returns
// a non-rangedel kind.
func (i *scanInternalIterator) unsafeRangeDel() *keyspan.Span {
	return i.pointKeyIter.(spanInternalIterator).Span()
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
