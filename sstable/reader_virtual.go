// Copyright 2011 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package sstable

import (
	"context"

	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/keyspan"
	"github.com/cockroachdb/pebble/internal/rangekey"
	"github.com/cockroachdb/pebble/sstable/block"
)

// VirtualReader wraps Reader. Its purpose is to restrict functionality of the
// Reader which should be inaccessible to virtual sstables, and enforce bounds
// invariants associated with virtual sstables. All reads on virtual sstables
// should go through a VirtualReader.
//
// INVARIANT: Any iterators created through a virtual reader will guarantee that
// they don't expose keys outside the virtual sstable bounds.
type VirtualReader struct {
	vState     virtualState
	reader     *Reader
	Properties CommonProperties
}

var _ CommonReader = (*VirtualReader)(nil)

// Lightweight virtual sstable state which can be passed to sstable iterators.
type virtualState struct {
	lower            InternalKey
	upper            InternalKey
	fileNum          base.FileNum
	Compare          Compare
	isSharedIngested bool
}

// VirtualReaderParams are the parameters necessary to create a VirtualReader.
type VirtualReaderParams struct {
	Lower            InternalKey
	Upper            InternalKey
	FileNum          base.FileNum
	IsSharedIngested bool
	// Size is an estimate of the size of the [Lower, Upper) section of the table.
	Size uint64
	// BackingSize is the total size of the backing table. The ratio between Size
	// and BackingSize is used to estimate statistics.
	BackingSize uint64
}

// MakeVirtualReader is used to contruct a reader which can read from virtual
// sstables.
func MakeVirtualReader(reader *Reader, p VirtualReaderParams) VirtualReader {
	vState := virtualState{
		lower:            p.Lower,
		upper:            p.Upper,
		fileNum:          p.FileNum,
		Compare:          reader.Compare,
		isSharedIngested: p.IsSharedIngested,
	}
	v := VirtualReader{
		vState:     vState,
		reader:     reader,
		Properties: reader.Properties.GetScaledProperties(p.BackingSize, p.Size),
	}

	return v
}

// NewCompactionIter is the compaction iterator function for virtual readers.
func (v *VirtualReader) NewCompactionIter(
	transforms IterTransforms,
	statsAccum IterStatsAccumulator,
	rp ReaderProvider,
	bufferPool *block.BufferPool,
) (Iterator, error) {
	return v.reader.newCompactionIter(
		transforms, statsAccum, rp, &v.vState, bufferPool)
}

// NewPointIter returns an iterator for the point keys in the table.
//
// If transform.HideObsoletePoints is set, the callee assumes that filterer
// already includes obsoleteKeyBlockPropertyFilter. The caller can satisfy this
// contract by first calling TryAddBlockPropertyFilterForHideObsoletePoints.
//
// We assume that the [lower, upper) bounds (if specified) will have at least
// some overlap with the virtual sstable bounds. No overlap is not currently
// supported in the iterator.
func (v *VirtualReader) NewPointIter(
	ctx context.Context,
	transforms IterTransforms,
	lower, upper []byte,
	filterer *BlockPropertiesFilterer,
	filterBlockSizeLimit FilterBlockSizeLimit,
	stats *base.InternalIteratorStats,
	statsAccum IterStatsAccumulator,
	rp ReaderProvider,
) (Iterator, error) {
	return v.reader.newPointIter(
		ctx, transforms, lower, upper, filterer, filterBlockSizeLimit,
		stats, statsAccum, rp, &v.vState)
}

// ValidateBlockChecksumsOnBacking will call ValidateBlockChecksumsOnBacking on the underlying reader.
// Note that block checksum validation is NOT restricted to virtual sstable bounds.
func (v *VirtualReader) ValidateBlockChecksumsOnBacking() error {
	return v.reader.ValidateBlockChecksums()
}

// NewRawRangeDelIter wraps Reader.NewRawRangeDelIter.
func (v *VirtualReader) NewRawRangeDelIter(
	ctx context.Context, transforms FragmentIterTransforms,
) (keyspan.FragmentIterator, error) {
	iter, err := v.reader.NewRawRangeDelIter(ctx, transforms)
	if err != nil {
		return nil, err
	}
	if iter == nil {
		return nil, nil
	}

	// Note that if upper is not an exclusive sentinel, Truncate will assert that
	// there is no span that contains that key.
	//
	// As an example, if an sstable contains a rangedel a-c and point keys at
	// a.SET.2 and b.SET.3, the file bounds [a#2,SET-b#RANGEDELSENTINEL] are
	// allowed (as they exclude b.SET.3), or [a#2,SET-c#RANGEDELSENTINEL] (as it
	// includes both point keys), but not [a#2,SET-b#3,SET] (as it would truncate
	// the rangedel at b and lead to the point being uncovered).
	return keyspan.Truncate(
		v.reader.Compare, iter,
		base.UserKeyBoundsFromInternal(v.vState.lower, v.vState.upper),
	), nil
}

// NewRawRangeKeyIter wraps Reader.NewRawRangeKeyIter.
func (v *VirtualReader) NewRawRangeKeyIter(
	ctx context.Context, transforms FragmentIterTransforms,
) (keyspan.FragmentIterator, error) {
	syntheticSeqNum := transforms.SyntheticSeqNum
	if v.vState.isSharedIngested {
		// Don't pass a synthetic sequence number for shared ingested sstables. We
		// need to know the materialized sequence numbers, and we will set up the
		// appropriate sequence number substitution below.
		transforms.SyntheticSeqNum = 0
	}
	iter, err := v.reader.NewRawRangeKeyIter(ctx, transforms)
	if err != nil {
		return nil, err
	}
	if iter == nil {
		return nil, nil
	}

	if v.vState.isSharedIngested {
		// We need to coalesce range keys within each sstable, and then apply the
		// synthetic sequence number. For this, we use ForeignSSTTransformer.
		//
		// TODO(bilal): Avoid these allocations by hoisting the transformer and
		// transform iter into VirtualReader.
		transform := &rangekey.ForeignSSTTransformer{
			Equal:  v.reader.Equal,
			SeqNum: base.SeqNum(syntheticSeqNum),
		}
		transformIter := &keyspan.TransformerIter{
			FragmentIterator: iter,
			Transformer:      transform,
			SuffixCmp:        v.reader.Comparer.CompareRangeSuffixes,
		}
		iter = transformIter
	}

	// Note that if upper is not an exclusive sentinel, Truncate will assert that
	// there is no span that contains that key.
	//
	// As an example, if an sstable contains a range key a-c and point keys at
	// a.SET.2 and b.SET.3, the file bounds [a#2,SET-b#RANGEKEYSENTINEL] are
	// allowed (as they exclude b.SET.3), or [a#2,SET-c#RANGEKEYSENTINEL] (as it
	// includes both point keys), but not [a#2,SET-b#3,SET] (as it would truncate
	// the range key at b and lead to the point being uncovered).
	return keyspan.Truncate(
		v.reader.Compare, iter,
		base.UserKeyBoundsFromInternal(v.vState.lower, v.vState.upper),
	), nil
}

// UnsafeReader returns the underlying *sstable.Reader behind a VirtualReader.
func (v *VirtualReader) UnsafeReader() *Reader {
	return v.reader
}

// Constrain bounds will narrow the start, end bounds if they do not fit within
// the virtual sstable. The function will return if the new end key is
// inclusive.
func (v *virtualState) constrainBounds(
	start, end []byte, endInclusive bool,
) (lastKeyInclusive bool, first []byte, last []byte) {
	first = start
	if start == nil || v.Compare(start, v.lower.UserKey) < 0 {
		first = v.lower.UserKey
	}

	// Note that we assume that start, end has some overlap with the virtual
	// sstable bounds.
	last = v.upper.UserKey
	lastKeyInclusive = !v.upper.IsExclusiveSentinel()
	if end != nil {
		cmp := v.Compare(end, v.upper.UserKey)
		switch {
		case cmp == 0:
			lastKeyInclusive = !v.upper.IsExclusiveSentinel() && endInclusive
			last = v.upper.UserKey
		case cmp > 0:
			lastKeyInclusive = !v.upper.IsExclusiveSentinel()
			last = v.upper.UserKey
		default:
			lastKeyInclusive = endInclusive
			last = end
		}
	}
	// TODO(bananabrick): What if someone passes in bounds completely outside of
	// virtual sstable bounds?
	return lastKeyInclusive, first, last
}

// EstimateDiskUsage just calls VirtualReader.reader.EstimateDiskUsage after
// enforcing the virtual sstable bounds.
func (v *VirtualReader) EstimateDiskUsage(start, end []byte) (uint64, error) {
	_, f, l := v.vState.constrainBounds(start, end, true /* endInclusive */)
	return v.reader.EstimateDiskUsage(f, l)
}

// CommonProperties implements the CommonReader interface.
func (v *VirtualReader) CommonProperties() *CommonProperties {
	return &v.Properties
}
