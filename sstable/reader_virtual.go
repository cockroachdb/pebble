// Copyright 2011 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package sstable

import (
	"bytes"
	"context"

	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/keyspan"
	"github.com/cockroachdb/pebble/internal/manifest"
	"github.com/cockroachdb/pebble/internal/rangekey"
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

// Lightweight virtual sstable state which can be passed to sstable iterators.
type virtualState struct {
	lower            InternalKey
	upper            InternalKey
	fileNum          base.FileNum
	Compare          Compare
	isSharedIngested bool
	prefixChange     *manifest.PrefixReplacement
}

func ceilDiv(a, b uint64) uint64 {
	return (a + b - 1) / b
}

// MakeVirtualReader is used to contruct a reader which can read from virtual
// sstables.
func MakeVirtualReader(reader *Reader, meta manifest.VirtualFileMeta, isShared bool) VirtualReader {
	if reader.fileNum != meta.FileBacking.DiskFileNum {
		panic("pebble: invalid call to MakeVirtualReader")
	}

	vState := virtualState{
		lower:            meta.Smallest,
		upper:            meta.Largest,
		fileNum:          meta.FileNum,
		Compare:          reader.Compare,
		isSharedIngested: isShared && reader.Properties.GlobalSeqNum != 0,
		prefixChange:     meta.PrefixReplacement,
	}
	v := VirtualReader{
		vState: vState,
		reader: reader,
	}

	v.Properties.RawKeySize = ceilDiv(reader.Properties.RawKeySize*meta.Size, meta.FileBacking.Size)
	v.Properties.RawValueSize = ceilDiv(reader.Properties.RawValueSize*meta.Size, meta.FileBacking.Size)
	v.Properties.NumEntries = ceilDiv(reader.Properties.NumEntries*meta.Size, meta.FileBacking.Size)
	v.Properties.NumDeletions = ceilDiv(reader.Properties.NumDeletions*meta.Size, meta.FileBacking.Size)
	v.Properties.NumRangeDeletions = ceilDiv(reader.Properties.NumRangeDeletions*meta.Size, meta.FileBacking.Size)
	v.Properties.NumRangeKeyDels = ceilDiv(reader.Properties.NumRangeKeyDels*meta.Size, meta.FileBacking.Size)

	// Note that we rely on NumRangeKeySets for correctness. If the sstable may
	// contain range keys, then NumRangeKeySets must be > 0. ceilDiv works because
	// meta.Size will not be 0 for virtual sstables.
	v.Properties.NumRangeKeySets = ceilDiv(reader.Properties.NumRangeKeySets*meta.Size, meta.FileBacking.Size)
	v.Properties.ValueBlocksSize = ceilDiv(reader.Properties.ValueBlocksSize*meta.Size, meta.FileBacking.Size)
	v.Properties.NumSizedDeletions = ceilDiv(reader.Properties.NumSizedDeletions*meta.Size, meta.FileBacking.Size)
	v.Properties.RawPointTombstoneKeySize = ceilDiv(reader.Properties.RawPointTombstoneKeySize*meta.Size, meta.FileBacking.Size)
	v.Properties.RawPointTombstoneValueSize = ceilDiv(reader.Properties.RawPointTombstoneValueSize*meta.Size, meta.FileBacking.Size)

	return v
}

// NewCompactionIter is the compaction iterator function for virtual readers.
func (v *VirtualReader) NewCompactionIter(
	bytesIterated *uint64,
	categoryAndQoS CategoryAndQoS,
	statsCollector *CategoryStatsCollector,
	rp ReaderProvider,
	bufferPool *BufferPool,
) (Iterator, error) {
	i, err := v.reader.newCompactionIter(
		bytesIterated, categoryAndQoS, statsCollector, rp, &v.vState, bufferPool)
	if err == nil && v.vState.prefixChange != nil && !v.reader.syntheticPrefix.Implements(v.vState.prefixChange) {
		i = newPrefixReplacingIterator(i, v.vState.prefixChange.ContentPrefix, v.vState.prefixChange.SyntheticPrefix, v.reader.Compare)
	}
	return i, err
}

// NewIterWithBlockPropertyFiltersAndContextEtc wraps
// Reader.NewIterWithBlockPropertyFiltersAndContext. We assume that the passed
// in [lower, upper) bounds will have at least some overlap with the virtual
// sstable bounds. No overlap is not currently supported in the iterator.
func (v *VirtualReader) NewIterWithBlockPropertyFiltersAndContextEtc(
	ctx context.Context,
	lower, upper []byte,
	filterer *BlockPropertiesFilterer,
	hideObsoletePoints, useFilterBlock bool,
	stats *base.InternalIteratorStats,
	categoryAndQoS CategoryAndQoS,
	statsCollector *CategoryStatsCollector,
	rp ReaderProvider,
) (Iterator, error) {
	i, err := v.reader.newIterWithBlockPropertyFiltersAndContext(
		ctx, lower, upper, filterer, hideObsoletePoints, useFilterBlock, stats,
		categoryAndQoS, statsCollector, rp, &v.vState)
	if err == nil && v.vState.prefixChange != nil && !v.reader.syntheticPrefix.Implements(v.vState.prefixChange) {
		i = newPrefixReplacingIterator(i, v.vState.prefixChange.ContentPrefix, v.vState.prefixChange.SyntheticPrefix, v.reader.Compare)
	}
	return i, err
}

// ValidateBlockChecksumsOnBacking will call ValidateBlockChecksumsOnBacking on the underlying reader.
// Note that block checksum validation is NOT restricted to virtual sstable bounds.
func (v *VirtualReader) ValidateBlockChecksumsOnBacking() error {
	return v.reader.ValidateBlockChecksums()
}

// NewRawRangeDelIter wraps Reader.NewRawRangeDelIter.
func (v *VirtualReader) NewRawRangeDelIter() (keyspan.FragmentIterator, error) {
	iter, err := v.reader.NewRawRangeDelIter()
	if err != nil {
		return nil, err
	}
	if iter == nil {
		return nil, nil
	}
	lower := &v.vState.lower
	upper := &v.vState.upper

	if v.vState.prefixChange != nil {
		lower = &InternalKey{UserKey: v.vState.prefixChange.ReplaceArg(lower.UserKey), Trailer: lower.Trailer}
		upper = &InternalKey{UserKey: v.vState.prefixChange.ReplaceArg(upper.UserKey), Trailer: upper.Trailer}

		iter = keyspan.Truncate(
			v.reader.Compare, iter, lower.UserKey, upper.UserKey,
			lower, upper, !v.vState.upper.IsExclusiveSentinel(), /* panicOnUpperTruncate */
		)
		return newPrefixReplacingFragmentIterator(iter, v.vState.prefixChange.ContentPrefix, v.vState.prefixChange.SyntheticPrefix), nil
	}

	// Truncation of spans isn't allowed at a user key that also contains points
	// in the same virtual sstable, as it would lead to covered points getting
	// uncovered. Set panicOnUpperTruncate to true if the file's upper bound
	// is not an exclusive sentinel.
	//
	// As an example, if an sstable contains a rangedel a-c and point keys at
	// a.SET.2 and b.SET.3, the file bounds [a#2,SET-b#RANGEDELSENTINEL] are
	// allowed (as they exclude b.SET.3), or [a#2,SET-c#RANGEDELSENTINEL] (as it
	// includes both point keys), but not [a#2,SET-b#3,SET] (as it would truncate
	// the rangedel at b and lead to the point being uncovered).
	return keyspan.Truncate(
		v.reader.Compare, iter, lower.UserKey, upper.UserKey,
		lower, upper, !v.vState.upper.IsExclusiveSentinel(), /* panicOnUpperTruncate */
	), nil
}

// NewRawRangeKeyIter wraps Reader.NewRawRangeKeyIter.
func (v *VirtualReader) NewRawRangeKeyIter() (keyspan.FragmentIterator, error) {
	iter, err := v.reader.newRawRangeKeyIter(&v.vState)
	if err != nil {
		return nil, err
	}
	if iter == nil {
		return nil, nil
	}
	lower := &v.vState.lower
	upper := &v.vState.upper

	if v.vState.isSharedIngested {
		// We need to coalesce range keys within each sstable, and then apply the
		// global sequence number. For this, we use ForeignSSTTransformer.
		//
		// TODO(bilal): Avoid these allocations by hoisting the transformer and
		// transform iter into VirtualReader.
		transform := &rangekey.ForeignSSTTransformer{
			Equal:  v.reader.Equal,
			SeqNum: v.reader.Properties.GlobalSeqNum,
		}
		transformIter := &keyspan.TransformerIter{
			FragmentIterator: iter,
			Transformer:      transform,
			Compare:          v.reader.Compare,
		}
		iter = transformIter
	}

	if v.vState.prefixChange != nil {
		lower = &InternalKey{UserKey: v.vState.prefixChange.ReplaceArg(lower.UserKey), Trailer: lower.Trailer}
		upper = &InternalKey{UserKey: v.vState.prefixChange.ReplaceArg(upper.UserKey), Trailer: upper.Trailer}
		iter = keyspan.Truncate(
			v.reader.Compare, iter, lower.UserKey, upper.UserKey,
			lower, upper, !v.vState.upper.IsExclusiveSentinel(), /* panicOnUpperTruncate */
		)
		return newPrefixReplacingFragmentIterator(iter, v.vState.prefixChange.ContentPrefix, v.vState.prefixChange.SyntheticPrefix), nil
	}

	// Truncation of spans isn't allowed at a user key that also contains points
	// in the same virtual sstable, as it would lead to covered points getting
	// uncovered. Set panicOnUpperTruncate to true if the file's upper bound
	// is not an exclusive sentinel.
	//
	// As an example, if an sstable contains a range key a-c and point keys at
	// a.SET.2 and b.SET.3, the file bounds [a#2,SET-b#RANGEKEYSENTINEL] are
	// allowed (as they exclude b.SET.3), or [a#2,SET-c#RANGEKEYSENTINEL] (as it
	// includes both point keys), but not [a#2,SET-b#3,SET] (as it would truncate
	// the range key at b and lead to the point being uncovered).
	return keyspan.Truncate(
		v.reader.Compare, iter, lower.UserKey, upper.UserKey,
		lower, upper, !v.vState.upper.IsExclusiveSentinel(), /* panicOnUpperTruncate */
	), nil
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
	if v.prefixChange != nil {
		first = v.prefixChange.ReplaceArg(first)
		last = v.prefixChange.ReplaceArg(last)
	}
	// TODO(bananabrick): What if someone passes in bounds completely outside of
	// virtual sstable bounds?
	return lastKeyInclusive, first, last
}

// EstimateDiskUsage just calls VirtualReader.reader.EstimateDiskUsage after
// enforcing the virtual sstable bounds.
func (v *VirtualReader) EstimateDiskUsage(start, end []byte) (uint64, error) {
	_, f, l := v.vState.constrainBounds(start, end, true /* endInclusive */)
	if v.vState.prefixChange != nil {
		if !bytes.HasPrefix(f, v.vState.prefixChange.SyntheticPrefix) || !bytes.HasPrefix(l, v.vState.prefixChange.SyntheticPrefix) {
			return 0, errInputPrefixMismatch
		}
		// TODO(dt): we could add a scratch buf to VirtualReader to avoid allocs on
		// repeated calls to this.
		f = append(append([]byte{}, v.vState.prefixChange.ContentPrefix...), f[len(v.vState.prefixChange.SyntheticPrefix):]...)
		l = append(append([]byte{}, v.vState.prefixChange.ContentPrefix...), l[len(v.vState.prefixChange.SyntheticPrefix):]...)
	}

	return v.reader.EstimateDiskUsage(f, l)
}

// CommonProperties implements the CommonReader interface.
func (v *VirtualReader) CommonProperties() *CommonProperties {
	return &v.Properties
}
