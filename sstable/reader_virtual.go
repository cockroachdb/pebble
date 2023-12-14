// Copyright 2011 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package sstable

import (
	"bytes"
	"context"
	"encoding/hex"
	"fmt"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/keyspan"
	"github.com/cockroachdb/pebble/internal/manifest"
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
	lower     InternalKey
	upper     InternalKey
	fileNum   base.FileNum
	Compare   Compare
	isForeign bool
}

func ceilDiv(a, b uint64) uint64 {
	return (a + b - 1) / b
}

// MakeVirtualReader is used to contruct a reader which can read from virtual
// sstables.
func MakeVirtualReader(
	reader *Reader, meta manifest.VirtualFileMeta, isForeign bool,
) VirtualReader {
	if reader.fileNum != meta.FileBacking.DiskFileNum {
		panic("pebble: invalid call to MakeVirtualReader")
	}

	vState := virtualState{
		lower:     meta.Smallest,
		upper:     meta.Largest,
		fileNum:   meta.FileNum,
		Compare:   reader.Compare,
		isForeign: isForeign,
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
	return v.reader.newCompactionIter(
		bytesIterated, categoryAndQoS, statsCollector, rp, &v.vState, bufferPool)
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
	return v.reader.newIterWithBlockPropertyFiltersAndContext(
		ctx, lower, upper, filterer, hideObsoletePoints, useFilterBlock, stats,
		categoryAndQoS, statsCollector, rp, &v.vState)
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
		v.reader.Compare, iter, v.vState.lower.UserKey, v.vState.upper.UserKey,
		&v.vState.lower, &v.vState.upper, !v.vState.upper.IsExclusiveSentinel(), /* panicOnUpperTruncate */
	), nil
}

// NewRawRangeKeyIter wraps Reader.NewRawRangeKeyIter.
func (v *VirtualReader) NewRawRangeKeyIter() (keyspan.FragmentIterator, error) {
	iter, err := v.reader.NewRawRangeKeyIter()
	if err != nil {
		return nil, err
	}
	if iter == nil {
		return nil, nil
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
		v.reader.Compare, iter, v.vState.lower.UserKey, v.vState.upper.UserKey,
		&v.vState.lower, &v.vState.upper, !v.vState.upper.IsExclusiveSentinel(), /* panicOnUpperTruncate */
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

type prefixReplacingIterator struct {
	i         Iterator
	cmp       base.Compare
	src, dst  []byte
	arg, arg2 []byte
	res       InternalKey
	err       error
}

var errInputPrefixMismatch = errors.New("key argument does not have prefix required for replacement")
var errOutputPrefixMismatch = errors.New("key returned does not have prefix required for replacement")

var _ Iterator = (*prefixReplacingIterator)(nil)

// NewPrefixReplacingIterator wraps an iterator over keys that have prefix `src`
// in an iterator that will make them appear to have prefix `dst`. Every key
// passed as an argument to methods on this iterator must have prefix `dst`, and
// every key produced by the underlying iterator must have prefix `src`.
func NewPrefixReplacingIterator(i Iterator, src, dst []byte, cmp base.Compare) Iterator {
	return &prefixReplacingIterator{
		i:   i,
		cmp: cmp,
		src: src, dst: dst,
		arg: append([]byte{}, src...), arg2: append([]byte{}, src...),
		res: InternalKey{UserKey: append([]byte{}, dst...)},
	}
}

func (p *prefixReplacingIterator) SetContext(ctx context.Context) {
	p.i.SetContext(ctx)
}

func (p *prefixReplacingIterator) rewriteArg(key []byte) []byte {
	if !bytes.HasPrefix(key, p.dst) {
		p.err = errInputPrefixMismatch
		return key
	}
	p.arg = append(p.arg[:len(p.src)], key[len(p.dst):]...)
	return p.arg
}

func (p *prefixReplacingIterator) rewriteArg2(key []byte) []byte {
	if !bytes.HasPrefix(key, p.dst) {
		p.err = errInputPrefixMismatch
		return key
	}
	p.arg2 = append(p.arg2[:len(p.src)], key[len(p.dst):]...)
	return p.arg2
}

func (p *prefixReplacingIterator) rewriteResult(
	k *InternalKey, v base.LazyValue,
) (*InternalKey, base.LazyValue) {
	if k == nil {
		return k, v
	}
	if !bytes.HasPrefix(k.UserKey, p.src) {
		panic(errOutputPrefixMismatch)
	}
	p.res.Trailer = k.Trailer
	p.res.UserKey = append(p.res.UserKey[:len(p.dst)], k.UserKey[len(p.src):]...)
	return &p.res, v
}

// SeekGE implements the Iterator interface.
func (p *prefixReplacingIterator) SeekGE(
	key []byte, flags base.SeekGEFlags,
) (*InternalKey, base.LazyValue) {
	return p.rewriteResult(p.i.SeekGE(p.rewriteArg(key), flags))
}

// SeekPrefixGE implements the Iterator interface.
func (p *prefixReplacingIterator) SeekPrefixGE(
	prefix, key []byte, flags base.SeekGEFlags,
) (*InternalKey, base.LazyValue) {
	return p.rewriteResult(p.i.SeekPrefixGE(p.rewriteArg2(prefix), p.rewriteArg(key), flags))
}

// SeekLT implements the Iterator interface.
func (p *prefixReplacingIterator) SeekLT(
	key []byte, flags base.SeekLTFlags,
) (*InternalKey, base.LazyValue) {
	cmp := p.cmp(key, p.dst)
	if cmp < 0 {
		// Exhaust the iterator by Prev()ing before the First key.
		p.i.First()
		return p.rewriteResult(p.i.Prev())
	}
	return p.rewriteResult(p.i.SeekLT(p.rewriteArg(key), flags))
}

// First implements the Iterator interface.
func (p *prefixReplacingIterator) First() (*InternalKey, base.LazyValue) {
	return p.rewriteResult(p.i.First())
}

// Last implements the Iterator interface.
func (p *prefixReplacingIterator) Last() (*InternalKey, base.LazyValue) {
	return p.rewriteResult(p.i.Last())
}

// Next implements the Iterator interface.
func (p *prefixReplacingIterator) Next() (*InternalKey, base.LazyValue) {
	return p.rewriteResult(p.i.Next())
}

// NextPrefix implements the Iterator interface.
func (p *prefixReplacingIterator) NextPrefix(succKey []byte) (*InternalKey, base.LazyValue) {
	return p.rewriteResult(p.i.NextPrefix(p.rewriteArg(succKey)))
}

// Prev implements the Iterator interface.
func (p *prefixReplacingIterator) Prev() (*InternalKey, base.LazyValue) {
	return p.rewriteResult(p.i.Prev())
}

// Error implements the Iterator interface.
func (p *prefixReplacingIterator) Error() error {
	if p.err != nil {
		return p.err
	}
	return p.i.Error()
}

// Close implements the Iterator interface.
func (p *prefixReplacingIterator) Close() error {
	return p.i.Close()
}

// SetBounds implements the Iterator interface.
func (p *prefixReplacingIterator) SetBounds(lower, upper []byte) {
	//
	if x, ok := p.i.(interface{ SetBoundsMaterialized() bool }); ok && x.SetBoundsMaterialized() {
		p.i.SetBounds(lower, upper)
		return
	}
	p.i.SetBounds(p.rewriteArg(lower), p.rewriteArg2(upper))
}

func (p *prefixReplacingIterator) MaybeFilteredKeys() bool {
	return p.i.MaybeFilteredKeys()
}

// String implements the Iterator interface.
func (p *prefixReplacingIterator) String() string {
	return fmt.Sprintf("%s [%s->%s]", p.i.String(), hex.EncodeToString(p.src), hex.EncodeToString(p.dst))
}

func (p *prefixReplacingIterator) SetCloseHook(fn func(i Iterator) error) {
	p.i.SetCloseHook(fn)
}

type prefixReplacingFragmentIterator struct {
	i          keyspan.FragmentIterator
	err        error
	src, dst   []byte
	arg        []byte
	out1, out2 []byte
}

// NewPrefixReplacingFragmentIterator wraps a FragmentIterator over some reader
// that contains range keys in some key span to make those range keys appear to
// be remapped into some other key-span.
func NewPrefixReplacingFragmentIterator(
	i keyspan.FragmentIterator, src, dst []byte,
) keyspan.FragmentIterator {
	return &prefixReplacingFragmentIterator{
		i:   i,
		src: src, dst: dst,
		arg:  append([]byte{}, src...),
		out1: append([]byte(nil), dst...),
		out2: append([]byte(nil), dst...),
	}
}

func (p *prefixReplacingFragmentIterator) rewriteArg(key []byte) []byte {
	if !bytes.HasPrefix(key, p.dst) {
		p.err = errInputPrefixMismatch
		return key
	}
	p.arg = append(p.arg[:len(p.src)], key[len(p.dst):]...)
	return p.arg
}

func (p *prefixReplacingFragmentIterator) rewriteSpan(sp *keyspan.Span) *keyspan.Span {
	if !bytes.HasPrefix(sp.Start, p.src) || !bytes.HasPrefix(sp.End, p.src) {
		p.err = errInputPrefixMismatch
		return sp
	}
	sp.Start = append(p.out1[:len(p.dst)], sp.Start[len(p.src):]...)
	sp.End = append(p.out2[:len(p.dst)], sp.End[len(p.src):]...)

	// TODO(dt): RESOLVE DURING CODE REVIEW
	// do I need to touch sp.Keys? is sp.Start/End actually assured to both have dst prefix?
	return sp
}

// SeekGE implements the FragmentIterator interface.
func (p *prefixReplacingFragmentIterator) SeekGE(key []byte) *keyspan.Span {
	return p.rewriteSpan(p.i.SeekGE(p.rewriteArg(key)))
}

// SeekLT implements the FragmentIterator interface.
func (p *prefixReplacingFragmentIterator) SeekLT(key []byte) *keyspan.Span {
	return p.rewriteSpan(p.i.SeekLT(p.rewriteArg(key)))
}

// First implements the FragmentIterator interface.
func (p *prefixReplacingFragmentIterator) First() *keyspan.Span {
	return p.rewriteSpan(p.i.First())
}

// Last implements the FragmentIterator interface.
func (p *prefixReplacingFragmentIterator) Last() *keyspan.Span {
	return p.rewriteSpan(p.i.Last())
}

// Close implements the FragmentIterator interface.
func (p *prefixReplacingFragmentIterator) Next() *keyspan.Span {
	return p.rewriteSpan(p.i.Next())
}

// Prev implements the FragmentIterator interface.
func (p *prefixReplacingFragmentIterator) Prev() *keyspan.Span {
	return p.rewriteSpan(p.i.Prev())
}

// Error implements the FragmentIterator interface.
func (p *prefixReplacingFragmentIterator) Error() error {
	if p.err != nil {
		return p.err
	}
	return p.i.Error()
}

// Close implements the FragmentIterator interface.
func (p *prefixReplacingFragmentIterator) Close() error {
	return p.i.Close()
}
