// Copyright 2024 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package sstable

import (
	"bytes"
	"context"
	"encoding/binary"
	"math"
	"slices"
	"sync"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/bytealloc"
	"github.com/cockroachdb/pebble/internal/invariants"
	"github.com/cockroachdb/pebble/internal/keyspan"
	"github.com/cockroachdb/pebble/objstorage"
	"github.com/cockroachdb/pebble/sstable/blob"
	"github.com/cockroachdb/pebble/sstable/block"
	"github.com/cockroachdb/pebble/sstable/block/blockkind"
	"github.com/cockroachdb/pebble/sstable/colblk"
	"github.com/cockroachdb/pebble/sstable/rowblk"
	"github.com/cockroachdb/pebble/sstable/valblk"
)

// RawColumnWriter is a sstable RawWriter that writes sstables with
// column-oriented blocks. All table formats TableFormatPebblev5 and later write
// column-oriented blocks and use RawColumnWriter.
type RawColumnWriter struct {
	comparer *base.Comparer
	meta     WriterMetadata
	opts     WriterOptions
	err      error

	dataFlush           block.FlushGovernor
	indexFlush          block.FlushGovernor
	blockPropCollectors []BlockPropertyCollector
	blockPropsEncoder   blockPropertiesEncoder
	obsoleteCollector   obsoleteKeyBlockPropertyCollector
	props               Properties
	// block writers buffering unflushed data.
	dataBlock struct {
		colblk.DataBlockEncoder
		// numDeletions stores the count of point tombstones in this data block.
		// It's used to determine if this data block is considered
		// tombstone-dense for the purposes of compaction.
		numDeletions int
		// deletionSize stores the raw size of point tombstones in this data
		// block. It's used to determine if this data block is considered
		// tombstone-dense for the purposes of compaction.
		deletionSize int
	}
	indexBlock                colblk.IndexBlockWriter
	topLevelIndexBlock        colblk.IndexBlockWriter
	rangeDelBlock             colblk.KeyspanBlockWriter
	rangeKeyBlock             colblk.KeyspanBlockWriter
	valueBlock                *valblk.Writer // nil iff WriterOptions.DisableValueBlocks=true
	blobRefLivenessIndexBlock blobRefValueLivenessWriter
	// filterWriter accumulates the filter block. If not nil, the filter writer
	// ingests the prefix of each key.
	filterWriter base.TableFilterWriter
	prevPointKey struct {
		trailer    base.InternalKeyTrailer
		isObsolete bool
	}
	pendingDataBlockSize int
	indexBlockSize       int
	queuedDataSize       uint64

	// indexBuffering holds finished index blocks as they're completed while
	// building the sstable. If an index block grows sufficiently large
	// (IndexBlockSize) while an sstable is still being constructed, the sstable
	// writer will create a two-level index structure. As index blocks are
	// completed, they're finished and buffered in-memory until the table is
	// finished. When the table is finished, the buffered index blocks are
	// flushed in order after all the data blocks, and the top-level index block
	// is constructed to point to all the individual index blocks.
	indexBuffering struct {
		// partitions holds all the completed, uncompressed index blocks.
		//
		// TODO(jackson): We should consider compressing these index blocks now,
		// while buffering, to reduce the memory usage of the writer.
		partitions []bufferedIndexBlock
		// partitionSizeSum is the sum of the sizes of all the completed index
		// blocks (in `partitions`).
		partitionSizeSum uint64
		// blockAlloc is used to bulk-allocate byte slices used to store index
		// blocks in partitions. These live until the sstable is finished.
		blockAlloc []byte
		// sepAlloc is used to bulk-allocate index block separator slices stored
		// in partitions. These live until the sstable is finished.
		sepAlloc bytealloc.A
	}

	writeQueue struct {
		wg  sync.WaitGroup
		ch  chan block.OwnedPhysicalBlock
		err error
	}
	layout layoutWriter

	lastKeyBuf            []byte
	separatorBuf          []byte
	tmp                   [blockHandleLikelyMaxLen]byte
	previousUserKey       invariants.Value[[]byte]
	validator             invariants.Value[*colblk.DataBlockValidator]
	disableKeyOrderChecks bool
	cpuMeasurer           base.CPUMeasurer
}

// Assert that *RawColumnWriter implements RawWriter.
var _ RawWriter = (*RawColumnWriter)(nil)

// cpuMeasurer, if non-nil, is only used for calling
// cpuMeasurer.MeasureCPUSSTableSecondary.
func newColumnarWriter(
	writable objstorage.Writable, o WriterOptions, cpuMeasurer base.CPUMeasurer,
) *RawColumnWriter {
	if writable == nil {
		panic("pebble: nil writable")
	}
	if !o.TableFormat.BlockColumnar() {
		panic(errors.AssertionFailedf("newColumnarWriter cannot create sstables with %s format", o.TableFormat))
	}
	o = o.ensureDefaults()
	w := &RawColumnWriter{
		comparer: o.Comparer,
		meta: WriterMetadata{
			SmallestSeqNum: math.MaxUint64,
		},
		opts:                  o,
		disableKeyOrderChecks: o.internal.DisableKeyOrderChecks,
	}
	w.layout.Init(writable, o)
	w.dataFlush = block.MakeFlushGovernor(o.BlockSize, o.BlockSizeThreshold, o.SizeClassAwareThreshold, o.AllocatorSizeClasses)
	w.indexFlush = block.MakeFlushGovernor(o.IndexBlockSize, o.BlockSizeThreshold, o.SizeClassAwareThreshold, o.AllocatorSizeClasses)
	w.dataBlock.Init(o.KeySchema, o.TableFormat.TieringColumnConfig())
	w.indexBlock.Init()
	w.topLevelIndexBlock.Init()
	w.rangeDelBlock.Init(w.comparer.Equal)
	w.rangeKeyBlock.Init(w.comparer.Equal)
	if !o.DisableValueBlocks {
		flushGovernor := block.MakeFlushGovernor(o.BlockSize, o.BlockSizeThreshold, o.SizeClassAwareThreshold, o.AllocatorSizeClasses)
		// We use the value block writer in the same goroutine so it's safe to share
		// the physBlockMaker.
		w.valueBlock = valblk.NewWriter(flushGovernor, &w.layout.physBlockMaker, func(compressedSize int) {})
	}
	if o.FilterPolicy != base.NoFilterPolicy {
		w.filterWriter = o.FilterPolicy.NewWriter()
	}

	numBlockPropertyCollectors := len(o.BlockPropertyCollectors)
	if !o.disableObsoleteCollector {
		numBlockPropertyCollectors++
	}
	if numBlockPropertyCollectors > maxPropertyCollectors {
		panic(errors.New("pebble: too many block property collectors"))
	}
	w.blockPropCollectors = make([]BlockPropertyCollector, 0, numBlockPropertyCollectors)
	for _, constructFn := range o.BlockPropertyCollectors {
		w.blockPropCollectors = append(w.blockPropCollectors, constructFn())
	}
	if !o.disableObsoleteCollector {
		w.blockPropCollectors = append(w.blockPropCollectors, &w.obsoleteCollector)
	}
	var buf bytes.Buffer
	buf.WriteString("[")
	for i := range w.blockPropCollectors {
		if i > 0 {
			buf.WriteString(",")
		}
		buf.WriteString(w.blockPropCollectors[i].Name())
	}
	buf.WriteString("]")
	w.props.PropertyCollectorNames = buf.String()

	w.props.ComparerName = o.Comparer.Name
	w.props.CompressionName = o.Compression.Name
	w.props.KeySchemaName = o.KeySchema.Name
	w.props.MergerName = o.MergerName

	w.writeQueue.ch = make(chan block.OwnedPhysicalBlock)
	w.cpuMeasurer = cpuMeasurer
	w.writeQueue.wg.Go(w.drainWriteQueue)

	return w
}

// Error returns the current accumulated error if any.
func (w *RawColumnWriter) Error() error {
	return w.err
}

// EstimatedSize returns the estimated size of the sstable being written if
// a call to Close() was made without adding additional keys.
func (w *RawColumnWriter) EstimatedSize() uint64 {
	// Start with the size of the footer, which is fixed, and the size of all
	// the finished data blocks. The size of the finished data blocks is all
	// post-compression and the footer is not compressed, so this initial
	// quantity is exact.
	sz := uint64(w.opts.TableFormat.FooterSize()) + w.queuedDataSize

	// Add the size of value blocks. If any value blocks have already been
	// finished, these blocks will contribute post-compression size. If there is
	// currently an unfinished value block, it will contribute its pre-compression
	// size.
	if w.valueBlock != nil {
		sz += w.valueBlock.Size()
	}

	// Add the size of the completed but unflushed index partitions, the
	// unfinished data block, the unfinished index block, the unfinished range
	// deletion block, and the unfinished range key block.
	//
	// All of these sizes are uncompressed sizes. It's okay to be pessimistic
	// here and use the uncompressed size because all this memory is buffered
	// until the sstable is finished.  Including the uncompressed size bounds
	// the memory usage used by the writer to the physical size limit.
	sz += w.indexBuffering.partitionSizeSum
	sz += uint64(w.dataBlock.Size())
	sz += uint64(w.indexBlockSize)
	if w.rangeDelBlock.KeyCount() > 0 {
		sz += uint64(w.rangeDelBlock.Size())
	}
	if w.rangeKeyBlock.KeyCount() > 0 {
		sz += uint64(w.rangeKeyBlock.Size())
	}

	// TODO(jackson): Include an estimate of the properties, filter and meta
	// index blocks sizes.
	return sz
}

// ComparePrev compares the provided user to the last point key written to the
// writer. The returned value is equivalent to Compare(key, prevKey) where
// prevKey is the last point key written to the writer.
//
// If no key has been written yet, ComparePrev returns +1.
//
// Must not be called after Writer is closed.
func (w *RawColumnWriter) ComparePrev(k []byte) int {
	if w == nil || w.dataBlock.Rows() == 0 {
		return +1
	}
	return int(w.dataBlock.KeyWriter.ComparePrev(k).UserKeyComparison)
}

// IsLikelyMVCCGarbage implements the RawWriter interface.
//
// If no key has been written yet, IsLikelyMVCCGarbage returns false.
func (w *RawColumnWriter) IsLikelyMVCCGarbage(k []byte, keyKind base.InternalKeyKind) bool {
	if w == nil || w.dataBlock.Rows() == 0 {
		return false
	}
	return w.prevPointKey.trailer.Kind().IsSet() &&
		keyKind.IsSet() &&
		w.dataBlock.KeyWriter.ComparePrev(k).PrefixEqual()
}

// SetSnapshotPinnedProperties sets the properties for pinned keys. Should only
// be used internally by Pebble.
func (w *RawColumnWriter) SetSnapshotPinnedProperties(
	pinnedKeyCount, pinnedKeySize, pinnedValueSize uint64,
) {
	w.props.SnapshotPinnedKeys = pinnedKeyCount
	w.props.SnapshotPinnedKeySize = pinnedKeySize
	w.props.SnapshotPinnedValueSize = pinnedValueSize
}

// Metadata returns the metadata for the finished sstable. Only valid to call
// after the sstable has been finished.
func (w *RawColumnWriter) Metadata() (*WriterMetadata, error) {
	if !w.layout.IsFinished() {
		return nil, errors.New("pebble: writer is not closed")
	}
	return &w.meta, nil
}

// EncodeSpan encodes the keys in the given span. The span can contain either
// only RANGEDEL keys or only range keys.
func (w *RawColumnWriter) EncodeSpan(span keyspan.Span) error {
	if span.Empty() {
		return nil
	}
	for _, k := range span.Keys {
		w.meta.updateSeqNum(k.SeqNum())
	}

	blockWriter := &w.rangeKeyBlock
	if span.Keys[0].Kind() == base.InternalKeyKindRangeDelete {
		blockWriter = &w.rangeDelBlock
		// Update range delete properties.
		// NB: These properties are computed differently than the rowblk sstable
		// writer because this writer does not flatten them into row key-value
		// pairs.
		w.props.RawKeySize += uint64(len(span.Start) + len(span.End))
		count := uint64(len(span.Keys))
		w.props.NumEntries += count
		w.props.NumDeletions += count
		w.props.NumRangeDeletions += count
	} else {
		// Update range key properties.
		// NB: These properties are computed differently than the rowblk sstable
		// writer because this writer does not flatten them into row key-value
		// pairs.
		w.props.RawRangeKeyKeySize += uint64(len(span.Start) + len(span.End))
		for _, k := range span.Keys {
			w.props.RawRangeKeyValueSize += uint64(len(k.Value))
			switch k.Kind() {
			case base.InternalKeyKindRangeKeyDelete:
				w.props.NumRangeKeyDels++
			case base.InternalKeyKindRangeKeySet:
				w.props.NumRangeKeySets++
			case base.InternalKeyKindRangeKeyUnset:
				w.props.NumRangeKeyUnsets++
			default:
				panic(errors.Errorf("pebble: invalid range key type: %s", k.Kind()))
			}
		}
		for i := range w.blockPropCollectors {
			if err := w.blockPropCollectors[i].AddRangeKeys(span); err != nil {
				return err
			}
		}
	}
	if !w.disableKeyOrderChecks && blockWriter.KeyCount() > 0 {
		// Check that spans are being added in fragmented order. If the two
		// tombstones overlap, their start and end keys must be identical.
		prevStart, prevEnd, prevTrailer := blockWriter.UnsafeLastSpan()
		if w.opts.Comparer.Equal(prevStart, span.Start) && w.opts.Comparer.Equal(prevEnd, span.End) {
			if prevTrailer < span.Keys[0].Trailer {
				w.err = errors.Errorf("pebble: keys must be added in order: %s-%s:{(#%s)}, %s",
					w.opts.Comparer.FormatKey(prevStart),
					w.opts.Comparer.FormatKey(prevEnd),
					prevTrailer, span.Pretty(w.opts.Comparer.FormatKey))
			}
		} else if c := w.opts.Comparer.Compare(prevEnd, span.Start); c > 0 {
			w.err = errors.Errorf("pebble: keys must be added in order: %s-%s:{(#%s)}, %s",
				w.opts.Comparer.FormatKey(prevStart),
				w.opts.Comparer.FormatKey(prevEnd),
				prevTrailer, span.Pretty(w.opts.Comparer.FormatKey))
			return w.err
		}
	}
	blockWriter.AddSpan(span)
	return nil
}

// Add adds a point key/value pair when writing a
// strict-obsolete sstable. For a given Writer, the keys passed to Add must be
// in increasing order. Span keys (range deletions, range keys) must be added
// through EncodeSpan.
//
// forceObsolete indicates whether the caller has determined that this key is
// obsolete even though it may be the latest point key for this userkey. This
// should be set to true for keys obsoleted by RANGEDELs, and is required for
// strict-obsolete sstables.
//
// Note that there are two properties, S1 and S2 (see comment in format.go)
// that strict-obsolete ssts must satisfy. S2, due to RANGEDELs, is solely the
// responsibility of the caller. S1 is solely the responsibility of the
// callee.
func (w *RawColumnWriter) Add(
	key InternalKey, value []byte, forceObsolete bool, meta base.KVMeta,
) error {
	switch key.Kind() {
	case base.InternalKeyKindRangeDelete, base.InternalKeyKindRangeKeySet,
		base.InternalKeyKindRangeKeyUnset, base.InternalKeyKindRangeKeyDelete:
		return errors.Newf("%s must be added through EncodeSpan", key.Kind())
	case base.InternalKeyKindMerge:
		if w.opts.IsStrictObsolete {
			return errors.Errorf("MERGE not supported in a strict-obsolete sstable")
		}
	}

	eval, err := w.evaluatePoint(key, len(value))
	if err != nil {
		return err
	}
	eval.isObsolete = eval.isObsolete || forceObsolete
	w.prevPointKey.trailer = key.Trailer
	w.prevPointKey.isObsolete = eval.isObsolete

	var valuePrefix block.ValuePrefix
	var valueStoredWithKey []byte
	if eval.writeToValueBlock {
		vh, err := w.valueBlock.AddValue(value)
		if err != nil {
			return err
		}
		n := valblk.EncodeHandle(w.tmp[:], vh)
		valueStoredWithKey = w.tmp[:n]
		var attribute base.ShortAttribute
		if w.opts.ShortAttributeExtractor != nil {
			// TODO(sumeer): for compactions, it is possible that the input sstable
			// already has this value in the value section and so we have already
			// extracted the ShortAttribute. Avoid extracting it again. This will
			// require changing the RawWriter.Add interface.
			if attribute, err = w.opts.ShortAttributeExtractor(
				key.UserKey, int(eval.kcmp.PrefixLen), value); err != nil {
				return err
			}
		}
		valuePrefix = block.ValueBlockHandlePrefix(eval.kcmp.PrefixEqual(), attribute)
	} else {
		valueStoredWithKey = value
		if len(value) > 0 {
			valuePrefix = block.InPlaceValuePrefix(eval.kcmp.PrefixEqual())
		}
	}
	return w.add(key, len(value), valueStoredWithKey, valuePrefix, eval, meta)
}

// AddWithBlobHandle implements the RawWriter interface.
func (w *RawColumnWriter) AddWithBlobHandle(
	key InternalKey,
	h blob.InlineHandle,
	attr base.ShortAttribute,
	forceObsolete bool,
	meta base.KVMeta,
) error {
	// Blob value handles require at least TableFormatPebblev6.
	if w.opts.TableFormat <= TableFormatPebblev5 {
		w.err = errors.Newf("pebble: blob value handles are not supported in %s", w.opts.TableFormat.String())
		return w.err
	}
	switch key.Kind() {
	case base.InternalKeyKindRangeDelete, base.InternalKeyKindRangeKeySet,
		base.InternalKeyKindRangeKeyUnset, base.InternalKeyKindRangeKeyDelete:
		return errors.Newf("%s must be added through EncodeSpan", key.Kind())
	case base.InternalKeyKindMerge:
		return errors.Errorf("MERGE does not support blob value handles")
	}

	eval, err := w.evaluatePoint(key, int(h.ValueLen))
	if err != nil {
		return err
	}
	eval.isObsolete = eval.isObsolete || forceObsolete
	w.prevPointKey.trailer = key.Trailer
	w.prevPointKey.isObsolete = eval.isObsolete

	n := h.Encode(w.tmp[:])
	valueStoredWithKey := w.tmp[:n]
	valuePrefix := block.BlobValueHandlePrefix(eval.kcmp.PrefixEqual(), attr)
	err = w.add(key, int(h.ValueLen), valueStoredWithKey, valuePrefix, eval, meta)
	if err != nil {
		return err
	}
	w.props.NumValuesInBlobFiles++
	if err := w.blobRefLivenessIndexBlock.addLiveValue(h.ReferenceID, h.BlockID, h.ValueID, uint64(h.ValueLen)); err != nil {
		return err
	}
	return nil
}

func (w *RawColumnWriter) add(
	key InternalKey,
	valueLen int,
	valueStoredWithKey []byte,
	valuePrefix block.ValuePrefix,
	eval pointKeyEvaluation,
	meta base.KVMeta,
) error {
	// Append the key to the data block. We have NOT yet committed to
	// including the key in the block. The data block writer permits us to
	// finish the block excluding the last-appended KV.
	entriesWithoutKV := w.dataBlock.Rows()
	w.dataBlock.Add(key, valueStoredWithKey, valuePrefix, eval.kcmp, eval.isObsolete, meta)

	// Now that we've appended the KV pair, we can compute the exact size of the
	// block with this key-value pair included. Check to see if we should flush
	// the current block, either with or without the added key-value pair.
	size := w.dataBlock.Size()
	if shouldFlushWithoutLatestKV(size, w.pendingDataBlockSize, entriesWithoutKV, &w.dataFlush) {
		// Flush the data block excluding the key we just added.
		if err := w.flushDataBlockWithoutNextKey(key.UserKey); err != nil {
			w.err = err
			return err
		}
		// flushDataBlockWithoutNextKey reset the data block builder, and we can
		// add the key to this next block now.
		w.dataBlock.Add(key, valueStoredWithKey, valuePrefix, eval.kcmp, eval.isObsolete, meta)
		w.pendingDataBlockSize = w.dataBlock.Size()
	} else {
		// We're not flushing the data block, and we're committing to including
		// the current KV in the block. Remember the new size of the data block
		// with the current KV.
		w.pendingDataBlockSize = size
	}

	for i := range w.blockPropCollectors {
		v := valueStoredWithKey
		if key.Kind() == base.InternalKeyKindSet || key.Kind() == base.InternalKeyKindSetWithDelete || !valuePrefix.IsInPlaceValue() {
			// Values for SET, SETWITHDEL keys are not required to be in-place,
			// and may not even be read by the compaction, so pass nil values.
			// Block property collectors in such Pebble DB's must not look at
			// the value.
			v = nil
		}
		if err := w.blockPropCollectors[i].AddPointKey(key, v); err != nil {
			w.err = err
			return err
		}
	}
	w.obsoleteCollector.AddPoint(eval.isObsolete)
	if w.filterWriter != nil {
		w.filterWriter.AddKey(key.UserKey[:eval.kcmp.PrefixLen])
	}
	w.meta.updateSeqNum(key.SeqNum())
	if !w.meta.HasPointKeys {
		w.meta.SetSmallestPointKey(key.Clone())
	}

	w.props.NumEntries++
	switch key.Kind() {
	case InternalKeyKindDelete, InternalKeyKindSingleDelete:
		w.props.NumDeletions++
		w.props.RawPointTombstoneKeySize += uint64(len(key.UserKey))
		w.dataBlock.numDeletions++
		w.dataBlock.deletionSize += len(key.UserKey)
	case InternalKeyKindDeleteSized:
		var size uint64
		if len(valueStoredWithKey) > 0 {
			var n int
			size, n = binary.Uvarint(valueStoredWithKey)
			if n <= 0 {
				return errors.Newf("%s key's value (%x) does not parse as uvarint",
					errors.Safe(key.Kind().String()), valueStoredWithKey)
			}
		}
		w.props.NumDeletions++
		w.props.NumSizedDeletions++
		w.props.RawPointTombstoneKeySize += uint64(len(key.UserKey))
		w.props.RawPointTombstoneValueSize += size
		w.dataBlock.numDeletions++
		w.dataBlock.deletionSize += len(key.UserKey)
	case InternalKeyKindMerge:
		w.props.NumMergeOperands++
	}
	w.props.RawKeySize += uint64(key.Size())
	w.props.RawValueSize += uint64(valueLen)
	return nil
}

type pointKeyEvaluation struct {
	kcmp              colblk.KeyComparison
	isObsolete        bool
	writeToValueBlock bool
}

// evaluatePoint takes information about a point key being written to the
// sstable and decides how the point should be represented, where its value
// should be stored, etc.
func (w *RawColumnWriter) evaluatePoint(
	key base.InternalKey, valueLen int,
) (eval pointKeyEvaluation, err error) {
	eval.kcmp = w.dataBlock.KeyWriter.ComparePrev(key.UserKey)

	// When invariants are enabled, validate kcmp.
	if invariants.Enabled {
		colblk.AssertKeyCompare(w.comparer, key.UserKey, w.previousUserKey.Get(), eval.kcmp)
		w.previousUserKey.Set(append(w.previousUserKey.Get()[:0], key.UserKey...))
	}

	if !w.meta.HasPointKeys {
		return eval, nil
	}
	keyKind := key.Kind()
	// Ensure that no one adds a point key kind without considering the obsolete
	// handling for that kind.
	switch keyKind {
	case InternalKeyKindSet, InternalKeyKindSetWithDelete, InternalKeyKindMerge,
		InternalKeyKindDelete, InternalKeyKindSingleDelete, InternalKeyKindDeleteSized:
	default:
		panic(errors.AssertionFailedf("unexpected key kind %s", keyKind.String()))
	}
	prevKeyKind := w.prevPointKey.trailer.Kind()
	// If same user key, then the current key is obsolete if any of the
	// following is true:
	// C1 The prev key was obsolete.
	// C2 The prev key was not a MERGE. When the previous key is a MERGE we must
	//    preserve SET* and MERGE since their values will be merged into the
	//    previous key. We also must preserve DEL* since there may be an older
	//    SET*/MERGE in a lower level that must not be merged with the MERGE --
	//    if we omit the DEL* that lower SET*/MERGE will become visible.
	//
	// Regardless of whether it is the same user key or not
	// C3 The current key is some kind of point delete, and we are writing to
	//    the lowest level, then it is also obsolete. The correctness of this
	//    relies on the same user key not spanning multiple sstables in a level.
	//
	// C1 ensures that for a user key there is at most one transition from
	// !obsolete to obsolete. Consider a user key k, for which the first n keys
	// are not obsolete. We consider the various value of n:
	//
	// n = 0: This happens due to forceObsolete being set by the caller, or due
	// to C3. forceObsolete must only be set due a RANGEDEL, and that RANGEDEL
	// must also delete all the lower seqnums for the same user key. C3 triggers
	// due to a point delete and that deletes all the lower seqnums for the same
	// user key.
	//
	// n = 1: This is the common case. It happens when the first key is not a
	// MERGE, or the current key is some kind of point delete.
	//
	// n > 1: This is due to a sequence of MERGE keys, potentially followed by a
	// single non-MERGE key.
	isObsoleteC1AndC2 := eval.kcmp.UserKeyComparison == 0 &&
		(w.prevPointKey.isObsolete || prevKeyKind != InternalKeyKindMerge)
	isObsoleteC3 := w.opts.WritingToLowestLevel &&
		(keyKind == InternalKeyKindDelete || keyKind == InternalKeyKindSingleDelete ||
			keyKind == InternalKeyKindDeleteSized)
	eval.isObsolete = isObsoleteC1AndC2 || isObsoleteC3
	// TODO(sumeer): storing isObsolete SET and SETWITHDEL in value blocks is
	// possible, but requires some care in documenting and checking invariants.
	// There is code that assumes nothing in value blocks because of single MVCC
	// version (those should be ok). We have to ensure setHasSamePrefix is
	// correctly initialized here etc.

	if !w.disableKeyOrderChecks && (eval.kcmp.UserKeyComparison < 0 ||
		(eval.kcmp.UserKeyComparison == 0 && w.prevPointKey.trailer <= key.Trailer)) {
		previousKey := base.InternalKey{
			UserKey: w.dataBlock.MaterializeLastUserKey(nil),
			Trailer: w.prevPointKey.trailer,
		}
		return eval, errors.Errorf(
			"pebble: keys must be added in strictly increasing order: %s, %s",
			previousKey.Pretty(w.comparer.FormatKey),
			key.Pretty(w.comparer.FormatKey))
	}

	// We might want to write this key's value to a value block if it has the
	// same prefix.
	//
	// We require:
	//  . Value blocks to be enabled.
	//	. The value to be sufficiently large. (Currently we simply require a
	//	  non-zero length, so all non-empty values are eligible for storage
	//	  out-of-band in a value block.)
	//  . IsLikelyMVCCGarbage to be true; see comment for MVCC garbage criteria.
	//
	// Use of 0 here is somewhat arbitrary. Given the minimum 3 byte encoding of
	// valueHandle, this should be > 3. But tiny values are common in test and
	// unlikely in production, so we use 0 here for better test coverage.
	useValueBlock := !w.opts.DisableValueBlocks &&
		w.valueBlock != nil &&
		valueLen > 0 &&
		w.IsLikelyMVCCGarbage(key.UserKey, key.Kind())
	if !useValueBlock {
		return eval, nil
	}
	// NB: it is possible that eval.kcmp.UserKeyComparison == 0, i.e., these two
	// SETs have identical user keys (because of an open snapshot). This should
	// be the rare case.
	eval.writeToValueBlock = true
	return eval, nil
}

func (w *RawColumnWriter) flushDataBlockWithoutNextKey(nextKey []byte) error {
	serializedBlock, lastKey := w.dataBlock.Finish(w.dataBlock.Rows()-1, w.pendingDataBlockSize)
	w.maybeIncrementTombstoneDenseBlocks(len(serializedBlock))
	// Compute the separator that will be written to the index block alongside
	// this data block's end offset. It is the separator between the last key in
	// the finished block and the [nextKey] that was excluded from the block.
	w.separatorBuf = w.comparer.Separator(w.separatorBuf[:0], lastKey.UserKey, nextKey)
	if err := w.enqueueDataBlock(serializedBlock, lastKey, w.separatorBuf); err != nil {
		return err
	}
	w.dataBlock.Reset()
	w.pendingDataBlockSize = 0
	return nil
}

// maybeIncrementTombstoneDenseBlocks increments the number of tombstone dense
// blocks if the number of deletions in the data block exceeds a threshold or
// the deletion size exceeds a threshold. It should be called after the
// data block has been finished.
// Invariant: w.dataBlockBuf.uncompressed must already be populated.
func (w *RawColumnWriter) maybeIncrementTombstoneDenseBlocks(uncompressedLen int) {
	minSize := w.opts.DeletionSizeRatioThreshold * float32(uncompressedLen)
	if w.dataBlock.numDeletions > w.opts.NumDeletionsThreshold || float32(w.dataBlock.deletionSize) > minSize {
		w.props.NumTombstoneDenseBlocks++
	}
	w.dataBlock.numDeletions = 0
	w.dataBlock.deletionSize = 0
}

// enqueueDataBlock compresses and checksums the provided data block and sends
// it to the write queue to be asynchronously written to the underlying storage.
// It also adds the block's index block separator to the pending index block,
// possibly triggering the index block to be finished and buffered.
func (w *RawColumnWriter) enqueueDataBlock(
	serializedBlock []byte, lastKey base.InternalKey, separator []byte,
) error {
	w.lastKeyBuf = append(w.lastKeyBuf[:0], lastKey.UserKey...)
	w.meta.SetLargestPointKey(base.InternalKey{
		UserKey: w.lastKeyBuf,
		Trailer: lastKey.Trailer,
	})

	if invariants.Enabled {
		v := w.validator.Get()
		if v == nil {
			v = &colblk.DataBlockValidator{}
			w.validator.Set(v)
		}
		if err := v.Validate(serializedBlock, w.comparer, w.opts.KeySchema); err != nil {
			panic(err)
		}
	}

	// Compress and checksum the data block and send it to the write queue.
	pb := w.layout.physBlockMaker.Make(serializedBlock, blockkind.SSTableData, block.NoFlags)
	return w.enqueuePhysicalBlock(pb.Take(), separator)
}

// enqueuePhysicalBlock enqueues a physical block to the write queue; the
// physical block will be automatically released.
func (w *RawColumnWriter) enqueuePhysicalBlock(
	pb block.OwnedPhysicalBlock, separator []byte,
) error {
	dataBlockHandle := block.Handle{
		Offset: w.queuedDataSize,
		Length: uint64(pb.Length().WithoutTrailer()),
	}
	w.queuedDataSize += dataBlockHandle.Length + block.TrailerLen
	w.writeQueue.ch <- pb

	var err error
	w.blockPropsEncoder.resetProps()
	for i := range w.blockPropCollectors {
		scratch := w.blockPropsEncoder.getScratchForProp()
		if scratch, err = w.blockPropCollectors[i].FinishDataBlock(scratch); err != nil {
			return err
		}
		w.blockPropsEncoder.addProp(shortID(i), scratch)
	}
	dataBlockProps := w.blockPropsEncoder.unsafeProps()

	// Add the separator to the index block. This might trigger a flush of the
	// index block too.
	i := w.indexBlock.AddBlockHandle(separator, dataBlockHandle, dataBlockProps)
	sizeWithEntry := w.indexBlock.Size()
	if shouldFlushWithoutLatestKV(sizeWithEntry, w.indexBlockSize, i, &w.indexFlush) {
		// NB: finishIndexBlock will use blockPropsEncoder, so we must clone the
		// data block's props first.
		dataBlockProps = slices.Clone(dataBlockProps)

		if err = w.finishIndexBlock(w.indexBlock.Rows() - 1); err != nil {
			return err
		}
		// finishIndexBlock reset the index block builder, and we can
		// add the block handle to this new index block.
		_ = w.indexBlock.AddBlockHandle(separator, dataBlockHandle, dataBlockProps)
		w.indexBlockSize = w.indexBlock.Size()
	} else {
		w.indexBlockSize = sizeWithEntry
	}
	// Incorporate the finished data block's property into the index block, now
	// that we've flushed the index block without the new separator if
	// necessary.
	for i := range w.blockPropCollectors {
		w.blockPropCollectors[i].AddPrevDataBlockToIndexBlock()
	}
	return nil
}

// finishIndexBlock finishes the currently pending index block with the first
// [rows] rows. In practice, [rows] is always w.indexBlock.Rows() or
// w.indexBlock.Rows()-1.
//
// The finished index block is buffered until the writer is closed.
func (w *RawColumnWriter) finishIndexBlock(rows int) error {
	defer w.indexBlock.Reset()
	w.blockPropsEncoder.resetProps()
	for i := range w.blockPropCollectors {
		scratch := w.blockPropsEncoder.getScratchForProp()
		var err error
		if scratch, err = w.blockPropCollectors[i].FinishIndexBlock(scratch); err != nil {
			return err
		}
		w.blockPropsEncoder.addProp(shortID(i), scratch)
	}
	indexProps := w.blockPropsEncoder.props()
	bib := bufferedIndexBlock{nEntries: rows, properties: indexProps}

	// Copy the last (greatest) separator key in the index block into bib.sep.
	// It'll be the separator on the entry in the top-level index block.
	//
	// TODO(jackson): bib.sep.Trailer is unused within the columnar-block
	// sstable writer. Its existence is a code artifact of reuse of the
	// bufferedIndexBlock type between colblk and rowblk writers. This can be
	// cleaned up.
	bib.sep.Trailer = base.MakeTrailer(base.SeqNumMax, base.InternalKeyKindSeparator)
	w.indexBuffering.sepAlloc, bib.sep.UserKey = w.indexBuffering.sepAlloc.Copy(
		w.indexBlock.UnsafeSeparator(rows - 1))

	// Finish the index block and copy it so that w.indexBlock may be reused.
	blk := w.indexBlock.Finish(rows)
	if len(w.indexBuffering.blockAlloc) < len(blk) {
		// Allocate enough bytes for approximately 16 index blocks.
		w.indexBuffering.blockAlloc = make([]byte, len(blk)*16)
	}
	n := copy(w.indexBuffering.blockAlloc, blk)
	bib.block = w.indexBuffering.blockAlloc[:n:n]
	w.indexBuffering.blockAlloc = w.indexBuffering.blockAlloc[n:]

	w.indexBuffering.partitions = append(w.indexBuffering.partitions, bib)
	// We include the separator user key to account for its bytes in the
	// top-level index block.
	//
	// TODO(jackson): We could incrementally build the top-level index block
	// and produce an exact calculation of the current top-level index
	// block's size.
	w.indexBuffering.partitionSizeSum += uint64(len(blk) + block.TrailerLen + len(bib.sep.UserKey))
	return nil
}

// flushBufferedIndexBlocks writes all index blocks, including the top-level
// index block if necessary, to the underlying writable. It returns the block
// handle of the top index (either the only index block or the top-level index
// if two-level).
func (w *RawColumnWriter) flushBufferedIndexBlocks() (rootIndex block.Handle, err error) {
	// If there's a currently-pending index block, finish it.
	if w.indexBlock.Rows() > 0 || len(w.indexBuffering.partitions) == 0 {
		if err := w.finishIndexBlock(w.indexBlock.Rows()); err != nil {
			return block.Handle{}, err
		}
	}
	// We've buffered all the index blocks. Typically there's just one index
	// block, in which case we're writing a "single-level" index. If we're
	// writing a large file or the index separators happen to be excessively
	// long, we may have several index blocks and need to construct a
	// "two-level" index structure.
	switch len(w.indexBuffering.partitions) {
	case 0:
		// This is impossible because we'll flush the index block immediately
		// above this switch statement if there are no buffered partitions
		// (regardless of whether there are data block handles in the index
		// block).
		panic("unreachable")
	case 1:
		// Single-level index.
		rootIndex, err = w.layout.WriteIndexBlock(w.indexBuffering.partitions[0].block)
		if err != nil {
			return rootIndex, err
		}
		w.props.IndexSize = uint64(len(w.indexBuffering.partitions[0].block))
		w.props.NumDataBlocks = uint64(w.indexBuffering.partitions[0].nEntries)
		w.props.IndexType = binarySearchIndex
	default:
		// Two-level index.
		for _, part := range w.indexBuffering.partitions {
			bh, err := w.layout.WriteIndexBlock(part.block)
			if err != nil {
				return block.Handle{}, err
			}
			w.props.IndexSize += uint64(len(part.block))
			w.props.NumDataBlocks += uint64(part.nEntries)
			w.topLevelIndexBlock.AddBlockHandle(part.sep.UserKey, bh, part.properties)
		}
		topLevelIndex := w.topLevelIndexBlock.Finish(w.topLevelIndexBlock.Rows())
		rootIndex, err = w.layout.WriteIndexBlock(topLevelIndex)
		if err != nil {
			return block.Handle{}, err
		}
		w.props.TopLevelIndexSize = uint64(len(topLevelIndex))
		w.props.IndexSize += uint64(len(topLevelIndex))
		w.props.IndexType = twoLevelIndex
		w.props.IndexPartitions = uint64(len(w.indexBuffering.partitions))
	}
	return rootIndex, nil
}

// drainWriteQueue runs in its own goroutine and is responsible for writing
// finished, compressed data blocks to the writable. It reads from w.writeQueue
// until the channel is closed. All data blocks are written by this goroutine.
// Other blocks are written directly by the client goroutine. See Close.
func (w *RawColumnWriter) drainWriteQueue() {
	// Call once to initialize the CPU measurer.
	w.cpuMeasurer.MeasureCPU(base.CompactionGoroutineSSTableSecondary)
	for pb := range w.writeQueue.ch {
		if _, err := w.layout.WritePrecompressedDataBlock(pb); err != nil {
			w.writeQueue.err = err
		}
		// Report to the CPU measurer immediately after writing (note that there
		// may be a time lag until the next block is available to write).
		w.cpuMeasurer.MeasureCPU(base.CompactionGoroutineSSTableSecondary)
	}
}

func (w *RawColumnWriter) Close() (err error) {
	defer func() {
		if w.valueBlock != nil {
			w.valueBlock.Release()
			// Defensive code in case Close gets called again. We don't want to put
			// the same object to a sync.Pool.
			w.valueBlock = nil
		}
		w.layout.Abort()
		// Record any error in the writer (so we can exit early if Close is called
		// again).
		if err != nil {
			w.err = err
		}
	}()
	if w.layout.writable == nil {
		return w.err
	}

	// Finish the last data block and send it to the write queue if it contains
	// any pending KVs.
	if rows := w.dataBlock.Rows(); rows > 0 {
		serializedBlock, lastKey := w.dataBlock.Finish(rows, w.pendingDataBlockSize)
		w.separatorBuf = w.comparer.Successor(w.separatorBuf[:0], lastKey.UserKey)
		w.err = errors.CombineErrors(w.err, w.enqueueDataBlock(serializedBlock, lastKey, w.separatorBuf))
		w.maybeIncrementTombstoneDenseBlocks(len(serializedBlock))
	}
	// Close the write queue channel so that the goroutine responsible for
	// writing data blocks to disk knows to exit. Any subsequent blocks (eg,
	// index, metadata, range key, etc) will be written by the goroutine that
	// called Close.
	close(w.writeQueue.ch)
	w.writeQueue.wg.Wait()
	// If the write queue encountered any errors while writing out data blocks,
	// it's stored in w.writeQueue.err.
	w.err = firstError(w.err, w.writeQueue.err)
	if w.err != nil {
		return w.err
	}

	// INVARIANT: w.queuedDataSize == w.layout.offset.
	// All data blocks have been written to disk. The queuedDataSize is the
	// cumulative size of all the data blocks we've sent to the write queue. Now
	// that they've all been flushed, queuedDataSize should match w.layout's
	// offset.
	if w.queuedDataSize != w.layout.offset {
		panic(errors.AssertionFailedf("pebble: %d of queued data blocks but layout offset is %d",
			w.queuedDataSize, w.layout.offset))
	}
	w.props.DataSize = w.layout.offset
	if _, err = w.flushBufferedIndexBlocks(); err != nil {
		return err
	}

	// Write the filter block.
	if w.filterWriter != nil {
		blockSize, filterFamily, err := w.layout.WriteFilterBlock(w.filterWriter)
		if err != nil {
			return err
		}
		w.props.FilterFamily = string(filterFamily)
		w.props.FilterSize = blockSize
	}

	// Write the range deletion block if non-empty.
	if w.rangeDelBlock.KeyCount() > 0 {
		w.props.NumRangeDeletions = uint64(w.rangeDelBlock.KeyCount())
		sm, la := w.rangeDelBlock.UnsafeBoundaryKeys()
		w.meta.SetSmallestRangeDelKey(sm)
		w.meta.SetLargestRangeDelKey(la)
		if _, err := w.layout.WriteRangeDeletionBlock(w.rangeDelBlock.Finish()); err != nil {
			return err
		}
	}

	// Write the range key block if non-empty.
	if w.rangeKeyBlock.KeyCount() > 0 {
		sm, la := w.rangeKeyBlock.UnsafeBoundaryKeys()
		w.meta.SetSmallestRangeKey(sm)
		w.meta.SetLargestRangeKey(la)
		if _, err := w.layout.WriteRangeKeyBlock(w.rangeKeyBlock.Finish()); err != nil {
			return err
		}
	}

	// Write out the value block.
	if w.valueBlock != nil {
		_, vbStats, err := w.valueBlock.Finish(&w.layout, w.layout.offset)
		if err != nil {
			return err
		}
		w.props.NumValueBlocks = vbStats.NumValueBlocks
		w.props.NumValuesInValueBlocks = vbStats.NumValuesInValueBlocks
		w.props.ValueBlocksSize = vbStats.ValueBlocksAndIndexSize
	}

	// Write the blob reference index block if non-empty.
	if w.blobRefLivenessIndexBlock.numReferences() > 0 {
		// Blob references (and thus blob reference index blocks) require at
		// least TableFormatPebblev6.
		if w.opts.TableFormat < TableFormatPebblev6 {
			return errors.AssertionFailedf("blob reference index block not supported in table format %s", w.layout.tableFormat)
		}
		var encoder colblk.ReferenceLivenessBlockEncoder
		encoder.Init()
		for refID, buf := range w.blobRefLivenessIndexBlock.finish() {
			encoder.AddReferenceLiveness(int(refID), buf)
		}
		if _, err := w.layout.WriteBlobRefIndexBlock(encoder.Finish()); err != nil {
			return err
		}
	}

	// Write the properties block.
	{
		// Finish and record the prop collectors if props are not yet recorded.
		// Pre-computed props might have been copied by specialized sst creators
		// like suffix replacer.
		if len(w.props.UserProperties) == 0 {
			userProps := make(map[string]string)
			for i := range w.blockPropCollectors {
				scratch := w.blockPropsEncoder.getScratchForProp()
				// Place the shortID in the first byte.
				scratch = append(scratch, byte(i))
				buf, err := w.blockPropCollectors[i].FinishTable(scratch)
				if err != nil {
					return err
				}
				var prop string
				if len(buf) > 0 {
					prop = string(buf)
				}
				// NB: The property is populated in the map even if it is the
				// empty string, since the presence in the map is what indicates
				// that the block property collector was used when writing.
				userProps[w.blockPropCollectors[i].Name()] = prop
			}
			if len(userProps) > 0 {
				w.props.UserProperties = userProps
			}
		}

		w.props.CompressionStats = w.layout.physBlockMaker.Compressor.Stats().String()
		var toWrite []byte
		if w.opts.TableFormat >= TableFormatPebblev7 {
			var cw colblk.KeyValueBlockWriter
			cw.Init()
			w.props.saveToColWriter(w.opts.TableFormat, &cw)
			toWrite = cw.Finish(cw.Rows())
		} else {
			var raw rowblk.Writer
			// The restart interval is set to infinity because the properties block
			// is always read sequentially and cached in a heap located object. This
			// reduces table size without a significant impact on performance.
			raw.RestartInterval = propertiesBlockRestartInterval
			if err = w.props.saveToRowWriter(w.opts.TableFormat, &raw); err != nil {
				return err
			}
			toWrite = raw.Finish()
		}
		if _, err = w.layout.WritePropertiesBlock(toWrite); err != nil {
			return err
		}
	}

	if w.opts.TableFormat >= TableFormatPebblev7 {
		w.layout.attributes = w.props.toAttributes()
	}

	// Write the table footer.
	w.meta.Size, err = w.layout.Finish()
	if err != nil {
		return err
	}
	w.meta.Properties = w.props
	// Release any held memory and make any future calls error.
	*w = RawColumnWriter{meta: w.meta, err: errWriterClosed}
	return nil
}

// rewriteSuffixes implements RawWriter.
func (w *RawColumnWriter) rewriteSuffixes(
	r *Reader, sstBytes []byte, wo WriterOptions, from, to []byte, concurrency int,
) error {
	for _, c := range w.blockPropCollectors {
		if !c.SupportsSuffixReplacement() {
			return errors.Errorf("block property collector %s does not support suffix replacement", c.Name())
		}
	}
	l, err := r.Layout()
	if err != nil {
		return errors.Wrap(err, "reading layout")
	}
	// Copy data blocks in parallel, rewriting suffixes as we go.
	blocks, err := rewriteDataBlocksInParallel(r, sstBytes, wo, l.Data, from, to, concurrency, w.layout.physBlockMaker.Compressor.Stats(), func() blockRewriter {
		return colblk.NewDataBlockRewriter(wo.KeySchema, w.comparer, wo.TableFormat.TieringColumnConfig())
	})
	if err != nil {
		return errors.Wrap(err, "rewriting data blocks")
	}

	// oldShortIDs maps the shortID for the block property collector in the old
	// blocks to the shortID in the new blocks. Initialized once for the sstable.
	oldShortIDs, n, err := getShortIDs(r, w.blockPropCollectors)
	if err != nil {
		return errors.Wrap(err, "getting short IDs")
	}
	oldProps := make([][]byte, len(w.blockPropCollectors))
	for i := range blocks {
		// Load any previous values for our prop collectors into oldProps.
		for i := range oldProps {
			oldProps[i] = nil
		}
		decoder := makeBlockPropertiesDecoder(n, l.Data[i].Props)
		for !decoder.Done() {
			id, val, err := decoder.Next()
			if err != nil {
				return err
			}
			if oldShortIDs[id].IsValid() {
				oldProps[oldShortIDs[id]] = val
			}
		}
		for i, p := range w.blockPropCollectors {
			if err := p.AddCollectedWithSuffixReplacement(oldProps[i], from, to); err != nil {
				return err
			}
		}
		var separator []byte
		if i+1 < len(blocks) {
			w.separatorBuf = w.comparer.Separator(w.separatorBuf[:0], blocks[i].end.UserKey, blocks[i+1].start.UserKey)
			separator = w.separatorBuf
		} else {
			w.separatorBuf = w.comparer.Successor(w.separatorBuf[:0], blocks[i].end.UserKey)
			separator = w.separatorBuf
		}
		if err := w.enqueuePhysicalBlock(blocks[i].physical.Take(), separator); err != nil {
			return err
		}
	}

	if len(blocks) > 0 {
		props, err := r.ReadPropertiesBlock(context.TODO(), nil /* buffer pool */)
		if err != nil {
			return errors.Wrap(err, "reading properties block")
		}
		w.meta.updateSeqNum(blocks[0].start.SeqNum())
		w.props.NumEntries = props.NumEntries
		w.props.RawKeySize = props.RawKeySize
		w.props.RawValueSize = props.RawValueSize
		w.meta.SetSmallestPointKey(blocks[0].start)
		w.meta.SetLargestPointKey(blocks[len(blocks)-1].end)
	}

	// Copy range key block, replacing suffixes if it exists.
	if err := rewriteRangeKeyBlockToWriter(r, w, from, to); err != nil {
		return errors.Wrap(err, "rewriting range key blocks")
	}
	// Copy over the filter block if it exists.
	if family, filterBH, ok := getExistingFilter(l); ok {
		filterBlock, _, err := readBlockBuf(sstBytes, filterBH, r.blockReader.ChecksumType(), nil)
		if err != nil {
			return errors.Wrap(err, "reading filter")
		}
		// Clone the filter block, because readBlockBuf allows the
		// returned byte slice to point directly into sst.
		w.setFilter(slices.Clone(filterBlock), family)
	}
	return nil
}

// getExistingFilter returns any existing table filter block handle, along with
// the policy name (derived from the block name). Returns ok=false if there is
// no filter block.
func getExistingFilter(layout *Layout) (family base.TableFilterFamily, bh block.Handle, ok bool) {
	if len(layout.Filter) == 0 {
		return "", block.Handle{}, false
	}
	if invariants.Enabled && len(layout.Filter) > 1 {
		panic(errors.AssertionFailedf("cannot handle more than one filter"))
	}
	family, ok = filterFamilyFromBlockName(layout.Filter[0].Name)
	if !ok {
		if invariants.Enabled {
			panic(errors.AssertionFailedf("l.Filter has invalid filter block name %q", layout.Filter[0].Name))
		}
		return "", block.Handle{}, false
	}
	return family, layout.Filter[0].Handle, true
}

func shouldFlushWithoutLatestKV(
	sizeWithKV int, sizeWithoutKV int, entryCountWithoutKV int, flushGovernor *block.FlushGovernor,
) bool {
	if entryCountWithoutKV == 0 {
		return false
	}
	if sizeWithoutKV < flushGovernor.LowWatermark() {
		// Fast path when the block is too small to flush.
		return false
	}
	return flushGovernor.ShouldFlush(sizeWithoutKV, sizeWithKV)
}

// copyDataBlocks adds a range of blocks to the table as-is. These blocks could be
// compressed. It's specifically used by the sstable copier that can copy parts
// of an sstable to a new sstable, using CopySpan().
func (w *RawColumnWriter) copyDataBlocks(
	ctx context.Context, blocks []indexEntry, rh objstorage.ReadHandle,
) error {
	const readSizeTarget = 256 << 10
	var buf []byte
	readAndFlushBlocks := func(firstBlockIdx, lastBlockIdx int) error {
		if firstBlockIdx > lastBlockIdx {
			panic("pebble: readAndFlushBlocks called with invalid block range")
		}
		// We need to flush blocks[firstBlockIdx:lastBlockIdx+1] into the write queue.
		// We do this by issuing one big read from the read handle into the buffer, and
		// then enqueueing the writing of those blocks one-by-one.
		//
		// TODO(bilal): Consider refactoring the write queue to support writing multiple
		// blocks in one request.
		lastBH := blocks[lastBlockIdx].bh
		blocksToReadLen := lastBH.Offset + lastBH.Length + block.TrailerLen - blocks[firstBlockIdx].bh.Offset
		buf = slices.Grow(buf[:0], int(blocksToReadLen))
		if err := rh.ReadAt(ctx, buf[:blocksToReadLen], int64(blocks[firstBlockIdx].bh.Offset)); err != nil {
			return err
		}
		for i := firstBlockIdx; i <= lastBlockIdx; i++ {
			offsetDiff := blocks[i].bh.Offset - blocks[firstBlockIdx].bh.Offset
			dataWithTrailer := buf[offsetDiff : offsetDiff+blocks[i].bh.Length+block.TrailerLen]
			pb := block.AlreadyEncodedPhysicalBlock(dataWithTrailer)
			if err := w.enqueuePhysicalBlock(pb.Take(), blocks[i].sep); err != nil {
				return err
			}
		}
		return nil
	}
	// Iterate through blocks until we have enough to fill readSizeTarget. When we have more than
	// one block in blocksToRead and adding the next block would exceed the target buffer capacity,
	// we read and flush existing blocks in blocksToRead. This allows us to read as many
	// blocks in one IO request as possible, while still utilizing the write queue in this
	// writer.
	lastBlockOffset := uint64(0)
	for i := 0; i < len(blocks); {
		if blocks[i].bh.Offset < lastBlockOffset {
			panic("pebble: copyDataBlocks called with blocks out of order")
		}
		start := i
		// Note the i++ in the initializing condition; this means we will always flush at least
		// one block.
		for i++; i < len(blocks) && (blocks[i].bh.Length+blocks[i].bh.Offset+block.TrailerLen-blocks[start].bh.Offset) <= uint64(readSizeTarget); i++ {
		}
		// i points to one index past the last block we want to read.
		if err := readAndFlushBlocks(start, i-1); err != nil {
			return err
		}
	}
	return nil
}

// addDataBlock adds a raw uncompressed data block to the table as-is. It's specifically used
// by the sstable copier that can copy parts of an sstable to a new sstable,
// using CopySpan().
func (w *RawColumnWriter) addDataBlock(b, sep []byte, bhp block.HandleWithProperties) error {
	// Compress and checksum the data block and send it to the write queue.
	pb := w.layout.physBlockMaker.Make(b, blockkind.SSTableData, block.NoFlags)
	if err := w.enqueuePhysicalBlock(pb.Take(), sep); err != nil {
		return err
	}
	return nil
}

// setFilter sets a pre-populated filter. It is used when we are rewriting
// suffixes or copying parts of an sstable via CopySpan().
//
// The writer takes ownership of the filterData buffer.
func (w *RawColumnWriter) setFilter(filterData []byte, family base.TableFilterFamily) {
	w.filterWriter = &copyFilterWriter{
		data:   filterData,
		family: family,
	}
}

// copyProperties copies properties from the specified props, and resets others
// to prepare for copying data blocks from another sstable, using the copy/addDataBlock(s)
// methods above. It's specifically used by the sstable copier that can copy parts of an
// sstable to a new sstable, using CopySpan().
func (w *RawColumnWriter) copyProperties(props Properties) {
	w.props = props
	// Remove all user properties to disable block properties, which we do not
	// calculate for CopySpan.
	w.props.UserProperties = nil
	// Reset props that we'll re-derive as we build our own index.
	w.props.IndexPartitions = 0
	w.props.TopLevelIndexSize = 0
	w.props.IndexSize = 0
	w.props.IndexType = 0
}

// SetValueSeparationProps implements RawWriter.
func (w *RawColumnWriter) SetValueSeparationProps(
	minValueSize uint64, disableSeparationBySuffix bool,
) {
	w.props.ValueSeparationMinSize = minValueSize
	w.props.ValueSeparationBySuffixDisabled = disableSeparationBySuffix
}
