// Copyright 2024 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package sstable

import (
	"context"
	"fmt"
	"io"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/keyspan"
	"github.com/cockroachdb/pebble/objstorage"
	"github.com/cockroachdb/pebble/sstable/blob"
	"github.com/cockroachdb/pebble/sstable/block"
)

// NewRawWriter returns a new table writer for the file. Closing the writer will
// close the file.
func NewRawWriter(writable objstorage.Writable, o WriterOptions) RawWriter {
	return NewRawWriterWithCPUMeasurer(writable, o, base.NoopCPUMeasurer{})
}

// NewRawWriterWithCPUMeasurer is like NewRawWriter, but additionally allows
// the caller to specify a CPUMeasurer. Only
// CPUMeasurer.MeasureCPU(CompactionGoroutineSSTableSecondary) is used by the
// writer.
func NewRawWriterWithCPUMeasurer(
	writable objstorage.Writable, o WriterOptions, cpuMeasurer base.CPUMeasurer,
) RawWriter {
	if o.TableFormat <= TableFormatPebblev4 {
		// Don't bother plumbing the cpuMeasurer to the row writer since it is not
		// the default and will be removed.
		return newRowWriter(writable, o)
	}
	return newColumnarWriter(writable, o, cpuMeasurer)
}

// Writer is a table writer.
type Writer struct {
	rw  RawWriter
	err error
	// To allow potentially overlapping (i.e. un-fragmented) range keys spans to
	// be added to the Writer, a keyspan.Fragmenter is used to retain the keys
	// and values, emitting fragmented, coalesced spans as appropriate. Range
	// keys must be added in order of their start user-key.
	fragmenter keyspan.Fragmenter
	comparer   *base.Comparer
	// isStrictObsolete is true if the writer is configured to write and enforce
	// a 'strict obsolete' sstable. This includes prohibiting the addition of
	// MERGE keys. See the documentation in format.go for more details.
	isStrictObsolete bool
	rkBuf            []byte
	keyspanKeys      []keyspan.Key
}

// NewWriter returns a new table writer intended for building external sstables
// (eg, for ingestion or storage outside the LSM) for the file. Closing the
// writer will close the file.
//
// Internal clients should generally prefer NewRawWriter.
func NewWriter(writable objstorage.Writable, o WriterOptions) *Writer {
	o = o.ensureDefaults()
	rw := NewRawWriter(writable, o)
	w := &Writer{}
	*w = Writer{
		rw: rw,
		fragmenter: keyspan.Fragmenter{
			Cmp:    o.Comparer.Compare,
			Format: o.Comparer.FormatKey,
			Emit:   w.encodeFragmentedRangeKeySpan,
		},
		comparer:         o.Comparer,
		isStrictObsolete: o.IsStrictObsolete,
	}
	return w
}

func (w *Writer) encodeFragmentedRangeKeySpan(s keyspan.Span) {
	// This method is the emit function of the Fragmenter.
	//
	// NB: The span should only contain range keys and be internally consistent
	// (eg, no duplicate suffixes, no additional keys after a RANGEKEYDEL).
	//
	// Sort the keys by trailer (descending).
	//
	// Note that sstables written in TableFormatPebblev4 and earlier (rowblk
	// encoding) will always re-order the Keys in order to encode RANGEKEYSETs
	// first, then RANGEKEYUNSETs and then RANGEKEYDELs. See rangekey.Encoder.
	// SSTables written in TableFormatPebblev5 or later (colblk encoding) will
	// encode the keys in this order.
	//
	// Iteration doesn't depend on this ordering, in particular because it's
	// inconsistent between the two encodings, but we may want eventually begin
	// to depend on this ordering for colblk-encoded sstables.
	keyspan.SortKeysByTrailerAndSuffix(w.comparer.CompareRangeSuffixes, s.Keys)
	if w.Error() == nil {
		if err := w.rw.EncodeSpan(s); err != nil {
			w.err = err
		}
	}
}

// Error returns the current accumulated error if any.
func (w *Writer) Error() error {
	return errors.CombineErrors(w.rw.Error(), w.err)
}

// Raw returns the underlying RawWriter.
func (w *Writer) Raw() RawWriter { return w.rw }

// Set sets the value for the given key. The sequence number is set to 0.
// Intended for use to externally construct an sstable before ingestion into a
// DB. For a given Writer, the keys passed to Set must be in strictly increasing
// order.
//
// TODO(peter): untested
func (w *Writer) Set(key, value []byte) error {
	if err := w.Error(); err != nil {
		return err
	}
	if w.isStrictObsolete {
		return errors.Errorf("use AddWithForceObsolete")
	}
	// forceObsolete is false based on the assumption that no RANGEDELs in the
	// sstable delete the added points.
	return w.rw.Add(base.MakeInternalKey(key, 0, InternalKeyKindSet), value, false, base.KVMeta{})
}

// Delete deletes the value for the given key. The sequence number is set to
// 0. Intended for use to externally construct an sstable before ingestion into
// a DB.
//
// TODO(peter): untested
func (w *Writer) Delete(key []byte) error {
	if err := w.Error(); err != nil {
		return err
	}
	if w.isStrictObsolete {
		return errors.Errorf("use AddWithForceObsolete")
	}
	// forceObsolete is false based on the assumption that no RANGEDELs in the
	// sstable delete the added points.
	return w.rw.Add(base.MakeInternalKey(key, 0, InternalKeyKindDelete), nil, false, base.KVMeta{})
}

// DeleteRange deletes all of the keys (and values) in the range [start,end)
// (inclusive on start, exclusive on end). The sequence number is set to
// 0. Intended for use to externally construct an sstable before ingestion into
// a DB.
//
// Calls to DeleteRange must be made using already-fragmented (non-overlapping)
// spans and in sorted order.
//
// TODO(peter): untested
func (w *Writer) DeleteRange(start, end []byte) error {
	if err := w.Error(); err != nil {
		return err
	}
	return w.rw.EncodeSpan(keyspan.Span{
		Start: start,
		End:   end,
		Keys: append(w.keyspanKeys[:0], keyspan.Key{
			Trailer: base.MakeTrailer(0, base.InternalKeyKindRangeDelete),
		}),
	})
}

// Merge adds an action to the DB that merges the value at key with the new
// value. The details of the merge are dependent upon the configured merge
// operator. The sequence number is set to 0. Intended for use to externally
// construct an sstable before ingestion into a DB.
//
// TODO(peter): untested
func (w *Writer) Merge(key, value []byte) error {
	if err := w.Error(); err != nil {
		return err
	}
	if w.isStrictObsolete {
		return errors.Errorf("use AddWithForceObsolete")
	}
	// forceObsolete is false based on the assumption that no RANGEDELs in the
	// sstable that delete the added points. If the user configured this writer
	// to be strict-obsolete, addPoint will reject the addition of this MERGE.
	return w.rw.Add(base.MakeInternalKey(key, 0, InternalKeyKindMerge), value, false, base.KVMeta{})
}

// RangeKeySet sets a range between start (inclusive) and end (exclusive) with
// the given suffix to the given value. The resulting range key is given the
// sequence number zero, with the expectation that the resulting sstable will be
// ingested.
//
// Keys must be added to the table in increasing order of start key. Spans are
// not required to be fragmented. The same suffix may not be set or unset twice
// over the same keyspan, because it would result in inconsistent state. Both
// the Set and Unset would share the zero sequence number, and a key cannot be
// both simultaneously set and unset.
func (w *Writer) RangeKeySet(start, end, suffix, value []byte) error {
	return w.addRangeKeySpanToFragmenter(keyspan.Span{
		Start: w.tempRangeKeyCopy(start),
		End:   w.tempRangeKeyCopy(end),
		Keys: []keyspan.Key{
			{
				Trailer: base.MakeTrailer(0, base.InternalKeyKindRangeKeySet),
				Suffix:  w.tempRangeKeyCopy(suffix),
				Value:   w.tempRangeKeyCopy(value),
			},
		},
	})
}

// RangeKeyUnset un-sets a range between start (inclusive) and end (exclusive)
// with the given suffix. The resulting range key is given the
// sequence number zero, with the expectation that the resulting sstable will be
// ingested.
//
// Keys must be added to the table in increasing order of start key. Spans are
// not required to be fragmented. The same suffix may not be set or unset twice
// over the same keyspan, because it would result in inconsistent state. Both
// the Set and Unset would share the zero sequence number, and a key cannot be
// both simultaneously set and unset.
func (w *Writer) RangeKeyUnset(start, end, suffix []byte) error {
	return w.addRangeKeySpanToFragmenter(keyspan.Span{
		Start: w.tempRangeKeyCopy(start),
		End:   w.tempRangeKeyCopy(end),
		Keys: []keyspan.Key{
			{
				Trailer: base.MakeTrailer(0, base.InternalKeyKindRangeKeyUnset),
				Suffix:  w.tempRangeKeyCopy(suffix),
			},
		},
	})
}

// RangeKeyDelete deletes a range between start (inclusive) and end (exclusive).
//
// Keys must be added to the table in increasing order of start key. Spans are
// not required to be fragmented.
func (w *Writer) RangeKeyDelete(start, end []byte) error {
	return w.addRangeKeySpanToFragmenter(keyspan.Span{
		Start: w.tempRangeKeyCopy(start),
		End:   w.tempRangeKeyCopy(end),
		Keys: []keyspan.Key{
			{Trailer: base.MakeTrailer(0, base.InternalKeyKindRangeKeyDelete)},
		},
	})
}

func (w *Writer) addRangeKeySpanToFragmenter(span keyspan.Span) error {
	if w.comparer.Compare(span.Start, span.End) >= 0 {
		return errors.Errorf(
			"pebble: start key must be strictly less than end key",
		)
	}
	if w.fragmenter.Start() != nil && w.comparer.Compare(w.fragmenter.Start(), span.Start) > 0 {
		return errors.Errorf("pebble: spans must be added in order: %s > %s",
			w.comparer.FormatKey(w.fragmenter.Start()), w.comparer.FormatKey(span.Start))
	}
	// Add this span to the fragmenter.
	w.fragmenter.Add(span)
	return w.Error()
}

// tempRangeKeyBuf returns a slice of length n from the Writer's rkBuf byte
// slice. Any byte written to the returned slice is retained for the lifetime of
// the Writer.
func (w *Writer) tempRangeKeyBuf(n int) []byte {
	if cap(w.rkBuf)-len(w.rkBuf) < n {
		size := len(w.rkBuf) + 2*n
		if size < 2*cap(w.rkBuf) {
			size = 2 * cap(w.rkBuf)
		}
		buf := make([]byte, len(w.rkBuf), size)
		copy(buf, w.rkBuf)
		w.rkBuf = buf
	}
	b := w.rkBuf[len(w.rkBuf) : len(w.rkBuf)+n]
	w.rkBuf = w.rkBuf[:len(w.rkBuf)+n]
	return b
}

// tempRangeKeyCopy returns a copy of the provided slice, stored in the Writer's
// range key buffer.
func (w *Writer) tempRangeKeyCopy(k []byte) []byte {
	if len(k) == 0 {
		return nil
	}
	buf := w.tempRangeKeyBuf(len(k))
	copy(buf, k)
	return buf
}

// Metadata returns the metadata for the finished sstable. Only valid to call
// after the sstable has been finished.
func (w *Writer) Metadata() (*WriterMetadata, error) {
	return w.rw.Metadata()
}

// Close finishes writing the table and closes the underlying file that the
// table was written to.
func (w *Writer) Close() (err error) {
	if w.Error() == nil {
		// Write the range-key block, flushing any remaining spans from the
		// fragmenter first.
		w.fragmenter.Finish()
	}
	return errors.CombineErrors(w.rw.Close(), w.err)
}

// KVMeta reexports base.KVMeta, so callers can use RawWriter.Add.
type KVMeta = base.KVMeta

// RawWriter defines an interface for sstable writers. Implementations may vary
// depending on the TableFormat being written.
type RawWriter interface {
	// Error returns the current accumulated error if any.
	Error() error
	// Add adds a key-value pair to the sstable.
	//
	// forceObsolete indicates whether the caller has determined that this key is
	// obsolete even though it may be the latest point key for this userkey. This
	// should be set to true for keys obsoleted by RANGEDELs, and is required for
	// strict-obsolete sstables. It's optional for non-strict-obsolete sstables.
	//
	// Note that there are two properties, S1 and S2 (see comment in format.go)
	// that strict-obsolete ssts must satisfy. S2, due to RANGEDELs, is solely the
	// responsibility of the caller. S1 is solely the responsibility of the
	// callee.
	Add(key InternalKey, value []byte, forceObsolete bool, meta base.KVMeta) error
	// AddWithBlobHandle adds a key to the sstable, but encoding a blob value
	// handle instead of an in-place value. See Add for more details. The caller
	// must provide the already-extracted ShortAttribute for the value.
	AddWithBlobHandle(key InternalKey, h blob.InlineHandle, attr base.ShortAttribute,
		forceObsolete bool, meta base.KVMeta) error
	// AddWithDualTierBlobHandles adds a key to the sstable with both a primary and
	// secondary blob handle for dual-tier blob values. This is used when a value exists
	// in multiple tiers simultaneously. The primary handle is encoded in the value column,
	// and the secondary handle is stored in a separate column. See Add for more details.
	AddWithDualTierBlobHandles(key InternalKey, hotHandle, secondaryHandle blob.InlineHandle,
		attr base.ShortAttribute, forceObsolete bool, meta base.KVMeta) error
	// EncodeSpan encodes the keys in the given span. The span can contain
	// either only RANGEDEL keys or only range keys.
	//
	// This is a low-level API that bypasses the fragmenter. The spans passed to
	// this function must be fragmented and ordered.
	EncodeSpan(span keyspan.Span) error
	// EstimatedSize returns the estimated size of the sstable being written if
	// a call to Close() was made without adding additional keys.
	EstimatedSize() uint64
	// ComparePrev compares the provided user key to the last point key written
	// to the writer. The returned value is equivalent to Compare(key, prevKey)
	// where prevKey is the last point key written to the writer.
	//
	// If no key has been written yet, ComparePrev returns +1.
	//
	// Must not be called after Writer is closed.
	ComparePrev(k []byte) int
	// SetSnapshotPinnedProperties sets the properties for pinned keys. Should only
	// be used internally by Pebble.
	SetSnapshotPinnedProperties(keyCount, keySize, valueSize uint64)
	// Close finishes writing the table and closes the underlying file that the
	// table was written to.
	Close() error
	// Metadata returns the metadata for the finished sstable. Only valid to
	// call after the sstable has been finished.
	Metadata() (*WriterMetadata, error)
	// IsLikelyMVCCGarbage determines whether the given user key is likely MVCC
	// garbage according to the previous key written to the writer.
	//
	// We require:
	//	* The previous key to be a SET/SETWITHDEL.
	//	* The current key to be a SET/SETWITHDEL.
	//	* The current key to have the same prefix as the previous key.
	IsLikelyMVCCGarbage(key []byte, kind base.InternalKeyKind) bool

	// SetValueSeparationProps sets the value separation props that were used when
	// writing this sstable. This is recorded in the sstable properties.
	SetValueSeparationProps(minValueSize uint64, disableSeparationBySuffix bool)

	// rewriteSuffixes rewrites the table's data blocks to all contain the
	// provided suffix. It's specifically used for the implementation of
	// RewriteKeySuffixesAndReturnFormat. See that function's documentation for
	// more details.
	rewriteSuffixes(r *Reader, sst []byte, wo WriterOptions, from, to []byte, concurrency int) error

	// copyDataBlocks copies data blocks to the table from the specified ReadHandle.
	// It's specifically used by the sstable copier that can copy parts of an sstable
	// to a new sstable, using CopySpan().
	copyDataBlocks(ctx context.Context, blocks []indexEntry, rh objstorage.ReadHandle) error

	// addDataBlock adds a raw data block to the table as-is. It's specifically used
	// by the sstable copier that can copy parts of an sstable to a new sstable,
	// using CopySpan().
	addDataBlock(b, sep []byte, bhp block.HandleWithProperties) error

	// setFilter sets a pre-populated filter. It is used by the sstable copier
	// that can copy parts of an sstable to a new sstable, using CopySpan().
	//
	// The writer takes ownership of the filterData buffer.
	setFilter(filerData []byte, family base.TableFilterFamily)

	// copyProperties copies properties from the specified props, and resets others
	// to prepare for copying data blocks from another sstable. It's specifically
	// used by the sstable copier that can copy parts of an sstable to a new sstable,
	// using CopySpan().
	copyProperties(props Properties)
}

// WriterMetadata holds info about a finished sstable.
type WriterMetadata struct {
	Size          uint64
	SmallestPoint InternalKey
	// LargestPoint, LargestRangeKey, LargestRangeDel should not be accessed
	// before Writer.Close is called, because they may only be set on
	// Writer.Close.
	LargestPoint     InternalKey
	SmallestRangeDel InternalKey
	LargestRangeDel  InternalKey
	SmallestRangeKey InternalKey
	LargestRangeKey  InternalKey
	HasPointKeys     bool
	HasRangeDelKeys  bool
	HasRangeKeys     bool
	SeqNums          base.SeqNumRange
	Properties       Properties
}

// SetSmallestPointKey sets the smallest point key to the given key.
// NB: this method set the "absolute" smallest point key. Any existing key is
// overridden.
func (m *WriterMetadata) SetSmallestPointKey(k InternalKey) {
	m.SmallestPoint = k
	m.HasPointKeys = true
}

// SetSmallestRangeDelKey sets the smallest rangedel key to the given key.
// NB: this method set the "absolute" smallest rangedel key. Any existing key is
// overridden.
func (m *WriterMetadata) SetSmallestRangeDelKey(k InternalKey) {
	m.SmallestRangeDel = k
	m.HasRangeDelKeys = true
}

// SetSmallestRangeKey sets the smallest range key to the given key.
// NB: this method set the "absolute" smallest range key. Any existing key is
// overridden.
func (m *WriterMetadata) SetSmallestRangeKey(k InternalKey) {
	m.SmallestRangeKey = k
	m.HasRangeKeys = true
}

// SetLargestPointKey sets the largest point key to the given key.
// NB: this method set the "absolute" largest point key. Any existing key is
// overridden.
func (m *WriterMetadata) SetLargestPointKey(k InternalKey) {
	m.LargestPoint = k
	m.HasPointKeys = true
}

// SetLargestRangeDelKey sets the largest rangedel key to the given key.
// NB: this method set the "absolute" largest rangedel key. Any existing key is
// overridden.
func (m *WriterMetadata) SetLargestRangeDelKey(k InternalKey) {
	m.LargestRangeDel = k
	m.HasRangeDelKeys = true
}

// SetLargestRangeKey sets the largest range key to the given key.
// NB: this method set the "absolute" largest range key. Any existing key is
// overridden.
func (m *WriterMetadata) SetLargestRangeKey(k InternalKey) {
	m.LargestRangeKey = k
	m.HasRangeKeys = true
}

func (m *WriterMetadata) updateSeqNum(seqNum base.SeqNum) {
	if m.SeqNums.Low > seqNum {
		m.SeqNums.Low = seqNum
	}
	if m.SeqNums.High < seqNum {
		m.SeqNums.High = seqNum
	}
}

// LoggingRawWriter wraps a sstable.RawWriter and logs calls to Add and
// AddWithBlobHandle to provide observability into the separation of values into
// blob files. This is intended for testing.
type LoggingRawWriter struct {
	LogWriter io.Writer
	RawWriter
}

func (w *LoggingRawWriter) Add(
	key base.InternalKey, value []byte, forceObsolete bool, meta base.KVMeta,
) error {
	fmt.Fprintf(w.LogWriter, "RawWriter.Add(%q, %q, %t)\n", key, value, forceObsolete)
	return w.RawWriter.Add(key, value, forceObsolete, meta)
}

func (w *LoggingRawWriter) AddWithBlobHandle(
	key base.InternalKey,
	h blob.InlineHandle,
	attr base.ShortAttribute,
	forceObsolete bool,
	meta base.KVMeta,
) error {
	fmt.Fprintf(w.LogWriter, "RawWriter.AddWithBlobHandle(%q, %q, %x, %t)\n", key, h, attr, forceObsolete)
	return w.RawWriter.AddWithBlobHandle(key, h, attr, forceObsolete, meta)
}
