// Copyright 2024 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package sstable

import (
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/keyspan"
	"github.com/cockroachdb/pebble/objstorage"
)

// Writer is a table writer.
type Writer struct {
	rw RawWriter
	// To allow potentially overlapping (i.e. un-fragmented) range keys spans to
	// be added to the Writer, a keyspan.Fragmenter is used to retain the keys
	// and values, emitting fragmented, coalesced spans as appropriate. Range
	// keys must be added in order of their start user-key.
	fragmenter  keyspan.Fragmenter
	comparer    *base.Comparer
	rkBuf       []byte
	keyspanKeys []keyspan.Key
}

// NewWriter returns a new table writer intended for building external sstables
// (eg, for ingestion or storage outside the LSM) for the file. Closing the
// writer will close the file.
//
// Internal clients should generally prefer NewRawWriter.
func NewWriter(writable objstorage.Writable, o WriterOptions) *Writer {
	o = o.ensureDefaults()
	rw := NewRawWriter(writable, o)
	return &Writer{
		rw: rw,
		fragmenter: keyspan.Fragmenter{
			Cmp:    o.Comparer.Compare,
			Format: o.Comparer.FormatKey,
			Emit:   rw.encodeFragmentedRangeKeySpan,
		},
		comparer: o.Comparer,
	}
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
	if err := w.rw.Error(); err != nil {
		return err
	}
	if w.rw.IsStrictObsolete() {
		return errors.Errorf("use AddWithForceObsolete")
	}
	// forceObsolete is false based on the assumption that no RANGEDELs in the
	// sstable delete the added points.
	return w.rw.AddWithForceObsolete(base.MakeInternalKey(key, 0, InternalKeyKindSet), value, false)
}

// Delete deletes the value for the given key. The sequence number is set to
// 0. Intended for use to externally construct an sstable before ingestion into
// a DB.
//
// TODO(peter): untested
func (w *Writer) Delete(key []byte) error {
	if err := w.rw.Error(); err != nil {
		return err
	}
	if w.rw.IsStrictObsolete() {
		return errors.Errorf("use AddWithForceObsolete")
	}
	// forceObsolete is false based on the assumption that no RANGEDELs in the
	// sstable delete the added points.
	return w.rw.AddWithForceObsolete(base.MakeInternalKey(key, 0, InternalKeyKindDelete), nil, false)
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
	if err := w.rw.Error(); err != nil {
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
	if err := w.rw.Error(); err != nil {
		return err
	}
	if w.rw.IsStrictObsolete() {
		return errors.Errorf("use AddWithForceObsolete")
	}
	// forceObsolete is false based on the assumption that no RANGEDELs in the
	// sstable that delete the added points. If the user configured this writer
	// to be strict-obsolete, addPoint will reject the addition of this MERGE.
	return w.rw.AddWithForceObsolete(base.MakeInternalKey(key, 0, InternalKeyKindMerge), value, false)
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
	return w.rw.Error()
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

// Close finishes writing the table and closes the underlying file that the
// table was written to.
func (w *Writer) Close() (err error) {
	if w.rw.Error() == nil {
		// Write the range-key block, flushing any remaining spans from the
		// fragmenter first.
		w.fragmenter.Finish()
	}
	return w.rw.Close()
}

// RawWriter defines an interface for sstable writers. Implementations may vary
// depending on the TableFormat being written.
type RawWriter interface {
	// Error returns the current accumulated error if any.
	Error() error
	// IsStrictObsolete returns true if the writer is configured to write and
	// enforce a 'strict obsolete' sstable.
	IsStrictObsolete() bool
	// AddWithForceObsolete must be used when writing a strict-obsolete sstable.
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
	AddWithForceObsolete(
		key InternalKey, value []byte, forceObsolete bool,
	) error
	// EncodeSpan encodes the keys in the given span. The span can contain
	// either only RANGEDEL keys or only range keys.
	//
	// This is a low-level API that bypasses the fragmenter. The spans passed to
	// this function must be fragmented and ordered.
	EncodeSpan(span keyspan.Span) error
	// Close finishes writing the table and closes the underlying file that the
	// table was written to.
	Close() error
	// Metadata returns the metadata for the finished sstable. Only valid to
	// call after the sstable has been finished.
	Metadata() (*WriterMetadata, error)
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
	SmallestSeqNum   base.SeqNum
	LargestSeqNum    base.SeqNum
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
	if m.SmallestSeqNum > seqNum {
		m.SmallestSeqNum = seqNum
	}
	if m.LargestSeqNum < seqNum {
		m.LargestSeqNum = seqNum
	}
}
