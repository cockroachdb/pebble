// Copyright 2025 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package tieredmeta

import (
	"cmp"
	"encoding/binary"
	"maps"
	"slices"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/sstable/colblk"
)

// A TieringHistogramBlockWriter writes a tiering histogram block. The block
// records StatsHistograms for rows being written to a sstable or blob file.
// There is a one-to-one mapping between TieringHistogramBlockWriter and files;
// for example, writing a key-value pair that will be value-separated requires
// two writers: one for the sstable (containing the key) and one for the blob
// file (containing the value).
//
// The tier of the (sstable or blob) file to which the tiering histogram block
// is being written is implicit in the context, and does not need to be stored
// in the histogram.
//
// For an sstable, we want to distinguish which values it refers to are in only
// cold storage. This is useful since before starting a future compaction, we
// want to decide whether to write new blob files, and in which tier, and which
// input blob files will have some spanIDs rewritten. This decision needs to be
// made with the help of these histograms to balance the cost of rewriting with
// the benefit, and to avoid producing tiny blob files. Additionally, we want to
// know which values are inside the sstable and which are blob references, since
// if most of the hot => cold transitions are for values inside the sstable, we
// may choose to not rewrite the blob references since the gain is small.
//
// To handle the above cases, an sstable can have up to 3 histograms for a
// spanID, the ones prefixed with SSTable in the KindAndTier enum. A blob file
// has one histogram for the values it contains, BlobFileValueBytes.
//
// We also track per-sstable summary counters (not full histograms):
//   - Key bytes: total and below-threshold, for future decisions about whether
//     to also tier keys to cold storage.
//   - Hot-and-cold blob reference bytes: values that exist in both tiers. The
//     decision to drop the hot copy is based on storage pressure, not the
//     tiering attribute distribution, so we just track total bytes.
//
// Below is the block structure:
//
//	SSTable's TieringHistogramBlock:
//	│
//	├── SSTableSummary (keyBytesTotal, keyBytesBelowThreshold, hotAndColdBlobRefBytes)
//	├── Key{SSTableInPlaceValueBytes,      SpanID=1} → StatsHistogram
//	├── Key{SSTableBlobReferenceHotBytes,  SpanID=1} → StatsHistogram
//	├── Key{SSTableBlobReferenceColdBytes, SpanID=1} → StatsHistogram
//	└── ...
type KindAndTier uint8

const (
	// SSTableInPlaceValueBytes is the histogram for values stored inside the
	// sstable.
	SSTableInPlaceValueBytes KindAndTier = iota
	// SSTableBlobReferenceHotBytes is the histogram for blob references from the
	// sstable to hot tier blob files only. The size is the size of the value.
	SSTableBlobReferenceHotBytes
	// SSTableBlobReferenceColdBytes is the histogram for blob references from the
	// sstable to cold tier blob files only. The size is the size of the value.
	SSTableBlobReferenceColdBytes
	NumKindAndTiers
)

// Key is a unique identifier for a histogram.
type Key struct {
	KindAndTier
	base.TieringSpanID
}

func (k *Key) encode() []byte {
	buf := make([]byte, 1+binary.MaxVarintLen64)
	buf[0] = byte(k.KindAndTier)
	n := binary.PutUvarint(buf[1:], uint64(k.TieringSpanID))
	return buf[:1+n]
}

func (k *Key) decode(data []byte) error {
	if len(data) < 2 {
		return errors.AssertionFailedf("key too short")
	}
	kt := KindAndTier(data[0])
	if kt >= NumKindAndTiers {
		return errors.AssertionFailedf("invalid KindAndTier %d", kt)
	}
	id, n := binary.Uvarint(data[1:])
	if n <= 0 {
		return errors.AssertionFailedf("cannot decode TieringSpanID")
	}
	*k = Key{KindAndTier: kt, TieringSpanID: base.TieringSpanID(id)}
	return nil
}

// TieringHistogramBlockWriter is instantiated for each output file that
// requires tiering histograms.
type TieringHistogramBlockWriter struct {
	writers map[Key]*histogramWriter
	// Per-sstable summary counters.
	keyBytesTotal          uint64
	keyBytesBelowThreshold uint64
	hotAndColdBlobRefBytes uint64
}

func (w *TieringHistogramBlockWriter) Add(
	kt KindAndTier, spanID base.TieringSpanID, attr base.TieringAttribute, bytes uint64,
) {
	if w.writers == nil {
		w.writers = make(map[Key]*histogramWriter)
	}
	k := Key{kt, spanID}
	hw, ok := w.writers[k]
	if !ok {
		hw = makeHistogramWriter()
		w.writers[k] = hw
	}
	hw.record(attr, bytes)
}

// AddKeyBytes records key bytes for the per-sstable summary. If the key's
// tiering attribute is below the threshold, the bytes are also counted toward
// keyBytesBelowThreshold.
func (w *TieringHistogramBlockWriter) AddKeyBytes(
	attr base.TieringAttribute, threshold base.TieringAttribute, bytes uint64,
) {
	w.keyBytesTotal += bytes
	if attr < threshold {
		w.keyBytesBelowThreshold += bytes
	}
}

// AddHotAndColdBlobRefBytes records bytes for values that exist in both hot and
// cold tiers. These don't need a full histogram since the decision to drop the
// hot copy is based on storage pressure, not the tiering attribute distribution.
func (w *TieringHistogramBlockWriter) AddHotAndColdBlobRefBytes(bytes uint64) {
	w.hotAndColdBlobRefBytes += bytes
}

// reset clears the writer to an empty state, allowing it to be reused.
func (w *TieringHistogramBlockWriter) reset() {
	w.writers = nil
	w.keyBytesTotal = 0
	w.keyBytesBelowThreshold = 0
	w.hotAndColdBlobRefBytes = 0
}

// IsEmpty returns true if no histograms have been added to the histogram block
// and the per-sstable summary fields (keyBytesTotal, etc.) are zero.
func (w *TieringHistogramBlockWriter) IsEmpty() bool {
	return len(w.writers) == 0 && w.keyBytesTotal == 0 &&
		w.keyBytesBelowThreshold == 0 && w.hotAndColdBlobRefBytes == 0
}

// Finish returns the encoded block contents. The writer is reset and can be
// reused after calling this method. The encoding format is:
//
//	<keyBytesTotal> <keyBytesBelowThreshold> <hotAndColdBlobRefBytes> <histograms>
func (w *TieringHistogramBlockWriter) Finish() []byte {
	var cw colblk.KeyValueBlockWriter
	cw.Init()
	keys := slices.Collect(maps.Keys(w.writers))
	slices.SortFunc(keys, func(a, b Key) int {
		return cmp.Or(
			cmp.Compare(a.KindAndTier, b.KindAndTier), cmp.Compare(a.TieringSpanID, b.TieringSpanID))
	})
	for _, k := range keys {
		cw.AddKV(k.encode(), w.writers[k].encode())
	}
	histograms := cw.Finish(cw.Rows())

	// 3*binary.MaxVarintLen64 for the per-sstable summary, +len(histograms) for
	// the histograms.
	buf := make([]byte, 0, 3*binary.MaxVarintLen64+len(histograms))
	buf = binary.AppendUvarint(buf, w.keyBytesTotal)
	buf = binary.AppendUvarint(buf, w.keyBytesBelowThreshold)
	buf = binary.AppendUvarint(buf, w.hotAndColdBlobRefBytes)
	buf = append(buf, histograms...)

	w.reset()
	return buf
}

// SSTableSummary is the per-sstable summary counters (not full histograms).
type SSTableSummary struct {
	// KeyBytesTotal is the total key bytes in the sstable.
	KeyBytesTotal uint64
	// KeyBytesBelowThreshold is key bytes with tiering attribute below threshold.
	KeyBytesBelowThreshold uint64
	// HotAndColdBlobRefBytes is bytes for values that exist in both hot and cold
	// tiers. These don't need a histogram since the decision to drop the hot copy
	// is based on storage pressure, not the tiering attribute distribution.
	HotAndColdBlobRefBytes uint64
}

// DecodeTieringHistogramBlock decodes the tiering histogram block and returns
// the in-memory contents: the per-sstable summary and the per-spanID histograms.
func DecodeTieringHistogramBlock(data []byte) (SSTableSummary, map[Key]StatsHistogram, error) {
	var summary SSTableSummary
	var n int

	summary.KeyBytesTotal, n = binary.Uvarint(data)
	if n <= 0 {
		return SSTableSummary{}, nil, errors.AssertionFailedf("cannot decode KeyBytesTotal")
	}
	data = data[n:]

	summary.KeyBytesBelowThreshold, n = binary.Uvarint(data)
	if n <= 0 {
		return SSTableSummary{}, nil, errors.AssertionFailedf("cannot decode KeyBytesBelowThreshold")
	}
	data = data[n:]

	summary.HotAndColdBlobRefBytes, n = binary.Uvarint(data)
	if n <= 0 {
		return SSTableSummary{}, nil, errors.AssertionFailedf("cannot decode HotAndColdBlobRefBytes")
	}
	data = data[n:]

	var histograms map[Key]StatsHistogram
	var decoder colblk.KeyValueBlockDecoder
	decoder.Init(data)
	for i := range decoder.BlockDecoder().Rows() {
		var k Key
		err := k.decode(decoder.KeyAt(i))
		if err != nil {
			return SSTableSummary{}, nil, err
		}
		hist, err := DecodeStatsHistogram(decoder.ValueAt(i))
		if err != nil {
			return SSTableSummary{}, nil, err
		}
		if histograms == nil {
			histograms = make(map[Key]StatsHistogram)
		}
		histograms[k] = hist
	}
	return summary, histograms, nil
}
