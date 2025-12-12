// Copyright 2025 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package tieredmeta

import (
	"cmp"
	"encoding/binary"
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
// We do want to distinguish between key bytes and value bytes, since even
// though the keys will always be in the hot tier for now, it helps us
// understand how much gain we *could* have if we were to store sstables in the
// cold tier too.
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
// To handle the above cases, an sstable can have up to 4 histograms for a
// spanID, the ones prefixed with SSTable in the KindAndTier enum. A blob file
// has one histogram for the values it contains, BlobFileValueBytes.
//
// Below is the block structure:
//
//	SSTable's TieringHistogramBlock:
//	│
//	├── Key{SSTableKeyBytes,                SpanID=1} → StatsHistogram
//	├── Key{SSTableInPlaceValueBytes,       SpanID=1} → StatsHistogram
//	├── Key{SSTableBlobReferenceHotBytes,   SpanID=1} → StatsHistogram
//	├── Key{SSTableBlobReferenceColdBytes,  SpanID=1} → StatsHistogram
//	├── Key{SSTableKeyBytes,                SpanID=2} → StatsHistogram
//	└── ...
//
// TODO(annie): For observability of the live bytes and their TieringAttribute,
// one can sum:
//
// SSTableKeyBytes + SSTableInPlaceValueBytes + SSTableBlobReferenceHotBytes +
// SSTableBlobReferenceColdBytes
type KindAndTier uint8

const (
	// SSTableKeyBytes is the histogram for sstable keys. The tier is implicit
	// based on whether the sstable is in hot or cold storage.
	SSTableKeyBytes KindAndTier = iota
	// SSTableInPlaceValueBytes is the histogram for values stored inside the
	// sstable.
	SSTableInPlaceValueBytes
	// SSTableBlobReferenceHotBytes is the histogram for blob references from the
	// sstable to hot tier blob files. The size is the size of the value.
	SSTableBlobReferenceHotBytes
	// SSTableBlobReferenceColdBytes is the histogram for blob references from the
	// sstable to cold tier blob files. The size is the size of the value.
	SSTableBlobReferenceColdBytes
	NumKindAndTiers
)

// TieringHistogramBlockWriter is instantiated for each sstable or blob file
// being written.
type TieringHistogramBlockWriter struct {
	writers map[Key]*histogramWriter
}

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

func NewTieringHistogramBlockWriter() *TieringHistogramBlockWriter {
	return &TieringHistogramBlockWriter{
		writers: make(map[Key]*histogramWriter),
	}
}

func (w *TieringHistogramBlockWriter) Add(
	kt KindAndTier, spanID base.TieringSpanID, attr base.TieringAttribute, bytes uint64,
) {
	k := Key{kt, spanID}
	hw, ok := w.writers[k]
	if !ok {
		hw = newHistogramWriter()
		w.writers[k] = hw
	}
	hw.record(attr, bytes)
}

// Flush returns the encoded block contents.
func (w *TieringHistogramBlockWriter) Flush() []byte {
	var cw colblk.KeyValueBlockWriter
	cw.Init()
	keys := make([]Key, len(w.writers))
	i := 0
	for k := range w.writers {
		keys[i] = k
		i++
	}
	slices.SortFunc(keys, func(a, b Key) int {
		return cmp.Or(
			cmp.Compare(a.KindAndTier, b.KindAndTier), cmp.Compare(a.TieringSpanID, b.TieringSpanID))
	})
	for _, k := range keys {
		cw.AddKV(k.encode(), w.writers[k].encode())
	}
	return cw.Finish(cw.Rows())
}

// TieringHistogramBlockContents is the in-memory contents of the tiering
// histogram block. We may temporarily load it into TableMetadata for
// convenience, like we do for TableStats, but we don't need to long term,
// since the memory overhead is likely too high.
type TieringHistogramBlockContents struct {
	histograms map[Key]StatsHistogram
}

func DecodeTieringHistogramBlock(data []byte) (TieringHistogramBlockContents, error) {
	var c TieringHistogramBlockContents
	var decoder colblk.KeyValueBlockDecoder
	decoder.Init(data)
	for i := range decoder.BlockDecoder().Rows() {
		var k Key
		err := k.decode(decoder.KeyAt(i))
		if err != nil {
			return TieringHistogramBlockContents{}, err
		}
		hist, err := DecodeStatsHistogram(decoder.ValueAt(i))
		if err != nil {
			return TieringHistogramBlockContents{}, err
		}
		if c.histograms == nil {
			c.histograms = make(map[Key]StatsHistogram)
		}
		c.histograms[k] = hist
	}
	return c, nil
}
