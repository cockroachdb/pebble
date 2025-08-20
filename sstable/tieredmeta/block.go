// Copyright 2025 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package tieredmeta

import (
	"cmp"
	"fmt"
	"slices"
	"strconv"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/sstable/colblk"
)

// A TieringHistogramBlockWriter writes a tiering histogram block. The block
// records StatsHistograms for rows being written to a sstable or blob file.
// If the key and value are being written to different files, there will be a
// different TieringHistogramBlockWriter for each file.
//
// The tier of the (sstable or blob) file to which the tiering histogram block
// is being written is implicit in the context, and does not need to be stored
// in the histogram.
//
// We do want to distinguish between key bytes and value bytes, since even
// though the keys will always be in the hot tier for now, it helps us
// understand how much gains we could have by storing sstables in the cold
// tier too.
//
// For a sstable, we want to distinguish which values it refers to are in hot
// storage and which in cold storage. This is useful since before starting a
// future compaction, we want to decide whether to write new blob files, and
// in which tier, and which input blob files will have some spanIDs rewritten.
// This decision needs to be made with the help of these histograms to balance
// the cost of rewriting with the benefit, and to avoid producing tiny blob
// files. Additionally, we want to know which values are inside the sstable
// and which are blob references, since if most of the hot => cold transitions
// are for values inside the sstable, we may choose to not rewrite the blob
// references since the gain is small.
//
// To handle the above cases, an sstable can have up to 4 histograms for a
// spanID, the ones prefixed with SSTable in the KindAndTier enum. A blob file
// has one histogram for the values it contains, BlobFileValueBytes.
//
// For observability of the live bytes and their TieringAttribute, one can
// sum:
//
// SSTableKeyBytes + SSTableValueBytes + SSTableBlobReferenceHotBytes +
// SSTableBlobReferenceColdBytes
//
// Additionally,
//
// SSTableBlobReferenceHotBytes + SSTableBlobReferenceColdBytes <=
// BlobFileValueBytes
//
// since the blob file may contain values that are not referenced by any
// sstable.
//
// Why do we need the BlobFileValueBytes histogram? We could use it to copy a
// blob file from hot to cold tier, or vice versa. Additionally, it could be
// used to compare the sum of the stats for a spanID for the input sstables of
// a compaction, with the sum of the stats of the spanID for blob files
// referenced by the sstables in the compaction, to compute whether enough of
// a spanID will move between tiers to justify rewriting the references.
//
// TODO(sumeer): the above is quite handwavy. It will become clearer when we
// start implementing the decision functions. We err on the side of more
// information for now, since in addition to decision making, it helps with
// observability.

type KindAndTier uint8

const (
	// SSTableKeyBytes is the histogram for sstable keys. The tier is implicit
	// based on whether the sstable is in hot or cold storage.
	SSTableKeyBytes KindAndTier = iota
	// SSTableValueBytes is the histogram for values stored inside the
	// sstable. The tier is implicit based on whether the sstable is in hot or
	// cold storage.
	SSTableValueBytes
	// SSTableBlobReferenceHotBytes is the histogram for blob references from the
	// sstable to hot tier blob files. The size is the size of the value.
	SSTableBlobReferenceHotBytes
	// SSTableBlobReferenceColdBytes is the histogram for blob references from the
	// sstable to cold tier blob files. The size is the size of the value.
	SSTableBlobReferenceColdBytes
	// BlobFileValueBytes is the histogram for values stored inside the blob
	// file. The tier is implicit based on whether the blob file is in hot or
	// cold storage.
	BlobFileValueBytes
	NumKindAndTiers
)

type ColdTierThresholdRetriever interface {
	// GetColdTierLTThreshold returns the current cold threshold for the given
	// spanID. See the comment in histogram.go for how this is currently used,
	// which is subject to revision.
	//
	// REQUIRES: spanID > 0.
	GetColdTierLTThreshold(spanID base.TieringSpanID) base.TieringAttribute
}

// TieringHistogramBlockWriter is instantiated for each sstable or blob file
// being written.
type TieringHistogramBlockWriter struct {
	retriever ColdTierThresholdRetriever
	writers   map[Key]*histogramWriter
}

type Key struct {
	KindAndTier
	base.TieringSpanID
}

func (k *Key) encode() []byte {
	return append([]byte{byte(k.KindAndTier)}, []byte(fmt.Sprintf("%08d", k.TieringSpanID))...)
}

func (k *Key) decode(data []byte) error {
	if len(data) == 0 {
		return errors.AssertionFailedf("key too short")
	}
	kt := KindAndTier(data[0])
	if kt >= NumKindAndTiers {
		return errors.AssertionFailedf("invalid KindAndTier %d", kt)
	}
	id, err := strconv.ParseUint(string(data[1:]), 10, 64)
	if err != nil {
		return err
	}
	*k = Key{kt, base.TieringSpanID(id)}
	return nil
}

func (w *TieringHistogramBlockWriter) Init(retriever ColdTierThresholdRetriever) {
	*w = TieringHistogramBlockWriter{
		retriever: retriever,
		writers:   make(map[Key]*histogramWriter),
	}
}

func (w *TieringHistogramBlockWriter) Add(
	kt KindAndTier, spanID base.TieringSpanID, attr base.TieringAttribute, bytes uint64,
) {
	k := Key{kt, spanID}
	hw, ok := w.writers[k]
	if !ok {
		var t base.TieringAttribute
		if spanID != 0 {
			t = w.retriever.GetColdTierLTThreshold(spanID)
		}
		hw = newHistogramWriter(t)
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
	clear(w.writers)
	w.writers = nil
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
	for i := 0; i < decoder.BlockDecoder().Rows(); i++ {
		var k Key
		err := k.decode(decoder.KeyAt(i))
		if err != nil {
			return TieringHistogramBlockContents{}, err
		}
		var hist StatsHistogram
		if err := hist.decode(decoder.ValueAt(i)); err != nil {
			return TieringHistogramBlockContents{}, err
		}
		if c.histograms == nil {
			c.histograms = make(map[Key]StatsHistogram)
		}
		c.histograms[k] = hist
	}
	return c, nil
}
