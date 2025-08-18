// Copyright 2025 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package tieredmeta

import (
	"fmt"
	"slices"
	"strconv"

	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/sstable/colblk"
)

// A tiering-histogram block maps a TieringSpanID to a StatsHistogram. It is
// written to sstables and blob files. It is read for aggregating stats over the
// DB. Additionally, it is needed for all the inputs to a compaction, to decide
// up front what hot to cold transitions should be attempted.
//
// We may temporarily load them into TableMetadata for convenience, like we do
// for TableStats, but we don't need to long term, if the memory overhead is
// too high.

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
	writers   map[base.TieringSpanID]*histogramWriter
}

func NewTieringHistogramBlockWriter(
	retriever ColdTierThresholdRetriever,
) *TieringHistogramBlockWriter {
	return &TieringHistogramBlockWriter{
		retriever: retriever,
		writers:   make(map[base.TieringSpanID]*histogramWriter),
	}
}

func (w *TieringHistogramBlockWriter) Add(
	spanID base.TieringSpanID, attr base.TieringAttribute, bytes uint64,
) {
	hw, ok := w.writers[spanID]
	if !ok {
		var t base.TieringAttribute
		if spanID != 0 {
			t = w.retriever.GetColdTierLTThreshold(spanID)
		}
		hw = newHistogramWriter(t)
		w.writers[spanID] = hw
	}
	hw.record(attr, bytes)
}

// Flush returns the encoded block contents.
func (w *TieringHistogramBlockWriter) Flush() []byte {
	var cw colblk.KeyValueBlockWriter
	cw.Init()
	spanIDs := make([]base.TieringSpanID, len(w.writers))
	i := 0
	for spanID := range w.writers {
		spanIDs[i] = spanID
		i++
	}
	slices.Sort(spanIDs)
	for _, id := range spanIDs {
		cw.AddKV([]byte(fmt.Sprintf("%08d", id)), w.writers[id].encode())
	}
	return cw.Finish(cw.Rows())
}

type TieringHistogramBlockContents struct {
	histograms map[base.TieringSpanID]StatsHistogram
}

func DecodeTieringHistogramBlock(data []byte) (TieringHistogramBlockContents, error) {
	var c TieringHistogramBlockContents
	var decoder colblk.KeyValueBlockDecoder
	decoder.Init(data)
	for i := 0; i < decoder.BlockDecoder().Rows(); i++ {
		key := decoder.KeyAt(i)
		value := decoder.ValueAt(i)
		id, err := strconv.ParseUint(string(key), 10, 64)
		if err != nil {
			return TieringHistogramBlockContents{}, err
		}
		var hist StatsHistogram
		if err := hist.decode(value); err != nil {
			return TieringHistogramBlockContents{}, err
		}
		if c.histograms == nil {
			c.histograms = make(map[base.TieringSpanID]StatsHistogram)
		}
		c.histograms[base.TieringSpanID(id)] = hist
	}
	return c, nil
}
