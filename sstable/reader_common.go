// Copyright 2024 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package sstable

import (
	"context"

	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/keyspan"
)

// CommonReader abstracts functionality over a Reader or a VirtualReader. This
// can be used by code which doesn't care to distinguish between a reader and a
// virtual reader.
type CommonReader interface {
	NewRawRangeKeyIter(transforms IterTransforms) (keyspan.FragmentIterator, error)

	NewRawRangeDelIter(transforms IterTransforms) (keyspan.FragmentIterator, error)

	NewIterWithBlockPropertyFiltersAndContextEtc(
		ctx context.Context,
		transforms IterTransforms,
		lower, upper []byte,
		filterer *BlockPropertiesFilterer,
		useFilterBlock bool,
		stats *base.InternalIteratorStats,
		categoryAndQoS CategoryAndQoS,
		statsCollector *CategoryStatsCollector,
		rp ReaderProvider,
	) (Iterator, error)

	NewCompactionIter(
		transforms IterTransforms,
		categoryAndQoS CategoryAndQoS,
		statsCollector *CategoryStatsCollector,
		rp ReaderProvider,
		bufferPool *BufferPool,
	) (Iterator, error)

	EstimateDiskUsage(start, end []byte) (uint64, error)

	CommonProperties() *CommonProperties
}

// IterTransforms allow on-the-fly transformation of data at iteration time.
//
// These transformations could in principle be implemented as block transforms
// (at least for non-virtual sstables), but applying them during iteration is
// preferable.
type IterTransforms struct {
	SyntheticSeqNum    SyntheticSeqNum
	HideObsoletePoints bool
	SyntheticPrefix    SyntheticPrefix
	SyntheticSuffix    SyntheticSuffix
}

// NoTransforms is the default value for IterTransforms.
var NoTransforms = IterTransforms{}

// SyntheticSeqNum is used to override all sequence numbers in a table. It is
// set to a non-zero value when the table was created externally and ingested
// whole.
type SyntheticSeqNum base.SeqNum

// NoSyntheticSeqNum is the default zero value for SyntheticSeqNum, which
// disables overriding the sequence number.
const NoSyntheticSeqNum SyntheticSeqNum = 0
