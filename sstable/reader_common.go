// Copyright 2024 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package sstable

import (
	"context"

	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/keyspan"
	"github.com/cockroachdb/pebble/sstable/block"
)

// CommonReader abstracts functionality over a Reader or a VirtualReader. This
// can be used by code which doesn't care to distinguish between a reader and a
// virtual reader.
type CommonReader interface {
	NewRawRangeKeyIter(transforms FragmentIterTransforms) (keyspan.FragmentIterator, error)

	NewRawRangeDelIter(transforms FragmentIterTransforms) (keyspan.FragmentIterator, error)

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
		bufferPool *block.BufferPool,
	) (Iterator, error)

	EstimateDiskUsage(start, end []byte) (uint64, error)

	CommonProperties() *CommonProperties
}

type (
	// BufferPool re-exports block.BufferPool.
	BufferPool = block.BufferPool
	// IterTransforms re-exports block.IterTransforms.
	IterTransforms = block.IterTransforms
	// FragmentIterTransforms re-exports block.FragmentIterTransforms.
	FragmentIterTransforms = block.FragmentIterTransforms
	// SyntheticSeqNum re-exports block.SyntheticSeqNum.
	SyntheticSeqNum = block.SyntheticSeqNum
	// SyntheticSuffix re-exports block.SyntheticSuffix.
	SyntheticSuffix = block.SyntheticSuffix
	// SyntheticPrefix re-exports block.SyntheticPrefix.
	SyntheticPrefix = block.SyntheticPrefix
)

// NoTransforms is the default value for IterTransforms.
var NoTransforms = block.NoTransforms

// NoFragmentTransforms is the default value for FragmentIterTransforms.
var NoFragmentTransforms = block.NoFragmentTransforms

// NoSyntheticSeqNum is the default zero value for SyntheticSeqNum, which
// disables overriding the sequence number.
const NoSyntheticSeqNum = block.NoSyntheticSeqNum
